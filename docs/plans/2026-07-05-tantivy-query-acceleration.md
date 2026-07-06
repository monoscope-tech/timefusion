# Tantivy & Query Acceleration — Execution Plan (2026-07-05)

> **Status (2026-07-05, same-day implementation):** Phases 1, 2, 4, 5 fully
> implemented; Phase 3 implemented except full 3.3 (the manifest remains the
> read-side catalog — now TTL-cached per service — with mirrored per-file
> blobs live on the flush path, post-OPTIMIZE reindex, and the backfill job;
> a pure snapshot-derived read path can follow once backfill converges).
> Notable deltas from the plan:
> - Manifest `mutate` is now serialized per (table, project) — the
>   lost-upsert race the module doc claimed a lock for never had one.
> - `build_index_for_file` gained a `parquet_uri` param so its
>   `covered_files` match the URI-keyed coverage gate / GC (pre-existing
>   Phase-3 blocker).
> - Dedup skip (Phase 5) uses per-(project, date) partition file-set
>   fingerprints captured on 0-drop sweep passes instead of a single
>   swept-through watermark — no write-path hooks, invalidated by any
>   commit that changes the partition's files. Only Delta-only scans skip;
>   flag `TIMEFUSION_READ_DEDUP_SKIP_SWEPT` default off.
> - File pruning derives its selection inside `scan_delta_table` from the
>   same DeltaTable snapshot the scan uses (no list/scan version race);
>   flag `TIMEFUSION_TANTIVY_FILE_PRUNING` default on.

Goal: make selective lookups over old (week/month) windows approach point-lookup
cost, and remove per-query taxes from the scan path. Ordered by
value-per-risk; each phase is independently shippable and gated by tests.

## Current-state facts the plan builds on (verified in code)

- **F1** — Every prefilter query loads + parses the manifest JSON from S3
  (`search.rs:89`), and searches indexes **serially, once per predicate**
  (`database.rs:5696` loops predicates; `search.rs:99` loops entries with
  `.await` inside). Same index set opened N× for N predicates.
- **F2** — The flush path writes **flat random-UUID blobs** keyed
  `bucket-{uuid}` (`service.rs:87-93`). The partition-mirrored 1:1 path
  (`store::index_path_for_parquet`, `build_index_for_file`) exists but is
  **not wired into the live path** (`service.rs:99-101`).
- **F3** — `gc_after_compaction` (`service.rs:175`) **deletes** indexes for
  compacted-away files and nothing rebuilds them → post-OPTIMIZE, the
  coverage gate (`database.rs:5758`) disables the prefilter for those
  windows **forever**. This is the direct cause of slow old-window lookups.
- **F4** — OR subtrees and IN-lists are never routed
  (`udf.rs:collect_text_matches` skips OR; rewriter never touches IN).
- **F5** — Hits are fetched via TopDocs + doc-store lookup per hit
  (`reader.rs:30-37`); `open_index` + `index.reader()` re-run per query per
  index (`search.rs:118`).
- **F6** — DedupExec suppresses per-scan limit pushdown on every query of a
  dedup-keys table (`database.rs:5823`).
- **F7** — Already fixed since the June notes (verify only, no work):
  metadata-cache limit reaches the RuntimeEnv (`database.rs:4805` + test
  `runtime_env_applies_metadata_cache_limit`); `target_partitions` derives
  from the cgroup quota (`autotune.rs:159`).

---

## Phase 1 — Read-path quick wins (no format changes, no migration)

**1.1 Single search pass per index (boolean tree groundwork).**
Replace the per-predicate `search_with_stats` loop in
`ProjectRoutingTable::scan` with ONE call that takes all predicates.
Inside `search_with_stats`, per index: build one tantivy `BooleanQuery`
(leaf = per-field `QueryParser` query, `Occur::Must` for conjuncts), run
once, get one hit set. Union across indexes (indexes cover disjoint rows).
Removes the N× open/search and the `indexed_rows` max() bookkeeping.
- Files: `search.rs`, `database.rs` (scan), new shared query-builder fn used
  by `mem_index.rs` too.
- Test: unit test in `search.rs` — two predicates, hits = per-index AND;
  existing sqllogictest suite green.

**1.2 Parallelize per-index download+search.**
`futures::stream::iter(entries).map(...).buffer_unordered(8)` over the
per-entry work (download → open → search). Dedup/cap logic already
order-independent; keep the `max_hits` abort by short-circuiting the stream.
- Test: e2e latency not asserted; correctness via existing tests. Add a
  unit test that N entries all contribute hits.

**1.3 Reader/handle LRU cache.**
`DashMap`-backed LRU (or `moka`) of `blob_path → (Index, IndexReader)` in
`TantivySearchService`, capacity from a new config
`TIMEFUSION_TANTIVY_READER_CACHE_ENTRIES` (default 256). Invalidation:
entries are immutable per blob path (new data = new blob), so eviction-only.
- Test: unit — second search doesn't re-open (assert via counter).

**1.4 In-memory manifest cache.**
Cache parsed `Manifest` per (table, project) with a short TTL (default 5 s,
config) + write-through invalidation on local `upsert`/`remove_many`/gc.
Removes the per-query S3 GET + JSON parse without changing the format.
(Superseded later by Phase 3 discovery, but 20 lines now.)
- Test: unit — loader hit-counts; TTL expiry path.

**1.5 Fast-field hits (opportunistic).**
Add `_id` as a FAST field in `schema.rs` for **newly built** indexes (no
SCHEMA_VERSION bump — bumping would invalidate every existing index via the
`schema_version` check and disable the prefilter fleet-wide until reindex).
`query_index` reads `_timestamp`/`_id` via fast-field columnar access when
present, doc-store fallback otherwise.
- Test: unit — build index, assert fast path taken (feature-detect), hits
  identical to doc-store path.

## Phase 2 — Boolean predicate routing (OR / IN-lists)

**2.1 Predicate tree collector.**
Replace flat `collect_text_matches` with
`collect_text_match_tree(&[Expr]) -> PredTree` where
`PredTree = And(Vec<PredTree>) | Or(Vec<PredTree>) | Leaf(TextMatchPred) | Opaque`.
Soundness rules (the whole design):
- `And`: intersect children that produce hit sets; `Opaque` children are
  simply not narrowed by (sound — prefilter stays a superset).
- `Or`: **all** children must produce hit sets, else the whole node is
  `Opaque` (a missing branch would make the union a non-superset).
- Keep the flat collector's output shape for the pure-conjunction case so
  the MemBuffer path (2.4) can stay incremental.

**2.2 Rewriter: route IN-lists.**
`col IN ('a','b',...)` on raw-tokenized columns → AND-append
`text_match(col, ...)` per the same safety gates as `=` (eq-term-safe
chars, raw tokenizer, `route_equality` flag; cap list length, config
default 100). Under the tree collector an IN-list is an OR of leaves.
`text_match` UDF row-fallback semantics: extend to OR across
whitespace?  No — keep one `text_match` call per list item; the UDF stays
unchanged.
- Files: `tantivy_rewriter.rs`, `udf.rs`.
- Tests: rewriter unit tests (IN routed, NOT IN never, unsafe literal
  bails); collector tests (OR of routable leaves routes; OR with one
  unroutable branch is Opaque — regression guard for the 2026-06-16 bug);
  sqllogictest: `trace_id IN (...)` and `a='x' OR b='y'` return identical
  rows with prefilter on/off.

**2.3 Delta-side evaluation.**
Per index, translate `PredTree` to one tantivy `BooleanQuery`
(And→Must, Or→Should with minimum_should_match=1 semantics via BooleanQuery
of Shoulds; Leaf→per-field parsed query). Per-index hit set → union across
indexes. `field_coverage_gap` = any in-window index missing **any** field
referenced by a non-Opaque part of the tree (conservative, as today).
Selectivity/max_hits/coverage gates unchanged.
- Test: unit in `search.rs` — OR across two indexes unions; AND intersects
  within an index.

**2.4 MemBuffer side.**
`BucketTextIndex::search` gains a tree variant using the same shared
query-builder. Until then (or if tree contains Or): membuffer applies **no
prefilter** for non-conjunctive trees (SQL filter is the backstop) — never
intersect per-pred sets across an OR.
- Test: mem_buffer unit — OR query over bucket returns both sides.

## Phase 3 — Path unification: snapshot-derived index discovery

Completes the partition-mirror migration (phases 3-full/4/5 of the
2026-06-30 tantivy plan). End state: the Delta snapshot IS the index
catalog; manifest survives only as a legacy fallback until backfill
completes, then read path stops loading it.

**3.1 Flush writes per-parquet-file mirrored indexes.**
In the flush callback (`service.rs:build_and_publish`): group the flushed
batches by `(project_id, date)` partition values and match each group to
the added file with the same partition (the flush commit adds one file per
partition in practice). If >1 added file shares a partition (can't
attribute rows), fall back to `build_index_for_file` read-back per file.
Write blob at `index_path_for_parquet`, manifest entry keyed by the
table-relative parquet path (kept during transition for min/max + covered
bookkeeping).
- Test: e2e — flush, assert blob exists at the mirrored path and prefilter
  still engages (coverage gate passes).

**3.2 Compaction reindex (fixes F3 — the old-window cliff).**
After OPTIMIZE commits, for each surviving/new file lacking a mirrored
index: `build_index_for_file` (bounded concurrency, spawn_blocking build).
Then `gc_after_compaction` deletes blobs for dead files. Order:
build-new → gc-old, so coverage never regresses.
- Test: e2e — write, flush, optimize, then a `trace_id =` query over the
  compacted window still uses the prefilter (assert via
  `timefusion_stats`/metrics counter `prefilter_used`).

**3.3 Read-side snapshot discovery.**
In `scan`: the coverage-gate file listing (`database.rs:5429`) becomes the
*driver*: in-window live files → suffix-swap → candidate blob paths →
local-cache check / existence probe (negative-result cache with TTL).
Files with a mirrored index: open + query. Files without: consult the
(now cached) manifest for a legacy `bucket-` entry covering that file.
Any file covered by neither → prefilter off (exact same rule as today's
coverage gate, but computed in one pass and without loading the manifest
when everything is mirrored). Per-file min/max time-pruning comes from the
`date=` partition value at day granularity (already proven sound for the
gate); Delta add-action minValues/maxValues stats can tighten this later.

**3.4 Backfill job.**
Startup/interval background task (config-gated,
`TIMEFUSION_TANTIVY_BACKFILL=off|startup|interval`): list live files per
indexed (project, table), diff against mirrored blobs
(`index_to_parquet_rel` inverse mapping), `build_index_for_file` the gap,
oldest-partition-first, bounded concurrency (default 2) + bandwidth
politeness. Reconcile also deletes orphan blobs.
- Test: integration — table with pre-existing un-indexed files gains
  mirrored indexes; orphan blob removed.

## Phase 4 — File-level pruning + prefetch (the "super fast lookup" payoff)

**4.1 Per-file hit attribution → prune the Delta scan set.**
Requires 3.3 (1:1 index↔file). Search returns `HashMap<file_rel, HitSet>`.
Files whose index ran and returned zero hits are **excluded from the scan**:
build the `DeltaTableProvider` via `.with_files(surviving_adds)` (fork
exposes it; bypass the provider cache when a restriction is present).
The `id IN (...)` expr stays for surviving files (row-group pruning inside
them). On `max_hits` abort: keep file-level exclusions computed so far
only if every in-window file was searched (else abort entirely, as today) —
"which files" stays bounded even when "which rows" isn't.
- Tests: e2e — needle in 1 of N files: assert scanned-files metric drops to
  1; correctness: results identical with pruning forced off (config flag
  `TIMEFUSION_TANTIVY_FILE_PRUNING`, default on, instant rollback).

**4.2 Cache prefetch warmer.**
Background task (config-gated) warming `tantivy_cache` with blobs for the
last N days (default 7) of indexed projects at boot + after each flush/
compaction publish. Bounded disk via existing
`timefusion_tantivy_cache_disk_gb` accounting (add LRU eviction by atime if
absent).

## Phase 5 — Conditional DedupExec / restore limit pushdown

Investigation first (sweep semantics live in `database.rs` dedup job):
duplicates enter via (a) crash → WAL re-flush overlap, (b) client-side
DLQ replays (indistinguishable from normal inserts server-side). So a
"clean" flag cannot be inferred from the write path alone; the sweep is
the authority.

**5.1** Track per-(project, table) `swept_through: Option<i64>` — advanced
when a dedup sweep pass completes a time range with zero rewrites two
consecutive times (hysteresis); reset to `None` on WAL replay at boot and
on any flush into a bucket older than the watermark.
**5.2** In `scan`: if the query window `[lo,hi]` satisfies
`hi < swept_through`, skip DedupExec and restore per-scan `limit` pushdown.
Config `TIMEFUSION_READ_DEDUP_SKIP_SWEPT` (default **off** until the parity
harness validates COUNT parity on prod-shaped data).
- Tests: unit — watermark advance/reset transitions; integration — query
  older than watermark has no DedupExec in the plan (EXPLAIN), younger
  window keeps it; sqllogictest COUNT parity with flag on/off around a
  forced duplicate.

## Phase 6 — IMPLEMENTED (2026-07-05, same day)

- **6.1 Row-selection pushdown — DONE.** TF: `_row_ordinal` u64 FAST field
  (schema/builder), `Hit.row_ordinal`, per-file selections in
  `SearchResult.row_selections` (only entries with `ordinals_valid` — set
  ONLY by `build_index_for_file` read-back builds; flush-path indexes see
  pre-sort batches so their ordinals are intentionally untrusted), scan
  passes rel-keyed selections into the fresh-provider path
  (`TIMEFUSION_TANTIVY_ROW_SELECTION`, default on). Fork (working tree of
  `../delta-rs-timefusion`, branch fix/optimize-partition-scoped-conflict):
  `with_row_ordinal_selections` on `DeltaScan`/`TableProviderBuilder`;
  at plan time footer-derived per-RG `ParquetAccessPlan` attached via
  `PartitionedFile.extensions` (whole-file fallback on any doubt).
  **DEPLOY GATE:** fork changes are uncommitted; TF carries a DEV-ONLY
  `[patch]` to the local fork path — push the fork commit, bump the rev in
  Cargo.toml, and remove the patch before building a prod image.
- **6.2 COUNT(*) pushdown — DONE** (`src/count_pushdown.rs`, hooked in
  `DmlQueryPlanner`): answers `COUNT(*) WHERE project_id + both ts bounds`
  from `Σ stats.numRecords` when the window is fully flushed (MemBuffer
  bucket check), dedup-provably-clean (Phase 5 fingerprints) or no dedup
  keys, every overlapping file fully inside the window, and no deletion
  vectors. `TIMEFUSION_COUNT_PUSHDOWN` default on; declines → normal scan.
  MIN/MAX deferred (same skeleton applies when wanted).
- **6.3 Sort-order propagation — DONE.** Fork side was already live in rev
  1f83c34 (with_output_ordering + ordered equivalence). TF side: DedupExec
  now derives output ordering from its input (remapped through the output
  projection) and declares `maintains_input_order`, so
  `ORDER BY timestamp LIMIT n` can early-terminate instead of re-sorting.
  EXPLAIN-verification on prod-shaped data still recommended.
- **6.4 Plan-cache parameterization — DONE** (`plan_cache.rs`): literal-
  bearing SELECTs are shape-keyed (string literals → `$N`, numbers stay
  inline), the placeholder plan is built+optimized once, and each query
  substitutes typed literals (`get_parameter_types`-cast) — skipping parse,
  analyze AND optimize (`served` marker feeds `was_pre_optimized`).
  Unplannable shapes are negative-cached. Coupled fix: the tantivy rewriter
  now routes `= $N` / `LIKE $N` / `IN ($N,…)` via DEFERRED 3-arg
  `text_match(col, $N, kind)` calls classified at scan time post-
  substitution — closing the pre-existing gap where prepared-statement
  plans (cached with placeholders, optimize skipped) lost the prefilter.

## Cross-cutting

- Every phase behind a config flag defaulting to current behavior where
  risk is non-trivial (file pruning, dedup skip); pure caches default on.
- Metrics: extend `prefilter_used/skipped` with reason labels; add
  `files_pruned`, `reader_cache_hit`, `backfill_built` counters (visible in
  `timefusion_stats` for prod verification).
- Verification per phase: `cargo test` + `cargo test --test sqllogictest` +
  targeted `tests/e2e/`; final pass with the tf_vs_ts parity scripts and
  `bench/` query-latency runs (trace_lookup scenario) before/after.
- Rollout order 1→2→3→4 strictly; 5 parallelizable after 1.

## Effort/risk summary

| Phase | Size | Risk | Depends on |
|-------|------|------|-----------|
| 1 quick wins | S–M | Low (caches, refactor) | — |
| 2 OR/IN routing | M | Medium (soundness rules tested) | 1.1 |
| 3 path unification | L | Medium (transition dual-read) | — |
| 4 file pruning + prefetch | M | Medium (flagged) | 3 |
| 5 dedup skip | M | High if wrong (flagged off) | investigation |
| 6 deferred | XL | — | 3/4/5 |
