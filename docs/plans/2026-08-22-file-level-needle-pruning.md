# File-level needle pruning for the delta leg

2026-08-22. Implements ranked residual #2 from
`2026-08-21-post-hot-tier-speed.md` and P5 ("resident plan-time pruning
index") from `2026-08-21-point-lookup-file-open-wall.md` (deleted 2026-08-24;
`git log --diff-filter=D -- docs/plans` has the SHA).

## Problem, with the EA evidence

A point lookup (`context___trace_id = '<hex>'`, and equally `id`,
`span_id`, `session_id`, `user_id` needles) selects **every file in the
time window** because file-level pruning has nothing to work with:

```
files_ranges_pruned_statistics = 284 → 284   (24h, final-cycle EA; 434 → 434 at 7d)
row_groups_pruned_bloom_filter = 352 → 2     (parquet blooms fire — but per-file, post-open)
```

Structural, not incidental: `stats_columns_for` (src/database/mod.rs:5711)
writes `delta.dataSkippingStatsColumns = timestamp, service, id, level,
updated_at` — trace_id has no min/max in the log, and random hex would
defeat min/max regardless. Parquet blooms fire (352→2 row groups) but live
inside each file: they save decode, never the open, never plan size. This
owns all A-shape misses in the final-cycle matrix (~7 cells).

Constraints measured previously (point-lookup doc §P5):

- Consulted **at file-selection time**, before planning, so the
  file-action list shrinks — pruning at exec time leaves the plan cost.
- **Resident, not fetched** — the plan-time tantivy prefilter (S3 IO at
  plan time) is the cautionary tale. The plan path only consults memory.

Ecosystem: Tempo prunes blocks via sharded blooms before planning; Loki
3.3 builds bloom sidecars with background builders; ClickHouse skip
indexes are per-granule sidecars consulted at selection time.

## Design (post-review — advisor findings folded in)

**Rejected — probe parquet blooms per-query through foyer.** Blooms sit
`AfterRowGroup` (mid-file, data-class 1MiB-aligned ranges). Probing 284
candidate files cold from the query path is exactly the plan-time-IO trap.

**Chosen — maintenance-built per-file bloom sidecar + resident registry.**
The builder never decodes rows: `ParquetRecordBatchStreamBuilder::
get_row_group_column_bloom_filter` returns each column chunk's `Sbbf`
straight off footer metadata. Verified against parquet 58.3 source:

- The **writer already folds** blooms at flush
  (`encoder.rs:190-194` → `fold_to_target_fpp`), so on-disk blooms are
  NDV-sized, not the 1.7MB write-allocation size. (This reconciles the
  P5 file sizes; the earlier "1.7MB per bloom per row group" sizing fear
  described the pre-flush allocation.)
- `Sbbf::write` and `Sbbf::from_bytes` are public → clean serialize/parse
  round-trip for the sidecar, **no merging** and no bitset access needed.

So: sidecar entry = per file, per needle column, per row group, the
serialized `Sbbf` as-is. No OR-merge (advisor #4: per-column keeps FPP
clean and avoids unverified bitset surgery). Needle check: file excluded
iff, for some conjunct column with sidecar blooms, EVERY row group's bloom
rejects EVERY value of that conjunct (Eq = 1 value; IN-list = all).

### Artifact

- `bloom_sidecars/{table}/{project_id}/{date}.bin` in object storage —
  bincode: version byte + `Vec<FileBlooms { rel: String, no_bloom: bool,
  columns: Vec<(String, Vec<Vec<u8>>)> }>` (rel = table-relative parquet
  path, same keying as the tantivy manifest).
- Per-(project,date) so the cron only rewrites blobs for dates with file
  churn, and a 30d query touches ≤30 small blobs per project.
- `no_bloom` marks files whose parquet carries no blooms (pre-bloom era)
  or whose bloom payload exceeds a per-file cap (~4MB — a whale file's
  saturated bloom prunes nothing and bloats the blob; advisor #3 density
  guard): never re-probed, never excluded.
- Best-effort posture: any read/parse failure ⇒ no pruning, never an error.

### Builder

- `spawn_db_cron` job (template: Tantivy reconcile, src/database/mod.rs:3899)
  every ~5 min + one run shortly after boot (STARTUP_REPAIR_DELAY pattern).
- Walk: group live `get_file_uris()` by (project,date), diff against blob
  entries per date; build missing entries, drop retired ones (GC in the
  same pass); write blob only when changed. Newest dates first,
  per-pass file cap, `buffer_unordered` concurrency.
- Per-file cost: footer GET (foyer-warm 35d deep) + bloom-range GETs.

### Read path (all in-memory, non-blocking)

- New slice `src/read/bloom_prune.rs`: `BloomPruneRegistry` — DashMap
  keyed `(table, project, date)` → `Arc<DateBlooms>`, byte-tracked with a
  cap (env, default ~256MB; evict oldest-loaded), TTL ~300s refresh.
  **Single-flight background loads** (advisor #5): a plan-time miss
  registers the key in an in-flight set and spawns ONE fetch; the plan
  path never awaits IO. First lookup after boot is unpruned; later ones
  prune. Staleness is safe in both directions: excluding a retired rel is
  a no-op, an unknown new file is included.
- Needle extraction in `ProjectRoutingTable::scan` next to
  `extract_time_range_from_filters`: top-level conjuncts only,
  `col = literal` / `col IN (…)`, where the schema field has
  `bloom_filter: true` AND the column is not in
  `version_mutable_columns()` (today all bloomed columns are immutable —
  only `hashes` is mutable — but guard it; a bloom on a mutable column
  could return a stale version by hiding the newest one).
- **Where the exclusion applies (advisor #1 — the load-bearing fix):**
  NOT via `tantivy_exclude` (when an include set exists, excludes are
  ignored — mod.rs:7377 — and `zero_hit_files` touches only the indexed
  leg). Instead, pass the rejected-rel set into
  `scan_delta_with_tantivy` and filter the live-file universe before leg
  partitioning: the `live` iterator (mod.rs:7552) drops rejected uris, so
  BOTH the indexed and raw legs shrink; the no-coverage and
  complete-coverage fast paths union rejected uris into `exclude_files`.
  A routed query with uncovered files gets those files bloom-pruned —
  that is the case that owns the A-shape misses.
- Dedup safety: needle columns are identity columns; UPDATE appends
  full-row copies and DELETE tombstones are full-row copies read through
  the routing provider (verified src/dml.rs:959ff), so every version of a
  matching row carries the needle and can never be in an excluded file.
- Known cost accepted: a file-selected scan bypasses the provider cache
  (~30ms build) — noise against seconds saved. **Zero-exclusion queries
  must NOT pay it** (codex): only engage the selection when the rejected
  set is non-empty; needle extraction caps IN-lists (~64 values) so a
  giant IN can't turn the registry probe into a hot loop.
- **All-files-rejected** (codex P0): must not fall through to the
  unrestricted-scan fallback at mod.rs:7575 — when the pre-bloom live set
  was non-empty and rejection emptied it, scan with an EMPTY include
  selection (schema-correct empty scan; verify the fork handles
  `FileSelection::from_file_paths(vec![])`, add a test).
- Key representation (codex P0): registry stores rels; membership tests
  against `get_file_uris()` output convert via `parquet_rel_of_uri`
  first. One canonical conversion point, tested.
- Compaction safety rests on three facts, now explicit: unknown new path
  ⇒ included; stale rejected path ⇒ no-op; **Delta never reuses a file
  path for different contents** (UUID part names).

### Correctness invariants

1. **Inclusion on unknown**: no entry / no blob / stale / error / flag off
   ⇒ file kept. Staleness costs speed, never rows.
2. **No false negatives**: parquet blooms have none; sidecar stores them
   byte-identical. Unit test: write parquet with blooms → build sidecar →
   every written value still hits through serialize/parse.
3. Mem-buffer leg untouched — recent rows served from memory regardless.
4. Tombstone test: DELETE a row, prune on, needle query must not
   resurrect it.

## Fat-needle plan-time fix (P5 anomaly — attributed, same branch)

Diagnosis (interleaved prod reps, scratchpad p5_anomaly/): P5's benchmark
trace_id matches **4.5M of 4.9M rows (92.6%)** — the 5.9s is plan-time
tantivy prefilter cost proportional to HIT COUNT, not per-file overhead.
`search_with_stats` materializes up to `HIT_CAP=1M` docs per index
(`query_with_searcher(…, None)`); the `max_hits` abort fires only after
whole per-index hit vectors return (`buffer_unordered(32)`), then throws
the work away — 4.4-5.6s per EXPLAIN, never cached. Below the cap it's
the other regime: 3.3k hits → 3,346-literal `IN` (~2.4s planning), and
P4's 59k-hit needle → ~28s. Blob fetches ruled out (0 across runs).

Fix here (count-first — codex: `TopDocs::with_limit(k)` still scores all
matches, so a limit alone bounds materialization but not scoring; the
`Count` collector bounds the expensive part, doc-store reads):
(a) per index, run `Count` first; raw count > max_hits ⇒ index reports
overflow and the search aborts before materializing anything (an index
whose raw hits exceed the cap can no longer prove completeness anyway —
today's behavior, minus the 1M-doc materialization);
(b) materialize survivors with `Some(max_hits.saturating_add(1))`
(`search()` passes `usize::MAX` — no naive `+1`) as a belt-and-braces
bound; (c) `timefusion_tantivy_prefilter_max_hits` default 100_000 →
**2_000** (~2.4s of planning at 3.3k IN-literals says 100k sits ~30x too
high). New stat `hits_materialized` makes the bound observable.
Bug-fix workflow: failing test first — >max_hits matches must return the
abort verdict with `hits_materialized` bounded, not O(total matches).

Benchmark hygiene: P5/P4 A-shape cells measured the fat-needle penalty,
not point lookup; the harness should assert trace cardinality ≤ ~100 when
sampling needles. Other shapes unaffected.

## Rollout

- `TIMEFUSION_FILE_BLOOM_PRUNING` (default **true**) — read-path kill
  switch; builder cron keyed off the same flag.
- Tests: (a) unit — sidecar round-trip no-false-negatives, extractor
  conjunct/mutable-column rules, fat-needle abort bound; (b) integration —
  write → flush → build sidecar → needle query excludes files, correct
  rows incl. updated-version row and deleted-row non-resurrection;
  (c) routed-query-with-uncovered-file gets bloom-pruned (advisor #1).
- Counters: `bloom_prune.{files_excluded,candidates,registry_miss,
  sidecar_files,build_errors}` in timefusion_stats.
- Verify on prod with the A-shape EA (a REAL ≤100-row trace): expect
  file_groups ~2-6 at 24h, sub-second wall at every window.

## DEPLOYED 2026-08-22 (576e1c3) — verification results

All four steps below PASSED on the live deploy:

1. No crashloop; the only ERROR lines were the old replica's normal
   drain-for-deployment messages during handoff.
2. First cron pass: `bloom sidecar reconcile: built=512 errors=0` within
   ~5 min of boot; second pass +512 (backlog converges over hours,
   newest-first so the hot window is covered first).
3. A-shape EA (real 12-row trace, dcad860a, 24h): delta leg selected
   **24 files (was 284)**, 12/12 rows correct, wall 1.6-2.2s (execution
   ~100ms; residual is the known ~1-1.5s planning floor). Live traffic
   within minutes of boot: `queries_pruned=483, files_rejected=5186,
   resident_bytes=130MB` — the feature fires fleet-wide, unprompted.
4. Fat needle (P5's 4.5M-row trace 0025a61a…): planning **4.1-6.0s
   (never warmed) → 1.0-1.5s**, 3 reps.

Remaining ceiling for point lookups is now the non-tantivy planning
residual (P2b of the point-lookup doc), not file selection.

## Post-deploy verification (in order)

1. Watch service logs ~10 min for crashloop. If it restarts, check kern.log
   for the anon-125GB memcg signature BEFORE reverting anything (08-20
   lesson: that OOM is pre-existing and deploy-independent).
2. Confirm the first cron pass logged `bloom sidecar reconcile: built=N`
   (≤5 min after boot). Full backlog at 512 files/pass is hours, but
   newest-first covers the 24h window almost immediately.
3. A-shape EA **twice** with a real ≤100-row trace: the first query spawns
   registry loads and prunes nothing; the second is the measurement.
   Expect `bloom_prune.files_rejected` > 0 and file_groups ~2-6 at 24h.
4. Fat-needle fix separately: EXPLAIN on P5's degenerate trace (92.6% of
   rows) should plan sub-second; `tantivy.hits_materialized` stays O(2k).
   This deploy carries TWO levers (bloom pruning + prefilter cap 100k→2k)
   with independent kill switches (`TIMEFUSION_FILE_BLOOM_PRUNING`,
   `TIMEFUSION_TANTIVY_PREFILTER_MAX_HITS`) — attribute per-lever via the
   counters before crediting either.

Known fast-follow (recorded so it isn't rediscovered as a GET storm):
`bloom_sidecar_reconcile` GETs every (project,date) sidecar per pass,
including converged cells — thousands of GETs per 5-min pass on the
unified table. Fix: in-memory per-cell file-count fingerprint to skip
unchanged cells, or bound the walk to ~45 days.

## Companion small items in this branch

- `journal.retry()` now logs `key/reason/attempts/not_before` at debug —
  the observability hole that stalled two diagnoses this week. (DONE)
