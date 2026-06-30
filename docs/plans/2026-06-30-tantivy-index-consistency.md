# Tantivy index ↔ parquet consistency + partition-mirrored layout

Date: 2026-06-30. Goal: keep sidecar tantivy indexes correct and consistent
with the Delta parquet files **regardless of who compacts/vacuums** (TimeFusion,
another node, or `delta-rs` CLI directly), and lay indexes out to mirror the
parquet partition tree so the mapping is ~1:1 and trivially verifiable.

## Done already (this session): time-pruning (P0.5)

`search_with_stats` now takes the query's `[lo,hi]` timestamp window and skips
manifest entries whose `[min,max]_timestamp_micros` can't overlap it — so a
needle lookup over a 1h window opens only the indexes covering that window, not
every index the project ever built. Conservative on unknown bounds; sound
because a pruned index only covers rows the query's own timestamp filter
excludes. Tests: `tantivy_index::search::time_prune_overlap_logic` (unit),
`search_time_prunes_non_overlapping_indexes` (integration). This removes the
cold-old-data latency cliff but does NOT fix the consistency problem below.

## Current state (verified)

- Indexes are built **only** in the BufferedWriteLayer flush callback
  (`build_and_publish(project, table, batches, added_files)`), from the
  in-memory flush batches — the builder **never reads parquet back**.
- Blob key is a **random `bucket-{min_ts}-{uuid}`**; manifest entry carries
  `covered_files` (the parquet URIs that commit added). So mapping is
  *1 flush → 1 index covering N files*, not 1:1, and **not derivable** from a
  parquet path.
- Blob path: `indexes/{table}/v1/{project_id}/{uuid}.tantivy.tar.zst` — flat,
  not partition-mirrored.
- TF's **own** optimize calls `gc_after_compaction(live_uris)` (drops entries
  whose `covered_files` are gone) but **builds no index for the new compacted
  files** → compacted data is unindexed until the next flush touching it.
- **No Delta-log-driven reconciliation.** Nothing rebuilds/cleans based on the
  table's current add-file set.
- Tantivy blobs live under a **separate `indexes/` prefix**, so `delta-rs`
  `VACUUM` (parquet-only) never touches them.

## Problems this causes

1. **External optimize**: new parquet unindexed (callback never fires) AND old
   entries/blobs not GC'd (TF didn't run optimize) → orphaned blobs + stale
   entries. Even TF-internal optimize leaves compacted files unindexed.
2. **External vacuum**: deletes parquet, leaves tantivy blobs orphaned (S3 leak)
   + dangling manifest entries.
3. **No backfill**: historical / externally-written parquet can't be indexed.
4. **Correctness landmine**: the read prefilter does `id IN (hits)` and
   intersects. If any live file overlapping the query isn't covered by a live,
   successful index (compacted file, failed build, external write), its rows are
   **silently dropped**. Today this is masked only because TF indexes every
   flush — optimize/external ops break that invariant.

## Target design

Make tantivy consistency a **function of the Delta log** (the source of truth),
not of who triggered the change. Three pieces:

### 1. Partition-mirrored, deterministic 1:1 layout

One index blob per parquet **file**, at a path derived from the parquet path:

```
parquet:  timefusion/{table}/project_id=<uuid>/date=<d>/part-<fileid>-c000.zstd.parquet
index:    indexes/{table}/v1/project_id=<uuid>/date=<d>/part-<fileid>-c000.tantivy.tar.zst
```

- The index path is a pure function of the parquet path (swap prefix + suffix).
  → "do all live parquet files have an index?" and "are there orphan indexes?"
  become a **list + diff**, scriptable and CI-checkable.
- Manifest entry keyed by the parquet file path (relative), `covered_files`
  becomes exactly `[that file]`. (Manifest stays as the fast lookup + stats
  cache; the paths alone are now authoritative, so the manifest can even be
  rebuilt from a listing if lost.)
- `store::blob_path` / `bucket_key` change from `{uuid}` to derive-from-parquet.

### 2. Delta-log-driven reconcile (per table+project)

Run (a) periodically and (b) after each commit/optimize:

```
live = current Delta snapshot add-files
for f in live where index_for(f) missing/failed:  build_index_for_file(f)   # backfill / post-optimize
for entry where entry.file ∉ live:                delete blob + drop entry  # GC / post-vacuum
```

Idempotent; converges regardless of who optimized/vacuumed. Last-writer-wins on
the manifest + deterministic blob keys make concurrent TF/external reconcile
safe.

### 3. `build_index_for_file(table, project, file_uri)` — read parquet → index

The missing primitive: read the parquet back via the object store →
RecordBatches → `store::build_to_dir` → `pack` → upload at the derived path →
manifest upsert. Powers backfill, post-(external)-optimize reindex, and
reconcile from one path. (Today the only builder consumes live flush batches.)

### 4. Read-side coverage gate (correctness — do FIRST)

Before applying `id IN (hits)`, require that every live file overlapping the
query window is covered by a live, successful index; else skip the prefilter
(full scan). Cheap once the manifest is keyed by file. This makes partial
coverage degrade to "slower," never "wrong" — the invariant that lets reconcile
run lazily and tolerate build failures + external ops.

## External-op handling, concretely

- **External optimize** → reconcile sees new files with no index → backfills
  them; sees old files gone → GCs their indexes. No inline hook needed.
- **External vacuum** → reconcile GCs blobs for files no longer live. (VACUUM
  can't touch them — separate prefix — so reconcile is the *only* cleanup path;
  don't rely on vacuum.)
- **Trigger**: external nodes either call a TF reconcile endpoint after their
  op, or just let TF's periodic reconcile pick up the Delta-log delta.

## Migration

Dual-read during rollout: read both the old flat `{uuid}` blobs and the new
partition-mirrored ones; new builds write only the new layout; a one-time
reconcile pass backfills the new layout for all live files, then old blobs are
GC'd. Manifest `schema_version` bump gates the switch.

## Phasing

1. **Coverage gate** (correctness, smallest, independent) — makes the prefilter
   safe under optimize/failed-builds/external ops *today*. ✅ **DONE
   (2026-06-30).** `ProjectRoutingTable::prefilter_coverage_complete` +
   `uri_date_in_window` (database.rs): before applying `id IN (hits)`, require
   every live `.parquet` whose `date=` partition overlaps the query window to be
   in the search's `covered_files` (union over successful manifest entries);
   else skip the prefilter (full scan). `SearchResult.covered_files` carries the
   set. Sound at day granularity vs search's µs time-prune (divergences only
   touch rows the timestamp filter already excludes). Fail-safe if the table
   can't be resolved. Tests: `uri_date_in_window_gates_on_partition_day` (unit),
   `uncovered_live_file_skips_prefilter_not_rows` (e2e — neuters one of two live
   files' index, asserts no rows dropped).
2. **`build_index_for_file`** (read-parquet builder) — the reused primitive.
   ✅ **DONE (2026-06-30).** `TantivyIndexService::build_index_for_file` reads a
   committed parquet back via `store::read_parquet_batches` and publishes at the
   deterministic path; shares `build_pack_upload` with the flush callback.
   Test: `build_index_for_file_reads_parquet_and_publishes_searchable_index`.
3. **Partition-mirrored deterministic layout** + manifest keyed by file.
   ◑ **PARTIAL.** `store::index_path_for_parquet` / `index_to_parquet_rel` (pure
   1:1 + reverse, tested) land the layout, and `build_index_for_file` writes it
   with the manifest keyed by the parquet rel path. STILL TODO: switch the flush
   callback off the random-`bucket-{uuid}` key/flat path onto the same
   derivation, plus dual-read + a one-time migration pass (the hard-to-reverse
   part — do deliberately).
4. **Reconcile task** (backfill + GC) wired to a schedule + post-optimize, and a
   reconcile trigger for external nodes. TODO — reuses `build_index_for_file`
   (backfill) + `index_to_parquet_rel` (orphan detection) + existing
   `gc_after_compaction` (GC).
5. **Backfill** historical parquet via the same reconcile pass. TODO.

## Open decision

If the prefilter isn't worth this much bookkeeping, the alternative is to lean
on **parquet bloom filters** (already enabled on the high-card columns; they
live *inside* the parquet, so they're automatically consistent across
optimize/vacuum with zero sidecar state) and keep tantivy best-effort for
substring/LIKE only. Bloom = self-consistent but coarser (row-group pruning);
tantivy = exact id-prefilter but needs all of the above. Decide before
investing in steps 3–5.
