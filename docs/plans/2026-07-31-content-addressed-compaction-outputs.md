# Content-addressed compaction outputs (retry adoption)

**Status:** spec, unassigned · **Repo:** `tonyalaribe/delta-rs-timefusion` (fork), branch `timefusion-main` · **Surface:** `crates/core/src/operations/optimize.rs` · **Effort:** ~2-4 days incl. tests

## Motivation (measured, 2026-07-30/31)

A compaction bin's cost is dominated by sort + a ~1 GB multipart upload
(minutes each). Today the output object key is a random UUID
(`part-00000-<uuid>-c000.zstd.parquet`), so ANY retry — transient S3 failure,
OCC conflict on the commit, killed pass, wedge-reaped container — rebuilds and
re-uploads bins whose upload had already completed but wasn't yet committed.
During the 2026-07-30 backlog drain, flaky-uplink and pool-exhaustion retries
re-did the same bins repeatedly; each redo wastes 3-10 min of compute+transfer.
Off-box compaction from consumer links makes this the dominant inefficiency.

## Design

### 1. Deterministic output naming
In `rewrite_files` (optimize.rs), derive the output key from the bin's
*content identity* instead of `Uuid::new_v4()`:

```
bin_id = hex(sha256(
    sorted([ (add.path, add.modification_time, add.size,
              add.deletion_vector.as_ref().map(|dv| (dv.storage_type, dv.path_or_inline_dv, dv.cardinality))) for add in bin ])
    ++ optimize_type_tag            // Compact | SortBy(cols) | SortByDedup(cols, dedup_cfg)
    ++ writer_fingerprint           // compression codec+level, target_size, sorted_output flag,
                                    // stats/indexed-cols config, table schema hash
))[..32]
part-00000-<bin_id>-cadr.zstd.parquet        // distinct suffix "cadr" marks adoptable outputs
```

Key properties:
- Any input-set change (file added/removed from bin, **DV mutated**, schema or
  writer config change) → different hash → no false adoption. DV identity MUST
  be part of the hash: a dedup/DELETE between attempts changes output content
  for the same file paths.
- Same bin retried → same key → retry is a no-op upload or an adoption.

### 2. Adoption path
At the top of `rewrite_files`, before building the writer:

1. `HEAD` the deterministic key on the table's object store.
2. If present: **validate** (a) size > 0, (b) parquet footer readable
   (reuse the `parquet_tail_ok`-style structural check from TF, or full
   `ParquetObjectReader::get_metadata`), (c) footer KV carries
   `timefusion.bin_id == bin_id` (write it at creation — guards against a
   different writer generation colliding on the truncated hash).
3. On valid hit: skip sort+upload entirely; reconstruct the `Add` action from
   the parquet footer (row count, per-column min/max/null stats from footer
   metadata — same construction path the writer uses; extract into a shared
   helper `add_action_from_footer(path, footer, partition_values, tags)`), then
   proceed to the normal commit accumulation. Increment a new metric
   `optimize.bins_adopted`.
4. On invalid/corrupt hit: delete the object, fall through to a fresh rewrite.

Multipart atomicity guarantees an object either exists whole or not at all —
no torn-file risk on the HEAD-exists check.

### 3. Orphan lifecycle (interaction with vacuum/FSCK — REQUIRED reading)
An uploaded-but-never-committed output is an orphan until adopted. Two
existing janitors may delete it prematurely:
- **Vacuum `VacuumMode::Full`** removes untracked objects older than the
  retention floor (24 h). Fine: adoption windows are minutes-to-hours. Confirm
  vacuum's orphan handling honors modification-time retention for `-cadr`
  files (it does for regular orphans; add a test).
- **TF hourly FSCK** (post-2026-07-09 incident) removes dangling files —
  verify it only removes files REFERENCED-then-lost (dangling adds), not
  never-referenced orphans; if it also sweeps unreferenced objects, exempt
  `-cadr` younger than 24 h.

### 4. Concurrency
Two compactors planning the same bin produce the same key. Outcomes:
- Both upload: last-write-wins on an identical-content object — harmless.
- One adopts the other's in-progress... impossible: incomplete multipart ≠
  visible object. The only observable states are absent/whole.
- Both commit an Add for the same path in different commits: second commit's
  conflict check sees the first's Remove of inputs → OCC conflict → retry →
  replan finds inputs gone → bin vanishes. Same as today, minus the wasted
  upload.

## Non-goals
- Multipart resume (tracking uploadId/part ETags across retries) — separate,
  follow-up item; this spec makes retries cheap enough that resume is a
  nice-to-have.
- Content-addressing flush/ingest writes (different lifecycle, tiny files).

## Tests (failing-first, per repo CLAUDE.md)
1. Unit: `bin_id` stability — same bin ⇒ same id; permuted file order ⇒ same
   id; DV change / file swap / writer-config change / optimize-type change ⇒
   different id.
2. Unit: `add_action_from_footer` equals the writer-produced Add (stats,
   row count, size, tags) on a golden parquet.
3. Integration (`command_optimize.rs`): kill-after-upload-before-commit
   simulation — run optimize with an injected commit failure, rerun, assert
   second run adopts (metric `bins_adopted` == bins, wall-time ≪ first run,
   byte-identical table state).
4. Integration: DV mutated between attempts ⇒ NO adoption (fresh hash).
5. Corrupt-object hit (truncate the object manually) ⇒ deleted + rebuilt.

## Rollout
- Fork PR → TF rev bump. No table-format change (name shape is opaque to
  Delta); old UUID files unaffected. Metric + a log line per adoption for the
  first prod week. Revert = rename scheme back; adoption simply stops firing.

## Pointers
- `rewrite_files`, `read_sorted`, `PartitionWriter::try_with_config` in
  `crates/core/src/operations/optimize.rs` (fork).
- TF-side: no changes required; optional metrics plumb-through in
  `src/metrics.rs`.
- Context: memory notes `tf_compaction_binfanin_leak_2026-07-30`,
  `tf_cli_offbox_2026-07-30`; this week's fork commits b6ab839c, c5d23524,
  8a4b7bc9 show the file's recent surgery style.
