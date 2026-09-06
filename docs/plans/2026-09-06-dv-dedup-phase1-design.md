# DV-dedup — Phase 1 design (route dedup through deletion vectors)

Status: **DESIGN — awaiting user approval.** Shipping is a code push → prod
deploy, so this is not to be shipped autonomously. Phase 0 is CLOSED green
(`2026-09-06-dv-writer-phase0.md`): the DV writer is built, wired, DML-live, and
the resurrection trap + read-path are proven on master.

## Why (the lever)

Dedup rewrites **454M rows to drop 3,782 duplicates** (0.0008%) — 96.5% of heavy
worker time, and the per-unit cost is what caps 10x/100x (`2026-09-06-ten-x-readiness.md`:
the ceiling is per-unit-cost × slots, and DV is the only "cheaper unit" lever).
A DV dedup writes a ~3,782-entry RoaringBitmap `.bin` + one commit — write cost
drops from O(file rows) to O(deleted rows).

## Why NOT reuse the DML DELETE-with-DV path (settled, don't relitigate)

Impossible, not merely harder: (1) WAL-replay duplicates are **value-identical**
to their survivors, so no row-value predicate can pick the loser — position is the
only distinguisher; (2) keep-greatest losers ("not the max version per key") need
a window/subquery `DeleteBuilder`'s predicate cannot express. The loser-extraction
operator is the ONLY mechanism, and it already maximally reuses the existing scan
projections, survivor rule, and `write_deletion_vectors` sink.

## The core change: INVERT dedup from survivors to losers

| | today (copy-on-write) | DV-dedup |
|---|---|---|
| what dedup emits | SURVIVOR rows → new parquet | LOSER `(file, physical row idx)` → bitmap |
| commit | `Remove(old adds) + Add(new file)` (`maintain.rs:3049/2864`) | `Remove(old) + Add(same path, +DV desc)` (`write_deletion_vectors`) |
| bytes written | whole file | one small `.bin` |
| operator | `DedupExec` emits kept rows (`read/mod.rs:441`) | must emit loser positions |

## Feasibility: primitives exist, but ONE fork visibility commit is needed

- `PATH_COLUMN = "__delta_rs_path"` — source file path per row (`delta_datafusion/mod.rs:85`).
- `TableProviderBuilder::with_row_index_column(name)` — physical within-file row
  index per row — **but `pub(crate)` (`table_provider.rs:376`), NOT reachable
  from TF.** Phase 1 needs a one-line fork commit making it `pub`, then a pin
  bump. **A pin bump is a code push and deploys prod** — so Phase 1 is "TF-side +
  one fork visibility commit under fork/pin discipline," not "TF-side only."
- `retained_row_index_field` (`.../next/scan/plan.rs:72`) — the row index is
  retained through projection (and, per the retention logic, through DV selection).
- `write_deletion_vectors(log_store, root, Vec<FileDeletion>)` — the sink
  (`operations/deletion_vectors.rs:120`), unions any existing DV.

## Implementation sketch (TF-side + one fork visibility commit)

1. **New dedup path** (behind an env kill-switch, e.g. `TIMEFUSION_DEDUP_USE_DV`,
   default OFF): scan the partition's files with `with_row_index_column` +
   `PATH_COLUMN` projected, plus `with_pushdown_with_deletion_vectors(true)` so an
   already-DV'd file is read masked.
2. **Loser extraction**: reuse the survivor rule (keep-first / keep-greatest,
   `read/mod.rs`) but emit, for each key group, the NON-survivor rows'
   `(path, row_index)` instead of the survivor's payload. Group by path →
   `FileDeletion { add, deleted_indexes }`.
3. **Commit** via `write_deletion_vectors` → `Remove(old) + Add(same path, +DV)`.
   Then the existing certification/verification seams already handle it
   (`maintain.rs:8393` subtracts `dv.cardinality`).

## The one correctness question Phase 1 must settle (RUN, not reason)

`write_deletion_vectors` takes **physical** 0-based parquet row indexes and unions
with the file's existing DV. So the operator must emit **physical** indexes, not
post-DV-mask logical ordinals. If `with_row_index_column` on a file that already
has a DV returns *masked/compacted* ordinals, a second DV-dedup pass would delete
the WRONG rows. Test: DV-dedup a file (pass 1), add more duplicates, DV-dedup
again (pass 2), assert exactly the intended rows are gone and no survivor is
masked. This is the DV analogue of the merge-accumulate fork test.

## Cost spike (Gate 3, now runnable once step 1 lands, on STAGING not prod)

`run-unit --op dedup` with `TIMEFUSION_DEDUP_USE_DV=true` vs false on a seeded
whale date in `timefusion-staging` (real R2 latency; MinIO under-measures the
fixed per-unit object-store cost). Deliverable: the DV/rewrite wall-clock and
bytes-written ratio.

## IMPLEMENTATION LOG (in progress, ON-by-default per user directive)

Gate on the EXISTING `timefusion_use_deletion_vectors` (default true) — no new knob.

**Fork branch `timefusion-dv-physical-row-index`** (off pinned `cb04a41e`):
- `6e9244fe` test: pins per-file + physical row-index semantics (reproduce).
- `044f7c98` **fix: a LIVE prod bug** — `with_row_index_column` returned post-mask
  LOGICAL ordinals on DV'd files, corrupting `deletion_vector_delete` +
  `merge_update_with_deletion_vectors` (both default-on) on a 2nd delete/update
  over an already-DV'd file. Fixed in `append_row_index` (physical index over the
  RAW batch, filtered by the DV selection). 6/6 DV + 55/55 exec tests green.
  **Independently deploy-worthy regardless of DV-dedup.**
- visibility (uncommitted, building): `with_row_index_column`,
  `write_deletion_vectors`, `FileDeletion`, `dv_object_store_relative_path`,
  `pub mod deletion_vectors` — so TF can call the writer + derive `.bin` paths.

**TF-side plan (verified against the code):**
- Insertion: `compact.rs::stage_dedup_chunk`, after `targets` (the Add actions)
  are mapped. Uniform DV path — the fork fix makes multi-pass DV on an
  already-DV'd file correct, so NO copy-on-write fallback is needed for
  correctness (consolidation is separate, task 13).
- Loser selection (option D): scan the chunk via `narrow_provider` with
  `with_file_column(DEDUP_FILE_COL)` + `with_row_index_column` +
  `with_pushdown_with_deletion_vectors(true)`, projected to
  `(file, row_index, dedup keys, tiebreak)`, `ORDER BY dedup key`
  (SortPreservingMerge; key == sort prefix so no real sort). Stream run-by-run:
  per equal-key run pick the winner via the shared `beats()` comparator (task 10),
  emit the rest as `(file, row_index-1)` losers. Group by file → `FileDeletion`
  → `write_deletion_vectors` (writes `.bin`, returns Remove(old)+Add(same,+DV)).
- Commit via existing `commit_wave` with the swapped action set. Keep the
  fail-closed count guard (`before == expected_live_rows`, `after = before −
  losers`, `before == after → Converged`).
- **Staging-machinery reconciliation** (the structural conflict caught + resolved
  with the advisor): `cleanup_orphaned_parquet` deletes add paths
  UNCONDITIONALLY — fatal for DV bins whose adds are same-path LIVE files. Fix:
  add `StagedBin.discardable_paths` (explicit cleanup set). Copy-on-write bins:
  empty (their existing live-checked `discard_bin_parquet` add-logic is unchanged
  and already skips live paths). DV bins: the `.bin` sidecar paths (fresh UUIDs,
  always safe to delete on discard), with `stage_store` = the live table store
  where `write_deletion_vectors` put them. `record_staged_intent.paths` = the
  `.bin`s (boot-reconcile crash safety, same mechanism as staged parquet).
- Skip `mark_written_sorted` for DV bins (same-path files, sortedness unchanged,
  `repair_verified_sorted` path-keyed stays valid). Passing `sorted=true` on files
  we didn't write is how a wrong seed enters.
- **One race to test (pre-existing, DV multiplies it):** a concurrent DML DELETE
  can DV the same file between stage and commit; path-based liveness passes but
  the staged Add merged from the OLD descriptor would drop the concurrent delete.
  Delta's conflict checker should fire remove-remove on the old add — verify with
  a targeted test.

## Risks / standing caveats

- **THE BIG ONE — DV-bearing files lose per-file parquet predicate pushdown.**
  TF's own comment (`mod.rs:9525`) states "Actual DV-bearing FILES still disable
  the predicate per-file inside the fork." Dedup sweeps the ENTIRE fleet, so
  DV-marking at dedup scale could disable pushdown across most of the table — a
  read-path regression of exactly the 2026-08-20 shape (2.11M rows emitted for a
  0-match equality; 24h SELECT * died at the dedup 2 GiB cap). **This makes the
  OPTIMIZE consolidation policy load-bearing for READ latency, not a nice-to-have.**
  Design must include: (a) a bound on the DV'd-file fraction, or consolidation
  triggered by DV *presence* (not just DV size); (b) a named read-side metric to
  watch alongside the write-side win (e.g. rows-scanned / rows-returned on the
  hot dashboard path). The write-side win is worthless if it re-breaks reads.
- **Crash seam**: crash between the `.bin` write and the commit leaves an orphan
  DV file. The vacuum test covers *live* DVs; confirm an UNcommitted orphan `.bin`
  is cleaned (like `cleanup_orphaned_parquet` for staged adds) and does not
  accumulate forever.
- **OCC**: the DV merge commit must reuse the append-tolerant DV commit path
  (`config.rs` note near `timefusion_use_deletion_vectors`: a concurrent flush
  AddFile-only commit rebases instead of aborting), or a concurrent flush aborts
  every dedup wave — the same failure that motivated append-tolerant DML DV commits.
- **Certification unchanged**: a DV dedup still has `dropped > 0`, still refuses a
  grant, so the one-pass-delay before a date certifies persists
  ([[tf_certification_must_be_contiguous]]). DV makes each pass cheap, not the
  pipeline shorter.
- **DV accumulation**: repeated DV-dedup grows a file's DV; a periodic OPTIMIZE
  consolidates it to a DV-free file (already tested:
  `dv_compaction_consolidates_deletion_vectors`). Policy interacts with the
  pushdown risk above.
- **External log readers** of these Delta tables must understand DVs (TF does;
  the table property is already on). No new exposure beyond DML DVs already live.
- **Deploy discipline**: one change per deploy, env kill-switch OFF at ship, ≥2h
  quiet before trusting throughput numbers, name the `timefusion_stats` metric it
  moves.
