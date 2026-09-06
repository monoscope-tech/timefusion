# Deletion-vector writer — Phase 0 (scoping), day 1

Decision (2026-09-06): GO as a bounded 2-3 day Phase 0. Rationale: today's 10x
sim proved the ceiling is per-unit cost × slots; DV is the only "cheaper unit"
lever (dedup emits a bitmap instead of rewriting a whole file to drop 0.0008%
of rows). Upstream delta-rs cannot write DVs (#4079 open); our fork controls
both sides; the table property `delta.enableDeletionVectors` is already ON.

## Finding 1 (done, no build needed): the read/verify seams ALREADY handle DVs

The two correctness-critical row-count consumers already subtract DV cardinality
or skip on DV'd inputs — the seam the advisor flagged as the top risk is largely
pre-built:

| site | DV-aware? | evidence |
|---|---|---|
| dedup-rewrite verification (`maintain.rs:8393`) | YES | `n - dv.cardinality` per file |
| repair witness (`maintain.rs:7443`) | YES | skips the row check when any input `deletion_vector.is_some()` |
| `file.deletion_vector_descriptor()` | present in the kernel we build | used above |

## Finding 2 (done): the RAW `add_row_count` sites are SIZING, not gates

`add_row_count` (`maintain.rs:489`) reads `numRecords` WITHOUT DV subtraction.
Its two callers both feed size ESTIMATES, so a DV over-count is conservative,
never incorrect:

| site | use | effect of DV over-count |
|---|---|---|
| `maintain.rs:2026` | rollup slice projection | over-states rows → slightly smaller slices — safe |
| `maintain.rs:7191` | repair bytes-per-row | over-states rows → smaller per-row width → smaller budget — safe |

The packer's own row cap (`select_*`, `packer_admits_pair`) reads `numRecords`
too and treats absent as zero ("can only admit"); on a DV'd file it would
over-count and refuse slightly more eagerly — conservative, and these are hot
sealed files unlikely to carry DVs anyway. Note but do not block.

## Finding 3 (day 2, THE big one): the DV writer is already BUILT, WIRED, and DML-LIVE in prod

The scoped multi-day "build the DV writer in the fork" work does not exist —
it is already done at the exact pinned rev (`cb04a41e`, on branch
`fix/widen-scan-parallelism`, NOT canonical `timefusion-main` — topology drift
since [[tf_deltars_fork_branch_topology_2026-07-21]]):

| capability | where | state |
|---|---|---|
| DV write primitive | fork `crates/core/src/operations/deletion_vectors.rs` `write_deletion_vectors()` | emits exactly `Remove(old) + Add(same-path, +DV descriptor)`, unions any pre-existing DV, kernel `StreamingDeletionVectorWriter` for protocol framing/CRC, z85 UUID path |
| commit shape (advisor's gate 2) | `remove_for_add()` same-path, same stats/partitionValues | DONE — the exact shape, no rabbit hole |
| fork tests | 4 in-module (`dv_write_hides_exactly_the_deleted_rows`, full-mask, vacuum-safety, merge-accumulate) | present at rev; running to confirm green |
| wired to DELETE | fork `delete.rs:768` via `DeleteBuilder::with_deletion_vectors` | TF calls it: `dml.rs:1289` |
| wired to UPDATE | fork `merge_dv.rs` `merge_update_with_deletion_vectors` (public) | TF calls it: `dml.rs:1249`, `dml.rs:1519` |
| DV read keep-mask pushdown | `with_pushdown_with_deletion_vectors(true)` | TF opts in: `mod.rs:9525/9629/9631` |
| config gate | `timefusion_use_deletion_vectors` **defaults true** (`config.rs:2875`) | so DML DVs are ALREADY live in prod |

Consequence: advisor gates 1 (read-path) and 2 (commit shape) are effectively
proven-in-prod for the **DML** path — DELETE/UPDATE write DVs and the scan reads
them masked, and prod shows no resurrection. Phase 0 collapses from "build" to
"prove the ONE untested seam + measure."

## Finding 4 (day 2): the resurrection trap + read-path proof ALREADY have tests

`tests/e2e/deletion_vectors.rs` (against real MinIO, full prod path) already covers
the two gates below — Phase 0 is confirming they pass on current master, not
writing them:

| gate | test | asserts |
|---|---|---|
| read-path proof (gate 2) | `dv_update_and_delete_hide_rows_without_rewriting_files` | DV UPDATE keeps the masked original + appends; DELETE hides exactly the matched row; count correct; deleted row `gone==0` |
| **resurrection trap (the killer)** | `dv_compaction_consolidates_deletion_vectors` | OPTIMIZE reads DV-masked, drops rows, writes DV-FREE files; `count==17 "compaction resurrected DV-deleted rows"`; `gone==0 "deleted row reappeared after compaction"` |
| restart durability | `dv_state_survives_restart` | `count==4 "DV-deleted rows resurrected across restart"` (WAL/replay leg) |
| merge path | `dv_merge_update_from_source_masks_and_appends` | mask + append, count unchanged |

Gap vs advisor's list: the resurrection test uses `optimize_table` (compaction).
Pack (hot-tail), sealed consolidation, and repair use the same
`replace_where`/DataFusion rewrite machinery, so they inherit the mask — but that
is inference, not a test. If DV-for-dedup ships, add one test per op that DV-marks
then runs THAT op. Still, the shared-machinery result means the trap is very
unlikely to be where Phase 0 dies.

## VERDICT (day 2): Phase 0 correctness gates CLOSED — GREEN

Both proofs run, not reasoned:

- **Fork writer, 4/4 at pin `cb04a41e`** (`cargo test --lib deletion_vectors::`):
  `dv_write_hides_exactly_the_deleted_rows`, `dv_delete_all_rows_in_file_reads_empty`,
  `full_vacuum_keeps_live_dv_files_and_preserves_deletes`, `dv_second_delete_merges_with_existing`.
- **TF full-path e2e, 4/4** (`cargo nextest run --features e2e --test e2e deletion_vectors`):
  read-path proof (`dv_update_and_delete_hide_rows_without_rewriting_files`), the
  **resurrection-trap killer** (`dv_compaction_consolidates_deletion_vectors` —
  `count==17`, `gone==0` after OPTIMIZE), restart durability (`dv_state_survives_restart`),
  merge path (`dv_merge_update_from_source_masks_and_appends`).

Gates 1 (read-path) + 2 (commit shape) + the resurrection trap are proven on
current master. DV write/read is correct and shipping for DML.

### Gate 3 (cost spike) — NOT runnable as scoped until Phase 1

`run-unit --op dedup` runs *today's* whole-file-rewrite dedup; it does not use DVs
(dedup never calls `write_deletion_vectors`). And run-unit commits, so it cannot
touch prod (read-only) — staging only. So the literal "DV-write vs rewrite via
run-unit" number requires Phase-1 wiring first. What is already known analytically:
dedup rewrites **454M rows to drop 3,782** (0.0008%); the DV path writes a
~3,782-entry RoaringBitmap `.bin` + one commit, i.e. write cost drops from
O(file rows) to O(deleted rows) — the ~5-orders-of-magnitude reduction that is
the whole 100x business case. The real S3-latency ratio is a Phase-1 staging
measurement, not a Phase-0 blocker.

## Phase 1 (the actual lever — user-gated, NOT to ship autonomously)

Route dedup through the existing writer. The hard part is NOT the writer (built)
but the INVERSION: today's dedup selects SURVIVORS and rewrites them; DV-dedup
must identify LOSERS by physical `(file path, row index)` and emit a bitmap per
source file. That row-position tracking through the dedup query is the Phase-1
design question (DataFusion `row_index` metadata column; cf. `row_index_column`
already used at `mod.rs:9525`). Shipping is a code push → prod deploy, so it is a
separate user-approved step per standing deploy discipline.

## (superseded) earlier framing of remaining gates

1. **Resurrection trap (the promoted go/kill gate).** Dedup does NOT use DVs
   today (`maintain.rs` only *reads* DV descriptors — `:8396` subtracts
   cardinality, `:7446` skips DV'd repair targets — never `write_deletion_vectors`).
   The lever is routing dedup through the existing writer. But once dedup DV-marks
   a file, every later rewrite of that file (Pack, sealed consolidation, repair)
   must read it DV-masked or the dropped rows resurrect. `maintain.rs:8393` proves
   *verification* subtracts cardinality; it does NOT prove the *rewrite row-read*
   applies the mask. The raw `ParquetObjectReader` uses in `compact.rs:729/767`
   and `maintain.rs:8659` are footer/metadata probes only (safe); the actual
   row rewrite goes through the Delta scan (`replace_where`/DataFusion). One
   targeted test settles it: DV a row, run Pack/compact over that file, assert
   the row stays gone. If red, DV-for-dedup is unsafe regardless of everything
   above — this is the one place Phase 0 can still die.
2. **Read-path proof through OUR scan** (not the fork's `get_data_sorted`): DV'd
   file read via pgwire/ProjectRoutingTable with dedup SKIPPED (certified path)
   AND active (DedupExec input must carry the keep-mask), plus the mem-buffer
   union leg must not double-count/resurrect.
3. **Cost spike** — `run-unit --op dedup` DV-write vs rewrite on a whale date.
   The ratio is the 100x business case (dedup rewrites 454M rows to drop 3,782).

## Standing caveat

DV changes the COST of removal, not certification: a DV dedup still has
`dropped > 0`, still refused a grant, so the one-pass-delay before a date
certifies persists ([[tf_certification_must_be_contiguous]]). DV makes each pass
cheap, not the pipeline shorter.
