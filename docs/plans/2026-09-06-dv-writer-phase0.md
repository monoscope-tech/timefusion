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

## What Phase 0 still owes (needs the fork)

1. **Read-path proof** — write a DV'd file into a staging table via the fork,
   read through OUR scan with dedup SKIPPED (the certified-window path), assert
   the DV-deleted row is absent. The seams above make this likely-green but it
   is the go/kill gate and must be RUN, not reasoned.
2. **Commit shape** — `remove(old) + add(new-with-DV-descriptor)`; confirm the
   fork's writer serializes the RoaringBitmap DV format the kernel reads.
3. **Cost spike** — `run-unit --op dedup` on a whale date, DV-write vs rewrite.
   That ratio is the 100x business case.

## Standing caveat

DV changes the COST of removal, not certification: a DV dedup still has
`dropped > 0`, still refused a grant, so the one-pass-delay before a date
certifies persists ([[tf_certification_must_be_contiguous]]). DV makes each pass
cheap, not the pipeline shorter.
