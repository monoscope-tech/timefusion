# The rollup staleness witness: what it actually refuses, and what to replace it with

Base: `e67a149` (an ancestor of the branch point `1447e6f`, which adds two
unrelated rollup-routing commits). Nothing in the live path changed. The
deliverable is the diagnosis, the option comparison, and a proven predicate:
`rollup::verify_slice_witness`, with a case table asserting the four transitions
the brief names plus the three that carry the corrected diagnosis.

## 1. Verified mechanism, with corrections

The brief's account of the plumbing is right and I re-derived every line of it:

| claim | verified at |
|---|---|
| read side sums `num_records` | `PartitionStats::rows`, `database/mod.rs:1601`, folded in `partition_stats_bounded`, `database/maintain.rs:2940` |
| read-side comparison | `database/mod.rs:4046` → `rollup::slice_coverage_agrees`, `rollup.rs:603` |
| write side stamps the same number | `database/maintain.rs:1600`, tagged at `:1950` |
| ledger side recomputes it | `database/maintain.rs:3175` / `:3199` |
| `source_fp` fallback is dead and must stay dead | note at `database/mod.rs:4038`, warning at `maintain.rs:3196`; removed in `7e5bb5a` |

Both sides call `partition_stats_bounded(.., &|_, _| i64::MAX)` — unbounded, the
whole date partition. So the comparison is genuinely consistent; the defect is in
what is being compared. Three corrections:

**(a) Bin-pack compaction does NOT move the witness.** Merging N files into one
preserves the `num_records` sum. Only rewrites that DROP rows move it: dedup
(collapsing merge-on-read versions), a DML delete, vacuum of live-but-superseded
content. The brief's "compaction … WITHOUT changing what the rollup aggregated"
is true of dedup, not of packing. The first case in the predicate's table asserts
this so it stays true.

**(b) The dominant failure is not dedup at all — it is other-hours ingest.**
`rollup_stale_grew = 4103` against `rollup_stale_shrank = 400`, and the
classifier (`database/mod.rs:9768`) defines `grew` as *current > witness*, i.e.
rows ARRIVED. The witness is a statement about the WHOLE date partition
(`rollup.rs:589`, deliberately — witnesses are never summed) while the claim it
guards is a sub-range of that date. A slice covering 00:00–01:00 is therefore
voided by a flush into 23:00. Worse, this is monotone: once a day has taken any
ingest after a slice was built, that slice is stale *forever*, because nothing
re-enqueues a witness-**moved** slice — `enqueue_witnessless_rebuilds`
(`maintain.rs:3665`) selects `source_rows.is_none()` only. That is the amplifier
that turns 511 partition cells into 20 usable ones while the contiguity gauge
still reads 30/30. **A fix that does not address `grew` fixes 400 of 4503.**

**(c) The witness is a belt over braces that are already exact.** Every inbound
write funnels through `insert_records_batch` → `invalidate_rollup_batches`
(`database/write.rs:601`), which derives the touched hours FROM THE ROWS and
calls `invalidate_rollup_hours`. That already bumps `rollup_source_epochs`,
`retain`s away exactly the overlapping slice coverage, enqueues the repair and
persists the journal — before the write is acknowledged, and before the rows
reach Delta. DML routes to the same place via `invalidate_rollup_dml`. So for
anything this process ingested, invalidation is already exact and range-scoped.

What the witness is left guarding is narrow and worth naming, because it bounds
how aggressive a replacement may be:

- coverage re-adopted after a restart from Delta file tags, where the journal's
  dirty-hour history is not consulted;
- a second writer against the same table;
- the unscoped fallback (`rollup_invalidate_unscoped`, `maintain.rs:2805`) and
  any bug in hour derivation.

**(d) Recovery precedence matters for any carry-forward design.** In
`recover_rollup_coverage` the journal publications are inserted first and the
**Delta file tags overwrite them** (`maintain.rs:3600` then `:3670`). Tags win.
A carry-forward that updates only the journal is undone by the next restart, and
prod restarts constantly.

**(e) One asymmetry worth a line:** the slice path checks the witness but not
`source_epoch` (`mod.rs:4046`); only the date path checks the epoch
(`mod.rs:3975`). The epoch is bumped by every ingest and DML invalidation, so on
the slice path the witness is currently the only cross-restart guard.

## 2. Options

Evaluated against: does it fix `grew`; does it survive dedup; what does the READ
path pay per query; does the BUILDER have to stamp something new; can existing
slices be migrated.

### A. Bounded physical witness — `num_records` over files wholly below `covered_through`

Sum only the live files that lie entirely below the slice's `covered_through`.
`partition_stats_bounded` already takes exactly this `bound_for` parameter and is
already used bounded elsewhere (`maintain.rs:2832`), and `covered_through` is
durable — it is the slice end, present in the key and in the tags.

- **grew: fixed.** Ingest past the bound is excluded on both sides.
- **dedup: not fixed.** A dedup inside the covered range still moves the sum.
- **read cost: nil.** Same single pass over the add-actions the read path already
  makes; one comparison per file instead of none.
- **builder: new tag.** The stamped number changes meaning, so it needs its own
  tag (`TAG_SOURCE_ROWS_BOUNDED`), with v1 slices keeping v1 semantics.
- **migration: none possible.** An existing witness was computed unbounded and
  cannot be recomputed without the snapshot the build read. Old slices keep
  today's rule until republished.
- **The trap, and it is the reason this is not the recommendation.**
  `partition_stats_bounded` silently *excludes* a file whose `max_ts >= bound`.
  Inheriting that is unsound: a late file straddling the bound, carrying rows
  INSIDE the covered range, would be excluded and the witness would not move —
  serving a stale number silently. The predicate therefore poisons on a straddle
  (returns `Unverifiable`). That is sound, and it is dark: once hot-tail packing
  consolidates a sealed day into one file per date, **every** slice bound falls
  inside a file and the whole date is unverifiable. Both facts are asserted in
  the case table.

### B. Carry the witness forward across a benign rewrite

When maintenance rewrites a source partition preserving logical content, it knows
the rewrite was benign, so it can restamp the witness it is about to invalidate.

- **grew: not addressed at all.** This is the 400, not the 4103.
- **dedup: exactly fixed**, and provably so — the rewriter has both counts.
- **read cost: nil.**
- **builder: no change to what is stamped**, but a real cost elsewhere: per (d),
  the authority is the **tier file tags**, and the rewrite happens on the
  **source** table. Restamping means committing a metadata-only Remove+Add
  against the *tier* for every affected tier file, plus updating
  `rollup_slice_coverage`, the journal publication and the coverage ledger. Four
  writers of one fact, and the tier commit is the one that is easy to forget.
- **migration: yes** — it repairs slices already stamped, at the next rewrite.
- Composes well with A: it also repairs the straddles that our own cross-bound
  packing creates.

### C. Logical row count — and it already exists

`read::LogicalCountIndex::count(lo, hi)` (`read/mod.rs:3013`) is an exact,
range-scoped, dedup- and tombstone-aware row count, built per `(project, date)`
by `build_logical_count_partition` (`maintain.rs:3990`) and cached in memory with
a persistent Arrow tier. This is the invariant the problem asks for, verbatim:
invariant under packing, invariant under dedup, invariant under ingest outside
`[lo, hi)`, and it moves on genuine ingest or a delete inside the range.

- **grew: fixed** (range-scoped). **dedup: fixed** (logical).
- **read cost: cheap when resident** — a lookup on a packed index, no scan.
- **preconditions: satisfied on both affected sources.** The index requires
  `dedup_keys == ["timestamp", "id"]` (`maintain.rs:4015`) plus a tiebreak and a
  tombstone column; `schemas/otel_logs_and_spans.yaml` and
  `schemas/otel_metrics.yaml` both declare exactly `[timestamp, id]` /
  `updated_at` / `deleted`. All four darkened tiers are in scope.
- **availability is the catch, and it is a big one.** The index is
  memory-budgeted (`logical_count_memory_bytes`, a quarter per index) and
  keyed by a file fingerprint, so a compaction invalidates the cached index and
  the witness reads `Unverifiable` until the rebuild lands — the same darkness as
  A, arriving by a different door. A 30-day window across many tenants will not
  have every day resident.
- **builder: new tag**, and the build must take the count from the index it
  aggregated against, not from a later read.
- **migration: none.** Old slices stay v1.

### D. Monotonic per-partition logical version counter

A counter bumped only by genuine ingest/DML. `rollup_source_epochs` is already
this, and already consulted on the date path.

- Rejected as the primary: it is per (project, source, date) — whole-partition
  granularity again, so it does not fix `grew` — and it is **in-memory**
  (`mod.rs:2332`), rebuilt at boot from the journal, so it is weakest in exactly
  the restart window the witness exists for. Making it per-slice-range and
  durable is a strictly larger change than A.

### E. Appendix — max tiebreak stamp (evaluated, rejected)

`partition_stats_bounded` already folds `max.{tiebreak}` per partition, and it is
attractive: packing preserves it, and dedup preserves it too (the row dedup
*keeps* is the max-tiebreak one, so a global max cannot be a dedup loser). But it
is whole-partition, so it does not fix `grew`; and it is one-sided — a delete of
any non-maximal row leaves it unchanged, which is the silent-wrong-number
direction. Rejected.

## 3. Recommendation

**C, gated, with B as the immediately shippable half — and neither before the
`grew` amplifier is fixed by the cheapest means available.**

In order of value per unit of risk:

1. **Re-enqueue witness-MOVED slices on sealed days.** `enqueue_witnessless_rebuilds`
   already has the shape; extend its selection to slices whose witness disagrees
   with a partition that is no longer live. Zero correctness surface — it only
   creates work — and it converts "stale forever" into "stale until rebuilt",
   which is what makes every option below actually converge. Do this first.
2. **B (carry-forward)** for the 400 shrink class. Bounded, provable, no new
   comparison semantics, no re-darkening. The work is plumbing: four places hold
   the witness, and the tier tags are the authority.
3. **C (logical witness)** as the target shape, stamped under a new tag, with the
   verdict three-valued so `Unverifiable` (index not resident) is measured before
   anyone reasons about how much coverage it wins. Ship it dark: stamp the new
   witness, verify it, and record agreement against the v1 verdict for a week
   before letting it route anything.
4. **A** only if C's residency turns out to be the binding constraint. It is
   strictly weaker (dedup still voids) and its straddle rule fights hot-tail
   packing.

## 4. What was built

`src/rollup.rs`: `LiveFile`, `SliceWitness` (`Physical` / `PhysicalBelow` /
`Logical`), `WitnessVerdict` (`Valid` / `Stale` / `Unverifiable`), and
`verify_slice_witness`. Pure, no IO, not yet called from the live path.

Two properties are asserted, not asserted-ish:

- `SliceWitness::Physical` reproduces `slice_coverage_agrees` exactly, over its
  own case values — so versioning the witness cannot change how an existing slice
  is judged.
- The four transitions the brief names, per variant, plus: the `grew` shape and
  its fix, the straddle poison, and the cost of the straddle poison on a packed
  day.

The verdict is three-valued because `Stale` and `Unverifiable` demand opposite
work — a rebuild versus making the evidence exist — and today's read path
collapses them into one `stale_coverage` bucket. (The existing
`stale_coverage_metric` splits the *reason* in metrics but not in the decision.)

Also corrected: the doc comment on `storage::CoverageEntry::source_rows` claimed
the field held LOGICAL deduped rows "deliberately not the tag's number". It holds
the tag's number — `record_readable_coverage` copies `TAG_SOURCE_ROWS` straight
in (`maintain.rs:3661`).

## 5. Unresolved — human decisions

- **What fraction of days would read `Unverifiable` under C** given the index's
  memory budget and its file-fingerprint invalidation? This is measurable in prod
  today without shipping anything: count resident-and-current indexes against
  partition cells.
- **Is a metadata-only tier commit acceptable for carry-forward?** It rewrites no
  data but it does add a Delta version per rewrite, and the tier's log is already
  the thing `OPTIMIZE` damages by stripping tags.
- **Whether to do (1) at all**, given that a rebuild wave costs throughput on a
  system whose maintenance units already average ~21 minutes. It is the cheapest
  fix by far and the one with the largest queue consequence.
- **The straddle rule under A** is a policy choice with a real cost, not a bug to
  be engineered away. Someone has to accept the darkness or reject the option.
