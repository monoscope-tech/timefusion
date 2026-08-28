# The rollup staleness witness: what it actually refuses, and what to replace it with

Base: `e67a149` (an ancestor of the branch point `1447e6f`, which adds two
unrelated rollup-routing commits). Default behaviour is unchanged. The deliverable
is the diagnosis, the option comparison, and a predicate —
`rollup::verify_slice_witness` — with a case table asserting the four transitions
the brief names plus the three that carry the corrected diagnosis.

> **VERIFICATION STATUS: VERIFIED (updated 2026-08-25).** This supersedes an
> earlier banner here that said the code had never been compiled or run — that
> was true when written, and is no longer.
>
> These commits were merged with five other branches onto
> `integration-2026-08-25` and verified centrally over the combined tree:
> `cargo check --lib --tests` exit 0, and **`cargo lint` exit 0** (the alias in
> `.cargo/config.toml`, which is CI's exact clippy invocation with `-D warnings`).
> The full suite was run on that tree; no failure traced to this change.
>
> What is verified is that the predicate COMPILES and LINTS and that its case
> table runs. What is NOT verified is any production behaviour: nothing here is
> on a default path — `slice_coverage_agrees` delegates under
> `SliceWitness::Physical`, which is the same comparison as before, so this
> change is behaviour-neutral by construction. `PhysicalBelow` and `Logical`
> remain unreachable until a builder stamps a v2 witness.
>
> Every FACTUAL claim below is a code reading with a file:line, independent of
> compute, and stands on its own.

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
(`maintain.rs:3238`, fed from `:3596` and `:3644`) selects `source_rows.is_none()`
only. That is the amplifier
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

The brief asked whether the rewrite paths hold the information this needs. **They
hold it already, pre-computed, and the arithmetic is exact.** This is the cheapest
option on the list by a wide margin and the finding that most changes the picture.

Every source-partition rewrite lands through `Database::commit_wave`
(`maintain.rs:5834`), and a wave is homogeneous by assertion — *"a wave must not
mix data-preserving and row-dropping units"*. `StagedBin::data_change`
(`mod.rs:6979`) is **derived, never stored**, from `dedup.is_some()`, so the
commit site knows with certainty which kind of rewrite it is committing:

- **`data_change == false` (hot-tail packing / light optimize): the witness needs
  no update at all.** A bin-pack preserves `num_records`, so the physical sum is
  unchanged and the witness is still true. Nothing to carry.
- **`data_change == true` (dedup): the delta is already computed.**
  `DedupUnit { date, before, after }` with `dropped() = before - after`
  (`mod.rs:7099`), and `wave_dropped_rows(&result.landed)` (`mod.rs:7119`) already
  sums it **over the landed bins only** — a unit that failed to commit contributes
  nothing, which is exactly the correctness property a witness update needs. It is
  called today at `compact.rs:1029` purely to return a metric.

So the carry-forward is, per landed dedup bin,
`witness(project_id, bin.dedup.date) -= bin.dedup.dropped()`. No scan, no
recomputation, no extra object-store round trip. The same shape as the
independently-derived equality in `resume_verdict` (`mod.rs:6946`), which already
compares `target_rows` against `staged_rows` from Add statistics and calls a
repair *"data-preserving by construction"*.

- **grew: not addressed at all.** This is the 400, not the 4103.
- **dedup: exactly fixed**, arithmetically, at zero marginal cost.
- **read cost: nil** — the read path is unchanged.
- **builder: no change to what is stamped.** The real cost is per (d): the
  recovery authority is the **tier file tags**, and the rewrite happens on the
  **source** table. A durable carry-forward means updating four holders of one
  fact — in-memory `rollup_slice_coverage`, the journal publication, the coverage
  ledger, and the tier files' `TAG_SOURCE_ROWS`. The first three are cheap; the
  tags need a metadata-only Remove+Add commit against the tier. Skip the tags and
  the repair is undone by the next restart, which prod does constantly.
- **soundness precondition, and it holds:** dedup is logically neutral to a
  rollup only if it drops exactly rows the build never counted. It does: the
  sweep collapses versions of the same `dedup_keys` keeping the greatest
  `dedup_tiebreak`, and the builder shards on `source_schema.dedup_keys`
  (`maintain.rs:1818`) reading the same deduped view — one schema declaration
  drives both. A row dropped for a tombstone predating the build was already
  absent from the build's input; a tombstone written after it went through
  `invalidate_rollup_dml` and removed the slice coverage outright.
- **migration: yes, and uniquely so** — it repairs witnesses already stamped, at
  the next rewrite, with no republish. Every other option leaves existing slices
  on v1 semantics forever.
- Composes with A: it also repairs the straddles our own cross-bound packing
  would otherwise create.

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

### E. Content digest over the logical rows, carried forward (rejected)

A hash of the logical rows in `[lo, hi)` is the ideal invariant — strictly
stronger than C, since it also catches an UPDATE that preserves the row count.
It fails on the read side, which is where it has to be paid.

The witness must be re-proved **per query**, and re-proving a digest means
rehashing every row it covers — a full scan of the window, i.e. exactly the raw
read the rollup exists to avoid. Carrying it forward across a rewrite (which is
sound: the digest is invariant under any logical-content-preserving rewrite, so
it needs no update at all) removes the *write*-side recomputation but not the
read-side one, and the read side is the binding constraint.

The only way a digest becomes affordable is if something maintains it
incrementally on ingest — at which point it is D with a hash instead of a
counter, and inherits D's problems. C dominates it: a count is the same idea with
an O(1) verification, already implemented and already resident.

### F. Appendix — max tiebreak stamp (evaluated, rejected)

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
2. **B (carry-forward)** for the 400 shrink class, and it is a much smaller job
   than it sounds: the delta is `wave_dropped_rows`, already computed over the
   landed bins at `compact.rs:1029`. No new comparison semantics, no re-darkening,
   and it is the only option that repairs slices already stamped. The work is
   plumbing — four holders of one fact, and the tier tags are the authority.
3. **C (logical witness)** as the target shape, stamped under a new tag, with the
   verdict three-valued so `Unverifiable` (index not resident) is measured before
   anyone reasons about how much coverage it wins. Ship it dark: stamp the new
   witness, verify it, and record agreement against the v1 verdict for a week
   before letting it route anything.
4. **A** only if C's residency turns out to be the binding constraint. It is
   strictly weaker (dedup still voids) and its straddle rule fights hot-tail
   packing.

## 4. What was built

`src/rollup.rs`: `SourceFile`, `SliceWitness` (`Physical` / `PhysicalBelow` /
`Logical`), `WitnessVerdict` (`Valid` / `Stale` / `Unverifiable`), and
`verify_slice_witness`. Pure, no IO.

**What is wired, and what is not.** `slice_coverage_agrees` now delegates to
`verify_slice_witness` under `SliceWitness::Physical`, which is the same
comparison it performed inline — so there is one definition of what re-proves a
slice, and no behaviour change and no flag. `PhysicalBelow` and `Logical` are
never constructed outside the tests: STAMPING either means a new Delta tag on the
build path, which this change deliberately does not touch. They carry a narrow
`#[allow(dead_code)]` with that reason, because a predicate that cannot express a
candidate cannot be used to evaluate it, and the case table is the evaluation.

No config flag was added. A flag would only be honest if there were a second
behaviour to select, and there is not: nothing stamps a v2 witness yet, so a flag
would gate an unreachable branch. The flag belongs with the builder change.

The delegation's proof is the **pre-existing** nine-case table
(`slice_coverage_is_trusted_only_when_every_witness_matches_the_partition_now`),
whose hard-coded verdicts were written against the old body and are re-asserted
against the new one. A test comparing the two implementations would be circular
now that both run the same code, so one was written, recognised as circular, and
removed.

The new case table asserts the four transitions the brief names, per variant,
plus the three that carry the corrected diagnosis: the `grew` shape and the fact
that only a bounded or logical witness survives it, the straddle poison that makes
a bounded witness sound, and what that poison costs on a packed day. Also
asserted, against the brief: benign bin-pack compaction is `Valid` even under v1.

**These tests have not been run** — see the status note at the top.

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
