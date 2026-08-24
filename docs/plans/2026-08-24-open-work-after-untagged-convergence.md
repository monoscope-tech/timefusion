# Open work after the untagged-tier convergence

**Status:** in progress, 2026-08-24.

| Item | State |
|---|---|
| §1 immutability gap | **§1b instrumented** (`8b8ad30`), default OFF; decision still pending real data |
| §2 preflight floor | **DONE** (`69e6503`) — committed, lint clean, 895/895 lib tests |
| §3 ledger | **steps 1-3 done** (`3ec8003`, `f6b50e5`, `b9572cf`) — types, trait, JSON backend, merge/replace/retirement, populated from the tag replay, and VERIFIED against it (`coverage_ledger_disagreements`). Still additive; tags remain the authority. Next: let the counter read zero over real prod data, then move reads over, then drop the tags |
| §4 escalation treadmill | not started (re-evaluate after §3) |
| §5 fragment debris | not started (deploy with §2, not before) |
| §6 whale cells | no action; 7 as of 11:26 UTC |

**Nothing is pushed.** A non-docs push restarts prod, which kills in-flight
maintenance units (~21 min each) and voids the coverage map — that would undo the
untagged drain still in progress. Push once §6 reaches 0.

History and the diagnosis that produced this list are in
[`2026-08-22-untagged-tier-convergence.md`](./2026-08-22-untagged-tier-convergence.md)
— that doc is the record of what was fixed; this one is what was deliberately
left.

Ordered by value, not by size. Each item names a **success criterion** that can
be checked, because "improve X" cannot be closed.

---

## 1. Write-path immutability gap — the only correctness item

**Size:** medium. **Depends on:** nothing.

### What is wrong

`schema.rs:483` declares columns immutable by default, and that declaration is
load-bearing for performance: *a filter on an immutable column is pushed BELOW
the merge-on-read dedup.* If `level` never changes across versions of a row, it
is safe to evaluate `level = 'error'` before collapsing versions, which is what
makes point lookups cheap.

The declaration is enforced in exactly one place. `extract_dml_info`
(`dml.rs:323`) refuses an UPDATE that assigns an undeclared column
(`dml.rs:434`):

```
UPDATE cannot assign `{blocked}` on `{table_name}`: columns are immutable unless
declared `mutable: true`, and read filters on immutable columns are pushed below
the merge-on-read dedup on that basis
```

**Nothing enforces it on INSERT.** The ingest path — pgwire INSERT, gRPC, WAL
replay — will happily append a second version of an existing `(id, timestamp)`
whose `level` disagrees with the first. Merge-on-read then has two versions that
differ on a column the optimizer was told could not differ.

### Failure scenario

1. A client emits a span with `level='info'`, then re-emits the corrected record
   with `level='error'` (same id). Both land.
2. `WHERE level = 'error'` is pushed below `DedupExec`. The predicate is
   evaluated per-version.
3. Depending on which version survives the pushdown, the row is either
   double-counted or dropped entirely. **Silently.** No error, no metric — the
   dashboard is just wrong.

This is more likely than it sounds: retry-on-timeout with an enriched payload is
a normal client behaviour, and monoscope dual-writes.

### Fix directions

- **(a) Detect and refuse at write time.** Costly — requires a read of existing
  versions on the insert path, which is the hot path. Almost certainly wrong.
- **(b) Detect and report.** A maintenance-time check (it can ride the dedup
  sweep, which already reads every version of a key) that counts keys whose
  versions disagree on an immutable column, exposed in `timefusion_stats`.
  Tells us whether this is theoretical or live before paying for a fix.
- **(c) Narrow the pushdown.** Only push filters on columns that are *both*
  declared immutable *and* part of the identity key. Safe, costs point-lookup
  latency on the rest.

**Recommended order: (b), then decide.** We do not currently know whether any
real tenant produces disagreeing versions, and (c) has a measured latency cost
that should not be paid on a hypothetical.

### Success criterion

A `timefusion_stats` counter `immutable_column_disagreement_total` reporting a
real number from prod over ≥24h of quiet uptime. Non-zero ⇒ implement (c) and
prove it with a test that produces two disagreeing versions and asserts the
query result is correct with and without the pushdown.

---

## 2. Preflight bisection floor — the queue-debt engine

**Size:** medium. **Hot path.** **Depends on:** nothing.
**Note:** this section is materially cheaper than previously scoped — the
escape hatch already exists in the code.

### What is wrong — this is a PRICING bug, not an ordering one

`byte_bounded_units` (`maintenance_coordinator.rs:434-468`) is the splitter.
Its structure:

1. bytes fit `MAX_DECODED_BYTES` (512 MiB) ⇒ one unit;
2. width > `MIN_SLICE_MICROS` (1 min) ⇒ **bisect in time**, recursing on halves;
3. only at the 1-minute floor ⇒ **hash-shard**.

The defect is how step 2 prices the children (`:444-451`):

```rust
let left_bytes = bytes * (midpoint - start) / width;   // prorate by TIME SHARE
```

**Children are priced by time share — a model, never a measurement.** So the
recursion is guaranteed to terminate on paper: keep halving and the number
eventually drops under 512 MiB, whatever the real cost is.

Reality has a floor. A slice must read **at least one row group of every file it
overlaps**, so below some width the true cost stops tracking the time range.
Measured this session: **302 MB for a 5-minute slice.**

The two disagree, and the disagreement is discovered *later*: the preflight
(`database/maintain.rs:1603`) measures the child's bytes for real at claim time,
finds it over budget, and calls `split_time_task` again. Each level costs a
claim, a real scan-estimate and a journal write, and the cycle runs to the
1-minute floor. Prod held **3,455 units for one (project, tier, day)**, 1,423
pending — the origin of the ~12,700 fragment units in §5.

Note `split_time_task:2213` already refuses a split whose children would need
hash shards, and step 3 exists — so the machinery to divide **rows** instead of
**time** is present. It is simply unreachable until bisection has already ground
the slice to a minute.

### Why the obvious fix is wrong

"Hash-shard when bisecting stops paying" needs to know where the floor is. The
floor is `files × row_group_bytes`. `InputFootprint`
(`maintenance_coordinator.rs:331-342`) records `whole_file_bytes` and `files` —
**but not row-group size.** Using `whole_file_bytes` as the floor would
hash-shard days that bisection handles perfectly well today, because a file with
100 row groups genuinely does cost ~1/100th for a narrow slice.

**Do not guess a constant here.** The floor moves with file layout and therefore
per tenant and per day.

### Fix direction

Close the loop with the measurement we already take, rather than modelling it:

1. Record the parent's **measured** bytes on each child at split time
   (`split_time_task` already receives `observed_bytes`).
2. At the next preflight, compare the child's freshly measured bytes against
   that inherited parent measurement. If halving the width bought less than
   ~25%, the floor dominates — **hash-shard at the current width instead of
   bisecting again.**

This learns the floor per unit instead of assuming it, needs no new datum in
`InputFootprint`, and degrades to today's behaviour when bisection is working.

Alternative if that proves fiddly: record `row_groups` in `InputFootprint` at
selection time (it is available from the parquet metadata already being read)
and compute the floor directly. Costs a journal format field; gives an exact
answer instead of a feedback loop.

**Do not** change the bisection arithmetic itself, and do not lower
`MIN_SLICE_MICROS`. Both make more units, which is the symptom.

### Success criterion

Replay a known whale day through `timefusion sim` (or `run-unit --op
BaseRollup` against staging) and show the same day completes in **tens of
units, not thousands**, with no unit exceeding `MAX_DECODED_BYTES` at run time.

Regression test, per the mandatory bug-fix workflow: a synthetic day whose
measured child bytes do **not** fall with width must produce hash-sharded units
at a width above `MIN_SLICE_MICROS`, and must not recurse to the floor. Write
this test **first** — the pathology is a multi-level cycle through the journal,
so a single-call unit test on `byte_bounded_units` does not reproduce it.

---

## 3. Coverage off file tags — the durable ledger

**Size:** large. **This is the chosen architectural end-state** (decided
2026-08-24). **Depends on:** nothing, but §5 should land first so the migration
does not carry debris forward.

### What is wrong

Every parquet file in a rollup tier carries its own identity in Delta metadata
tags (`maintenance_coordinator.rs:123-137`):

```
timefusion.slice_start_micros / slice_end_micros   what time range this covers
timefusion.generation                              which rebuild produced it
timefusion.source_fingerprint / source_rows        what it was built from
timefusion.project / source
```

**The tags are the coverage record — there is no other one.** Answering "is
2026-08-14 rolled up and current for project X?" means replaying the whole Delta
log and reading tags off every live file (`recover_rollup_coverage`,
`maintain.rs:3137+`).

Every pathology of the 08-22 session traces to that:

| Symptom | Cause |
|---|---|
| 85 immortal untagged files | a delta-rs `OPTIMIZE` rewrote files and dropped custom tags — coverage state destroyed by an unrelated maintenance op |
| cannot simply delete them | an untagged file is uninterpretable; hence the three `slice_retires` proofs and the `rows > 0` guard (`rollup.rs:749`) |
| old tail has no ETA | cells with no tagged ranges and 30+ day old sources can satisfy no proof, ever |
| coverage read is ~25 min | full log replay + per-file tag scan; hence hourly recovery, hence lagging routing |
| routing dark for ~1h after every deploy | `rollup_min_contiguous_days` is rebuilt from that replay and resets to 0 on restart |
| **tier files can never be compacted** | two files covering different slices cannot merge into one file with one slice tag — so the tier stays thousands of ~1 MB files. **The tier built to make 30d queries fast is itself fragmented.** |

### The design

Move coverage out of per-file tags into an explicit durable ledger:

```
(source, project, table, date) → [ { slice: [a,b), generation, source_fp, source_rows }, … ]
```

Then:

- **Files become anonymous.** A tier file is just data. Compaction merges freely
  because there is no identity to destroy. **The untagged-file damage class
  cannot exist** — not "is defended against", cannot exist.
- **Tier compaction becomes legal** → thousands of small files collapse into a
  few large sorted ones → 30d rollup queries stop paying per-file open cost.
- **Coverage read is one GET**, not a log replay. Routing works from the first
  second after a restart instead of an hour later.
- **The staleness witness becomes sound.** `source_rows` in the ledger can hold
  a *logical* row count. Today the tag is compared against `num_records`, which
  is **physical** (tombstones + merge-on-read versions), so benign dedup
  invalidates correct rollups — see
  `tf_witness_counts_physical_rows_2026-08-23`.

### Invariants — these are the whole design

1. **Commit-then-ledger, never the reverse.** The Delta commit and the ledger
   write are not one transaction, and the failure modes are asymmetric:
   - ledger *understates* coverage ⇒ a wasted rebuild. Cheap.
   - ledger *claims* coverage that is not there ⇒ **wrong query results.**

   So the ledger is written only after the commit lands, and a crash in between
   leaves an understating ledger. Never the reverse ordering.
2. **Recovery prefers rebuild over trust.** Any disagreement between ledger and
   reality resolves to "rebuild", never to "assume the ledger is right".
3. **The ledger is the authority, so it must be auditable.** Keep
   `recover_rollup_coverage` as a *verifier* — a periodic pass that replays the
   log and asserts the ledger agrees. It becomes an alarm rather than the
   primary read path. Cheap insurance against the one risk this design adds:
   today's tags cannot drift, because each file carries its own truth.

### Storage: write against an interface

A real datastore (Postgres, SlateDB, …) is planned for all sidecar state. The
ledger must therefore be defined as a **trait**, with today's JSON-in-S3 as the
first implementation, so the datastore is a backend swap and not a rewrite.

Existing sidecars to model it on and eventually migrate behind the same
interface (`storage.rs:3541-3588`):

```
dedup_certifications.json    CERTIFICATIONS
dedup_slice_coverage.json    SLICE_COVERAGE
rollup_untagged_cells.json   UNTAGGED_CELLS
dedup_dirty_bins.json        DIRTY_BINS
```

They currently share one shape: whole-file `store_sidecar` replace, best-effort,
warn-on-failure. That is adequate for hints and **not** adequate for an
authority. The interface needs at minimum: point read by cell, ranged write,
and a durability contract stronger than "warn and continue".

### Cleanup — required, not optional

The ledger grows forever without it. Needs:

- **Retirement:** drop ledger entries whose partition has aged past retention.
- **Compaction:** merge adjacent slices of the same generation into one entry,
  or the entry count per cell grows with every incremental build.
- **Orphan sweep:** entries naming files no longer live (found by the §3
  verifier pass).

### Also approved: tag-aware compaction

The user approved doing **both**. Tag-aware compaction is the cheap 80% and can
ship first, independently: merge only files whose slices are contiguous and
share a partition/generation, and write the union slice as the output tag.
Bounded change, gets the file-count win, leaves the self-describing property
intact. It does not kill the damage class — that is what the ledger is for.

### Success criterion

- Ledger: a rollup tier where **every live file carries no identity tags at
  all** and coverage/routing/staleness are fully correct, proven by the verifier
  pass reporting zero disagreements over ≥24h.
- Compaction: rollup tier live-file count down by ≥10x, and the 30d whale
  dashboard query re-measured against the numbers in
  `tf_query_latency_matrix_2026-08-22`.

---

## 4. Escalation treadmill

**Size:** medium. **Depends on:** §2 (fewer fragments ⇒ fewer escalations).

### What is wrong

`covered_by_wider` (`database/maintain.rs:1896-1924`) refuses to publish a slice
contained inside a live wider tagged slice — correctly, because publishing the
contained span would leave two overlapping claims of coverage.

The consequence is a treadmill: a 10-minute invalidation cannot be repaired by a
10-minute rebuild. It must re-target the **covering** slice, which for a whale is
1.5–3 hours of work. Repeatedly. `rollup_skipped_covered_by_wider` fired 5 times
within an hour of one deploy.

Each escalation is correct in isolation. In aggregate this is where sealed
maintenance capacity goes.

### Fix directions

- **(a) Split the covering slice on demand.** Rather than rebuild 3h to repair
  10 min, first split the live covering slice into (before, hole, after) so a
  narrow rebuild becomes publishable. Costs one metadata-only commit.
- **(b) Batch escalations.** If N holes inside the same covering slice are known,
  rebuild the covering slice **once**. Requires the coordinator to see the set,
  which it does — `uncovered_gaps` (`rollup.rs:797`) already computes exactly
  this per partition.
- **(c) Do nothing after §3.** In the ledger world, coverage is not carried by
  files, so there is no "wider file" to be contained by, and this refusal has no
  reason to exist. **If §3 is happening, this item may be free.**

**Check (c) before building (a) or (b).**

### Success criterion

`rollup_skipped_covered_by_wider` at or near zero over ≥24h of quiet uptime,
with tier coverage still converging (the counter must not fall because nothing
is being attempted).

---

## 5. Fragment debris cleanup

**Size:** small-medium. **Depends on:** §2 landing first, or the debris returns.

### What is wrong

~12,700 units of queue debt from earlier shreds. Most describe slices that are
subsumed by, or irrelevant to, current coverage. They inflate every queue gauge
and cost `claim_next` real work on every tick — the queue was measured at
**339x inflated** at one point (`tf_queue_is_339x_inflated_2026-08-19`).

### Fix direction

A one-shot cursor-guarded migration. Precedent exists and should be copied
directly: `migrate_fine_grained_backfill`
(`maintenance_coordinator.rs:843`), driven from `database/mod.rs:4141`, guarded
so it runs exactly once.

**Landmine, from the 08-19 session:** `checkpoint()` cannot delete — a removing
pass must rewrite the journal, not append to it. See
`tf_journal_checkpoint_cannot_delete_2026-08-19`. The existing migration already
handles this correctly; follow it rather than inventing a path.

**Do not run this before §2.** Clearing debris while the engine that produces it
still runs just regenerates it, and burns the one-shot cursor.

### Success criterion

Pending unit count drops to within ~2x of the real cell count (the 08-19
measurement put 88,100 pending against 260 real cells), and
`oldest_task_age` stops reporting values in the tens of days.

---

## 6. Remaining whale cells — no action

**Size:** none. Recorded so it is not re-investigated.

**7 untagged files as of 2026-08-24 09:49 UTC** (down from 85). All are project
`87576849` July days plus 08-01, 139–1,422 minutes of uncovered hole each,
draining internally at ~740 min/hour.

This is wall-clock, not a defect. The count only steps when a whole cell's hole
closes, so long flat stretches between steps are expected, **not** evidence of a
stall — that misreading cost most of a night on 08-22.

**Closing criterion:** re-run
`scripts/rollup_untagged_cells.py` (or the session's `measure.sh`) and confirm
0. If it is still non-zero after ~24h of quiet uptime, *then* it is a defect and
the first thing to check is whether `recover_rollup_coverage` is running at all
(it is hourly since `b68f042`).

**Measure it from the Delta log, never from a counter.** Four separate
instruments lied during the 08-22/23 sessions, and all four lied in the
direction of good news.

---

## Suggested order

```
§5 gated behind §2       (debris returns if cleaned first)
§2  → unblocks throughput, cheapest real win, escape hatch already in the code
§1b → one counter; tells us whether §1 is real before paying for it
§3  tag-aware compaction  → the cheap 80% of the latency win
§3  ledger                → the architectural end-state
§4  → re-evaluate; may be free once §3 lands
```
