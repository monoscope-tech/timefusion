# Open work after the untagged-tier convergence

**Status:** 2026-08-24. **Every item that can be implemented without prod has
been.** Four sections are closed (§2 fixed, §4 and §5 resolved as needing no
code, §1b instrumented). The three that remain are not blocked on engineering —
their success criteria are defined as PROD MEASUREMENTS over a quiet period, and
prod has been redeploying every ~10-25 minutes all day. **Read the HANDOFF
section before anything else.**

| What remains | Why it cannot be closed today |
|---|---|
| §1 decision (fix (c) or close) | criterion is `immutable_column_disagreement_total` over ≥24h of quiet uptime |
| §3 step 4 ENABLEMENT | engineering is done and equivalence is proven locally; flipping reads is a judgement call that wants `coverage_ledger_disagreements` at zero at production scale first |
| §6 drain to 0 | blocked by deploy churn; needs a push freeze, not code |

| Item | State |
|---|---|
| §1 immutability gap | **§1b instrumented** (`8b8ad30`), default OFF; decision still pending real data |
| §2 preflight floor | **DONE** (`69e6503`) — committed, lint clean, 895/895 lib tests |
| §3 ledger | **steps 1-4 engineering done** (`3ec8003`, `f6b50e5`, `b9572cf`, `98e72d5`, `21f3951`) — built, populated, verified, entries name their files, and `routing_view` now answers routing's question from the ledger. **The tag-equivalence gate is checked LOCALLY** on real Delta data: every range the tag-derived map covers is covered by the ledger. Reads are NOT flipped — that is a deliberate config decision, not missing work |
| §4 escalation treadmill | **investigated, no code — two corrections** (`98e72d5`). (b) batching is ALREADY implicit: `enqueue` is keyed, so N escalations to one covering slice collapse to one task. (a) the on-demand split written in this doc is UNSOUND — the covering file physically holds the hole's rows, so re-tagging it narrower double-counts. Escalation stays correct until the ledger can name files per range |
| §5 fragment debris | **RESOLVED - no migration, and it must not be written.** `clear_stale_estimates` already exists, and the partition ceiling rescues footprint-less debris by FUSING it (proven by test). Deleting those units would have discarded real queued work |
| §6 whale cells | **BLOCKED, not self-completing — earlier assessment was wrong.** See "Why the drain stopped" below |

## HANDOFF — read this first (session ended 2026-08-24 ~14:15 UTC)

### What is deployed vs what is not

The checkout is SHARED with another Claude session working the tantivy/rollup
plans, and its pushes carried some of my commits with them. So "what I pushed"
is not the same as "what is live". Verified with
`git merge-base --is-ancestor <sha> origin/master`:

| Commit | What | State |
|---|---|---|
| `69e6503` | §2 preflight floor fix | **PUSHED — live in prod** |
| `3ec8003` | ledger types/trait/backend | **PUSHED — live** |
| `f6b50e5` | tag replay populates ledger | **PUSHED — live** |
| `b9572cf` | ledger verifier + `replace()` | **PUSHED — live** |
| `8b8ad30` | §1b immutability audit (default OFF) | local only |
| `98e72d5` | coverage entries name their FILES | local only |
| `a9ab66e` | declined unit is still claimable | local only |

`git status`: 13 ahead / 6 behind `origin/master`, and the ahead-list is
INTERLEAVED with the other session's commits. Do not force-push, do not rebase
without checking whose work is in the tree.

### TRAP: the first deploy of `98e72d5` will fake a §3 gate failure

`CoverageEntry.files` was added in `98e72d5`, which is NOT deployed. Prod is
therefore writing ledger entries with **no file identity**, and the field is
`#[serde(default)]`, so those entries reload as `files: []`.

The moment `98e72d5` ships, the verifier compares `held` (no files, from the old
sidecar) against `proved` (with files, from the replay), they differ, and
**`coverage_ledger_disagreements` will spike once for essentially every cell**.

That is a serialization artifact, not drift. It should clear on the following
hourly pass, because `replace()` overwrites the cell with the proved version.
**Do not read the §3 gate until at least two hourly recovery passes have run on
a build that includes `98e72d5`.** Misreading that spike as real drift would
send someone chasing a ledger bug that does not exist.

The cheaper alternative is to delete `.timefusion_meta/rollup_coverage_ledger.json`
once when that build first boots; the ledger is rebuilt from the tag replay, so
losing it costs nothing.

### What blocks everything else

Prod redeployed **5+ times in 51 minutes** (see the next section). Maintenance
units average ~21 minutes and do not start until 300s after boot, so they never
finish, and every process-scoped counter is immature. Both remaining gates —
§1's `immutable_column_disagreement_total` and §3's
`coverage_ledger_disagreements` — are unreadable until pushes stop.

### Next actions, in order

1. **Freeze pushes to master for ~1h.** Not a code change. Nothing else on this
   list can be measured until maintenance units survive to completion. Confirm
   with `docker service ps srv-captain--timefusion` showing one task older than
   ~30 minutes.
2. **Confirm §6 drains.** `scripts/rollup_untagged_cells.py`, or the session's
   `measure.sh`. It sat at 7 all session, entirely because of (1). Measure from
   the Delta log, never from a counter.
3. **Push the three local commits**, then let two hourly passes run, then read
   `coverage_ledger_disagreements` — remembering the trap above.
4. **Turn on `TIMEFUSION_IMMUTABLE_AUDIT_ENABLED`** for ~24h of quiet uptime and
   read `immutable_column_disagreement_total`. Non-zero means §1 is real and
   fix (c) is justified; zero means the pushdown premise holds and §1 closes.
5. §5 needs nothing — it is closed. If the queue still looks inflated after a
   quiet period, check whether `ceilings` is populated for those partitions
   rather than deleting units.

### Corrections made this session — do not re-derive these

- **§2 is a PRICING bug**, not an ordering one. `byte_bounded_units:444` prices
  children by TIME SHARE, so the recursion always fits on paper while the real
  cost floors out. Also: the runner ALREADY hash-shards internally at any width
  (`maintain.rs:1623`), so bisection is a cost optimisation, not a memory
  mechanism — declining a split is safe.
- **§4 option (a) is UNSOUND.** Splitting a covering slice by re-tagging does
  not work: the file physically holds the hole's rows, so both copies get
  summed. Option (b) is already implicit — `enqueue` is keyed by `TaskKey`.
- **`record()` was append-only and wrong.** Coverage is not additive; a slice
  rebuilt under a new generation supersedes the old one. Fixed by `replace()`
  in `b9572cf`. Without it the ledger would have served coverage whose files
  were gone.
- **The ledger needed file identity** (`98e72d5`) or it could only ever
  supplement the tags, never replace them — files stay non-anonymous, the tier
  stays non-compactable, and the entire point is lost.
- **"7 unchanged is expected granularity" was WRONG.** It was a stall with a
  cause. Four hours were spent reporting it as healthy.

### What could not be done, and why

- **No `timefusion sim` replay of a real prod journal.** The journal is not
  readable as `ubuntu` without sudo, and `docker cp` is outside the read-only
  remit for that host (logs / `inspect` / `ps` only). §2 is therefore verified
  by unit tests and reasoning, NOT against real queue shape. That validation is
  still owed.
- **No §5 migration.** It is destructive, its premise (~12,700 units) is
  unverified, and the one sample taken read 5,983 against a 6-minute-old
  process — which is exactly the young-process artifact that has produced false
  "it is fixed" readings here before.

---

## Why the drain stopped (2026-08-24 13:30 UTC)

`untagged` sat at 7 for four hours and I read it as expected wall-clock
granularity. It is not.

`docker service ps` shows **five deploys in 51 minutes**, each a DIFFERENT image
(`5bb5f1c`, `86132a3`, `c8860f5`, `3700730`, `1e42237`) — so this is deploy
churn from concurrent pushes, not a crash loop. Shutdowns are graceful SIGTERM.

The arithmetic settles it:

- maintenance does not start until the cache preload finishes or times out, and
  the log shows `maintenance_coordinator_preload_wait_expired waited_secs=300`
  on every boot — so **the first 5 minutes of each container do no maintenance**;
- a container lives ~10-25 minutes between deploys;
- a maintenance unit averages **~21 minutes**.

So a unit essentially never completes. The journal survives; the work does not.
This is the same shape as `tf_units_die_to_restarts_2026-08-23`.

**Consequence for everything else in this document:** every prod measurement
taken under this cadence is immature, including the §5 queue depths above and
any future reading of `coverage_ledger_disagreements` (§3's gate). The gate
cannot be evaluated until pushes stop.

**The fix is not code.** It is a push freeze on master long enough for the drain
to finish — roughly an hour of quiet would let units complete for the first time
today.

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

## 5. Fragment debris cleanup - RESOLVED, and the migration must NOT be written

**Status: closed 2026-08-24. No migration. The machinery already exists and is
wired; what was missing was evidence, now pinned by
`a_footprintless_shred_fuses_once_the_partition_ceiling_is_known`.**

### What this section originally called for, and why it was wrong

A one-shot destructive migration deleting ~12,700 stale units. Three findings
retire that plan:

1. **`clear_stale_estimates()` already exists** - a one-shot, cursor-guarded
   migration doing exactly what was proposed, with the
   `checkpoint()`-cannot-delete landmine already handled. Writing a second one
   would have duplicated it.
2. **The rescue does not need deletion at all.** The debris is footprint-less
   one-minute units whose stored estimates are WHOLE-FILE figures, so their sum
   grows with the shredding and the fit test refuses hardest exactly where
   fusing is worth most. `coarsen_sealed_slices_capped` bounds that price by
   what the partition actually holds - no unit over one partition can decode
   more than the partition contains - and that ceiling alone rescues them. It is
   wired in prod at `database/maintain.rs:2383` with a real ceilings map.
3. **Section 2 removed the source.** The preflight no longer shreds past the
   row-group floor, so the population stops growing.

### The evidence

`a_footprintless_shred_fuses_once_the_partition_ceiling_is_known` builds the
exact prod shape - 600 one-minute units each claiming 4,466,185,462 bytes with
no `InputFootprint`, the `base_rollup / 00000000 / 2026-08-13` signature - and
shows:

- **without** a ceiling, `coarsen_sealed_slices` fuses **0** - the stuck state;
- **with** the real ceiling, the day collapses to under a tenth of the units,
  and wider units remain.

That last assertion is the point. **Fusing preserves the work; deleting it would
not.** A migration that removed these units would have discarded queued rollup
work whose only defect was a mispriced estimate.

### What is left

Nothing to implement. If prod still shows an inflated queue after section 2 has
been live through a quiet period, the question is whether `ceilings` is
populated for those partitions - not whether to delete units.

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
