# Why the untagged tier files stopped converging — 2026-08-22

Measured, not assumed. Sources: a fresh `_delta_log` replay of all four tiers
(checkpoint + every later commit) and prod's `maintenance_tasks.json` pulled
read-only with `docker cp`.

## Where it stands

85 untagged tier files, down from 197 → 158 → 123 → 85. Both 1h tiers are at 0.
But the drain has **stopped**, not slowed:

| block | files | movement since the last measurement |
| --- | --- | --- |
| 2026-08-18 | 30 → 0 | cleared completely |
| 2026-08-19 | 54 | unchanged |
| old tail (< 08-12) | 18 | unchanged, every date byte-identical |

## Which proof is missing, per file

`slice_retires` retires an untagged file on three proofs: (A) a day-wide slice
publishes, (B) the file's stats sit inside one live tagged slice, (C) the union
of live tagged slices covers its stats. Attributing every one of the 85:

- **~15 files: proof B already holds.** Their stats sit inside a live tagged
  slice, yet they are still live — because a proof is only *evaluated* when that
  slice publishes, and nothing has republished it since. One republish of the
  covering slice retires each of them.
- **~52 files: proof C, union has holes.** The gaps are printed per file. Mid-tail
  gaps are tiny (`08-15 18:00–18:22`, `08-17 12:00–12:22`, `08-14 07:30–09:00`) —
  one targeted publish each. The 08-19 gaps are 12–18 **hours**: those partitions
  hold exactly ONE live tagged range apiece, so essentially nothing has published
  for that day since the damage.
- **18 files: the old tail.** Several of those cells have ZERO tagged ranges and
  their source dates are 30+ days old. If raw has aged out, `rows > 0` correctly
  refuses to retire what may be the only copy. No tag-based proof can ever reach
  them soundly.

Two hypotheses were tested and **refuted**: the `covered_by_wider` publish veto
reads slice TAGS only, so an untagged file cannot veto the publishes that would
retire it; and the 08-19 units are not missing — the journal holds 160+ COMPLETE
units per tier for that day.

## The actual blocker: a sealed day shreds into ~1,440 one-minute units

For (`00000000`, one tier, one day) the journal holds **3,455 units**: 2,015
superseded, **1,423 pending**, 17 complete. Their widths are a clean bisection
ladder — 1440m → 720 → 360 → … → 1m — all created in a single instant. That is
one `byte_bounded_units` recursion, not a split/fuse cycle.

It shreds to the floor because the preflight measured the day at **387 GB**
decoded, and `split_time_task` is called with the whole estimate (unlike
`abandon_running`, which deliberately passes `MAX_DECODED_BYTES + 1` to ask for a
single bisection). Each 1-minute child still estimates **188–282 MB** — because
the estimate has a floor no time-split can lower: a slice claims at least one ROW
GROUP of every file whose range overlaps it, and these partitions hold hundreds
of files. Bisecting past that point stops reducing per-unit cost and only
multiplies the unit count.

Meanwhile each of those minutes publishes ~57 rows.

347 cells hold more than 50 units each; 12,734 units are pending overall, 12,692
of them already past their deadline. A damaged cell cannot converge because
closing its coverage union requires *all* of its ~1,440 units to publish, and they
are queued behind everything else.

## Correction: the shred is the MID-TAIL's problem, not 08-19's

The fragmented cells above are 08-11..08-15, not 08-19 — a day-bucket arithmetic
slip on my part. 08-19 is the opposite shape, and its own evidence is decisive:

- Every untagged 08-19 file carries `delta-rs.optimize.sort_by` and was written
  on **08-19**, the newest at 22:54. The tag-stripping OPTIMIZE has not run since.
  That is why there is no new damage — the stripper is already dead.
- Every tagged file in those partitions was written on **08-20**, after the
  retirement code shipped.
- Every damaged 08-19 partition has its day-wide rebuild **already queued, already
  past its deadline, at `attempts=0`**, estimated at 268–537 MB — small enough to
  run unsplit.

So the 08-19 work is queued, due, cheap, and simply never claimed. The reason is
`claim_next`'s `hole_rank`: a cell whose tier output is missing outranks one that
"already has output and is merely being re-derived". These partitions DO have
output, so they rank last — behind ~12,000 hole-filling units that the backfill
mints about as fast as it drains them.

That is the bug. An untagged file cannot be certified and cannot be retired; the
partition is missing coverage no matter how much tagged output sits beside it.

## Shipped

1. **`untagged_cells` ranks as a hole.** `recover_rollup_coverage` already reads
   which partitions hold untagged files; it now publishes them to the journal and
   `fills_a_hole` consults them. Kept as a separate set from `tier_holes` because
   the two are produced by different passes — a wholesale replace by either would
   erase the other, which is exactly how `base_tier_ready` and `tier_holes` were
   both made inert on prod once already. Cleared per `(source, tier)`, and cleared
   again by the publish that leaves a partition clean, because recovery runs only
   at startup.
2. **Rebuild the GAPS, not the day.** `enqueue_untagged_rebuilds` queued a day-wide
   unit, which is what fed the shred. It now queues `uncovered_gaps(untagged
   spans, tagged ranges)` — minute-aligned and clamped to the partition. A cell
   with nothing tagged still yields the file's own span, so this is a refinement of
   the old behaviour and never narrower than what it replaced.
3. **A gap-free partition still gets a publish.** ~15 of the 85 files satisfy a
   proof ALREADY and are live only because proofs are evaluated at publish time
   and nothing has republished the partition. Those now re-publish their own spans.

## What this means

- The damage is no longer wrong data — both tiers declare identity and reads
  collapse versions. What is left is scan cost: 85 redundant files of ~6,265 live
  (~1.4%).
- The mechanism is sound and proven (08-18 went 30 → 0 through exactly this path).
- The unreachable population is not "large tenants" as first reported. It is
  "partitions whose day was shredded to the minimum slice", which is a property of
  file count, not tenant size — that is why the *default* project is the worst
  offender, with 3,455 units for a single day.

## Expected, and how to falsify it

- **08-19 (54 files)** — 20 due day-wide units that now rank as holes and fit the
  budget. Should clear within hours. If `rollup_tier_untagged_found` does not move
  for those cells, the hole rank is not reaching them and the next thing to check
  is whether `recover_rollup_coverage` ran at all after the deploy.
- **Mid-tail (~13 files)** — now queued as minute-to-hour gaps instead of days, so
  each is one unit that fits. If these stall, the gap computation is wrong, not the
  scheduling: compare `uncovered_gaps` against the printed per-file gaps.
- **Old tail (18 files)** — **accepted floor, no ETA.** Several of those cells have
  no tagged ranges at all and their source dates are 30+ days old; if raw has aged
  out, `rows > 0` correctly refuses to delete what may be the only copy. Removable
  only by tier compaction (Phase 4), and worth ~0.3% of live files.

Three speeds are expected, because two of the three blockers were fixed and the
third — the preflight shred — was not:

- **08-19 metrics cells: fast.** The whale's metrics day already completed
  day-wide at ~537 MB / 537k rows, so it fits, and `-width` puts day-wide units
  first within the hole set.
- **08-19 logs cells for the big tenants: possibly still shredding.** Their
  queued `estimated_decoded_bytes` is just the `MAX_DECODED_BYTES` passed at
  enqueue — unmeasured. The preflight measures for real at claim time, and an 18h
  gap for a large tenant may still be over budget. The fragments now inherit the
  hole boost, so the union still closes, only slowly. **If these cells are still
  more than half intact ~12h after the deploy, the next required change is the
  preflight passing `MAX_DECODED_BYTES + 1` like `abandon_running` does — one
  bisection with re-measurement, instead of a linear extrapolation to the floor.**
- **Mid-tail gap units: fast.** A 22-minute gap sits at the row-group floor
  (~270 MB), under budget.

Replaying `uncovered_gaps` over the live log before the deploy predicts exactly
what will be queued: **83 units across 42 cells, and not one of them day-wide.**
The mid-tail cells come out at 3–90 minutes each (`dcad860a` 08-14 is 3 minutes,
`6297304f` 08-17 is 22), which is the whole point — those replace day-wide units
that shred. The 08-19 cells still come out at 21–22 hours where only one tagged
range exists, so those remain the population at risk from the preflight.

A behavioural consequence worth stating: because no queued slice spans a whole
day, retirement now comes through proof B/C rather than proof A. That is sound —
each slice is derived from the untagged file's own span and rounded OUTWARD to
the minute, so `lo >= start && hi < end` holds by construction — but it does mean
a partition converges as its files are individually contained, not all at once.

Two things that will appear in the logs and are correct, not failures:
`maintenance_rollup_escalated_to_covering_slice` for the gap-free fallback units
(their spans are contained in a live tagged slice, so `covered_by_wider` escalates
once, and the covering slice then publishes and retires them via proof B); and the
mid-tail's existing ~1,400 one-minute fragments now ranking as holes too — width
ordering keeps them behind the day-wide and gap units, and `clear_untagged_cell`
un-boosts the cell as soon as it is clean. That fragment debris remains as queue
debt after convergence and is follow-up work, not a blocker here.

Caveat, written rather than coded because no live file has it: a partition whose
only untagged file carries no statistics gets an `untagged_cells` entry but no
enqueued slice — a rank boost with no work attached.

## Measured after the deploy — two more defects, both found by disbelieving a counter

**The gap units run and publish.** Comparing log replays either side of the
deploy, the 08-19 cells GREW tagged ranges (`00000000` 1 → 4, `87576849` 1 → 3)
while the mid-tail cells consolidated (74 → 58, 89 → 68) as wider slices absorbed
narrower ones. That is the mechanism working.

**But the live untagged count did not move: still exactly 85.** Two reasons, both
now fixed:

1. **`rollup_tier_untagged_retired` counted INTENDED retirements.** It fired where
   the replace-set is computed, ~80 lines before the commit, past `still_running`
   and past `slice_occ_stale`. Prod read 21 retired inside an hour while the log
   replay said nothing had left the table — three deploys had killed units in
   between. The counter was the harmless half: `clear_untagged_cell` sat in the
   same block and removed a partition's hole boost the moment a unit *decided* it
   would be clean, which switches the prioritisation off on exactly the cells that
   still need it. Both moved after the commit (`a60c87c`).
2. **Coverage recovery ran ONCE at startup.** It is the only pass that sees an
   untagged file, so it is the only thing that can enqueue the republish that
   retires one — and a file becomes retirable when OTHER slices publish, long
   after boot. An hour in, **30 of the 85 satisfied a proof and none had been
   retired**, because nothing queued the publish that would evaluate it. Now
   hourly (`b68f042`); it is metadata-only, far cheaper than the 60s planner tick
   beside it.

The general lesson, which cost most of the session: a gauge that only updates on
publish reads 0 both when clean and when unset, and a counter placed before the
commit reads progress that did not happen. The authority for "did a file leave
the table" is a Delta-log replay, never a process-scoped counter.

## It is converging — 85 → 47, and the stall was my ruler

The count sat at exactly 85 across three snapshots while the service logged real
retirements (`dcad860a` 08-19 retired 3, `28f62f01` retired 4 + 1 + 4). Both
could not be true.

The enumerator merged **every** checkpoint parquet in the log directory — 34 of
them for the 1m tier — unioning their adds. The removes that retired those files
live in commits at or below the newest checkpoint, which the replay then skips by
design. So the count was a high-water mark that could never fall. Reading only
the NEWEST checkpoint version (keeping every part of it, since multi-part
checkpoints share a version) gives **47**, down from 85 and still falling.

Fixed in `scripts/rollup_untagged_cells.py`, which had the same defect and is the
tool anyone else would reach for. This is the THIRD measurement error in one day
on the same question, after the pre-commit counter and the publish-only gauge —
and the tell was the same each time: **a number that will not move is a claim
about the ruler, not about the system.**

What actually cleared: the whale's 17 metrics files and `28f62f01`'s 17 log files
for 08-19 — exactly the population the hole-rank fix targeted.

## Overnight: 85 → 26, and a FOURTH broken ruler

The count reached 30 by 00:48 and then read **0 for six consecutive hours**. It
was not converged: the hourly script's `aws s3 sync` had silently downloaded
nothing, every tier replayed as "0 live, 0 untagged", and a total of zero is
indistinguishable from success. The real figure at 07:07 was **26**.

The measurement script now refuses to report at all unless every tier has commit
files on disk and the replay finds >1,000 live files — it prints `MEASURE FAILED`
and exits non-zero instead. It is also incremental now; re-downloading ~150 MB
hourly had degraded to 17 KiB/s.

That is four measurement failures in one investigation — a publish-only gauge, a
counter before its commit, an enumerator that could only ratchet up, and a sync
that reported success over an empty directory. Every one of them read as *good
news*. **Build the guard into the instrument, not into the reader's memory.**

## Damage repair was ranked below the starvation window

`scheduling_class` returns `(class, starved, …)` and `starved` is compared BEFORE
`hole_rank`. It is set only for work aged **3 to 31 days**. Every untagged file
still standing on 08-23 was **32 to 37 days old**, so it fell outside the window,
sorted below the entire ~12,000-unit backfill queue, and could never be reached
no matter what its hole rank said. Damage now leads its class outright
(`fa5d2f7`): the starvation window is a freshness heuristic, and damage is not a
freshness question.

Observed while diagnosing this, and NOT yet addressed: narrow units repeatedly
escalate to a covering slice — a 10-minute unit escalates, its 1.5h or 3h covering
slice rebuilds and publishes, and another 10-minute unit inside the same day
arrives and does it again. Each cycle spends a multi-hour rebuild on a
ten-minute invalidation. It is correct (the escalation exists to prevent a double
count) but it is a treadmill, and it is where sealed-tier capacity is going.

## Explicitly rejected

- **Journal-derived `covered`.** The journal knows which slices were built, but a
  built slice whose file was later rewritten into the untagged file proves nothing
  — retiring on that record would delete the only copy. Only LIVE tags prove
  reproducibility.
- **Zero-row coverage markers.** The theory was that empty sub-slices leave
  permanent holes in the union. The 08-19 gaps are 12–18 hours of real data, so
  this is the wrong mechanism for this population.
- **Tagging the untagged files in place.** Circular: their tags would then count
  as coverage proving themselves redundant.
- **Damaged-cell scheduling priority beyond the hole rank.** The last change to
  the sealed claim share OOM-killed prod at 124.9 GB. Measure the hole rank first.
