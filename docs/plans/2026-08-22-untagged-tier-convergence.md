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
