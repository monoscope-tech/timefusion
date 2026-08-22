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

## What this means

- The damage is no longer wrong data — both tiers declare identity and reads
  collapse versions. What is left is scan cost: 85 redundant files of ~6,265 live
  (~1.4%).
- The mechanism is sound and proven (08-18 went 30 → 0 through exactly this path).
- The unreachable population is not "large tenants" as first reported. It is
  "partitions whose day was shredded to the minimum slice", which is a property of
  file count, not tenant size — that is why the *default* project is the worst
  offender, with 3,455 units for a single day.
