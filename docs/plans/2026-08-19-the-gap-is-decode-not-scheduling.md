# The gap is decode, not scheduling

Status: **measured 2026-08-19 against the live prod journal and prod tiers.**
Contains one implemented-and-rejected fix, three corrections to claims made
earlier in the week, and two measurement traps that produced them.

The short version: `rollup_min_contiguous_days` is stuck at 2 because ~4.9 TB of
base-tier decode has not happened yet, not because the scheduler picks the wrong
task. Every scheduling change of 2026-08-17..19 acted downstream of that.

## The causal chain, end to end

```
~4.9 TB of pending base (1m) work across 2026-08-06 .. 08-14
   -> those days' 1m tier is INCOMPLETE
   -> the proof loop skips any day still missing its base tier
   -> the 1h derived tier gets ~0 coverage before 08-13
   -> 1h contiguity = 2
   -> 14d/30d dashboards cannot use rollups
```

The third link is one condition in the backfill census path in `database.rs`:

```rust
for ((project_id, date), missing) in &missing_tiers {
    if *date >= today || missing.iter().any(|i| schema.rollups[*i].derive_from.is_none()) {
        continue;   // still missing its BASE tier -> never prove the DERIVED tier
    }
```

**This is correct and must not be "fixed".** It refuses because the 1m tier
genuinely is not built for those days, not because of a bookkeeping bug. At
least three hypotheses this week (including two of mine) went looking for a
defect at this layer. There isn't one.

## The measurement

1m vs 1h coverage by date, projects with any coverage, read from prod:

| date | 1m projects | 1h projects |
| --- | --- | --- |
| 2026-08-19 | 12 | 12 |
| 2026-08-15 | 12 | 12 |
| 2026-08-14 | 13 | 7 |
| 2026-08-13 | 12 | 3 |
| 2026-08-12 | 10 | 0 |
| 2026-08-10 | 8 | 0 |
| 2026-08-02 | 11 | 0 |

1h coverage appears **only** where 1m is complete. This is not an
intersect-across-projects bug — `all_base_tier_ready` is built per
(project, date). Those days simply still carry thousands of pending base tasks
each.

Pending `base_rollup` decode per day (`estimated_decoded_bytes`, logs source):

| date | pending | complete | superseded | pending GB |
| --- | --- | --- | --- | --- |
| 2026-08-10 | 2353 | 8 | 1626 | 807.5 |
| 2026-08-13 | 1985 | 87 | 1866 | 634.3 |
| 2026-08-14 | 682 | 28 | 611 | 210.2 |
| 2026-08-15 | 357 | 612 | 289 | 129.4 |
| 2026-08-16 | 14 | 888 | 13 | 0.3 |
| 2026-08-17 | 715 | 1059 | 24 | 0.6 |
| 2026-08-18 | 620 | 1096 | 11 | 0.0 |

Two regimes: 08-16 onward is essentially done; 08-06..08-14 carry 200-800 GB
each. Days with ~0 GB but a high pending COUNT are frontier-minted slices
carrying `estimated_decoded_bytes: 0` — not free work, just unestimated.

**Scale, stated carefully.** 4.9 TB is the `otel_logs_and_spans` source over
those nine days. Across all sources and dates the pending estimate is **~26 TB
over 211 (source, project, day) cells**. The raw sum is trustworthy: `TaskKey`
is unique per (table, source, project, slice, operation), and `split_time_task`
marks the parent `Superseded`, so children partition the parent's bytes rather
than duplicating them. The per-unit estimate was independently verified on prod
at 515 MB actual against 491 MB predicted.

Do **not** try to "correct for overlap" by applying a densest-byte-rate across
merged spans. I did; it returned 37 TB, which is larger than the raw sum, and it
is simply wrong.

## Implemented and rejected: "split children inherit the parent's width"

Sealed ordering is `(class, starved, -width, -recency)`. Width proxies backfill
provenance — a day-sized unit comes from the backfill planner, a ten-minute one
is live-minted. `split_time_task` breaks that proxy: a day unit's children are
still backfill work but measure 180s and rank below every day-wide unit in
history. Prod bears this out — project `87576849`'s 2026-08-10 day unit was split
into 928 fragments in one burst on 08-17 11:23-11:31, and over the next 40
minutes sealed BaseRollup claims went to 2026-07-22 while 08-10 got none.

So I added `backfill_priority_micros`, inherited it in `split_time_task`, and
read it from `scheduling_class`. It compiles, it has a regression test that fails
without it and passes with it, and all 60 coordinator tests pass.

**It is counterproductive and was not shipped.** The comparator is pure, so both
orderings can be replayed over the real journal in Python in seconds:

```
best rank of a 2026-08-10 task among 78,121 sealed pending base_rollup
OLD (own width):            5,375    (whale 5,382)
NEW (inherit day width):   12,940    (whale 12,943)   <- WORSE

first 300 sealed claims by date
OLD:  2026-08-18 x300
NEW:  2026-08-15 x297, 08-16 x1, 08-17 x2             <- capacity moves BACKWARD
```

Promoting every sub-600s fragment to day weight also promotes 08-15/16/17's
fragments, and those are newer, so the recency tiebreak puts them ahead of 08-10.
I optimised the rule I had identified instead of the outcome I wanted.

The test was not wrong — it asks a local ordering question and the answer really
did change. It cannot express "does the blocking day get reached sooner". **A
green targeted test is not evidence that a scheduling change helps; only a replay
over real queue state is.** Branch `fix/split-inherits-backfill-priority` keeps
the work so nobody re-derives it.

Under *either* ordering the blocking day sits 5k-13k deep in a 78k queue, so no
comparator tweak reaches it in useful time.

## Three corrections to earlier claims

- **`derived_refusal` is a SAMPLE, not a veto.** It is `first_refused_sealed(...)`
  — the first refused sealed derived task, printed as an example.
  `dependencies:87576849:2026-08-10` was never "one project-day gating 406 derived
  units". Chasing it cost hours. The code documents the pairing: `cells_missing>0`
  with `cells_wanted=0` means the work is already queued and the question is why
  it is not *claimed*.
- **Sealed work is not starved to zero.** Live `maintenance_task_started` shows
  BaseRollup claiming 2026-07-22 alongside 08-18 and 08-19. The `sealed_turn`
  reservation works.
- **`attempts` is not a reliable "was this ever claimed" signal.**
  `split_time_task` sets `child.attempts = 0`, so a zero means "not claimed since
  the split". Verify against `event="maintenance_task_started"` aggregated by
  slice date instead.

## Two measurement traps

1. **`docker service logs --since 24h` only reaches back to the CURRENT
   container.** With a 22-minute-old container it returns 22 minutes, so "event X
   never happened in 24h" is unprovable that way. Read
   `docker ps --format "{{.Status}}"` first. This nearly produced a false
   conclusion about the tantivy backlog reconcile.
2. **Deploy cadence invalidates throughput numbers.** Six images in ~2.5 hours on
   2026-08-19 (`01caa46 -> 35bd1cf -> 0920aaf -> 441421a -> 032d64b -> ae22152`),
   a restart every 15-20 minutes, against rollup units whose scan phase alone is
   ~8 minutes and debt units at 12-15. A large share of maintenance is killed in
   flight. Batch the merges, then leave prod alone for hours before believing any
   convergence number.

## What moves a 26 TB number — and two levers that turned out NOT to

The obvious candidates were tested against the journal and the code, and both
are refuted. Recording them so they are not proposed a third time.

**Compaction is not the lever, because these days are already compacted.**
`sealed_consolidation` state for the gating days:

| date | consol pending | consol complete | base_rollup pending |
| --- | --- | --- | --- |
| 2026-08-06 | 0 | 37 | 1376 |
| 2026-08-10 | 1 | 34 | 2353 |
| 2026-08-13 | 9 | 49 | 1985 |
| 2026-08-14 | 5 | 53 | 682 |

Consolidation has essentially finished for 08-06..08-15 and the decode estimate
is still 200-800 GB/day. Consolidation reduces FILE COUNT (and therefore
file-open overhead and query planning cost); it does not reduce DECODED BYTES,
which is what a rollup must pay to aggregate every row once.

**The estimator is not inflating rollups either.** It would have been a good
story — a full-row estimate over a narrow rollup projection would both overstate
the TB and over-split units (the 928-fragment explosion). It is false: the rollup
path scales by projection exactly as the dedup path does,

```rust
let decoded = add.size * 12;
estimated += decoded * projected_numerator / projected_denominator;
```

and its numerator is genuinely narrow — `project_id, date, timestamp` plus
`dedup_keys`, `dedup_tiebreak`, `tombstone_column`, `spec.dimensions`,
`spec.measures` columns and filter tokens, over `source_schema.fields.len()`.

**So the ~4.9 TB is real, projection-aware, irreducible decode.** Building 9 days
of 1m rollups for this data means reading those bytes once. That leaves only:

1. **Decode throughput** — bytes/sec actually achieved, which is where per-unit
   phase timing (`scan_ms=481682` for 142 rows) still deserves attention: the
   question is no longer "how many bytes" but "why so few bytes/sec".
2. **Not being restarted.** Six deploys in 2.5 hours discards partial work on
   units that need 8-15 minutes. This is the cheapest available win and costs
   nothing but patience.
3. **Deriving rather than rebuilding.** The 1h tier derives from 1m. Where 1m is
   complete this is cheap; it is not happening for 08-01..08-14 because of link
   one of the chain, not the derive path.

Convergence is wall-clock physics and cannot be compressed. What can be
compressed is decode throughput and the number of restarts that discard partial
work.
