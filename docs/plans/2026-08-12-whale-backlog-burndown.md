# Whale backlog: burndown, not a drain

**Status 2026-08-12.** Replaces P1 §4 of
[2026-08-11-repair-and-resilience-next-steps.md](2026-08-11-repair-and-resilience-next-steps.md)
("Drain 07-31 (36 files) … ~8 hours"). That item's numbers were inferred from `EXPLAIN`; these
are read from the Delta checkpoint. The work is real, it is ~30x larger than budgeted, and it
does not fit the shape of a single supervised drain.

**Do not start the 8-hour drain.** There is no 8-hour job here.

---

## The measurement

Active add files for project `87576849-4941-49d3-a15d-680fef88a1a8` ("past3" / monoscope-self),
from checkpoint `v478202` (`numOfAddFiles=2494` table-wide):

| | |
|---|---|
| Affected band | **2026-07-20 … 2026-08-02** (14 dates) |
| Active files in band | **1038** |
| Bytes in band | **751.2 GB** |
| Median file | 758 MB |
| p90 / max | 917 MB / 1918 MB |
| Over the 1 GiB repair cap | **27 files (2.6%), 40.0 GB** |
| Repairable today | **1011 files, 711.2 GB** |

Every date outside the band sits at 1–6 active files. Compaction works; something specific to
that window left ~750 GB uncompacted and it never caught up.

Two numbers in the old plan are simply wrong, and both mattered:

- 07-31 is **82 files / 36 GB**, not "36 files".
- 08-01 is **80 files / 35 GB**. The old plan records `08-01=0` — clean. It is not.

## What the repair loop can actually do

From `config.rs` plus the prod service env:

| knob | value | note |
|---|---|---|
| `timefusion_footer_repair_schedule` | `0 30 * * * *` | hourly at :30 |
| `timefusion_footer_repair_budget_secs` | 8640 (default; unset in prod) | 2.4 h per pass |
| `timefusion_repair_max_file_bytes` | **1 GiB** (prod raises the 512 MiB default) | above this, skipped |
| `timefusion_repair_lookback_days` | 31 | the band is 10–23 days old, so in scope |
| `timefusion_repair_resume_enabled` | on | a restart costs the bin, not the pass |

`spawn_cron_job` skips overlapping ticks rather than queueing them, so the hourly cadence with
a 2.4 h budget means: one pass runs, the ticks it overruns are dropped, the next starts after
it ends. Effective duty cycle is ~1 pass per ~2.4 h, not 24 passes/day.

## Why this is weeks

At the old plan's own observed rate — **~13 min/bin** — 1011 repairable files is
**~220 hours of repair time**. Even at 100% duty cycle that is nine days; at the real duty
cycle, with deploys interrupting bins, meaningfully longer.

That is the whole point: **the job is a backlog burndown with a throughput target, and it must
be measured in files/day, not run to completion in one sitting.**

## How to run it

1. **Set a rate target and watch it, not the clock.** The only number that matters is active
   files in the band, per day, from the checkpoint. Re-run the measurement (one object read,
   costs nothing, no query against the poisoned partitions):
   `_delta_log/_last_checkpoint` → `NNNN.checkpoint.parquet` → count `add.path` matching
   `project_id=87576849…/date=…` in `2026-07-20 … 2026-08-02`.
2. **Burn newest-first.** 08-02 (32) and 07-25 (33) are the cheapest dates; 07-24 (121),
   07-22 (116) and 07-23 (113) are the expensive ones. Clearing cheap dates first converts
   whole partitions to bounded sooner, which is what the read path actually cares about — a
   partially repaired partition still voids ordering.
3. **Leave the 27 over-cap files for last, and decide them separately.** They are 2.6% of
   files but 40 GB, concentrated on 07-28 (12) and 07-25 (8). Options: raise
   `timefusion_repair_max_file_bytes` past 1918 MB (the sort then holds proportionally more —
   the plan's memory note measured a whale repair sort at 14.4 GB against a 4 GiB budget, so
   this is not free), or split them via the recompress path first. **Do not raise the cap
   blind**; size the sort against the memory budget first.
4. **Stop deploying TF during a pass if you want the rate to hold.** Resume protects
   correctness, not throughput: each restart discards the in-flight bin's work. The 2.4 h
   budget means a deploy has a ~2.4 h window to land in and a good chance of hitting one.

## Exit criteria

- Active files for the band back to the 1–6/date that every neighbouring date already shows.
- A single-date `EXPLAIN` on each cleared date reports `bounded` — **run it twice, warm**, and
  treat a single reading as meaningless (see the method note below).
- monoscope's enrichment `UPDATE`s stay clear. Already true as of 2026-08-12 (6 h of prod logs:
  zero `SortPreservingMergeExec`, zero `full-set` fallbacks), so this is a regression check,
  not a fix to wait for.

## Method note — do not repeat these mistakes

- **`EXPLAIN` is not a measurement here.** The same single-date probe returned no `DedupExec`,
  `bounded`, and `full-set` within one hour, depending on cache warmth. Every EXPLAIN-derived
  number in the predecessor plan — including "10d bounded, 11d full-set" — is unreliable, and
  two conclusions were drawn from it today in opposite directions before the checkpoint settled
  it.
- **Physical object listing is not ground truth either.** 07-31 lists 124 parquet objects
  against 82 active; the difference is retention-protected tombstones.
- **The Delta checkpoint is ground truth**, costs one object read, and cannot OOM the server.
  A `count(*)` against a poisoned partition can — do not reach for one.
