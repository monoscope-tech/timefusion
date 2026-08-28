# 2026-08-28 — the base damage repair CONVERGED (81/81), and the 1h derived tier did not

Two results, one closing an item and one opening a bigger one.

## 1. The repair cursor finished, and it was never observable

`TaskJournal::repair_cursor` reads `snapshot.source_cursors` out of
`.timefusion_meta/maintenance_tasks.json`. It has **no `timefusion_stats` key** —
the same two-hand-maintained-lists trap as ever. `pending_repair`,
`repair_bins_in_flight` and `repair_resumed_total` are all exposed and are all
*different populations*; `pending_repair` is `Operation::Repair` queue depth, not
the damage-list position. Being unable to read the one number that mattered is
what kept this item "blocked" for several sessions.

Read it with `docker cp` (host path is root-owned; the image is distroless, so
neither `cat` on the host nor `docker exec cat` works):

```
__maintenance_damage_repair_v2:otel_logs_and_spans = 81
__maintenance_damage_repair_v2:otel_metrics        = 81
```

against an 81-entry `DAMAGED_CELLS`. **The walk is complete.** The cursor only
advances past cells a pass resolved, and `damage_repair.get(81..)` is already an
empty slice — so the mechanism is *inert in production today* and deleting the
const is cleanup, not a behaviour change.

## 2. Base tier: 81 of 81 cells converged

`sum(request_count)` on `otel_logs_and_spans_rollup_dashboard_1m_v3` vs a raw
control, all 81 pairs. **79 exact on the first pass; the remaining 2 were
mid-rebuild while being measured** and read exact ~40 min later:

| cell | stored @T0 | stored @T1 | raw | final |
|---|---|---|---|---|
| `28f62f01`/08-25 | 3,435,243 | 3,732,004 | 3,735,349 | −0.09% |
| `87576849`/08-25 | 3,077,293 | 3,633,115 | 3,633,893 | −0.02% |

The two headline cells from the original report:

| cell | was | now | raw |
|---|---|---|---|
| `28f62f01`/08-24 (deadline-bound) | 1,036,140 | 3,837,269 | 3,837,269 |
| `87576849`/08-02 | −95.9% | 15,109,489 | 15,109,489 |

`28f62f01`/08-24 was the cell that "becomes permanently wrong the moment it
seals". It converged before sealing.

### Two measurement traps, both of which produce a FALSE PASS

* **A "raw" control that silently routes.** `AND coalesce(name,'~') =
  coalesce(name,'~')` returned the correct number for a 15M-row day **in 0.2s** —
  the predicate is folded away and the query routes to the rollup, so it compares
  the rollup to itself. Only a `GROUP BY` on a non-dimension column forces a real
  scan: same cell, **34.4s**. Any control must be *timed*; a fast raw scan is not
  a raw scan.
* **A 60s server statement timeout.** Whole-project scans died at exactly 60s and
  emitted `ERROR: canceling statement due to statement timeout` **onto stdout**,
  where a naive parse reads it as a row. Sweep per (project, date) and assert the
  result matches `^[0-9]+$`.

Also: **stored and raw must be read close together.** Reading all 81 stored
values first and the raw controls ~40 min later is what produced the two false
"still short" cells above — they were being rebuilt in between.

## 3. NEW — the 1h derived tier disagrees with its own base on 29 of 81 cells

Same 81 cells, `dashboard_1h_v2` (derived) vs `dashboard_1m_v3` (its base). The
base is now verified against raw, so the base is the reference.

**29 of 81 disagree by >0.5%.** Both directions:

| cell | 1h | 1m (verified) | delta |
|---|---|---|---|
| `87576849`/08-01 | 39,412,317 | 23,103,653 | **+70.6%** |
| `6297304f`/08-17 | 31,969,113 | 22,258,419 | **+43.6%** |
| `28f62f01`/08-01 | 3,542 | 2,281,455 | −99.8% |
| `00000000`/08-13 | 26,297 | 2,243,077 | −98.8% |
| … 25 more | | | −60% to −90% |

Single `rollup_generation` per cell, so this is **not** a generation artifact.

Slot analysis says there are two distinct failures:

* `28f62f01`/08-01 — **1 hour slot** present (hour 23), 3 rows. A cell that
  published essentially nothing. This is the "hour 23 absent" signature from the
  original report, now on the derived side.
* `87576849`/08-01 — **19 of 24 slots**, yet **70.6% over** a complete base. Both
  incomplete *and* inflated, which is the signature of summing merge-on-read
  versions of base rows (see the 2026-08-20 note on derived rollups summing MoR
  versions).

### It is NOT limited to `otel_logs_and_spans`

The v2 cursor read 81/81 for **both** sources, and the same comparison on
`otel_metrics_rollup_metrics_1h_v2` vs `_1m_v2` (`point_count`) over the same
project/date pairs: **12 of 64 cells disagree by >0.5%**, all SHORT, up to
**−84.7%**:

| cell | 1h | 1m | delta |
|---|---|---|---|
| `6297304f`/08-25 | 728,651 | 4,754,256 | −84.7% |
| `8100121c`/08-25 | 4,395,496 | 28,311,615 | −84.5% |
| `8100121c`/08-24 | 6,779,495 | 34,116,402 | −80.1% |
| … 9 more | | | −2.3% to −74.9% |

Caveat on the reference: the **metrics 1m tier has not itself been verified
against raw** — only the logs base was. So this measures *disagreement between
tiers*, not which side is wrong. It is enough to say the derived path is not a
logs-only problem, and no over-counts appeared on this table.

**Why it matters:** the matcher picks the coarsest grain dividing the requested
bucket width, so wide-window dashboards (30d charts) read *this* tier. A +70%
cell renders as a traffic spike that did not happen; a −99% cell renders as an
outage that did not happen.

**Correction to the record:** an earlier session logged `87576849`/08-01
converging to "39,412,317 = truth". Today the raw control says **23,103,653** and
the 1m tier agrees with raw, while **39,412,317 is exactly what the 1h tier
holds**. Whether the old note read the wrong tier, or raw really was ~39M
physical then and dedup compaction has since collapsed it (this project is in the
32x-duplicates family), is **not re-verifiable after the fact** — do not assert a
mechanism. The actionable conclusion is unchanged: 23.1M is the number today.

## What is NOT done, and why

* **`DAMAGED_CELLS`** — emptied in `5d696741`, and made a config parameter.
  Emptied rather than deleted: the 1h tier repair below needs the same machinery,
  so refill it under a **v3** migration key (v2's cursor is spent). Emptying it
  silently disarmed the end-to-end cursor guard — it drove a real pass against
  the const and passed vacuously with nothing to consume, which is the v1
  truncation bug's blind spot — so that test now supplies its own 30-cell list.
  **Committed, not pushed**: a non-docs push redeploys prod and kills in-flight
  maintenance.
* **The 1h tier repair** — NOT started, and the attribution now says a rebuild
  would be actively wrong.

  **It is not lag.** Every one of the 29 has `derived_rollup` tasks in the
  journal in state `complete`; none are pending. The units ran, finished, and the
  tier is still wrong. The unit counts split the population: the old severely
  short cells completed **1-2** derived units for a whole day (`28f62f01`/08-01:
  2 units, and exactly 1 of 24 hour slots present), while the recent -70% cells
  completed 40-49.

  **And it is LIVE, not a stale artifact.** Dating the bad 1h cells by
  `max(updated_at)`:

  | 1h cell | written | delta |
  |---|---|---|
  | `87576849`/08-01 | **2026-08-28 09:44** | **+70.6%** |
  | `28f62f01`/08-25 | **2026-08-28 12:31** | -75.7% |
  | `6297304f`/08-17 | 2026-08-20 | +43.6% |
  | `28f62f01`/08-01 | 2026-08-19 | -99.8% |

  Two cells were written TODAY, after the 2026-08-26 fix (`43ee5d84`), and are
  still publishing both inflated and short. So a rebuild re-runs the same code
  that produced them. **Fix the derived path first, then rebuild** — and note the
  -75.7% cell was derived while its base was itself mid-rebuild (that base had 2
  `base_rollup` units still `pending`), which is precisely what the 2026-08-25
  "a derived unit whose base does not tile its slice RETRIES instead of
  publishing short" guard was supposed to prevent. That guard is the first thing
  to look at.

## 4. The two directions are TWO DIFFERENT DEFECTS, and only one is a live bug

Later on 2026-08-28, against a 2.7 h-quiet prod process on `2e45a8c`. Section 3
treated over-count and short as one derived-path bug. They are not.

### 4.1 The over-count is STALENESS. It is not an aggregation bug.

`87576849`/08-01, single generation, 39,412,317 against a verified base of
23,103,653 (+70.6%). Write times settle it:

| tier | min(updated_at) | max(updated_at) |
|---|---|---|
| `dashboard_1h_v2` | 2026-08-28 **09:44:32** | 2026-08-28 09:44:32 |
| `dashboard_1m_v3` | 2026-08-28 **09:49:13** | 2026-08-28 **14:46:12** |

The derived cell was built **five minutes before** its base began being rebuilt,
and the base kept being rewritten for five more hours. The 1h cell is a faithful
aggregate of the pre-repair base. And it will never correct itself: its unit is
`complete`, and **rebuilding a base tier does not invalidate its derived tier.**

Ruled out by measurement, not by argument: there are **no duplicate**
`(timestamp, service_name, kind, status_code)` rows in that cell, so it is not
merge-on-read duplication at the output level.

**The defect is therefore a missing invalidation edge: base rebuilt ⇒ derived
stale.** That is what the v3 rebuild has to fix, and a rebuild alone is not
enough — without the edge the next base repair reproduces it.

### 4.2 A day now holds SEVERAL generations — real, new, and NOT lost rows

`generation_id` hashes the spec **restricted to the measures the cell
materialized** (`7249b36c`, 2026-08-26 — deliberately, so that *appending* a
measure stops orphaning 30 days of history). Derived units are per HOUR, so two
hours whose base cells proved different measure sets now mint different
generation ids.

Measured, `28f62f01`/08-25 `dashboard_1h_v2` — the two generations agree on every
measure column except one:

| generation | rows | request_count | duration_sum | server_duration_sum | **service_name_hll** |
|---|---|---|---|---|---|
| `33903afe6182754a` | 4 | 4 | 3 | 2 | **4** |
| `bc815739d2507900` | 73 | 73 | 55 | 37 | **0** |

Days holding >1 generation, last 30 days: `28f62f01` 08-24/25/27/28, `87576849`
08-24/25/26(**3**)/28, `6297304f` 08-26(**3**)/28. **Nothing before 08-24.**

**Correction to an earlier reading in this session.** I first concluded the read
path pins ONE generation per `(project, date)` and that the rest of the day is
therefore unreadable. **That is wrong.** `ProjectRoutingTable` pushes a
`(project, date, generation)` triple **per SLICE**
(`src/database/mod.rs`, the `for (key, coverage) in fresh` loop), and
`RollupRewrite::sql` joins them with **`OR`**. A day with two generations is read
as a disjunction over both. No rows are lost to the split itself.

What the split *does* cost is `measures_available`: it is checked **per slice**,
so a query using `service_name_hll` now drops the `bc815739` slices to the raw
fringe. Safe, but it converts a routed read into a partly-raw one. Before
`7249b36c` a day's cells shared one measure set and this could not happen.

### 4.3 What is still unexplained — the short cells

Generation handling does not account for it. `28f62f01`/08-25 holds **906,302
across BOTH generations** against a base of 3,732,004 — still **-76%**. Hours
01, 18, 20, 21, 22 are absent outright, and most present hours are fractions of
their base (hour 02: 29,431 of 209,685). Hours 00 and 23 are exact.

"Absent hours plus fractional hours plus a few exact ones" is the shape of a day
whose derived units were **split into children and only some children
published** — `split_time_task` cuts a unit, and a sub-hour child publishes a row
bucketed at the whole hour holding only its own range. **Not yet verified.** The
captured journal (`maintenance_tasks.json`, 2026-08-28) is the place to check it:
look for `derived_rollup` units on this cell whose slice is narrower than an hour.

### 4.4 Measurement rule this session established

**Summing a rollup table directly sums ACROSS generations and is NOT what a
routed read returns.** Every tier comparison must `GROUP BY rollup_generation`
and state which generations a routed read would name. A generation-blind sum
reported `28f62f01`/08-25 as one -75.7% cell; it is two disjoint cells.

## 5. The short cells: units COMPLETE on the first attempt holding 11-47% of their base

From the prod journal captured 2026-08-28 (`maintenance_tasks.json`, 59,076
tasks), cell `28f62f01`/08-25, `dashboard_1h_v2`. Every derived unit against the
rows it actually published:

| hour | unit state | attempts | published | base | ratio |
|---|---|---|---|---|---|
| 00 | complete | **4** | 140,962 | 140,962 | **100.0%** |
| 01 | complete | 1 | 0 | 136,778 | 0.0% |
| 02 | complete | 1 | 29,431 | 209,685 | 14.0% |
| 03-17 | complete | 1 | — | — | **11.4% - 46.6%** |
| 18 | complete | 1 | 0 | 162,311 | 0.0% |
| 19 | complete | 1 | 28,200 | 135,429 | 20.8% |
| 20 | complete | 1 | 0 | 151,566 | 0.0% |
| 21 | **NO UNIT** | — | 0 | 142,718 | 0.0% |
| 22 | **NO UNIT** | — | 0 | 118,081 | 0.0% |
| 23 | complete | **2** | 124,870 | 124,870 | **100.0%** |

Two results, both sharp:

**1. The `attempts` column is the whole story.** The only two hours that are
EXACT are the only two that ran more than once. Every single-attempt unit
published a fraction — and then marked itself `complete`, so it will never be
retried. Whatever the first attempt races against, a second attempt wins.

**2. Two hours were never enqueued at all.** The day's original day-wide derived
unit is `superseded / migrated_to_aligned_hour_slice`; that migration minted 22
hour units, not 24. Hours 21 and 22 have no `derived_rollup` task in the journal
in any state. (`9f679039 leave split children out of the hour migration` is in
the 2026-08-28 push and is in this territory — re-check after it is live.)

### Two hypotheses refuted here, so they are not re-tried

* **Split children.** Every `derived_rollup` unit for this cell is exactly
  60 minutes wide. There are no sub-hour children. The section-4.3 hypothesis is
  dead.
* **"Fewer base units ⇒ smaller ratio."** Hour 02 has 2 of 6 ten-minute base
  units and reads 14.0%; hour 17 has **all 6** and reads 14.0% as well. The
  shortfall does not track base-unit count.

### What this means for the guard

`base_tier_incomplete` retries only when `rollup_slice_coverage` shows an
uncovered range. These units saw no hole — they were admitted, ran, and published
a fraction. So either coverage claimed a range whose rows were not yet readable
by the derived scan, or the scan selected only part of what coverage promised.
**The guard is checking a different thing from what the scan reads**, and that
gap — not generations, not splitting — is the short-cell defect.

**Next probe, and it is cheap:** re-run one of these hours as a single unit
(`timefusion run-unit --op DerivedRollup`) against the same storage and compare
what it publishes to the 11-47% already there. If a fresh single run is exact,
the defect is a race and the fix is in admission; if it reproduces the fraction,
the defect is in selection and the fix is in the scan.
