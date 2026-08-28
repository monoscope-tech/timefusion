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
