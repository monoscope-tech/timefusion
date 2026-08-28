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

## 6. Fleet-wide: 17.9% of first-attempt derived hours publish ZERO rows and complete

The section-5 correlation was one cell. Tested against every `complete`
hour-wide `derived_rollup` unit carrying a publication record in the same
journal — **1,895 units across 144 cells**:

| | n | zero-row publications | median rows |
|---|---|---|---|
| `attempts <= 1` | 1,546 | **277 (17.9%)** | 6 |
| `attempts > 1` | 349 | **2 (0.6%)** | 9 |

**A first attempt is ~30x more likely to publish nothing than a retry.** 277
hours in one journal were published empty and marked `complete`, so they are
never revisited. This is the fleet-wide form of hours 01/18/20 in section 5, and
it is the single largest identified contributor to the coarse tier being short.

### The partial-publication half does NOT generalize — do not carry it forward

Within the 92 cells holding both first-attempt and retried units, the retried
mean exceeded the first-attempt mean in **46 of 92 — exactly a coin flip.** So
`attempts` explains the *zero* publications and says nothing about the 11-47%
fractions in section 5. Those remain unexplained, and the section-5 table should
not be read as evidence that a retry fixes a partial hour.

### What to fix, in order

1. **A derived unit that publishes ZERO rows must not be marked `complete`
   unless its base is provably empty for that slice.** Coverage already records
   empty publications as covered, which is what makes an empty publication
   permanent. This is the one-line-shaped change with the largest measured
   effect, and it needs a failing test first: a derived unit over a non-empty
   base that publishes zero must retry, not complete.
2. **The hour migration must mint all 24 hours** (section 5: hours 21 and 22
   have no unit in any state).
3. **A base rebuild must invalidate its derived tier** (section 4.1).
4. Only then the v3 rebuild of the 29 cells.

## 7. THE INVARIANT VIOLATION — 735 slices published EMPTY over a non-empty source

Same journal, both tiers, every `complete` unit that recorded a publication:

| tier | complete + published | published **0 rows** | of those, `source_rows == 0` |
|---|---|---|---|
| `base_rollup` | 21,775 | **450 (2.1%)** | **0** — median source_rows **3,284** |
| `derived_rollup` | 2,309 | **285 (12.3%)** | **0** — median source_rows **1,174,646** |

**Not one of the 735 empty publications was over an empty source.** Every single
one had raw rows in its partition and published nothing, then marked itself
`complete` — so none will ever be revisited.

`base_tier_present` was `false` on **278 of the 279** empty hour-wide derived
publications. It is a *claim* gate (`dependencies_complete`), not a publish gate,
and the interval fallback admits units it cannot prove.

### This is the propagation path, and it explains everything above

A base slice publishes empty → `rollup_slice_coverage` records it, and **an empty
publication counts as covered** (stated outright at the derived guard:
"a genuinely empty base slice counts as covered and cannot deadlock this") → the
derived unit over that slice sees no hole, is admitted, reads nothing, publishes
empty, and completes. **Empty propagates up the tiers and freezes at every
level.** The `base_tier_incomplete` guard cannot catch it because coverage is
present; it is the *content* that is absent, and nothing compares content.

### The guard this justifies

**CORRECTION — `source_rows` is the wrong comparator.** It is keyed on
`(project, date)` while a unit is an HOUR, so a genuinely empty hour of a busy day
reads as non-empty and a guard built on it would fire on correct work. The claim
"by construction never fires on a genuinely empty source" was an overclaim.

The right comparator is **the base tier's own published rows over the SAME
slice**, which the journal already holds. Joined offline, it splits the 285 empty
derived completions cleanly:

| | count | share |
|---|---|---|
| base published rows in the overlapping slices, derived published 0 | **276** | **96.8%** |
| base also published 0 there — a legitimately empty hour | 9 | 3.2% |

(median base rows available on a violation: **14,660**; max 844,029)

So the guard is `rows == 0 && base rows in slice > 0`, at the publish site.
**DERIVED only for now**: the base tier's comparator would be the raw source, and
the only per-slice figure available there is the day-keyed `source_rows` — the
same overclaim. The base tier's 450 need their own comparator and are NOT covered.

Two things it must not become:
* **An infinite retry.** The code's own warning ("refusing forever is its own
  outage") applies. Bound it: retry while attempts are low, then complete and
  COUNT it, so a persistent cause is visible instead of silent.
* **A fix without a cause.** This guard stops empties becoming permanent; it does
  not explain why a unit with 1.17M source rows reads zero. That cause is still
  open, and section 5's fractional hours (11-47%) are probably the same cause
  seen at partial strength.

### Shipped: the counter, not the guard

`rollup_published_empty_over_full_base` — incremented at the publish site when a
DERIVED unit publishes zero rows and
`TaskJournal::published_rows_overlapping(project, base_tier, slice)` is non-zero,
with a matching `maintenance_rollup_published_empty_over_full_base` warn carrying
the slice.

Counter first, on the `rollup_skipped_covered_by_wider` precedent (#145): that
branch also shipped as a counter because its failure could not be reproduced in a
test, and became behaviour only once the counter moved on prod. The comparator
itself IS pinned — `published_rows_are_summed_per_slice_and_per_project` covers
the empty-hour case that would make a `source_rows`-based guard wrong.

**Read it against the offline number: 276 in one journal.** If it moves at a
comparable rate, the bounded-retry guard is justified and the counter's firing
conditions will name the mechanism the offline join cannot.

## 8. The latency this costs: 7d is 11.7 s WARM and 30d does not complete

Measured against prod `6693295` (2026-08-28), project `28f62f01`, same shapes
monoscope's dashboards issue. Run twice to separate cold cache from structure:

| shape | cold | **warm** |
|---|---|---|
| 1h count | 558 ms | 363 ms |
| 24h count | 3,252 ms | **283 ms** — cache-bound, fine once warm |
| 7d hourly chart | 19,978 ms | **11,698 ms** |
| 30d daily chart | 60,271 ms | **times out at the 60 s statement limit** |

24h collapses on a warm cache. **7d and 30d do not** — 11.7 s warm and a
timeout are structural, not cold storage.

**Those are exactly the windows the 1h tier exists to serve**, and they are the
windows this document has just shown the 1h tier cannot answer: 276 derived
slices published empty over a base holding rows, hours 21-22 never enqueued, and
`measures_available` dropping the measure-poor generation's slices to the raw
fringe. A 30d chart that cannot route reads raw across 30 days and hits the
statement timeout.

**The shape was verified routable — that control is required and it passed.**
Reading `rollup_miss_*`/`rollup_hits_*` either side of one 7d run:
`rollup_hits_hybrid_total` **4 → 5**. So the tier DID answer part of the window
and the query still took 11.7 s warm, which is what an incomplete tier plus wide
raw fringes costs. Had the shape merely declined, the timing would have been
evidence about raw-scan cost and nothing about the tier.

(The same run also moved `rollup_miss_unaligned_bucket_total` +9 and
`rollup_miss_filter_not_eligible_total` +2 — one query probes many candidate
specs, so a miss counter moving does not mean the query missed.)

**The 30d attribution is NOT controlled and must not be stated as one.** That
query timed out, so no hit/miss can be attributed to it, and it should not be
re-run as a probe: a 60 s raw scan on a memory-tight instance is the shape that
OOMs prod. What is supported: *the 7d window routes, is served partly by the
tier, and is still 11.7 s.* Fixing the empty publications should move that
number, and it is the one to re-measure — not the 30d timeout.

Caveat on method: prod had been up ~10 minutes, so the cold column is a genuine
cold-cache reading and the warm column is one repeat, not a distribution.


## 9. Side observation — a maintenance-worker panic during drain

`thread 'maintenance-worker' panicked at bytes-1.11.1/src/bytes.rs:392: range end
out of bounds: 1367439 <= 1196032` (and `4899040 <= 81920`), surfacing as
`immutable-column audit failed error=Join Error` in `database::compact`.

Six occurrences in 90 minutes, **all on the one draining task**, clustered at the
shutdown instant — a truncated object-store read being sliced at its intended
length while the process tears down. It is caught (the audit logs and continues)
and does not crash the process, and `immutable_column_disagreement_total` stood at
118. Filed as an observation, not a defect: nothing was seen outside a drain
window. If it is ever seen on a steady-state process, that changes.

## 10. The empty publications are an HOUR-WIDE FIRST-ATTEMPT defect — day-wide units never do it

Same join (derived unit published 0 rows while its base published >0 over the
overlapping slices), split by unit shape. 276 violations against 2,033 healthy
publications:

| slice width | violations / total | rate |
|---|---|---|
| **60 min** | **274 / 1,895** | **14.5%** |
| 1,440 min (day-wide) | **0 / 398** | **0.0%** |

| attempts | violations / total | rate |
|---|---|---|
| **1** | **274 / 1,723** | **15.9%** |
| 2 | 2 / 343 | 0.6% |
| 3, 4, 5 | 0 / 231 | **0.0%** |

**Not one day-wide derived unit has ever published empty over a non-empty base,
across 398 of them. Not one unit on its third attempt or later, across 231.**

By table: `otel_metrics_rollup_metrics_1h_v2` 15.5%, `dashboard_1h_v2` 8.0%. By
project: 5.4% to 21.6%, every project affected — so it is not tenant-specific.
By hour of day: spread across 01-22 at 4-35%, with hours **00 (1.9%) and 23
(0.0%)** the near-clean ones. No clean time-of-day structure; width and attempts
are the whole signal.

### What this makes the question

Not "why does a unit with 1.17M source rows read zero" but: **what does an
hour-wide unit on its first attempt do that a day-wide unit never does, and that
a retry of the same hour fixes?**

The leading candidate is base-file SELECTION, which is width-sensitive in exactly
this way. A derived unit selects base files by tag OVERLAP against its own slice
(`src/database/maintain.rs`, the `derived` arm of the selection loop). A day-wide
unit overlaps every base file for the day and cannot miss one. An hour-wide unit
only takes files whose tagged range touches its hour — and a base file whose tag
describes a different, narrower slice than the rows it physically holds (which is
what compaction and re-publication at varying widths produce) is invisible to it.
That also explains the retry: by the second attempt the base has been
re-published at a width whose tag does overlap.

**Note the drop is silent.** The selection loop counts `untagged_inputs` when a
file has no tags, but a file that IS tagged and fails the overlap or project test
is skipped with no counter at all — `rollup_untagged_inputs` reads 0 in prod
while this happens. Instrumenting that skip is the cheapest next step and is
strictly cheaper than reproducing the race.

## 11. FIXED — the hour migration collapsed a DAY into hour 00 and dropped the other 23

The section-5 observation "hours 21 and 22 have no derived unit in any state" has
a cause, and it is a one-character-class bug in `migrate_derived_slices`.

The guard was `task.key.slice.width() != DERIVED_SLICE_MICROS`. That matches
slices **wider** than an hour as well as the ten-minute fragments the migration
was written for — and the replacement key is the single hour containing the slice
**start**:

```rust
let start = task.key.slice.start_micros.div_euclid(DERIVED_SLICE_MICROS) * DERIVED_SLICE_MICROS;
key.slice = TimeSlice { start_micros: start, end_micros: start + DERIVED_SLICE_MICROS };
```

So a day-wide derived unit was marked `Superseded` and re-enqueued as **hour 00
alone**. Its other 23 hours were never planned by anything.

Measured on the captured journal, by the width of every task superseded with
`migrated_to_aligned_hour_slice`:

| width | count | |
|---|---|---|
| 1-45 min | 2,525 | the intended case — several fragments collapse into their hour |
| 90 / 180 / 360 / 720 min | 17 | collapsed |
| **1,440 min (a full day)** | **248** | collapsed to hour 00 |

**265 collapses, roughly 5,799 hours of derived work silently dropped.**

**Fix: `width() < DERIVED_SLICE_MICROS`** — migrate only what is narrower than an
hour. A wider unit is left alone rather than expanded into 24 hour tasks, for two
reasons: expanding mints 24x the journal entries this migration's own comment
warns about (the 4,632-record incident), and section 10 measured day-wide derived
units as the **healthy** shape — 0 of 398 published empty against 14.5% for
hour-wide.

Pinned by `the_hour_migration_leaves_a_slice_wider_than_an_hour_alone`, witnessed
failing first (migrated 2 where 1 is right).

Note this is NOT what `9f679039` fixed: that one excluded split children via
`parent_measured_bytes`, a different way into the same function.
