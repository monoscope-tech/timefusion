# 96% of the dedup queue's bytes are day-wide units that are never selected

**Measured 2026-09-03 ~15:20 UTC** from the live prod journal (checkpoint 11:52
replayed with the `.wal` to 15:12 — 78,910 records, 0 unparsed).

This is the 100x question in concrete numbers, and it connects to the read path:
these are the sealed days that never get deduped, which is why certification
declines on dirty bins, which keeps `DedupExec` in every plan over those days.

## The two populations

| dedup state | n | ≤10 min | day-wide |
| --- | --- | --- | --- |
| **complete** | 16,926 | **95%** | 2% |
| **pending** | 1,629 | 64% | 34% |
| **retry** | 215 | 25% | **73%** |

Bytes tell the real story:

| width bucket | units | GiB queued |
| --- | --- | --- |
| **≥12h (day-wide)** | **719** | **5,132.1** |
| ≤1h | 21 | 152.5 |
| ≤6h | 1 | 30.7 |
| ≤10 min | 1,103 | 19.1 |

**719 day-wide units hold 96% of 5,334 GiB.** The frontier stream (≤10 min) is
healthy — created and completed the same day (p50 created today). The sealed mass
is not: day-wide units date back to **2026-08-16**, p50 created 08-21. **18 days
without moving.**

The largest single unit is **1,150 GiB estimated decoded**, day-wide,
`attempts=0`, project `87576849` — the same whale whose hot-tail staging fails on
every process. 52 of the 96 over-`MAX_DECODED_BYTES` units are that one project.

## They are not being worked slowly — they are NEVER SELECTED

The `.wal` covers ~3h20m of coordinator activity, 78,910 task updates. In all of
it, **exactly 2 distinct day-wide dedup keys were touched, 4 records total.** The
1,150 GiB unit has **zero** wal records.

## What has been RULED OUT (checked, not assumed)

| candidate | verdict |
| --- | --- |
| deferred by `not_before_micros` | **No** — set on 0 of 719 |
| not yet eligible (`deadline_micros > now`) | **No** — **700 of 719 eligible NOW**, oldest deadline 08-17 |
| blocked by `dependencies_complete` | **No** — dedup's `required` is `None`; only DerivedRollup has one |
| out-ranked by recency | **No** — `scheduling_class` puts WIDTH above recency for sealed work, deliberately |
| refused by admission | **No** — the request is `clamp(1, MAX_DECODED_BYTES)`, so a huge estimate asks for 512 MiB like everyone else |
| too big to split | **No** — width p50 is a full day, none near the 60 s floor, and a pre-claim split (`estimated_bytes > MAX && width > MIN_SLICE`) bisects a *selected* unit cheaply |

So they are eligible, dependency-free, width-favoured, and splittable — and still
not chosen. **The defect is in selection, and the cause is not yet identified.**

Also ruled in as secondary, not the main story: 69 day-wide units carry
`source_not_flushed` and 29 `resource_admission`; 561 carry no retry reason at
all.

## Correction to an inference I made on the way

I reasoned "an estimate is present ⇒ the unit was preflighted at least once."
That is probably wrong: estimates are likely stamped at **mint** for sealed units
(from the snapshot walk) and absent for flush-minted frontier smalls — which
matches the 662-with / 1,182-without split almost exactly. **The zero-wal-records
trace is the real evidence of never-selected; the preflight inference is not.**

## Next step, and the guardrails

Reproduce the non-selection deterministically rather than inferring further from
the ordering code: load this checkpoint+wal into a `TaskJournal` locally and call
`claim_next(Dedup, ...)` in a loop, printing which comparison a target day-wide
key loses. Zero prod risk. The calibrated sim is the other route.

**Do not "fix" this by minting narrower slices.** That multiplies unit count
against known per-unit fixed costs, and sizing is not the defect — the pre-claim
split already bisects a selected unit for free. **Selection is the defect.**

**Selection/ordering changes are the most outage-prone category in this repo's
history** (`tf_sealed_lane_pinned_by_metrics`, the superseded-vetoes incident), so
whatever comes out of this runs through the sim first and ships alone.
