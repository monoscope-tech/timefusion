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
against known per-unit fixed costs, and the pre-claim split already bisects a
selected unit for free.

> ~~**Selection is the defect.**~~ **WITHDRAWN — see the update at the end of this
> document.** Reading the whole selection path afterwards showed every ordering
> rule *favours* these units, and a 36.8%-of-capacity kill rate is the better
> candidate. The line is kept because it shows how confidently a wrong diagnosis
> can follow from a correct measurement.

**Selection/ordering changes are the most outage-prone category in this repo's
history** (`tf_sealed_lane_pinned_by_metrics`, the superseded-vetoes incident), so
whatever comes out of this runs through the sim first and ships alone.

---

## Update — the ordering analysis REFUTES my own "selection excludes them"

I read the full selection path afterwards. It does **not** exclude day-wide
units; if anything it should PREFER them:

- `claimable` = right operation, `Pending|Retry`, `deadline <= now`, not
  quarantined. **561 day-wide units have `retry_reason = None`**, so they are not
  quarantined; 700 of 719 pass the deadline gate.
- `hole_rank` returns **2 for any non-rollup operation**, so dedup tasks have
  `hole = 2`. That matters: `rank` zeroes width only when `hole == 0`, so for
  dedup **width is preserved**, not discarded (I briefly believed the opposite).
- The sealed branch returns `(1, starved, -scheduling_width(), benefit, …)`.
  Width is **negated**, so a day-wide unit sorts BEFORE a ten-minute one.
- `starved` = 255 while `waited < STARVATION_MICROS` (3 d), else 254 (until the
  31-day horizon). The day-wide cohort waited ~18 days -> **254**; freshly minted
  smalls -> **255**. Smaller runs first, so day-wide **outranks** them here too.
- `eligible_watermark_lag_seconds = 0`, so `sealed_turn` is `claim_tick % 2`,
  i.e. a **50%** share, not 25%.

**So "never selected" is not established, and I withdraw it as a diagnosis.**
Every ordering rule I can find favours these units. What is measured and stands
is the OUTCOME: 2 distinct day-wide keys touched in a 3h20m journal window, and a
cohort that has not moved in 18 days. Mechanism: **still open.**

## The finding that IS solid, and is separately serious

`work.Dedup.killed_secs = 9,905` against `work.Dedup.worker_secs = 26,933` on a
3,835 s process:

> **36.8% of all dedup worker capacity produced nothing** — units claimed, run to
> their deadline, killed, requeued.

Dedup holds ~7.0 of the fleet's 16 slots on average (26,933/3,835), so this is
not a small lane wasting a little; it is the largest lane wasting over a third of
itself. `dedup_timed_out_total` reads 0, so **the kills are not being counted as
timeouts** — another zero-that-means-not-measured, like the audit denominator.

That waste is a plausible mechanism for the frozen mass without any selection
defect at all: if day-wide units are the ones being claimed and killed, they
would show as "touched rarely, never completing" exactly as observed — and
`retry` state is **73% day-wide**, which is consistent.

**Next measurement, and it is cheap:** correlate the killed units' identities.
If `killed_secs` is concentrated on day-wide units, the diagnosis collapses into
one sentence — *the mass is claimed, cannot finish inside the deadline, and is
requeued forever* — and the lever is unit cost and deadline, not selection.
