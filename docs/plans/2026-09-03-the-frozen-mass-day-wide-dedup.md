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

---

## RESOLVED — the mechanism is a starvation livelock past the 31-day horizon

The withdrawal above was itself wrong. A second journal pull, diffed against the
first on `attempts` (which increments AT CLAIM), settles it:

| dedup band | claims in ~50 min |
| --- | --- |
| **day-wide** | **0** |
| mid | 0 |
| ≤10 min | **35** (15 completed) |

All **96 newly minted** dedup keys in that window were ≤10 min. So "never
claimed" is confirmed twice, by two independent signals — and my code reading was
the thing that was wrong.

### What the code reading missed

`starved` is not a two-value flag. It is:

```rust
if waited < STARVATION_MICROS { u8::MAX }                                  // 255
else { (u8::MAX - 1).saturating_sub((waited - STARVATION_HORIZON).max(0) / DAY) }  // 254 and BELOW
```

Past the **31-day horizon** it keeps improving, one rank per extra day. So a unit
whose DATA is 43 days old scores 243 and permanently outranks everything at
254/255. Measured on the live journal:

- **148 of 761** sealed claimable dedup units have data older than 31 days, so
  `starved < 254`. They outrank **all 613 others**, forever.
- Their data age is 32–43 days; **76 of the 148 are the whale project**.
- **55 of the 148 are in `retry`** — `worker_error` (26), `resource_admission`
  (14), sort-OOM (9).

**The livelock:** the oldest-data units win every sealed turn; a large share of
them fail; they requeue; they are still the oldest; they win again. Nothing
younger — including the 5,132 GiB of day-wide work — ever gets a sealed turn.
This is the 31-day cliff from `tf_starvation_cliff_and_refuted_premises` seen
from the other side: the cliff does not merely protect old work, it **pins the
lane**.

A second, smaller distortion found on the way: `scheduling_width()` is
`backfill_priority_micros.unwrap_or(slice.width())`, so a **360-second** unit
carrying a day-sized `backfill_priority` presents as day-wide to the ranker. 54
of the 148 privileged units carry one. Width is therefore not a reliable
statement of unit size in the ordering.

### Confidence and its limits

The rank comparison was reproduced by transcribing `rank`/`scheduling_class` into
Python and running it over the real journal — a **diagnostic aid, not an
authority** (transcribing this logic is exactly how the sim drifted). It is
believed because it PREDICTS the observed behaviour: it picks a small
old-data unit as the sealed winner, and prod claims only small units.

### Do not fix by raising STARVATION_MICROS

Already refuted locally (9 test failures) per the code comment — it evicts the
query window from the privileged lane. The shape that fits this codebase is a
bounded SHARE, which is what both existing reservations (`sealed_turn`,
`window_turn`) already do. **Design it in the sim first; selection changes are
this repo's most outage-prone category.**

### The mechanism's own prediction, tested

The livelock predicts: sealed turns land on the >31-day privileged cohort, and
the 3–31 day band — where the entire day-wide mass lives — gets nothing. Bucketing
the 29 claimed units by DATA AGE:

| data age of claimed unit | claims |
| --- | --- |
| <3 d (fresh frontier) | 20 |
| **>31 d (privileged)** | **9** |
| **3–31 d (the 5,132 GiB)** | **0** |

All 29 were ≤10 min wide; zero day-wide. And of the 6 units claimed twice or
more, **two are 42-day-old privileged smalls** failing with `Resources exhausted`
and `worker_error` — the cyclers the mechanism requires.

**This unifies the two findings.** The 36.8% killed capacity and the frozen mass
are one defect: units that fail are not demoted, so the oldest-data cohort holds
the front of the sealed lane and burns capacity re-failing, while everything
between 3 and 31 days old — 96% of the queued bytes — is never reached.

Note the privileged cohort *contains* 85 day-wide units, yet all 9 privileged
claims were small: within the band, ranking is oldest-data-first, and the very
oldest (42–43 d) happen to be small. So even inside the privileged lane the
day-wide work waits behind cyclers.
