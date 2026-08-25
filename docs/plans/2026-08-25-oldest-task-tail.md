# `oldest_task_age` = 85.6 days: what the tail is, and what to do about it

Settles `tasks/01-NEXT-2026-08-25.md` item **P1-5** ("Nothing old is ever worked").
It was posed as a design decision, and it is one — but the premise it was posed
with is wrong in two places, and correcting them changes what the fix is.

**Decision: (a). The horizon is correct and the beyond-horizon tail is
intentionally abandoned. Say so in the code, and stop the gauge lying about it.
No scheduler change.**

---

## 1. What the 85.6-day task actually is — PROVEN, not inferred

`oldest_task_age_seconds` is `now − min(created_unix_ms)` over every
non-`Complete`/`Superseded` task (`maintenance_coordinator.rs:2663, :2714`).
It never names the task, which is why the brief had to guess. The stamp names it
by arithmetic:

| read at (UTC) | `oldest_task_age_seconds` | implied `created_unix_ms` |
|---|---|---|
| 2026-08-25 14:51:49 | 7,397,505 | 2026-06-01T00:00:04Z (4 s publish skew) |
| 2026-08-25 14:56:53 | 7,397,813 | **2026-06-01T00:00:00Z** (0 s) |

Two points five minutes apart, both landing on an exact UTC midnight. A record's
birthday lands at an arbitrary time of day; only one production path can stamp a
task with a midnight:

```rust
// src/database/maintain.rs:465
let sealed_at_ms = u64::try_from(slice.end_micros.max(0).div_euclid(1_000)).unwrap_or_default();
let created_unix_ms = if date == today { created_unix_ms } else { sealed_at_ms.min(created_unix_ms) };
```

A day-wide hygiene slice for date `D` ends at `D+1 00:00:00Z`. So:

> **The tail is a `SealedConsolidation` unit for partition date 2026-05-31.**
> Project and table are not determinable from the gauge — do not guess them.

Every other stamp in the tree is ~`now`: `plan_compaction_debt`'s base
(`maintain.rs:298`, which is also what `Repair` uses at `:503`), the rollup
backfill (`:1087`), and `invalidate` (`coordinator.rs:1935`, `observed_at`).
`coarsen_to_width` inherits its oldest member (`:1262, :1359`), but its members
come from those same ~`now` paths and hygiene is day-wide so it is never fused.
`enqueue_inner` never rewrites an existing `created_unix_ms`. By elimination,
`maintain.rs:466` is the only source of a 2026-06-01 stamp.

**This refutes the brief's "the 85-day tail is a ROLLUP task."** That was inferred
from `benefit` being 0 for rollup operations, not measured. Rollup tasks are
stamped with the wall clock at enqueue and cannot carry an 85-day stamp at all.

### It is not a zombie — it is live debt

The brief's option "it should have been removed" does not apply.
`plan_compaction_debt` re-proves this cell every 60 s: it re-lists the partition,
re-applies the `mergeable` test (the two smallest files must fit the target
together, `maintain.rs:435`), and its retire pass (`:513`) deletes hygiene tasks
for partitions it proves compliant. A cell that survives that loop has real,
mergeable file debt right now. It can succeed. It is never scheduled.

### The task record is probably NOT 85 days old

Because the stamp is the *data's* seal time, the record may have been minted 60 s
ago. `oldest_task_age` therefore **cannot distinguish** "queued 85 days and never
picked" from "re-minted every minute for 85 days" from "picked, ran, and the cell
is still non-compliant". Its wall-clock-exact advance — the brief's headline
evidence — is the arithmetic of a fixed calendar date, not evidence of
starvation. The starvation conclusion is still true; it is true from the code
below, not from the gauge.

---

## 2. What the code does to a task past the horizon — CONFIRMED, with one correction

`rank` (`coordinator.rs:1475`) minimises
`(class, hole>0, starved, hole, width, benefit, order)`.

- `class`: `is_frontier_task` excludes `SealedConsolidation` outright
  (`:2980` — `3465ecc` holds), so all sealed hygiene is class 1.
- `hole > 0` / `hole`: `hole_rank` returns 2 for anything that is not
  `BaseRollup`/`DerivedRollup` (`:1664`). Constant across hygiene; decides nothing.
- `starved` (`:2884`): `waited = now − slice.end`;
  `starved = !(3d..=31d).contains(&waited)`. **Smaller wins**, so `starved == 0`
  — *inside* the band — is the escalated state. A slice that ended more than 31
  days ago scores `1`, the same as work younger than 3 days.
- `order` (`:2925`): `starved == 0` ⇒ oldest-first; otherwise **newest-first**.

So a 2026-05-31 cell is ranked in the same group as yesterday's, and ordered
newest-first inside it. It is reachable only when the 3–31 day band is empty of
eligible `SealedConsolidation` work *and* its benefit bucket beats every fresher
out-of-band cell. `pending_sealed_consolidation` was 76 → 83 → 94 → 99 over the
day and `out_of_policy_cells` has been 51 for twelve censuses: the band is never
empty. Measure zero.

**Correction to the brief's second premise.** "`benefit` is 0 for every operation
except SealedConsolidation/HotPacking/Repair, so benefit ordering does not touch
this" is right about rollup and wrong about this task, which *is* a
`SealedConsolidation`. `benefit` ranks at index 5, ahead of recency — a 200-file
ancient cell does outrank a 3-file fresh one. It loses anyway because `starved`
is compared first at index 2, and band membership is absolute. The conclusion
survives; the reason is band membership, not benefit.

Prod confirms the band ordering directly (14:47–14:51Z, image `e67a149`,
four consecutive ticks):

```
maintenance_hygiene_debt_unclaimed operation=SealedConsolidation
  refusal="outranked_by:8100121c:2026-08-20:8100121c:2026-08-24:files=199"
```

The most-indebted unclaimed cell is `2026-08-24` with **199 files**, and the
winner is `2026-08-20` with fewer. Five days old (in band, `starved=0`) beats one
day old (out of band, `starved=1`) despite 199 files of benefit. The 85-day tail
sits in the *same* group as that losing 08-24 cell, with an additional 85 days of
newest-first ordering against it.

### The comment on `STARVATION_HORIZON_MICROS` is falsified

> "Outside the window a partition still gets served by ordinary newest-first
> ordering. It is deprioritised, not abandoned." — `coordinator.rs:2819`

Newest-first inside a class that ingest and the 60 s planner replenish
continuously is not service. In practice it is abandonment, and the gauge has
been saying so for at least three days while the comment said otherwise.

### One more finding: the backdating is vestigial for scheduling

`scheduling_class` computes `waited` from `slice.end_micros`, not from
`created_unix_ms` (`:2883`). The seal-time stamp was introduced so starvation
would survive restarts (`a_long_sealed_partition_is_aged_from_when_it_sealed_not_when_rescanned`,
`database/mod.rs:11733`); the scheduler has since moved to the slice clock and no
longer reads it. **Its only remaining consumer is `oldest_task_age`.** That is
what makes fixing the gauge safe — and it is also why "just stop backdating" is
wrong: the stamp would reset to record age on every 60 s replan, and the gauge
would read a permanently pleasant ~0 while the debt sat untouched.

---

## 3. The decision

**(a). The horizon is correct.** Its two documented incidents are real and both
are the same failure: plain oldest-first sent 10 of 10 historical starts to data
months old while the last 30 days went untouched (2026-08-17, 84 starts), and at
a 45-day bound prod spent escalation on 2026-07-17/19/20 — outside any window a
30 d panel reads. The horizon exists to protect the goal window, the goal window
is what queries read, and 2026-05-31 is not in it. Deprioritising it is right.

What is *not* right is that the abandonment is undeclared and that the gauge
meant to expose queue health has been pinned red by it for months. A
permanently-red gauge is worse than no gauge: it trains readers to skip the one
number that would show a real scheduling stall inside the goal window.

**So: declare the abandonment in the code, and split the gauge from it.**

1. Correct the `STARVATION_HORIZON_MICROS` comment: beyond the horizon work is
   abandoned in practice whenever fresher work of the same operation keeps
   arriving, which for hygiene is always.
2. `oldest_task_age_seconds` counts only work the scheduler still intends to do —
   tasks whose slice ended within `STARVATION_HORIZON_MICROS`. It is then bounded
   by 31 days by construction and becomes an actionable number: anything near the
   bound is a real stall inside the goal window.
3. Pair it with `beyond_horizon_tasks`, a count of the abandoned set. The
   abandonment becomes **sized and visible**, not hidden. Shipping (2) without
   (3) would be exactly this repo's favourite failure — a metric change that
   reads as good news.

### Rejected: (b) make the tail reachable

Rejected on cost/benefit and on precedent. The only shape that works is the
codebase's existing answer to "strict priority starves a class": a reserved share
of claims, like `sealed_turn` in `claim_next`. That knob has an incident
attached — raising the sealed share to three-in-four OOM-killed prod at 124.9 GB
RSS on 2026-08-17, because historical partitions are far larger than frontier
slices, and it was cut back to one-in-two. An "ancient turn" routes the *largest*
partitions in the fleet through the heavy path and reproduces exactly that risk,
to compact data no dashboard window reads. It also cannot be validated anywhere
but prod: `timefusion sim` does not model per-unit bytes on this path, and the
change is a throughput claim. Not worth a half-day prod experiment and an OOM
risk for a 2026-05-31 partition.

### Rejected here, kept as the better follow-up: stop *minting* beyond-horizon work

The systemically clean version of (a): have `plan_compaction_debt` decline to
enqueue hygiene for partitions past the horizon, and sweep the existing ones out.
Then every queue gauge becomes truthful at once rather than one gauge being
patched — and `pending_repair = 543` very likely carries the same dead weight
(`Repair` is planned for every sealed date with sorting columns, `maintain.rs:483`).
Rejected for *this* change only on size: it is a planner change plus a removal
sweep plus a re-admission rule for when a beyond-horizon partition is queried
again, and it deletes durable state. It should be the next step.

---

## 4. Done when

- **In tree:** `oldest_task_age_seconds` is defined over within-horizon work
  only, with the definition stated where it is computed;
  `beyond_horizon_tasks` exists and is exposed in `timefusion_stats`; the
  `STARVATION_HORIZON_MICROS` comment no longer claims beyond-horizon work is
  served. Pinned by a coordinator test: a 40-day-old slice must be excluded from
  the age gauge AND counted in `beyond_horizon_tasks`, while an in-band task
  drives the age.
- **On prod, after the next deploy, on a ≥1 h container:**
  `oldest_task_age_seconds ≤ 2,678,400` (31 d) **and**
  `beyond_horizon_tasks > 0`. Both, together — the second is what proves the
  first is a definition change and not a silent hide.
- **Not in scope, deliberately:** the 2026-05-31 partition stays fragmented. That
  is the decision, not an oversight.
