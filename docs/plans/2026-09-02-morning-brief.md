# Morning brief — night of 2026-09-02

Everything is pushed to master and green (suite **1323/1323**, e2e **61/61**).
Prod is healthy on my build. **Nothing risky was shipped.** Three decisions are
waiting for you. **They are one problem in three places:** every stuck lane is a
unit that does not fit a budget it must pass through, and three budgets are
calibrated below real unit sizes — admission 512 MiB, per-sort slice 510 MB,
repair 1,280 MiB.

The full evidence chain, including the wrong turns, is in
`2026-09-02-scale-readiness-10x-100x.md` and
`2026-09-02-stop-manufacturing-duplicates.md`.

---

## Decision 1 — Repair is serialized by a byte budget smaller than one bin

**197 repair units, bulk-enqueued 2026-08-16, are claimed ~3,409 times a day and
have never completed in 17 days.** Three hours of prod logs say why:

| event | count |
| --- | --- |
| **`repair_rewrite_permit_busy`** | **243** (≈81/hour) |
| `light_optimize_tail_selected` | **0** |

Repair rewrites are gated by a byte-priced semaphore whose budget is
`COORDINATOR_PER_SORT_BUDGET_BYTES` = **1,280 MiB**. Prod logs the request:

```
want_mib=1280   budget_mib=1280   event="repair_rewrite_permit_busy"
```

**Every observed unit asks for the entire budget** — its decoded size is at or
above 1.25 GiB, and the request is clamped to the budget. So repair is
**serialized**: one 40+ minute rewrite at a time while every other unit bounces.

The design intent was the opposite, and the code comment says so: *"Bins below
the budget now share it … a count of 1 … held repair to ~2/hour."* **In
production no bin is below the budget, so the intended sharing never happens**
and repair sits in the state that change was written to escape.

**The root of it: two gates price the same unit differently, and nothing
reconciles them.**

| gate | prices on | result |
| --- | --- | --- |
| admission (`maintain.rs:3298`) | `task.estimated_decoded_bytes` = **0.25 GiB** stored | **passes** |
| rewrite semaphore (`~6857`) | actual files = **≥1.25 GiB** | clamps to the whole budget → **bounces** |

`split_time_task` — the preflight that re-measures a unit and splits it — has
exactly two call sites and **both are dedup**. Repair never calls it, so the unit
is admitted ~5x underpriced, refused on the real price, and requeued *unchanged*
~17 times a day for 17 days.

**No value of the repair budget fixes this**, because the unit is priced one way
to get in and another way to run.

**RESOLVED — it is three constants that cannot all be true.** A repair unit is
exactly one file (`.take(1)`), so it cannot be split. Then:

| constant | value | where |
| --- | --- | --- |
| `COORDINATOR_HOT_TARGET_BYTES` | **256 MiB** compressed | `database/mod.rs:1378` |
| `DECODED_BYTES_PER_COMPRESSED` | **x12** | `maintain.rs:150` |
| repair budget | **1,280 MiB** decoded | `config.rs:222` |

```
one target-sized file = 256 MiB x 12 = 3,072 MiB  ->  2.4x the whole budget
```

**A file that compaction produced exactly as intended cannot fit the repair
budget.** Only files under 107 MiB compressed fit — i.e. ones compaction
considers too small. So repair is serialized **by construction**, and the "bins
below the budget share it" case the byte-pricing change was written for cannot
occur for any correctly-sized file. It was correct in intent and inert in
practice: the comment reasoned about a pathological 28 GB bin and missed that
the *ordinary* bin is also over budget.

- **Fix is derivable, not guessed:** to let `N` repair rewrites share, the budget
  must be ≥ `N x 256 MiB x 12` — **3,072 MiB for one**, 6,144 for two.
- **Judgement required:** only how much memory repair may hold, given admission
  (Decision 2) draws on the same pool.
- **Status:** not implemented. Needs no new measurement.

**Verified three independent ways:**

| direction | evidence |
| --- | --- |
| source constants | 256 MiB x 12 = 3,072 MiB vs a 1,280 MiB budget |
| prod logs | `want_mib=1280 budget_mib=1280`, 243 bounces in 3h |
| journal data | 312 units, median **256 MiB**, one creation stamp |

**Secondary (worth its own fix):** those stored estimates are **compressed**
bytes in a field named `estimated_decoded_bytes` — missing the x12. It is stale
data, not a live planner bug (today's planner applies the multiplier correctly),
but **admission decides on the stored number**: 256 MiB clears the 512 MiB
ceiling while the work it authorises costs 3,072 MiB. `clear_stale_estimates`
exists for exactly this class; this cohort appears to have escaped it.

**A data problem worth fixing on its own:** these tasks' stored
`estimated_decoded_bytes` median is **0.25 GiB** while runtime pricing of the
same work asks for **≥1.25 GiB** — a ~5x disagreement. Estimates were bulk-cleared
once before as "measured with a broken ruler"; admission and coarsening both
make decisions on that stored number.

## Decision 2 — Admission ceiling and the per-sort slice (must move together)

**Confirmed on live code**, not just a snapshot: re-fetching the journal 4.4
hours later, of 1,168 tasks stuck >48h, **1,144 (98%) were still queued**, only
24 drained, and 359 had attempts incremented.

Admission's ceiling is `MAX_DECODED_BYTES x (available/capacity)` clamped to
**[32 MiB, 512 MiB]**, against a **median unit of 316 MiB**:

| `MAX_DECODED_BYTES` | 100% free | 50% free | **25% free** | never-admissible |
| --- | --- | --- | --- | --- |
| **512 MiB (today)** | 86% | **33%** | **7%** | 14.1% |
| 2 GiB | 90% | 88% | **86%** | 10.5% |

At half occupancy only a third of the queue can be admitted. It is
*over*-guarded: admission capacity is 60 GiB, so today one unit may use **under
1%** of the pool it enters, and all 10 permits at maximum size use **8%** of it.

**But it cannot be raised alone.** Of the 338 sort-OOM failures (41 in the
*fresh* cohort, i.e. current behaviour), **every sized one is at or below today's
512 MiB ceiling, median 256 MiB** — a quarter-GiB unit already exhausts the
510 MB per-sort slice, because a sort's working set is a multiple of what it
decodes. Raising admission alone converts starved units into sort-failing units
on the OOM path.

- **Fix shape:** raise the admission ceiling **and** the per-sort slice together.
- **Risk:** two constants, OOM path, real interaction. Wants a canary.

---

## Decision 3 — Turn on `TIMEFUSION_LANDED_SKIP_ENABLED`?

Shipped, tested, **off**. It stops TimeFusion re-writing rows it already has:
58% of duplicate groups in a real prod bin are byte-identical rows our own WAL
replay re-inserted, and dedup is ~96% of maintenance time.

- **Failure direction:** a wrong answer produces a duplicate, never a lost row.
  The only path to a skip is a full 128-bit match against an identity a landed
  commit recorded.
- **Validated by:** an end-to-end test on real Delta + object storage that
  reproduces the duplicate the way prod makes it (a commit that lands while its
  cursor advance is lost). There is no staging service to prove it on.
- **Cost when on:** the digest is ~0.37x the parquet encode it avoids; in steady
  state (no dirty boot) it is a map lookup.

It interacts with Decision 2: with the flag off, any OOM-driven unclean restart
manufactures exactly these duplicates.

---

## Scale readiness

| load | result |
| --- | --- |
| **10x data per customer** | rollup backlog still drains, zero timeouts |
| 30x | drains, straining |
| 50x | needs ~2x the workers — a pool re-slice, not more memory |
| 100x | **more workers stop helping** — the timeout rate is a flat ~51% across an 8x worker range, because units do not fit their budget |

**More customers is a different axis from more data.** Maintenance arrival is
linear in *active* stream count and independent of data volume: 21 of our 124
streams are active, generating ~5,782 tasks/day. 10x customers means ~10x that,
regardless of how much each sends.

**The whale already dominates**: one project is 83.4% of queued bytes on 33% of
tasks, and 83.8% of all oversized units. We are running the 100x-customer
experiment in miniature today.

---

## What shipped

| area | change |
| --- | --- |
| duplicates | landed-batch skip (off by default) + `wal.landed_skips` counters |
| hashing | XXH3-128 for the digest (**17x** the hash vs blake3); XXH3 for in-process hashing incl. `query_fingerprint`, which ran SHA-256 on **every query** |
| hashing safety | six persisted hashes marked `FROZEN HASH` — a blanket sweep would have orphaned rollup history and reset certification coverage |
| observability | split `dedup_probe_timeouts_total` out of `dedup_bin_stage_timeouts_total` |

## Ruled out by measurement — do not re-litigate

- **Raise `MAX_DECODED_BYTES` for fusion**: buys **0.7%**. Its *admission* effect
  is separate and real (Decision 2); the sim cannot test admission — *"Memory
  admission … outside the model."*
- **Coarsen mint granularity**: real arrival is 6x lower than the sim modelled.
- **Restart cadence**: within noise.
- **The "climbing dedup timeouts" alarm**: those were the cheap probe. Zero real
  staging timeouts in an hour of logs.

## Method notes worth keeping

- **`synth:whale` models ZERO ingest arrivals** and flattered us; the real-journal
  default models **6x too many** and alarmed us. Validate any keep-up claim's
  arrival rate against `created_unix_ms` first.
- **`resource_admission` / `admission_busy` are never logged** — they are retry
  reasons stored on the task. Grepping logs for them proves nothing; diff the
  journal by task key instead.
- **`attempts` is reset, not cumulative.** Reading it as cumulative made a
  plentiful lane look starved.
- The journal is **not** on the host disk or in S3; it is inside the container at
  `/app/data/timefusion/.timefusion_meta/maintenance_tasks.json`.
