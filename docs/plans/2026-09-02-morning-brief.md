# Morning brief — night of 2026-09-02

Everything is pushed to master and green. Prod is healthy. **Nothing risky was
shipped** — but read the incident note first.

> **⚠ I published another session's work by accident.** A concurrent session
> shared this checkout on branch `tf-monoscope-compat`. Several of my commits
> used `git add -A`, which stages *their* files too, so four of my "docs:"
> commits carried their work to master — including **`src/rollup.rs`** — and
> **that triggered a prod deploy** (prod runs `e07421d`). Verified after the
> fact: master builds clean, their `monoscope_query_shapes` test passes, prod is
> serving normally (23,465 queries, dedup ~37/hour), and the **full suite is
> 1330/1330 green on current master**, including their added tests. No damage,
> but that is luck rather than process, and it means my "no deploys since X" claims are wrong for
> those windows. I did not revert — their work is intentional and may be built
> upon. Detail in the scale-readiness doc. Three decisions are
waiting for you. **They are one problem in three places:** every stuck lane is a
unit that does not fit a budget it must pass through, and three budgets are
calibrated below real unit sizes — admission 512 MiB, per-sort slice 510 MB,
repair 1,280 MiB.

The full evidence chain, including the wrong turns, is in
`2026-09-02-scale-readiness-10x-100x.md` and
`2026-09-02-stop-manufacturing-duplicates.md`.

---

## Decision 0 — Ship the instrument first (branch `maintenance-pool-stats`)

`timefusion_stats` reports query-pool **usage** and the maintenance/coordinator
pool **sizes** — but **not their usage**. Every decision below moves a
maintenance memory limit, and none can currently be verified against the pool it
moves: after shipping one, there is no way to see whether maintenance ended up
closer to or further from its ceiling.

Branch **`maintenance-pool-stats`** adds `maintenance_pool_used_bytes/_pct` and
`coordinator_pool_used_bytes/_pct` — two closures over existing `RuntimeEnv`
memory pools plus four stats rows, read-only, no behaviour change. Lint clean,
tests pass.

It also settles the memory-source question below with numbers instead of an
argument: if maintenance runs near its ceiling while query reads 0%, the
rebalance makes its own case.

*Ship this before the three decisions.* "Ship the instrument before the fix" is
what made `dedup_plan_shape` answer its question on the first prod unit and made
`wal.replay_rows` price a restart.

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
- **⚠ Do NOT do it by raising `COORDINATOR_PER_SORT_BUDGET_BYTES`.**
  `repair_rewrite_budget_bytes()` returns that shared constant, and it is also
  the **divisor for `light_optimize_k`** — raising it to 3 GiB cuts hot-tail
  packing concurrency by **2.4x**. The code records that exact outage five days
  ago in the opposite direction: K went 3 → 1 and *"HotPacking stopped being
  claimed at all — zero units in 45 minutes with 17 pending."* **Give repair its
  own constant**; the function is already separate and merely returns the shared
  value. Re-check `light_optimize_k` afterwards, since its assertion encodes the
  one-budget holdback for repair.
- **Judgement required:** only how much memory repair may hold, given admission
  (Decision 2) draws on the same pool.
- **Status:** not implemented, but **there is a reproducing test on branch
  `repair-budget-repro`** (`config::tests::repair_budget_must_fit_one_target_sized_file`).
  It fails today with the whole diagnosis in its message:

  ```
  repair budget 1280 MiB cannot hold ONE target-sized file
  (3072 MiB decoded = 256 MiB x 12), so every repair unit clamps to
  the whole semaphore and repair serializes
  ```

  Branch, not master — master deploys to prod. Raising the budget is a memory
  decision that interacts with admission (same pool), so the test makes the
  defect executable without pre-empting the judgement.

**Prior art says we are the outlier.** For "a unit does not fit its budget",
Cassandra **drops the largest SSTables from the input list** until it fits;
ClickHouse **caps merge size at selection and scales the cap down as the pool
fills**. Both adapt the work to the budget, so neither has a state where a unit
is permanently unrunnable. We refuse and requeue unchanged — which is the 310
stuck units. Raising the budget fixes today's numbers but leaves the structural
property intact, and the whale's largest file is already 1,150 GiB. **But shrinking is not available here either**, which simplifies rather than
complicates: `TARGET_ROW_GROUP_BYTES` is 128 MiB, so **one row group decodes to
1,536 MiB — already 20% over the 1,280 MiB budget**, and a target-sized file
holds only 2 of them. The indivisible read unit is over budget, so there is no
smaller unit to fall back to. **The budget must be raised; shrinking only has a
move to make above 1,536 MiB.** Minimums: 1,536 MiB for one row group, 3,072 MiB
for one whole file.

**Confirmed against LIVE state**, not the (hours-stale) checkpoint: replaying
both journal files gives **310 repair tasks in Retry right now**, against 311
survivors in the checkpoint — not draining, not a snapshot artifact. The
actionable queue across all ops is **2,191**, not the ~21,000 the sim's
`pending` reports (the rest is superseded bookkeeping). Repair is *not*
universally broken — 2,188 have completed — which fits the arithmetic exactly:
files under ~107 MiB compressed fit the budget and finish; target-sized 256 MiB
files never can.

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

**Dedup — the lane that is ~96% of maintenance time — is squeezed from BOTH
sides.** Of its 1,540 queued tasks (622 priced): median size **512 MiB, exactly
the ceiling**, and **58.7% exceed it and can never be admitted at any
occupancy**. Meanwhile the largest failure among tasks that *do* enter is
`Not enough memory to continue external sort` — **245 tasks**. The
all-operations figure (14.1% never-admissible) badly understated this for the
lane that matters most.

**But it cannot be raised alone.** Of the 338 sort-OOM failures (41 in the
*fresh* cohort, i.e. current behaviour), **every sized one is at or below today's
512 MiB ceiling, median 256 MiB** — a quarter-GiB unit already exhausts the
510 MB per-sort slice, because a sort's working set is a multiple of what it
decodes. Raising admission alone converts starved units into sort-failing units
on the OOM path.

- **Fix shape:** raise the admission ceiling **and** the per-sort slice together.
  Raising admission alone moves dedup tasks from "cannot enter" to "fails its
  sort"; raising the slice alone leaves 58.7% unable to enter. **One change, not
  two.**
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

## The one-line answer to "are we breaking a sweat?"

> **No, on flow.** Dedup (~50 commits/hour), hot-tail packing and rollups all
> keep pace with arrivals — `pending_dedup` is flat at ~1,434 over two hours,
> with **zero failures and zero staging timeouts**.
>
> **Yes, on a bounded permanently-stuck set.** ~900–1,400 dedup units that can
> never be admitted, plus 310 repair units that cannot fit the rewrite budget.
> This set does not grow with load — it is a fixed toll — but it never runs, so
> the data it should maintain goes unmaintained indefinitely.

**Why that reframes 10x:** the risk is *not* falling behind. Throughput scales
(the sim drains a 10x-costlier backlog fine). The risk is that **the unrunnable
fraction grows with tenant size** — every constant in the three decisions below
is a fixed byte budget.

**Verified, not assumed** (whale vs everyone else, from the journal):

| operation | whale median | others median | ratio |
| --- | --- | --- | --- |
| dedup | 0.50 G | 0.25 G | **2.0x** |
| repair | 0.25 G | 0.01 G | **23.8x** |
| base_rollup | 0.37 G | 0.29 G | 1.26x |

Larger tenant ⇒ larger units in *every* lane, at the median as well as the tail
(the whale's biggest dedup unit is **295x** anyone else's). Small tenants never
reach the 256 MiB compaction target so their files stay comfortably inside every
budget; the whale's files sit *at* the target — exactly the size that does not
fit. **Adding capacity cannot help: the exclusion is a comparison against a
constant, not a worker shortage.**

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

## Where the memory comes from (open, cheap to close)

Decisions 1 and 2 both raise a memory budget, and `budget.slack_mb` is **0** —
nothing is spare. But `query_pool` is **16,384 MB, 27% of the committed budget,
and read 0% across five samples** while pgwire served ~5.3 queries/sec
(`plan_cache.hit_pct` is 100.0%, which plausibly explains the low pressure).

These are reservation ceilings, not allocations, so 0% is not wasted RSS — but
the partition is **static**, so that 16 GB is unavailable to maintenance
regardless of query load.

**Measured, 58 samples over ~20 min** (the five-sample reading was wrong — those
landed in idle gaps): the pool is non-zero **12%** of the time and **peaks at
1.32 GB = 8.3%** of its 16 GB ceiling.

**So the rebalance is on, and sized:** leaving ~4 GB (3x the observed peak)
frees roughly **12 GB** — far more than the repair budget needs (**+1,792 MB**)
and enough for the admission/per-sort-slice pair, without touching the cgroup.

Caveat: one hour, one workload, and the peak under a *heavy report* is
unmeasured — DataFusion reserves for sorts/joins/aggregates, exactly what such a
query uses. Reclaim conservatively and watch, which is what Decision 0's
instrument makes possible.

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
- **Compute elapsed time from `boot_micros`; never eyeball it.** I published a
  "dedup rate is decaying 3x" reading built on an estimated timestamp. Measured,
  the rate went UP (37.6 → 49.5/hour) and pending is flat. Retracted — every
  rate needs its denominator measured.
- **"pending" means three different things.** `timefusion_stats`'
  `pending_dedup`, the sim's `pending_start`, and journal `state == Pending` are
  three different populations — the sim's includes SUPERSEDED, which is ~89% of
  it. The actionable queue is ~2,000, not ~21,000. Say which one you mean.
- The journal is **not** on the host disk or in S3; it is inside the container at
  `/app/data/timefusion/.timefusion_meta/`.
- **That JSON is a periodic CHECKPOINT, not live state** — live state is the
  27.7 MB `maintenance_tasks.wal` beside it. The checkpoint can lag by hours (it
  was ~2h stale when checked). Cohort *diffs* between two checkpoints are valid;
  "right now N tasks are queued" is not. `load_sandboxed` copies both files, so a
  faithful replay needs the `.wal` too.
