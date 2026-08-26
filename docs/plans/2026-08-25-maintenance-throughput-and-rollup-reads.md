# Maintenance throughput & rollup reads — plan of record

**Date:** 2026-08-25 · **Basis:** prod `d3b44f7`, 67-min-old process, a matched
12.15-min `timefusion_stats` window, and the maintenance journal (`docker cp`,
checkpoint 06:59, ~4h stale — used for *shape*, never for rates).

> **⚠️ RE-BASELINED LATER THE SAME DAY.** Prod is now **`e67a149`** (= repo HEAD)
> on a **4.4h-quiet** process. Several headline numbers in the table below came
> from a 67-min-old process and are distorted by the known
> process-scoped-coverage artifact. Corrected readings on the mature process:
>
> | Signal | 67-min read | **mature read** |
> |---|---|---|
> | `rollup_hits_hybrid_total` | 0 | **17** — routing works |
> | `rollup_misses_total` | 55 | 414 (top reason `unwalkable_source` 126, a counter that did not exist at 67 min) |
> | `sealed_compaction_debt_bytes` | 509 GB | **216 GB** — compaction crushing it |
> | `tasks_complete` | 123/hr | **~243/hr** |
> | `tasks_pending` | 6,942 | 6,863 — slightly **down** |
>
> **"Rollups build fine and are never read" below is WRONG** and is superseded by
> P1/P1c. Read the per-section verdicts, not the summary table.

### Verdict index — read this before actioning any section

| § | Status |
|---|---|
| P0a dedup ladder | ✅ root cause confirmed, **implemented**, test written |
| P0b resume | ✅ diagnosed — **two** distinct bugs, one destructive |
| P0c deploy discipline | ▶ in force (4.4h natural quiet) |
| P1 / P1c rollup reads | ✅ fixed upstream; `unwalkable_source` defined; generation hypothesis **refuted live** |
| P1b percentile NULL | 🚨 open, most damaging |
| P2 the 87-day tail | ✅ **root cause found** — a 31-day `starved` cliff |
| P2b dirty-bin | ⏸ still gated on P0a |
| P3 supersede churn | ❌ **premise refuted** — one closed incident |
| P3b witness | ❌ premise refuted, but defect **real and promoted to #1 coverage blocker** |

## The one-line state

Compaction is healthy and draining. Rollups **build** fine and are **never read**.
Dedup is looping on a confirmed bug. The queue treads water at **+52/hr net** over
4.2h, yesterday (08-24) never drained (3,178 units), and a **3,084-unit tail back
to 2026-05-30 never retires**.

Measured throughput, 12.15-min window:

| Signal | Rate | Verdict |
|---|---|---|
| `processed_bytes_total` | **161 GB/hr** rewritten | healthy |
| `sealed_compaction_debt_bytes` | 520 → 509 GB (**57 GB/hr net**) | draining, ~9h to clear |
| `rollup_rebuilds_{full,incremental}` | **109/hr** | building fine |
| `light_optimize_waves_committed` | 25/hr | fine |
| `tasks_complete` | **123/hr** | vs 6,942 pending |
| `tasks_pending` | +216 (burst); **+52/hr over 4.2h** | treading water |
| `dedup_bins_committed_total` | ~5/hr | **blocked (P0a)** |
| `rollup_hits_{full,hybrid}` | **0**, vs 55 misses | **read path dead (P1)** |

**Measurement precondition for everything below:** 5 deploys in 2h against
~21-min units. No number here is steady-state. See P0c.

---

## P0a — Dedup rewrite has no sort-partition ladder ✅ root cause confirmed

**Symptom.** 56 units fleet-wide (17 Shipbubble) loop on
`Not enough memory to continue external sort`, attempts up to 15, pool
`fair(pool_size: 5.0 GB)`. `ExternalSorterMerge[0]` 475.1 MB and `[1]` 453.7 MB,
both **`can spill: false`**.

**Root cause.** `src/database/compact.rs:1395` builds the dedup rewrite session as:

```rust
build_optimize_session_state(
    self.config.memory.timefusion_query_partitions,  // WIDE
    self.maintenance_runtime_env(),                  // the 5.0 GB heavy pool
)
```

No `UncappedSort` (so no per-partition reservation cap) and **no degradation
ladder** — a bin that exhausts the pool retries at the *same* width, forever.

The pool figure is arithmetically exact, not inferred:
`maintenance_pool 16,964 MB − coordinator ¼ (4,241) = 12,723 × HEAVY_MIN_SHARE 0.40`
= **5,089 MB = 5.0 GB**, matching the error string.

Repair already solves this — `maintain.rs:5471` selects
`REPAIR_SORT_PARTITION_LADDER = [16, 4, 1]` by `repair_degradation` level, and the
comment at `maintain.rs:5464` states the principle outright: *"the unspillable
merge exec is per-partition, so this is the one dial that changes whether the same
file can fit."* Dedup simply never got the dial.

**Correction to the arithmetic above.** The first arg to
`build_optimize_session_state` is capped: `maintenance_session_config`
(`mod.rs:1092`) does `target_partitions.min(MAINTENANCE_MAX_PARTITIONS)` and
**`MAINTENANCE_MAX_PARTITIONS = 2`** (`mod.rs:971`). So dedup was never "WIDE" — it
ran at **2**, which is exactly the two `ExternalSorterMerge[0]`/`[1]` streams in the
error. The ladder is therefore `[2, 1]`, a single rung. It is still the right fix:
at **1 partition there is no merge exec at all**, so the sort spills within its fair
share — removing ~929 MB of unspillable reservation against the 582 MB the pool
refused.

Note `UncappedSort` is the wrong tool here — it *lifts* the cap. Dedup needs to go
**below** it, which the plain `target_partitions` argument already does.

### ✅ IMPLEMENTED 2026-08-25 (uncommitted — see "Concurrency hazard" below)

- `DEDUP_SORT_PARTITION_LADDER: [usize; 2] = [MAINTENANCE_MAX_PARTITIONS, 1]` +
  `dedup_sort_partitions(attempts)` — `mod.rs`, beside repair's ladder.
- `DedupExecutionLimits.sort_partitions` — new field (`mod.rs`).
- `maintain.rs` (coordinator dedup): `sort_partitions: dedup_sort_partitions(task.attempts)`.
- `compact.rs:1395`: `limits.map_or(configured, |l| l.sort_partitions)`.
- Regression test `a_retried_dedup_rewrite_narrows_to_a_single_partition`
  (`mod.rs`), mirroring the repair ladder tests.

### ⚠️ Near-miss caught in review: `attempts` is POST-CLAIM

The first cut indexed the ladder with `task.attempts` directly, on the assumption
that a first run has `attempts == 0`. **It has `attempts == 1`:** `claim_next`
calls `mark_running` — which does `attempts += 1` (`coordinator.rs:2035`) — and only
*then* clones the task it returns (`:2180`). Shipping that would have put **every
first-ever dedup on the single-partition floor**, halving the whole fleet's sort
parallelism to fix 56 units. Corrected to `attempts.saturating_sub(1)`.

**The unit test passed both before and after the bug** — it exercises
`dedup_sort_partitions` in isolation, so it can pin the ladder's *shape* but never
the *wiring's* off-by-one. The test now asserts the `attempts == 1` boundary
explicitly and says why. Generalising: a pure-function test of a value that is
derived from mutable shared state does not test the derivation.

**Design choice — level from `attempts`, not a `repair_degradation`-style map.**
Repair's degradation map is an in-process `DashMap`, lost on restart. Prod replaces
the process every 15–28 min, so a map would leave the units already looping at
attempts 8–15 retrying at the width that just failed — the exact bug. `attempts` is
**persisted in the journal**, so the ladder survives restarts and the looping units
narrow on their very next claim. Narrowing on *any* retry (not just an exhaustion)
is deliberate: the failure reason is not plumbed to this call site, a narrower sort
costs only parallelism, and a unit that succeeded has `attempts == 0`.

**Moves:** `dedup_bins_committed_total`, `dedup_waves_committed_total`,
`tasks_retry` (373 → expect ~317), `tasks_complete`/hr.
**Verify:** the unit test proves the ladder. Do **not** use `timefusion run-unit`
against prod to verify — it commits a real rewrite, and prod is read-only while
unattended. Staging is the optional second rung; prod last.

---

## P0b — Resume never fires: 0 resumed AND 0 declined across 5 restarts 🔍 needs diagnosis

`repair_resumed_total = 0`, `rollup_resumed_total = 0`, and **every**
`repair_resume_declined_*` counter is also 0, across 5 restarts in 2h.

**Correction to CLAUDE.md:** it documents
`TIMEFUSION_REPAIR_RESUME_ENABLED` as *"default false"*. The code is
`#[serde_inline_default(true)]` (`config.rs:2029`). Prod does not set the var, so
resume is **on**. This is a docs bug — fix the CLAUDE.md line as part of this work.

### ✅ DIAGNOSED (2026-08-25) — TWO different failures, one per half

**Repair half — genuinely never reached. Dead code in prod.**
`resumable_staged_bin` (`maintain.rs:6438`) has exactly two callers
(`maintain.rs:5204`, `:5343`), both inside `optimize_table_light_until`, whose only
prod entry is `run_hot_compact_sweep` — and its three call sites (`mod.rs:4472`,
`:4510`, `:4536`) all sit inside `if !COORDINATOR_OWNS_SLICE_MAINTENANCE`. **That
const is `true`** (`mod.rs:1006`). The live path is
`run_coordinator_compaction_once` → `stage_hot_bin` (`maintain.rs:2439`) with **no
resume consult**. A regression from the coordinator migration, not by design — the
comment at `maintain.rs:5192` records 6 real `staged_intent_resumed` on the old path.

**It is worse than inert: it is destructive.** `stage_hot_bin` still *records*
intents (`maintain.rs:5780`), and the only remaining reader is boot-time
`reconcile_staged_intents`, which **deletes** the staged parquet (`maintain.rs:6545`,
`clear_staged_intent` `:6594`). So every crash-orphaned 40-min rewrite is written,
orphaned, then thrown away.

**Fix (~6 lines):** in `run_coordinator_compaction_once`, before `stage_hot_bin`
(`maintain.rs:2438`), call `resumable_staged_bin(...)` and on `Some(bin)` commit it
through the `commit_wave` already used at `:2443` instead of re-staging.

**⚠️ IMPLEMENTED BUT GATED — `b376eda`, branch `p0a-dedup-sort-ladder`.** Consults
`resumable_staged_bin` before staging; on a landed commit marks the task complete
and returns; a resume that loses its race falls through to normal staging. This
closes the *destructive* half — prod no longer writes intents that nothing reads.

**It will almost never fire as written.** `classify_resume` (`mod.rs:6883`) returns
`Skip` for any intent younger than `STAGED_INTENT_MIN_AGE_SECS = 30 min`, and
`requeue_running` sets `deadline = now`, so a restarted unit is re-claimed within
*minutes* while its intent is still young. **It is the same pincer diagnosed for the
rollup half** — I wired the call site and inherited the gate. Caught in review, not
by the compiler or a test.

**Do not shorten the timeout.** The gate guards a real hazard, stated at
`mod.rs:6877`: on an overlapping rolling deploy a still-running instance may be
mid-staging, and committing its half-written output is exactly the hazard. Wall-clock
age is a *proxy* for "is another instance live"; **process identity answers it
exactly**. That is P0b's fix (b) and is now the unblocker for this item — deliberately
not attempted unattended, since it changes the `StagedIntent` format and turns on a
concurrency invariant.

**Prereq CLOSED:** `TIMEFUSION_DATA_DIR` *is* persistent — `docker inspect` shows the
host bind `/home/ubuntu/timefusion-data → /app/data/timefusion`, which survives
container replacement. So `staged_intent.jsonl` outlives a restart and the resume
can actually find something. The earlier "unverified" caveat is resolved.

**Rollup half — my premise was WRONG. It IS reached; every exit is UNCOUNTED.**
`resume_rollup_unit` runs on every rollup claim (`maintain.rs:1744`). So
`0 resumed && 0 declined` does **not** prove "never reached" here. The silent exits:
- `candidates.is_empty() → Ok(false)` (`:6339`) — uncounted, almost certainly
  dominant. The intent exists on disk only between `record_staged_intent` (`:2128`)
  and `clear_staged_intent` (`:2241`) — **seconds**, at the very end of a ~21-min
  unit. A restart almost never lands in that window.
  > **Consequence:** resume by construction can only save the **commit**, never the
  > scan — directly contradicting its own doc comment at `:6299-6306`.
- `ResumeVerdict::Skip` when the intent is younger than
  `STAGED_INTENT_MIN_AGE_SECS = 30 min` (`mod.rs:6801`, applied `:6875`/`:6912`) —
  silent `continue` at `:6386`. **This is the pincer:** `requeue_running`
  (`coordinator.rs:2501`) preserves the `TaskKey` but sets `deadline_micros = now`,
  so the unit is re-claimed *within minutes* of boot while its intent is <30 min old
  → always Skip → it rebuilds all 21 minutes and orphans the old intent. **The age
  gate does not delay the resume; it forfeits it.**
- `AlreadyLanded` uncounted on both paths (`:6383`, `:6494`); rollup `RowMismatch`
  folded into the same silent arm (`:6385`). Today's counters cannot distinguish
  "never fired" from "fired only as AlreadyLanded".

**Fix shapes (rollup):** (a) count `Skip`/`AlreadyLanded` so the blackout is
observable *first*; (b) key the rolling-deploy safety gate on **process identity**
(boot epoch / instance id in `StagedIntent`) rather than 30 min of wall clock — the
hazard it guards is "another *live* instance staged this", which identity answers
exactly and age only approximates; (c) if resume is meant to save the scan, the
intent must be recorded *before/while* parquet is staged, not after.

**Ruled out:** the env flag; a process-scoped map (the manifest is a file,
`TIMEFUSION_DATA_DIR/staged_intent.jsonl`); a run-id in the path; `requeue_running`
discarding the key.
**Unverified:** whether `TIMEFUSION_DATA_DIR` is on a persistent CapRover volume. If
it is not, the manifest vanishes each restart and that alone explains both halves.
Indirect evidence it *is* persistent: the 2026-08-11 root-owned-WAL-dir incident.
The decisive check is `staged_intent_reconciled entries=N` in a **boot** log window.

**Moves:** `repair_resumed_total`, `rollup_resumed_total`, `tasks_complete`/hr.

---

## P0c — Deploy discipline (process, no code)

5 deploys in 2h vs ~21-min units. This is both a *throughput* loss and the reason
no measurement is trustworthy. It is the **precondition** for honestly measuring
P0a and P0b.

- Batch changes behind env kill switches; **one change per deploy**.
- **≥2h quiet** before quoting any rate.
- Every experiment PR names the `timefusion_stats` metric it moves.
- Follow the ladder: `sim` → `run-unit` → staging → prod. Prod is the last rung.

---

## P1 — Rollup read path: 0 hits ✅ DIAGNOSED AND FIXED (2026-08-25, `6a401dc` + `e67a149`)

**The stated hypothesis was wrong.** This section guessed generation orphaning
(stored generation ≠ current). That is not why hits were 0. Diagnosed by running
monoscope's *real generated SQL* against prod with a counter delta around each run:

**Cause 1 — CSE hoisting made the matcher refuse SILENTLY.** `GROUP BY
COALESCE(<dim>::text, 'null')` — the form monoscope emits for **every** chart —
plans as a `CASE` naming its operand twice, so DataFusion lifts the operand into
`Projection: … AS __common_expr_1`. `source_and_filters` refused the aliased
projection, `match_aggregates` returned `Ok(Vec::new())`, and the caller's
`Ok(None)` arm recorded **nothing** — no hit, *and no miss*. So "0 hits, 55 misses"
undercounted the failure: most declines were not in the 55 at all.
`inline_common_exprs` now substitutes CSE's own aliases back for matching only.
Measured: **39.2s → 5.10s** (7.7x), one project, 7 days.

The pre-existing test passed for a year because a **bare column is not worth
hoisting** — the `::text` cast monoscope adds is the entire difference. Any future
matcher test must use the real generated SQL.

**Cause 2 — `count(*)` under a null guard.** Under `col IS NOT NULL` (consumed, not
pushed) `count(*)` is exactly `count(col)`, which `duration_count` declares. It was
refused outright. Fixed, then deliberately **withheld from queries also carrying a
`percentile_agg`** (`carries_digest`) — see P1b.

**Correctness:** routed vs raw totals over a fixed 7d window are exactly
**25,329,372 = 25,329,372**.

**New instrument:** `MissReason::UnwalkableSource` counts the previously-invisible
class. It is **not** a dashboard-latency class (0.43s, ~1–2/hr); its likely emitter
is a background `count(*) FROM (SELECT … LIMIT 5000) s`.

**Step 2's (a)/(b) generation decision is moot for hits** and should not be actioned
on this basis. Coverage was never the blocker — `rollup_min_contiguous_days = 30`
was true and correct throughout.

### P1c — `unwalkable_source` defined, and the generation hypothesis REFUTED live

`UnwalkableSource` did not exist in `d3b44f7`; it shipped **today** in `a2c1328`
alongside the CSE fix. So the 126 is the **residue after** that fix, not the shape
it targeted.

**Definition** (`rollup.rs:1707`, inside `match_aggregates`): `source_and_filters`
(`:1475`) walks only three node kinds from the outermost `Aggregate` down to the
scan — `Filter`, `TableScan`, and a `Projection` **whose every expr is a bare
`Expr::Column`**. Anything else → `None`. So **unwalkable = any other node between
aggregate and scan**: `SubqueryAlias` (derived tables / CTEs), `Join`, `Union`,
`Limit`/`Sort` under the aggregate, `Window`, `Unnest`, or any renaming/computed
projection `inline_common_exprs` cannot undo (it only inlines `__common_expr_N` and
bails on anything else, `:1530`).

**Proved by controlled prod experiment** (the one permitted bounded query; a 20s
control showed zero counter drift first): a 5-minute
`count(*) FROM (SELECT id FROM otel_logs_and_spans WHERE …) t` moved
`unwalkable_source` 134 → **135**. A derived table under the aggregate is exactly
one unwalkable miss.

- **Fixable:** `SubqueryAlias` — walking through is semantics-preserving modulo
  qualifiers, same template as the CSE inliner. **Monoscope's own convention emits
  it** (`docs/monoscope-query-shapes.md:296`: *"use `count(1) FROM (SELECT id …) t`"*).
  Cheapest win here.
- **Inherent:** aggregates over `Join` (§6 top-K-endpoints CTE join) — needs a real
  rewrite, not a walker tweak.
- Which dominates the live 126 is **speculation**, and must stay so — see the
  instrument gap below.

**Generation orphaning: mechanism real, REFUTED as the live cause.** The mechanism
exists exactly as described (`maintain.rs:3630` skips on `generation_id != generation`;
the gauge uses `partitions_of()`, paths + non-emptiness only). But the running
container's own hourly recovery log, both passes, both sources:
```
14:33  otel_logs_and_spans  recovered=21126 unverifiable=2349 stale_generation=0
14:33  otel_metrics         recovered=5151  unverifiable=2037 stale_generation=0
15:33  otel_logs_and_spans  recovered=21249 unverifiable=2349 stale_generation=0
```
**`stale_generation = 0`.** Either healed (367 full rebuilds this process) or never
applied to the current spec pair. **What actually blocks coverage is the witness —
see P3b, now promoted.**

### 🚨 Instrument gap — the top miss reason is invisible to the only shape instrument

The sampler exists (`dml.rs:266`, `warn!(reason, plan, event = "rollup_miss_sampled")`),
read with:
```bash
ssh ubuntu@captain.s.past3.tech 'docker logs $(docker ps -qf name=timefusion) 2>&1 | grep rollup_miss_sampled'
```
(container-direct; `docker service logs --since 5h` takes >10 min on this host.)

Two defects:
1. **Structurally blind to `UnwalkableSource`.** It is recorded inside
   `match_aggregates`, which returns `Ok(Vec::new())` → `rollup_sql` returns
   `Ok(None)` → `dml.rs`'s `Ok(None) => {}` arm. The sampler only runs on the
   `Err(reason)` arm.
2. **Rate-starved.** `sample_rollup_miss()` (`observability.rs:789`) samples 1 in
   512 across three call sites — at ~1.7 misses/min that is one line per ~5h. In
   4.5h of prod the log holds exactly **one** sample.

**Action:** a sampled `warn!` at `rollup.rs:1709` rendering the plan, and a far
smaller divisor. Until then, any shape attribution for the 126 is guesswork.

---

## P1b — Rollup percentiles read back NULL, and a mixed window returns a BIASED number 🚨

Found while verifying P1. **Pre-existing, unrelated to the P1 fix, and the most
damaging thing in this document.**

`approx_percentile(0.95, duration_digest)` is NULL on every stored row of both
tiers for sealed dates, while `duration_count` beside it is healthy (33,585 /
59,155 / 64,522). Audited all 15 declared measures: **14 sound, only
`duration_digest` broken**; `server_duration_digest` reads back correctly, so
tdigests as such are fine.

Cause: `duration_digest` was **declared 08-22 but not materialized until 08-24
~10:30 UTC**. Files written before that lack the column, Delta null-fills it, and
the reader serves it. So **generation is not a usable proxy either** — 08-22/08-23
cells carry the *current* generation and still have no digest.

**Why it is worse than it looks:** `tdigest_merge` skips NULLs, so a wide window
does not fail — it returns a plausible number computed from the covered slice
alone. Measured: p95 over 08-22..08-25 = 392,149,642 from **3,438 of 14,830 rows
(23%)**. A 7d chart silently reports the last ~1.5 days as the whole window.

**The class:** adding a measure to a spec lands it NULL in every already-written
cell, and nothing distinguishes "built without the measure" from "legitimately
empty". This gates any future measure addition (see the dcount/countif items).

**Holding position shipped (`e67a149`):** the guarded-count rewrite is withheld from
percentile-bearing queries, so monoscope's latency widget scans raw — 32.9s and
verified complete at 29/29 buckets. Delete that clause once cells rebuild.

**Real fix in progress:** record measure presence per cell at publish
(`TAG_MEASURES`) and refuse a cell for a query needing a measure it cannot prove.
Note `SUM` skips NULLs exactly like `tdigest_merge`, so a missing **scalar** measure
biases just as silently — the state-vs-scalar distinction is NOT a valid rule.

**Note:** build-side needs nothing. The 08-24 `No field named duration_digest`
blocker is **fixed** — 0 occurrences in 45 min of logs. Do not re-litigate it.

---

## P2 — The 87-day tail: 3,084 units back to 2026-05-30 📊 measure, don't fix yet

Steady ~30–60 units/day across the whole history; **mostly `attempts = 0`** — that
is *planned-and-never-claimed*, not failing. Per project:

| Project | Fresh (08-24/25) | Tail | Oldest |
|---|---|---|---|
| past3 | 466 | **1,734** | 2026-05-30 |
| Talstack Prod | 499 | 159 | — |
| Mainhedge | 407 | 146 | — |
| **Shipbubble** | 438 | **104** | 2026-07-20 |

### ✅ ROOT CAUSE FOUND (2026-08-25) — `starved` is a 31-day cliff, and the naming is inverted

`TaskJournal::rank` (`maintenance_coordinator.rs:1489`) sorts on
`(class, damaged, starved, hole, width, benefit, order)`, smallest first. The term:

```rust
// maintenance_coordinator.rs:2896
let waited = now_micros.saturating_sub(task.key.slice.end_micros);
let starved = u8::from(!(STARVATION_MICROS..=STARVATION_HORIZON_MICROS).contains(&waited));
```
`STARVATION_MICROS = 3d`, `STARVATION_HORIZON_MICROS = 31d`.

`starved == 0` **wins**, and requires `waited ∈ [3d, 31d]`. A 2026-06-15 slice has
waited 71 days → `starved = 1` → it lands in the losing half of a **strict-priority
split**, behind *every* unit in the 3–31d band. It is never compared on `hole`,
`width`, `benefit` or `order` at all. `starved` sits at index 2, **ahead of `hole`**,
so an ancient *missing* day loses to an in-window *re-derive*.

Three bands in the journal (7,087 Pending/Retry):

| band | count | fate |
|---|---|---|
| A: ≥08-23 (<3d, too fresh) | 4,026 | `starved=1`, waits |
| **B: 07-25…08-22 (3–31d)** | **1,824** | **wins everything** |
| **C: ≤07-24 (>31d)** | **1,237** | **permanently last** |

**Empirical proof** — every `maintenance_task_started` in a 40-min prod window,
bucketed by slice date, lands only in band A or B. **Zero starts anywhere in
05-30 … 07-20.** The one apparent exception (BaseRollup 2026-07-21, 73 starts) is
the `damaged` escape hatch (`hole_rank == 0`, which precedes `starved`) — and it is
a claim *loop*, not progress: `rollup_tier_untagged_retired_total = 0` with 41
`unit_timed_out` in the same 40 min.

**Why it never self-heals:** band B is refilled every midnight and does not drain
(its units time out — observed `ran_secs=300 input_files=1` on 07-25 dedup units),
so its residue crosses the 31-day cliff into band C. The tail's attempt histogram
confirms the mechanism: 1,019 at `attempts=0` but **207 at `attempts=1`** — tried
while inside the window, then aged out. **Corollary: `oldest_task_age_seconds` can
never decrease under this design. It gauges band C, not queue health.**

The comment at `:2831` states the intent — *"Outside the window a partition still
gets served by ordinary newest-first ordering. It is deprioritised, not
abandoned."* Empirically it **is** abandonment, because the premise (band B empties
first) is false.

### Correction: `most_indebted_unclaimed` cannot see this tail

SHIPPED AND WORKING, but **wrong scope**. It is a **log line**, not a stats row —
`maintain.rs:354`, fired every ~90s:
```bash
ssh ubuntu@captain.s.past3.tech 'docker service logs srv-captain--timefusion --since 40m 2>&1 \
  | sed -e "s/\x1b\[[0-9;]*m//g" | grep maintenance_hygiene_debt_unclaimed | tail'
```
(the `sed` matters: ANSI escapes sit *inside* `event="…"`.) It loops only
`[SealedConsolidation, HotPacking]` and selects by max `input.files`, **not by age** —
and band C contains zero hygiene units. Likewise **3465ecc is irrelevant here**:
hygiene is re-derived from live file debt every 60s and cannot accumulate a
months-old queue.

### Second, separate finding: a planning-side horizon

`timefusion_rollup_backfill_days = 31` (`maintain.rs:610`) means the planner never
*re-plans* a rollup cell older than 31 days. The 469 ancient `base_rollup` units are
legacy enqueues from before the horizon passed them: queued, eligible, unretired,
**unrankable**. That is the filter-shaped thing the evidence pointed at — but it
lives in planning, not claiming. There is **no** hard filter on the claim side
(`deadline` long past, `is_quarantined` needs attempts≥N and these are 0,
`dependencies_complete` returns `None` for BaseRollup).

**Fix decision required (do not pick silently):** (a) make `starved` a graded term
rather than a binary cliff so age keeps accruing rank past 31d; (b) raise/remove
`STARVATION_HORIZON_MICROS`; (c) retire band C deliberately as un-plannable and
stop gauging it. (a) is most faithful to the stated intent. **Whichever is chosen,
band B's timeouts must be fixed too, or the cliff simply refills.**

**Moves:** `oldest_task_age_seconds` (currently 85 days), tail count.

---

## P2b — Dirty-bin queue: 24,594 deep, 0 processed ⏸ re-measure after P0a

I verified the drain **does** have a caller and the cold-starvation reserve fix is
in place (`select_drain_bins`, `maintain.rs:4337` — hot/cold split with a reserved
cold share). So this is throughput, not a missing call.

It stages through the **same heavy pool** as P0a. **Propose no separate fix until
P0a lands and this is re-measured** — it may be the same root cause.

---

## P3 — Derived-rollup supersede churn ❌ PREMISE REFUTED — one closed incident, not steady state

5,322 superseded vs 2,027 complete fleet-wide; Shipbubble 463 vs 187 (71%).

**My stated premise was wrong.** Nothing supersedes a derived task because the base
tier moved: base motion goes through `invalidate()` (`coordinator.rs:1934`), which
flips a task back to **Pending** and never supersedes. There are exactly two
supersede writers — `split_time_task` (`:2482`) and `migrate_derived_slices`
(`:821`).

**It is one two-day incident, and it is over.** By slice date, **87%** (4,632 +
224 of 5,322) are 08-22/08-23. By creation day:

| created | superseded | complete |
|---|---|---|
| 08-23 | 4,632 | 508 |
| **08-24** | **4** | **515** |
| **08-25** | **0** | **77** |

Live prod agrees: 45 min of logs show every `DerivedRollup` finishing
`outcome=Complete` in `ran_secs=0..4`, zero `worker_error`, `pending_derived`
draining. The underlying 08-22/23 failure was very likely the `duration_digest`
schema break (*corroborated inference* — `retry_reason` is overwritten on a
superseded record, so it is not provable from the journal).

**Base-rollup supersession is cheap bookkeeping**, not waste: 4,834 of 4,932 (98%)
at `attempts == 1`, split at the first claim's **preflight** — before the scan
(`maintain.rs:1739` marks the line explicitly). Cost accounting by claims burned:
derived 10,242 nothing-producing claims (65%), base 22%, dedup 13%, repair 13%.
**Do not multiply by the 900s deadline** — per-claim cost is unrecoverable from the
journal and today's derived units cost seconds.

### ❌ Day-seal gating REJECTED

Derived already gates on base completion (`dependencies_complete`,
`coordinator.rs:2275`), already waits a 15-min quiet period
(`FINALIZATION_DELAY_MICROS`, `:1941`), and already gets a permanent sealed turn
(`:2205`). The supersessions were **two failed runs on a schema mismatch**, not
partition-chasing — sealing would have delayed the same failure by 24h while adding
up to 24h latency to the tier that 14d/30d dashboards read. Regression, no benefit.

### The churn loop (three signatures agree)

Unit fails twice → split into sub-hour children → next boot `migrate_derived_slices`
supersedes the children and re-enqueues the 1h key → `enqueue_inner` resurrects the
superseded parent to Pending/attempts=0 (`:1816-1840`) → it fails again. Signatures:
not one superseded record survives at width 1.0h; the migrated population peaks at
width 1 **minute** (2,117 of 2,718 = `MIN_SLICE_MICROS`, bottom of the bisection
ladder); most migrated records carry `attempts >= 2`.

**Two hardening fixes (not urgent — the episode is over):**
1. **`abandon_running` bisects unconditionally at `attempts >= 2`** (`:2386`) while
   `retry_or_split` correctly requires `is_capacity_failure` (`:2431`). A schema,
   missing-column or permission error **cannot** be fixed by halving a time slice —
   bisecting just multiplies the failing units. Gate it on capacity-shaped failure
   (or wall-time overrun) and quarantine everything else.
2. **`migrate_derived_slices` should skip split children.** It was written for
   legacy 10-min derived units; it collapses split children, erases the bisection
   ladder and resurrects the parent. `parent_measured_bytes.is_some()` is a clean
   discriminator already on the record.

**Prior art, both verified FIXED on master:** "superseded parents vetoed the
backfill" (`blocks_rollup_backfill` excludes `Complete | Superseded`, `:2951`), and
"a terminal Superseded task vetoes its own re-enqueue" (`enqueue_inner` reopens,
`:1812`). Note the second fix is *also* step 3 of the churn loop — correct in
itself, but it is what closes the loop.

---

## P3b — Witness ❌ PREMISE REFUTED, but the defect is REAL and now the top coverage blocker 🚨

**The physical-vs-logical mismatch does not exist.** Both sides are physical,
deliberately. Comparison at `mod.rs:4026` → `rollup.rs:603`. Read side `current` =
`PartitionStats::rows` = **Σ `num_records`** (`mod.rs:1603`); write side
`source_rows` = `stats.rows`, the same computation (`maintain.rs:1595`, stamped as
`TAG_SOURCE_ROWS` at `:1942`); ledger side identical (`maintain.rs:3191/3211`). The
`PartitionStats::rows` doc says outright it *"must stay THIS computation on both
sides."*
> The `CoverageEntry::source_rows` comment at `storage.rs:3629` claiming "LOGICAL
> deduped source rows" does **not** match the writer. It is **stale** — treat it as
> a docs bug, not a spec. Fixing that comment is part of this item.

**The real defect:** *because* the witness is physical, a benign dedup or compaction
changes `num_records` without changing what the rollup aggregated, and voids a
**correct** slice. That is false invalidation.

### This is now the #1 coverage blocker, ahead of generation orphaning

`recover_date_coverage` (`maintain.rs:3195`) admits a date only if **every** slice's
witness still matches. Measured on the mature process:

| tier | contiguous_days (gauge) | partition_cells | **usable_cells** |
|---|---|---|---|
| `..._dashboard_1m_v3` | 30 | 511 | **20** |
| `..._dashboard_1h_v2` | 30 | 496 | **61** |
| `..._metrics_1m_v2` | 30 | 373 | **14** |
| `..._metrics_1h_v2` | 30 | 357 | **54** |

`rollup_stale_moved = 4,503`, `stale_grew = 4,103`, `stale_shrank = 400`,
`stale_no_witness = 0`, plus 2,037 witnessless (pre-tag, unverifiable rather than
disagreeing). **The 30/30 gauge and 4–12% usable cells are both true** — the gauge
counts partitions, the read path needs witnesses. The date-level map is nearly
empty; the 17 hybrid hits come from *per-slice* coverage.

### ❌ The `source_fp` fallback is unsound and was ALREADY BUILT AND REMOVED

`mod.rs:4017-4025` records it: built and measured 2026-08-22, reverted in
**`7e5bb5a`**. A slice's `source_fp` is an `FnvHasher` over the files *selected for
that slice*; the partition fingerprint is a `DefaultHasher` over the partition's
*whole live file set* — different hasher, different set, so they can never be equal.
It routed nothing, failing safe as a permanent miss. Same warning at
`maintain.rs:3193`. **Do not re-propose it.**

**Correct direction:** a witness invariant under dedup/compaction — a logical row
count, or a content digest carried forward across a rewrite. That means changing
what the **builder stamps**, on both sides at once. This is a design item, not a
patch.

**Promoted:** no longer deferred behind P1. P1's hit path is fixed, so this is what
still holds coverage down.

---

## Watch-only — no action this round

- `dedup_denied_never_certified` **100%** — read-latency, not maintenance.
- `read_dedup.ordering_violations_delta = 15,250`.
- jemalloc `frag_pct 49.3`, `retained 1,084 GB` (RSS 31 GB, host has 109 GB free).
- Ingest/flush **healthy**: pressure 19%, 0 backpressure, 0 failed flushes.

## OPEN ITEMS as of 2026-08-26 — what is left, and why it is left

Canonical list. Each row: the item, why it is not done, and what unblocks it.
Nothing here is forgotten work; every row is blocked, deferred on a stated
condition, or waiting on wall clock.

| # | Item | Why not done | Unblocked by |
|---|---|---|---|
| 25 | Verify the repair converged; delete `DAMAGED_CELLS` | **Physics.** 71 forced pairs x ~21-min base units against a live backlog. Mechanism verified (`forced=71`, rebuilds +703, `pending_base_rollup` draining 2752→2338 — first drain all day); no sample pair converged yet | wall clock; a monitor watches the reference cell |
| 27 | Attribute `unwalkable_source` from real traffic | **Not deployed yet.** The `rollup_declined_shape` warn is pushed but prod has not picked it up, so there are no logs. Attribution *by construction* was done instead | prod picking up the diagnostics |
| 26 | ✅ **DONE** — audit the unchecked measures | **11 measures verified EXACT** on both tiers (error_count, server_error_count, server_request_count, server_error_scope_count, duration_count/sum/min/max, server_duration_count/sum, server_duration_digest); zero invariant violations over 1.66M rows. `CLEAN_LIST.csv` **is** a bill of health FOR THOSE ELEVEN BY NAME, and **void** for the two state measures below | — |
| — | ⚠️ `duration_digest` boundary MOVED LATER | First fully healthy date is **2026-08-25, not 08-22**. ≤08-23 is 100% unreadable; **08-24 is 22-45% empty** — it answers from ~half its rows instead of declining. Byte-identical 6 min apart: **the repair rebuilds `request_count`, not digests**, so it does NOT self-heal | a digest rebuild, which nothing currently schedules |
| — | ✅ `service_name_hll` MITIGATED same session | Declared 08-26 and empty on every earlier date; `distinct_count` of an empty sketch is **0, not NULL**, so a routed widget rendered **0 services** (measured est 1 vs 2, est 15 vs 18). `MEASURES_NOT_YET_SERVABLE` now refuses it on EVERY cell, tagged or not — dcount falls back to raw. Declaration deliberately STAYS so history accrues | delete the entry when #24 lands AND an audit shows it reads back |
| 29 | The 1h tier's merged p95 reads **+6.1%** vs 1m | ONE data point on healthy data (98fdd4f3/08-25: 1m 389.4M at 0.09% from raw, 1h 413.4M). Either t-digest merge physics or a derived-fold defect — opposite responses, so not actioned | compare 3-4 more healthy cells (dates ≥08-25): one-directional bias = defect, symmetric = physics |
| 24 | `materialized_measures` checks schema presence, not value presence | Found while verifying #14, not while looking. Affects **two** measures now (`duration_digest`, `service_name_hll`) | in progress |
| 23 | No read-side guard for a cell short on ROWS | Architecture question with a real fail-closed cost. Its motivating population may be mostly #24 + the already-fixed write path | #24's finding sizes it |
| 18 | Variant projection below DedupExec | **Deprioritised twice.** Real (~2-4s at 24h) but touches dedup ordering, which is correctness-critical and produced three undercount incidents today. Its safer sibling (#17, 4.2x measured) shipped instead | everything else landing + a quiet prod; and proving the dedup keys do not depend on dropped columns |
| 15 | `server_error_count` counts outbound client spans | **Deferred on stated preconditions**, unchanged: a filter change alters what the measure MEANS, so it must orphan the tier | repair converged + verified, backlog drained |
| 28 | The `countif` near-miss | **Entangled with 15**, not hard. The one-line dashboard fix was prepared and REVERTED — it would make a tenth widget adopt the scope convention 15 questions, for ~2 declines/hr | resolved with 15, together |
| 6 | `level` as a dimension | **Deferred on stated preconditions**, unchanged: a DIMENSION change alters the grouping of every stored row, so no per-cell mechanism rescues it. Unavoidable fleet rebuild | repair converged + verified, backlog drained, quiet process |

**The defers (6, 15) stand.** Their preconditions have not arrived; only time has
passed. They are decisions, not open questions — do not re-litigate them, check
the conditions.

**Pattern worth naming:** four of these rows (23, 24, 26, 27) exist because
verifying a fix found a *different* problem. None was on any list this morning.
The fixes were mostly straightforward; the expensive part was discovering that
"clean" measurements were not.

## 2026-08-26 — short-cell repair DEPLOYED, and two spec changes DEFERRED

**Damage, measured not estimated:** 320 comparable (project, date) pairs across 15
projects, **81 disagreeing**, ~**211M rows** missing from the 1h tier. 41 pairs are
missing 15+ hours; many read `1/24` — one hour surviving a whole day. Three are
OVER-counted instead (healthy base, full coverage — duplicate/MoR summing).
Durable: `scratchpad/DAMAGE_LIST.csv`, `CLEAN_LIST.csv`.

**Repair shipped** (`43ee5d8`, deployed `81f63de`): a one-shot on a NEW cursor
(`__maintenance_damage_repair_v1`) forcing every tier of a measured LIST of 81 pairs
into `missing_tiers`. A list, not a date window — the window form would have dragged
236 clean pairs into a queue measured GROWING at ~+80 units/hr behind an 11.3-day
starved tail. Ordering needed no special handling: `Journal::rank` compares
`hole > 0` BEFORE `starved`, verified in code.

Fired: `rollup_damaged_cell_repair forced=71 listed=81`. Within ~40 min
`rollup_rebuilds_full` +703, `rollup_derived_base_incomplete` +252 (the
derived-witness gate making derived units wait for their base), and
**`pending_base_rollup` draining for the first time** (2752 → 2338). Convergence is
wall-clock and NOT done.

**⚠️ Exposure GREW before the repair landed.** On 28f62f01 / 2026-08-20:
two-half split (does not route) = **3,580,826** (truth); plain day-range `count(*)`
= 3,084,885 (the short tier); tier stored = 3,084,885. That same query fell back to
raw and was CORRECT this morning. Routing over these cells widened during the day,
converting "slow but correct" into "fast but wrong", and it is not limited to
dashboards — any count over a damaged day is affected.

**The gap that allows it:** there is a read gate for a cell missing a MEASURE
(`measures_available`) but **none for a cell short on ROWS**. The derived-witness fix
is write-side, and the source-row witness cannot help because a derived cell
witnesses the RAW partition rather than the base tier it read, so it agrees forever
on a sealed day.

**Only `request_count` was audited** — `duration_*`, `error_count`, `server_*` and
the digests were never compared. `CLEAN_LIST.csv` is NOT a full bill of health.

### Two spec changes DEFERRED (decisions, not open questions)

**`level` as a dimension** — a DIMENSION change alters the grouping of every stored
row, so no per-cell mechanism rescues it (unlike measures, which `TAG_MEASURES` plus
the narrowed `generation_id` now let coexist). Unavoidable fleet rebuild.
Preconditions: repair converged and verified, base backlog drained, quiet process.
Cardinality is NOT the blocker; the rebuild is.

**`server_error_count` semantics** — 24,030 of 24,054 rows it counts are
`kind='client'` spans named `monoscope.http`; only 24 are genuinely server. Faithful
to the spec; the spec is wrong. A filter change alters what the measure MEANS, so it
must orphan. Interim needing no spec change is monoscope-side: point genuine
inbound-error charts at unscoped `error_count`, or relabel the widgets.

**A one-line dashboard fix was prepared and DELIBERATELY REVERTED.** The "Error Rate"
panel (`_overview.yaml:120`) uses a 2-term scope while nine siblings and the declared
measure use 3 terms, so it declines `unknown_filter`. Adding the third term makes it
route with no rebuild — but it would adopt the very convention the `server_error_count`
decision is questioning, changes live numbers, and buys ~2 declines/hr. Ship it with
that decision, not before. A deliberate omission, not an oversight.

## Explicitly excluded

- **Compaction** — 161 GB/hr, 57 GB/hr net drain, ~9h to clear. Working. Don't touch.
- **Scheduling lag** and **`duration_digest`** — both verified fixed this session.
- **Raising the maintenance pool** — the merge streams are unspillable; partitions
  are the dial (P0a). Raising the pool defers rather than fixes, and the 3-way
  split is deliberate.

---

## Read-path items opened 2026-08-25 after P1 (swept with real monoscope SQL)

26 generated chart shapes run once each on `e67a149` with a counter delta. **No
silent class remains** — every shape now moves a counter. Ranked by time lost:

1. **Variant JSON-path group-by fails to PLAN — 17 errors in 90 min of prod logs.**
   `::` binds tighter than `->>`, so monoscope's unparenthesized
   `attributes->>'route'::text` (Parser.hs:430, :445 — neither site parenthesizes)
   casts the *path literal*. `extract_path_component` matched only a bare literal,
   the variant planner bailed, and `json_as_text` got a Variant struct. **Not a
   timeout — an instant planning error.** Fixed TF-side by unwrapping the cast.
2. **`variant_to_json(v) -> path` is 87–91% of its query.** It serializes the whole
   struct per row (~10 µs/row) so `json_get` can re-parse it for one field.
   Measured on `otel_metrics`: 6h **5.93s → 1.61s**, 24h **26.4s → 6.26s** (4.2x)
   using the native arrow path. Fix as a TF peephole
   (`json_get(variant_to_json(v), k)` → `variant_get(v, k)`); monoscope's rewrite
   at `Stats.hs:269-274` is the other half.
3. **Non-dimension filters are the dominant steady loss.** Ambient traffic in a
   10-min window with nothing of ours running: **+26 misses, 0 hits**, 20 of them
   `unknown_filter` (~120/hr). A/B: `kind='server'` routes in 6.11s, `name='GET'`
   scans for 19.8s.
4. **Miss reasons are MASKED and cannot rank the above.** `match_aggregates` reports
   `first_miss` = the *coarsest* spec's reason. The 1h tier always fails
   `PartialBucket` on a sub-hour bucket, so it wins for every decline at the 30-min
   width monoscope uses at 3d. Proven: identical query reports `unaligned_bucket`
   at 30min and `unsupported_shape` at 1h. **Prod's `unaligned_bucket = 14` is 14
   unknown reasons.** Fix this before ranking anything in item 3.
5. **`countif` / `dcount` near-misses.** `COUNT(*) FILTER (WHERE status_code='ERROR')`
   does not match `error_count`'s wider declared filter; `approx_count_distinct` is
   routable in principle but **no `hll` measure is declared**. Both gated on P1b —
   adding a measure lands it NULL in every existing cell.
6. **The Variant projection sits ABOVE DedupExec** on the spans path, so ~5.2M rows
   of struct cross the merge stack before one field is read. Payload cost (~2.0s
   merge + ~2.5s IO at 24h) exceeds the extraction itself (2.68s).

**Ruled out by measurement — do not pursue:** routing these shapes (the flat-column
control does the same scan in 1.53s at 24h; routing removes 9% of the cost), the
scan, spills (`spill_count = 0` on every operator of all 7 plans), group cardinality
(cheapest shape has 193k groups, dearest has 182), cold cache, the scan-pressure
valve, and any "3-day cliff" — cost is **linear** at 10.7 µs/row from 90k to 2.47M
rows, a straight line crossing a fixed 60s ceiling.

Also opened: `server_error_count` counts **outbound client spans** as server errors
(24,030 of 24,054 are `kind='client'`, pulled in by the `name='monoscope.http'`
OR-arm) — a third-party outage renders as the tenant's own. And the 08-20 slice
**undercounts by 13.9%** (hour 23 absent entirely); verified *latent* — a routed
count returns the raw value today — but stored wrong, with the source-row witness
not catching it.

## Order of execution

Revised 2026-08-25 after the diagnosis round. Every item below now has a confirmed
root cause; nothing here is still "go look".

```
DONE   P0a dedup ladder      implemented + test (UNCOMMITTED — see hazard below)
DONE   P0c deploy discipline in force (4.4h natural quiet)
DONE   P1  rollup hits       fixed upstream (6a401dc, e67a149)

NOW, in value order:
 1. P1b  percentile NULL / biased mixed window        🚨 correctness, user-visible
 2. P3b  witness false-invalidation                   #1 coverage blocker (20/511 cells)
 3. P0b  resume — repair half (~6 lines)              destructive today: stages then deletes
 4. P2   the 31-day `starved` cliff                   needs a decision, see below
 5. P1c  walk SubqueryAlias in source_and_filters     cheapest routing win
 6. P0b  resume — rollup half (count Skip FIRST, then identity-key the gate)
 7. P1c  fix the miss sampler (blind + 1-in-512)      unblocks all future shape work
 8. P3   two hardening fixes (episode over, not urgent)
 9. P2b  re-measure dirty-bin  [still gated on P0a landing]
```

**Two decisions needed from the user — do not pick silently:**
- **P2:** grade the `starved` term / raise the horizon / retire band C. (a) is most
  faithful to the stated intent, but band B's timeouts must be fixed too or the
  cliff just refills.
- **P3b:** the sound witness is a *builder* change on both sides at once (logical
  count or a carried-forward digest). That is a design item, not a patch.

**What changed about this plan's method.** Four of my own premises were refuted by
the diagnosis round — derived churn (not base motion; a closed incident), the
witness (not physical-vs-logical; both physical), the rollup resume half (reached,
not unreached), and generation orphaning (`stale_generation=0` live). Three of the
four had a *plausible mechanism that was real in code* but was not what prod was
doing. **Confirming a mechanism exists is not confirming it fires.** Every section
above now separates the two.

---

## ⚠️ Concurrency hazard — why P0a is uncommitted

A second session was editing this checkout concurrently (11 modified files,
including all three P0a touches). Committing would have swept its half-finished
work into my commit — the exact failure that broke HEAD on 2026-08-24. Also, its
in-flight edit to `src/read/functions.rs:1958` (an rkyv `ArchivedOption` mismatch)
**does not compile in the lib-test target**, so the ladder test could not run in the
main tree.

Actions taken instead:
- Full diff of my four files saved to `scratchpad/all-changes.patch`.
- P0a re-applied cleanly onto a **detached worktree at `e67a149`**
  (`scratchpad/tf-verify`) and verified there — the isolation my 2026-08-24 notes
  prescribe.
- **Nothing pushed.** `deploy.yml` auto-deploys master, and an unattended deploy
  would also destroy the quiet window P0b/P2 measurements depend on.

## Branches produced 2026-08-25 — four commits across three branches, none pushed

| branch | commit | item | state |
|---|---|---|---|
| `p0a-dedup-sort-ladder` | `ec54155` | dedup sort-partition ladder | ✅ test + lint + full suite |
| `p0a-dedup-sort-ladder` | `b376eda` | resume call site wired | ⚠️ inert until `49bf7f9` |
| `resume-process-identity` | `7521679` | count every silent resume exit | ✅ 948 tests, lint 0 |
| `resume-process-identity` | `49bf7f9` | **ownership gate replaces the 30-min age gate** | ✅ tests+lint · 🚨 **deploy-gated, see below** |
| `worktree-agent-a4dd…` | `bf83153` | bisection gated on `ran_micros`; migration skips split children | ✅ 949 tests, lint 0 |

`resume-process-identity` branches off `b376eda`, so it carries the ladder too.
`bf83153` is independent of the other two.

### 🚨 `49bf7f9` must not deploy before this is resolved

If two instances overlap, B resumes and commits A's adds; A's commit then fails on
the Delta conflict. On the **rollup** path that is harmless. On the **repair/dedup
wave** path a failed wave commit calls `discard_bins → cleanup_orphaned_parquet`,
which would **delete parquet B just made live** — live-data deletion, not wasted
work. The shape is **pre-existing** (it already applied to any intent older than 30
minutes) but the ownership gate **widens the window** to any foreign-instance
intent, which is precisely the point of the change.

Fix order: make the wave-failure path skip adds that are live in the current
snapshot (small, and worth doing regardless since this can fire today), *then*
confirm Swarm never runs two replicas concurrently. Full options in task #18.

### Two judgement calls worth a second opinion

- **`bf83153`** gates bisection on `ran_micros >= deadline/2`. `is_capacity_failure`
  could **not** be reused: `abandon_running` fires from `TaskLease::drop` and sees a
  deadline-dropped future and an `Err`-returning run function as *indistinguishable*
  — the lease drops before any error reaches the caller, so no reason string exists.
  `ran_micros` is the only honest discriminator on hand, but **half-the-deadline is
  a judgement call, not a measured constant.** Revisit with real `ran_micros` data.
- **`49bf7f9`** keeps `staged_orphan_deletions` on pure age deliberately: identity
  proves "not ours", never "its owner is dead", and deleting a peer's staged parquet
  destroys work whereas committing it only races. Also note PID is unusable as the
  identity — prod is PID 1 in a container, so every restart would look like the same
  process; it uses a per-process UUID.

### Branch `p0a-dedup-sort-ladder` (worktree `scratchpad/tf-verify`, based on `e67a149`)

| commit | item | verified |
|---|---|---|
| `ec54155` | dedup sort-partition ladder | ✅ `cargo nextest run --lib dedup_rewrite_narrows` → 1 passed |
| `b376eda` | coordinator consults the staged-intent manifest | ⚠️ compiles, **gated by the 30-min age check**, no behavioural test |

Only `ec54155` delivers throughput on its own. `b376eda` removes the destructive
write-then-delete, but needs P0b fix (b) — the identity-keyed gate — before it can
actually resume anything.

**`cargo lint` (CI's exact definition) passes clean on the branch — exit 0.** It did
*not* on the first try: two `clippy::cloned_ref_to_slice_refs` errors from the
resume change (`&[date_marker.clone()]` → `std::slice::from_ref(&date_marker)`),
both `-D warnings` failures that a bare `cargo clippy` would have printed and
exited 0 on. That trap is documented in CLAUDE.md and it fired here exactly as
described.

**Full `make test` RUN on the branch — 1,164 tests, 2 failures, NEITHER from this
work:**

1. `dedup_compaction_test::count_is_identical_with_and_without_the_per_file_dedup_skip`
   — **MinIO flake.** It failed on an HTTP connection error to
   `127.0.0.1:9000/timefusion-tests`, the documented bucket gotcha in CLAUDE.md, not
   an assertion. **Passed on re-run.**
2. `dedup_compaction_test::a_count_star_beside_the_null_guard_refuses_to_route`
   — **PRE-EXISTING ON MASTER.** Proven: checked out clean `e67a149` in the worktree
   and it fails identically (`left: 1, right: 0`). This branch touches no
   `rollup.rs` / `read/` / `dml.rs` file — the whole diff is 3 files, 90 insertions,
   all under `src/database/`.

> ### 🔴 master is red — separate issue, flagged not fixed
> `a_count_star_beside_the_null_guard_refuses_to_route` asserts that a `count(*)`
> under a null guard **must not** route, because it counts a different row set than
> the tier's `request_count`. It is routing. The test came from `91030f9` and
> `e67a149` ("keep the guarded-count rewrite away from the latency widget") touched
> only `src/rollup.rs` — both the concurrent session's work, which still has
> uncommitted changes in that file. **Correctness-adjacent** (P1b territory: wrong
> counts on guarded charts). Owner is that session; do not fix in parallel.

**Nothing is pushed.** Pushing master *is* deploying — `deploy.yml` auto-deploys —
so it was not done unattended, and it would also have destroyed the quiet window
P0b/P2 measurements depend on.

### Runbook — deploy and measure (execute in order, one change per deploy)

Baseline captured 2026-08-25 on `e67a149`, **6.1h quiet, no redeploy** — two reads
1.7h apart, so these are rates not snapshots:

| metric | @4.4h | @6.1h | rate |
|---|---|---|---|
| `tasks_retry` | 405 | **429** | **+14/hr — still climbing** |
| `dedup_bins_committed_total` | 16 | 21 | ~3/hr |
| `tasks_pending` | 6,863 | **7,183** | **+188/hr** |
| `tasks_complete` | 39,789 | 40,016 | ~134/hr |
| `repair_resumed_total` / `rollup_resumed_total` | 0 / 0 | **0 / 0** | never fires |
| `dirty_bin_processed_total` | 0 | **0** | never drains |
| `sealed_compaction_debt_bytes` | 216 GB | 214.8 GB | ~flat — bulk already drained |

Three things this second read establishes that the first could not:
- **`tasks_retry` is still rising** (+24 in 1.7h) on a quiet process — the OOM
  loopers are live and compounding, so P0a is not stale.
- **The queue grows faster than first measured** (+188/hr here vs +52/hr over the
  earlier 4.2h window). It is not treading water; it is losing ground.
- **Compaction has essentially finished its debt** (216 → 214.8 GB). The earlier
  "57 GB/hr draining, ~9h to clear" was real and has largely played out — so
  compaction is no longer where the headroom is. **Maintenance throughput is.**

```bash
export PGURL="$(grep -m1 '^TIMEFUSION_PG_URL=' ../monoscope/.env | cut -d= -f2-)"
psql "$PGURL" -t -A -F'|' -c \
  "SELECT component||'.'||key,value FROM timefusion_stats WHERE key<>'retry_reason' ORDER BY 1;"
```

**Step 1 — merge and gate.** Confirm the other session is done, `git merge` (never
rebase) `p0a-dedup-sort-ladder`, then `make prepush`. Expect the two known failures
above; re-run the MinIO one, and treat the guarded-count one as master's
pre-existing red (see the 🔴 box) — do not attribute either to this branch.

**Step 2 — deploy `ec54155` ALONE.** Wait for `docker service ps` to show the new
SHA, then **≥2h quiet** (no further deploys) before reading anything. Success looks
like: `tasks_retry` **405 → ~350** (the 56 OOM loopers stop retrying) and
`dedup_bins_committed_total`/hr rising from ~3.6. If `tasks_retry` does not fall,
the ladder is not being reached — check `dedup_sort_partitions` is receiving
`attempts >= 2` units, not that the pool got bigger.

**Step 3 — do NOT deploy `b376eda` expecting a result.** It cannot fire until the
identity-keyed gate lands (P0b fix (b)). Deploy it only bundled with that work, and
measure `repair_resumed_total` 0 → nonzero.

**Step 4 — only then re-measure P2b** (`dirty_bin_processed_total`, still 0). It
shares the heavy pool with dedup and may need no separate fix at all.

**Rule that makes all of the above meaningful:** one change per deploy, ≥2h quiet
before quoting any rate, and every experiment names the metric it moves. The
2026-08-25 session watched prod redeploy itself mid-investigation (`d3b44f7 →
e67a149`), which is precisely why several early numbers had to be thrown out.
