# Cheap compaction, sort and dedup — so the backlog never piles up

**Goal (user, 2026-08-27):** compaction / sort / dedup must be cheap and fast
enough that the backlog never accumulates, ahead of a coming increase in message
volume. Measurably improved, not just refactored.

**Success metric.** The single number this plan moves:

> sealed rewrites that **land** per hour, vs sealed rewrites that **fail** per hour.

Baseline measured 2026-08-27 23:15 UTC on `ba6390c` (process up 24 h):

| metric | value | source |
|---|---|---|
| `Light optimize staging failed` | **5 in 90 min = 3.3/hr** | prod logs |
| `OPTIMIZE` commits | **17 in 6.1 h = 2.8/hr** | Delta history |
| `light_optimize_memory_brakes_total` | 263 | `timefusion_stats` |
| `light_optimize_bins_committed_total` | 108 | `timefusion_stats` |
| `dirty_bin_queue_depth` | 31,170 | `timefusion_stats` |
| `tasks_pending` | 6,613 | `timefusion_stats` |
| files in 08-24…08-27 | **3,779** (59% of table, 1.3% of bytes) | Delta snapshot v506587 |

**More sealed rewrites fail than land.** That is the throughput ceiling, and no
scheduling/ordering change can lift it — nothing that gets picked finishes.

---

## The diagnosis this plan starts from

Chart + evidence: `docs/dashboards/compaction-chart.html` (artifact republished
2026-08-27), memory `tf_sealed_retries_are_memory_not_deadline_2026-08-27`.

Prod fails staging with, verbatim:

```
Light optimize staging failed … table=otel_metrics: Resources exhausted:
  Additional allocation failed for SortPreservingMergeExec[0]
Light optimize staging failed … table=otel_logs_and_spans: Not enough
  memory to continue external sort.
```

Both returned `outcome=Retry` at **763 s and 121 s against a 900 s deadline** —
well inside it. **These are not timeouts.** Two prior revisions of the compaction
chart wrongly attributed them to the deadline.

### The structural finding

This codebase has hit the unspillable-merge OOM twice before and fixed it both
times with a **sort-partition ladder**, because the merge operator is
per-partition and unspillable, so at 1 partition there is no merge at all:

- `REPAIR_SORT_PARTITION_LADDER = [16, 4, 1]` — `TailPass::Repair`
- `DEDUP_SORT_PARTITION_LADDER = [2, 1]` — dedup (added 2026-08-25 for 56 looping units)

**`TailPass::Pack` — light optimize / sealed consolidation — has neither ladder
nor slicing, and a hard floor of 2 partitions:**

- `pack_sort_partitions()` (`src/database/mod.rs:8211`) ends in
  `.max(MAINTENANCE_MAX_PARTITIONS)` = **2** → always ≥1 `SortPreservingMergeExec`.
- `coordinator_slice_target(TailPass::Pack, …)` (`mod.rs:7673`) returns `None`
  unless it is a single oversized L0 file.
- The failure handler (`maintain.rs:6140`) is gated `if pass == TailPass::Repair`
  — **Pack records no degradation at all**, so it retries at the identical width
  forever.

Sealed consolidation is the one lane with no memory adaptation, and it is exactly
the lane failing 3.3/hr in prod. Same bug, third lane.

---

## ROOT CAUSE (3rd and evidenced) — repair slices are sized in COMPRESSED bytes, but the merge costs DECODED bytes (12×)

**The full memory-consumer dump settles it.** The `SortPreservingMergeExec` named
in the error is the *victim*, not the cause — it consumed **4.4 MB**:

```
ExternalSorterMerge[0]#8312043(can spill: false) consumed 3.0 GB, peak 3.0 GB,
ExternalSorterMerge[0]#8301006(can spill: false) consumed 1078.3 MB, peak 1626.5 MB,
SortPreservingMergeExec[0]#8329013(can spill: false) consumed 4.4 MB, peak 8.6 MB,
Error: Failed to allocate additional 2.1 MB for SortPreservingMergeExec[0] …
  126.2 KB remain available for the total memory pool: fair(pool_size: 4.1 GB)
```

Two **unspillable** `ExternalSorterMerge` reservations (3.0 GB + 1.08 GB) hold
essentially the entire **4.1 GB** shared light pool. A cheap 21-file sealed
consolidation asked for **2.1 MB** and there were **126 KB** left.

**Where the 3.0 GB comes from — a unit-of-measure bug.**

- The hogs are whale **Repair** units: `selected_files=1`,
  `bytes_in=910,749,060` and `1,717,176,058` — single whole files.
- Repair slices to bound memory: `want = (bytes_in / slice_target) + 1`
  (`maintain.rs:5993`), with `bytes_in = Σ add.size` = **compressed** bytes and
  `REPAIR_SLICE_TARGET_BYTES = 256 MB` — also compressed.
- But sort memory is **decoded**. This repo already knows the ratio:
  `estimated_decoded_bytes(n) = n * 12` (`maintain.rs:152`).
- So 910 MB / 256 MB + 1 = **4 slices** of ~227 MB compressed = **~2.7 GB
  decoded each** — matching the observed 2.5–3.0 GB merge to within noise.

Slicing is doing exactly what it was told; it was told the wrong unit. Every
repair slice is ~12× larger than intended, and two of them fill the pool.

**Why this starves everything.** `ExternalSorterMerge` is `can spill: false`, so
the pool cannot reclaim it. Sealed consolidation, hot packing and dedup all share
this pool and are cheap; they fail on whatever few MB they ask for next. That is
why sealed consolidation shows as the failing lane while the *cause* is Repair —
and why fixing the ordering, the fan-in, or Pack's partition count would all have
been no-ops.

**Fix:** denominate the repair slice target in decoded bytes —
`want = estimated_decoded_bytes(bytes_in) / DECODED_SLICE_TARGET + 1` — so the
intent is explicit and survives a compression-ratio change. Sizing for a merge of
~512 MB implies ~43 MB compressed per slice (≈21 slices for the 910 MB file).
Slower per unit, but bounded and it **lands**; today these units burn 763–851 s
and commit nothing.

### Refuted along the way (kept so none is re-attempted)

1. **"Give Pack a sort-partition ladder 2 → 1."** The coordinator path — ALL
   sealed consolidation — passes `runtime_env: Some(..)` and takes the
   `UncappedSort{partitions: 1}` branch (`maintain.rs:5928`). **Already at 1.**
   `pack_sort_partitions()`'s floor of 2 applies only to the cron path.
2. **"Unbounded merge fan-in from smallest-first packing (`8844064`)."**
   Plausible and timeline-matching, but **the numbers refute it**: the failing
   unit staged `selected_files=21`, not 118 (`input_files=118` in the task log is
   *candidates*, not the staged bin), and its SPM consumed 4.4 MB. A file-count
   cap would not have moved this. `8844064` may still deserve a cap as hygiene —
   demoted to P5, not a fix for this incident.
3. **Baseline metric mixes lanes.** Of 3 sampled staging failures, 2 are Repair
   (whale/otel_logs_and_spans, 3 ms before an `operation=Repair` finish) and 1 is
   Pack. Split failure counts by lane when measuring — and note the fix above is
   in the **Repair** lane even though the visible symptom is in Pack.

**`8844064` (2026-08-23 21:08 +0200) — "pack smallest-first, so one large file
cannot retire a unit empty".** It fixed a real bug: candidates arrived in
event-time order, the loop pushed the first unconditionally, so one 252 MB file
consumed the whole 256 MB budget and the unit retired having selected 1 file.

But the replacement sorts `candidates.sort_by_key(|add| add.size)` and bounds the
bin by **bytes only** (`select_coordinator_compaction_candidates`,
`src/database/mod.rs:7620`). **There is no file-count cap.** Smallest-first with a
byte budget *maximizes* files per bin: a 256 MB budget filled with 2 MB files
takes ~128 of them. The failing prod unit had **`input_files=118`**.

The staging plan merges the already-sorted per-file streams with a
`SortPreservingMergeExec` (deliberate — `maintain.rs:5966`), whose memory is
**fan-in × batch** and which is **unspillable**. So:

    files per bin ↑  →  SPM fan-in ↑  →  unspillable merge memory ↑  →
    "Resources exhausted: SortPreservingMergeExec[0]"  →  bin never lands  →
    files stay fragmented  →  MORE files per bin next pass

That is a **positive feedback loop**, which is why the cliff is a step that never
recovers rather than a slope. Timeline matches exactly: commit 08-23 21:08, cliff
begins 08-24, in **both tables and every tenant at once** — consistent with a
maintenance-path change and inconsistent with any tenant behaviour.

The commit's own test only asserts `picked.len() >= 2`. Nothing bounds the top end.

**The fix: cap files per bin.** Keep smallest-first (it fixed a real bug); add a
fan-in cap so the merge is bounded. Convergence is still monotonic — 118 files at
a cap of 16 is 8 passes, then 8 files, then 1 — and each pass is cheap and
*lands*, which is the whole point. This also directly answers P4.

**Refuted, kept so it is not re-attempted:** the original P0 ("give Pack a
sort-partition ladder 2 → 1"). The coordinator path — which is ALL sealed
consolidation — passes `runtime_env: Some(..)` and so takes the
`build_optimize_session_state_tuned(..., UncappedSort{partitions: 1, ..})` branch
at `maintain.rs:5928`. **It is already at 1 sort partition.** A ladder has nowhere
to descend. `pack_sort_partitions()`'s floor of 2 applies only to the
`light_optimize_session_state()` cron path, not to the failing lane. Also note the
baseline metric mixes lanes: of 3 sampled staging failures, 2 are `Repair`
(whale/otel_logs_and_spans, 3 ms before an `operation=Repair` finish) and only the
otel_metrics one is Pack. Split failure counts by lane when measuring.

## Ordered work list

Ranked by (measurable throughput gain) / (risk × effort). Ship one change per
deploy behind a kill switch where behaviour changes.

### P0 — give `TailPass::Pack` a sort-partition ladder  ⬅ IN PROGRESS

The established fix, third application. Descend 2 → 1 on retry so the
unspillable merge disappears entirely.

- Level source must be the **persisted `attempts`**, not an in-process map.
  Prod replaces the process every 15–28 min, so `repair_degradation`-style maps
  are lost with it and units looping at attempts 8–15 keep retrying at the width
  that just failed (this is documented at `DEDUP_SORT_PARTITION_LADDER` and is
  the bug that ladder ended).
- `attempts` is **POST-CLAIM**: a first-ever run arrives as `attempts == 1`, so
  index with `saturating_sub(1)`. Indexing directly would put every first-ever
  pack on the floor and halve fleet sort parallelism.
- verify: unit test on the pure fn (first run → 2 partitions, retry → 1);
  then prod `Light optimize staging failed`/hr must fall.

### P1 — slice a Pack bin by event time when it is too big to sort whole

`coordinator_slice_target` returns `None` for multi-file Pack bins. Repair
slices at 256 MB. A sealed-consolidation bin that cannot sort whole should
slice rather than fail — slices feed ONE writer in sort order, so the output
stays globally sorted and footers stay honest (mechanism already exists and is
row-count verified).

### P2 — stop paying for sorts that cannot help

Whale 07-20…08-02 (1,082 files / 760 GB) is at 0.5–0.8 GB/file against a
**512 MB** sealed target — already **above** target. Rewriting it *adds* files.
It has been bit-identical for 8 days while absorbing claims. Exclude
already-at-or-above-target partitions from candidacy outright.

### P3 — rank sealed candidates by files-removed, not age

`maintenance_hygiene_debt_unclaimed` fires every ~90 s:
`refusal="outranked_by:8100121c:2026-08-22:…:28f62f01:2026-08-26:files=294"`.
SealedConsolidation is **one lane shared across tables**, so an otel_metrics cell
from 08-22 outranks the 294-file otel_logs_and_spans cell from 08-26. Rank by
estimated files removed, keep a starvation floor. **Do P0/P1 first** — ordering
cannot help while nothing that gets picked finishes.

### P3b — the cron light-optimize lane is brake-stopped more often than it commits

`light_optimize_memory_brakes_total` = **263** vs `light_optimize_bins_committed_total`
= **108**. The brake is `process_memory_bytes() > memory_brake_limit_bytes()`
(prod `limit=68719476736` = **64 GB** RSS against the 120 GB cgroup), and it
returns `Brake::Stop`, which ends the tick. Prod logs show **10 brakes inside
5 ms** (23:10:00.986–.991) — the per-project loop hitting the same brake once per
project, so a whole tick's planning is done and thrown away.

Two separate questions, don't conflate them:
1. *Why is RSS > 64 GB?* Known, long-running ([[tf_oom_peak_anon_back_to_124gb_2026-08-20]]).
   The 3–4 GB unspillable repair merges are a contributor the P0 fix should
   reduce — **check whether brake count falls after the deploy before doing
   anything here.**
2. *Should one brake stop the whole tick?* Stopping per project re-plans from
   scratch next tick. A brake that pauses rather than abandons would keep the
   planning work.

### P3c — the STRUCTURAL repair debt: 917 whole-file sorts, 745 GB

Measured 2026-08-28 while the P0 deploy was building. This is the reason repair
holds the pool for so long, and it is a standing condition, not an incident.

- **917 active files exceed 512 MB, totalling 745 GB.** Each is one repair unit
  (`coordinator_compaction_files` does `.take(1)` for Repair), i.e. one
  whole-file sort.
- Sampling one large file per date across 12 dates: **11 NONE, 1 DECLARED, 0
  errors** — they genuinely carry no declared sort order, so repair is right to
  target them. (`ERROR` was tallied as its own bucket and never folded into
  `NONE` — see `tf_ovh_checksum_breaks_pyarrow_footers_2026-08-19`.)
- Whale 07-28 alone is 104 files / 86.6 GB with individual files at **1.6–1.9 GB**.
  The 1717.2 MB file is exactly the `bytes_in=1717176058` repair keeps re-slicing.

At the new sizing a 1.2 GB file is ~14 GB decoded ≈ 15 slices. 917 of these
cannot drain against a 900 s deadline while sharing one pool with every other
lane — even bounded, repair will occupy maintenance for a very long time.

**The strategic question this raises** (do NOT act before the P0 measurement):
historical repair competes directly with compaction of *recent* days, which is
what queries actually read. Candidates, in order of preference:
1. **Pool isolation** — give repair a bounded share so it can never starve the
   lanes serving recent data. Principled, and directly serves "the backlog never
   piles up".
2. **Age-deprioritise repair** — a 07-28 file's sortedness benefits few queries;
   recent days benefit many.
3. **Off-box** via `timefusion optimize --recompress`.

If P0 alone brings repair's footprint from 3 GB to ~1 GB and the other lanes
drain, this may not need doing at all. **Measure first.**

**Retracted, my own broken probe:** an earlier pass here reported "0 of 6,385
files carry tags", i.e. every file repair-eligible. That was an artifact —
`deltalake`'s `get_add_actions(flatten=True)` does not expose a `tags` key at
all, so `.get("tags")` returned `None` for every row. The uniform extreme across
100% of rows is the tell ([[tf_uniform_extreme_means_broken_probe_2026-08-19]]).
Reading tags needs the `_delta_log` directly. The **footer** results above are
unaffected — those came from pyarrow reading real footers, and they succeeded.

### P4 — find what broke on 08-24

A cross-tenant, cross-table file-count cliff starts exactly at 08-24 (logs
22→561, metrics 251→523) with flat daily bytes. `tf_sealed_backlog_is_flat_2026-08-24`
records the sealed backlog flat at 931→933 that same day.
**Still unattributed — do not close it on mechanism resemblance.**

Two candidates, neither confirmed:

1. **Deploy density, not any single commit.** `git log --since=2026-08-22
   --until=2026-08-25 -- src/database/ src/maintenance_coordinator.rs` returns
   **30+ commits on 08-24 alone**. Every push restarts prod, and units average
   ~21 min and die to process exit rather than to re-claiming
   (`tf_units_die_to_restarts_2026-08-23`, `tf_deploy_cadence_starves_dedup_2026-08-18`).
   A day of continuous deploys is a day in which no long unit completes — which
   would fragment 08-24 — and the pool starvation above is then sufficient to
   explain why it never recovered. Fits the "step, not slope" shape without
   needing a logic defect.
2. **`3465ecc` "a SealedConsolidation unit is never the live frontier"** — on-topic,
   landed 08-24, unexamined.

Both predict the cliff in both tables, so cross-table evidence does not separate
them. The discriminator is when the whale Repair units were first enqueued —
a journal query, not a log grep.

---

## Guardrails (from CLAUDE.md + hard-won memory)

- **Bug-fix workflow is mandatory:** failing test FIRST, at the level closest to
  the bug, asserting the specific symptom. No skipping because a fix looks obvious.
- Targeted tests while iterating (`cargo nextest run --lib <substring>`,
  `cargo check --lib`); full `make test` + `cargo lint` only pre-push.
- **Never `cargo test`** — always `cargo nextest run`.
- **Shared checkout**: another session edits this tree. Commit the moment it
  compiles; verify/push from a detached worktree.
- Prod is the LAST resort, not the loop: `timefusion sim` → `run-unit` → staging
  → prod. One change per deploy, ≥2 h quiet before trusting numbers.
- Any non-docs push **restarts prod** — and a restart resets process-scoped
  counters and kills in-flight units, so it invalidates measurement for ~2 h.
- Don't trust a zero from `timefusion_stats` without grepping `pg_compat.rs` for
  the key — stats keys are two hand-maintained lists.

---

## Log

- **2026-08-27 23:2x** — Plan created. Two diagnoses raised and both refuted (see
  "Refuted along the way"). Third diagnosis — repair slices denominated in
  compressed bytes — confirmed by the reservation dump and by prod logging
  `slices=4` for `bytes_in=910749060` and `slices=7` for `1717176058`, exactly as
  the arithmetic predicts.
- **2026-08-27 23:4x** — `caf7963` (docs) and **`93cd806` (the fix)** committed.
  `REPAIR_SLICE_TARGET_BYTES` (256 MB compressed) → `REPAIR_SLICE_DECODED_TARGET_BYTES`
  (1 GB decoded), `repair_slice_want()` extracted so the formula has one home,
  Pack's oversized-L0 path re-denominated through it and unchanged in effect.
  Tests: 5 pass, including the invariant `decoded_per_slice <= target` on the four
  exact prod `bytes_in` values.
  - Note: `a_repair_slice_splits_big_files_but_leaves_small_ones_alone` kept its
    **own copy** of the arithmetic, so it passed while production was wrong by
    12×. It now calls the real function. Watch for this pattern elsewhere.

- **2026-08-28 00:0x** — Gate green: `cargo lint` clean, `make test` **1227/1227
  passed** (232 s). Pushed `ba6390c..eadffa1` from a **detached worktree** pinned
  at the tested commit, so a concurrent session could not alter what shipped.
  Deploy triggered. Baseline for comparison is the table at the top of this file,
  taken on `ba6390c` with 24 h uptime.

- **2026-08-28 00:09:50** — `eadffa1` live on prod. **First 25 minutes, new build:**

  | signal | before (`ba6390c`, 24 h uptime) | after (25 min) |
  |---|---|---|
  | `Light optimize staging failed` | 3.3–4.8 /hr | **0** |
  | `Resources exhausted` | continuous | **0** |
  | BaseRollup `Complete` | — | **44** |
  | Dedup `Complete` | — | **21** |
  | `repair_bin_sliced` | every 5–15 min | **0** (see below) |

  **Do not read this as confirmation yet — two reasons.**
  1. **The process is 25 minutes old.** Comparing it to a 24 h process is the
     exact trap in [[tf_young_process_reads_as_fixed_2026-08-23]]; accrual has
     read 0 → 124/hr on age alone. The ≥2 h quiet window still applies.
  2. **Prediction 1 is UNVERIFIED.** Repair has not sliced anything, because all
     20 of its units returned `outcome=Retry` at **`ran_secs=0`** — the
     `repair_bin_already_sorted` / `take(1)` short-circuit. It has not yet
     reached one of the 1.6–1.9 GB whale files, so `slices=11` is still untested.
     The *effect* (no exhaustion, other lanes completing) is visible; the
     *mechanism* is not. Until a big-file repair slices, an alternative
     explanation — that the restart alone cleared the retry storm — is not
     excluded.

  Also note the first watcher window spanned the restart and returned
  `bytes_in=722216602 slices=3`, the OLD value. Timestamping showed every such
  line predated 00:09:50. Any post-deploy claim must filter on timestamp.

### Predictions this fix makes (check these, in this order)

| # | prediction | falsifies if |
|---|---|---|
| 1 | repair logs `slices=11` for `bytes_in=910749060` (was 4) | still 4 → fix not deployed |
| 2 | `ExternalSorterMerge` peaks fall from 3.0 GB to ~1 GB | stays ~3 GB → wrong constant path |
| 3 | `Light optimize staging failed` for **non-Repair** lanes → ~0, **with zero changes to sealed code** | persists → diagnosis incomplete ⬅ THE falsifier |
| 4 | 08-24/25/26 file counts fall on the next chart pull | flat after ≥2 h → sealed lane has a second blocker |

**Predicted residual, not a refutation:** the 1.7 GB file becomes ~21 slices and
may still exceed the 900 s deadline. Bounded and next in line — and even then it
no longer pins 3 GB of unspillable pool, so the other lanes drain regardless.

**Do not retune from the measured ~73 s/slice.** That number was taken *inside*
the failure mode: 2.7 GB slices thrashing a pinned pool, where the spill-merge is
both the memory hog and much of the latency. A 1 GB slice against a ~2 GB fair
share should mostly not spill. Treat 73 s as a ceiling, not an expectation.

## P0 RESULT — measured at 54 min on `eadffa1` (01:04 UTC)

**The sealed lane is draining again.** The strongest evidence is Delta state,
which no restart can reset:

| partition | pre-deploy | at 54 min | Δ |
|---|---|---|---|
| logs 08-24 | 561 | **515** | **−46** |
| logs 08-25 | 964 | 964 | 0 |
| logs 08-26 | 736 | 736 | 0 |
| logs 08-23 | 22 | 22 | 0 (converged) |
| metrics 08-24 | 523 | **480** | **−43** |

08-24 is the oldest unconverged day, so oldest-first draining it and not yet
touching 08-25/26 is the expected shape. **Before this, 08-24/25/26 had not moved
for days.**

Process counters, same window (54 min vs a 24 h baseline — rates only):

| metric | before | after |
|---|---|---|
| `Light optimize staging failed` | 3.3–4.8 /hr | **0** |
| `Resources exhausted` | continuous | **0** |
| `light_optimize_memory_brakes_total` | 263 / 24 h ≈ 11/hr | **0** |
| `light_optimize_bins_committed_total` | 108 / 24 h ≈ 4.5/hr | 10 / 54 min ≈ **11/hr** |
| `tasks_pending` | 6,613 | **6,112** |
| `pending_sealed_consolidation` | 142 | **123** |

**Verdict against the predictions:**
- ✅ **3 (THE falsifier)** — non-Repair staging failures → 0 **with zero changes to
  sealed code**. It survived.
- ✅ **4** — 08-24 counts fell.
- ✅ **2, indirectly** — zero `Resources exhausted` means no merge is over budget.
- ❌ **1 STILL UNVERIFIED** — repair has not sliced once in 54 min (it sliced every
  5–15 min before), because every Repair unit returns `Retry` at `ran_secs=0` via
  the already-sorted short-circuit. `slices=11` is proven by the pure-fn test but
  **not observed on prod**. Honest reading: the *effect* is confirmed; the
  *mechanism* is inferred. Since P3b's brake also went to 0, the pool genuinely
  has headroom it did not have — consistent with the diagnosis, not proof of it.

**What this does NOT fix.** At ~46 files/hr, the remaining 2,215 files across
logs 08-24/25/26 need **~48 hours** to drain. That is precisely the ceiling
change 2 addresses, and this measurement is the argument for shipping it.

## Change 2 — PUSHED as `665f95e` (01:16 UTC)

Gate: `cargo lint` clean; `make test` 1227/1228 with the single failure being a
**MinIO connection error** (`error sending request`) — a concurrent session was
creating `timefusion-kill-*` buckets against the same MinIO during the run. Not
an assertion failure. Re-ran the test in isolation (pass) and the whole
`dedup_compaction_test` module, nearest my change (**55/55 pass**).

Rebased onto `0d88e9b`, the autofmt workflow's reformat of change 1 — inspected
it first and it only reflowed long lines, no logic change. Ran `cargo fmt` myself
before pushing so the workflow does not land a follow-up commit and a second
restart (`cargo lint` does not catch rustfmt —
[[tf_push_gates_and_deploy_triggers_2026-08-15]]).

### CHANGE 2 RESULT — confirmed at 5 min (01:34 UTC), live 01:29:14

Post-deploy `wave_bin_staging_started`:

| bin type | baseline | after |
|---|---|---|
| contains unsorted files | 3–7 files, 0.4–13.2 MB | **15 files / 47.8 MB**, **20 files / 29.8 MB** |
| all sorted runs | 21–24 files, 260–266 MB | 24 files / 263 MB (unchanged, as designed) |

Both new bins sit under the 64 MB compressed budget, so the cap is doing what it
should rather than being unbounded. **~3–4× more files retired per unit**, and
the all-sorted path is untouched.

`Light optimize staging failed` strictly after the new process started: **0**.
Two failures at 01:29:00 belong to the *old* process being terminated mid-unit by
the deploy (the `foyer close` line follows them) — deploy artifacts, not a new
failure mode. Always filter on the process start, not the log window.

### DRAIN STALLED at 47 min — and it is NOT change 2

logs 08-24/25/26 moved **0** files in the 47 min after change 2 (metrics: −1),
against −89 in the P0 window. Investigated before touching anything:

- **Change 2 is exonerated.** The new unsorted bins are 15–29 files / 0.2–62 MB,
  all inside budget; permit waits are **0.0 s**; memory failures are **0**. No
  mechanism links it to the stall.
- **All six 900 s units are `otel_metrics`** — projects `00000000` and
  `8100121c`, slices **08-17, 08-18, 08-19** — finishing `outcome=Some(Running)
  ran_secs=900`, i.e. abandoned at the deadline having committed nothing. Not one
  is `otel_logs_and_spans`. **The logs bins are never reached because the shared
  sealed lane is pinned by old metrics cells.**
- **Refuted en route:** the tag/footer disagreement theory (that all-sorted bins
  silently fall back to a whole-bin sort because OPTIMIZE strips tags). Probed
  the 10 largest files in those exact cells: **10/10 `DECLARED`, 0 errors.** They
  really are sorted; the cheap streaming SPM should apply.
- **Also checked and dismissed:** a second restart. The container ID changed
  between samples, but `docker service ps` shows `df2vdc2…` = `665f95e` up 47 min
  — the other ID was the old `eadffa1` container.

**The blocker, named by our own instrument:**

```
SealedConsolidation refusal="outranked_by:00000000:2026-08-20:28f62f01:2026-08-27:files=433"
```

The most indebted cell in the fleet — **28f62f01 / 08-27, 433 files** — is
outranked by a small, nearly-converged 08-20 cell, and the metrics cells at
08-17/18/19 are older still, so they win the lane and burn 900 s each.

**This promotes P3 (rank by benefit, not age) from "later" to THE measured
blocker.** P0 removed the memory ceiling and change 2 raised per-unit yield ~4×;
what remains is that the lane is pointed at the wrong work. It is the same defect
the 08-20 and 08-22 chart revisions both named and neither fixed — now with an
instrument that prints the victim and the winner on every tick.

**Second, separate defect to keep:** a metrics cell that cannot finish in 900 s
is a bug regardless of ordering — 861 files / 13.5 GB across 08-17…19, median
13.4 MB, all sorted, yet a ~263 MB bin grinds the full deadline. Ordering will
stop it *monopolising* the lane but not make it complete. Needs its own
investigation (candidate: cost is the commit, not the sort —
[[tf_rollup_unit_cost_is_the_commit_2026-08-19]]).

**Calibration:** drains are lumpy (one commit retires 20–46 files), so 0-in-47-min
is only ~2 missed commit events and weak alone. The **six Running@900
abandonments** are the real signal.

### (history) Change 2 while it was held

**Held deliberately.** Pushing restarts prod and resets every counter, which would
end P0's clean measurement window and make P3c's "may not need doing at all"
unanswerable. Order is fixed: measure P0 at ~02:10 UTC → record → then push.

**The same bug class, found by looking for it.** A bin containing any unsorted
file was capped at `COORDINATOR_L0_SORT_TARGET_BYTES` = **16 MB compressed**
(~192 MB decoded) against a 4.1 GB pool. Prod 2026-08-28, staged bins are bimodal:

| bin | files retired | bytes |
|---|---|---|
| contains an unsorted file | **3–7** | 0.4–13.2 MB |
| all sorted runs | **21–24** | 260–266 MB |

The sorted-run tag is written only by OPTIMIZE (the selector's own comment says
it "effectively never ran"), so a bin over fresh flush output essentially always
takes the capped path. **The cap taxed exactly the fragmented partitions that
most need compaction** — 08-25 holds 964 files in 3.55 GB and drains ~4 per unit.

Adds `UNSORTED_BIN_DECODED_BUDGET_BYTES` = 768 MB decoded (64 MB compressed),
sized from the same `budget × concurrent sorts ≤ ~half pool` invariant and kept
at or below the repair slice budget so a bin can never out-reserve the heavier
lane. `DECODED_BYTES_PER_COMPRESSED` now names the conversion.

`COORDINATOR_L0_SORT_TARGET_BYTES` is **left alone on purpose** — it also gates
single oversized L0 files into the slicing path, and raising it in place would
silently stop slicing 20–80 MB files. That double duty is the trap here.

**Prediction:** bins containing unsorted files go from 3–7 files / ≤16 MB to
~15–40 files, so sealed drain per unit rises ~4–5×. Self-limiting by design:
outputs carry `SORTED_RUN_TAG`, so second-pass bins graduate to the 256 MB path —
the cap mainly taxed the *first* pass over fresh flush output.

**Hygiene noted, not bundled:** the `has_unsorted → limit` line is where a
file-count cap would go if merge fan-in ever needs bounding (the refuted
diagnosis #2). Not needed on current evidence.

## WHERE THE NIGHT ENDED (02:30 UTC) — read this first in the morning

**Two fixes shipped and both hold. The remaining blocker is named, instrumented,
and NOT the thing I would have guessed.**

### Shipped and verified

| | change | verified by |
|---|---|---|
| `93cd806` | repair slices sized in decoded bytes | 08-24 drained **561→515** (logs) and **523→480** (metrics) — first movement in days; staging failures 3.3–4.8/hr → **0**; memory brakes 11/hr → **0** |
| `665f95e` | unsorted-bin budget in decoded bytes | bins **3–7 → 15–45 files**, mean 20.5; all-sorted path unchanged |

The unspillable merge peak fell **3.0 GB → 1.46 GB**, and pool use went from
4.08/4.1 GB to ~1.7/4.1 GB. That is prediction 2, confirmed.

### The blocker now: SealedConsolidation is pinned by otel_metrics

- **HotPacking is healthy.** logs bins of 28–45 files stage in **1–46 s**.
- **Every 900 s unit is `otel_metrics`** (08-17/18/19), finishing
  `outcome=Running` having committed nothing. Its bins *start* staging and never
  emit `wave_bin_staged`.
- Sealed logs 08-24/25/26 therefore got **0 capacity** and drained 0 since 01:29.
- **P0 changed the failure shape here:** these metrics units used to die fast on
  memory (121–763 s) and free the lane between attempts; now they grind the full
  900 s. Same pinning, more expensive per attempt. Not a regression to revert —
  the lane was pinned before too — but it is why the drain plateaued.

### Why the metrics bins grind — NOT yet answered

Ruled out tonight, each with evidence:
- **Not tag/footer disagreement.** Probed the **smallest** files (what the
  selector actually picks — my first probe wrongly sampled the largest):
  **23/23 `DECLARED`, 0 errors**, and my reconstructed bin (23 files, 260.8 MB)
  matches the staged one exactly.
- **Not memory.** 0 staging failures; the pool has headroom now.
- **Not permit contention.** `permit_wait_ms` = 0.0 s on every bin.
- **Contributory, not sufficient:** metrics rows are **47 bytes** vs logs' **155**,
  so a 260 MB metrics bin is **5.58 M rows / 21,786 batches** at the coordinator's
  `batch_override = Some("256")`, against logs' 1.73 M rows / 6,740. A 3.2×
  batch-count difference does not explain a ~300× time difference. **Next
  hypothesis to test: cold object-store reads** — these are 9–11 day old
  partitions, so foyer is cold, while the fast logs bins are today's data.

### Do NOT do these without the stated precondition

1. **Do not revert either shipped fix.** No evidence against them; both measured.
2. **Do not change the rank tuple without a `timefusion sim` backtest.** The sim
   exists (`timefusion sim <journal.json|data-dir|synth:whale> [--hours N]`).
   Blocked tonight only because the journal lives in the container data dir and
   fetching it needs a `docker cp`, which crosses the read-only prod boundary.
   **Fetch the journal first, backtest, then change ordering.** The rank tuple's
   own doc comments are a graveyard of ordering regressions.
3. **Do not "fix" ordering by adding benefit ranking — it already exists.**
   `scheduling_class` computes `benefit = -files/BENEFIT_BUCKET_FILES(64)` for
   SealedConsolidation/HotPacking/Repair. The defect is upstream: the tuple is
   `(class, damaged, starved, hole, width, benefit, order)` and **`starved` is a
   pure age window `[3 days, 31 days]`** that sits before benefit. So
   `28f62f01/08-27` with **433 files** (1 day old → `starved=1`) loses to
   `00000000/08-20` with ~4 files (8 days old → `starved=0`). Also note **`width`
   precedes `benefit`**, and the comment justifying that ("every hygiene unit is
   day-wide") is false — the 8100121c unit traced tonight was a **12 h** slice.
   Both need to be part of any redesign.

### Measurement discipline for the morning

A push **restarts prod**, which resets process-scoped counters and kills the
current retry storm (costing nothing — none of it was landing). Wait **≥2 h**
before trusting any counter, and check `docker service ps` uptime first.
Split staging-failure counts **by lane**: the fix is in Repair, the visible
symptom was in Pack.
