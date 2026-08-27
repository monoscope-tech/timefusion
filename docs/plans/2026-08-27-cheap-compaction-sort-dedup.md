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

### P4 — find what broke on 08-24

A cross-tenant, cross-table file-count cliff starts exactly at 08-24 (logs
22→561, metrics 251→523) with flat daily bytes. `tf_sealed_backlog_is_flat_2026-08-24`
records the sealed backlog flat at 931→933 that same day. Not yet attributed to a
commit.

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

- **2026-08-27 23:2x** — Plan created. Diagnosis complete and evidenced (see
  above). Confirmed by reading source that Pack has neither ladder nor slicing
  nor degradation recording, while Repair and Dedup have all three. Starting P0.
