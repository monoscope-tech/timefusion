# Rollup tiers: stop the damage recurring, then clear what exists

Written 2026-08-20. Scope: the rollup tiers
(`otel_logs_and_spans_rollup_dashboard_1m_v3`, `..._1h_v2`).

## The failure, end to end

1. **HotPacking / SealedConsolidation targeted tier tables.** Packing rewrites
   files and keeps only `delta-rs.optimize.sort_by`, dropping every
   `timefusion.slice_*` / `generation` / `source_fingerprint` tag.
2. **An untagged file can never be retired.** The publish path's replace-set is
   `let Some((start, end)) = slice_tag_range(add) else { return false }` — no
   tags means not contained, means never removed. The file is **immortal**.
3. **So every later rebuild stacks a new version beside it.** Rows share an `id`
   and differ only by `updated_at`; multiplicity only ever grows.
4. **A derived tier SUMmed those versions.** It read its base with a bare
   `SELECT *`, so the 1h tier reported 157,110 requests where the truth was
   31,018 — every measure inflated by the same factor.
5. **Reads of a tier sum them too**, so dashboards over an affected day are
   inflated until something collapses the versions — which step 2 guarantees
   nothing ever will.

## What is already fixed

| cause | status |
| --- | --- |
| Packing/consolidation targeting tiers (step 1) | **Fixed** — `plan_compaction_debt` now excludes tier tables. 58 of 119 hygiene claims had been hitting them. |
| Derived tiers summing versions (step 4) | **Fixed** — `rollup_tier_dedup` + `slice_input_sql` collapse `(timestamp, id)` keep-greatest for the maintenance read. |

Evidence both hold: every 08-20 tier partition is slice-tagged, and no new
untagged files have appeared since.

**What is NOT fixed is step 2, and it is the one that makes damage permanent.**
The tag-stripper is closed today, but the mechanism that turns a stripped file
into forever-damage is untouched. The next bug that drops a tag — a new
maintenance op, a manual `optimize --table <tier>`, a delta-rs change — puts us
straight back here. That is the hole this plan closes.

## The damage that exists

Untagged (immortal) live files, from the current checkpoints:

| tier | files | projects | days | span |
| --- | --- | --- | --- | --- |
| 1m | **248** | 15 | 26 | 2026-07-17 … 08-19 |
| 1h | **104** | 13 | 17 | 2026-07-17 … 08-19 |

Concentrated at the end: the 1m tier has 111 on 08-19 alone, then 19/14/15 on
08-18/17/16. It is not a single incident — low-level stripping ran for a month
and spiked from 08-11.

---

## Phase 1 — make an untagged file retirable

**The one change that stops this being permanent.** In the publish path's
replace-set: when a unit publishes a slice spanning a whole partition day AND
produced rows, additionally remove untagged files in that `(project_id, date)`
partition.

Why it is safe, precisely:

- Files are partitioned by `(project_id, date)`, so a file in `date=D` cannot
  hold rows outside `D`. A day-wide slice reproduces all of `D` from raw.
- **Untagged files contribute zero coverage today** — `recover_rollup_coverage`
  skips any file missing any identity tag (`let Some(tags) = … else continue`).
  Removing them cannot weaken coverage; it can only remove rows that a tagged
  file now supersedes.
- The `rows > 0` guard matters: if raw has aged out and the rebuild produces
  nothing, the untagged file may be the only copy. Do not remove it then.

Restrict to full-day slices. A sub-day unit must not remove a file it only
partly reproduces.

*Verify:* an integration test that seeds a partition with an untagged file, runs
a day-wide publish, and asserts the partition ends with exactly one version per
`id`. This is the regression guard the whole plan rests on.

*Cost:* small, one function. *Risk:* low, and bounded by the two guards above.

## Phase 2 — clear the existing damage

With Phase 1 in place the repair needs **no new tooling**: rebuilding a damaged
day retires its immortal files as a side effect.

Per `(project, date)` with untagged files, oldest tier first:

```
run-unit --project <id> --source otel_logs_and_spans --date <D> --op base
run-unit --project <id> --source otel_logs_and_spans --date <D> --op derived
```

~250 base + ~104 derived units. Measured cost is ~18 s/unit (scan 12.9 s,
staging 4.4 s, commit 0.3 s), so ≈ 1.8 h serial — run it a few at a time to stay
off prod's maintenance capacity.

*Gate before fanning out:* repair ONE `(project, date)` and confirm
`versions_per_id == 1.00` for that partition. Without Phase 1 this exact check
already failed once — the rebuild published a correct 8,892-row file and the
22,505-row untagged file survived, leaving the partition unchanged. Do not skip
the gate.

*Verify per day:* `versions_per_id == 1.00` in the 1m tier, and the 1h tier's
latest-version sum within a few percent of `count(distinct id)` on raw.

**`run-unit` needs its own fix first** (already made, unmerged elsewhere):
`main.rs` forced `TIMEFUSION_BUDGET_PROFILE=maintenance-cli`, under which
`coordinator_share_bytes()` is a hard 0 — while `run_unit_once` builds its
session from `coordinator_runtime_env()`, which *is* that pool. Every invocation
died at `pool_size: 0.0 B` before reading a row. It has never worked.

## Phase 3 — make this impossible to miss again

The reason a month of stripping went unnoticed is that nothing counted it.

- **Gauge `rollup_tier_untagged_files`** per tier, and
  **`rollup_tier_versions_per_id`** (max over recent partitions). Both are cheap
  — the first is a log scan, the second is a stats read. Alarm on
  `untagged > 0`, because after Phase 1 the steady state is genuinely zero.
- **Reject rather than strip:** a commit to a `*_rollup_*` table that writes
  files without identity tags should log loudly. If any path still does it, that
  log names it immediately instead of costing another archaeology session.

This phase is what converts "fixed" into "stays fixed".

## Phase 4 — take correctness off file tags entirely (structural)

Phases 1–3 make tag loss survivable. Phase 4 makes it irrelevant, and is what
unlocks the latency goal.

The root design weakness: **identity that lives in file tags is destroyed by any
file rewrite.** Coverage recovery reads only tags, which is exactly why tiers had
to be excluded from compaction — and that exclusion is why tiers hold thousands
of tiny files and a 30d tier read costs 4.5–8.4 s for 38k rows.

The tier's rows already carry everything needed: `project_id`, `timestamp`,
`date`, `id`, `updated_at`, `rollup_generation`. And a durable rollup journal
already exists (`rollup_journal::{load,store}`, `persist_rollup_journal`).

Move coverage onto the journal (plus row-level min/max) instead of file tags.
Then:

- a tier can be compacted freely, because compaction no longer destroys proof;
- with compaction allowed, add a tier compaction that collapses
  `(timestamp, id)` keep-greatest — `rollup_tier_dedup` already expresses that
  identity — producing few large files;
- that addresses both the fragmentation and the 30d latency goal.

Sequence it last: it is the largest change, and Phases 1–3 must hold first so it
is an optimisation rather than a rescue.

## Deliberately not doing

**Declaring `dedup_keys` on the tier schema.** It looks like the obvious fix —
reads would collapse versions and nothing above would matter. It was tried: it
made every routed read plan a `DedupExec` over `id`, a column the rewrite does
not project ("DedupExec key `id` not in input schema"), and every query fell back
to a raw scan. It also taxes every tier read forever to defend against a state
that should not exist. Prefer Phase 1 (make the state impossible) plus Phase 3
(alarm if it happens) over paying per-read for it.

**Preserving tags through compaction.** Merging files from different slices
produces genuinely disagreeing identities; `carried_coverage_tags` cannot invent
one, so a packed file legitimately proves nothing. Phase 4 removes the need
rather than pretending the merge is lossless.

## Order and why

1. **Phase 1** — until untagged files can be retired, damage is permanent and
   any repair is wasted work.
2. **Phase 2** — clear the 352 files, gated on one partition first.
3. **Phase 3** — the guardrail that keeps it closed.
4. **Phase 4** — structural, and the path to the latency goal.

Phases 1–3 are days of work; Phase 4 is the real project. Steps 1 and 4 of the
failure chain are already fixed, so this is a smaller job than it looked when
the inflation was first spotted.
