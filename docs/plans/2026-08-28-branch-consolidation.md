# Branch consolidation — finish or delete every abandoned branch

2026-08-28. Baseline `origin/master` = `f7be1f74`. **33 branches** are ahead of it.
This plan converts each into one of three terminal states: **landed**, **deleted**,
or **explicitly parked with a reason**. No branch stays in limbo.

## The two facts that structure everything

**1. `integration-2026-08-25` is a superset of eight branches.** Its history contains,
*by exact SHA*, every commit of `fix-wave-discard`, `resume-process-identity`,
`p0a-dedup-sort-ladder`, `worktree-agent-a73bca5d623a0f0f6`,
`worktree-agent-a860ca57ad5e1a0f1`, `worktree-agent-a984cc9333b5af8db`, and a
subject-identical rebase of `resume-behaviour-test`. Landing it retires eight
branches in one action. It is the single highest-leverage move in this plan.

**2. There is a hard refactor boundary, and it is not where `CLAUDE.md` says.**
`origin/master` has `src/database/{mod,write,compact,maintain}.rs`,
`src/maintenance_coordinator.rs`, `src/maintenance_sim.rs`, `src/rollup.rs`,
`src/read/*`, `src/tantivy/*`. It does **not** have `src/database.rs`,
`src/tantivy_index/`, `src/hot_tier.rs`, `src/metrics.rs`, `src/dirty_bin_queue.rs`
— nor `src/database/rollup.rs`, `src/database/scan.rs` or `src/maintenance/`, which
`CLAUDE.md` documents but which do not exist. **Classify from `git ls-tree
origin/master src/`, never from the module map.** A branch touching a dead path is
*pre-split*: it cannot be rebased, only ported.

## Method: the rebase is the triage (post-split only)

For post-split branches, `git rebase origin/master` drops patch-equivalent commits
by itself. Whatever survives is genuinely unlanded; whatever vanishes is proven
landed. **This replaces symbol-grepping entirely, and it must.** Two grep passes
during triage produced false positives — `\b`-alternation reported
`scheduling_width` as landed when it is absent, and a fixed-string pass matched
commit-message prose and DataFusion node names (`SubqueryAlias`) that master
references for unrelated reasons. Content-comparison judgment is reserved for
pre-split branches, where rebase cannot go empty even when the work landed.

## Merge trains, not merges

Every non-docs push to master restarts prod; a restart kills in-flight ~21-minute
maintenance units and resets every `timefusion_stats` counter, and the 08-28
sortmark verification still wants quiet uptime. Landing 15 branches as 15 pushes is
a self-inflicted outage pattern. **Batch phases 1–3 into at most 1–2 pushes per
day.** Gates (`cargo lint` + `make test`, plus `make test-e2e` once per train) run
per *train*, from a detached worktree per the shared-checkout discipline.

---

## Progress — 2026-08-28

**Done.** Phase 0 delete. Phase 1 rebase of `integration-2026-08-25` onto
`f7be1f74`: 14 of 15 commits survived, `ec54155e` (dedup sort ladder) dropped as
already-landed — so `p0a-dedup-sort-ladder` was **half** landed, which no grep would
have told us. Three conflicts, each a real evolution rather than noise:

1. `src/observability.rs` / `src/server/pg_compat.rs` — the six new resume counters
   are genuinely absent from master, but master's `atomic_stats!` macro now
   generates `stats_rows()`, so the commit's hand-written `pg_compat` half is
   **obsolete and was dropped**. That is the macro doing exactly what it was
   introduced to do.
2. `src/rollup.rs` sampler — master still had the global 1-in-512
   `sample_rollup_miss()`, so the per-reason version landed. **This retires
   `fix/rollup-miss-sampler-per-reason`.** Master's neighbouring comment asserting
   "1-in-512 across ALL misses" was updated; it would otherwise have become a stale
   doc, the exact failure mode recorded in `tf_sortedness_known_at_write_time`.
3. `src/rollup.rs` walker — master had added a *diagnostic* `SubqueryAlias` arm that
   names what it refuses. The commit adds a *walking* arm. The walking arm wins and
   the diagnostic arm is deleted, because the inner refusal now propagates through
   `?` — strictly better: it names the node that actually blocked, not the alias
   wrapping it.

Gates: `cargo lint` **0**, suite **1267/1270**. All three failures were master's own
`an_unwalkable_shape_…` case table asserting pre-widening behaviour — one case was
literally labelled *"the one shape a widening could reach"*. Fixtures retired in
`5c4638e2`; no routing was loosened.

**Not done — `worktree-agent-a4dd3af67abdf7806` cherry-pick ABORTED, deliberately.**
`bf831539` gates bisection on `ran_micros >= deadline/2`. Master has since solved
the *same* prod incident (2026-08-22/23 spec-change failures bisected to the floor)
with a **better discriminator**: positive evidence of a deterministic schema failure
(`abandon_running(key, now, failure: Option<&str>)`). The residual gap is real —
a unit that fails fast for a *non-schema* reason is still bisected — but closing it
means a 4-arg signature and ~8 call sites, which does not belong inside an already
14-commit train. **Moved to Phase 3.**

Also corrected: `9700d050` is *not* an ancestor of master, but its patch-id is
(`git cherry` = 0), i.e. it landed under a different SHA. `wip/revert-and-fixture`
is patch-identical to `hold/schema-evolution` minus that commit → deleted as a
duplicate; the revert question survives in `hold/schema-evolution` alone.

**A stale comment on master nearly cost us a real fix — the exact trap this plan's
method section warns about.** `src/database/maintain.rs:809` states the backfill
pass "aims them at the worst project's earliest hole", i.e. contiguity ordering.
It does not. `admit_backfill_pass` sorts **forced-first, then newest-date-first**,
and 30 lines further down master documents that choice deliberately ("Newest first:
recent days are what dashboards actually read"). So `5a139c67`
(`backfill_cells_by_contiguity`, from the salvage cluster) is **genuinely unlanded**,
and it is the fix for `tf_horizon_frozen_by_newest_first`. Landing it is not a
mechanical port — it *contradicts* a rationale master states on purpose — so it
needs a decision, and it is promoted to its own Phase 3 item. Its sibling
`eef020a3` (coverage-short bypasses the ceiling) **is** landed, as
`pending >= BACKFILL_PENDING_CEILING && !coverage_is_short()`.

Deleted so far (9): `worktree-agent-addf7c8a238435a91`,
`fix/count-dedup-and-rollup-overcount`, `fix/rollup-split-uncovered-projects`,
`port/rollup-dedup-fixes`, `worktree-agent-a183d83cdeaaa461a`, `distill-seg1-5`,
`work/distill-storage`, `wip/revert-and-fixture` — the first four verified by
distinctive test-fn names present on master (`slice_input_sql`,
`an_uncertified_window_is_never_granted_the_dedup_skip`,
`an_uncovered_project_reads_raw_while_the_others_still_route`), the last two because
they refactor files master no longer has.

## Phase 0 — hygiene (no gates needed)

- [ ] Delete `worktree-agent-addf7c8a238435a91` — `git cherry` says **unmerged=0**;
      its commit `9700d050` is in master. Proven, not inferred.
- [ ] Sweep dirty worktrees (`tf-cert`, `tf-e2eflake`, `bisect-prune`,
      `gate-combined`, `precedence-fix`): commit, stash, or discard each.
- [ ] Prune stale worktrees + their `CARGO_TARGET_DIR`s. **Disk checked: 7% used,
      165 Gi free — not a constraint today.** (An earlier 81% reading was a different
      volume; a full disk fakes suite failures and would poison every gate below.)

## Phase 1 — the centerpiece: land `integration-2026-08-25`

- [ ] Rebase `integration-2026-08-25` onto `origin/master` in a scratch worktree.
      Record which commits drop — that is the proof-of-landing for its subsumed set.
- [ ] Cherry-pick `bf831539` from `worktree-agent-a4dd3af67abdf7806`. Integration
      carries only *half* of it (`144e184b`, the hour-migration half); the
      **bisect-on-wall-time-evidence half is verifiably absent** from both
      integration and master.
- [ ] Diff `fix/rollup-miss-sampler-per-reason` (`84a7d70d`) against integration's
      `554d..` commit — same subject family. If equivalent, it retires here too.
- [ ] Gate + land. **Retires: 9–10 branches.**

## Phase 2 — small post-split independents

- [ ] `ci/local-run-attestation` — **do this first in the train.** `CLAUDE.md`
      already documents `ci/checks.tsv` and `scripts/ci/ci.sh` as missing from this
      branch; landing it makes `make ci` work and its attestation cache speeds every
      later gate. 10 files, no `src/` changes.
- [ ] `fix/tantivy-reconcile-progress-logging` — 1 file, 17 lines, post-split.
- [ ] `lever2diag2` — 1 file, `src/database/mod.rs`, post-split.
- [ ] `hold/schema-evolution` — its `9700d050` is already in master (see Phase 0),
      and `wip/revert-and-fixture` is a **subset** of it. One branch to evaluate, two
      to delete. Re-check whether the row-span *revert* is still wanted against
      today's master: `bf8b9a16` (the MemBuffer→Delta mask fix) touched the same
      territory.
- [ ] `docs/measured-rollup-gap` — docs-only, so its push does **not** restart prod.
      Safe to land out-of-train.

## Phase 3 — pre-split ports, by value

- [ ] **`fix/split-inherits-backfill-priority` — highest value.** *"A split narrows
      the work, not the priority."* Maps directly onto the live 08-28 finding that
      `benefit` is per-unit, so splitting a day divides its debt. `scheduling_width`
      and `backfill_priority_micros` are both confirmed absent from master (verified
      twice, by fixed string). Port to `src/database/*` + `src/maintenance_coordinator.rs`.
- [ ] `fix/tier-merge-slice-set` — `TAG_MERGED_SLICES` / merged-slice-bounds-as-a-SET.
      Self-contained tag encoding.
- [ ] Salvage cluster `{simnew, simtest, fix/precise-restart-reconcile}` — all three
      share `25242120` (touched-hours reconcile), `5a139c67` (contiguity-aimed
      backfill), `eef020a3` (coverage-short ceiling bypass). **One evaluation, three
      deletions.** `fix/precise-restart-reconcile` is otherwise the module-split
      itself (114 files) and is unmergeable.
- [ ] `tier1-14d-30d-complete` — verify its four features **individually**: per-query
      byte ceiling, cancellable long-running operators, the null-guard raw-fringe
      fix, the null-guard-is-a-no-op fix. Land what is absent, drop the rest.

## Phase 4 — verify then delete

Spot-check **one hunk each** against master, then delete:
`port/rollup-dedup-fixes`, `worktree-agent-a183d83cdeaaa461a` (both are the module
split, which landed), `fix/count-dedup-and-rollup-overcount`,
`fix/rollup-split-uncovered-projects`.

Delete with a note, no check needed: `distill-seg1-5` and `work/distill-storage` —
refactors of files (`src/database.rs`, `src/hot_tier.rs`) that no longer exist.

Per-commit triage, expect to drop most: `fix/dml-bounded-key-pushdown` (12d, half
`src/tantivy_index/`), `tantivy-cache-gc` (4 weeks, `src/metrics.rs` +
`src/tantivy_index/`).

## Phase 5 — needs a decision, do not auto-merge

- [ ] `cleanup/remove-grpc` — removes gRPC ingest as "unused". **That premise may be
      stale:** the 08-04 finding is that OVH's conditional-PUT behaviour routed
      ingest through the gRPC path. Ask before deleting the interface.
- [ ] `cleanup/docs-triage` — a 48-file docs deletion, 8 days old against heavy docs
      churn. Ask, or regenerate the triage against today's `docs/`.

## Ledger

| State | Branches |
|---|---|
| Retired by Phase 1 | integration-2026-08-25, fix-wave-discard, resume-behaviour-test, resume-process-identity, p0a-dedup-sort-ladder, w-a73bca5d, w-a860ca57, w-a984cc93, w-a4dd3af6, fix/rollup-miss-sampler-per-reason |
| Landed in Phase 2 | ci/local-run-attestation, fix/tantivy-reconcile-progress-logging, lever2diag2, hold/schema-evolution, docs/measured-rollup-gap, (delete wip/revert-and-fixture) |
| Ported in Phase 3 | fix/split-inherits-backfill-priority, fix/tier-merge-slice-set, salvage of {simnew, simtest, fix/precise-restart-reconcile}, tier1-14d-30d-complete |
| Deleted in Phase 4 | port/rollup-dedup-fixes, w-a183d83c, fix/count-dedup-and-rollup-overcount, fix/rollup-split-uncovered-projects, distill-seg1-5, work/distill-storage, fix/dml-bounded-key-pushdown, tantivy-cache-gc |
| User decision | cleanup/remove-grpc, cleanup/docs-triage |
| Deleted in Phase 0 | worktree-agent-addf7c8a238435a91 |

Every deleted branch's tip SHA is recoverable from this file and from `git reflog`.
