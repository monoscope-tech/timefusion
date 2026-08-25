# Active plans

This directory contains active implementation plans only. It does not contain
incident reports, production snapshots, or completed handovers.

An active plan must state its owner, status, completion conditions, and last
review date. The plan must describe the current code, not a proposed predecessor.

Delete the plan when one of these conditions is true:

- The implementation is complete.
- A newer plan replaces it.
- The product direction changes.
- The measurements are no longer reproducible.

Use an issue for short work. Use the runbook for operating procedures. Use a
reference document for behavior that remains true after the work is complete.

Deletion is not loss: `git log --diff-filter=D -- docs/plans` lists every deleted
plan with the SHA that still holds it, and `git show <sha>:<path>` reads it back.
Twelve were deleted on 2026-08-24 under the rules above; their surviving open
items were transplanted into the handoff before the deletion, not dropped. Two
more were on that list and were kept only because a live plan cites them:
`2026-08-23-a-440gb-scan-no-longer-starves-the-box.md` and
`2026-08-22-untagged-tier-convergence.md`. Deleting a page something still points
at trades one kind of stale for a worse one.

## Current plans

- [What is still open on 14d/30d](2026-08-24-handoff-open-work.md) — **start
  here.** The standing handoff: open items, the traps that produce wrong
  readings, and the commands for each.
- [Making 14d and 30d complete for every project](2026-08-22-make-14d-30d-complete.md)
  — the goal and its binary pass condition.
- [Seven windows, six projects, monoscope's real SQL](2026-08-22-seven-window-six-project-matrix.md)
  — the measured baseline the pass condition is defined against.
- [An architecture that keeps up](2026-08-18-an-architecture-that-keeps-up.md) —
  the throughput direction; `CLAUDE.md` points at its Phase 0 by path.
- [A trivial query costs seconds after hours of uptime](2026-08-24-a-trivial-query-costs-seconds-after-hours-of-uptime.md)
  — open investigation; holds a quiet-window protocol that a push to master
  resets.
- [Open work after untagged convergence](2026-08-24-open-work-after-untagged-convergence.md)
  and [handoff results](2026-08-24-handoff-results.md) — the coverage-ledger
  workstream.
- [Query latency matrix](2026-08-22-query-latency-matrix.md) — filter-shape
  matrix; `src/database/mod.rs` cites it.

## Measurements taken 2026-08-25 — read these before re-deriving anything

The afternoon sweep answered five open questions and **falsified the premise of
four of them.** Two of those premises were written that same morning. If an item
below contradicts an older page, these are newer.

- [Prod quiet-window baseline](2026-08-25-prod-quiet-window-baseline.md) — the
  session's measurement record: both ledger gates read clean, the ledger lag is
  benign and proven by a monotone `held` walk across a restart, and
  `stale_generation = 0` falsifies the "~92% of the tier is dark" figure.
  **§11 records that `out_of_policy_cells` was unreachable by construction** —
  the census counted any ≥2 files under target while the planner requires the
  two smallest to SUM under target, and the packer emits only sub-target files.
  Corrected: **51 → 14 cells, 425 → 292 small files.**
- [The lag alarm and the ledger over-claim](2026-08-25-prod-followup-lag-and-ledger.md)
  — a "3.5x worse" lag reading was a **window-selection artifact**; whole-life is
  0.183/min, below pre-fix. The multi-second tail did collapse (max 6,029 →
  550 ms) but the RATE comparison is **not sound** and must not be claimed. The
  ledger over-claim is **benign**: `replace_many` makes `held(N) ≡ proved(N−1)`,
  98 links confirmed with 0 violations, and `routing_view` has **no production
  caller**, so blast radius is zero. **Also: the `coverage_ledger_disagreements`
  gate as specced can never open** — a spec problem blocking the union-tag work.
- [The 85-day `oldest_task_age` tail](2026-08-25-oldest-task-tail.md) — it is a
  `SealedConsolidation` for 2026-05-31, **not a rollup task**, identified by
  arithmetic from three midnight-aligned reads. Decision: the 31-day horizon is
  correct, the tail is abandoned, and the gauge must stop counting it. The
  horizon's own comment ("deprioritised, not abandoned") is falsified.
- [The 30d `log_list` failure is a pruning problem](2026-08-25-log-list-30d-is-a-pruning-problem.md)
  — **not an OOM.** It is refused at the scan guard (1,404 files / 431 GB), so it
  never reaches `DedupExec` and no bounded-dedup work can fix it. Two different
  query shapes are refused on a byte-identical selection, so nothing is pruned.
  17 of the matrix's 25 failures are statement timeouts, not guard refusals.

## Decisions taken 2026-08-25

Five items that were open as decisions, not as code. Each states its verdict in
the first paragraph and each names the premise it found false — three of the five
were deferred or blocked on a reason that did not survive being checked.

- [Union tags stay deferred](2026-08-25-union-tags-stay-deferred.md) — tag-aware
  rollup-tier compaction. Deferred, with a four-condition revisit trigger; the
  ledger is not the reason and should stop being cited as one.
- [The preflight floor needs a byte model in `sim`](2026-08-25-preflight-floor-needs-a-byte-model-not-a-prod-journal.md)
  — `sim` does not model byte-driven splits, so the prod journal was never the
  blocker. Specifies the model and the synthetic fixture.
- [Partial certification](2026-08-25-partial-certification-design.md) — design
  only; the §3a rule is already wired, and missing add-action timestamp stats are
  an unnamed second blocker. Supersedes the "diagnosed, fix not built" page below.
- [Dedup strategy triage](2026-08-25-dedup-strategy-triage.md) — ranked order for
  the dedup backlog; measure duplication before choosing a policy.
- [Ingest-profile leftovers, closed](2026-08-25-ingest-profile-leftovers-closed.md)
  — both phases were fixed by `a3a4b25`; no profile was needed.

## Reference pages kept for live code

These are complete, but live code cites them as the rationale for a current
setting or mechanism, so they are kept as reference rather than deleted:

- [File-level needle pruning](2026-08-22-file-level-needle-pruning.md) — the
  bloom sidecars (`src/read/bloom_prune.rs`, `src/config.rs`).
- [Post-hot-tier speed](2026-08-21-post-hot-tier-speed.md) — the foyer sizing
  (`src/config.rs`).
- [Rollup correctness and routing](2026-08-22-rollup-correctness-and-routing.md)
  — the source-row witness (`src/rollup.rs`).

## Diagnosed, fix not built

- [Certification only works where it is not needed](2026-08-23-certification-only-works-where-it-is-not-needed.md)
- [The row witness counts the wrong rows](2026-08-23-the-row-witness-counts-the-wrong-rows.md)
- [`log_list` at 30d is a footer-repair problem](2026-08-23-log-list-30d-is-a-footer-repair-problem.md)
