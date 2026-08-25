# Dedup strategy triage: what task 05 moved, what is left, in what order

**Owner:** unassigned. **Status:** triage recorded, no implementation.
**Last reviewed:** 2026-08-25. **Supersedes:** `tasks/14`'s ordering.

## Verdict

Item A's premise — "genuinely unordered legs have no policy at all" — is
**stale**. There is a named, tested, shipped policy: unbounded keep-greatest
under a 2 GiB per-query ceiling that fails the query with a `ResourcesExhausted`
carrying a remediation string (`src/read/mod.rs:59-71`). That is option 3 of the
three the task file asks us to choose between, already chosen. What is genuinely
missing is narrower and much cheaper than "pick a policy": the error **does not
name the partition or the files** that caused it, and **nothing schedules the
repair** that would stop it recurring. Branch `task05-bounded-dedup` (`08095e4`)
*advances* A by collapsing retention from O(versions) to O(winners), which moves
the threshold a long way — but it **closes nothing**, because the ceiling is
still the backstop and the question "what happens when the winners themselves
exceed the budget" is untouched. B is the item to do first and it is far cheaper
than written: four of its six numbers are already emitted by existing metrics and
readable from `EXPLAIN ANALYZE` today, no code. C stays last and stays correct.
Ranked order: **B (measure) → A′ (name the partition + enqueue the repair) → C
(overlap-scoped bypass) → A″ (a real policy for winners-over-budget, only if B
shows it happens)**.

---

## What is actually in the code today

| Mechanism | Where | State |
|---|---|---|
| Bounded dedup (leading sort col is a dedup key) | `read/mod.rs:389`, `detect_bound` | default ON, kill switch `timefusion_read_dedup_bounded` |
| Unordered fallback: unbounded keep-greatest | `read/mod.rs:595-604` | always available; **correct**, replaced a keep-first that served pre-update rows |
| The ceiling | `UNBOUNDED_GREATEST_MAX_BYTES = 2 GiB`, `read/mod.rs:59` | hard fail with remediation text |
| Ordering restoration when merely coalesced away | `DedupNeedsOrderedInput`, `read/optimizers.rs:2968`, registered `database/mod.rs:4939` | shipped |
| Per-query rows in/out | `input_rows` counter + `BaselineMetrics`, `read/mod.rs:628-629` | shipped, visible in `EXPLAIN ANALYZE` |
| Reservation against the query pool | `MemoryConsumer::new("DedupExec[keep-greatest]")`, `read/mod.rs:605` | shipped — an oversized window kills its own query, not the box |
| Cooperative yielding | `coop::make_cooperative`, `read/mod.rs:679` | shipped — statement timeouts can now fire mid-buffer |

The 2026-08-02 and 2026-08-03 incidents named in those comments are the reason
this shape exists. Any proposal that reintroduces a blocking `SortExec` to
manufacture an ordering, or that drops the pool reservation, is re-running a
known outage.

## What `task05-bounded-dedup` does, and what it does not

`08095e4`, reviewed read-only from git; the branch and its worktree
`/Users/tonyalaribe/Projects/apitoolkit/tf-task05` are another agent's and were
not touched.

**Does:** adds `compact_to_winners` — when the retained buffer grows, collapse it
to the rows that are current winners, plus `compact_batch` after the filter so
Arrow views stop pinning the original parquet column-chunk buffers. Re-armed at
2x the winners' cost so it does not re-filter every batch. Sound **only** in
unbounded keep-greatest with the whole scan as one open run; bounded mode must
not call it, because its masks carry closed-run winners not yet emitted. Six
parity cases across arrival orders, including a NULL stamp and a key updated
after an earlier version survived a compaction.

**Effect on A:** the retained buffer now grows with **distinct keys**, not with
**versions**. p1's 30d `log_list` failed where p4's identical shape succeeded
because the variable was duplicate density; this removes duplicate density as
the variable. That is a large, correct win.

**Does not:**

- Remove the 2 GiB ceiling, or change what happens when it is hit.
- Bound anything. O(distinct keys over a 30-day window) is still unbounded in
  the formal sense; it is merely proportional to the answer instead of to the
  duplicates. A 30d `SELECT *` over a high-cardinality key still exceeds it.
- Name the offending partition in the error, or schedule the footer repair.
- Touch bounded mode, which is the path most scans take.

**Triage:** A is **advanced, not closed**. Do not mark it done when task05
merges.

## B — duplication accounting: mostly already emitted

The task file lists six numbers per profiled query. Four are available today:

| Number | Source today |
|---|---|
| physical rows | `DedupExec`'s `input_rows` metric (`read/mod.rs:629`) |
| unique dedup keys | `DedupExec`'s output rows (`BaselineMetrics`) — for a full-set scan these *are* the distinct keys |
| versions per key | the ratio of the two above |
| predicate-rejected rows | `FilterExec` / `DataSourceExec` metrics in the same `EXPLAIN ANALYZE` |
| files touched | `DataSourceExec` file-group metrics + `timefusion_stats` scan counters |
| object-store requests | `timefusion_stats` foyer/storage counters (process-scoped — see the trap below) |

So B is a **measurement exercise, not a build**. Run one profiled 30d query under
`EXPLAIN ANALYZE`, read the plan, publish the table.

Three caveats that will otherwise produce a wrong number:

1. **The per-date and per-file dedup splits change the population.** When the
   per-date skip fires (`database/mod.rs:8710`), certified dates are unioned
   *above* `DedupExec`, so `input_rows` sees only the deduped leg. Record which
   split shape the plan took (`DEDUP_SKIPPED_PER_DATE` /
   `DEDUP_SKIPPED_PER_FILE`) beside the numbers, or the ratio understates
   duplication by exactly the certified fraction.
2. **`mode=bounded` vs `mode=full-set` changes the meaning of output rows.**
   `DedupExec`'s `Display` reports which (`read/mod.rs:525`). In bounded mode
   output rows are winners-per-run, not global distinct keys.
3. **The "32.6x duplicates" figure conflates versions-at-rest with tier copies**,
   and the "7.24M rows scanned → 613 output rows" figure additionally conflates
   predicate rejection with duplication. Separating `FilterExec` rejection from
   `DedupExec` collapse is the whole point of the exercise. **Retire 32.6x unless
   the measurement reproduces it with its population named.**

**Done when** one profiled 30d query has all six numbers published with its split
shape and dedup mode recorded, and 32.6x is confirmed-with-population or retired.

## A′ — the cheap half of A, worth doing before C

Two changes, both small, neither a policy decision:

1. **Name the partition in the error.** `check_unbounded_growth`
   (`read/mod.rs:61`) says "narrow the time window or compact unsorted files"
   with no way to know *which* files. The operator-actionable version names the
   `(project_id, date)` partition and, ideally, the count of file groups lacking
   footer `sorting_columns` — the exact diagnosis already recorded at
   `read/mod.rs:577-585` (38 of p1's 86 file groups carry no `sorting_columns`,
   and **one** unordered branch erases the ordering the whole leg declares).
2. **Enqueue the footer repair from the failure.** Today a user hits the ceiling,
   the query dies, and nothing about the system changes; the next identical query
   fails identically. A failure that schedules its own remedy converts a standing
   defect into a self-healing one.

Note the standing caution from `docs/plans/2026-08-23-log-list-30d-is-a-footer-repair-problem.md`:
footer repair reduces the **frequency** and is not a memory-safety invariant. A′
is not a substitute for a policy — it is the part that pays regardless of which
policy A″ eventually picks.

## A″ — the actual policy question, deferred behind B

"What happens when the *winners* exceed the budget" is the residue after task05.
The three candidates from the task file, re-priced against the current code:

- **Spillable / hash-partitioned dedup.** Correct in general. The machinery
  partly exists already — `sort_flush_group_spilling` shows the pooled/spilling
  pattern on the write side. Costs spill IO on a path where correctness incidents
  live. Expensive to get right.
- **Reject before decode, with the repair scheduled.** This is A′ plus a
  plan-time refusal instead of a runtime one. Attractive, but plan-time cannot
  know the winner count; it can only know the file-group ordering state. So it
  refuses on a *proxy*, which will refuse queries that would have succeeded.
- **Tightly bounded fallback.** Already shipped. The 2 GiB ceiling *is* this.

**Do not choose between these until B has run.** If B shows that post-task05
winners never approach 2 GiB on any real dashboard shape, A″ closes as
"the shipped ceiling is the policy" and the cost of the other two is never paid.
That is the likely outcome and the reason B ranks first.

## C — overlap-scoped dedup, unchanged and still last

The task file's C is correct as written and needs no revision. One update: its
soundness rule is **the same rule as certification's §3a, and §3a is already
implemented and wired** — `skippable_certified_files` (`read/mod.rs:200-212`),
with a 12-row `test_case` table at `read/mod.rs:1112` covering the
single-microsecond touch, the no-stats file, and the empty certified set. C is
therefore a *generalisation of working code*, not a new mechanism: replace "is
this file in a `Certification`" with "did compaction prove this file unique
within the dedup key", keeping the same overlap filter.

Two constraints C inherits, both load-bearing:

- **A file with unknown min/max timestamp stats must be treated as overlapping
  everything.** `FileSpan = Option<(i64,i64)>` and the rule fails closed on
  `None` (`read/mod.rs:203`). This is not optional; a `None` treated as disjoint
  is a silent wrong answer.
- **The all-or-nothing trap moves, it does not vanish.** `skippable_certified_files`
  returns the empty set the moment *any* uncertified file lacks stats, so one
  stats-less recent file disables the whole partition's bypass. C will inherit
  that unless it is designed around it. See the companion doc
  `2026-08-25-partial-certification-design.md` §5.

C.4 ("binary certification retires once bypass coverage exceeds it") remains the
strategic intent, and the certification doc is written to be consistent with it:
partial certification is scoped there as the **minimal stepping stone C
subsumes**, not as a permanent mechanism.

## The correctness caution that governs all four

`DedupExec`'s ordering is a **correctness** requirement, not overhead. Two logged
prod incidents live on this path. Every change here needs a parity diff against
the un-bypassed answer over a **churning** partition — not a latency number, and
not a parity diff over a quiet fixture, which passes vacuously.

## Ranked order, with the measurement that justifies each

| Rank | Item | Justifying measurement |
|---|---|---|
| 1 | **B** — publish the six numbers for one 30d query | none needed; it *is* the measurement. Blocks honest argument about everything below. |
| 2 | **A′** — name the partition, enqueue the repair | B's "files touched" plus the count of file groups without `sorting_columns` on the failing partition |
| 3 | **C** — overlap-scoped bypass | B's versions-per-key and files-touched: C only pays if duplication is concentrated in overlapping file groups. If B shows most files are already non-overlapping, C is a large win; if it shows heavy overlap, C buys little and says so cheaply. |
| 4 | **A″** — a policy for winners-over-budget | B's unique-key count against 2 GiB on the worst real shape, re-measured **after** task05 merges. If the margin is >5x, close A″. |

## Explicitly closed, do not re-open

- The cert-grant watch — certification was diagnosed healthy 2026-08-22;
  `cert_granted_total = 0` is process-scoped and sent three fixes at a working
  mechanism.
- Resumable dirty-bin staging — shipped.
- "Unordered legs have no policy" — see the verdict.
