# Partial certification: design only, and the blocker nobody has named

**Owner:** unassigned. **Status:** design recorded. **Implementation
deliberately not started** — the failure mode is a silent wrong answer.
**Last reviewed:** 2026-08-25. **Supersedes:** `tasks/13`'s scoping.

## Verdict

The diagnosis in `tasks/13` is right — `record_certification` grants only when
the partition's whole file fingerprint held still, so a churning tenant's day can
never certify and the three slow dashboards hold zero certifications — but one of
its premises is **wrong**: the §3a soundness rule is not "proved and tested but
never wired". It is **implemented, wired and reachable**
(`src/read/mod.rs:200-212`, called from `certified_files_in_partition`
→ `certified_file_split` → `database/mod.rs:8736`), with a 12-row `test_case`
table including the single-microsecond touch and the equal-timestamp tie, and an
integration parity test. It is simply dark, because
`timefusion_read_dedup_skip_per_file` is default-OFF and because the producer
never emits a partial `Certification` for it to consume. Wiring is not part of
this work; only the producer is. And there is a **second blocker the task file
does not name**, which would make a partial producer grant nothing on exactly the
tenants it targets: `skippable_certified_files` returns the empty set the moment
*any* uncertified file in the partition lacks min/max `timestamp` stats
(`read/mod.rs:203-205`), and nothing in the codebase repairs missing add-action
stats — footer repair fixes `sorting_columns`, a different thing. **Recommended
fix: candidate 2 (per-file grant from the clean-slice accumulator) plus a
mandatory stats-coverage precondition, shipped default-OFF behind the existing
flag, gated on a churn-parity test that does not exist yet.** A partial version of
this is worse than none.

---

## 1. The mechanism, precisely

### The grant

`record_certification`, `src/database/maintain.rs:3710-3757`. Line 3720:

```rust
if dropped == 0 && complete && !post.is_empty() && partition_file_fp(pre.to_vec()) == fp_post {
```

`post` is re-read live inside the function from the partition's current live file
list; `fp_post` is its fingerprint. `partition_file_fp`
(`src/database/mod.rs:1582-1589`) is an order-insensitive hash over the **sorted
list of full object-store URIs** — paths only, not sizes or stats. So *any* add,
remove or rewrite anywhere in the day moves it.

Refusal is attributed into four counters (`maintain.rs:3747-3753`):
`CERT_REFUSED_DROPPED`, `CERT_REFUSED_INCOMPLETE`, `CERT_REFUSED_EMPTY`,
`CERT_REFUSED_FP_MOVED`. **Read these before building anything** — they say which
conjunct is actually refusing the whale, and the design below assumes it is
`FP_MOVED`. If it is `DROPPED` or `INCOMPLETE` the diagnosis is different and
this document does not apply.

On refusal, any existing certification is **removed** (`maintain.rs:3754-3756`).

### The accumulator

`record_clean_slice`, `maintain.rs:3654-3702`. Merges proven-clean
`[start, end)` intervals into `SliceCoverage.intervals`
(`database/mod.rs:455-458`), write-through persisted on every mutation so a
restart does not strand the day. Two ways the accumulation dies:

- **Dirty slice** (`:3671-3677`): drops the accumulated entry *and* delegates to
  `record_certification`, which takes its removal arm and voids the day.
- **Fingerprint moved between clean slices** (`:3682-3683`):
  `if entry.fp != fp { *entry = SliceCoverage { fp, intervals: vec![(start,end)] } }`
  — accumulation resets to just this slice.

It grants only when one merged interval covers the entire UTC day (`:3687`).

**This is the actual failure loop for a whale.** The dedup sweep proves slice
after slice clean; each proof is correct; ingest lands a file; the fingerprint
moves; every accumulated proof is discarded. The work is done and thrown away, on
a loop, forever. A quiet tenant's day goes still and certifies. That is why
certification succeeds only where it is not needed.

### The consumer, which already exists

- `Certification` (`database/mod.rs:428-448`): `fp`, `since`, **`files:
  Arc<[String]>`** — the exact file list the pass proved — and `stale`.
- `StoredCertification` (`storage.rs:3479-3496`): persisted to
  `.timefusion_meta/dedup_certifications.json`, with `files` under
  `#[serde(default)]`, which is why the prod sidecar's older entries read
  `files = []`.
- `certified_file_split` (`maintain.rs:2833-2856`) → `certified_files_in_partition`
  (`:2876-2891`) → **`read::skippable_certified_files`** (`read/mod.rs:200-212`),
  which is §3a verbatim: a certified file may skip `DedupExec` iff no uncertified
  file's timestamp span overlaps its own, inclusive bounds, failing closed on any
  missing span.
- Read path: `database/mod.rs:8734-8737`, attempted only when the whole-window
  skip did not fire and the per-date set is empty. Certified leg unions **above**
  `DedupExec` (`:8784`); uncertified leg feeds it (`:8763`).

**So the entire per-file read path is built, tested and connected.** The one
missing part is a producer that emits a `Certification { files }` when the
partition fingerprint has moved but a subset of files is still provably clean.

## 2. The blocker the task file does not name

`skippable_certified_files` fails closed on unknown spans, correctly:

```rust
if uncertified.iter().any(Option::is_none) { return HashSet::new(); }
```

Spans come from `partition_file_spans` (`maintain.rs:2906-2920`), reading
`min.timestamp` / `max.timestamp` out of `snapshot.add_actions_table(true)`.
A missing column, a wrong type, or a null on either side yields `None`.

Three independent signals say `None` is not rare:

- `count_pushdown` bails with "stats gaps or boundary files"
  (`read/mod.rs:2126-2129`) when `stats.minValues.timestamp` is absent.
- `rollup::stats_time_range` (`src/rollup.rs:97-110`) documents `None` "when
  stats or timestamp bounds are absent" and accepts **both** epoch-micros and
  RFC-3339 spellings — writers are not uniform.
- Nothing in the repo backfills missing add-action min/max. Footer repair
  (`config.rs:1755-1830`) repairs parquet `sorting_columns`. Different artefact,
  different problem.

Consequence: **one stats-less recent file disables per-file skipping for the
entire date** — the same all-or-nothing failure the per-file path was built to
escape, relocated from the fingerprint to the stats. A whale partition ingesting
continuously is exactly where a fresh, stats-poor file is most likely.

Two spellings are read in two places — `min.timestamp` (`maintain.rs:2910`) and
`stats.minValues.timestamp` (`read/mod.rs:2255`). Confirm they are the same
flattening before trusting either as a coverage measure.

**Therefore: measure stats coverage on `dcad860a`, `87576849` and `00000000`
before writing any producer.** If uncertified files routinely lack spans, a
partial producer grants nothing and the work is wasted. This measurement is the
first task, it is read-only, and it needs no new code:

```sql
-- per (project, date): fraction of live files with usable min/max timestamp
-- read from add_actions; or read CERT_REFUSED_* counters against uptime.
```

## 3. Candidate fixes

### Candidate 1 — soften the fingerprint to an "additive-only" check

Grant when the post file list is a **superset** of `pre` (files were added, none
removed), certifying the `pre` set as `files`.

- **Effort:** small. One conjunct in `record_certification` becomes a subset test.
- **Risk of wrong answers: MODERATE-HIGH.** The proof was that *those* files
  contain no duplicates *among themselves*. A newly added file may hold a newer
  version of a row inside a certified file. §3a is precisely the rule that
  handles this — but only if the new file's span is known and overlaps are
  checked. Under an additive-only grant, a new append whose span overlaps a
  certified file is the common case, so §3a will refuse the skip most of the
  time, and the grant is mostly cosmetic. Where it does *not* refuse is where the
  new file has no stats — and then §3a fails closed too. So the risk is not that
  it answers wrongly; it is that it appears to work and grants nothing.
- **Verdict: reject.** Weakens the invariant for no measured benefit.

### Candidate 2 — per-file grant from the clean-slice accumulator (recommended)

Change `record_clean_slice` so a fingerprint move **narrows** rather than
**voids**: keep the intervals proven under the old fingerprint, and grant a
`Certification` over the intersection of (files that existed at proof time) ∩
(files still live), with `fp` set to the current post fingerprint.

- **Effort:** medium. `SliceCoverage` must carry the file set per interval, not
  just the interval. `record_certification` grows a partial arm that skips the
  whole-partition fingerprint equality and instead asserts the narrowed file set
  is still live.
- **Risk of wrong answers: LOW, conditional on §3a.** Each certified file's
  proof is genuinely about that file: the sweep read it and found no duplicate
  version of any key it holds. The cross-file question is exactly what §3a
  answers, and §3a is already the gate on the read side. The residual risk is
  *not* in the dedup logic; it is in whether the file identity survives — see
  the two preconditions below.
- **Preconditions, both mandatory:**
  1. **A rewritten file is not the file that was proved.** If compaction rewrites
     `a.parquet` + `b.parquet` into `c.parquet`, `c` was never certified and must
     not inherit. Because the grant is by **relative path** and paths are unique
     per write, this holds by construction — but it must be asserted in a test,
     because a future "carry the certification across a rewrite" optimisation
     would break it silently. Add
     `a_rewritten_file_does_not_inherit_its_inputs_certification`.
  2. **Stats coverage.** Per §2. If the uncertified remainder has unknown spans,
     the grant is sound and useless. Emit a counter for
     "certified set non-empty but skip refused for missing spans" so the
     difference between "no grant" and "grant that buys nothing" is visible.

### Candidate 3 — prove uniqueness at compaction time instead of by sweep

Have the compactor emit a "unique within dedup key" property on its outputs, and
retire certification entirely.

- **Effort:** large. This is item C of the dedup backlog
  (`2026-08-25-dedup-strategy-triage.md`).
- **Risk of wrong answers: LOW and structurally lower than 1 or 2** — the writer
  knows with certainty whether its output is unique, where a sweep infers it.
- **Verdict: the right end state, wrong thing to do now.** It requires the
  duplication profile (item B) that has never been measured, and it does not help
  files that were never compacted, which on a churning partition is the recent
  tail — i.e. exactly the window the slow dashboards query.

**Recommendation: candidate 2, gated on the §2 measurement, shipped default-OFF
behind `timefusion_read_dedup_skip_per_file`, framed as the stepping stone
candidate 3 subsumes.**

## 4. The test that catches a wrong answer before prod

`tasks/13` asks for "a prod parity diff of `count(*)` with the flag on and off
over a churning partition". That cannot run unattended, and it is the wrong last
line of defence anyway: it runs *after* the code exists, on prod, on one shape.
The gap this document closes is the local test.

What exists today, in `tests/suite/dedup_compaction_test.rs`:

- `count_is_identical_with_and_without_the_per_file_dedup_skip` (`:3265`) —
  runs the fixture twice with the flag off then on, asserts `control_skips == 0`,
  `skips > 0`, and equal results. It selects `id` rather than `count(*)`
  deliberately, because a bare count is answered from add-action stats and never
  builds the scan. **Reuse that trick; do not write a `count(*)` parity test.**
- The 12-row unit table at `read/mod.rs:1112`.

What is **missing**, and is the actual gate:

1. **A churn-parity integration test where the bands OVERLAP.** The existing test
   constructs its churned band disjoint by a full second (`:3274-3276`), so it
   proves the skip fires in the favourable case. The dangerous case is a new file
   whose span *straddles* a certified file's span. Assert: skip refused, answer
   identical. Name it
   `an_overlapping_late_arrival_refuses_the_per_file_skip`.
2. **A missing-stats integration test.** Write a file with no timestamp stats
   into the partition, certify a neighbour, assert the skip is refused and the
   answer is identical. This exists only as a unit case today; it is the most
   likely real-world trigger and it must be exercised through the real read path.
   Name it `a_file_without_timestamp_stats_disables_the_per_file_skip`.
3. **A property test over interleavings.** Generate N files with random spans and
   random certified/uncertified assignment; assert that for every assignment, the
   split answer equals the un-split answer. This is the only construction that
   catches a boundary case nobody thought of, and a silent wrong answer is
   precisely the class where "nobody thought of it" is the failure.
4. **An equal-timestamp tie at the boundary**, integration level: two rows with
   identical `timestamp` and different `id`, one in each leg. The unit table has
   it; the read path has not been shown to.

**All four must be green before the flag is turned on anywhere**, and the flag
must stay default-OFF in the commit that adds the producer. A prod parity diff
remains the *last* step, not the gate.

## 5. What NOT to do

- **Do not** relax `skippable_certified_files`' fail-closed handling of unknown
  spans in order to make the counters move. That converts a useless-but-safe
  outcome into a silent wrong answer, and it is the single most likely shortcut
  someone will take when the grant lands and nothing changes.
- **Do not** re-diagnose certification as broken. It is not: 97 grants across 13
  projects live in the durable sidecar, and `cert_granted_total = 0` is
  process-scoped. Three separate fixes have already been aimed at a healthy
  mechanism on the strength of that zero.
- **Do not** carry a certification across a rewrite (see 2's precondition 1).
- **Do not** widen this to the per-date path. The per-date skip
  (`database/mod.rs:8710`) rests on a *different* argument — `date` derives from
  `timestamp` (`maintain.rs:3768-3773`) — and does no span-overlap check. It is
  correct on its own terms and must not inherit per-file reasoning.

## Done when

1. §2's stats-coverage measurement is published for `dcad860a`, `87576849` and
   `00000000`, and the `CERT_REFUSED_*` split confirms `FP_MOVED` dominates.
2. §4's four tests are green with the flag on, and the flag ships default-OFF.
3. Only then: those three projects hold non-zero certifications on recent dates,
   `dedup_skipped_per_date` / `_per_file` move off 0 for them, and the prod
   parity diff over a churning partition shows identical results.

Steps 1 and 2 need no prod and no supervision. Step 3 needs both.
