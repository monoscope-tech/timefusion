# Tier 1 — what shipped, what it was measured against, what is left

2026-08-23, branch `tier1-14d-30d-complete`. Goal: **14d and 30d queries should
at least COMPLETE for every project.** Baseline and failure-mode taxonomy:
`2026-08-22-make-14d-30d-complete.md`.

Three of five shipped and verified. **1.1 is closed, not deferred** — its lever
was deleted from the codebase ninety minutes after the build the checklist named,
because it was measured to do nothing. **1.2 is blocked on a rebuild-class fix**;
the reason is below and it is not "ran out of time".

## 1.4 — a null guard on a measure column is a no-op *(shipped)*

The one new finding. monoscope emits `AND duration IS NOT NULL` on every latency
chart. `duration` is a MEASURE column, so no declared filter mentions it, the
residual matched nothing, and the panel declined `filter_not_eligible` — **at
every window, however complete coverage is.** That is why no coverage-side work
ever moved it, while `duration_digest` sat declared and unfiltered for exactly
this panel.

Measured, prod, counter-diff A/B, 3 reps per arm, arms differing in this one
predicate:

| arm | miss reason |
|---|---|
| p95 as monoscope emits it | `filter_not_eligible` |
| the same p95, minus `IS NOT NULL` | `not_built` |

Every other arm reported `not_built`, so the eligibility check runs *before*
coverage — the conclusion does not depend on the coverage state it was measured
in. Confirmed in the code at the eligibility guard, which tests the residual's
columns against declared FILTER columns only.

The fix drops the predicate only when **both** hold, and the second is the one
that is easy to miss:

- every aggregate reads the same column, so each already ignores null rows.
  `count(*)` does not — dropping the predicate beside one silently counts rows
  the raw query excluded. That is the §6 top-K shape, not a corner case.
- a declared `count` over that column exists to serve as the `HAVING` guard.
  Without it a bucket whose rows were ALL null returns as a zero instead of
  being absent — a *row-presence* change, not just a value change.

Four tests. The first fails with `FilterNotEligible` without the change, which
is the prod symptom exactly; two pin the cases that must keep declining; the
fourth covers the HYBRID path.

**A review catch worth recording, because the single-leg test could not see
it.** The guard flows through the existing `RoutedMeasure` construction whose
no-filter arm emits a bare `COUNT(*)` — an arm that could not fire before, since
filter promotion always had a declared filter. But the fringe's WHERE carries
dimension filters only, and the promotion clears `duration IS NOT NULL`, so
`COUNT(*)` counted the null rows and `HAVING > 0` kept exactly the all-null
buckets the guard exists to eliminate — the same resurrection, reintroduced on
the other leg. Since monoscope's windows end at `now()`, **every** routed latency
chart has a fringe, so this was the common path. `COUNT(col)` skips nulls, which
is precisely the dropped predicate. The lesson generalises: a rewrite with two
legs needs a test per leg, and `horizon: None` is the half that hides this.

## 1.3 — a per-query byte ceiling *(shipped, default off)*

**Why `GatedScanExec` "didn't fire": it never could.** Its own doc says "decode
accounting only — this operator never denies on memory". It is a concurrency
throttle that bounds how MANY wide scans decode at once, never how much any one
of them decodes.

The guard that *does* refuse, `bounded_otel_scan_reason`, checks a query's
SHAPE — a project filter, and a lower bound or a LIMIT. A 30-day dashboard
aggregate satisfies both and is admitted while selecting tens of GB. It also
defaults to `Off`, and prod reads `bounded_otel_scan_candidates = 0`.

So nothing bounded a single query. 2026-08-18: one scan selected 514 files /
32.8 GB. 2026-08-22: while a 30-day aggregate ran, **new connections timed out
entirely** — a `SELECT 1` against `timefusion_stats` could not get in, and the
same condition killed a benchmark harness mid-run. One query denying service to
every other session.

`gate_if_wide` already computed `(files, bytes)` post-pruning and already warned
above 1 GB, with the comment *"this is the only place that can see a single
query large enough to take the process down"* — and then admitted it. The
refusal now lives there, behind `TIMEFUSION_WIDE_SCAN_REJECT_MB`, **default 0 =
off**, because it refuses work the server would otherwise attempt. The 1 GB warn
threshold stays below it so `wide_scan_oversize_total` shows how often the
ceiling would bite before anyone enables it.

Note this trades a failure mode rather than removing one: an oversize 30d query
goes from "may take the process down" to "returns a clear error". That is
strictly better for every *other* session, and it is not the same as making the
query complete.

## 1.5 — the statement timeout is cooperative *(shipped)*

Mode D: a 7-day aggregate ran **>20 min against a 60 s effective cap that never
fired**, on a container with 2 h uptime still answering `SELECT 1` on new
connections.

`run_with_statement_timeout` enforces by DROPPING the in-flight future, and
`tokio::time::timeout_at` only runs when a poll returns `Pending`. `DedupExec`'s
unbounded keep-greatest buffers to end-of-stream, so one `poll_next` can run
minutes of pure CPU with the deadline unobservable throughout.

DataFusion's own `coop` module names this failure: *"If a Stream runs for a long
period of time without yielding back to the Tokio executor ... this prevents the
query execution from being cancelled."* Built-in sources carry yield points;
custom operators opt in. `DedupExec` and `GatedScanExec` are ours and are the two
that can run long inside one poll, so both now wrap their output in
`coop::make_cooperative`.

The test asserts the LIMITATION, not the fix — a timer-awaiting future is
interruptible, a non-yielding one runs to completion regardless of the deadline
— so it keeps passing and keeps the reason discoverable. Made exact with a
paused clock.

**This is a mitigation, not a proven fix, and should be read that way until
reproduced.** The wedge itself was never reproduced locally; the change follows
from the mechanism, not from a failing repro. Two known coverage limits:
`make_cooperative` spends budget per *produced batch*, so a long poll that
produces nothing still starves the timer; and `GatedScanExec` only wraps scans
past the wide-scan threshold, so a below-threshold plan fed from an all-ready
in-memory input is still uncovered.

Post-deploy check: re-run the wedge shape (a 7-day aggregate on p5) and a clean
statement-timeout error at ~60 s is the pass signal. A second >20 min run means
the long poll is somewhere these two wrappers do not reach.

## 1.1 — investigated and CLOSED: the lever no longer exists

The item said "first action is flipping `TIMEFUSION_ROLLUP_SLICE_FP_FALLBACK` on
the currently-deployed `a7a4eb0`, after reading `rollup_stale_no_witness` vs
`_moved`". I did the read. The flip is not possible, and would be wrong.

**The measurement, taken with coverage valid at 30** (not during a post-restart
rebuild):

| sub-reason | count | share |
|---|---|---|
| `rollup_stale_no_witness` | 2,448 | **79.7%** |
| `rollup_stale_moved` | 624 | 20.3% |
| `rollup_stale_no_source_rows` | 0 | 0% |

`no_witness` dominates, which is what the flag was built for — so on this
evidence alone the flip looks right. **It is not.** The flag was removed in
`7e5bb5a`, ninety minutes after the `a7a4eb0` that introduced it, and removed
*because it does nothing*:

> The two fingerprints are incomparable in two independent ways: slice
> `source_fp` is an `FnvHasher` over the files SELECTED FOR THAT SLICE; the
> partition fingerprint is a `DefaultHasher` over the partition's WHOLE live
> file set. Either difference alone makes equality impossible. Measured
> end-to-end: with the flag on, a stripped-witness slice routed NOTHING.

It was deleted rather than left default-off with the explicit reasoning that "a
flag that does nothing is worse than no flag: it reads as an available lever,
and the next person costs themselves a night finding out it is not." Re-adding
it — which is the only way to "flip" it now — would reintroduce dead code that a
teammate had just deleted with an end-to-end measurement behind it.

**So the correct status is closed, not deferred, and my earlier note in
`2026-08-22-make-14d-30d-complete.md` recommending the flip is wrong and is
corrected there.** What the 79.7% actually says is that `no_witness` clears only
by republishing those slices — throughput, which is the same wall as frozen
compaction. That is a rebuild, and this goal excluded rebuilds. The remaining
20.3% are genuinely `moved` and no amount of rebuilding fixes a churning
partition.

The three split counters (`ba87ed3`) survive and are what made both this
measurement and the revert possible.

Worth carrying forward, from the revert's own note: its unit tests over the
predicate all passed and the integration test failed, and the integration test
was right. Both fingerprints are `u64` fields named `source_fp`, so the types
had nothing to say. Testing a pure predicate proves the predicate, not that the
values fed to it mean what you think.

## 1.2 — root-caused, and NOT shipped on purpose

The target was "bound `log_list` so `LIMIT 251` doesn't dedup 30 days". It does
not decompose the way the checklist assumed.

`DedupExec` has two modes. With an ordered bound column it emits per run and
never buffers the stream — a `LIMIT` above it genuinely terminates early, and
there is an existing test asserting exactly that. Without one it falls to
**unbounded keep-greatest, which is blocking by nature**: keep-greatest cannot
emit any key until it has seen every version of it, so a downstream `LIMIT`
cannot bound it *in principle*. p1's 30d `log_list` hit the unordered path and
the 2 GiB ceiling; p4's identical shape at 30d returns in 879 ms because its
input is ordered. The variable is the input's advertised ordering, not the
window.

That makes the real fix "get the ordering advertised" — the sorted-footer /
compaction workstream, which is Tier 2/3 and a rebuild — not a change to the
`LIMIT` path. The alternatives I considered and rejected:

- Push the `LIMIT` below dedup: returns wrong rows whenever a duplicate is in
  the window, because the top-251 by timestamp *before* version resolution is
  not the top-251 after it.
- Degrade unordered keep-greatest to keep-first to bound memory: this is exactly
  the 2026-08-02 regression the current code documents — a merge-on-read table
  then answers with the PRE-update row.

Both are silent-wrong-answer changes in version resolution, which is the last
place to make a speculative fix unattended. Mode B already fails safely today
(the reservation is registered with the memory pool, so an oversize window fails
its own query and the server survives).

### The design that WOULD work, and the one hazard it has to clear

Not shipped, but specified, because the blocking fact turned out to be in our
favour and the next person should not re-derive it.

`dedup_keys` for this table is `[timestamp, id]`, and the schema states that an
UPDATE "appends the row's ORIGINAL timestamp into a NEW file". **`timestamp` is
part of the dedup key, so every version of a key carries the same timestamp.**
That licenses a bounded top-N *inside* the operator: for `ORDER BY timestamp
DESC LIMIT n`, a key whose timestamp is below the n-th best distinct-key
timestamp can never reach the output, and neither can any of its other versions.
Evicting it caps state at O(n) instead of O(window).

This is NOT the truncation the optimizer already forbids. `OrderedUnionForTopK`
refuses to push a fetch through a `DedupExec` because "a `with_fetch` cut on a
leg below it truncates that input... an equal-timestamp cut can keep a stale
version whose newer sibling was truncated away." The eviction above still
consumes the WHOLE input, so every version of every retained key is seen; it
only bounds what is *remembered*.

The hazard it inherits is that same tie: eviction must be **inclusive of ties**
at the boundary (keep every key whose timestamp equals the n-th best), or a key
can lose a version to the cut. Tie-inclusivity means the bound is O(n + tie
width), which a bulk insert sharing one timestamp can inflate — so the ceiling
has to stay as a backstop.

Cost: a `fetch` on `DedupExec`, a rule to set it from a `SortExec(fetch, on a
dedup-key column)` directly above (DataFusion will not do this for us — it
pushes fetch into the sort, not below it), the eviction, and a test for the
stale-version-across-a-tie case specifically. That is a change to version
resolution, which has produced two logged prod incidents from this exact class,
so it wants a review and a day, not an unattended night.

## Verification

`cargo lint` clean; `cargo nextest run --lib` 864/864. Every commit is on
`tier1-14d-30d-complete` and pushed. Nothing here touches `master`, so no deploy
was triggered by this work.

**None of this is measured against prod yet.** The pass condition remains: re-run
the sweep and require zero `fail` cells at 14d/30d for all six projects. 1.4 is
the item most likely to move it, and it moves `p95_latency` — one of the two
shapes that fails most often at 30d.
