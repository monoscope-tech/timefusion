# Tier 1 — what shipped, what it was measured against, what is left

2026-08-23, branch `tier1-14d-30d-complete`. Goal: **14d and 30d queries should
at least COMPLETE for every project.** Baseline and failure-mode taxonomy:
`2026-08-22-make-14d-30d-complete.md`.

Four of five items shipped. Item 1.1's decision is measured and made; the action
it implies is a production credential change and is handed over rather than
taken. Item 1.2 was root-caused and deliberately not shipped — the reason is
below and it is not "ran out of time".

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

Three tests. The first fails with `FilterNotEligible` without the change, which
is the prod symptom exactly; the other two pin the cases that must keep
declining.

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

## 1.1 — measured, decided, and handed over

The gate on this item was "read `rollup_stale_no_witness` vs `_moved` first; the
flag does nothing for `moved`." Read with **coverage valid at 30**, not during a
post-restart rebuild:

| sub-reason | count | share |
|---|---|---|
| `rollup_stale_no_witness` | 2,448 | **79.7%** |
| `rollup_stale_moved` | 624 | 20.3% |
| `rollup_stale_no_source_rows` | 0 | 0% |

`no_witness` dominates, so **`TIMEFUSION_ROLLUP_SLICE_FP_FALLBACK=true` is the
right call** — it targets ~80% of stale slices, and the fallback is pessimistic
rather than unsound (a fingerprint changes on compaction where `num_records`
does not, so it refuses more than strictly necessary, never serves stale rows).
The remaining ~20% are genuinely `moved` and no flag helps them.

**Not done here, deliberately.** The flag lives in CapRover's app env, not in
this repo — `deploy/caprover-service-override.yml` documents the service
override and explicitly is not env. Setting it needs the CapRover admin
credential and restarts prod, and this repo's standing rule is that the
production host is read-only: no restart, redeploy, or scale. Flipping it while
unattended also lands in the middle of an unusually busy deploy train (prod moved
`5062a7d` → `ba87ed3` → `7e5bb5a` → `11cd17a` → `a7a4eb0` inside two hours),
where `rollup_min_contiguous_days` needs ~25 min to rebuild before the result of
the flip is even readable.

So: decision made and evidenced, execution handed over. After flipping, read
`rollup_hits_hybrid_total` — but only once `rollup_min_contiguous_days` is back
at 30, or the measurement has not started.

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
its own query and the server survives). Leaving it is the correct call; the
honest status is "blocked on ordering", not "done".

## Verification

`cargo lint` clean; `cargo nextest run --lib` 864/864. Every commit is on
`tier1-14d-30d-complete` and pushed. Nothing here touches `master`, so no deploy
was triggered by this work.

**None of this is measured against prod yet.** The pass condition remains: re-run
the sweep and require zero `fail` cells at 14d/30d for all six projects. 1.4 is
the item most likely to move it, and it moves `p95_latency` — one of the two
shapes that fails most often at 30d.
