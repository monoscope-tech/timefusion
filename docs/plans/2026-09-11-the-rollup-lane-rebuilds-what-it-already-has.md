# The rollup lane rebuilds what it already has

2026-09-11. Prod image `f6954a2d`, process up 11h, 48 cores, ~2700-3500% CPU.

## The measurement

Worker-seconds by operation, process lifetime:

| operation | worker_secs | share |
|---|---:|---:|
| BaseRollup | 193,539 | **59%** |
| Dedup | 78,815 (+19,864 killed) | 24% |
| HotPacking | 24,394 | 7% |
| SealedConsolidation | 6,326 | 2% |
| DerivedRollup | 6,310 | 2% |
| Repair | 35 | ~0 |

DV-dedup (shipped 09-06) did its job: dedup is no longer the top lane.
**BaseRollup is.** And most of it is not work.

3h of `maintenance_rollup_published` lines, 3,110 parsed:

- BaseRollup: **2,428 publications over 968 unique slices** — repeat factor 2.51.
- **72.6% of BaseRollup decoded bytes republish a slice already published in the
  same 3h window.**
- **99.6% of consecutive republication pairs emit an IDENTICAL row count**
  (1,462 identical, 6 changed). 650 of 656 repeated slices never varied at all.
- DerivedRollup: 682 publications over 259 slices, repeat factor 2.63.

Backfill is **not** the explanation, and the census says so directly:
`cells_missing=0 cells_wanted=0 tier_holes=0`, `contiguous_days=30` on every
tier. Coverage converged. This is churn on a converged system.

## The loop, traced

`dashboard_1m_v3` / 87576849 / `[1787097600000000,1787140800000000)`, 90 minutes:

```
20:51:14 DerivedRollup  attempts=76  → Retry
20:51:14 BaseRollup     attempts=1   → Superseded      (day-wide)
20:51:15 BaseRollup     attempts=1   input_fp=1613351232941607
20:52:07   → rows=6013  Complete  ran_secs=52
21:23:14 DerivedRollup  attempts=77  → Retry
21:23:14 BaseRollup     attempts=1   → Superseded
21:23:14 BaseRollup     attempts=1   input_fp=1613351232941607
21:24:05   → rows=6013  Complete  ran_secs=51
21:55:14 DerivedRollup  attempts=78  → Retry
21:55:15 BaseRollup     attempts=1   input_fp=1613351232941607
21:56:15   → rows=6013  Complete  ran_secs=60
```

An exact **32-minute cycle**. `input_fp` — the unit's own hash of its selected
file list — is byte-identical every pass. The output is byte-identical every
pass. The unit logs the proof that it is a no-op and rebuilds anyway.

**Where the 32 minutes comes from.** `run_coordinator_rollup_selected`, the
derived branch: a base file whose `TAG_GENERATION` is not in the set of
generations held by current base coverage is refused, and on `skipped_generation
> 0` the unit enqueues a fresh day-wide BaseRollup and retries itself at
`60 << attempts.min(5)` seconds — **1920s = 32 minutes** at the cap. The day-wide
mint is Superseded by narrower children, which rebuild and republish.

**How little it takes.** The prod histogram of that warning:

```
6  dashboard_1h_v2  87576849  skipped_generation=1
5  dashboard_1h_v2  28f62f01  skipped_generation=1
6  metrics_1h_v2    00000000  skipped_generation=1
1  dashboard_1h_v2  6297304f  skipped_generation=6
```

**One base file** carrying a stale generation tag wedges a whole derived day,
forever. The rebuild it demands cannot clear it: `slice_retires` only retires a
tagged file whose range is CONTAINED in the publishing slice, and the day is
published as half-day children, so a wider stale file is never in any
replace-set. `retry.DerivedRollup.base_generation_unverified = 366`.

## Fix 1 — prove the rebuild redundant (SHIPPED, this branch)

A unit now compares its input against the live slice coverage before doing any
work, and completes if they agree. The coverage map is the right witness because
of what clears it: `invalidate_rollup_hours` — the CONTENT path, taken by ingest
and DML — drops the covering entries, while the reconciler's commit observation
does not.

The proof is a new `content_fp`: the input file set **with deletion vectors
folded in**. `InputFootprint::fp` and `source_fp` hash paths alone, which is
right for pricing and wrong here — a DV supersedes an `Add` under the same path,
so a DV'd file is path-identical and row-different. `Option`, `None` for
coverage rebuilt from tier tags at boot, and `None` only ever declines.

Derived units are excluded: their correctness also rests on `base_covered`.

Counter: `rollup_noop_rebuild_skipped_total`, read against
`rollup_staged_projects_total`. Kill switch:
`TIMEFUSION_ROLLUP_NOOP_SKIP_ENABLED=false`.

Expected: removes ~72.6% of BaseRollup decoded bytes ≈ ~43% of maintenance
worker-seconds **at this process's mix**. It does not touch rebuilds triggered by
compaction, which really does move the paths — that is a separate, later lever.

It also does **not** unwedge the derived tier. That is fix 2.

## Fix 2 — stop minting a rebuild that cannot help (NEXT)

The `skipped_generation` mint fires even when current base coverage already
spans the excluded file's range, and in that case the exclusion is harmless: the
derived unit already has a real safety net one step further on, which refuses to
publish when `uncovered(slice, base_covered)` finds a hole. The mint is a second,
redundant guard, and it is the one that livelocks.

Proposed: mint and retry only for excluded ranges that current base coverage does
**not** already reproduce (`ranges_cover(&base_covered, range)`, remembering that
its `hi` is inclusive while a slice end is exclusive). Non-destructive — it
retires nothing, it only stops demanding a rebuild that provably cannot change
the outcome, and lets the existing hole check make the publish/retry call.

The stale file stays live as garbage. Retiring it — extending the replace-set to
obsolete-generation files whose range is covered by current-generation live files
— is a separate, data-removing change and should not ride along with this one.

## Not yet investigated

- `pack_value_refused_rows = 74,058,028,385` against 27,620 refusals. Unread.
- `dedup_skipped_pct = 0.1%` still: `cert_slice_files_proved` 10,693 vs
  `files_unproven` 99,513, `cert_skip_blocked_overlap` 120,321. The read-side
  dedup skip remains blocked by overlap, as on 09-05.
- pgwire `p50 = 402ms`, `p95 = 14.9s`, `p99 = 36s`, `p999 = 69s`.
- `wide_scan_oversize_total = 19,831`, selected p50 1,483 MB per scan.
