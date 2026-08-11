# Making dedup certification survive — measure before you build

**Status:** proposal, Phase 0 not started. Nobody should write Phase 1 code until Phase 0
returns a number.

**Owner:** unassigned. Prerequisite reading: `record_certification` and `dedup_window_clean`
in `src/database.rs`, and `docs/plans/2026-08-09-per-date-dedup.md`.

## The one-paragraph version

The swept-partition dedup skip (`timefusion_read_dedup_skip_swept`, enabled by default in
`f1f0b90`) is supposed to stop decoding dedup-key columns out of Parquet on windows a sweep
has proved duplicate-free. Measured on prod 2026-08-11 it fires on **0.2–0.5%** of the scans
that read Delta. The gate is not the MemBuffer and not the plan shape — it is that
partitions are almost never *certified* at read time. The lever, if the byte win is ever
wanted, is making certification survive. But the size of that prize is unmeasured, and there
is a cheap experiment (Phase 0) that will either justify the work or kill it in a day.

## Evidence

From `timefusion_stats WHERE component='scan'`, two independent process lifetimes on
2026-08-11, shortly after `d0d5757` shipped the counters:

| counter | run A | run B |
|---|---|---|
| `dedup_eligible` (scans that read Delta) | 410 | 432 |
| `dedup_skipped` | 2 (0.5%) | 1 (0.2%) |
| `dedup_denied_uncertified` | 408 | 431 |
| `dedup_denied_by_leg` | **0** | **0** |

Scan-source mix at the same time: `mem_only` 42%, `mem_plus_delta` 39%, `delta_only` 18%.

Two things follow, and the second one is the surprise:

1. The skip does essentially nothing today.
2. **The MemBuffer is not why.** `dedup_denied_by_leg` is flat zero. The prior hypothesis —
   that the union path (which cannot skip) was eating the win because 80% of scans touch the
   buffer — is wrong. Every denial is `pre_skip_dedup == false`, i.e. the window was never
   eligible in the first place.

## What NOT to do

**Do not restructure the projection decision.** The obvious-looking redesign — resolve the
table above the projection so one verdict binds every leg, instead of `pre_skip_dedup`
predicting at plan time what the legs will later decide — targets a race that is not the
binding constraint. `denied_by_leg == 0` says the legs are not the ones refusing. It would be
a moderately invasive change to the read path for a ceiling of ~0.5%.

(That two-decision split *was* a real bug — it caused the 2026-08-09→11 outage where any scan
projecting away `id` failed with `DedupExec key 'id' not in input schema`. That is fixed in
`f8d7278` by keeping the keys in the scan unconditionally. The cost of that fix is bounded by
the same 0.5%, so it is effectively free, and it is not what this document is about.)

## The actual constraint

`dedup_window_clean` (`src/database.rs`) certifies a window only if **every** in-window
`(project_id, table, date)` partition has an entry in `dedup_clean_fp` whose value equals
`partition_file_fp(current files)`.

```rust
dedup_clean_fp: Arc<dashmap::DashMap<(String, String, String), u64>>   // (pid, table, date) -> fp
```

Three properties make it fragile:

1. **It is process-local memory.** Created empty in `Database::new`. Nothing loads it at
   startup. Every restart begins with zero certified partitions.
2. **It only fills from a sweep.** `record_certification` inserts only when
   `dropped == 0 && complete && !post.is_empty() && fp(pre) == fp(post)` — a 0-drop, complete
   pass over a file set that did not move under it. `dedup_today_partitions` covers today plus
   `timefusion_dedup_lookback_days`; `certify_partition` covers sealed days for the rollup
   backfill.
3. **Any commit invalidates it.** `partition_file_fp` hashes the sorted file-path list, so a
   single flush into that partition moves the fingerprint and the entry stops matching.

TF deploys several times a day. While measuring this I lost a rate-based scrape to two
restarts inside 20 minutes (counters went negative). So in production the cache spends much of
its life cold, and the partitions most queried — recent dates — are exactly the ones still
receiving writes.

That last point is why this may be structurally unwinnable, and why Phase 0 exists.

## Phase 0 — the measurement that decides it (half a day)

**Question:** of the denials, how many would a *warm, persistent* cache have converted?

Split `dedup_denied_uncertified` into two causes at the point of denial:

- **`denied_never_certified`** — the `(pid, table, date)` key is **absent** from
  `dedup_clean_fp`. This partition was never certified in this process. Persistence and
  startup warming could recover these.
- **`denied_fp_moved`** — the key is **present but the fingerprint differs**. The partition
  was certified and has since been written to. This is the irreducible floor; no amount of
  persistence recovers it, because the data genuinely changed.

Implementation sketch (mirrors `d0d5757`, which added the existing `dedup_*` counters):

- In `dedup_window_clean`, return a reason rather than a bare `bool` — or, to keep that
  function hot-path cheap, bump the counter inline at the `is_some_and` site around
  `src/database.rs:8471`. A partition that is absent and one that mismatches are already
  distinguishable there.
- Surface as `dedup_denied_never_certified` / `dedup_denied_fp_moved` in
  `src/stats_table.rs`, next to the existing rows.
- Also record **certification dwell time**: on each `record_certification` insert, and on each
  observed invalidation, accumulate how long the entry survived. A histogram bucket is enough.

**Read it after ≥ 24h with no deploy**, because the counters are cumulative since process
start and a restart resets them. Diffing two scrapes only works inside one lifetime.

### Exit criteria

- **`denied_fp_moved` dominates (say >70%)** → **stop.** Partitions are being written
  continuously; certification cannot hold no matter where it is stored. Close this out, leave
  the flag on (it is harmless at 0.5%), and consider whether `timefusion_read_dedup_skip_swept`
  should simply default to `false` again to remove a live code path that earns nothing.
- **`denied_never_certified` dominates AND dwell time is long** → the prize is real; proceed to
  Phase 1.
- **`denied_never_certified` dominates but dwell time is short** → persistence alone will not
  help; the sweep cadence or its lookback is the lever instead (`timefusion_dedup_lookback_days`),
  which is a much smaller change than Phase 1.

Expected result, stated so it can be falsified: I expect `fp_moved` to dominate for recent
dates and `never_certified` to dominate for sealed ones — i.e. the win is available only on
historical windows, which are the least latency-sensitive queries we serve. If that is what
comes back, the correct decision is to stop.

## Phase 1 — options, only if Phase 0 justifies it

### A. Persist the fingerprint

- **A1 — Delta commit metadata.** Record the fp in `commitInfo` on the sweep's own commit.
  Atomic with the log, no new storage. A metadata-only commit does not change the file list, so
  it does not invalidate the fp it is recording. Cost: log churn; readers must scan recent
  commits to rebuild.
- **A2 — sidecar Delta table** (`timefusion_certifications`, keyed by project/table/date).
  Queryable, easy to inspect and to backfill. Cost: another table in the maintenance path.
- **A3 — object-store marker.** One small key per certified partition,
  e.g. `_certified/<table>/<project>/<date>` holding the fp. Cheapest to write (one PUT per
  certification) and to warm (one prefix LIST at startup). No Delta involvement. Cost: a second
  source of truth that can drift from the log.

**Recommendation: A3, with a lazy read.** Do not warm the whole map at startup — that is an
unbounded LIST on a cold path. Read the marker on first miss for a `(project, table, date)` and
memoise into the existing `dedup_clean_fp`, so the in-memory map keeps its current role as a
cache and gains a durable backing store. Nothing about the hot read path changes shape.

### B. Derive certification from the log instead of remembering a verdict

Make cleanliness a property of the *data*: if every file in the partition is a sweep output and
nothing has landed since, the partition is duplicate-free by construction. See
`docs/plans/2026-07-31-content-addressed-compaction-outputs.md` — content-addressed outputs may
already give the marker this needs.

This is the most elegant answer: it deletes the cache-survival problem rather than solving it,
and it cannot go stale. It is also the largest change and the one most able to be subtly wrong.
Worth a spike only if Phase 0 shows a large prize.

### C. Warm at startup

The complement to A, not an alternative. Without A there is nothing to warm from.

## Correctness constraints — read before touching anything

- **`record_certification` is THE rule, and it is shared.** Its own doc comment says so: both
  the sweep and the rollup backfill certify through it, because *"a rollup built on a partition
  certified by a laxer rule is silently wrong"*. Any persistence layer must record exactly what
  that function decided — never re-derive a verdict at a different strictness.
- **A stale certification is a correctness bug, not a perf regression.** It makes `DedupExec`
  disappear over a partition that has duplicates, so counts over-report on every dashboard.
  Whatever stores the fp must be invalidated by the same rule that invalidates the in-memory
  entry: fingerprint equality against the live file list, checked at read time.
- **Do not weaken the fingerprint.** `partition_file_fp` hashes the sorted file-path list. It is
  deliberately conservative — any commit moves it. Making it coarser (e.g. hashing only row
  counts) to win hit-rate would trade correctness for throughput in the exact place the
  2026-08-02 duplicate-row incidents came from.

## Test strategy

- `dedup_compaction_test.rs` is the home. `count_is_identical_with_and_without_the_dedup_skip`
  is the parity gate — but note it has **no timestamp predicate**, and `dedup_skip_allowed`
  refuses without a window, so it very likely never exercised the skip it was gating. Fix that
  first, or any Phase 1 work ships behind a green test that proves nothing.
- **A skip test needs a warm fast-resolve cache.** `pre_skip_dedup` reads
  `try_fast_resolve`; cold, it is always false and the path is never reached. Issue a throwaway
  query against the project first, then assert. Three test attempts were green against broken
  code before this was understood — see
  `a_certified_partition_with_buffered_rows_still_answers_without_the_keys_projected`.
- Add a restart test: certify, drop and rebuild the `Database`, and assert the skip still
  fires. That is the whole point of Phase 1 and nothing currently covers it.

## Files

| file | why |
|---|---|
| `src/database.rs` | `dedup_clean_fp`, `record_certification`, `certify_partition`, `dedup_window_clean`, `partition_file_fp`, `record_scan` |
| `src/stats_table.rs` | the `scan` rows — where new counters surface |
| `src/config.rs` | `timefusion_read_dedup_skip_swept`, `timefusion_dedup_lookback_days` |
| `tests/suite/dedup_compaction_test.rs` | parity + skip coverage |

## History

- `f1f0b90` (2026-08-09) — enabled the skip by default.
- 2026-08-09→11 — every scan projecting away `id` failed with `DedupExec key 'id' not in input
  schema`; the service-map rollup was down for two days.
- `f8d7278` (2026-08-11) — fix: dedup keys always in the scan; `project_indices` restores the
  requested columns when the `DedupExec` that would have done it is skipped.
- `d0d5757` (2026-08-11) — the `dedup_*` counters this document's evidence comes from.
