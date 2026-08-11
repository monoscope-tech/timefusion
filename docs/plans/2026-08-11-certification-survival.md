# Making dedup certification survive — measure before you build

**Status:** **Phase 0 and Phase 1 are both built.** Phase 0's counters are live — see "Reading
Phase 0". Phase 1 is shipped dark behind `timefusion_dedup_certification_persist` (default
off) and **must not be enabled until Phase 0 has been read** — see "Turning it on". The
remaining work is a measurement, not code.

The one change that is unconditionally on is the sweep fix (finding 3 below), because it is a
bug rather than an optimisation. It may move the numbers on its own, so read Phase 0 after it
has been live for a day before judging whether persistence is worth enabling at all.

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

## What building Phase 0 changed about the question

Three things surfaced while implementing it. The first two mean the 2026-08-11 numbers above
**overstate the certification problem**; the third is a candidate fix far cheaper than Phase 1.

1. **Two denials were being charged to certification that are not about it.** A query with no
   usable time bound (`NoWindow`) and a table not yet in the fast-resolve cache
   (`Unresolved`) both landed in `dedup_denied_uncertified`. Neither is a partition failing to
   be certified, and `Unresolved` in particular is common on a cold process — the very
   condition that follows every deploy. They are now separate counters, and the exit criteria
   below are stated over `never_certified` vs `fp_moved` **only**.

2. **The dedup counters were blind on the no-buffered-layer scan path.** `record_scan` gates
   every one of them on `has_delta`, and that path never tagged it. Prod runs with the buffer
   so the numbers above stand, but any deployment without it reported zero eligible scans no
   matter how many it served — and `query_delta_only` takes that path, which is why no test
   could observe a skip. Fixed in the same commit.

3. **A sweep pass cannot certify what it just rewrote, and the confirming pass may never
   run.** `record_certification` requires `dropped == 0` over an unmoved file set, so a pass
   that drops duplicates certifies nothing — by design; the next 0-drop pass is meant to
   confirm the rewrite held. But `dedup_today_partitions` returns immediately while the
   table's version is unchanged (`src/database.rs`, the `last_dedup_versions` guard), and the
   rewriting pass is the last thing that moved it. So the confirming pass runs only once some
   *other* commit bumps the table version — and on a quiet table that may be never.

   In production continuous ingest supplies that bump, so this was not a stall; it was a
   **systematic delay concentrated exactly on the partitions that had duplicates**, which are
   the ones the skip is most worth having on. On a quiet table the confirmation may never have
   arrived at all.

   **Fixed** (`fix(dedup): let the confirming sweep pass run…`): the sweep no longer records
   the table version after a pass that dropped rows, so the confirming pass runs off the back
   of the rewrite. This is a far smaller change than persistence and may recover much of the
   same ground — which is why Phase 0 should be re-read after it has been live for a day.

## Reading Phase 0

```sql
SELECT key, value FROM timefusion_stats WHERE component = 'scan' AND key LIKE 'dedup_denied%';
SELECT key, value FROM timefusion_stats WHERE component = 'scan' AND key LIKE 'cert_%';
```

Counters are cumulative since process start and **a deploy resets them**, so read after ≥ 24h
with no deploy; diffing two scrapes is only valid inside one process lifetime. TF deploys
several times a day, so this needs deliberately arranging — during the 2026-08-11 measurement
two restarts inside 20 minutes drove a rate-based scrape negative.

The rows that answer the question:

| row | what it means |
|---|---|
| `dedup_denied_never_certified_pct` | the headline: share of *certification* denials a warm, persistent cache could convert |
| `dedup_denied_fp_moved` | the irreducible floor |
| `dedup_denied_no_window` / `_unresolved` | denials this feature never owned; exclude from the ratio |
| `cert_dwell_p50_secs` / `_p90_secs` | how long a certification survives before a write moves it |
| `cert_granted_total` | how many were ever granted — a small number here makes every ratio above noise |

`cert_dwell_*` is an **upper bound** on true lifetime: a flush moves a fingerprint without
touching `dedup_clean_fp`, so the interval closes when a read first trips over the stale entry,
not when it actually died. The bound errs toward making persistence look *better* than it is,
so a short measured dwell is conclusive; a long one is not.

## Phase 0 — the measurement that decides it (half a day)

> **Built.** `DedupSkipVerdict` in `src/database.rs` carries the reason from
> `dedup_window_clean` through to `record_scan`; `src/stats_table.rs` surfaces it. Covered by
> `a_denied_skip_says_whether_it_was_never_certified_or_written_to_since`, which walks one
> partition through never-certified → certified → written-to and asserts each counter in turn.
> The sketch below is retained because it records the reasoning, not the remaining work.

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

Stated over `never_certified` and `fp_moved` only — `no_window` and `unresolved` are denials
this feature never owned and must not be counted into either side.

- **`denied_fp_moved` dominates (say >70%)** → **stop.** Partitions are being written
  continuously; certification cannot hold no matter where it is stored. Close this out, leave
  the flag on (it is harmless at 0.5%), and consider whether `timefusion_read_dedup_skip_swept`
  should simply default to `false` again to remove a live code path that earns nothing.
- **`denied_never_certified` dominates AND dwell time is long** → the prize is real. Before
  Phase 1, rule out finding (3) above: if partitions are uncertified because the confirming
  sweep pass never ran, fixing the sweep is a fraction of the cost of persisting anything.
- **`denied_never_certified` dominates but dwell time is short** → persistence alone will not
  help; the sweep cadence or its lookback is the lever instead (`timefusion_dedup_lookback_days`),
  which is a much smaller change than Phase 1.

Expected result, stated so it can be falsified: I expect `fp_moved` to dominate for recent
dates and `never_certified` to dominate for sealed ones — i.e. the win is available only on
historical windows, which are the least latency-sensitive queries we serve. If that is what
comes back, the correct decision is to stop.

## Phase 1 — BUILT, and OFF

Both levers are implemented and shipped dark. Neither changes behaviour until
`timefusion_dedup_certification_persist` is turned on, except the sweep fix, which is
unconditional because it is a correctness-shaped bug rather than an optimisation.

**The sweep fix (finding 3) is on.** `dedup_today_partitions` no longer records the table
version after a pass that dropped rows, so the confirming 0-drop pass runs off the back of the
rewrite instead of waiting for an unrelated commit. Covered by
`a_rewriting_sweep_is_confirmed_by_the_next_pass_with_no_other_commit`. **Read Phase 0 again
after this has been live for a day before judging persistence** — it may move
`never_certified` on its own, and if it does, the persistence layer below is solving a problem
that no longer exists at the measured size.

**Persistence is built as option A, adapted, and defaults OFF.** `src/certification_store.rs`
snapshots `dedup_clean_fp` to the data dir at the end of each sweep and reloads it at boot.

*Deviation from the A3 recommendation above, deliberately.* A3 proposed object-store markers
with a lazy read on first miss. The lazy read is the problem: `dedup_window_clean` is
synchronous and sits on the scan planning path, so a first-miss GET would put object-store
latency in front of the 99.5% of scans that are *not* certified — paying on the common path to
speed up the rare one. Making it async instead would push that await onto every scan. So this
persists to `timefusion_data_dir` (a persistent volume; the same place
`dirty_bin_queue` already keeps cross-restart state, and the same tmp+rename helper), which is
a local file read at boot and no IO at all on the read path.

What that gives up is sharing between replicas. TF runs one instance per box, and a
certification is about a partition's file list — so a marker written by another replica would
be valid, just unavailable. If TF ever runs multi-replica AND Phase 0 says the prize is large,
promote the store to option A3's object-store keys; the seam is `certification_store::{load,
store}` and nothing else has to change.

**The safety property that makes this cheap:** loading cannot widen certification. A reloaded
entry passes the same `partition_file_fp` equality check against the live file list as an
in-memory one, so a stale, truncated, or corrupted file costs a skip and can never grant a
wrong one. That is why this is a snapshot-and-reload rather than a transaction.

Covered by `a_certification_survives_a_restart_and_still_grants_the_skip` — which fails with
the flag off, so it is testing persistence and not something incidental.

### Turning it on

Read the Phase 0 rows first. If `dedup_denied_fp_moved` dominates, **leave it off** and close
this out: partitions are being written to continuously and no store recovers that. Only if
`never_certified` still dominates *after* the sweep fix has been live for a day is there
anything for persistence to recover.

## Phase 1 — the options as originally written

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
  is the parity gate, and it did **not** exercise the skip. Fixed; it now runs a bounded window
  and asserts `dedup_skipped` moved, so it can never silently go back to proving nothing.
  Getting it to engage took four separate things, each of which will bite the next person:
  - a **timestamp predicate** — `dedup_skip_allowed` returns `NoWindow` without one;
  - a **warm fast-resolve cache** — `pre_skip_dedup` reads `try_fast_resolve`, and cold it
    declines as `Unresolved` before consulting any certification. Issue a throwaway query
    first. Three earlier attempts were green against broken code for exactly this reason (see
    `a_certified_partition_with_buffered_rows_still_answers_without_the_keys_projected`);
  - **not `count(*)`** — `count_pushdown` answers a bare count from Delta statistics without
    building a scan, so it exercises neither `DedupExec` nor the skip. Count the rows a
    projection returns instead;
  - **a commit between the two sweeps** — see finding (3) above.
- The restart test now exists: `a_certification_survives_a_restart_and_still_grants_the_skip`
  certifies, drops the `Database`, rebuilds it over the same data dir and asserts the skip
  still fires. It fails with `timefusion_dedup_certification_persist` off, which is what makes
  it a test of persistence rather than of something incidental.
- `count_pushdown_declines_where_tombstones_are_possible` used to assert `DedupExec` survived.
  It held only while the partition happened never to be certified; once it is, the read-side
  skip legitimately removes `DedupExec` and the tombstone filter — which sits outside dedup —
  still gives the right answer. It now asserts a real scan survives, which is what
  "count_pushdown declined" actually means. Worth knowing about, because anything that raises
  the certification rate will trip assertions written against the operator rather than the
  answer.

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
- `878a36c` (2026-08-11) — Phase 0: the denial split, certification dwell, the `has_delta`
  instrumentation gap, and a parity test that now exercises the skip it gates.
