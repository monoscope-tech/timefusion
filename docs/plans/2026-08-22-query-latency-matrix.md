# Query latency across 3d/7d/30d — measured, and what actually blocks each shape

Prod `b6d8c86`, 2026-08-22, read-only. Goal: fast queries across 3d/7d/30d for
different projects and different filter types. This is the baseline that
decision-making should start from, plus the attribution of every slow cell to a
named blocker.

## The matrix

Wall-clock ms, `count`-shaped outputs, `rep2` (warm) unless noted. Projects:
`whale` = `dcad860a…`, `mid` = `28f62f01…`, `small` = `d062e010…`.

| shape | whale 3d | whale 7d | mid 3d | mid 7d | small 3d | small 7d |
|---|---|---|---|---|---|---|
| `count(*)` | 402 | 6,121 | 1,405 | 17,044 | 212 | 1,651 |
| `trace_id =` (needle) | 170 | 78 | 2,153 | 79 | 227 | 90 |
| `level = 'ERROR'` | 1,427 | 12,539 | 2,423 | — | 239 | 308 |
| `service_name =` | 131 | 682 | 1,735 | — | 410 | 1,058 |
| `name LIKE 'GET%'` | 2,387 | 4,021 | 4,298 | — | 470 | 173 |
| `time_bucket(1h)` group-by | 2,512 | 10,570 | 5,085 | — | 698 | 355 |
| `ORDER BY ts DESC LIMIT 100` | 333 | 255 | 221 | — | 226 | 168 |
| `kind IN ('server','client')` | — | **22,753** | — | — | — | — |

## 30 days, which is where it breaks

`fail` = `ERROR: canceling statement due to statement timeout` at ~60 s — with
the client's `statement_timeout` set to 90 s and then 120 s. That is **not a
bug**: `DEFAULT_MAX_STATEMENT_SECS = 60` (`timefusion_pgwire_max_statement_secs`)
and `effective_statement_timeout` takes `min(client, server)`, so a client cannot
raise its own ceiling past the server's. The cap is deliberate and tested. What
it means for this table is simply that these queries genuinely exceed 60 s — the
server is reporting the failure, not causing it.

| shape | whale 30d | mid 30d | small 30d |
|---|---|---|---|
| `count(*)` | **fail** | **fail** | 5,814 |
| `trace_id =` (needle) | 95 | 200 | 75 |
| `level = 'ERROR'` | **fail** | **fail** | 221 (0 rows) |
| `service_name =` | 176 (0 rows) | 486 (0 rows) | 9,206 (307k rows) |
| `name LIKE 'GET%'` | **fail** | **fail** | 512 (0 rows) |
| `time_bucket(1h)` group-by | 36,942 | **fail** | 3,148 |
| `ORDER BY ts DESC LIMIT 100` | 431 | 931 | 152 |

The split is not 3d-versus-30d. It is **aggregate versus needle, and it holds at
every window**: the needle and TopK rows stay double-digit-to-few-hundred ms
across 3d, 7d and 30d, while every aggregate degrades until it stops returning.
`small` is the control that rules out "the whale is just big" — 1.8M rows over
30d still costs 5.8 s to count and 9.2 s to filter by service name.

Two things the 3d/7d table says immediately:

- **Point lookups and TopK are solved** (78–330 ms at every window). The tantivy
  + bloom + sorted-footer work landed; nothing here needs more of it.
- **Everything that AGGREGATES or filters on a non-leading column is slow**, and
  it degrades superlinearly with the window: whale `count(*)` goes 402 ms → 6.1 s
  from 3d to 7d; the 1h group-by 2.5 s → 10.6 s.

Cold (`rep1`) is 3–10x worse than warm on the big cells — whale 7d `level=`
measured 39.4 s cold against 12.5 s warm. Any number quoted from a single rep is
a cache measurement, not a query measurement.

## Where the time goes on the raw path

`EXPLAIN ANALYZE`, whale 3d 1h group-by (~2.4 s wall):

```
AggregateExec  partial          elapsed_compute=160ms   output_rows=73
FilterExec                      elapsed_compute=53ms    7.34M → 7.34M (100%)
DedupExec                       elapsed_compute=822ms   input_rows=9.03M → 7.34M
SortPreservingMergeExec         elapsed_compute=502ms   output_bytes=930MB
DeltaScanExec                   elapsed_compute=238ms   9.02M rows, 60 files
DataSourceExec                  row_groups 329 → 309 matched
```

**DedupExec + its SortPreservingMerge are 1.32 s of the ~2.4 s — 55%.** They are
there because the window holds **1.69M physical duplicates in 9.03M rows (23%)**:
merge-on-read versions from monoscope's enrichment UPDATEs. This is not waste
that can be deleted — it is version resolution, and it is correct.

Note `DedupExec`'s required input ordering is deliberate, not incidental: without
it `EnforceSorting` deletes the merge, keep-greatest degrades to keep-FIRST, and
a merge-on-read table answers with the PRE-update row. Removing the SPM to
reclaim its 502 ms would reintroduce a known correctness bug. Not a lever.

## Rollups are the lever, and they are routing NOTHING

```
rollup_hits_full_total    = 0
rollup_hits_hybrid_total  = 0
rollup_misses_total       = 155
rollup_min_contiguous_days = 30      <- coverage is NOT the blocker any more
```

Coverage reached 30 contiguous days. The blocker moved to routing, and it splits
four ways. Each of the four slow shapes was run against prod with the miss
counters diffed around it, so the mapping is measured, not inferred:

| shape | miss reason | count | owner |
|---|---|---|---|
| `kind IN ('server','client')` | `unknown_filter` | 33 | **fixed here** |
| `level = 'ERROR'` | `filter_not_eligible` | 31 | no rollup carries `level` |
| plain `count(*)` | `stale_coverage` | 49 | parallel session (per-slice witness) |
| `time_bucket` group-by | `unknown_filter` / `stale_coverage` | — | both of the above |

Caveat on the per-shape attribution: prod carries live monoscope traffic, so a
single-increment counter diff can be contaminated. The `unknown_filter` mapping
below is not — it is confirmed by the sampled plan text in the logs, matched to
the query text.

## Fix shipped: an OR-of-`text_match` is a hint, and the stripper could not see it

`rollup_promotion_unmatched`, prod, verbatim:

```
promoted = ((kind Eq "client") OR (kind Eq "server"))
           AND (text_match(kind,"client") OR text_match(kind,"server"))
```

and, for `select distinct project_id … kind in (?,?,?,?)` whose `IN` was already
consumed as a dimension filter:

```
promoted = (text_match(kind,"client") OR text_match(kind,"consumer")
            OR text_match(kind,"producer") OR text_match(kind,"server"))
```

`optimizers::tantivy_rewriter` ADDITIVELY ANDs `text_match` hints beside a
predicate it can accelerate, and by its own stated invariant never removes the
original comparison — so a hint is always semantically redundant.
`strip_index_hints` already dropped them, but only when the conjunct's TOP node
was a `text_match` call. The `IN`-list spelling expands to an **OR of per-item
`text_match` calls**, which is a `BinaryExpr(Or)` at the top and therefore
invisible to it. The hint survived into the promoted filter, matched no declared
measure, and the whole query declined `unknown_filter`.

The fix recurses `hint_column` through `Or`, accepting the subtree only when
EVERY leaf hints the SAME column — a mixed OR is a real predicate and dropping it
would widen the filter. The existing guard still applies on top: a hint is only
dropped when this AND level already compares that column directly, so a
`text_match` the user wrote against some other column is preserved and the filter
correctly fails to match.

Regression test `an_in_list_hint_or_tree_does_not_become_a_residual`, which fails
before the change with `hinted=Err(UnknownFilter)` against `plain=Ok(routed)` —
and the plain control routes through `row_filters: ["kind = 'server' OR kind =
'client'"]`, so this shape genuinely serves from the 1h tier once the hint is
gone. The blocked query measured **22.8 s** at 7d.

## Also shipped: the manifest age bound could not fire

Last session's 60-second durability bound is not merely unobserved — it is
**broken, and prod says so**: `tantivy_backfill_built = 2` next to
`tantivy_manifest_commits = 0` at 44 minutes of uptime, well past the bound.

Both `full` and `stale` were evaluated only for the project whose build had just
completed (`pending.get(&pid)`, `pending_since.get(&pid)`). A project with a
single build per pass therefore set its `pending_since` once and was never
re-examined — nothing else could flush it, so it waited for a pass end that on
this box has never arrived. That is the same failure the bound was added to fix,
reintroduced per project.

`due_manifest_flushes` now sweeps every pending project on each build completion.
It is a pure function, so the case is a unit test rather than another prod
observation. A build costs 4-5 minutes, so a whole-map sweep runs at most every
few minutes and its cost is noise against that.

## Also shipped: the prefilter skip breakdown

`prefilter_skipped` was incremented from two call sites — `decide_prefilter`'s
three exits and the search-abort branch — so the standing "63% of prefilter
attempts are skipped and thrown away" could not be attributed to a decision at
all. Six named counters now split it; the first three are decisions (the index
answered, the rule declined it), the rest are the index failing to answer, and
the fixes are opposite.

## What is NOT worth doing, with the measurement that says so

- **Removing DedupExec's SortPreservingMerge** (502 ms, 930 MB). Its required
  ordering is what keeps keep-greatest from degrading to keep-first under
  merge-on-read. Correctness, not overhead.
- **A provider cache for file-pruned scans** — `pruned_build_us` is 0.10 ms,
  0.1% of the pruned-scan path. Already recorded; restated because it keeps
  looking attractive.
- **Adding `level` as a rollup dimension** to fix `filter_not_eligible`. It would
  work, but it is a spec change requiring a full rebuild across 30 days, which is
  wall-clock physics and not an overnight change.

## Post-deploy verdict (`ebfa7e0`)

**The hint strip works, confirmed directly.** `rollup_promotion_unmatched` now
logs a clean promoted filter — `((kind Eq "client") OR (kind Eq "server"))`, the
`text_match` OR gone — and the `select distinct project_id … kind in (?,?,?,?)`
query that monoscope fires every 5 minutes went from one unmatched decline per
spec per run to **zero**.

**The prefilter breakdown works, and it refutes the standing hypothesis.**
`prefilter_attempts=65, used=27, skipped=38`, and the 38 splits as
`no_index_or_cap` **29 (76%)**, `low_selectivity` 9 (24%), `field_coverage_gap`
**0**, everything else 0. They sum exactly. `field_coverage_gap` was written into
`pg_compat.rs` as the most plausible cause and it is flatly zero — the wasted
fan-out is the index not being there, not a rule declining a usable one. That
sends the fix back to coverage, not to `min_selectivity_pct`.

**Everything I measured about ROUTING after the deploy is invalid**, and this is
worth more than the measurements were:
`rollup_min_contiguous_days` read **0** on the five-minute-old container against
**30** before the restart. The gauge is process-scoped and rebuilds after boot.
With coverage empty the router does not attempt routing at all — so a novel
group-by shape (fresh literals, fresh bucket width, past the plan cache) produced
**neither a hit nor a miss**, and `rollup_misses_total` sat flat at 3 across
several distinct queries. A flat miss counter after a deploy is an artifact of
the restart. **Absence of a miss is not evidence of a hit.** Read
`rollup_min_contiguous_days` before quoting any routing number.

So "does anything route now" is still open, and needs a re-measure once coverage
is back at 30 days. Note also that `kind IN ('server','client')` has a second,
independent blocker even with the hint gone: no declared measure matches that
filter.

`tantivy_manifest_commits` is likewise unverifiable on a young container — the
reconcile cron fires at minute 20 and `tantivy_backfill_built` was still 0.

## The routing re-measure, with valid coverage

Run at 17:03 UTC on `5062a7d` (which contains `ebfa7e0` — verified with
`git merge-base --is-ancestor`), 20 min uptime, with
`rollup_min_contiguous_days = 30` restored. Counters diffed around each query.

| shape | before | now | miss reason | verdict |
|---|---|---|---|---|
| `kind IN ('server','client')` 7d | 22,753 | 21,186 | `unknown_filter` | unchanged |
| 1h group-by 3d | 2,512 | 2,902 | `stale_coverage` | unchanged |
| 1h group-by 7d | 10,570 | 6,077 | `stale_coverage` | cache, not routing |
| plain `count(*)` 7d | 6,121 | 5,424 | `stale_coverage` | cache, not routing |

`rollup_hits_full_total` and `rollup_hits_hybrid_total` are **still 0**. The
timing movement is warm-cache variation, not routing — every one of these queries
recorded a miss.

**Two corrections to what this page said earlier.**

*First*, the claim that the dashboard group-by "never reaches the router" was
wrong twice over: it was measured with coverage at 0, and it used a
`SELECT count(*) FROM (…)` wrapper whose outer aggregate changes the plan shape.
The **bare** group-by — the shape monoscope actually issues — does reach the
router, and misses `stale_coverage` every single time.

*Second*, and this is the useful result: **`stale_coverage` is the sole blocker
for both the dashboard group-by and plain `count(*)`.** Three separate bare
shapes, three `stale_coverage` misses, no other reason. That is the largest
latency item on this page and it is the per-slice witness change already in
flight from a concurrent session — not something to duplicate.

**On `kind IN`, the hint fix did what it claimed and it is not enough.** The
promoted filter is clean in prod — `((kind Eq "client") OR (kind Eq "server"))`,
hint gone — and the `distinct project_id … kind in (?,?,?,?)` shape stopped
declining entirely. But this query still declines `unknown_filter`, because the
clean filter matches no declared count measure: the specs declare
`request_count` (unfiltered), `error_count`, and three `server_*` measures keyed
on `kind='server' OR name IN (…)`. Nothing covers `kind IN ('server','client')`.
The fix removed one of two layers, which is exactly the caveat recorded when it
shipped. Closing this one needs a declared measure, i.e. a spec change.

## Why it took three attempts to measure this

Prod restarted twice during this session — `ebfa7e0`, then `5062a7d` fourteen
minutes later — and each restart zeroes `rollup_min_contiguous_days`, which gates
whether the router attempts anything at all. Coverage took **~25 minutes** to
rebuild, during which every routing measurement is an artifact: with coverage at
0 the router does not attempt, so shapes record neither a hit nor a miss and it
reads exactly like "my fix did nothing".

**On this box the gauges that answer "did the fix work" can take longer to
rebuild than the interval between deploys.** Read `rollup_min_contiguous_days`
first; if it is 0, the measurement has not started. The compaction chart shows
the same constraint from the write side — six sealed days frozen because units
are re-claimed rather than finished.

## `rollup_coverage` has no producer — the date-level path is dead code

This corrects the section that stood here before, which said the date-level map
was "not persisted" and was repopulated by the maintenance planning pass. It is
worse and simpler than that.

`rollup_coverage` is a **private field**. Here is every use of it in the crate:

```
src/database/mod.rs:2259      declaration
src/database/mod.rs:2926      DashMap::new()          <- construction
src/database/mod.rs:3648      .get(&key)              <- read, in the routing path
src/database/maintain.rs:2277 .remove(&key)
src/database/maintain.rs:2333 .retain(...)
```

Two reads and two removals. **No insert, no `entry`, no `or_insert` — nothing
ever writes to it.** A private field with no producer cannot be non-empty, so
`self.rollup_coverage.get(&key)` at the routing site returns `None` for every
date, forever, on every process.

`maintenance_coordinator.rs:1164` states "`recover_rollup_coverage` is the only
producer and it runs ONCE at [boot]". That function writes only
`rollup_slice_coverage` (two inserts, both to the slice map). The comment
describes behaviour that no longer exists, and `maintain.rs:2572` documents the
same vanished mechanism — "each stored `(date, generation)` is re-proved against
the current source partition's generation". Nothing is stored, so nothing is
re-proved. The insert was almost certainly lost in a refactor.

**Why this matters more than it looks.** It is not a wrong-answer bug — the
`None` branch deliberately `continue`s without setting a miss, so the query falls
through to slice coverage and is correct, just unrouted. But it means:

- **Every routed query in production depends solely on slice coverage**, and
  always has — not merely for 25 minutes after a restart, as the previous version
  of this section claimed.
- Slice coverage is precisely the path gated by the per-slice witness rule, which
  is what returns `stale_coverage` on every bare dashboard shape measured
  tonight. **The two findings are one finding.** There is no second route to fall
  back on, because the second route is inert.
- `rollup_ticket_current` can only ever return `false` for a non-empty
  `ticket.dates` — consistent, because `ticket.dates` is only pushed inside the
  branch that the dead `.get` guards, so it is always empty.

**Not fixed here, deliberately.** Restoring the producer means writing coverage
identity (`source_fp`, `source_epoch`, `generation`, `covered_through`) that the
read path trusts to decide a rollup may serve a range. Getting it wrong serves
rows the rollup never aggregated — the silent-wrong-number failure this module's
own comments warn about twice. That needs a failing test that exercises a commit
→ read cycle, and a verification loop that costs ~25 minutes per iteration on
prod. It is the wrong change to make unsupervised overnight. It is, however, the
single highest-value item on this page.

## The stale_coverage split, measured: 95% is unverifiable, not moved

Captured 17:52 UTC on `ba87ed3` with `rollup_min_contiguous_days = 30`, after
three bare dashboard shapes:

```
rollup_stale_no_witness      = 1567   (95.2%)
rollup_stale_moved           =   78    (4.7%)
rollup_stale_no_source_rows  =    0
rollup_hits_full_total       =    0
```

**It is a throughput wall, not a correctness wall.** The slices exist and cover
the days. They are refused because they were written before `TAG_SOURCE_ROWS` and
so cannot be verified — `slice_coverage_agrees` refuses a `None` witness by
design. Only 4.7% are partitions that genuinely moved, which is the irreducible
share under live ingest.

Combined with the dead date-level map above, the whole picture closes:

1. Date-level coverage would not be subject to the witness rule at all — but it
   has no producer, so it never contributes.
2. Every query therefore depends on slice coverage.
3. 95% of slice coverage is unverifiable and refused.
4. It clears only when the coordinator republishes those slices, which needs the
   same starved maintenance capacity that has left six compaction days frozen
   bit-identical for 58 hours.

**Rollup routing and frozen compaction are the same problem.** Both are units
that need to run and do not.

### Two ways out, and the choice is a real one

**(a) Republish throughput.** Correct by construction — a republished slice
carries a witness and verifies normally. But 1,567 slices against a maintenance
pipeline whose units are re-claimed rather than finished (30 of 31 sealed starts
already on `attempts > 3`) is exactly the convergence problem the compaction
chart documents. This is wall-clock physics; it cannot be compressed.

**(b) Fall back to `source_fp` for witness-less slices — BUILT, MEASURED, AND
REMOVED. It cannot work.** Slice coverage carries a `source_fp` and the
date-level path verifies with a field of the same name, so trusting a
witness-less slice whose fingerprint still matches looked like it would unlock
most of the 1,567 immediately.

The two fingerprints are **incomparable**, in two independent ways:

| | slice `source_fp` (`maintain.rs:1480`) | partition `fingerprint` (`partition_file_fp`) |
|---|---|---|
| hasher | `FnvHasher` | `DefaultHasher` (SipHash) |
| input | files selected **for that slice** | the partition's **whole live file set** |

Either difference alone makes equality impossible. Implemented behind a flag and
tested end-to-end: with the flag on, a stripped-witness slice routed **nothing**
— the comparison fails safe as a permanent miss. The parity test is what caught
it; the unit tests over the predicate all passed, because the predicate was
correct and its *inputs* were meaningless.

Removed rather than left disabled. A flag that does nothing is worse than no
flag: it reads as an available lever. The finding is recorded in the read path
itself so the next person does not re-derive it.

That leaves **(a) republish throughput** as the only route, which is the same
wall as frozen compaction.

*Method note.* The unit tests passed and the integration test failed, and the
integration test was right. Testing a pure predicate proves the predicate; it
says nothing about whether the values fed to it mean what you think. Both
fingerprints are `u64` named `source_fp`, so the type system had nothing to say
either.

## 2026-08-23: routing works for the first time — and latency did not move

`937350c` restored the `rollup_coverage` producer. First read after the deploy:

```
rollup_hits_full_total   = 936      (was 0, always)
rollup_hits_hybrid_total = 691      (was 0, always)
rollup_misses_total      = 1512
```

**Hits now exceed misses.** The date-level route does not consult the per-slice
witness at all, so it is unaffected by the 95.2% of slices that cannot be
verified — which is exactly why it was the right thing to fix first.

**But the latency measurement is worthless, and the controls prove it.** The
routed shapes read 3d group-by 9.5-11.6 s (baseline 2.5 s) and 7d group-by
6.9-7.9 s (baseline 10.6 s). Before reading anything into that, the in-round
controls — shapes that do not route at all:

| control | baseline | now |
|---|---|---|
| `trace_id =` needle 7d | 78-170 ms | 215-225 ms |
| `ORDER BY ts DESC LIMIT 100` | 255 ms | 932 ms |

TopK is 3.6x slower and it cannot have been touched by rollup routing. The cause
is on the same page: `cpu_tokens_used = 16` of 16, and `pending_base_rollup`
climbed **7,026 → 12,247 in ~25 minutes**. The box is saturated draining its own
queue, so every number in that window is a load measurement, not a query
measurement.

So: routing is achieved and verified; the latency claim is **not yet made**. It
needs the quiet window CLAUDE.md already prescribes (≥2 h, no redeploys), which
this box has not had all session.

## The queue is the live problem, and it is not what task #2 assumed

`abandon_running` ALREADY does what "stop re-claiming doomed units" asks: it
bisects at `attempts >= 2` and otherwise backs off floored at the operation's own
deadline, with comments citing the 2026-08-18 and 2026-08-21 incidents that
prompted it. And `retry_reason = compaction_debt_remaining` is a by-design
requeue, not a failure — memory records that `attempts` is a LOOP COUNTER for
Repair, so the "30 of 31 on attempts > 3" reading from the compaction chart does
not mean what it appeared to.

What the current binary actually shows:

```
pending_base_rollup            12,231      eligible_base_rollup     6,892
pending_dedup                   6,024      dirty_bin_queue_depth   18,904
pending_sealed_consolidation       67      eligible_sealed_total    9,736
oldest_task_age_seconds     7,166,084  = 83 days
rollup_stale_moved            222,410      rollup_stale_no_witness      0
```

Two things to chase, in order:

1. **Is the queue real?** Memory `tf_queue_is_339x_inflated` records 88,100
   pending resolving to 260 real cells — a 339x inflation from shredding days to
   the 1m floor. If 12,231 is similarly inflated, this is a de-shredding problem,
   not a capacity one, and adding workers would do nothing.
2. **Is it a feedback loop?** `rollup_stale_moved` at 222,410 says partitions are
   moving constantly. Every compaction or dedup rewrite changes a partition's
   fingerprint, which invalidates its rollup coverage, which enqueues a rebuild.
   With 18,904 dirty bins that loop would mint rollup work faster than it can be
   drained, and no amount of rollup throughput would converge. That would make
   compaction and rollup one problem in a far more precise sense than "they share
   a worker pool".

Neither is settled. `timefusion sim <journal.json>` over the real prod journal
answers both offline, in seconds, without a deploy — that is the next step, not
another prod experiment.

## Open, in priority order

1. `stale_coverage` (49 misses, and it owns plain `count(*)`) — the per-slice
   witness change in flight.
2. `filter_not_eligible` (31) — `level` / `status_code` are not rollup
   dimensions. Needs a spec decision, then a rebuild.
3. Read the new `prefilter_skipped_*` rows once prod has an hour on them.
