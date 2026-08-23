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

### What a 10-minute sample actually showed, because it spanned a restart

One-minute interval, all 16 CPU tokens held throughout:

```
07:10 -> 07:16   commits 4,323 -> 4,326 (+3 in 6 min), output_rows +668
                 pending_base_rollup 12,231 -> 12,218 (-13)
07:17            EVERYTHING resets to 0 — restart onto a new deploy
```

Sixteen units running concurrently at ~45 commits/hr means **each unit averages
~21 minutes**, and the restart killed all sixteen in flight at once — up to ~5.6
worker-hours discarded in a single deploy. Prod took **8 deploys** during this
session.

**So the mechanism is not re-claiming, and it is not merely slowness: units take
longer than the interval between restarts, so in-flight work is destroyed before
it can commit.** `abandon_running` already bisects a unit that times out; nothing
handles a unit whose *process exits*. `oldest_task_age_seconds = 7,197,321` — 83
days — is the signature. A merely-slow unit finishes eventually.

That one mechanism explains all three standing symptoms: six sealed compaction
days bit-identical for 58 hours, 1,567 witness-less slices never republished, and
a queue flat-to-growing at 12k while the pool runs 100% busy.

The journal persists correctly — `dirty_bin_queue_depth` and
`pending_base_rollup` both survived the 07:17 restart to the unit. What is lost
is the work, not the plan.

### The real journal, copied out and counted: the queue is 14.9x inflated

`docker cp <container>:/app/data/timefusion/.timefusion_meta/maintenance_tasks.json`
(note the in-container path is `/app/data/timefusion`, not `/data`; the host
volume itself is root-owned and unreadable as `ubuntu`). 46 MB, 94,223 tasks:

```
superseded  61,267   (65% — dead parent records from splitting)
pending     21,598
complete    11,120
retry          231
running          7
```

Pending resolves to **1,452 distinct (operation, project, date) cells — 14.9x
inflation**. The worst single cell:

```
base_rollup  00000000  2026-08-13   1,474 units
base_rollup  87576849  2026-07-25     660
base_rollup  87576849  2026-07-23     571
base_rollup  87576849  2026-07-29     539
```

And the widths say why:

```
600s  2,714 units      60s  1,860 units      360s  1,651
300s  1,380            180s  1,531           86400s  678
```

**1,860 units sit at the 60-second floor.** This is the shredding pattern memory
already records once — "3,455 units for ONE cell from shredding a day to the 1m
floor when the real holes were 22 MINUTES". It is here again at 1,474.

So question 1 is answered: **the queue is not 21,598 pieces of real work, it is
~1,452 cells wearing 21,598 costumes.** Since unit cost is dominated by the
commit and the object-store round trips rather than by width, 14.9x inflation is
close to 14.9x wasted fixed cost. Coarsening pending slices per cell before
execution is worth more than any amount of extra worker capacity — and
`coarsen_sealed_slices` already exists to do exactly this, so the question is why
these survived it.

### The sim on the real journal: restarts RE-MINT, they do not (mainly) destroy

`timefusion sim maintenance_tasks.json --hours 2 --workers 16`, same seed, with
and without hourly restarts:

| | no restarts | `--restarts-every-hours 1` |
|---|---|---|
| pending 83,103 → | **22,484** | **57,444** |
| executions | 2,190 | 2,190 |
| completions | BaseRollup 1325, Dedup 120, DerivedRollup 662, Repair 32 | identical |
| timeouts | Repair 23 | identical |

**Identical work done, 2.6x the queue left behind.** Restarts do not slow the
workers at all in this model — they flood the queue, because the boot reconcile
re-enqueues per partition that saw commits while down. Two virtual hours of
hourly restarts mint ~35,000 tasks.

**This corrects the diagnosis written above.** That section argued in-flight work
is destroyed by process exit. The sim does not model that at all — its restart
branch only calls `mint_stream` and never touches `workers[].current` — so it
cannot speak to it either way. What it does show is that **re-minting alone is
sufficient to explain the queue never converging**, without any in-flight loss.
With prod taking 8 deploys in one session, that is the dominant term.

Two caveats, both load-bearing:

- The sim's restart model is explicitly **pre-fix**: its own comment says it
  enqueues `ALL_HOURS` per partition (~312 tasks per stream per restart) and that
  the 2026-08-18 change derives touched hours from commit stats, shrinking it to
  ~13. So 57,444 is the *before* number. **The model needs updating to measure
  the post-fix state** — that is a concrete task in itself, and until it is done
  this comparison overstates today's damage.
- Coarsening demonstrably works at scale: 83,103 → 22,484 in two virtual hours,
  and prod's live `pending` of 21,598 sits right at that post-coarsen figure. So
  prod is already coarsened, and my earlier suspicion that the pass never runs
  (from zero log hits) is not supported — it was very likely a grep artifact, the
  same class of error as the `event=` count that returned 0 across 3,271 lines.

Which sharpens the 1,440-unit cell: those units **survive** coarsening. With
nothing wider on that table subsume cannot fire, so fusion is the only route, and
the remaining suspect is the budget test — for a sealed day each 60s unit selects
nearly all the day's files (their statistics span the whole day), so 60 children
can sum far past `MAX_DECODED_BYTES` even though the real work is one hour. That
is a specific, testable hypothesis and the next thing to check.

Also from the sim, unrelated but worth having: **Repair timed out 23 times
against 32 completions** — a 42% timeout rate, by far the worst of any operation.

Two further fixes, and the first already exists for one operation:

1. **Resume instead of redo.** `timefusion_repair_resume_enabled` (default true,
   on in prod) already commits a matching staged-but-uncommitted rewrite rather
   than redoing 40+ minutes — for Repair. Extend it to `BaseRollup`, which is the
   operation holding 12,218 of the queue.
2. **Bound unit WALL TIME, not just decoded bytes.** `abandon_running`'s own
   comment says byte-splitting misses this: "it fires on decoded bytes, while
   what overran was WALL TIME — a day-sized slice with modest bytes still pays an
   object-store round trip per file."

## The clean before/after for the producer

The first post-deploy reading (936 full + 691 hybrid) was taken against a
historical "always 0", which is a weak control — those zeros came from processes
whose coverage map had not rebuilt. This is the honest baseline, measured
2026-08-23 on `45b9ab4` (the reverted state) with coverage fully loaded:

```
rollup_min_contiguous_days = 30      <- coverage IS loaded
rollup_hits_full_total     = 0
rollup_hits_hybrid_total   = 2
rollup_misses_total        = 136
```

So with slice coverage alone and a fully-warmed map, routing serves 2 of 138
attempts. That is the number the re-landed producer (`82bb304`) has to beat, and
comparing against it — rather than against a cold-start zero — is what makes the
claim mean anything.

## Why the producer underperforms its own result: coverage rebuilds slower than prod redeploys

`recover_rollup_coverage` restores SLICE coverage at boot. It does NOT restore
the DATE-level map — that is filled only by the producer at publish time. So
after every restart the date-level route starts empty and refills as units
publish.

Measured 2026-08-23, and the two numbers do not fit together:

- Coverage takes **~25 minutes** to become usable after a restart (three
  observations).
- Prod deploys landed **~15 minutes** apart: `45b9ab4` → `82bb304` → `e6315af`.

So the window never opens. Verified directly rather than inferred: on a
15-minute-old container with `rollup_min_contiguous_days = 0`, a 7d dashboard
group-by ran **8.98 s and incremented hits by exactly zero**. It went raw.

That is the honest caveat on the 936-full + 691-hybrid result — it was measured
on a container that happened to stay up long enough. On a box redeploying faster
than coverage rebuilds, the date-level route is empty most of the time.

The fix is to rebuild the date-level map at boot from the tags recovery already
reads, using the slice row-witness to prove currency once at boot rather than per
query. Stamping the current partition fingerprint without that proof would assert
freshness nobody established — which is the same trap the `source_fp` fallback
fell into.

## THE RESULT: the 7d dashboard group-by routes, and it is 2.8x faster

2026-08-23, `e6315af` (contains the producer), coverage rebuilt to 30, per-query
hit verification and non-routing controls in the SAME round:

| | routed? | rep1 | rep2 |
|---|---|---|---|
| CONTROL `trace_id =` needle 7d | no | 79 ms | 80 ms |
| CONTROL `ORDER BY ts DESC LIMIT 100` | no | 112 ms | 131 ms |
| **`time_bucket(1h)` group-by 7d** | **yes, 1 hit each rep** | **3,713 ms** | **3,667 ms** |

Two comparisons, both honest:

- against the original raw baseline of **10,570 ms** → **2.8x faster**
- against the best UNROUTED-on-a-quiet-box figure measured this session
  (5,839 / 6,335 ms) → **~1.6x faster**

The controls are the reason this reading is trustworthy where the first
post-deploy attempt was not: they sit at their best-ever values, so the box is not
flattering the routed number. Note `cpu_tokens_used = 16` throughout — maintenance
saturation does not imply query saturation, and conflating the two is what made
the earlier reading unusable.

Not everything routes yet. The 3d group-by missed `not_built` (30 of them) and
stayed at 2,905 ms, because on a young container the date-level map has only the
days that have published since boot — which is exactly the ceiling described in
the section above, and the case for rebuilding that map at boot.

## Boot recovery (#7, `2321c47`) verified — and 3d/30d still do not route

`recover_date_coverage` fires: prod logged `rollup_date_coverage_recovered`
three times with **recovered = 20, 7, 20**. The date map is populated from
restart now rather than refilling only as units publish.

| | routed? | rep1 | rep2 |
|---|---|---|---|
| CONTROL needle 7d | no | 159 ms | 75 ms |
| CONTROL topk 7d | no | 149 ms | 103 ms |
| groupby 3d | **no** | 2,397 ms | 2,693 ms |
| **groupby 7d** | **yes** | 3,883 ms | 4,718 ms |
| groupby 30d | **no** | 29,624 ms | 28,450 ms |

So 7d holds up across containers (3.7 / 3.9 / 4.7 s against a 10.6 s baseline),
and **7d is still the only window that routes**. Two open questions, neither
answered yet:

- **30d** needs 30 contiguous covered days and recovery restored 20. That is a
  coverage-depth problem, not a routing one — it is the queue, which is the
  stale-estimate defect below.
- **3d not routing is the surprising one**, since three days should be easier
  than seven. The likely mechanism is the hybrid cost guard: a 3d window whose
  today-and-yesterday are still churning has a covered interior near the 20%
  threshold below which routing is declined as not worth it. Worth confirming
  before acting — a guess about a threshold is exactly the kind of thing that has
  been wrong repeatedly here.

One caveat on the earlier claim that this would not shorten the blind window:
coverage came up **183 s** after this container started, against ~25 minutes
observed on earlier ones. But `rollup_min_contiguous_days` is computed by the
maintenance planner pass, which this change does not touch, so that is more
likely variance in when the planner ran than an effect of boot recovery. One
observation, and the mechanism does not support the attribution — so it is
recorded, not claimed.

## The queue defect, quantified — and the one-line fix that needs a decision

With coarsening now modelled in the sim (`0927494`), the real prod journal says:

```
coarsen: subsumed 1861 fused 276 | candidates 5141743 blocked 365916 over_budget 4775551
```

**92.9% of every fusion candidate is refused on budget.** The queue cannot shrink.

It is NOT stale estimates. `clear_stale_estimates` exists and the prod journal
shows it has run (`__maintenance_stale_estimate_v1: 1`, `v2: 1`). The 282 MB
values were regenerated by the live estimator, because `slice_share_of_file`
prorates by ROW GROUPS and a ~10 MB file holds one — so a sixty-second slice
honestly does read whole files. The per-unit number is right. **The sum is not**,
because the fused unit reads those row groups once.

Experiment, run and reverted rather than argued:

| | baseline | uniform priced as one child |
|---|---|---|
| pending 83,103 → | 20,949 | **16,661** (−20%) |
| coarsen **fused** | 276 | **4,769** (17×) |
| over_budget | 4,775,551 | 3,565,773 |
| timeouts (Repair) | 18 | 21 (+3) |
| completions (BaseRollup) | 1,087 | 1,063 (−24) |

17x more fusion and a 20% smaller queue for +3 timeouts in two virtual hours.

**Why it is not shipped.** It breaks
`coarsening_skips_a_day_that_would_not_fit_the_decode_budget` and
`a_day_over_the_decode_budget_lands_at_a_narrower_width_not_at_ten_minutes`, both
of which encode the #178 regression (BaseRollup hitting 900 s for the first time,
output collapsing from ~9,000 rows/min to 10). Their fixtures use identical
estimates — exactly the signal the rule keys on — so the rule cannot distinguish
their scenario from the pathological one.

**The tension is already in the codebase, and it points at the answer.**
`clear_stale_estimates`'s own comment says: *"Zero is what a freshly minted unit
already carries: the claim-time preflight computes the real estimate and splits if
it genuinely must, so the worst case is one over-sized claim that immediately
right-sizes itself. That is strictly better than a queue that can never fuse."*
If that reasoning holds, an over-budget uniform group should fuse **with estimate
0** and let the preflight right-size it — which resolves the conflict in favour of
fusing, and is also one line.

## Open, in priority order

1. `stale_coverage` (49 misses, and it owns plain `count(*)`) — the per-slice
   witness change in flight.
2. `filter_not_eligible` (31) — `level` / `status_code` are not rollup
   dimensions. Needs a spec decision, then a rebuild.
3. Read the new `prefilter_skipped_*` rows once prod has an hour on them.

## The queue can shrink now — `InputFootprint`, and the two shredding paths

The 2026-08-23 experiment above (price a uniform group as one child) was the
right instinct with the wrong key. Estimates are not a proxy for "same work";
the file set is. Both halves shipped together:

**FIX A — `uncovered_gaps` merges adjacent holes.** Each untagged file
contributes its own statistics span, so a day whose files tile it end-to-end
produced a gap per file, and `enqueue_untagged_rebuilds` minted a unit each.
`dedup` only removed exact repeats. Adjacent and overlapping gaps are now one
gap (`merge_ranges`), and a genuine separation still survives — asserted both
ways. The `gaps.is_empty()` fallback that republishes the untagged spans
themselves is merged for the same reason.

**FIX B — a unit records what it READS, and fusion charges a file set once.**
`estimated_decoded_bytes` prorates a file by the time share its slice covers,
which is right for one unit and wrong the moment `coarsen_to_width` sums
siblings: parquet prunes at row-group granularity, so `slice_share_of_file`
floors every slice at one row group — and on ~10 MB files that IS the file. Each
of prod's 1,440 sixty-second estimates was therefore *honest*
(`__maintenance_stale_estimate_v2` had already run); the defect was summing
1,440 reads of the same 35 files.

`MaintenanceTask.input: Option<InputFootprint>` carries `{fp, whole_file_bytes}`
— an order-independent hash of the selected file paths, and their unprorated
decoded cost. `coarsen_to_width` prices a bucket as the sum over **distinct**
`fp`. Members with no footprint keep the old summed price, so this can only
lower an estimate, never raise one; partial overlap counts twice, which refuses
a fusion that would have fit — the safe direction, and the existing behaviour.

Both preflights record the footprint on **every** claim, not only when they
split. That is load-bearing: the split that shredded prod was not the byte one.
A unit that fits its estimate and then times out is bisected by
`abandon_running`, which knows only a key — so without a footprint already on
the parent, the bisect ladder mints footprint-less children and is one-way
again. The stored widths confirm the ladder rather than a single planner
decision: 600 s ×3,118, 3,600 s ×2,638, 60 s ×2,088, 360 s ×2,016, 300 s ×1,679.

**The legacy queue needs a migration, because its units predate the field.**
`__maintenance_coarse_backfill_v2` re-runs the one-shot that drops non-Complete
sealed sub-day units for the coarse-planned operations; the planner re-derives
what coverage actually lacks, and the split that follows now stamps its children.
Measured against the real prod journal (95,655 tasks, 2026-08-23):

| | before | after |
|---|---|---|
| live non-Complete units | 22,040 | 2,954 |
| of which the migration drops | — | 19,086 |
| real cells behind them | 760 | 760 |
| inflation | **25.1x** | 1x |

**What was previously ignored now passes.**
`a_day_shredded_to_the_minute_floor_collapses` was `#[ignore]`d as a documented
live defect. It now splits a day to the floor through the real path, asserts
every child inherits the footprint, and asserts one coarsen pass collapses
1,440 units to ≤24. Its doc comment's "stale estimates" diagnosis is corrected
in place — that reading was wrong, and it was the reasoning that sent the
previous fix at the wrong mechanism.

`fusion_charges_a_shared_file_set_once_and_disjoint_sets_twice` pins both
directions, including that footprint equality is order-independent (a snapshot
lists files in no fixed order).

### Correction to FIX A: merging raw gaps is not enough

Adjacent-gap merging as first written would mostly not have fired. Gaps come
from row statistics, so two holes either side of one file's last row are
separated by **milliseconds** — far too little to matter, far too much for
`merge_ranges` to bridge. Minute-aligning afterwards then turned them into two
*overlapping* `TaskKey`s: the shredding the fix targets, plus overlap for the
subsume pass to clean up.

`rollup::rebuild_slices` now does align → substitute-covering-slice → clamp →
**merge**, in that order, and `rebuild_slices_aligns_before_merging_so_a_sub_minute_separation_is_one_unit`
pins it with a 247 ms separation and asserts the raw-merge alternative still
returns two. Aligning first cannot swallow real coverage: no published slice is
narrower than `MIN_SLICE_MICROS`.

### Backward compatibility, proven rather than assumed

`timefusion sim` on the real 95,655-task prod journal (written before
`MaintenanceTask.input` existed) loads and runs unchanged, and its coarsen
figures are the old ones — `over_budget 456,374` against `fused 69`. That is the
design, not a disappointment: footprint-less units price exactly as before, so
**the migration, not the pricing rule, is what clears the legacy queue.**

### Prod after the deploy: the migration removed nothing, and that is the finding

Image `1608958` carries the fix. The v2 migration ran at the first boot that had
it (16:50 UTC — `maintenance_runtime_started` proves the block executed) and
logged **no** `maintenance_coarse_backfill_migrated` line, which is emitted only
when it removed something. Live `tasks_pending` was already 5,294.

So the 22,040 → 2,954 table above is arithmetic on a **stale journal snapshot**:
by deploy time the live queue no longer held those 19,086 sealed sub-day units.
The prediction was sound and the input was old. Do not quote it as an outcome.

Worse, there was no instrument for the pricing rule itself — `record_input` has
no counter, and the coarsen line reported only the summed verdict, so the first
prod read could not distinguish "working" from "no unit carries a footprint
yet". `955f05f` adds `priced_by_footprint` (buckets that fit ONLY because a
shared file set was charged once; zero means the rule changes nothing) and
`input_fp` on `maintenance_task_started`. Those two are the pass condition —
read them before claiming Fix B moved anything.

## #3 — sealed hygiene now ranks by benefit (unblocked by the footprint)

This was blocked on a signal the journal did not carry, and `InputFootprint`
supplied it. The planner already counts the sub-target files to decide a
partition is out of policy, so `files` rides along with the fingerprint the
fusion pricing needs — no extra work, no extra field.

Every hygiene unit is day-wide, so `-width` ties among them and the tie-break
was pure recency, which says nothing about how much debt a claim retires. Four
sealed cells held ~850 removable files in 4.9 GB while capacity went to
whichever sealed most recently.

**Bucketed at 64 files, and that is load-bearing.** `claim_next` matches the
winning tuple EXACTLY, so a raw count makes one cell the sole winner of every
claim and defeats the per-project rotation `fair_cursors` exists for — the same
starvation the width ordering above it already records twice. Comparable cells
tie and rotate; a 200-file cell still outranks a 3-file one.

Narrow on purpose: starvation still leads the tuple (so no small cell is
stranded), benefit is zero for every non-hygiene operation (so damage repair's
deliberate tie is untouched), and an unknown benefit orders LAST rather than
first. Both earlier proposals stay rejected — widening the size band is lossy,
and sorting bin candidates by size breaks the `event_range` ordering that keeps
output runs time-disjoint for range pruning.

`sealed_hygiene_ranks_by_files_removed_not_by_date` pins all four properties.

## #5 — `level` is a real filter shape, and it must NOT be added tonight

The check the task asked for, done against monoscope's actual query surface
rather than a dashboard inventory: queries are user-authored KQL compiled to
SQL, and `level == "ERROR"` is canonical enough to appear three times in
`src/Pkg/AI.hs`'s prompt examples and in `src/Pkg/LiveTail.hs`'s doctests. So
the premise holds — `level` is not an incidental filter, and it is genuinely
absent from the spec (`[resource___service___name, kind, status_code]`).
Cardinality is 4. On the merits it should be a dimension.

**The cost is the whole tier, and the timing is wrong.** `rollup::generation_id`
hashes `format!("{spec:?}")` (src/rollup.rs:69) precisely so a measure added
without a table-name bump cannot serve rows built under the old spec. Adding
`level` therefore invalidates **every existing rollup file**, and the tier stays
dark until 30 days rebuild. That would delete tonight's only measured win — the
2.8x routed 7d group-by — for days, and it would do so at the exact moment the
queue has just acquired its first working shrink mechanism. Two changes whose
effects cannot be told apart is also how the last four measurements lied.

**What makes it cheap, and is the real task.** Tag each rollup file with the
dimension set it was built with, and let the router accept a file whose
dimensions are a SUPERSET of what the query needs. Then a spec addition stops
being invalidation: old files keep serving every query that does not filter on
the new dimension, and new files serve both. `generation_id` would narrow to the
things that genuinely change row semantics (measures, filters, grain) rather
than the whole `Debug` of the spec. That is a self-contained change to the
identity/routing pair and it converts #5 from a 30-day outage into an
incremental rollout.

Sequenced, not dropped: do the dimension-set tag first, then add `level` behind
it.

### Fix B verified in production, on the instrument built for it

Six consecutive coarsen ticks on `fd99a91`, 18:15–18:23 UTC:

| tick | fused | priced_by_footprint |
|---|---|---|
| 18:15 | 0 | 0 |
| 18:16 | 4 | **2** |
| 18:17 | 0 | 0 |
| 18:20 | 4 | **2** |
| 18:21 | 10 | **5** |
| 18:23 | 9 | **2** |

`priced_by_footprint` counts buckets whose summed member estimates exceeded
`MAX_DECODED_BYTES` and which fit ONLY because members sharing a file set were
charged once — **11 of the 27 buckets fused in that window would have been
refused before this change.** `input_fp` is populated on claims and repeats
across units of the same partition, which is the shape the pricing depends on.

Not quoted: `over_budget` per tick (12–27). There is no pre-deploy reading from
the same instrument at the same cadence, and the sim's cumulative figure is not
comparable.
