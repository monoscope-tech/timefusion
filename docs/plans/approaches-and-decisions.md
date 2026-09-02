# Approaches and decisions — running log

Newest first. One entry per decision that changed direction, with what refuted it.
Detail lives in the dated plan files; this is the index.

## 2026-09-02 — morning summary

Four changes shipped, each measured before and after. The chain started from one
line the user changed in a YAML file.

| # | change | effect |
| --- | --- | --- |
| `a5381a0` | dedup key widened to the sort prefix + `RunCollapse` | **2.4x** per rewrite; the old path OOMs where this completes |
| `e4414b2` | `dedup_plan_shape` instrument | answered the next question on its first prod unit |
| `5a445001` | footer sort index is a parquet LEAF, not a field | the remaining `SortExec` **disappears** once footers converge |
| `8efa0952` | pin one ordered stream for the collapse | closes the stall that #3 could otherwise cause |
| `608a385d` | the routing canary names its decline reason | `tiny_interior` — the mechanism, at last |
| `f74f3429` | `wal.replay_rows` | lets a restart be priced against the dedup work it creates |

Plus two latent bugs the work flushed out — the logical-count index keyed
narrower than dedup (would under-count `COUNT(*)`), and the mem leg's ordering
claim using projected instead of original schema indices (regrew a blocking sort
on every top-K dashboard query) — and one retraction: the rollup-routing canary
declines on `tiny_interior`, so the verdict it produced against the
`sorting_columns` reorder is unproven.

**MEASUREMENT WARNING — I became the confounder.** Seven deploys in one night;
prod uptime at hand-off was 17.6 minutes. A maintenance unit averages ~20 min, so
a process that never lives that long cannot finish one, and
`dedup_bin_stage_timeouts_total : dedup_bins_committed_total` moved 20:9 -> 30:4
across my last two restarts. That is almost certainly MY deploy cadence killing
in-flight units, not a regression — the exact trap CLAUDE.md records from
2026-08-18 and `tf_deploy_cadence_starves_dedup` describes. Code deploys stopped
after `f74f3429`; docs-only pushes do not restart prod. **Do not read any
throughput number taken before ~2h of quiet.**

### OPEN CONCERN — check this FIRST: stage timeouts are climbing

After 24 minutes of quiet (no deploys), so this is not restart churn:

| build | uptime | `dedup_bin_stage_timeouts_total : dedup_bins_committed_total` |
| --- | --- | --- |
| `e4414b2` (pre-footer-fix) | ~15 min | 20 : 9 = 2.2 |
| `4934cb0` (footer fix) | ~25 min | 20 : 6 = 3.3 |
| `f253ba2` (now) | ~24 min | **40 : 5 = 8.0** |

And `merges=1` is now **7 of 13** units (54%) against 31 of 100 (31%) before.

**Suspected mechanism, and the fork warns about it in its own comment.** More
files now carry a CORRECT footer, so more scans get an ordering claim;
`regroup_for_declared_ordering` then splits file groups to keep the claim
alive, and `MAX_ORDERED_FILE_GROUPS = 512` exists precisely because "a
SortPreservingMergeExec over that many concurrent parquet streams costs more than
the ordering claim is worth". During the MIXED-generation phase we may be paying
the merge without yet getting the sort removed — net negative until footers
converge.

**Why I did not revert:** `dedup_failed_total = 0`, no errors, queries healthy,
and `pending_dedup` is flat (~1,760-1,820, down from ~2,500 at session start), so
there is no user-facing harm — this is maintenance throughput, and the sample is
13 units. Reverting on 24 minutes of a moving file population would be the same
mistake as trusting a number taken during a deploy storm.

**Kill switch, in order of preference:**
1. Wait for convergence — the win only arrives when a partition is all-new
   footers, and `light_optimize` converts fast.
2. If it worsens: revert `5a445001` (the leaf-index fix). New files go back to
   writing the old wrong footer, no scan gains a claim, and the plans return to
   exactly what `e4414b2` planned. Nothing else depends on it.
3. `RunCollapse` and the 2.4x are INDEPENDENT of this and should not be reverted
   with it — they are gated on `dedup_keys_lead_the_sort`, not on footers.

**What would settle it:** the `ordered_scan=true` share against the timeout
ratio, over hours. If timeouts fall as that share rises, this is a transition
cost. If they keep climbing while it rises, the regroup/merge is the problem and
the fix belongs in the fork's grouping heuristic, not here.

**What to check first:** the `ordered_scan=true` share in `dedup_plan_shape`, and
`dedup_bin_stage_timeouts_total : dedup_bins_committed_total` (was 20:9 before
the footer fix). `pending_dedup` fell ~2,500 → 1,767 across the session.

**Not done, and deliberately:** the ingest-side duplicate prevention (designed
below; its memory/FPP/horizon tradeoffs are the user's call) and the DataFusion
`filter_map` fix (a read-path correctness change needing its own latency matrix).

## 2026-09-02 — WE MANUFACTURE THE DUPLICATES. 58% of them are our own replay.

The strategically important finding of the night, and it reframes the whole 10x
question. Dedup is ~96% of maintenance worker time. **A majority of its input is
rows TimeFusion re-inserted into itself.**

### The evidence

Decomposing the 101,563 duplicate groups in a real 204 MB prod bin
(`recent204.parquet`, 1,639,811 rows) by `updated_at`:

| shape | groups | what it is |
| --- | --- | --- |
| every copy shares one `updated_at` | **58,965 (58%)** | one stamping event |
| copies differ in `updated_at` | 42,598 (42%) | merge-on-read enrichment versions |

`updated_at` is TF-OWNED and stamped at write time (`insert_coerce::stamp_version`),
so two copies written by DIFFERENT inserts necessarily carry different stamps.
One stamp means one stamping event.

And all 58,965 of those groups are **byte-identical in every sampled column** —
including `hashes`, the one column enrichment mutates, and `deleted`. Not a
version. An exact re-insert.

### The mechanism, already written down in the code

`settle_flushed_group` (`src/write/mod.rs`):

> *"A failed advance is benign: the cursor stays behind and the next boot
> re-replays rows that are already in Delta — dedup_keys (write-side) and
> DedupExec (read-side) collapse them."*

That is correct for DURABILITY and it is the entire dedup workload's source. It
also compounds with the thing we already know kills maintenance: restarts. More
restarts → more replayed rows → more duplicates → more dedup → less maintenance
throughput → and the deploy cadence never lets it drain
(`tf_deploy_cadence_starves_dedup`, `tf_units_die_to_restarts`).

### Two candidate producers, and the instrument separates them

Be precise about what is proven. "One stamping event" has TWO possible causes,
and one prod file cannot distinguish them:

1. **replay** of an already-stamped batch (replay preserves stamps — see
   `observe_stamp`, "the first stamp issued after this boot must exceed every
   replayed one"), or
2. **a single client batch containing the same row twice**, stamped once.

What IS ruled out is the ordinary client retry: `stamp_version` overwrites the
column on every write to a `version_append` table, so a genuinely separate insert
of the same span carries a NEW stamp and would land in the 42% bucket, not the
58% one.

`wal.replay_rows` decides between the two in production: if it stays near zero
across a window in which duplicates keep accruing, the producer is the batch
path; if it spikes with unclean restarts and duplicate creation tracks it, it is
replay. That is the whole reason the instrument was the right thing to ship
before any fix.

**A first data point, already:** on the clean deploys tonight,
`wal.recovery_complete = true` with `recovery_duration_ms = 0` — this boot
replayed NOTHING. So a clean drain does advance the cursor, and replay duplicates
would come from UNCLEAN exits: OOM kills and SIGKILLs, which this system has a
documented history of (`tf_oom_peak_anon_back_to_124gb`, `tf_units_die_to_restarts`).
If that holds, the chain is:

```
OOM kill -> unclean exit -> cursor not advanced -> replay re-inserts flushed rows
         -> duplicates -> dedup consumes ~96% of maintenance -> backlog never drains
```

which links the memory pressure work and the maintenance throughput work that
have been treated as separate problems all along.

### Why this is THE 100x lever

Everything else tonight made dedup *cheaper per unit*: 2.4x from `RunCollapse`,
and the sort disappearing once footers converge. This makes **more than half the
work not exist**. A 100x customer does not need us to remove their duplicates
faster; it needs us to stop generating our own.

### What was NOT done, and why

Nothing. This is a DURABILITY path — the area with the worst incident history in
this repo (`tf_acked_loss_recovery_cutoff`, `tf_two_live_loss_bugs`) — and
"replay fewer rows" is one wrong assumption away from acked-write loss. It is not
a change to make while the person who owns the risk is asleep.

What shipped instead is the measurement that was missing: `wal.replay_rows` in
`timefusion_stats` (`f74f3429`). `recovered_rows` was already computed at replay
and thrown away, so nothing could compare a restart's cost against the dedup work
it creates. Now a restart can be priced.

### The design question for the morning

Make replay IDEMPOTENT rather than smaller — never skip on a guess:

- **Durable flush watermark per `(project, table, bucket)`.** Replay skips an
  entry only when the bucket is provably committed. The information exists at
  flush time; the failure is that a lost cursor advance loses it. A tiny separate
  durable record survives what the cursor does not.
- **Prior art:** ClickHouse deduplicates INSERTS by block hash over a window
  bounded by BOTH count (`replicated_deduplication_window`) and time
  (`..._seconds`), and lets a client override with an explicit
  `insert_deduplication_token`. The lesson for us is the granularity: they hash
  the BLOCK, not the row — one hash per batch. Our replay duplicates are
  whole-batch re-inserts, so a batch-identity check is the cheap equivalent and
  needs no per-row filter at all.
- This also supersedes the per-row Bloom design sketched below: that was aimed at
  client retries, and client retries are not where our duplicates come from.

Sources: <https://clickhouse.com/docs/guides/developer/deduplicating-inserts-on-retries>,
<https://kb.altinity.com/altinity-kb-schema-design/insert_deduplication/>

## 2026-09-02 — widen the dedup key to the sort prefix (the inversion that worked)

**The user's idea, and it is the right one:** the failed experiment reordered
`sorting_columns` to match `dedup_keys` and broke rollup routing. Do it the other
way — widen `dedup_keys` to `(timestamp, resource___service___name, id)`, which is
already the leading run of `sorting_columns`. Reads are untouched, so the routing
failure mode does not apply. This is the ClickHouse ReplacingMergeTree arrangement
(the `ORDER BY` carries the FULL dedup key so merges stream).

### Correctness, measured before writing any code

Widening only loses collapsing power if two rows share `(timestamp, id)` but
differ on service. On 2.75M rows of three real prod bins — including one holding
**104,949 duplicate rows across 19 services** — the dup-group count is IDENTICAL
under both keys: `LOST=0`. It holds by construction too: a client retry carries
the same payload, and a merge-on-read version only ever mutates `hashes` (the one
`mutable: true` column). Where the keys genuinely differ, today's narrow key is
COLLAPSING TWO DISTINCT SPANS — widening fixes that, it does not risk it.

### The trap: the YAML change on its own is worth NOTHING

Measured, not assumed. The rewrite plans **2 `SortExec` either way**:

```
Sort#2 (ts DESC, svc, id, level, status)   <- the output ORDER BY
  Filter (__tf_rn = 1)
    BoundedWindowAggExec  mode=Sorted
      Sort#1 (ts ASC, svc, id, updated_at DESC)   <- the window's own requirement
```

Two planner facts, both dead ends — do not re-derive them:

1. **A window normalizes its PARTITION BY requirement to ASC** and takes no
   direction hint from the outer sort or from its own `ORDER BY`. So the window's
   sort can never double as the DESC output sort.
2. **A subquery `ORDER BY` without `LIMIT` is semantically void and DataFusion
   deletes it** — which is why an inner `ORDER BY … DESC` vanished and why
   `prefer_existing_sort` did nothing (there was no existing sort left to prefer).
   An ordering is only real to the planner when a SOURCE declares it.

Every SQL formulation was tried. One sort is reachable only in ASC, which would
make the footer's DESC claim a lie — the 2026-08-07 under-count bug class.

### What actually ships: one sort, and the collapse done by hand

`RunCollapse` (`src/database/compact.rs`): sort ONCE in schema order, then keep
the greatest tiebreak per RUN of equal keys in a single order-preserving pass,
holding back only the trailing run (the one that can continue into the next
batch). No window, no second sort. Valid exactly when `dedup_keys_lead_the_sort`
— which is what the user's YAML change created. Tombstones retained; ties keep
the first row, matching `ROW_NUMBER() … DESC NULLS LAST` + `__tf_rn = 1`.

Gated: any schema whose keys do not lead its sort keeps the window path.

### Two bugs the widening flushed out

- **The logical-count index was keyed on `(timestamp, id)` while dedup was not** —
  it would have UNDER-counted. Its key tail is now the full dedup key
  (`KeyTail`), and `FORMAT_VERSION` is bumped to `"2"` so a cached `"1"` file
  cannot be appended to with wider keys.
- **`create_memory_exec` declared its ordering with PROJECTED column indices**,
  but `try_with_sort_information` validates them against the source's ORIGINAL
  schema. It passed only while the claim was one column whose index happened to
  coincide. A second column made it fail — silently dropping the claim and
  regrowing a **blocking `SortExec` over the mem leg of every top-K dashboard
  query**. Fixed; the leg now declares `[ts DESC, service ASC, id ASC]` where it
  previously declared `[ts DESC]`, so this is a read-path improvement that was
  sitting there unnoticed.

### Measured: the real 204 MB prod bin, 1 GB pool

| variant | secs | MB/s |
| --- | --- | --- |
| dedup WINDOW b256 p1 — what shipped | **47.4** | 4.31 |
| dedup COLLAPSE b256 p1 | **19.9** | 10.25 |
| dedup WINDOW b2048 p1 | **OOM** (ExternalSorterMerge, unspillable, 1017.8 MB peak) | — |
| dedup COLLAPSE b2048 p1 | **13.5** | 15.11 |

**2.4x at prod's exact config** (`batch_size=256, target_partitions=1`), and at
b2048 the window FAILS while the collapse completes — **3.5x** against the
window's best working config. The window emits 1,534,862 rows to the collapse
shape's 1,639,811: exactly the 104,949 duplicates the key probe found, so the two
shapes are doing the same job.

Caveat stated honestly: the bench prices the PLAN. `RunCollapse` adds an O(n)
`RowConverter` encode over three short key columns on top of the COLLAPSE row —
the same encode `dedup_batches` already does, and small against 27.5 s saved.

### Suite verdict, and a master regression it exposed

| tree | result |
| --- | --- |
| pre-change (master `5b2f7254`) | 1314/1315 — `a_chart_under_a_derived_table_routes_and_agrees_with_raw` FAILS |
| with this change | 1316/1317 — the SAME single failure, nothing else |

So that test is **already red on master**, from the concurrent session's two
schema commits (`attributes___http___route` promoted to a column, migrate-columns
Utf8) — not from this change. Establishing that took one full suite run on a
reverted tree, and it was worth it: the same test is what condemned the
`sorting_columns` reorder, and that verdict still stands (it was measured against
a genuinely green 1314/1314 baseline, before master regressed).

**Method note:** "the canary is red" is only evidence when you have re-measured
the baseline THAT DAY. A shared checkout moves under you.

**CORRECTION — the canary is not trustworthy, and it retracts an earlier
verdict.** Chasing which of the two master commits broke it, both alibi out:

- removing `attributes___http___route` from the YAML does NOT fix it (full suite,
  and it costs two other failures that assert the column is there);
- `f1681269` adds one match arm to a CLI subcommand — it cannot reach routing.

Meanwhile the test fails 2/2 in ISOLATION on a quiet machine at a HEAD whose full
suite passed it earlier today. So it passes or fails on scheduling, not on the
tree. That means **the 2026-09-01 verdict "the `sorting_columns` reorder breaks
rollup routing, 2/2 reproducible" was built on this same unreliable signal and
must be treated as UNPROVEN**, not as a refutation.

It happens not to matter: widening `dedup_keys` reaches the same alignment
WITHOUT touching physical layout, so the reorder is moot either way. But the
lesson generalises — a test that is order-sensitive can manufacture a clean
2-for-2 and retire a good idea.

**Mechanism, now named (`608a385d`).** The assertion was a bare bool; it now
reports the miss reasons, and answered immediately: the decline is
**`tiny_interior`**. `MIN_INTERIOR_BUCKETS = 2` is checked against the CERTIFIED
INTERIOR, not the query window — so the test fails whenever
`run_maintenance_units(1024)` certifies less than two grains' worth of its
window. How much gets certified depends on how many units run and which work the
coordinator picks: machine load and unit ordering, not the tree. That is the
whole explanation for "fails alone, passes in suite".

Note the report includes `rollup_misses_total` alongside the per-reason counters.
Without it there is no way to tell "declined for a reason not in this list" from
"the router was never consulted" — the first run showed `misses: []`, which
looked like the latter and was actually the former.

**The real fix** is to assert the PRECONDITION — enough of the window certified —
before asserting the routing, so a starved maintenance run fails as "setup
incomplete" instead of as "routing broken". Left for its owner; the diagnosis is
now in the failure message either way.

### Live on prod as `a5381a0` — verified, not assumed

~10 minutes into a fresh process:

| metric | value |
| --- | --- |
| `dedup_bins_committed_total` / `dedup_waves_committed_total` | **4 / 4** |
| `dedup_failed_total`, `dedup_timed_out_total`, `dedup_bin_stage_timeouts_total` | **0** |
| `immutable_column_disagreement_total` | **0** — the widening's premise, live |
| `pending_dedup` | 1840 (was ~2,500 at session start) |

Like-for-like on process age, the PREVIOUS build read
`dedup_bins_committed_total = 0` at the same point.

`RunCollapse` is doing the work, across six projects, arithmetic exact:

```
dcad860a … 07:40-07:50   dropped=37317 (before=160768 after=123451)
28f62f01 … 07:50-08:00   dropped=25734 (before=85405  after=59671)
00000000 … 07:50-08:00   dropped=7142  (before=78009  after=70867)
be87ebc1 … 07:30-07:40   dropped=1889  (before=23641  after=21752)
```

The `expected_logical_rows` oracle rejects any staged unit whose row count
disagrees with an independent `COUNT(DISTINCT keys)`, so a collapse bug shows up
as `dedup_failed_total` climbing or bins looping `Retry` — never as silent row
loss. It has not fired.

**Note for whoever reads these drop rates:** 23% on a hot bin is NOT the 0.0004%
measured on sealed dates. Different population — the last 30 minutes carry
merge-on-read version churn that later collapses. Do not mix the two numbers.

**Kill switch:** revert the one-line `dedup_keys` widening in the YAML. The
`dedup_keys_lead_the_sort` gate then flips the window path back on by itself.

### A live correctness bug in DataFusion's footer→ordering mapping

Probing whether phase 2 is reachable turned this up. Real prod footers are
correct — `recent204.parquet` declares all five sorting columns in order,
verified with pyarrow. But `sorting_columns_to_physical_exprs`
(`datafusion/datasource-parquet/src/metadata.rs:798`) maps them with
**`filter_map`**: a footer sort column that is not in the read schema is
**skipped and the iteration CONTINUES**.

So a file sorted `(timestamp, service, id, level, status)` read with a projection
that omits `service` is declared as ordered by **`(timestamp, id)`** — which is
false. Within one timestamp, ids are not ascending across services. It must
truncate at the first missing column (`map_while`), not skip it.

Observed live: the top-K plan's Delta scan advertises
`output_ordering=[timestamp@0 DESC, id@3 ASC NULLS LAST]`. The fork's own layers
are careful — `stats_backed_prefix_len` uses `take_while` and only ever shortens
— so the false claim comes from upstream, below them.

Why it has not bitten: TF consumes the LEAD column only (`DedupExec
bounded[timestamp]`, TopK on `timestamp DESC`), and `dedup_key_idxs` keeps the
bound in the key precisely so a false ordering can only under-dedup. Anything
that trusts the full ordering — a `SortPreservingMerge` fed by this claim, which
is exactly phase 2 — would be wrong.

**CORRECTION — this does NOT gate phase 2.** The bug bites only a projection that
OMITS a sorting column. The dedup rewrite selects `{columns}` = every schema
field, so no sorting column is ever missing from its read schema and `filter_map`
behaves identically to `map_while` there. Phase 2 is reachable without touching
DataFusion. What the bug does threaten is the QUERY path, where narrow
projections are the norm.

The fix (`filter_map` → `map_while` in the `tonyalaribe/datafusion` fork, which
already carries a `datafusion-sql` patch) is therefore its own change on its own
deploy, and it is not free: some scans would declare SHORTER orderings, which is
correct but can turn a streaming TopK into a blocking sort wherever the current
claim is false-but-useful. Measure the latency matrix before shipping it.

**And phase 2 may be partly done already.** The delta-rs fork's
`derive_common_ordering` declares footer ordering on Delta scans automatically
(conforming files only, prefix-limited by stats), and `regroup_for_declared_ordering`
even repacks file groups so the claim survives. The bench numbers above come from
a plain `register_parquet` with no footer pushdown at all — so **13.5 s
UNDERSTATES what the same shape can do in prod**. The open question is not "how
do we declare the ordering" but "does the rewrite's scan actually get it, and
does the sort disappear when it does". Answer it with an EXPLAIN on the
maintenance path, not by building anything.

### Ingest-side dedup prevention — design, for the morning

The scoped version ("dedup-key check inside the MemBuffer's 10-minute bucket")
buys almost nothing: `dedup_batches` ALREADY collapses within a bucket at flush.
The duplicates that reach Delta are cross-flush — late client retries and WAL
replay overlap — so they land in a DIFFERENT bucket than the row they duplicate.

The real structure is a **recently-flushed-keys filter**: after a bucket flushes,
retain its `(timestamp, service, id)` digests for the retry horizon (hours, not
the 70-minute buffer retention), and drop an insert whose key is already present.
The design decisions are the user's, because they trade memory against
correctness:

- **Exact vs probabilistic.** A blocked Bloom filter at 1% FPP costs ~1.2 bytes
  per key; at prod rates that is order-100 MB for a day's keys across tenants. A
  false positive DROPS A REAL ROW, so a Bloom filter must be a pre-filter in
  front of an exact check, never the decision.
- **Horizon.** Too short and retries slip past; too long and it is another 100 GB
  problem. `probe_dup_bins` can measure the actual retry age distribution — the
  answer should be measured, not picked.
- **Where it runs.** In `insert()` it costs latency on the hot path; at flush it
  costs nothing extra but cannot reject, only collapse.

Worth it because it is the only item that makes dates born certifiable: no
rewrite, no probe, and dedup stops being 96% of maintenance instead of 2.4x
cheaper at it.

### Phase 2, answered: the footer-ordering pushdown is DEAD on the maintenance path

Measured, not reasoned. The `dedup_plan_shape` instrument (`e4414b2`), first unit
on prod:

```
sorts=1  merges=0  collapse=true  ordered_scan=false
```

- `collapse=true` — the new path is what runs in prod.
- `sorts=1` — one sort, down from two. That IS the 2.4x, confirmed live.
- **`ordered_scan=false`** — the Delta scan declares NO ordering at all.

And the local best case is barely better. `the_maintenance_scan_keeps_the_footer_ordering_it_was_written_with`
builds one file that TF itself just wrote with all five sorting columns in its
footer, scans it through `narrow_provider`, and gets:

```
SortExec: [timestamp DESC, resource___service___name ASC, id ASC, level ASC, status_code ASC]
  DeltaScanExec
    DataSourceExec: … output_ordering=[timestamp DESC, id ASC, level ASC, status_code ASC]
```

`resource___service___name` is **dropped** — though it is projected and second in
`sorting_columns`. So even the single-conforming-file case cannot satisfy the
schema sort, and the claim it does make is FALSE, not merely short: data sorted by
`(timestamp, service, id)` is not sorted by `(timestamp, id)`.

**This is the whole remaining cost of a dedup rewrite.** With the window gone the
sort is all that is left; making it a `SortPreservingMerge` is the next large
win, and it is blocked on the ordering declaration in exactly two places:

1. why prod declares nothing at all (conformance? `stats_backed_prefix_len`
   finding the lead column unbacked? per the 08-22 note, 38/86 of p1's file
   groups carry no footer at all), and
2. why a conforming file still drops a middle column — the `filter_map` in
   `sorting_columns_to_physical_exprs` skips instead of truncating, so an index
   that fails to resolve silently shifts everything after it.

**Retracting my own correction from earlier in this file:** I wrote that the
DataFusion bug does not gate phase 2 because the rewrite projects every column.
That reasoning was right and the conclusion was wrong — the column is projected
and still dropped. The bug gates phase 2 after all.

### ROOT CAUSE, and it is ours: a footer sort index is a LEAF, not a field

`SortingColumn.column_idx` indexes parquet **leaves**. A Variant/struct column
occupies as many leaves as it has children, so counting FIELDS under-shoots for
every sort key that follows one. `TableSchema::sorting_columns()` counted fields.

Proved against a real prod file (`recent204.parquet`, 97 leaves / 90 fields) by
resolving each recorded index through the parquet leaf schema:

```
idx=  0  leaf='timestamp'                     ← meant timestamp                 OK
idx= 76  leaf='attributes___user___email'     ← meant resource___service___name  WRONG
idx=  2  leaf='id'                            OK
idx=  9  leaf='level'                         OK
idx=  7  leaf='status_code'                   OK
```

`timestamp` survived only because nothing nested precedes it. So **every file
TimeFusion has ever written carries a sort claim naming the wrong column**, and
the reader — which resolves the leaf's NAME — cannot find it in the scan schema,
drops the entry, and advertises `[timestamp, id, level, status_code]`. That is
not a short claim, it is a false one.

**The fix removes the sort outright.** With leaf indices the scan declares all
five columns and DataFusion satisfies the `ORDER BY` from the files themselves:

```
ProjectionExec
  DeltaScanExec
    DataSourceExec: … output_ordering=[timestamp DESC, resource___service___name ASC,
                                       id ASC, level ASC, status_code ASC]
```

No `SortExec`. Not downgraded to a merge — **gone**. With `RunCollapse` needing
only adjacency, which that ordering guarantees, a dedup rewrite over conforming
files becomes a streaming pass. The bench put the sort at 81% of a rewrite
(scan-only 2.2 s vs 11.6 s for the prod shape).

Suite: **1317/1318**, lint clean. The ONLY new failure was
`sorting_columns_index_excludes_partitions` — the existing test that asserted the
old expectation directly, i.e. it encoded the bug it was meant to guard. Its real
intent (a partition column must not consume a leaf) was folded into the new test,
which checks it the stronger way. Every ordering-sensitive test — bounded dedup,
TopK, merge-on-read — passed with the corrected footers.

Caveats to carry into the morning:
- **Old files keep their wrong footers.** The win phases in as rewrites replace
  them — and every dedup/compaction rewrite now writes a correct one.
- **So expect `ordered_scan=false` to persist at first.** A dedup unit reads a
  whole `(project, date)` partition, which for a while mixes old and new
  footers; `derive_common_ordering` then gives the claim to the conforming subset
  and scans the rest separately, so the sort only disappears for partitions whose
  files were ALL written after this deploy. Today's data converges within hours;
  older dates convert as they are rewritten. Judge this on the ratio
  `dedup_bin_stage_timeouts_total : dedup_bins_committed_total` over hours, not
  on the first unit.
- The DataFusion `filter_map` (skip instead of truncate) is still there and still
  wrong; it is what converted our bad index into a silently shortened ordering
  rather than an error. Worth fixing upstream so the next such bug is loud.
- The test resolves each recorded index back through the same leaf order instead
  of asserting a hardcoded number, which would only re-encode the bug.

**The metric this must move:** `dedup_bin_stage_timeouts_total`. On `e4414b2`
prod logged **20 stage timeouts against 9 commits** in ~15 minutes — every
timeout is a bin that failed to stage inside its deadline and gets retried, i.e.
work done twice. The remaining sort is what those deadlines are spent on, so
removing it should collapse that ratio. Watch it against
`dedup_bins_committed_total`, not on its own.

**Mixed old/new files are safe by construction, but watch `dedup_failed_total`.**
Old files declare an ordering naming `attributes___user___email`; new ones name
`resource___service___name`. `derive_common_ordering` only claims an ordering for
files that AGREE, so the two generations cannot be merged under one claim — the
minority scans separately, unordered. And if that reasoning is wrong, the failure
is caught rather than silent: a `SortPreservingMerge` fed a false ordering would
leave equal keys non-adjacent, `RunCollapse` would emit more rows than
`expected_logical_rows` predicts, and the unit is REJECTED before any Remove
action commits. Fail-closed, visible as `dedup_failed_total` climbing.

### First 100 units after the leaf-index fix

```
 68  sorts=1 merges=0 collapse=true ordered_scan=false
 31  sorts=1 merges=1 collapse=true ordered_scan=false
  1  sorts=1 merges=0 collapse=true ordered_scan=true     <- the first one
```

Exactly the predicted shape: the win phases in with new files. The single
`ordered_scan=true` unit still sorts because on a mixed partition the claim goes
to whichever generation conforms, and an old-footer set names the wrong column,
so it cannot satisfy the schema sort. **The number to watch is the
`ordered_scan=true` share, and then `sorts=0` on partitions written entirely
after the deploy.**

`merges=1` on 31 of 100 also settles something: the multi-partition case is real,
not theoretical, which is why the run collapse now pins a single ordered stream
explicitly (`8efa0952`) instead of trusting `execute_stream`.

**It converges as a STEP, not a ramp — do not read a flat line as failure.**
`derive_common_ordering` picks the ordering **the most files agree on**
(`max_by_key(count)`) and isolates the rest into their own unordered scan. So
while old-footer files outnumber new ones in a `(project, date)` partition, the
claim goes to the OLD, wrong ordering and the correctly-footered files are the
ones set aside. A partition flips only when new footers become the MAJORITY
there — which one full rewrite of that partition accomplishes in a single commit,
since a rewrite replaces many files at once.

Practical reading: expect `ordered_scan=true` to sit near zero, then jump
per-partition.

**`light_optimize_tail` is the fast converter, and it is confirmed to be one.**
It derives `declare_sorted` from `choose_optimize_type(schema, false,
timefusion_optimize_sort_by)`, and that flag defaults to `true` — so every
hot-tail bin it commits rewrites files with a CORRECT footer. It committed 28
bins in ~25 minutes against dedup's 6, so it converts roughly an order of
magnitude faster than the path that benefits. Worth checking before assuming a
flat `ordered_scan` means the fix did not land.

Early and not yet claimable — young process — `dedup_bin_stage_timeouts_total` is
**0 at 4 commits**, against **20 at 9 commits** on the previous build.

### Still open

- `narrow_provider` declaring footer ordering (gated on EVERY selected file
  advertising `sorting_columns` — one unordered branch erases the leg's ordering)
  would turn the remaining sort into a `SortPreservingMerge`. That is the
  zero-sort endgame.
- Preventing duplicates at ingest, in the MemBuffer's 10-minute bucket. Still the
  only item that scales to 100x on its own.

## 2026-09-01/02 — maintenance capacity night

**Goal:** make dedup/sorting/hotpacking/rollups keep up, toward 10x and a
prospective 100x customer.

### Shipped to prod

| # | change | status |
| --- | --- | --- |
| 19 | split the certified-skip refusal (`no_stats` vs `overlap`) | live |
| 20 | rollup admission reclassified, deploy-15 footgun made unwritable | live, verified |
| 21 | certify from the batch probe | live, measured a NULL |
| 22 | certify sealed dates from the Delta snapshot | live — **first grants in system history** |
| 23 | project-major ordering | live |
| 24 | 16x probe rate + decline memo + `cert_probe_declined` | live |
| 25 | certification reaches `otel_logs_and_spans` at all | live |
| 26 | instrument HOW dirty a declined date is | live |
| — | window claim reservation (`5ed8c6b5`) | live, A/B'd at ~1% |

### The chain, and where it broke

1. Dedup is **~96% of maintenance worker time** and drops **0.0004%** of rows —
   it is a cleanliness PROOF, not a removal.
2. So certify cheaply instead of rewriting. Built it; `cert_granted_total` left a
   zero held since 2026-08-20, and prod queries were measured skipping
   `DedupExec`.
3. **Refuted as a leading strategy:** duplicates are sparse but SPREAD (~26-50 of
   144 bins per date), so every partition is dirty and certification cannot grant
   until removal happens. Removal is the constraint.
4. **Refuted again:** the removal queue's ordering is worth ~1%. A/B on the real
   77k-task prod journal — 22,162 pending, 27,175 executions/24h, backlog only
   halves either way. **The queue is CAPACITY-bound, not order-bound.**
5. **The cost, located:** the rewrite's sort **OOMs a 1 GB pool on one 204 MB
   production bin**; prod survives only by slicing 13 ways at 15 MB/s.

### Open, in priority order

1. **Align `sorting_columns` with the dedup keys** (`timestamp, id` leading;
   `service` after). Today `service` sits BETWEEN the dedup keys, so files are not
   prefix-ordered for the window's `PARTITION BY (timestamp, id)` and the sort is
   unavoidable. NOT DONE: changes physical layout for every future file and every
   query's read path, needs the latency matrix re-run, and does **not**
   retroactively fix existing files — so it cannot drain the current backlog on
   its own.
2. **Prevent duplicates at ingest** — dedup-key check inside the MemBuffer's
   10-minute bucket, so dates are born certifiable and need neither rewrite nor
   probe. The only item that scales to 100x. Design decision, not a patch.
3. Cheaper units (batch sizing already gave 39.4s -> 20.3s on one file).

### Decisions NOT taken, and why

- **Raising `STARVATION_MICROS` 3d -> 15d.** Refuted: `starved` is `u8::MAX` when
  NOT starved, so any starved task outranks any non-starved one — raising the
  threshold EVICTS the query window from the privileged lane. 9 test failures.
- **Declaring output ordering in `narrow_provider`.** Refuted before building: the
  file order and the window's partition key are misaligned (see 1 above), so it
  would have been a no-op shipped into the row-deleting path.
- **Bin-scoped dedup instead of whole-date.** Refuted: duplicates are spread over
  ~18-35% of bins and `stage_dedup_chunk` re-reads every file a chunk touches, so
  the whole-date unit is roughly right-sized.

### Method notes that cost real time

- **`COUNT(*)` cannot probe dedup coverage** — count pushdown answers it without a
  Delta scan. `GROUP BY … HAVING count(*)>1` also lies: it reads THROUGH
  `DedupExec` and sees duplicates already collapsed.
- **A counter stuck at an exact value means SCOPE, not throughput.** 437 grants
  frozen = the producer never ran on the main table (the dedup cron skips
  rollup-declaring tables).
- **`git stash` no-ops on an already-committed change**, which silently made an
  A/B compare a build against itself. Verify the arm with a marker
  (`grep -c window_turn` must be 0 in the baseline).
- **`synth:whale` cannot validate scheduling** — a fixed 813-task backlog that
  always drains, so no reservation ever binds. Use the real journal
  (`docker cp <ctr>:/app/data/timefusion/.timefusion_meta/maintenance_tasks.json`).
- **A young process reads as fixed.** Check uptime before quoting any counter.

### Prior art (2026-09-02): ClickHouse already solved this, and we did it backwards

**ClickHouse ReplacingMergeTree deduplicates on the `ORDER BY` sorting key — not
the primary key — and requires `PRIMARY KEY` to be a PREFIX of `ORDER BY`.** The
whole point is that a merge can then dedup by streaming, with no sort.

Their documented pattern for exactly our tension (dedup key contains identifier
columns you don't want in the index):

```sql
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (tenant_id, user_id, device_id)  -- FULL dedup key: makes merges streaming
PRIMARY KEY (tenant_id, user_id)          -- lean sparse index, a prefix of it
```

**We have it backwards.** TimeFusion sorts files
`(timestamp, service, id, ...)` and dedups on `(timestamp, id)` — `service` sits
between the dedup keys, so no merge can stream and every rewrite pays a full
external sort. That is the cost measured tonight: the sort OOMs a 1 GB pool on one
204 MB bin and dominates ~96% of maintenance time.

**What this de-risks about option 1.** The objection to leading `sorting_columns`
with `(timestamp, id)` is that it would hurt reads that prune on `service`.
ClickHouse's answer is that the SORT key and the INDEX do not have to be the same
thing — you keep the full dedup key in the sort and serve pruning from a shorter
index. TimeFusion already has that separation: `schemas/otel_logs_and_spans.yaml:67`
records that point lookups are served by bloom filters and tantivy, not by the
sort. So the read-path objection is likely weaker than it looks, and it is
measurable with the existing latency matrix.

Still needs the matrix re-run before landing — but the design is the industry
norm, not a novel gamble, and the current layout is the deviation.

Sources:
- https://clickhouse.com/docs/engines/table-engines/mergetree-family/replacingmergetree
- https://docs.peerdb.io/bestpractices/clickhouse_datamodeling
- https://queryplane.com/blog/clickhouse-partition-by-order-by-primary-key-guide/

### And why option 1 could NOT be shipped blind (11th refutation)

`schemas/otel_logs_and_spans.yaml` documents what position 2 is for:

> Point lookups on `id`/`trace_id`/`span_id` are served by bloom filters +
> tantivy, and **`service_name` by the secondary sort column**.

So `resource___service___name` sitting at position 2 is LOAD-BEARING for
service-filtered pruning — one of the most common filters in an observability
product. Moving `id` ahead of it trades a dedup win for a read regression on those
queries. My earlier reading ("bloom filters and tantivy cover it") was wrong: that
clause covers `id`/`trace_id`/`span_id`, not service.

**This is a real trade, not an oversight to fix.** It needs numbers on both sides:

1. Dedup gain: how much of a `run-unit --op Dedup` is the `SortExec` (the sort
   OOMs a 1 GB pool on one 204 MB bin, so the ceiling is high).
2. Read loss: re-run the latency matrix with a service-filtered shape, which the
   existing matrix does not currently isolate.

ClickHouse's own resolution suggests a third option worth pricing first: keep the
sort dedup-key-leading AND recover service pruning from an index rather than the
sort (bloom/tantivy already exist for other columns; a service bloom is cheap).
That would get both, and is why their design separates sort key from index.

Not shippable overnight in any variant: every path changes what queries read.

### The deciding number: the sort is 81% of a dedup rewrite (5.4x ceiling)

`TF_BENCH_PARQUET=<204 MB real prod bin> TF_BENCH_POOL_MB=1024 cargo bench
--bench rewrite_throughput`:

| variant | secs | MB/s |
| --- | --- | --- |
| **scan only** | **2.2** | **94.24** |
| sort b2048 p1 | 13.2 | 15.49 |
| sort b256 p1 | 19.4 | 10.54 |
| sort b256/b2048/b8192 p8, b8192 p1 | **FAILED — external sort OOM at 1 GB** | — |
| **PROD (b256 p1 x13 slices)** | **11.6** | **17.58** |

**The sort is 9.4 s of prod's 11.6 s — 81% of the unit.** Removing it takes a
rewrite from 17.58 MB/s to ~94 MB/s: a **5.4x ceiling** on the operation that is
~96% of maintenance worker time. It also removes the OOM: every `p8` variant and
`b8192 p1` fail outright at a 1 GB pool, which is the same external-sort failure
prod has been fighting all month.

So the layout question is worth real money: **~5x on the fleet's dominant cost,
plus the memory hazard.** That is the 10x-class change; the claim reservation was
1%.

It still must be paid for on the read side — `service_name` pruning depends on the
secondary sort position — so the morning sequence is:

1. Service-filtered latency matrix (the read cost, currently unmeasured).
2. If the read cost is material, price ClickHouse's separation instead: keep the
   sort dedup-key-leading and serve `service_name` from a bloom index, which the
   codebase already does for `id`/`trace_id`/`span_id`.
3. Land whichever wins, then re-run this bench to confirm the sort is gone.

Both sides are now quantified except the read cost, which is one matrix run.

### The read cost is MEASURED, and it is near zero — the trade resolves in favour of the layout change

The blocker was "moving `service` after `id` regresses service-filtered pruning,
cost unknown". Measured on prod, read-only, 1-hour windows:

**Selectivity** — a service filter on `dcad860a` selects **50,114 of 50,333 rows
(99.6%)**. Timings identical (0.31 s filtered vs 0.33 s unfiltered): no pruning
benefit, because there is nothing to prune.

**Cardinality** — distinct services per busy project-day:

| project | services | rows |
| --- | --- | --- |
| 28f62f01 | **1** | 197,973 |
| 6297304f (whale) | 2 | 1,118 |
| dcad860a | 3 | 50,334 |
| 87576849 | 19 | 123,063 |

**Three of four busy projects have 1-3 services.** Sorting by `service` at
position 2 clusters data that is already homogeneous — it cannot prune what does
not vary. The schema comment ("`service_name` by the secondary sort column")
describes an intent the data does not support at current cardinality.

**So the trade is:** ~5.4x on dedup rewrites (the sort is 81% of a unit) and the
removal of the external-sort OOM, against a read cost that is measurably ~zero for
3 of 4 busy projects and modest for the fourth (19 services over 123k rows).

**Recommendation: make the change** — `sorting_columns` leading
`timestamp DESC, id ASC`, `service` after. `timestamp` stays the leading column,
so time-range pruning (what every dashboard query uses) is untouched. For
`87576849`-shaped projects, recover pruning with a `service_name` bloom index, as
the codebase already does for `id`/`trace_id`/`span_id`.

Not landed in this session only because it is a physical-layout change that
deserves a full suite run and a before/after on the latency matrix, and the
session is out of budget to do that properly. The EVIDENCE no longer blocks it —
this is now a scheduling question, not an open risk.

### The layout change is UNRESOLVED, not refuted — my inference was wrong

Sequence, recorded exactly:

1. Full suite BEFORE the sort-order change: **1314/1314 green**.
2. Sort-order change applied (`timestamp, id` leading): **1314/1315**, with
   `a_chart_under_a_derived_table_routes_and_agrees_with_raw` failing
   ("a derived table only re-qualifies; the chart under it must route").
3. Re-ran that test IN ISOLATION with the change: failed. I concluded the change
   broke rollup routing, and reverted.
4. Re-ran it in isolation with the change REVERTED (clean tree): **still fails.**

So the test fails in isolation independent of the change — it depends on state
other tests in the suite establish. **It was never evidence about the sort order,
and my conclusion in step 3 was wrong.**

**Status of the layout change: UNKNOWN.** The single suite failure in step 2 may
have been this same isolation/ordering artefact surfacing under parallelism, or a
real regression. One run cannot distinguish them.

**To resolve** (do this before touching the layout again):
1. Re-run the FULL suite 2-3x with the change applied. If the failure recurs
   consistently, it is real; if not, it is the artefact.
2. Fix or characterise the test's isolation dependency either way — a test that
   fails alone is a broken instrument, and it cost a wrong conclusion here.

**Also corrected: the "read cost is ~zero" measurement was confounded.** The
schema comment (lines 34-38) says `service_name` at position 2 buys **page-level**
pruning within row groups. My timing test used a project with 1-3 services where a
service filter selects 99.6% of rows — there was nothing to prune, so the
experiment could not detect the cost it was meant to measure. Re-measure on
`87576849` (19 services) or synthetically, at the page level, not by wall clock.

Tree left at the known-good state (original ordering, suite green at 1314/1314).

### RESOLVED: the layout change DOES break rollup routing (2/2 reproducible)

Final evidence, after correcting my own correction:

| tree | full suite |
| --- | --- |
| original ordering | **1314/1314 green** |
| `timestamp, id` leading (run 1) | 1314/1315 — `a_chart_under_a_derived_table_routes_and_agrees_with_raw` FAILS |
| `timestamp, id` leading (run 2) | 1314/1315 — same test FAILS |

**Reproducible 2/2 against a green baseline. The change breaks rollup routing.**

The isolation result was a red herring: that test also fails when run alone with
the ORIGINAL schema, because it depends on state the suite establishes. I briefly
concluded from that the change was exonerated — wrong. **Isolation behaviour said
nothing; the full-suite delta is the signal**, and it is unambiguous.

Assertion: *"a derived table only re-qualifies; the chart under it must route"* —
so with dedup-key-leading files, a chart under a derived table stops routing to
its rollup and falls through to raw. That is a dashboard-latency regression, i.e.
the change trades a 5.4x maintenance win for a read-path loss.

**So `sorting_columns` cannot simply be reordered.** The 81%-of-a-rewrite sort is
still the right target, but the remedy must not move `service` out of position 2.
Open options, none yet tested:

1. Find why routing depends on sort order (`dedup_compaction_test.rs:2748` is the
   entry point) and decouple it — most likely the real fix.
2. Declare the ordering only for the REWRITE's provider (`narrow_provider`),
   leaving the written layout unchanged — but this needs an ordering the files
   actually have, which today they do not for `(timestamp, id)`.
3. Sort by `(timestamp, id)` only for files that no rollup routes over.

Tree restored to the green baseline. Net for the night: the target is quantified
(81% of a unit, 5.4x, plus the OOM) and one remedy is now definitively excluded.

### Why routing depends on sort order — the mechanism, named

`sorting_columns` is not just physical layout: it becomes the parquet footer's
**advertised ordering**, which the read path consumes directly. See
`src/read/mod.rs`:

- `:98` — "a parquet footer's `sorting_columns` is lying" (the ordering probe)
- `:379` — "footer missing/misreporting `sorting_columns` makes a scan declare..."
- `:586` — "no footer `sorting_columns` and ONE unordered branch erases the
  ordering"

So changing `sorting_columns` changes what every scan ADVERTISES, and an
advertised ordering that no longer matches what a consumer requires can silently
disqualify a plan — which is consistent with the chart under a derived table
ceasing to route.

**Morning entry point, one command to reproduce:**

```
cargo nextest run a_chart_under_a_derived_table_routes_and_agrees_with_raw
```

(fails alone for an unrelated isolation reason — judge it by the FULL suite:
green at 1314/1314 on the original ordering, 1314/1315 twice with the dedup-key
ordering.)

Trace from `rollup_hits_hybrid`/`rollup_hits_full` back to whatever ordering
precondition the derived-table route checks. If routing can be decoupled from the
advertised ordering — or if the rewrite provider can declare an ordering without
changing what files advertise — the 5.4x is reachable without touching reads.

### Narrowing: the route CHECK has no ordering precondition

`src/rollup.rs` (where `rollup_hits_hybrid`/`_full` are recorded) contains no
sort- or ordering-based decline condition. So the sort-order coupling is NOT in
the routing predicate.

That redirects the search: with dedup-key-leading files, the rollup BUILD or its
COVERAGE most likely produces different content/extent, and the query then finds
nothing to route to. Look at the build and coverage path first, not the route
check — the opposite of where the failing assertion points.

(`rollup_hits_*` are also incremented in `src/dml.rs` and
`src/database/maintain.rs`; `src/observability.rs:840` is the sink.)
