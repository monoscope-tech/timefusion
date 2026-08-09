# Dashboard query profiling — prod, 2026-08-09

Method: monoscope's real `_overview.yaml` widget SQL, run as `EXPLAIN ANALYZE`
against prod pgwire (`timefusion.s.past3.tech`), one query in flight at a time.

**Second correction: the box was NOT otherwise idle.** Sampling container CPU
with none of my queries running gave **575%, 653%, 676%, 743%, 806%, 930%,
1021%, 1683%** — 6-17 cores of background maintenance (`dirty_bin_queue_depth`
250, `dirty_bin_rewrite_duration_ms_total` 11.27 M ms, `cron_long_running_total`
6). So "serial" means only that I issued one query at a time; every timing below
was taken while heavy dedup rewrites competed for CPU, for the shared pools and
for object-store bandwidth. That is representative of real prod conditions, but
it is NOT a controlled benchmark, and it means prod cannot currently separate
I/O-wait from decode from contention — the background load varies 3x between
consecutive samples. Bin widths are monoscope's own adaptive
ones (`calculateAutoBinWidth`): 1h→30s, 3d→1h, 7d→6h, 14d→6h.

Three queries per (project, range):

- **throughput** — Golden Signals *Traffic*: `count(*)` by `time_bucket`,
  `kind='server' OR name IN ('apitoolkit-http-span','monoscope.http')`.
- **latency** — Golden Signals *P95* and *Request Latency Percentiles*: the
  `bucket_digests` CTE (`percentile_agg` per bucket, `CROSS JOIN` 5 quantiles) —
  this is exactly what `Pkg.Parser` emits for `summarize percentiles(...) by bin_auto`.
- **services** — *Services Health* table: apdex + throughput + error rate + p95
  `GROUP BY resource___service___name`.

## Headline

1h is fine (0.3–1.0 s). 3d is 17–31 s, 7d is 29–69 s, 14d is 55–163 s — and for
the busiest project 14d **fails outright**. Below 14 days the cost is
essentially all parquet scan *wait* — not dedup, not aggregation, not planning.
At 14 days a separate, older problem reappears for one project (§5).

**These numbers are a best case, and by a large factor.** I ran one query at a
time. A real Overview tab load fires ~15 wide queries at once, and
`heavy_scan_sem` is a single process-wide semaphore (`Database` builds one at
src/database.rs:2479; `gate_if_wide` clones it) with **16 permits shared across
all queries and all tenants**. So a concurrent dashboard load does not get 15×
the throughput — it queues on the same 16 permits, and every other tenant's
wide query queues behind it too. Treat the tables below as the floor.

## Wall clock — 60 runs

| project | query | 1h | 3d | 7d | 14d |
|---|---|---:|---:|---:|---:|
| monoscope-self | throughput | 0.3 s | 18.2 s | 43.3 s | 286.7 s |
| monoscope-self | latency | 0.4 s | 31.4 s | 47.7 s | 284.4 s |
| monoscope-self | services | 0.5 s | 30.5 s | 69.1 s | 214.1 s |
| default/shared | throughput | 0.5 s | 16.8 s | 33.1 s | 117.1 s |
| default/shared | latency | 0.6 s | 19.8 s | 34.4 s | 162.9 s |
| default/shared | services | 0.9 s | 24.0 s | 43.0 s | 141.2 s |
| proj-dcad | throughput | 0.4 s | 22.9 s | 46.3 s | 101.5 s |
| proj-dcad | latency | 0.8 s | 25.9 s | 47.4 s | 109.1 s |
| proj-dcad | services | 0.7 s | 22.6 s | 40.4 s | 125.0 s |
| proj-be87 | throughput | 0.4 s | 24.4 s | 29.6 s | 54.7 s |
| proj-be87 | latency | 1.0 s | 19.9 s | 28.7 s | 61.3 s |
| proj-be87 | services | 0.7 s | 25.6 s | 42.5 s | 59.7 s |
| blockradar | throughput | 0.4 s | 0.4 s | 6.9 s | 98.0 s |
| blockradar | latency | 0.3 s | 0.3 s | 4.9 s | 106.9 s |
| blockradar | services | 0.3 s | 0.3 s | 4.7 s | 120.2 s |

monoscope-self's three 14d cells are **failures**, not slow successes — see §5.

> **CORRECTION (2026-08-09, before any code was written).** Sections 2-4 below
> originally derived an I/O-vs-decode split, an "effective read concurrency", and
> a per-stream MB/s from `time_elapsed_scanning_total` and
> `time_elapsed_processing`. **That derivation was wrong.** DataFusion 54 defines
> `time_elapsed_scanning_total` as "scanning + record batch decompression /
> decoding", and adds "this metric also includes the time of the parent
> operator's execution"; `time_elapsed_processing` is "data decompression +
> decoding / time spent waiting for the FileStream's input". Since
> `GatedScanExec` is the parent operator, semaphore *queueing* is very likely
> inside `scanning_total` too. So these numbers do NOT separate object-store wait
> from decode from gate-queueing, and the claims "97.8% of the gated window is
> waiting", "effective concurrency 4-6" and "0.9-2.9 MB/s per stream" are
> RETRACTED. The wall-clock table, the plan-shape findings, the read
> amplification, the dedup-mode boundary and the repeat test are unaffected —
> they are direct measurements, not derived from these two counters.

## The five measurements that explain it

### 1. Read amplification: we decode 100–450× more rows than we keep

`supports_filters_pushdown` marks every dashboard predicate `Unsupported`, so
the only thing pushed to parquet is `timestamp >= …`. `kind`, `name`,
`duration IS NOT NULL`, `status_code`, `resource___service___name` are all
applied in a `FilterExec` **above** `DedupExec`.

This is deliberate and correct: `Database::version_mutable_columns`
(src/database.rs:12904) treats every column except the dedup keys
(`timestamp`, `id`) and partition columns (`project_id`, `date`) as
version-mutable on a `version_append` table, because a predicate applied below
the dedup can select a superseded version and drop the newer one (the
2026-08-02 `status_code='OK'` bug). `Inexact` is not sufficient — it has to be
`Unsupported`.

The price, measured:

| project | range | rows decoded | rows kept | amplification |
|---|---|---:|---:|---:|
| monoscope-self | 7d | 15,970,000 | 35,890 | **445×** |
| monoscope-self | 3d | 6,160,000 | 26,630 | 231× |
| default/shared | 7d | 15,810,000 | 3,660,000 | 4.3× |
| proj-be87 | 3d | 3,670,000 | 489,400 | 7.5× |
| proj-dcad | 7d | 15,890,000 | 1,760,000 | 9.0× |

Row-group pruning confirms it: on monoscope-self 7d only **26 of 219 row
groups** are pruned, by timestamp alone.

### 2. The 1h→3d cliff is a plan-shape change, and it is exact

`GatedScanExec` appears in **all 30** of the 3d/7d plans and **none** of the 15
1h plans. The 1h plans use `HotLegPooledExec` instead. Three settings all cut
at roughly the same boundary, and they compound:

- **The hot tier stops covering the window.** `timefusion_hot_tier_retention_hours`
  defaults to **24**, so nothing beyond a day can be served from local disk.
  `hot_tier|unproven_windows_total` = **334** counts exactly these fallbacks
  (`read_misses_total` is 0 — when it covers, it always hits).
- **The wide-scan gate engages.** `timefusion_wide_scan_lookback_hours` = **2**,
  so any scan reaching back further is throttled to
  `timefusion_max_concurrent_scan_readers` = **16** concurrent batch-polls
  *across all queries*. `gate_if_wide` does have a release valve — a wide scan
  that selected `<= timefusion_wide_scan_max_files` (256) **and**
  `<= timefusion_wide_scan_max_mb` (**64**) is let through — but these queries
  select 85–445 MB, so the **64 MB byte threshold** is what keeps every one of
  them gated. File count (18–105) is never the binding constraint.
- **Wide scans do not populate the Foyer cache.** `timefusion_cache_bypass_scan_hours`
  = **24**, and `gate_if_wide` passes `bypass_cache` into `GatedScanExec`, which
  wraps the fetch in `scan_bypass_scope(true, …)`. Precisely: `bypass_active()`
  is consulted in exactly one place — `FoyerCache::admit`
  (src/object_store_cache.rs:1487) — so this suppresses cache **population** of
  data blocks, not cache reads.

  I tested what that costs by running one 3d query three times back to back:
  **18.79 s -> 13.68 s -> 11.38 s.** So repeat runs *do* improve, by ~39%, then
  plateau — the gain comes from the caches the bypass does *not* touch (parquet
  metadata: `metadata_cache_hits` 85,559 vs 7,716 misses; the provider cache at
  80.1%), not from data blocks being admitted. A repeatedly-refreshed dashboard
  panel therefore settles at ~11 s rather than converging toward the sub-second
  behaviour a fully warm working set would give.

The gate exists for a real reason — Parquet decode heap is untracked by the
DataFusion memory pool, and 48 concurrent decoders OOM-restarted prod from one
7-day dashboard on 2026-07-20. But `GatedScanExec::execute` holds its permit
across the entire `poll_next`, and the object-store fetch happens *inside* that
poll (src/database.rs:13284-13317). So a gate sized for **decode heap** is also
capping **I/O concurrency** — and I/O is where the time actually goes:
`time_elapsed_scanning_total` 413 s vs `time_elapsed_processing` 9 s on the
worst run, i.e. **97.8% of the gated window is waiting, not decoding.**

`scan_decode|polls_inflight_peak` = **17** confirms the gate saturates;
`pressure_throttled_total` = **0** confirms the pressure valve is not the cause.

### 3. Effective read concurrency is 4–6, no matter what the plan says

`time_elapsed_scanning_total / wall` — with nothing else running:

| run | declared file_groups | files opened | scan-stream s | wall s | effective concurrency |
|---|---:|---:|---:|---:|---:|
| monoscope-self 7d services | **48** | 105 | 413.2 | 69.1 | **6.0** |
| monoscope-self 7d latency | 48 | 104 | 284.8 | 47.7 | 6.0 |
| default/shared 7d services | 6 | 28 | 207.1 | 43.0 | 4.8 |
| proj-dcad 7d throughput | 4 | 28 | 177.7 | 46.3 | 3.8 |
| proj-be87 3d services | 5 | 18 | 75.5 | 25.6 | 3.0 |

48 declared partitions still yield 6-way concurrency. The scan is not
I/O-parallel — and this is *wait*, not CPU: `time_elapsed_processing` (decode)
is only 2–10 s against 150–413 s of scanning.

### 4. Per-stream read rate collapses 10× once the window leaves the cache

| range | MB/s per scan stream |
|---|---:|
| 1h | 10–24 |
| 3d | 0.9–2.9 |
| 7d | 0.9–2.9 |

The absolute volumes are tiny — 85–445 MB — so this is round-trip latency, not
bandwidth. Foyer explains the cliff: L2 is **full** (118.5 GB of 120 GB, 12,281
evictions) and `cache_recent_days=8`, so a 14-day window is half uncacheable
by construction.

### 5. At 14 days the whale does not run at all — and it is ONE partition

All three monoscope-self 14d queries **failed** after 214–287 s:

```
ERROR:  Resources exhausted: unordered merge-on-read dedup exceeded its
        2048 MiB per-query limit; narrow the time window or compact unsorted files
```

Plan-only `EXPLAIN` (no scan, no OOM risk) locates the boundary exactly:

| project | 7d | 8d | 9d | 10d | 12d | 14d |
|---|---|---|---|---|---|---|
| monoscope-self | bounded | **bounded** | **full** | full | full | full |
| default/shared | — | — | — | — | — | bounded |
| proj-dcad | — | — | — | — | — | bounded |
| proj-be87 | — | — | — | — | — | bounded |
| blockradar | — | — | — | — | — | bounded |

So the ordering breaks between **day 8 and day 9** — around `date=2026-07-31` —
and **only for monoscope-self**. Every other project is `bounded` at 14 days.
One partition's unsorted / footer-less files void the scan's ordering, `DedupExec`
falls back to `mode=full`, buffers the whole set and dies on the 2 GiB cap.

This is a correction to the plan's 2026-08-09 note ("bounded at every width").
That was measured at 3d/7d/14d on *talstack*; it does not hold for this project
past 8 days. Repairing that single partition should take monoscope-self's 14d and
30d dashboards from ERROR to merely slow — much the highest value-per-effort item
in this whole document, and the probe above finds it without ever running the
expensive query.

## What is NOT the problem

- **Dedup — for four of the five projects, up to 14 days, and for the fifth up
  to 8 days.** `mode=bounded[timestamp]` on every executed plan. It removes 0.3–1.5% of rows (worst: proj-dcad,
  4.6–11.5%) and costs 1–4.9 s of the 20–70 s. This confirms the plan's
  2026-08-09 update for those cases — but see §5 for where it does not hold.

  **blockradar is the exception, and it is a different problem.** Its 14d
  services plan scans **77.73 M rows** and `DedupExec` emits **15.77 M** —
  **79.7% of what we read from storage is superseded row versions**, a 4.9×
  version amplification. Dedup there costs 16.9 s + 12.3 s of merge, ~29 s of
  the 120 s. That is not a read-path bug; it is a signal that this project's
  merge-on-read churn is outrunning the dedup sweep, and the fix is
  maintenance-side (get those partitions swept), not query-side.
- **Planning.** `scan|provider_scan_us_avg` = 49.9 ms, `mem_plan_us_avg` = 15 ms.
  Real, but it is the floor under the 0.3–1.0 s 1h queries, not the 20–70 s ones.
- **Spilling.** `spilled_bytes=0` on every node of every run.
- **Aggregation.** The `percentile_agg` sketch costs almost nothing: latency and
  throughput differ by <15% at the same width.

## Secondary effect worth naming: the dedup key projection

Dedup forces `id`, `updated_at`, `deleted` and `__delta_rs_file_id__` into the
projection. A throughput query reads 7 columns of which **4 exist only for
dedup/tombstones**. Column count costs more than its bytes suggest — more
columns means more byte ranges means more round trips:

| run | scanned MB | scan-stream s |
|---|---:|---:|
| monoscope-self 7d throughput | 356 | 256.9 |
| monoscope-self 7d services | 378 (+6%) | 413.2 (**+61%**) |

## Blast radius

From `timefusion_stats` at the time of the run: `scan|total` 39,109, of which
`mem_only` 33,101 (85.1% `skipped_delta`), `mem_plus_delta` 3,674 and
`delta_only` 2,334. So **~15% of scans touch Delta at all** — and that 15% is
where the 20–70 s lives. The other 85% are the recent-window panels that are
already fast. Any fix here is aimed at a minority of queries that consume the
overwhelming majority of read time.

## Counters worth fixing while here

- `parquet|selected_row_groups` reads **0** — dead counter.
- `read_dedup|ordering_violations_total` = **61,120**. Plans all show
  `bounded`, so this is not currently costing a full-set fallback, but a
  five-figure violation count against a repaired footer set deserves a look.
- `plan_cache|hit_pct` = **50.1%**, with `shape_hits` 109 vs `shape_skips` 20 —
  `TIMEFUSION_PLAN_CACHE_TIME_FNS` is effectively unused in prod.


## §6. Maintenance contention: TESTED AND REFUTED (2026-08-09)

The "6-17 cores of background maintenance" finding above raised the obvious
hypothesis that dashboard latency is really maintenance contention. I tested it:
the same 3d throughput query, 12 times, recording container CPU and
`dirty_bin_rewrite_duration_ms_total` around each run.

```
n=12   wall mean 15.53 s, sd 2.09, range 12.37-19.95
       cpu  mean 406%,    range 209-620%
       Pearson r(wall, cpu) = -0.130
```

**No correlation** — slightly negative if anything, and the two slowest runs
(19.95 s, 17.25 s) landed at the two LOWEST CPU samples (248%, 210%). An
eyeball of the first 8 samples suggested ~17%; computing r over all 12 removed
it. Maintenance CPU is not what makes these queries slow.

`rewrite_ms_delta` was 0 for 11 of 12 runs, so dirty-bin rewrites were not even
running during most of the test — the CPU is footer repair, ingest and flush.

**What this leaves as the best-evidenced lever: warmth.** The identical query
measured 24.37 s during the matrix, 18.79 -> 13.68 -> 11.38 s in the repeat
test, and 15.53 s mean an hour later. That is a persistent ~35% improvement
from cache state, and it is the only large, reproducible effect I have actually
measured. It promotes item C above B.

## Where the leverage is, in measured order

Ordered by (measured impact ÷ effort), not by the existing plan's ordering.

### A. Repair the one partition that breaks monoscope-self past day 8

Highest value per unit of effort by a wide margin: it converts three hard
failures into working queries, it is one project and one date, and §5's
`EXPLAIN`-only probe verifies the fix in seconds without running the query.

### B. Split the wide-scan gate's one permit into an I/O permit and a decode permit

**Status: premise unproven, and possibly unimplementable.** The code fact holds
(the permit spans the object-store fetch), but the metrics cannot separate I/O
from decode from gate-queueing, `poll_next` fuses fetch and decode, and moving
decode outside the permit would recreate the unbounded decode heap the gate
exists to prevent. Do not build this until a controlled benchmark establishes
the split. Ranked below C now. `GatedScanExec` holds a
single permit across a `poll_next` that is **97.8% object-store wait and 2.2%
decode**. Fetch the batch's byte ranges *outside* the permit, hold it only
across the decode: the OOM protection the gate was built for (2026-07-20) keeps
exactly its current strength, while all 48 partitions get their reads in flight
instead of 4–6. Everything in §2/§3/§4 is downstream of this one conflation.

Do **not** simply raise `timefusion_max_concurrent_scan_readers` — that raises
decode-heap concurrency too, which is precisely what OOM-restarted prod.

### C. Let wide scans populate the cache, or give them a reason not to

`timefusion_cache_bypass_scan_hours` = 24 stops a wide panel admitting its own
data blocks. Measured, that costs the difference between the ~11.4 s plateau the
repeat test reached and what a fully warm working set would give — it is not the
whole 18.8 s, because metadata and provider caching still warm (§2). The bypass
exists to stop a one-off wide scan evicting the hot set, a real concern with L2
already full at 118.5/120 GB. But a *dashboard* panel is the opposite of a
one-off: it is the most repeated query in the system. Distinguishing "wide and
repeated" from "wide and ad-hoc" — even crudely, admitting on the second
identical scan — would let these converge instead of plateauing.

Worth sizing before building: the repeat test says the ceiling here is bounded,
so this ranks below B despite being conceptually similar in cost.

### D. Rollups (§5 of the existing plan)

Still the right end-state and my numbers raise its value: it removes both the
read amplification and the file opens rather than making them cheaper — a 7d
throughput panel would read a few hundred KB instead of 378 MB across 105
objects. It is also much the largest build. A and B are worth doing first
because they are constant-factor wins that also help every ad-hoc query no
rollup can anticipate.

### E. Predicate pushdown on dedup-certified-clean partitions

New, and strictly larger than the dedup skip it generalizes.
`dedup_skip_allowed` (src/database.rs:13036) already computes "this
(project,date) partition was swept clean and nothing has committed since", and
already refuses outright on `version_append` tables. But a partition certified
duplicate-free has exactly one version per key, so within it there is no
superseded version for a pushed predicate to select — the soundness argument in
`version_mutable_columns` does not apply. Lifting that bail buys **both** the
dropped key projection *and* `kind='server'` reaching the parquet reader, i.e.
it attacks the 445x amplification rather than just the 4-columns-of-7
projection. That is a bigger prize than the currently-inert
`timefusion_read_dedup_skip_swept`.

Two honest caveats. `dedup_window_clean` (src/database.rs:7615) requires
**every** date in the window to be certified, so a 7d query needs 7 clean dates
— one dirty day disables it entirely. And `dedup_clean_fp` is an in-memory
`DashMap`, so every restart starts cold; given how often this process restarts,
the certification would need to survive a restart to be worth much at 7d/14d.

### F. Coalesce the Golden Signals scans (§4 of the plan)

Traffic, P95 and Error Rate are three separate near-identical full scans of the
same rows on every dashboard load. One shared per-bucket aggregate cuts
Overview's cost ~3x independently of everything above — and because the gate is
process-wide, removing two of every three wide scans also shortens the queue
for everyone else.

### G. Extend hot-tier coverage — but price it first

`hot_tier` is at 79.6 GB of its 128 GB cap for **24 h** of retention, so
covering 3 days costs roughly 240 GB and does not fit today. This is a
disk-budget decision, not a knob turn.

### Not this

**blockradar's dedup load is a maintenance problem, not a read-path one.** Its
79.7% version amplification wants the dedup sweep to catch up on that project,
not a query-planner change.

## Raw data

`out/*.txt` — full `EXPLAIN ANALYZE` dumps, `<project8>_<range>_<query>.txt`.
`bench.sh` regenerates any cell; `table.py` / `bytes.py` rebuild the tables.

## Implementation status of A-G (2026-08-10)

| item | status | commit |
|---|---|---|
| A repair the day-9 partition | **done** (code fix, not an ops action) | `fix(repair): walk down sort parallelism…` |
| B gate: I/O vs decode | **done**, reframed — see below | `perf(scan): charge the wide-scan gate for decode HEAP…` |
| C cache admission for repeated wide scans | **done** | `perf(cache): let a REPEATED wide scan warm…` |
| D rollups | **specified, not built** — see below | — |
| E pushdown on swept partitions | **done** | `perf(read): push dashboard predicates into a sweep-certified window` |
| F coalesce Golden Signals | **specified, not built** — needs a monoscope feature | — |
| G hot-tier extension | **done** (disk-bound window) | `perf(hot-tier): let the disk cap…` |

**B was reframed, deliberately.** "Fetch outside the permit" is not implementable:
`poll_next` fuses fetch and decode, and moving decode to an ungated producer
recreates the unbounded decode heap the gate exists to prevent. What shipped
attacks the same waste from the measurable side — the gate charged every poll
for a 145 MB worst-case batch while real batches measured 0.19-0.23 MB — and
leaves the heap ceiling untouched, which was the explicit constraint.

### D. Rollups — why this is specified rather than started

CLAUDE.md is explicit: "Minimum code that solves the problem. Nothing
speculative", "Delete unused code completely". A routing predicate with no
rollup table behind it, or a build pipeline nothing reads, is exactly the dead
scaffolding those rules forbid — so this is either built whole or not begun.
Whole is a multi-session feature. The spec below is what it needs; §5a-5d of
`2026-07-16-dashboard-query-performance.md` remains the design of record.

Build order, each step independently useful and testable:

1. **Sibling Delta table** `otel_rollup_1m`, same `[project_id, date]`
   partitioning so `ProjectRoutingTable` gives multi-tenant isolation free.
   Columns: `bucket`, the dimension set, `request_count`, `error_count`,
   `duration_sum/min/max`, and a mergeable t-digest as a binary column.
2. **Build trigger is the certification signal, not a timer.** Build a bucket
   only from a bin dedup has certified clean and invalidate it when that bin
   re-enters the dirty queue. `dedup_window_clean` and `dirty_bin_*` already
   track precisely this — and E now depends on the same signal, so a bug in it
   is already load-bearing and already tested.
3. **Routing predicate** — the part §5a says gets missed: route only if every
   GROUP BY key AND **every filtered column** is a dimension, every aggregate is
   decomposable, and the range covers whole buckets. Land this WITH the table,
   not before.
4. **Grain cascade** by re-aggregating 1m -> 1h -> 1d; the merge is associative
   so there is no second pipeline. Planner picks the coarsest grain <= the
   requested bucket width.
5. **`rollup_hit` / `rollup_miss{reason}`** from the first commit. Without the
   miss-reason breakdown there is no feedback loop telling us which dimension to
   add next.

Dimension budget: rows per bucket is roughly the product of the dimensions'
distinct counts, and at 1m grain that product must stay in the low thousands.
Start with `resource___service___name`, `kind`, `status_code`.

### F. Coalesce the Golden Signals — blocked on a monoscope capability

Traffic, P95 and Error Rate are three separate full scans of identical rows on
every Overview load, differing only in aggregate. Because the wide-scan gate is
process-wide, removing two of three also shortens the queue for every other
tenant — so this is worth more than a 3x on one dashboard.

It cannot be done in `_overview.yaml` as it stands. Both existing sharing
mechanisms are the wrong shape:

* `constants` expand via `constantToSQLList` to a VALUE LIST — `('a','b')` for
  `IN` clauses — not a reusable subquery or CTE.
* `Widget.queries :: Maybe [Query]` combines several queries INTO one widget; it
  does not let three widgets share one result.

So F needs a monoscope feature: a widget that declares its data comes from
another widget's already-executed result. Smallest version —

1. add `source :: Maybe Text` to `Pkg.Components.Widget`, naming a sibling
   widget's `id`;
2. in `Pages.Dashboards`, execute source widgets first, then hand each dependent
   widget the cached result instead of issuing its own query;
3. in `_overview.yaml`, add one hidden `sql:` widget computing per bucket:
   `count(*)`, `count(*) FILTER (WHERE status_code='ERROR' OR …>=500)`,
   `percentile_agg(duration)` — the exact shape §4 of the 2026-07-16 plan
   specifies — and point the three tiles at it via `source:`.

The three tiles keep their current appearance, which is why this is a data-flow
change and not a product decision. It is a monoscope change end to end; nothing
in TimeFusion blocks it.

