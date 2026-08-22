# Keep tantivy, fix how it is consulted

> **Status 2026-08-21 night — increments 1 and 2 shipped and verified in prod.**
>
> **Increment 1** (`8229acf`): **D0** — per-phase timers + fan-out counters as
> the `tantivy` component of `timefusion_stats`; **D1** — seed-on-publish,
> hot-window re-warm cron, `prefetch_days` 0→3, `search_concurrency` 8→32,
> `manifest_ttl` 5s→300s, `reader_cache_entries` 256→2048, `cache_disk_gb`
> 64→200. The property the increment exists for: **an index this process built
> is never fetched back from S3.**
>
> **Increment 2** (`7602c0a`): **D5** — the coverage drain, i.e. the
> keep-S3-current half. Reconcile went daily→**hourly**, made possible by
> bounding a pass (`backfill_max_files_per_pass`, default 400); it was
> unbounded, which is what forced the nightly cadence. Plus two fixes D0's own
> counters exposed in increment 1: the manifest cache (see below) and a
> sequential `warm_recent` that prod logged as "still in progress after 600s".
>
> **Measured on prod** — 5 interleaved reps per sample, minimums, in-round
> `SELECT 1` control. Four samples across both deploys; only the tantivy-routed
> shapes ever move, which is what makes the attribution credible:
>
> | shape | pre | inc1 +15m | inc1 +50m | inc2 |
> |---|---|---|---|---|
> | `trace_id =` @7d (routed) | 2481 ms | 579 | 484 | **436 ms** |
> | `trace_id =` @30d (routed) | 2652 ms | 720 | 674 | **649 ms** |
> | range @7d (control) | 198 ms | 202 | 185 | 192 |
> | range @30d (control) | 312 ms | 347 | 336 | 323 |
> | `SELECT 1` (control) | 34 ms | 36 | 35 | 39 |
>
> **5.7x at 7d, 4.1x at 30d.** The equality tax fell from ~2.2s to ~250-330ms
> above the unrouted baseline, with tantivy still serving LIKE/regex/substring.
> That **materially weakens the earlier P0 case for flipping `route_equality`
> off** — the tax it was meant to remove is now ~300ms, not 2.2s. Do not flip
> it without re-measuring.
>
> Supporting counters: `search_us_avg` 13.7ms → **0.34ms** (indexes local, not
> fetched), `reader_hit_pct` 83% → **96.9%**, and after the increment-2 restart
> `blob_fetches = 0` — the extracted cache survived the restart and served
> every query, which is the local-first guarantee holding across a deploy.
>
> **D0 paid for itself immediately** by exposing a defect in the same increment
> that shipped it: `manifest_hit_pct = 0.0` across 56 loads for 54 queries,
> because invalidating the cached manifest on every publish threw away the entry
> the next query needed (busy projects publish far more often than they are
> queried). Increment 2 folds the entry in instead. Verified by controlled test
> rather than by the aggregate percentage — 3 identical queries now cause **zero**
> additional manifest loads. The fleet-wide percentage stays low only because
> real traffic sweeps many distinct projects, each a forced first miss.
>
> **Still deferred:** D2 (per-day index granularity), D3 (prefilter off the
> planning path), D4 (benefit-based routing). Stated so the narrowing is visible.
>
> **Observed after shipping — the cap is tuned too high.** At the measured
> throughput a 400-file pass runs ~2 hours, so the reconcile is effectively
> CONTINUOUS rather than hourly: prod logged `Tantivy reconcile job run still in
> progress after 600s … (skips=1)`, i.e. the next tick was correctly dropped
> rather than piling up. The drain itself is healthy and converging
> (`cache_seeded` 100 → 421 in ~50 min, with query latency flat throughout), so
> this is not a functional defect — but `deferred_to_next_pass` and the
> `uncovered` gauge only update at pass end, so observability is far coarser
> than the bounded-pass design intended. **Next deploy should lower
> `timefusion_tantivy_backfill_max_files_per_pass` to ~150**, which at measured
> rates gives a ~30-minute pass that fits inside the tick and reports each time.
> Deliberately not deployed tonight: prod has already taken three restarts, and
> this changes reporting cadence, not correctness.
>
> **Known gap carried:** `gc_after_compaction` prunes manifest entries without
> invalidating this process's cache, so for up to `manifest_ttl` a query can
> consult an entry whose blob is gone. Soft — the prefilter treats it as "no
> usable index" and falls back.

2026-08-21 evening. Follow-up to
`2026-08-21-point-lookup-file-open-wall.md` (P0: flip
`TIMEFUSION_TANTIVY_ROUTE_EQUALITY=false`) and
`2026-08-21-planning-floor-attributed.md` (the controls).

**Premise change:** the flip is a tourniquet, not a fix. Tantivy is the only
mechanism we have for `LIKE`, regex and substring search — blooms and min/max
cannot serve those at all. So the goal is not "stop using the index", it is
**"stop paying O(files) synchronous object-store work on the planning path to
consult it."** This doc is the architectural attribution and the options.

## What the read path actually does today

`src/database/mod.rs:8593` → `TantivySearchService::search_with_stats`
(`src/tantivy/search.rs:123-215`), per query, synchronously, before planning
finishes:

1. `load_manifest_cached` — one manifest GET per (table, project), 5s TTL.
2. Time-prune manifest entries by `[min,max]_timestamp_micros`.
3. **One task per surviving index**: `ensure_cached` (dir stat; on miss an S3
   GET + untar) → `open_cached` (tantivy open + reader) → build query → search.
   Fanned out at `SEARCH_CONCURRENCY = 8`.
4. Union hits, compute covered / zero-hit / row-selection sets.

### The structural problem: one index per parquet file

`m.entries.insert(parquet_key, entry)` (`src/tantivy/mod.rs:690`) keys the
manifest **by parquet file**. Confirmed against the real manifest (read-only
GET of `index_manifests/otel_logs_and_spans/6297304f…/manifest.json`, 745 KB):
**950 entries, of which 932 cover exactly one parquet file** (15 cover two, 3
cover three). Average 114k rows per index, 108.6M rows indexed.

Index count actually searched per window, computed from that manifest's
`[min,max]_timestamp_micros` — **measured, not extrapolated**:

| window | indexes searched | measured planning |
|---|---|---|
| 1 h | 38 | ~0.4–0.7 s |
| 3 h | 131 | ~0.4–0.7 s |
| 6 h | 254 | ~0.3–1.2 s |
| 12 h | 457 | ~1.3–2.5 s |
| 24 h | 503 | ~1.7–2.5 s |
| 7 d | 913 | ~3.2 s |
| 30 d | 918 | ~2.5–3.5 s |

Two things fall out. Cost tracks index count roughly linearly (~3ms per index
at concurrency 8 across the range). And **the manifest saturates at 950
entries**, which is the real reason planning is flat from 7d to 30d — 913 vs
918 indexes — not any property of the query. An earlier draft of this doc
extrapolated ~1,860 indexes at 30d from parquet file counts and predicted 233
rounds; that was wrong, and the flat slope my own timings showed is what
disproved it.

Time-pruning itself is healthy: median entry spans 0.007 days, max 1.9 days,
**zero** entries with degenerate or missing bounds. So no entry is
unprunable — the fan-out is genuine work, not a pruning bug.

This is still the *same* fragmentation that produces the file-open wall,
mirrored into the index tier. Tantivy is designed for a few large indexes with
merged segments; we run ~one per parquet file and pay per-index fixed cost on
every query.

### Four compounding defects around it

1. **Reader cache is smaller than one query's working set.**
   `READER_CACHE_ENTRIES = 256` (`search.rs:38`) against **457 indexes at 12h,
   503 at 24h, 913 at 7d**. The budget is exceeded somewhere between a 6h
   (254) and a 12h (457) window — i.e. by nearly every real dashboard query.
   *Tested and inconclusive:* repeat-query warming did not track the 256
   boundary cleanly (12h warmed 1.9x while 3h did not), which is expected
   given prod's own traffic shares and churns the same LRU. Treat the
   undersizing as a design smell to fix, not as a measured cause.
2. **The startup cache warmer is off by default.**
   `timefusion_tantivy_prefetch_days` is `#[serde(default)]` → **0**
   (`config.rs:738`). Every index blob is re-downloaded on demand after each
   restart, and this process restarts often (deploys, plus OOM kills every
   8–20h).
3. **Partial coverage forces a second Delta planning leg.** Confirmed by plan
   shape at 7d — the `=` plan carries **2** parquet scans / `file_groups`, the
   unrouted plan **1**. With `tantivy_uncovered_files = 5506` and diverging,
   the split is the normal case, so we plan the window twice.
4. **The manifest is 745 KB behind a 5-second TTL.** `load_manifest_cached`
   (`search.rs:249-259`) re-GETs and re-parses all 950 entries whenever the
   TTL lapses — and a single routed query takes 1.7–3.2s, so back-to-back
   dashboard queries routinely straddle it. Negative lookups are worse: the
   cache inserts only on success, so a project with no index re-issues the
   miss every single time. This is the leading candidate for the fixed
   component below, and D0 would confirm it in one measurement.

### The honest gap: part of the tax is still unattributed

Measured at 30d, `SELECT 1` control 396ms (noisy round, use ratios not
absolutes):

| | `=` (routed) | range (not routed) |
|---|---|---|
| whale | 4307 ms | 917 ms |
| **project with no data and no index** | **2640 ms** | 847 ms |

A project with **zero manifest entries** still pays ~1.8s for the `=` form.
That cannot be per-index search — there are no indexes. So the per-index
fan-out above is real but is **not the whole tax**, and I cannot split the
remainder from outside the process.

**That is the first thing to build, and it is small:** there is no latency
instrumentation on this path at all. The counters
(`tantivy_prefilter_attempts/used/skipped/errors`) exist but export only to
OTel, and `index_opens` (`search.rs:68`) is incremented and **never read by
anything** — a dead counter. A handful of timers around manifest load, blob
fetch, index open, and search, surfaced in `timefusion_stats`, converts every
question below from argument into measurement.

## Directions, cheapest first

### D0 — Instrument the path (hours, no risk)
Timers for manifest / fetch / open / search + hit counts, in
`timefusion_stats`. Revive or delete `index_opens`. Everything below is
guesswork until this exists.

### D1 — Small knobs (one env flip; the rest are one-line code + a deploy)
**Only the first is config.** The other three are compile-time `const`s or new
code, so they need a build and a deploy — and on this repo any non-docs push
restarts prod, which is not free (in-flight maintenance units die, the rollup
coverage map resets). Ship them together, not as four separate deploys.

- `timefusion_tantivy_prefetch_days` > 0 — **env only, no deploy.** Stops
  re-downloading every blob after each restart.
- `READER_CACHE_ENTRIES` 256 → a few thousand (`search.rs:38`). An entry is an
  mmap handle plus small structs, but each open index mmaps ~10 segment files —
  **check the container's fd limit before picking the number**, or this trades
  a cache miss for `EMFILE`.
- `SEARCH_CONCURRENCY` 8 → 32+ (`search.rs:42`). These tasks are IO-bound;
  8 makes a 7d query serialize into ~114 rounds.
- Cache negative manifest lookups, and raise `MANIFEST_CACHE_TTL` above the
  duration of a single query (currently 5s vs 1.7–3.2s queries).

None changes query results; all are revertible, but reverting costs a deploy.

### D2 — Consolidate index granularity (the structural fix)
Build **one index per (project, day)** — or per hour for whales — instead of
one per parquet file. Against the measured counts, a 7d query would open ~7
indexes instead of **913**, and 30d ~30 instead of **918**: a ~30–130x
reduction in per-query index fan-out, and at ~3ms per index that is the
dominant term.

Note the win is bounded by the same saturation that flattened 7d→30d: the
whole manifest is 950 entries, so consolidation buys ~30x, not ~1000x.

Row-selection pushdown survives: store the source `file_uri` alongside
`row_ordinal` as fast fields in the doc, so a per-day index still emits
per-file `ParquetAccessPlan`s. Rebuild the day's index when its files compact —
the index tier's maintenance then rides the compaction it already needs.

This is the same insight as compaction, applied one tier up, and it is what
every tantivy-at-scale system does (see prior art).

### D3 — Get the prefilter off the planning path
Even a fast prefilter is currently *serial* with planning. DataFusion's
dynamic-filter machinery (as used for TopK and join pushdown) allows emitting
a plan whose file/row pruning resolves while execution starts, overlapping the
index lookup with Delta scan setup instead of stacking them.

### D4 — Route by expected benefit, not by operator class
The current gate is "is it `=` on a raw-tokenized column". A better gate asks
whether the lookup can pay:
- Is there a bloom that already serves this predicate? (`=` on
  id/parent_id/trace_id/span_id — yes.)
- Is coverage over this window good enough to avoid the split leg?
- Is the in-window index count under budget?

That keeps tantivy for `LIKE`/regex/substring — where it is the *only*
option and where the DSL actually needs it — and stops spending it where a
4ms bloom already wins. This is the nuanced form of the P0 flip, and it is
what I should have proposed first.

### D5 — Fix coverage so the split leg disappears
5,506 uncovered files, growing ~85/hr, with the only drain a 03:30 cron that
has not been firing (`tf_tantivy_reconcile_never_drains_2026-08-21`). Until
that converges, every routed query plans the window twice.

## Prior art — how tantivy-at-scale systems do it

- **Quickwit** (tantivy's own distributed engine, object-storage native) is the
  closest reference. It indexes into **splits** of millions of documents, not
  per source file, and keeps a **hot cache** of each split's footer/term
  dictionary so a search costs one or two ranged GETs per split. Both halves
  of our problem — index granularity and per-index round trips — are exactly
  what its split + hot-cache design targets.
- **Lucene / Elasticsearch** merge segments precisely to avoid thousands of
  tiny indexes; per-segment fixed cost dominating query time is the classic
  "too many shards" failure, and the standard remedy is fewer, larger shards.
- **Grafana Tempo** keeps per-block blooms **outside** the block and caches
  them locally, so block selection happens before any block is fetched —
  the D2+D3 combination for the trace-id case specifically.
- **Apache Hudi** keeps `column_stats`, `bloom_filters` and `record_index` in a
  secondary metadata table written on commit, so pruning never opens data
  files. **Iceberg Puffin** is the same idea as a sidecar format.

Two mechanical borrowings worth noting, both variants of D2/D3 rather than new
directions: the manifest could be **fully resident instead of TTL'd** — the
indexer runs in this same process, so it can push-update the cache on publish
and remove the plan-time GET entirely; and Quickwit's hot-cache idea is
adoptable as a custom tantivy `Directory` served out of foyer, which would
also delete the download-and-untar step in `ensure_cached`.

The common thread, and the answer to "can we keep tantivy": **yes — but the
index must be coarse-grained, its hot bytes resident, and it must not be
consulted synchronously on the planning path.** All three are fixable here
without giving up text search.

## Suggested order

D0 (instrument) → D1 (config knobs) → D5 (coverage) → D2 (granularity) →
D3 (off the plan path) → D4 (benefit-based routing).

D1 alone may make the P0 flip unnecessary. Until D0 lands, treat the flip as
the available mitigation rather than the plan, and re-measure after each step —
prod contention moves `SELECT 1` between 63ms and 2.4s, so every comparison
needs an in-round control.

---

## 2026-08-22: the residual attributed — it is not what any of us guessed

Measured on prod against `eb796ac`, per-query counter deltas, in-round unrouted
control of the identical shape and window (differing only in a predicate the
rewriter never routes). Steady state, 7d, `trace_id` equality:

| phase | cost |
|---|---|
| routed wall MINUS unrouted wall | **~470-680 ms** |
| manifest loads / index opens / blob fetches | **0 / 0 / 0** |
| `plan_us` (manifest -> work list) | 0.3-0.7 ms |
| `prepare_us` (490x stat + LRU lookup) | 15-23 ms |
| `search_us` (490 tantivy queries) | 32-58 ms |
| `fanout_us` (WALL of the whole fan-out) | **49-83 ms** |
| merge bookkeeping (`fanout - prepare - search`) | 1.4-1.7 ms |

**The entire tantivy prefilter costs ~50-85 ms of a ~500 ms residual.** Roughly
85% of the routing tax is spent outside the search service altogether.

Three hypotheses died here, each of which would have justified real work:

1. **"The 490 searches are serial CPU."** They ARE serial —
   `buffer_unordered` over async blocks whose tantivy work is synchronous, with
   no await left once an index is cached, so each future completes in a single
   poll and `search_concurrency = 32` buys IO concurrency only. But they cost
   ~0.09 ms each, so it does not matter. A `spawn_blocking` rewrite would have
   bought ~40 ms.
2. **"D2 — the per-file fan-out is too wide."** Same refutation. 490 indexes
   cost 83 ms of wall, all in. Coarser indexes cannot repay their cost out of a
   budget that small. **D2 is not justified by this measurement.**
3. **"The hit list is expensive to plan."** The residual is FLAT from
   `LIMIT 1` to `LIMIT 501` (~600-720 ms at every level, 490 indexes
   throughout), so it does not scale with hits and is not the `id IN (...)`
   expression. The plan cache is also innocent: 99.9% hit rate, and **zero**
   misses for either shape.

So the tax is **fixed per routed query**, independent of hits, independent of
the search, and not IO. What remains is the scan-construction consumption of the
result — partitioning the snapshot's file list against `tantivy_covered_files`
(built over EVERY manifest entry, ~950, not just in-window ones), applying
`tantivy_exclude`, and building per-file `ParquetAccessPlan`s from
`tantivy_row_selections`. All three are fixed in the number of manifest entries
and files, which is exactly the observed shape. One timer around that split
would name the line; it was not deployed tonight so as not to kill the reconcile
pass the coverage measurement needs.

**Consequence for the deferred P0.** The unrouted control for this shape runs
200-300 ms while the routed one runs ~700 ms, so for a `trace_id` point lookup
at 7d, routing is currently a NET LOSS of ~450 ms. That is not an argument for
flipping `route_equality` off wholesale — the control here is `>`, which cannot
use parquet blooms either, and blooms prune 653 row groups to 1 in ~4 ms for
this exact shape, so an unrouted `=` would likely be faster still. It IS a
direct argument for **D4 (benefit-based routing)** over D2: the index should be
consulted where it wins, and the fixed consumption cost is what decides that.

**Method note worth keeping.** The first attribution probe reported NEGATIVE
per-query search time, because it reconstructed totals as `avg * count` from the
`*_us_avg` rows — and each of those divides by its own denominator
(`search_us_avg` by per-index `searches`, not by `queries`, which was not
exposed at all). A mean cannot be differenced. The `*_us_total` rows exist
because of this.

## 2026-08-22: coverage does NOT converge — and the cap was never the constraint

Measured over a 90-minute window on `576e1c3`, container up since 01:41 and
uninterrupted across the 02:20 tick (the first clean pass of the night).
`tantivy_coverage_census` computes the diff LIVE every 15 minutes (metadata-only
Delta-live-files vs manifest-covered-files), so these are real measurements, not
the pass-end gauge:

| time | uncovered |
|---|---|
| 01:26 | 5557 |
| 02:07 | 5607 |
| 02:22 | 5646 |
| 02:38 | 5668 |
| 02:56 | 5684 |

**+127 in 90 minutes (~85/hr) — including 36 minutes with an uninterrupted
backfill pass in flight.** The 02:20 reconcile finished its GC phase in 9
seconds and then ran for 40+ minutes without emitting `tantivy_backfill_pass`,
without a failure warning, and without bending the curve.

**What is measured vs what is inferred.** MEASURED: net +85/hr with a pass in
flight, and no `tantivy_backfill_pass` line 49+ minutes in. Those alone
establish that coverage does not converge and that a 150-file pass does not fit
an hourly tick — the cap is not the binding constraint, because it is never
reached inside a tick.

INFERRED, and flagged as such: the config says `build_concurrency = 2` and
"each 1 GB parquet takes ~2-3 min to index", which would put backfill at ~48
files/hr against ~133/hr gross accrual and a 150-file pass at ~3h. Do not act
on that split yet. Most uncovered files are nowhere near 1 GB — ~1MB
fragmentation is this repo's standing problem — so the true rate may be several
times higher, and the split is partly circular (gross accrual was derived by
assuming the build rate). Taking a number from a CODE COMMENT rather than a
counter is the same mistake as the one below, one step removed. The direct
measurement is `built=` on the pass line over its elapsed time.

**This corrects the premise of the cap change (400 -> 150).** The 150 was sized
against a measured "~456 builds/hr", taken from the `cache_seeded` counter —
but that counter also counts FLUSH publishes, which are ordinary ingest work,
not backfill. So a number ~10x the true backfill rate was used to size a pass.
This is the same proxy-metric trap as the `tantivy build produced N segments`
WARN that undercounted by 20x, in the opposite direction: **before sizing
anything from a counter, check what else increments it.** The cap change is
harmless and its observability rationale still holds, but it cannot deliver
convergence and should not be credited with it.

**What would actually converge, in rough cost order:**
1. **Reduce accrual.** ~133 uncovered files/hr are being created, largely by
   compaction rewriting files whose indexes are then GC'd as stale. Indexing at
   compaction time (the wave-reindex path already does this for some) removes
   the work rather than racing it.
2. **Raise `build_concurrency`** above 2. The comment says 2 is "safe alongside
   prod query load", but the box now has the query path off the critical
   section (blob fetches are 0, everything resident); this is worth re-testing.
3. **Cheaper builds.** Seven "produced N segments (> 32); merging inline"
   warnings in 45 minutes say some builds pay an inline merge on top.

None of these were deployed — the measurement is the deliverable, and picking
among them is a throughput decision that wants the `sim`/`run-unit` loop rather
than another prod deploy at 03:00.

### Confirmed on a second, cleaner window

Container `d327df8` came up 03:05, survived the 03:20 tick and never restarted:

| time | uncovered |
|---|---|
| 03:29 | 5622 |
| 03:44 | 5649 |
| 03:59 | 5674 |
| 04:14 | 5683 |

**+61 in 45 min (~81/hr), monotonic**, matching the first window's ~85/hr — two
independent measurements on different containers agreeing. And the 03:20 pass
ran **60+ minutes with no completion line** on a container that never
restarted, which is the cleanest evidence that a 150-file pass does not fit an
hourly tick. Earlier passes could always be excused as deploy-killed; this one
cannot.

One methodological note, since it briefly produced a wrong number: a single dip
(5684 at 02:56 -> 5622 at 03:29) spans the 03:05 restart boundary, and
averaging across it gave ~40/hr, which looked like a much slower divergence.
Both clean single-container segments say ~81-85/hr. **Don't average a rate
across a restart** — the census is a live diff, so a restart boundary can move
it for reasons unrelated to drain throughput.
