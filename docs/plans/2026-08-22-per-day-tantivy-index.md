# One tantivy index per day — the shape where coverage converges

2026-08-22 morning. Follow-up to `2026-08-21-tantivy-keep-it-fix-it.md`, which
measured the divergence, and to `2026-08-22-file-level-needle-pruning.md`,
which shipped the per-file bloom sidecars this design leans on.

## The defect is the definition of "covered", not the throughput

Measured this morning (method and caveats in the sibling doc):

| quantity | value |
|---|---|
| new live parquet files/hr | ~529 |
| of which flush output, self-indexed at commit | ~370 |
| **uncovered accrual** — arriving with no index | **~160/hr** |
| **backfill drain** at `build_concurrency = 2` | **~100/hr** |
| uncovered growth | ~60/hr |
| standing backlog | ~5,700 files |

The instinct is to chase the 60/hr with throughput. That is treating a symptom,
and the decomposition says why. Flush indexes its own output at commit, so
**new rows are covered at birth**. Everything the backfill spends its life on —
that whole ~160/hr — is compaction rewriting rows that were already indexed.
Ingest adds rows; **compaction adds files without adding a single row**.
Coverage is computed as

```
uncovered = live parquet files − ∪ manifest_entry.covered_files
```

so every compaction rewrite un-covers rows that were already indexed, and the
backfill re-indexes bytes it has already read to produce an index that says
what the old one said. The work is manufactured by the bookkeeping. **No
throughput setting converges against a producer that is fed by the drain's own
neighbour.**

## What makes a per-day index different

Key the index by `(project_id, date)` instead of by parquet file, and make its
output what the read path actually consumes: **`id` terms**, not row addresses.
Then a compaction rewriting A+B+C into D changes nothing the index knows —
the same rows carry the same ids, and the index's answer ("these ids match
`message LIKE '%timeout%'`") stays true. Coverage becomes a property of a
**date**, not of a file list, and it is invalidated only by rows the index has
never seen, i.e. by genuinely new data.

That is the whole convergence argument. Uncovered accrual drops from ~160
files/hr to roughly one unit per active (project, date) per seal — because it
stops counting rewrites as new information, and rewrites are ~all of it.

### Division of labour with the blooms

Losing per-file granularity sounds like losing file pruning. It isn't, because
the file-level mechanism already shipped and already converged:

- **tantivy answers "which ids match this text predicate"** — the only
  mechanism that can serve `LIKE`, regex and substring at all.
- **bloom sidecars answer "which files could hold these ids"** — per-file,
  per-date, metadata-only, and demonstrably keeping up (its reconcile went
  cap-bound → steady state at ~40 files per 5-minute pass, and it is the
  counter this morning's accrual number came from).

Feeding tantivy's id set into the existing bloom prune is strictly better than
what per-file indexes do today: it prunes against the CURRENT file set, including
files written after any index was built. The 08-22 result — 284 files → 24 on a
needle lookup — was produced by exactly that path.

### Why the manifest already supports it

No new format is required:

- `ManifestEntry.covered_files` is already a `Vec<String>`; flush-built entries
  already cover several files.
- `ordinals_valid` is already computed as
  `e.ordinals_valid && e.covered_files.len() == 1` (`search.rs:218`), so a
  multi-file entry already turns row-ordinal selection off and degrades to
  id-plus-file-coverage on its own.
- Hits already carry the `_id` term (`search.rs:902`).

The read path therefore keeps working the day a multi-file entry appears; the
change is in what gets built and how coverage is judged.

## Design

**Blob and key.** Manifest key `day-{date}`, blob at
`{INDEX_PREFIX}/{table}/{VERSION}/project_id={pid}/date={date}/day.tar.zst`.
`file_uuid()` (the cache-dir namer) must accept that key alongside `bucket-`.

**Two tiers, mirroring sealed vs hot everywhere else in this codebase.**

- **Sealed day index** — built once per (project, date) once the date stops
  changing, over every file then live.
- **Hot tail** — the current date, and any date still being compacted, keeps
  today's cheap per-file indexes until the next seal folds them in. Bounded by
  construction: at most one or two dates per project have a tail.

**Coverage.** A date is covered when a sealed entry exists whose
`max_timestamp_micros` reaches the end of that date and whose `built_at` is
after the last row landed for it. A compaction rewrite does NOT un-cover it.
Late-arriving rows for a sealed date re-open it — that is the only invalidation
that should exist, and it is proportional to actual new information.

**GC.** Today an entry dies when its covered files die. A day entry must not:
partial file death is the normal case. It dies when its date leaves retention,
or when a reseal replaces it.

**Correctness direction.** The index is a candidate generator. Stale entries
produce false positives (an id whose row moved files), never false negatives —
the row is still there under the same id, and the bloom/scan stage filters. A
day entry that misses rows written after it was built is the only unsound case,
which is exactly what the hot tail and the reseal-on-late-arrival rule cover.

**Read fan-out, for free.** `indexes_per_query = 5`, `index_opens = 1478`,
`blob_fetch_us_avg = 3.5s`. A 30-day query over one project goes from hundreds
of tiny index opens to 30. That also makes "always local" affordable, which is
the second half of the ask: `warm_recent` currently warms every blob within
`prefetch_days`; at one blob per project-day, warming the entire 30-day window
for every project is a few hundred objects, and `reader_cache_entries = 2048`
comfortably holds all of it.

## Sequencing

1. **Raise `build_concurrency`** (`TIMEFUSION_TANTIVY_BUILD_CONCURRENCY`) to
   buy headroom over the ~160/hr while the rest is built. Sized honestly: 4
   gives ~200/hr, a ~40/hr surplus and a **~140h** drain of the 5,700 backlog;
   clearing it inside a day needs ~8. Reversible by the same var, but 8
   concurrent parquet decodes is a real memory question on a box that OOMs —
   watch peak anon RSS and `oversized_skipped`. This is headroom, not the fix.
2. **Day-index builder + manifest key**, behind
   `timefusion_tantivy_day_index`, default off. Prove it on staging: a day
   index plus the bloom prune must return the same rows as per-file indexes for
   `LIKE`, regex and equality shapes.
3. **Date-level coverage**, so the census stops counting rewrites. This is the
   change that converges; steps 1-2 are what make it safe to make.
4. **Seal at compaction time** — a compaction that rewrites a date partition is
   already holding those rows in memory, so it is the cheapest possible moment
   to build that date's index, and it removes the S3 read-back entirely.

## The measurement that decides step 1 alone is not enough

The flush-vs-backfill split above was derived from `flush_completed_total`
assuming ~1 file per flush commit. That is the load-bearing assumption, and it
is worth one direct check before step 3: log the added-file count on the flush
commit path, or count `tantivy_backfill_pass built=` against the census over a
pass that actually completes. If flush commits routinely add several files, the
uncovered accrual is larger than ~160/hr and step 1 needs to be more aggressive
— but the shape of the fix does not change, because those files are still
covered at birth.

## Not addressed here

`search_us_avg` has regressed 0.34ms → 2.1ms and `reader_hit_pct` 96.9% →
85.2% since the 08-21 increment-2 measurement, with 334 blob fetches averaging
**3.5 seconds** each. Two hypotheses, both untested, and they are not
alternatives — they can both be true:

- **Working set > cache.** `reader_cache_entries = 2048` open readers against
  `indexes_searched_total = 15302` over 2895 queries. With one index per
  parquet file the live working set is far larger than the cache, so the LRU
  churns. This one *is* granularity: at one index per project-day, a month for
  every project fits in 2048 entries with room left, and `warm_recent` could
  warm the entire retention window instead of `prefetch_days = 3`.
- **IO contention.** The backfill pass never completes, so it is continuously
  reading parquet back and uploading blobs over the same OVH link a query's
  blob fetch uses. 3.5s for a download+unpack that used to be free is what a
  saturated link looks like. Disk is not the constraint — the host has 714G
  free against a 200G budget.

The first is fixed by this design. The second is fixed by not having a
permanently-running backfill, which is also what this design delivers.
