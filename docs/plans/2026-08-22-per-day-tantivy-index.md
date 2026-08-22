# One tantivy index per day — the shape where coverage converges

2026-08-22 morning. Follow-up to `2026-08-21-tantivy-keep-it-fix-it.md`, which
measured the divergence, and to `2026-08-22-file-level-needle-pruning.md`,
which shipped the per-file bloom sidecars this design leans on.

## The defect is the definition of "covered", not the throughput

Measured this morning (method and caveats in the sibling doc):

| quantity | value |
|---|---|
| new live parquet files/hr | **~529** |
| tantivy build rate at `build_concurrency = 2` | **~469/hr** |
| uncovered growth | **~60/hr** |
| standing backlog | ~5,700 files |

The instinct is to chase the 60/hr with throughput. That is treating a symptom.
Ask instead where 529 files/hr of *new information* comes from — and it does
not. Ingest adds rows; **compaction adds files without adding a single row**,
and compaction is the bulk of that 529. Coverage is computed as

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

That is the whole convergence argument. Accrual drops from ~529 files/hr to
roughly one unit per active (project, date) per seal — two to three orders of
magnitude less work — because it stops counting rewrites as new information.

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

1. **`build_concurrency` 2 → 4** — one env var
   (`TIMEFUSION_TANTIVY_BUILD_CONCURRENCY`), buys the ~14h drain of the
   existing 5,700 backlog while the rest is built. Independent of everything
   below and reversible by the same var. Watch peak anon RSS and
   `oversized_skipped`.
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

If accrual were genuinely new rows, ~530/hr would be real work and only
throughput would help. The test is cheap and should be run before step 2: split
the bloom `built=` population into files whose `(project, date)` was already
covered (rewrites) versus dates seen for the first time (new data). The
prediction of this design is that the first group dominates. If it does not,
step 3 buys far less than claimed and the ordering should change.

## Not addressed here

`search_us_avg` has regressed 0.34ms → 2.1ms and `reader_hit_pct` 96.9% →
85.2% since the 08-21 increment-2 measurement, with 334 blob fetches averaging
**3.5 seconds** each. That is the local-first guarantee leaking, and it is a
separate defect from coverage — a day index makes it cheaper to hold, but does
not explain the regression. Worth its own investigation.
