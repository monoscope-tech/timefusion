# One chain from "maintenance lags" to "the server dies"

Status: **fixed — #136, #137, #139, #141.** This is the answer to "with each
spill or any random thing queries get slow cos maintenance lags". It is one
causal chain, not several problems.

> **Correction.** The first draft of this document blamed step 3 on coverage
> holes left by failing units. That mechanism is real but it was NOT what
> emptied the coarse tier. The actual cause was a file-selection predicate
> (below), found after this was first written. The chain is corrected here;
> the wrong version is not preserved because it would mislead.

## The chain

```
1. A derived (1h) unit selects base files by CONTAINMENT of their slice tags
2. -> a base file tagged with a DAY can never fit an HOUR-wide derived slice
3. -> the unit reads nothing, publishes rows=0, and is marked Complete   [#139]
4. -> the 1h tier is "built" and EMPTY for every backfilled day
5. -> a 14d/30d query finds no 1h rollup: rollup_miss_not_built
6. -> it falls back to a raw scan of 14-30 days
7. -> anon-rss 125 GB, oom-killer, exit 137 — every query dies, not just this one
```

Independently, and also fixed: a unit that failed on memory was requeued
unchanged and so failed identically forever (#137), and `DedupExec` charged the
parquet reader's whole column-chunk blocks, failing customer UPDATEs after 16.9 s
(#136).

## Steps 1-4 — why the coarse tier was empty (fixed, #139)

A base rollup file is tagged with the slice of the UNIT that wrote it, and that
width is unrelated to the derived unit reading it. The backfill writes base units
a whole DAY wide; derived units are an HOUR. The filter demanded containment:

```rust
tag(SLICE_START) >= slice.start && tag(SLICE_END) <= slice.end
```

`day_end <= hour_end` is never true, so an hour-wide derived unit could never
select a day-tagged base file.

Measured, prod 2026-08-17:

- project `87576849` holds **17,705 rows** in the 1m tier for 08-03, and its 1h
  unit for 08-03 published **rows=0**
- every `dashboard_1h_v2` / `metrics_1h_v2` publication in a 12-minute window:
  `rows=0`, while base published 33,628 / 7,269 / 1,426 rows
- derived slices measured at exactly 3,600,000,000 micros — one hour
- the only days with a real 1h tier (08-15..08-17) are those whose base files
  came from the LIVE FRONTIER in ten-minute units — the one width that fits
  inside an hour

Fixing the read exposed a latent double-count: slice width is not stable for a
range (`split_time_task` cuts a published day into children,
`coarsen_sealed_slices` fuses them back), and the publish replace-set matched
only the IDENTICAL slice, so a day-wide file and an hour-wide file inside it both
stayed live and a dashboard SUMmed both. Also fixed in #139.

## The dependency that is NOT the bug

`dependencies_complete` requires a DerivedRollup slice to have contiguous,
complete BaseRollup coverage. That is correct and must not be relaxed: a 1h
rollup silently missing an hour is a wrong answer, which is worse than a slow
one. It was a red herring here — the units were passing this check and then
reading nothing.

## The retry loop (fixed, #137)

```
tasks_retry = 50
retry_reason = dedup: Not enough memory to continue external sort ...
               Failed to allocate additional 1090.9 MB for ExternalSorter[1]
               ... fair(pool_size: 5.0 GB)
```

Same slice, same pool, so the exponential backoff only decided how slowly it
never finished — while each pass burned a worker and 50-80 s of object-store
work. #137 shrinks the unit on a repeat instead, the argument `abandon_running`
already makes for wall-clock overruns.

## What the tiers actually looked like

Measured, project `6297304f` (busiest), 2026-08-17:

```
1h tier (dashboard_1h_v2):  3 days only — 08-15, 08-16, 08-17  (12, 57, 5 rows)
1m tier (dashboard_1m_v3):  8 days, holey — 08-17,16,15,13,08,02,01, 07-31
```

The asymmetry is the point, and it is the fingerprint of the containment bug:
the fine tier is 8 days deep while the coarse tier — the one 14d/30d queries
actually read — is 3 days deep, and those 3 days are exactly the ones the live
frontier wrote in ten-minute units.

**`MIN(date)` is a misleading progress metric.** It advanced 08-01 -> 07-31 ->
07-30 during this session while the coarse tier stayed at 3 days. It reaches
further back while the middle stays holey, and a 30d panel needs 30 CONTIGUOUS
days in the tier it reads. Tracked properly by `rollup_min_contiguous_days`
(#140).

## Step 5 — confirmed by counter, not by inference

Diffing `timefusion_stats` across one 14d query on the busiest project:

```
rollup_miss_not_built_total   0 -> 1
rollup_misses_total         981 -> 982
```

`NotBuilt`, not `StaleCoverage` or `IncompleteCoverage`. The tier does not
exist for that range.

## Step 7 — the raw scan is fatal, not merely slow

```
2026-08-17T16:39:15Z  tokio-rt-worker invoked oom-killer
  Killed process 63853 (timefusion) anon-rss:125163268kB  (125 GB)
  task: non-zero exit (137)
```

`tokio-rt-worker`, not `maintenance-wor` — the query path. 14d and 30d both hit
the 60 s statement timeout first; the box died shortly after.

**The scan guard does not prevent this.** Two separate mechanisms, neither of
which bounds a single scan's heap:

- `bounded_otel_scan_reason` checks query SHAPE only — `project_id = <value>`
  plus a timestamp lower bound or LIMIT. A 30-day lower bound satisfies it
  exactly as a 5-minute one does.
- `gate_if_wide` bounds CONCURRENCY (`heavy_scan_sem`), not size. Once admitted,
  a scan decodes without bound.

So an unroutable dashboard query is admitted and then takes the process down
with it.

## What is fixed, and what is open

Fixed this session:

- **#136** — `DedupExec` charged the parquet reader's whole column-chunk blocks
  (15.2 GB claimed against 847 KB of files), failing customer UPDATEs after
  16.9 s. `DedupExec` is only in those plans *because* the partition is
  uncertified, which is how maintenance lag became a query-path failure.
- **#137** — capacity failures shrink instead of retrying at the same size.

Open, in the order they matter:

1. **A single unroutable scan can kill the server.** `selected_file_work(&plan)`
   already computes selected files and bytes inside `gate_if_wide`, and is used
   only to *release* scans from the gate. The same measurement could refuse a
   scan whose selected work is catastrophic. Refusing customer dashboard queries
   is a product decision — stage it through the existing
   `OtelScanGuard::Observe` first and measure how much real traffic would trip
   it before enforcing anything.
2. **Track coverage as contiguous days from today**, per (project, tier). Both
   `MIN(date)` and `tasks_pending` moved in encouraging directions all session
   while the metric that governs query latency — is the 1h tier complete over
   the last 30 days — did not.
3. **The heavy share is over-committed.** `HEAVY_REWRITE_PERMITS` went 4 -> 10
   today and `PER_SORT_BUDGET_BYTES` was halved to 2 GiB to pay for it: 10 x
   2 GiB = 20 GiB declared against a **4.98 GiB** heavy share (16.6 GiB pool,
   minus a 4.15 GiB coordinator quarter, x 0.40). The code's premise — "a spill
   THRESHOLD, so exceeding it degrades to bounded disk spill rather than
   failing" — is falsified by the `retry_reason` above: it *failed*, because
   `ExternalSorterMerge` cannot spill. #137 makes this self-correcting rather
   than fatal, but the sizing is still wrong.
4. **Derived-tier priority.** `pending_derived_rollup` is 1,206 against
   `pending_base_rollup` 4,856. Long-window queries are served by the coarse
   tier, so once its dependencies are satisfiable it should be scheduled ahead
   of more fine-grained work, not behind it.

## How to tell this is fixed

Not by `tasks_pending`, and not by `MIN(date)`. The test is:

- `otel_logs_and_spans_rollup_dashboard_1h_v2` has **contiguous** dates covering
  the last 30 days for every project
- `rollup_miss_not_built_total` stops incrementing on 14d/30d dashboard queries
- a 30d dashboard query returns without a raw-scan fallback
- no `exit 137` with `tokio-rt-worker` as the oom-killer caller
