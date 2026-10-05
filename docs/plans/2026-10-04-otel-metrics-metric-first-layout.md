# otel_metrics: metric-first layout

Status: proposed (2026-10-04). Owner decision: re-sort everything.

## Why

Web vitals on the demo project read **44.7M rows to return ~58k** for a 24h window (count alone 69.8 s).
`otel_metrics` is sorted `(timestamp DESC, metric_name, series_id)`, so every row group interleaves every
metric and nothing prunes on `metric_name` — not row groups, pages or blooms.

Query pattern (owner, 10-04): **never across metrics; one metric, then a time window.** Queries never name
`series_id` — they select series by attribute filters and usually aggregate across many of them
(`sum(rate(...))` over instances, p95 over pods, `GROUP BY attributes.route`).

Measured on prod (hourly rollup, demo project, last 24h):

| | value |
|---|---|
| distinct metrics/day | demo 441, whale 291, shipbubble 17 |
| rows/day (demo) | 43.8M; top 10 metrics = 61%; the 4 biggest ~4.5M each |
| `browser.web_vital.*` (demo) | ~58k rows/day = 0.13% |
| sealed-day files (demo / whale / shipbubble) | 4-5 / 1 / 1 files, ~2 / 0.4 / 0.1 GB |
| read-side dedup today (all tables) | 34,045 bounded vs 838 full-set (97.6% bounded) |

## Decision

Sort every `otel_metrics` file by **`(metric_name, timestamp DESC, series_id)`** and reorder
`dedup_keys` to **`(metric_name, timestamp, series_id)`** — the same key set, now the full sort prefix.
Keep the `[project_id, date]` partitions.

This matches prior art: ClickHouse's OTel exporter `ORDER BY (ServiceName, MetricName, Attributes, TimeUnix)`
partitioned by day; InfluxDB 3 sorts each table's Parquet by tags then time; Cortex/Prometheus Parquet sorts
row groups by `__name__`.

### Alternatives rejected

- **Partition by `metric_name`.** 441 metrics × ~2 GB/day ≈ 4.5 MB partitions; Delta guidance is ≥1 GB per
  partition. Thousands of tiny files, a bigger log, more commits, more maintenance units.
- **Liquid clustering in our delta-rs fork.** Its pruning is file-level, and our partitions hold 1-5 files.
  Hilbert order inside files destroys the timestamp/sort order dedup and footer ordering need. It is a
  protocol table feature (`clustering` + `delta.clustering` domain metadata) other writers must honour — the
  one option that would tie the data to our implementation. Not in delta-rs upstream (issue #2043).
- **Metric-range files** (split sealed days into files by `metric_name` range, timestamp-sorted inside).
  Kept the time-first paths but pruned only to ~1/16 of a day, grew the Delta log, and needed a cutting
  writer + converged tag + tail re-cluster. Its premise — time-only reads matter — does not hold for queries.
- **`(metric_name, series_id, timestamp)`.** Makes each series contiguous (the window-function shape
  `PARTITION BY series_id ORDER BY timestamp`), but queries never filter `series_id`, so its second place
  prunes nothing and spreads every series' run across the whole day — a time window inside a metric then
  prunes nothing either. Time second keeps metric + window pruning down to row groups and pages; the
  per-series windows keep their sort, now over a pruned metric+window slice instead of the whole day.

### Compatibility

No fork change and no protocol feature. Files are ordinary Parquet + Delta `add` actions; the sort is recorded
in each footer's `sorting_columns`. `metric_name` and `series_id` already get per-file stats
(`stats_columns_for`, `src/database/mod.rs`: sorting columns + dedup keys), so any engine — Databricks
included — skips files and row groups on them.

## Schema change (stage 3)

```yaml
dedup_keys:            # same set, reordered: must stay a prefix of sorting_columns
  - metric_name
  - timestamp
  - series_id
sorting_columns:
  - { name: metric_name, descending: false, nulls_first: false }
  - { name: timestamp,   descending: true,  nulls_first: true }
  - { name: series_id,   descending: false, nulls_first: false }
```

Every write path already reads the schema's order (flush `sort_batches_by_schema`, packing/repair
`schema_order_by_clause`, delta-rs optimize `schema_optimize_sort_columns`, DV-merge append), and footers come
from `schema.sorting_columns()`, so new files are honest the moment this ships.

## What breaks today, and the stage that fixes it

From a code map of the read and maintenance paths (file:line refs as of `98bf79e9`).

| # | Risk | Kind | Fixed by |
|---|---|---|---|
| 1 | Bounded read dedup needs an Int64/timestamp lead (`leading_bound`, `src/read/mod.rs:334`; `keep_greatest_ordering`, `src/database/scan.rs:785`). A string lead → full-set dedup, capped at 2 GiB/query → large windows fail. | availability | Stage 1 |
| 2 | Repair checks a footer *exists*, not *which* columns (`maintain.rs` ~9696, `compact.rs` ~628). Old days never migrate. | cost (forever mixed) | Stage 2 |
| 3 | Mixed layouts: the minority layout gets a read-time `SortExec` up to 1 GiB compressed per query (`repair_isolated_scan_ordering`, `src/read/optimizers.rs` ~1714); over budget it is unordered → full-set dedup → >2 GiB fails. One demo day is ~2 GB compressed, so any window touching an unmigrated demo day hits this. | availability during migration | Stage 2b (per-layout date legs) + Stage 4 gate |
| 3b | Packing slices on the leading sort column and its range probe casts it to `i64` (`maintain.rs:8393`, `mod.rs:5151`); a `metric_name` lead declines slicing, and the tail packer has no row cap because it relies on slicing (`mod.rs:6585`) → unbounded sorts. | availability (maintenance OOM) | Stage 2c |
| 3c | A falsely declared ordering (lying footer) makes a streaming collapse emit a key, clear its state, then see the key again: `A, B, A` emits `A` twice or an older and a newer version (`src/read/mod.rs:1003-1006`: `close_run` + `seen.clear()`). Unrecoverable once emitted. The existing timestamp bound has the same gap. | correctness | Stage 1 (fail closed + verified legs only) |
| 4 | Time locality lost inside a day: dedup bins (10-min, rewrite every overlapping file), the dedup probe's time sharding, per-file certification overlap, packing "event-time disjoint" cuts. | maintenance cost | Stage 0 measurement, Stage 3 adjustments |
| 5 | `ORDER BY timestamp DESC LIMIT` across a metric no longer streams. | cost (queries we do not run) | accepted |
| 6 | File regrouping by declared ordering degrades when flush files hold every metric (`regroup_for_declared_ordering`, fork). | cost | accepted; watch |
| 7 | Inexact string min/max for `metric_name` trusted for file grouping. | theoretical correctness | Stage 1 test |

Unaffected: dedup-key checks are `any(== "timestamp")`; `RunCollapse` and the dedup operator compare by
equality/hash; rollup SQL orders explicitly; tantivy is not configured on `otel_metrics`.

## Stages

Each stage is its own deploy, signed off with `make ci-signoff`, and proven on prod before the next.

### Stage 0 — measure the time-only maintenance readers (no deploy)

Queries never cross metrics; maintenance does. Before Stage 3, size what a metric-first day costs each lane:

- Rollup base builds on `otel_metrics`: unit width per day age (`base_hour_units` mints hour units for
  today's writes only — sealed days build in day units, which read the day once either way). Confirm from the
  journal on prod.
- Dedup dirty bins and the probe for `otel_metrics`: units/day and bytes read per unit (`timefusion_stats`
  + monoscope maintenance metrics). A bin rewrites every file overlapping its 10 minutes; today that is the
  day's 1-5 files; after the flip it is the same files, so bytes should be flat — verify.
- Late-arrival and merge-on-read UPDATE volume on sealed `otel_metrics` days (these land as tail files).

Gate: write the numbers into this file. If any lane's cost would rise by more than ~2× on sealed days, adjust
it in Stage 3 before the flip.

**Results (prod journal, last 24 h, 2026-10-04):**

| Lane | Units/day | Slice | Whole-file bytes |
|---|---|---|---|
| dedup | 1,016 | 10 min (1,008), day (4) | 11.1 GB |
| base rollup `series_5m_v1` | 228 | 60 min (168), 10 min (48), day (7) | 13.5 GB |
| base rollup `metrics_1m_v2` | 226 | same | 12.5 GB |
| derived `metrics_1h_v2` | 174 | 60 min | 0.8 GB (reads the tier) |
| repair | 12 | day | 9.6 GB |

- Hour and day units read whole files either way → flat.
- At risk: **dedup's 10-minute bins on today's hourly files.** Row groups are 128 MB (`timefusion_max_row_group_size`),
  so the time pruning a bin gets today is mostly page-level (20k-row pages). After the flip each metric's run is
  still time-ordered (`timestamp` second), so page pruning survives for metrics above ~20k rows/hour and is lost
  for the long tail. Expected: dedup decode for `otel_metrics` rises by up to the bin/file ratio (≤6×) on small
  metrics, ~flat on large ones.
- Gate for Stage 4: `otel_metrics` dedup unit decode bytes/day ≤ 2× the pre-flip day. Mitigation if exceeded:
  dedup `otel_metrics` in hour bins (one unit per hourly file instead of six) — a per-table bin width, which needs
  the dirty-bin producer, prober and drain to agree per table (`bin_micros`, `compact.rs`).

### Stage 1 — read-side dedup collapses runs when the ordering covers the keys  ✅ implemented

When the input's declared ordering has every dedup key as its prefix (in any column type), all versions of
a key are adjacent: `DedupExec` keep-greatest can emit each run as soon as the key tuple changes, with no
per-run buffering — strictly better than today's timestamp bound. Implement as a generalisation of `Bound`:
track the leading **key tuple** via the arrow row format (`RowConverter` with the sort options, so byte
order = sort order), keep the i64 fast path for timestamp-led tables (logs: 97% of dedups, CPU-sensitive).

- `leading_bound` / `detect_bound` (`src/read/mod.rs`) and `keep_greatest_ordering` (`src/database/scan.rs`):
  accept a non-numeric lead when the declared ordering covers all dedup keys.
- **False orderings fail closed.** A streaming collapse cannot retract a run it emitted, so a lying footer
  (`A, B, A`) is not recoverable in-stream. Two layers:
  1. Engage the tuple bound only on legs whose files are *verified* sorted (written by our sorted writer or
     confirmed by repair: `repair_verified_sorted` / `SORTED_RUN_TAG`); any other leg keeps today's path.
     **Lives at the scan, in Stage 2b** — `DedupExec` sees a declared ordering, not files.
  2. On a backward key-tuple move the new path returns an error (`ordering violation in <table> leg <leg>;
     file ordering is false`) and bumps `ordering_violations_*`. It does not emit a possibly-duplicated
     answer. The existing i64 timestamp bound keeps its current behaviour (a separate follow-up — prod's
     `ordering_violations_delta` is non-zero today).
- Ship with no table using it (otel_metrics is still timestamp-led), so the deploy is inert for prod reads.

Tests (rs-minimal-tests ladder):
- Property test: for random batches sorted by a `(Utf8, ts, Utf8)` key with random duplicate versions,
  streaming collapse == full-set keep-greatest, and peak buffered bytes stay O(one run).
- Case table: lead types Utf8 / Utf8View / Int64 / Timestamp; asc/desc; nulls first/last.
- False footer: input `A(v1), B, A(v2)` declared sorted on the tuple path → the query errors with the
  ordering-violation message and the counter rises; no row is emitted for the second `A`. An unverified leg
  with the same data takes the full-set path and returns exactly `A(v2), B`.
- Inexact string stats (risk 7): **moved to Stage 3** — it needs a string-led Delta table.
- Cost assertion: bounded path chosen (counter), not just correct output.

Gate: suite + e2e green; prod `dedup_full_set_total` unchanged after deploy.

### Stage 2 — repair rewrites files whose footer order differs from the schema  ✅ implemented

- Compare each file's footer `sorting_columns` with `schema.sorting_columns()` (names + direction), not just
  presence (`maintain.rs` ~9696, `compact.rs` ~628). A mismatched file is a repair candidate.
- `repair_verified_sorted` is persisted and filled at write time: key it by the sort signature (or clear
  entries whose footer no longer matches) so a schema change re-queues old files.
- Admission: one (project, date) unit at a time, priced in decoded bytes like today. Pace so the table
  (~185 GB) migrates over days without starving dedup/rollups — reuse the repair lane's budget, newest days
  first (they are what dashboards read).
- Implementation: one predicate `footer_declares` (columns resolved by name through the file's own schema,
  plus direction and null order) shared by repair, the seed sweep and recompress; `repair_verified_sorted.txt`
  carries a `# sorts:` header of every table's order, and a file under any other header is discarded at load.
- **Not inert:** the first boot finds no header and re-probes every footer once — a dry run of the cost Stage 3
  pays. Prod sample (10-04): 122/122 `otel_metrics` and 120/121 `otel_logs_and_spans` footers match exactly;
  the one miss is an old leaf-index footer naming the wrong column — a lying footer the old "any non-empty
  footer" check accepted, now repaired.

Tests:
- A file written under one sort and read under another is selected; a matching file is not
  (`a_footer_written_under_another_sort_order_is_a_repair_suspect`, plus a case table for the header).
- Fixed point after repair: **moved to Stage 3**, where the shipped schema actually changes order.

Go/no-go for the Stage 2 deploy (the re-probe dry run), on a ≥1 h process:
- Expected: `pending_repair` rises as every sealed partition of every sorted table becomes a suspect, then
  drains as `footer_repair_suspect_cleared` fires; actual repair rewrites ≈ the <1% lying footers.
- No-go (revert): rewrites climbing on files the sample said match, or `pending_repair` flat for an hour —
  either means `footer_declares` disagrees with prod footers for a reason the 243-file sample missed.

### Stage 2b — dedup each layout's dates separately  ✅ implemented

Dedup keys include `timestamp` and partitions are `date(timestamp)`, so **a key never spans dates**. When a
scan's files carry more than one footer ordering, split it into one leg per layout by date set — the same
date-restricted leg machinery the per-date dedup skip already uses (`scan_side!` / `date_restrict`,
`src/database/scan.rs` ~582-607) — and dedup each leg with its own ordering (old days: timestamp bound; new
days: tuple bound), then union. No read-time sort of a whole layout, no full-set fallback across layouts.

- **Verified-files gate (from Stage 1, layer 1):** a leg declares a string-led ordering to `DedupExec` only
  when every file in it is verified sorted (`repair_verified_sorted` / `SORTED_RUN_TAG`); unverified files go
  to an undeclared leg (full-set path). This keeps a lying footer from turning into a failed query.
- A day is single-layout once repair rewrites it (one commit per (project, date)). A day can be mixed only by
  late files landing on an unmigrated day after the flip; those tail files are small, so they are sorted to the
  day's majority order at read time (well under the 1 GiB budget), and the day is already a repair candidate.
- MemBuffer rows follow the current schema order; they belong to today (new layout) or to late days, handled
  as tails above.
- Inert until Stage 3 (one layout everywhere).

**As built** (decisions that differ from the text above):
- Mechanism: for a table whose sort leads with a non-time column (`sort_led_by_time`), `scan` splits the
  window into runs of consecutive same-layout dates (`layout_runs`) and runs the whole existing scan once per
  run (`scan_whole`), each filtered to its run AFTER dedup and unioned. Same partition as per-layout legs, with
  no surgery inside leg assembly; the per-date/per-file certification splits keep working within a run.
- Requirement: a string-led table takes `DedupExec`'s merge requirement from the Delta leg's declared ordering
  (`keep_greatest_requirement`), so an old-layout run stays `bounded[timestamp]`.
- Verified gate: decides only which run a date joins (any unverified file ⇒ legacy). Within a run the leg's
  declared order is trusted and a lie fails closed (`OrderingViolation`, counted) — rather than the
  undeclared-leg fallback, which would send an honest but not-yet-verified day to full-set dedup and fail the
  same large windows at the 2 GiB cap.
- Found while testing: TimeFusion's Utf8→Utf8View coercion projection erased every string-led ordering
  (DataFusion's `CastExpr` preserves order only for numeric widening). `StringViewCast` keeps it, gated like
  the split so timestamp-led tables plan exactly as before.

Tests:
- A window spanning an old-layout day (footer written timestamp-first), a metric-first day, and a newer
  version of an old-day key in MemBuffer: one row per key, the newest wins, `count(*)` exact, the window splits,
  `dedup_full_set_total` unchanged (`a_window_across_two_sort_layouts_dedups_each_day_under_its_own_order`,
  fixture `mor_metric_first`). Red without the split, and red with the schema's requirement on the old run.
- `only_a_string_led_table_plans_by_layout`: logs and (pre-flip) metrics stay on the old path.

### Stage 2c — bounded packing for a non-time leading sort column  ✅ implemented

Prerequisite to the flip. Generalise slicing so it does not need an `i64` lead:

- Range probe over the bin: `SELECT <lead>, count(*) … GROUP BY 1 ORDER BY 1` (the bin is small; `metric_name`
  is non-null in the schema). Cut contiguous lead-value ranges with balanced row counts:
  `WHERE metric_name >= a AND metric_name < b`, emitted in the output's sort direction into one writer, so the
  concatenation stays globally sorted and the footer honest.
- A single lead value larger than one slice (the 4.5M-row k6 metrics) is cut further on the next sort column
  (`timestamp`, still i64) within that value.
- ~~Until that exists, give the tail packer a row cap whenever slicing declines.~~ Not built: slicing exists, and
  its remaining declines (a failed probe, a NULL lead — `metric_name` is non-null) are exactly the timestamp
  slicer's.
- As built: `lead_value_slices` (pure; row-balanced ranges of whole values, a value heavier than a range cut on
  time) and `lead_value_slice_clauses` (one grouped probe, clauses in the schema's order). A bin of ONE value
  is cut on time too — declining there was the unbounded sort this stage removes.

Tests:
- `lead_value_slices_cover_every_row_once` (proptest): every (value, timestamp) falls in exactly one slice.
- `a_string_led_bin_packs_in_slices_that_concatenate_sorted` (a dominant value among others; one value only):
  the slices, sorted one by one and concatenated, equal one sort of the whole bin. Red with the time cuts in
  the wrong direction, and red if a one-value bin declines.

### Stage 3 — flip the schema

- Prerequisites: Stages 1, 2, 2b and 2c deployed and their gates met.
- Apply the yaml above. Update the `schema.rs:1047` assertion message ("lead sort key must be leaf 0") and
  the yaml comment that names the old key order.
- Adjust any lane Stage 0 flagged. Expected candidates:
  - dedup probe time sharding (`compact.rs` ~1088): shard by metric range instead of time for tables whose
    sort leads with a non-time key, so each shard prunes on `metric_name` stats;
  - packing's "event-time disjoint" cut comment (`maintain.rs` ~8507) — cuts stay on contiguous slices of the
    sorted stream (still honest footers); only the comment's claim changes.
- Rollup tiers on `otel_metrics` (`metrics_1m_v2`, `metrics_1h_v2`, `series_5m_v1`, `series_attrs_1d_v2`) are
  unaffected: their SQL orders explicitly and their own sort is independent.

Tests:
- Fixed point (from Stage 2): after repairing a day written under the old order, a second pass selects nothing.
- The existing `the_shipped_dedup_keys_lead_the_shipped_sort` (`compact.rs:2252`) passes with both lists
  reordered together.
- Routed rollups over a mixed-layout window equal raw (`a_per_series_counter_rate_routes_to_the_series_tier_and_equals_raw`).
- Risk 7: two files whose metric names share a >64-character prefix (inexact footer min/max) are not grouped
  under a false ordering — the query returns the exact answer, no `OrderingViolation`.
- New: a metric + time-window query over a migrated day prunes to that metric's row groups inside the window
  — assert `row_groups_pruned_statistics` / bytes scanned, not only the answer.

### Stage 4 — migrate and verify

- Watch per deploy (`bench/prod_report.py`, monoscope metrics): repair backlog draining for `otel_metrics`,
  `ordering_repair_declined`, `dedup_full_set_total`, query-pool `Resources exhausted`, p99.

**Go/no-go after the flip** (checked at +1 h and +24 h; process ≥10 min old):

| Signal | Go | Roll back |
|---|---|---|
| `otel_metrics` queries failing with `unordered merge-on-read dedup exceeded` or `Resources exhausted` | 0 | any |
| tuple-path ordering-violation errors | 0 | any (a lying footer exists — investigate before resuming) |
| `ordering_repair_declined` for `otel_metrics` | 0 (Stage 2b keeps layouts in separate legs) | > 0 sustained over 1 h |
| `dedup_full_set_total` delta vs the 24 h before the flip | ≤ +10% | > +10% |
| repair backlog for `otel_metrics` | drains every hour | flat for 6 h |

Rollback = revert the yaml order and redeploy. Both layouts carry honest footers and Stage 2b handles either
majority, so rollback is a read-safe deploy; repair then migrates the already-converted days back (cost, not
correctness).
- Done when every sealed `otel_metrics` file's footer matches the schema.

Success criteria (prod, process ≥10 min old, ≥3 runs, alternate arms):
- Demo 24h web vitals: rows read ≥10× lower than 44.7M; wall time reported before/after.
- No increase in `dedup_full_set_total` for `otel_metrics` reads.
- Maintenance lanes within the Stage 0 budget.

## Out of scope / follow-ups

- Attribute-value filters (`attributes->>'page' = …`) select series inside a metric; `attributes` is Variant
  with no stats, and `series_id` (a hash of the attribute set) is only the tiebreak. If those queries are slow: bloom or
  tantivy sidecars on metric attributes, or promote hot attributes to columns.
- Today's partition: hourly files are sorted metric-first too, so today-window reads benefit immediately
  after Stage 3, without waiting for migration.

## Sources

- ClickHouse OTel exporter schema: https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/exporter/clickhouseexporter/README.md
- ClickStack schemas: https://clickhouse.com/docs/use-cases/observability/clickstack/ingesting-data/schemas
- InfluxDB 3 primary key / sort: https://docs.influxdata.com/influxdb3/core/get-started/
- Cortex Parquet storage: https://cortexmetrics.io/docs/proposals/parquet-storage/
- Delta partitioning guidance: https://learn.microsoft.com/en-us/azure/databricks/delta/best-practices
- Liquid clustering: https://docs.delta.io/delta-clustering/ · delta-rs issue: https://github.com/delta-io/delta-rs/issues/2043
