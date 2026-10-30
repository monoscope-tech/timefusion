# Explicit-bucket histogram quantiles

Owner: TimeFusion (functions), with one Monoscope browser SDK change that ships first.
Status: spec, not started.

## Why

Monoscope's RUM Web Vitals show the P75 of OTel `HISTOGRAM` points stored in `otel_metrics`
(`hist_explicit_bounds List<Float64>`, `hist_bucket_counts List<Int64>`). TimeFusion has no function
that merges bucket arrays, so Monoscope computes the quantile in a 300-line window-function SQL
(`monoscope/src/Models/Telemetry/RUM.hs:302` `rumVitalPopulation`). For CUMULATIVE series, a second
query (`:320` `withHistory`) gets the baseline points from before the window. Dashboards cannot
express this, so `rum.yaml` uses `distribution_sum / distribution_count` (the mean), which is the
wrong statistic. The goal is to make this one expression:

```sql
SELECT histogram_quantile(0.75, histogram_agg(hist_explicit_bounds, hist_bucket_counts))
FROM otel_metrics WHERE metric_name = 'browser.web_vital.lcp' AND …
```

## Step 0: Fix the source (monoscope-web, ships first)

`monoscope-web/src/tracing.ts:176-205` uses OTel defaults for both temporality and buckets:

1. **Temporality is CUMULATIVE.** Add `temporalityPreference: AggregationTemporalityPreference.DELTA`
   to the `OTLPMetricExporter` (`@opentelemetry/exporter-metrics-otlp-http` ^0.203 supports it).
   With DELTA, each point holds only its own observations. A plain sum is then correct, and nothing
   needs baselines, reset detection or the `withHistory` re-query.
2. **Buckets are the defaults** `[0,5,10,25,50,75,100,250,500,750,1000,2500,5000,7500,10000]`.
   - **CLS bug:** CLS is a 0–1 score, so every non-zero CLS value lands in the `(0,5]` bucket.
     Linear interpolation then reports a P75 around 3.75 for any population mostly above 0, which is
     rated "Poor" for every site.
   - **Fix:** pass `advice: { explicitBucketBoundaries }` to `createHistogram`
     (`@opentelemetry/api` ^1.9 supports it). Make the Web Vitals thresholds bucket edges, so a
     Good/Needs-improvement/Poor rating never has to interpolate across a threshold:

| vital | unit | bounds |
|---|---|---|
| LCP | ms | 500, 1000, 1500, 2000, **2500**, 3000, **4000**, 6000, 10000, 20000 |
| FCP | ms | 500, 1000, **1800**, 2400, **3000**, 4000, 6000, 10000 |
| INP | ms | 50, 100, **200**, 300, 400, **500**, 1000, 2000 |
| TTFB | ms | 100, 200, 400, 600, **800**, 1200, **1800**, 3000, 6000 |
| CLS | 1 | 0.01, 0.05, **0.1**, 0.15, 0.2, **0.25**, 0.4, 0.6, 1 |

Every vital point carries `session.id`, `pageview.id` and the full `page.url`
(`tracing.ts:356` `commonAttrs`). So each page view is its own series, holding about one
observation per vital. That is why the CUMULATIVE handling in step 3 can be simple. Stored data
stays CUMULATIVE with default bounds, so TimeFusion still has to read it (step 3, merge rule 1).

## Step 1: Functions (`src/read/functions.rs`)

Copy the pair TimeFusion already ships for percentiles:
`percentile_agg` → `tdigest_merge` → `approx_percentile` (`:660-661`, `ApproxPercentileUDF` `:1414`,
the `Sketch` trait, `SketchAccumulator`). The new functions follow the same pattern.

```text
histogram_agg(bounds List<Float64>, counts List<Int64>) -> Binary   -- aggregate
histogram_merge(state Binary)                           -> Binary   -- aggregate, for rollups
histogram_quantile(q Float64, state Binary)             -> Float64  -- scalar
histogram_quantile_bucket(q Float64, state Binary)      -> Struct{lower Float64, upper Float64}
histogram_count(state Binary)                           -> Int64
```

- **State:** `HistogramState { bounds: Vec<f64>, counts: Vec<u64> }`, where
  `counts.len() == bounds.len() + 1` and the last entry is the `+Inf` bucket. Serialise with bincode,
  as the t-digest does. An empty state means no input rows.
- **`histogram_agg`** takes two inputs, so build it with `create_udaf` directly (`binary_state_udaf`
  only takes one). Implement it as `Sketch` for `HistogramState` with `insert_array` over a pair of
  list arrays, or make the trait generic over its input arity. Either way, `SketchAccumulator`
  supplies `state`/`merge_batch`.
- **Input validation:** skip a row (and add it to the invalid count) when any of these hold:
  - `counts.len() != bounds.len() + 1`
  - the bounds are not strictly increasing
  - a bound is non-finite
  - a count is negative
  - either list is NULL

  Never fail the query. One OTel quirk: a histogram with no bounds arrives as `bounds = []`,
  `counts = [n]`. That is valid: a single `+Inf` bucket.

### Merge rules

1. **Mismatched bounds: coarsen to the intersection.** Merging two states whose bounds differ keeps
   only the bounds present in both, and sums the adjacent buckets that were dropped. Summing is
   exact; splitting a bucket is not. Monoscope's SQL already does this ("bound present in every
   point", `RUM.hs:439`). With the step 0 bounds fixed, this only triggers across SDK versions.
   Mixing old default-bound data with new data coarsens to their common edges (`1000, 2500, 10000`
   for LCP). The answer is still correct, just less precise, until the old data ages out.
2. **Associative and commutative.** `merge(merge(a,b),c) == merge(a,merge(b,c))`, so partial
   aggregation and rollups are safe.

### Quantile semantics (Prometheus `histogram_quantile`)

```text
N = Σ counts; rank = q · N
i = first bucket whose cumulative count ≥ rank
lower = bounds[i-1], or 0 for i = 0 (vitals are non-negative; use bounds[0] if bounds[0] ≤ 0)
upper = bounds[i]
value = lower + (upper - lower) · (rank - cum[i-1]) / counts[i]
```

| Case | `histogram_quantile` | `histogram_quantile_bucket` |
|---|---|---|
| `N = 0` | NULL | NULL |
| `i` is the `+Inf` bucket | NULL | `{lower: bounds[last], upper: NULL}` (Monoscope's `UnboundedBucket`) |
| Normal | the interpolated value | `{lower, upper}` |

Monoscope rates a vital from the bucket rather than the point estimate: Good when
`upper ≤ goodAt`. Once the bounds are the thresholds, that rating is exact.

### Worked examples (unit-test fixtures; these are the acceptance criteria)

The LCP bounds below are `[1000, 2500, 4000, 8000]`, so each `counts` array has 5 entries.

1. **Three DELTA points.** Counts `[2,5,1,0,0]`, `[0,3,2,1,0]`, `[1,1,0,0,0]` merge to `[3,9,3,1,0]`,
   so `N = 16` and `q = 0.75` gives rank 12. Cumulative counts are `3,12,15,16`, so `i = 1`.
   Value `1000 + 1500·(12−3)/9 = 2500`, bucket `{1000, 2500}`, rating Good. This also tests that
   the bucket edge belongs to the lower bucket.
2. **Quantile in `+Inf`.** `[0,0,0,1,3]`: `N = 4`, rank 3 falls in `+Inf`. Value NULL, bucket
   `{8000, NULL}`.
3. **Coarsening.**
   - Inputs: A has bounds `[1000,2500,4000]` and counts `[1,1,1,1]`. B has bounds `[2500]` and
     counts `[2,2]`.
   - The intersection is `[2500]`, so A becomes `[2,2]`. Merged counts are `[4,4]`, `N = 8`.
   - P50 is `0 + 2500·4/4 = 2500`, bucket `{0, 2500}`.
   - P75 is NULL, bucket `{2500, NULL}`.
4. **CLS with default bounds** (the bug from step 0). 7 points at `[0,1,0,…]` and 3 at `[1,0,…]`:
   `N = 10`, rank 7.5 falls in `(0,5]`, so the value is `5·(7.5−3)/7 ≈ 3.21`. Pin this as a test so
   the behaviour is documented, not "fixed" in TimeFusion.
5. **Equivalence check.** On a production-shaped DELTA fixture, run
   `histogram_quantile(0.75, histogram_agg(…))` grouped by field, by `time_bucket` and by page. It
   must equal `rumVitalPopulation`'s `p75`/`lower`/`upper` for the same rows.

Also test:
- associativity over a random split of the inputs into partitions
- the empty state
- invalid rows being skipped and counted
- `histogram_merge(histogram_agg(x)) == histogram_agg(x)`

## Step 2: Monoscope consumer

`rumVitalPopulation` collapses to one `GROUPING SETS ((metric), (bucket, metric), (page, metric))`
query over `histogram_agg`, followed by `histogram_quantile_bucket(0.75, …)` and `histogram_count`.
- `classifyVital` stays in Haskell, rating from the bucket.
- The 10 `VitalCoverage` reasons shrink to `{complete, unbounded, invalid_buckets, missing_baseline}`.
  The rest were symptoms of handling CUMULATIVE data at query time.
- `rum.yaml`'s vitals widgets switch from the mean to the same expression. The RUM page's
  Performance tab then becomes dashboard-expressible, which is the goal of the RUM consolidation.
- k6 `GAUGE` vitals (`k6.browser_web_vital_*`) are out of scope: they are scalar values, and the
  existing `approx_percentile(0.75, percentile_agg(value))` already covers them. Monoscope picks the
  expression by metric name; it no longer mixes the two.

## Step 3: CUMULATIVE data already stored

- **v1 (Monoscope-side SQL, no TimeFusion change).** Before aggregating, take the last point per
  `(series_id, start_timestamp)` among points whose `start_timestamp ≥ window_start`. Exclude series
  that started earlier and report them as `missing_baseline`.
  - This is correct for page-view series (one view, one start).
  - It is wrong for a long-lived series that started before the window. Browser series rarely do,
    and DELTA from step 0 retires the case.
  - Wrap the dedupe as `ROW_NUMBER() OVER (PARTITION BY series_id, start_timestamp ORDER BY timestamp DESC) = 1`
    in a subquery. Keep the window inline: TimeFusion drops partitions when a named window is inherited
    with a new order (`RUM.hs:338`).
- **Phase 2 (only if a non-browser CUMULATIVE source needs it): an exact per-series delta before the
  merge.**
  - Precedent: the `series_5m_v1` rollup (`schemas/otel_metrics.yaml`) keeps counter cells per
    series for `rate`/`increase`.
  - The histogram version keeps, per `(series_id, start_timestamp, 5m cell)`, the last cumulative
    counts. The delta is the last counts of this cell minus the last counts of the previous cell for
    the same `(series, start)`, computed per bucket. A drop in `distribution_count` or a new
    `start_timestamp` is a reset, and the reset point's own counts are the delta.
  - This is `counter_agg` + `interpolated_delta` (see the Timescale section), applied to every bucket.

## Step 4: Rollups (performance follow-up, not v1)

`src/rollup.rs:2464` maps `percentile_agg → Merge::TDigest`. A `histogram_agg → Merge::Histogram`
arm with a `histogram` measure kind is the natural extension. Two schema gaps block it, and neither
is solved here:
- A rollup measure takes one `column`; this one takes two (`hist_explicit_bounds`,
  `hist_bucket_counts`).
- `page.url` lives in the `attributes` Variant, and the matcher only routes plain dimension columns.
  So "vitals by page" cannot use a rollup without promoting the column, which is the four-step
  promotion order.

v1 is a raw scan over `otel_metrics` filtered by `metric_name`. That is already pruned by the sort
order (`metric_name` leads `sorting_columns`).

## Out of scope

- Exponential histograms (`exp_hist_*`). Keep the state versioned (a leading tag byte) so a later
  `ExpHistogramState` can share `histogram_quantile`.
- Exact quantiles, and `summary_quantiles`.

## How TimescaleDB / Tiger Data would do it (for comparison)

`timescaledb_toolkit` has no aggregate that merges OTel explicit buckets. Its `percentile_agg`,
`uddsketch` and `tdigest` take raw values. Its pattern is the one TimeFusion already copies: a
state aggregate, a merge (`rollup`), and an accessor. The Timescale approach is the Prometheus model:

1. **Normalise at ingest** to one row per bucket: `(series_id, ts, le, cumulative_count)`, where
   `le` is the bucket's upper bound.
2. **Continuous aggregate:**
   `counter_agg(ts, cumulative_count) GROUP BY series_id, le, time_bucket('5m', ts)`.
   `counter_agg` detects resets itself.
3. **Per-bucket delta:**
   `interpolated_delta(agg, bucket_start, '5m', LAG(agg) OVER w, LEAD(agg) OVER w)` with `w`
   partitioned by `(series_id, le)`. This is exactly what Monoscope hand-rolls with `LAG` plus the
   `withHistory` re-query.
4. **`SUM(delta) GROUP BY le`**, then a small SQL function
   `histogram_quantile(q, le float8[], counts float8[])` that applies the interpolation in step 1.

DELTA data skips steps 2–3 and goes straight to `SUM … GROUP BY le`. Compared with TimeFusion, the
row-per-bucket layout multiplies row count by the bucket count (11–16 rows per point). Keeping the
arrays and merging them in a UDAF, as in step 1, avoids that.
