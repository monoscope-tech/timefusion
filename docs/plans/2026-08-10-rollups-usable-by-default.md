# Rollups that are safe and used by default

**Status:** the current build path writes rollup rows. The read path does not
exist. `rollup::route` is called only by its unit tests, so `rollup_hits` stays
at zero.

The current build is not yet a safe input for automatic routing. Source
certification does not prove that the rollup build succeeded. A rebuild also
appends rows and can leave obsolete groups visible.

**Goal:** TimeFusion serves an eligible dashboard aggregate from a rollup
without a SQL change. The result includes the live tail and preserves the raw
query result.

**Initial target:** three production dashboard queries with these properties:

- one `otel_logs_and_spans` scan.
- one aggregate.
- one exact `project_id`.
- one bounded timestamp range.
- an optional `time_bucket` group.
- plain rollup dimensions.
- `count`, `sum`, `min`, `max`, `avg`, and approximate t-digest percentiles.

Use the SQL generated for these widgets as the named target fixtures:

- `Traffic` in `../monoscope/static/public/dashboards/_overview.yaml`.
- `P95 Latency` in the same file.
- `Error Rate` in the same file.

Capture the generated SQL before implementation. Do not use a hand-written
approximation in the regression tests.

The first release does not promise that every Overview panel routes. Current
panels also contain joins, high-cardinality groups, APDEX expressions, and
filters that the declared rollup cannot answer.

### Assumptions to confirm before implementation

- The generated target SQL uses `percentile_agg` and `approx_percentile`.
- The generated `countif` uses an aggregate `FILTER` expression.
- Only TimeFusion writes generated rollup tables.
- Raw fallback after a process restart is acceptable for the first release.
- The exact-file rewrite can reuse the current dedup staging and commit code.
- Operations can keep the unused v1 table through the canary.
- Product owners will record the percentile tolerance before read enablement.

If any assumption is false, update this plan before implementation.

---

## Definition of done

The feature is done after all of these conditions hold:

1. A named production dashboard query scans rollup data for its certified
   history.
2. The same query scans raw data for its uncertified tail and partial edge
   buckets.
3. Routing on and routing off return equal exact measures.
4. T-digest results stay within the accepted fixture ranges and canary delta.
5. A failed or stale build always causes a raw fallback.
6. A concurrent write cannot disappear from a routed result.
7. Prepared statements make a new routing decision for each execution.
8. `rollup_hits` increases only after a routed physical plan is accepted.
9. Each miss records one bounded reason.
10. All rollup flags restore a documented safe state without a code change.

Performance acceptance for the target seven-day panel:

- the physical plan reads rollup data plus at most the required raw tail.
- the plan reads no raw Parquet files for certified rollup dates.
- historical bytes read decrease by at least 10 times.
- the existing target remains less than 3 seconds cold and 1 second warm.

---

## Review findings that change the original plan

These findings come from the current implementation. They are prerequisites,
not optional hardening.

### 1. `dedup_clean_fp` is not rollup coverage

`dedup_clean_fp` proves that a source partition matched a clean file
fingerprint. `rebuild_rollup_partition` writes after that proof and treats
failures as best effort.

A build can fail after `dedup_clean_fp` is installed. Routing from that map
can treat missing rollup rows as a valid empty result.

The read gate therefore needs a separate marker for each rollup build. The
marker is installed only after a complete write.

### 2. Deterministic IDs do not replace a partition

The current builder calls `insert_records_batch`, which appends to Delta. A
stable `bucket_id` lets duplicate keys collide during later deduplication. It
does not remove a dimension tuple that disappeared from a rebuild.

The build needs an exact partition replacement and an explicit generation. The
read path must use only the generation that a complete coverage marker names.

### 3. The current `route` predicate is not complete

`Ask` stores aggregate names only. Correct routing also depends on:

- the source column.
- `COUNT(*)` versus `COUNT(column)`.
- `DISTINCT`.
- an aggregate `FILTER` predicate.
- the stored measure that answers each aggregate.
- null semantics for `avg`.
- the timestamp range and bound inclusivity.

For example, `avg(duration)` needs `sum(duration) / count(duration)`. If
`duration` is nullable, division by `request_count` is wrong.

### 4. Aggregate filters and row filters are different

A `WHERE` predicate filters every requested measure. An aggregate `FILTER`
predicate filters only one aggregate.

A stored `error_count` can answer an exact matching
`COUNT(*) FILTER (WHERE ...)`. By itself, it does not make the same
non-dimension predicate safe for `SUM(duration)`.

### 5. `percentile_agg::merge_batch` is not a SQL digest merger

`percentile_agg(Float64) -> Binary` merges partial states inside DataFusion.
SQL cannot call it with a stored `Binary` digest.

The read path needs an exposed aggregate such as
`tdigest_merge(Binary) -> Binary`. This aggregate can reuse the existing
`TDigestWrapper` merge operation.

### 6. `Binary` is not a schema type today

`schema_loader.rs` does not parse `Binary` into Arrow or Delta types.
`to_rollup_batches` also assumes hard-coded string dimensions and `Int64`
measures.

A t-digest measure needs `Binary` schema support and schema-driven row shaping.
Adding `agg: tdigest` alone cannot work.

### 7. An optimizer rule runs at the wrong lifecycle point

The plan cache stores optimized plans with placeholders. It substitutes values
later and can skip logical optimization for each execution.

Coverage also needs asynchronous table resolution and a current Delta snapshot.
`OptimizerRule::rewrite` is synchronous and cannot provide either property.

The routing decision belongs in `DmlQueryPlanner::create_physical_plan`. This
method runs after parameter substitution, including cached prepared plans. It
is also asynchronous.

### 8. A date-only split loses partial buckets

Dashboard bounds are not always minute-aligned. If a query starts at
`12:34:12`, the `12:34` rollup row contains 12 seconds that the query excluded.

The hybrid plan needs raw leading and trailing fringes. The uncertified date
tail is an additional split.

### 9. An unbucketed aggregate can use a rollup

A query without `GROUP BY time_bucket(...)` can aggregate all selected rollup
buckets into one result. Unaligned range bounds still need raw fringes.

The router must not reject an aggregate only because it has no time bucket.

### 10. Candidate metrics cannot live inside `route`

Calling `route` for several specs records several hits and misses for one query.
Metrics must record the final planner decision once.

### 11. The build trigger is behind a legacy gate

`rebuild_rollup_partition` runs only from `dedup_today_partitions`. The normal
maintenance path calls that sweep only if `timefusion_dedup_sweep_fallback` is
true. Its default is false.

Rollup build enablement must invoke certification independently of that legacy
fallback gate. A clean-fingerprint skip must also retry missing rollup coverage.

### 12. A build query must not route through a rollup

`query_delta_only` creates a normal TimeFusion session. After read routing is
enabled, the build aggregate can match the rollup that it is rebuilding.

The builder needs an internal raw-only planning flag. Hierarchical builds must
name their parent explicitly instead of using automatic query routing.

---

## What already exists and remains useful

| Piece | Location | Use in this plan |
|---|---|---|
| `RollupSpec` and `RollupMeasure` | `schema_loader.rs` | Base configuration model. Extend validation and measure kinds. |
| `RollupSpec::synthesize` | `schema_loader.rs` | Base generated-table schema. Add internal generation and `Binary` support. |
| Registry synthesis | `schema_loader.rs` | Keep generated table registration and collision checks. |
| `build_partition_sql` | `rollup.rs` | Keep spec-driven aggregate SQL. Correct count and t-digest handling. |
| `to_rollup_batches` | `rollup.rs` | Replace hard-coded shaping with schema-driven column transfer. |
| Certification trigger | `database.rs` | Keep certification as the only normal build trigger. |
| `dedup_clean_fp` | `database.rs` | Keep as source-cleanliness input. Do not use it as build coverage. |
| `TDigestWrapper` | `functions.rs` | Reuse its serialization and merge operation. |
| `DmlQueryPlanner` | `dml.rs` | Add the late rollup routing decision here. |
| Rollup metrics | `metrics.rs` | Move recording from candidate matching to final plan selection. |

---

## Core invariants

### Source invariant

A rollup generation is valid only for the exact clean source fingerprint from
which TimeFusion built it.

### Build invariant

A coverage marker names one complete generation. A partial or failed build has
no marker.

### Read invariant

Each source row belongs to exactly one branch:

- the accepted rollup core.
- a raw leading fringe.
- a raw uncovered tail.
- a raw trailing fringe.

The ranges are half-open and cannot overlap.

### Concurrency invariant

The planner accepts a routed physical plan only after source epochs, source
fingerprints, and coverage markers remain unchanged across physical planning.

### Fallback invariant

Any parse, match, coverage, snapshot, or planning uncertainty returns the
original raw plan. A rollup error must not fail an otherwise valid query.

---

## Phase 0. Add kill switches

Add these configuration fields:

```rust
timefusion_rollup_enabled: bool          // default false, master build and read gate
timefusion_rollup_read_enabled: bool     // default false, read gate
timefusion_rollup_realtime_tail: bool    // default false, hybrid raw-tail gate
```

Add optional build and read canary selectors:

```text
TIMEFUSION_ROLLUP_BUILD_PROJECTS=p1,p2
TIMEFUSION_ROLLUP_READ_PROJECTS=p1,p2
```

If a value is unset, its gate applies to all projects. If it is set, only the
listed projects can build or route.

Required behavior:

- If `timefusion_rollup_enabled` is false, the builder returns before query or
  write work.
- If `timefusion_rollup_read_enabled` is false, `DmlQueryPlanner` does not run
  rollup matching.
- If `timefusion_rollup_realtime_tail` is false, only fully covered and
  grain-aligned windows can route.
- The read gates do not change build behavior.

**Acceptance:** master-off stops rollup builds and reads. Read-off preserves the
current build-only behavior.

---

## Phase 1. Make build output safe to route

### 1.1 Add a generation and atomic partition replacement

Add an internal `rollup_generation` `Utf8` column to synthesized rollup tables.
One build uses one UUID for all rows in its `(project, source, spec, date)`
partition.

The coverage map stores:

```rust
struct RollupCoverage {
    source_fp: u64,
    source_epoch: u64,
    generation: String,
    rows: u64,
}

type RollupCoverageKey = (String, String, String, chrono::NaiveDate);
//                         project source rollup date
```

A routed rollup scan includes the generation predicate for each covered date.
Old generations cannot affect the result.

Include the generation in each generated `id`. Two generations must not share a
dedup key. This rule avoids dependence on filter execution before `DedupExec`.

Do not append each generation through `insert_records_batch`. Replace the exact
rollup `(project_id, date)` partition in one Delta commit:

1. Stage every new Parquet file.
2. Resolve every active target file for the partition.
3. Commit exact `Remove` and `Add` actions together.
4. For an empty build, commit only the exact `Remove` actions.
5. On an OCC retry, refresh the target snapshot and resolve files again.

Reuse the exact-file rewrite pattern from deduplication. Do not use
`replace_where`, which cannot safely express both partition columns here.
Use the same project mapping for unified and custom tables.

Stage outside the target commit lock. Hold that lock only for refresh and commit.

This commit removes groups that disappeared and prevents active generation
growth. Delta tombstone cleanup remains a separate maintenance concern.

A successful zero-row build still installs a marker with `rows = 0`. The
planner omits that date from the rollup scan. This behavior distinguishes an
empty partition from a missing build.

The coverage map can remain process-local for the first release. Prune entries
outside the maximum routable date window and entries for removed specs. A
marker eviction causes raw fallback.

After a restart, reads fall back to raw until certification rebuilds the
markers.

### 1.2 Invalidate before source writes

Maintain a monotonic source epoch for each `(project, source, date)`.

Increment the epoch before an inbound source write can become visible. Remove
all coverage markers for that source partition at the same time. Add each
affected rollup key to the bounded retry map.

Cover all write paths:

- pgwire and gRPC inserts.
- buffered writes before the WAL and MemBuffer acknowledgement.
- `__bulk` direct writes.
- merge-on-read appends.
- in-place UPDATE and DELETE operations before their commits.

If a path cannot identify the exact project and date, invalidate its wider
project or table scope. Never preserve coverage from an uncertain write.

A flush of rows that were already invalidated does not need a second logical
invalidation. A duplicate invalidation is safe. Prune old epochs only after
their coverage and retry entries are absent.

### 1.3 Close the build race

The builder performs this sequence:

1. Remove the old coverage marker.
2. Capture the clean source fingerprint and source epoch.
3. Build the aggregate from Delta.
4. Replace the target partition with one new generation.
5. Resolve the current source snapshot again.
6. If the fingerprint and epoch still match, install coverage.

If step 6 fails, leave the generation without a marker. A later clean pass
replaces it and installs a new marker.

Run the build aggregate in a raw-only internal session. The session still reads
Delta only, but `DmlQueryPlanner` must skip automatic rollup routing.

### 1.4 Make row shaping schema-driven

`to_rollup_batches` must stop matching `DIMENSIONS` and measure names as
constants.

For each synthesized target field:

- create identity fields explicitly.
- copy dimensions by the names in `spec.dimensions`.
- copy measures by the names in `spec.measures`.
- preserve the aggregate output array type.
- cast values for target types that require it.
- assign the same `rollup_generation` to every output row.

This change is required for named rollups, non-string dimensions, and binary
digests.

Remove the module constants as runtime sources of truth. This includes
`ROLLUP_TABLE`, `GRAIN_SUFFIX`, `GRAIN_MICROS`, `DIMENSIONS`, and
`DECOMPOSABLE`. Keep fixed values only inside focused test fixtures.

Make bucket helpers accept the spec grain. Make every builder and matcher use
the selected `RollupSpec`.

### 1.5 Reject invalid specs at load

Reject these configurations during `SchemaRegistry::new`:

- an invalid or zero grain.
- an aggregate outside the supported set.
- a missing required source column.
- a nonnumeric t-digest source.
- duplicate dimensions.
- duplicate measure names.
- a measure name that collides with an identity or dimension field.
- an invalid measure filter.
- a filter that does not return Boolean.
- an unsafe generated table-name suffix.

Use `[a-z][a-z0-9_]*` for explicit rollup names. Quote all generated SQL
identifiers and literals. Never insert an unescaped project ID into build SQL.

Use the aggregate output type for the synthesized measure. Do not assume that
every `SUM` returns the source type.

### 1.6 Version the generated table

The new table adds `rollup_generation`, `duration_count`, and a binary digest.
The existing generated Delta table has a different stored schema.

Use a new explicit rollup name for the first routed version:

```yaml
rollups:
  - grain: 1m
    name: dashboard_1m_v2
    dimensions:
      - resource___service___name
      - kind
      - status_code
    measures:
      - { name: request_count, agg: count }
      - { name: duration_count, agg: count, column: duration }
      - name: error_count
        agg: count
        filter: "status_code = 'ERROR' OR COALESCE(attributes___http___response___status_code, 0) >= 500"
      - { name: duration_sum, agg: sum, column: duration }
      - { name: duration_min, agg: min, column: duration }
      - { name: duration_max, agg: max, column: duration }
      - name: server_request_count
        agg: count
        filter: "kind = 'server' OR name = 'apitoolkit-http-span' OR name = 'monoscope.http'"
      - name: server_duration_count
        agg: count
        column: duration
        filter: "kind = 'server' OR name = 'apitoolkit-http-span' OR name = 'monoscope.http'"
      - name: server_duration_sum
        agg: sum
        column: duration
        filter: "kind = 'server' OR name = 'apitoolkit-http-span' OR name = 'monoscope.http'"
      - name: server_duration_min
        agg: min
        column: duration
        filter: "kind = 'server' OR name = 'apitoolkit-http-span' OR name = 'monoscope.http'"
      - name: server_duration_max
        agg: max
        column: duration
        filter: "kind = 'server' OR name = 'apitoolkit-http-span' OR name = 'monoscope.http'"
      - name: server_error_scope_count
        agg: count
        filter: "kind = 'server' OR name = 'apitoolkit-http-span'"
      - name: server_error_count
        agg: count
        filter: "(kind = 'server' OR name = 'apitoolkit-http-span') AND (status_code = 'ERROR' OR COALESCE(attributes___http___response___status_code, 0) >= 500)"
      - name: server_duration_digest
        agg: tdigest
        column: duration
        filter: "kind = 'server' OR name = 'apitoolkit-http-span' OR name = 'monoscope.http'"
```

This creates `otel_logs_and_spans_rollup_dashboard_1m_v2` from a fresh schema.
It avoids an unsafe in-place schema change on the current table.

The server predicate is stored in filtered measures. This avoids a
high-cardinality `name` dimension while preserving the current widget SQL.

The Error Rate widget omits `name = 'monoscope.http'`. Its separate scope count
is intentional. Do not merge it with `server_request_count`.

Before deployment, measure the p50, p95, and p99 tuple count per minute. Also
measure serialized digest bytes per day. If either value exceeds its recorded
budget, do not enable builds.

Keep the old table until the new read path finishes its canary. Remove it in a
separate cleanup change.

### 1.7 Wire certification to the normal maintenance path

If a source declares rollups and the master gate is on, run its complete
partition certification independently of `timefusion_dedup_sweep_fallback`.
Reuse the current lookback and per-partition fingerprint skips. Obey the build
project selector before per-project certification.

A matching clean source fingerprint can skip deduplication. It cannot skip a
missing rollup build. In that case, build directly from the certified snapshot.

Make `rebuild_rollup_partition` return its result. Put failed or stale builds in
a bounded retry map with backoff. Process that map even if the source Delta
version is unchanged.

Do not build a partition that still has dirty bins or buffered source rows. Its
query range remains raw until a later complete certification.

**Phase 1 acceptance:** build failure, concurrent invalidation, or a stale
source fingerprint leaves no usable coverage marker. A later maintenance pass
retries the build without a new source commit.

---

## Phase 2. Expose mergeable t-digest state

### 2.1 Add `Binary` schema support

Add `Binary` to both schema parsers:

- `parse_arrow_data_type("Binary") -> DataType::Binary`.
- `parse_delta_data_type("Binary") -> PrimitiveType::Binary`.

Add a schema round-trip test through a generated Delta table.

### 2.2 Add `tdigest_merge`

Register this UDAF beside `percentile_agg`:

```text
tdigest_merge(Binary) -> Binary
```

Its accumulator:

1. ignores null input.
2. deserializes each digest.
3. merges it with `TDigestWrapper::merge`.
4. compresses to `TDIGEST_MAX_CENTROIDS`.
5. returns the existing serialized format.

Keep this expression unchanged above the aggregate:

```sql
approx_percentile(0.95, tdigest_merge(server_duration_digest))
```

The planner maps the existing raw expression as follows:

```sql
approx_percentile(0.95, percentile_agg(CAST(duration AS DOUBLE)))
```

It replaces only the aggregate state expression. The outer
`approx_percentile` call remains in place.

### 2.3 Extend build SQL

Map spec measures as follows:

| Spec | Build expression |
|---|---|
| `{ agg: count }` | `COUNT(*)` |
| `{ agg: count, column: c }` | `COUNT(c)` |
| `{ agg: sum, column: c }` | `SUM(c)` |
| `{ agg: min, column: c }` | `MIN(c)` |
| `{ agg: max, column: c }` | `MAX(c)` |
| `{ agg: tdigest, column: c }` | `percentile_agg(CAST(c AS DOUBLE))` |

Apply `filter:` as SQL aggregate `FILTER (WHERE ...)`. Do not move it into the
partition `WHERE` clause.

### 2.4 Define percentile acceptance

Do not invent a general numeric error bound for the current t-digest code.
Use two measurable checks instead:

- deterministic fixtures have fixed accepted quantile ranges.
- canary queries record the relative raw-versus-rollup delta for p50, p95, and
  p99.

If the canary distribution exceeds the agreed product tolerance, stop rollout.
Record that tolerance in the canary report before broad enablement.

**Phase 2 acceptance:** merging stored partition digests matches a manual merge
of the same digest bytes. Fixture quantiles remain in their accepted ranges.

---

## Phase 3. Route after parameter substitution

Keep routing in `rollup.rs`. Add a small call from
`DmlQueryPlanner::create_physical_plan`. Do not add a logical optimizer rule.

The planner order is:

1. preserve existing DML handling.
2. preserve the logical-count fast path.
3. If the read gate permits it, try a rollup physical plan.
4. use the normal physical planner on any miss or stale check.

The logical-count path is cheaper than a rollup scan, so it keeps precedence
for shapes it can answer.

### 3.1 Plan with current values

`create_physical_plan` receives the plan after pgwire calls
`replace_params_with_values`. This property applies to prepared and cached
plans.

The matcher can therefore read the actual:

- project ID.
- lower timestamp bound.
- upper timestamp bound.
- `time_bucket` interval.

Do not store the routing decision in `PlanCacheHook`. The cache stores query
shape. Coverage and bound alignment remain execution-specific.

### 3.2 Use asynchronous snapshot checks

`try_rollup_plan` performs this sequence:

1. Analyze the logical plan without side effects.
2. Resolve the source table and candidate rollup tables.
3. Capture source epochs and source file fingerprints.
4. Match and capture coverage markers for those fingerprints.
5. Build the rewritten logical plan.
6. Ask the default planner to create its physical plan.
7. Resolve source epochs, fingerprints, and coverage markers again.
8. If all captured values still match, accept the physical plan.

The table providers capture their MemBuffer and Delta inputs during physical
planning. If a write starts during steps 3 through 6, its epoch change makes
step 7 reject the rollup plan.

If a write starts after step 7, the physical plan already represents the
pre-write query snapshot. This is valid query behavior.

A source file rewrite that bypasses ingestion still changes the file
fingerprint and causes rejection.

### 3.3 Keep failure silent but observable

`try_rollup_plan` returns `Ok(None)` for any safe miss. It logs unexpected
internal errors at debug or warning level and returns the raw plan.

Do not return an error to the client unless the original raw plan also fails.

Record a hit only after step 8 accepts the physical plan.

**Phase 3 acceptance:** one prepared statement can route for one execution and
fall back for the next after coverage changes.

---

## Phase 4. Normalize and match an aggregate

Replace the string-only `Ask` with a typed request. Keep the type local to
`rollup.rs` unless another module needs it.

```rust
struct RollupAsk {
    source: String,
    project_id: String,
    range: HalfOpenMicros,
    bucket: Option<BucketAsk>,
    group_dimensions: Vec<String>,
    row_filters: Vec<Expr>,
    measures: Vec<MeasureAsk>,
}

enum MeasureAsk {
    CountAll { filter: Option<Expr> },
    CountColumn { column: String, filter: Option<Expr> },
    Sum { column: String, filter: Option<Expr> },
    Min { column: String, filter: Option<Expr> },
    Max { column: String, filter: Option<Expr> },
    Avg { column: String, filter: Option<Expr> },
    TDigest { column: String, filter: Option<Expr> },
}
```

Preserve the original aggregate expression, output type, and alias beside each
normalized measure. The rewrite needs them to restore the original schema.

### 4.1 Accepted logical shape

The first implementation accepts one aggregate over one source scan. It can
peel this unary input chain:

- `SubqueryAlias`.
- `Filter`.
- a column-pass-through `Projection`.
- `TableScan`.

Outer `Projection`, `Sort`, `Limit`, `Filter` for HAVING, and supported scalar
expressions can remain above the aggregate.

Reject these inputs for the first release:

- joins.
- unions already present in the source query.
- window input below the aggregate.
- nested aggregates.
- grouping sets, cubes, and rollups.
- `DISTINCT` aggregates.
- correlated subqueries.
- more than one independent aggregate source.

A rejection preserves the complete original plan.

### 4.2 Extract one exact project

Accept an equality between `project_id` and one scalar literal. Handle either
operand order and harmless casts.

Reject:

- missing project filters.
- `IN` with several projects.
- OR branches with different projects.
- expressions whose value is not known at planning time.

This rule preserves tenant isolation and keeps coverage keys exact.

### 4.3 Normalize time bounds to `[lo, hi)`

Convert supported predicates as follows:

| Input | Half-open bound |
|---|---|
| `timestamp >= x` | `lo = x` |
| `timestamp > x` | `lo = x + 1µs` |
| `timestamp < y` | `hi = y` |
| `timestamp <= y` | `hi = y + 1µs` |
| `timestamp BETWEEN x AND y` | `[x, y + 1µs)` |

Use checked arithmetic at timestamp limits. Reject overflow, empty or inverted
ranges, and unsupported OR or NOT time predicates.

Require both bounds in the first release. Expand covered dates through
`hi - 1µs`, not `hi`. This avoids an extra date at an exact UTC midnight.

Reuse existing literal evaluation only for a `now()` value that this execution
fixed.

### 4.4 Normalize group expressions

Accept zero or one time group. Initially accept only the two-argument
`time_bucket` form with a constant fixed-width interval.

The requested width must be at least the stored grain and an exact multiple of
it. Both use the UTC Unix epoch as bucket origin.

Accept every other group expression only as a plain declared dimension.
Reject computed groups and undeclared columns.

An absent time group is valid. The final state merge aggregates all selected
rollup buckets into one result per dimension group.

Do not add general `date_bin` support in this phase. Its explicit origin needs a
separate alignment proof.

### 4.5 Classify filters

Split top-level `AND` terms into these classes:

1. routing predicates for `project_id`, `timestamp`, and derived `date`.
2. row predicates whose columns are all declared dimensions.
3. one residual row predicate for prefiltered measures.
4. a supported input-null predicate.
5. aggregate-local `FILTER` predicates.
6. unsupported predicates.

Dimension predicates can run unchanged on rollup rows.

A residual predicate cannot run on rollup rows. Instead, combine it with each
aggregate-local filter. Match that effective filter to a stored measure filter.
Every requested aggregate must find such a measure.

For grouped output, also require a stored row-existence count under the residual
predicate. Remove the final group if the merged existence count is zero. For
ungrouped output, keep the one SQL aggregate row.

This rule maps the target widget predicate on `kind OR name` to the stored
`server_*` measures. It avoids adding `name` as a dimension.

Treat `c IS NOT NULL` as an input-null predicate only if every requested
measure ignores nulls from `c`. Use the matching `COUNT(c)` as its existence
count. Do not generalize this rule to comparisons or arbitrary null expressions.

Canonicalize harmless differences before filter comparison:

- aliases and qualifier names.
- commutative operand order.
- flattened associative `AND` and `OR` trees.
- redundant Boolean parentheses.
- literal casts that DataFusion already folded.

Do not attempt general Boolean equivalence.

### 4.6 Match measures by semantics

A measure matches on all of these fields:

- aggregate kind.
- source column or `COUNT(*)`.
- the normalized effective filter.
- non-distinct semantics.
- compatible output type.

The effective filter is the conjunction of the residual row predicate and the
aggregate-local filter. For a null-ignoring aggregate, omit its accepted
`c IS NOT NULL` term.

Mappings are:

| Query state | Stored state |
|---|---|
| `COUNT(*)` | matching count-all measure |
| `COUNT(c)` | matching count-column measure |
| `SUM(c)` | matching sum measure |
| `MIN(c)` | matching min measure |
| `MAX(c)` | matching max measure |
| `AVG(c)` | matching sum plus count-column measures |
| `percentile_agg(CAST(c AS DOUBLE))` | matching t-digest measure |

Exact percentile functions and `COUNT(DISTINCT ...)` always miss.

### 4.7 Select one candidate

Filter all source rollups by semantic answerability before selecting a table.

For full-coverage routing, choose among fully covered candidates by:

1. coarsest compatible grain.
2. fewest extra dimensions.
3. table name for a deterministic tie break.

For hybrid routing, first choose the candidate with the longest contiguous
covered core. Use the same three tie breaks after coverage length.

A finer complete rollup can beat a coarser incomplete rollup.

If no candidate remains, record one final miss reason. Do not record a miss for
each rejected spec.

**Phase 4 acceptance:** a table-driven matcher test covers every accepted and
rejected semantic distinction above.

---

## Phase 5. Rewrite through mergeable states

Use one state pipeline for full and hybrid routing. Do not special-case each
final aggregate.

### 5.1 Create a common branch schema

Each raw or rollup branch emits:

- the optional requested output bucket.
- the requested dimension groups.
- one or more mergeable state columns for each query aggregate.

State columns are:

| Query aggregate | Branch state | Final merge |
|---|---|---|
| `COUNT(*)` | count | `SUM` |
| `COUNT(c)` | non-null count | `SUM` |
| `SUM(c)` | nullable sum | `SUM` |
| `MIN(c)` | nullable minimum | `MIN` |
| `MAX(c)` | nullable maximum | `MAX` |
| `AVG(c)` | nullable sum and non-null count | `SUM`, then divide |
| `percentile_agg(c)` | digest | `tdigest_merge` |

The final aggregate groups the union by requested bucket and dimensions. A
final projection restores the original expressions, names, order, and types.

For `AVG(c)`, inspect the merged `COUNT(c)`. If it is zero, return a typed null.
Otherwise, divide the merged sum by that count with the original DataFusion output type.

If every branch is empty, use an empty relation with the common state schema.
This preserves the one-row result of an ungrouped SQL aggregate.

This two-stage form preserves an output bucket that receives data from both raw
and rollup branches.

### 5.2 Build the rollup branch

The rollup branch:

- scans the selected generated table through normal project routing.
- applies the exact `project_id`.
- applies each accepted dimension filter.
- applies the covered date and generation pairs.
- applies the rollup core timestamp range.
- re-buckets stored `timestamp` for a coarser requested width.
- maps stored measures to the common state schema.

Generation predicates can use an OR of `(date, generation)` pairs. Bound the
number of dates with the existing query-window limit.

### 5.3 Build raw branches

Each raw branch keeps the original source semantics and adds one half-open split
predicate. It computes the same common state schema from raw rows.

Do not remove accepted dimension filters from the raw branch. Do not reuse the
rollup measure filter as a row filter.

### 5.4 Split boundaries without gaps

For query range `[lo, hi)` and stored grain `g`:

```text
aligned_lo = ceil_to_grain(lo, g)
aligned_hi = floor_to_grain(hi, g)
```

Classify each UTC source date in `[aligned_lo, aligned_hi)` as:

- `Covered(generation)`: its marker, epoch, and source fingerprint match.
- `Empty`: its source snapshot has no rows, files, or buffer data.
- `Uncovered` otherwise.

The first release uses one contiguous covered core. It stops at the first
`Uncovered` date. An `Empty` date does not break the core and emits no rollup
scan.

The branches are:

```text
raw leading fringe: [lo, aligned_lo)
rollup core:         [aligned_lo, covered_hi)
raw tail:            [covered_hi, hi)
```

`covered_hi` cannot exceed `aligned_hi`. Therefore the raw tail also includes
the trailing partial bucket `[aligned_hi, hi)`.

Skip empty ranges. Require `rollup core` to contain at least one full stored
bucket.

When `timefusion_rollup_realtime_tail` is false, route only when:

- `lo == aligned_lo`.
- `hi == aligned_hi`.
- every nonempty date is covered.

### 5.5 Preserve time-bucket labels

Both branch types compute the requested bucket from their own timestamp column.
They use the same width and epoch origin.

This requirement matters for a coarse output bucket that crosses the raw-rollup
boundary. The final aggregate must merge those branch rows into one label.

### 5.6 Keep the original plan on any rewrite defect

Before physical planning, compare the rewritten output schema with the original
aggregate subtree schema. Names, order, nullability, and data types must match.

If they differ, log the mismatch and use the raw plan.

**Phase 5 acceptance:** every accepted plan has no overlap or gap in its branch
ranges. Its output schema equals the raw plan schema.

---

## Phase 6. Add bounded historical coverage

A new `dashboard_1m_v2` table has no old generations. Without backfill, a
seven-day query cannot benefit for seven days. Backfill is therefore required
for usability, even though it is not part of steady-state routing.

Reuse `timefusion_dedup_lookback_days` for the initial backfill. During the
build-only deployment, set it to the widest canary window plus one day. Keep the
build project selector on the canary projects.

The Phase 1 certification path processes those dates with the existing
maintenance semaphore and per-partition loop. It uses the same generation,
replacement, snapshot, and retry protocol.

Do not create a command or a second scheduler for the first release. After the
backfill, restore the normal dedup lookback. Existing coverage remains valid.

**Phase 6 acceptance:** a bounded seven-day backfill can stop and resume without
duplicating visible results.

---

## Phase 7. Add hierarchical grains after base routing works

Do not build 1-hour and 1-day tables from raw rows. Build them from a complete
finer rollup generation.

Add an explicit parent reference. Do not use grain alone because several named
rollups can share one grain.

```yaml
rollups:
  - grain: 1m
    name: dashboard_1m_v2
    # dimensions and measures omitted

  - grain: 1h
    name: dashboard_1h_v1
    from: dashboard_1m_v2
    # dimensions and measures omitted
```

Load-time rules:

- `from` resolves to one sibling rollup name.
- the graph is acyclic.
- parent grain divides child grain exactly.
- bucket origins match.
- child dimensions are a subset of parent dimensions.
- each child measure is mergeable from a parent state.
- filtered measures preserve the same normalized filter.

Build parents before children. A child coverage marker records its parent
fingerprint or generation set. Parent invalidation removes descendant coverage.

Use these merge expressions:

- count and sum use `SUM`.
- minimum uses `MIN`.
- maximum uses `MAX`.
- digest uses `tdigest_merge`.

Candidate selection already chooses the coarsest compatible complete grain, so
this phase needs no new routing semantics.

**Phase 7 acceptance:** one-hour results built from one-minute rows match a
manual one-minute re-aggregation.

---

## Observability

### Query metrics

Record one result for each aggregate candidate that reaches rollup matching:

```text
rollup_hits{mode="full|hybrid", grain="1m"}
rollup_misses{reason="..."}
```

Keep miss reasons closed and bounded:

- `unsupported_shape`.
- `missing_project`.
- `unbounded_time`.
- `unknown_group_by`.
- `unknown_filter`.
- `missing_measure`.
- `non_decomposable`.
- `unaligned_range`.
- `incomplete_coverage`.
- `stale_during_plan`.
- `rewrite_schema_mismatch`.

`grain` values come from loaded configuration, so their cardinality is bounded.
Do not label metrics with project, table, SQL, date, or generation.

Add counters for:

- rollup rows scanned.
- raw-tail rows scanned.
- covered dates used.
- raw dates used.

If physical row counters are not available without invasive wrappers, defer
these four counters. Do not estimate and report them as exact.

### Build metrics

Add:

```text
rollup_builds{result="success|failed|stale"}
rollup_build_rows
rollup_coverage_partitions
rollup_build_duration
```

Use logs for source, project, date, and rollup name. Keep those values out of
metric labels.

### Plan inspection

A routed physical plan must expose the generated rollup table name. A hybrid
plan must expose both source and rollup scans.

Add concise debug output for:

- selected rollup.
- selected mode.
- half-open branch ranges.
- final miss reason.

Do not log full SQL at info level.

---

## Test plan

Follow the repository bug-fix rule. Add each regression test before its fix.
Confirm the named failure.

### Build safety tests

1. `rollup_build_failure_does_not_publish_coverage`
2. `rollup_partition_replace_removes_old_files_atomically`
3. `rollup_partition_replace_handles_unified_and_custom_tables`
4. `rollup_rebuild_hides_a_group_that_disappeared`
5. `source_write_invalidates_every_rollup_for_its_date`
6. `write_during_build_prevents_coverage_publication`
7. `zero_row_build_removes_old_files_and_publishes_coverage`
8. `restart_without_memory_coverage_falls_back_to_raw`
9. `binary_rollup_schema_round_trips_through_delta`
10. `schema_driven_batch_conversion_preserves_binary_and_nulls`
11. `failed_rollup_retries_without_a_source_commit`
12. `rollup_build_does_not_require_legacy_sweep_fallback`
13. `rollup_build_query_never_routes_to_its_target`

### Matcher unit tests

Use table-driven cases for:

- exact and missing project filters.
- prepared literal casts.
- every timestamp comparison and `BETWEEN`.
- range overflow and inversion.
- bucket finer than grain.
- bucket not divisible by grain.
- unbucketed aggregate acceptance.
- declared and undeclared dimensions.
- dimension row predicates.
- absorbed mixed `kind OR name` predicates.
- a missing prefiltered measure.
- a missing row-existence count.
- combined row and aggregate filters.
- the distinct Traffic and Error Rate server predicates.
- exact and nonmatching aggregate filters.
- accepted and rejected `column IS NOT NULL` predicates.
- grouped all-null input removal.
- `COUNT(*)` versus `COUNT(column)`.
- `AVG` with nullable input.
- t-digest recognition under `approx_percentile`.
- exact percentile rejection.
- distinct rejection.
- multiple candidate selection.
- deterministic miss reason selection.

### Target widget tests

Run the captured generated SQL for the three named widgets:

- `Traffic` routes through `server_request_count`.
- `P95 Latency` routes through `server_duration_digest` and
  `server_duration_count`.
- `Error Rate` routes through `server_error_count` and
  `server_error_scope_count`.
- `Apdex Score` records a stable miss and uses raw data.

Compare each routed result with the same SQL under the read-off configuration.

### Rewrite tests

Inspect logical and physical plans for:

- full rollup scan.
- leading raw fringe.
- trailing raw fringe.
- uncovered current-date tail.
- coarse bucket crossing the branch boundary.
- an empty covered date.
- generation predicates for several dates.
- unchanged output schema.
- fallback after the source epoch changes during physical planning.
- fallback after a target generation changes during physical planning.

Assert half-open predicates exactly. Do not accept a plan test that matches only
a table-name substring.

### Exact parity matrix

For `count`, `sum`, `min`, `max`, and `avg`, compare routing on and off across:

- null and non-null dimensions.
- null durations.
- no matching rows.
- one row.
- several services and kinds.
- aggregate filters.
- 1-minute, 1-hour, and unbucketed groups.
- aligned and unaligned bounds.
- strict and inclusive bounds.
- a midnight UTC boundary.
- a current-date raw tail.
- duplicate source versions before certification.

Compare Arrow values and nulls, not formatted pgwire strings.

### Percentile tests

Test:

- digest serialization compatibility.
- accepted quantile ranges over several merge orders.
- null and empty input.
- repeated compression.
- raw and rollup p50, p95, and p99 fixture ranges.
- raw-edge plus rollup-core digest merging.

Do not assert exact floating-point equality for independently merged digests.

### Pgwire and plan-cache tests

Run the same prepared statement with:

- different projects.
- different timestamp bounds.
- aligned and unaligned bounds.
- coverage present and absent.
- coverage invalidated between executions.

Assert that each execution uses its current values and coverage.

### End-to-end test

Add one MinIO test that follows the production path:

1. insert duplicate source rows.
2. flush.
3. certify and build the rollup.
4. insert a live-tail row.
5. execute the same pgwire SQL with reads off and on.
6. compare results.
7. inspect counters or the physical plan for one hybrid hit.

### Verification commands

Use targeted checks while implementing:

```bash
cargo nextest run --lib rollup
cargo nextest run rollup
cargo check --lib
```

Run the final gate before rollout:

```bash
cargo lint
make test
make test-e2e
```

Never use `cargo test`.

---

## Rollout

### Step 1. Deploy code with all flags false

Confirm:

- no new table writes.
- no routing attempts.
- raw query plans and latency stay unchanged.

### Step 2. Enable build only

Set:

```text
TIMEFUSION_ROLLUP_ENABLED=true
TIMEFUSION_ROLLUP_READ_ENABLED=false
TIMEFUSION_ROLLUP_BUILD_PROJECTS=<canary-projects>
```

Confirm successful generations and coverage markers. Confirm that failed builds
have no markers.

### Step 3. Backfill the canary window

Temporarily increase `TIMEFUSION_DEDUP_LOOKBACK_DAYS` for the widest target
window plus one day. Restore its normal value after coverage is complete.

Run raw-versus-rollup parity queries outside the request path. Store the exact
SQL, time range, result delta, and physical plan in the rollout report.

### Step 4. Enable full-coverage reads for one project

Set `TIMEFUSION_ROLLUP_READ_PROJECTS` to the canary project. Enable reads but
keep real-time tail routing off.

Confirm:

- accepted plans scan only the new generated table.
- `rollup_hits{mode="full"}` increases.
- miss reasons match expected query gaps.
- exact measures have no mismatch.
- percentile delta stays inside the recorded tolerance.

### Step 5. Enable the real-time tail

Enable `TIMEFUSION_ROLLUP_REALTIME_TAIL` for the same project.

Confirm that a seven-day query scans historical rollup dates and only the
required raw tail. Test at least one unaligned bound.

### Step 6. Expand projects

Expand the build selector first. After coverage is complete, expand the read
selector for the same projects. Compare:

- cold and warm panel latency.
- bytes and files read.
- hit rate by mode.
- miss reason distribution.
- build failure and stale-build rates.
- query errors and memory pressure.

### Step 7. Make reads the normal path

Remove both project selectors after the canary remains clean for the agreed
period. Keep all kill switches.

Stop and disable reads immediately on:

- any exact-measure mismatch.
- any tenant-routing defect.
- any missing live-tail row.
- repeated internal planning errors.
- percentile delta beyond the accepted tolerance.

Disabling reads is sufficient for a read incident. Disabling the master gate
also stops build cost.

---

## Work breakdown

| Change | Main files | Confirm |
|---|---|---|
| Flags and project selectors | `config.rs`, `.env.example` | defaults keep reads and builds off |
| Binary and spec validation | `schema_loader.rs` | invalid specs fail at registry load |
| Digest merger | `functions.rs` | digest merge unit and fixture tests |
| Atomic build generation and generic batches | `rollup.rs`, `database.rs` | stale groups and active files disappear |
| Source epochs and invalidation | `database.rs`, `dml.rs`, buffered write paths | concurrent write regression |
| Coverage classification | `database.rs`, `rollup.rs` | marker and fingerprint matrix |
| Typed matcher | `rollup.rs` | table-driven semantic cases |
| Late physical routing | `dml.rs`, `rollup.rs` | prepared-plan coverage changes |
| Hybrid state rewrite | `rollup.rs` | exact parity matrix |
| Metrics | `metrics.rs`, `rollup.rs` | one outcome per candidate aggregate |
| Bounded backfill | existing maintenance entry point | resumable seven-day build |
| Hierarchical grains | `schema_loader.rs`, `rollup.rs`, `database.rs` | 1m to 1h parity |

Keep the first implementation in existing modules. If the matcher makes
`rollup.rs` difficult to review, create a new module.

---

## Explicit non-goals for the first routed release

- exact percentiles.
- `COUNT(DISTINCT ...)`.
- arbitrary expression equivalence.
- joins and nested aggregates.
- arbitrary `date_bin` origins.
- a general materialized-view framework.
- automatic rollup deletion or retention.
- cross-source rollups.
- `otel_metrics` rollups.
- routing every current Overview panel.
- persisted coverage across restart.

Persisted coverage is a later optimization. Raw fallback after restart is the
safe initial behavior.

---

## Decisions recorded by this revision

1. Route in `DmlQueryPlanner`, after parameter substitution.
2. Keep the plan cache shape-only.
3. Use a separate coverage map, not `dedup_clean_fp`.
4. Replace each target partition atomically and select one build generation.
5. Retry missing coverage without a new source commit.
6. Match target row predicates through explicit prefiltered measures.
7. Add raw edge fringes before real-time routing.
8. Treat unbucketed aggregates as eligible.
9. Represent `avg` as sum plus non-null count.
10. Expose a SQL t-digest merger.
11. Version the generated table because its stored schema changes.
12. Reuse bounded dedup lookback for the initial backfill.
13. Delay hierarchical grains until direct one-minute routing is correct.
14. Record metrics only for the final planner decision.
