# Rollups that are actually used

**Status:** the BUILD side is shipped and config-driven. The READ side does not
exist — `rollup::route` is called only from its own unit tests, so no query has
ever been served from a rollup and `rollup_hits` cannot leave zero. Today the
feature costs one aggregate-and-write per certified partition and returns
nothing.

**Goal:** a dashboard panel is answered from the rollup, by default, without the
panel being rewritten — including the current minute, including percentiles,
and at whatever grain the panel asked for.

---

## What already exists (do not rebuild)

| piece | where | what it does |
|---|---|---|
| `RollupSpec` / `RollupMeasure` | `schema_loader.rs` | `rollups:` block on the SOURCE table: `grain`, optional `name`, `dimensions`, `measures`. |
| `RollupSpec::synthesize` | `schema_loader.rs` | Derives the rollup's whole `TableSchema` from the spec + source schema. Measure types come from the source column, so they cannot drift. |
| registry synthesis | `schema_loader.rs` (`SchemaRegistry::new`) | Registers `{source}_rollup_{name\|grain}` at load. Same-grain collisions panic with a message naming the fix. |
| `build_partition_sql` / `to_rollup_batches` | `rollup.rs` | Spec-driven SQL + row shaping. `bucket_id` is deterministic in (bucket, dims), which is what makes a rebuild idempotent. |
| build trigger | `database.rs` (`rebuild_rollup_partition`) | Fires for every rollup declared on whichever table was just CERTIFIED duplicate-free. |
| `route` + `MissReason` | `rollup.rs` | Answerability predicate: dimensions cover group-by AND filters, aggregates are decomposable, requested bucket is a whole multiple of the grain. **Complete, and unused.** |
| `rollup_hits` / `rollup_misses{reason}` | `metrics.rs` | Already wired into `route`. |
| `dedup_skip_allowed` | `database.rs` | Plan-time check that every `(project, date)` partition in a window carries a still-matching clean fingerprint. **This is the coverage signal**, already consulted during planning. |
| `percentile_agg` UDAF | `functions.rs` | Builds a t-digest, returns `DataType::Binary`, and `merge_batch` merges serialized digests. |
| `approx_percentile(p, digest)` | `functions.rs` | Reads a quantile out of a Binary digest. |
| analyzer/optimizer hook | `database.rs:3322-3351` | Eight rules already registered; a rollup rule slots in beside them. |

**Correction to the 2026-08-09 plan.** It records percentiles as blocked on
"a mergeable t-digest column". They are not: `percentile_agg` already returns a
serialized digest as `Binary` and already merges digests in `merge_batch`, and
`approx_percentile` already reads one. Percentiles are a *measure kind*, not a
research project — see §3.

---

## The invariant everything hangs off

**A rollup bucket is only valid for a partition that was certified
duplicate-free**, because a bucket computed over a bin that is later deduped is
simply wrong. The build already respects this (certification IS the trigger).
The read path must respect the same boundary, from the same state, or it will
be wrong in one of two ways:

* route a window that includes an uncertified partition ⇒ **that partition's
  rows silently vanish** from the answer;
* count a partition on both the rollup leg and the raw leg ⇒ **double count**.

Both are fast and plausible, which is what makes them dangerous. The boundary
must come from `dedup_skip_allowed`'s fingerprints — the same map
(`dedup_clean_fp`) the builder writes — and never from a timer, a watermark, or
"today's date".

---

## §1. Feature flag (do this first)

`TIMEFUSION_ROLLUP_ENABLED`, default **false**.

The rollup is currently the only write-path feature with no kill switch —
`light_optimize_enabled`, `repair_resume_enabled` and `read_dedup_skip_swept`
all have one. Today the only way to stop a misbehaving build is revert and
redeploy.

Two separate gates, because they fail differently:

```rust
timefusion_rollup_enabled: bool          // default false — build AND read
timefusion_rollup_read_enabled: bool     // default false — read only
```

Build-on/read-off is the canary state: let the table populate and compare it
against raw aggregates for a day before a single user query depends on it.
Read-off must be a pure no-op in the planner, not a rule that runs and declines.

**Done when:** flipping either flag off restores today's behaviour exactly, and
`rebuild_rollup_partition` returns before doing any work.

---

## §2. The rewrite rule — fully-certified windows only

New `src/optimizers/rollup_rewriter.rs`, registered as an **optimizer** rule
(not an analyzer rule: it needs types resolved and predicates pushed down).

### 2.1 Match

```
Aggregate { group_expr, aggr_expr }
  └── (Filter)?
        └── TableScan { table_name: <source>, filters, projection }
```

Bail on anything else — a Join, a Window, a Distinct, a nested Aggregate. The
rule must be conservative: not routing is always correct.

### 2.2 Build the `Ask`

* `group_by` — plain column refs in `group_expr`, plus the time-bucket
  expression's column.
* `bucket_micros` — from `time_bucket(<interval>, ts)` / `date_bin`. No time
  bucket at all ⇒ `None` ⇒ `route` refuses (a rollup cannot answer an
  un-bucketed aggregate without collapsing every bucket, which is a different
  question).
* `aggregates` — the function names in `aggr_expr`.
* **`filtered` — every column referenced by the `Filter` node AND by
  `TableScan::filters`.** This is the one that is easy to get wrong and
  catastrophic when wrong: rows for a non-dimension are already summed
  together and cannot be subtracted back out, so a filter on a non-dimension
  served from the rollup returns *unfiltered totals* — fast, plausible, wrong.
  Walk both places; a predicate pushed into the scan is invisible in the
  `Filter` node.

### 2.3 Decide

`route(spec, &ask)` per registered rollup of that source. Among those that
answer, pick the **coarsest grain that divides `bucket_micros`** — fewest rows
read. Record hit/miss; the reason breakdown is the feedback loop that tells you
which dimension to add.

### 2.4 Rewrite

Swap the scan to the rollup table and re-aggregate the stored measures:

| query asks | rollup answers |
|---|---|
| `count(*)` | `sum(request_count)` |
| `count(*) FILTER (p)` where `p` is the declared measure filter | `sum(error_count)` |
| `sum(x)` | `sum(x_sum)` |
| `min(x)` / `max(x)` | `min(x_min)` / `max(x_max)` |
| `avg(x)` | `sum(x_sum) / sum(request_count)` — expand BEFORE routing |
| `percentile(x, p)` | `approx_percentile(p, percentile_agg_merge(x_digest))` — §3 |

The group-by keeps the same time-bucket expression: re-bucketing 1m buckets
into 5m is just a coarser `time_bucket` over the stored `timestamp`, which is
why `route` insists the request is a whole multiple.

### 2.5 Coverage gate (this version)

Route only if `dedup_skip_allowed(table, project_id, window, dedup_keys)` is
true for the **whole** query window. That is conservative and correct.

It also means the rule will rarely fire in production, because nearly every
dashboard query includes *now*. That is expected and fine: this step exists to
prove the rewrite is correct in isolation, under a parity test, before §4 makes
it useful. Ship it behind the read flag and measure `rollup_misses{reason}`.

---

## §3. t-digest measures — percentiles, and why they change the economics

Latency panels are the reason dashboards are slow, and count/sum/min/max cannot
answer them. Without percentiles a rollup skips the expensive panel and
optimises the cheap ones.

TF already has every piece:

* `percentile_agg(Float64) -> Binary` builds a digest;
* its `merge_batch` merges two serialized digests — i.e. digests are
  **re-aggregable across buckets and across collapsed dimensions**, which is the
  exact property `DECOMPOSABLE` demands;
* `approx_percentile(p, Binary) -> Float64` reads a quantile out of one;
* `TDIGEST_MAX_CENTROIDS = 200` bounds the stored size.

### 3.1 Spec

```yaml
measures:
  - { name: duration_digest, agg: tdigest, column: duration }
```

`synthesize` maps `agg: tdigest` to a `Binary`, nullable column. `build_partition_sql`
emits `percentile_agg(CAST(duration AS DOUBLE)) AS duration_digest`.

### 3.2 Read

`route` gains `tdigest` to `DECOMPOSABLE`, and the rewrite maps
`percentile(x, p)` / `approx_percentile(p, …)` onto a **merge of the stored
digests**, then one `approx_percentile` on the merged result. Merging N digests
is the same operation `merge_batch` already performs, so no new numerics.

### 3.3 Encourage it, honestly

A t-digest is ~200 centroids ≈ a few KB per bucket per dimension tuple — far
larger than an i64 measure. That is the trade to state plainly in the schema
docs: **one digest column replaces the panel that forces a full raw scan**, and
it is the difference between a rollup that serves the cheap half of a dashboard
and one that serves all of it. But it also multiplies rollup size by the digest
size, so it belongs on the measures you actually chart (duration), not on
everything numeric.

`route` must keep refusing **exact** percentiles and **exact**
`count(distinct)`. Answering those approximately without being asked is a
correctness lie, not an optimisation. If a caller wants approximation it should
say so by calling `approx_percentile`.

---

## §4. Real-time tail

This is what makes the feature real, and the piece with the sharp edge.

Certification is per `(project, table, date)`, so the boundary is a **date**:

```
first_uncertified = min date in window whose fingerprint is missing/stale

rollup leg : date <  first_uncertified   -> rollup table, re-aggregated
raw leg    : date >= first_uncertified   -> source table, aggregated live
result     : Aggregate over Union(rollup_leg, raw_leg)
```

Both legs must produce the **same schema and the same bucket expression**, and
the final aggregate re-aggregates across them — `sum` of `sum`s, `min` of
`min`s, digest-merge of digests. This is exactly Timescale's real-time
aggregation, and the reason it is safe here is that the split key is the same
`(project, date)` key certification is recorded under: a partition is on exactly
one side by construction.

Three things to get right:

1. **Derive the boundary from `dedup_clean_fp`, once, at plan time**, and use
   the same value for both legs. Computing it twice invites a partition that
   certifies between the two calls appearing in both legs.
2. **Date-partition pruning must apply to both legs**, or the raw leg scans
   history and the whole exercise is pointless.
3. **The uncertified tail includes the MemBuffer and hot legs.** The raw leg is
   an ordinary source scan, so it already unions them — do not hand-roll it.

Gate on its own flag (`timefusion_rollup_realtime_tail`, default false) so the
fully-certified path from §2 can ship and bake first.

---

## §5. Hierarchical grains

1m → 1h → 1d. The merge is associative, so a coarser grain re-aggregates the
finer one and there is no second pipeline and no second correctness argument.

Two ways to get a 1h answer:

* **Derive at read time** from 1m (what §2.4 already does when the request is a
  whole multiple). Zero storage, reads 60x more rows than necessary.
* **Materialise 1h from 1m.** Cheaper reads for wide windows, and the natural
  home for long-retention panels.

Materialising needs one change the current design deliberately forbids: today
`synthesize` sets `rollups: vec![]` on the generated table, so a rollup cannot
declare rollups and the loader cannot recurse. Replace that with an explicit
`from:` on the spec:

```yaml
rollups:
  - { grain: 1m, dimensions: [...], measures: [...] }
  - { grain: 1h, from: 1m, dimensions: [...], measures: [...] }
```

Then:

* the 1h build reads the **1m rollup**, not the source — `count` becomes
  `sum(request_count)`, digests merge;
* the trigger fires 1h *after* 1m for the same partition, in `from` order,
  which a topological sort over `from` gives for free;
* load-time validation must reject a cycle and reject a `from` grain that does
  not divide this one.

Read-side selection is then "coarsest grain that divides the request", which
§2.3 already specifies — hierarchical grains need no separate routing logic.

---

## Tests

Unit (`rollup.rs`, `schema_loader.rs`):

* `route` accepts/refuses each `MissReason`, including a filter on a
  non-dimension (the dangerous one).
* Coarsest-grain selection picks 1h over 1m for a 1h request, and 1m for 5m.
* `tdigest` measure synthesizes a Binary column; `avg` expands to sum/count.
* `from:` cycle and non-dividing grain are rejected at load.

Integration (`tests/suite/`):

* **Parity, and it is the whole test suite in one line:** for a range of
  windows, grains, dimension subsets and filters, the routed plan and the raw
  aggregate return the SAME numbers. Percentiles compare within the digest's
  documented error, everything else exactly.
* Route with an uncertified partition in the window ⇒ §2 refuses; §4 splits and
  still matches the raw aggregate.
* A partition certified *between* the two legs' planning must not be
  double-counted (pin the single-boundary-read from §4.1).
* Re-certification rebuilds a bucket in place (idempotence) and the routed
  answer does not change.

E2E (`tests/e2e/`): a dashboard-shaped query — bucketed, filtered, grouped —
over a window spanning certified and uncertified dates returns the same rows
with routing on and off, and `rollup_hits` moves.

---

## Rollout

1. `TIMEFUSION_ROLLUP_ENABLED=true`, read off. Let the table build. Compare
   against raw aggregates for a day.
2. Read on for fully-certified windows. Watch `rollup_misses{reason}` — it is
   the dimension backlog, in priority order.
3. Add the `duration_digest` measure; confirm latency panels start routing.
4. Real-time tail on. This is the change that can be *wrong* rather than slow;
   canary it on one project and diff the panel against the raw query.
5. Materialise 1h/1d once the 1m table's read volume justifies it.

Kill switches at every step, and each step is independently revertible.

---

## Explicitly out of scope

* **Exact percentiles and exact `count(distinct)`** from a rollup. Refused, not
  approximated silently.
* **Rollups over `otel_metrics`.** Possible — the spec is generic now — but it
  needs its own dimension set and its own decomposability argument. The current
  rollup is span-shaped (`kind`, `status_code`, `duration`, an HTTP-status error
  predicate), which is why it is named after its source.
* **Backfill of historical buckets.** The build only fires forward, at
  certification. A backfill job is a separate plan; until then a rollup covers
  the window since it was declared, and §4's boundary keeps that honest.
* **Retention/vacuum for rollup tables.** They inherit the source's partitioning
  but nothing prunes them yet.
