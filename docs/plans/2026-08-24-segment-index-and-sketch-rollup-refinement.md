# Segment index + sketch rollup: the two-tier proposal, refined against the tree

2026-08-24, re-reviewed 2026-08-25 against master `bf3abc8`. Owner:
anthony@talstack.com. Status: **design review — decision document, no code
yet.** Completion condition: the staging order at the bottom is either adopted
(each stage becomes its own plan) or rejected with a reason.
Last review: 2026-08-25.

**What the 08-25 re-review changed.** The 08-25 batch on master moved four of
this document's premises, all in its favor:

- The coverage-gauge `.min()` fold — this plan's stage-1 gate — **shipped**
  (`fold_fleet_gauge` in `database/maintain.rs`: a tier younger than the horizon
  abstains). One uncovered case remains per the YAML note: a long-lived tier
  rebuilt from scratch under the same name still pins.
- The measure-addition trap this plan only gestured at is now **measured and
  closed**: `duration_digest` was declared 08-22 but not materialized until
  08-24, and `tdigest_merge` skips NULLs, so a 7d p95 silently reported 23% of
  its window as the whole (`2026-08-25-maintenance-throughput-and-rollup-reads.md`
  §P1b). The fix — `TAG_MEASURES` per-cell materialization records +
  `MissReason::MeasureNotStored` refusal (`876f784`) — is exactly the gate
  stage 2 needed: a new measure is now refused until its cells rebuild, never
  silently biased.
- Rollup routing was **dead in prod** (0 hits) until 08-25: CSE hoisting of
  monoscope's `COALESCE(dim::text,'null')` made the matcher decline silently,
  and `count(*)` under a null guard was refused. Both fixed (`6a401dc`,
  `e67a149`; 39.2s → 5.10s measured). Consequence for this plan: every routing
  claim below now actually pays, and the miss telemetry is finally meaningful.
- With routing alive, the 08-25 sweep produced the first honest miss ranking:
  **non-dimension filters are the dominant steady loss** (~120/hr
  `unknown_filter`; A/B: `kind='server'` routes in 6.11s, `name='GET'` scans
  19.8s). That is direct evidence for the per-value tier in §3c — and it also
  found `server_error_count` counting outbound client spans as server errors
  (24,030 of 24,054), which joins stage 2's batch as a filter fix.

This refines an external two-table proposal — an exact per-file bitmap sidecar
("segment index") plus a sketch-carrying rollup tier with `field_summaries` —
against what actually exists in this repository. The proposal was written as if
for a greenfield stack. Most of its skeleton already exists here, some of it is
already deployed and verified, and three of its load-bearing assumptions are
broken by merge-on-read. What survives is narrower and more valuable than the
original: **two declared-measure gaps, one genuinely new table, and a retention
invariant nobody has written down yet.**

## 1. What the proposal reinvents (already in the tree)

| Proposal element | Existing implementation |
|---|---|
| Coarse-dim rollup keyed (bucket, kind, service, …) | `RollupSpec` in `schemas/otel_logs_and_spans.yaml:414` — dims `[resource___service___name, kind, status_code]`, exactly the "3–5 lowest-cardinality, most-filtered" rule |
| "Every aggregate must be a commutative monoid" | Enforced at declaration: only decomposable aggs are expressible (`src/schema.rs:61-65`); `avg` expanded to sum/count; exact percentiles refused |
| Distribution sketch column | `agg: tdigest` measures, stored `Binary`, `percentile_agg`/`tdigest_merge` UDAFs (`src/read/functions.rs:1101`) |
| Cardinality sketch machinery | `hll_agg`/`hll_merge`/`hll_count` UDAFs + vendored HLL (`src/read/functions.rs:1318`, `src/read/mod.rs:3368`) |
| Tiering by merge, not recomputation | `derive_from` (`src/schema.rs:48`): the 1h tier folds 1m states, never rescans raw |
| Planner picks per time range, stitches tiers | `route_for`/`rollup_rewrite_for`: hybrid rollup-interior + raw-fringe UNION with per-slice coverage, miss telemetry (`src/rollup.rs`, `src/database/mod.rs:3800-4102`) |
| Per-file sidecar, resident cache, rebuilt on compaction | Bloom sidecars, deployed 2026-08-22 (`2026-08-22-file-level-needle-pruning.md`, `src/read/bloom_prune.rs`): blob per (project, date), resident registry, single-flight loads, cron reconcile, carry-forward on compaction |
| Per-file value index over attributes | Tantivy per-parquet indexes with `flatten: kv` on `attributes` (`path:value` terms), manifest keyed 1:1 to the parquet tree, zero-hit file exclusion AND row-ordinal pushdown (`src/tantivy/`, `src/database/mod.rs:8226-8262`) |
| "Answer counts without deserializing" | Logical count index: exact `COUNT(*)` per (project, date) under MOR, snapshot-fingerprint-bound (`src/read/mod.rs:2295-2420`) |

So the honest framing is not "build two tables" but "what does the proposal add
to a system that already has both tiers?" Answer: HLL **measures** (the
machinery exists, no spec declares one), a facet/top-K tier (nothing serves it),
theta-style joint estimation (nothing serves it, nothing needs it until raw
expires), and a per-value bitmap sidecar (mostly dominated by what shipped —
see §4).

## 2. Three MOR breakages in the proposal as written

`otel_logs_and_spans` is merge-on-read: `version_append: true`, dedup keys
`(timestamp, id)` with tiebreak `updated_at`, tombstone `deleted`. UPDATE
appends a full new version at the row's ORIGINAL timestamp; DELETE appends a
tombstone copy. Every one of these breaks a piece of the proposal:

**2a. `row_count` popcount is not a count.** A file's rows include superseded
versions and tombstones, so a bitmap's popcount over one file answers "how many
physical rows", not "how many events". The sidecar's headline claim — "answer
counts without deserializing" — only holds for files proven unique within the
dedup key. That property is now actively being designed, but not by sidecars:
`2026-08-25-partial-certification-design.md` recommends per-file certification
grants from the clean-slice accumulator (candidate 2), with compaction-emitted
uniqueness (candidate 3) as the end state — the same machinery whose read side
(`skippable_certified_files` + the §3a overlap rule) already exists behind
`timefusion_read_dedup_skip_per_file`. If per-file bitmap counts ever become
sound, it is as a *consumer* of those grants, not a parallel proof system.
Until then, exact counts live where they already live: the logical count index
(dedup-aware by construction) and the rollup tier (built after `SliceDedup`).
A per-file index may **prune**; it may not **answer**.

**2b. Ingest-path rollup building double-counts.** The proposal builds rollup
rows in the ingest micro-batch, buffered per bucket, with a grace window and
read-merge-write union for late data. Here that is wrong twice over:

- A version re-append lands in a *new* flush but carries the *original*
  timestamp, so "union the late bucket into the row" recounts the same event.
  Sketch-union is only recount-free when the inserted identity is stable
  across versions — counts and top-K exact counters are not.
- The design already rejected this seam deliberately: rollups are built only
  after the source is duplicate-free (`src/rollup.rs:1-6`), buffered rows are
  excluded from every rollup partition and served raw
  (`src/database/mod.rs:3877-3881`, invariant test `mem_buffer.rs:4197`), and
  the write path's only rollup duty is invalidation with an hour mask
  (`src/database/write.rs:594` → `maintain.rs:2576`).
- Operationally: prod restarts every 20–40 minutes. In-memory rollup
  accumulators would be lost constantly; the invalidate-then-rebuild design is
  restart-proof by construction (journal loads-as-empty semantics,
  `src/rollup_journal.rs:1-5`).

The refinement worth keeping from the proposal's freshness instinct: the flush
path knows each bucket's exact `(min, max)` timestamps, and slice coverage is
already keyed by micros ranges — invalidation could enqueue 5-minute slices
instead of hours. That is a scheduling-precision win inside the existing
architecture, not a new build path.

**2c. Per-value sketches drift under UPDATE.** Theta/HLL keyed by a synthetic
row id are duplicate-tolerant only if the id is stable across versions — and it
is, but only if the id is the dedup key: `hash(timestamp, id)`, not the
proposal's `hash(trace_id + span_id + timestamp)` (logs have no span ids;
`id` is the identity column, client-supplied, non-null). Two residual errors
remain even then: a version that *changes* a field's value leaves the row id in
the old value's sketch forever (unions cannot subtract), and tombstoned rows
stay counted. For OTel data (updates rare, deletes rarer) this is acceptable
approximation error — but it must be stated, because it means per-value sketches
can never be promoted to "exact" even on certified data.

## 3. Table 2 refined: close the declared-measure gaps before inventing columns

The proposal's `rollup_1m` schema is ~80% a re-derivation of `dashboard_1m_v3`.
What it adds, ranked against the measured query shapes
(`docs/monoscope-query-shapes.md`) and the routing-miss evidence
(`2026-08-22-make-14d-30d-complete.md` §Tier-3):

**3a. HLL measures — the single biggest gap, and it is a YAML edit.**
`dcount` (§5 of the query shapes: distinct traces, users, services, names)
can never route because no spec declares an `hll` measure, even though
`synthesize` handles `agg: hll` and the UDAF triple exists. Declare
`trace_hll: { agg: hll, column: context___trace_id }` and
`user_hll: { agg: hll, column: attributes___user___id }` on both tiers.
Honesty rule for free: HLL answers only queries that themselves asked for
`approx_count_distinct`, so no "~" response-metadata channel is needed — the
approximation was requested in the SQL. As of 08-25 this is **de-risked**:
`TAG_MEASURES` + `MeasureNotStored` mean the new measure is refused on
old cells rather than silently NULL-merged (the P1b failure class). Cost:
the 30-day rebuild (wall-clock physics) — but a measure edit changes the
generation id, not the tier count, so it does not hit the young-tier trap.

**3b. One batched spec generation bump, not measure-by-measure.** Every
measure or filter change forces the same 30-day rebuild wave, so batch the
known set into a single `dashboard_1m_v4`:
- the `hll` measures (3a);
- `status_class` count measures — already named in the YAML's own post-mortem
  (`otel_logs_and_spans.yaml`) as "the one actually worth having";
- the `server_error_count` filter fix — the 08-25 sweep proved its
  `name='monoscope.http'` OR-arm pulls outbound `kind='client'` spans into a
  tenant's *server* error count (24,030 of 24,054 wrong), so a third-party
  outage renders as the tenant's own errors. This is user-visible wrong data,
  not a routing miss, and it is the same YAML edit + rebuild;
- the `COUNT(*) FILTER (WHERE status_code='ERROR')` near-miss (exact-filter
  measures so monoscope's narrower error predicate matches);
- `kind IN ('server','client')` coverage if the shape still misses after the
  sampler fix ranks it.
One rebuild wave, one refusal window under `MeasureNotStored`, one generation.

**3c. The per-value tier — the genuinely new table, now with measured
demand.** Three query families point at the same missing structure:

- §6 top-K tables (by `name`/`url_path` with counts, error rates, p95 per key)
  and §7 facets (top-50 values per field, one query per field) — the largest
  unserved shapes in `monoscope-query-shapes.md`;
- and, new on 08-25: **filters on non-dimension flat columns are the dominant
  steady routing loss** — ~120/hr `unknown_filter` from ambient traffic, with
  a measured A/B of 19.8s (scan, `name='GET'`) vs 6.11s (routed,
  `kind='server'`). These are the "endpoint-hash-scoped status-class charts"
  the latency matrix already blamed.

The proposal's `field_summaries.top_values` is the right idea in the wrong
encoding. An opaque per-row blob map cannot be routed to by the SQL matcher,
cannot be partially read, and freezes its own schema. Encode it relationally
instead:

```
otel_logs_and_spans_facets_1h:
  project_id, date          -- partition, same as every tier
  bucket_start TIMESTAMP    -- 1h grain; no 1m facet tier (nobody facets a 5-min window at scale)
  field        TEXT         -- declared list only: name, attributes___url___path,
                            --   attributes___http___request___method, level,
                            --   attributes___db___system___name, resource, …
  value        TEXT
  row_count    BIGINT       -- SpaceSaving state: count_lo
  err_overcount BIGINT      -- SpaceSaving error bound, so merges stay honest
  error_count  BIGINT
  duration_digest BYTES     -- tdigest, per (field, value) — serves the top-K table's p95 column
```

One row per (bucket, field, value), where "value" is: **all** values for a
field whose per-(project, bucket) cardinality stays under a cutoff (~1k), or
the SpaceSaving top-≤50 with `err_overcount` when it does not. The cutoff is
what lets the same table serve *filters*, not just facet listings: a chart
filtered `name = 'GET /users'` routes to `field='name' AND value='GET /users'`
**exactly** when the field stayed under cutoff for every bucket in the window
(absence is then a true zero), and is refused when any bucket overflowed —
the same fail-closed shape as `MeasureNotStored`. That is what converts the
~120/hr `unknown_filter` misses; top-K alone cannot, because absence from a
top-K is not zero. Row count stays bounded by
`buckets × |declared fields| × cutoff` with no cardinality product across
fields — the proposal's own key trick, kept. §7 facets are the physical
layout read back verbatim; §6 top-K is served with widened SpaceSaving
bounds, refused when the query's LIMIT exceeds what the bound can rank.

It rides the existing machinery: built by the coordinator after dedup like
any tier, covered by slice coverage, `TAG_MEASURES`-stamped, routed by a
matcher extension for the three shapes. Two scope guards from the 08-25
measurements: target **flat columns only** — the same sweep ruled out routing
for the Variant JSON-path group-by family (the cost there is struct
serialization, since fixed 4.2x by the native `variant_get` peephole, not
aggregation); and **rank the declared field list from the fixed miss sampler
before building** — today `first_miss` reports the coarsest spec's reason and
the sampler is 1-in-512 and blind to `UnwalkableSource`, so any field ranking
made now is guesswork (`maintenance-throughput-and-rollup-reads.md`,
"Instrument gap").

What is deliberately NOT taken from `field_summaries`: per-field
`presence_count`/`distinct_hll` (fold into 3a/3c as ordinary measures where a
shape actually asks), and the whole `theta:` branch — see 3e.

**3d. Sketch choices are already settled — do not churn them.** The proposal
says DDSketch; the tree says tdigest (`tdigests` crate), with stored digests in
prod and an explicit warning that changing sketch parameters requires a new
rollup table (`src/read/mod.rs:3387-3392`). Same for HLL. Relative-error
guarantees are not worth orphaning every stored state. Postcard-with-version-
byte for evolvability is likewise moot: measures are typed columns synthesized
from YAML, and a spec change already forces a new generation/table name by
construction (`generation_id` hashes the spec, `src/rollup.rs:69`).

**3e. Theta / joint cross-field estimation: defer until it has a customer.**
Its stated purpose — arbitrary AND-combinations after raw data is gone — has no
trigger today because **nothing expires raw data** (see §5). No measured miss
in the latency matrix is joint-estimate-shaped. And it is the piece with the
worst dependency story (no maintained Rust theta crate; binding
`datasketches-cpp` into a 1200-dep build for a speculative feature). If it is
ever built: vendor a bottom-k/KMV sketch in the same style as the vendored HLL,
key it by `hash(timestamp, id)` per §2c, and accept the update-drift caveat.
Until a retention policy exists, this is a solution ahead of its problem.

## 4. Table 1 refined: do not build the bitmap sidecar as specced

The proposal's segment index re-derives, feature by feature, what the last two
weeks already built — and each delta it adds is either broken here or already
served:

- **Exact per-value membership per file** → parquet blooms + the deployed bloom
  sidecar give this at FPP 0.01 for the seven needle columns, resident,
  version-free-safe, verified in prod (284 files → 24). A roaring bitmap's
  advantage over a bloom for *pruning* is the last 1% of false positives.
- **Row-level positions** → tantivy already pushes per-file row ordinals
  (`ordinals_valid`, row-selection pushdown), and the channel is generic.
- **Exact counts from popcount** → broken under MOR (§2a).
- **`file_version BIGINT` staleness check** → the deployed design proved the
  better model for prune-only artifacts is *no* version binding: Delta never
  reuses paths, unknown ⇒ included, stale rejection ⇒ no-op
  (`src/read/bloom_prune.rs:9-13`). Version-bound validation is the rollup
  ticket's model, needed only when an artifact *answers*.
- **"Store it as a Delta table so DataFusion pushdown does the lookup"** → this
  is the plan-time-IO trap with a second trap attached. The pruning decision
  runs inside `scan()` at file-selection time and must never await IO (the
  tantivy prefilter S3-at-plan-time incident is the cautionary tale, and the
  registry design is its fix). A Delta-backed index would additionally pay
  commit OCC + log churn + checkpoint listing on every compaction wave that
  rewrites its keyed files — on a box that restarts every 20–40 minutes.
  Blob + resident registry won this argument empirically; keep it.

What survives from Table 1 is its one real observation: **Variant/attributes
predicates have zero file-level pruning today** (`supports_filters_pushdown`
marks them Unsupported, `src/database/mod.rs:9756-9772`; the only accelerator
is tantivy's ngram3 kv index, which is heavy and lags flushes). The cheap
staircase, in order, each step measured before the next:

1. **Widen bloom coverage**: `bloom_filter: true` on more flattened
   low-to-mid-cardinality attribute columns is a YAML edit; the writer opts in
   per column and the sidecar builder picks them up automatically. Equality
   needle extraction already handles them.
2. **Per-file field-presence + small-dictionary sidecar** (the proposal's
   `value = NULL` meta-row idea, correctly scoped): per (file, declared field)
   store `present: bool`, `distinct_count`, and — iff under a cutoff (~64) —
   the exact value dictionary. Prune `field = 'v'` when the field is absent or
   the dictionary provably lacks `'v'`. Prune-only, blob + registry, built in
   `reindex_wave_outputs` (`maintain.rs:5907` — inputs and outputs explicit,
   no `sole_commit` caveat) plus the flush callback seam beside
   `TantivyIndexCallback` (`src/write/mod.rs:2707`), keyed 1:1 to the parquet
   tree like tantivy (list-diff reconcile) rather than per-date like blooms.
   Restricted to columns not in `version_mutable_columns()`, same as blooms.
3. **Roaring bitmaps only if step 2's dictionaries measurably leave row-group
   pruning on the table**, and then as value → row-ordinal selections feeding
   the existing row-selection channel — never as a count source (§2a stands
   until overlap-scoped dedup emits per-file uniqueness, at which point
   popcounts on certified-unique files become sound and this decision can be
   revisited).

Also inherited by any new sidecar, from the deployed one's scar tissue:
inclusion-on-unknown; per-file payload cap with a `no_index` marker; the
reconcile GET-storm fingerprint fix (recorded fast-follow in the needle-pruning
plan); exclusion applied by filtering the live-file universe before leg
partitioning, with the all-files-rejected empty-include case handled.

One more 08-25 data point that deprioritizes this whole section: the worst
pruning failure in the matrix is not value pruning at all —
`2026-08-25-log-list-30d-is-a-pruning-problem.md` shows a 251-row newest-first
`LIMIT` query selecting 431 GB / 1,404 files because file selection never
consults the limit, and 17 of 25 matrix failures are plain statement timeouts.
Limit-aware newest-first file selection is a bigger, cheaper pruning win than
any value sidecar, and it is already on the tree's own list.

## 5. Tiering ladder and retention: the invariant the proposal gets backwards

The proposal's `1m→48h / 1h→60d / 1d→2y` ladder assumes retention exists.
It does not: nothing in the tree deletes user rows on age — vacuum
(`timefusion_vacuum_retention_hours = 72`) removes *unreferenced files*, not
old data. Two consequences:

- The "sketches are the only thing between you and unanswerable questions once
  raw is gone" motivation is prospective. It becomes real the day a retention
  policy ships, so the tiers must be in place *before* that day — that is the
  actual deadline, and it is currently unset.
- **Raw expiry breaks today's coverage model.** The staleness witness is strict
  equality between each slice's recorded `source_rows` and the raw partition's
  current `num_records` (`2026-08-22-rollup-correctness-and-routing.md` §1).
  Deleting an expired raw partition would invalidate every coverage witness
  over it and drop those dates to a raw scan of data that no longer exists.
  Retention therefore needs a new terminal coverage state — sealed-and-frozen:
  a date whose raw is past retention keeps its last-certified coverage
  permanently and is exempt from the witness. The coverage ledger
  (`2026-08-24-open-work-after-untagged-convergence.md` §3, reads still behind
  `TIMEFUSION_COVERAGE_LEDGER_READS`) is the right home for that state; file
  tags are not. **A rollup tier must reach certified coverage of a date before
  that date's raw may be vacuumed** — write that as the retention feature's
  first invariant. Timing note from 08-25: the witness is already being
  redesigned for its own reasons — false invalidation is now the #1 coverage
  blocker (P3b), and the sound replacement is a builder-side change (logical
  count or carried-forward digest). Design the frozen terminal state into that
  redesign rather than retrofitting it later; it is one requirements bullet
  now and a second migration if missed.

The 1d tier itself is currently inexpressible — `derive_from` may not chain
(`src/schema.rs:130`), and the coordinator's `Operation` enum, dependency
gating, and cycle weights assume exactly two levels. Lifting that is
mechanical but real work. And before ANY new tier (1d, facets): fix the
`.min()` fold in `rollup_min_contiguous_days` — the withdrawn `level` tier
proved a young tier pins the whole fleet into coverage-short mode
(`otel_logs_and_spans.yaml:461-483`). That fold fix is the gate for half of
this document.

## 6. Staging order (revised 2026-08-25 against master `bf3abc8`)

Each stage is independently shippable and measured by the existing matrix
(`bench/local/query_matrix.py`) and `timefusion_stats` counters — never
`EXPLAIN`, which does not show routing. Stages this plan listed on 08-24 that
master has since closed: the coverage-gauge fold fix (`fold_fleet_gauge`),
the measure-addition safety gate (`TAG_MEASURES` + `MeasureNotStored`), and —
outside this plan but load-bearing for it — the routing fixes that took
`rollup_hits` from 0 to real (`6a401dc`, `e67a149`). What remains, in value
order, deferring to `2026-08-25-maintenance-throughput-and-rollup-reads.md`
"Order of execution" for the open defects (P1b holding clause, P3b witness,
resume, starved cliff) that outrank all of it:

1. **Fix the miss-reason instruments first** (cheap, unblocks ranking): the
   `first_miss` coarsest-spec masking, the 1-in-512 sampler, and its
   `UnwalkableSource` blindness. Every field/shape choice in stages 2–3 is
   guesswork until this lands — the tree's own item 7.
2. **One batched spec bump `dashboard_1m_v4`/`1h_v4`** (§3a/§3b): `hll`
   measures (dcount routes for the first time), `status_class` counts, the
   `server_error_count` client-span filter fix (user-visible wrong data
   today), exact-filter error measures. One 30-day rebuild wave, refusal-safe
   under `MeasureNotStored`.
3. **Per-value tier `facets_1h`** (§3c) + matcher support: converts the
   measured ~120/hr `unknown_filter` steady loss plus shapes §6/§7. The
   genuinely new table this proposal contributes. Field list ranked from
   stage 1's sampler output; flat columns only.
4. **Land the coverage ledger reads** (flag exists, gated on zero
   disagreements) and tag-aware tier compaction — makes wide-window tier
   reads cheap; prerequisite for tier growth and retention.
5. **Retention policy** with the certified-before-vacuum invariant and the
   frozen-coverage terminal state (§5) — folded into the P3b witness redesign
   that is happening anyway. This, not sketches, is the prerequisite to
   "queries outlive raw".
6. **1d tier**: lift the `derive_from` chain ban; only worth it once 4 makes
   tier files compactable and 5 sets a retention deadline.
7. **Attribute pruning staircase** (§4): wider blooms → presence/dictionary
   sidecar → bitmaps only on measured need. Further deprioritized on 08-25:
   the dominant matrix failures are statement timeouts and limit-blind file
   selection, and the Variant read cost fell 4.2x via the `variant_get`
   peephole — none of which a value sidecar touches.
8. **Theta/KMV joint estimation**: only after 5 ships and only if a real
   query shape demands cross-field estimates on expired ranges (§3e).

The proposal's ordering ("coarse rollup first, theta second, sidecar third")
collapses under the facts that stage "one" shipped weeks ago and its theta
stage has no consumer. What remains that actually improves the system is
stages 2, 3, and 5 — a YAML-and-rebuild batch, one new table aimed at the
largest measured miss class, and a retention invariant written before it is
needed. None of them require a new storage engine, a new sketch library, or a
second index architecture.
