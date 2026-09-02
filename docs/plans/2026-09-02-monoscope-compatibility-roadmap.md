# TimeFusion ← monoscope: a compatibility and capability roadmap

*Drafted 2026-09-02, from a full sweep of both trees. Not committed — a plan for
review, not a decision.*

## Why this document exists

TimeFusion's backlog — `tasks/01-NEXT-2026-08-25.md` plus the eighteen
superseded numbered files — contains **zero Postgres-compatibility or
client-capability items**. It is entirely maintenance, rollup, ingest and
read-latency work, all of it well-evidenced.

Meanwhile a sweep of monoscope, TimeFusion's only client, turns up **roughly
fifty places where the SQL we generate is contorted, split, capped, or moved
into Haskell because TimeFusion cannot do the natural Postgres thing.** Several
carry measured costs (107s vs 21s, 24.7s cold page loads, a 60-round-trip
billing loop). Several are load-bearing product decisions — a feature flag
defaulted off, a user-visible per-widget backend selector, an entire
materialised rollup table.

Neither list is wrong. They have simply never been joined. That is the actual
finding, and it is why the **first recommendation is structural, not a feature**.

"Postgres expectations" is read here in both senses, and both are covered:
wire/dialect fidelity for real clients (pgAdmin, DBeaver, psql, Hasql), and the
PG-shaped semantics monoscope's query-building code assumes.

### How to read the tiers

- **Tier 0** — wrong answers today.
- **Tier 1** — each item deletes a monoscope subsystem that exists only to work around it.
- **Tier 2** — wire fidelity; deletes dual-dialect code and string-matched error handling.
- **Tier 3** — rollup expressiveness; what the sessions page actually needs.
- **Tier 4** — write path.

Evidence is tagged **[measured]** (a number exists), **[incident]** (it caused a
production event), or **[unquantified]**.

---

## 0. CLOSED — verify, then clean up the client

Four gaps monoscope still carries workarounds for have already been fixed here.
Each stale workaround is itself evidence for the contract suite in §5.

| gap | TF state | client action |
|---|---|---|
| `UPDATE … FROM (unnest)` | Native via DmlQueryPlanner + MergeBuilder | Already consumed — `BackgroundJobs.hs:2710`. Nothing to do. |
| **Utf8View OR predicate returning wrong rows** | Fixed, `e0bf291`. Root cause was *not* Utf8View: the tantivy id-prefilter intersected per-term id sets, sound for AND, empty for OR → `id IN ([])`. Three regression layers. | **Re-benchmark, do not revert blind.** monoscope's comment (`ServiceGraph.hs:629`) also says `kind` is raw-indexed and `IN` routes through the index, so `IN` may still be the right spelling on speed alone. Annotate `docs/monoscope-query-shapes.md:249` as historical either way. |
| `COALESCE(hashes, '{}')` 500ing every TF lookup | `PgCoalesceUdf` + `PgArrayLiteralRewriter` (`optimizers.rs:1709`, `:1791`) exist for exactly this query, with PG divergences pinned by test | Verify against live TF, then delete the caveat at `Telemetry.hs:673`. |
| **Footer-declared sort order the data does not have** | **Fixed today, `5a445001`.** `SortingColumn.column_idx` indexes parquet *leaves*, not fields; a Variant column is several leaves, so every sort key after one under-shot. On a real prod file `resource___service___name` was recorded as leaf 76 — which is `attributes___user___email`. | See Tier 0.2 — **not fully closed on the read side.** |

---

## Tier 0 — correctness

### 0.1 `COUNT(*)` returns fewer rows than were acked *(already yours)*

`tasks/01-NEXT-2026-08-25.md` §10, promoted from flake to correctness bug;
reproduced 3 of 4 e2e runs on 2026-08-28. Count pushdown refuted as the cause;
live hypothesis is that the Delta leg reads a snapshot predating the flush
commit. Listed here only to connect it: monoscope bills on counts
(`getUsageTotals`), so an under-count is a revenue defect, not just a test
failure. **No new work proposed — it is correctly prioritised already.**

### 0.2 Historical files still advertise a false ordering **[incident]**

`5a445001`'s own commit message: *"Old files keep their wrong footers; the win
phases in as rewrites replace them."* The write side is fixed; the read side
still consumes lying footers on every file written before today.

This is very likely the root cause of the 2026-08-07 bounded-dedup under-count
that monoscope built a watchdog around: `bulkInsert`'s `checkReadConsistency`
runs `tfTopProjectCounts` **twice** per tick because *"a narrow timestamp RANGE
selects bounded mode; the same window pinned with `date_trunc` inside a full-day
range does not (verified on prod: 94448 vs 87421 for one project-hour)"*
(`BackgroundJobs.hs:1166`). Two semantically identical queries, different
answers. It also forces every chart refresh to re-read 15–60 minutes of already
cached data (`QueryCache.hs:145`).

**Ask:** decide whether the reader should distrust footers written before
`5a445001` (a version stamp or a conservative fall to full-set), or whether
compaction coverage is fast enough to close the window on its own — and say
which, with a date. Until then the exposure is live on all historical data.

**Then, client-side:** retire `checkReadConsistency`'s double query and shrink
`deltaOverlapSeconds`. Both are pure cost that exists only because of this.

---

## Tier 1 — each item deletes a monoscope subsystem

Ranked by what disappears, not by implementation size.

### 1.1 Memory-safe self-joins and wide aggregates **[incident ×3]**

A span self-join is *"the query shape that has repeatedly OOM-killed TimeFusion
in production"* — stated independently in four monoscope files
(`ServiceMap.hs:5`, `Config.hs:155`, `Containers.hs:304`, `Containers.hs:8`).

What exists solely to avoid it:
- the `apis.service_dependency_edges_env` materialised table,
- `rollupServiceEdges`, a 5-minute background job, its idempotent upsert, and a fan-out pre-query,
- `enableServiceMapRollup`, **defaulted off** for staged rollout,
- the Containers window-function pivot, plus faceting and counts done in Haskell over a `LIMIT 500`.

Related and already understood here: `target_partitions` is capped at 16
(`config.rs:3266`) because *"sort machinery reserves per partition and the merge
halves cannot spill"* — and that note names the monoscope RUM/containers
`row_number() OVER (…)` queries as the surfacing symptom. **Spillable
sort-merge is the shared fix**; the partition cap is the tourniquet, and its own
comment says "16 is a starting point, not a law".

### 1.2 Lazy/projected access into wide Variant columns **[measured]**

Two independent monoscope wins came from *avoiding* the blob, not from touching
it faster:
- `Endpoints.hs:353` — *"The COALESCE over JSON paths this replaced forced the wide `attributes` blob to be materialised for every span in the window (**107s vs 21s** on a busy project)."*
- `Endpoints.hs:249` — grouping by a stamped hash instead of re-deriving per span *"more than halved the query cost on a busy host (**22s → 10s**)"*.

And where it cannot be avoided, `Containers.containersInWindow` pays ~15
`variant_get` lookups per row and *"over a day that is the difference between
**0.7s and not completing at all**"* (`Containers.hs:285`). The product
consequence is shipped: the container freshness window is **15 minutes**, so a
container that stopped reporting hours ago silently drops off the list.

`VariantJsonAccessorPeephole` (`optimizers.rs:848`) already proves the shape of
the win — 4.2× — by rewriting to native `variant_get`. This asks for the next
step: don't materialise leaves the projection never reads.

### 1.3 A batch statement-timeout ceiling **[measured]**

The cap is `min(client statement_timeout, TIMEFUSION_PGWIRE_MAX_STATEMENT_SECS)`
(`pg_compat.rs:172`), so a client can only ever lower it. **The ask is not to
raise or remove the global cap** — the 2026-08-18 incident where one 514-file /
32.8 GB scan made the box unreachable to *new connections* settles that.

The ask is a **per-role or per-session ceiling**: a batch/billing connection may
raise to N minutes, interactive stays at 60s.

What it deletes: `dayWindows` (`Telemetry.hs:1058`) splits a billing cycle into
one-day slices because *"a whole billing cycle in one aggregate exceeds
TimeFusion's 60s statement timeout for high-volume projects (DSI-APP writes ~33M
metric rows a day), and a timeout there fails the entire usage report."* A
30-day cycle is **60 sequential statements**. Given usage metering has been dead
since 2026-08-24, this one has revenue attached.

Probably also retires the 4-second render budget (`Dashboards.hs:788`), added
after 2026-08-02 when *"every render thread parked on TF, the three replicas
grew to their 24GB cap and Swarm OOM-killed them in a loop"* — though that is a
concurrency fix as much as a timeout one, so treat it as a hoped-for consequence,
not a promise.

---

## Tier 2 — wire fidelity

### 2.1 pgwire type fidelity **[unquantified, but structural]**

Every one of these is a live divergence in `Telemetry.hs`'s insert path:

| symptom | site | ask |
|---|---|---|
| Statement planned at Parse **without client param OIDs**, so uncast `unnest($N)` types as Null and the INSERT fails | `Telemetry.hs:1707` | Honour declared param OIDs |
| `uuid[]` / `jsonb[]` bind params rejected | `Telemetry.hs:1635` | Accept them |
| `_timestamptz` / `_date` bind params undecodable — timestamps travel as **text** both ways | `Telemetry.hs:1785` | Accept them |
| Nested `text[]` cannot be sent at all — serialised **0x1F-joined** and rebuilt with `string_to_array(_, chr(31))` | `Telemetry.hs:1774` | Accept 1-D array-of-array, or say it will never be supported |
| `date::timestamptz` drops tz → OID 1114 ≠ the decoder's 1184, crashing every TF-routed lookup | `Telemetry.hs:678` | Preserve tz |
| `exemplars` reports as jsonb even under `::text`, so the same column is spelled two ways in one statement | `Telemetry.hs:1015` | Honour the cast |

Together these delete the `usePgTypes :: Bool` flag threaded through
`AuthContext`, the insert path and the tests, plus every `castColumn` /
`jsonColumn` / `splitColumn` divergence combinator.

### 2.2 Real SQLSTATE codes and column-scoped errors **[incident]**

TF returns an empty code and a DataFusion-wrapped message. monoscope therefore
maintains **three independent error-string parsers** (`Utils.hs:889`,
`Log.hs:748`, `Charts.hs:362`), each pinned by doctests against both dialects.

Worse: a bad column fails the *whole* query with `No field named level`,
rendering the widget empty (issue `74c3a90c`, 2026-08-30). To avoid that,
monoscope introspects **both** `otel_logs_and_spans` and `otel_metrics` column
sets at boot into global `IORef`s and validates every KQL field in-app
(`Config.hs:580`, `Parser/Expr.hs:757`).

**Ask:** `42703` for unknown column with the column name and position, `57014`
for timeout (already correct), `42601` for parse errors. That retires three
parsers and an entire schema-introspection subsystem.

### 2.3 Small, cheap, and owed

- **`PERCENTILE_CONT` has zero test coverage.** `README.md:148` advertises it;
  grep for `WITHIN GROUP` across `src/`, `tests/`, `benches/` returns nothing. It
  works only via upstream DF54's four-name allowlist. monoscope's sessions header
  depends on it (`LogQueries.hs:786`). One `.slt` file.
- **`mode()`** — absent, so `fetchLogPatterns` groups by `(pattern, bucket,
  level, service)` and re-aggregates in Haskell under a `LIMIT 20000` that can
  **silently truncate** a high-cardinality project (`LogQueries.hs:649`).
- Missing surface, each with a named caller: `jsonb_build_object`,
  `jsonb_array_elements_text`, `now()` in a `SET` clause (all three force a
  PG-only exec path, `BackgroundJobs.hs:2163`); `to_jsonb(row)` (would retire
  monoscope migration 0099's per-row PL/pgSQL shim); `ORDER BY <ordinal>` over an
  aggregate (three independent workaround sites); unqualified column resolution
  in a self-join (`ServiceGraph.hs:632`, guarded with "Do not tidy an alias away").

---

## Tier 3 — rollup expressiveness

This is the tier that answers the sessions page, and it is small.

### 3.1 `first_value(x ORDER BY ts)` as a decomposable measure

Today the measure set is `count | sum | min | max | tdigest | hll`
(`schema.rs:168`). No argmin.

**It is decomposable.** Store the state as a `(value, timestamp)` pair; merge by
taking the pair with the smaller timestamp. That is associative and commutative,
so it derives across tiers exactly like the others — the same argument
`schema.rs:48` already makes for `derive_from`.

What it unlocks:
- The **sessions rollup** — `landing_url`, `user_agent`, `first_error` are all
  first-observed values, and they are the only reason a session rollup cannot be
  expressed today.
- The raw path too: monoscope builds three full arrays per session group just to
  read element 1, because `FIRST_VALUE` needs `OVER` on both backends
  (`LogQueries.hs:721`). **Measured TF 7.0s** for that shape.

### 3.2 The sessions rollup, concretely

With 3.1 landed, the sessions page becomes:

- **monoscope side (primary):** promote `session_key` at ingest so the
  three-way `COALESCE(NULLIF(…))` becomes a plain dimension column. This is a
  four-step, two-system migration and the PG leg needs the same column or every
  insert fails.
- **TF side:** `dimensions: [session_key]`, measures `count`, a filtered
  `error_count`, `min(timestamp)`, `max(end_time)`, and the three first-observed
  values from 3.1.
- **Percentiles never route** — med/p95 duration are aggregates over
  *per-session* aggregates, and no rollup can answer that. The split: one routed
  `GROUP BY session_key` returns a few thousand rows; Haskell computes
  percentiles, histogram, sort and paging over those; a second page-bounded raw
  query fetches services and the sparkline for the 100 page rows, which is the
  shape `svcs`/`hourly` already use.
- **`MAX(COALESCE(end_time, timestamp))` will not resolve to a measure.** Needs
  a second promoted column or accepting `max(end_time)` plus null handling.

**Secondary, optional:** the group-by matcher could accept a COALESCE chain over
*several* declared dimensions. It is sound by the argument already at
`rollup.rs:2477` — `COALESCE(d₁, d₂, d₃)` is a function of all three, so the
rollup's partition refines it. That would let the sessions rollup work *without*
the ingest promotion. Cheaper, but strictly worse: it leaves a high-cardinality
key computed per query. **Promotion is the recommendation.**

### 3.3 A cardinality guard for dimensions

`RollupSpec::validate` (`schema.rs:110`) checks structure only — no estimate, no
limit, no runtime check. The YAML doc warns rows-per-bucket must stay "in the low
thousands", and `schema.rs:280` records a partition that held 45,483 rows for
7,923 buckets. A session-keyed rollup is *deliberately* high-cardinality, which
makes this the moment to add the guard rather than after it bites: a build-time
row-count ratio against the source with a metric and a refusal threshold.

---

## Tier 4 — write path

| item | evidence | what it deletes |
|---|---|---|
| Make a no-op `UPDATE` free on merge-on-read | **[incident 2026-07-29]** an UPDATE *"minted a duplicate version of every ingested row, doubling every scan until compaction"*; the sustained storm OOM-killed TF (`ProcessMessage.hs:308`) | ingest-time hash stamping, the `@>`/`NOT @>` guards, `enableHashUpdates`, `hashUpdateMaxAgeSecs` |
| Allow unscoped `UPDATE`, or make scoping unnecessary | TF *"rejects any UPDATE without a `project_id` filter"* (`BackgroundJobs.hs:2186`) | an N+1 statement loop per 90s sweep, plus its `SELECT DISTINCT project_id` discovery query |
| Bound the per-connection prepared-statement store; stop CAST-wrapping every insert param | **[measured]** prod 2026-08-14: plan cache at **43%**, pgwire's per-connection store unbounded in count (`Telemetry.hs:1663`) | nothing client-side — this is pure TF-side waste |
| `ON CONFLICT` / upsert | monoscope appends it on PG only; TF dedup absorbs retries (`Telemetry.hs:1673`) | **Low priority.** Store-level dedup is the right answer here; listed for completeness only |

---

## Explicitly deferred, with reasons

- **`apis.*` relational tables or a federated PG↔TF join.** It would delete the
  Haskell-side joins and the un-pushdownable pagination in `Endpoints.hs`, the
  `ShellOnly` render tier and `hostStatsCache`. It is also by far the largest
  item here and pulls TimeFusion toward being a general-purpose database. **Do
  not rank it** — solve pagination pushdown for the specific query if it hurts.
- **`PERCENTILE_DISC`, `json_agg`, `json_object`, `jsonb_array_elements`.** No
  monoscope caller. Note them, do not build them.
- **Exact `COUNT(DISTINCT)` made fast.** Measured 23.9s exact vs 15.9s sketch,
  and KQL's `dcount` is approximate *by definition* — so the sketch is arguably
  the correct lowering, not a workaround. The real cost is accuracy above ~512
  distinct values, which is a documentation problem before it is an engine one.
  Deferred, not rejected.

## Guard rails

This plan does not collide with `tasks/18`, but every reader of that README is
warned to check: **nothing here proposes on-demand covering-slice splitting
(§4a) or a debris migration (§5)**, both of which were investigated and found
harmful. Tier 3.3's cardinality guard is a *build-time* refusal, unrelated to
coverage.

---

## 5. The structural fix, and the actual recommendation

`docs/monoscope-query-shapes.md` already exists and is the right idea — a
compatibility contract sourced from the real client, on the stated principle
that *"anything benchmarked against invented SQL measures a system nobody
runs"*. It is also a hand-made snapshot dated 2026-08-22, and it is **already
stale**: it still lists the Utf8View OR workaround that `e0bf291` fixed.

**Recommendation: turn it into a test suite.** Extract monoscope's actual query
shapes into `.slt` fixtures that run in TF's CI. Each shape asserts it plans,
and where relevant that it routes. Then:

- A TF change that breaks a client query fails in TF's CI, not in monoscope's
  production three weeks later.
- A TF fix that *unblocks* a client workaround becomes visible — the four items
  in §0 would have surfaced automatically.
- The document stops drifting, because it is executable.

This is the item to do first. Every capability below it is worth more once
there is a mechanism that keeps the two trees honest with each other, and it is
the direct fix for the finding at the top of this page: TF's roadmap has no
compatibility items because nothing feeds them in.

## Suggested sequence

1. **The contract suite** (§5) — small, and it changes how everything else is verified.
2. **Tier 0.2**, historical footers — live correctness exposure, and the write-side fix already landed today.
3. **Tier 3.1**, `first_value` as a measure — small, self-contained, unblocks the sessions page, deletes a measured 7.0s client contortion.
4. **Tier 1.3**, the batch timeout ceiling — smallest of Tier 1, and usage metering has been dead since 2026-08-24.
5. **Tier 1.1 / 1.2** — the big ones. Spillable sort-merge first; it is the shared root of the self-join OOMs and the partition cap.
6. **Tier 2** as capacity allows; 2.3's `PERCENTILE_CONT` test is an hour's work and is owed regardless.

Tier 4 and the rest of Tier 2 have no urgency attached and should be picked up
opportunistically when adjacent code is already open.

---

# Implementation log — 2026-09-02 night

Branch `tf-monoscope-compat`. **Not pushed, not deployed.**

## Shipped

| commit | what |
|---|---|
| `7522172b` | harness: floats, CTEs and error detail were all unassertable |
| `ea8491dd` | the contract suite (§5), plus two stale doc claims corrected |
| `f7b50cb9` | `first` as a decomposable rollup measure (Tier 3.1) |
| `6a107961` | a session may raise its statement timeout by asking (Tier 1.3) |
| `39a3dc35` | pin the float-cast fix beside the truncation bug |
| monoscope `e2f917c29` | the "% of Total" column was truncating to whole numbers |

Full library suite green (1104 tests), full slt suite green (17 files).

## The contract suite proved its own argument immediately

Three defects in the sqllogictest harness, each of which had silently disabled
a whole class of assertion:

- `is_query` was `starts_with("select")`, so any CTE went to `execute()` and
  came back as a row count. **No CTE over a data table was testable at all** —
  which is monoscope's two-stage top-N dashboard shape.
- the float8 arm decoded only as `f64`, so every `::float` column — which is
  what monoscope's KQL lowering appends to nearly every aggregate — read back
  as `error:float`.
- `tokio_postgres::Error`'s Display is the bare string `db error`, discarding
  the SQLSTATE and server message.

**This is the answer to the question at the top of this document.** TF's
backlog carried no compatibility items partly because the tests that would have
surfaced them could not express the assertions.

## Two new divergences found, pinned not fixed

Both in `monoscope_query_shapes.slt` §13, with the mechanism deliberately
NOT claimed:

1. **`COUNT(*) * 100.0` renders `300`, not `300.0`.** The fraction is gone
   before any division, so "% of total" columns truncate. Scalar float
   arithmetic is exact (`200.0/6` → `33.333333333333336`), so it is specific to
   an Int64 aggregate against a float literal. **Client-side fix shipped**
   (monoscope `e2f917c29`); the engine-side fix is unowned.
2. **`x::float` resolves to Float32 and narrows.** In Postgres `float` IS
   `double precision`. monoscope appends `::float` to nearly every aggregate it
   emits, so every chart value crosses the wire at float32 precision.

Also recorded: `ROUND(<numeric>, n)::text` does not carry scale.

**Discriminating check for both: do they reproduce on stock DataFusion 54?**
That decides fork-patch versus upstream report. Not run.

## Tier 0.2 — historical footers: INVESTIGATED, deliberately not built

`5a445001` fixed the write side (`SortingColumn.column_idx` indexes parquet
leaves, not fields, so every sort key after a Variant column under-shot). Its
own message says **"Old files keep their wrong footers; the win phases in as
rewrites replace them."**

The reader still consumes those footers. `detect_bound`
(`src/read/mod.rs:394`) enables bounded dedup "iff the input's leading sort
column is a dedup key of an i64-backed type", read from the advertised
ordering — which for every pre-fix file is a claim the data does not satisfy.

**Not implemented, and I would not implement it unattended.** The conservative
guard (distrust footers below a version stamp, fall to full-set) lands directly
on the path that 500'd prod on 2026-08-15, when unordered merge-on-read dedup
blew its 2048 MiB per-query limit — and `bounded_dedup_enabled` defaults ON
precisely because "full-set has no LIMIT early termination"
(`src/read/mod.rs:1379`). Trading a latent wrong-answer for a fleet-wide
latency regression is not a call to make overnight.

**What the decision needs, and neither is available locally:** how much of the
live population still carries pre-`5a445001` footers, and whether compaction
closes that window on its own. If it does, this needs nothing but time.

Until it is closed, monoscope's `checkReadConsistency` double-query and the
15–60 min `deltaOverlapSeconds` chart re-read must stay. They are the
compensating control.

## Tier 2.2 — SQLSTATE codes: scope multiplier, writeup only

The error text monoscope string-matches is produced in
`apitoolkit/datafusion-postgres` @ `timefusion-df54` — a **separate repository**
pinned at `Cargo.toml:258`. Real SQLSTATEs (`42703` with the column name and
position, `42601` for parse errors) mean a change there, a push, and a pin
bump, so it is not a TimeFusion-only change and did not fit tonight.

Worth doing: it retires three independent error-string parsers in monoscope
(`Utils.hs:889`, `Log.hs:748`, `Charts.hs:362`) **and** the boot-time
schema-introspection subsystem that exists only because a bad column fails the
whole query opaquely (`Config.hs:580`, `Parser/Expr.hs:757`).

The harness now surfaces `Postgres error [XX000]: …`, so the work is at least
observable from the test suite — every refusal in the contract suite currently
reports `XX000`, which is the gap made visible.

## What `first` still needs before it does anything

It ships **inert**: no production spec declares a `first` measure, because the
sessions rollup needs `session_key` promoted at ingest first — a coordinated
TimeFusion + monoscope schema migration, and the user's call.

Be precise about what "tested" covers. The validation invariant, the build, the
tier-to-tier derive, the matcher's order-by gate and both SQL-generation arms
are pinned — but at **SQL-string level**. The hybrid rollup/raw union actually
*executing* a `first` measure cannot run until a spec declares one, so that
path arrives with the sessions rollup and should be measured then, not assumed.
