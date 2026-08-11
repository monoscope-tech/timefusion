# Rollups: next steps and refactors

**Status 2026-08-11.** Read routing is live and correct. `otel_metrics` routes and
was verified against raw scans in production, to the digit. `otel_logs_and_spans`
still declines the commonest dashboard shape — see step 1, which is the only
thing standing between here and beating the baselines.

This is a handoff. Every item says what is wrong, why the obvious fix is wrong,
and how to know you are done.

---

## Where things are

Live on prod (`srv-captain--timefusion`), canary-scoped:

```
TIMEFUSION_ROLLUP_ENABLED=true
TIMEFUSION_ROLLUP_BUILD_PROJECTS=be87ebc1-08b9-4293-a390-283460fa6202
TIMEFUSION_ROLLUP_READ_ENABLED=true
TIMEFUSION_ROLLUP_READ_PROJECTS=be87ebc1-08b9-4293-a390-283460fa6202
TIMEFUSION_ROLLUP_REALTIME_TAIL=true
TIMEFUSION_ROLLUP_BACKFILL_DAYS=31
```

They live in CapRover's `/captain/data/config-captain.json` (backed up as
`config-captain.json.bak-rollup-*`) **and** on the swarm service. Change both, or
the next CapRover deploy reverts you.

Built so far for the canary: `otel_metrics` 25 days at 1m and 25 at 1h,
`otel_logs_and_spans` 9 days at 1m. The backfill walks newest-first, ~4 units per
10-minute tick.

**The one query that answers "where am I":**

```sql
SELECT key, value FROM timefusion_stats WHERE key LIKE 'rollup%' AND value <> '0';
```

`hits_full` / `hits_hybrid` prove routing fires; the `miss_*` reasons say which
gap to close. Do not use `EXPLAIN` — the substitution happens in
`DmlQueryPlanner`, and EXPLAIN plans its inner plan with the DEFAULT planner, so
an explained query always shows a raw scan even when the real one routes. Do not
benchmark with `count(*)` either — `try_count_pushdown` answers it from Delta
stats before the rollup path is reached.

---

## Step 1 — Replace the Aggregate in place, delete the peeling machinery

> **DONE.** `match_aggregates` now finds the outermost `Aggregate` anywhere in
> the tree and `dml.rs` swaps the rewrite in for that node; `PlanWrapper`,
> `aggregate_with_having`, `decline`, `unqualified`, both peeling loops and the
> positional `output_names` logic are deleted (net −107 lines). The shape below
> is covered by `a_partly_covered_window_unions_the_rollup_with_raw_and_matches_the_raw_answer`,
> which asserts it routes **and** equals the raw answer on a real `Database`
> session — the only place that shape exists.
>
> Two guards were added because searching the whole tree is strictly wider than
> peeling a root, and both protect things the counters cannot show:
> statements (`Dml`/`Explain`/…) are excluded, so an aggregate nested in an
> UPDATE is never substituted out from under the DML interception; and
> `substitute` errors if the target node is not found, rather than returning the
> untouched raw plan and reporting it as a rollup hit.
>
> The rest of this section is kept as the rationale.

**This is the whole remaining gap, and it is a deletion, not an addition.**

`match_aggregates` (`src/rollup.rs:813`) dismantles the plan by peeling known node
types off the root, finds the `Aggregate`, and hands the caller pieces to
reassemble in `src/dml.rs:150-170`. Production plans keep having one more layer
than the peeler knows about. The history is three commits of chasing it:

| | shape | fix |
|---|---|---|
| root peel | `Sort(Projection(Aggregate))` | original |
| `f96aa07` | `Projection(Limit(Sort(Aggregate)))` | peel below the projection too |
| now | `Projection(Sort(Projection(Aggregate)))` | — a third layer |

The current declined plan, straight from prod's `rollup_declined_shape` log:

```
Projection: count AS c, CAST(avg) AS m
  Sort: count DESC, fetch=2
    Projection: avg, count            <- rename-free, drops the group key
      Aggregate: groupBy=[time_bucket(…)], aggr=[avg, count]
```

Peeling cannot win. The nodes above the aggregate are not a fixed grammar, they
are whatever the optimizer produced, and **the shape differs by session** — a
pgwire session nests the Sort under a projection, a bare `SessionContext` leaves
it at the root. That is why the existing root-peel unit test passes while prod
declines every such query.

### The refactor

Do not take the plan apart. Rewrite the `Aggregate` subtree **in place** and
leave every node above it untouched:

```rust
use datafusion::common::tree_node::{Transformed, TreeNode};

/// Replace the first Aggregate that a declared rollup can serve, leaving every
/// node above it exactly as the optimizer produced it.
///
/// The nodes above an aggregate reference its OUTPUT NAMES, so a rewrite that
/// preserves those names is substitutable for it — whatever they are. That is a
/// property of the aggregate alone, which is why nothing above it needs to be
/// understood, peeled, or rebuilt.
plan.clone().transform_down(|node| match node {
    LogicalPlan::Aggregate(_) if /* matched */ => Ok(Transformed::yes(rewritten_aggregate)),
    node => Ok(Transformed::no(node)),
})
```

`transform_down` with an early `Transformed::yes` stops at the outermost
aggregate, which is the one to replace.

**What deletes:** `PlanWrapper` (`rollup.rs:233-260`), `RoutedRollup::wrappers`,
`RoutedRollup::inner_wrappers`, `RoutedRollup::outer_projection`,
`RoutedRollup::having`, the whole reassembly block in `dml.rs:150-170`, and the
`output_names` positional-alias logic in `match_aggregates` — with it, the two
bugs that logic caused (`ba87c58`, and the HAVING guard before it).

**What the rewrite must preserve, and this is the load-bearing part:** the
replacement node's output schema must have the same field NAMES and types as the
aggregate it replaces — `count(Int64(1))`, `avg(otel_logs_and_spans.duration)`,
`time_bucket(Utf8View("1 hours"),otel_logs_and_spans.timestamp)` — because that
is what the untouched nodes above reference. Today the rewrite deliberately emits
*unqualified* aliases and the acceptance gate uses
`has_equivalent_names_and_types` (which ignores qualifiers). In-place substitution
is stricter: a `Column` reference in the Sort above must still resolve. Build the
replacement's aliases from `aggregate.schema().fields()` verbatim, and keep the
existing whole-plan schema check as the backstop.

**Order of work.** Land this behind the existing gates with the current tests
green first; it is a pure refactor of how the rewrite is attached, not of what
the rewrite computes. `route_with_spec` (`rollup.rs:912`), the union SQL, the
measures and `interior()` are all untouched.

**Done when:** the prod query below routes (`rollup_hits_*` increments) and
`rollup_declined_shape` stops appearing for it.

```sql
SELECT count(*) c, avg(duration)::bigint m FROM otel_logs_and_spans
WHERE project_id='be87ebc1-08b9-4293-a390-283460fa6202'
  AND timestamp >= '2026-08-05T00:00:00Z' AND timestamp < '2026-08-09T00:00:00Z'
GROUP BY time_bucket('1 hours', timestamp) ORDER BY 1 DESC LIMIT 2;
```

---

## Step 2 — Beat the baselines, then widen the canary

Only meaningful after step 1. Measured on `be87ebc1` before any rollup existed:

| window | rows | raw |
|---|---|---|
| 24h count+avg | 1.4M | **17s** |
| 7d count+avg | 8.4M | **48s** |

Compare routed vs forced-raw on the SAME window by appending `AND id IS NOT NULL`
— `id` is not a dimension, so it forces a residual filter and a raw fallback
without changing a single row. That is the parity harness used all night; it is
how the metrics path was proved correct to the digit.

Then widen `TIMEFUSION_ROLLUP_READ_PROJECTS` one project at a time, watching the
miss histogram between each.

---

## Step 3 — Make coverage survive more than compaction

Coverage is now keyed on DATA identity — row count plus timestamp span from the
add actions (`7ee528f`) — which survives compaction. Two known limits remain.

**MOR is a blind spot.** On a `version_append` table an UPDATE appends a row, so
the count rises and coverage correctly invalidates. But the rollup is then
rebuilt from scratch for that whole partition, every time. For a table under
continuous enrichment that is a treadmill. The principled discriminator is
Delta's `data_change` flag: compaction commits `data_change: false`, real writes
`true`. Walking the commits since the build and accepting a run of
`data_change: false` is strictly more precise than any statistic.

**Do not loosen it further without that.** A too-loose validity check serves a
rollup built from different data — a silently wrong number on a dashboard, which
is the worst failure this system can produce and the one every other guard here
exists to prevent.

---

## Step 4 — Widen what can route (in miss-count order)

Read the histogram first; do not guess. Known gaps, cheapest first:

1. **`missing_measure` on the Golden Signals widget.** Its
   `WHERE (kind='server' OR name='apitoolkit-http-span' OR name='monoscope.http')`
   is a ROW filter identical to the `server_*` measures' declared filter, so it
   hits the residual-filter refusal. Either add unfiltered `duration_digest` /
   `error_count` measures and drop the row filter from
   `monoscope/static/public/dashboards/_overview.yaml:80-104`, or move it into
   per-aggregate `FILTER (WHERE …)` clauses, which the matcher already handles —
   an aggregate filter changes values, never which groups exist. Also align
   `server_error_count` / `server_error_scope_count`, which omit
   `name = 'monoscope.http'` that the widget includes.
2. **The percentile CTE.** `monoscope/src/Pkg/Parser.hs:384-393` wraps digests in
   a CTE plus `CROSS JOIN (VALUES …)`. The inline form at
   `shared/src/Pkg/Parser/Stats.hs:150` routes; prefer it, or emit the quantiles
   as sibling projections over one `percentile_agg`.
3. **`count(*) OVER ()`** (`Parser.hs:340-354`) puts a `WindowAggr` above the
   aggregate. After step 1 this may just work — retest before changing monoscope.
4. **Sub-minute bins.** `calculateAutoBinWidth` picks 1s–30s for ranges ≤ 1h; a
   1m grain cannot serve those and never will. Leave them raw, they are cheap.

Unrelated bug found while surveying: `http-stats.yaml:39-57` interpolates
`{{time_filter_sql}}`, which is not in `variablePresets`, so it ships an
unsubstituted literal. Also `kind = 'SERVER'` there vs `'server'` everywhere else.

---

## Step 5 — The enrichment bound: leave it alone

Recorded because it was asked for and the answer is counter-intuitive.

`hashUpdateMaxAgeSecs = 7200` (`monoscope/src/System/Config.hs:268`, hardcoded, no
env override) already caps enrichment at 2h — stricter than the 24h that was
requested. It was **not** what invalidated rollup coverage; those partitions were
3–5 days old and the real cause was the file-list fingerprint (step 3). Lowering
it to 1h buys nothing for rollups and costs real behaviour: it is also the floor
for `safetyNetReprocess` (`BackgroundJobs.hs:2015-2025`), so spans arriving more
than an hour late would permanently lose their `pat:*` tags and
`hashes @> ARRAY[…]` endpoint filtering would miss them.

---

## How to test this thing

**Unit tests cannot see most of these bugs, and that is structural.** A
`MemTable` session has no Variant rewriter, no `DedupExec`, and no scan admission
guard, so it plans cleanly through code that fails in production. Six bugs found
overnight were all invisible to a green suite:

| bug | what the MemTable lacked |
|---|---|
| `SELECT *` probe (`c2be44d`) | the Variant rewriter |
| rollup `dedup_keys` (`d17e3eb`) | `DedupExec` |
| unbounded measure probe (`d706045`) | the scan admission guard |
| declined plan shapes (step 1) | pgwire's analyzer rules |

So: put the guard on the **schema or the invariant**, not on a query shape that
happened to reproduce it (see `declared_rollup_is_generated_with_its_configured_fields`,
which now asserts a rollup declares no dedup keys). And treat the prod miss
counters and `rollup_declined_shape` as the oracle for anything plan-shaped.

The integration harness in `tests/suite/dedup_compaction_test.rs` is the closest
thing to real: `a_partly_covered_window_unions_the_rollup_with_raw_and_matches_the_raw_answer`
asserts row-for-row parity against a raw scan **and** asserts it actually routed.
Keep that second assertion on anything new — a miss returns the right answer, so
a parity test without it passes vacuously.

---

## Deploying

`git push origin master` builds and deploys (~25 min, serialized). The WAL wedge
that used to eat deploys is fixed (`026b876`): an orphaned container swarm had
lost track of held the lock forever and was never sent SIGTERM. Holder now exits
when a takeover request goes unanswered for `TAKEOVER_ESCALATE_AFTER` (180s),
contender exits non-zero after `LOCK_WAIT_GIVE_UP` (900s). If it ever recurs:

```bash
ssh ubuntu@captain.s.past3.tech 'docker ps --format "{{.Names}}|{{.Status}}" | grep timefusion'
# more than one container, or one "(health: starting)" for minutes = wedged
ssh ubuntu@captain.s.past3.tech 'docker stop -t 60 <the healthy old container>'
```

That stop is graceful and lossless — measured at 23s.
