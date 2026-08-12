# Rollups: state of play

**Status 2026-08-12.** Supersedes `2026-08-11-rollup-next-steps.md`, whose steps
1–3 are done and whose step 4 is re-scoped below. Read routing is live for **all
tenants**; the shape gap is closed; the one open functional item is a promotion
that works in tests and does not fire in prod.

---

## What is live

Gates on prod (`srv-captain--timefusion`) — the two project allow-lists were
REMOVED on 2026-08-11, so rollups are on for every tenant:

```
TIMEFUSION_ROLLUP_ENABLED=true
TIMEFUSION_ROLLUP_READ_ENABLED=true
TIMEFUSION_ROLLUP_REALTIME_TAIL=true
TIMEFUSION_ROLLUP_BACKFILL_DAYS=31
```

They live in CapRover's `/captain/data/config-captain.json` (backed up as
`config-captain.json.bak-rollup-*`) **and** on the swarm service. Change both, or
the next CapRover deploy reverts you.

Verified in production:

| | before | after |
|---|---|---|
| `miss_unsupported` (78% of all misses) | 40 | **0** |
| spans, 7-day window | 48s | **1s**, full hit, digit-identical to raw |
| spans, 4-day window | 21s | **1s** |
| coverage lifetime | seconds (blanket DML wipe) | survives DML **and** restarts |
| measure-probe planning cost | 1782 misses in minutes | **7 in 25 minutes** |

---

## Golden Signals: ROOT-CAUSED and fixed (`76eedbb`)

The promotion failed in prod and passed in every test because of **tantivy index
hints**, not because of anything in the promotion logic.

Isolated on prod by snapshotting the counters, running exactly one Golden
Signals-shaped query, and snapshotting again: **+1 `unknown_filter`, nothing
else.** The log then printed both canonical strings:

```
query    (((kind = "server") AND text_match(kind,"server")
                              AND text_match(kind,"server","eq")) OR name=... )
declared  ((kind = "server") AND text_match(kind,"server")) OR name=...
```

`optimizers/tantivy_rewriter` **additively** ANDs `text_match(col, q)` beside a
predicate it can accelerate and never removes the original comparison — so the
semantics live entirely in the other terms and the hint is noise for an equality
comparison. But the two sides do not receive the same hints: the query carried
three across two arities where the declared filter carried one.

Fix: at each AND level, drop any `text_match` on a column that level already
compares. Scoped that way, a `text_match` the *user* wrote against a different
column survives and the filter correctly matches no declared measure. Two
supporting normalizations were required — dedupe at every level (the duplicates
were nested inside an OR, out of reach of a top-level dedupe) and collapse a
one-element conjunction to its operand.

**Note a second, separate bug:** `tantivy_rewriter`'s invariant 3 claims
idempotence under repeated passes. Two arities of the same hint on one expression
say otherwise. The matcher no longer depends on it either way, but the rewriter
is still emitting redundant hints into every plan.

**Why no test caught it:** a bare test session registers no tantivy rewriter, so
nothing in a test ever carried a hint. The regression test now builds the exact
prod expression by hand.

---

## Re-scoping step 4: there is no "endpoint tier" to build

The remaining `unknown_filter` misses were attributed to "monoscope's
`jsonb_path_exists` traffic chart", with a proposed 1h endpoint-dimensioned tier
as the fix. That attribution is wrong. `jsonb_path_exists` appears in monoscope
only in `src/Pkg/Parser.hs`, as part of the **ad-hoc query DSL** — user-typed
search predicates, not a fixed widget. Their shape is unbounded, so no declared
dimension set can route them, and a coarse endpoint tier would serve none of them.

Correctly refusing them is the right behaviour.

**Cardinality measured anyway, so the judgment is data-backed** (2026-08-12, one
project, one 3-hour window): **252 distinct `attributes___url___path` values**
against 3 distinct services. Rows per bucket scale with the product of the
dimensions, so an endpoint tier would be ~250x wider per bucket than the current
3-dimension spec — a slightly smaller copy of the source table, which is exactly
what the schema comment warns against. Do not build it.

---

## What to watch, and what each number means

`SELECT component,key,value FROM timefusion_stats WHERE key LIKE '%rollup%'`

- `rollup_miss_other_total` **no longer exists.** `cb04661` made the reason match
  exhaustive over `MissReason`, so `missing_project`, `unbounded_time`,
  `non_decomposable` and `rewrite_schema_mismatch` now have their own counters and
  a new variant fails the build rather than joining a bucket nobody can read.
- `rollup_rebuilds_incremental` vs `_full`. Sat at 0 vs 14 through 2026-08-12.
  That is **expected** immediately after a restart — an untracked partition
  rebuilds fully by design — but a sustained zero means something is widening the
  dirty set to the whole day. Note the fingerprint-only path: a compaction that
  rewrites files without changing rows moves `source_fp` while the dirty mask is
  0, and an empty range set falls through to a full rebuild. That is the safe
  direction (dedup can drop rows, so an unchanged mask does not prove unchanged
  content) but it means compaction-driven rebuilds will always read as `full`.
- `rollup_miss_unknown_filter_total` **is dominated by queries that were never
  candidates, so do not read it as a promotion failure.** 3h of prod logs on
  2026-08-12: 84 `rollup_promotion_unmatched` events, **none of them Golden
  Signals** — 48 `attributes___db___system___name IS NOT NULL` (facet `SELECT
  DISTINCT` discovery), 14 `jsonb_path_exists(to_jsonb(hashes), …)`, 10
  `Like(Like …)`, 10 `duration IS NOT NULL`, 2 `array_has_all(hashes, …)`. All
  log-explorer and facet traffic. The counter conflates "a filter we should have
  matched and didn't" with "never eligible", which is the same defect the dedup
  counters have; until those are split, a nonzero value means nothing. Two
  follow-ups: classify never-eligible residuals into their own counter, and drop
  the WARN to debug when the residual references no declared measure column at
  all — 84 WARNs in 3h on a hot path will bury the real case when it comes.
- `rollup_backfill_tick` logs `pool/queued/built/uncertifiable/failed` whenever
  there was work; `rollup_backfill_idle` logs `pool/gated/backoff/covered` when a
  non-empty pool survived nothing. **The backfill is currently queuing nothing at
  all** — zero ticks logged across 40 minutes on 2026-08-12, while the rollup
  table showed 08-01..08-08 covered for only the original canary and 08-09..08-11
  for 4-6 projects. So the widening has NOT reached the sealed days. `7b651f3`
  ships the funnel that says which of gated / backoff / already-covered is
  swallowing them; read `rollup_backfill_idle` first.

---

## Unrelated prod bug found while reading logs

The container's open-file **soft** limit was 1024 against a 524288 hard limit
(Docker's daemon default). On 2026-08-12 prod spent its first 5.5 minutes after
boot emitting **1.6M** `Too many open files` lines and **refusing pgwire
connections** (`Error accept socket`), which also pushed every other log line out
of the ring buffer — including the rollup diagnostics this session needed.

`cb04661` raises the soft limit to the hard limit at boot. **Verified in prod on
e8f7f98: 1024 -> 524288.** (Check the TF process, not the container's main PID —
that is `docker-init`, whose limits are still 1024 and will fool you.) It raises, in `main()` before the
runtime is built and in `bootstrap()` for the e2e harness (which does not go
through `main`). It has to be in-process: CapRover rewrites the service config on
every deploy, so an out-of-band ulimit does not survive.

---

## Testing notes

`make test` needs local MinIO **and** the `timefusion-tests` bucket. Without it,
`tombstoned_row_hidden_from_select_and_count` and the two rollup integration
tests fail with an S3 connect error that reads like a code failure. It is not:

```bash
make minio-start
AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
  aws s3 mb s3://timefusion-tests --endpoint-url http://localhost:9000
```

That flake is what made `tombstoned_row_hidden_from_select_and_count` look
intermittent across earlier sessions. With MinIO up it passes.
