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

## The one open functional item: promotion does not fire in prod

Golden Signals filters `(kind='server' OR name='apitoolkit-http-span' OR
name='monoscope.http')` — a *row* filter that textually equals the `server_*`
measures' declared filter. The matcher promotes such a filter to the pre-filtered
measures plus a `HAVING SUM(count_F) > 0` guard. It works in unit tests and in a
real-`Database` integration test. **It does not fire in prod, and we do not yet
know why.**

What has been ruled out:

- Not the canonical string's Arrow type (`Utf8` vs `Utf8View`) — fixed in
  `da6aa3b`, necessary but not sufficient.
- Not the log level: prod runs `RUST_LOG=info`, so the `rollup_promotion_unmatched`
  warn prints. Once `cb04661` deployed it fired within minutes.

**What the deployed log actually said (2026-08-12 12:33Z).** Two things, and the
prime suspect was wrong:

1. **`measure_filters` is healthy in prod — zero `rollup_measure_probe_*` events.**
   Every declared measure resolves. That eliminates the probe as the cause.
2. **Every conjunct was printed TWICE, on both sides** — `server_request_count`
   came out as `(kind=... OR name=... OR name=...) AND (kind=... OR name=... OR
   name=...)`. The optimizer leaves a predicate on the Filter node *and* re-pushes
   it into the TableScan's `partial_filters`, so `source_and_filters` collects
   both copies. `canonical_and` sorted (order-independent) but did not dedupe
   (not multiplicity-independent). Both sides duplicated equally, so the
   comparison still succeeded — by accident, not by construction. Fixed in
   `6f0ce46`; `AND` is idempotent, so dedupe after the sort.

That duplication is the **leading hypothesis** for the Golden Signals miss: no
test session registers the tantivy pushdown rule, so nothing in a test ever
duplicates a conjunct, and no test can observe an asymmetry that exists only on a
real session. The log also confirms the rewrite is real — declared filters carry
`text_match(kind, "server")` and `text_match(status_code, "ERROR")` alongside the
equality.

**Still unconfirmed:** no Golden Signals query has been observed since the fix
deployed. The two promotion declines captured so far were a `SELECT DISTINCT
attributes___db___system___name ... IS NOT NULL` probe — a genuinely
non-dimension predicate, correctly refused. Confirm by watching for a
`rollup_promotion_unmatched` whose `promoted=` field mentions `kind`, or for
`rollup_hits_*` moving off zero.

---

## Re-scoping step 4: there is no "endpoint tier" to build

The remaining `unknown_filter` misses were attributed to "monoscope's
`jsonb_path_exists` traffic chart", with a proposed 1h endpoint-dimensioned tier
as the fix. That attribution is wrong. `jsonb_path_exists` appears in monoscope
only in `src/Pkg/Parser.hs`, as part of the **ad-hoc query DSL** — user-typed
search predicates, not a fixed widget. Their shape is unbounded, so no declared
dimension set can route them, and a coarse endpoint tier would serve none of them.

Correctly refusing them is the right behaviour. Measure endpoint cardinality only
if a *specific, fixed* widget shows up in the miss log — not on the strength of
this counter.

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
- `rollup_backfill_tick` now logs `queued/built/uncertifiable/failed` whenever
  there was work. Previously it logged only on success, so a tick that queued 200
  partitions and certified none was byte-identical to an idle tick — which is how
  a frozen backfill hides. If `queued` is high and `built` is 0, the blocker is
  certification, not the rollup build.

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
