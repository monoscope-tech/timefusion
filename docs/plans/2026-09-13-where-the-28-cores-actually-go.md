# Where prod's 28 cores actually go

2026-09-13, measured on a 9-hour-mature process (uptime 32,120 s), ingest
~1,400 rows/s.

## The container is CPU-saturated

`cpu.max` is `2800000 100000` — a **28-core** cap. The cgroup's own
`usage_usec` delta reads **25.00 cores**, i.e. **89% of the ceiling**. This is
not the 2026-09-04 situation where query CPU was indistinguishable from idle;
the box is genuinely out of CPU.

## Attribution, and how to measure it without lying

| pool | cores | share |
|---|---:|---:|
| tantivy (`thrd-tantivy-in`, `merge_thread_0`, `thread-tantivy-`) | 9.7-10.2 | **~39%** |
| `tokio-rt-worker` (queries, ingest, async maintenance) | 8.0-9.1 | ~34% |
| `maintenance-wor` | 5.7-7.0 | ~25% |
| jemalloc/foyer/etc | ~0.5 | ~2% |
| **TOTAL** | **24.0-25.8** | reconciles with cgroup |

**Two measurement traps, both hit before the numbers above were trusted:**

1. **Aggregating by thread NAME double-counts a churning pool.** Summing
   per-comm in snapshot A and snapshot B separately, then differencing, credits
   a newly-spawned thread's entire lifetime to the delta. Tantivy churns
   threads constantly, so it read 17-18 cores. **Join per TID**, then aggregate.
2. **`snap()` itself took longer than the sleep.** One `awk` per file over 2,893
   threads is ~2,900 processes; dividing by the intended 15 s rather than the
   measured elapsed inflated everything. The first "corrected" run still summed
   to 38 cores against a 25-core cgroup. **A per-thread total that exceeds the
   cgroup is a broken instrument, not a finding** — reconcile before believing.

The working recipe is one `awk` over `/proc/<pid>/task/*/stat`, stamped with
`date +%s.%N` on both sides, joined on TID.

## Tantivy is ~39% of the box, and it is not insert cost

| | rows/s |
|---|---:|
| ingested | ~1,400 |
| **indexed by tantivy** | **3,100-9,400** (bursty; big builds dominate) |

Indexing runs at **2-7x the ingest row rate**. Spending ~10 cores to index
1,400 rows/s would be ~134 rows/s/core, one to two orders of magnitude below
what tantivy actually does — which is independent evidence the cost is not
ingest-proportional.

The mass is concentrated: in one 30-minute window, **6 builds of ≥300 k rows
carried 4.80 M rows while 100 smaller builds carried 0.82 M** — 85% of the work
in 6% of the builds. The largest were 1,074,522 / 971,277 / 935,760 rows, and
**one project's 711,283-row build appeared twice in the same window.**

### What it is NOT — refuted, with the instrument that refuted each

- **NOT re-indexing maintenance rewrite output.** The obvious hypothesis, given
  ~36x write amplification. `reindex_wave_outputs` already carries coverage
  forward instead of rebuilding, and prod measured **256 carried, 0 rebuilt
  across 98 waves in 90 minutes — a 100% carry rate.** The counter for this was
  already in the tree; the hypothesis died in one query.
- **NOT the post-optimize path** (`compact.rs:237`): zero such log lines in 120
  minutes.
- **NOT a historical backfill backlog.** `tantivy_coverage_census` reads
  `uncovered=708 today=627 week=34 older=47`, and `older` is *shrinking*
  (50 → 49 → 47). The uncovered set is today's churn, not history.

### What remains open

The flush callback (`server/mod.rs:153`) is documented as "the write layer
invokes this callback after commit" — ingest only — and should therefore track
ingest at ~1,400 rows/s. It does not account for a 2-7x rate, and the repeated
711,283-row build is unexplained. The remaining candidates are
`spawn_deferred_tantivy_reindex` (`mod.rs:3904`) and `backfill_table_indexes`
(`mod.rs:4301`).

**Do not size a fix before this is named.** Note that `tantivy_backfill_built`
sitting frozen does NOT exonerate backfill: only the `mod.rs:4344` site
increments it, and the deferred path at 3904 increments nothing.

## Correction: the rollup prior-art doc's headline numbers are not sound

`2026-09-13-rollup-maintenance-prior-art.md` opens with "BaseRollup alone would
want **25-39 of 48 cores** at ten times today's writes", derived from
`work.BaseRollup.progress_rows / rows_ingested_total` ≈ 1,490:1 (and a later
reading of 2,158:1). **Both counters mean something other than what that
inference needs**, per `observability.rs:81`:

> `worker_secs`/`killed_secs` are wall time a worker held for this operation.
> `progress_rows` is the liveness counter's tally — summed over every operator
> in the plan tree, so it is a same-shape trend proxy, **NOT a count of rows of
> work**.

So `progress_rows` is not rows read, and `worker_secs` is wall-held including
every wait on a semaphore, the sort pool, or the journal lock — not CPU. Two
independent checks confirm the extrapolation cannot stand:

- 92.2 B `progress_rows` against 3,044 s of published `scan_ms` implies 30 M
  rows/s, which is not physical.
- `work.BaseRollup.worker_secs` = 119,740 s against **6,004 s** of published
  end-to-end duration across all 1,732 publications — a 20x gap that is waiting,
  not work.

**The real figure is the measured one: the whole `maintenance-wor` pool is
5.7-7.0 cores, all operations combined.** Any 10x CPU claim has to be re-derived
from that. The prior-art doc's *direction* (merge partial aggregates instead of
re-reading raw, per ClickHouse/Druid/TimescaleDB/IVM) is unaffected — it is the
cost premise that needs redoing, and the cardinality measurement that doc
already gates the design on is still the right next step.

## Why this matters for the 1.2 TB backlog

`sealed_compaction_debt_bytes` is 1.185 TB and the sealed lane gets ~1.4% of
maintenance worker time. Reclaiming tantivy's ~10 cores is the largest single
source of headroom on the box — but headroom is not the backlog fix: the sealed
lane is permit- and stall-bound, not CPU-starved, so more cores alone will not
drain it.

## UPDATE, post-#271: the cap now BINDS, and `nr_throttled` proves it

With the hygiene lanes unblocked, cgroup CPU moved from **25.0** to **25.9-27.7
of 28**, and `cpu.stat`'s `nr_throttled` increments **~85-102 per 12 s (~8/s)**.

That second number is the one that matters. A cores figure near the cap is
suggestive — scheduling noise, a burst, a bad sample. **A rising `nr_throttled`
is proof**: the kernel is stopping runnable threads because the CFS quota is
exhausted. Maintenance work is now being held back by the container's CPU limit
rather than by a lock, a permit, or a stalled sort.

Measure `nr_throttled` alongside `usage_usec` whenever asking "is the CPU cap
binding?" — it is the difference between "busy" and "capped", and this codebase
has mistaken the former for a finding before.

Two consequences:

- **Raising NanoCpus 28 -> 32 is now a measured fix**, not a guess. The value is
  in `deploy/caprover-service-override.yml`; the repo file alone does not apply
  it, it needs a CapRover admin update. Note it does **not** change
  `light_optimize_k`, which stays at 2 from 28 cores to ~48 — it buys throughput
  for the lanes that already run, not more sealed concurrency.
- **Tantivy's ~10 cores is now directly displacing maintenance.** While the box
  had headroom, the 39% share was a cost without a victim. Under throttling it
  is not: every core tantivy holds is a core the backlog does not get. That
  promotes the attribution work from "headroom project" to "on the critical
  path", which is what the `cause` span shipped in #273 exists to resolve.
