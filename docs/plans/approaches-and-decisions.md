# Approaches and decisions — running log

Newest first. One entry per decision that changed direction, with what refuted it.
Detail lives in the dated plan files; this is the index.

## 2026-09-01/02 — maintenance capacity night

**Goal:** make dedup/sorting/hotpacking/rollups keep up, toward 10x and a
prospective 100x customer.

### Shipped to prod

| # | change | status |
| --- | --- | --- |
| 19 | split the certified-skip refusal (`no_stats` vs `overlap`) | live |
| 20 | rollup admission reclassified, deploy-15 footgun made unwritable | live, verified |
| 21 | certify from the batch probe | live, measured a NULL |
| 22 | certify sealed dates from the Delta snapshot | live — **first grants in system history** |
| 23 | project-major ordering | live |
| 24 | 16x probe rate + decline memo + `cert_probe_declined` | live |
| 25 | certification reaches `otel_logs_and_spans` at all | live |
| 26 | instrument HOW dirty a declined date is | live |
| — | window claim reservation (`5ed8c6b5`) | live, A/B'd at ~1% |

### The chain, and where it broke

1. Dedup is **~96% of maintenance worker time** and drops **0.0004%** of rows —
   it is a cleanliness PROOF, not a removal.
2. So certify cheaply instead of rewriting. Built it; `cert_granted_total` left a
   zero held since 2026-08-20, and prod queries were measured skipping
   `DedupExec`.
3. **Refuted as a leading strategy:** duplicates are sparse but SPREAD (~26-50 of
   144 bins per date), so every partition is dirty and certification cannot grant
   until removal happens. Removal is the constraint.
4. **Refuted again:** the removal queue's ordering is worth ~1%. A/B on the real
   77k-task prod journal — 22,162 pending, 27,175 executions/24h, backlog only
   halves either way. **The queue is CAPACITY-bound, not order-bound.**
5. **The cost, located:** the rewrite's sort **OOMs a 1 GB pool on one 204 MB
   production bin**; prod survives only by slicing 13 ways at 15 MB/s.

### Open, in priority order

1. **Align `sorting_columns` with the dedup keys** (`timestamp, id` leading;
   `service` after). Today `service` sits BETWEEN the dedup keys, so files are not
   prefix-ordered for the window's `PARTITION BY (timestamp, id)` and the sort is
   unavoidable. NOT DONE: changes physical layout for every future file and every
   query's read path, needs the latency matrix re-run, and does **not**
   retroactively fix existing files — so it cannot drain the current backlog on
   its own.
2. **Prevent duplicates at ingest** — dedup-key check inside the MemBuffer's
   10-minute bucket, so dates are born certifiable and need neither rewrite nor
   probe. The only item that scales to 100x. Design decision, not a patch.
3. Cheaper units (batch sizing already gave 39.4s -> 20.3s on one file).

### Decisions NOT taken, and why

- **Raising `STARVATION_MICROS` 3d -> 15d.** Refuted: `starved` is `u8::MAX` when
  NOT starved, so any starved task outranks any non-starved one — raising the
  threshold EVICTS the query window from the privileged lane. 9 test failures.
- **Declaring output ordering in `narrow_provider`.** Refuted before building: the
  file order and the window's partition key are misaligned (see 1 above), so it
  would have been a no-op shipped into the row-deleting path.
- **Bin-scoped dedup instead of whole-date.** Refuted: duplicates are spread over
  ~18-35% of bins and `stage_dedup_chunk` re-reads every file a chunk touches, so
  the whole-date unit is roughly right-sized.

### Method notes that cost real time

- **`COUNT(*)` cannot probe dedup coverage** — count pushdown answers it without a
  Delta scan. `GROUP BY … HAVING count(*)>1` also lies: it reads THROUGH
  `DedupExec` and sees duplicates already collapsed.
- **A counter stuck at an exact value means SCOPE, not throughput.** 437 grants
  frozen = the producer never ran on the main table (the dedup cron skips
  rollup-declaring tables).
- **`git stash` no-ops on an already-committed change**, which silently made an
  A/B compare a build against itself. Verify the arm with a marker
  (`grep -c window_turn` must be 0 in the baseline).
- **`synth:whale` cannot validate scheduling** — a fixed 813-task backlog that
  always drains, so no reservation ever binds. Use the real journal
  (`docker cp <ctr>:/app/data/timefusion/.timefusion_meta/maintenance_tasks.json`).
- **A young process reads as fixed.** Check uptime before quoting any counter.

### Prior art (2026-09-02): ClickHouse already solved this, and we did it backwards

**ClickHouse ReplacingMergeTree deduplicates on the `ORDER BY` sorting key — not
the primary key — and requires `PRIMARY KEY` to be a PREFIX of `ORDER BY`.** The
whole point is that a merge can then dedup by streaming, with no sort.

Their documented pattern for exactly our tension (dedup key contains identifier
columns you don't want in the index):

```sql
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (tenant_id, user_id, device_id)  -- FULL dedup key: makes merges streaming
PRIMARY KEY (tenant_id, user_id)          -- lean sparse index, a prefix of it
```

**We have it backwards.** TimeFusion sorts files
`(timestamp, service, id, ...)` and dedups on `(timestamp, id)` — `service` sits
between the dedup keys, so no merge can stream and every rewrite pays a full
external sort. That is the cost measured tonight: the sort OOMs a 1 GB pool on one
204 MB bin and dominates ~96% of maintenance time.

**What this de-risks about option 1.** The objection to leading `sorting_columns`
with `(timestamp, id)` is that it would hurt reads that prune on `service`.
ClickHouse's answer is that the SORT key and the INDEX do not have to be the same
thing — you keep the full dedup key in the sort and serve pruning from a shorter
index. TimeFusion already has that separation: `schemas/otel_logs_and_spans.yaml:67`
records that point lookups are served by bloom filters and tantivy, not by the
sort. So the read-path objection is likely weaker than it looks, and it is
measurable with the existing latency matrix.

Still needs the matrix re-run before landing — but the design is the industry
norm, not a novel gamble, and the current layout is the deviation.

Sources:
- https://clickhouse.com/docs/engines/table-engines/mergetree-family/replacingmergetree
- https://docs.peerdb.io/bestpractices/clickhouse_datamodeling
- https://queryplane.com/blog/clickhouse-partition-by-order-by-primary-key-guide/

### And why option 1 could NOT be shipped blind (11th refutation)

`schemas/otel_logs_and_spans.yaml` documents what position 2 is for:

> Point lookups on `id`/`trace_id`/`span_id` are served by bloom filters +
> tantivy, and **`service_name` by the secondary sort column**.

So `resource___service___name` sitting at position 2 is LOAD-BEARING for
service-filtered pruning — one of the most common filters in an observability
product. Moving `id` ahead of it trades a dedup win for a read regression on those
queries. My earlier reading ("bloom filters and tantivy cover it") was wrong: that
clause covers `id`/`trace_id`/`span_id`, not service.

**This is a real trade, not an oversight to fix.** It needs numbers on both sides:

1. Dedup gain: how much of a `run-unit --op Dedup` is the `SortExec` (the sort
   OOMs a 1 GB pool on one 204 MB bin, so the ceiling is high).
2. Read loss: re-run the latency matrix with a service-filtered shape, which the
   existing matrix does not currently isolate.

ClickHouse's own resolution suggests a third option worth pricing first: keep the
sort dedup-key-leading AND recover service pruning from an index rather than the
sort (bloom/tantivy already exist for other columns; a service bloom is cheap).
That would get both, and is why their design separates sort key from index.

Not shippable overnight in any variant: every path changes what queries read.

### The deciding number: the sort is 81% of a dedup rewrite (5.4x ceiling)

`TF_BENCH_PARQUET=<204 MB real prod bin> TF_BENCH_POOL_MB=1024 cargo bench
--bench rewrite_throughput`:

| variant | secs | MB/s |
| --- | --- | --- |
| **scan only** | **2.2** | **94.24** |
| sort b2048 p1 | 13.2 | 15.49 |
| sort b256 p1 | 19.4 | 10.54 |
| sort b256/b2048/b8192 p8, b8192 p1 | **FAILED — external sort OOM at 1 GB** | — |
| **PROD (b256 p1 x13 slices)** | **11.6** | **17.58** |

**The sort is 9.4 s of prod's 11.6 s — 81% of the unit.** Removing it takes a
rewrite from 17.58 MB/s to ~94 MB/s: a **5.4x ceiling** on the operation that is
~96% of maintenance worker time. It also removes the OOM: every `p8` variant and
`b8192 p1` fail outright at a 1 GB pool, which is the same external-sort failure
prod has been fighting all month.

So the layout question is worth real money: **~5x on the fleet's dominant cost,
plus the memory hazard.** That is the 10x-class change; the claim reservation was
1%.

It still must be paid for on the read side — `service_name` pruning depends on the
secondary sort position — so the morning sequence is:

1. Service-filtered latency matrix (the read cost, currently unmeasured).
2. If the read cost is material, price ClickHouse's separation instead: keep the
   sort dedup-key-leading and serve `service_name` from a bloom index, which the
   codebase already does for `id`/`trace_id`/`span_id`.
3. Land whichever wins, then re-run this bench to confirm the sort is gone.

Both sides are now quantified except the read cost, which is one matrix run.

### The read cost is MEASURED, and it is near zero — the trade resolves in favour of the layout change

The blocker was "moving `service` after `id` regresses service-filtered pruning,
cost unknown". Measured on prod, read-only, 1-hour windows:

**Selectivity** — a service filter on `dcad860a` selects **50,114 of 50,333 rows
(99.6%)**. Timings identical (0.31 s filtered vs 0.33 s unfiltered): no pruning
benefit, because there is nothing to prune.

**Cardinality** — distinct services per busy project-day:

| project | services | rows |
| --- | --- | --- |
| 28f62f01 | **1** | 197,973 |
| 6297304f (whale) | 2 | 1,118 |
| dcad860a | 3 | 50,334 |
| 87576849 | 19 | 123,063 |

**Three of four busy projects have 1-3 services.** Sorting by `service` at
position 2 clusters data that is already homogeneous — it cannot prune what does
not vary. The schema comment ("`service_name` by the secondary sort column")
describes an intent the data does not support at current cardinality.

**So the trade is:** ~5.4x on dedup rewrites (the sort is 81% of a unit) and the
removal of the external-sort OOM, against a read cost that is measurably ~zero for
3 of 4 busy projects and modest for the fourth (19 services over 123k rows).

**Recommendation: make the change** — `sorting_columns` leading
`timestamp DESC, id ASC`, `service` after. `timestamp` stays the leading column,
so time-range pruning (what every dashboard query uses) is untouched. For
`87576849`-shaped projects, recover pruning with a `service_name` bloom index, as
the codebase already does for `id`/`trace_id`/`span_id`.

Not landed in this session only because it is a physical-layout change that
deserves a full suite run and a before/after on the latency matrix, and the
session is out of budget to do that properly. The EVIDENCE no longer blocks it —
this is now a scheduling question, not an open risk.
