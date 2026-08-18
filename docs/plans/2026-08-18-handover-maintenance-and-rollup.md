# Handover: rollup routing and maintenance throughput

Written 2026-08-18 09:20 UTC, at the end of an overnight session, by the agent
who did the work. Deployed SHA at handover: `da03e6a`.

**Read this first: the goal was not achieved.** Maintenance does not keep up.
14d/30d dashboards are not fast. The one metric that defines the goal —
`rollup_min_contiguous_days` — was 0 when this started and is 0 now. Everything
below is written so the next person does not repeat the parts that did not work.

---

## 1. The goal

Standing goal, as given:

- Maintenance (dedup, sort, rollups) must keep up with traffic, and support 10x
  project growth on the same server.
- Rollups must be always up to date and always used, so that popular 14d/30d
  dashboard queries are fast — **1 second is the ideal limit, especially 30d** —
  for **all** projects including shipbubble.
- Continue the tantivy reindex to a conclusion.

The measurable form of that goal:

| target | metric | required |
| --- | --- | --- |
| 30d dashboards fast | `rollup_min_contiguous_days` | 30 |
| maintenance keeps up | drain rate vs enqueue rate | drain >= enqueue |
| tantivy done | `tantivy_uncovered_files` | 0 |

---

## 2. Where everything stands (measured 2026-08-18 09:18 UTC)

```
rollup_min_contiguous_days        0        <- THE goal metric. Unmoved.
tantivy_uncovered_files       3,031        <- was ~3,003. Unmoved/drifting.

tasks_pending                38,035        <- was 25,129 at session start
tasks_complete               11,514
pending_base_rollup          15,809
pending_dedup                12,894
pending_hot_packing           5,348
pending_sealed_consolidation  2,115
pending_derived_rollup        1,628
pending_repair                  373

rollup_hits_full              1,019
rollup_hits_hybrid              303
rollup_misses                 3,915
rollup_output_rows_total      5,180        <- was 6 before #169
```

Throughput, measured over 206 minutes (05:11 -> 08:37 UTC):

```
drain    2.0 tasks/min
enqueue  3.9 tasks/min
net     +1.9/min  -> the queue DIVERGES
```

**Even eliminating every timeout only buys ~1.45x. Closing the gap needs ~2x.
Incremental fixes of the kind shipped here cannot get there.** That is the
single most important conclusion in this document.

---

## 3. What was achieved

Real, verified in production:

| change | effect | evidence |
| --- | --- | --- |
| #153 jemalloc `dirty_decay_ms` 0 -> 10000 | kernel memory mgmt CPU ~15% -> ~3.8% | before/after `perf record`; `smp_call_function_many_cond` and `flush_tlb_func` went to zero, `native_queued_spin_lock_slowpath` 5.41% -> 0.61%, with `md5::compress` flat as a control |
| #156 `md5` -> `hash_bucket` in dedup sharding | 5.87% of all CPU, the largest single symbol, now absent from the profile | `perf report` before/after |
| #155/#159/#160/#166 rollup routing | routing went from **0 hits ever** to hits; cross-project overview ~19s -> 8.0s | `rollup_hits_*`, timed query |
| #169 untagged base files | `rollup_output_rows_total` 6 -> 5,180 | derived publications went from 16-of-16 at `rows=0` to non-zero |
| #168 abandon backoff floor | repair timeouts 7 per 15 min -> 2 per 30 min | log counts |
| #142 tantivy oversized | 917 permanently-blocked files unblocked | `tantivy_oversized_skipped` 917 -> 0 |

**None of this is visible to a user.** No dashboard got faster. The work was all
upstream of the thing that matters.

---

## 4. What was NOT achieved, and why

### 4.1 Coverage depth — the actual goal

The 1h tier (what 14d/30d queries read) holds **3-6 days per project spread
across a 21-day span**. A 30d panel needs 30 *contiguous* days. Roughly **390
project-days** are needed; about 60 exist.

Why it did not move: building coverage requires `BaseRollup` -> `DerivedRollup`
units to complete, and completions run at 2.0/min against 15,809 pending base
rollups. It is a throughput problem, and throughput did not improve.

### 4.2 Maintenance keeping up

Never achieved. The queue grew from 25,129 to 38,035 during the session.

### 4.3 Tantivy

Untouched beyond #142. Gated on the same capacity.

---

## 5. Things that were tried and DID NOT work

This section is the most valuable part of the document.

### 5.1 Reweighting the scheduling cycle (#167) — no effect

**Hypothesis:** of the 10-slot maintenance cycle, 6 slots went to work that
`dependencies_complete` proves cannot advance coverage (dedup, hot packing,
sealed consolidation, repair) and only 4 to the rollup chain. Give the rollup
chain 6.

**Result:** the ratio changed *exactly* as designed — verified in the logs at 40
BaseRollup / 20 DerivedRollup / 10 each of the rest — and **throughput did not
move at all**. `pending_base_rollup` went 15,490 -> 15,518 over 44 minutes.

**Lesson:** slot allocation was never the binding constraint. Per-unit cost is.
Do not tune the cycle again without first reducing unit duration.

### 5.2 Removing dedup's hash sharding (#161) — net negative

**Hypothesis:** dedup split each rewrite into K hash-bucketed passes (K = 84-256
observed) that each rescan the same files. The streaming branch plans as
`BoundedWindowAggExec(mode=Sorted)` over a spillable `SortExec`, so it is already
bounded and the sharding is redundant. Verified by EXPLAIN.

**Result:** the sharding did stop (the `dedup_rewrite_sharded` event went
silent). But **dedup timeouts went from 3 per 15 min to 15 per 30 min** — 2.5x
worse — because one long pass overruns dedup's 300s deadline where an individual
shard did not.

**Status: this is a live regression.** Either raise dedup's deadline to the 900s
the other rewrites get, or revert #161. See section 7.

### 5.3 Tuning the dedup byte estimate — refuted before shipping

**Hypothesis:** `est_decoded_bytes = max(rows x 4096, compressed x 12) x 2` was
wildly over-estimating (a 130 MB file was estimated at 9,191 MB), inflating K.
Plan was to lower `bytes_per_row`.

**Measurement (#157):** `actual_decoded_mb=515` against
`predicted_decoded_mb=491`. The estimate slightly *under*-states. A 77x
compression ratio is simply real for ZSTD over repetitive Variant/JSON columns.

**Lowering those constants would have caused OOMs.** This was caught only
because the measurement was shipped before the "fix".

### 5.4 Attributing the OOM to a wide scan — wrong

Claimed a 32.8 GB scan explained the 125 GB OOM. It did not: that query ran 55
minutes earlier, and there were **zero** oversize scans in the 7 minutes before
the kill. See section 6.3 for what was actually running.

---

## 6. Root causes found (all real, all fixed unless noted)

### 6.1 Four silent refusals in rollup routing, each hiding the next

Routing had **never produced a hit**. Four independent defects, each only
visible once the one above it was removed:

1. **#155** — routing required `project_id = <literal>`; monoscope's overview
   query GROUPS BY it. **2,870 of 2,948 misses.**
2. **#159** — `date_trunc('hour', timestamp) = X` is a *window*, but was
   classified as a residual row filter. Same query, 18 minutes later at the next
   SHA, now failing as `filter_not_eligible`.
3. **#166** — coverage is INTERSECTED across projects, so one project short of
   coverage refused the query for all of them. Project
   `00000000-0000-0000-0000-000000000000` (the largest tenant) did exactly that
   for all 11 projects in the window. Fixed by splitting: covered projects route,
   uncovered read raw. The two legs partition (project x time) exactly.
4. **#169** — a base file with **no slice tags** was dropped outright, so history
   written before tagging existed could never reach the coarse tier. 16 of 16
   derived publications were `rows=0`. Now pruned on its own timestamp
   statistics instead.

### 6.2 The miss taxonomy was lying (#164)

`not_built` was **5,454 of 5,516** misses — not because rollups were unbuilt,
but because `recover_rollup_coverage` never repopulates the *date-level*
coverage map after a restart (only slice coverage). That lookup misses for every
date on any restarted process, and it poisoned `miss.unwrap_or(<real reason>)`.

Over an hour was spent chasing that artifact. It also means **rollup reads work
in production only because `TIMEFUSION_ROLLUP_REALTIME_TAIL` happens to be on** —
with it off, the strict path refuses every window on a freshly restarted
process. That is undocumented and worth revisiting.

### 6.3 Repair units were burning 44% of all maintenance capacity (#168)

7 Repair units timing out at 900s inside a 15-minute window = 6,300 of the
14,400 available slot-seconds. They cannot be bisected (a repair unit's cost is
the *file* it rewrites; time-bisection cannot shrink a file set whose members
all span the day), so they were requeued whole with a backoff capped at 256s:
burn 900s, wait 256s, burn 900s — a ~78% duty cycle forever on units that never
produce anything. Fixed by flooring the backoff at the operation's own deadline.

### 6.4 The OOM driver is probably a JOIN, not a scan

Prod OOMs (~125 GB anon, exit 137, `tokio-rt-worker`) recur every 7-15 hours:
08-17 09:39, 08-17 16:39, 08-18 07:27. **Pre-existing, not caused by this
work.**

In the 90 seconds before the 07:27 kill: 453 SELECTs, 44 UPDATEs, dominated by
**287 executions** of monoscope's service-map rollup
(`monoscope/src/Models/Telemetry/ServiceGraph.hs:666`, dispatched from
`BackgroundJobs.hs:3662`):

```sql
sp   AS (SELECT ... FROM otel_logs_and_spans WHERE project_id = ? AND timestamp >= ? AND timestamp < ?)
hops AS (... FROM sp c JOIN sp p ON c.tid = p.tid AND c.par = p.sid       -- self-join
          UNION ALL ... WHERE NOT EXISTS (SELECT 1 FROM sp c ...)          -- anti-join
          UNION ALL ... WHERE NOT EXISTS (SELECT 1 FROM sp p ...))         -- anti-join
```

`sp` is referenced **six times**. DataFusion inlines CTEs rather than
materialising them, so each execution builds a hash self-join plus two
anti-joins over every span in the window. `dispatchServiceMapRollups` fans out
one job **per project per 5-minute bucket**, concurrently.

**The scan guard structurally cannot see this** — it measures scan bytes, and
these are small 5-minute windows. The memory goes into the joins. A size cap on
scans would not have prevented this OOM.

It is a *background job*, not a customer query, so throttling its concurrency is
a much safer lever than refusing user queries.

### 6.5 Separately: one query really does scan 32.8 GB

```sql
select metric_name, count(*)::bigint from otel_metrics where project_id = $1 group by metric_name
```

`selected_files=514 selected_mb=32822 threshold_mb=1024`, no time filter, no
LIMIT. It reads the entire history of `otel_metrics` for the largest tenant to
list distinct metric names. Ran once in the observed hour. Not the OOM cause,
but 32x the oversize threshold and trivially avoidable (add a time bound, or
answer it from metadata).

---

## 7. What I would do next, in order

### 7.1 Fix or revert #161 (live regression)

Dedup timeouts are 2.5x worse than before this session. Two options:

- **Raise dedup's deadline** from 300s to the 900s the other rewrites get. The
  original rationale for 300s ("its memory is bounded by input size, unlike a
  rollup") stopped being true when #161 made it a spillable streaming pass.
  **But this only helps if those units finish inside 900s** — if they need
  2000s, it turns 15x300s of waste into 15x900s. **#172 (open, unmerged) logs
  exactly this**: `maintenance_unit_slow` reports elapsed vs deadline for any
  unit using more than a quarter of its budget. Merge it, read the distribution,
  then decide.
- **Or revert #161** and accept K rescans.

### 7.2 Profile ONE base-rollup unit end to end

**This is the highest-value unknown.** 16 slots at 2.0 completions/min means the
average unit takes ~8 minutes. Nobody has ever profiled where those 8 minutes
go. Every throughput conclusion downstream depends on it, and it was never done.

Note `rollup_scan_duration_ms_total`, `rollup_staging_duration_ms_total`,
`rollup_commit_duration_ms_total` and `rollup_end_to_end_duration_ms_total` all
exist as counters and all read **0** — they are declared but never populated.
Populating them is probably the cheapest way in.

### 7.3 Stop enqueueing work that cannot advance the goal

`dependencies_complete` proves `BaseRollup` depends on nothing and
`DerivedRollup` only on `BaseRollup`. Dedup, hot packing, sealed consolidation
and repair — **19,730 of 38,035 pending tasks, 52%** — cannot advance coverage.
The backfill enqueues 31 days x 13 projects x every tier regardless.

A targeted backfill of only the (project, day) pairs the 30d queries actually
need is ~390 units instead of ~16,000. That is a bounded, enumerable set, and it
is the direct path to the goal rather than draining a queue that is mostly
irrelevant to it.

### 7.4 Verify against the real thing, not a gauge

**No 30d query was ever timed during this session.** Tier-depth gauges and hit
counters were used as proxies throughout. Before declaring anything fixed, run
an actual 30d dashboard query for a real project and time it. Be careful: doing
this on the busiest project caused an OOM earlier (that one *was* self-inflicted).

### 7.5 Consider the service-map job (section 6.4)

Bounding its concurrency, or materialising `sp` instead of letting DataFusion
inline it six times, is the most likely fix for the recurring OOM.

---

## 8. Process mistakes worth not repeating

- **~25 PRs merged and deployed in one night, each restarting a single-process
  database.** Dedup units take longer than the gap between deploys, so in-flight
  work was repeatedly killed. Measured: 0 dedup commits on a 30-minute-old
  process vs 7 on a 2-hour-old one, with zero failures and zero timeouts — the
  fingerprint of interruption, not of a stall. **Batch the merges; leave prod
  alone for hours before believing any throughput number.**
- **Optimising proxies instead of the goal.** Hit rates and tier depths went up;
  no user-visible query got faster.
- **Not questioning the line of attack.** Each fix was defensible in isolation.
  The aggregate never had a path to 30 contiguous days, and that was knowable
  from the arithmetic on day one.

---

## 9. Open PRs at handover

- **#172** — log slow-unit durations. **Merge this first**; it unblocks 7.1.
- **#171** — corrects the now-stale `maintenance_rollup_untagged_input` log text
  (it still says the rows "can never reach this tier", which #169 made false).
- #103 — unrelated, someone else's CI change.

## 10. Reference

Related documents in `docs/plans/`:

- `2026-08-18-dedup-rescans-the-partition-k-times.md` — the K-amplification
  analysis, and the correction that the byte estimate is accurate
- `2026-08-18-plan-close-the-latency-goal.md` — the plan this session followed,
  including what cross-project routing does and does not buy
- `2026-08-17-why-maintenance-lag-becomes-query-death.md` — the chain from
  maintenance lag to the OOM kill
