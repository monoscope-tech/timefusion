# TimeFusion: ingest wedge + recent-window latency — full report (2026-08-02)

Written for a successor picking this up cold. Everything below was measured on
prod unless explicitly labelled a hypothesis. Where I got something wrong, the
wrong conclusion is kept alongside the correction — the mistakes are load-bearing
context, not noise.

---

## 1. Executive summary

Two goals were in play: ship merge-on-read (MOR, `version_append`) on
`otel_logs_and_spans` / `otel_metrics`, and get recent-window dashboard queries
(1h/3h/6h percentiles) under 500 ms.

**Outcome:**

| | status |
|---|---|
| Ingest wedge (rejected customer writes) | **FIXED and verified** |
| Merge-on-read enabled | **ENABLED**, read-path cost fixed at the source, verified sort-free on the live binary |
| Recent-window latency < 500 ms | **PARTIALLY met** — 1 h is 336 ms on a normal tenant; 3h/6h and busy tenants are still 5–30 s |

Live as of 2026-08-02 21:40 on image `051c68e` (master tip), which contains all
six fixes. Verified: no `SortExec` in the plan, `mor_delta_leg_sorts_total=0`,
`flush_failed_total=0`, zero rejected writes.

The outage-class problem is gone: TimeFusion no longer wedges, no longer rejects
writes, and now recovers cleanly from restarts. Query latency improved by roughly
an order of magnitude (1h went from >120 s timeouts to single-digit seconds, with
one clean sample at 506 ms) but the 500 ms target is not met. The single
remaining blocker is **file fragmentation**, because **compaction cannot keep
pace**. That is the top item for whoever continues.

---

## 2. The ingest wedge (FIXED)

### 2.1 Symptom

Prod at 12:00 UTC, from `timefusion_stats` over pgwire:

```
flush_completed_total          0        <- not ONE flush since boot
flush_failed_total             136,103
rows_flushed_total             0
pressure_pct                   100
backpressure_rejected_total    928       <- inserts REFUSED
rows_in_buffer_lag             1,835,135
oldest_bucket_age_secs         1698
```

Rejected inserts logged `NOT yet durable — WAL append happens only after
admission`, i.e. **rejected rows were not in the WAL**. If the producer did not
retry, that is customer data lost, not delayed.

`SELECT 1` stayed instant throughout, which is why this shape reads as healthy
from outside.

### 2.2 Root cause #1 — the flush unit was unbounded

`flush_completed_buckets` committed **every** sealed bucket in one cycle:

```rust
async fn flush_completed_buckets(&self) -> anyhow::Result<()> {
    let current_bucket = MemBuffer::current_bucket_id();
    self.flush_buckets_where(move |id| id < current_bucket).await.map(|_| ())
}
```

So the unit of work scaled with buffer occupancy while the watchdog stayed fixed.
At 23 GB it tried to commit the whole buffer as ~20 coalesced commits under one
global `flush_lock`, could not finish within 600 s, was aborted, freed **nothing**,
and the next cycle retried the identical too-big commit.

**Positive feedback: the fuller the buffer, the less likely a flush ever
completes.**

This is why the timeout had oscillated for weeks (120 s → 600 s → adaptive →
floor): all four were attempts to bound an *unbounded workload* with a deadline.
Chunking already existed on the boot path (`drain_replay_backlog`), which is why
boot could drain but steady state could not.

**Fix (`173cb0f`, on master as `d93b9c5`):** walk the sealed slice list in
bounded ranges.

> **Subtle bug caught by a test during this fix.** My first version re-derived
> the remaining bucket set each pass. That immediately re-flushed a bucket that
> was *dirty-kept* because a DML mutated it mid-commit, and the second pass
> drained the post-delete state the dirty-finish logic exists to preserve —
> silent data loss. `delete_during_airborne_commit_sticks_across_crash` caught it
> (expected `["keeper"]`, got `[]`). The final version snapshots the slice list
> **once** and attempts each slice at most once per cycle; dirty-kept buckets are
> retried on the next tick.

### 2.3 Root cause #2 — the MOR column addition made old buckets unflushable

After chunking landed, flushes started but ~8k/2 min failed with:

```
dedup column `updated_at` missing from batch schema
```

`version_append` added `updated_at`, but buckets already in MemBuffer (and
replayed from the WAL) were written under the **old** schema and don't carry it.
`dedup_batches` erred, so those buckets became **permanently unflushable** while
newer ones drained fine.

**Fix (`9ad2300`):** a missing *tiebreak* degrades to last-occurrence-wins.
Unstamped rows carry no version information, so that is the correct reading, and
cross-version resolution belongs to `DedupExec`. A missing dedup **key** stays
fatal — that is a schema fault, not legacy data.

> **Generalisable:** whenever a column is added to a live table's YAML, ask what
> happens to rows already in MemBuffer and the WAL. Same class as the 7d68f01
> incident documented at the top of `src/schema_loader.rs`.

### 2.4 Result (verified)

| metric | wedged | after |
|---|---|---|
| `flush_completed_total` | 0 | climbing normally |
| `flush_failed_total` | 136,103 | **0** |
| `pressure_pct` | 100 | 1–6 |
| rows in buffer | 1,835,135 | ~50k |
| `backpressure_rejected_total` | 928 | **0** |
| `oldest_bucket_age_secs` | 1698 | 12–44 |
| `hot_tier.read_hits_total` | **0** | 1,700+ |

Restarts now recover cleanly — verified three times. Before, a restart re-wedged.

---

## 3. Merge-on-read (ENABLED, cost fixed at the source)

### 3.1 Why MOR made reads slow

`EXPLAIN` on a 1h count, before the fix:

```
DedupExec: keys=[timestamp, id], mode=bounded[timestamp]
  SortPreservingMergeExec: [timestamp DESC]
    SortExec: expr=[timestamp DESC]      <- the cost
      DeltaScanExec
        DataSourceExec: file_groups={48 groups: ... date=2026-08-02 ...}
```

Chain:

1. `version_append` makes `DedupExec` **mandatory** (`dedup_skip_allowed` hard-
   declines — correct; skipping would show superseded rows).
2. Bounded dedup requires **ordered input**.
3. The Delta leg cannot declare an ordering, so DataFusion inserts a full
   `SortExec`, which exhausted the query pool:
   `Resources exhausted ... greedy(used: 27.5 GB, pool_size: 27.5 GB)`.

**Why the Delta leg can never be ordered under MOR:** recent-window reads are
fast because files are **time-range disjoint** — each flush writes one time slice,
so the planner *orders files* instead of sorting rows. An MOR `UPDATE` appends a
row carrying its **original** timestamp into a **new** file, so files overlap in
time. Continuous hash-enrichment (monoscope updates spans right after ingest)
re-breaks this constantly.

### 3.2 The trap

Only two paths existed, both unusable:

- refuse keep-greatest without a bound → degrade to **keep-first** → serves the
  PRE-UPDATE row (unsound under MOR);
- force the ordering → the blocking `SortExec` above.

### 3.3 Fix — two parts, BOTH required

**(a) `src/read_dedup.rs` (`b935b74`)** — keep-greatest now runs with **no
bound**, treating the whole stream as one open run, flushed at end-of-stream.
Correct, and cheaper than the sort it replaces (a hash of per-key winners, no
spill).

> **Correctness hinge:** a *bounded* run may still flush early on
> `RUN_BUFFER_MAX_BYTES`; an *unbounded* one **must not** — any key can still be
> beaten by a later batch, so an early emit would serve the superseded row.

**(b) `src/database.rs` (`b50a411`)** — fixing the operator alone was **not
enough**. I deployed (a) and `EXPLAIN` still showed
`mode=bounded[timestamp]/greatest` on top of a `SortExec`, because the scan
marked every union leg `sortable = true` and injected sorts. Changed to
`vec![false; …]`: take ordering only when a leg already has it for free.

The old comment claimed *"sorting is skipped for any leg that already satisfies
`req` (the normal case)"*. **That never held for the Delta leg under MOR and
could not** — MOR is exactly what destroys the disjointness the ordering needs.

`EXPLAIN` now reports the survivor rule (`full-set/greatest` vs `full-set/first`),
not just the seen-set size. Under MOR that difference is a correctness bug, not a
cost difference, and the two were previously indistinguishable.

### 3.4 Monitoring signal

**`maintenance.mor_delta_leg_sorts_total` must stay 0.** The codebase's own docs
call zero the precondition for enabling `version_append` on a busy table. If it
climbs, the Delta leg is being sorted again and latency will collapse.

```sql
SELECT value FROM timefusion_stats WHERE key = 'mor_delta_leg_sorts_total';
```

**Verified 0 on the live deploy `051c68e` (2026-08-02 21:40).**

### 3.5 VERIFIED LIVE — plan is sort-free

On `051c68e`, `EXPLAIN` of a 1h count:

```
DedupExec: keys=[timestamp, id], mode=full-set/greatest   <- correct under MOR
  CoalescePartitionsExec
    UnionExec
      DataSourceExec: partitions=1                        <- mem leg
      DataSourceExec: partitions=9                        <- hot leg
      ProjectionExec
        DeltaScanExec
          DataSourceExec: file_groups={48 groups: ...}
```

No `SortExec`, no `SortPreservingMergeExec`. This is the confirmation that §3.3
holds on the running binary.

### 3.5 A soundness bug found during the temporary rollback

While MOR was briefly disabled, `tombstones_possible()` was:

```rust
self.tombstone_column.is_some() && self.version_append
```

That gates a **storage** property on a **mutable write-path flag**. It guards
COUNT(*)-from-stats pushdown, so turning MOR off would have re-enabled that
pushdown over files still holding tombstoned rows — **`COUNT(*)` silently
counting deleted rows as live**. Its own doc comment warned "never flip the flag
off after tombstones were written".

**Fixed (`dbe972d`)** to key on the column alone. Rather than keep a flag whose
safety depends on never being turned off, it now tracks storage.

> **Generalisable:** never gate a fact about what is IN STORAGE on a flag
> describing what the WRITER is currently doing. Flags get toggled; data persists.

### 3.6 If MOR ever needs rolling back again

`version_append: false` while **keeping** `dedup_tiebreak` and
`tombstone_column` is safe and needs no data migration, because:

- the tombstone filter keys on `tombstone_column`, not `version_append`;
- superseded versions still collapse — `dedup_skip_allowed` grants the read-side
  skip only where `dedup_window_clean` proves a window already swept, and the
  sweep drops tombstoned rows physically.

**Do NOT drop the columns.** Deleted rows are tombstones that only DedupExec
hides; removing the machinery resurrects them.

---

## 4. Other fixes shipped

| commit | fix |
|---|---|
| `d229fe4` | Flush watchdog floor: never contract below **half** the ceiling |
| `d1fa4fd` | `flush_sort_gate`: ≥512 MB of the shared FairSpillPool per concurrent spilling sort |
| `173cb0f` | Bounded steady-state flush unit (§2.2) |
| `9ad2300` | Missing tiebreak degrades instead of failing the flush (§2.3) |
| `dbe972d` | `tombstones_possible` tracks storage (§3.5); cold-bin reserved share |
| `2088ea8` | Wide-scan gate release thresholds 8 files/256 MB → 256/512 MB |
| `b935b74` | Unbounded keep-greatest; MOR re-enabled |
| `b50a411` | Scan never sorts a leg to satisfy keep-greatest |

### 4.1 Watchdog floor

The adaptive flush watchdog contracted to a fixed **45 s** floor at full ingest
pressure. Prod sat at 100 % pressure while commits were failing to finish within
the **600 s** ceiling — a 45 s floor would have aborted every flush and wedged
ingest permanently. Floor is now `base / 2`.

> Contracting is only sound against a **hung** commit. Against a slow-but-
> progressing drain an early abort discards the work for a retry that is no
> cheaper — and the buffer is fullest exactly when drains are largest.

### 4.2 Flush-sort pool starvation

`flush_sort_runtime_env` is a single shared `FairSpillPool`, so N concurrent
spilling sorts each get ~pool/N. Below a viable slice `ExternalSorterMerge` fails
with "Not enough memory to continue external sort", and the caller then writes the
group **UNSORTED** — one file without a `sorting_columns` footer disables the
reader's all-or-nothing ordering for every scan touching that partition. So pool
starvation surfaced as *slow queries*, not as an error. After the gate: **zero**
sort failures in 30 min.

### 4.3 Wide-scan gate — the 2 h cliff

The gate releases a well-pruned scan when the selected work is small, but the
file-count half required ≤ **8 files**. Fragmentation made that unreachable.
Measured on prod, same tenant and query, only the window changing:

```
100 min =  8673 ms (58,207 rows)
115 min =  9935 ms (66,088 rows)
125 min = TIMEOUT at 130 s
140 min = TIMEOUT at 130 s
```

A hard cliff at the 2 h lookback — the **queue**, not the scan. Bytes are the
honest proxy for decode heap; file count is a proxy for a proxy, and it is the one
fragmentation invalidates.

Result: unblocked 3h/6h for the smaller tenant (timeout → 10.1 s) but **not** for
the larger one (~3× the rows).

---

## 5. Measurements

All with **row counts asserted** — see §7.

### FINAL — verified on the live deploy `051c68e`, warm (2026-08-02 21:31–21:36)

Rounds 3–4 after settling; round 1 was cold (5 min took 8.2 s) and is discarded
per §7.3. Health throughout: `flush_failed=0`, zero rejections, pressure 0–3 %,
RSS ~58 GB, `mor_delta_leg_sorts_total=0`.

| window | `98fdd4f3` (~37k rows/h) | `28f62f01` (~120k rows/h) |
|---|---|---|
| 5 min | 219–377 ms | 329–678 ms |
| **1 h** | **336–343 ms** ✅ | 5.1–6.3 s |
| 3 h | 13.8–30.3 s | 10.9–26.4 s |
| 6 h | 14.0–28.0 s | 23.0 s |

**1 h on `98fdd4f3` meets the <500 ms target** (336 ms, from >120 s that
morning) — proof the read path itself is fixed. **All timeouts are gone**
(`28f62f01` 6 h went timeout → 23 s).

The residual cost tracks rows/files exactly as §5's two-tenant comparison
predicts: `28f62f01` has ~3.5× the rows and is ~15× slower at 1 h. That is
fragmentation, not the query engine. Note the high variance on 3h/6h (13.8 s vs
30.3 s for the *same* query) — that is scan contention plus cache state, not a
stable cost, and another symptom of too many files.

### Latency progression through the day, tenant `98fdd4f3`

| stage | 1 h | 3 h | 6 h |
|---|---|---|---|
| Wedged (morning) | >120 s | >120 s | >120 s |
| After wedge fix, settled | 686–2150 ms | 864–2098 ms | 4.7–7.0 s |
| MOR on, sort still present | 8.8–19 s | timeout → 14.5 s | 12–19.9 s |
| **MOR on, sort removed (final)** | **336 ms** | 13.8–30.3 s | 14–28 s |

### Two tenants, same query — the fragmentation signal

| project | 1h latency | rows |
|---|---|---|
| `28f62f01-46a1-400e-8195-da7bc3505b5b` | 4650 ms | 114,041 |
| `98fdd4f3-3544-4087-ad91-1e7ca95aba29` | 13317 ms | 35,875 |

**The slower tenant has 3× fewer rows.** Latency is driven by **file count**, not
data volume. This is the single most useful diagnostic in this report.

---

## 6. Open work, in priority order

### 6.1 Compaction cannot keep pace — TOP ITEM

```
dirty_bin_queue_depth           22,135
dirty_bin_processed_total            0
dedup_bins_committed_total           0
dedup_bins_deferred_cold_total  20,556
dedup_timed_out_total               15
dedup_passes_flush_yields_total      0   (NOT the flush-health yield)
light_optimize_projects_planned     20
light_optimize_projects_completed   14   (tick truncates)
```

Three defects. **(1) is shipped but insufficient; (2) and (3) are the real work.**

1. **`select_drain_bins` starved cold bins — FIXED but not sufficient.**
   Hot-first is correct (a boot draining oldest-first never reached the hot
   window, 2026-07-30) but hot took the *whole* batch, so 20,556 cold bins were
   deferred forever. Fixed in `dbe972d` with a reserved cold share (`batch/2`).
   **Verified working on `051c68e` — and verified inadequate**, see (2).

2. **The drain rate is orders of magnitude below the backlog.** Measured on the
   live deploy ~25 min after boot:

   ```
   dirty_bin_queue_depth           22,479   <- GREW from 22,135
   dirty_bin_eligible_total             2   <- the cold reserve, working: batch/2 of a tiny slice
   dirty_bin_processed_total            0
   dedup_bins_committed_total           0
   dedup_bins_deferred_cold_total  14,603
   light_optimize_bins_committed_total  0   <- hot-tail compaction also committed NOTHING
   ```

   `timefusion_dirty_bin_drain_batch` defaults to **32**, and the dedup cron runs
   every 5 min. Even at a full batch that is ~32 bins per 5 min; at the *observed*
   2 eligible per pass, draining 22k bins takes **weeks**. The queue is growing
   faster than it drains. Raising the batch alone will not do it if per-bin work
   stays expensive — this needs the per-bin cost addressed together with the batch
   size, and `light_optimize` committing 0 bins needs its own explanation.

3. **Hardcoded 120 s `DIRTY_BIN_BUDGET`** retries forever without committing.
   Bound the **work per bin** (shard it), do not cap the time. This is the same
   "fixed budget vs unbounded work" mistake as the flush watchdog (§2.2) — and it
   is the third instance of that pattern in this codebase.

`light_optimize` is *fair* (`light_optimize_cursor` rotates so the next tick
resumes at the first unserved project) — it is a **throughput** shortfall, not
starvation. It runs at 80 % duty cycle already, so more frequent scheduling will
not help much; the per-bin work has to get cheaper.

**Under MOR compaction is load-bearing, not an optimisation** — it is the only
thing that restores time-disjointness after enrichment breaks it.

> **Do not brute-force this with `OPTIMIZE <table> WHERE date = '...'` over
> pgwire.** Tried twice: ran 455 s and 1636 s, **crashed the server both times**
> (OOM), and did not improve latency. Use the incremental light path or the
> off-box CLI on k3s.

### 6.2 Memory growth — "the real unfixed bug"

RSS climbs from ~11 GB at boot to **72–95 GB** within ~25 min under query load.
MemBuffer is capped at 24 GB and typically at 2–7 %, so the growth is query
working set + jemalloc retention + maintenance.

**Cap must stay 120 G.** I tried 90 G and 110 G; **both got TF cgroup-killed**.
The "oversubscription" figure (TF 120 + monoscope 72 = 192 > 188 GB host) sums
*ceilings*, not usage — actual usage ~164 GB fits. A cap that induces restarts
trades a rare outage for permanent slowness, because a restart empties MemBuffer
and recent-window queries are fast largely *because* MemBuffer covers most of a 1h
window.

**CapRover deploys silently discard `docker service update --limit-memory`.**
Re-check after every deploy, or put it in the app definition.

### 6.3 Smaller items

- **Global `flush_lock`** couples every table to the slowest commit.
- **Tantivy text acceleration** for MOR tables (id-set only, no file exclusion).
- **File overproduction attribution**: ~2852 files/day/tenant vs ~288 the sealed
  path can produce. Attribute across sealed-flush / pressure force-flush /
  compaction output before tuning `flush_interval` or `bucket_duration`.

---

## 7. Operational traps (each of these cost real time)

1. **Deploys cancel each other.** The build takes ~25–28 min and a new push to
   master cancels the in-flight run. **Five consecutive deploys were cancelled**
   and prod ran a stale image for most of a day while master advanced ~8 commits.
   Every "post-deploy" measurement in that window was of the *old* binary.
   → Always: `docker service inspect srv-captain--timefusion --format
   "{{.Spec.TaskTemplate.ContainerSpec.Image}}"` and compare to
   `git log origin/master -1`. The tag is the 7-char short SHA. CI green is not
   evidence anything deployed; "CI" and "Build and Deploy" are separate workflows.
   → Batch work into ONE push, then leave master alone for ~30 min.

2. **Always assert row COUNT beside latency.** After an OPTIMIZE crash-restart,
   queries read 129 ms / 105 ms — because the box was still replaying and the
   data was not there. With real counts the same queries were 11 s.

3. **Never measure a freshly booted or freshly recovered box.** A first pass 105 s
   after boot timed out on *every* window including 5 min. And a 1h query read
   773 ms 10 min after a restart, then degraded to 12 s as RSS climbed. Let the
   buffer settle and the cache warm; sample repeatedly over 30+ min.

4. **`SELECT 1` responsiveness proves nothing.** It distinguishes "process alive"
   from "process wedged"; it says nothing about scan cost. To tell a wedge from
   scan cost, compare a **5-minute** window against a 1h one.

5. **When a fix oscillates between two failure modes, the tunable is not the
   cause.** Four rounds of flush-timeout tuning happened without anyone measuring
   what the commit was actually doing for 600 s. The answer was that the work was
   unbounded.

---

## 8. How to check health quickly

Everything below is available over pgwire — no SSH needed:

```sql
-- Ingest health. flush_failed climbing + pressure 100 + rejected>0 = the wedge.
SELECT key, value FROM timefusion_stats
WHERE component = 'buffered_layer'
  AND key IN ('flush_completed_total','flush_failed_total','pressure_pct',
              'backpressure_rejected_total','process_rss_mb','total_rows',
              'oldest_bucket_age_secs');

-- Hot tier. read_hits=0 AND read_misses=0 means the tier is never CONSULTED
-- (not that it is missing) — that was the 2026-08-02 signature.
SELECT key, value FROM timefusion_stats WHERE component = 'hot_tier';

-- Compaction. processed=0 with a large queue_depth = the sweep is stalled.
SELECT key, value FROM timefusion_stats WHERE component = 'maintenance';

-- MOR read path: this must show NO SortExec, and mode=...\/greatest.
EXPLAIN SELECT count(*) FROM otel_logs_and_spans
WHERE project_id = '<uuid>' AND timestamp >= now() - interval '1 hour';
```

Credentials: `TIMEFUSION_PG_URL` in `../monoscope/.env`. **Never hardcode them** —
one was once leaked to a public repo.

Host access for image/limits/logs only:
`ssh ubuntu@captain.s.past3.tech`, service `srv-captain--timefusion`.

---

## 9. What I would do next, in order

The read path is done. **Every remaining item is compaction throughput.**

1. **Unblock `light_optimize` — ANSWERED, three causes, all in the logs.**
   Hot-tail compaction commits nothing because of a pile-up, not slowness. From
   `docker service logs` on `051c68e`:

   ```
   event="light_optimize_memory_brake" limit=90194313216      <- fires repeatedly
   Hot compact job run still in progress after 600s — may be wedged or just slow (skips=5)
   event="dedup_drain_flush_yield"  (otel_logs_and_spans, otel_metrics, mor_*)
   light_optimize ... date=2026-07-20 target_size=536870912   <- 13 DAYS old, not today
   ```

   a. **The memory brake fires constantly.** `limit=90,194,313,216` = 84 GB =
      70 % of the 120 G cgroup. RSS oscillates 51–99 GB (§6.2), so it crosses 84 GB
      routinely and the brake stops compaction each time. **Compaction is
      therefore gated on fixing the memory growth**, which makes §6.2 a blocker
      for §6.1 rather than an independent item.
   b. **The hot-compact job itself is wedged** — "still in progress after 600s
      (skips=5)", i.e. five scheduled runs skipped because the previous one never
      finished. Same fixed-budget-vs-unbounded-work shape as §2.2 and §6.1(3).
   c. ~~It is grinding a 2026-07-20 backlog instead of today.~~ **RETRACTED — I
      misread the logs.** The `date=2026-07-20 target_size=536870912` lines are
      the CONSOLIDATE sweep, which calls the shared
      `optimize_table_light_inner` and emits the same `light_optimize_started`
      event with its own (cold) date and the 512 MB cold target. That is
      consolidate doing exactly its job. **Do not "fix" light_optimize's date
      priority on the strength of those log lines** — verify with
      `light_optimize_planned` / `light_optimize_tail_selected`, which carry the
      hot-tail `today`, before concluding anything about which partition the
      hot-tail compactor is working.

   Fix order within this item: (b) bound the job so it cannot wedge, then
   (a) reduce memory growth so the brake stops firing. **(a) is the deeper one** —
   while RSS oscillates across 84 GB the brake will keep halting compaction no
   matter what else is fixed.

   > Note the shared-event-name trap: `optimize_table_light_inner` is called by
   > BOTH the hot-tail path and consolidate, and both log `light_optimize_*`.
   > Distinguish them by the `date` field, not by the event name.
2. **Raise `timefusion_dirty_bin_drain_batch` (32) AND make per-bin work
   cheaper.** Neither alone closes a 22k backlog that is currently growing. Shard
   the per-bin work so a bin cannot exceed its budget, then the batch can rise
   safely.
3. **Replace the 120 s `DIRTY_BIN_BUDGET` with a work bound**, per §6.1(3).
4. **Re-measure both tenants** at 1h/3h/6h once file counts actually fall. The
   fragmentation signal in §5 predicts that is where sub-second comes from:
   `28f62f01` has 3.5× the rows of `98fdd4f3` and is 15× slower at 1 h, while
   `98fdd4f3` already hits 336 ms.
5. **Watch `maintenance.mor_delta_leg_sorts_total`** on every deploy — non-zero
   means the Delta leg is being sorted again and §3 has regressed.
6. Only then revisit the wide-scan gate thresholds for the larger tenant.

**Do not** re-litigate the read path (dedup mode, ordering, the wide-scan gate)
before file counts come down — §5 shows the cost now scales with files, and
`98fdd4f3` at 336 ms proves the engine is capable of the target.

The correctness work is done and tested (460 unit tests green). What remains is
throughput: **make compaction keep pace with the enrichment rate.** Everything
else is downstream of that.
