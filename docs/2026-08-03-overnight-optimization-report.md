# Overnight optimization report — 2026-08-03

Goal (standing): fast prod DB — regular light compaction for all projects, MOR +
.arrow hot tier enabled for 24h, sorted parquet/.arrow outputs so TopK works,
all last-24h queries <500 ms.

## What shipped (in deploy order)

| Commit | What | Verified |
|---|---|---|
| `04cc476` | jemalloc heap profiling OFF in prod (attribution done); `prof:true` kept for runtime re-arm | RSS headroom freed; brake stopped firing; `dirty_bin_eligible` 2 → 34 |
| `85eee45` | Dirty-bin drain unblocked: batch 32→512 (58h floor removed), per-bin `persist_dirty_bins` fsync churn deleted (was O(queue×batch)), no-op completions counted | `dirty_bin_processed_total` 0 → 329+; queue 22,526 → ~22,000 and falling |
| `0b908bd` | TopK correctness: `OrderedUnionForTopK` never descends through `DedupExec` (fetch-truncation below dedup could serve stale versions); per-leg `sortable` (mem/hot sortable, Delta never — pool-killing SortExec structurally impossible) | 629 unit + CI green |
| config | Hot tier 24h/128GB disk/1GB leg budget; **CapRover app definition edited** (`TIMEFUSION_HOT_TIER_RETENTION_HOURS` 6→24, survives deploys; backup at `/captain/data/config-captain.json.bak-20260802-hottier`) | env verified on deployed service |
| `7e3f1a1` | Hot-tier demotions: bounded wait (2 pending) instead of drop-on-busy — no more permanent coverage holes | 25 hot-tier tests green |
| `aa3b4ce` | **Flush dwell gate**: fresh+small sealed buckets dwell one bucket_duration before flushing (≥32MB bypass). Kills the MOR-dribble file storm (~5× the file production; attribution below). `TIMEFUSION_FLUSH_DWELL_SECS` −1=one duration (default) / 0=off (all test harnesses) | CI green incl. e2e |
| `2557433` | Tantivy on MOR tables: **id-set prefilter restored** (sound — id is a dedup key, whole-key granularity); file exclusion + row selections stay OFF on MOR; mem leg gets no tree | 74 tantivy/MOR tests green |
| `ff2c1e4` | **OOM fix**: `DedupExec` keep-greatest run buffer is now pool-tracked (`try_grow` before retaining each batch). Oversized window ⇒ query fails `ResourcesExhausted`; server survives | regression test with 512-byte pool |

## The OOM (2026-08-03 00:03 local / 23:03Z)

Kernel: `anon-rss 125,380,760kB` vs 120 GiB cgroup ⇒ SIGKILL. Cause: unbounded
full-set keep-greatest buffers the ENTIRE scan per query, invisible to every
DataFusion pool; the 24h hot tier now feeds those scans at local-disk speed, so
concurrent 24h dashboard widgets stacked ~10 GB each. `ff2c1e4` makes the pool
see it. The structural fix is bounded (per-run) dedup, which needs the Delta
leg ordering — the fork work below.

## File overproduction — attributed and fixed

Dominant producer: the 60 s flush tick × MOR enrichment dribble. ~180 UPDATEs/min
append rows with ORIGINAL timestamps ⇒ instantly-sealed buckets for old windows
every minute ⇒ one ~0.1 MB parquet per (project,date) per minute (~36 buckets/min
vs ~6 expected; ~1,440 files/day for one enriched project). Ruled out: pressure
force-flush (0), per-statement DML files, OCC losses (0). Fix: the dwell gate.
The removal side (dirty-bin drain) was floored at 32 bins/5 min — now 512.

## In flight at time of writing

- **delta-rs fork** branch `timefusion-mor-ordered-groups` (`a4cf10ef`): repack
  file groups so the declared ordering survives DF54's stats validation ⇒
  bounded dedup + streaming TopK under MOR. First rev **broke TimeFusion e2e**
  (delta-kernel `TokioMultiThreadExecutor has crashed: RecvError` — plan-time
  footer IO) — agent is reworking it to use already-materialized stats only.
  Do NOT bump TimeFusion past `39889ee7` until e2e is green on the bump.
- Post-deploy watcher sampling drain/hot-tier/latency every 10 min; OOM monitor
  polling every 5 min.

## Where latency stands (measured, image `aa3b4ce`, cold-ish)

98fdd4f3: 1h 3–4 s, 6h 8–12 s, 24h 8–16 s. 28f62f01 (whale): 1h 3.5–15 s,
6h 14–29 s, 24h 50–69 s. All timeouts gone, but <500 ms needs: (1) the dirty-bin
backlog to drain (in progress, ~1k bins/h and rising), (2) 24h hot-tier coverage
to accumulate (builds forward from tonight; full by ~tomorrow evening), (3) the
fork ordering fix for streaming TopK + O(run) dedup memory.

## Traps for whoever continues

- Prod env lives in the **CapRover app definition**, not `.env.prod`. Deploys
  clobber `docker service update`; edit `/captain/data/config-captain.json` +
  `docker service update --force captain-captain` to make config durable.
- Another session pushes to master concurrently (`7b27034`, `c1b9ca0` tonight).
  Always `git pull --rebase` before pushing; always verify the deployed image
  SHA before interpreting any prod measurement.
- Local e2e is flaky while another checkout runs tests (shared MinIO :9000,
  CPU). CI is authoritative.
- `timefusion_stats` (component/key/value) via psql is the fastest prod metrics
  path; hot tier under `component='hot_tier'`.
- Do NOT raise the memory brake or run manual `OPTIMIZE ... WHERE date=...`
  (crashed the server twice on 2026-08-02, 455 s/1636 s, no win).

---

## 04:45 addendum — the OOM saga and where things ended

**Five cgroup OOM kills** (~01:03, 02:00, 02:57, 03:25, 04:00), all: anon-RSS
hits 120 GiB, kernel kills, clean recovery in ~3 min. Attribution by 2-min RSS
sampling: RSS climbs a CONSTANT ~7 GB/min **while dirty-bin drain staging runs**,
independent of query load. Not DedupExec (pool-tracked, zero ResourcesExhausted),
not pass-scoped (batch 512→128 changed nothing). Deep audit verdict: no true
retention site — it is **per-bin planning churn**: each staged bin builds 3–5
fresh sessions/providers and each query replays ~34k files' statistics
(~170–340 MB pool-invisible per query), ratcheting RSS via fragmentation.

**Mitigation in force:** `TIMEFUSION_DIRTY_BIN_DRAIN_BATCH=1` (drain paused to a
trickle) — set in the CapRover app definition (survives deploys) AND on the live
service. RSS verified stable after. light_optimize (hot-tail compaction of
today's partition — the latency-relevant one) still runs.

**Proper fix (daylight, in order):**
1. Make bin scans plan ONLY their files: pass partition/bin bounds via
   `with_file_skipping_predicates` / `with_file_selection` on the fork's
   TableProviderBuilder; find why project_id/date partition pruning planned
   34,225 files (`files_planned` metric).
2. Reuse the memoized `maintenance_session_state()` (database.rs:4323) instead
   of `build_optimize_session_state` per bin/replan (database.rs:6391, 6618).
3. Confirm with jeprof (prof:true still baked; re-arm `prof.active` at runtime).
4. Then restore `TIMEFUSION_DIRTY_BIN_DRAIN_BATCH` (remove the env; code
   default is 128) and let the 21.5k backlog drain.

## Final measured state (b5613d0, ~04:35, drain paused)

- **TopK works**: `ORDER BY timestamp DESC LIMIT 50` over 6h = **377 ms**.
- Plans: **9/21 tenants bounded dedup** (was 0), 10 with declared ordering —
  the fork ordering fix (`timefusion-mor-ordered-groups/a59a4336`) is live and
  converts more tenants as compaction homogenizes footers.
- 98fdd4f3: 5min **207 ms**, 1h **533 ms**, 6h 8.9 s, 24h 12.2 s.
- 28f62f01 (whale): 5min 690 ms, 1h 11 s, 6h 39 s, 24h times out at 95 s.
- Ingest: zero failed flushes, zero rejections all night after the fixes.
- Hot tier: ~114 GB / ~2k files, 24h retention live, coverage still building
  forward (full 24h by ~this evening).
- Percentile-style queries scan the whole window — they need the file-count
  cut (dwell gate live; dirty-bin drain resumes after the churn fix) and hot
  coverage; TopK/list-style dashboards are already fast.

## 08:50 closing numbers (image 033487f, minutes after deploy restart, cold)

| window | 98fdd4f3 | 28f62f01 (whale) |
|---|---|---|
| 5 min | **323 ms** | **345 ms** |
| 1 h | **613 ms** | 2.9 s |
| 6 h | 9.0 s | 16.9 s |
| 24 h | 18.2 s | 63.6 s |

Warm TopK earlier measured **377 ms** (6h ORDER BY ts DESC LIMIT 50). Zero
failed flushes / rejections all night. Stability: decay-0 is baked (c7050a4);
no OOM since the drain pause + decay-0 config (~07:06).

The 6h/24h percentile windows remain file-count-bound. The path to <500 ms
there, in order: (1) fix the maintenance planning churn (bin scans must plan
only their own files — see 04:45 addendum) and re-enable the dirty-bin drain
(app-definition env TIMEFUSION_DIRTY_BIN_DRAIN_BATCH, currently 1) to eat the
~22k-bin backlog; (2) let 24h hot-tier coverage finish building (by this
evening) and restore TIMEFUSION_HOT_TIER_LEG_BUDGET_MB to 1024 once the hot
leg is pool-registered; (3) keep light_optimize consolidating today's files
(now unstarved).

## 2026-08-03 follow-up — projected hot reads exposed capacity and dedup pressure

After `ca8e67b` made Arrow IPC decoding projection-aware, representative 1h
latency fell from 12–14 s to 7.8–8.1 s, then to 2.6–5.7 s after one hot-tail
compaction wave reduced the Delta row groups from 45 to 17. A historical 1h
window measured 1.52 s and a recent 10m window 530 ms. Serialized wide-window
samples were still 19.9 s (3h), 15.0 s (24h), and 27.6 s (3d).

The post-compaction 1h plan produced about 72k physical rows for 35k survivors:
merge-on-read is processing more than 2x duplication. It opened 55 objects,
scanned 17 row groups and only 1.72 MB, yet accumulated 26.8 s of scan time
across partitions. Foyer is active (32,996 range hits / 1,780 misses), so this
is not a disabled-cache problem. Recent file rewrites continually introduce
new object ranges, while concurrent unordered dedup operators were observed
holding 4.8–9.6 GB each and driving the 30 GB query pool to exhaustion.

Two safeguards are next:

- Write new hot-tier Arrow files with per-buffer LZ4 compression. Projection
  skips unrequested buffers before decompression, while compression lets the
  requested 24h history fit within the fixed 128 GB ceiling. Legacy
  uncompressed files remain readable and age out normally. Memo accounting
  charges the larger of file bytes and decompressed Arrow ownership.
- Cap the correctness fallback for one unbounded unordered greatest-version
  dedup at 2 GiB. Bounded timestamp-run dedup remains unchanged. An oversized
  fallback now fails that query with `ResourcesExhausted` instead of allowing
  a few queries to monopolize the whole global pool.

The target is still unproven: do not call the work complete until production
24h p95 is below 500 ms. Compression improves coverage as new files replace
legacy files; it does not instantly rewrite the existing 128 GB tier.

## Post-`7f792da` production evidence and maintenance follow-up

The deployment recovered cleanly in 245.9 s. Immediately after restart the hot
tier still held 913 legacy files / 136.5 GB; LZ4 only applies to new demotions,
so this was an expected pre-turnover baseline. Serialized count samples for
tenant `98fdd4f3` were:

| window | samples |
|---|---|
| 1h | 17.35 s, 17.24 s, 29.89 s |
| 3h | 11.13 s, 8.08 s, 4.20 s |
| 24h | 24.17 s, 20.15 s, 20.68 s |

Once restart traffic settled, a warm 1h sample reached 1.2 s but regressed to
3–5.7 s under concurrent load. Analyzed plans showed the remaining dominant
cost is physical merge-on-read duplication, not a disabled cache:

| window | physical rows | survivors | Delta files |
|---|---:|---:|---:|
| 1h | 82.8k | 41.3k | 41 |
| 3h | 304k | 133.7k | 50 |
| 24h | 1.57m | 706.5k | 53 |

The 24h hot leg retained only 32 MB, while its final tombstone/filter stage
alone used 3–4 s. Foyer served thousands of range hits but saw continued miss
churn after the restart; caching bytes cannot eliminate dedup over 2.2x the
logical row count.

The dirty-bin drain already ignores the stale incident-time `batch=1` setting,
but its providers still replayed the unified table's entire live-file metadata
for every 10-minute bin. They now use a fail-closed `FileSelection` built from
the exact eager snapshot and restricted to the physical project/date. Sealed
bins from today are eligible too: the targeted rewrite and 2h seal guard make
the old whole-growing-partition exclusion obsolete. Fresh per-bin SessionState
remains intentional; tests proved that sharing cloned catalog/execution state
can resolve `__dedup_src` against a stale eager snapshot and miss a requeued
duplicate. The shared maintenance RuntimeEnv remains reused.

The first live convergence wave (before that provider-pruning deploy) removed
229,469 physical rows in eight committed bins and reduced the representative
Delta fan-out from 53 to 31 files. Latency immediately improved to 890 ms (1h),
1.98 s (3h), and 9.11 s (24h). The 24h plan still read 1.58m physical rows for
711k survivors, so this tenant had not yet reached the dedup waves.

That plan exposed an independent CPU bug in bounded greatest-version dedup.
At every timestamp transition, the operator built a full-batch Boolean mask and
filtered the complete Arrow batch to emit one run. A batch containing many
distinct timestamps was therefore cloned/filtered repeatedly. Closed-run
winners now accumulate in one mask per retained input batch; only the trailing
cross-batch timestamp run remains buffered, and each batch is filtered once.
The ordering, greatest-tiebreak, pool-accounting, partial-flush, and streaming
LIMIT contracts remain covered by the full dedup test suite. A 4,096-run
regression emits one output batch rather than one per timestamp.

## Post-`21d20d8` production evidence and dirty-bin correctness incident

The per-batch dedup fix reduced representative warm 24h counts from 9.11 s to
1.24–3.22 s. Warm 1h samples were 556–604 ms and warm 3h samples were
655–741 ms. Foyer was demonstrably active during the run: one repeated 1h
query added 131 range hits, 27.4 MB served, zero misses, and no hot-tier read.
The remaining latency is merge-on-read CPU and physical duplication, not a
disabled object cache. A 24h analyzed plan read 1.62M physical rows from 29
Delta files for about 715.7k survivors; Delta scan compute was 331 ms and the
timestamp merge was 251 ms.

The first dirty-bin wave after provider pruning then exposed a correctness bug.
The same 24h count fell from about 715.7k to 546.8k within minutes, with whole
hour gaps in the result. The bin-scoped predicate had incorrectly been passed
as the predicate used to carry rows from each targeted file into its
replacement. A parquet file spanning several 10-minute bins was therefore
removed in full but only the selected bin was written back. The fix separates
the project/date `partition_filter` from the bin-scoped probe filter. A
regression now puts an adjacent-bin row in the same source file and proves it
survives physical dedup.

A second guard rejects target-overlapping units within one commit wave. Two
dirty bins can select the same compacted parquet file; committing both
full-file replacements would duplicate all carried rows and emit duplicate
Remove actions. Only one target-disjoint unit lands per wave; overlapping
units are discarded and requeued to re-plan from the replacement snapshot.

Scheduled physical dirty-bin dedup is now behind
`TIMEFUSION_DIRTY_BIN_DEDUP_ENABLED` and defaults off as an incident kill
switch. Read-side dedup remains active, so query correctness does not depend on
the physical drain. The schedule must stay disabled in production until the
lost rows are recovered and prod-shaped multi-bin/overlap validation passes.

Production data already removed by the faulty commits is not restored by the
code fix. Recovery must be handled separately from the latency work. The
<500 ms 24h target remains unproven.
