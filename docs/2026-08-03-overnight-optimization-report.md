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
