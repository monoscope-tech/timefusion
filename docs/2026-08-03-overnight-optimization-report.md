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
