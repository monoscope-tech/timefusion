# Post-hot-tier speed plan — measured, not ideas

2026-08-21 afternoon. The hot tier is removed (823746a). This plan replaces
the retired hot-leg-pruning doc; every lever below carries a prod
measurement (raw: session scratchpad `lever_measurements/`) or an ecosystem
citation, and several proposed levers are explicitly KILLED by measurement.

## Measured verdicts

| lever | measurement | verdict |
|---|---|---|
| "Add parquet blooms for point lookups" | Blooms ALREADY EXIST on 7 columns (id, parent_id, name, trace_id, span_id, session_id, user_id; `bloom_filter: true` in schema YAML, `bloom_filter_on_read=true`) and FIRE HARD: row groups 263→2 (P1), 77→1 (P2) on trace lookups | **DEAD — already shipped.** Only gap: `attributes___user___email` |
| Delta-only 24h point lookup (the post-tier shape, window aged 48-72h) | P1 2.9s cold → **1.05s warm**; P2 **26.0s cold → 1.28s warm** — identical 77-file scan both times | Warm is target-adjacent TODAY. Cold = ~100-200 serial object-store opens at ~300ms each. **The whole problem is cache coverage x file count** |
| 7d delta-only lookup | P1 6.0s; P2 TIMEOUT — the 2-9d window sits exactly outside foyer `ttl=7d` / `cache_recent_days=8` | **Foyer policy mis-sized for the query mix** |
| Fragmentation | 197-217 files per project-day (old AND recent partitions); fleet ~57 files/scan | **Compaction VALIDATED** — the multiplier blooms cannot fix |
| Foyer headroom | l2 130GB used of 600GB budget; host 1.1TB free; 73-75% hits while warming; disk cache SURVIVES restarts | **VALIDATED — cheapest big win.** "Cold" = never-queried window, not post-restart |
| Certification / dedup skip | cert_granted_total=0; denials 100% never_certified; dedup_denied_uncertified == dedup_eligible (5078) → EVERY eligible scan pays DedupExec | **VALIDATED + live diagnosis item** — slice-coverage is deployed yet grants nothing |
| "Compact the rollup tier" | 30d rollup count: **845ms**, 60 files (2/day), 28KB scanned | **DEAD — already compact.** The ~300ms-class dashboard path exists via rollups now |

## Operative plan (ordered by measured $/ms)

1. **Foyer coverage for the tier-less world** (config + small code):
   - raise `disk_gb` toward host headroom (600 → ~900GB of the 1.1TB free)
   - extend `ttl_seconds` past 7d and `cache_recent_days` 8 → ~35 (the 7d
     TIMEOUT cell is literally the ttl boundary)
   - ensure footers + bloom bytes of every live file stay admitted in the
     metadata cache so cold windows pay data GETs only, never discovery GETs.
   Precedent: Thanos/Mimir index-headers (derived, local, mmap'd, never
   re-fetched); Tempo's dedicated bloom/footer cache at ~90% hit rates.
2. **Warm-path toucher**: background task walks last-N-days footers+blooms
   per live project post-boot and on commit (KBs/file), so the 26s
   first-touch cell cannot happen. (Quickwit hotcache-first protocol,
   translated to Foyer.)
3. **Compaction to <20 files/project-day** (from ~200): tighten intra-day
   light-optimize cadence + keep sealed consolidation at pace. At 1.05-1.28s
   warm with ~200 files, ~20 files puts semi-cold lookups in-target too.
   (IOx: files born clean, compactor maintains non-overlap; ClickHouse:
   part-count pain from small inserts — same lesson.)
4. **Certification: diagnose why grants=0, then let dedup skip fire** —
   removes DedupExec from 100% of eligible scans. Suspects: coordinator
   dedup units not completing (300s deadline; OOM-retry noise in logs), or
   completing without reaching record_clean_slice's keying. End-state per
   IOx: per-file-group overlap check, dedup only where ranges overlap.
5. **Wide aggregates default to rollups** (845ms/30d measured) — remaining
   work is routing coverage of more shapes, not tier repair.
6. **email bloom**: one-line YAML (`bloom_filter: true` on
   attributes___user___email) at the next natural deploy.

## Ecosystem synthesis (citations)

- Tempo: per-block sharded blooms (FPP 0.01, 100KiB shards) gate BLOCK
  SELECTION before planning; bloom/footer caches ~90% hit.
  grafana.com/docs/tempo — block-format, configuration, backend_search.
- Loki 3.3: bloom sidecar BLOCKS built by background builders over declared
  keys only, data files untouched — the retrofit template.
  grafana.com/docs/loki — operations/bloom-filters; 3.3 release blog.
- ClickHouse: skip indexes = per-granule sidecars; blooms effective exactly
  when IDs cluster by time within parts (ours do); FINAL affordable only
  when the filter bounds the merge — our immutable-columns/pushdown fix is
  the same move; OPTIMIZE FINAL is an anti-pattern (never global-rewrite to
  fix dedup). clickhouse.com/docs — skipping-indexes, avoid-optimize-final;
  Altinity RMT post.
- InfluxDB 3/IOx: recent window served from ingester RAM (our MemBuffer);
  files BORN clean (dedup+sort at persist); compactor maintains
  non-overlap; planner drops dedup per non-overlapping file group — the
  finer-grained certification end-state. influxdata.com IOx architecture
  blog; docs storage-engine reference.
- Thanos/Mimir: downsampled tiers (5m@40h, 1h@10d) are THE wide-window path;
  index-headers derived locally and mmap'd. thanos.io — compact, store,
  binary index-header.
- Quickwit: hotcache — one ~0.1% blueprint object per split; cold needle
  query = O(candidate splits) small GETs, split open ~60ms.
  quickwit.io — Quickwit 101, architecture.

Cross-cutting: every fast-on-object-store system (a) separates INDEX from
DATA FILE and caches the index locally, and (b) treats "no dedup needed" as
a maintained file-layout invariant checked at plan time. Items 1-2 are (a);
item 4 is (b).


## Verification (2026-08-21 evening, deployed 2f5d221)

The paced body warm ran at full scope (per-table warms of 2,434 / 2,235 /
4,596 / 696 files; foyer L2 130->152GB; hits 97.6-98.7%). The formerly-cold
band (24h trace lookups aged 48-72h, previously 23-31s / TIMEOUT):

| project | pre-warm | first touch | steady |
|---|---|---|---|
| dcad860a (P1) | 23.4s | 4.5s | **1.2s** |
| 6297304f (P2) | 28.9s | 3.7s | 3.7s |
| 28f62f01 (P3) | 31s TIMEOUT | 6.8s | **1.1s** |

~20-28x improvement to steady state on two of three; P2's 3.7s floor is
per-query file count (its partitions), i.e. item 3 (compaction), not cache.
Slice-coverage persistence deployed same day (grants accumulate across
restarts now; expect cert_granted_total to move within hours-days as sealed
days complete their slice sets — dedup-skip is the next step change for
count shapes). Remaining ordered work: compaction to <20 files/project-day,
dedup-deadline/unspillable-sort ticket, rollup routing coverage.
