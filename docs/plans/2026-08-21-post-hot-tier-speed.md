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


## Addendum: the immortal-unit family (2026-08-21 night, deployed 1824627 + 525f6ec)

The compaction/capacity blocker was diagnosed to a FAMILY of loops keeping
whole-day 0.65-1.1TB Repair units alive at attempts=140-211 for days on
project 87576849:
1. Every timeout split the parent, but the 60s planner re-mint resurrected
   the Superseded parent (attempts intact, backoff floor cancelled,
   quarantine tag erased) and its day width outranked its own children —
   across 24h of logs no child EVER started. Fixed: a Superseded parent with
   live descendants is never resurrected; worker_error retries keep their
   floor (1824627).
2. Post-fix the loop persisted at ~2min cadence WITHOUT timeouts: the 1.1TB
   estimate can never pass resource admission, and the admission retry was
   the one retry path with no split and no backoff escalation. Fixed:
   resource_admission is a capacity failure; retry_task routes through
   retry_or_split; split-refused repeats escalate exponentially (525f6ec).
Expected effect: poisoned days bisect to admittable sizes, quarantine slots
free, sealed consolidation reaches the backlog — the direct path to P2's
3.7s floor and the remaining wide-window TIMEOUTs. Residual: deterministic-
failing files (footer_repair_quarantined) still need off-box
`optimize --recompress`; convergence itself is now unblocked but is
wall-clock work over days.


## Final night status (2026-08-22 ~01:00)

Post-fix verification revised the picture once more: the surviving day-wide
Repair starts at ~2.8min cadence are NOT a loop — Repair takes ONE candidate
file per claim (take(1)) and requeues with `compaction_debt_remaining` while
debt remains; attempts is a loop counter on this path. The unit is
progressing (~21 files/h on 87576849). With resurrection (1824627) and the
admission hot-loop (525f6ec) both closed, the maintenance tier is draining.

Time-gated residuals (nothing further is code-actionable tonight):
- cert_granted_total: still 0 — needs full clean-slice day-sets to complete;
  evidence now durable across restarts. RE-CHECK after ~24h; if still 0 with
  completed sealed dedup days, diagnose the grant keying.
- Repair/consolidation backlog: 529 pending days at ~1 file/2.8min/worker —
  days of wall clock; timeout rate should fall as day units bisect.
- P2 3.7s floor + remaining wide-window TIMEOUTs: re-run
  bench_per_project/run_bench.sh after ~24-48h of drain.
- Deterministic-failing files (footer_repair_quarantined): off-box
  `timefusion optimize --recompress` maintenance window.
- Observability gap worth one small PR: `journal.retry()` logs nothing — the
  night's two hardest diagnoses both stalled on invisible retry reasons.


## Deploy-test-attribute cycle CLOSED (2026-08-22 ~01:00, bench_final_cycle/)

Full 100-cell matrix + 24 EXPLAIN ANALYZEs on 525f6ec, per-operator
attribution of every miss (raw: scratchpad bench_final_cycle/matrix.md):

- Scorecard: completed 46 -> 75/100; 24h<1s 0 -> 13/25; 30d<10s 4 -> 8/25;
  combined targets 4/50 -> 21/50 vs the 08-20 baseline.
- Attribution splits cleanly: (a) file-count/IO owns most misses (P1 B 30d:
  27.2s wall / 0.02s compute / 746 cold opens; A-shapes: zero FILE-level
  pruning, blooms then cut row groups 352->2); (b) dedup amplification owns
  every E miss (cert still 0, DedupExec 5.36M->3.55M at 24h, 31.98M->23.73M
  at 7d); (d) one anomaly: P5 A 24h 5.9s WARM over 43 files/6.8MB.
- Measurement trap: shape D (kind='SPAN') returns count=0 everywhere — its
  fast cells are zero-match stat pruning, not count-pushdown evidence.
- Iteration shipped from the attribution: warm depth 9d -> 35d (the largest
  per-cell delta was beyond-warm-depth cold opens).
- Ranked remaining: (1) cert unblock [accumulating via persistence; re-check
  keying if still 0 after clean day-sets], (2) FILE-level trace_id pruning
  (Tempo/Loki-style sidecar for the delta leg — the earlier design, now with
  EA evidence), (3) sealed backlog drain [wall-clock], (4) shipped, (5) P5
  A-shape warm anomaly (5.9s/43 files — roundtrip overhead, investigate
  before optimizing).

## Cert instrumentation shipped (cdaadc4) — and the hypothesis to test with it

`cert_slice_{outside_day,dirty,partial,day_covered}` +
`cert_refused_{dropped,incomplete,empty,fp_moved}` are now in
`timefusion_stats`. Read them together; the slice counters should sum to
`record_clean_slice`'s call count.

**Hypothesis: `cert_slice_partial` dominates, via fingerprint RESET rather
than genuinely-incomplete coverage.** `record_clean_slice` accumulates clean
intervals until they cover the UTC day, but keyed on one unmoved file
fingerprint — `if entry.fp != fp { *entry = SliceCoverage { fp, intervals:
vec![(start, end)] } }` (`maintain.rs`). Any change to the partition's file
set discards every interval accumulated so far. For today's partition that is
correct (ingest churns files). For sealed days it should be stable — except
compaction, repair and rollup rewrite them continuously, and there is a
529-day repair backlog doing precisely that. If so, certification and the
maintenance tier are in a livelock: the work that makes a day clean is also
the work that keeps resetting the proof that it is.

**Known gap in what shipped:** `cert_slice_dirty` catches the partition moving
*during* a pass; the reset above happens *between* passes and is not
separately counted, so a dominant `cert_slice_partial` will not by itself
distinguish "still accumulating" from "repeatedly reset". If the first read
points at `partial`, add `cert_slice_fp_reset` (one counter, in the `entry.fp
!= fp` branch) before theorising further. Deliberately not stacked onto this
deploy — one change per deploy, and the first read may already be decisive.

## Cert diagnosis CLOSED (2026-08-22) — the premise was wrong

**`cert_granted_total = 0` was a red herring.** It is a process-scoped counter
and prod restarts constantly (5 deploys in ~90 min during this diagnosis), so
it reads 0 on a young process no matter how well certification works. Every
prior fix — coordinator deadline, day-wide units, restart-straddling
persistence — was aimed at a mechanism that was not broken.

Read the durable sidecar instead (`.timefusion_meta/dedup_certifications.json`,
NOT the data-dir root — read-only via `sudo cat` on the host; note `ubuntu`
does have sudo, contrary to CLAUDE.md):

- **97 certifications across 13 projects**, granted continuously from
  2026-08-12 through 2026-08-22 00:54 — i.e. minutes before this read.
  Certification works and is actively granting.

The exit counters shipped in `cdaadc4` are still useful and agree: over 33 min,
`cert_slice_partial=8`, `cert_slice_dirty=4` (all four → `cert_refused_dropped`,
i.e. real duplicates found), `cert_slice_day_covered=0`.

### What actually blocks the skip: contiguity, not grants

The read path refuses the skip if **any** in-window partition lacks an entry
(`maintain.rs:1056`). Certified dates per project, and the longest consecutive
run:

| project | certified dates | longest consecutive run |
|---|---|---|
| 94c5dc1f | 11 | **5** |
| 8100121c | 11 | 3 |
| 28f62f01 | 9 | 4 |
| 98fdd4f3 | 9 | 4 |
| 6297304f (whale) | 7 | 3 (newest 08-16) |

**A 7-day query needs 7 consecutive certified dates. The best any project
achieves is 5. A 30-day query needs 30.** So `dedup_skipped_pct = 0.0` despite
97 live certifications — the grants are real but never form a contiguous
window, and they decay as compaction/repair move file fingerprints on sealed
days, punching holes faster than slices fill them.

Corroborating, from `dedup_slice_coverage.json`: 51 days in flight, median
**12.5%** of a day covered, max 87.5%, and **zero** at ≥99%. New day-grants
complete slowly, so runs grow slower than they are broken.

### The fix follows directly, and it is not more instrumentation

**Make the skip per-date (or per-file-group) instead of all-or-nothing over the
window.** With 5-day runs already existing, per-date skipping would convert
today's 97 grants into real savings immediately, with no change to how
certification is earned. This is the IOx model already named in this file's
own end-state ("per-file-group overlap check, dedup only where ranges
overlap") — it was listed as the ideal and is in fact the unblocking change.

Second, stop invalidating certifications that maintenance did not need to
disturb: a compaction that rewrites an already-clean sealed day moves the
fingerprint and voids the proof, which is the livelock in the small.

**Do not add `cert_slice_fp_reset`** (queued in the previous section) — the
question it was to answer is now answered by the sidecars, and it would cost a
deploy to learn nothing new.
