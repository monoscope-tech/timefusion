# Five levers to 500 ms — plan, measurements, execution log

Goal: queries across all timeranges for the top-10 projects complete in 500 ms,
including 7-day windows. The list shape already meets this (2026-09-15:
`perf/delta-leg-spm` took 7d reads from 24–45 s to 0.2–1.6 s). What remains is
the aggregate/dashboard shape (1.3–36 s at 7d) and first-read cold latency.

Standing constraint for every item: **no work that voids existing
certifications or rollups, or manufactures a maintenance backlog.** Any step
that would move a partition fingerprint (file rewrites, re-footering) is
census-and-report only until its cert impact is understood.

Baselines (2026-09-15, laptop psql incl. ~150–200 ms RTT):

| shape | 7d today | goal |
|---|---|---|
| list (`ORDER BY ts DESC LIMIT 100`) | 0.2–1.6 s (warm 0.2–0.5 s) | met |
| aggregate (`time_bucket 1h count`) | 1.3–36 s | 500 ms |
| fleet pgwire p95 / p99 | 76–252 ms / 94–590 ms | hold |

## 1. Certification durability (THE lever)

98% of scans cannot skip merge-on-read dedup; rollup misses are 58% not_built +
35% stale_coverage. The 09-14 diagnosis: proofs have a ~36-minute half-life —
production rate is irrelevant while proofs die faster than they accumulate.
#290/#294/#296 attack exactly this (span-disjoint retention, whole-day proof
survival, read-skip from accumulated coverage) and grants moved 1→11 in hours.

- [ ] Measure on the current build over a quiet stretch: `cert_granted_total`
      rate, `cert_dwell_p50`, `dedup_skipped_pct`, and the rollup miss mix.
- [ ] If dwell still ~minutes: instrument WHAT invalidates each proof
      (coverage_reset reasons), fix the top destroyer. Iterate.
- [ ] Success: `dedup_skipped_pct` climbing day over day; aggregate 7d falling.

## 2. Rollup coverage that survives maintenance (watermark + real-time union)

stale_coverage misses are maintenance invalidating coverage the read path
needs. The TimescaleDB shape — durable watermark over sealed rollup rows, union
a real-time tail computed from raw — lets dashboards read pre-aggregated rows
plus a small hot tail, never 7 days of raw spans. Biggest item; design doc
first, execute after 1 stabilizes (they overlap: certified days feed rollups).

- [ ] Design doc: where the watermark lives, how reads clip to it, how the
      tail unions (the pieces exist: rollup read path, mem∪delta union).
- [ ] Success: `rollup_miss_stale_coverage_total` ≈ 0; aggregate 7d ≤ 500 ms.

## 3. Lying-footer census (census FIRST — repair moves fingerprints)

The SPM fix exposed footer-declared orderings that are false at scale
(`ordering_violations_delta` ~630/min steady-state). Harmless for correctness
(bound column stays in the dedup key) but every lying file forfeits streaming
reads and bounded dedup. **Rewriting liars voids certs — forbidden for now.**

- [ ] Census: which tables/projects/dates carry the violations (per-leg + probe
      counters, sampled EXPLAIN/scan). Report distribution, no rewrites.
- [ ] Fix the SOURCE if it is still writing lies today (sort-at-write is
      already prod: `repair_sorted_at_write_total` — verify new files honest).
- [ ] Old liars: leave to the existing repair lane's natural cadence.

## 4. Stop manufacturing duplicates (landed-skip validation)

~8–14% of rows in sampled windows are same-id re-inserts from WAL replay after
unclean exits (58% of duplicate groups, 09-02). `TIMEFUSION_LANDED_SKIP_ENABLED`
declines provably-committed flushes on a dirty boot; shipped dark because the
skip only fires after an unclean restart, which prod (read-only host) cannot
stage.

- [ ] Local/e2e validation: force a dirty boot (kill -9 after flush, replay),
      assert `wal.landed_skips` > 0 and zero data loss (row-set equality).
- [ ] If green: propose enabling in prod env (user flips it — env change
      restarts prod, batch with a deploy).
- [ ] Success: manufactured-duplicate share falls after the next unclean exit;
      `wal.landed_skips` / `wal.replay_rows` observable.

## 5. Hygiene: failure metric + cold reads

- [ ] `pgwire.stream_failed` exists in `timefusion_stats` but as a maintenance
      counter, log-only history. Ensure query FAILURES are a first-class
      counter (component=pgwire) so an outage like 09-15's is visible between
      incidents. Trivial, ship first.
- [ ] Cold first reads are 2–5x warm (916 ms vs ~300 ms) right after each
      deploy — footer/metadata fetches. Foyer metadata pre-warm exists
      (`warm_footer`, `TIMEFUSION_WARM_FULL_FILES=true` in prod): measure what
      cold reads still fetch, close the gap only if it is not already covered.

## Execution order

5a (failure metric, trivial) → 1 (measure, then fix top proof-destroyer) →
4 (local validation) → 3 (census) → 5b (cold reads) → 2 (design, then build).

## Log

- 2026-09-15: plan written. perf/delta-leg-spm (38db0748) verified in prod;
  ordering_violations spike diagnosed sound (un-laundered lying footers).
