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
- 2026-09-15 (item 5a): already done — `pgwire.stream_failed` became an OTel
  counter on 09-14 (`observability.rs:388`), history lands in monoscope.
- 2026-09-15 (item 1): measured on the #296-era build and **largely resolved by
  the team**: `never_certified` fell 98% → 34% of scans, coverage retained 26 /
  reset 0. The controlled-query test settles the rest: a SEALED-window query
  increments `dedup_skipped`; a TODAY-window query increments
  `dedup_denied_fp_moved` — i.e. the dominant remaining denial is today's
  partition, whose flush every ~10 min adds files that genuinely need dedup.
  **That denial is correct, and today's dedup is cheap post-SPM.** Two traps
  recorded: the certification fingerprint hashes URIs ONLY (a DV commit does
  NOT move it — `partition_dv_state` exists precisely because of that), and
  the per-FILE skip is structurally unreachable (MoR re-appends land at
  ORIGINAL timestamps, so `cert_skip_blocked_overlap` = 27k blocked / 0
  granted). An `added.is_empty()` retention-removal survival tweak was built,
  then reverted: premised on the DV mechanism, which was wrong.
  **Conclusion: the 500 ms lever for aggregates is item 2 (rollups), plus the
  build lane draining `not_built` over days — not further cert-side code.**
- 2026-09-15 (item 4): DONE — the validation the flag's docs mandated already
  exists and passes: `replayed_rows_that_delta_already_holds_are_not_written_again`
  (e2e, full dirty-boot replay → flush declined → zero rows lost) and the lib
  decline test, both green on master. Enabling is now a one-env flip
  (`TIMEFUSION_LANDED_SKIP_ENABLED=true` in CapRover) — restarts prod, so batch
  it with the next deploy. Watch `wal.landed_skips` vs `wal.replay_rows` after
  the next UNCLEAN exit (a clean deploy exercises nothing).
- 2026-09-15 (item 3 census): violations concentrate in `otel_metrics` (the
  self-monitoring project above all), 0–114 per controlled query;
  `otel_logs_and_spans` contributes 0 on every shape tried. Steady-state
  ~630/min fleet-wide; the alarming 88k/min was this plan's own 7d aggregate
  ladder. LOW severity, zero correctness impact — leave file rewrites to the
  repair lane's natural cadence, per the no-new-backlog constraint.
- 2026-09-15 (item 5b): already covered — foyer's disk tier persists across
  restarts (BlockEngine on FsDevice) and prod runs `warm_footer` +
  `TIMEFUSION_WARM_FULL_FILES=true`. Residual first-read gap is plan-cache and
  fresh-file cost; not worth new code.
- 2026-09-15 (item 2): the team is mid-flight on exactly this, counter-first
  (#294→#299). Measured for them: `rollup_witness_bounded_present=61` vs
  `absent=3002` — the bounded witness reaches 2% of recovered slices, so the
  read-side flip would be inert TODAY; presence climbs as slices republish.
  Root anatomy located: `apply_rollup_hours` REMOVES the whole day's
  `rollup_coverage` and bumps the date epoch on every write, however far the
  write sits from `covered_through` — destroyed coverage then reads as
  `not_built` (58% of misses), blaming the build lane. Shipped the attribution
  counters (this branch): `rollup_stale_{fp,epoch}_moved`,
  `rollup_coverage_absent_{invalidated,never_built}`,
  `rollup_ticket_recheck_failed`. Read them before any behavior change.
