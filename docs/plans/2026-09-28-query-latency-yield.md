# Maintenance yields to query latency

Status: design, not built. Ships after W42, when the maintenance lane is measurable again.

## Problem

Maintenance concurrency degrades client queries, and the current yield does not see it.

- Median pgwire p95 by units in flight (W32 step 2, pre-18:55 processes on 2026-09-27, measured):

  | Units in flight | 0–10 | 10–20 | 20–30 | 30–45 |
  |---|---|---|---|---|
  | Median p95 | 180 ms | 327 ms | 458 ms | ~1000 ms |

  Time of day confounds this (r = 0.18).
- Throughput does not need that concurrency. Processed bytes are flat at 160–178 MB/s from 10 to 45+ units. Above ~30 units, each extra unit adds only 0.14 cores.
- `queries_starving` (`database/mod.rs`) fires only when `scan.heavy_query_queue_timeout` moves, i.e. after a query has already waited 30 s for a heavy slot. It yielded 0 times in the windows where p95 was worst.
- 208 "blocking section held a runtime worker" warnings in 40 minutes show a direct mechanism.

## Signal

- `PGWIRE_LATENCY.recent_ms(0.95)`: end-to-end pgwire latency over the last 1–2 minutes (`LatencyHistogram`, `observability.rs`). It is user-facing and already windowed.
- Count only windows with at least `MIN_SAMPLES = 50` queries. A quiet window keeps the previous state, so a few slow ad-hoc queries cannot throttle the box.
- `recent_ms` rotates the window when it is read. The exporter and the yield ticker both read it. Rotation is time-based, so two readers only change which read triggers it, not the window length.

## Control law (AIMD on the admission ceiling)

- **Shut:** p95 ≥ 800 ms for 2 consecutive 30 s ticks. The effective maintenance ceiling is multiplied by 0.75 per tick, down to a floor.
- **Reopen:** p95 ≤ 400 ms for 2 consecutive ticks. The ceiling grows by +2 units per tick, up to the configured maximum.
- **Hold:** between the two thresholds, leave the ceiling unchanged. That band is the hysteresis.
- **Floor:** 8 units, so backfill always progresses. At the measured slope that is about 16 cores of maintenance.
- **Thresholds:** 800 ms is about the midpoint between the 20–30 unit band (458 ms) and the 30–45 unit band (1000 ms); 400 ms sits just under the 20–30 band. So the loop settles near 20–30 units, where throughput per core is best.

## What it throttles

- **New admissions only.** Units already running finish, because cancelling them wastes their work.
- **Mechanism:** a third multiplier in `AdmissionController::try_acquire_for`, next to `lag_scaled_cpu_ceiling`. The live ceiling is the minimum of the lag-scaled ceiling and the query-scaled ceiling.
- **Resource capped:** the `object_reads` dimension, i.e. units in flight, because each unit takes exactly one read token. CPU tokens are left alone, since they price bytes rather than units.
- **Exempt:** today's HotPacking and flush-adjacent work, because freshness and MemBuffer pressure outrank query latency. Rollup and sealed Dedup/compaction are throttled.
- **Coexistence:** the existing `claim_yields_to_queries` stays as the emergency brake for heavy-slot timeouts.

## Knobs and observability

- `timefusion_maintenance_query_yield` (default `false`, envy name `TIMEFUSION_MAINTENANCE_QUERY_YIELD`). The thresholds are consts next to the hygiene gate constants.
- OTel gauge `timefusion.maintenance.query_yield_ceiling` and counter `timefusion.maintenance.query_yield_transitions{direction}`. No `timefusion_stats` keys.

## Tests

- A pure function `query_scaled_ceiling(prev, p95, samples, cfg) -> ceiling` with a case table:
  - shut after 2 consecutive high ticks, not after 1;
  - hold inside the band;
  - reopen additively;
  - floor and max respected;
  - windows under `MIN_SAMPLES` keep the previous state.
- An admission test: with the ceiling at 8, the 9th Rollup unit is refused and HotPacking is not.
- Cost assertion: it guards the ceiling, not correctness.

## Rollout and proof

1. Dark behind the flag after W42.
2. Enable on a process at least 1 h old. Compare 2 h against the previous 2 h at the same time of day, with ≥3 samples per arm, alternating.
3. Success means both of these hold:
   - pgwire p95 median at high maintenance load drops below ~500 ms;
   - `maintenance.processed_bytes` per hour stays within 10%.
4. If processed bytes drop by more than 10%, raise the floor before tuning the thresholds.

## Risks

- **p95 not caused by maintenance:** large 30-day dashboard windows raise p95 on their own, and the loop then throttles maintenance for nothing. The floor bounds the cost.
- **Time-of-day confound:** evening traffic raises p95 whatever maintenance does. The A/B must compare the same hours.
- **Relation to `coordinator_job_slots`:** lowering that from 66 to ~30 (the W32 recommendation) is the static version of this. If the dynamic loop lands, 30 becomes its default maximum.
