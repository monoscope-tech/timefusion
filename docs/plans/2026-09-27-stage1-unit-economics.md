# Stage 1 unit economics from production logs (W7)

Status: measurement. It is the read-only half of W7. The per-unit cost on real S3 is **blocked on staging**:
no seeded staging prefix exists, and `run-unit` writes into whatever store it is pointed at. The harness half
(`run-unit` per-pass plan metrics, CPU and output) is on branch `ws/w7-run-unit-passes`.

## Method

- Process `cc6bfa40411d`, started 23:15:02 UTC. Window 23:57:28–00:57:28 UTC (60 min), at 42–102 min uptime.
- Events: `maintenance_task_finished` records every attempt with its outcome, lease seconds (`ran_secs`) and
  retry reason. `maintenance_rollup_published` records every success with its hash shards, estimated decoded
  bytes, rows and files. `maintenance_scan_pruning` records physical `bytes_scanned` per scan pass.
- Log lines carry ANSI colour codes between keys and values. Strip them before matching.
- `lease_s` is wall time held, not CPU. There is still no per-lane CPU metric; W19's `lease_ms` is its exported
  equivalent from the next deploy on.

## Per-tier, per-slice-width cost (60 min)

| tier | width | attempts | complete | retry | lease s | published | GB decoded (est.) | shards |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| metrics_1m_v2 | 180 m | 200 | 128 | 28 | 30,009 | 128 | 72.0 | 1:84 2:44 |
| dashboard_1m_v3 | 720 m | 216 | 19 | 20 | 7,841 | 19 | 12.0 | 1:1 2:18 |
| metrics_1m_v2 | 90 m | 114 | 85 | 29 | 6,913 | 63 | 28.2 | 1:41 2:22 |
| dashboard_1h_v2 | 60 m | 462 | 303 | 159 | 2,510 | 215 | 0.9 | 1:215 |
| metrics_1h_v2 | 60 m | 288 | 210 | 78 | 2,099 | 188 | 2.7 | 1:188 |
| metrics_1m_v2 | 360 m | 218 | 120 | 10 | 1,054 | 2 | 1.7 | 2:2 |
| dashboard_1m_v3 | 360 m | 414 | 355 | 0 | 623 | 1 | 0.0 | — |
| all other shapes | | | | | < 1,000 each | | | |

Of the 720 m `dashboard_1m_v3` attempts, 177 were `Superseded` (22 s in total). Each of the 19 completions
held a lease for about 410 s.

## Lease seconds by operation and outcome

| operation | outcome | lease s/h | units |
| --- | --- | --- | --- |
| BaseRollup | Complete | 48,812 | 1,342 |
| Dedup | Complete | 13,782 | 444 |
| Dedup | Retry | 4,699 | 6,653 |
| DerivedRollup | Complete | 4,520 | 572 |
| SealedConsolidation | Complete | 1,174 | 31 |
| HotPacking | Complete / Retry | 838 / 585 | 229 / 1,217 |
| BaseRollup | Retry / Superseded | 157 / 113 | 144 / 508 |

Physical bytes scanned (`maintenance_scan_pruning`): Dedup 899.2 GB, BaseRollup 75.1 GB, DerivedRollup 1.6 GB.

Rollup retry reasons: `admission_busy` 345, `source_not_flushed` 26, `base_tier_incomplete` 11,
`slice_occ_stale` 8, `noop_proof_changed` 3.

## Findings

1. **Failed rollup attempts are now cheap.** BaseRollup `Retry` plus `Superseded` held 270 lease-s/h, against
   48,812 for completions. W16's baseline had 96% of leases ending in `Retry`, and W3 had 69% of scan passes
   wasted on unpublished units. `slice_occ_stale` is down to 8 per hour, against about 100 per hour in W9's sample.
   That is consistent with W13 and W14 being live, but one window is not a before/after comparison. Compare W19's
   `rollup.scan_estimated_bytes` / `published_input_bytes` per tier across processes.
2. **Rollup cost is wall time, not I/O.** BaseRollup held about 13.6 workers of lease time but scanned 75 GB/h
   physical. Dedup held about 5.1 workers and scanned 899 GB/h. So the remaining rollup cost is aggregation
   (and waiting), which is what Stage 1B/1C target. This is where the `run-unit` per-pass plan metrics
   (`peak_mem_used`, elapsed per pass) belong, run on staging.
3. **Most rollup lease time goes to `metrics_1m_v2` 180 m units (30,009 s/h, 128 units, about 234 s each)** and to
   the 720 m `dashboard_1m_v3` slices (about 410 s each). Those two shapes are the representative units for the
   staging `run-unit` measurement. One third of the 180 m metrics units ran 2 shards, so Stage 1B (shard
   amplification) applies to them.
4. **Dedup is the largest physical reader.** It is outside the rollup plan's scope, but it is the larger I/O
   budget on the same server.

## Blocked on staging

The remaining Stage 1 measurements need the representative units above to run through
`run-unit --explain` against real S3 latency: physical plan, scan count, shards, dedup operators, decoded
bytes, aggregate-state memory and CPU. Creating or writing a staging prefix is an owner decision (integrator,
2026-09-27). Until then, Stage 1A/1B/1C savings cannot be priced per unit.
