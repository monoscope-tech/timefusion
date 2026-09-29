# Residual maintenance cost after the 2026-09-27/28 deploys

Date: 2026-09-28 06:20 UTC. Read-only. Sources: monoscope OTel metrics (project 87576849; counters
differenced per 5-min series max, restarts treated as resets), prod logs of the current task
(image `0ee621d0`, started 04:56 UTC), and the prod task journal (`.json` checkpoint replayed with its `.wal`).

Windows: **A** = 2026-09-27 12:00–18:00 (6 h, daytime, before fix B / W42 / W41). **B** = 2026-09-28
05:00–06:10 (70 min, night, process under 75 min old). B is not a matched-traffic ABBA window, so treat
the ratios as direction and order of magnitude, not an accepted gate.

## 1. Work per accepted event

| Metric | A (6 h) | B (70 min) | A per M rows | B per M rows | A/B |
| --- | --- | --- | --- | --- | --- |
| Ingested rows (`timefusion.ingest.rows`) | 26.31 M | 4.02 M | – | – | – |
| TF container CPU-seconds (all work) | 602,189 | 21,500 | 22,891 | 5,350 | 4.3× |
| Maintenance lease seconds (`lease_ms`) | 479,108 | 5,146 | 18,213 | 1,281 | 14× |
| Maintenance processed bytes | 3,529 GB | 4.24 GB | 134.1 GB | 1.06 GB | 127× |
| Average concurrent units (lease/wall) | 22.2 | 1.2 | – | – | – |

By lane, per million ingested rows:

| Lane | A GB/M | B GB/M | A lease s/M | B lease s/M |
| --- | --- | --- | --- | --- |
| BaseRollup | 79.1 | 0.14 | 14,929 | 141 |
| HotPacking | 35.8 | 0.56 | 997 | 59 |
| Dedup | 17.9 | 0.14 | 1,819 | 1,074* |
| DerivedRollup | 0.95 | 0.003 | 456 | 3 |
| SealedConsolidation | 0.36 | 0.21 | 9.5 | 4 |

\* B's Dedup lease includes a one-off drain: 35 W39-minted bins for 87576849 on **2026-08-08**,
outside the 31-day horizon. They cost about 4,768 lease-s from the logs and finished by 05:15. Steady-state
dedup lease is a fraction of the figure shown.

Most of A's cost was waste the deploys removed: the 09-18/08-27 re-mint loop (~75% of BaseRollup
inflow), whole-day packing rewrites, and the derived deadlock. Whole-process CPU dropped less (4.3×)
than maintenance work (14× lease), because query and ingest CPU are now the larger share.

## 2. Top consumers in the current process (04:56–06:05, from `maintenance_task_finished` / `published`)

| # | Consumer | Lease s | Bytes | Stage that would cut it | Avoidable share (est.) |
| --- | --- | --- | --- | --- | --- |
| 1 | Dedup, 87576849 2026-08-08, 35 bins (W39 declined-bin mint beyond horizon) | 4,768 | ~0.4 GB | Not a stage: gate `mint_declined_bins` to the backfill horizon | ~100%, one-off (done) |
| 2 | Dedup, today plus 1–7 d (235 units) | 808 | ~0.2 GB | Stage 5 (dedup fusion) / 1C (share today's cell read with BaseRollup) | ≤50% of a small number |
| 3 | BaseRollup, today (114 units, all 1 shard, `certified_clean=false`) | 520 | 0.57 GB | 1A can't apply (today is never certified); 1C with #2 | ≤30% |
| 4 | HotPacking, today (251 units, 86 `compaction_debt_remaining` retries) | 423 | 2.24 GB (top by bytes) | Stage 6 layout (W34 already cell-aligned) | low; retries cost 159 s |
| 5 | SealedConsolidation (6 units) | 45 | 0.85 GB | none needed | ~0 |

The `admission_busy` retries are cheap: 1,795 Dedup retries total 26 s.

## 3. Hash-shard exposure (Stage 1B)

| Hash shards | BaseRollup units published | Estimated decoded bytes | Share |
| --- | --- | --- | --- |
| 1 | 115 | 685 MB | 100% |
| >1 | 0 | 0 | 0% |

Byte-weighted shard amplification is **1.00×**. Stage 1B is not material at current unit sizes.
It becomes relevant again only if large units return, e.g. the W29 31-day v4 backfill of
87576849. Output-state pricing and the state split (08:50 batch) push those toward time splits, not shards.

## 4. Why `oldest_task_age_seconds` rose to ~10.7 h

The replayed journal at 06:15 held 495 active non-paused tasks: 234 Dedup, 222 BaseRollup, 39 Derived.
Ages: 234 under 1 h, 247 at 1–6 h, 14 at 6–24 h, none older.
The oldest are **6297304f's 2026-09-27 20:00–21:00 cells** (Dedup bins, BaseRollup cells, derived hour),
created about 19:55–20:00 on 09-27, with 2–5 attempts each. They are not stuck:

- Late writes into 6297304f's 09-27 kept re-arming those cells (latest deadline 06:12:30).
- The whole-day 09-27 BaseRollup unit retried `source_not_flushed` 10 times. It then ran at 05:57 (6 s, 109 MB,
  7,087 rows) and superseded the 20:xx cells (`covered_by_running_base_unit`); the derived day followed in 1 s.
- `created_unix_ms` is not reset on re-arm, so a cell that is repeatedly re-armed and completed "ages" without being
  starved. The metric reads as starvation when it is not. Better signals: oldest *due-and-unclaimed*
  task, or age since last re-arm.

## Recommendation

At current load, no remaining maintenance stage (1B, 3, 5, 1C) has material avoidable cost.
Maintenance is about 1.2 concurrent units and about 1 GB per million rows. Shard amplification is 1.00×.
The journal lock is about 0.7% busy (W32), so Stage 3's publication/commit overhead is small.

1. **Next: the plan's combined measurement, not another stage.** Re-measure A's exact hours
   (12:00–18:00 today) on a process at least 2 h old, per the ABBA protocol, and confirm the whole-server
   CPU-per-event saving at matched traffic. The remaining CPU is mostly query and ingest.
2. **Cheap fixes found here:**
   (a) Gate W39's `mint_declined_bins` to the backfill horizon (item #1).
   (b) Change `oldest_task_age` to exclude re-armed-and-completed churn (item 4).
3. If a build stage must be picked, choose **Stage 5 / 1C for today's cells**: Dedup and BaseRollup both read the same
   10-minute cells. It's the largest remaining steady lane (~800 lease-s/h), but the absolute saving is small.
   Re-rank after the W29 backfill, which is the only foreseeable source of large units.
