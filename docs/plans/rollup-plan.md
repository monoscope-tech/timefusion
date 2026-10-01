# Rollup plan

Status as of 2026-09-30 ~04:00 UTC. This is the single entry point for the rollup plan. It replaces both
handovers, the workstream sheet, the first-deployment checklist, the 09-27 review and the unit-economics,
query-yield and residual-ranking notes. Their history is in git (`git log -- docs/plans/`).

Standalone design references that stay:

- [2026-09-24-rollups-on-a-fixed-server.md](2026-09-24-rollups-on-a-fixed-server.md): stage designs and
  gates for the conditional stages (1C, 2, 4, 5, 6). Most of it is an implementation diary; read the stage
  sections only.
- [2026-09-26-stage3-publication-design.md](2026-09-26-stage3-publication-design.md): the build spec for
  the Stage 3 publication queue.

## 1. Goal and how progress is judged

**Goal.** Lower total CPU, memory and I/O per accepted event on the same server. No larger server.
Rollups must serve dashboard reads, and maintenance must keep up without a growing backlog.
More concurrency, deferred backlog or shorter scans alone do not count as a saving.
A lower visible load with growing debt fails the plan.

**The plan is judged by its gates, not by prod stability.** Report the scorecard before and after every deploy.

| Gate | Definition | Status |
| --- | --- | --- |
| Plan gate | CPU-seconds and maintenance processed bytes per million ingested rows, matched hours, process ≥2 h old, no deploy inside the window. CPU baseline: 09-25 (clean). Bytes and lease baseline: 09-27 | **Accepted 09-30 (14:00–16:00, all days compared over the same hours).** 3,409 CPU-s/M rows (5.1 cores) vs 21,079 baseline (6.2× lower) and 8,555 on 09-29; 7.9 GB/M rows (09-29 10.6, 09-27 127); lease 706 s/M rows (995). `gate0930.py` |
| Falling backlog | Pending base / dedup falls over the gate window | **Accepted on direction 09-30.** Gate window: base 227 → 222, dedup 204 → 200 (09-29 same hours: rising 303 → 328, 161 → 171). After the re-arm fixes (`dfb0701a`, `36bbbe6a`) the queue is bounded: whole-day waves gone, hourly new-hour bump drains within the hour, peaks ~155–160 vs 250–370. Flat (not falling) in the evening |
| Paired protocol | Every percentage gate uses counterbalanced ABBA then BAAB blocks, same hours, 95% bounds that account for time correlation. A savings gate passes on its lower bound, a regression gate on its upper bound. Inconclusive means extend, not pass | **Run and used** (09-29/30): query-latency yield (deleted), CPU-token cap 66 vs 128 (not binding), W46 twice (deleted, p95 lower bound ≤ 0), byte admission (deleted). Tooling: `tf-helper-scripts/paired_ab/` |
| Capacity replay | `timefusion sim --calibrated` at rows/projects 1/2/4 | **Done** (W26). About 2x headroom. Scheduler only; CPU and I/O not modelled |

The scorecard (`scorecard.sh`, section 6) reports:

- `maintenance.pending_base_rollup`, `pending_derived_rollup`, `pending_dedup`, `tasks_pending`, `oldest_task_age_seconds`.
- `scan.cert_skip_files`, `scan.dedup_partial_skipped`, `scan.dedup_denied_slice_only`.
- `pgwire.lat_p95/p99/p999_us_approx` (recorded at stream end since `8184a638`).
- The 18-query probe: route outcome and wall time per shape and project.

Do not use `rollup_dirty_partitions` (written, never cleared or read) or raw `oldest_task_age_seconds`
(counts re-armed-and-completed cells as old; see section 5).

## 2. Rules that were paid for

Deploys and pushes:

- A push to master is a deploy. Each restart costs users a cold dashboard for 10–20 min and resets
  `timefusion_stats`, the coverage map and in-flight units. Docs-only pushes do not deploy.
- Batch code into coherent releases. One restart per batch. Keep ≥2 h between code deploys when measuring.
- No code pushes inside a gate window (12:00–18:00 UTC on a gate day). Docs are fine.
- Run `make ci-signoff` green locally before every master push. It also builds the prod image, so the
  deploy that follows is ~1 min to Ready. `make prepush` is not enough.
- Run one signoff at a time. Uncontended: ~5–11 min. With 3–5 parallel builds: 69–90 min.
- Use `gated_push.sh`: it refuses when master moved with code, and after a rebase it requires the code
  to match what was signed off. An "emergency" push on targeted tests once shipped a DV-delete correctness hole.
- Any delta-rs fork fix must also land on the integration branch `timefusion-upgrade-55-dv`, or the next
  pin bump drops it.

Prod access:

- Prod is read-only: logs, `inspect`, `ps`, `docker stats`, `docker cp` out, pgwire SELECTs on
  `timefusion_stats` and tightly time-bounded queries. A broad scan can OOM prod. Never `docker exec`,
  restart, scale or touch volumes.
- Allowed prod writes: `ROLLUP PAUSE` / `ROLLUP RESUME … FROM`, and `FLAG SET|RESET` (in memory only; a
  restart returns to config).

Engineering:

- Every bug fix starts with a failing test, and the guard is shown red with the fix reverted.
  Test read-path pushdown through pgwire: the local `ctx.sql` path re-runs optimizer passes prod does not.
- Assert cost, not only correctness, where the fix is about cost.
- Measure before building: find the saturated resource (`docker stats` during the query vs idle).
- Never measure a process younger than ~10 min (≥1–2 h for maintenance counters). Take ≥3 samples,
  alternate arms, and difference counters over a window.
- Separate warmup, restarts and cache effects. A ladder of windows pre-warms itself.
- Use your own worktree and `CARGO_TARGET_DIR` under `~/Projects/apitoolkit/` (`/tmp` is wiped on reboot).
  Never delete a target dir you did not create; check `pgrep -f <dir>/target` first. Keep ≥30 GB free.
- One Cargo process per build cache. A cached test binary from another worktree is not evidence.
- Inspect patches with `git diff --no-ext-diff` (and `--cached` for staged work); plain `git diff` can
  hide staged changes or use an external differ.
- A widening step for a tier backfill needs memory and `admission_refused_state_bytes_total` checked first.

## 3. Current prod state (2026-09-29 ~20:00 UTC)

- **Image:** master `9e21fc3d` (03:48, version-append witness) on `f62fb4ae` (03:14, `batch/overnight`: 11 dead flags deleted, certified-clean on, query yield deleted, byte-admission FLAG, escalation + due-age metrics, OR-chain bloom pruning, derived null-measure fix, RUM sessions e2e).
- **Tiers:**
  - `dashboard_1m_v4` / `dashboard_1h_v3` (with `level`) are the dashboard tiers. v3 and `1h_v2` are dropped.
  - v4 policy `RESUME FROM 2026-08-29` (full 31 days) since 19:02. At 18:51 v4 had 15 contiguous days.
    Windows older than ~15 days read raw until the backfill fills them. The backfill is bound by state
    memory (19,273 `admission_refused_state_bytes_total` in the first ~50 min after the 09-07 step).
  - `sessions_1h_v2` resumed from 2026-09-22 at 18:08. `sessions_1h_v1` paused.
  - Metrics tiers `metrics_1m_v2` / `metrics_1h_v2` unchanged.
- **On by default:** DV strip for all dates (fleet cap 12 per 10 min), W59 dirty re-flush drain, one heavy
  slot per query, measure re-mint backfill (2 cells per pass).
- **Dark flags (FLAG-switchable):** `timefusion_maintenance_cpu_tokens` (8..256; prod runs 66). W21 witness carry stays off.
- **Latency:** p95/p99 are recorded at stream end since 18:09. Values before that excluded streaming time
  and are not comparable.
- **Last clean numbers (09-29 14:00–18:00, image `8bea392b`):** see the gate table. Shipbubble 24h status
  ~0.3–1.6 s, 24h by service ~0.3–1.0 s, 6h window ~0.5 s. Dashboard-shape hit rate ~30–35%.

## 4. Task list

Done items carry evidence (commit or number). Open items carry the next action, blocker or owner.

### Gates and experiments

- [x] **Plan gate 09-30, 14:00–16:00** (`tf-helper-scripts/gate/gate0930.py`, every day compared over the same hours;
      process `abe49d09`, last push 11:51). CPU-s per M rows: 21,079 (09-25) · 21,152 (09-27) · 4,629 (09-28) ·
      8,555 (09-29) · **3,409** (09-30) — 2.5× below yesterday, 6.2× below baseline; cores 5.1; GB rewritten/M 7.9
      (09-29 10.6, 09-27 127); lease-s/M 706 (995). Backlog first→last hour: base rollup 227→222, dedup 204→200,
      due tasks 3→2 — FALLING (09-29 same hours: 303→328, 161→171 rising). Passed on direction; the decline is small
      (4–5 tasks/2 h) and pending dedup is higher in absolute terms than 09-29. Re-check the evening trend on `f2017216`.
      **Evening re-check (`36bbbe6a`, 18:45–20:50):** no whole-day re-arm waves (0 corpse removals; largest reconcile
      73 units vs 314 before); pending base rollup sawtooths hourly (top-of-hour bump = new-hour work for every
      tenant, e.g. 74→159 at 20:08) and drains within the hour (159→84 by 20:48); peaks ~155–160 vs 250–370
      earlier. Hourly troughs 55 → 74 → 84 (last still draining). Verdict: the backlog is bounded and kept up with at
      a much lower level; "falling" held for the daytime gate, flat this evening.
- [x] **Experiment: query-latency yield** — deleted: A/B showed no benefit. ABBA/BAAB on prod 09-29
      20:01–21:55 UTC, 16 workers, ~57k queries: the yield engaged (ceiling 66 → 8) yet p95 B−A +0.24 s
      (CI −0.52…+1.00) and p99 +0.53 s (CI −0.28…+1.34) on a 4.95 s baseline. Query latency under load is
      not driven by maintenance concurrency.
- [x] **Experiment: CPU-token cap 66 vs 128** (W32), paired ABBA/BAAB on prod 09-29 21:57–23:43, 16-worker load.
      Inconclusive and not binding: p95 B−A −0.66 s (CI −3.7…+2.4), GB/h −11% (CI −70…+59); tokens read 0/66 while
      `admission_refused_state_bytes_total` was 29,201 — state memory binds, not the token cap. Default stays 66;
      the `FLAG SET timefusion_maintenance_cpu_tokens` override stays as an ops knob.
- [x] **Experiment: W46 key-level dedup restriction** — deleted: A/B showed no benefit. Paired ABBA/BAAB on prod
      09-30, 8 windows: p95 B−A −0.05 s (CI −0.26…+0.16), p99 −0.29 s (CI −1.68…+1.10). The enable rule needed the
      p95 lower bound > 0; not met. Flag, restricted-scan path and `dedup_key_restrict_*` counters removed.
- [x] Capacity replay 1x/2x/4x: W10 calibration within ±10% per lane; W26 (`5610c94e`, `d19631fb`); ~2x headroom.
- [x] Stage 0 rollout gates: release 2 (#323).
- [x] Byte-aware batches: measured, missed the 10% CPU gate (1.87–1.95 s vs 1.91–1.95 s). Stays off.
- [x] Stage 1A certified-clean path: ON by default (`f62fb4ae`). Staging A/B 34–71% less build CPU, identical
      counts/sums/sketches; prod parity on 09-28 totals (6297304f, 28f62f01) matched.

### Stage 0: coverage reconciliation and loops

- [x] Release 1 resource safety (#322, 09-25): DataFusion 54 spill-reader ownership, quota-rejection cleanup,
      sketch memory accounting. Rollback drill passed.
- [x] Release 2 (#323): durable `ROLLUP PAUSE/RESUME/POLICIES`, range coverage, derived claims wait for parents.
      Its OOM (admission priced after file selection) fixed by `96b093f5`.
- [x] Release 3 (#324, `8343dcd7`): DataFusion 55.1 / Arrow 59.3 and all forks. Follow-ups `8645be45`
      (`variant_get` missing path), `c9b4e809` (bound `->` step), `now()` µs binding (`eb17f52a`).
- [x] Loops: W13 narrow slice OCC (`7e03e0e6`, `202b10b0`), W14 sealed-day republication (`2b8c46d4`),
      fix A (overlapping ranges), fix B (`c042fc06`), W38 census re-admit guard, W41 per-cell re-arm,
      W42 no fusion of unended cells, orphan-derived prune. Re-mint loop gone (100/15 min → 0).
- [x] Throughput: claim fast path, `edit_tasks` → `mark_dirty`, tier backfill cap 16, output-state pricing
      (~48 KB per output row), state split, coarsening cap, v4 sibling prior, state waiter (`06727291`).
- [x] Packed repairs: deleted (`f62fb4ae`, on-or-delete policy). `rollup_escalated_*` counters measure what
      the escalations cost; revisit only if they show a large lane.
- [x] `ws/recovery-adopt-v2`: dropped 09-30 (~4 requeues per restart, not worth the code); kept in `tf-helper-scripts/timefusion-sweep-2026-09-30.bundle`.

### Stage 1: measurement and repeated work

- [x] W3 shard inventory, W7 `run-unit` harness and prod unit economics, W16 exported history, W19 lane counters.
- [x] W7 real-object-store unit cost (09-29, staging prefix on OVH): large units CPU-bound, small units
      fetch-bound, staging+commit ≤4 s per unit; isolated whale 12h unit ~150 s vs ~410 s prod lease.
- [x] 1B closed by measurement: byte-weighted shard amplification 1.00x (all units single-shard).
      Reopen only if large units return (v4 31-day backfill of the whale).
- [x] 1D closed by measurement: ~0% irrelevant churn; W8 shadow classifier and W17 design shipped.
- [x] **Stage 1C closed by measurement (09-30 14:00–16:00 UTC, process abe49d09, no deploy in window).** Evidence:
      `tf-helper-scripts/stage1c/` (units.json, parse.py, window log). Trigger met in shape: 59/59 `sessions_1h_v2`
      hours ran in the same coordinator wave as their six v4 cells (same `input_fp`); Dedup ran on the same cell as
      v4 in 407/409 units. But lease (7,631 s) is HotPacking 41%, v4 30%, Dedup 12%, sessions 7%, metrics_1m 6%;
      the sharing ceiling (all of the cheaper side of each pair) is 387 s = 14% of the pair (5% of maintenance),
      ≤12% with Dedup; realistic ~5% (a whole-hour sessions unit costs 1.7 s median vs 33 s for its six v4
      cells). Lease is wall time: maintenance averaged 1.06 concurrent units on 5.1 cores, so the bounded saving
      is ~1% of container CPU. Below the ≥20% gate → no build (as 1B, 1D). Reopen only if multi-hour units return.

### Stage 2: source visibility and today-window hits

- [x] W31 content-fingerprint slice proof (`682179d0`).
- [x] **Today's slices go stale under `hashes`-only MoR UPDATEs** (measured 09-29 night): ~90% of today's slices
      refuted wherever monoscope's pattern-tag UPDATE lands (~7/min on 6297304f); a 24h window keeps ~1 h of
      provable today coverage, falls under the 1/5 interior floor and runs raw (RUM page views 23–45 s). Every
      sealed-day window routes fine. Fix built: `ws/version-append-witness` (version-only files tagged
      `timefusion.version_only_columns`, skipped by the row-count and content witnesses when no tier reads those
      columns; kill switch `timefusion_rollup_version_only_witness`). Deploy separately from `batch/overnight`;
      verify the 6297304f 24h page-view query flips to a hybrid hit and `scan.rollup_stale_grew` falls.
      **Deployed `9e21fc3d` 03:48 09-30.** First 20 min: quarantine 0, coordinator errors 0, `stale_coverage`
      3 of 370 misses, 53 hybrid hits. `version_only_declined_rows` (171k vs 2.7k tagged) are UPDATEs whose
      predecessor was still buffered: their flush is the row's first, i.e. new data, so declining is correct.
- [x] W21 witness carry: closed, stays off. Recovers ~0.7%; blockers: double count, ledger lost on restart.
- [x] `ws/w31-transparent-witness`: dropped, superseded by the version-only witness (`9e21fc3d`).
- [ ] **Captured source view** (goal doc Stage 2 protocol) — trigger: a reproduced hybrid snapshot race, or
      logical-only routing needed. Not triggered: W31 solved today-window hits physically.

### Stage 3: publication

- [x] W9 design; step 1 = W13 narrow OCC (on master).
- [ ] **Steps 2–3: `StagedRollup` split and batched queue** (stage3 design doc) — trigger: Stage 4 approved
      or publication volume grows. Not triggered: publications are <1% of journal checkpoints; journal ~0.7% busy.

### Stages 4–6 (conditional)

- [ ] **Stage 4 flush-time aggregation** — needs Stage 2 view + Stage 3 queue. Not triggered.
- [ ] **Stage 5 dedup fusion** — trigger: unresolved dedup still a large lane. Not triggered: Dedup is
      1.3 GB/M rows after W33/W37/W39.
- [ ] **Stage 6 minute revisions** — trigger: ≥30% saving on ≥20% of the relevant resource. Not triggered.

### Stage 5 prerequisites: certification and dedup

- [x] W33 skip dedup on certified-clean days; W37 per-project spans for the per-file skip (unproven:proved 23:1 → 2:1);
      W39 mint Dedup units for probe-declined bins + counter honesty; mint horizon gate; W22/W23 analyses.
- [x] W50 Dedup `processed_bytes` records measured bytes (was 0).
- [x] W46 key-level dedup restriction built dark (`0f131e24`), then deleted after its A/B (see Experiments).

### Read path

- [x] W20 narrow whale merges ungated (`187a2811`); W36 order-preserving MemBuffer leg; W43 (fork `79e98104`);
      W44 (fork `2b9c7e21`); per-file dedup split fix (`cd27a17d`).
- [x] W47 2a day scans cached on first view; W47 2b boot body preload (`310636d4`).
- [x] `1e79a30b`: W49 rollup cast fix, W50, W51 bounded AND/OR time range (cache bypass), W52 8-way preload,
      W53 tantivy cache 400 GB, W55 `count(<expr>)` wrong-answer fix.
- [x] W48 be87ebc1 slow warm: resolved by W44 + W51 (6.1 s cold → 0.7 s warm).
- [x] `8184a638`: pgwire latency at stream end; one heavy slot per query; FLAG switch.
- [x] Byte admission for wide scans (FLAG `timefusion_query_scan_byte_admission`) — deleted. ABBA 09-30 04:05–04:43
      on the mixed sealed-day load (4 workers): the gate never queued (12,646 admitted, 0 queued; ~10 GB/h scanned,
      peak 23.5 GiB), p99 0.78 s ON vs 0.83 s OFF. The memory it was meant to bound was tantivy installs, not scan
      decode (see Memory spikes), fixed separately. Flag, `ScanByteGate` and `scan_bytes_*` counters removed; the
      heavy-query (sort) admission stays.
- [ ] Remove W36 `split_sorted_runs` at the next DataFusion fork bump, after carrying sort info through
      `MemorySourceConfig::repartitioned` in the fork.

### DV strip

- [x] W54 hardened, flag off (`ecc2483f`): park-on-refusal, backoff kept, carry gated to version-append sources.
- [x] W57 sealed dates (`43af26a4`); its latency incident (per-partition rate limit) fixed by the fleet cap.
- [x] W58 today's partitions + fleet cap 12 per 10 min (`99e0b314`). Strip on for all dates.
- [x] W59 dirty re-flush drains unchanged batches (`ed59a3dd`, on by default).
- [x] Follow-ups in `4319b951`: `dv_strip_plan_sorts_total` counts only unexpected 1:1 sorts
      (`dv_strip_resorts_total` counts undeclared footers); all-DV cells strip one file at a time;
      bodies of today's/yesterday's outputs ≤256 MB warmed within the preload budget.
- [x] Parks persist in the `lossy_rewrite_parks.json` sidecar, so a restart does not retry a refused set.
      A changed DV or a lapsed deadline still unparks; a missing or corrupt file loads as no parks.
- [x] Re-measure the 2-day-old shipbubble trace lookup once 09-26/27 are fully stripped (last: 15.9 s cold, 3.1 s warm).
      09-30 20:00: 2.64 s cold / 0.61 s warm (was 15.9 s / 3.1 s).
- [x] Watch query latency against the 12-per-10-min cap while today's strip debt drains (it dominates 09-29 HotPacking).
      09-30: realistic dashboard mix p95 1.8 s / p99 2.6 s with the cap in force; no strip-driven latency seen.

### Tiers

- [x] W29 v4 / `1h_v3` with `level`; v4-fallback relabel; `run-unit --tier`.
- [x] v4 widening: 09-21 → 09-14 → 09-07 → `RESUME FROM 2026-08-29` (19:02, all 31 days).
- [x] drop-v3 (`4319b951`): owner overrode the contiguity gate (queries older than 14 days are rare).
- [x] v4 backfill: contiguous days min/median 30/30 (09-30 04:07).
- [x] Re-rank residual cost once the backfill finishes (1B and 1C may matter for large units).
      Done 09-30 as the Stage 1C measurement: HotPacking 41%, v4 30%, Dedup 12%; churn was re-arm waves (fixed dfb0701a, 36bbbe6a).
- [x] W28 `service_name_hll` unblocked (`e0e27dbe`).
- [x] RUM measures on v4 (`180ced63`, `8bea392b`); measure re-mint backfill for older cells (`8184a638`, in progress).
- [x] RUM widgets (monoscope branch merged into `overnight-exploration-29-09`) route to v4 `rum_*` measures:
      confirm hits in prod once the backfill re-mints historical cells. 09-30 09:15: after re-mint at 8/pass
      (`dadf3311`, ~480/h) sealed Talstack page views route: 7d 0.88 s, 28d 2.2 s (were 3.4–4.2 s raw / 90 s timeout).
      09-30 16:10: backfill complete — 28-day page views (09-01..09-29) are FULL rollup hits: Talstack 0.49 s,
      shipbubble 0.7 s. 09-30 04:20: the 7-day sealed Talstack
      page-view query still runs raw (3.4–4.2 s, `measure_not_stored`). Re-mints run at 2 per pass, about 25–44
      per hour, so expect several more hours.
- [x] `otel_metrics` dedup key widened to `[timestamp, metric_name, series_id]` (`132b98e7`; owner-approved; logical-count FORMAT_VERSION 3→4 rebuilds once).
- [x] `log(x)` / `log(b,x)` returned NULL through pgwire: the plan cache lifted literals into untyped placeholders that DataFusion's `log` simplifier folded to NULL. Shapes whose placeholders fold away are now declined (`132b98e7`); prod now returns 12.2877… / 2.
- [ ] W12 `name` HLL (owner): add the unfiltered `name_hll` only if a daytime sample shows service-tab misses. 09-30: owner OK with sampling first — do it during the day.

### Sessions

- [x] W4: `sessions_1h_v1` had no consumer; stays paused.
- [x] W56 hour-grain session tiers (the proper W40 fix, `ecc2483f`). `rollup_miss_sub_grain_slices_total` read 0.
- [x] W27 `sessions_1h_v2` resumed from 2026-09-22 (18:08).
- [x] Land `test/rum-sessions-e2e` (`prepared_rum_sessions_use_browser_rollups_and_raw_edges`, passes) with the next deploy.
- [x] Confirm the RUM sessions query routes to `sessions_1h_v2` in prod on a mature process.
      09-30: sealed-day sessions query is a hybrid rollup hit, 2.1 s.
- [x] Dropped `sessions_1h_v1` (`6c3e8b3b`, deployed 09-30 04:45); its durable pause is retired at boot with its queued work.

### Scheduler and ops

- [x] W10 sim, W18, W26 matrix, W11 OTel 0.33, W15 observability, W25 part 4 (rollout phase timing),
      ingest dirty-livelock fix (`d7d44054`, `3bfa405e`, `4ee017dd`), task-age-from-reopen metric.
- [x] Journal lock wait: recheck on a ≥1 h process (mature ~7 s/min; 14–18 s/min seen on young ones).
      `coordinator_claim` ~371/s is the baseline, not a regression.
      09-30 21:55 on `90004268` (68 min old): 10.0 s/min summed across threads; avg wait ~0.37 ms, max 93 ms. Slightly above the ~7 s/min baseline, not a bottleneck.
- [x] `oldest_due_unclaimed_age_seconds` added (`f62fb4ae`).
- [ ] W25 parts 2–3: the 4.9 s container create → start; CapRover's double service update (1.6 s per deploy).
- [ ] **Look at tomorrow (owner, 09-30 decision): CapRover config.** Needs someone with CapRover access; TF sessions
      must not change prod config. (1) Remove the stale env var `TIMEFUSION_FLUSH_COALESCE_COMMITS=false` — the flag was
      deleted in `f62fb4ae`, so it is ignored but misleading. (2) W25: container create → start takes 4.9 s, and CapRover
      issues a double service update per deploy (+1.6 s). Also note: heap profiling is armed with
      `docker service update --env-add TIMEFUSION_HEAP_PROFILE_ACTIVE=true` and is cleared by every CapRover deploy (off now).
- [ ] Dependency advisories not closable by patch bumps: rustls-webpki 0.101 (AWS SDK), tokio-tar,
      tokio-postgres/postgres-types (0.2.14 breaks arrow-pg encoding).
- [x] HotPacking "selected nothing" / 9 invariant violations were false alarms (a strip outranking a pair); now
      `refused_cells` via `select_bin`, WARN only on a real refusal (`02f9f0a1`, `27440d30`). Ledger
      disagreements are a boot burst (the ledger is written only at replay); seeded coverage is gated by the
      same output + row checks as replay.
- [x] `ordering_pushdown::one_unsorted_file_does_not_cost_the_majority_its_ordering` e2e flakes under load
      (passes alone). Leave unless it blocks signoff.
      Already fixed by `a84e3775` (light-optimize disabled in the test); stress runs 0 failures.
- [x] Old-process shutdown can panic in `buoyant_kernel_engine` executor (`RecvError`) after the WAL drained.
      Cause: cancelling maintenance dropped the maintenance runtime at once, under in-flight kernel IO. Fixed:
      that runtime now outlives `maintenance_tasks_tracker` (preload + dedup cron now tracked).

- [x] **Two DV-strip tests fail by design 00:00–03:00 UTC** — fixed (`c2464d2a`): `compact.rs` seal gate + sweep dates read the virtual clock; tests pin tomorrow noon. (`rollup_noop_skip_tests::todays_dv_files_strip_under_a_live_rollup_slice`,
      `a_retired_corpse_goes_first_and_counts_as_progress`: "dedup masks only bins sealed 2 h; run after 03:00 UTC"),
      which blocks every signoff for 3 h a day. Pinning `support::set_micros` alone is not enough: some part of the dedup
      path still judges "sealed 2 h" by wall time. Find it, route it through the virtual clock, then pin the tests.
- [x] **Memory spikes / OOM — root cause found by a prod heap profile (09-30).** The 09-29 OOM and 09-30 spikes of
      41–106 GB (also with no synthetic load, 07:00–07:07) were tantivy `ensure_cached` installing whole index blobs
      in memory: `zstd::decode_all` 83.7 GB + `GetResult::bytes` 28.3 GB live, up to 32 installs at once, outside every
      pool. Fixed in `fa75bbd3` (stream download → zstd → tar to disk, ≤4 installs, one download per blob), deployed
      `7a259c3c` 07:53. Verify: realistic-mix load (60% today, 25% 7/14/30 d, 15% sealed; 4 workers) with no spike
      past ~30 GB. Recipe: `TIMEFUSION_HEAP_PROFILE_ACTIVE=true` via `docker service update --env-add` (owner-approved;
      a CapRover deploy resets it), `scratchpad/heapcatch/catch.sh`, `scratchpad/heapsym/sym.sh`.
      Not the cause, all ruled out: DedupExec (absent from these plans), the deploy drain (178k rows, 1 s), byte admission.
      Verified: realistic load anon peak 19.5–24.8 GB across four runs after `7a259c3c`.
- [x] Whole-file GET on a bypassed miss of a ≤16 MB parquet file (`7a259c3c`): real but small (+3.5 → +1.5 GB locally).
- [x] Realistic 4-min baseline before the fix (09-30 06:45): 330 queries, p50 0.69 s, p95 6.8 s, p99 15.1 s, 7 errors.
- [x] **Tantivy install fix verified** (`7a259c3c`, 09-30 08:04–08:34, 2,086 queries): anon peak 23.6 GB (was 41–106 GB spikes).
- [x] **Hybrid raw legs ORed every gap into one scan** whose pruning hull spanned the whole window: Talstack 7d opened
      2,500–4,150 tantivy indexes. One leg per gap (`dd89735c`): 110 indexes, 0.9–1.95 s warm (was 5.9–10.4 s).
      Realistic mix (4 workers, 12 min) before → after: 898 → 2,556 queries, p95 7.3 → 2.2 s, p99 48 → 8.7 s,
      timeouts 10 → 1, anon peak 24.8 GB.
- [x] ERROR-count tail: the `status_code` tantivy prefilter overflowed its 2,000-hit cap on busy sealed-day indexes AFTER
      downloading every cold blob (cold whale sealed 24h: 14.8 s vs 1.05 s unfiltered). Over-cap memo (`c122952c`):
      realistic mix p95 1.8 s, p99 2.6 s, 3,779 queries/12 min, anon peak 19.5 GB.
- [x] Window-aware cap deployed (`3b8d42b2`, 11:04): in-window hits only. Did NOT remove the cold first-query cost:
      whale sealed 24h 7 d back 88.9 s cold / 1.85 s warm (8 d back 10.3 s / 1.95 s); unfiltered ~1 s.
- [x] Never block a query on cold index installs (`abe49d09`, deployed 11:51): cold → prefilter skipped, scan runs the
      predicate, bounded background warm (≤2 permits, 64-deep queue). Whale sealed 24h ERROR count on untouched days:
      1.97 s / 1.44 s cold (was 88.9 s / 10.3 s). First hour: 107 warms spawned, 155 dropped (queue full, by design).
      Histogram readers (`histogram_reader`) still install cold indexes synchronously — not covered.
- [x] 09-30 decisions (owner): batch sealed-day rebuilds to ≤1/h per cell + enforce the rebuild deadline (in progress);
      delete byte admission (in progress); heap profiling left OFF (recipe in memory); monoscope items handed to the
      monoscope session (array_has lowering, issue-chart backoff, 10 s session-lookup timeout; hashes cadence unchanged).
      Done: sealed-day rebuilds ≤1/h (`bfd17aed`; deadline NOT enforced — owner: long whale rebuilds are fine if progressing); byte admission deleted (`24929428`); monoscope items shipped on its `tf-followups` branch.
- [x] Flaky: `kill_recovery::acked_rows_survive_sigkill_*` (multi_tenant, concurrent_writers) each retried once 09-30.
      Root cause: concurrent first CREATE of a table failed on "protocol changed"; fixed `45d02461` (adopt the winner's table on any CREATE failure).
- [x] Flaky e2e: `recent_window_pruning::dv_bearing_file_keeps_parquet_pushdown_on_its_siblings` (1 retry in 0930e signoff). Fixed (`16de4ec8`): the harness 1 s eviction tick split the fixture's files; the test now owns its flushes.
- [x] Flaky e2e `postcommit_hooks::commit_path_does_not_checkpoint`: the harness left the wall-clock checkpoint cron on (even minutes); disabled in e2e (`90004268`).
- [x] Late RUM rows void a whole sealed day slice (09-29 day slice rebuilt 256×; one rebuild took 2,461 s while a sealed
      consolidation overlapped). Fixed on `rollup/batch-sealed-rebuilds`: a built rollup slice over a date before today
      is rebuilt at most once per `SEALED_REBUILD_INTERVAL_MICROS` (1 h); the re-arm stays queued and reads use the raw
      leg meanwhile. Today, never-built slices and damage (untagged) cells are not held. The 15-min "deadline" is an IDLE
      window by design (rollups use the 60-min one, no lifetime cap), so long but progressing rebuilds are not killed.
      Consolidation and rollup of one partition are not serialized: no per-partition cross-lane lock exists.

### Monoscope-owner decisions

- [ ] **Session lookup by `attributes___session___id = ANY($ids)`** — 09-30 owner decision: add an ENRICHMENT-ONLY column class (updates may only fill NULL/'' → value; monoscope only fills late session ids across a trace); 34 s is not acceptable. Deployed `132b98e7` (23:00 UTC 09-30): `enrich_only` column class, UPDATEs may only fill NULL→value (others refused). First prod read: 16 h IN-lookup 10–22 s (was ~34 s); a sealed-day lookup still 9.8 s. Cause: the bloom sidecar builder stubbed every compacted file whose 7 columns' blooms exceeded 4 MB as no-bloom, so sealed days never pruned. Fixed `0bc19668` (keep the cheapest columns under the cap; v1 stubs re-lifted once; registry cap 256 → 768 MB). 00:35 01-10: absent ids on 09-28 0.19 s (5/6 files skipped), 09-29 0.3 s warm, 3 days 0.24 s; 224 entries rebuilt, 0 errors; resident registry 665/768 MB — watch `registry_misses`. Also: parquet row-group blooms never fired for IN lists of ≥4 items (monoscope's `= ANY($1)`) because the Utf8→Utf8View cast survives on IN lists; scan now lowers ≤32-item IN lists on bloom columns to OR-of-equalities (`a710ea56`). Permanent fix belongs in the DataFusion fork's `unwrap_cast` (InListExpr case). Known edge: span re-delivery or a hashes UPDATE racing the backfill can append an empty-session version — pruned and unpruned answers then differ (pre-existing lost update). (~34 s over 16 h): the column is `mutable: true`,
      so bloom, tantivy and stats pruning all refuse it (correctness gate). Options: (a) look up by an immutable key
      (`id`, `context___trace_id`); (b) add an "enrichment-only" column class (values only go NULL → set) that the
      pruners may trust; (c) two-phase key lookup. TF side shipped only OR-chain bloom pruning for immutable columns.

- [x] Pattern-tag `update2Sql` source fix: measured on prod 09-30 — the statement already guards re-tagging (0 re-tags, 0 duplicates, 0 identity writes). Masked rows come from the one legit tag append landing AFTER its bucket flushed (92.5% of appends are retracted in memory). Lever proposed to owner: a ~90 s flush grace after a bucket closes. (`BackgroundJobs.hs` ~2944): ~40% of today's masked rows are its
      MoR versions, and it blocks certification.
- [x] `array_has` lowering for `hashes` predicates (`monoscope/plans/array-has-lowering.md`, ~40x cheaper).
      Implemented on monoscope `tf-followups` (09-30); PR pending on the monoscope side.
- [x] Back off issue-chart auto-refresh after a timeout (Talstack `jsonb_path_exists(to_jsonb(hashes))` charts hit 90 s).
      Implemented on monoscope `tf-followups` (09-30); PR pending on the monoscope side.
- [x] Add a timeout to the `Issues.hs:488-497` session lookup (`tryWithin Nothing`; stalls up to 77 s).
      Implemented on monoscope `tf-followups` (09-30); PR pending on the monoscope side.
- [x] Optional: compute `trace_count` in `rollupServiceEdges` (done in `rollupEndpointDependencyEdges`, monoscope PR #620: 48 → 24 scans, identical results) without referencing `bucketed` twice (12 → 6 scans).

### 2026-09-29/30 overnight log (condensed)

- Deployed `8184a638`, `4319b951` (drop-v3 + DV follow-ups), `db63a37b` (cpu-token FLAG), `f62fb4ae` (batch/overnight,
  net −2.8k lines), `9e21fc3d` (version-append witness). All after a full green local signoff.
- Experiments (paired ABBA/BAAB, synthetic load): query-latency yield → deleted (engaged, no latency benefit);
  CPU-token cap → not binding (state memory binds); W46 → invalid run (see its task).
- Staging A/B: certified-clean −34…−71% build CPU, t-digest accuracy within 1.14× → on by default. Prod parity check after
  deploy: 09-28 sealed totals routed == raw for 6297304f and 28f62f01.
- Incident (mine): synthetic raw-scan load OOM-killed prod at 23:47:02; auto-restart, WAL replay 6.29M rows, no loss.
- Bug found and fixed: derived `dashboard_1h_v3` builds failed over base cells predating the `rum_*` measures
  (non-nullable count column) → 66 quarantined units; all measure columns now nullable, TAG_MEASURES still gates reads.
- Backlog: 33–65 GB after drop-v3 vs 171–309 GB on 09-29 morning.

## 5. Key findings worth keeping

Where cost and slowness come from:
- **v4 / sessions cost is re-arm churn and aggregation, not cross-tier duplicate preparation (09-30).** Whole-day
  rebuild waves of 6297304f ran 59 min apart (14:29: 49 units, 15:28: 67). Lease on cells built ≥2× in 2 h: v4 874
  of 2,267 s, sessions 458 of 564 s; the whale's 12:00 hour alone was 63% of sessions lease. Next levers: the
  re-arm source and per-cell v4 cost (131/409 v4 cell reads repeat another cell's `(input_fp, hour)`) — unmeasured.
  **Re-arm source found and fixed (`dfb0701a`, deployed 16:56):** each wave was ONE late row. A flush writing a late
  row with current rows produces a file whose min/max stats span every hour between them; the per-minute cursor
  reconcile turned that span into a re-arm of every cell in it (~6 of 42 cells actually held rows). The two waves
  were 70% of Talstack's v4 run time in the window. Flushed files now carry `timefusion.dirty_cells` (the cells
  their rows occupy) and the reconcile re-arms only those; untagged/rewrite files keep the old behaviour.
  **Second trigger (`36bbbe6a`, deployed 18:44):** a remove-only commit that retired ONE fully-masked corpse file was
  marked `data_change=true`; the reconcile's "removes, no adds ⇒ whole day" rule re-armed Talstack's entire day
  (72 tasks → 314 units across Dedup + 3 tiers, incl. hours with no data) at 17:49. Corpse retirement now commits
  `data_change=false` (no logical change: every row was already masked). An unexplained 78-unit reconcile at 18:01
  remains; being watched on the new build.
- [x] Ingest-time client-retry dedup deleted (`3faac43f`, 18:16): prod ran it on every insert with a 2.79M-entry
      index and `key_hits_total=0` — the 09-07 revert (`6a69edbb`) had never been applied.

- **Masks on today's partition** (all 214 DV bitmaps decoded): ~57–62% of masked rows are exact duplicates TF
  made. An UPDATE/DELETE during a flush commit left the bucket dirty, and the whole bucket was re-flushed
  (≥21% of rows flushed that day). W59 fixes it. The landed-batch skip cannot catch this (0 repeated digests
  in 1,206 commits). The other ~40% are real MoR versions from monoscope's `hashes` UPDATE.
- **Nothing stripped DVs before W54–W58.** Masked rows accumulated (Talstack 74.6% of today, shipbubble 17.5%);
  point lookups into DV files read whole files.
- **Shipbubble was slow from cache policy, not the scheduler:** W51 (hybrid raw leg OR bounds → cache bypass),
  W52 (serial preload), W53 (tantivy cache 200 GB < ~300 GB working set).
- **Strip bursts hurt other tenants:** new outputs are cold, have no tantivy index yet, and each commit forces
  a snapshot refresh into planning. Rate-limit fleet-wide, not per partition.
- **Release-2 OOM:** admission priced after file selection admitted ~25 sorts in the same second. Price before
  selection, from the queued estimate.
- **Prod lease is mostly contention:** an isolated whale 12h unit costs ~150 s against ~410 s of prod lease.
- **Throughput does not need concurrency:** processed bytes flat at 160–178 MB/s from 10 to 45+ units in flight,
  while pgwire p95 rose from 180 ms to ~1 s.

Unit economics and residual cost (09-27/28):

- Before the loop fixes, BaseRollup held ~13.6 workers of lease but scanned 75 GB/h; Dedup scanned 899 GB/h.
  Rollup cost was wall time (aggregation and waiting), dedup cost was I/O.
- ~75% of BaseRollup inflow was the Stage 0 re-mint loop. After the fixes, maintenance was ~1.2 concurrent
  units and ~1 GB per million rows at night. No remaining stage (1B, 3, 5, 1C) had material avoidable cost.
- Dedup of sealed days was steady state, not backlog: each sealed day was re-deduplicated for ~4 days.

Rollup routing and coverage:

- ~88% of `rollup_misses_total` are shapes no tier can serve: TF self-alerts on `otel_metrics`,
  `unnest(hashes)` jobs, `hashes`/`body` regex filters, and service-map self-joins. Dashboard shapes hit ~30–35%.
- A 1h window is served from the MemBuffer tail (`tiny_interior`); that is not a routing bug.
- `rollup_dirty_partitions` is OR-only and never read. Not a backlog signal.
- `oldest_task_age` does not reset on re-arm, so repeatedly re-armed cells look starved when they are not.
- Hashes-only version appends move the physical witness on every today slice; a carry recovers ~0.7%.
  The content-fingerprint proof (W31) was the fix.
- A non-derived hourly tier built from 10-min slices undercounts (W40). Build it in whole hours (W56).
- On DataFusion 55 the plan cache bound `now()` as a nanosecond placeholder; CSE hoisted a cast the matcher
  cannot walk, and hits went to ~0. Bind instants at microsecond precision.
- `sessions_1h_v1` never matched the client query: the session key falls back to user id/email, which the
  tier does not store.
- DELETE/UPDATE on version-append sources write tombstones and versions, never DVs. Every DV on a rollup
  source is a dedup mask the rollup never counted.

Certification:

- A live day cannot be day-certified: a grant needs 00–24 proved and slices trail ~1 h. Today is served only
  by the per-file skip.
- No certification carry exists: every file-set change moves `fp`, even row-preserving compaction.
- The per-file skip was blocked by building spans from every project's files (W37). Filter per project;
  the dedup key cannot match across projects.

Capacity (W26 sim, scheduler only):

- 2x rows or projects is stable at ~60% busy; every 4x cell grows. Growing projects saturates Dedup first;
  growing rows saturates BaseRollup. DerivedRollup waits on pending base and does not scale with load.

Measurement lessons:

- The W57 incident: a correct change can still regress other tenants through cold files and planning-time
  refreshes. Check `foyer.inner_bytes_read`, `prefilter_skipped` and `delta_snapshot_refresh` rates after any
  rewrite-heavy deploy.
- A matched-hours gate on a young process gave "127x"; the mature figure was ~11–13x.
- `rollup_shared_commits_total` has no incrementer (dead since `032d64bc`).
- The sim needs `--calibrated`; uncalibrated it runs out of work and under-predicts by ~9–80x.

## 6. Tools and locations

Prod (read-only):

- Host: `ssh ubuntu@captain.s.past3.tech`, service `srv-captain--timefusion`; image tag = deployed short SHA
  (`docker service ls | grep timefusion`).
- pgwire: `psql "$(grep -m1 '^TIMEFUSION_PG_URL=' ../monoscope/.env | cut -d= -f2-)"`.
- Stats: `SELECT component,key,value FROM timefusion_stats` (resets on restart; flags under component `flags`).
- Journal copy for the sim: `docker cp <cid>:/app/data/timefusion/.timefusion_meta/maintenance_tasks.json`
  (and `.wal`), then `timefusion sim <dir> --calibrated`.
- Exported history (survives restarts): monoscope project `87576849-4941-49d3-a15d-680fef88a1a8`,
  `monoscope chart --source metrics`.

Allowed prod writes (the tier is the full rollup table name, as `ROLLUP POLICIES` prints it):

```sql
ROLLUP POLICIES otel_logs_and_spans
ROLLUP PAUSE  otel_logs_and_spans otel_logs_and_spans_rollup_sessions_1h_v1
ROLLUP RESUME otel_logs_and_spans otel_logs_and_spans_rollup_dashboard_1m_v4 FROM '2026-08-29T00:00:00Z'
FLAG SHOW
FLAG SET timefusion_maintenance_cpu_tokens 128        -- 8..256
FLAG RESET timefusion_maintenance_cpu_tokens
```

Scripts:

- `bench/prod_report.py`: one-command read-only prod report (`--window 120` for rates, `--json`).
- `~/Projects/apitoolkit/tf-helper-scripts/`: `scorecard.sh` (history in `scorecard.log`), `probe.sh`
  (18 shapes × 3 projects with rollup counter deltas), `gated_push.sh <branch> <worktree> "YYYY-MM-DD HH:MM"`
  (`gated_push_b.sh` reads CI from `$CIB`).
- `scripts/rollup_work_inventory.py`, `scripts/rollup_cpu_inventory.py`: shard and CPU-profile inventories.
- `benches/rollup_work.rs`: local ABBA/BAAB mechanism bench.
- `timefusion run-unit --source X --project Y --date Z --op BaseRollup [--tier T]`: one unit with phase timers.
- Staging: `s3://timefusion-eu/timefusion-staging/` (OVH, not R2; seeded 09-25/26), `bench/staging_seed.py`.
  There is no staging host.

Experiment and gate tooling (durable copies in `~/Projects/apitoolkit/tf-helper-scripts/`):

- `paired_ab/`: paired ABBA/BAAB harness. `load.py` (N workers of dashboard/explorer shapes against prod,
  per-query latency to `lat.jsonl`), `run.py` (toggles a `FLAG` per window: `FLAG=… A_VAL=… B_VAL=…
  WARM=120 MEAS=720`), `chain.sh` (runs several experiments back to back), `analyze.py <dir>` (per-window
  p50/p95/p99 and GB/h, paired B−A with a 95% t-interval over adjacent pairs).
- `gate/gate0929.py`: the plan-gate computation (monoscope metrics, counters differenced per process);
  `raw/` holds the fetched series for the 09-25/27/28/29 windows.
