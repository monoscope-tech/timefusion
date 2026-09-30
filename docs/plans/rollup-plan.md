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
| Plan gate | CPU-seconds and maintenance processed bytes per million ingested rows, matched hours (14:00–18:00 UTC), process ≥2 h old, no deploy inside the window. CPU baseline: 09-25 14:00–18:00 (clean). Bytes and lease baseline: 09-27 (restarts inside, state the caveat) | **Not accepted.** 09-29: 9,676 CPU-s/M rows (14.0 cores), 2.2x below the 21,723 baseline; 12.8 GB/M rows (HotPacking 6.2, BaseRollup 5.0, Dedup 1.3); lease 1,079 s/M rows. The backlog grew, so it fails |
| Falling backlog | Pending base / dedup falls over the gate window | **Not shown in a gate window yet.** 09-29 14–18: base 303 → 394, dedup 161 → 206 (W58 strip of today's DV files, v4 backfill). By 23:43 after drop-v3: backlog bytes 33 GB (morning 171–309 GB), pending base 189, dedup 126. Confirm in the next 14:00–18:00 window |
| Paired protocol | Every percentage gate uses counterbalanced ABBA then BAAB blocks, same hours, 95% bounds that account for time correlation. A savings gate passes on its lower bound, a regression gate on its upper bound. Inconclusive means extend, not pass | **First runs in progress** tonight via runtime `FLAG` (no restarts). Never yet used for an accepted gate |
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
- **Dark flags (FLAG-switchable):** `timefusion_maintenance_cpu_tokens` (8..256; prod runs 66). Byte admission for wide scans
  stays behind config flags. W21 witness carry stays off.
- **Latency:** p95/p99 are recorded at stream end since 18:09. Values before that excluded streaming time
  and are not comparable.
- **Last clean numbers (09-29 14:00–18:00, image `8bea392b`):** see the gate table. Shipbubble 24h status
  ~0.3–1.6 s, 24h by service ~0.3–1.0 s, 6h window ~0.5 s. Dashboard-shape hit rate ~30–35%.

## 4. Task list

Done items carry evidence (commit or number). Open items carry the next action, blocker or owner.

### Gates and experiments

- [ ] **Plan gate on a clean day.** Re-run after the v4 backfill settles and W58's strip debt drains.
      No code push 12:00–18:00; run `gate0929.py`-style at 18:05. Must show the backlog falling.
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
- [ ] `ws/recovery-adopt-v2` (`3ff9bb66`): deprioritized (~4 requeues per restart). Decide finish or drop.

### Stage 1: measurement and repeated work

- [x] W3 shard inventory, W7 `run-unit` harness and prod unit economics, W16 exported history, W19 lane counters.
- [x] W7 real-object-store unit cost (09-29, staging prefix on OVH): large units CPU-bound, small units
      fetch-bound, staging+commit ≤4 s per unit; isolated whale 12h unit ~150 s vs ~410 s prod lease.
- [x] 1B closed by measurement: byte-weighted shard amplification 1.00x (all units single-shard).
      Reopen only if large units return (v4 31-day backfill of the whale).
- [x] 1D closed by measurement: ~0% irrelevant churn; W8 shadow classifier and W17 design shipped.
- [ ] **Stage 1C shared scans** — trigger: two useful consumers of one source scan and measured duplicate
      preparation; gate ≥20% lower combined CPU. The 09-28 ranking put today's cells (Dedup + BaseRollup
      reading the same 10-min cells) at ~800 lease-s/h, a small saving. **Now measurable:** with
      `sessions_1h_v2` resumed, dashboard and session tiers both read the same source. Measure duplicate
      preparation once the v4 and sessions backfills settle, then decide.

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
- [ ] Byte admission for wide scans (FLAG `timefusion_query_scan_byte_admission`). ABBA 09-30 04:05–04:43 on the
      mixed sealed-day load (4 workers): the gate never queued (12,646 admitted, 0 queued; ~10 GB/h scanned, peak
      23.5 GiB), p99 0.78 s ON vs 0.83 s OFF. So it costs nothing when idle, but this load cannot show what it
      is for. Next: a ramp with the gate ON, 8/12/16 raw-scan workers under a 60 GiB watchdog (`abba/ramp`). Turn
      it on by default if memory stays bounded and `scan_bytes_queued` rises; delete it if it never binds.
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
- [ ] Parks are in-memory: each restart retries a refused set once. Decide if persistence is worth it.
- [ ] Re-measure the 2-day-old shipbubble trace lookup once 09-26/27 are fully stripped (last: 15.9 s cold, 3.1 s warm).
- [ ] Watch query latency against the 12-per-10-min cap while today's strip debt drains (it dominates 09-29 HotPacking).

### Tiers

- [x] W29 v4 / `1h_v3` with `level`; v4-fallback relabel; `run-unit --tier`.
- [x] v4 widening: 09-21 → 09-14 → 09-07 → `RESUME FROM 2026-08-29` (19:02, all 31 days).
- [x] drop-v3 (`4319b951`): owner overrode the contiguity gate (queries older than 14 days are rare).
- [x] v4 backfill: contiguous days min/median 30/30 (09-30 04:07).
- [ ] Re-rank residual cost once the backfill finishes (1B and 1C may matter for large units).
- [x] W28 `service_name_hll` unblocked (`e0e27dbe`).
- [x] RUM measures on v4 (`180ced63`, `8bea392b`); measure re-mint backfill for older cells (`8184a638`, in progress).
- [ ] RUM widgets (monoscope branch merged into `overnight-exploration-29-09`) route to v4 `rum_*` measures:
      confirm hits in prod once the backfill re-mints historical cells. 09-30 04:20: the 7-day sealed Talstack
      page-view query still runs raw (3.4–4.2 s, `measure_not_stored`). Re-mints run at 2 per pass, about 25–44
      per hour, so expect several more hours.
- [ ] W12 `name` HLL (owner): add the unfiltered `name_hll` only if a daytime sample shows service-tab misses.

### Sessions

- [x] W4: `sessions_1h_v1` had no consumer; stays paused.
- [x] W56 hour-grain session tiers (the proper W40 fix, `ecc2483f`). `rollup_miss_sub_grain_slices_total` read 0.
- [x] W27 `sessions_1h_v2` resumed from 2026-09-22 (18:08).
- [x] Land `test/rum-sessions-e2e` (`prepared_rum_sessions_use_browser_rollups_and_raw_edges`, passes) with the next deploy.
- [ ] Confirm the RUM sessions query routes to `sessions_1h_v2` in prod on a mature process.
- [x] Dropped `sessions_1h_v1` (`6c3e8b3b`, deployed 09-30 04:45); its durable pause is retired at boot with its queued work.

### Scheduler and ops

- [x] W10 sim, W18, W26 matrix, W11 OTel 0.33, W15 observability, W25 part 4 (rollout phase timing),
      ingest dirty-livelock fix (`d7d44054`, `3bfa405e`, `4ee017dd`), task-age-from-reopen metric.
- [ ] Journal lock wait: recheck on a ≥1 h process (mature ~7 s/min; 14–18 s/min seen on young ones).
      `coordinator_claim` ~371/s is the baseline, not a regression.
- [x] `oldest_due_unclaimed_age_seconds` added (`f62fb4ae`).
- [ ] W25 parts 2–3: the 4.9 s container create → start; CapRover's double service update (1.6 s per deploy).
- [ ] Dependency advisories not closable by patch bumps: rustls-webpki 0.101 (AWS SDK), tokio-tar,
      tokio-postgres/postgres-types (0.2.14 breaks arrow-pg encoding).
- [x] HotPacking "selected nothing" / 9 invariant violations were false alarms (a strip outranking a pair); now
      `refused_cells` via `select_bin`, WARN only on a real refusal (`02f9f0a1`, `27440d30`). Ledger
      disagreements are a boot burst (the ledger is written only at replay); seeded coverage is gated by the
      same output + row checks as replay.
- [ ] `ordering_pushdown::one_unsorted_file_does_not_cost_the_majority_its_ordering` e2e flakes under load
      (passes alone). Leave unless it blocks signoff.
- [x] Old-process shutdown can panic in `buoyant_kernel_engine` executor (`RecvError`) after the WAL drained.
      Cause: cancelling maintenance dropped the maintenance runtime at once, under in-flight kernel IO. Fixed:
      that runtime now outlives `maintenance_tasks_tracker` (preload + dedup cron now tracked).

- [x] **Two DV-strip tests fail by design 00:00–03:00 UTC** — fixed (`c2464d2a`): `compact.rs` seal gate + sweep dates read the virtual clock; tests pin tomorrow noon. (`rollup_noop_skip_tests::todays_dv_files_strip_under_a_live_rollup_slice`,
      `a_retired_corpse_goes_first_and_counts_as_progress`: "dedup masks only bins sealed 2 h; run after 03:00 UTC"),
      which blocks every signoff for 3 h a day. Pinning `support::set_micros` alone is not enough: some part of the dedup
      path still judges "sealed 2 h" by wall time. Find it, route it through the virtual clock, then pin the tests.
- [ ] **Synthetic prod load OOM'd prod** (09-29 23:47, 16 workers of raw sealed-day scans; wide-scan decode memory is
      outside the query pool). Before any heavy raw-scan experiment: enable `timefusion_query_scan_byte_admission` (FLAG)
      and cap raw-shape workers at ≤4. Byte admission's A/B is next.

### Monoscope-owner decisions

- [ ] **Session lookup by `attributes___session___id = ANY($ids)`** (~34 s over 16 h): the column is `mutable: true`,
      so bloom, tantivy and stats pruning all refuse it (correctness gate). Options: (a) look up by an immutable key
      (`id`, `context___trace_id`); (b) add an "enrichment-only" column class (values only go NULL → set) that the
      pruners may trust; (c) two-phase key lookup. TF side shipped only OR-chain bloom pruning for immutable columns.

- [ ] Pattern-tag `update2Sql` source fix (`BackgroundJobs.hs` ~2944): ~40% of today's masked rows are its
      MoR versions, and it blocks certification.
- [ ] `array_has` lowering for `hashes` predicates (`monoscope/plans/array-has-lowering.md`, ~40x cheaper).
- [ ] Back off issue-chart auto-refresh after a timeout (Talstack `jsonb_path_exists(to_jsonb(hashes))` charts hit 90 s).
- [ ] Add a timeout to the `Issues.hs:488-497` session lookup (`tryWithin Nothing`; stalls up to 77 s).
- [ ] Optional: compute `trace_count` in `rollupServiceEdges` without referencing `bucketed` twice (12 → 6 scans).

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
FLAG SET timefusion_query_scan_byte_admission ON     -- ON|OFF
FLAG SET timefusion_maintenance_cpu_tokens 128        -- 8..256
FLAG RESET timefusion_query_scan_byte_admission
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
