# Rollup plan: parallel workstreams

Coordination sheet for working through
[`2026-09-24-rollups-on-a-fixed-server.md`](2026-09-24-rollups-on-a-fixed-server.md)
with several people or agents at once. The plan is the requirements source; the
[handover](2026-09-25-rollup-handover.md) holds current production state and the open-items checklist.
Claim a stream by putting your name in its **Owner** cell in one commit, before you start.

## Ground rules (read before touching anything)

- **A push to `master` is a deploy.** Every non-docs push rebuilds and redeploys prod.
  Docs-only pushes (`docs/**`) do not deploy. Only the integrator (see below) pushes code to `master`;
  everyone else pushes a branch `ws/<stream>-<topic>` and hands it over.
- **Sign off locally first:** `make ci-signoff CHECKS="fmt clippy test"` must be green for the exact tree.
  Tests: `cargo nextest run`, never `cargo test`. Lint: `cargo lint`, never bare clippy.
- **One cargo process per build cache.** Each build lane uses its own worktree under
  `~/Projects/apitoolkit/tf-<stream>` (NOT `/tmp`: a reboot wipes it) with its own `target/`.
  This machine (10 cores, 32 GB) sustains **two** build lanes; analysis-only streams need no build.
- **Production is read-only.** `ssh ubuntu@captain.s.past3.tech`: logs, `ps`, `inspect` only; never restart,
  exec-mutate, scale or touch volumes. Prod pgwire (`TIMEFUSION_PG_URL` in `../monoscope/.env`):
  `timefusion_stats` and tightly time-bounded SELECTs only; a broad scan can OOM the instance.
- **Measure on a mature process:** ≥1h since the last deploy (`docker service ps srv-captain--timefusion`),
  ≥3 samples, difference counters over a window. `timefusion_stats` resets on every deploy.
- **Bug fixes start with a failing test**, and the guard must be shown to fail with the fix reverted.
- **Stay in your files.** The ownership column below is the conflict boundary; if you must edit another
  stream's file, note it in the stream row first. `src/database/maintain.rs` and `src/rollup.rs` are large
  and shared: keep edits to the functions your stream names, and rebase often.

## Roles

| Role | Who | Responsibility |
| --- | --- | --- |
| Integrator | Claude (main session) | Owns build lane 1, merges `ws/*` branches, runs sign-off, pushes to `master`, watches deploys, keeps the handover checklist current |
| Build lane 2 | one developer or agent at a time | Code streams that need compiling (marked **build**) |
| Analysis | any number | Streams marked **analysis**: no cargo, no code on `master`; output is a findings section appended to this file or a plan doc |

## Streams

Status: ⬜ open · 🟡 in progress · ✅ done. Priority is the plan's execution order.

| # | Stream | Kind | Plan § | Deliverable | Files / area | Depends on | Owner | Status |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| W1 | Release batch + `now()` hit-rate re-measure | build (lane 1) | Execution priorities | Batch on `master`; hit rate / miss mix on a ≥1h process vs the pre-fix sample (0 hits / 1530 misses per h on `e1da854c`) | Integrator | — | Claude | 🟡 |
| W2 | Attribution baseline | analysis | "Before changes: attribution" | One-hour CPU by lane/spec reconciled with whole-process CPU; tier-usage inventory (queries per tier, routing eligibility, maintenance cost) | prod read-only, monoscope query shapes | W1 deployed + 1h | | ⬜ |
| W3 | 1B shard-count inventory | analysis | Stage 1B | Executed-unit shard-count distribution (1, 2, 3–4, 5–8, >8) weighted by estimated input bytes, from publication logs / journal — no new scans | `scripts/rollup_work_inventory.py` (+ its test), prod logs, `maintenance_tasks.json` | — | agent (Claude) | ✅ |
| W4 | Session-tier consumer inventory | analysis | First deliverable (B1) | Every reader of `*_rollup_sessions_1h_v1` (monoscope code paths, direct SQL, infrequent jobs) and a keep/pause/remove recommendation; unserved HLL review | monoscope repo (read-only), prod pgwire logs | — | agent (Claude) | ✅ |
| W5 | Per-range witness (restart recovery) | build | Stage 0 | **Deprioritized by measurement**: the 18:05 restart re-queued 26 `WitnessMoved` slices, all in the paused `sessions_1h_v1`, zero in dashboard tiers (`rollup_unverifiable_rebuild_queued` log). Revisit only if a dashboard tier shows it | — | W1 | Claude | ✅ measured |
| W6 | `multi_scan_source` shapes | analysis → build (lane 2) | Rollup misses | Classify the 192/h self-join/UNION declines (service-edges); decide: route per-leg, rewrite in monoscope, or leave raw; then implement the chosen matcher change with a case-table test | `src/rollup.rs` matcher (`source_and_filters`, `match_aggregates`) | — | agent (Claude) | ✅ |
| W7 | Stage 1 build measurement harness | build (lane 2) | Stage 1 | For representative units via `timefusion run-unit`: physical plan, scan count, hash shards, dedup ops, decoded bytes, aggregate-state memory, CPU; publication economics (commits/actions/latency incl. failures) | `src/main.rs` `run-unit`, `benches/rollup_work.rs`, report under `docs/plans/` | W3 for unit choice | Claude (timefusion-2e) | 🟡 harness handed off `ws/w7-run-unit-passes`; prod economics ✅ [report](2026-09-27-stage1-unit-economics.md); **real-S3 unit cost blocked on staging** (owner decision) |
| W8 | 1D dependency classification — design + fixtures | analysis → build (lane 2) | Stage 1D | Per-spec dependency map (dims, measures, filters, identity, ts, version, delete); mutation fixture table proving zero missed relevant invalidations; then the classifier | new test module; `apply_rollup_hours` call sites in `database/rollup.rs` | W2 (which mutations dominate) | | ⬜ |
| W9 | Stage 3 publication batching — design | analysis | Stage 3 | Concrete design mapped to existing staged publication / journal group-commit; WAL-compat decision (does the payload change?); queue limits; test list | design doc only | — | agent (Claude) | ✅ |
| W10 | Capacity replay harness | build (lane 2) | Shared gates | `timefusion sim` replay of a prod journal at 1x/2x/4x rows and 1x/2x/4x projects; report backlog stability and work per accepted row | `src/maintenance_sim.rs`, `src/main.rs` | W2 | | ⬜ |
| W11 | OpenTelemetry 0.33 upgrade | build (lane 2) | Housekeeping | Closes the `opentelemetry_sdk` advisory (unbounded baggage alloc) | `Cargo.toml`, `src/observability.rs` | — | Claude (timefusion-2e) | ✅ handed off |
| W12 | `dcount(name)` measure decision | analysis | Rollup misses | Cost/benefit for a `name` HLL + 2-way server-scope count guard on `dashboard_1m_v3` (schema comment ~L501 declines it on purpose); owner decides | `schemas/otel_logs_and_spans.yaml` (proposal only) | W2 | | ⬜ |
| W13 | Narrow the slice OCC gate (Stage 3 step 1) | build (lane 2) | Stage 3 | A non-overlapping sibling slice no longer makes a staged unit `slice_occ_stale`; a unit may retire narrower contained slices; target proof re-recorded under the commit lock; cause logging; tests 1, 3, 3b of the W9 design failing-first | `maintain.rs` `run_coordinator_rollup_selected` OCC check | W9 | Claude | 🟡 branch `ws/w13-occ-narrow` (842617b9), **top priority per W3** |
| W14 | 720-minute republication loop | analysis → build | Stage 0 / 1 | Why 58 slices (mostly 720-min `dashboard_1m_v3`) republished 431 times in 65 min (56% of publications, 228/361 GB); fix the re-mint source (census / witness moved / no-op skip miss); log the retry reason on `maintenance_task_finished` | `maintain.rs` census + no-op decision, `database/rollup.rs` | W3 | Claude | ✅ deployed `2b8c46d4` |
| W15 | Observability batch: retry reason on `maintenance_task_finished`, per-tier rollup hit counters, sessions near-miss warn → debug | build (lane 3, niced) | Attribution | Confirms W3's post-scan retry cause; per-tier hit rate | `observability.rs`, stats key lists (`server/pg_compat.rs`), finish log in `maintenance_coordinator.rs`/`maintain.rs`, one warn in `rollup.rs` | — | agent (Claude) | 🟡 |
| W16 | W2 via exported metric history | analysis | Attribution | Multi-day rollup/maintenance CPU and work attribution from OTel metrics in monoscope (`monoscope chart --source metrics`), not young-process counters | monoscope CLI, prod read-only | — | agent (Claude) | ✅ |
| W17 | W8 design: dependency classification | analysis | Stage 1D | Per-spec dependency map + mutation fixture table + where the classifier hooks in | design doc | — | agent (Claude) | ✅ design |
| W18 | W10 design: capacity replay | analysis | Shared gates | How to drive `timefusion sim` at 1x/2x/4x rows and projects; what it can/can't prove; code plan | design doc | — | agent (Claude) | ✅ design |
| W19 | Export per-lane work as OTel counters | build (lane 2) | W16 gap | Lease ms by operation/outcome, processed bytes by operation, rollup publications / published input bytes / scan bytes by tier; no stats-key changes | `src/observability.rs`, `maintenance_coordinator.rs` lease drop, `maintain.rs` byte sites | W16 | Claude (timefusion-2e) | ✅ handed off `ws/w19-lane-counters` |
| W20 | Heavy-query admission starves the largest project | analysis → build (lane 2) | Read path | Why every 87576849 dashboard query timed out waiting for a heavy slot; fix + holder attribution | `src/read/admission.rs` | — | Claude (timefusion-2e) | ✅ handed off `ws/admission-narrow-merge` |

Later stages (2, 4, 5, 6, 1C, certified-clean activation, adaptive batches) stay **conditional** on W2/W7
numbers, per the plan; do not start them without a measured residual cost.

## Handoff format

Append a section per finished stream:

```
### W<n> result — <date> — <owner>
Question · Method (commands, windows, commit/image) · Numbers (with units and sample counts) ·
Decision/recommendation · Branch (if code) · What the integrator must do next
```

Code handoffs: branch `ws/<stream>-<topic>` rebased on current `master`, `cargo lint` clean, the stream's
targeted tests green, and the failing-first test named. The integrator batches compatible branches into one
sign-off + deploy (the plan's "deploy fewer, coherent releases").

## Results

### W6 result — 2026-09-26 — agent (Claude)
**Question:** what are the ~190/h `multi_scan_source` declines, and can any route?
**Method:** 22 min of prod logs (18:05–18:25, image `eb17f52a`), 70 declines; plans traced to monoscope.
**Numbers:** all 70 come from `rollUpServiceMap` (`monoscope/src/BackgroundJobs.hs:4951`), one run per project per 5-minute slice:
`rollupServiceEdges` (`ServiceGraph.hs:719`, 6 scans, refused at `Projection: CAST(floor`) and
`rollupEndpointDependencyEdges` (`ServiceGraph.hs:785`, 12 scans, refused at `Inner Join`). They build parent→child
edges from raw `trace_id`/`span_id` joins and write Postgres tables, so no user waits on them.
**Decision:** they cannot route (every aggregate sits above a raw identity join). Leave them raw. They stay out of the
hit-rate denominator, and their per-query 2–3 KB warn is sampled: branch `ws/w6-eligible-misses` (`c36be3d8`),
with the test asserting `rollup_misses_total` does not move for a multi-scan plan.
**Follow-ups:** (1) monoscope could compute `trace_count` without referencing `bucketed` twice (12 → 6 scans; unmeasured, W2/W7).
(2) 21 of 26 `unwalkable_source` are `autoAckProvenEndpoints`' `unnest(hashes)` query (`BackgroundJobs.hs:3709`), also raw-only.

### W4 result — 2026-09-26 — agent (Claude)
**Question:** does `sessions_1h_v1` have a consumer; is there an unserved HLL?
**Method:** full monoscope inventory (deployed `fb0cac263`), router eligibility per shape, 25 min prod logs, `ROLLUP POLICIES`.
**Numbers:** 0 deployed query shapes can route to v1. The one query written for it (RUM `otelSessionCoreRows`,
`RealUserMonitoring.hs:369`) needs a browser-only filter no measure stores, plus `MAX(service_name)` and an env filter.
`fetchSessions` (Log Explorer) is pinned unroutable by TF tests. 0 direct reads of the table. 232/284 near-miss warns
name `sessions_1h`, all dashboard server-scope filters sharing the `name` column — noise, not demand.
**Decisions:** keep `sessions_1h_v1` **paused** (it is; durable). Owner decisions: (a) remove v1, or spec a browser-scoped v2
for RUM; (b) `service_name_hll` (dashboard tiers) is computed on every build but never served
(`MEASURES_NOT_YET_SERVABLE`, `rollup.rs:1017`, pre-08-26 sketches empty) — audit stored sketches and unblock, or drop it.
**Follow-ups (optional):** demote the sessions near-miss warn to debug; per-tier hit counters.

### W9 result — 2026-09-26 — agent (Claude)
**Design:** [`2026-09-26-stage3-publication-design.md`](2026-09-26-stage3-publication-design.md) — no WAL change.
**Numbers (provisional; process 24–34 min old):** each rollup unit is one Delta commit + one journal fsync; ≈10.8k rollup
commits/day, 2.1 actions/commit; publications are <1% of journal checkpoints. `rollup_shared_commits_total` is dead
(its incrementer was deleted in `032d64bc`). `slice_occ_stale` discarded 17 of 78 publications in a 10-min window
(243/405 since boot) — each throws away staged output and rescans (≤3% of base-rollup time, ≈0.6 workers).
**Estimated batching saving:** 10 s linger → ≈6.1k commits/day, 60 s → ≈2.7k; ≤1 worker-hour/day of commit time.
**Decision:** ship step 1 (W13: narrow the OCC gate) now; build the batched queue (steps 2–3) only when Stage 4 is
approved or publication volume grows. Re-measure rates on a ≥1h process.

### W11 result — 2026-09-26 — Claude (timefusion-2e)
**Question:** close the `opentelemetry_sdk` advisory. GHSA-w9wp-h8wv-79jx (unbounded W3C Baggage allocation)
affects `<= 0.32.0`; first patched release is **0.32.1**, so bare 0.32.0 would not close it.
**Method:** `metrics-exporter-opentelemetry` 0.2.1 (its latest release) pins opentelemetry 0.31, so the stack cannot
move without it. Upstream main is already on 0.33 with `Recorder::with_meter` unchanged, so it is pinned by git rev
(`b93abba8`) instead of vendored. The whole family went to **0.33** (`opentelemetry`/`_sdk`/`-otlp`/`-appender-tracing`
0.33, `tracing-opentelemetry` 0.34) via `cargo update -p` on those crates only. Lock delta: the OTel family plus
`prost-types`/`tonic-types` 0.14 (no second tonic/prost major); `tokio-postgres` 0.7.16 / `postgres-types` 0.2.12
unchanged. No source change needed; only a comment's version number. Metric names unchanged.
**Numbers:** `cargo lint` clean; `cargo nextest run observability stats telemetry` 25/25. No test calls
`init_metrics`/`init_telemetry`, so runtime was smoke-tested: the dev binary against local MinIO and a throwaway
`otel/opentelemetry-collector` (debug exporter) received 510 spans, 329 log records and 57 metric data points in
~2 min. Names included facade-recorded `timefusion.scan.pgwire_total`, which proves the git-pinned bridge.
Clean shutdown, no panic.
**Branch:** `ws/w11-otel-032` @ `8e3d65b4`, one commit on `30cd0c59` (touches `Cargo.toml`, `Cargo.lock`,
`src/observability.rs`).
**Integrator next:** batch into the next sign-off. Swap the git pin for a crates.io release when upstream publishes
one (> 0.2.1).

### W3 result — 2026-09-26 — agent (Claude)
**Question:** executed hash-shard distribution (Stage 1B exposure).
**Method:** 65 min of prod logs (18:05–19:10, young process), executed `hash_shards` from publication events, units
paired start/publish/finish; reconciles with `rollup_scan_cohorts_total` to 0.4%.
**Numbers:** 2,230 executed units / 3,670 shard passes: 1 shard 820 units, 2 shards 1,380, 3–4 shards 30, >4 none.
73% of published input ran >1 shard (each pass re-reads ~1.7 GB of whole files).
**Bigger findings:** (1) **69% of scan passes (2,528/3,670) belong to units that scanned and then did not publish** —
1–10 min `dashboard_1m_v3` slices retrying after 1–4 s, cause inferred as `slice_occ_stale` → W13 is top priority.
(2) **58 slices republished 431 times** (56% of publications, 228/361 GB), mostly 720-min slices → W14.
(3) 62% of published units were a 2nd+ attempt.
**Decision:** 1B shard capping is a small lever; do W13 and W14 first; log the retry reason on `maintenance_task_finished`.

### W14 result — 2026-09-26 — agent (Claude) + integrator
**Question:** why 58 slices republished 431 times in 65 min.
**Root cause (defect):** sealed days (09-05…09-17) holding one pre-`output_rows` tier file that straddles the new half-day
split (e.g. 28f62f01/09-14 has a 04:00–14:00 file). Overlapping tagged ranges in one (project, generation) make
`rollup_output_coverage` drop the group (`counts.retain`), so the census sees a whole-day hole every 1–2 min; the 837 MB
day splits into two 720-min halves (`split_time_task`); each half republishes identical rows; `slice_retires` retires only
CONTAINED files, so the straddler survives; the no-op skip uses the same coverage and declines. Each republish also
reopens the 1h derived tier (97 repeated 1440-min `dashboard_1h_v2` publications). Not ingest, not source maintenance.
**Fix:** `slice_retires` also retires a tagged file WITHOUT the `output_rows` proof when it overlaps the published slice
and the other live slices tile its range (guarded against a gap and against proven files); case-table test shown red
without the rule. Current publications always stamp `output_rows`, so no new straddlers are created.
**Verified (image `2c97a1b5`, 20:02 deploy, 25 min window):** no sealed-day slice repeats; top repeats are today's live 10-min slices (ingest churn); census `cells_wanted` 1–2/tick (was 2–6); publications ≈530/h vs ≈708/h in W3's window.

### W18 result — 2026-09-26 — agent (Claude)
**Finding:** `timefusion sim` cannot measure a ROWS axis yet: unit duration (`duration_range_secs`, calibrated 09-03 from
676 prod units) depends only on operation and width, and a real journal runs with `byte_model = None`.
**Design (W10 code, ~1 lane-day):** `ByteModel::from_journal` + byte-priced `unit_secs` (keep each operation's no-op share;
expensive mode = `fixed + secs_per_byte × bytes`, slope from prod `maintenance_task_finished.ran_secs` joined to journal
bytes); `--rows K` scales bytes per day, `--projects K` clones every ingesting stream (keeps the whale/small mix);
`--matrix`, `--drain-hours`, `--now <fetch time>`; metrics: pending bytes, worker-secs and bytes per accepted row
(timeouts charged), backlog slope, drain hours; 5 case-table tests.
**Limits:** IO-free — proves backlog stability/fairness/lag, not CPU contention, latency, memory or object requests; recent
admission changes (CPU-token pricing, client-query yield) are not in `SimConfig`. 1x must reproduce prod executions/h first.
**Journal fetch (read-only):** `docker cp <cid>:/app/data/timefusion/.timefusion_meta/maintenance_tasks.json` (+ `.wal`, ~1.5h newer).

### W17 result — 2026-09-26 — agent (Claude)
**Finding:** a classifier at the DML append alone saves ~nothing today: MoR version rows keep their timestamp, so the flush
moves the partition fingerprint + row witness below `covered_through`, and `reconcile_maintenance_task_cursors`
(60 s loop) re-mints BaseRollup for every spec from the untagged Add. Acting on the verdict needs Stage 2 (witness carry
+ read path accepting fingerprints moved only by irrelevant commits).
**Ship now (shadow):** carry assigned-column evidence (`WriteOrigin::VersionAppend`) through the bucket to the flush
commitInfo; `RollupSpec::dependencies` at schema load; pure `classify(mutation, deps, hour_seq)` with a stale-replacement
guard (per-hour last-relevant sequence vs the statement's `read_seq`); shadow counters per spec. Suppresses nothing.
**Traffic:** TF's UPDATEs are monoscope `updateHashesSql`/`update2Sql` (`hashes` only → irrelevant to every spec) and the
session backfill (irrelevant to dashboard tiers, relevant to the paused sessions tier). Epoch keying is per
(project, source, date) across all specs — per-target epochs needed for the session backfill case.
**Fixtures:** 25-case table (tombstones, stale full-row re-sends, guard trips, mixed statements, replay, reconcile …).

### W16 result — 2026-09-26 — agent (Claude)
**Source:** TF metrics/logs live in monoscope project `87576849-4941-49d3-a15d-680fef88a1a8`; counters differenced per
process (max per 5 min, drop = restart); CPU from `container.cpu.usage.total`; per-lane work from
`maintenance_task_finished` / `maintenance_rollup_published` logs (match host counts exactly).
**Baseline "before" (window B, process `ihftpuuf`, 09-24 23:00–09-25 20:00, 22 h):** 33.6 cores; 14.1 queries/s;
maintenance 125.5k lease-s/h (~35 busy workers; wall time, not CPU), 83% BaseRollup; 511 publications/h
(sessions 208, dashboard_1m 136, dashboard_1h 78, metrics_1m 65, metrics_1h 24); 195.6 GB/h processed (71% sessions;
scan-side ≈7x, not exported); 96% of leases ended in Retry; hit rate 5.3% outside the midnight `not_built` burst.
**Churn:** 65 process starts in 7.8 days; a 02:10–04:15 crash loop today (9 starts).
**Sessions pause** freed capacity that dashboard_1m immediately consumed (136 → 352–387 publications/h) — W14's loop.
**Hit collapse:** 40–72 hits/h until 11:00 today, near 0 from the **12:10** deploy (packed repairs on), not fixed by
the 18:05 `now()` fix — consistent with W14's unreadable overlapping cells; being re-measured after the W14 fix.
**Gaps:** no per-lane CPU metric; `work.*`, `rollup_scan_*`, `processed_bytes` are stats-only (not exported);
`worker_secs` never recorded → export per-operation `ran_secs` and per-tier publications/bytes as OTel counters.

### W19 result — 2026-09-27 — Claude (timefusion-2e)
**Question:** W16 could not show that W13/W14 saved work, because per-lane work lived only in logs and in
stats keys that reset on deploy. **Method:** five OTel counters wired like `rollup_hits`, with `timefusion_stats`
keys unchanged:
- `timefusion.maintenance.lease_ms{operation,outcome}`, recorded in `TaskLease::drop` beside `maintenance_task_finished`.
- `maintenance.processed_bytes{operation}`: all three existing byte sites go through one helper that still bumps the stats atomic.
- `rollup.publications{tier}` and `rollup.published_input_bytes{tier}`.
- `rollup.scan_estimated_bytes{tier}`: every pass, including failed and repeated shard passes.

`tier` is the rollup table, the same label as the hit counter; there is no `project_id`.
**Read it as:** `scan_estimated_bytes / published_input_bytes` per tier is the wasted-scan share W13 should move;
`lease_ms{outcome=Retry}` is the lease time spent on units that did not complete.
**Numbers:** `cargo lint` clean. `nextest --lib observability|maintenance_coordinator|rollup` 456/456. The guard
`lane_work_is_exported_per_operation_and_tier` (SDK `ManualReader`; dev-dependency feature
`experimental_metrics_custom_reader`, which adds no crates) goes red when one export is removed.
**Branch:** `ws/w19-lane-counters` @ `4e89c823` on `cf05ac93`. **Integrator next:** batch it; after the deploy,
compare the wasted-scan share per tier on a ≥1 h process.

### W20 result — 2026-09-27 — Claude (timefusion-2e)
**Question:** at 21:46 UTC every query on project 87576849 failed after 30 s with "too many concurrent heavy
queries". The same shapes on other projects took 0.3–25 s.
**Numbers (prod, image `6667af57`, read-only):**
- Every whale dashboard shape plans `AdmissionExec class=ordered_mor_merge`, fan-in 26 (1 h/6 h) or 7 (24 h),
  over a 5-column merge. Other projects' fan-in is 1, so they are never gated.
- K = `query_pool / (64 MiB × partitions × 2)` = 24,576 / (64 × 24 × 2) = **8**. The queue wait is 30 s.
- The gate is saturated in steady state: ~190 admissions per ~25 s against 8 slots, and timeouts went
  176 → 192 while measuring. Only 18% of admissions are ordered merges; 82% are unbounded sorts.

The slot holders cannot be named from existing logs. `record_statement_latency` runs when `do_query` returns,
before rows stream, so `slow_statement` excludes execution time: a 5.1 s whale probe left no line.
**Fix:** `ws/admission-narrow-merge` @ `5a4a7667`.
- (A) A merge is gated only when fan-in × batch × estimated row bytes exceeds a 64 MiB sort reservation.
  The whale count (~15 MB) is ungated. The full-width table at fan-in 7 and batch 4096 (~383 MB) stays gated.
- (B) A permit held over 5 s logs `heavy_query_held` inside its own statement span, whether completed or cancelled.

Guards: `a_narrow_whale_count_merge_is_not_heavy` (red on old code) and `a_full_width_log_explorer_merge_stays_heavy`.
Lint is clean and 30/30 admission tests pass, including e2e.
**Integrator next:** batch it. After the deploy, `EXPLAIN` of a whale count shows no `AdmissionExec`, and
`heavy_query_queue_timeout` flattens on a ≥1 h process. `heavy_query_held` then attributes the unbounded-sort share.

### W7 result (part) — 2026-09-27 — Claude (timefusion-2e)
**Report:** [`2026-09-27-stage1-unit-economics.md`](2026-09-27-stage1-unit-economics.md). It covers 60 min of process `cc6bfa40`
(42–102 min uptime), read-only.
**Numbers:**
- BaseRollup completions held 48,812 lease-s/h (about 13.6 workers) but scanned only 75 GB/h physical.
  Dedup scanned 899 GB/h.
- Failed rollup attempts are now cheap: Retry plus Superseded held 270 s/h, against W16's 96%-retry baseline.
- `slice_occ_stale` is down to 8/h.
- Top shapes: `metrics_1m_v2` 180 m (30,009 s/h, 128 units, one third at 2 shards) and `dashboard_1m_v3`
  720 m (about 410 s per unit).

**Next:** those two shapes are the staging `run-unit --explain` units. Real-S3 per-unit cost is blocked until an
owner creates a staging prefix.

