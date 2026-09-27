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

- **Deploy windows (from 2026-09-27):** every code push restarts prod and resets the counters measurements depend on
  (dedup waves take >40 min; mature-process reads need ≥1–2 h). The integrator batches code into windows with a
  **≥2 h quiet gap** between deploys. Current window: quiet until **10:20 UTC** (last deploy `d56a02d6` at 08:17).
  Docs-only and `scripts/deploy/**` pushes are always fine.

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
| W2 | Attribution baseline | analysis | "Before changes: attribution" | One-hour CPU by lane/spec reconciled with whole-process CPU; tier-usage inventory (queries per tier, routing eligibility, maintenance cost) | prod read-only, monoscope query shapes | W1 deployed + 1h | | ✅ covered by W16 + W19 counters |
| W3 | 1B shard-count inventory | analysis | Stage 1B | Executed-unit shard-count distribution (1, 2, 3–4, 5–8, >8) weighted by estimated input bytes, from publication logs / journal — no new scans | `scripts/rollup_work_inventory.py` (+ its test), prod logs, `maintenance_tasks.json` | — | agent (Claude) | ✅ |
| W4 | Session-tier consumer inventory | analysis | First deliverable (B1) | Every reader of `*_rollup_sessions_1h_v1` (monoscope code paths, direct SQL, infrequent jobs) and a keep/pause/remove recommendation; unserved HLL review | monoscope repo (read-only), prod pgwire logs | — | agent (Claude) | ✅ |
| W5 | Per-range witness (restart recovery) | build | Stage 0 | **Deprioritized by measurement**: the 18:05 restart re-queued 26 `WitnessMoved` slices, all in the paused `sessions_1h_v1`, zero in dashboard tiers (`rollup_unverifiable_rebuild_queued` log). Revisit only if a dashboard tier shows it | — | W1 | Claude | ✅ measured |
| W6 | `multi_scan_source` shapes | analysis → build (lane 2) | Rollup misses | Classify the 192/h self-join/UNION declines (service-edges); decide: route per-leg, rewrite in monoscope, or leave raw; then implement the chosen matcher change with a case-table test | `src/rollup.rs` matcher (`source_and_filters`, `match_aggregates`) | — | agent (Claude) | ✅ |
| W7 | Stage 1 build measurement harness | build (lane 2) | Stage 1 | For representative units via `timefusion run-unit`: physical plan, scan count, hash shards, dedup ops, decoded bytes, aggregate-state memory, CPU; publication economics (commits/actions/latency incl. failures) | `src/main.rs` `run-unit`, `benches/rollup_work.rs`, report under `docs/plans/` | W3 for unit choice | Claude (timefusion-2e) | 🟡 harness handed off `ws/w7-run-unit-passes`; prod economics ✅ [report](2026-09-27-stage1-unit-economics.md); **real-S3 unit cost blocked on staging** (owner decision) |
| W8 | 1D dependency classification — design + fixtures | analysis → build (lane 2) | Stage 1D | Per-spec dependency map (dims, measures, filters, identity, ts, version, delete); mutation fixture table proving zero missed relevant invalidations; then the classifier | new test module; `apply_rollup_hours` call sites in `database/rollup.rs` | W2 (which mutations dominate) | Claude (timefusion-2e) | ✅ shadow classifier handed off `ws/w8-shadow-classifier` |
| W9 | Stage 3 publication batching — design | analysis | Stage 3 | Concrete design mapped to existing staged publication / journal group-commit; WAL-compat decision (does the payload change?); queue limits; test list | design doc only | — | agent (Claude) | ✅ |
| W10 | Capacity replay harness | build (lane 2) | Shared gates | `timefusion sim` replay of a prod journal at 1x/2x/4x rows and 1x/2x/4x projects; report backlog stability and work per accepted row | `src/maintenance_sim.rs`, `src/main.rs` | W2 | | ✅ `e9ce6dc3`; calibrated 1x replay handed off `ws/w10-calibrate` (all ops within ±20% of prod, steady stock; see W10 calibration) |
| W11 | OpenTelemetry 0.33 upgrade | build (lane 2) | Housekeeping | Closes the `opentelemetry_sdk` advisory (unbounded baggage alloc) | `Cargo.toml`, `src/observability.rs` | — | Claude (timefusion-2e) | ✅ handed off |
| W12 | `dcount(name)` measure decision | analysis | Rollup misses | Cost/benefit for a `name` HLL + 2-way server-scope count guard on `dashboard_1m_v3` (schema comment ~L501 declines it on purpose); owner decides | `schemas/otel_logs_and_spans.yaml` (proposal only) | W2 | Claude (timefusion-2e) | ✅ analysis — owner decides (see result) |
| W13 | Narrow the slice OCC gate (Stage 3 step 1) | build (lane 2) | Stage 3 | A non-overlapping sibling slice no longer makes a staged unit `slice_occ_stale`; a unit may retire narrower contained slices; target proof re-recorded under the commit lock; cause logging; tests 1, 3, 3b of the W9 design failing-first | `maintain.rs` `run_coordinator_rollup_selected` OCC check | W9 | Claude | ✅ on master (`7e03e0e6` + `202b10b0`) |
| W14 | 720-minute republication loop | analysis → build | Stage 0 / 1 | Why 58 slices (mostly 720-min `dashboard_1m_v3`) republished 431 times in 65 min (56% of publications, 228/361 GB); fix the re-mint source (census / witness moved / no-op skip miss); log the retry reason on `maintenance_task_finished` | `maintain.rs` census + no-op decision, `database/rollup.rs` | W3 | Claude | ✅ deployed `2b8c46d4` |
| W15 | Observability batch: retry reason on `maintenance_task_finished`, per-tier rollup hit counters, sessions near-miss warn → debug | build (lane 3, niced) | Attribution | Confirms W3's post-scan retry cause; per-tier hit rate | `observability.rs`, stats key lists (`server/pg_compat.rs`), finish log in `maintenance_coordinator.rs`/`maintain.rs`, one warn in `rollup.rs` | — | agent (Claude) | 🟡 |
| W16 | W2 via exported metric history | analysis | Attribution | Multi-day rollup/maintenance CPU and work attribution from OTel metrics in monoscope (`monoscope chart --source metrics`), not young-process counters | monoscope CLI, prod read-only | — | agent (Claude) | ✅ |
| W17 | W8 design: dependency classification | analysis | Stage 1D | Per-spec dependency map + mutation fixture table + where the classifier hooks in | design doc | — | agent (Claude) | ✅ design |
| W18 | W10 design: capacity replay | analysis | Shared gates | How to drive `timefusion sim` at 1x/2x/4x rows and projects; what it can/can't prove; code plan | design doc | — | agent (Claude) | ✅ design |
| W27 | `sessions_1h_v2`: browser-scoped session tier for RUM `otelSessionCoreRows` | build (lane 2) | Owner decision | Spec with browser filter in measures + service measure; routes the RUM sessions query; 7-day backfill; v1 stays paused | `schemas/otel_logs_and_spans.yaml`, `rollup.rs` tests | W4 | | ⬜ |
| W28 | `service_name_hll` audit → unblock | analysis → build | Owner decision | Prove stored sketches over the served window are non-empty/current; then remove it from `MEASURES_NOT_YET_SERVABLE` | tier reads (time-bounded), `rollup.rs:~1017` | — | | ⬜ |
| W29 | Dashboard v4 with `level` dimension | build | Owner decision | `dashboard_1m_v4` + derived `1h_v3` = v3 + `level`, REPLACING v3 (31-day backfill; dual-run until covered, then drop v3/1h_v2); status chart `COALESCE(status_code, level)` routes | schema yaml, routing tests | — | Claude (timefusion-7c) | 🟡 ws/w29-dashboard-v4 (after W26 claim index, rebased on W27) |
| W30 | R2 staging prefix + guard | build | Owner decision | `timefusion-staging/` prefix with prod creds; startup guard refusing prod table paths; `run-unit` against it | config / `main.rs` run-unit | — | | ⬜ |
| W31 | Stage 2: rollup freshness that survives version appends | design → build (lane 1) | Stage 2 | Today-window hits: witness that ignores spec-irrelevant physical churn (logical row count / captured source view) | `database/rollup.rs`, `maintain.rs`, `write/` | W8, W21 | Claude | 🟡 |
| W19 | Export per-lane work as OTel counters | build (lane 2) | W16 gap | Lease ms by operation/outcome, processed bytes by operation, rollup publications / published input bytes / scan bytes by tier; no stats-key changes | `src/observability.rs`, `maintenance_coordinator.rs` lease drop, `maintain.rs` byte sites | W16 | Claude (timefusion-2e) | ✅ handed off `ws/w19-lane-counters` |
| W20 | Heavy-query admission starves the largest project | analysis → build (lane 2) | Read path | Why every 87576849 dashboard query timed out waiting for a heavy slot; fix + holder attribution | `src/read/admission.rs` | — | Claude (timefusion-2e) | ✅ handed off `ws/admission-narrow-merge` |
| W21 | Carry the rollup witness across rollup-irrelevant version appends | build (lane 2) | Stage 0/2 | hashes-only MoR UPDATEs keep today's slices readable; dark flag + shadow counter; failing-first + guard tests | `dml.rs` append, `database/write.rs` flush commit, `maintain.rs` WitnessCarry | W17 | Claude (timefusion-2e) | ✅ on master `54327594` (dark) |
| W22 | Dedup scan share: steady state or backlog? (Stage 5 input) | analysis | Stage 5 | Is dedup's ~12x larger physical scan a draining backlog or recurring work | prod logs, exported `pending_dedup` | W7 | Claude (timefusion-2e) | ✅ steady state (see result) |
| W23 | Dedup certification that survives fingerprint moves (W22 lever) | analysis → build | Stage 5 | Step 1: what moves sealed-day certification, and which moves are dedup-preserving; step 2 (dark carry) only if step 1 finds the volume | `maintain.rs` certification, `commit_wave` | W22 | Claude (timefusion-2e) | 🟡 step 1 done; denial attribution handed off `ws/w23-deny-attribution` (measure 1 h after deploy, then pick lever a/b) |
| W24 | 24 h status-breakdown shape misses as `unsupported` | analysis | Rollup misses | Why `COALESCE(coalesce(status_code, level)::text, 'null')` never routes | `src/rollup.rs` matcher (test ~3059) | — | Claude (timefusion-2e) | ✅ by design; routing it needs `level` as a dimension (owner decision, see result) |
| W25 | Deploy rollout availability (32 s unready vs 30 s budget) | analysis | Deploys | Where the client-visible unready interval goes, and fixes | `scripts/deploy/rollout.sh`, swarm spec, `src/main.rs` shutdown | — | Claude (timefusion-2e) | ✅ (4) handed off `ws/w25-rollout-timing`; (1) skipped (unprovable locally); (2)/(3) open |
| W26 | Capacity matrix: calibrated sim at rows/projects 1/2/4 | analysis | Shared gates | First saturating lane and stability threshold per cell | `maintenance_sim.rs`, prod journal copy | W10 | Claude (timefusion-2e) | ✅ headroom ~2x on 66 workers; saturates at 4x (part 2) |
| W32 | Maintenance CPU-token cap vs idle cores | analysis → build | Shared gates | With ≥300 tasks due: median 26/48 cores while `cpu_tokens_used` = 66/66; `admission_refused_cpu` 24–83k/day → ~1.5M/day after `d9f00cce` (1 token/64 MiB decoded). Find the resource that binds (store vs cap); staging experiment at a higher cap | `maintenance_coordinator.rs` `AdmissionController::try_acquire_for` / `lag_scaled_cpu_ceiling`; token pricing in `maintain.rs` | W30 | Claude (timefusion-7c) | 🟡 analysis |
| W33 | Stage 5: fewer repeat dedups of sealed days | analysis → build | Stage 5 | Dedup scans 899 GB/h vs 75 GB/h for BaseRollup (W7) and is steady state (W22); 98.7% of eligible scans denied the skip. Build W23 lever (a) or (b) from a daytime denial sample | `maintain.rs` certification, read-side cert check | W23 | Claude (timefusion-7c) | ⬜ |
| W36 | MemBuffer-leg SortExec memory blowup | analysis → build | Read path | With more in-memory buckets than target partitions, DataFusion adds a full SortExec over the mem leg feeding the ordered MoR merge; ExternalSorter peaked ~7.6 GB for 44 MB locally. Fix: order-preserving mem leg (time buckets are disjoint) and/or compaction before the sort; cost test | `write/mem_buffer.rs`, mem-leg scan, `read/optimizers.rs` | ws/spm-query-memory (e85e7054) | Claude (timefusion-7c) | 🟡 ws/memleg-sort-memory |

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

### W21 result — 2026-09-27 — Claude (timefusion-2e)
**Finding:** carrying at the flush alone yields nothing. A hashes-only version append already deletes the
overlapping slice coverage at write time (`insert_records_batch_bounded` → `invalidate_rollup_batches` →
`apply_rollup_hours`). So the change also skips that invalidation for appends that no spec reads.

`appended − retracted` arithmetic is a false-hit hole: a relevant v2 retracting an irrelevant buffered v1
leaves a +1 carry. The design therefore uses per-batch stamps instead.
**Design** (`ws/w21-witness-carry` @ `48bd61b6`, dark behind `timefusion_rollup_witness_carry`):
- Relevance is the union of every spec's source columns, paused specs included.
- An irrelevant, non-tombstone batch that retracted nothing is admitted to an in-memory ledger by version stamp.
- A relevant or tombstone append refuses the carry for every earlier read of that table (guard a).
- At the staged flush commit, the rows written with ledger stamps are added to that date's slice witnesses.
  The bounded witness moves only when every written file lies below its bound.
- Shadow counters: `rollup_carry_eligible_rows_total` and `rollup_carry_applied_rows_total`.

**Tests:** a 6-case table, red before the implementation. The guard, relevance and flag mutations each go red.
Two cases are structural; the test says so.
**Blocks enablement:**
1. Coverage records no source version, so a slice built after a carried commit is carried twice.
   `carry_dedup_witness` has the same gap.
2. The ledger is in-memory only, so a restart forfeits pending carries.
3. Reconcile still re-mints the slice from the untagged Add.

**Next:** deploy it dark and read `rollup_carry_eligible_rows_total` against `mor_version_rows_appended_total`.
Enabling needs gap 1 closed and an owner decision.

### W12 result — 2026-09-27 — Claude (timefusion-2e)
**Question:** should `dashboard_1m_v3` gain a `name` HLL so monoscope's `dcount(name)` panels route? monoscope renders
`dcount(x)` as `distinct_count(approx_count_distinct(x))`. Three panels use it:

| Panel | Filter | What routing it needs |
| --- | --- | --- |
| Overview "Unique Endpoints" | `(kind == server or name == apitoolkit-http-span) and name != null` | A **filtered** name HLL. The scope is two-way, and `name` is not a dimension. This is the filter-variant kind the tier deliberately does not declare. |
| Service tab "Endpoints" | `service == X and kind == server and name != null` | An **unfiltered** name HLL (service and kind are dimensions), plus matcher acceptance of `name IS NOT NULL` on the HLL's own column. |
| Endpoint Analytics "Distinct Operations" | `hashes[*] == X` | **Never routable**: `hashes` is not a dimension. |

**Cost:** storage is negligible.
- The tier has about 15.5k rows/day for the largest project (09-26).
- Its busiest measured hour has about 1.2 distinct names per tier row (max 10), so a sparse HLL is tens of bytes per row,
  about 0.3 MB/day for that project.
- The existing `service_name_hll` is 9 B/row.

Build CPU is one sketch update per row, small next to the scan. Adding a measure does not orphan history: older slices
decline only for queries that need it (`MeasureNotStored`) until they are rebuilt.
**Benefit: unmeasured.** In 60 min of a mature process (00:00–01:00 UTC, night), there were zero `approx_count_distinct(name)`
statements over 1 s and zero sampled misses for that shape. A 24 h search would be a broad scan of prod TF, so it was not run.
**Recommendation (owner decides):** don't add it yet.
- Take one daytime window of `rollup_miss_sampled` or `pgwire.slow_statement` for `approx_count_distinct(name)`.
- If the service-tab panel misses at a material rate, add the **unfiltered** `name_hll` only. It is cheap, needs no new
  filter variant, and needs a matcher check for the `IS NOT NULL` guard.
- Keep declining the two-way-scope Overview panel unless its own rate justifies a filter variant.

### W22 result — 2026-09-27 — Claude (timefusion-2e)
**Question:** W7 measured dedup scanning 899 GB/h against 75 GB/h for BaseRollup. Is that a backlog draining
or steady-state work?
**Numbers:**
- Same 60 min window, process `cc6bfa40`. Dedup completions by partition age: yesterday to 4 d old held about
  12.2k lease-s (2 d: 14 units at ~355 s each; 3 d: 123 units, 4.9k s). A tail of 24–26 d-old partitions held
  110 units and ~1.5k s. Today had none.
- Retries were 6,531 × `admission_busy`, mostly costing seconds each. The exception was yesterday's partitions,
  where retries held 4.6k s.
- The skip-proof almost never fires: `dedup_skipped_pct` is 1.3%, and 60.8% of denials are never-certified
  partitions. `dedup_denied_fp_moved` is 3,418.
- The exported `timefusion.maintenance.pending_dedup` over 7 days (max per 6 h, monoscope metrics) oscillates
  between ~270 and ~1,400. Each daily spike returns to ~300 within 6–12 h, and there is **no downward trend**.

**Conclusion:** mostly **steady state**. Each newly sealed day is re-deduplicated for several days, plus
restart re-inflation (65 starts in 7.8 d per W16). It is not a finite backlog that will drain on its own; only
the 24–26 d sweep looks like backfill. The Stage 5 lever is fewer repeat dedups of the same sealed day. That
means certification that survives fingerprint moves, since 98.7% of eligible slices are denied the skip, rather
than faster dedup.
**Not measured:** bytes per dedup unit (`maintenance_scan_pruning` carries no unit key), so the
per-age split is by lease time, not by bytes.

### W23 step 1 result — 2026-09-27 — Claude (timefusion-2e)
**Mechanism** (code map, `tf-w11` @ master):
- The certification is `(project, table, date)` → `fp` (hash of the sorted full URIs, paths only) plus `files`
  (`(path, DV)` visibility map). Slice coverage keeps proved intervals.
- **No certification carry exists anywhere.** Every commit that changes the file set moves `fp`, including
  compaction, which is row-preserving. The read path survives only when every added `(path, DV)` misses the
  query window. A removal is never waved through. The only same-path rule is the dedup pass's own masked arm.
- The best hook for a carry would be `commit_wave`'s landed paths (beside `reindex_wave_outputs`), which
  already verify each input's exact `(path, DV)` under the lock.

**Numbers:**
- The hash-update version rows do not reach sealed days. `dirty_bin_enqueued`: 368 today, 2 yesterday, 0 on
  days 2–4.
- Compaction on sealed days is 6 SealedConsolidation units, on yesterday only. HotPacking (525) is all today.
- Dedup wave commits: 47 in-place DV masks (bytes_in == bytes_out) plus 2 CoW drops.

So on days 2–4, dedup's own progressive cleaning is essentially the only mover.
**Conclusion:** carrying a certification across dedup-preserving rewrites (compaction) has **little to recover
today**: the carryable class hardly touches sealed days. The 43% `never_certified` and the fp-moved denials come
from days still being cleaned slice by slice in their first ~4 days. Each DV mask changes a file's visibility,
which un-matches the whole day's `files` for any window overlapping that file.
**Before building:** the fp-moved denials cannot be attributed to a date or mover from counters. Proposed next
step (cheap, dark-safe): a sampled `dedup_skip_denied` log per `(date age, reason, mover: added | dv_changed |
removed)`. Then decide between two levers: (a) narrowing the read check so a DV mask that only removed *losers*
does not un-match windows it cannot affect, or (b) certifying days faster in their first 4 days. The compaction
carry stays unbuilt unless sealed-day compaction volume grows.

### W8 result — 2026-09-27 — Claude (timefusion-2e)
**Built (shadow only, per W17):** `rollup_tiers_reading(source, columns)` gives a per-tier verdict from W21's
`spec_source_columns`; a derived tier also reads what its base reads. After each merge-on-read statement,
its version rows are counted in OTel `timefusion.rollup.version_append_rows{tier, relevant}`. A tombstone is
relevant to every tier. Nothing acts on the verdict.
**Tests:** `rollup_relevance_tests` pins the real schema:
- `hashes` touches no tier.
- `attributes___user___id` touches only `sessions_1h_v1`.
- A measure-filter column touches both dashboard tiers.
- `timestamp` touches every tier.

Breaking the classifier turns 3 of the 4 red. The base-inheritance rule cannot be isolated on this schema,
because `dashboard_1h_v2` restates every base column; the test says so.
**Read it as:** after deploy, the `relevant=false` share per tier is the volume that per-tier invalidation could
keep readable, beyond W21's all-tiers rule (session enrichment on the dashboard tiers, for example).

### W24 result — 2026-09-27 — Claude (timefusion-2e)
**Finding:** this is not a matcher bug. `an_unservable_group_expression_is_counted_rather_than_silent`
(`rollup.rs` ~3059) asserts this exact shape declines. When `status_code` is NULL, the group key falls back to
`level`, and no tier declares `level`. Only a variant whose predicate makes the fallback unreachable
(`status_code IS NOT NULL`) routes. The `now()` placeholder, bucket spelling and pgwire path play no part.
**Cost of routing it:** in 12:00–13:00 on 09-26, 505,652 raw rows (44% with null `status_code`) formed 774 tier
groups whether or not `level` was a dimension. So `level` adds no tier rows. The cost is the spec change itself:
a new tier version (`dashboard_1m_v4` / `1h_v3`) plus a re-backfill.
**Options (owner):**
1. Add `level` in a v4, batched with any other dimension change.
2. monoscope splits the chart: the `status_code IS NOT NULL` part routes, and the remainder stays raw.
3. Leave it.

### W25 result — 2026-09-27 — Claude (timefusion-2e)
**Measured.** Rollout lines from 13 deploy runs (09-26 15:40 → 09-27 01:55): unready was 9.5–15.3 s, apart from
23.5 s (20:46) and 32.1 s (21:31). The `HANDOFF` row count did not track it (407 k → 23.5 s, 339 k → 12.3 s,
321 k → 14.3 s). **The interval does not grow with buffered data.** The write fence drains before SIGTERM (shutdown
logs "flush skipped: already drained"), and reads stay available until SIGTERM.
**Decomposition of the 01:55 deploy (14.3 s),** from container timestamps and both processes' logs. The swarm
order is `stop-first` (`start-first` deadlocks on the WAL lock, 08-10), so these steps run in series:

| Step | Duration |
| --- | --- |
| SIGTERM → PGWire stops accepting → TimeFusion "Shutdown complete" | 1.0 s |
| "Shutdown complete" → container exit (`async_main` drops db, buffers and caches before `process::exit`) | **2.4 s** |
| Old exit → new task created (every deploy's first new-image task fails with `No such container`: CapRover updates the service twice) | 1.6 s |
| New container created → started, image already pulled | **4.9 s** |
| Process start → early-bind 57P03 → real PGWire | 2.0 s |
| Probe slop (1 s timeouts) and first answers | ~2.4 s |

**21:31 (32.1 s), from TimeFusion's own logs in monoscope (2.5 min window).** SIGTERM 21:31:24.8, shutdown done
in 1 s, new process started 21:31:44.6, ready 21:31:46.2. So "old shutdown → new start" took **19.8 s against
10.4 s**, and the new process first answered the probe about **10 s after** "startup complete" (inferred from the
interval's end, 2.3 s normally). TimeFusion's own shutdown and boot did not grow; the container lifecycle and the
first seconds after start did. A burst of slow statements completes from 21:32:01, consistent with reconnecting
clients swamping the new process.
**Proposals (need sign-off, not built):**
1. `process::exit(0)` right after "Shutdown complete", skipping the heap teardown. That saves ~2.4 s, and more on a
   larger heap. Caveat: `TaskLease::drop` checkpoints an unstarted-unit release, so the exit must come after
   `db.shutdown_by` has released the leases.
2. Find the 4.9 s create → start. It happens with the image cached, so the cause is container setup (mounts or
   network).
3. Stop CapRover's double service update: deploy with a single `docker service update --image`, or accept the
   1.6 s it costs.
4. Keep the 30 s budget, but have the rollout log old-exit / new-start / first-answer timestamps, so the next miss
   is attributable without a log search.

**W25 follow-up — 2026-09-27.**
- **(4) Built:** `ws/w25-rollout-timing` @ `8d3726a3`. The rollout measurement line now adds `handoff phases`:
  last-old-answer → new-boot (container lifecycle) and new-boot → first-answer (boot plus first answer). Both come
  from the replacement's `boot_micros`, which the probe already reads. Deploy-script tests pass 31/31. A dry run on
  the 01:55 numbers gives 11.7 s / 2.6 s.
- **(1) Early exit: skipped.** The integrator's bar is a demonstrated clean reopen of the Foyer disk cache after
  skipping destructors. A local run cannot reproduce what fills prod's 2.4 s (6 GB of in-memory entries whose
  explicit close is abandoned at its budget). The 2.4 s is also not what broke either outlier: at 21:31 the
  container lifecycle doubled and the first answer came ~10 s late.
- **Next if (4) shows the lifecycle phase dominating:** (2), the 4.9 s container create → start.

### W10 calibration — 2026-09-27 — Claude (timefusion-2e)
**Question:** does `timefusion sim` at 1x reproduce prod's executions per hour? The sheet required this before any
2x/4x multiplier is trusted.
**Method:** the prod journal (`maintenance_tasks.json` 82 MB plus `.wal` 56 MB, copied read-only with `docker cp`,
state 02:23 UTC plus WAL to 04:21) replayed with `timefusion sim <dir> --hours 1 --now 02:21Z` at master
`e9ce6dc3`+, with 44 and 66 workers (prod runs `job_workers=66`, `runtime_workers=44`). Prod baseline: W7's mature
60 min window.

| per hour | sim 1x (66 workers) | prod |
| --- | --- | --- |
| BaseRollup completions | 152 | 1,342 |
| Dedup completions | 156 | 444 |
| DerivedRollup completions | 7 | 572 |
| HotPacking / SealedConsolidation | not simulated | 229 / 31 |
| pending over the hour | 8,097 → 248 (drains) | steady (stock does not drain) |

**Findings:**
- The 44 and 66 worker runs give identical results, so the sim is not worker-bound. It **runs out of work**: minting
  from the journal's 21 ingesting streams replenishes far below prod's continuous re-mint of rollup hours on ingest
  invalidation, which drives roughly 9x more BaseRollup and 80x more DerivedRollup work.
- Compaction lanes are absent.

**Conclusion:** the 1x baseline is uncalibrated, so no capacity multiplier from the sim is trustworthy yet.
**Next:** model per-stream re-mint from invalidations at prod's measured rate. W19's `lease_ms`/`publications`
counters give the target. Add the compaction lanes, then re-check 1x against a fresh mature-window baseline.


### W21 / W23 measurement — 2026-09-27 — integrator
**Window:** image `3ecb066b` (W21 + W23 instrumentation), ~75 min uptime.
**W21 shadow:** `rollup_carry_eligible_rows_total` 6,585 of `mor_version_rows_appended_total` 920,323 (**0.7%**).
Most hashes UPDATEs hit rows still in the MemBuffer (retracted, never carryable by the false-hit rule), so enabling the
carry would barely move today-window hits. **Recommendation: leave it off.** Today-window staleness is structural —
the physical witness moves with every version append; the real fix is Stage 2 (captured source view / logical witness),
not a carry.
**W23 denials (sampled):** ~160 `never_certified` at date_age 0 (today, expected while ingesting), 1 `fp_moved` at age 1.
Sealed-day denials were rare this hour — neither (a) narrowing nor (b) faster certification has a case yet; re-sample
during a daytime backlog before building either.
**Also:** `heavy_query_queue_timeout` 0 over the hour (admission fix holds); `flush_failed` 0.

**W10 calibration, built — 2026-09-27 — Claude (timefusion-2e).** Branch `ws/w10-calibrate`, one commit, touching only
`maintenance_sim.rs` and the sim CLI.
- `timefusion sim <journal> --calibrated` adds per-stream re-mint at rates **fitted** to prod's completions (not raw
  mint counts; they net out frontier mints and absorption on pending slices). It also adds the missing HotPacking and
  SealedConsolidation lanes.
- Measured mean unit seconds (Base 36, Derived 8, Dedup 31, Hot 4, Sealed 38) replace the pre-W13/W14 duration table
  and are scaled by the rows axis.
- Derived re-mint spans a week, as prod's does. Otherwise each stream would have only 24 hourly keys.
- Without `--calibrated`, existing runs and tests are unchanged.

Prod journal, 3 h at 66 workers (`--now` 02:21Z):

| per hour | sim | prod | ratio |
| --- | --- | --- | --- |
| BaseRollup | 1,279 | 1,342 | 0.95 |
| Dedup | 448 | 444 | 1.01 |
| DerivedRollup | 524 | 572 | 0.92 |
| HotPacking | 209 | 229 | 0.91 |
| SealedConsolidation | 28 | 31 | 0.90 |
| pending | 326 → 1,038 → ~1,076–1,110 plateau | steady | — |

**Test:** `a_calibrated_replay_does_prods_work_at_a_steady_stock` uses a synthetic journal: 3 of 21 streams, 10 workers,
targets scaled by 3/21, and a week of built base slices. It asserts every operation is within 20% and pending drift is
≤10%/h of its level, and runs in ~6 s. `cargo lint` is clean and the sim tests pass 33/33.
**Use:** 2x/4x deltas via `--rows` / `--projects` / `--streams` on top of `--calibrated`. Refit the rates when prod's
operation mix moves.

### W26 result — 2026-09-27 — Claude (timefusion-2e)
**Method:** `timefusion sim <prod journal> --calibrated --hours 6 --workers 66 --now 02:21Z` over a matrix of
`--rows` 1/2/4 × `--projects` 1/2/4. Built on `ws/w26-sim-base-tier` (`74de5bee`), which makes the sim publish
`base_tier_ready` as prod's planner does. Before that fix, 98% of sim CPU was in `dependencies_complete`.
These are **scheduler** numbers: unit costs are W10's calibrated means, not server resources. Drain is an estimate.

| cell | pending end | slope | util | worker-s/row | pending BaseRollup | drain |
| --- | --- | --- | --- | --- | --- | --- |
| rows 1x, projects 1x | 658 | +64/h | 29% | 0.074 | 348 | ~0.25 h |

Per hour at 1x: Base 1,358, Dedup 465, Derived 542, Hot 220, Sealed 28, matching W10's calibration.

**Only the 1x cell finished.** It ran for about 9 min. The `projects 2x` and `4x` cells each passed 2 h of CPU without
finishing (more than 13x), so I stopped the matrix. A 5 s sample of the 4x cell:

- About 95% of samples are in `TaskJournal::dependencies_complete`, reached from `claim_candidates`.
- Within that: SipHash of `TaskKey`, the scan of active BaseRollup candidates, and the fallback walk over **every**
  task (`snapshot.tasks`, 127,242 in the prod journal, 53,331 of them complete BaseRollup) for each DerivedRollup
  candidate not covered by `cached_base_tier_proven`.
- Each claim therefore costs O(derived candidates × journal size). Both factors scale with projects, and so does
  the number of claims.

**Findings:**
1. **At 1x the scheduler is stable, with headroom.** Utilization is 29% of 66 workers and pending grows slowly
   (+64/h). The drain estimate is under an hour.
2. **The first thing to saturate as projects grow is the coordinator's claim path, not a worker lane.** Claim cost
   is superlinear in projects. No per-lane threshold for 2x/4x can be read until this is fixed, because the sim
   spends its time claiming.
3. **Prod exposure is real but unmeasured.** Prod runs the same `claim_next`. It short-circuits whenever
   `cached_base_tier_proven` hits, and the planner refreshes that cache every pass. The fallback is also a full
   journal walk per derived candidate, so prod's cost grows with journal size (127k tasks) × derived pending.
   There is no claim-latency metric to check this.

**Next (proposed, not built):**
- (a) Export claim-pass duration from `claim_next`, so prod's exposure is measured rather than inferred.
- (b) Index complete BaseRollup slices by (source, project, base table), so the fallback reads only its own
  slices instead of the journal.
- Then rerun the matrix. Rows × projects 2x/4x cells are then minutes each.
- Artifacts: `scratchpad/matrix/{run.sh,report.py,r1_p1.json}`, `r1p4.sample`.

### Staging seed — 2026-09-27 — Claude (timefusion-7c)
`s3://timefusion-eu/timefusion-staging/otel_logs_and_spans` is a version-0 Delta table holding prod's active files for
2026-09-25 and 2026-09-26 at prod version 686219: 35 data files and 18 DV files, 8.13 GB. The add actions are
byte-identical to prod's (tags, stats, DVs); the table id is new. Rollup tables and sidecars start empty, so a staging
process rebuilds them, which is the W32 step-2 workload.
Reproduce (server-side copies; every write is asserted to sit under `timefusion-staging/`; it refuses a non-empty
staging `_delta_log`):
```
set -a; source .env.prod; set +a
export AWS_REGION=de AWS_REQUEST_CHECKSUM_CALCULATION=when_required AWS_RESPONSE_CHECKSUM_VALIDATION=when_required
python3 bench/staging_seed.py plan   # replay only
python3 bench/staging_seed.py apply  # copy + commit
```
Edit `DATES` in the script to seed other days.

### W26 part 2 — 2026-09-27 — Claude (timefusion-2e)
Rerun with the claim index (`ws/w26-claim-index` `d3b6b579`). A cell now takes 3–10 min, where the projects ×2 and ×4
cells previously did not finish. r1p1 reproduces part 1 exactly, so the index does not change behaviour.
Same method: prod journal, `--calibrated`, 6 h, 66 workers. These are **scheduler** numbers, not server CPU, memory or IO.
Unit cost is W10's calibrated mean unit seconds, scaled by rows. Drain is pending ÷ completions/h.

| rows × projects | busy | pending end | slope/h | pending Base / Dedup / Derived | drain |
| --- | --- | --- | --- | --- | --- |
| 1 × 1 | 29% | 658 | +64 | 348 / 82 / 228 | 0.25 h |
| 1 × 2 | 59% | 1,409 | −56 | 723 / 215 / 471 | 0.29 h |
| 2 × 1 | 63% | 784 | −25 | 462 / 82 / 240 | 0.28 h |
| 1 × 4 | 93% | 6,054 | +299 | 2,123 / 2,909 / 1,022 | 0.78 h |
| 2 × 2 | 94% | 3,219 | +123 | 1,138 / 1,566 / 515 | 0.79 h |
| 4 × 1 | 95% | 1,258 | +96 | 857 / 105 / 296 | 0.59 h |
| 2 × 4 | 94% | 13,848 | +1,731 | 5,653 / 5,398 / 1,361 (+1,436 Hot) | 3.2 h |
| 4 × 2 | 95% | 7,423 | +1,080 | 3,242 / 2,657 / 768 (+756 Hot) | 3.2 h |
| 4 × 4 | 95% | 21,610 | +1,504 | 9,983 / 6,836 / 2,002 (+2,789 Hot) | 9.6 h |

**Findings:**
1. **Stability threshold: about 2x today's load.** Every 2x cell (rows or projects) is stable at about 60% busy with
   a flat or falling queue. Every 4x cell pins the 66 workers at 93–95% and the queue grows. 8x runs away
   (+1–1.7k/h, drain more than 3 h). So the maintenance scheduler has about 2x headroom, not 4x.
2. **The first saturating lane depends on the axis.**
   - Growing **projects** saturates **Dedup** first: 1 × 4 has 2,909 pending Dedup, more than Base.
   - Growing **rows** saturates **BaseRollup**: 4 × 1 has 857 pending Base against 105 Dedup, because unit seconds
     scale with rows.
   - HotPacking backs up only at 8x.
3. **DerivedRollup does not scale with load (the dependency scaling risk).** Derived completions go 542 → 630 → 820/h
   at projects 1/2/4, while BaseRollup nearly doubles per step. Derived pending grows 228 → 1,022. Derived units wait on
   pending base slices (`dependencies_complete`), so derived freshness degrades with pending BaseRollup. Pending base per
   cell goes 348 → 2,123 (1 × 4) → 5,653 (2 × 4). This is the risk to watch on 1h/1d tiers as tenants grow. The claim
   index removed the CPU cost of that check, not the waiting.
4. **Levers, in order:**
   - Dedup volume per project-day (W33).
   - Base unit cost per row (Stage 1B/1C, W7's `run-unit` on staging).
   - More workers only after those two, because at 95% busy the queue grows by work count, not by claim cost.

**Caveats:** mean-cost units, with no stragglers or memory or IO contention. The rows axis scales unit seconds
linearly. Numbers are relative to the 02:21Z journal's load.

