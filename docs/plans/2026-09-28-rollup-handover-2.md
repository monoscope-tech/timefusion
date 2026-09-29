# Rollup plan: handover 2 (2026-09-28 ~12:00 UTC)

Continues [2026-09-25-rollup-handover.md](2026-09-25-rollup-handover.md). Goal document: [2026-09-24-rollups-on-a-fixed-server.md](2026-09-24-rollups-on-a-fixed-server.md).
Per-workstream detail: [2026-09-26-rollup-parallel-workstreams.md](2026-09-26-rollup-parallel-workstreams.md). Read this file first; it supersedes the others where they disagree.

## 1. How to judge progress

The plan is judged by its gates, not by prod stability: less total work per accepted event, fewer re-mints, more rollup hits, falling backlog.
Report these before and after every deploy (script: `scorecard.sh`, see section 7):

- `maintenance.pending_base_rollup`, `pending_derived_rollup`, `pending_dedup`, `tasks_pending`, `oldest_task_age_seconds`.
- `scan.cert_skip_files`, `scan.dedup_partial_skipped`, `scan.dedup_denied_slice_only`.
- `pgwire.lat_p95/p99/p999_us_approx`. These now include streaming time (W47 2a).
- The 18-query probe (section 7): route outcome and wall time per shape and project.

Do not use `rollup_dirty_partitions`. It is only ever added to, never cleared or read, so it is not a backlog signal.

## 2. Prod state at handover

- Image `59af0721`, master `a8792783`. Memory 10 GiB of 120. Buffer pressure 4%, 0 insert rejections.
- Pending: 359 tasks (base 254, derived 40, dedup 114).
- pgwire latency: p95 0.40 s, p99 1.06 s.
- v4 tier policy: `RESUME FROM 2026-09-21` (newest week only). Next step: widen by one week per day (09-14, 09-07, 08-31, then the full 31 days), checking memory and `admission_refused_state_bytes_total` between steps.
- `sessions_1h_v1` and `sessions_1h_v2` are PAUSED. Do not resume v2: W40 showed that a non-derived hourly tier built from 10-minute slices undercounts (routing is now refused). The real fix is still open (section 5).

Measured wins since 2026-09-27 20:00 (probe projects 28f62f01, 87576849, d062e010):

| Measure | Before | Now |
|---|---|---|
| 6h dashboard window | 10–33 s | 0.6–1.6 s |
| 24h status breakdown | 14–38 s, raw | 1.6–8.2 s, rollup (W29 v4 `level`) |
| Sealed days | | 0.3–0.5 s, full rollup hits |
| Derived backlog | 435 deadlocked | 20–50, draining |
| Journal lock wait | ~75 s/min | ~7 s/min |
| Unproven:proved certification files | 23:1 | 2:1 |
| Maintenance bytes per million rows | | about 127x lower (young-process sample; matched-hours check not done) |

## 3. What shipped (2026-09-27 20:50 → 2026-09-28 ~11:40)

All shipped with CI green on the exact commit. Names match the workstream sheet.

- **Stage 0 loops:**
  - fix A (overlapping ranges);
  - fix B (never split a unit through a live slice);
  - W38 (census re-admit guard; edge-day proofs kept until midnight; prune cutoff shared with the census);
  - W42 (never fuse unended cells into whole-day units; a boot migration retires existing ones);
  - W41 (a write re-arms only the cells it can change);
  - orphan-derived prune.
- **Maintenance throughput:**
  - claim fast path (idle-claim memo);
  - `edit_tasks` → `mark_dirty` (reopened derived cells are claimable);
  - tier backfill cap (default 16);
  - output-state pricing (~48 KB per output row);
  - state-driven time split;
  - coarsening state cap;
  - v4 sibling prior;
  - state waiter with frontier reserve (`06727291`).
- **Certification and dedup:**
  - W37 (per-project spans for the per-file skip);
  - W33 (skip dedup on certified-clean days);
  - W39 (mint Dedup units for probe-declined bins; stop probing tier tables; counter honesty);
  - dedup-mint horizon gate;
  - task age measured from the last reopen;
  - W46 key-level dedup restriction, DARK (`0f131e24`; find its flag in the commit).
- **Read path:**
  - W31 (content-fingerprint slice proof);
  - W43 (Parquet predicate withheld only from DV-masked files; fork `79e98104`);
  - W44 (DV scans keep one stream per file group; fork `2b9c7e21`);
  - `cd27a17d` (per-file dedup split no longer double-counts under the tantivy prefilter);
  - W47 2a (`a8792783`: day-long dashboard scans are cached on first view; latency recorded at stream end).
- **Ingest incident fix:** `d7d44054`, `3bfa405e`, `4ee017dd`. A bucket whose snapshot flush finishes dirty twice in a row is now taken through the destructive path. Before this, continuous DML kept buckets from draining, and inserts were rejected with "Memory limit exceeded" (about 830 in 20 minutes, 08:30–08:50). New stat: `buffered_layer.dirty_livelock_takes_total`.
- **Tiers:** W29 `dashboard_1m_v4` / `dashboard_1h_v3` with `level`, and the `run-unit --tier` option.
- **Dark:** query-latency yield `TIMEFUSION_MAINTENANCE_QUERY_YIELD`. Enable it only as an A/B at matched hours (design: `2026-09-28-query-latency-yield.md`).
- **Fork:** the delta-rs integration branch `timefusion-upgrade-55-dv` was fast-forwarded to `2b9c7e21`. Any future fork fix must be merged or fast-forwarded there, or the next pin bump drops it.

## 4. Known issues to check first

1. **1h window routes `not_built` for 87576849 and d062e010 since W29.** It is not a counting error: no hit counter moves. 7c's agent was on it on `ws/v4-fallback`. Candidates: route generation excludes v3 for this shape; v4 returns a zero-coverage Ok; v4's recent cells are not built yet. Check whether it landed; if not, reproduce with the probe's `1h_by_1m` shape.
2. **W47 2b, boot body preload** (`ws/w47-boot-body-preload`, 2e). Today and yesterday for the top 10 projects; caps 8 GiB total, 2 GiB per project, 5 min. After a restart, the first dashboard load took 15 s cold and 1–3.5 s warm. Ship it once CI is green.
3. **v4 backfill memory:** v4 units peak at about 41–43 GB each. They run alone under the state clamp, and the waiter plus frontier reserve should stop starvation. Watch `admission_refused_state_bytes_total` against BaseRollup completions, and the new `admission_busy:<dim>` retry reasons.
4. **be87ebc1 is slow even when warm** (7.6 s; 96% of rows in one partition): W48, not started.
5. **Monoscope pattern tagging** (`update2Sql`, `BackgroundJobs.hs` ~2944) causes the late MoR version files that block certification (88% of blockers). The source fix is the owner's decision; they said earlier not to change monoscope. W46 is the TimeFusion-side fix. Evaluate it dark before enabling.

## 5. Plan stages still open

- **Measured by the residual ranking** (`docs/plans/2026-09-28-residual-cost-ranking.md`, if it was committed): Stage 1B single-pass is not material (all units single-shard). Stages 3, 5 and 1C are small after the fixes. Re-rank once the v4 backfill finishes.
- **The plan's gate:** a matched-hours comparison (12:00–18:00 UTC against 2026-09-27 12:00–18:00) of CPU-seconds and processed bytes per million ingested rows, on a process at least 2 h old, with no deploy inside the window. Not done yet; it is the plan's acceptance evidence.
- **Drop v3 / 1h_v2:** `ws/drop-v3` (7c, CI only). Ship it after v4's per-tier `rollup_coverage_contiguity` matches v3's (usable_cells and contiguous_days) for 3 consecutive censuses on a process at least 1 h old, and after all 31 days are resumed.
- **Sessions:** the proper W40 fix (build non-derived tiers at their grain, or re-aggregate partials on read). Only then can sessions v2 (W27) resume.

## 6. Rules that were paid for

- **Deploys are batched.** Every push to master restarts prod, and each restart costs users a cold dashboard for 10–20 minutes. Docs-only pushes do not deploy.
- **CI green on the exact commit** before pushing to master. Use the gated push script (section 7): it refuses when master moved with code, and after a rebase it requires the code to match what CI tested. An "emergency" push on targeted tests once shipped a DV-delete correctness hole.
- **Prod is strictly read-only:** logs, `inspect`, `ps`, `docker stats`, pgwire SELECTs. Allowed writes: `ROLLUP PAUSE`/`RESUME`. Never `docker exec`.
- **Every bug fix needs a test that fails first,** shown red with the fix reverted. For read-path pushdown, test through pgwire; the local `ctx.sql` path re-runs optimizer passes that prod does not.
- **Use your own worktree and `CARGO_TARGET_DIR` under `~/Projects/apitoolkit/`** (`/tmp` is wiped on reboot). Never delete a target dir you did not create; check `pgrep -f <dir>/target` first. Keep at least 30 GB free.
- **Measure before building:** find the saturated resource (`docker stats` during a query vs idle), and never measure a process younger than about 10 minutes.

## 7. Tools and locations

- Prod pgwire: `grep TIMEFUSION_PG_URL ../monoscope/.env`. Host: `ssh ubuntu@captain.s.past3.tech`, service `srv-captain--timefusion`.
- Probe script (18 shapes × 3 projects, with rollup counter deltas): `~/Projects/apitoolkit/tf-helper-scripts/probe.sh`.
- Scorecard and gated push: `~/Projects/apitoolkit/tf-helper-scripts/{scorecard.sh,gated_push.sh,gated_push_b.sh}`; scorecard history is in `scorecard.log`. Usage: `gated_push.sh <branch> <worktree> "YYYY-MM-DD HH:MM"`; the `_b` variant reads CI from `$CIB`.
- Collaborating sessions:
  - timefusion-2e: certification, read path, W47, W46.
  - timefusion-7c: pricing, waiter, W29, drop-v3, v4 fallback.
  - Reach them with SendMessage; `ListAgents` shows their addresses.
- Branches not yet on master (check each against master first):
  - `ws/w47-boot-body-preload`
  - `ws/v4-fallback`
  - `ws/drop-v3`
  - `ws/sessions` work (none)
  - `ws/w28-service-hll` (a duplicate of `e0e27dbe`; delete it)

## 8. Update 2026-09-28 ~21:30 UTC (timefusion-21)

**Plan gate, measured (indicative, not an accepted pass).** The 09-27 12:00–18:00 baseline has 3 deploys inside it, and the maintenance counters (added 09-26 22:28) have no clean ≥2 h window before 09-28. "After" window: 09-28 14:00–18:00 on image `59af0721` (started 11:38, no deploy). Query volume matched (~52.7k/h).

| Per million ingested rows | Before | After | Change | Baseline |
|---|---|---|---|---|
| Whole-container CPU-seconds | 21,723 (09-25 14–18, image `2f2df90c`) | 4,137 | 5.3x lower (32.8 → 5.8 cores) | clean |
| Maintenance processed bytes | 133.5 GB (09-27) | 9.9 GB | ~13x lower (11x without Dedup) | restarts inside |
| Maintenance lease-seconds | 16,189 (09-27) | 963 | ~17x lower | restarts inside |

- The "about 127x" in section 2 came from a young process; the matched-hours figure is ~11–13x.
- The backlog did not fall in the window (pending base 350 → 386). Part of the CPU drop is deferred scope: the v4 week cap, the per-tier backfill cap (16), and the paused sessions tiers. So "less work per accepted event" is not yet shown.
- `maintenance.processed_bytes{operation=Dedup}` reads 0 since ~08:00 while Dedup still holds lease time: a recording gap. Fix it before a bytes gate can cover Dedup.
- For a valid gate: keep 09-25 14:00–18:00 as the CPU baseline and re-run "after" on a day with no deploys, the process ≥2 h old by 14:00, and v4 widened.
- Raw data: `scratchpad/gate/` of session 66d75f9a.

**Rollup misses.** ~88% of `rollup_misses_total` are shapes no tier can serve by design: TF self-alerts on `otel_metrics`, `unnest(hashes)` jobs, `hashes`/`body`-regex filters. On dashboard shapes the hit rate is ~30–35%. Issue 4.1 no longer shows: a 1h window is served from the MemBuffer tail (`tiny_interior`), so `0e003824` only relabels. New bug: `rollup_rewrite_failed stage="schema"` for `max((value)::float)` on `otel_metrics` (Float32 vs Float64), which falls back to raw.

**Shipbubble (28f62f01) 24h dashboards, 3 alternating runs, all hybrid hits:** status breakdown 9.8 s cold, then 1.6 s and 1.4 s; by-service 1.8 s, 1.0 s, 1.0 s. The cold first query is still ~10 s on a 9 h old process, so the remaining cold cost is not the W47 2a bypass. Likely cause (not measured): today's files are rewritten and the new files are cold. W47 2b only warms at boot.

**In flight:** `batch/2b-v4fallback` (`22895ce9` = W47 2b `6e4e9a6e` + `0e003824`), CI run 36481759922. It pushes via `gated_push.sh` when green. v4 widening to 09-14 is due ≥ 2026-09-29 10:00 UTC. Status at 20:48: 7 contiguous days, 104/116 usable; memory 18/120 GiB; `admission_refused_state_bytes_total` 2,143 in 9 h.

## 9. Night of 2026-09-28/29 (timefusion-21) — shipbubble focus

**Shipbubble (28f62f01) now, fresh process (image `4d314718`, ~2 min after boot):**

| Query | Before tonight (fresh boot, 21:30) | After `1e79a30b` (00:57): cold → warm |
|---|---|---|
| 24h status breakdown | 15.2 s → 7.2 s → 1.2 s | **1.4 s → 0.3 s → 0.3 s** |
| 24h by service | 5.9 s → 1.3 s → 1.0 s | **0.3 s → 0.3 s → 0.4 s** |
| 6h window | 0.8 s → 0.6 s → 0.5 s | 0.9 s → 0.5 s → 0.5 s |

All routed hybrid; `foyer.insert_bypassed` did not move. Script: `scratchpad/ship/` of session 66d75f9a (psql loop with rollup-counter deltas).

**What was actually making shipbubble slow (measured; it was not "maintenance not keeping up" in the scheduling sense):**
1. **Every hybrid dashboard skipped the cache on first view (W51).** The raw leg of a hybrid rewrite filters `(range) OR (range)`; `extract_time_range_from_filters` only understood top-level comparisons, so the lookback was "unbounded" and `gate_if_wide` set `bypass_cache`. Only the second sighting was admitted. Fixed by bounding AND/OR recursively (hull for OR, sound for every caller).
2. **The boot preload fetched one file at a time** (~11 MB/s) and hit its 5-minute budget before reaching shipbubble (W52: 8-way concurrency, exact byte reservation, newest-day round-robin). New boot: 5.17 GB in ~1 min, no budget stop.
3. **Tantivy index cache thrash (W53):** the prefetch working set is ~300–350 GB against a 200 GB budget, so the reaper freed ~100–135 GB every 10 min and point lookups paid S3 index downloads at planning (12–16 s cold for trace lookups). Default raised to 400 GB.
4. **Files with deletion vectors are never rewritten (open, W54).** Nothing in prod strips DVs. Today's partition: Talstack 74.6% masked rows (queries decode ~4x live rows), shipbubble 17.5%, 87576849 41.7%; 56 stranded DV files (11.7 GiB) on sealed days. Point lookups into DV files read whole files (the W43 design).

**Shipped tonight:**
- 21:15 `310636d4`: W47 2b boot preload + v4-fallback relabel.
- 00:54 `1e79a30b`: W49 (rollup rewrite cast: `max(value::float)` Float32 vs Float64 no longer falls back to raw), W50 (Dedup `processed_bytes` records measured bytes, not the 0 creation estimate), W51, W52, W53, **W55 (wrong-answer fix: `count(<expr>)` was served from the stored row count — 24 vs raw 12 in the test; now declines unless the argument is `*`, a non-null literal, or a column with a stored non-null count)**.
- `AGENTS.md`: prefer local signoff (`make ci-signoff`) over remote CI.

**Local signoff timings (for planning):** a full `make ci-signoff` took 69–90 min tonight, but under heavy contention (3–5 builds/signoffs in parallel, load 70–157). Test ≈ 25 min, e2e ≈ 4 min, pg-smoke dominated by its release image build. Run one signoff at a time. `make ci-signoff` also pushes the production image, so the deploy that follows reuses it (~1 min to Ready).

**Other findings (not shipbubble):**
- **Issue/pattern charts time out (Talstack 6297304f):** `jsonb_path_exists(to_jsonb(hashes), …)` charts hit the 90 s timeout ~150 times in 10 h (one auto-refreshing tab). Cause: today's partition is 133.6M rows in 574 time-overlapping files with 74.6% DV-masked rows, so nothing prunes. Memory correction: `readmit_mutable_filters` does not run on prod's buffered path, so the `hashes` predicate is never pushed down. Levers: W54 (strip DVs), monoscope `array_has` lowering (`monoscope/plans/array-has-lowering.md`, ~40x cheaper predicate), and monoscope backing off auto-refresh after a timeout (owner's call).
- **Issue page stalls up to 77 s:** monoscope `Issues.hs:488-497` looks up the session id by trace with `tryWithin Nothing` (no timeout); sibling lookups use 5 s. Mostly the Demo project. Owner's call.
- **Rollup hit rate** reads ~5%, but ~88% of misses are shapes no tier can serve (TF self-alerts, `unnest(hashes)` jobs, `hashes`/`body`-regex filters). Dashboard shapes hit ~30–35%.

**Open for the morning:**
1. **W54 DV-strip — built, signed off, NOT deployed** (`ws/w54-dv-strip` @ `af08d77e`; local `make ci-signoff` all 5 green, test 2451/2451). Flags: `timefusion_dv_strip_enabled` (default false), `_per_interval` 4, `_interval_secs` 600.
   - **Always on after deploy, even with the flag off:** `carry_rewrite_witness` on every landed compaction with a DV input (incl. fully-masked-file removal), and an exact-count guard that refuses a DV rewrite whose written rows ≠ inputs' physical rows − DV cardinality. Review these before deploying.
   - **DML question, answered:** both rollup sources are version-append tables; DELETE/UPDATE write tombstones/versions, never DVs (`dml.rs` ~1050/~1132), so every DV on a rollup source is a dedup mask the rollup never counted. Non-version-append sources invalidate hours before their DV lands.
   - When enabled, watch `dv_rewrites_landed_total`, `dv_rewrite_rows_retired_total`, `rollup_witness_carried_total`, and `dv_strip_plan_sorts_total` (must stay 0). Known limits: carries don't survive a restart; the planner's size estimate sums all DV files in a partition (Talstack: 465), so huge partitions only run when the lane is idle; fully-masked files drain ~1 per tick.
   - Enabling it is the owner's decision (burst ≈ 11.7 GiB sealed + ~5.4 GiB today).
2. **v4 widen to 09-14** (handover 2 §2) is due ≥ 10:00 UTC; check `admission_refused_state_bytes_total` first (2,143 in 9 h yesterday).
3. **Recheck `journal_lock_wait`** on a ≥1 h process (handover mature figure ~7 s/min; tonight's 9 h process implied ~14 s/min).
4. **W48 appears resolved** by W44/W51: be87ebc1 24h status now 6.1 s cold → 0.8 s → 0.7 s warm (was 7.6 s warm). drop-v3, the W40 proper fix and the gate on a clean day remain as in §5.

**1 h check on `4d314718` (01:54 UTC, process 58 min old):**
- Shipbubble: 24h status 1.0 s, 24h by service 0.4 s.
- Shipbubble trace session lookups (issue-page shape, ±300 s window):

  | Trace age | First run | Second run |
  |---|---|---|
  | 30 min | 0.29 s | 0.28 s |
  | 20 h | 0.27 s | 0.25 s |
  | 2 days | 11.1 s | 2.3 s |

  - Before tonight these took 12–45 s cold. The 2-day case still reads whole DV-bearing files: 09-26 and 09-27 are stranded DV days, which W54 would strip.
- **W53 works:** no tantivy reap has fired on the new container in 57 min (the reap only logs when it removes something), so the prefetch set (~286 GB) now fits the 400 GB budget. The old container freed ~100 GB every 10 min.
- **Latency:** p50 55 ms, p95 0.17 s (was 0.49 s on the 9 h pre-deploy process), p99 1.4 s.
- **Pending:** base 243, derived 39, dedup 108 (was 253 at boot).
- **Journal lock wait ≈ 18 s/min** on this process against the 9 h pre-deploy process's ≈ 14 s/min. It is not a regression from tonight's batch: `coordinator_claim` runs ~371 claims/s on both (≈ 381/s before), `journal_hold` is only ~7% duty (255 s in 58 min), and the wait total is workers queueing behind the claim path. Worth a look alongside the claim fast path (`ws/claim-poll-fastpath` in the earlier queue).

**Summary for the owner.** The data did not support "the scheduler is not keeping up": shipbubble's slowness came from cache-admission policy (W51), preload throughput (W52) and index-cache sizing (W53), now fixed. But it did find a **missing maintenance operation**: nothing ever strips deletion vectors, so masked rows accumulate (Talstack 74.6% of today's rows, shipbubble 17.5%). Shipbubble's remaining slow case is a lookup into a 2-day-old DV-bearing day: 11 s cold, 2.3 s warm. W54 fixes that class.

**Post-deploy checks at 1 h:**
- `rollup_misses_total` jumped to ~21/min on the new process. W55 is **not** the cause: `missing_measure` = 8. It is `unknown_filter` (855), the `hashes` issue charts (>1,000 such queries in the hour), plausibly counted more on a young process whose plan cache is cold. Recheck the rate on a mature process.
- Shipbubble log explorer: 1h listing 0.29 s cold / 0.31 s warm; 24h 0.85 s / 0.34 s. The slow log-explorer statements in the old container were 93/97 Talstack (mostly `hashes`-filtered).

**Morning plan (clock times UTC, 2026-09-29):**
1. **≥ 10:00:** widen v4 to 09-14 (`ROLLUP RESUME otel_logs_and_spans dashboard_1m_v4 FROM '2026-09-14T00:00:00Z'`), only if `admission_refused_state_bytes_total` is not growing faster than yesterday's ~2,143 per 9 h and memory is < ~40 GiB.
2. **W54 go/no-go** (owner; review notes in session scratchpad `w54review.md`). If go, deploy the flag-off build **before 11:30**, so the process is ≥ 2 h old by 14:00. Enabling the flag is a separate, later step.
3. **No code pushes 12:00–18:00** (docs are fine). This protects the plan gate's clean window.
4. **18:05:** run the gate on 14:00–18:00: CPU against 09-25 14:00–18:00 (clean baseline); processed bytes and lease against 09-27, with the restart caveat stated. Dedup bytes are now real (W50).
5. After any W54 deploy: re-time shipbubble cold→warm, including a 2-day-old trace lookup. After 2 h, if the flag is enabled, check `dv_strip_plan_sorts_total` = 0 and `dv_rewrite_rows_retired_total` climbing.

**Leave alone:** the claim rate (~371/s is the pre-deploy baseline; commit wait averages 5 ms); the `ordering_pushdown::one_unsorted_file_does_not_cost_the_majority_its_ordering` e2e flake (flaky under load in 4 independent runs; passes alone). Monoscope-side items remain owner decisions: `array_has` lowering, backing off issue-chart auto-refresh after a timeout, and the missing timeout on the issue-page session lookup.

