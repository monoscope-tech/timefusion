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

**W54 review (02:40 UTC; full notes in session scratchpad `w54review.md`): `af08d77e` was NO-GO as built. A hardening follow-up is in progress on the same branch.**
- **Blocker:** when the always-on exact-count guard refuses a rewrite, the unit gets a flat 30 s retry, and the 60 s planner tick re-pends it to Pending at `now` (`enqueue_inner`). A refused bin would re-stage every ~35–65 s, forever, at full rewrite cost: the 09-15 wedge shape. The re-pend overriding a backoff is a pre-existing master gap; it also cancels the 09-24 600 s backoff.
- **With the flag off, the carry and guard still fire nightly,** not rarely. After midnight, yesterday's partition is sealed-consolidated with its DV files (~25–40 DV-bearing bins per night, inferred). The intended effect is good: yesterday's rollup slices are carried across the nightly pack instead of going stale and being rebuilt.
- **Correct:** the carry fires only when the held witness equals the pre-rewrite physical count (no stale → valid, double carry is a no-op) and uses the read path's `rows_below` rule. DV metadata is sound: all 3,480 DV files in the snapshot have `numRecords` and `1 ≤ cardinality ≤ numRecords`, and the dedup oracle logged 0 validation failures in 10 h.
- **The follow-up (`ws/w54-dv-strip`):**
  - Park a refused file set, with growing backoff, a counter and a WARN.
  - Stop the planner re-pend from pulling a Retry deadline earlier.
  - Gate the carry on `version_append` + `dedup_tiebreak` sources.
  - Rebase onto master and run one full local signoff.
- **Before enabling the flag (later):** one clean night on the flag-off build; strip units priced by the bin they rewrite, not the whole partition (Talstack's estimate sums ~48 GiB decoded); sealed dates first; large DV files stripped alone; today's files only after dedup settles; `dv_strip_plan_sorts_total` = 0.

**Ready for the morning decision (both signed off locally, all 5 checks; NOT deployed):**
- **W54 hardened: `ws/w54-dv-strip` @ `de4ce84c`** (rebased on master `37538d9e`; test 2481/2481, e2e 73/73). It resolves the review blocker:
  - **Park on refusal:** a lossy-rewrite refusal parks its input files (keyed on path plus deletion-vector size; 1 h, doubling to 16 h). The planner and packer both skip parked files, so they still agree on the debt.
  - **Backoff kept:** the planner's re-pend keeps a hygiene retry's backoff. This also fixes master's cancelling of the 09-24 600 s backoff. Rollups still re-mint immediately.
  - **Monitoring:** counter `lossy_rewrite_refusals_total` and WARN `lossy_rewrite_parked`.
  - **Carry gated:** only sources with `version_append` + `dedup_tiebreak`.
  - **Known gaps:** the older light-optimize/`hot_bin_admits` paths don't skip parked files (dead in prod); parks are in-memory, so each restart retries once.
  - **Recommendation:** deploy flag-off before 11:30 if the owner agrees. On the first 00:00–02:00 rollover, watch `lossy_rewrite_refusals_total` (expect 0), `dv_rewrites_landed_total` and `rollup_witness_carried_total` (> 0), and yesterday's rollup hit rate.
- **W56 sessions proper fix: `ws/w56-subgrain-sessions` @ `66e0f29d`** (test 2461 passed, e2e 73/73). Option (a): hour-grain base tiers are minted and bisected only in whole hours.
  - **Where the rule applies:** `rollup_unit_grain`, used in `invalidate_touched`, `bisect_time_unit`, `enqueue_inner` and the boot migration.
  - **Old 10-min partials:** recognised by slice width. They are not served (`SubGrainSlices`), and the census rebuilds the hour, retiring them.
  - **Routing refusal:** the blanket W40 refusal is removed.
  - **Test:** `a_sub_grain_tier_routes_only_over_whole_grain_slices` covers publish, republish, old partials and rebuild, exact against raw. It fails with either half of the fix off.
  - **Also changes derived dashboard hour tiers:** misaligned queue entries are widened to whole hours. Review before deploying.
  - **Deploy anytime; sessions stay paused either way.** Before resuming sessions v2 (W27), watch `rollup_miss_sub_grain_slices_total` drain.
- **Batching:** W54 and W56 can go out as one deploy (one restart) after combining them on a branch and running one combined `make ci-signoff`. Run nothing else in parallel; tonight's parallel signoffs took 69–90 min under contention.

**`batch/morning` (W54 hardened + W56) is signed off and ready to deploy: `c57b6dcd`** on master `40ada60e`. The combined `make ci-signoff` passed all 5 checks: test 2483/2483, e2e 73/73, pg-smoke, fmt, clippy.
- **To deploy, if the owner says go:**
  1. `cd ~/Projects/apitoolkit/tf-w54 && git fetch origin`.
  2. If `origin/master` moved with code, rebase and re-run `make ci-signoff`; docs-only moves are fine.
  3. `git push origin batch/morning:master`.
  4. After it lands, re-time shipbubble (including a 2-day-old trace lookup) and watch `lossy_rewrite_refusals_total`.
- **Signoff timing, uncontended (load ~6): 11 min total.** test 6.3 min (nextest 4.1 min), clippy 8.8 min (runs in the background in parallel), e2e 2.1 min, pg-smoke 9 s (image cached). The same signoff took 69–90 min last night with 3–5 builds competing. **Run one signoff at a time.**

**04:48 UTC watch:** another session deployed twice (02:15 `5f370b60`, 03:08 `37538d9e`; code change `1ddc2a59` "Cut local signoff from ~65 to ~5-10 minutes"). Prod is on image `f7697fd2`, up ~1.6 h. It is healthy:
- Latency: p50 44 ms, p95 0.16 s, p99 0.49 s.
- Journal lock wait: ~6.8 s/min, back to the handover's ~7.
- Memory: 12 GiB.
- Pending: base 278, derived 44, dedup 126.
- Shipbubble 24h status: 1.5 s.

`batch/morning` was built on `40ada60e`, so rebase it onto current master and re-run `make ci-signoff` (now ~5–11 min) before pushing.

**06:00 UTC: the owner said "do everything now".**
- **v4 widened to 09-14 at 05:57** (`resume_from` 2026-09-14). Preconditions: `admission_refused_state_bytes_total` = 0 over the prior process's 3 h, memory 14 GiB.
- **Deployed `ecc2483f` at 05:58:** W54 with the DV-strip flag off (park-on-refusal, backoff kept, carry gated) and W56 (hour-grain session tiers). The signoff reused all 5 attestations: the rebase only added docs. New image `46451938` was Ready at ~06:00; preload 202 files / 5.25 GB, no budget stop.
- **Shipbubble on the fresh process (~1 min up), first → second run:**
  - 24h status: 2.6 s → 0.65 s.
  - 24h by service: 0.76 s → 0.64 s.
  - Log explorer 24h: 0.85 s → 0.54 s.
  - Trace lookup, 30 min old: 0.42 s → 0.43 s.
  - Trace lookup, 2 days old: 30.2 s → 8.5 s. This is latent for shipbubble and measured by a synthetic probe; the 10 h of slow-statement logs had no shipbubble instance of that shape. It reads the stranded DV files of 09-26/27 whole. Re-measure at 1 h before quoting.
- **Counters at ~3 min:**
  - `lossy_rewrite_refusals_total` 0; `dv_rewrites_landed_total` 0; `rollup_witness_carried_total` 0 (too young).
  - `admission_refused_state_bytes_total` 64.
  - Pending: base 396 (after the v4 widening), derived 68.
- **W57, building:** DV strip on by default for sealed dates only (`date < today` inside `dv_strip_admits`). This avoids the review's two today-partition risks (whole-partition pricing, racing dedup). **Owner decision:** deploy it (one more restart, before 11:30 or after 18:05)? The alternative, setting `TIMEFUSION_DV_STRIP_ENABLED=true` on the CapRover service, also enables today's partitions, which the review advised against.
- **Gate window:** asked timefusion-65 (it deployed twice overnight) to make no code pushes 12:00–18:00 UTC.

**06:43 UTC: W57 deployed (`43af26a4`, owner-approved): DV strip on by default for sealed dates only.** Image `e71cbf2b`, up 06:45. At 06:58 (13 min in):
- `dv_rewrites_landed_total` 62; `dv_rewrite_rows_retired_total` 4.99M masked rows removed.
- `rollup_witness_carried_total` 466: rollups stayed routed through the strips.
- `lossy_rewrite_refusals_total` 0; `admission_refused_state_bytes_total` 0.
- Container: ~29 cores during the burst (normal maintenance ~17), memory 18 GiB.
- Query latency held: p95 0.21 s, p99 0.49 s. Shipbubble 24h status 0.84 s.
- Shipbubble 2-day-old trace lookup mid-burst: 30 s → 15.9 s cold, 8.5 s → 3.1 s warm. Re-measure once 09-26/27 are fully stripped.
- **Watch:** `dv_strip_plan_sorts_total` = 17. The counter fires for any strip-flagged pass whose plan has a `SortExec`, including multi-file sealed packs that legitimately merge-sort and files without a sorted-run footer, not only 1:1 strips. Memory and latency are fine. Follow-up: split the counter by 1:1 strip vs pack, and confirm 1:1 strips never sort.

**Today's partition — where the masks come from** (measured over every live 09-29 file for Talstack (131) and shipbubble (77); all 214 DV bitmaps decoded; details in scratchpad `maskorigin/results.md`):
- **~57% (Talstack) / 62% (shipbubble) of masked rows are EXACT duplicates TF manufactures.** When an UPDATE/DELETE lands on a bucket during its flush commit, `finish_flushed_snapshot` (mem_buffer.rs ~1395) finishes "dirty", keeps ALL of the bucket's rows and re-flushes the whole bucket, re-committing the prefix that already landed. Signature: two commits of the same bucket 1.8–62 s apart. Talstack had 12 events over 10 of ~70 buckets.
- **Table-wide, re-flush copies are ≥ 21% of all rows flushed today.** The landed-batch skip can't catch this: 0 repeated digests across 1,206 commits.
- **The other ~40%** are real MoR versions from monoscope's pattern-tag `hashes` UPDATE (differ only in `hashes` and `updated_at`).
- Same class as the 09-02 "we manufacture the duplicates" finding, at a different seam: the dirty-flush re-commit rather than WAL replay.

**In progress:**
- **W58 (DV strip for today):** prices strip units by the bin they rewrite; a lost race with dedup is a plain retry, not a park; per-cell rate limit, most-masked files first.
- **W59 (stop the dirty re-flush from re-committing the whole bucket):** drain the unchanged snapshotted batches, re-flush only what changed; kill switch; counters `flush.dirty_reflush_rows_{drained,reflushed}`. It is on the write path, so its worst case is lost or duplicated acked rows. **Deploy decision for the owner:** hold W59 until after 18:05 (recommended), or accept that a problem between 12:00 and 18:00 means pushing a fix into the gate window and re-running the gate tomorrow.

**INCIDENT 07:31 UTC — W57's sealed strip burst regressed query latency for other tenants.** (Deployed 06:43; shipbubble itself was unaffected: 24h status 1.6 s.)
- **Symptoms, cumulative on the process since 06:45:**
  - p95 0.20 → 0.48 s; p99 0.45 → 1.98 s; p999 3.6 → 15.7 s.
  - Slow statements in the last 30 min: service-map `with sp as` p50 4.2 s, max 43 s (was p50 2.7 s); 87576849 log explorer three ~93 s timeouts; Demo/Talstack charts up to 32–37 s; session lookups 27–32 s.
- **Scale:** 238 strips landed and 51M masked rows retired in 45 min, using 22–29 cores and 26 GiB. 09-28 sealed at midnight with ~1,100+ DV files in the top 3 projects alone, far more than the 56-file pre-midnight census. `dv_strip_plan_sorts_total` 63: large DV-only sealed partitions are being packed and merge-sorted instead of filter-copied 1:1.
- **Attribution (2-min rates):**
  - `foyer.inner_bytes_read` +6.1 GB (~50 MB/s) with +3,029 misses: stripped outputs are new, cold files, and the post-commit warm is footers only.
  - `prefilter_skipped` +83 vs `prefilter_used` +99: new files have no tantivy index yet, so lookups fall back to raw scans.
  - `delta_snapshot_refresh` +23 s per 2 min: one commit every ~12 s pushes a refresh into query planning.
  - Rollups are fine: 0 stale-coverage misses, 2,063 witness carries.
- **Cause:** W57's rate limit is per partition, not fleet-wide. The W57 agent flagged this, and the W58 brief already contained a global cap.
- **Action:** hotfix `ws/w57b-dv-strip-global-cap` is building (fleet-wide in-flight cap, default 2), to deploy by ~10:30 after one signoff. **Fallback** if it slips past ~10:45: flip `timefusion_dv_strip_enabled` default to false and redeploy, then re-enable with the cap after 18:05.
- **Revised deploy order:** cap hotfix → nothing 12:00–18:00 → W58 (today strip, must include the global cap; after the sealed burst is measured) → W59 (dirty re-flush), both after 18:05.
- **Follow-ups:** prefer 1:1 strips when every file in a bin carries a DV; warm stripped outputs' bodies for top projects (now measured as needed).


## 10. Plan status checklist (2026-09-29 17:10 UTC)

Evidence is `origin/master` at `8bea392b`; a branch counts as merged when its patch or its distinctive lines are on master. Handover 2 wins over the older docs where they disagree.

### Gates

- [ ] **Plan gate** (CPU-seconds and processed bytes per million ingested rows, matched hours). 09-28 was only an indication (CPU 5.3x lower, bytes ~13x lower) and is not accepted. Today's last code push was `8bea392b` at 11:28, so 14:00–18:00 is a clean window: run it at 18:05. Compare CPU against 09-25 14:00–18:00, and bytes and lease against 09-27 (restart caveat). Dedup bytes are real since W50.
- [ ] **Falling backlog / less work per accepted event.** Not shown. Pending base rose 350 → 386 in the 09-28 window, then to 396 after today's v4 widening. Part of the CPU drop is deferred scope (v4 week cap, backfill cap 16, paused sessions).
- [ ] **Paired ABBA/BAAB protocol with 95% bounds** (goal doc): never run for any gate.
- [x] Capacity replay 1x/2x/4x (W26 `5610c94e`, `d19631fb`): ~2x headroom. Scheduler only; CPU and IO not measured.
- [x] Stage 0 rollout gates: release 2 (#323).
- [ ] Byte-aware batches (≥ 10% build CPU): missed its gate; stays off.
- [ ] Stage 1A certified-clean (≥ 20% CPU): not run; off by default.
- [ ] **drop-v3 gate**: blocked until v4 is resumed for all 31 days and its contiguity matches v3 for 3 censuses.

### Done

- **Stage 0 loops and throughput.**
  - Loops: W13, W14, fix A, fix B, W38, W41, W42, orphan-derived prune.
  - Throughput: claim fast path, edit → `mark_dirty`, tier backfill cap, output-state pricing, state split, v4 sibling prior, state waiter.
- **Stage 1 measurement.** W7 `run-unit` harness and unit economics. 1B closed by measurement (shard amplification 1.00x). 1D closed (~0% irrelevant churn; W8 classifier shipped).
- **Stage 2.** W31 content-fingerprint slice proof (`682179d0`) and step-0 attribution.
- **Stage 3.** W9 design and step 1 (W13).
- **Stage 5 prerequisites.** W33, W37, W39, the mint horizon, and the W22/W23 analyses.
- **Read path.**
  - W20, W36, W43, W44, the per-file dedup split fix, W47 2a, W47 2b (`310636d4`).
  - The `1e79a30b` batch: W49, W50, W51, W52, W53, W55.
  - W48: resolved by W44 and W51; no commit of its own.
- **DV strip.** W54 hardened (`ecc2483f`), W57 sealed dates (`43af26a4`), W58 today plus fleet cap 12 per 10 min (`99e0b314`). Strip is on for all dates.
- **W59** dirty re-flush drain (`ed59a3dd`, on by default).
- **Tiers.**
  - W29 v4 / `1h_v3` with `level`; v4-fallback relabel.
  - W56 hour-grain session tiers (the W40 proper fix).
  - RUM rollup measures on the v4 tiers, plus the tantivy LIKE edge-space fix (`180ced63`, `8bea392b`).
- **Scheduler, sim and ops.** W10, W18, W26, W11 (OTel 0.33), W15, the ingest incident fix, the task-age metric, W25 part 4, W28.

### To do (next action or blocker)

- **v4 widening.** Resumed from 09-14 at 05:57. Next: 09-07, then 08-31, then all 31 days, one week per day. Check memory and `admission_refused_state_bytes_total` between steps.
- **drop-v3.** `ws/drop-v3` (`7d9033cd`) is not on master. Blocked on the v4 gate above.
- **W27 sessions v2.** v1 and v2 are both paused. Resume after `rollup_miss_sub_grain_slices_total` drains.
- **RUM measures history.** Cells built before 11:32 today decline with `measure_not_stored`, and nothing rebuilds a cell just to add a measure. Needs a low-priority backfill, or accept that windows older than today stay raw.
- **DV strip follow-ups:**
  - split `dv_strip_plan_sorts_total` into 1:1 strip vs pack; prefer 1:1 strips;
  - warm stripped outputs' bodies;
  - parks are in-memory;
  - re-measure the 2-day-old shipbubble trace lookup;
  - watch latency against the 12-per-10-min cap.
- **DARK, awaiting evaluation:**
  - W46 key-level dedup restriction (owner decision after a dark evaluation);
  - query-latency yield (A/B at matched hours);
  - W21 witness carry (recommendation: leave off).
- **Unmerged branches to decide:**
  - `ws/scan-byte-admission` (`11aedc2b`): byte admission for wide scans, and one heavy slot at the root. It fixes the bug `tf-heavyslot` reproduces. Flags off.
  - `ws/w47-cache-admit-stream-latency` (`b74f6c2e`): pgwire latency recorded at stream end. Master still records at `do_query` return, so p99 excludes streaming time. Handover 2 §1 assumed this had shipped.
  - `ws/w31-transparent-witness` (2 WIP commits, paused): finish or drop.
  - `ws/recovery-adopt-v2` (`3ff9bb66`): deprioritized (~4 requeues per restart).
- **Not started (conditional):** Stage 2 captured source view, Stage 3 batched publication, Stage 4 flush-time aggregation, Stage 5 dedup fusion, Stage 6 minute revisions, 1C shared scans, packed repairs (need a packed-remainder witness).
- **Unknown, check:** W7 real-S3 unit cost on staging; the W32 staging experiment (token cap vs idle cores).
- **Ops:** journal lock wait on a process at least 1 h old; W25 parts 2–3 (container start, double service update); dependency advisories (rustls-webpki 0.101, tokio-tar, tokio-postgres).
- **Owner decisions (monoscope):**
  - the pattern-tag `update2Sql` source fix (~40% of today's masked rows);
  - `array_has` lowering;
  - backing off issue-chart auto-refresh after a timeout;
  - a timeout on the `Issues.hs` session lookup;
  - W12 `name` HLL.

### Uncommitted and unmerged work (inventory 2026-09-29)

- **Main checkout** (`timefusion`, on master `2f2df90c`, 229 commits behind):
  - **`AGENTS.md`**: the full ~790-line project guide. It has never been committed; master's `AGENTS.md` is the 32-line CI note. Save this first.
  - The `src/` diff in 11 files: already on master.
  - Untracked plans and scripts: already on master or older than master's copies, except `docs/plans/2026-09-28-residual-cost-ranking.md`.
  - After saving those two files, the checkout can be reset to master.
- **Uncommitted elsewhere:**
  - `timefusion-overnight-rum-session-rollup-29-09`: a new e2e test (`prepared_rum_sessions_use_browser_rollups_and_raw_edges`).
  - `tf-heavyslot`: a reproduction test for the heavy-slot bug; the fix is in `ws/scan-byte-admission`.
- **Do not commit:** `tf-w29` has a staged reverse diff (2,638 lines deleted) from an aborted operation. `tf-w54` has a DV-strip grant superseded by `d1313093`.
- **Safe to delete:**
  - 65 worktrees and 94 remote branches whose content is on master.
  - 12 stale worktrees (August experiments, the distill sweep, the old fork bump).
  - Remote branches `heavy-admission-e2e`, `timefusion-deploy-completed`, `timefusion-deploy-lease`, `ws/w28-service-hll`.

## 11. 2026-09-29 evening

**Plan gate, 14:00–18:00** (image `8bea392b`, process started 11:32, no code deploy in the window; scripts in session 66d75f9a `scratchpad/gate/gate0929.py`):

| Per million ingested rows | 09-25 14–18 (clean baseline) | 09-28 14–18 | **09-29 14–18** |
|---|---|---|---|
| CPU-seconds (avg cores) | 21,723 (32.8) | 4,137 (5.8) | **9,676 (14.0)**: 2.2x below baseline |
| Maintenance GB processed | 133.5 (09-27, restarts inside) | 9.9 | **12.8**: HotPacking 6.2, BaseRollup 5.0, Dedup 1.3 |
| Lease-seconds | 16,189 (09-27) | 963 | 1,079 |
| Pending base / dedup, first hour → last hour | | 350 → 386 / 170 → 190 | **303 → 394 / 161 → 206** |

- **Verdict: not accepted.** Work per event is still well below the clean baseline, but the backlog grew again.
- Today costs 2.3x more CPU than 09-28, mostly HotPacking (6.2 against 0.8 GB per M rows): the W58 strip of today's DV files runs continuously.
- This morning's v4 widening to 09-14 also adds BaseRollup backfill.
- The paired ABBA/BAAB protocol is still not run; the runtime `FLAG` switch in `batch/evening` exists to make it possible without restarts.

**Prod steps at 18:08** (the only prod writes; both allowed by §6):
- `ROLLUP RESUME … dashboard_1m_v4 FROM '2026-09-07'`. Refusals were 416 in 6 h (yesterday 2,143 in 9 h); memory 21 GB. Next step is 08-31, then all 31 days.
- `ROLLUP RESUME … sessions_1h_v2 FROM '2026-09-22'` (W27; its 7-day horizon). `rollup_miss_sub_grain_slices_total` read 0 on a 6 h process after W56. `sessions_1h_v1` stays paused.

**W21 witness carry: closed, stays off.** It recovers ~0.7% and has two open enable blockers (double count, ledger lost on restart).

**Cleanup done:**
- The full `AGENTS.md` guide and the residual ranking are committed (`d6cec8ce`).
- 44 landed worktrees were removed (~200 GB freed).
- 368 remote branches were deleted: no unique patches and older than 24 h. 174 heads remain.
- Worktrees with uncommitted or unmerged work, and those touched in the last 6 h, were left alone.

**`batch/evening`** (signing off; deploy when green):
- `land/stream-latency` (`ce965e21`): pgwire latency recorded at stream end.
- `land/scan-byte-admission`: one heavy slot per query, on by default. Byte admission stays behind flags.
- `ws/measure-backfill`: re-mint cells that predate a declared measure. Low priority: 2 cells per pass, after v4 gaps; knob `TIMEFUSION_ROLLUP_MEASURE_REMINTS_PER_PASS`.
- `ws/runtime-flag-toggle`: `FLAG SET|RESET|SHOW` for `timefusion_maintenance_query_yield` and `timefusion_read_dedup_key_restrict`, in memory only; effective values under `timefusion_stats` component `flags`.
- **Pending:** `ws/dv-strip-followups` (strip-sort counter split, 1:1 strips preferred, body warm).

**Evening deploys (both after a full green local signoff):**
- **18:09 `8184a638` (`batch/evening`):**
  - latency recorded at stream end;
  - one heavy slot per query;
  - measure re-mint backfill;
  - runtime `FLAG` switch.
- **19:13 `4319b951` (`batch/night`):**
  - **drop-v3** (`dashboard_1m_v3` and `dashboard_1h_v2` removed; owner's call, since queries older than 14 days are rare);
  - DV-strip follow-ups:
    - `dv_strip_plan_sorts_total` now counts only unexpected 1:1 sorts; `dv_strip_resorts_total` counts footers without a declared order (the incident's 63);
    - all-DV cells strip one file at a time;
    - bodies of today's and yesterday's strip outputs up to 256 MB are warmed within the boot-preload budget.
  - The sigkill drill flaked once under load and passed 3/3 in isolation.

**v4:** `RESUME FROM 2026-08-29` (the full 31 days) at 19:02, owner's call. At 18:51 v4 had 15 contiguous days; v3/1h_v2 had 30. Windows older than about 15 days read raw until the v4 backfill fills them. The backfill is bound by state memory: 19,273 `admission_refused_state_bytes_total` in the first ~50 min after widening to 09-07. Dropping v3 frees that budget.

**W7 real-object-store unit cost** (local dev build against the staging prefix `timefusion-eu/timefusion-staging`, which lives on OVH, not R2; notes in session 66d75f9a):
- Large units are CPU-bound. 6297304f 12h: 229 MB / 122.6M rows, ~70 s CPU ≈ wall.
- Small units are bound by object-store fetches (28f62f01 6h: 11–25 s cold, 5–9 s warm).
- Staging plus commit is ≤ ~4 s per unit.
- The whale's 2 hash shards each re-read the same 267 MB, a direct measure of the Stage 1B shard cost (units of this size only).
- An isolated whale 12h unit costs ~150 s against ~410 s of prod lease per slice, so most prod lease time is contention, not work.

**W32 staging experiment: no staging host exists** (only the seeded prefix). It moves to prod instead: a runtime `FLAG` override for the CPU-token cap is being built, then the same ABBA/BAAB method, with no restarts.

**Paired protocol, first run:** query-latency yield (A off, B on) under synthetic load (16 workers, dashboard and explorer shapes, p95 ~3 s). Windows are 2 min warmup + 12 min measured, ABBA then BAAB. The 18:48 run was aborted by the drop-v3 deploy and restarts at 19:35.
