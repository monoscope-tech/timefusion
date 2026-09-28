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
- Probe script (18 shapes × 3 projects, with rollup counter deltas): `/private/tmp/claude-501/-Users-tonyalaribe-Projects-apitoolkit-timefusion/b143aa08-537f-4b4f-ac75-4672e7af6a39/scratchpad/probe.sh`.
- Scorecard and gated push: `…/ca5458e8-fb13-4404-be05-f6ba3c85c244/scratchpad/{scorecard.sh,gated_push.sh,gated_push_b.sh}`. Usage: `gated_push.sh <branch> <worktree> "YYYY-MM-DD HH:MM"`; the `_b` variant reads CI from `$CIB`. These are in `/private/tmp`, which does not survive a reboot. Copy them if you rely on them.
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
