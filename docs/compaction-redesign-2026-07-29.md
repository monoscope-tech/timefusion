# Compaction Redesign — every project, every tick, inside 5 minutes

Status: DESIGN (approved direction: stay Delta Lake; no new env vars; delete env vars via derivation)
Date: 2026-07-29

## 1. Measured problems (prod, 2026-07-29)

| # | Problem | Evidence |
|---|---------|----------|
| P1 | Per-project greedy drain starves the project list | past3 held 16:58→17:04 exclusively; Talstack's first bin at minute 8 of a 5-min cron; 8 of 11 hot projects never reached |
| P2 | Metadata replay dominates CPU | 34.5% of process CPU in `ScanLogReplayProcessor::process_actions_batch`; 18.4% in Add-stats `parse_json_impl` (cpu-000014.svg). `light_optimize_tail` re-walks + re-parses per project per pass: up to 132 walks/tick over 24,343 files |
| P3 | Commit granularity causes OCC storms | ~130 commits/tick on ONE shared Delta log vs concurrent ingest appends; retry ladders to attempt 9–20 (~10s each); hard aborts on delete-delete vs dedup (3× on 07-29); 40–65 s/pass is mostly commit waiting |
| P4 | Memory budget is hand-set env vars that drift | `MEMORY_LIMIT_GB=26` vs real cgroup 120 GiB; 07-21 crash-loop was 90 GB budgets vs 85 GB limit; every concurrency default is an OOM scar (delta-rs num_cpus OOM 06-11; 2-sorts-in-6GB starvation 07-23) |
| P5 | Compaction competes with durability | Emergency WAL flushes (55 GB vs 6 GB threshold) ran while light-optimize held both rewrite permits and dedup ran 572s; WAL quarantine is the known silent-loss sink |
| P6 | Tick has no deadline | "still in progress after 600s" on a 300s cron; ticks stack, skips compound |

Scale context: 11 hot projects, 24,343 active files / 992 GB (past3 = 21% of files, 76% of bytes), ~50k physical objects. Server: 48 cores (~25 idle), 188 GiB host, TF cgroup 120 GiB (59 in use).

## 2. Design principles

1. **Stay Delta Lake.** Everything below is standard protocol: snapshot reads, staged parquet, `CommitBuilder` transactions of Remove+Add. Any Delta client can still read the table.
2. **Physically disjoint work must not contend.** Per-(project, date) partitions never overlap; contention between them is an artifact of commit granularity, not a data constraint. (InfluxDB IOx shape: independent per-partition compaction units, one catalog update; we emulate with one *batched* commit per wave.)
3. **No knobs. Derive from the box.** One derivation tree from cgroup limits, fixed fractions, empirical constants in code. Runtime signals are one-way safety brakes only (skip work), never closed-loop controllers (no oscillation).
4. **Durability outranks compaction.** Compaction makes reads fast; the WAL prevents data loss. Under contention, compaction yields — binary, not proportional.
5. **Fairness by construction.** Every project gets its Nth bin before any project gets its (N+1)th (round-robin already implemented + tested 07-29).

## 3. Architecture: plan-once → rewrite-parallel → commit-once

Per table, per 5-min tick:

### Phase A — Plan (one tag-first metadata walk, ~10 ms)
- **Explicit levels via file tags** (IOx L0/L1/L2, RocksDB leveling — formalizing tiers we already have): **L0** = flush output (untagged small files), **L1** = 256 MB sorted runs (SORTED_RUN_TAG), **L2** = nightly consolidated/recompressed output.
- Read the snapshot ONCE per tick. Selection walks **only L0 + at most one sub-cap L1 run per bin** — converged L1 and all L2 are excluded by tag before any stats parse. O(live tail) ≈ 50–100 files, not O(all 10k+); planning cost stays flat as the table grows.
- `select_all_hot_bins(snapshot) -> Vec<(project, Vec<Bin>)>` — existing selection semantics unchanged (15-min seal lag, target/4 sorted-run cap, 7/8-target converged exclusion, min_files, event-time binning for time-disjoint runs).
- **Ordering**: round 0 ordered by compaction debt — small-file ratio weighted by read traffic (per-project query counters exist) — not raw file count (ClickHouse merge-selector / RocksDB compaction-priority shape). A partition nobody queries is deferred work, not urgent work.
- Kills P2: 132 walks → 1, and each walk parses stats for the live tail only.

### Phase B — Rewrite (parallel, lock-free, ~2–4 min)
- Each bin rewrite is pure object-store work: read selected files, streaming sort-merge by schema keys, stage new parquet to R2 (same staging as the dedup dirty-bin path, `RecordBatchWriter` + `cleanup_orphaned_parquet` on failure). No commit, no table lock.
- Scheduling: round-robin rounds (fairness, P1) × `buffer_unordered(K)` (parallelism). K from the derivation tree (§4), ≈4–6 on this box.
- Wave = one round-robin round's worth of finished bins.
- **Staged-intent manifest (restart-proofing the leak, not correctness)**: before a bin's first staged write, append its intended staged paths to a small manifest on local disk (WAL volume); drop the entry after the wave commits or cleanup runs. On boot, before maintenance starts: entries whose paths the log doesn't reference are DELETEd from R2 directly (no LIST — listing in recovery paths is forbidden per the R2/minio-hs history). Correctness never depends on it: staged files are invisible to readers until the atomic wave commit (put-if-absent via the lock client), so a crash at ANY point is either "wave never happened" or "wave fully applied"; the manifest only bounds the orphan window that VACUUM would otherwise take hours-to-days to reap (worst case ~K × 256 MB per crash). All other tick state (plan, rotation cursor) is derivable and deliberately not persisted.

### Phase C — Commit (one transaction per wave, ~2 s)
- Collect all finished bins' Remove+Add actions → ONE `CommitBuilder` commit under the existing per-table commit lock.
- Pre-commit liveness verification exactly as the dedup path (src/database.rs:6055): refresh snapshot; any bin whose target file is no longer live → drop that bin's actions only, its files re-selected next tick.
- Kills P3: ~130 commits/tick → 2–3. Ingest appends don't conflict with disjoint removes; the OCC ladder collapses. Delete-delete vs dedup is also gone once dedup batches the same way (Phase 2 below).
- Post-commit: `swap_and_refresh_cache` with the wave's pre/post URI sets — **warm-before-evict**: the new runs are warmed into Foyer BEFORE the replaced files are evicted. Wave commits concentrate invalidation (K bins swap at once); evict-first would cold-start the hottest query window every wave (the 07-21 cache-thrash lesson).
- **Checkpoint cadence is owned by the wave engine**: checkpoint after the last wave of each tick (with a T-minutes fallback). 130→3 commits/tick makes per-N-versions checkpoints ~40× rarer exactly when replay-tail length is the top CPU cost — cadence must not be an accidental side effect of commit count.

### Tick deadline (P6)
- Wall-clock budget = 80% of the cron period, derived from the schedule the scheduler already knows (240 s for a 5-min cron). Stops new waves; in-flight bins finish and commit. Truncation increments `light_optimize_tick_truncated` and logs remaining projects. A rotation cursor carries the truncation point to the next tick so the same tail is never skipped twice in a row.

### Time budget check (worst case, this box)
- Round 0, 11 projects, K=4: 3 waves × ~40 s pure rewrite (≤256 MB bins, no commit waits) ≈ 2 min, + 2 commits ≈ 4 s. Remaining ~2 min → extra rounds for the busiest projects. Backlog bins (e.g. Velox's 994-file bin ~110 s) fit inside a wave slot without blocking other projects' commits.

## 4. Self-sizing budget tree (deletes env vars, P4)

At startup, read cgroup memory limit and cpu quota (fallback `available_parallelism()`); derive:

```
limit = cgroup memory limit                     (120 GiB prod)
├─ query pool        = existing MEMORY_FRACTION logic over REAL limit
├─ ingest buffer     = fixed fraction
├─ foyer memory      = fixed fraction
├─ writer_reserve    = K × merge_tasks × ~1.5 GiB   (the "pool-invisible"
│                       delta-rs writer buffers — budgeted explicitly,
│                       previously budgeted NOWHERE = the 06-11 OOM)
└─ maintenance       = remainder
     ├─ per-sort budget = 8 GiB (empirical constant in code; conservative —
     │                     from the legacy blocking-sort peak of 5.8 GiB;
     │                     tighten after sorted-run transition completes)
     ├─ light share / heavy share split as today (heavy keeps ≥ 1/4)
     └─ K = min( light_share / per_sort_budget,
                 cores / 4,
                 hot_project_count )            (≈4–6 prod; degrades to 1
                                                  on small boxes instead of OOMing)
```

Measured headroom (07-29 profiles): 59 GiB used vs 120 GiB cgroup; the env tree sums to ~71 GiB — **~50 GiB genuinely idle**. The derivation should allocate it deliberately: K's memory term has more room than the conservative 4–6 estimate, and Foyer's 4 GiB read cache is the most starved consumer relative to impact (cache hit-rate is what the seal-lag/thrash history says query latency lives on) — surplus goes there first.

WAL thresholds join the same tree (flush threshold as a fraction of the ingest buffer, not a free-standing 6 GB constant that is currently 9× exceeded by design drift).

**Env vars deleted** (defaults become derivations): `TIMEFUSION_LIGHT_OPTIMIZE_CONCURRENCY`, `TIMEFUSION_MAINTENANCE_REWRITE_CONCURRENCY`, `TIMEFUSION_MAINTENANCE_POOL_GB`, `TIMEFUSION_MEMORY_LIMIT_GB`, `TIMEFUSION_OPTIMIZE_MAX_CONCURRENT_TASKS`, `TIMEFUSION_LIGHT_OPTIMIZE_TICK_BUDGET_SECS` (added 07-29, removed again), WAL byte/file thresholds. Keep only: schedules (cron strings) and `TIMEFUSION_LIGHT_OPTIMIZE_ENABLED` as the incident kill switch.

## 5. Safety brakes (one-way, no feedback loops)

- **Memory brake**: at wave boundaries only, check jemalloc `stats.allocated` vs ~85% of cgroup limit → don't start the next wave this tick; counter + log. Converts a would-be OOM-kill (exit 137 → WAL recovery → quarantine risk) into a truncated tick. Never sizes K.
- **WAL yield (P5)**: while the emergency-flush predicate is active, start no new bins; in-flight bins finish; resume when under threshold. Binary, uses the flusher's own hysteresis. Prerequisite: WAL threshold re-derivation (§4) so the condition is bursty, not chronic — otherwise the yield would starve compaction permanently.

## 6. Implementation phases

**Phase 0 — immediate, independent of the engine work**
- **Stats trimming**: `src/database.rs:3249` sets `delta.dataSkippingNumIndexedCols = -1` — stats JSON for EVERY column of the wide OTel schema on every Add. This is a major driver of the 18.4% `parse_json_impl` CPU, paid by queries and maintenance alike. Change to an explicit stats-column list (timestamp + sort/prune keys): new Adds shrink ~10× immediately, old Adds age out through compaction itself. One line; deployable today.

**Phase 1 — hot path (the 5-min goal)**
1. Factor `light_optimize_tail`'s selection body into a pure function over pre-fetched (path, size, tags, stats); add `select_all_hot_bins` = one walk + per-project apply. Unit-test selection parity against the old path on synthetic Add sets.
2. Bin rewrite executor reusing the dedup stage path (TableProviderBuilder with eager snapshot pinned at plan time + file-scoped scan, `sort_batches_by_schema`, `RecordBatchWriter`, `cleanup_orphaned_parquet`).
3. Wave commit: batched `CommitBuilder` Remove+Add + liveness verification + `swap_and_refresh_cache`. Replaces the per-bin OptimizeBuilder path outright (no parallel code paths, no flag; `LIGHT_OPTIMIZE_ENABLED=false` remains the kill switch).
4. Keep the 07-29 round-robin + deadline + truncation counter as Phase B's scheduler; add the rotation cursor.
5. Metrics: per-tick projects-planned / projects-completed / bins-committed / waves; alert if completed < planned for N consecutive ticks.

**Phase 2 — dedup joins the same engine**
- Dirty-bin dedup rewrites become bins in the same wave pipeline (sealed-date bins, so still disjoint from hot bins). One commit per wave regardless of which job produced the bin. Ends the 572s serial dedup runs AND the optimize-vs-dedup delete-delete aborts (P3 fully closed).

**Phase 3 — budget tree + env deletion**
- Implement §4 derivation; delete the listed env vars; move WAL thresholds into the tree; add the two brakes (§5). Deploy is config-simplification only at this point — behavior already proven under Phase 1/2.

Each phase is independently deployable and observable; rollback is redeploy of the previous image (Delta table format untouched throughout).

## 7. Verification

- Unit: selection parity (old vs new planner, synthetic snapshots); wave-commit action-set correctness (removes ⊆ planned, adds = staged); liveness-drop semantics; round-robin fairness (already green: 3 tests, 07-29); rotation cursor.
- Integration (local MinIO/R2 sim): concurrent ingest + hot compact soak — assert zero hard OCC aborts, row-count invariance per (project, date), every hot project compacted per tick.
- Prod acceptance: `light_optimize_tick_truncated == 0` steady-state; per-tick planned == completed for all 11 projects; p95 tick wall-time < 240 s; hard-abort count 0; file count for date=today trending to ~size/256 MB per project; no WAL-threshold breach during compaction waves.

## 8. Pros and cons per decision

### D1. Plan-once (single metadata walk per tick)
**Pros**
- Removes the dominant CPU cost: 132 walks/tick → 1 (34.5% of process CPU was log replay, 18.4% stats JSON parse).
- Planning from one snapshot gives a consistent cross-project view — bins can't overlap because two passes saw different snapshots.
- No semantic change to selection; parity is unit-testable.

**Cons / risks**
- The plan is stale by the time late waves run (files compacted, new flushes landed). Mitigated by the same commit-time liveness check the dedup path uses — a stale bin drops out, costing only the wasted rewrite of that bin (bounded at 256 MB).
- One walk per tick is still O(active files) — at 10× today's file count the 1 s plan becomes ~10 s. Acceptable; vacuum/consolidate keep active-file count bounded.

### D2. Rewrite-parallel, lock-free staging
**Pros**
- Rewrites become pure R2 + CPU work → parallelism is limited by resources, not by the table lock; ~25 idle cores become useful.
- Staging + `cleanup_orphaned_parquet` means a crashed/failed rewrite leaks nothing and corrupts nothing (uncommitted files are invisible to Delta readers).
- Reuses the battle-tested dedup stage path rather than a second implementation.

**Cons / risks**
- Wasted work is possible: a bin rewritten but dropped at liveness check re-runs next tick (write amplification, bounded ~3× per byte by the existing sorted-run-cap policy, worst case one extra bin per conflict).
- More parallel R2 PUTs during waves — R2 egress/ingress bursts; needs the upload-tee cap from the 07-29 optimization batch to stay bounded.
- Higher peak memory during waves than serial (why K is memory-derived, and why the writer_reserve line exists).

### D3. Commit-once per wave (batched Remove+Add)
**Pros**
- ~130 commits/tick → 2–3: OCC retry ladders collapse; ingest appends have almost nothing to collide with.
- Delta log grows 2–3 versions/tick from compaction instead of ~130 — directly slows the metadata-size problem (D1's own cost driver, and checkpoint frequency).
- All-projects-per-wave atomicity: readers see a consistent jump, never a half-compacted wave.

**Cons / risks**
- Bigger blast radius per commit: one conflicting file among 11 bins used to fail 1 commit of 11; naive batching would fail all. Mitigated: liveness check drops only the stale bin's actions pre-commit, and the commit itself retries under the lock.
- A large commit (hundreds of Remove+Add actions) is a bigger JSON write per version — negligible vs 130 separate versions, but checkpointing cadence should be watched.
- This is the one genuinely new engine code path (custom commit instead of OptimizeBuilder) — the riskiest part of the design; carries the heaviest test burden (§7) and keeps `LIGHT_OPTIMIZE_ENABLED` as the kill switch.

### D4. Self-sizing budget tree (delete env vars)
**Pros**
- Ends config drift as an incident class (26 GB config vs 120 GiB cgroup today; the 07-21 90-vs-85 crash-loop).
- One derivation is auditable; six hand-set numbers with an implicit sum constraint are not.
- Portable: small boxes degrade to K=1 instead of OOMing; bigger boxes speed up with zero retuning.
- Directly serves the "fewer env vars" direction — net −7 knobs.

**Cons / risks**
- Fixed fractions are opinions; a workload where the split is wrong has no override anymore (that's the point, but it removes the escape hatch — the kill switch and redeploy-with-code-change become the only levers).
- cgroup detection must be right in every runtime (docker swarm, local dev, tests); a misread limit mis-sizes everything. Needs explicit startup logging of the derived tree.
- Migration churn: existing deployments' tuned values are discarded — must verify the derivation reproduces (or improves on) today's working numbers on prod's box before deleting.

### D5. Binary WAL yield + wave-boundary memory brake
**Pros**
- Deterministic, explainable ("compaction pauses while durability catches up"); no control-loop oscillation by construction.
- Converts OOM-kill (exit 137 → WAL recovery → quarantine risk = our known silent-loss sink) into a truncated tick.
- Self-limiting: flush bursts create small files; sweeping them next tick does less total rewriting than compacting mid-burst.

**Cons / risks**
- If WAL-over-threshold is chronic (it is today: 9× over), the yield starves compaction permanently — hard prerequisite: WAL thresholds must be re-derived in the same tree first.
- Brake at wave boundaries only → a single wave can still overshoot between checks (bounded by K × per-sort budget, which the tree already reserves).
- Two more states in the tick lifecycle to reason about in logs/metrics (mitigated by explicit counters for both).

### D6. Round-robin fairness + tick deadline + rotation cursor
**Pros**
- Fairness is a construction-time guarantee, not an emergent property; already implemented and unit-tested (07-29).
- Deadline stops tick-stacking (P6); cursor guarantees eventual coverage even on truncated ticks.

**Cons / risks**
- Round-robin is worse than greedy for the single most-fragmented project (past3 waits for others' bins each round) — deliberate trade: worst project's convergence slows slightly so 10 others converge at all.
- Truncation + cursor means "every project every tick" degrades to "every project within k ticks" under overload — the metrics in §7 make that visible rather than silent.

## 9. Explicitly rejected

- Per-project Delta tables / catalog layer — violates "stay Delta Lake" as deployed; not needed for the goal.
- jemalloc-driven K sizing — oscillates against decay; brake only.
- Proportional WAL→K scaler — unexplainable at 3 AM; binary yield only.
- New env vars / feature flags for the new path — direction is fewer knobs, kill switch retained.
