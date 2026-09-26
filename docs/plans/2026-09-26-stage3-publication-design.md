# W9: Stage 3 publication batching — implementable design

Status: analysis/design only. Code references are to `~/Projects/apitoolkit/tf-packed` at `77ab1518`.
Plan: `docs/plans/2026-09-24-rollups-on-a-fixed-server.md` § "Stage 3: batch publication and reuse
existing durability", § "Local checkpoint-sharing candidate", § "Dependencies, not a serial rewrite".

## 0. Answers first

| Question | Answer |
| --- | --- |
| Does the WAL payload change? | **No.** The publication path never touches `WalEntry` / walrus (`src/write/wal.rs`, `WAL_VERSION = 1` at L76). The three files it does touch are separate: the task journal (`TaskJournal` JSONL + WAL file in `maintenance_coordinator.rs`), `rollup_invalidations.json` (`rollup_journal.rs`) and `staged_intent.jsonl` (`maintain.rs::staged_intent_path`). No `WAL_VERSION` migration. The plan's WAL-migration section is about the *deferred* "source batch carries affected ranges" integration that would remove `commit_journal()` from the write-ack path. This design excludes that integration. |
| Does the task-journal format change? | No. `Publication` / `PublicationEvidence` / `JournalRecord` stay byte-identical. |
| Does `staged_intent.jsonl` change? | Values only, no new field is required (§4.4). An optional `batch_id` is **not** needed for v1. |
| New permanent stats | One: `rollup_publication_pending_bytes` (gauge). The dead counter `rollup_shared_commits_total` is revived as the batch-commit counter (§6). |
| Flush/insert acknowledgement waits on a rollup commit? | No, today or after. Rollup publication locks `commit_lock(project, <rollup table>)`; flush and ingest lock the *source* table (`write.rs::commit_lock_and_waiters`). The one real coupling is the shared `task_journal_group_commit`, which the insert-ack `commit_journal()` also rides. Batching *reduces* that coupling (§5). |
| Expected saving | **Small at today's rate.** It is about 4–8 k fewer rollup Delta commits/day plus 9–24 k object requests/day, and a ≤0.6-worker (≤3 % of rollup unit time) reduction from removing `slice_occ_stale` discard-and-rescan (≤1.1 workers during a post-deploy burst). Stale units are cheap 10-min slices (p50 12–18 s), not the 144 s mean (§2.1). The queue's main value is as the **bounded publication path that Stage 4 requires**. Per the plan, build it when Stage 4 is approved or the commit rate grows, and ship §9 step 1 on its own now. |

## 1. How a rollup publication commits today

Only one live path publishes fresh rollup output: `Database::run_coordinator_rollup_selected`
(`src/database/maintain.rs` L2433–L3297). One coordinator unit = one `(project, target table, slice)`.
Every unit that gets past the scan does this, in order:

| # | Step | Where | Durable op | Scope/lock |
| --- | --- | --- | --- | --- |
| 1 | Scan + aggregate (`hash_shards` passes) | L2896–L2955 | — | admission permit `_permit` (L2841) held to function end |
| 2 | Write parquet (`RecordBatchWriter`, one flush) and stamp identity tags (`TAG_SOURCE…TAG_MEASURES`) on each Add | L2961–L2999 | object PUTs (staged, unreferenced) | — |
| 3 | Compute `replaced` via `rollup::slice_retires` against the **staging snapshot**, `covering_slice_for` safety net, packed remainders (`stage_packed_rollup_remainders`, L2102) | L3000–L3065 | more PUTs if packed | — |
| 4 | `record_staged_intent` with `RollupResume { key, publication, source_rows, target: RollupTargetProof{before, after}, date }` | L3097 → L8235 | **append, no fsync** (`append_state_lines`, L9292) | `staged_intent_manifest_lock` |
| 5 | Take `commit_lock(project, physical_table)`, `refresh_table_snapshot`, then the **partition guard**: `partition_unchanged` (version equal, or every live Add in `(project,date)` byte-identical to the staging view) and `target_paths ⊆ live`. Failure → delete staged parquet, `clear_staged_intent`, `retry("slice_occ_stale", 1s)` | L3118–L3143 | intent rewrite | per-table commit lock |
| 6 | `CommitBuilder` `Write{Overwrite}` with `Remove(replaced) + Add(adds)`, lane `rollup_publish`, **`with_max_retries(0)`** | L3144–L3151 | **1 Delta commit** (log PUT + refresh reads) | per-table commit lock |
| 7 | `swap_and_refresh_cache` (version-guarded swap, `persist_snapshot` ≤1/min/table, detached warm/evict) | L3153, `mod.rs` L4287 | local snapshot file (rate-limited) | — |
| 8 | Under `rollup_journal_lock`: insert `rollup_slice_coverage`, maybe `rollup_coverage` (date level), `journal.publish(key, publication)`, `reopen_derived_over` for child tiers | L3167–L3252 | — (in memory) | `rollup_journal_lock` + journal mutex |
| 9 | `checkpoint_task_journal()` → `task_journal_group_commit.commit(journal.checkpoint())` | L3255, L4169 | **append + fsync** of the task journal (group-committed) | journal mutex held across fsync |
| 10 | `clear_staged_intent(&[wave])` — read whole manifest, filter, **rewrite whole file** | L3259 → L8250 | file rewrite, no fsync | manifest lock |

So each published unit costs **one Delta commit, one task-journal fsync (sometimes shared), two
manifest writes (one append, one full rewrite)**, one snapshot refresh and one cache swap. Its
`rollup_commit_duration_ms` (L3274) is timed from step 4 to step 10, so it includes the commit-lock
wait, the refresh, the Delta commit, the journal-lock wait, the fsync and the manifest rewrite.

Other publication-shaped paths, which batching must not break:

- `resume_rollup_unit` (L8311): commits a previous process's intent, lane `rollup_resume`,
  `with_max_retries(0)`, then `publish_resumed_rollup` (L8271) → `checkpoint_task_journal` →
  `clear_staged_intent`. It also handles `AlreadyLanded` (bookkeeping only, no commit).
- `complete_rollup_noop` (L3314) and `settle_covered_by_wider` (L4443): no Delta commit, only a
  journal checkpoint.
- `retried` / `completed` → `journal.checkpoint()` directly (not group-committed).

**What is already batched:**

- Task-journal fsyncs: fresh publications, resumed publications, no-op completions and insert-path
  invalidations share `task_journal_group_commit` (`support.rs::GroupCommit`, a sync condvar group
  commit). This is the plan's "local checkpoint-sharing candidate", and it is live in tf-packed
  (`checkpoint_task_journal`, L4169). Prod: `task_journal_checkpoints_coalesced = 25` of `16264`.
  With one unit publishing every few seconds, almost nothing shares the barrier.
- The rollup invalidation journal is rate-limited to ≤1 write/s (`ROLLUP_JOURNAL_MAX_STALENESS`,
  L1152; prod `rollup_journal_persist_deferred_total = 14722` vs `persists = 1049`). The publication
  path does not write it.
- Delta commits: **nothing.** The "shared commit" notion is gone. The pre-coordinator
  `commit_bounded_rollup_wave` / `RollupCommitWave` batched several units into one transaction and
  incremented `rollup_shared_commits`. Commit `032d64bc` ("delete the orphaned pre-coordinator
  backfill", 2026-08-19) deleted it, and its message notes that prod's `rollup_shared_commits_total = 0`
  had already been misread once. **The counter is still exported (`observability.rs` L1340) and
  has no incrementer.** Prod reads 0 for that reason, not because a batching path never fires.
- The compaction and dedup lanes do batch: `Database::commit_wave` (L7755) commits many `StagedBin`s in
  one transaction, with flush-priority yield, per-bin liveness split, self-landed split and a
  target-disjoint subset. It is the template for §4.

**Where coverage metadata is persisted** (nothing new is needed):

1. **Delta Add tags** on every output file: `TAG_SOURCE, TAG_PROJECT, TAG_SLICE_START/END,
   TAG_SOURCE_FINGERPRINT, TAG_CONTENT_FINGERPRINT, TAG_GENERATION, TAG_OUTPUT_ROWS,
   TAG_SOURCE_ROWS(_BELOW), TAG_MEASURES` (L2980–L2996). They travel inside the same Delta commit as
   the data, so batching several units into one commit keeps them atomic with their files.
2. **Task journal** `Task.publication` (`journal.publish`, `maintenance_coordinator.rs` L2120). Only
   *non-derived* keys are written durably (`checkpoint`'s `durable` filter, L2446). Derived-tier
   coverage recovers from Add tags only.
3. **Recovery**: `recover_rollup_coverage` (L5010) joins the tags (from the Delta log only) with
   `journal.published_rollups` / `rollup_slice_complete`. A non-empty slice needs **both** a tagged
   live file and a `Complete` journal task. An empty slice recovers from the journal publication plus
   `empty_base_rollup_current`.
4. `rollup_invalidations.json`: invalidation scheduling state only. The publication path does not write it.

## 2. Current publication economics (prod `timefusion_stats`)

Process: image `sha256:118b35b9…` (deployed 18:04 UTC 2026-09-26). Three samples of
`SELECT component,key,value FROM timefusion_stats` at uptime **1438 s, 1763 s and 2063 s** (s1 18:28:59, s3 18:39:24 UTC).
**Every rate below is provisional.** The process was 24–34 min old, below the ≥1 h rule, and the
first 24 min include a post-deploy backlog burst. Deltas are s1→s3 (625 s) unless marked
"since boot". Raw files: `s1.txt s2.txt s3.txt`, logs `logs40m.txt logs20m.txt` (this directory).

### 2.1 Counters

| Counter | since boot (s3) | Δ s1→s2 (325 s) | Δ s2→s3 (300 s) | per published unit (s1→s3) |
| --- | --- | --- | --- | --- |
| `rollup_staged_projects_total` (units published = Delta commits) | 405 | 41 | 37 | — |
| `rollup_shared_commits_total` | **0 (dead counter)** | 0 | 0 | — |
| `rollup_commit_actions_total` | 817 | 90 | 74 | 2.1 actions/commit |
| `rollup_output_files_total` | 404 | 41 | 37 | 1.0 file/unit |
| `rollup_commit_duration_ms_total` | 2 821 641 | 268 409 | 39 331 | **6.5 s** (s1→s2) / **1.06 s** (s2→s3) |
| `rollup_scan_duration_ms_total` | 30 510 544 | 5 135 405 | 6 096 152 | **144 s** |
| `retry.BaseRollup.slice_occ_stale` (+Derived) | 240 (+3) | 10 | 6 (+1) | **0.22 per publication** (since boot 0.60) |
| `rollup_skipped_covered_by_wider` | 199 | 30 | 30 | — |
| `task_journal_checkpoints` / `_coalesced` | 25 988 / 29 | 2 621 / 3 | 7 103 / 1 | — |
| block `journal_commit_wait` count / total ms / max ms | 35 344 / 1 509 448 / 6 578 | 4 067 / 508 546 | 9 467 / 195 878 | — |
| block `journal_lock_wait.total_ms` | 17 404 812 | 6 896 669 | 1 143 526 | — |
| `flush_completed_total`, `flush_stalled_total` | 152, 0 | 22 | 22 | — |

Rates:

- **Publications = Delta commits: 78 in 625 s ≈ 7.5/min ≈ 10.8 k/day.** The boot burst ran at 327 in 24 min,
  ≈ 19.6 k/day. Four tiers share them (log window of 344 publications): `dashboard_1m_v3` 74 %,
  `metrics_1m_v2` 14 %, `dashboard_1h_v2` 10 %, `metrics_1h_v2` 2 %. 88 % are BaseRollup.
- **Commit latency** (`rollup_commit_duration_ms`, from intent record to intent clear) swings 6×
  between adjacent 5-min windows (6.5 s → 1.06 s per unit). It tracks `journal_lock_wait`
  (6 897 s → 1 144 s of waiting in the same windows), so most of it is journal-mutex contention, not
  Delta. The logged slow units (`maintenance_rollup_slow_unit`, n = 98, units ≥ 60 s only, so a biased
  sample) show `commit_ms` p50 **375 ms**, p90 2.1 s, max 22.5 s, min ≈ 80 ms. Take ~0.1–0.4 s as the
  Delta refresh+commit floor per commit.
- **Stale discards:** 17 `slice_occ_stale` retries against 78 publications. The since-boot count was 243
  against 405. Each one deletes a finished staged output and rescans. What they cost comes from
  `maintenance_task_finished.ran_secs` (claim to lease drop, which covers the full scan for a stale unit).
  In s1→s3 there were only **21** BaseRollup `Retry` outcomes. 9 of them ran >10 s, totalling
  **268 s** (p50 18 s, max 125 s). The rest ran ≤10 s. So stale retries cost **≤ 388 s per 625 s ≈ ≤ 0.6
  worker-equivalents ≈ ≤ 3 %** of BaseRollup unit time (Complete units: ≈ 12 000 s in the same window).
  In the 25-min boot burst, *all* 1 358 BaseRollup retries (including ≈1 131 zero-second `admission_busy`)
  totalled 1 659 s against 23 131 s of Complete units, i.e. **≤ 6.7 %, ≤ 1.1 workers**. The stale units
  are the cheap same-partition 10-min slices, not the 144 s mean published unit.
- **Journal share:** publications cause 78 task-journal checkpoints in 625 s (≈ 0.12/s) against
  ≈ 15/s total (`task_journal_checkpoints` Δ 9 724). Publication is **< 1 %** of journal
  fsync traffic. Batching it cannot measurably relieve the insert-ack `journal_commit_wait`. §5 is a
  safety argument, not a saving.

### 2.2 Unit shapes (logs, 25 min, 344 publications)

Slice widths: 10 min (184), 12 h (90), 24 h (36), 5 min (18), 6 h (11), others (5). There were 42
distinct `(tier, project, date)` partitions, and one partition (today, project `87576849…`) took 96 of
the 344. In the steadier s1→s3 window: 80 publications, 21 partitions, top partition 17. Same-partition
concentration is what turns sibling commits into `slice_occ_stale`.

### 2.3 Why `slice_occ_stale` is the target

`retry("slice_occ_stale")` has exactly one emitter: `maintain.rs` L3142, the partition guard at step 5
of §1. It fires when the refreshed `(project,date)` partition differs in *any* Add from the staging
view, or a retire target vanished. A sibling unit of the same partition committing a *disjoint* slice
between this unit's staging and its commit satisfies the first condition. So does a **narrower unit
contained in this one's slice** committing first (mixed widths run concurrently in one partition:
10 min, 12 h and 24 h slices, plus `rollup_skipped_covered_by_wider` +30 per 5 min). Its consequence is
`cleanup_orphaned_parquet` + `clear_staged_intent` + retry, which means the whole scan is repeated.
The guard is required (the plan's normal-publication regression reproduced two live outputs without it),
but it is wider than the property it protects (§4.4 step 3).

Candidate causes, all unmeasured. Step 1 of §9 logs which condition failed (`version_delta`,
`missing_targets`, `changed_in_partition`, and `new_files_relation ∈ {disjoint, contained, covering,
other}`), as log fields and not stats:

1. a disjoint sibling slice committed (most likely, given the partition concentration);
2. a contained narrower slice committed (nested widths);
3. a spurious difference. The equality compares `add.stats` as a string plus `modification_time`, so
   a refresh that re-materializes stats differently (e.g. from a Delta checkpoint written by the
   `Checkpoint` cron) would fire it with nothing changed;
4. another writer rewrote rollup files. No compaction lane touched a rollup tier in the 40-min log (task
   tables were `otel_logs_and_spans` / `otel_metrics` only for HotPacking/Dedup/SealedConsolidation).
   Packed repairs by other rollup units do rewrite them, which is case 2 or 5 here;
5. a wider covering publication landed (correctly stale).

### 2.4 How much a linger would coalesce (replaying logged publication times per tier)

| Linger | 25-min burst window (344 pubs) | s1→s3 window (80 pubs) |
| --- | --- | --- |
| 5 s | — | 53 commits (1.5×) |
| 10 s | 113 (3.0×) | 45 (1.8×) |
| 30 s | 69 (5.0×) | 28 (2.9×) |
| 60 s | 49 (7.0×) | 20 (4.0×) |

This replay ignores the `max_entries` cap and assumes publication times do not shift. It is a planning
estimate only.

## 3. Design goals and non-goals

Goals, in value order:

1. **Stop discarding finished scans.** Units that target the same `(project, date)` partition of the
   same tier must not invalidate each other's staged output.
2. Commit several ready ranges per target table in **one** Delta commit, with **one** task-journal
   checkpoint and **one** manifest rewrite.
3. Bound what waits: global and per-table limits on entries, staged bytes and age. Hold no Arrow
   data in the queue.
4. Keep every existing proof: partition revalidation, `Running`-state gate, `covering_slice_for`
   safety net, packed remainders, generation tags, resume and recovery.

Non-goals: no WAL or `WalEntry` change, no new journal or ledger, no packed publication manifest in v1
(§4.7), no cross-table atomicity (the plan says so explicitly), no change to the insert-ack
`commit_journal()`.

## 4. Design

### 4.1 Split the unit into "stage" and "publish"

Refactor `run_coordinator_rollup_selected` at step 3/4 boundary into:

```rust
/// Everything a staged unit needs to land, and nothing that holds Arrow data.
struct StagedRollup {
    key: TaskKey,
    lease: TaskLease,                     // moved in; its Drop still owns abandonment
    date: String,                         // partition date
    staging_version: i64,                 // target version the replace-set was decided on
    partition_before: Vec<Add>,           // live Adds of (project,date) at staging (today's `live_adds` ∩ in_partition)
    replaced: Vec<Add>,                   // retire set incl. packed originals
    adds: Vec<Add>,                       // staged output incl. packed remainders, tags stamped
    publication: Publication,
    coverage: RollupCoverage,             // the slice_coverage value built at L3170
    partition_identity: Option<(u64, i64, i64)>,
    derived: bool, from: String, rows: u64, retiring_untagged: u64, leaves_partition_clean: bool,
    wave_id: String,                      // staged-intent id
    stage_store: Arc<dyn ObjectStore>,
    timings: (u64 /*scan_ms*/, u64 /*stage_ms*/, std::time::Instant /*unit_started*/),
}
```

The existing L3066–L3297 body becomes `publish_rollup_batch(&self, table: &str, Vec<StagedRollup>)`.
A batch of one reproduces today's behaviour exactly, so the flag-off path and a
`TIMEFUSION_ROLLUP_PUBLISH_BATCH=false` kill switch are the same code with `max_entries = 1`.
The `_permit` (admission) is dropped at hand-off. Decode memory is already released and the staged
parquet is on the object store.

### 4.2 The per-table publication queue (async group commit)

`Database` gains one field: `rollup_publications: DashMap<(String /*lock key*/, String /*table*/), Arc<PublishQueue>>`.
Key it with `table_lock_key`, the same key as `commit_lock`, so one queue maps to one commit lock.

```rust
struct PublishQueue {
    pending: parking_lot::Mutex<Vec<(StagedRollup, oneshot::Sender<Result<Outcome>>)>>,
    leader: tokio::sync::Mutex<()>,   // at most one drainer per table
    notify: tokio::sync::Notify,
}
```

A worker that finishes staging calls `self.enqueue_publication(staged).await`:

1. **Admission check (the bound).** If adding this entry would exceed a global or per-table limit
   (§4.3), do not enqueue. Publish inline as a batch of one, which is today's behaviour. This fallback
   never loses work and never waits on the queue.
2. Push the entry and add `Σ add.size` to the global `rollup_publication_pending_bytes`.
3. **Leader/follower, same rule as `GroupCommit`: take the ticket, then wait.** If `leader.try_lock()`
   succeeds, this worker is the drainer. It waits until one of these holds: `pending.len() ≥ max_entries`,
   pending bytes ≥ `max_bytes`, the oldest entry's age ≥ `linger`, or shutdown is cancelled. It then
   takes the whole `pending` vector and runs `publish_rollup_batch`. It keeps draining while entries
   arrived during its commit, because committing the work of others is what satisfies their waits.
   Followers `await` their oneshot.
4. The worker returns the unit's outcome (`Ok(true)` / retry) exactly as today.

**Who waits, and why this choice.** The worker keeps its `TaskLease` and waits on the batch. That wait
is bounded by `linger + one commit`. The fire-and-forget alternative frees the job slot sooner. But
`TaskLease::drop` (`maintenance_coordinator.rs` L664) abandons any unit it finds still `Running`, so
detaching would need a new `TaskState` (e.g. `Publishing`) plus journal replay handling. That is a
format change and a scheduler change. The cost of waiting is measurable: at most
`coordinator_job_slots × linger` worker-seconds per linger window, against a median `scan_ms` of
60–700 s per slow unit. A 10 s linger costs <2–15 % of a unit's wall time and holds no decode memory or
admission tokens. Revisit only if the experiment shows job-slot starvation (`eligible_base_rollup`
rising while `coordinator_pool_pct` falls).

`GroupCommit` itself is not reused. It is a sync condvar, and wrapping an async Delta commit in it
would block a maintenance runtime thread for the whole commit. The async queue keeps its invariant: a
caller's work is enqueued *before* it waits, and the drainer takes everything present when it starts.

### 4.3 Limits (config, `MaintenanceConfig`)

| Knob | Default | Rationale |
| --- | --- | --- |
| `timefusion_rollup_publish_linger_ms` | 10 000 | 10 s collapses 344 → 113 table-commits in the measured window (§2.4); commit p50 is 0.4 s |
| `timefusion_rollup_publish_max_entries` | 32 | bounds Delta actions/commit (~2 actions/unit today) and journal records per checkpoint |
| `timefusion_rollup_publish_max_bytes` | 1 GiB | Σ staged `add.size` across **all** queues. If this cap is reached, workers publish inline and `claim_coordinator_task` for BaseRollup/DerivedRollup declines new claims until pending bytes fall below half the cap. This is the plan's "optional aggregate production defers" rule; durable source evidence (Pending tasks) keeps the missing coverage discoverable |
| kill switch | `max_entries = 1` | identical to today |

Memory held per entry: `Add` metadata (paths, stats JSON, tags), the partition's `Add` list and a
`Publication`. This is a few KB to a few hundred KB, with no `RecordBatch`. The entry count is bounded by
`coordinator_job_slots` by construction, because each entry's worker is parked on it. The queue cannot
grow without bound: a worker that cannot enqueue publishes inline.

### 4.4 `publish_rollup_batch`: validation and commit, modeled on `commit_wave`

Under the table's `commit_lock`:

1. **Flush-priority yield.** Rollup tables have no ingest flushes, so `flush_waiters("", table)` is
   always 0 and no yield is needed. State this in a comment instead of copying the branch.
2. `refresh_table_snapshot` once, bounded by `bounded_commit_await(COMMIT_LOCK_OP_TIMEOUT, …)` exactly as
   `commit_wave` does. Today's rollup path awaits it unbounded (L3120).
3. For each entry in FIFO order, validate it against a **working view** of its partition. The view
   starts as the refreshed live `(project,date)` Adds and absorbs each accepted entry's Remove/Add:
   - `journal.state(key) == Running`. Otherwise drop the entry: delete its parquet, clear its intent,
     outcome `Ok(true)`. This is today's L3090 check, moved to the latest point.
   - **Narrow partition revalidation** (the fix for `slice_occ_stale`). This replaces the byte-equality
     `partition_unchanged` check (L3126–L3138). Recompute on the working view what staging decided on
     `partition_before`:
     a. `replaced` is still exactly `slice_retires(view, publish')`, where `publish'.covered` is
        **recomputed from the view**, because `slice_retires` retires untagged files by `ranges_cover(covered, …)`
        (`rollup.rs` L800), so a sibling's new range can change the retire set. Every retired Add must also be
        byte-identical in the view (same fields as today: tags, partition values, stats, DV, size, mtime);
        **Nested exception:** the retire set may *grow* by tagged Adds of the same project whose slice is
        contained in this entry's slice. Those are exactly what this publication supersedes. It may not
        shrink, and no other change is allowed. When it grows, `target_paths` and the proof are re-recorded
        (step 4);
     b. no Add in the view satisfies `covering_slice_for` (the post-write safety net at L3043);
     c. if the entry is packed, `packed_groups(view)` equals the groups staged against.
     Adds that are new in the view but disjoint from the slice (a sibling hour of the same day) no longer
     invalidate the entry. That sibling commit is the case that produces `slice_occ_stale` today.
   - **Containment among pending entries first.** If entry W's slice contains pending entry N's slice
     (same project and partition), W would retire N's *staged* Add, and an Add and a Remove of one path in
     one commit is invalid. Drop N before validation: orphan its parquet, clear its intent, and complete
     it through the existing `settle_covered_by_wider` path once W is live, as if the pre-scan check had
     found W. Commit only the container. This mirrors today's outcome with less waste.
   - Target-disjointness within the batch. Two entries must not retire the same path (Delta action
     validity, same rule as `commit_wave`'s `claimed_targets`). This covers Remove/Remove. The previous
     bullet covers Add/Remove. The later entry is re-validated against the view that already contains the
     earlier one's Adds. If (a)–(c) still hold it stays, otherwise it is returned for retry.
   - Failure keeps today's consequence for **that entry only**: delete its staged parquet, clear its
     intent, `retry("slice_occ_stale", 1s)`. The rest of the batch still commits.
4. **Re-record proofs.** For every accepted entry, set `RollupTargetProof { before: fp(view₀), after:
   fp(view_final) }` over *its* partition, where `view₀` is the refreshed pre-batch partition. Rewrite
   the accepted entries' intent lines in **one** manifest rewrite. Generalize `clear_staged_intent` to
   `rewrite_staged_intents(drop: &[&str], upsert: &[StagedIntent])`, reusing the same read-filter-write.
   This happens under the commit lock, before the commit, so a crash at any later point leaves intents
   whose `after` matches what landed.
5. One `CommitBuilder` over all accepted entries' `Remove + Add`, lane `rollup_publish`, still
   `with_max_retries(0)`. The final-window rule reviewed in the plan is unchanged: an external commit
   after the refresh fails the whole batch. On a commit error, every entry keeps its intent (their
   parquet is resumable) and gets `retry("rollup_batch_commit_failed", backoff)`. Do **not** delete
   staged parquet on an ambiguous error, the same rule as `commit_wave`'s self-landed split. The next
   claim's `resume_rollup_unit` decides between `AlreadyLanded` and `Commit`.
6. `swap_and_refresh_cache` once, with every accepted date marker.
7. Drop the commit lock. Take `rollup_journal_lock` **once**. For each accepted entry, run today's L3167–L3252
   body unchanged: `Running` gate, `rollup_slice_coverage` insert, date-level `rollup_coverage`
   insert, `journal.publish`, `reopen_derived_over`, the empty-over-full warning, untagged retirement
   bookkeeping.
8. Release both locks, then call `checkpoint_task_journal()` **once**.
9. `clear_staged_intent(&accepted_wave_ids)` **once**. It already takes a slice.
10. Stats and per-entry `maintenance_rollup_published` logs, as today. Also add
    `rollup_shared_commits += 1` and log `batch_entries`.

Ordering invariants kept from today:

- Coverage is inserted **after** the Delta commit and **before** the checkpoint (L3163 comment).
- The intent is cleared **after** the checkpoint, never before (L3257 comment).
- The journal lock is never held across fsync (the checkpoint-sharing rule).
- A base publication reopens derived children in the same journal-lock hold that publishes it.

### 4.5 Crash and recovery (staged-intent interaction)

| Crash point | State on disk | Next boot | Change vs today |
| --- | --- | --- | --- |
| after stage, before enqueue or while pending | parquet staged; intent with the staging-time proof (step 4 of §1; the append is not fsynced, so it may be lost) | the task is `Running` in the journal, so `TaskLease` never dropped and boot reclaims it. `resume_rollup_unit` → `Commit` if `before` still matches, otherwise declines and rebuilds. A lost intent means orphan parquet, collected by `reconcile_staged_intents` / VACUUM | same as today; the window is up to `linger` longer. Without a crash, an invalidation during linger re-enqueues the task (no longer `Running`), and a second worker may claim and scan it while the first entry is still pending. The same race exists during today's scan; linger lengthens it. Step 3's `Running` gate drops the stale entry, so no double publication occurs |
| after the proof rewrite, before the Delta commit | intents carry `before = fp(view₀)`, `after = fp(view_final)` | the first sibling to resume matches `before` and commits **alone**; its partition then ≠ `before` for the other siblings, which decline (`rollup_resume_declined`) and rebuild | **new cost**: siblings of a same-partition batch lose resume. Rare (a sub-second window), counted, conservative |
| after the Delta commit, before the checkpoint | all Adds live; tasks still `Running` | each sibling's `AlreadyLanded` check requires `after == fp(current)`, which holds for every sibling because `after` is the batch-final fingerprint → `publish_resumed_rollup`. If an unrelated later commit touched the partition, resume declines and rebuilds (same as today) | no loss |
| after the checkpoint, before the intent clear | tasks `Complete`, intents present | `resume_rollup_unit` is never called for completed keys, and `reconcile_staged_intents` treats the Adds as live and referenced, so nothing is deleted | same as today |

Derived units: their journal records are not durable (`durable` filter). After a crash following the
commit, recovery uses Add tags alone, as today.

A future option, not v1: stamp a `batch` id on the intents so resume can re-commit all siblings together.
It is a serde-default field in a local JSONL, not the WAL, and older binaries ignore it. Add it only if
`rollup_resume_declined` after restarts shows sibling loss matters.

### 4.6 Cancellation and shutdown

- Shutdown (`maintenance_shutdown` cancelled): the drainer stops lingering and publishes what is pending
  once, bounded by `COMMIT_LOCK_OP_TIMEOUT`. If that fails or times out, entries keep their intents and
  `TaskLease::drop` runs its *shutdown* branch, which is an unstarted release with no attempt charged.
  The next boot resumes them.
- `COORDINATOR_LOOP_TIMEOUT` (`2 × MAX_OPERATION_DEADLINE_SECS`, `mod.rs` L1292) can drop a *follower*
  future. Its entry is still in `pending`, its lease still inside `StagedRollup`, and the drainer
  publishes it. The oneshot receiver is simply gone. If the *drainer's* future is dropped mid-commit,
  the `leader` mutex guard releases, remaining entries stay in `pending`, and the next arriving worker
  (or a periodic sweep in the coordinator loop) drains them. Any in-flight Delta commit is
  ambiguous and is handled by resume (§4.5). Hold the drained `Vec` in a guard whose `Drop` pushes
  unpublished entries back, so a dropped drainer strands nothing.
- An invalidation while an entry is pending re-enqueues the task, so it is no longer `Running`. Step 3
  drops the entry before any commit, which saves a useless commit that today's path would make (today
  the gate is at L3090, before the lock wait).

### 4.7 Packed publication manifest: defer

`staged_intent.jsonl` already embeds each unit's Adds verbatim, and Delta Add tags already carry all
per-file coverage identity. A batch of 32 units adds about 64 actions to one commit JSON. That is well
within normal Delta commit sizes, so there is no metadata-size problem to solve. Define the trigger
instead: build a packed manifest (an immutable object referenced from `commitInfo`) only if measured
`rollup_commit_actions_total / rollup_shared_commits_total` exceeds about 1 000 actions per commit or commit JSON
size shows up in commit latency. Until then it would add an object PUT per commit for no saving.

### 4.8 Interaction with the other stages

- Stage 0 (`W5`, per-range witness): independent. Coverage inserts are unchanged per entry.
- Stage 4 (flush-time aggregation) **requires** this queue with its byte cap. The inline fallback plus
  claim-decline is the "queue full → defer" behaviour Stage 4 relies on.
- Stage 1B/1C: shared scans produce several `StagedRollup`s at once. They enqueue together and land in one
  commit per table, but still as separate publication identities.
- Logical-evidence activation stays gated on Stage 2 (plan). Nothing here reads logical evidence.

## 5. Why flush and insert acknowledgement cannot wait on this

- Delta: rollup commits hold `commit_lock(<rollup table>)`. Flush and ingest hold the source table's
  lock (`write.rs` L681/L890). They are disjoint mutexes.
- Journal: the insert-ack path (`insert_records_batch_bounded` → `invalidate_rollup_batches` →
  `commit_journal`, `write.rs` L589, `maintain.rs` L4152) rides `task_journal_group_commit` and the
  journal mutex. That mutex is held across the fsync (`TaskJournal::checkpoint`, L2435). A publication
  checkpoint is one more leader turn an insert may queue behind (`journal_commit_wait` max 6.6 s). But
  publications are <1 % of checkpoints (§2.1), so batching them is a **no-regression requirement,
  not a saving**. The requirement: the drainer adds exactly one checkpoint per batch and never holds a
  journal lock across the Delta commit.
- The drainer never takes `rollup_journal_lock` while waiting for the commit lock, and never holds it
  across fsync (§4.4 step 7–8).

## 6. Metrics the experiment reads

Existing counters (all `timefusion_stats`, component `maintenance` unless noted):

| Metric | Reads as |
| --- | --- |
| `rollup_staged_projects_total` | published units (1 per unit) |
| `rollup_shared_commits_total` | **revived**: Delta commits made by `publish_rollup_batch` (batch of 1 included). Units/commit = Δstaged / Δshared |
| `rollup_commit_actions_total`, `rollup_output_files_total` | actions and files; actions/commit = Δactions / Δshared |
| `rollup_commit_duration_ms_total` | keep per-unit semantics (enqueue → own intent cleared), which now includes linger. Report separately: batch commit latency from the `rollup_batch_published` log (`lock_wait_ms, refresh_ms, delta_commit_ms, checkpoint_ms, manifest_ms, entries`) |
| `retry.BaseRollup.slice_occ_stale`, `retry.DerivedRollup.slice_occ_stale` | **the primary target**: must fall per published unit |
| `rollup_resume_declined_total`, `rollup_resume_already_landed_total`, `rollup_resumed_total` | crash-path behaviour after restarts |
| `task_journal_checkpoints(_coalesced)`, `journal_commits(_coalesced)` | fsync pressure |
| block `journal_commit_wait.*`, `journal_lock_wait.*`, `journal_hold.*` | insert-ack contention |
| `flush_stalled_total`, `buffered_layer.flush_*` | must not move |
| `eligible_base_rollup`, `pending_*_rollup`, `coordinator_pool_pct` | job-slot starvation from linger |
| `rollup_scan_duration_ms_total` Δ / Δstaged | scan work per published unit, which must fall as stale rescans disappear |

New permanent stat, the only one: `rollup_publication_pending_bytes` (gauge, Σ staged `add.size` in all
queues). The gate: it stays below `timefusion_rollup_publish_max_bytes`, and reaching it produces
inline publications and declined claims, never lost discovery. The plan's second key,
`rollup_recovery_suffix_bytes`, belongs to the checkpoint/suffix work, which this design does not touch.

## 7. Tests (failing-first)

All real Delta on a tempdir or MinIO. There are no mocks. They live next to the existing publication and
recovery fixtures in `src/database/maintain.rs` tests (e.g. the shared-checkpoint fixture around
L11060) and `src/database/tests.rs`. Prefer one `test_case` table where rows differ only by the
interleaving.

| # | Name | Red on today's code because |
| --- | --- | --- |
| 1 | `sibling_slices_of_one_partition_both_publish_without_rescan` | two units stage against version v in the same `(project,date)`. Unit A commits, then unit B's `partition_unchanged` is false → `slice_occ_stale`, staged parquet deleted. Assert B lands with `rollup_scan_cohorts` unchanged (no second scan) |
| 2 | `ready_units_of_one_table_share_one_commit` | two units in different partitions of one tier → assert the table version advances by 1, both coverage entries exist, `rollup_shared_commits` +1, one task-journal checkpoint, `staged_intents()` empty. Today: +2 versions, 0 shared |
| 3 | `batch_drops_only_the_unit_whose_partition_changed` (case table: overlapping external publication / generation-tag change on same path / wider covering file / DV change on a retired file) | the existing per-unit guards, re-expressed per entry: the stale entry retries, the others commit. Guards against the narrowed check being too narrow. Must fail if (a), (b) or (c) of §4.4 is removed |
| 3b | `a_contained_narrower_publication_does_not_stale_the_wider_unit` | 24 h unit stages, a 10-min unit inside it commits, the 24 h unit must still land and retire the 10-min file. Red today (`slice_occ_stale`), and red under rule (a) without the nested exception |
| 4 | `two_entries_retiring_one_file_do_not_double_remove` + `a_pending_contained_slice_yields_to_its_container` (case table) | Remove/Remove and Add/Remove validity inside a batch. The contained entry completes via `settle_covered_by_wider`, and the tier ends with exactly the container's files |
| 5 | `a_crash_after_the_batch_commit_recovers_every_sibling` | kill between Delta commit and checkpoint, reload → every sibling `AlreadyLanded`, coverage restored, no rebuild. Red if the proof rewrite (step 4) is skipped, because `after` would be the single-unit fingerprint |
| 6 | `a_crash_before_the_batch_commit_resumes_one_sibling_and_declines_the_rest` | documents the accepted cost (§4.5 row 2): exactly one `rollup_resumed`, others `rollup_resume_declined`, final tier correct (no double count) |
| 7 | `an_invalidated_pending_unit_is_dropped_before_commit` | apply_rollup_hours while pending → entry dropped, parquet deleted, the others commit, version advances only for them |
| 8 | `a_full_queue_publishes_inline_and_declines_claims` | `max_bytes` tiny → inline batch-of-one, `claim_coordinator_task` returns None for rollup ops, `rollup_publication_pending_bytes` returns to 0 |
| 9 | `linger_or_entry_cap_forces_the_drain` | virtual clock (`support` clock) → commit at the age limit with fewer than the cap; at the cap with no linger |
| 10 | `shutdown_during_linger_publishes_or_releases_without_charging` | cancel → either published or intents kept and tasks unstarted-released (attempts unchanged) |
| 11 | `a_dropped_drainer_strands_nothing` | drop the drainer future mid-drain → next enqueue (or sweep) publishes the leftovers |
| 12 | `insert_ack_never_waits_on_a_rollup_commit` | hold a rollup table's commit lock and the batch drainer mid-commit; an insert with rollup invalidation must return within a bound. Green today, as a regression guard. Say so in the test, because it cannot be shown red against today's code |
| 13 | `max_entries_one_is_todays_behaviour` | kill switch equivalence: same versions, tags and journal records as the pre-change path on test 2's fixture |

Guards 1, 3b, 2, 5 and 7 must be shown red with the fix reverted, per the repo's bug-fix rule. Test 12
cannot be red today. Its comment must say so.

## 8. Estimated saving

All provisional (§2 caveats). "Now" = s1→s3 rate. The burst rate is shown where it differs.

| Quantity | Now | Batched, linger 10 s | linger 30 s | linger 60 s |
| --- | --- | --- | --- | --- |
| Rollup Delta commits/day | ≈ 10.8 k (burst ≈ 19.6 k) | ≈ 6.1 k (burst ≈ 6.5 k) | ≈ 3.8 k (≈ 3.9 k) | ≈ 2.7 k (≈ 2.8 k) |
| Object requests saved/day (≈ 2–3 per commit: log refresh LIST/GET + commit PUT) | — | ≈ 9–14 k | ≈ 14–21 k | ≈ 16–24 k |
| Delta commit floor, worker-time/day (0.1–0.4 s × commits) | 0.3–1.2 h | 0.2–0.7 h | 0.1–0.4 h | 0.1–0.3 h |
| Task-journal checkpoints from publication/day | ≈ 10.8 k of ≈ 1.3 M | ≈ 6.1 k | ≈ 3.8 k | ≈ 2.7 k |
| Added per-unit latency | — | ≤ 10 s + one commit | ≤ 30 s | ≤ 60 s |
| `slice_occ_stale` rescans/day | ≈ 2.4 k (0.22/publication) | → ≈ 0 for sibling- and nested-caused stales (§9 step 1 alone removes them) | same | same |
| Scan time recovered | — | ≤ ≈ 15 worker-hours/day (≤ 0.6 workers, ≤ 3 % of BaseRollup unit time); ≤ 1.1 workers in a post-deploy burst. Measured from `ran_secs` of Retry outcomes, not assumed | same | same |

Reading this table:

- **The commit-count saving is real but small in wall-clock terms.** At a 0.1–0.4 s Delta floor,
  10.8 k commits/day is 0.3–1.2 worker-hours/day. Batching saves roughly half to three quarters of that,
  plus 9–24 k object requests/day and proportionally less `_delta_log` growth and replay at boot. The
  per-unit `rollup_commit_duration_ms` is dominated by journal-mutex waits that batching barely touches
  (publication is <1 % of checkpoints).
- **The rescan saving is about an order of magnitude larger than the commit-time saving** (≤15 vs
  ≤1 worker-hour/day). It does not need the queue, only the narrowed revalidation (§9 step 1).
  Both are small next to the ≈18 workers continuously scanning for rollups (Δscan 11 231 s / 625 s).
  Stage 3 does not change rollup CPU materially. Stages 1A/1B/1D do.
- **Therefore:** ship §9 step 1 now (cheap, removes wasted work, and its cause log tests the attribution).
  Build steps 2–3 when Stage 4 is approved, because Stage 4 needs the bounded queue, or when publications/day
  grow enough that commit and object-request counts matter (e.g. flush-time aggregation would multiply
  them by the number of eligible specs per flush).
- Recommended linger for the first experiment: **10 s** (≤7 % of a mean unit's wall time, most of the
  achievable coalescing on a steady load). Move to 30 s only if `rollup_shared_commits` shows
  ≤ 1.5 units per commit on a ≥1 h process.
- Acceptance (matched windows, ≥1 h uptime, ≥3 samples): Δ`slice_occ_stale`/Δ`rollup_staged_projects`
  falls ≥ 80 %. Δ`rollup_scan_duration_ms`/Δ`rollup_staged_projects` does not rise. Δstaged/Δshared ≥ 1.5.
  `flush_stalled_total`, insert p99 and `journal_commit_wait.avg_us` do not regress beyond 5 %.
  `rollup_publication_pending_bytes` stays below its cap.

## 9. Implementation order (one lane, 3 PRs, each shippable; steps 2–3 gated per §8)

1. **Narrow partition revalidation only** (§4.4 step 3a–c, including the nested exception) inside today's single-unit path, with tests 1,
   3 and 3b, plus the stale-cause log fields (§2.3) and `scan_ms` on the `slice_occ_stale` path. In the
   single-unit path, the intent's `RollupTargetProof` must be recomputed and re-recorded under the lock
   whenever the partition moved (§4.4 step 4 with a batch of one). Otherwise a crash after commit declines
   `AlreadyLanded`. This alone should remove the sibling-caused `slice_occ_stale` rescans. It is the
   smallest change with the largest measured effect, and it needs no queue. Ship and measure it first.
   If the cause log shows that stales are *not* sibling commits, §3 goal 1 must be re-planned before
   step 3.
2. **`StagedRollup` split + `publish_rollup_batch` with `max_entries = 1`** (pure refactor, test 13),
   and revive `rollup_shared_commits`.
3. **The queue** (§4.2–4.3, 4.5–4.6), `rollup_publication_pending_bytes`, tests 2, 4–12. Ship it with
   `linger` configured, measure against the step-2 baseline on a ≥1 h process, alternating arms if
   possible.
