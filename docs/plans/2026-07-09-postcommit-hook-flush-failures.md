# Post-commit hook failures: stop R2 500s from failing flushes and deleting committed parquet

**Incident (2026-07-09):** R2 returns intermittent 500s on `POST ?delete` (bulk delete)
and `PUT _delta_log/_last_checkpoint`. Both run in delta-rs **post-commit hooks** —
after `N.json` is durably written — but `CommitBuilder::build().await` surfaces them
as a commit error. TF's staged-flush error arm (`database.rs:3396`) treats any
non-OCC error as "commit never landed": it calls `cleanup_orphaned_parquet`
(deleting the parquet the landed commit references → dangling `Add`s; 14 confirmed
in prod, 7 projects) and leaves the bucket in MemBuffer (→ 9GB sawtooth →
backpressure rejects). Root causes:

1. Flush commits use `CommitProperties::default()`, whose delta-rs default is
   `create_checkpoint: true`; plus table property `enableExpiredLogCleanup=true`
   makes `cleanup_expired_logs_for` run on **every** commit. At ~1.5s/commit and a
   checkpoint every 10 versions, ingest availability is coupled to R2's flakiest
   endpoints.
2. TF cannot distinguish "commit failed" from "commit landed, hook failed".

Same bug on the dedup-rewrite arm (`database.rs:4641`) — worse there, because the
landed commit `Remove`s the old files and TF deletes the *new* ones → real data loss.

## Phase 1 — Take checkpoint/log-cleanup off the ingest commit path

- Add one helper next to `build_watermark_commit_properties` (database.rs:575):

  ```rust
  fn base_commit_properties() -> CommitProperties {
      // Post-commit checkpoint/log-cleanup run out-of-band (maintenance
      // scheduler); a hook failure here would fail a commit that already landed.
      CommitProperties::default().with_create_checkpoint(false).with_cleanup_expired_logs(Some(false))
  }
  ```

- Route `build_watermark_commit_properties` and `incremental_commit_properties`
  (database.rs:589) through it. That covers every ingest/maintenance commit site:
  staged flush (3369), schema-merge WriteBuilder (3440), DML/optimize ops
  (3754, 3975, 4153, 4873), dedup rewrite (4615).
- Leave the table-create commit (2579/2608) as-is: it runs once, and checkpoint-at-v0
  is harmless.
- Keep the `delta.checkpointInterval` / `enableExpiredLogCleanup` table properties —
  `Some(false)` overrides the property per-commit (fork `transaction/mod.rs:1053`),
  and the out-of-band task (Phase 2) reuses the property values as its policy.

**Verify:** e2e test — failpoint 500s on `_last_checkpoint` PUT and `_delta_log/`
deletes; `force_flush` must succeed, bucket drained, every active `Add` object
present, row count exact.

## Phase 2 — Out-of-band checkpoint + log cleanup in the maintenance scheduler

Nothing creates checkpoints after Phase 1; without this phase the log regrows
unboundedly (2026-06-25: 68k log objects → 35s commits). Ship in the same commit.

- New task in `start_maintenance_schedulers` (database.rs:1597), cron-configured
  like its siblings: `timefusion_checkpoint_schedule` (default every 2 min).
  Per registered table:
  1. `latest_version - last_checkpointed_version >= delta.checkpointInterval`
     → `create_checkpoint` (delta-rs public checkpoint API; confirm the fork
     exports `create_checkpoint_for`/`cleanup_expired_logs_for` — fall back to
     `deltalake::checkpoints::*`).
  2. Then `cleanup_expired_logs_for` with the table's `logRetentionDuration` cutoff.
  - Every step: failure → `warn!` + metric, retry next tick. Never touches ingest.
- Stats/metrics in `timefusion_stats`: `maintenance.checkpoint_failed`,
  `maintenance.log_cleanup_failed`, `maintenance.checkpoint_lag_versions`
  (alert if lag ≫ interval — the regression guard for "oob task silently broken").

**Verify:** e2e — commit ≥interval versions with hooks off, run the task: checkpoint
parquet + `_last_checkpoint` written, expired `N.json` pruned. Arm failpoint on the
checkpoint PUT: task warns, ingest unaffected, succeeds next tick.

## Phase 3 — Distinguish "commit landed" from "commit failed" before deleting parquet

Defense in depth: even with hooks off, the post-commit `snapshot.update()` can fail
on a transient S3 read and report a landed commit as failed.

- Helper on the error arm (shared by staged flush + dedup):

  ```rust
  /// After a non-OCC commit error, refresh the snapshot and check whether every
  /// one of our Add paths is active — i.e. the commit actually landed and only
  /// post-commit work failed. Returns the refreshed table on success.
  async fn probe_commit_landed(...) -> Option<DeltaTable>
  ```

- Staged flush (3396–3405): non-OCC error → probe.
  - Landed → proceed exactly like the `Ok` arm (`record_committed_write`, return
    added URIs) so the bucket drains and WAL holds release. Log `warn!` with the
    hook error.
  - Not landed → `cleanup_orphaned_parquet` as today.
  - Probe itself errored (S3 down) → **do not delete**; log paths and return `Err`.
    An orphan leak (reclaimed by Phase 4's inverse or manual cleanup) beats a
    dangling `Add`.
- Dedup rewrite (4638–4643): same probe; landed → treat as success.
- Schema-merge WriteBuilder arm (3452–3468): no file deletion happens there, but a
  landed-marked-failed flush re-flushes the bucket → **duplicates** (originals not
  deleted). Apply the same probe → treat landed as success.

**Verify:** integration test driving `insert_batch` with checkpoint re-enabled via
test-only commit properties + failpoint on `_last_checkpoint`: flush returns Ok,
staged parquet still present, no re-flush duplicates. Dedup variant: failpoint during
dedup post-commit → new files not deleted, no active-missing.

## Phase 4 — Reconcile task: repair dangling `Add`s (prod remediation + self-heal)

The 14 dangling entries already in prod break every scan touching those partitions
(rows themselves were re-flushed from MemBuffer, so `Remove` is lossless). A manual
out-of-process commit would race the 1.5s commit cadence and the DynamoDB lock, so
run the repair *inside* TF's commit machinery:

- Maintenance task `reconcile_dangling_adds` (cron, default hourly; also worth a
  first run shortly after boot): per table, HEAD every active `Add` object
  (~3.5k HEADs, concurrency ~16) and commit `Remove` actions for missing ones via
  the normal commit-lock/OCC path. Guard: only files whose `modificationTime` is
  >15 min old (staged files always exist before commit; guard is belt-and-braces).
- `warn!` loudly + metric `reconcile.dangling_removed` — a nonzero count means a
  bug elsewhere destroyed committed data.

First run after deploy remediates the incident. Confirm afterwards by re-running the
checkpoint↔R2 HEAD reconciliation and querying the 7 affected projects over
2026-07-08/09 via pgwire.

> **Addendum 2026-07-09 ~02:52 UTC:** the 14 dangling Adds were already
> remediated out-of-band via python-deltalake `repair()` (FSCK) — same op the
> reconcile task wraps — and the affected queries/charts healed once TF's
> cached snapshot refreshed. Expect `reconcile.dangling_removed = 0` (not 14)
> on the first post-deploy run; nonzero would mean NEW damage.

## Test infrastructure

Failpoint object-store wrapper (test-support, `#[cfg(any(test, feature = "e2e"))]`),
wired where `database.rs` builds the table store: wraps `ObjectStore`, global
registry of matchers `(operation, path-substring) → fail N times with Generic 500`.
The e2e harness (real MinIO, `bootstrap()`) arms it per test. Test names per
bug-fix workflow:

- `flush_survives_post_commit_hook_500` (Phase 1 — write failing first)
- `landed_commit_hook_failure_drains_bucket_without_deleting_parquet` (Phase 3)
- `dedup_hook_failure_keeps_rewritten_files` (Phase 3)
- `oob_checkpoint_prunes_log_and_tolerates_500` (Phase 2)
- `reconcile_removes_dangling_adds` (Phase 4)

## Rollout / verification in prod

1. Failing tests → fix → full suite (`cargo test`, e2e via native minio +
   `--features e2e`).
2. Deploy. Watch: `staged commit failed` disappears; MemBuffer bytes steady (no
   9GB sawtooth); `pgwire` rejects = 0; `maintenance.checkpoint_lag_versions`
   small; `_delta_log` object count bounded; `reconcile.dangling_removed` = 14
   once, then 0.

## Risks / decisions

- **Checkpoint cadence now depends on the oob task.** Mitigated by the lag metric +
  alert; failure mode is gradual (slow commits), not data loss.
- **Fork alternative rejected for now:** making hook errors non-fatal in the
  delta-rs fork (return `FinalizedCommit` + warn) is cleaner but drifts the fork
  further from upstream; the TF-side probe achieves the same with the fork
  untouched. Revisit if upstream adopts it.
- Defaults chosen (checkpoint every 2 min, reconcile hourly) are conservative;
  both are cron-configurable.
