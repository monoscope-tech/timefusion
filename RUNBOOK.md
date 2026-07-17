# TimeFusion Operations Runbook

Production playbook. Pair this with the alerting recipe in your OTel
backend and the architecture overview in `CLAUDE.md`.

---

## Required configuration

These env vars must be set or startup fails. Set them in your deployment
manifest, never commit them.

| Var | Purpose |
|---|---|
| `PGWIRE_PASSWORD` | SQL endpoint password. Empty/unset rejects startup. |
| `GRPC_TOKEN` | Bearer token for gRPC ingest (`Authorization: Bearer <token>`). |
| `AWS_S3_BUCKET` | Delta + tantivy sidecar storage. |
| `AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | S3 credentials. |
| `AWS_S3_ENDPOINT` | Required for non-AWS S3 (R2, MinIO, etc.). |

**Local dev escape hatch**: set `TIMEFUSION_ALLOW_INSECURE_AUTH=true` to
skip the password/token checks. Logs a loud warning. Never in production.

Most other knobs auto-tune from host RAM/disk/CPU at startup —
see `src/autotune.rs`. The startup log line `Auto-tune applied: ...`
prints what was derived.

---

## Liveness & readiness probes

- **Cold start** is dominated by WAL replay. Replay time is `O(retention
  × throughput)`. With default 70-min retention and moderate ingest,
  expect 10–30s before the SQL endpoint accepts queries.
- **Readiness probe**: set to `tcp_check pgwire_port` with `initial-delay
  ≥ expected_wal_replay_seconds`. Kubernetes default 30s is usually
  sufficient; bump to 60-120s for high-volume tenants.
- **Liveness probe**: keep a longer timeout than readiness. WAL fsync
  pauses (with `wal_fsync_mode=sync_each`) can briefly delay the event
  loop; don't kill the pod for a 200ms blip.

---

## Durability & backup

**What's durable**:
- Delta tables on S3 — the authoritative store. Use S3 versioning + lifecycle
  rules for retention. Tantivy sidecar indexes are derivable; if they're
  lost, queries fall back to full scan (correctness preserved).
- WAL on local disk — durable up to the fsync cadence (default 200ms).
  Lost on host failure unless you ship segments to durable storage.

**What's NOT durable**:
- MemBuffer rows that haven't flushed AND haven't been fsync'd to WAL
  yet. Default loss window: ≤200ms on hard crash.

**Backup strategy**:
- Enable S3 versioning on `AWS_S3_BUCKET`. Delta time-travel + bucket
  versioning give point-in-time recovery without an explicit backup job.
- Retain WAL segments off-host (S3 upload as a background task) if your
  durability SLA can't tolerate the fsync window. Not implemented today
  — track the work item.

**Restore**:
- New TimeFusion instance pointed at the same `AWS_S3_BUCKET` +
  `TIMEFUSION_TABLE_PREFIX` will replay Delta on the next query. No
  explicit restore action.
- If WAL on the dead host is recoverable, copy it to
  `${TIMEFUSION_DATA_DIR}/wal/` on the new host before startup. WAL
  recovery on startup is idempotent. Only ever run **one** instance
  against a given WAL directory — the startup `flock` (see *WAL ownership
  & rolling deploys*) enforces this; do not bypass it by pointing a second
  live process at the same dir.

---

## Graceful shutdown

The process handles SIGINT (`Ctrl-C`) and SIGTERM (k8s rolling restart).
Drain sequence:

1. gRPC server stops accepting new connections; existing streams drain
   up to `TIMEFUSION_SHUTDOWN_TIMEOUT_SECS` (default 5s, plus 1s per
   100MB of buffered data).
2. BufferedWriteLayer flushes remaining buckets to Delta.
3. Database shuts down — releases foyer cache, log store handles.

If drain exceeds the timeout, the process exits anyway. In-flight
requests may see connection resets. `TIMEFUSION_STOP_GRACE_SECS` defaults
to 70 seconds; keep it below the orchestrator's 90-second `StopGracePeriod`
so the cursor snapshot and lock release finish before SIGKILL.

---

## WAL ownership & rolling deploys

The WAL is **single-writer** — two processes on the same WAL directory
at once will fork it and silently lose the older one's acked-but-unreplayed
writes (see `docs/WAL.md` → *Single-Writer Invariant & Directory Lock*).
Startup enforces this with an exclusive `flock` on
`${TIMEFUSION_DATA_DIR}/wal/.timefusion_meta/wal.lock`, held for the
process lifetime.

- **Symptom of contention**: a starting instance logs `WAL dir ... is
  locked by another TimeFusion process; waiting for it to exit before
  recovery` every ~10s. This is expected during a deploy handoff — the new
  instance is waiting for the old one to finish draining and exit. It
  serves `57P03` ("starting up") meanwhile, which libpq/Hasql retry.
- **No stale lock to clear**: the lock is tied to the open fd, so the
  kernel releases it on any exit including SIGKILL/OOM. If a starting
  instance waits indefinitely, the *previous* process is genuinely still
  alive — find and stop it (`docker ps` on the host); never delete
  `wal.lock` to "unstick" it (deleting it while a holder is live breaks
  the mutual exclusion and re-enables the fork).
- **Keep the readiness probe a `tcp_check`** on the pgwire port (see
  *Liveness & readiness probes* above). Do **not** switch it to a deep
  check that only passes after WAL recovery — that would deadlock a
  start-first deploy (new can't pass readiness until it holds the lock;
  the orchestrator won't stop old until new is ready). With the TCP probe,
  start-first self-resolves.
- **Recommended (hardening, not required)**: deploy **stop-first with a
  single replica** so the old instance fully drains + flushes + writes its
  clean-shutdown snapshot before releasing the lock, minimizing the new
  instance's replay. Ensure the orchestrator stop-grace (e.g. Swarm
  `StopGracePeriod`) ≥ `TIMEFUSION_SHUTDOWN_TIMEOUT_SECS`.

---

## GitHub deployment verification

The deployment workflow records the current `buffered_layer.boot_micros`, deploys
one replacement, then waits for a *different* boot ID with
`wal.recovery_complete=true`. This avoids treating a routable pgwire listener
or an old Swarm task as proof that the replacement has finished WAL recovery.

The workflow waits up to 12 minutes for recovery and rejects a completed replay
over 10 minutes. Inspect the replacement with:

```sql
SELECT component, key, value
FROM timefusion_stats
WHERE (component = 'buffered_layer' AND key = 'boot_micros')
   OR (component = 'wal' AND key IN ('recovery_complete', 'recovery_duration_ms'));
```

If recovery exceeds the budget, investigate WAL replay volume and flush health;
do not reduce the verification to `SELECT 1` or bypass the WAL directory lock.

---

## Schema migrations

Schemas are YAML files under `schemas/`, compiled into the binary via
`include_dir!`. To add or modify a column:

1. Edit the YAML file (`schemas/otel_logs_and_spans.yaml` etc.).
2. Rebuild the binary (`cargo build --release` or `make build-prod`).
3. Deploy. Restart picks up the new schema.

**Adding a nullable column** is safe — existing Delta files don't have
it; reads project NULL. No backfill needed.

**Adding a NOT NULL column** breaks all existing Delta reads. Don't.

**Removing a column** is safe at the schema level but be aware that
existing parquet files still contain it; storage cost stays until
compaction rewrites them.

**Adding `tantivy: indexed: true`** to a field starts indexing on the
next flush. Historical data isn't backfilled — queries against old
partitions fall back to UDF substring scan until the bucket flushes
again (or you re-ingest).

**Removing a tantivy field**: index entries for that field stay in
existing index blobs until the corresponding Delta files are compacted
and the tantivy GC drops the dead entries.

---

## WAL format upgrades

The WAL header carries a version byte. When the binary's `WAL_VERSION`
changes (currently `131`), entries written by an older build are
unreadable by the new build — recovery emits an `error!` log
("WAL on-disk version mismatch on shard … IN-FLIGHT DATA WILL BE LOST"),
the entries are skipped, and any rows that hadn't yet flushed to Delta
are dropped on the floor.

Procedure for a WAL-incompatible upgrade:

1. **Drain.** Stop ingest at the load balancer / client side.
2. **Wait for flush.** Watch `oldest_bucket_age_seconds` and the WAL
   directory size. Once buckets stop turning over and pending entries
   reach 0, all in-flight writes are durable in Delta.
3. **Stop the service** (graceful shutdown — `SIGTERM`).
4. **Back up the WAL directory** (`${TIMEFUSION_DATA_DIR}/wal`) — small,
   cheap insurance against a botched migration.
5. **Wipe the WAL directory.** `rm -rf ${TIMEFUSION_DATA_DIR}/wal/*`.
6. **Deploy the new binary and restart.** Recovery scans an empty
   directory cleanly; first writes start a fresh WAL at the new version.

Rolling back from a newer version to an older one needs the inverse:
the old binary can't read the new format either. Always back up before
upgrading.

---

## Monitoring

All metrics export via OTel to `OTEL_EXPORTER_OTLP_ENDPOINT`. Page-level
and warn-level alert thresholds are in
`~/.claude/projects/...memory/alerting_recipe.md`. Critical metrics:

- `timefusion.mem_buffer.oldest_bucket_age_seconds` — staleness signal.
  Alert at `> 2× flush_interval_secs`.
- `timefusion.wal.corruption_events` — alert on any rate > 0.
- `timefusion.tantivy.build_failures` — alert on rate > 0; sustained
  failures = silent index drift = slow text-match queries.
- `timefusion.tantivy.index_lag_seconds` — gauge of how far behind
  ingest the newest published sidecar index is.
- `timefusion.ingest.{inserts,rows,errors}` — labeled by `project_id`
  and `table_name`. Use for per-tenant SLA breakdown.

For operator inspection, every counter/gauge is also queryable via SQL:
`SELECT * FROM timefusion_stats`.

---

## Disk capacity

WAL, Foyer cache, and the data dir all live under
`TIMEFUSION_DATA_DIR`. Plan capacity:

- **WAL**: bounded by retention window × ingest rate. With default
  70-min retention and 10k rows/s × 1KB/row ≈ 42 GB worst case. Set up
  a disk-usage alert at 70% of the volume.
- **Foyer disk cache**: auto-tuned to 40% of free disk at startup (capped
  500GB). Will stay within budget — but if disk fills from other sources,
  Foyer can't evict fast enough; writes may slow.
- **Quarantine** (`${data_dir}/wal/quarantine/`): bounded by corruption
  rate. Typically empty. If non-empty, investigate (corrupt WAL entries
  ≠ ok).

---

## Common incidents

### Flush stuck (oldest_bucket_age_seconds growing)

Symptoms: pressure_pct climbs, eventually inserts start failing with
"memory budget exceeded".

Likely causes:
1. **S3 connectivity** — check `aws s3 ls` to your bucket from the host.
2. **Delta callback panic** — check process logs for "Failed to flush
   bucket". May indicate a schema mismatch.

Mitigation: if S3 is recovered, flushes resume on next interval. If
not, drain the host: `kubectl drain --grace-period=$LONG` so the
buffered layer has time to flush; otherwise data in MemBuffer is lost
on SIGKILL.

### Query latency spike with text predicates

Symptoms: queries with `LIKE '%...%'` or `level = '...'` are slow.

Likely causes:
1. **`tantivy.build_failures` rate > 0** — index drift. Recent flushes
   didn't produce indexes; queries fall back to UDF scan.
2. **`tantivy.index_lag_seconds` large** — flush is delayed (see above).
3. **`tantivy.prefilter_skipped` rate high with `low_selectivity`
   reason** — the query matches too much of the corpus; prefilter is
   bypassed by design. User query needs to be more specific.

Mitigation for #1: tantivy indexes rebuild on every flush. A transient
S3 error self-heals once S3 recovers.

### WAL corruption alert

A single corruption event is rare but possible (disk error, partial
write before fsync). Behavior:

- The corrupt entry is **quarantined** to `${data_dir}/wal/quarantine/`
  with a `.bin` (raw bytes) + `.meta` (context). File mode 0600.
- Recovery continues with subsequent entries.
- `wal_corruption_threshold` (default 10) caps tolerance — exceeding it
  fails recovery hard and the process exits.

Mitigation: copy quarantine files off-host for forensic analysis,
then delete (they're not source-of-truth). Set
`TIMEFUSION_WAL_CORRUPTION_THRESHOLD=1` if you want fail-fast.

### Stale cursor snapshot

The fast-boot path restores `<wal>/.timefusion_meta/cursor_snapshot.json`
to skip the per-table Delta scan. The snapshot is rewritten after every
flush cycle (`clean_shutdown=false`) and on graceful shutdown
(`clean_shutdown=true`). When a write fails (disk full, perms, etc.)
the post-flush helper deletes the now-stale file so the next boot
reconciles against Delta.

Failure mode this runbook addresses: both the write **and** the delete
fail (e.g. `meta_dir` became read-only). The pre-existing snapshot then
survives indefinitely. On the next boot it seeds cursors, then the
Delta verifier runs with `TIMEFUSION_DELTA_SCAN_DEPTH` (default 8). If
more than 8 commits accumulated since the last good write, the cursor
is left behind — Delta will be re-scanned on those commits at the next
flush ("at-least-once").

Signals to watch:
- repeated `warn!` lines like `write_cursor_snapshot (post-flush) failed`
  or `delete stale cursor snapshot also failed`.
- on boot, `Cursor snapshot restored … age=<large>h` — a 24h+ age also
  fires its own warn.

Recovery:
```sh
# inside the container
rm /app/data/timefusion/wal/.timefusion_meta/cursor_snapshot.json
```
Forces the next boot to run the full Delta verifier. Optionally raise
`TIMEFUSION_DELTA_SCAN_DEPTH` (env var, hot-restart) if you suspect
many commits to recover. Fix the underlying perms/disk issue separately.

### Cold-start taking > 5 minutes

WAL replay is taking too long. Causes:

1. **WAL accumulated past retention** — flush task was stuck before
   shutdown. Replay processes everything; eventually catches up.
2. **MemBuffer bound too small** — replay can't fit, fails partway.
   Bump `TIMEFUSION_BUFFER_MAX_MEMORY_MB` or accept partial replay.

Mitigation: increase the k8s readiness probe `initial-delay` and
shutdown grace period accordingly.

---

## Reference

- Architecture overview: `CLAUDE.md`
- Source-of-truth for env vars: `src/config.rs`
- Auto-tune logic: `src/autotune.rs`
- Schemas: `schemas/*.yaml`
- Alerting recipe: stored in personal memory at
  `~/.claude/projects/.../memory/alerting_recipe.md`
