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
  recovery on startup is idempotent.

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
requests may see connection resets. Set `terminationGracePeriodSeconds`
in your k8s pod spec to **at least** `TIMEFUSION_SHUTDOWN_TIMEOUT_SECS
+ flush_overhead` (rough rule: 60s default is enough for ≤4GB
MemBuffer).

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
3. **DynamoDB lock contention** (if `aws_s3_locking_provider=dynamodb`) —
   check DynamoDB throttling metrics.

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
