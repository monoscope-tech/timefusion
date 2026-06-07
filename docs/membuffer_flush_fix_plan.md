# MemBuffer + Flush + Scan Fix Plan

## Incident summary (2026-06-07)

Production TimeFusion (`timefusion.s.past3.tech`) is serving the monoscope log-explorer at unusable latency:

| Query | Last 1h | Last 24h |
|---|---|---|
| List `ORDER BY timestamp DESC LIMIT 251` | 1.0 s | **487 s** |
| Histogram `time_bucket + count(*)` | **164 s** | 146 s |

`SELECT * FROM timefusion_stats` at the time:

| Component | Key | Value |
|---|---|---|
| mem_buffer | total_rows | 612,146 |
| mem_buffer | total_batches | 9,341 (→ **65 rows/batch**) |
| mem_buffer | estimated_bytes | 5.5 GB (→ **~9 KB/row**) |
| mem_buffer | oldest_bucket_age_secs | 4,568 (**>** retention 4,200) |
| buffered_layer | pressure_pct | 13 |
| wal | disk_bytes | **405 GB** across 406 files |

## Root causes (in priority order)

1. **Flush task is not draining buckets on schedule.** `oldest_bucket_age_secs` exceeds `retention_mins·60` while `pressure_pct=13%` — flush is not pressure-blocked, it's time-broken. WAL bloat (405 GB) confirms this has been broken for a long time.
2. **Tiny-batch storm in MemBuffer.** 65 rows/batch + 9 KB/row from struct/dictionary bloat on `errors` (Variant) and `summary` (List<Utf8View>).
3. **48-way Union skew.** Physical plan: `partition_sizes=[11..1378]` — last partition has 70× more batches than first. One worker is the long pole on every scan.
4. **TopK dynamic filter never tightens** (`DynamicFilter [ empty ]`). Files overlap on timestamp so per-file min/max can't prune. ZORDER (commit 4a41fea) may be skipping partitions that need a full rewrite.
5. **Parquet auto-split worsens TopK.** 2–3 files split into 48 file_groups means each file is read 16× in parallel; TopK threshold never beats any split's min.

## Workstream A — Unblock the flush task

The unblocker. Nothing else matters if flush is broken.

- **A1.** Failing test first (bug-fix workflow rule): integration test in `tests/` that sets `FLUSH_INTERVAL=30s`, `RETENTION_MINS=2`, ingests known rows, asserts `oldest_bucket_age_secs < 120` after 3 min.
- **A2.** Instrument `run_flush_task` (buffered_write_layer.rs) with OTel counters: `flush_attempts`, `flush_buckets_drained`, `flush_skipped_incomplete`, `flush_failed{reason}`, `delta_commit_seconds`. Land + watch staging before changing logic.
- **A3.** Identify the stall. Likely modes in order:
  - `DeltaWriteCallback` blocked on Delta commit (S3/DynamoDB lock — see `[[drop_dynamodb_lock]]`). Add per-table commit timeout + retry.
  - "Bucket complete" predicate too strict; clock skew keeps buckets ineligible.
  - Flush task panicked once and `tokio::spawn` lost it. Wrap in supervisor loop.
- **A4.** Drain the WAL backlog after flush is provably stable for 72h: truncate orphaned segments whose Delta commits are present.

## Workstream B — In-RAM scan speed

- **B1.** Coalesce at flush boundary, not at insert. When `TableBuffer` rolls a batch into a bucket and the bucket has >N small batches, run `concat_batches` to bound at ≤16 batches/bucket.
- **B2.** Re-partition by `bucket_id` in `query_partitioned`, not insertion order. With ~65 buckets across 13 projects → balanced fan-out.
- **B3.** Investigate the 9 KB/row figure. Likely fragmented dictionaries on Variant `errors` + List<Utf8View> `summary`. Heap snapshot via `array_memory_size` per column; if confirmed, dictionary-rebuild on coalesce. Expected 5–10× RAM reduction.
- **B4.** Bump `TIMEFUSION_BUFFER_RETENTION_MINS` from 70 → 120 *after* B1+B3 land. With dictionaries rebuilt, 2h of data fits comfortably.

## Workstream C — Delta scans for "older than retention"

- **C1.** Verify ZORDER is producing tight per-file min/max for timestamp. The `4a41fea` idempotence guard may be skipping partitions that have legacy overlapping files. One-shot rewrite of recent date partitions to confirm.
- **C2.** Lower auto-split for TopK queries: raise `repartition_file_min_size`, or lower `target_partitions` for `otel_logs_and_spans`. Goal: one file = one partition. TopK threshold can then prune split boundaries cleanly.

## Sequencing

| Week | Work | Why |
|---|---|---|
| 1 | A1 → A2 → A3 | Unblock flush in isolation; don't conflate with other changes |
| 1, parallel | B2 | Local-only code change, immediate win |
| 2 | B1 + B3 (flag-gated) | Risk of regressing insert latency / memory accounting |
| 2 | C1 + C2 | Verify on one project first |
| 3 | A4, B4 | Only after flush proven stable 72h |

## Success criteria

| Metric | Now | Target |
|---|---|---|
| `oldest_bucket_age_secs` p99 | 4,568 | < 7,200 |
| `total_batches / total_rows` | 1 / 65 | < 1 / 5,000 |
| `estimated_bytes / total_rows` | 9 KB | < 2 KB |
| `wal.disk_mb` | 405 GB | < 5 GB |
| 1h histogram p95 | 164 s | < 1 s |
| 2h list p95 | untested | < 200 ms |
| 24h list p95 | 487 s | < 5 s |

## What we are not doing

- No parallel "hot cache" layer.
- No MemBuffer rewrite. Architecture is fine — fix the stuck flush, the batch fragmentation, the planner skew.

## Open question

After this lands the WAL is much smaller. If we eventually drop the WAL entirely (save write amplification), retention must halve and flush frequency must double. Separate decision; not blocking.

## Reproduction harness

See `bench/replay_prod_load.rs` (and companion `bench/download_prod_sample.sh`). Pulls a realistic slice of prod `otel_logs_and_spans` rows + their stats fingerprint, ingests them into a local TimeFusion via the same gRPC path used by monoscope, and asserts against `timefusion_stats` so each workstream's claim ("65 → <5000 rows/batch", "9 KB → <2 KB/row") can be re-run after every code change.
