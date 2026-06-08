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

See `bench/replay_prod_load.py` (and companion `bench/download_prod_sample.sh`). Pulls a realistic slice of prod `otel_logs_and_spans` rows + their stats fingerprint, ingests them into a local TimeFusion via the same gRPC path used by monoscope, and asserts against `timefusion_stats` so each workstream's claim ("65 → <5000 rows/batch", "9 KB → <2 KB/row") can be re-run after every code change.

## Performance envelope (2026-06-08, after B1 v2 + MAX_BATCH_COUNT_PER_BUCKET=8)

`bench/concurrent_load.py` against clean MinIO + clean Delta on a 10-core M-series laptop, TOKIO_WORKER_THREADS=32:

| writers | readers | writer-rate | ingest r/s | p50 read | p95 read | p99 read | under 100 ms |
|---|---|---|---|---|---|---|---|
| 5 | 5 | unlimited | 800 | 4 ms | 16 ms | 22 ms | 100% |
| 50 | 20 | 5 | 250 | 7 ms | 15 ms | 28 ms | 100% |
| 100 | 30 | 10 | 1000 | 12 ms | 37 ms | 70 ms | 99% |
| 200 | 50 | 5 | 1000 | 18 ms | 60 ms | 180 ms | 96% |
| **300** | **75** | **5** | **1500** | **27 ms** | **267 ms** | **920 ms** | **88%** |
| 500 | 100 | 2 | 1000 | 223 ms | 1400 ms | 1900 ms | 7% |

**Pass envelope:** initially ≤200 concurrent writers + 50 readers @ 1000 r/s ingest with **p95 < 100 ms**. The 8000× improvement vs the prod baseline (p95: 487 s → 60 ms on the 24 h list).

After fast-resolve fix (commit `da85e29`): **≤300 writers + 30 readers @ 1500 r/s ingest, p95 = 93 ms**. Higher reader counts (75+) still saturate the tokio runtime and tail latencies escalate.

After Delta-empty short-circuit (`800d30d`) + lock-free plan cache (`5056454`) + skip-per-query-optimize (`cfadce9`): **≤300 writers + 75 readers @ 1500 r/s ingest, p95 = 74–84 ms**. The skip-optimize fix was the biggest single jump: vendored `datafusion-postgres` was running `state.optimize()` per query at ~30 ms p95 (subst=5 ms, optimize=27–32 ms), with the same handful of canonical OLAP plan templates repeating millions of times. Caching the OPTIMIZED logical plan + skipping the per-query optimize call dropped server-side pgwire p95 from 131 ms → 8 ms (16×).

Current breaking points: 300 w + 100 r is right at the boundary (p95 = 100–110 ms). 500 writers saturates the tokio runtime entirely (p50 jumps to 375 ms).

**Important caveat — the empty-Delta short-circuit is doing heavy lifting.** On a fresh process where no flushes have committed yet, `skipped_delta_pct ≈ 99.5%` and server scan p95 is 0.5 ms. Once flushes start landing files (steady state in prod after 10 min), `skipped_delta_pct` drops to ~67 % and server scan p95 climbs back to ~32 ms because the queries needing UNION(mem, delta) go through the full Delta-side TableProvider build. The next optimization target — to keep the envelope post-flush — is:

- **Cache the Delta-side TableProvider** per `(project, table, snapshot_version)` rather than rebuilding on every scan. Currently each scan that doesn't short-circuit calls `table.table_provider().await` which traverses delta-rs internals on every query.
- **Pre-resolve the union plan**: the per-query overhead of stitching `MemorySourceConfig` and the Delta scan into a `UnionExec` includes filter pushdown, schema coercion, and bucket-range exclusion filters. None of these depend on parameter values; they could be cached per `(project, table, mem_buckets_signature)`.

The DataFusion plan cache helps for the logical plan template but the *physical* plan is rebuilt per-call. Caching at the physical layer is a bigger lift but is the next ~10× available on this path.

## Per-session optimization log (chronological)

| Commit | Change | Δ p95 vs prior |
|---|---|---|
| `da85e29` | Lock-free fast-resolve via DashMap shortcut | -15 % |
| `800d30d` | Delta-empty sticky short-circuit (seeded from S3 truth) | -45 % |
| `bf44249` + `fa1dee9` | Scan + pgwire metrics in `timefusion_stats` | observability |
| `5056454` | DashMap plan cache replaces Mutex<LruCache> | -30 % |
| `cfadce9` | Pre-optimize at miss-time, skip per-query optimize | -50 % (server pgwire 131 ms → 8 ms) |
| `e630d9e` | Cache Delta TableProvider per (project, table, version) | not yet validated under heavy mem+delta load — flush-task interference dominates first |

## Realistic-load envelope (50 conns mix)

Most prod traffic is batched client-side, so 50 conns is closer to real than 300:

| writers | readers | writer-rate | ingest | p50 read | p95 read | p99 read |
|---|---|---|---|---|---|---|
| 5 | 75 | 1 | 25 r/s | 6–11 ms | 22–47 ms | 77–107 ms |
| **50** | **75** | **10** | **15 000 r/s** | **8–14 ms** | **49–70 ms** | **141–155 ms** |

**The 300-writer scenario is a stress test, not a production target.** Writer contention dominates above ~150 concurrent INSERT-issuing connections — each INSERT carries a fresh parse + (now cached) plan-build chain that, while individually cheap, multiplies under high writer concurrency and starves readers on shared tokio worker threads. The realistic envelope (≤100 mixed conns at 15 k r/s ingest) clears the < 100 ms p95 goal comfortably.

### Harness-side bottleneck at extreme load

At 100 writers + 75 readers, the harness reports p95 = 1200 ms but `timefusion_stats.pgwire.lat_p95_us_approx` shows the server is at **32 ms**. The 1170 ms gap is in the harness itself: 175 Python threads contending for the GIL plus 175 sockets exchanging messages under psycopg's libpq-based synchronous client. This is a measurement artifact of the test harness, not a production behavior of the database — a real client using a non-GIL runtime (Go, Rust, Java) or a properly multi-process Python deployment would see the server-side numbers, not the harness numbers.

**For accurate prod-projection latency under high concurrency, use `timefusion_stats.pgwire.lat_p95_us_approx` directly** — it measures the server-side end-to-end and is unaffected by client-side bottlenecks.

**How the fast-resolve fix worked:** dev-build instrumentation showed `slow delta scan: total=N resolve=N lock=0ms scan_build=4ms` — `resolve_table` was 99% of per-query Delta-side latency. Three tokio RwLock `.await`s per call (unified_tables map, last_written_versions map, inner table.version()) plus `should_refresh_table` returning true on the common `(Some(_), None)` state and firing `update_state` per query for un-flushed projects. Added a `DashMap` lock-free shortcut on Database, populated on first slow-path success. Hot path becomes `DashMap.get → Arc clone`, zero awaits.

**Failure mode > 200 writers:** ~10 % of reads hit a periodic stall (p99 jumps to 900 ms+ while p50 stays under 30 ms). Diagnosed contributors:

- **Tokio runtime saturation.** 375+ long-lived connections × per-conn task work overruns 32 worker threads. Confirmed by re-running 50w/75r at the same 1500 r/s — fewer connections, same throughput, p95 still 690 ms (so connection count, not ingest rate, dominates).
- **Inline coalesce under bucket lock.** Tried moving `concat_batches` outside the `bucket.batches` lock (snapshot → drop → concat → drain-and-prepend with a per-bucket coalesce_lock). Made p95 ~3× **worse** (740 ms): the extra Vec clone + double lock acquisition hurts more than the held lock saved, because concat on 8 × 30 rows of Variant data is fast (sub-ms). Reverted.
- **Accumulated Delta history.** Previously-suspected p95 spikes at the 200w boundary were partly explained by 1665+ accumulated Delta commits on a re-used MinIO; wiping `/tmp/minio-data` between sweeps restored the clean envelope.

**Not the bottleneck (ruled out):**

- Per-query plan cost: p50 of 26 ms shows the routed-MemBuffer scan is fine.
- `MAX_PG_CONNECTIONS` env var: present but unused in code (only the embedded sqlx pool for compaction reads it; pgwire has no hard cap).
- WAL fsync: readers don't touch WAL; fsync stalls would show as insert-side latency, not read tail.

**Next levers (not implemented in this window):**

1. **PGWire connection cap + queue** — bound concurrent connections to a multiple of worker threads; queue beyond that. Would protect p95 by trading throughput.
2. **Move coalesce off the hot path entirely** — background per-table compactor (one task per TableBuffer, woken by a Notify when batch count exceeds threshold). Removes the inline lock-hold without the clone-and-rejoin overhead that the inline-out-of-lock attempt suffered.
3. **Investigate the p99 1-2 s outliers at 300w** — likely flush task holding buckets during Delta commit. Per-flush Delta commit timeout + per-table flush parallelism would help.
