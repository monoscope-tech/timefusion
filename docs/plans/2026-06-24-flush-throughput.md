# Flush throughput: root cause & solutions (2026-06-24)

## Symptom

During bulk replay of old-event-time data (DLQ backfill, event times ~06-19, days
old): `flush_completed_total` / `rows_flushed_total` freeze, `rows_ingested_total`
climbs ~25k/min, `rows_in_buffer_lag` grows past 1M, `pressure_pct` climbs to 100 and
TF rejects inserts. `oldest_bucket_age_secs` pinned at ~4.9 days. Flushes that commit
are tiny (1866 rows, 2 rows). No flush errors. tantivy logs flooded.

## Root cause: serialized, fixed-cost-dominated commit path — NOT a single stall

Old-event-time buckets **are** selected for flush (no age guard blocks them — both the
flush cutoff `id < current_bucket_id()` and `get_flushable_buckets` include them). The
collapse is **throughput**, the product of four multipliers:

1. **Per-date partition fan-out.** Unified table is partitioned `[project_id, date]`.
   The staged `RecordBatchWriter` (`database.rs:2920/2965`) emits **one parquet file per
   distinct `(project_id, date)`**. DLQ replay scatters small row counts across many days
   → one commit writes many tiny files, each its own S3 PUT. Tiny rows (1866) but high
   partition cardinality → expensive commit. This is why commits are small *and* slow.

2. **Global commit lock.** `delta_commit_lock` (`database.rs:947`) is one process-wide
   mutex around the commit-log append, shared by flush commits **and** dedup
   `replace_where` (`:3875`). `flush_parallelism=4` (`config.rs:130`) only parallelizes
   parquet encode/upload — **the commit is single-threaded across all tenants**. Effective
   throughput = `1 / per-commit-fixed-cost`.

3. **File-count-throttled dedup.** Each flush commit runs dedup `replace_where` + a Delta
   metadata walk over the partition's files. At 52.9k files the measured ceiling was
   **~3000–3300 rows/s (~3 commits/min)** (see `2026-06-24-overnight-throughput-report.md`).
   *Now mitigated:* the 2026-06-24 OPTIMIZE-conflict fix + `optimize --all` cut the table
   from ~20k → ~908 files, so each dedup walk is ~20× cheaper — drain ceiling should rise.

4. **Coalesce-into-one-giant-commit + concat/sort.** Flush coalesces per
   `(project,table)` (`buffered_write_layer.rs:1234`); a single-table backfill collapses
   into one huge `CombinedBucket`, then `sort_batches_by_schema` (`database.rs:4271`)
   concats + lexsorts the whole group before *every* commit.

**Why `oldest_bucket_age` stays pinned:** old buckets sit at the front of the flush set,
but new replay inserts re-create buckets in the same old 5-min windows faster than the
throttled commit drains them → the min-timestamp never advances.

## Backpressure hypotheses (from the new diagnostics) — verdicts

- **"flush isn't reaching the memory"** (`flush_freed_bytes_total` flat while pressure=100):
  **true in spirit, wrong mechanism.** Buckets are reachable and `drain_bucket` releases
  their charge correctly; the commits are just too slow/serialized, so freed bytes advance
  in rare large steps (looks flat). Disambiguate in prod with the new gauges: *large
  infrequent `flush_freed_bytes_total` steps + `flush_failed_total` flat* = slow serial
  commits (this diagnosis); *`flush_failed_total` climbing* = failing commits (offset
  overflow / OCC) — a different bug.
- **"estimate inflation, RSS ≪ estimated"**: **largely not supported.** Utf8View
  inflation is already fixed at ingest by `compact_batch`/`compact_view_arrays`
  (`mem_buffer.rs:1954`). Residual over-count only from dictionary arrays / the
  `+96/column` charge on pathologically narrow batches. `process_rss_bytes` confirms;
  unlikely to be the driver.
- **`force_flush_current_buckets` is inert for backfill** — it only touches buckets
  `>= current` wall-clock (`buffered_write_layer.rs:632`); the escalation tier that exists
  to relieve pressure does nothing for old-event-time replay.
- **Relief loop early-bails** at <1% freed over one round (`:1149`), quitting at
  pressure=100 while completed buckets still wait.

## Tantivy — NOT the proximate cause, but a real RSS contributor

Sidecar builds are detached/fire-and-forget after the Delta commit
(`buffered_write_layer.rs:1379`), coalesced to **one build per (project,table) per cycle**
(refutes "one build per file"), off the commit critical path — they cannot freeze
`rows_flushed`. BUT up to **16 concurrent builds** (`tantivy_spawn_sem`, `:421`) ×
**64 MB writer heap** (`tantivy_index/builder.rs:36`) + retained `RecordBatch`es feed the
OOM→restart→replay loop. The 16-permit cap is inconsistent with the `4×` budget
reservation in `max_memory_bytes` (`:454`). The log flood is just per-thread segment
rolls at backfill volume — symptom, not bug.

## CORRECTION (after reading the commit path): the flush is already a staged append

Two earlier claims were wrong — they described the *pre-staging* design:

- The Delta commit **already stages**: parquet encode + S3 upload happen OUTSIDE the
  global `delta_commit_lock`; only the sub-ms commit-log append is serialized
  (`database.rs:2907-3001`). The lock is **not** the bottleneck, and a per-table lock buys
  nothing for the **unified table** — `project_id` is a *partition*, so all default
  projects share ONE Delta log; commits to one log are inherently serial at the
  version-increment regardless of locking. (Per-table locking only helps decouple the
  unified table from genuinely-separate *custom-project* tables.)
- Flush is `SaveMode::Append` (`database.rs:2966`); it does **not** `replace_where`. Dedup
  on the flush path is in-memory `dedup_batches` (cheap); the expensive `replace_where` is
  the separate periodic `dedup_today_partitions`. So "defer dedup off the hot path" is
  already the design.

So `skip_queue` (drastic — loses WAL durability/dedup/ordering) is **not** pursued. The
goal is to raise the *staged-append* steady-state drain. Per-cycle drain ≈
`flush_parallelism × (sort + encode + upload of one commit's fan-out files)`.

## The six levers (status)

1. **Raise `flush_parallelism`** (`config.rs:130`, default 4→8). More concurrent staged
   uploads to the same table — safe for appends, which is why the staging exists. Cheapest
   real drain increase; bounded by CPU (encode) + R2 connection count + in-flight memory.
   **IMPLEMENTED (4→8).**

2. **File-count tax on the append path.** Each commit does `new_table.get_file_uris()
   .collect()` (`database.rs:2999`) — a full file-list walk **under the lock**, scaling with
   partition file count (a big chunk of the ~3000 rows/s ceiling at 52.9k files). Compaction
   (20k→908, now unblocked by the OPTIMIZE fix) already shrank it ~20×. *Follow-up
   optimization:* derive the "added" URIs from the committed `Action::Add` paths
   (`adds` is already in hand at `:2960`) instead of diffing `get_file_uris()`, eliminating
   the walk entirely. **MITIGATED by compaction; code optimization STAGED** (touches the
   shared warm/evict `record_committed_write`, do with care).

3. **Per-*project* commit fragmentation — biggest structural lever.** Flush coalesces by
   `(project_id, table_name)` (`buffered_write_layer.rs:1234-1239`), so a backfill across N
   projects on the unified table = N separate commits (the "2 rows" commit = pure overhead).
   Coalesce the **unified** table by `table` (or `table+date`) so one commit carries all
   projects (`RecordBatchWriter` fans out by the `project_id` partition internally). Cost:
   WAL advance is per-topic `{project}:{table}` (`:1282`), so a combined commit must advance
   **multiple** WAL topics — `CombinedBucket` must carry per-project `wal_shard_counts` +
   per-project `source_bucket_ids`, the drain loop must advance/drain each, and the change
   must distinguish the unified table from per-project custom tables. **STAGED — test-first
   (e2e WAL-replay + no-loss tests); durability-critical, NOT a blind prod push.**

4. **`sort_batches_by_schema` per commit** (`database.rs:2887`) concats + lexsorts the whole
   coalesced group before upload — a large serial materialization for a giant backfill
   group. Sort per-`date`-chunk after the split in lever 3. **STAGED (with lever 3).**

5. **R2 connection starvation under concurrent PUTs.** The shipped S3
   `request_timeout`/`connect_timeout` tuning stops a slow PUT being cut off; also raise the
   object_store connection pool so the raised `flush_parallelism` uploads don't starve.
   **IMPLEMENTED for `create_object_store` (typed `ClientConfigKey::PoolMaxIdlePerHost`);**
   flush-path (`build_storage_options` stringly map) left to a verify-then-enable follow-up
   to avoid a store-registration risk on an unrecognized key.

6. **Relief loop bails too early.** `run_flush_task` (`buffered_write_layer.rs:1149-1166`)
   stops the relief loop when a round frees <1% of bytes — so under backfill it quits at
   `pressure=100` even though more old buckets are still draining (each freed bucket is
   small). Drive the bail by **commit progress** (did this round flush any buckets?) with a
   bounded round cap, not a byte delta. **IMPLEMENTED.** (The other half — generalizing
   `force_flush_current_buckets` to old event-time buckets — is **STAGED**; lower value
   because `flush_completed_buckets` already selects all old "completed" buckets, so
   force-flush only matters for a single oversized *current* window.)

## Ops (no code)
- **Keep files compacted** — regular `optimize --all` now that the OPTIMIZE fix is in
  (directly shrinks lever 2's walk). Add a file-count-driven Compact sweep across *all*
  dates (the scheduler only covers 48h).
- **Apply the P0 OOM/container config** from `2026-06-22-write-throughput.md` (never landed).

## Suggested order
Shipped now: levers 1, 5 (create_object_store), 6 (relief loop) + ongoing compaction. →
Next, test-first: lever 3 (+4) cross-project coalescing with multi-topic WAL advance — the
big steady-state win. → Then lever 2 added-URI optimization and lever 5 flush-path pool.
