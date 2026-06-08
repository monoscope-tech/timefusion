# Clock-source audit (2026-06-07)

`crate::clock::now_micros()` is the **only** acceptable wall-clock read for
scheduling decisions (flush triggers, bucket id, retention cutoff). All other
`now`-style reads are either metrics or metadata and are safe to leave on the
real wall clock.

## Categories
- **scheduling** — drives whether/when background work runs. MUST go through `crate::clock`.
- **metrics**    — latency timing, monotonic-only. Tokio/std `Instant` is fine.
- **metadata**   — written into a log/header/file as the "produced at"
  timestamp; OK to use wall clock so the value is meaningful across restarts.

## Inventory (non-test code)

| file:line | call | category | notes |
|-----------|------|----------|-------|
| `src/buffered_write_layer.rs:381` | `crate::clock::now_micros()` | scheduling | ✓ default ts for batches missing one |
| `src/buffered_write_layer.rs:912` | `crate::clock::now_micros()` | scheduling | ✓ oldest-bucket-age gauge feeds alerts |
| `src/mem_buffer.rs:549` | `crate::clock::now_micros()` (via `current_bucket_id`) | scheduling | ✓ |
| `src/main.rs:161` | `clock::now_micros()` | metadata | cursor-snapshot age log line |
| `src/wal.rs:129` | `chrono::Utc::now()` | metadata | `WalEntry.timestamp_micros` — written into WAL header |
| `src/tantivy_index/service.rs:106,127` | `Utc::now()` | metadata | manifest `built_at` field |
| `src/object_store_cache.rs:*` | `SystemTime::now()` + `Instant::now()` | metrics / metadata | cache entry stamps + read-latency timing |
| `src/database.rs:* (4 sites)` | `Instant::now()` | metrics | query latency timing |
| `src/buffered_write_layer.rs:418` | `Instant::now()` | metrics | recovery duration |
| `src/pgwire_early_bind.rs:*` | `Instant::now()` | metrics | deadline for connect-readiness loop |
| `src/statistics.rs:*` | `Instant::now()` | metrics | sample timestamps |

## Outcome
- No scheduling reads bypass `crate::clock`. Phase 0 passes — no code fix needed.
- Background tasks (`run_flush_task`, `run_eviction_task`) sleep via
  `tokio::time::sleep`, which can be advanced via tokio's `pause()` if a test
  wants. We don't use it because the E2E harness runs real S3 IO; the
  `crate::clock` + explicit `force_*_now()` test hooks (Phase 1) give us
  determinism without pausing the runtime.

## Rule for new code
If you add a `now`-style read:
1. **scheduling** → `crate::clock::now_micros()`.
2. **metrics** → `std::time::Instant::now()`.
3. **metadata** → `chrono::Utc::now()` (or `crate::clock::now_micros()` if the
   metadata is going to be compared against scheduling time later).

If you're unsure, treat it as scheduling.
