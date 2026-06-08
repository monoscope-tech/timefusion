# TimeFusion E2E Test Suite — Execution Plan

Goal: a deterministic, fast E2E suite that exercises the **full prod path** (pgwire → BufferedWriteLayer → WAL → MemBuffer → flush → Delta on MinIO → Foyer cache → query) with **virtual time** so flush/eviction/retention paths run in milliseconds. Must catch the class of bug we just hit ("no response returned to queries") before deploy.

## Crates to add (`Cargo.toml [dev-dependencies]`)

```
testcontainers              = "0.20"
testcontainers-modules      = { version = "0.10", features = ["minio"] }
tokio-test                  = "0.4"
tracing-test                = "0.2"
insta                       = { version = "1", features = ["yaml"] }
proptest                    = "1"   # phase 5 only
```
`tokio` already pulled; ensure `test-util` feature is enabled for tests.

## Phase 0 — Audit time sources (½ day)
- [ ] Grep every `chrono::Utc::now`, `SystemTime::now`, `Instant::now`. Confirm scheduling-relevant ones go through `crate::clock`; flag any miss.
- [ ] Confirm `run_flush_task` / `run_eviction_task` use `tokio::time::sleep` (not `std::thread::sleep`).
- [ ] Document in `clock.rs` which categories of "now" must use it vs. which are metrics-only.

**Exit:** a one-page inventory in `docs/clock_audit.md` and any necessary fixes merged.

## Phase 1 — Test hooks on hot path (1 day)
Add minimum surface needed to deterministically drive the system:

- [ ] `BufferedWriteLayer::force_flush_now() -> Result<FlushReport>` — flushes *all* current buckets, returns `{buckets_flushed, rows, delta_commits}`.
- [ ] `BufferedWriteLayer::force_evict_now()` — runs the eviction pass once, synchronously.
- [ ] `BufferedWriteLayer::flush_completed_notify() -> Arc<Notify>` — pinged at end of each flush task iteration. Lets tests `notified().await` instead of sleeping.
- [ ] Same `Notify` for eviction.
- [ ] Expose a `FlushReport`/`EvictReport` counter snapshot (already partly in OTel metrics — wrap as a struct).

These are also useful in prod for `/admin/flush` style endpoints later.

**Exit:** unit test that asserts `force_flush_now()` empties MemBuffer and writes a Delta commit.

## Phase 2 — Harness: `tests/e2e/harness.rs` (1–2 days)
Builder-style:

```rust
let env = E2eEnv::builder()
    .with_minio()                // testcontainers, fresh bucket per test
    .with_clock_frozen_at("2026-06-01T00:00:00Z")
    .with_flush_interval(Duration::from_millis(10))
    .with_retention(Duration::from_secs(60))
    .start().await?;
let pg = env.pg_client().await?;   // real pgwire client
```

Capabilities:
- [ ] Spins MinIO via `testcontainers`, creates a per-test bucket, sets all `AWS_*` env vars **scoped** (no global mutation — pass via `AppConfig` builder).
- [ ] Initializes `crate::clock` in frozen mode at a known instant.
- [ ] Boots timefusion in-process on an ephemeral port, returns `tokio_postgres::Client`.
- [ ] Drives the full pgwire stack (no test-only bypass).
- [ ] `env.advance(Duration::from_secs(700)).await` — bumps `crate::clock` AND awaits the flush `Notify` so post-conditions are observable.
- [ ] `env.metrics().snapshot()` — pulls OTel counters as a plain struct for assertions.
- [ ] `env.foyer_stats()` — hit/miss counts.
- [ ] Auto-drops MinIO container + tempdir on test end.

**Exit:** `tests/e2e/smoke.rs` — insert 1 row, select it back, assert row count and **assert query returned in <500ms** (the specific bug we hit).

## Phase 3 — Core scenarios (`tests/e2e/scenarios/`) (2–3 days)
Each is a separate `#[tokio::test(flavor = "multi_thread", start_paused = false)]` file. Don't pause the runtime — we use `crate::clock` instead, because real S3 IO is happening.

1. **`smoke.rs`** — insert/select/restart/select. **Must always return a response.**
2. **`flush_lifecycle.rs`** — insert across two buckets, `advance(11min)`, assert one bucket flushed, one retained, Delta has expected files, MemBuffer freed memory.
3. **`eviction.rs`** — insert old + recent rows, `advance(retention + 1min)`, assert old evicted, recent retained, query still returns both via Delta+MemBuffer union.
4. **`cache_warmth.rs`** — flush, query twice. First reads from S3, second from Foyer. Assert `foyer_hits >= 1` on the second.
5. **`restart_recovery.rs`** — insert, kill server, restart. Assert WAL replay restores data; assert cursor snapshot skips cold-start delta scan (the optimization in 2d36136).
6. **`multi_tenant_isolation.rs`** — two `project_id`s, queries without filter must fail, queries with filter must not leak.
7. **`pressure_flush.rs`** — set tiny `max_memory_mb`, hammer inserts, assert early flush triggers and no OOM/deadlock.
8. **`zorder_idempotence.rs`** — run compaction twice, assert second is a no-op (the bug fixed in 4a41fea).

Each scenario asserts on:
- Correctness (row counts, content)
- **Liveness** (query returns within a timeout — guards the prod symptom)
- Observable state (Delta commit count, foyer hits, OTel counters)

## Phase 4 — CI integration (½ day)
- [ ] New `make test-e2e` target.
- [ ] GitHub Actions job: `cargo test --test e2e_*` on every PR. Needs Docker (testcontainers).
- [ ] Required check before merge.
- [ ] Target wall-time: < 90s for the whole suite (testcontainers MinIO startup is the long pole — share container across scenarios in one binary, isolate via bucket name).

## Phase 5 — Property tests (later, 2 days)
- [ ] `proptest` generator: sequences of `Insert(project, ts, n_rows) | AdvanceTime(d) | Query(project) | Restart`.
- [ ] Invariants: row count conservation, no row lost across flush, query result == union of MemBuffer + Delta, restart doesn't lose committed data.
- [ ] Run nightly, not on PR.

## Risks / open questions
- **testcontainers MinIO startup cost** — measure; if >5s, share one container per `cargo test` binary via a `OnceCell<Mutex<MinioContainer>>`, isolate tests by bucket prefix.
- **In-process server vs. subprocess** — in-process is faster and gives access to internals (force_flush_now), but masks process-level bugs (signal handling, atexit). Do in-process for most, add 1–2 subprocess tests for shutdown/restart paths.
- **AWS env vars are global** — must be passed via `AppConfig` not env. May require small refactor in `config.rs` to accept an explicit config rather than reading env in `init`.
- **Clock freeze coverage** — Phase 0 must catch every wall-clock read; one missed `SystemTime::now()` in the flush decision path makes the whole suite flaky.

## Order of execution
P0 audit → P1 hooks → P2 harness + smoke test → ship that PR.
Then P3 scenarios in priority order (smoke, flush_lifecycle, cache_warmth, restart_recovery first — they cover the recent prod incidents).
P4 CI gating after smoke+flush_lifecycle are green.
P5 nightly proptest last.

## Definition of done
- All P3 scenarios green in CI, < 90s wall-time.
- A new prod bug of the class "queries don't return" or "flush silently fails" would have been caught by at least one scenario in this suite.
- Documented in `CLAUDE.md` under Testing.
