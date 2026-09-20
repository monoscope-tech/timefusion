# Concurrency Performance Backlog

This document records the synchronization improvements identified in the TimeFusion concurrency audit. It treats correctness priority and performance priority separately: a correctness-critical fix is not necessarily a throughput optimization, while a safe implementation can still be a high-priority performance change.

## Executive ranking

| Item | Correctness priority | Performance priority | Expected effect |
|---|---:|---:|---|
| ~~Replace cache-stat `RwLock` with atomics~~ | **SHIPPED** | **SHIPPED** | Removed a global exclusive async lock from every cache hit and miss (`ccebc093`) |
| Extend journal group commit to direct checkpoint callers | Largely shipped | **P2 residual** | The ack path already coalesces fsyncs via `GroupCommit`; route the remaining direct `checkpoint()` callers through it |
| ~~Remove long Delta guards from light compaction~~ | **SHIPPED** | **SHIPPED** | Tail enumeration now clones out from under the guard (`ccebc093`) |
| Batch and conditionally publish Tantivy manifests | P1 | **P1** | Remove per-file object-store read-modify-write serialization and prevent cross-process lost updates |
| Instrument and tune maintenance admission | P2 | **P1 for instrumentation; P2 for redesign** | Identify idle capacity, double admission, and priority inversion |
| Parallelize resumed-bin discovery | P3 | **P2** | Shorten maintenance ticks at high project cardinality |
| Improve maintenance wake semantics | P3 | **P2** | Remove up to one polling interval of dispatch latency |
| Prototype immutable Delta snapshot publication | P3 | **P2** | Reduce planning-path lock and cache-line contention |
| Remove other Delta guards across awaits | P1/P2 | **P2-P3 depending on path** | Improve rare-path, migration, and startup latency |
| Bound per-key coordination registries | P2 | **P3** | Primarily memory/cardinality protection; premature cleanup could hurt throughput |
| ~~Replace the WAL allocator spinlock~~ | **SHIPPED** | **SHIPPED** | Infinite spinning after an allocation failure is gone (`ce5e4781`) |
| Add concurrency tests and observability | N/A | **P0 enabler** | Supply the evidence required to optimize safely |

## 1. Replace cache-stat locks with atomics

**Status: SHIPPED** (`ccebc093`). One `cache_stats!` macro now declares the ten
counters once, as both the plain `CacheStats` snapshot and the `AtomicCacheStats`
the hot path bumps with relaxed `fetch_add`. Every recorder is synchronous.

Two consequences worth knowing: `get_stats`/`reset_stats`/`log_stats` are no
longer `async` (callers dropped their `.await`), and `try_get_stats` is GONE —
it existed only to avoid blocking on the lock, and its `try_read` returned
all-zero DEFAULTS under contention, which silently corrupted cache-hit readings
during the md3 write-latency investigation. `get_stats` is now always exact.

### Problem (as it was)

Object-cache accounting stores ten additive counters behind a Tokio `RwLock`. Cache hits, misses, range hits, and byte accounting all acquire the write side of that lock.

Relevant code:

- `src/storage.rs`: `CacheStats`, `StatsRef`, `record_hit`, `record_miss_with_fetch`, `record_range_hit`, and `record_range_miss`.

This makes the cheapest cached-I/O path perform exclusive async synchronization. Under concurrent scans, otherwise independent cache hits serialize on either the main-cache or metadata-cache stats lock. Contention can add Tokio waiter queueing, suspension, and wake-up overhead.

### Proposed work

- Replace each additive field with `AtomicU64`.
- Use `Ordering::Relaxed`; the counters do not publish dependent state.
- Construct a `CacheStats` value by loading each atomic.
- Define reset semantics explicitly. A concurrent reset and increment can produce an approximate snapshot, which is acceptable if documented.
- Do not add per-hit histogram recording or dynamic labels while removing the lock.

### Expected performance effect

- Better high-concurrency cache-hit throughput.
- Lower query p99 for scans that perform many metadata and range-cache accesses.
- Less task scheduling and lock-cache-line contention.
- Slightly larger counter storage; only add cache-line padding if a benchmark demonstrates false sharing.

### Acceptance criteria

- Cache-hit accounting performs no async lock acquisition.
- Existing exported fields retain their meaning.
- Concurrent increment and reset tests do not panic or underflow.
- Benchmark 1, 4, 16, and 64 concurrent tasks on the exact accounting path.

## 2. Extend journal group commit to direct checkpoint callers

**Correctness priority:** P2  
**Performance priority:** P2 residual — the core of this item already shipped

### Status: largely shipped (2026-09-11)

Group commit for the maintenance journal already exists. `Database::commit_journal`
(`src/database/maintain.rs`) coalesces concurrent checkpoint+fsync calls through
`support::GroupCommit`, is the required pre-acknowledgement durability step on the
foreground write path, and exports `journal_commits` / `journal_commits_coalesced`
counters plus a `journal_commit_wait` block watch. Do not re-implement it.

### Remaining problem

`maintenance_tasks` is a process-global `std::sync::Mutex<TaskJournal>`, and
`TaskJournal::checkpoint` deliberately holds it across the fsync for durability
ordering (see the comment on `checkpoint`). Roughly twenty maintenance-side call
sites in `src/database/maintain.rs` invoke `journal.checkpoint()` directly under
that mutex instead of going through `commit_journal`, so those transitions pay
serialized, uncoalesced fsyncs and can hold the mutex through disk latency.

Relevant code:

- `src/database/mod.rs`: `Database::maintenance_tasks`.
- `src/maintenance_coordinator.rs`: `TaskJournal::checkpoint`, `TaskJournal::compact`, and `TaskLease`.
- `src/database/maintain.rs`: `commit_journal` and the direct `checkpoint()` call sites.
- `src/support.rs`: `GroupCommit`.

### Proposed work

- Read `journal_commits_coalesced` against `journal_commits` under load first; if
  coalescing is already high and the direct callers are off the latency-critical
  path, stop here.
- Route direct maintenance-side `checkpoint()` callers through `commit_journal`
  where their durability ordering allows it.
- Only if metrics then show the shared `GroupCommit` insufficient, escalate to a
  dedicated writer task owning `TaskJournal` behind a bounded mutation channel
  with sequence-numbered acknowledgements.

### Acceptance criteria

- Concurrent maintenance transitions share physical fsync operations.
- Crash recovery produces the same durable state as the current journal format.
- Acknowledgements never precede the fsync that covers their mutation.

## 3. Release the Delta table guard before light-compaction enumeration

**Status: SHIPPED** (`ccebc093`). The binned-consolidation caller now clones the
`DeltaTable` out from under the read guard before `light_optimize_tail` consumes
the async add-action stream, matching the `{ table_ref.read().await.clone() }`
idiom already used elsewhere in `compact.rs`.

NOT verified under contention: no test asserts that a delayed enumeration leaves
publication unblocked, and the third acceptance criterion below is untested.

### Problem (as it was)

Light compaction holds a `RwLock<DeltaTable>` read guard while asynchronously collecting active add actions. Tokio's `RwLock` is write-preferring. Once a snapshot publisher queues for the write lock, later query readers queue behind that writer while the maintenance reader remains active.

Relevant code:

- `src/database/compact.rs`: `light_optimize_tail` and its caller in binned consolidation.

The resulting latency chain is:

```text
maintenance reader holds lock
    -> commit publisher queues for write
        -> later query readers queue behind the writer
```

### Proposed work

- Clone the `DeltaTable` or extract an `EagerSnapshot` and `LogStoreRef` under the read lock.
- Release the shared guard before consuming the async action stream.
- Keep snapshot publication version-checked.

### Expected performance effect

- Little change when maintenance is idle.
- Lower planning and publication p99 during hot-tail maintenance.
- A small `Arc`/table-clone cost outside contention.

### Acceptance criteria

- No shared Delta table guard survives action enumeration.
- A deliberately delayed enumeration does not block query snapshot capture or commit publication.
- Published versions cannot regress.

## 4. Batch and conditionally publish Tantivy manifests

**Correctness priority:** P1  
**Performance priority:** P1

### Problem

Tantivy manifest mutation holds a per-`(table, project)` Tokio mutex through an object-store GET, JSON parse, mutation, serialization, and PUT.

Relevant code:

- `src/tantivy/mod.rs`: `ManifestLocks` and `mutate`.
- `src/tantivy/search.rs`: index publication, count proofs, carry-forward, and GC mutations.

This has two consequences:

1. Concurrent index builds for one project serialize their full publication round trips.
2. The mutex is process-local, so two TimeFusion instances can load the same generation and silently overwrite one another.

A naive ETag retry loop fixes correctness but can create a retry storm in which every publisher repeatedly downloads and uploads the growing manifest.

Note: `upsert_manifest_many` already batches many entries under one load+save for a single caller; the batching proposed here is the cross-caller kind, and `save_manifest` uses an unconditional `put`, so the cross-process hazard is real.

### Proposed work

- Create one in-process manifest writer per logical manifest key.
- Send typed mutations through a bounded channel.
- Drain and combine compatible mutations.
- Perform one GET and one conditional PUT per batch.
- Retry the combined batch on an ETag/version conflict.
- Include object-store or database identity in the in-process key.
- If manifests continue to grow, investigate immutable manifest shards plus a small conditionally updated root pointer.
- If the deployment guarantees exactly one writer, document and enforce that with a fenced lease rather than relying on convention.

### Expected performance effect

- Fewer object-store GET and PUT operations during backfill.
- Faster publication of completed indexes.
- Less JSON allocation and serialization.
- Fewer cross-process CAS conflicts than independent per-file retries.
- A small configurable batching delay before an index becomes visible.

### Acceptance criteria

- Two processes cannot silently lose manifest entries.
- Conflicts retry or return explicit errors.
- Metrics expose publication wait, batch size, manifest bytes, GET/PUT latency, and CAS retries.
- A concurrent test simulates two publishers starting from the same generation.
- Obsolete immutable generations have a cleanup policy.

## 5. Instrument before consolidating maintenance admission

**Correctness priority:** P2  
**Performance priority:** P1 for instrumentation; P2 for redesign

### Problem

Maintenance uses multidimensional admission together with independent semaphores for heavy rewrites, light rewrites, repair bytes, debt workers, quarantine workers, derived-rollup reservation, DML merges, scans, outer jobs, and repair passes.

These gates deliberately isolate workloads, but multiple independent schedulers can create priority inversion or double admission. The dangerous pattern is a task holding scarce resource A while waiting for resource B, preventing work that only needs A from running.

Relevant code:

- `src/database/mod.rs`: maintenance semaphore fields.
- `src/maintenance_coordinator.rs`: `AdmissionController`.
- `src/database/maintain.rs`, `src/database/compact.rs`, `src/database/rollup.rs`, and `src/database/scan.rs`: acquisition sites.

Tokio semaphores are FIFO-fair. A queued `acquire_many` at the head can prevent smaller requests from completing even when enough permits exist for the smaller request. Existing `try_acquire_many`, requeueing, and separate light/heavy lanes avoid this in several important paths and must be preserved.

Note: the hygiene/light-rewrite lane already exports permit metrics (`permit_held_seconds`, `permits_available`, `permits_acquired`, `permits_unavailable` in `src/observability.rs`); the instrumentation work below applies to the gates that still lack them.

### Proposed work

Instrument every gate with:

- Acquisition wait duration.
- Permit hold duration.
- Failed try-acquires.
- Available and total permits.
- Task class.
- Other permits already held while waiting.
- Requested versus actual decoded bytes, CPU time, object reads, and object writes.

Then change only the binding path:

- Move resources that must be acquired atomically into `AdmissionController`.
- Acquire scarce permits as late as possible.
- Release them before commit serialization or unrelated I/O.
- Prefer try-admit-and-requeue for background tasks over waiting while holding another permit.
- Retain dedicated lanes where they provide measured foreground or hot-tail isolation.
- Remove a gate only when telemetry shows that it never binds or duplicates another gate.

### Acceptance criteria

- The acquisition graph is documented and acyclic.
- No task waits for one gate while unnecessarily retaining another.
- Metrics identify the actual binding resource.
- Saturation tests demonstrate foreground and hot-tail progress under maintenance backlog.
- Rollup, repair, and quarantine reservations remain enforceable.

## 6. Parallelize resumed-bin discovery

**Correctness priority:** P3  
**Performance priority:** P2

### Problem

Tail maintenance holds the local plan mutex while sequentially awaiting `resumable_staged_bin` for each project.

Relevant code:

- `src/database/maintain.rs`: resumed-bin discovery in the light optimize pass.

At this point there may be no competing plan user, so the mutex itself is not necessarily contended. The performance issue is sequential object-store discovery across projects.

### Proposed work

- Run resumable discovery from the original planned vector before wrapping the mutable plan in a mutex, or clone a snapshot of the plan.
- Probe projects with bounded concurrency, initially 8-16.
- Remove successfully resumed projects from the mutable plan after discovery.
- Avoid unbounded object-store fan-out.

### Expected performance effect

- Faster maintenance tick startup for large project populations.
- Little or no benefit for ordinary ticks with few projects.
- Increased object-store burst if concurrency is oversized.

### Acceptance criteria

- No plan mutex is retained during object-store work.
- Recovery behavior remains deterministic.
- Probe concurrency is bounded and observable.
- Benchmark at realistic project counts and object-store latency.

## 7. Improve maintenance wake-up semantics

**Correctness priority:** P3  
**Performance priority:** P2

### Problem

`maintenance_work` uses `notify_waiters`. It wakes current waiters but does not retain a permit for a worker that begins waiting immediately afterwards. The polling fallback preserves correctness but may add up to one idle-backoff interval when the queue transitions from empty to non-empty.

Relevant code:

- `src/database/mod.rs`: `maintenance_work`.
- `src/database/maintain.rs`: dirty-bin enqueue notifications.
- `src/database/rollup.rs`: coordinator wait loop.

### Proposed work

- Measure enqueue-to-claim latency and lost-to-poll wakeups.
- Consider `notify_one` for a single newly runnable unit because it retains a permit.
- Notify multiple times only when multiple workers should become runnable.
- Alternatively use a monotonic queue-generation counter plus `Notify`, or a `watch` channel for latest-state semantics.
- Keep the queue itself authoritative; notifications must remain hints.

### Expected performance effect

- Lower time-to-start for bursty maintenance work.
- Little change when a backlog is already saturated.
- Potentially fewer thundering-herd wakeups than `notify_waiters`.

### Acceptance criteria

- Missing a notification cannot cause correctness failure.
- Newly queued work does not normally wait for the polling fallback.
- Deterministic tests cover enqueue/wait transition races.

## 8. Prototype immutable Delta snapshot publication

**Correctness priority:** P3  
**Performance priority:** P2

### Goal

Reduce query-planning dependence on `Arc<RwLock<DeltaTable>>` by publishing complete immutable table views.

### Proposed design

Evaluate a publication type similar to:

```rust
struct PublishedTable {
    snapshot: Arc<deltalake::kernel::EagerSnapshot>,
    log_store: deltalake::logstore::LogStoreRef,
    version: u64,
}
```

Publish `Arc<PublishedTable>` through `ArcSwap`. Keep mutable `DeltaTable` values local to commit and reload operations. Retain per-physical-table commit serialization.

### Expected performance effect

- Lock-free snapshot acquisition for query planning.
- No reader-held guard delaying publication.
- Lower p99 under high read concurrency and frequent commits.
- Additional atomic reference and reclamation traffic.
- Limited benefit if table locks are acquired only once per query and are currently uncontended.

### Risks

- `DeltaTable` itself may be the wrong publication unit.
- Broad call-site changes can introduce stale-snapshot bugs.
- Publishing large cloned state may increase allocation pressure.
- Commit construction still requires mutable state and serialization.

### Acceptance criteria

- Readers always observe a complete immutable version.
- Publication cannot regress table versions.
- Query planning requires no async table lock.
- Benchmark planning latency with concurrent commits before adopting broadly.

## 9. Remove remaining Delta guards across awaits

These paths have different performance priorities and should not be treated as one project.

### 9.1 Schema evolution reload

**Correctness priority:** P1  
**Performance priority:** P3 normally; potentially severe during migration

Relevant code:

- `src/database/mod.rs`: `evolve_table_columns`.

The table write guard is held while `DeltaTable::load().await` performs log or object-store work. Clone the table, load without the guard, then perform a version-checked swap. This is unlikely to change steady-state performance but prevents a schema migration from stalling all readers.

### 9.2 WAL history recovery

**Correctness priority:** P2  
**Performance priority:** P3 for live traffic

Relevant code:

- `src/database/write.rs`: `derive_wal_cursors_for_physical_table`.

This is primarily a startup and recovery path. Clone the table or extract its log store before awaiting history. The likely gain is faster parallel boot reconciliation, not query throughput.

### 9.3 Statistics refresh

**Correctness priority:** P3  
**Performance priority:** P3

Relevant code:

- `src/database/rollup.rs`: periodic statistics refresh.
- `src/read/mod.rs`: `DeltaStatisticsExtractor`.

The refresh runs periodically, and extraction mostly reads snapshot metadata. Release the table guard before awaiting the statistics cache to avoid lock coupling, but do not expect a large throughput gain.

### Shared acceptance criteria

- No production Delta table guard survives an await unless the exception is documented with a required invariant.
- Snapshot publication is version-checked.
- Slow-I/O tests prove readers and publishers remain live.

## 10. Bound per-key coordination registries only after measurement

**Correctness priority:** P2  
**Performance priority:** P3

Affected registries include:

- `commit_locks`.
- `dml_locks`.
- `flush_waiter_counts`.
- `tantivy_backfill_slots`.
- Tantivy manifest locks.

### Performance analysis

These maps can grow with historical physical tables, but unified tables collapse projects onto one physical lock key. Aggressive cleanup can reduce performance or break correctness:

- Extra `Arc::strong_count`, `remove_if`, and allocation work.
- Reallocation when a cold table becomes active again.
- Races that create two independent locks for one logical key.
- Fixed striping that introduces false contention between unrelated tables.

### Proposed work

- Add registry cardinality metrics.
- Estimate bytes per entry.
- Measure custom-storage tenant churn.
- Keep exact per-table locks when cardinality is modest.
- If necessary, reap entries only after a proven inactive lifecycle.
- Use fixed striping only where false contention is acceptable.

### Acceptance criteria

- Registry growth is bounded for the measured workload.
- Reaping cannot create simultaneous locks for the same key.
- Throughput is not reduced by allocation churn or false striping contention.

## 11. Replace the vendored WAL allocator spinlock

**Status: SHIPPED** (`ce5e4781`). `UnsafeCell<Block>` + `AtomicBool` became
`std::sync::Mutex<Block>` (not `parking_lot` — that would add a dependency to a
vendored crate for no gain here; poisoning is recovered via `into_inner`). Both
manual `unsafe impl Send/Sync` are gone, since `Mutex<Block>` derives them.

Guarded by `a_failed_rollover_releases_the_state_lock`, which was verified to
FAIL (5.02s, "allocator wedged") against the old release-on-success-only shape.
It runs via `make test-vendor` / `make prepush`, NOT in CI: ci/checks.tsv invokes
cargo directly and vendored crates are path deps rather than workspace members.

### Problem (as it was)

The vendored allocator uses `UnsafeCell<Block>` guarded by a manually managed `AtomicBool` spinlock. Fallible file creation and mmap operations use `?` before `unlock`. An error or panic can therefore leave the lock set forever, causing all later allocators to spin indefinitely.

Relevant code:

- `vendor/walrus-rust/src/wal/runtime/allocator.rs`: `BlockAllocator`, `get_next_available_block`, `alloc_block`, `lock`, and `unlock`.

### Performance analysis

The allocator hands out 10 MiB blocks. At 1 GiB/s of WAL payload, allocation occurs only about 100 times per second. Even tens or hundreds of nanoseconds of extra uncontended mutex overhead are immaterial compared with processing 10 MiB, updating trackers, and occasional file creation and mmap work.

The existing critical section is not reliably tiny because it can:

- Clone paths and mmap handles.
- Update `RwLock`-protected global trackers.
- Create and size a new file.
- Open or map storage.

Spinning during the slow path burns a core and interferes with query and maintenance work. After an error, performance collapses to zero allocator throughput with permanent CPU consumption.

### Proposed work

- Replace `UnsafeCell<Block> + AtomicBool` with `parking_lot::Mutex<AllocatorState>`.
- Remove manual `unsafe impl Send/Sync` if the new representation derives them safely.
- Initially keep the algorithm simple and RAII-safe.
- Only split file creation out of the critical section if profiling later shows rollover contention; a two-phase rollover protocol is substantially more complex.

### Expected performance effect

- Essentially neutral uncontended throughput.
- Lower CPU usage and better p99 during concurrent rollover.
- Elimination of permanent spinning on file or mmap failure.

### Acceptance criteria

- Error and panic paths always release synchronization.
- Concurrent allocations remain unique.
- Fault-injection tests cover file creation and mmap failure.
- Benchmarks cover 1, 4, and 16 allocator threads plus forced rollover.

## 12. Add low-overhead concurrency observability and tests

**Performance priority:** P0 enabler

### Metrics

Use low-cost primitives:

- Relaxed atomic total wait nanoseconds.
- Relaxed atomic wait counts.
- `fetch_max` for the worst observed wait.
- Slow-event logs above a fixed threshold.
- Sampled histograms outside extremely hot paths.
- Existing task-level spans for expensive operations.

Avoid:

- A metrics histogram call on every cache hit.
- High-cardinality table or project labels.
- Dynamic string construction in hot paths.
- Timers that allocate.
- Lock-protected metrics.

Measure at least:

1. Cache-stat lock contention before its removal.
2. Delta snapshot lock wait by operation.
3. Journal mutex wait, records per sync, and fsync duration.
4. Manifest lock wait and object-store GET/PUT duration.
5. Per-semaphore wait and hold duration.
6. Maintenance enqueue-to-claim latency.
7. Coordination-registry cardinality.
8. WAL allocator wait and rollover duration.

### Tests

- Loom tests for small custom atomic state machines.
- Tokio paused-time tests for notifications and shutdown.
- Fault injection for WAL rollover, journal fsync, object-store CAS conflicts, and Delta reload.
- Concurrent version-publication tests that reject stale swaps.
- Saturation tests that verify foreground progress under maintenance backlog.

## Patterns to preserve unless profiling disproves them

The following synchronization choices are currently appropriate and should not be replaced merely to remove locks:

- The 256 striped WAL append locks. They serialize only short in-memory walrus appends and distribute unrelated WAL collections.
- Per-bucket `parking_lot::Mutex` protection in `MemBuffer`. It keeps batch and WAL-hold snapshots consistent without async suspension.
- The logical-count cache admission mutex. It protects an aggregate resident-byte invariant that independent atomics cannot update transactionally.
- DML coalescer state extraction before asynchronous processing.
- Bounded MPSC channels for batch ingestion and Tantivy streaming.
- Per-key object-cache mutexes used for single-flight fetches. Holding the mutex across the leader's fetch is the intended behavior.
- Separate root heavy-query admission and per-batch scan admission. They govern different memory hazards and release at different granularities.
- `OnceLock` and `LazyLock` initialization.
- Relaxed atomics used only for counters, gauges, and scheduling hints.
- Separate light and heavy maintenance lanes where production evidence shows they prevent starvation.

## Recommended execution order

1. ~~Convert cache statistics to relaxed atomics.~~ DONE (`ccebc093`).
2. Instrument manifest publication, Delta lock waits, and the maintenance gates that lack metrics (journal group commit and the hygiene lane already export theirs).
3. If `journal_commits_coalesced` shows direct checkpoint callers missing the existing group commit, route them through `commit_journal`.
4. ~~Release the Delta guard before light-compaction action enumeration.~~ DONE (`ccebc093`).
5. Batch Tantivy manifest changes and add conditional cross-process publication.
6. ~~Fix the WAL allocator spinlock for correctness.~~ DONE (`ce5e4781`).
7. Improve resumed-bin probe parallelism and maintenance wake latency where metrics justify it.
8. Tune or consolidate maintenance gates only after identifying the binding resource.
9. Prototype immutable published snapshots only if table-lock wait is material.
10. Reap coordination registries only if cardinality metrics show meaningful growth.

## Validation expectations

For each implementation change:

- Record the exact benchmark or workload used.
- Compare throughput plus p50, p95, p99, and maximum latency.
- Record CPU utilization and runtime lag, not only wall-clock throughput.
- Test under both uncontended and deliberately contended conditions.
- Run the relevant local checks through `make ci-signoff CHECKS="..."` and record commands, results, and outstanding GitHub checks in the PR description.
- Never publish a local CI attestation for a check that did not pass.

## Prior art

- RocksDB WriteThread groups concurrent writes so WAL persistence and visibility publication can be shared: <https://github.com/facebook/rocksdb/blob/main/db/write_thread.h>
- RocksDB write stalls apply feedback when flush or compaction cannot keep up: <https://github.com/facebook/rocksdb/wiki/Write-Stalls>
- RocksDB shards its block cache to reduce mutex contention: <https://github.com/facebook/rocksdb/wiki/Block-Cache>
- RocksDB uses separate priority pools for foreground-sensitive flushes and background compaction: <https://github.com/facebook/rocksdb/wiki/Thread-Pool>
- ClickHouse stores table data as immutable parts and publishes metadata around those parts: <https://github.com/ClickHouse/clickhouse-docs/blob/main/docs/managing-data/core-concepts/parts.md>
- ClickHouse asynchronous inserts batch by size, time, and query count while retaining backpressure: <https://clickhouse.com/blog/asynchronous-data-inserts-in-clickhouse>
- ClickHouse distinguishes query admission, per-query concurrency, and workload resource scheduling: <https://clickhouse.com/resources/engineering/high-concurrency-sizing-user-analytics>
- Tokio's semaphore fairness and `acquire_many` head-of-line behavior: <https://docs.rs/tokio/latest/tokio/sync/struct.Semaphore.html>
- Tokio's `RwLock` is fair/write-preferring: <https://docs.rs/tokio/latest/tokio/sync/struct.RwLock.html>
- ArcSwap provides lock-free immutable pointer publication for read-heavy state: <https://docs.rs/arc-swap/latest/src/arc_swap/docs/performance.rs.html>
