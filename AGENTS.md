# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:

- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

## 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

## 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:

- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:

- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

## 4. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:

- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:

```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

**Run Rust tests with `cargo nextest run` (or a `make test*` target that invokes it), never `cargo test`.**

**Default to TARGETED tests while iterating; run the full suite only before you
push.** `make test` is ~150-220s and `make test-e2e` another ~140s, so a
change-verify loop built on them costs minutes per iteration and most of that is
re-running tests the change cannot reach. Use
`cargo nextest run --lib <substring>` (sub-second for pure-logic work) or
`cargo nextest run <substring>` for one integration test, plus `cargo check
--lib` for compile-only feedback. Reserve `cargo lint` + the full suite for the
pre-push gate — CI runs them anyway, and a green targeted run plus a single
final full run is the same coverage at a fraction of the wall clock.

### Batch work and release validation

- **Freeze the release batch.** Do not apply optional refactors while release checks run. Change the frozen batch only to fix a required gate failure.
- **Reuse valid results.** Rerun only checks affected by changed code, dependencies, toolchains, or build settings. Record the checked inputs, command, result, and log location. Missing output is not a passing result. Preserve valid results when another check fails.
- **Batch dependency pins.** Update related dependency revisions together, regenerate the lockfile, then run one integrated release validation. Do not repeat full validation after each individual pin.
- **Avoid competing builds.** Do not queue multiple Cargo processes against the same build cache, including from separate worktrees. Use the wait for independent review or documentation work. Do not restart a live check because it produces no output.
- **Deploy fewer, coherent releases.** Combine changes with compatible safety and rollback requirements. Keep broader rollup optimizations separate from the first resource-safety release. Defer optional changes instead of repeatedly expanding a release that is ready for validation.

These rules reduce repeated work, not acceptance requirements. Keep correctness, resource limits, rollback, and local CI signoff as release gates.

---

**These guidelines are working if:** fewer unnecessary changes in diffs, fewer rewrites due to overcomplication, and clarifying questions come before implementation rather than after mistakes.

## Project Overview

TimeFusion is a time-series database written in Rust that combines Apache DataFusion (query engine), Delta Lake (storage), and PostgreSQL wire protocol. It's designed for high-performance storage and querying of events, logs, traces, and metrics on S3-compatible object storage.

## Build & Test Commands

```bash
# Build
cargo build                    # Debug build
cargo build --release          # Release build
make build-prod               # Production build

# Testing — ALWAYS `cargo nextest run`, never `cargo test`
# (all default to LOCAL MinIO — see "Local-first testing" below)
# GOTCHA: ~15 database unit tests need the `timefusion-tests` bucket, which
# e2e/sqllogictest MinIO resets silently delete. If they fail with NoSuchBucket:
#   AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
#     aws s3 mb s3://timefusion-tests --endpoint-url http://localhost:9000
make test                     # THE inner loop: whole suite, ~617 tests
cargo nextest run <substring> # one test / one file, e.g. `cargo nextest run dedup_compaction`
make test-unit                # lib only
make test-all                 # also runs the #[ignore]d tests

# Linting — ALWAYS `cargo lint`, never a bare `cargo clippy`
cargo lint                     # == CI's Clippy step, exactly (alias in .cargo/config.toml)
cargo lint-fix                 # == the autoformat workflow's clippy --fix
make prepush                  # runs `cargo lint` first, then the whole suite

# Run the server
cargo run                      # Uses .env (defaults to local MinIO)
make run-prod                 # Production config (explicit; loads .env.prod)
```

Connect via: `psql "postgresql://postgres:postgres@localhost:5432/postgres"`

### Sign off locally before pushing — never push and wait on CI

**A push to `master` IS a deploy, and remote CI is far too slow to be a feedback
loop.** So `make ci-signoff` must be green *before* you push. It runs the checks
locally and publishes attestations, which CI's gate then reuses — a signed-off
push skips straight to the image build instead of re-running everything.

```bash
make ci-signoff                     # everything, attesting each pass
make ci-signoff CHECKS="fmt clippy" # scoped to a limited change
```

This is a rule, not a preference. Two caveats:

- **`make prepush` is not sufficient** for a master push — it is fmt + lint only,
  and proves nothing about `test`, `pg-smoke`, or `e2e`.
- **If a check genuinely cannot run here** (missing service or tool), say so in
  the PR/commit and let CI run that one. Never publish an attestation for a check
  that did not pass.

### Local CI quick reference

Run checks locally before pushing changes. Prefer laptop build caches and local services.
Use `make ci-signoff` to run checks and publish passing results for GitHub to reuse.
For a limited change, use `make ci-signoff CHECKS="..."` with the relevant checks from `ci/checks.tsv`.
The final status output lists every check that still needs GitHub.

**Use local signoff as much as possible, not remote CI.** Local runs are fast; remote CI is slow and is not a feedback loop.
Gate every master push (which is a deploy) on `make ci-signoff` for the exact tree being pushed.
Dispatch remote CI only for checks that cannot run locally, and do not wait on it when a local signoff already covers the change.
A full `make ci-signoff` takes about 11 minutes on an idle machine (measured 2026-09-29). Run one signoff at a time: several in parallel took 69–90 minutes each.

Record the local commands, results, and outstanding checks in the PR description.
If a required service or tool is unavailable, record that limitation and let GitHub run the affected checks.
Never publish an attestation for a check that did not pass.

Use standard GitHub-hosted runners for remote jobs. Do not add Blacksmith runners or actions without an explicit user request.
See [the local CI guide](docs/local-ci.md) for setup, capabilities, and cache controls.

In a fresh worktree, run `make warm` before the first build: it clones an idle sibling's `target/` copy-on-write, so you compile this crate rather than ~900 dependencies (`make ci` does it too).
Leave `CARGO_INCREMENTAL` unset; `0` makes every edit rebuild the whole crate (~3 min instead of ~40s).
Per-check wall times land in `.ci/timings.tsv` and per-test times in `target/nextest/ci/junit.xml`; read them before guessing where signoff time goes.

### Running CI locally, and the attestation cache

`ci/checks.tsv` is the single definition of what CI is; `.github/workflows/ci.yml`
and `scripts/ci/ci.sh` both read it, so a check cannot exist in one and not the
other. Each check is fingerprinted over the content it depends on, and a pass
publishes an attestation under `refs/ci-attest/v1/*`. CI's gate job skips any
check already proven for the exact tree it is about to test — by an earlier run,
another branch, or a developer's `make ci`.

```bash
make ci          # fmt, clippy, test, pg-smoke, e2e — with the MinIO CI starts
make ci-status   # what CI would run right now, without running any of it
make ci-selftest # fingerprint/capability logic + checks.tsv <-> run_body agreement
```

`make prepush` remains the fast pre-push gate; `make ci` is the whole thing and
its results are published, so they count.

**Adding or changing a check means editing `ci/checks.tsv` and `run_body` in
`scripts/ci/ci.sh` together** — `make ci-selftest` enforces they agree, and that
every declared input path exists (a typo silently narrows what a check depends
on, which is how an untested change ships).

Load-bearing, easy to break:

- **Capabilities are the safety property.** A check declares what it needs
  (`rust`, `protoc`, `nextest`, `minio`, `docker`); an attestation records what
  the environment had. Reuse requires provided ⊇ required. Never widen a `caps`
  string to make something pass.
- **A partitioned run proves only its slice.** CI splits `test` with
  `--partition hash:N/2`, so the shards run with `CI_NO_ATTEST=true` and a
  separate job records the check only once **both** are green. Do not attest from
  inside a shard.
- **Narrow `inputs` only where provably sound.** `fmt` is the one clear case —
  rustfmt reads `.rs` and `rustfmt.toml`, not `Cargo.lock` or `proto/`. Everything
  else takes `@rs`, which deliberately includes `.cargo/` (THE `cargo lint`
  definition) and `proto/` (build.rs codegens from it).
- **Fingerprints are pinned once per run**, so a step that rewrites the tree
  cannot make a job attest a fingerprint no gate asked for.
- **Never let bookkeeping fail a green check.** Attestation publishing is
  best-effort; a network or git failure must not turn a passing suite red.
- **`rust-toolchain.toml` is an input to every check**, so a toolchain bump
  invalidates everything by itself — which is correct.
- **`docs/local-ci.md` is the reference**, including the kill switches
  (`CI_ATTEST_DISABLED` repo variable, `EPOCH` in `ci.sh`).

### Local-first testing (default) — fast turnaround

**Tests never touch prod object storage by default.** Two guarantees:

1. **`.env` defaults to local MinIO** (`http://127.0.0.1:9000`). Prod R2/DynamoDB
   creds were removed from `.env` and live only in `.env.prod`. Reaching non-local
   storage takes explicit effort: `make run-prod` / `make build-prod` / `make test-prod`,
   or exporting the real `AWS_*` creds yourself.
2. **The `sqllogictest` harness auto-starts MinIO itself** — no `make minio-start`,
   no remembering anything. Resolution order is **local-first, Docker last**:
   1. `TIMEFUSION_TEST_S3_ENDPOINT` if set (reuse any running MinIO).
   2. An already-running MinIO on `127.0.0.1:9000` (e.g. `make minio-start`).
   3. The local **`minio` binary** — spawned on `:9000`, killed when the test ends.
   4. **Docker** (testcontainers) — *only* when no `minio` binary is on PATH.

   So Docker is a fallback, not the default: if you have `minio` installed, it's used.

```bash
# Zero-setup: auto-starts MinIO, runs all .slt files (each is its own test,
# so they run concurrently)
cargo nextest run sqllogictest

# Run ONE .slt file — the test is named after the file. (This replaces the old
# SLT_FILTER / SQLLOGICTEST_FILE env vars, which no longer exist.)
cargo nextest run distinct_on_variant

# Reuse an already-running MinIO (CI's, or a hand-run container) instead of
# spinning up a new one — set the endpoint explicitly:
TIMEFUSION_TEST_S3_ENDPOINT=http://127.0.0.1:9000 cargo nextest run sqllogictest
```

> Each `tests/slt/*.slt` file is a generated `#[test]` (see `slt_files!` in
> `tests/suite/sqllogictest.rs`). Adding a new .slt file without adding it to
> that list fails `every_slt_file_has_a_test`, so a file can't be silently unrun.

**Optional persistent MinIO** (skip per-run container startup across many iterations):

```bash
make minio-start   # local `minio` binary on :9000
make minio-stop
# then point the harness at it so it skips its own container:
export TIMEFUSION_TEST_S3_ENDPOINT=http://127.0.0.1:9000
```

### Connecting to production

The prod TimeFusion PGWire endpoint is **`timefusion.s.past3.tech:5432`** (user
`postgres`, db `postgres`). The full connection string — including the password —
lives in the **monoscope** repo's `.env` under the **`TIMEFUSION_PG_URL`** key
(monoscope dual-writes/reads to TF, so it always has the current prod URL):

```bash
grep TIMEFUSION_PG_URL ../monoscope/.env   # postgresql://postgres:<pw>@timefusion.s.past3.tech:5432/postgres
psql "$(grep -m1 '^TIMEFUSION_PG_URL=' ../monoscope/.env | cut -d= -f2-)" -c "SHOW server_version;"
```

`SHOW server_version` returns the live `datafusion`/`datafusion-postgres` versions
(handy for confirming a deploy landed). Treat prod as read-only when testing —
don't write test rows into prod tables.

### Reading prod logs / host (CapRover)

TF runs as a Docker Swarm service on the CapRover host. **SSH (READ-ONLY):**
`ssh ubuntu@captain.s.past3.tech` (key-based; `ubuntu` is in the docker group — no sudo).
Service is **`srv-captain--timefusion`**; its image tag is the deployed git short-SHA
(e.g. `ghcr.io/monoscope-tech/timefusion:6f66d4e`), so `docker service ls` confirms
what's actually live.

```bash
ssh ubuntu@captain.s.past3.tech 'docker service ls | grep timefusion'                                   # deployed SHA
ssh ubuntu@captain.s.past3.tech 'docker service logs srv-captain--timefusion --since 30m 2>&1 | grep -iE "ERROR|flush|panic"'
ssh ubuntu@captain.s.past3.tech 'docker service ps srv-captain--timefusion --no-trunc'                   # restart/OOM history
```

Live in-process diagnostics are also exposed over pgwire as the **`timefusion_stats`**
table (`SELECT component,key,value FROM timefusion_stats` — mem_buffer/buffered_layer/
wal/pgwire counters; `pgwire.queries_total` resets on restart, so it doubles as an
OOM-restart detector). **Treat the host as strictly read-only** — logs / `inspect` /
`ps` only; never restart, scale, redeploy, `exec`-mutate, or touch volumes. Heavy ad-hoc
`SELECT`s over `otel_logs_and_spans` can themselves push the memory-tight instance into
OOM — prefer `timefusion_stats` and tightly time-bounded queries.

## Architecture

```
PostgreSQL Clients → PGWire Protocol → DataFusion Query Engine
                                              ↓
                    ┌─────────────────────────┼─────────────────────────┐
                    ↓                         ↓                         ↓
            BufferedWriteLayer          Object Store Cache        Delta Lake on S3
            (WAL + MemBuffer)           (Foyer L1/L2 cache)       (Parquet format)
```

### Repository Layout

Where things live, one line each. `src/` is expanded under Module Structure.

```
.
├── src/                    the crate (see below)
├── tests/
│   ├── suite/              THE integration target — 32 files, each a `mod` of main.rs
│   ├── e2e/                full prod path on virtual time; harness.rs owns MinIO + bootstrap
│   └── slt/                16 .slt files, each generated into its own #[test]
├── benches/                8 criterion benches (`cargo bench`)
├── bench/                  python/shell load-gen and audit tooling — not cargo
├── schemas/                YAML table schemas, compiled in via include_dir!
├── proto/                  timefusion.proto; build.rs codegens from it
├── vendor/                 forked deps: pgwire, walrus-rust, tikv-jemalloc-sys
├── docs/                   architecture notes, incident reports, docs/plans/ (100+), dashboards
├── tasks/                  older task briefs, superseded by docs/plans
├── scripts/                prod benchmarking and probe scripts
├── deploy/                 CapRover service override + pgwire proxy config
├── .github/workflows/      ci.yml, deploy.yml, autoformat.yml, build-image.yml, …
├── .cargo/config.toml      THE `cargo lint` definition + the ld64.lld linker choice
├── Makefile                every test/lint/run entry point — read before inventing one
├── rust-toolchain.toml     pinned toolchain; an input to every CI check
├── Dockerfile              prod image; docker-compose.yml is local MinIO/dev only
└── data/ minio/ target/    gitignored local state — `target/` has reached 200 GB+;
                            check `df -h` before trusting a suite failure
```

### Module Structure

One folder per concern; a folder's `mod.rs` holds the concern itself and its
siblings are slices of the same module, not a layered API. Files are large on
purpose — related code lives together so it can be reused rather than
re-implemented.

```
src/
├── main.rs                # Entry point, CLI subcommands, server startup
├── config.rs              # OnceLock<AppConfig> singleton + autotune + secret encryption
├── schema.rs              # YAML schema registry (include_dir!)
├── storage.rs             # Foyer L1/L2 object-store cache + the delta-snapshot
│                          #   and best-effort JSON sidecars (certifications, dirty bins)
├── observability.rs       # OTel metrics + traces, pprof/jemalloc profiling, error helpers
├── support.rs             # Virtual clock + test helpers
├── dml.rs                 # UPDATE/DELETE interception + the DML coalescer
├── maintenance_coordinator.rs  # Durable byte-bounded work units: task journal, claim,
│                          #   leases, AdmissionController
├── maintenance_sim.rs     # Replays a TaskJournal through the real scheduler on virtual time
├── rollup.rs              # Rollup specs, the aggregate SQL that builds them, read routing
├── rollup_journal.rs      # Crash-safe dirty-range journal for rollup maintenance
├── database/              # The DB engine — slices of ONE module (`use super::*`)
│   ├── mod.rs             #   Database, types, construction, session + table resolution
│   ├── write.rs           #   insert path, coalesced commits, watermarks
│   ├── compact.rs         #   OPTIMIZE, hot-tail packing, dedup rewrites, sort machinery
│   ├── rollup.rs          #   maintenance planning, coordinator ticks, rollup waves
│   ├── maintain.rs        #   dedup sweeps, footer repair, vacuum, shutdown
│   ├── index.rs           #   Tantivy/bloom sidecar backfill queueing, GC, coverage census
│   ├── histogram.rs       #   Fixed-width count histograms over Delta
│   ├── scan.rs            #   ProjectRoutingTable, scan-pressure valve, GatedScanExec
│   └── tests.rs           #   the Database integration tests
├── write/
│   ├── mod.rs             #   BufferedWriteLayer (+ batch queue, INSERT coercion, gRPC ingest)
│   ├── mem_buffer.rs      #   In-memory storage with time-bucketed batches (5 min default)
│   └── wal.rs             #   Write-ahead log (walrus-rust)
├── read/
│   ├── mod.rs             #   Read-side dedup, count pushdown, logical-count index, HLL
│   ├── admission.rs       #   Heavy-query admission (concurrent unbounded sorts vs the pool)
│   ├── bloom_prune.rs     #   Per-(project,date) bloom sidecars for file-level needle pruning
│   ├── functions.rs       #   Custom SQL functions + VariantAwareExprPlanner
│   ├── plan_cache.rs      #   Cross-connection plan cache
│   └── optimizers.rs      #   Every analyzer/optimizer rule (variant, pg, tantivy, topk…)
├── server/
│   ├── mod.rs             #   bootstrap(), pgwire handlers, early bind, gRPC service
│   └── pg_compat.rs       #   pg_catalog compatibility + the timefusion_stats table
└── tantivy/
    ├── mod.rs             #   Index build/schema/manifest/blob store/mem index
    ├── search.rs          #   Search + reader + the indexing service
    ├── planner.rs         #   Recognizes fixed-width count histograms in the plan
    ├── histogram.rs       #   Exact histograms over indexed docs + visible row ordinals
    ├── visibility.rs      #   Physical-row winner masks via the merge-on-read operator
    └── udf.rs             #   text_match UDF and predicate extraction
```

### Core Components

- **database/**: Delta table management, query execution and multi-tenant routing via `(project_id, table_name)` keys, plus every maintenance operation that rewrites Delta
- **write/mod.rs**: Orchestrates WAL + MemBuffer for sub-second write latency with durability
- **write/mem_buffer.rs**: In-memory storage with time bucketing (5 min default, configurable) using DashMap for lock-free reads
- **write/wal.rs**: Write-ahead log using walrus-rust with topic partitioning (`{project_id}:{table_name}`)
- **storage.rs**: Foyer hybrid cache (memory + disk) implementing `ObjectStore`, and the persisted sidecars
- **dml.rs**: UPDATE/DELETE query interception via custom `DmlQueryPlanner`, plus coalescing
- **read/functions.rs**: Custom SQL functions (time_bucket, array ops, hashing, percentiles, variant ops)
- **schema.rs**: YAML schema registry compiled into binary via `include_dir!`

### Data Flow

**Insert**: Client → PGWire → DataFusion parser → BufferedWriteLayer → WAL.append() (durable) → MemBuffer.insert() (fast) → Response
**Select**: Client → PGWire → DataFusion → ProjectRoutingTable (routes by project_id) → Union of MemBuffer + Delta with time-range exclusion
**Flush**: Every `flush_interval_secs` (default 60s), completed time buckets flush to Delta, evict from MemBuffer, checkpoint WAL

### Multi-Tenant Storage Model

**Two table types:**

1. **Unified tables**: Default projects share one Delta table per schema

   - Partitioned by `[project_id, date]`
   - Path: `s3://bucket/timefusion/default/{table_name}/`

2. **Custom project tables**: Isolated tables for specific projects
   - Own S3 bucket/path
   - Path: `s3://bucket/timefusion/projects/{project_id}/{table_name}/`

**Routing:** `WHERE project_id = 'xxx'` is **mandatory** in queries for multi-tenant isolation.

### MemBuffer & TableBuffer Architecture

**Hierarchy:**

```
MemBuffer
  └── tables: DashMap<TableKey, Arc<TableBuffer>>
        └── TableBuffer
              └── buckets: DashMap<i64, TimeBucket>
                    └── TimeBucket
                          ├── batches: parking_lot::Mutex<Vec<RecordBatch>>
                          ├── wal_shard_state: parking_lot::Mutex<WalShardState>
                          ├── row_count / memory_bytes: AtomicUsize
                          ├── min/max_timestamp: AtomicI64   (ROUTING time — drives the MoR mask)
                          └── row_min_ts/row_max_ts: AtomicI64 (true row span — read pruning ONLY)
```

`min/max_timestamp` and `row_min/max_ts` are deliberately separate: widening the
routing span to the true row span masks Delta rows that share a timestamp with
buffered ones, losing rows.

**Key concepts:**

- `TableKey = (Arc<str>, Arc<str>)` - (project_id, table_name) composite key
- **Time buckets**: `bucket_id = timestamp_micros / bucket_duration_micros()` — 5 min default, set once at boot from `TIMEFUSION_BUCKET_DURATION_SECS`
- **DashMap for lock-free reads**: Concurrent access without global locks
- **Schema compatibility**: Validates incoming schema against existing (allows nullable field additions)
- **RecordBatch cloning is cheap**: Only clones Arc pointers (~100 bytes), not underlying data

**Memory tracking:**

- `estimated_bytes` tracks total memory across all tables
- Batch size estimated via `batch.get_array_memory_size()`
- Memory freed atomically when buckets are drained

**Operations:**

- `insert()` → `get_or_create_table()` → `TableBuffer.insert_batch()` → bucket lookup/creation
- `query()` → iterate all buckets, collect batches (cheap clone)
- `query_partitioned()` → returns `Vec<Vec<RecordBatch>>` for parallel execution
- `snapshot_bucket_for_flush()` / `finish_flushed_snapshot()` → the non-destructive flush path: snapshot a prefix, commit, then drain only if `mutation_gen` did not move
- `take_bucket_for_flush()` / `restore_taken_bucket()` → the destructive variant, restored on commit failure
- `evict_old_data()` → removes buckets older than cutoff

### BufferedWriteLayer Write Path

```
insert(project_id, table_name, batches)
  │
  ├─ Check memory pressure → trigger early flush if needed
  │
  ├─ try_reserve_memory() → CAS with exponential backoff
  │     └─ Hard limit = max_bytes + max_bytes/5 (120%)
  │
  ├─ WAL.append_batch() → durable write (fsync per entry — see below)
  │
  ├─ MemBuffer.insert() → fast in-memory write
  │
  └─ release_reservation() → memory now tracked by MemBuffer
```

**Memory reservation:**

- Atomic CAS prevents race conditions
- 15% overhead multiplier for costs `estimate_batch_size` can't see (Vec headers, DashMap nodes, fragmentation)
- Exponential backoff on contention (spin_loop → sleep up to ~1ms)

**Background tasks:**

- `run_flush_task()`: Every `flush_interval_secs` (default 600s), flush completed buckets
- `run_eviction_task()`: Evict buckets older than `retention_mins` (default 70 mins)
- `DeltaWriteCallback`: Must complete Delta commit before returning (critical for durability)

### WAL Implementation

**Format:**

```
[WAL_MAGIC: 4 bytes "WAL2"]
[VERSION: 1 byte (128)]
[OPERATION: 1 byte (0=Insert, 1=Delete, 2=Update)]
[BINCODE_PAYLOAD: WalEntry]
```

**WalEntry structure:**

- `timestamp_micros`: Entry creation time
- `project_id`, `table_name`: Routing keys
- `operation`: Insert/Delete/Update
- `data`: CompactBatch for Insert, DeletePayload/UpdatePayload for DML

**Topic partitioning:**

- Human-readable: `{project_id}:{table_name}`
- Walrus key: 16-char hex hash (walrus has 62-byte metadata limit)
- Topics persisted to `.timefusion_meta/topics` for discovery

**Safety:** `MAX_BATCH_SIZE = 1GiB` bounds per-entry allocation from corrupted WAL frames (ceiling = walrus `MAX_ALLOC`); INSERT entries are split at `WAL_SPLIT_TARGET = 100MB` on append so replay normally decodes small chunks.

### Optimizer Pipeline (Query Transformations)

**Analyzer rules (run before type checking):**

1. `VariantInsertRewriter`: Intercepts INSERT DML

   - Finds columns where target is Variant type
   - Wraps Utf8/Utf8View literals with `json_to_variant()`
   - Applies to Values and Projection nodes recursively

2. `VariantTableScanSchemaPatch` (always-on): restores Variant types on TableScan
   projected schemas that `ProjectRoutingTable::schema()` un-types to `Utf8View`
   for the INSERT-VALUES checker. Recomputes parent schemas bottom-up so the
   restored type propagates through cached DFSchemas.

3. `VariantPgwireRootWrap` (registered only on pgwire-facing sessions):
   wraps the outermost Projection's Variant-typed exprs with `variant_to_json()`
   for the wire. Peels Sort/Limit/Distinct/SubqueryAlias/Filter so intermediate
   UDFs still see binary Variant. Internal SQL contexts omit this rule.

**Physical planner:**

- `DmlQueryPlanner`: Intercepts UPDATE/DELETE, creates `DmlExec`

**Expression planner:**

- `VariantAwareExprPlanner` (in read/functions.rs): Handles `->` and `->>` operators
- Converts to `variant_get(col, "path.to.field")`

### Variant Type System

**Detection:** `is_variant_type()` in schema.rs checks for Struct with `metadata` and `value` fields.

**Key UDFs:**

- `json_to_variant(utf8)` → Variant struct
- `variant_to_json(variant)` → Utf8 JSON string
- `variant_get(variant, path)` → Variant sub-value

## Key Configuration (via environment)

- `AWS_S3_BUCKET`, `AWS_S3_ENDPOINT`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
- `PGWIRE_PORT` (default 5432)
- `GRPC_PORT` (default 50051), `GRPC_TOKEN` (optional bearer token; if unset, gRPC ingest is open)
- `TIMEFUSION_DATA_DIR` (base directory for WAL and cache, default `./data`)
- `TIMEFUSION_FLUSH_INTERVAL_SECS` (default 60; `.env` sets 300), `TIMEFUSION_BUFFER_MAX_MEMORY_MB` (default 4096), `TIMEFUSION_BUCKET_DURATION_SECS` (default 300)
- `TIMEFUSION_BUFFER_RETENTION_MINS` (default 70), `TIMEFUSION_FLUSH_IMMEDIATELY` (debug mode)
  > envy derives each env name from the STRUCT FIELD name, so the `BUFFER_` segment
  > exists only where the field itself carries it (`timefusion_buffer_retention_mins`
  > does; `timefusion_flush_interval_secs` does not). A knob spelled with a segment
  > the field lacks parses as nothing and silently leaves the default in place.
  > `.env` has always used the correct spellings — this doc did not.
- `TIMEFUSION_FOYER_*` (Foyer cache settings: memory_mb, disk_gb, ttl_seconds)
- `TIMEFUSION_S3_CONNECT_TIMEOUT` (humantime, default `3s` — tuned for same-region S3; widen behind slow proxies or cross-region links)
- `TIMEFUSION_MAINTENANCE_REWRITE_CONCURRENCY` (default 2 — concurrent heavy maintenance rewrites; peak transient heap ≈ `block_size_mb × this`)
- `TIMEFUSION_LIGHT_OPTIMIZE_CONCURRENCY` (default 1 — concurrent hot-tail per-project sorts; the light pool slice scales with it: `maintenance_pool/3 × N`, heavy pool gets the remainder)
- `TIMEFUSION_REPAIR_RESUME_ENABLED` (default **true** — before staging a bin, COMMIT a matching staged-but-uncommitted rewrite instead of redoing the 40+ min work; set `=false` to revert to re-stage + reconcile-and-delete). A repair bin is one 40+ min whole-file rewrite and prod replaces the task every 15-28 min, so with resume off every pass discarded a complete staged output. Note `repair_resumed_total`/`rollup_resumed_total` reading 0 does **not** mean the flag is off — check the decline counters too: 0 resumed *with* 0 declined means the path was never reached.
- `TIMEFUSION_PLAN_CACHE_CAPACITY` (default 1024 — cross-connection plan-cache templates), `TIMEFUSION_PLAN_CACHE_TIME_FNS` (default **true** — parameterize `now()` for shape caching on the hot dashboard path; it was canaried and turned on after 2026-07-19 flamegraphs put ~25% of CPU in `SessionState::optimize` on now()-bearing misses. Set `=false` only as an emergency kill switch)
- `TIMEFUSION_LANDED_SKIP_ENABLED` (default **false**) — decline a flush whose batch set is provably already committed, i.e. the duplicates WAL replay re-inserts after an unclean exit (58% of duplicate groups in a sampled prod file; see `docs/plans/2026-09-02-stop-manufacturing-duplicates.md`). Only ever active on a DIRTY boot — a clean boot skips the Delta history scan that loads the identities, so it costs nothing. Watch `wal.landed_skips` against `wal.replay_rows` in `timefusion_stats`. Validate in staging, not prod: the skip only fires after an unclean restart, which cannot be induced on the read-only prod host.
- `TIMEFUSION_CPU_PROFILE` (default on; set `false`/`0` to skip the pprof CPU sampler). Signal-handler + libunwind code that runs at boot — the shape of a SIGSEGV with no Rust panic. Prod crashlooped `exit 139` on 2026-08-11 with `starting cpu profiler` as the last line of every attempt, and there was no way to test that without shipping an image into an outage. Heap profiling is jemalloc's own and is unaffected.

## Key Constants

```rust
// MemBuffer
DEFAULT_BUCKET_DURATION_MICROS = 5 * 60 * 1_000_000  // 5 min; override via
                                   // TIMEFUSION_BUCKET_DURATION_SECS. Read it through
                                   // bucket_duration_micros(), never the const.

// BufferedWriteLayer
MEMORY_OVERHEAD_MULTIPLIER = 1.15 // safety margin over estimate_batch_size()
HARD_LIMIT_HEADROOM_DIVISOR = 5   // hard_limit() = max + max/5 = 120%
MAX_CAS_RETRIES = 100

// WAL
WAL_MAGIC = [0x57, 0x41, 0x4C, 0x32]  // "WAL2"
WAL_VERSION = 128
MAX_BATCH_SIZE = 1024 * 1024 * 1024   // 1GiB hard cap (replay acceptance; walrus MAX_ALLOC ceiling)
WAL_SPLIT_TARGET = 100 * 1024 * 1024  // INSERT entries split at append to this size (transparent)
// `timefusion_wal_fsync_mode` defaults to "sync_each" (config.rs) and prod does NOT
// override it, so walrus fsyncs INSIDE the append, before it returns. The interval for
// the Milliseconds mode is `timefusion_wal_fsync_ms`.
```

## Code Philosophy (PRIORITY)

**CODE CONCISENESS, SUCCINCTNESS, AND ZERO BOILERPLATE ARE TOP PRIORITIES.**

This codebase is maintained by a small team. Every line of code is a liability - less code means less to maintain, debug, and understand. When in doubt, write less code. **Boilerplate is unacceptable.**

- **No boilerplate**: Eliminate repetitive patterns ruthlessly. If you're writing similar code twice, abstract it
- **Conciseness over verbosity**: Favor compact, expressive code. If it can be done in fewer lines without sacrificing clarity, do it
- **Smart combinators and functional patterns**: Use iterator adapters, `?` operator, and functional chains to reduce imperative boilerplate
- **Use libraries liberally**: Prefer well-maintained crates over hand-rolling. Adding a dependency is better than 200 lines of custom code
- **Advanced Rust techniques encouraged**: derive macros, generics, `serde` attributes, proc macros, trait-based abstractions - use them aggressively to eliminate boilerplate
- **Prefer editing existing code** over creating new files/modules. Extend and generalize existing functions
- **Create general functions** that handle multiple use cases rather than similar variants
- **Delete unused code completely**: No backward-compat shims, no commented-out code, no `_unused` prefixes
- **Minimal comments**: Only where logic isn't self-evident. The code should be self-documenting
- Type alias: `TableKey = (Arc<str>, Arc<str>)` for (project_id, table_name)
- Global config via `OnceLock<AppConfig>` singleton pattern

## Build iteration tips

- **Dev builds (`cargo build`) compile ~5–15× faster than release.** Use them for code-correctness iteration; `tests/membuffer_concurrency_bench.rs` works on a dev binary in <2 min cycle time. Switch to release only for absolute-perf validation.
- **The dev loop is link-bound, not compile-bound.** `.cargo/config.toml` links with `ld64.lld` (macOS arm64; needs `brew install llvm@15`) and `[profile.dev]` uses `debug = "line-tables-only"` — together ~halving the warm recompile of the large `database.rs` crate (~22s → ~12s). Each Cargo test *target* adds a separate full-dep link, which is why all integration tests live in one target (`tests/suite/main.rs`); don't add `[[test]]` entries casually.
- **A bare `cargo clippy` does not tell you whether CI will pass.** CI runs
  `--all-targets --all-features --locked -- -D warnings`; without those it merely
  *prints* lints and exits 0, so code that CI rejects looks clean locally (2026-08-01:
  a 52-file sweep passed local clippy and failed CI on six lints, two of which had
  already been red on master for four commits). Use `cargo lint` — one definition,
  aliased in `.cargo/config.toml`, invoked by CI, the autofix workflow and `make prepush`.
- **Just run the whole suite.** `make test` is ~74s of tests on a warm build; hand-picking targets no longer buys anything and only hides failures until CI. `make test-unit ARGS=<name>` (lib only) is still the cheapest loop for pure-logic work.
- **Release builds with `RUSTFLAGS="-C debuginfo=0"`** drop several minutes off LTO/codegen for this 110 MB binary.
- **Skip the MinIO restart.** MinIO can stay running across TF iterations; reuse the bucket or `aws s3 rm s3://… --recursive`. Per-run isolation via `TIMEFUSION_DATA_DIR=./data/run-N` is faster than wiping `/tmp/minio-data`.
- **Wait shorter on background commands.** `cargo build --release` for a leaf-module change is rarely >5 min; lean on 60–180s ScheduleWakeup heartbeats. The harness re-invokes on task-notification anyway — don't pre-allocate 10-min sleeps "just in case."

### Throughput/scheduler iteration — prod is the last resort, not the loop

Measuring maintenance changes by deploying to prod costs ~half a day per hypothesis: the restart kills in-flight units and resets the rollup coverage map, then needs ~2h of quiet before any throughput number means anything (2026-08-18: ~25 deploys in one night produced mostly invalid measurements). The loop, fastest first:

1. **`timefusion sim <journal.json>`** — replays a real prod journal (`maintenance_tasks.json`, fetch via the read-only SSH) through the IO-free coordinator on virtual time. Divergence, lag, and policy questions answered in seconds. Backtest against a known night's queue shape before trusting a prediction.
2. **`timefusion run-unit --source X --project Y --date Z --op BaseRollup`** — one maintenance unit against real storage with the phase timers on; the per-unit cost decomposition in minutes, no deploy.
3. **Staging** (`timefusion-staging`, same R2, scratch prefix, seeded whale/shipbubble days) — MinIO validates correctness, not cost: the per-unit fixed cost is object-store round trips, so throughput experiments need real S3 latency. Restarts there are free.
4. **Prod, only after 1–3 agree.** One change per deploy (batch variants behind env kill-switches, e.g. `TIMEFUSION_REPAIR_RESUME_ENABLED`); ≥2h quiet before trusting numbers; every experiment PR names the `timefusion_stats` metric it moves.

Convergence (draining a backlog, building 30 contiguous days) is wall-clock physics and cannot be compressed — what this buys is ~1h time-to-knowledge per decision. If `sim`/`run-unit`/staging don't exist yet, building them is Phase 0 of `docs/plans/2026-08-18-an-architecture-that-keeps-up.md` — do that first.

### Diagnosing a slow query — the cheap measurements come FIRST

Every rule here was paid for. On 2026-09-04 a "make 14d/30d dashboards load"
task produced **four consecutive failed optimizations and one prod regression**,
all because the expensive step (read the plan, form a theory, ship it) came
before the cheap one (find out what is actually saturated).

- **Find the saturated resource before optimizing for one.**
  `ssh … 'docker stats --no-stream <container>'` during the query AND while idle.
  If CPU during the query is indistinguishable from idle, it is **IO-bound and no
  amount of parallelism will help**. Measured: 1242–1806% during a 30d query vs
  1330–1905% idle — the query added *nothing*. Four rewrites (overlap bypass,
  range split, range split + pruning fix, wider file groups) all added CPU
  parallelism to a query that uses no measurable CPU; one `docker stats` sample
  refutes all four in a minute. Note maintenance alone holds ~17 of 48 cores and
  ~150–200 MB/s, so "the box is busy" is the BASELINE, not a finding.

- **Never measure a young process.** Same build, same query: 14d read TIMEOUT at
  9 min uptime and 34s at 29 min. Stamp uptime with `docker service ps` and wait
  ≥8–10 min after any deploy. A cold read and a real regression are
  indistinguishable, and prod redeploys on every non-docs push.

- **One run is not a measurement.** 14d on the *unchanged* build ranges **14–33s**
  run to run. Take ≥3, and when comparing builds/approaches **alternate the arms**
  so both see the same load — a single sample "proved" a 3x win and a 2.2x
  regression that were both inside normal variance.

- **A ladder pre-warms itself.** Running 7,10,14,…,30d in sequence made 30d
  "complete in 55s" — because the 26d run had just cached its data. Standalone it
  timed out. Randomize order or measure each window independently.

- **Vary the FORMULATION to name the code path.** `count(*)` vs `count(1)` vs
  `count(col)` vs `sum(1)` vs a subquery isolated a COUNT defect to the
  column-free/empty-projection path in ONE query — after hours of plan-reading and
  flag-flipping had produced two confidently wrong diagnoses.

- **Prod is the only instrument for pushdown behaviour.** Reverting a
  filter-pushdown fix left the e2e test GREEN: DataFusion re-runs
  `push_down_filter` in the local `ctx.sql` path and the pgwire path does not. A
  green local test proves nothing about what prod's planner does.

- **Tests must assert COST, not just correctness.** The range split was exact —
  counts tiled perfectly, no gap, no overlap — and 4x more expensive, and the
  suite was green the whole way. A wrong-cost/right-answer bug walks straight
  through correctness assertions. Assert the thing that makes it cheap (e.g.
  "each branch prunes to its OWN files"), not just that the total is right.

- **Read the config comment before turning the knob, and judge both sides.**
  `QUERY_PARTITIONS_MAX` is capped *deliberately* — non-spillable sort
  reservations scale with it and exhausted the query pool at 48 (three dated
  incidents). Raising it fixed the 30d window; the check was completion rate
  **and** the absence of query-side `Resources exhausted`, because a knob with a
  documented failure mode needs evidence on the failure it was built to prevent.

## Safety Practices

- Prefer `ArrayData::try_new` over `new_unchecked` for Arrow data construction - validates buffers and prevents UB from corrupted data
- Use named constants for magic numbers (e.g., `MAX_BATCH_SIZE`, `FSYNC_SCHEDULE_MS`)
- WAL format uses version byte (v128+) to distinguish from legacy operation bytes (0-2)
- Size limits on deserialization prevent unbounded memory allocation from malicious/corrupted data
- Don't reach for `std::env::set_var` in tests. It is process-global, and under nextest's process-per-test model it silently stops meaning what you think while still looking correct. Put the value in the `AppConfig` the test builds and pass it explicitly (`Database::with_config`, `Walrus::with_root`). If a global is genuinely unavoidable, `#[serial]` plus a SAFETY comment is the minimum.

## Testing

**Minimal-footprint tests: run the `rs-minimal-tests` skill when writing or reviewing
tests.** One line of test code should verify as much behavior as possible — prefer
doctests, `test-case`/`rstest` case tables, `proptest` properties, and `insta`
snapshots over duplicated `#[test]` fns or mocks. Decision ladder, strongest coverage
per line first: doctest → property test → case table → snapshot → real-dependency
integration test → golden test (only for truly unmanaged externals). Never mock a
dependency this codebase manages (Delta, object_store, Postgres) — use the real thing
in-memory/tempdir instead.

```bash
make test                         # THE default: whole suite in one parallel run
cargo nextest run --lib           # unit tests only
cargo nextest run dedup_compaction  # substring filter on the test name
make test-e2e                     # E2E suite (local-first MinIO; Docker only as fallback)
RUST_LOG=debug cargo nextest run --no-capture   # with logging
```

**Use `cargo nextest run`, not `cargo test`.** `cargo test` runs the integration
binary's tests in one process where `#[serial]` serializes most of them, and it
runs each binary in turn — the full suite took **553s**. nextest runs one process
per test from a single pool: the same 617 tests finish in **~74s**. Every `make`
test target invokes nextest; install it with
`curl -LsSf https://get.nexte.st/latest/mac | tar zxf - -C ~/.cargo/bin`.

Two structural facts make that parallelism safe, and both are load-bearing:

- **One integration-test target, not 26.** Every `tests/suite/*.rs` file is a
  module of `tests/suite/main.rs` (`autotests = false` in `Cargo.toml`). Each
  extra Cargo test target is a separate full link of a ~100 MB binary, so the
  old layout cost 26 links on every edit (~50s); it is now one (~18s). Adding a
  test file means adding a `mod` line — Cargo will not pick it up otherwise.
- **The WAL directory is a parameter, not an env var.** `WalManager` passes
  `cfg.core.wal_dir()` to `Walrus::with_root`. Previously walrus read the
  process-global `WALRUS_DATA_DIR`, so two concurrent tests shared one WAL and
  corrupted each other (9/9 `tantivy_e2e` failed concurrent) — which is why the
  suite used to be pinned to `--test-threads=1`. Don't reintroduce a global here.

Anything a test needs to be unique — port, storage prefix, data dir — must come
from its own config. Bind `127.0.0.1:0` for ports; never pick from a fixed
window, and never `set_var` a per-test value.

All test harnesses default to **local MinIO** — `sqllogictest` auto-starts one
(local `minio` binary preferred, Docker only as a fallback); the rest read `.env`
(which now points at `127.0.0.1:9000`). Set `TIMEFUSION_TEST_S3_ENDPOINT` to reuse
an existing MinIO and skip startup. Non-local storage requires explicit opt-in
(`.env.prod` via `make *-prod`, or exported `AWS_*` creds).

### E2E suite (`tests/e2e/`)

End-to-end tests that exercise the **full prod path** (pgwire → BufferedWriteLayer
→ WAL → MemBuffer → flush → Delta on MinIO → query) with virtual time. MinIO is
resolved local-first exactly like sqllogictest (endpoint env → running :9000 →
local `minio` binary spawned detached → Docker container per test as last
resort); per-test isolation is the unique `e2e-<uuid>` bucket, not the server.
The harness uses `crate::bootstrap::bootstrap()`, which is the same wiring as
`main.rs`, so a test failure mirrors a prod failure. pgwire ports are
OS-assigned (bind `127.0.0.1:0`), never picked from a window. Full suite:
~112s wall on a warm build (2026-08-03).

Time is driven by `crate::clock`, not by `tokio::time::pause` (real S3 IO needs
the real runtime). Use `env.advance(Duration::...)` + `env.force_flush()` /
`env.force_evict()` for deterministic flush/eviction; use
`env.await_next_flush()` to await the periodic task without polling.

- UUID prefixes for test table names, data dirs and storage prefixes — that, not
  `#[serial]`, is what keeps tests isolated now
- `tokio::test(flavor = "multi_thread")` for async tests
- `make test-e2e` runs the suite; Docker is only needed when no `minio` binary
  is installed and nothing is listening on :9000
- `cold_start_under_five_seconds` is a latency benchmark: ~4s isolated, but the
  in-suite assert is 30s because ~10-way suite parallelism inflates wall-clock ~3x

### Bug-fix workflow (mandatory)

When fixing a bug, **always** follow this order:

1. **Reproduce the bug as a failing test first**, at the level closest to where the bug manifests:
   - Pure logic / parsing → unit test (`#[test]` in a `mod tests` block)
   - SQL behavior → `sqllogictest` case
   - End-to-end pgwire / write path → `integration_test`
     The test must fail in a way that exposes the _real_ bug. "Errored somewhere" is not enough — assert on the specific symptom (a parser error string, a row count, an error code).
2. **Then write the fix** and confirm the test now passes. Run the rest of the suite to confirm no regressions.
3. **Keep the test as a regression guard.** Name it after the bug (e.g. `pgwire_abort_alias_rewrites_to_rollback`, `buffered_write_layer_pressure_flushes_current_bucket`). Reference the incident in a comment if non-obvious.
4. **Don't skip the failing-test step because the fix looks obvious.** Several "obvious" fixes during the 2026-05-28 monoscope dual-write rollout (FairSpillPool → Greedy, simple-query ABORT rewrite) missed the actual broken path and only the next prod failure surfaced it. A failing test forces you to exercise the path you think is broken.
5. **Prove the guard can fail — revert the fix and watch it go red.** Writing the test after the fix is fine; *shipping* it without seeing it fail is not. 2026-09-04: one new guard caught a deliberately-reintroduced bug (49 rows vs 34 — real), and another stayed GREEN with the fix reverted, because the local path re-runs an optimizer pass that prod's pgwire path does not. A test that cannot fail is worse than no test: it converts "unverified" into "verified" in the reader's head. If it can't be made to fail, say so **in the test** rather than implying a guard it does not provide.

## Key Crates

| Crate                  | Purpose                                              |
| ---------------------- | ---------------------------------------------------- |
| `datafusion`           | Vectorized SQL query engine                          |
| `deltalake`            | ACID storage on S3                                   |
| `datafusion-variant`   | Variant type UDFs (json_to_variant, variant_to_json) |
| `datafusion-postgres`  | PostgreSQL wire protocol server                      |
| `foyer`                | Two-tier cache (memory + disk)                       |
| `dashmap`              | Lock-free concurrent HashMap                         |
| `tokio`                | Async runtime                                        |
| `serde` / `serde_json` | Serialization                                        |
| `walrus-rust`          | Write-ahead log                                      |
| `arrow`                | Columnar data format                                 |
| `bincode`              | WAL entry serialization                              |
