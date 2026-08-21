---
title: "TimeFusion — A Codebase Book"
subtitle: "A complete offline reading companion to the TimeFusion source"
---

# About this book

This is a narrative technical book about the TimeFusion codebase: a
single-process, multi-tenant time-series database written in Rust that stores
observability data as Delta Lake tables on S3-compatible object storage, executes
SQL with Apache DataFusion, and speaks the PostgreSQL wire protocol.

It is written to be read away from a computer. Every important type, function,
constant and schema is reproduced inline, verbatim, and then walked through — so
this document is your copy of the code, not a description of code you cannot see.
Roughly half of it is source listings.

**Pinned to commit `69bdfcd` (2026-08-21).** Every listing was read from the tree
at that commit and quoted verbatim, with `// ... elided: <what and why>` markers
where a long body was cut. Line numbers in listing captions refer to that commit.

⚠️ **One subsystem moved while this book was being written.** A concurrent branch
(`remove/hot-tier`) deleted `src/hot_tier.rs` mid-way through Part III. Chapter 20
covers it from the pinned commit, read back via `git show`, and says so at the
top. Nothing else in the book was affected.

**Audience.** An experienced Rust engineer with some DataFusion exposure. The
book assumes you can read Rust fluently and know roughly what an `ExecutionPlan`
and a `RecordBatch` are. It does not assume you know Delta Lake, Tantivy, Foyer,
walrus, or anything about this repository.

**Conventions.**

- Every claim is anchored to `path:line`.
- Listings are captioned `**Listing C.N — path:start–end**` and are verbatim.
- `⚠️` marks something I could not determine, or a place where the code and its
  own documentation disagree. I have not guessed.
- Diagrams are ASCII first; Mermaid where it adds something, always with prose.

**The recurring example.** One row is followed end to end: **the payments span** —
an OpenTelemetry server span, `id = 550e8400-…`, tenant `prod-api-001`, timestamp
`2025-01-17 14:25:00Z`, name `POST /api/v1/payments`, `duration = 3_421_000_000`
ns, `status_code = 'ERROR'`. It appears in Chapters 6, 8, 10, 12, 13, 18 and 21.

**Triage.** At ~78,000 lines of Rust plus ~15,000 of tests, this codebase is past
the point where a book can reproduce all of it. Parts I–III carry full depth over
the spine — the data model, the configuration and storage substrate, and every
major runtime flow. Part IV is deliberately thinner and covers at reference depth
what the flow chapters did not reach. Two files are covered by their load-bearing
types and functions rather than exhaustively: `src/database/mod.rs` (15,172 lines)
and `src/write/mod.rs` (6,838 lines).

**A note on the comments.** This codebase is unusually well commented, and the
comments are forensic rather than descriptive: they name a date, a measurement,
what broke, and often what was tried and rejected. A large fraction of this book
is those comments, quoted and connected across subsystems. Reading TimeFusion
without reading its comments would be reading half of it.

# Contents

**Part I — Orientation**

1. What this project is
2. The 10,000-foot architecture
3. Repository map
4. The technology choices
5. Domain glossary

**Part II — The spine: data model and core abstractions**

6. The schema registry and the core data model
7. The core abstractions
8. State, storage, and persistence

**Part III — The flows**

9. Startup: from `main` to serving
10. The write path: from `INSERT` to acknowledged
11. The write-ahead log
12. The MemBuffer and the flush to Delta
13. The read path, part I: from socket to legs
14. The read path, part II: rules, functions, and the plan cache
15. Merge-on-read: `DedupExec`, tombstones, and count pushdown
16. `UPDATE` and `DELETE`: interception, version append, and coalescing
17. The maintenance coordinator
18. Rollups: continuous aggregates and query routing
19. Compaction, dedup, footer repair, and vacuum
20. The hot tier — and the case for deleting it
21. Tantivy: per-file full-text sidecar indexes
22. Shutdown and the deploy handoff
23. The CLI subcommands

**Part IV — Module reference**

24. The modules the flow chapters did not reach

**Part V — Operations and evolution**

25. Configuration
26. Build, test, deploy
27. Observability of TimeFusion itself
28. Security and multi-tenancy boundaries
29. The honest chapter
30. Where to go next

**Appendices**

- A — Annotated directory tree
- B — Public entry points
- C — Error catalog
- D — Dependency inventory

# Part I — Orientation

## 1. What this project is

*After this chapter you will know what TimeFusion is for, who runs it, what it is
not, and which of its constraints are load-bearing enough that every later design
decision in the book bends around them.*

### The elevator pitch

TimeFusion is a single-process, multi-tenant time-series database for observability
data — logs, traces, spans, and metrics. It stores that data as Parquet under a
Delta Lake transaction log on S3-compatible object storage, executes SQL over it
with Apache DataFusion, and speaks the PostgreSQL wire protocol so that any
Postgres client is already a TimeFusion client. Writes land in a write-ahead log
and an in-memory buffer for sub-second acknowledged durability, and are flushed to
Delta in the background. Reads union the buffer with the Delta tables, so a row is
queryable the instant it is acknowledged.

`README.md:7–11` states the same thing in the project's own words, and
`README.md:13` supplies the tagline that explains the protocol choice: *"If you
already have a Postgres client, you already have a TimeFusion client."*

### The honest ten paragraphs

**It exists because of a bill and a lock-in.** TimeFusion is built and operated by
APIToolkit / Monoscope, an observability vendor. Their customers' telemetry is
high-volume, append-dominated, and queried through dashboards that ask a small set
of shapes over and over: a time-bucketed count, a latency percentile, a point
lookup by trace id. Storing that in a hosted time-series product means paying for
storage you do not own in a format you cannot leave. TimeFusion's first-order
design goal is that the bytes are ordinary Parquet in the operator's own bucket,
under an open transaction log, readable by anything that reads Delta
(`README.md:34–36`).

**The second goal is that nobody has to learn a new client.** The wire protocol is
Postgres, implemented through `datafusion-postgres` over a vendored `pgwire`
(`Cargo.toml`, `[patch.crates-io]`). This is not a thin veneer: a real chunk of
this codebase exists purely to make strict Postgres drivers happy — a `pg_catalog`
compatibility layer (`src/server/pg_compat.rs`, 1,431 lines), a JSONB type OID for
Variant columns so that Haskell's `hasql` does not reject a row
(`src/schema.rs:801–815`), and a fork of `pgwire` because libpq 18 requires a
`ProtocolVersionNegotiation` response listing unsupported `_pq_.*` startup options.

**It is deliberately one process.** There is no coordinator, no shard router, no
Kubernetes operator. One binary owns one WAL directory, holds an exclusive `flock`
on it for its entire life (`src/main.rs:401`), and serves everything: ingest,
queries, compaction, rollup builds, index maintenance. Every scaling lever is
therefore vertical or algorithmic, never horizontal, and a very large amount of the
engineering in this repository is a consequence of that: memory budgets that
partition one cgroup limit across query, ingest and maintenance; a maintenance
scheduler that must not starve ingest; a shutdown path that must release the WAL
lock inside the orchestrator's SIGTERM grace or the replacement container starves.

**Multi-tenancy is a partition key, not a process boundary.** Every table is
partitioned by `[project_id, date]`. `project_id` is the tenant. Queries are
expected — practically required — to carry `WHERE project_id = '…'`; the routing
table extracts that literal from the filters and selects which Delta table to
scan. Projects that need physical isolation get their own table path
(`s3://bucket/prefix/projects/{project_id}/{table}/`); everyone else shares a
unified table (`s3://bucket/prefix/default/{table}/`) and is separated by the
partition column. `README.md:80–81` states the rule bluntly: a query without
`project_id` scans across tenants and is far slower.

**Durability is layered, and the layering is the interesting part.** An `INSERT`
is acknowledged once it is in the WAL (durable, fsynced on a 200 ms schedule) and
in the in-memory buffer (queryable). Delta receives it minutes later, in a batch,
when the row's 10-minute time bucket has closed. That means there are always two
copies of recent data with different failure modes, and a large fraction of the
startup path exists to reconcile them: derive how far Delta got, fast-forward the
WAL cursors past it, then replay only the tail. Getting that reconciliation wrong
either loses acknowledged writes or resurrects rows Delta already has, and the
code carries scar tissue from both (`src/main.rs:496–563`).

**The read path is not a simple scan.** A query over `otel_logs_and_spans` unions
the memory buffer with Delta files, applies merge-on-read dedup because
`UPDATE`/`DELETE` append new row versions instead of rewriting files, filters
tombstones, and may be transparently rerouted to a pre-aggregated rollup table if
its shape allows. Layered on that are: a cross-connection plan cache, a Tantivy
full-text sidecar index used to prefilter `(timestamp, id)` pairs, bloom filters on
high-cardinality id columns, a logical count index, HLL sketches for approximate
distinct counts, and a scan-admission valve that refuses queries which would select
too many bytes. Each of those exists because a specific production query pattern
was unacceptably slow or killed the process.

**Maintenance is a first-class subsystem with its own durable scheduler.** Delta
tables written by a streaming ingest path accumulate thousands of small files;
merge-on-read accumulates row versions; rollups need building and rebuilding.
`src/maintenance_coordinator.rs` (4,240 lines) is a journaled task queue —
operations (`BaseRollup`, `DerivedRollup`, `Dedup`, `HotPacking`,
`SealedConsolidation`, `Repair`) are minted, claimed by workers, retried, split
when too large, and recorded so the work survives a restart. `timefusion sim`
replays a real production journal through that scheduler on virtual time, which is
how scheduling policy is evaluated without a deploy.

**Rollups are continuous aggregates with an unusual refresh policy.** A rollup is
declared on the source table's YAML (`schemas/otel_logs_and_spans.yaml`), and the
rollup table's schema is *synthesized* from the declaration plus the source schema
so a measure's type cannot drift from the column it aggregates
(`src/schema.rs:207–302`). The refresh trigger is not a timer: a bucket is built
when its partition has been *certified duplicate-free*, because an aggregate
computed over data that is later deduped is simply wrong.

**Almost every non-obvious line has an incident attached.** This codebase is
unusually well-commented, and the comments are not "what" comments — they are
forensic. A comment will say that a value was set to X on a specific date, what
broke, what the measurement was, and what will break if you change it back.
`src/main.rs:126–137` explains why the health-probe deadline is 1500 ms and not
750 ms (a 0.896 s probe on a healthy server killed a task mid-repair, discarding a
40-minute rewrite). `src/main.rs:45–56` explains why Tokio workers get 32 MiB
stacks (a 2 MiB stack overflowed while planning a merge-on-read `UPDATE`, aborting
the process). Reading this codebase without reading its comments would be reading
half of it.

**It is pre-1.0 and it shows.** `README.md:15–19` says so. There is genuine drift
between the documented module layout and the tree, feature flags whose defaults
were flipped after an incident and never cleaned up, an entire subsystem (the hot
tier) that was measured to be a net loss and disabled, and rollup schema
"generations" (`_v1`, `_v2`, `_v3`) kept registered as read-only aliases while
newer generations shadow-build. Chapter 29 catalogs all of it with evidence.

### Who runs it, and how

One Docker Swarm service (`srv-captain--timefusion`) on a CapRover host, image
tagged with the deployed git short SHA. The process serves pgwire on 5432 (the only ingress on this branch — see
Chapter 2), writes its WAL and Foyer disk cache to a mounted data volume, and
talks to S3-compatible object storage (production has migrated R2 → OVH). Live
in-process diagnostics are exposed *over the SQL protocol itself* as a
`timefusion_stats` table — `SELECT component, key, value FROM timefusion_stats` —
which means the debugging interface for the database is the database.

The full deploy, probe, and read-only-host guidance lives in `RUNBOOK.md` and in
this repository's `CLAUDE.md`; Chapter 26 covers the pipeline.

### What it is not

It is not a general-purpose OLTP database: there are no secondary indexes in the
B-tree sense, no foreign keys, no row-level locking, and `UPDATE`/`DELETE` are
implemented either as a Delta `MERGE` or as an append of a new row version. It is
not a distributed system: there is exactly one writer. It is not a Postgres
reimplementation: `SHOW server_version` reports the embedded protocol layer's
version, and the `pg_catalog` surface is exactly as large as real clients turned
out to need.

### A note on the map's resolution

At roughly 78,000 lines of Rust in `src/` plus 15,000 lines of tests, this
codebase is past the point where a book can reproduce all of it. This one triages
explicitly. Parts I–III carry full depth over the spine: the schema and data
model, the configuration and storage substrate, and every major runtime flow —
startup, write, WAL, flush, read, merge-on-read, DML, maintenance, rollups,
compaction, Tantivy, shutdown. Part IV is deliberately thinner and covers what the
flow chapters did not reach at reference depth: observability plumbing, the
virtual clock, the `pg_catalog` layer, the simulator, and the test suites. Two
files are large enough that they are covered by their load-bearing types and
functions rather than exhaustively: `src/database/mod.rs` (15,172 lines) and
`src/write/mod.rs` (6,838 lines). Where a listing is cut, an
`// ... elided:` marker says what was cut and why it does not change the argument.

### The recurring example

Throughout this book, one row is followed end to end. Call it **the payments
span**: an OpenTelemetry server span emitted by a service called `payments-api`,
belonging to tenant `prod-api-001`, recorded at `2025-01-17 14:25:00Z`, named
`POST /api/v1/payments`, with `duration = 3_421_000_000` nanoseconds and
`status_code = 'ERROR'`. It arrives over pgwire as an `INSERT`. By the end of the
book it will have been: coerced and stamped, appended to a WAL topic, bucketed in
memory, flushed into a Parquet file under `project_id=prod-api-001/date=2025-01-17/`,
indexed by Tantivy, counted into a 1-minute rollup bucket, compacted into a larger
file, and finally read back by three different query shapes that each take a
different path through the engine.

**Key takeaways.** TimeFusion is a single-process, Postgres-speaking, Delta-Lake-backed
observability database whose defining constraints are: one writer per WAL directory,
one cgroup memory limit shared by query/ingest/maintenance, `project_id` as both
tenant boundary and partition key, and durability split across a WAL and a
background flush. Nearly every complexity in the rest of this book descends from
one of those four.
## 2. The 10,000-foot architecture

*After this chapter you will be able to name every component that runs inside the
TimeFusion process, every external system it talks to, every port it listens on,
and the direction of every arrow between them. You will also know which
components are authoritative and which are derived — the single most useful
distinction for reasoning about failures here.*

### One process, many subsystems

TimeFusion is one binary. There is no sidecar, no worker pool on another host, no
control plane. Everything below runs as Tokio tasks inside a single multi-threaded
runtime whose workers are built with 32 MiB stacks (`src/main.rs:94`).

```
                         ┌──────────────────────────────────────┐
   psql / pgjdbc /       │        TimeFusion process            │
   hasql / BI tools ────►│ :5432  pgwire  ──┐                   │
   (Postgres wire)       │                  │                   │
                         │                  ▼                   │
                         │        DataFusion SessionContext     │
                         │   (analyzer + optimizer + physical)  │
                         │                  │                   │
                         │   ┌──────────────┴───────────────┐   │
                         │   ▼                              ▼   │
                         │ WRITE                          READ   │
                         │  BufferedWriteLayer      ProjectRoutingTable
                         │   ├─ WAL (walrus)          ├─ MemBuffer leg
                         │   │   local disk           ├─ Delta leg(s)
                         │   └─ MemBuffer             ├─ hot-tier leg (off)
                         │       10-min buckets       └─ rollup reroute
                         │          │                        │   │
                         │          │ flush (~10 min)        │   │
                         │          ▼                        ▼   │
                         │  ┌───────────────────────────────────┐│
                         │  │  Delta Lake tables (Parquet + _delta_log)
                         │  └───────────────┬───────────────────┘│
                         │                  │                    │
                         │  maintenance coordinator (journaled)  │
                         │   compaction · dedup · rollups ·      │
                         │   repair · vacuum · tantivy index     │
                         │                  │                    │
                         │        storage.rs ObjectStore         │
                         │        + Foyer cache (RAM + disk)     │
                         └──────────────────┬───────────────────-┘
                                            │
                                            ▼
                              S3-compatible object storage
                            (MinIO local · R2/OVH in production)
```

The same picture as a Mermaid diagram, for renderers that support it. If it does
not render, the ASCII above is authoritative.

```mermaid
flowchart TD
  C[Postgres clients] -->|:5432 pgwire| S[LoggingHandlerFactory]
  S --> DF[DataFusion SessionContext]
  DF -->|INSERT / UPDATE / DELETE| BWL[BufferedWriteLayer]
  DF -->|SELECT| PRT[ProjectRoutingTable]
  BWL --> WAL[(WAL — local disk, walrus)]
  BWL --> MB[MemBuffer — 10-min buckets]
  MB -->|flush| DL[(Delta Lake on S3)]
  PRT --> MB
  PRT --> DL
  PRT -.reroute.-> RU[(rollup tables)]
  MC[Maintenance coordinator] --> DL
  MC --> TV[(Tantivy index blobs)]
  DL --- FO[Foyer cache RAM+disk]
  PRT --- TV
```

### The processes, ports, and paths

There is exactly one OS process. Inside it:

- **pgwire listener**, `0.0.0.0:cfg.core.pgwire_port` (default 5432). Bound
  *before* any slow startup work (`src/main.rs:379–388`) so that clients arriving
  during a multi-minute boot get SQLSTATE `57P03` rather than `ECONNREFUSED`.
- **No second listener.** `CLAUDE.md` documents a gRPC ingest endpoint on
  `:50051` with a `GRPC_TOKEN` bearer token, and `src/server/mod.rs:1` still says
  the module holds a "gRPC service". Neither exists on this branch: `grep -rn
  "grpc\|tonic" src/` matches only `src/observability.rs`, where `tonic` is the
  transport for the *outbound* OTLP exporter, and `grep -rni "grpc_port|GRPC_TOKEN"`
  over `src/`, `.env`, `.env.example` and the `Dockerfile` matches nothing.
  pgwire is the only ingress. Chapter 29 records this as documentation drift.
- **OTLP exporter**, outbound, for TimeFusion's own telemetry
  (`src/observability.rs`).
- **Local disk**, under `TIMEFUSION_DATA_DIR`: the WAL directory (with its
  exclusive `wal.lock`), the Foyer disk cache, Tantivy index scratch, and profile
  dumps.
- **Object storage**, outbound HTTPS: Delta tables, Tantivy index blobs, and
  best-effort JSON sidecars.

### Authoritative versus derived

This is the distinction to internalize, and `docs/ARCHITECTURE.md:56–63` states it
directly: Foyer cache entries, Tantivy indexes, rollups, and maintenance indexes
are *derived* — TimeFusion can rebuild all of them from Delta data. Delta is
authoritative after a flush. The WAL is authoritative *until* its acknowledged
rows reach Delta, and not one moment less; the design rule in that same file reads
"Do not delete a WAL directory unless its durable cursors prove that Delta contains
every entry."

That rule is why so much of the startup path (Chapter 9) is about *proving* what
Delta already has before replaying anything, and why the shutdown path (Chapter 22)
is a race against the orchestrator's SIGTERM grace to write a clean cursor snapshot.

### The write path, one paragraph

An `INSERT` is parsed by DataFusion, coerced by the insert path into the table's
Arrow schema, appended to a WAL topic keyed `{project_id}:{table_name}`, and
inserted into the MemBuffer's 10-minute time bucket for its timestamp. The client
is acknowledged. A background flush task wakes on `flush_interval_secs` (default
600), drains buckets whose window has closed, and calls a `DeltaWriteCallback`
that commits them to Delta. There is also a *coalesced* callback that batches many
buckets into one commit per physical table (`src/server/mod.rs:133–158`), because
Delta commit cost is per-commit, not per-row.

### The read path, one paragraph

A `SELECT` is parsed, run through TimeFusion's analyzer and optimizer rules
(Variant typing, `pg_catalog` compatibility, Tantivy predicate rewriting, TopK
ordering, rollup routing), and planned against a `ProjectRoutingTable` that
resolves `(project_id, table_name)` into a set of *legs*: the MemBuffer's batches,
one or more Delta tables, optionally a rollup table instead of the raw one. The
legs are unioned, deduplicated by `(timestamp, id)` keeping the greatest
`updated_at`, tombstone-filtered, and streamed back. Chapters 13–15 take this
apart.

### The maintenance path, one paragraph

A journaled coordinator mints units of work per `(source table, project, date,
operation)`, workers claim them under a concurrency cap, execute them against
Delta, and record the outcome durably so a restart resumes rather than restarts.
The operations are `BaseRollup`, `DerivedRollup`, `Dedup`, `HotPacking`,
`SealedConsolidation`, and `Repair` (`src/main.rs:310–318` enumerates them where
the `run-unit` CLI parses `--op`). Chapters 17–19 cover the scheduler and each
operation.

### Where the payments span lives at each moment

| Time after INSERT | Where the row is | Who can see it |
|---|---|---|
| 0 ms | WAL frame on local disk + MemBuffer bucket | every query (MemBuffer leg) |
| ≤ 200 ms | fsynced WAL | survives process kill |
| ~10 min | Parquet file in Delta, MemBuffer bucket evicted later | Delta leg |
| +1 h | Tantivy index blob covers the file | `text_match` prefilter |
| +hours | compacted into a larger file; counted into the 1m rollup | rollup routing |

### Key takeaways

One process; two listeners; one authoritative store (Delta) plus one
authoritative-until-flushed store (the WAL); everything else is derived and
rebuildable. Reads union memory with storage; writes acknowledge from memory and
reach storage later; maintenance rewrites storage under a durable journal. Every
chapter in Part III is one arrow in the diagram above, followed function by
function.
## 3. Repository map

*After this chapter you will know where every kind of thing lives, which
directories are load-bearing and which are scaffolding, what is generated and by
what, and in what order to read the source if you later get a checkout.*

### The top level

```
timefusion/
├── src/                  the crate — 78,180 lines of Rust across 29 files
├── tests/
│   ├── suite/            THE integration target: 27 modules + main.rs
│   ├── e2e/              full prod path on virtual time; harness.rs owns MinIO
│   └── slt/              15 .slt files, each generated into its own #[test]
├── benches/              5 criterion benches (cargo bench)
├── bench/                python/shell load generators — NOT cargo, not run by CI
├── schemas/              5 YAML table schemas, compiled in via include_dir!
├── proto/                (documented; absent on this branch — see below)
├── vendor/               forked deps: pgwire, walrus-rust, tikv-jemalloc-sys
├── docs/                 architecture notes + docs/plans/ + two HTML dashboards
├── tasks/                older task briefs, superseded by docs/plans
├── scripts/              prod benchmarking and probe scripts
├── deploy/               CapRover service override + a pgwire proxy config
├── .github/workflows/    ci · deploy · autoformat · build-image · simd-bench · rollback
├── .cargo/config.toml    THE `cargo lint` definition + the ld64.lld linker choice
├── Makefile              every test/lint/run entry point
├── Dockerfile            production image (docker-compose.yml is dev-only)
├── rust-toolchain.toml   pinned toolchain
├── CLAUDE.md             39 KB of operational and architectural instructions
├── RUNBOOK.md            production runbook
└── data/ minio/ target/  gitignored local state
```

Two documented directories do **not** exist on this branch, and this matters
because tooling references them:

- `proto/` and the gRPC codegen `build.rs` are described in `CLAUDE.md` but the
  tree has no `proto/` and no `build.rs`, and the gRPC ingest service is gone from
  the source as well: `grep -rn "grpc\|tonic" src/` matches only
  `src/observability.rs` (the outbound OTLP exporter's transport), and
  `GRPC_PORT` / `GRPC_TOKEN` appear nowhere in `src/`, `.env`, `.env.example` or
  the `Dockerfile`. pgwire is the only ingress on this branch.
- `ci/checks.tsv` and `scripts/ci/ci.sh` — the whole local-CI attestation system
  described at length in `CLAUDE.md` — are absent. `make ci` therefore cannot run
  on this checkout. `.github/workflows/ci.yml` exists and is self-contained.

### `src/`, in dependency order

This is also the recommended reading order. Each line is one file, its size, and
what you get from reading it.

```
src/
├── lib.rs                 18     the module list — the whole public surface
├── main.rs             1,080     entry point, CLI subcommands, startup, shutdown
├── support.rs            263     virtual clock + test helpers (read this early:
│                                 every timestamp in the codebase goes through it)
├── schema.rs           1,021     YAML schema registry, rollup specs, Variant types
├── config.rs           3,053     OnceLock<AppConfig>, autotune, budget tree,
│                                 secret encryption — 138 config fields
├── storage.rs          3,551     Foyer L1/L2 ObjectStore + delta-snapshot and
│                                 JSON sidecars (certifications, dirty bins)
├── observability.rs    1,468     OTel metrics/traces, pprof + jemalloc profiling
├── write/
│   ├── wal.rs          2,663     write-ahead log over walrus-rust; the dir lock
│   ├── mem_buffer.rs   4,544     DashMap of tables → 10-minute time buckets
│   └── mod.rs          6,838     BufferedWriteLayer: batching, coercion, flush,
│                                 backpressure, recovery, memory reservation
├── database/
│   ├── mod.rs         15,172     Database, table resolution, session construction,
│                                 ProjectRoutingTable, scan-pressure valve
│   ├── write.rs        1,372     insert path, coalesced commits, watermarks
│   ├── compact.rs      1,727     OPTIMIZE, hot-tail packing, dedup rewrites
│   └── maintain.rs     5,865     dedup sweeps, footer repair, vacuum, shutdown
├── read/
│   ├── mod.rs          3,594     read-side dedup, count pushdown, HLL
│   ├── optimizers.rs   3,068     every analyzer/optimizer rule
│   ├── functions.rs    2,205     custom SQL functions + VariantAwareExprPlanner
│   └── plan_cache.rs   1,537     cross-connection plan cache
├── dml.rs              3,356     UPDATE/DELETE interception + the DML coalescer
├── maintenance_coordinator.rs
│                       4,240     durable task journal + scheduler
├── rollup.rs           3,039     rollup specs, builders, query routing
├── rollup_journal.rs     118     rollup bookkeeping
├── maintenance_sim.rs    655     IO-free simulator behind `timefusion sim`
├── hot_tier.rs         2,118     local Arrow-IPC hot tier (disabled — Ch. 20)
├── server/
│   ├── mod.rs          1,691     bootstrap(), pgwire handlers, early bind
│   └── pg_compat.rs    1,431     pg_catalog compat + the timefusion_stats table
└── tantivy/
    ├── mod.rs            968     index build/schema/manifest/blob store
    ├── search.rs       1,075     search + reader + indexing service
    └── udf.rs            450     text_match UDF and predicate extraction
```

**Note the drift from `CLAUDE.md`.** That file documents `src/maintenance/mod.rs`,
`src/maintenance/rollup.rs`, `src/maintenance/hot_tier.rs`, and
`src/database/rollup.rs` / `src/database/scan.rs`. None of those paths exist. The
real tree has `maintenance_coordinator.rs`, `rollup.rs`, `hot_tier.rs`, and
`maintenance_sim.rs` at the top level of `src/`, and `database/` has four files,
not seven. Trust the tree. Chapter 29 treats this as a defect.

### The module convention

`database/`, `read/`, `write/`, `server/`, and `tantivy/` each follow the same
pattern: `mod.rs` holds the concern itself, and the sibling files are *slices of
the same module* rather than a layered API — they open with `use super::*` and
freely use each other's private items. Files are large on purpose so that related
code lives together and gets reused instead of re-implemented. If you are looking
for a function and it is not in the obvious file, grep the whole folder.

### What is generated, and by what

- **`schemas/*.yaml` → Rust structs at runtime**, not at build time. `include_dir!`
  embeds the YAML into the binary (`src/schema.rs:642`) and `SchemaRegistry::new`
  parses it on first access. Rollup tables' schemas are *synthesized* in memory
  from the source table's schema (`RollupSpec::synthesize`) — there is no file for
  `otel_logs_and_spans_rollup_dashboard_1m_v3`.
- **`tests/slt/*.slt` → one `#[test]` each**, via the `slt_files!` macro in
  `tests/suite/sqllogictest.rs`. A `.slt` file not listed there fails the
  `every_slt_file_has_a_test` guard, so a file cannot be silently unrun.
- **Nothing else is codegen'd on this branch** (no `build.rs`).

### Test layout, and why it is shaped that way

`Cargo.toml` sets `autotests = false` and declares exactly two test targets:
`tests/suite/main.rs` and `tests/e2e/main.rs`. Every other file under those
directories is a `mod` of its `main.rs`. The reason is link time: each Cargo test
target is a separate full link of a ~100 MB binary against 1,200+ dependencies, so
the previous one-target-per-file layout cost 26 links on every source edit (~50 s);
it is now one (~18 s). **Adding a test file means adding a `mod` line** — Cargo
will not discover it.

- `tests/suite/` — 27 integration-test modules, ~13,000 lines. The biggest single file is
  `dedup_compaction_test.rs` at 2,938 lines.
- `tests/e2e/` — 28 test modules plus `main.rs` and `harness.rs`, exercising the *full production path* (pgwire → buffered
  layer → WAL → MemBuffer → flush → Delta on MinIO → query) on virtual time.
  `harness.rs` (715 lines) owns MinIO resolution and calls the same
  `server::bootstrap()` that `main.rs` uses, so an e2e failure mirrors a prod
  failure. Gated behind the `e2e` cargo feature.
- `tests/slt/` — 15 sqllogictest files covering SQL surface: aggregations, JSON
  functions, `pg_catalog`, Variant columns, merge-on-read, partition pruning.
- `benches/` — five criterion benches, `harness = false`.

### `bench/` versus `benches/`

Easy to confuse and completely different. `benches/` is Cargo criterion
micro-benchmarking. `bench/` is a directory of Python and shell load generators
that drive a *running* TimeFusion over pgwire — insert throughput, query latency
under concurrent ingest, whole-lifecycle timing, prod-load replay. `README.md:280–285`
lists the four most useful.

### `docs/`

Thirteen markdown files plus `docs/plans/` (three current plans and a README) and
`docs/dashboards/` (two standalone HTML dashboards published as artifacts).
`docs/ARCHITECTURE.md` is the short official version of Chapter 2;
`docs/WAL.md`, `docs/buffered-write-layer.md`, `docs/VARIANT_TYPE_SYSTEM.md`,
`docs/CACHING.md`, and `docs/DELTA_CHECKPOINT_HANDLING.md` are the deep dives that
Chapters 8, 10–12, and 14 draw on. `docs/plans/` is where design work happens
before it lands; the plan documents carry status headers and post-hoc results,
which makes them the best available record of *why* recent subsystems look the way
they do.

### Reading order, if you get the code

1. `src/lib.rs`, `src/main.rs` — the shape of the process.
2. `src/support.rs` — the clock. Everything else depends on it.
3. `src/schema.rs` + `schemas/otel_logs_and_spans.yaml` — the data model.
4. `src/config.rs` — but only `AppConfig` and the budget tree; skip the 138 fields.
5. `src/write/mod.rs` `insert()`, then `wal.rs`, then `mem_buffer.rs`.
6. `src/database/mod.rs` — `Database`, `ProjectRoutingTable::scan`.
7. `src/read/mod.rs` and `src/read/optimizers.rs`.
8. `src/maintenance_coordinator.rs`, then `src/rollup.rs`.
9. Everything else on demand.

**Key takeaways.** One crate, 29 source files, two test targets, five YAML
schemas, no build-time codegen. The documented module layout in `CLAUDE.md` has
drifted from the tree and the local-CI harness it describes is absent — verify
paths before trusting that file. Large files are intentional; grep the folder, not
the file.
## 4. The technology choices

*After this chapter you will know every significant dependency, why it is here,
what it replaces, and — more useful — the two or three things about **this
codebase's** use of it that will bite you. This is not a tutorial on DataFusion or
Delta Lake; it is a guide to the specific ways TimeFusion bends them.*

`Cargo.toml` is unusually worth reading in full, because it is one of the places
where the project records its reasoning. Four of its dependencies are *forks*, and
each fork comment names the exact upstream limitation that forced it.

### The four forks, and why they exist

**Listing 4.1 — Cargo.toml, `[patch.crates-io]`**

```toml
[patch.crates-io]
# libpq 18 requires a ProtocolVersionNegotiation response that reports every
# unsupported `_pq_.*` startup option, even when protocol 3.2 itself matches.
pgwire = { path = "vendor/pgwire" }
# datafusion-postgres ecosystem on our fork's DataFusion 54 branch
# (apitoolkit/datafusion-postgres @ timefusion-df54, off v0.16.0). Includes
# bind-parameter, cursor, catalog, and PostgreSQL-client compatibility fixes.
# Patching datafusion-postgres alone pulls its sibling
# arrow-pg/datafusion-pg-catalog from the same git workspace, so those need no
# separate patch.
datafusion-postgres = { git = "https://github.com/apitoolkit/datafusion-postgres.git", branch = "timefusion-df54" }
# datafusion-sql on our fork (tonyalaribe/datafusion @ timefusion-update-from-54,
# off tag 54.0.0): removes the SQL planner's defensive
# `not_impl_err!("UPDATE ... FROM is not supported")` guard so `UPDATE ... FROM`
# reaches TF's DmlQueryPlanner (Delta MergeBuilder / MemBuffer hash-join).
# Mirrors apache/datafusion#21530; drop once upstream merges #19950.
datafusion-sql = { git = "https://github.com/tonyalaribe/datafusion.git", branch = "timefusion-update-from-54" }
# tikv-jemalloc-sys 0.6.1 vendored with a single build.rs delta: honour
# `JEMALLOC_SYS_PROF_BACKTRACE` so the heap profiler can be built with a working
# unwinder (`--enable-prof-libunwind`). Upstream passes `--enable-prof` alone and
# exposes no hook for extra configure flags, and the resulting libgcc unwinder
# returns zero frames in our binary — every prod heap dump was one anonymous
# frame (2026-07-31). Re-vendor from the registry on upgrade; see the PATCH
# comment in vendor/tikv-jemalloc-sys/build.rs.
tikv-jemalloc-sys = { path = "vendor/tikv-jemalloc-sys" }
```

The pattern to notice: every fork is a *removal* or a *hook*, not a feature.
`datafusion-sql` deletes a `not_impl_err!` guard so a statement reaches
TimeFusion's own planner. `tikv-jemalloc-sys` adds one environment variable to a
`build.rs`. `pgwire` implements one protocol message. None of them is a divergent
reimplementation, which is what makes rebasing them onto upstream feasible.

The fifth pinned dependency is `deltalake` itself, on a fork:

**Listing 4.2 — Cargo.toml, the deltalake pin**

```toml
# delta-rs main + local Variant DML fixes + parquet sort-order pushdown. Fork
# `timefusion-variant-dml` (rebased onto upstream DataFusion 54). Pinned to a
# rev, not the branch tip: a moving branch changes Cargo.lock on every fork
# push, which busts cargo-chef's recipe.json and forces a full dependency
# recompile in CI. Bump deliberately to pick up fork changes:
#   cargo update -p deltalake --precise <new-sha>   (then update this rev)
deltalake = { git = "https://github.com/tonyalaribe/delta-rs-timefusion.git", rev = "e2e2c65e", features = [
  "datafusion",
  "s3",
] }
```

The *rev-not-branch* detail is a build-time economics decision, not a
correctness one: a moving branch invalidates `cargo-chef`'s dependency recipe on
every fork push, and a full dependency recompile of this tree is measured in tens
of minutes.

### The core four

**`datafusion` 54 — the query engine.** TimeFusion uses DataFusion the way a
database uses a query engine rather than the way an application uses a library: it
supplies its own `TableProvider` (`ProjectRoutingTable`), its own physical planner
extension for DML (`DmlQueryPlanner`), a stack of custom `AnalyzerRule`s and
`OptimizerRule`s (`src/read/optimizers.rs`, 3,068 lines), a custom `ExprPlanner`
for the `->` / `->>` JSON operators, dozens of UDFs, and a custom `ExecutionPlan`
node or two. Three things to know about this repo's usage:

1. Sessions are not interchangeable. There is a pgwire-facing session (which gets
   the rule that wraps Variant columns in `variant_to_json()` for the wire), and
   internal sessions used by maintenance and DML which deliberately omit it.
   Registering the wrong rule set on the wrong session changes results, not just
   performance.
2. `schema_force_view_types = false` is set on the session and on the DML session,
   because Variant's inner buffers must materialize as `Binary`, not `BinaryView`
   (`src/schema.rs:600–610`).
3. Recursion limit is raised to 512 (`#![recursion_limit = "512"]` in both
   `main.rs` and `lib.rs`) and Tokio workers get 32 MiB stacks, because planning
   depth on this schema is genuinely deep.

**`deltalake` (delta-rs) — ACID storage on object storage.** Delta gives
TimeFusion atomic multi-file commits, time travel, and a file-level statistics
index for pruning. This codebase leans on three Delta features hard: partition
columns (`project_id`, `date`) for pruning, per-file `tags` for maintenance
bookkeeping (rollup slice coverage lives in tags, which is why a stock `OPTIMIZE`
that strips tags is a *correctness* problem for the coordinator, not a cosmetic
one), and commit metadata for the WAL watermark that makes crash-mid-flush
recovery possible.

**`arrow` 58 — the in-memory format.** Everything internal is `RecordBatch`. Two
project-specific conventions: `Utf8` in the YAML means `Utf8View` in Arrow
(`src/schema.rs:585–586`) because zero-copy string ops matter at this volume; and
`RecordBatch` cloning is treated as free throughout the MemBuffer, because it
clones `Arc` pointers (~100 bytes) rather than data.

**`datafusion-postgres` + `pgwire` — the wire.** `DfSessionService` does the
protocol work; TimeFusion wraps it in `LoggingSimpleQueryHandler` /
`LoggingExtendedQueryHandler` to add tracing, statement timeouts, a giant-statement
admission gate, and interception of non-Postgres admin commands (`OPTIMIZE`,
`VACUUM`, `FLUSH`, `HANDOFF`, `DELTA HISTORY`, `DELTA ACTIONS`,
`DELTA RECOVERY AUDIT`). Chapter 13 walks that layer.

### The supporting cast, grouped by what they solve

**Durability and buffering.** `walrus-rust` (vendored, `vendor/walrus-rust`) is
the WAL engine: topic-partitioned append-only log with checkpointable read
cursors. `bincode` 2 serializes WAL entries. `fs4` provides the `flock` that makes
the WAL directory single-writer. `dashmap` gives the MemBuffer lock-free
concurrent reads over its table and bucket maps. `parking_lot` supplies the
non-async locks.

**Caching.** `foyer` 0.22 is a hybrid memory+disk cache; `src/storage.rs`
implements `object_store::ObjectStore` on top of it so every Delta read goes
through the cache transparently. `lru` backs the plan cache. `ahash` and `fnv` are
the hashers.

**Search and sketches.** `tantivy` 0.22 builds the sidecar full-text indexes;
`tar` + `zstd` package an index into a single blob for object storage; `memmap2`
maps it back. `tdigests` provides the `tdigest` rollup measure (approximate
percentiles); HLL sketches for approximate distinct counts are implemented in
`src/read/mod.rs`.

**Variant / JSON.** `datafusion-variant` (pinned upstream, no local changes)
supplies `json_to_variant` / `variant_to_json` / `variant_get`;
`parquet-variant`, `parquet-variant-compute`, and `parquet-variant-json` are the
encoding layer. `datafusion-functions-json` adds JSON functions. `sql-json-path`
(RisingWave's) implements the *PostgreSQL* SQL/JSON path dialect — chosen over
`serde_json_path` explicitly because RFC 9535 cannot parse the PG grammar
(`? (@ ..)`, `like_regex`), so `jsonb_path_exists` matches Timescale by
construction.

**Observability.** `tracing` + `tracing-subscriber` + `tracing-opentelemetry`,
`opentelemetry` 0.31 with the OTLP gRPC exporter, `metrics` +
`metrics-exporter-opentelemetry`, plus `datafusion-tracing` and
`instrumented-object-store` for engine- and store-level spans. Under
`--features profiling` (Linux only): `tikv-jemallocator` for heap profiling and
`pprof` for CPU sampling.

**Boilerplate elimination.** This is a stated project priority, and the dependency
list reflects it. `educe` is used for `Default` derives that need non-default field
values (`#[educe(Default = "postgres")]` in `AuthConfig`). `serde-inline-default`
exists because serde's `default = "..."` takes a *function path*, which would have
meant 138 hand-written functions for 138 config fields. `bon` provides typed
builders (`#[bon::bon]` on `LoggingHandlerFactory`). `derive_more`, `strum`,
`thiserror`, `tap`, `itertools`, and `scopeguard` fill the remaining gaps. The
`Cargo.toml` comments for `educe` and `serde-inline-default` state the
boilerplate count each one removed — that is the house style.

**Configuration and secrets.** `envy` deserializes the environment into the config
structs; `aes-gcm` + `base64` implement encrypted config secrets;
`dotenv` loads `.env`.

**Scheduling.** `croner` 3 parses cron expressions — and *only* parses them. The
comment is worth quoting because it is a good example of the project's
relationship with its dependencies:

**Listing 4.3 — Cargo.toml, croner**

```toml
# Cron parsing only. We drive the wall-clock loop ourselves (see
# spawn_cron_job) rather than tokio-cron-scheduler, which silently stopped
# dispatching ticks in prod (2026-07-13: 0 maintenance runs over 14h uptime).
croner = "3"
```

### The build profiles

**Listing 4.4 — Cargo.toml, `[profile.release]` and its rationale**

```toml
[profile.release]
lto = "thin"
codegen-units = 16
strip = "symbols"
```

The comment above it explains that whole-program `lto = "fat"` +
`codegen-units = 1` makes the final link single-threaded and *is* the dominant
deploy bottleneck; thin LTO with parallel codegen recovers most cross-crate
inlining at roughly 3× faster link, which is the right trade for a database
bottlenecked on IO and memory rather than inlined CPU. `[profile.release-iter]`
inherits release but keeps symbols so profilers resolve frames.

`[profile.dev]` sets `debug = "line-tables-only"`, and carries a long comment
recording an experiment that was tried and rejected: setting
`[profile.dev.package."*"] opt-level = 2` took the suite from 72 s to 63 s (11%)
but cost a 13-minute dependency rebuild every time `Cargo.lock` moved, and broke
one test whose merge became too fast to observe. "Not worth it; don't re-run the
experiment."

### Dev-only dependencies worth knowing

`cargo-nextest` is not a dependency but is mandatory: `cargo test` runs the
integration binary in one process where `#[serial]` serializes most tests, taking
553 s; nextest runs one process per test from a single pool and finishes the same
617 tests in ~74 s. `sqllogictest` (git, RisingLight's) drives the `.slt` files.
`insta` provides snapshot assertions, `proptest` property tests, `test-case`
parameterized cases, `criterion` benchmarks, `testcontainers` +
`testcontainers-modules` the *fallback* MinIO for e2e, and `tracing-test` log
assertions. Chapter 26 covers how they fit together.

**Key takeaways.** Five forks, each removing exactly one upstream obstacle;
DataFusion and delta-rs used as engine substrate rather than as libraries; Arrow
`Utf8View` and `Binary`-not-`BinaryView` as project-wide conventions; a deliberate
preference for derive-macro crates over hand-written boilerplate; and build
profiles tuned for deploy turnaround rather than peak inlining. When a dependency
here looks unusual, read its `Cargo.toml` comment first — it almost certainly names
the incident.
## 5. Domain glossary

*Every internal term, codename and abbreviation used in the code, with the type
that embodies it and the chapter that covers it. Read it once now; come back when
a comment uses a word you do not recognize.*

### Tenancy and tables

**project_id** — the tenant identifier, a Delta partition column, the WAL topic
component, and the routing key. Not an authorization boundary (Ch. 28).

**TableKey** — `(project_id, table_name)`. `(Arc<str>, Arc<str>)` in the
MemBuffer, `(String, String)` in `Database`. Identifies a Delta table, a WAL
topic, a commit lock, a DML lock. `src/write/mem_buffer.rs:248` (Ch. 7).

**unified table** — one Delta table per schema, shared by all default projects,
partitioned by `[project_id, date]`. Path
`s3://bucket/prefix/default/{table}/` (Ch. 2).

**custom project table** — an isolated table for one project, possibly with its
own bucket and credentials. Path
`s3://bucket/prefix/projects/{project_id}/{table}/` (Ch. 2, Ch. 28).

**partition** — a `(project_id, date)` directory. The unit of pruning, dedup
certification, rollup coverage, and most maintenance.

**`{table}__bulk`** — a second registered provider over the same physical table
whose `INSERT`s bypass the WAL and MemBuffer. For backfills and DLQ drains.
`ProjectRoutingTable::with_skip_queue` (Ch. 7).

### Schema

**TableSchema** — the YAML declaration, parsed once into a `OnceLock` registry.
`src/schema.rs:305` (Ch. 6).

**Variant** — the semi-structured column type. Physically
`Struct{metadata: Binary, value: Binary}`; presented to SQL as `Utf8View` so
`VALUES` accepts JSON literals (Ch. 6, Ch. 14).

**insert-compatible schema** — the lying, SQL-facing view where Variant columns
appear as `Utf8View`. `create_insert_compatible_schema`, `src/schema.rs:801`.

**real schema** — the storage view, with actual Variant types.
`ProjectRoutingTable::real_schema`.

**dedup_keys** — the composite identity of a logical row. `(timestamp, id)` on
`otel_logs_and_spans`.

**dedup_tiebreak** — the column whose greatest value wins among versions of one
key. TF-owned (`updated_at`) on merge-on-read tables; stamped on every write.

**tombstone_column** — a nullable `Boolean` marking a row version as a deletion.
NULL and `false` both mean live.

**version_append** — the per-table flag making `UPDATE`/`DELETE` append a new row
version rather than rewrite files.

**mutable** — a per-column declaration that an `UPDATE` may change it. **Columns
are immutable by default**, which is what lets a filter be pushed below the dedup.
Enforced at DML plan time (Ch. 6, Ch. 16).

**identity columns** — the seven columns every synthesized rollup table carries:
`project_id`, `timestamp`, `date`, `id`, `updated_at`, `deleted`,
`rollup_generation` (Ch. 6).

**triple underscore** — the flattening convention for OTel nested paths:
`attributes___http___response___status_code` is
`attributes.http.response.status_code`.

### Write path

**BufferedWriteLayer** — the write orchestrator: admission, reservation, WAL,
MemBuffer, flush (Ch. 10).

**MemBuffer** — `DashMap<TableKey, TableBuffer>`, each a `DashMap<i64, TimeBucket>`
(Ch. 12).

**bucket / time bucket** — rows whose event timestamp falls in one window.
`bucket_id = timestamp_micros / bucket_duration_micros`. 5-minute compiled
default.

**sealed bucket** — one whose window has closed (`bucket_id < current`) and which
is therefore flushable.

**open bucket** — the current window, still accepting inserts. Excluded from
routine flushing.

**force-flush** — draining a still-open bucket under memory pressure. Marks the
bucket `force_flushed` so its window stays exempt from the Delta-scan exclusion
forever after.

**flush dwell** — how long a sealed bucket has waited to flush
(`now - created_micros`). Deliberately not the rows' event age.

**hold** — a WAL read-cursor floor. While a bucket is unflushed, the cursor may
not advance past its hold. Four lifecycle stages: pending, in-MemBuffer,
in-flight, orphaned (Ch. 10).

**airborne** — a bucket taken out of MemBuffer for an in-flight Delta commit.

**orphaned hold** — a hold for rows a failed commit could not restore. They exist
only in the WAL until a restart replays them.

**watermark (WAL)** — how far the read cursor may safely advance, computed from
the tail and the registered holds (Ch. 11).

**watermark (Delta-flushed)** — the max row timestamp ever handed to a Delta
commit, per `(project, table)`. A query starting above it can skip Delta (Ch. 13).

**backpressure** — flushing to Delta to free RAM instead of rejecting a write.
Single-flighted through `relief_lock` (Ch. 10).

**reservation** — bytes claimed for an in-flight write via a CAS loop, released
once MemBuffer accounts for them.

**hard limit** — 120% of the ingest budget (`max + max/5`), where live writers are
rejected.

**quarantine** — parked payloads on disk that could not be decoded (WAL) or merged
(DML). Recoverable via `timefusion redrive-dml` (Ch. 11, Ch. 16).

### WAL

**topic** — `{project_id}:{table_name}`, the logical WAL stream.

**shard** — one of N walrus collections per topic (default 4), to escape walrus's
per-collection write lock.

**walrus** — the vendored WAL engine (`vendor/walrus-rust`).

**frontier (replay)** — the position of the *next* entry each shard will yield.
The honest watermark baseline, since the reader prefetches one entry ahead.

**cursor snapshot** — the JSON record of every topic's per-shard cursor, written
after every flush and on graceful shutdown.

**clean_shutdown** — "the shutdown path wrote this snapshot". Authorizes skipping
the Delta cursor scan.

**drained** — "nothing was left un-flushed". The *sole* authorizer of the boot WAL
GC sweep, and consumed on use.

**takeover request** — a marker file a blocked contender writes once, asking the
lock holder to exit. Its *age* drives escalation (Ch. 11, Ch. 22).

### Read path

**leg** — one branch of the scan union: `Mem`, `Hot`, or `Delta`. `LegKind`
carries its own sortability (Ch. 15).

**ProjectRoutingTable** — the `TableProvider` and `DataSink` for every user-visible
table (Ch. 7).

**DedupExec** — the merge-on-read operator: keep-greatest by tiebreak within each
key group (Ch. 15).

**keep-greatest / keep-first** — the two dedup modes. Keep-greatest needs an
ordered input; without it the operator degrades to keep-first.

**bounded / unbounded dedup** — bounded exploits an ordered input to emit per
*run* (64 MiB); unbounded buffers to end-of-stream (2 GiB).

**run** — a stretch of rows sharing one bound value (one timestamp).

**bound** — the leading sort column tracked to detect run boundaries. Always
retained in the dedup key, so a lying footer can only under-dedup.

**certification** — proof that a `(project, table, date)` partition is
duplicate-free over a specific file-set fingerprint. Lets the read path skip
`DedupExec` (Ch. 8, Ch. 13).

**fingerprint (fp)** — a hash of a partition's live file set. Any commit changes
it, invalidating the certification.

**DedupSkipVerdict** — why the skip was granted or refused. Six variants, each a
metric (Ch. 7).

**range exclusion** — the predicates that stop Delta from returning rows the
MemBuffer or hot leg is authoritative for.

**version gate** — `OR updated_at > gate`, weakening the range exclusion on
merge-on-read tables because an appended version carries the original timestamp.

**skip-Delta** — the fast path where a query's lower time bound is above the
Delta-flushed watermark.

**GatedScanExec** — the operator bounding Parquet decode heap, which no memory
pool tracks (Ch. 7).

**decode units** — sub-divisions of a reader slot (16 per slot), claimed in
proportion to the last batch's size.

**pressure valve** — the tiered escalation that makes a decode poll claim more of
the semaphore as RSS approaches the limit.

**scan-bypass** — task-local suppression of *cache population* during a deep scan,
so it cannot evict the hot tail.

**plan cache / shape cache** — the two-level cross-connection cache: exact text,
and literal-erased shape (Ch. 14).

**prefilter** — the Tantivy index lookup that narrows a scan four ways: `id IN`,
covered files, excluded files, row selections (Ch. 13, Ch. 21).

**text_match** — the UDF with two implementations (index lookup, row fallback)
where the fallback must be a *superset* of the index.

**covered files / zero-hit files / field coverage gap** — what an index proves
about the files it covers, and the case where it cannot.

### Rollups

**rollup** — a continuous aggregate declared on the source table, its schema
synthesized (Ch. 6, Ch. 18).

**grain** — the bucket width (`1m`, `1h`).

**base tier / derived tier** — a rollup built from raw rows, versus one built by
re-aggregating a finer rollup.

**dimension** — a column a query may `GROUP BY` **or filter on** and still route.

**measure** — a stored decomposable aggregate: `count`, `sum`, `min`, `max`,
`tdigest`, `hll`.

**state** — a partial aggregate that can be merged. A sketch is a state; an average
is not, which is why `Avg` has arity 2.

**generation** — a deterministic id for one rollup spec + source + project + date.
Excludes the source fingerprint so slices merge.

**coverage / ticket** — the record that a rollup's rows are valid for the source
they aggregate, plus the bound it was proved to.

**covered_through** — the exclusive upper bound on source timestamps a build
aggregated. Stored, never recomputed.

**hybrid routing** — unioning the rollup's certified interior with raw scans over
the fringes.

**interior** — the certified region, which must partition the row set for the
merges to be sound.

**MissReason** — the fifteen reasons a query cannot use a rollup. The variant names
are the telemetry labels (Ch. 18).

**contiguity** — consecutive days of coverage counting back from yesterday.
`rollup_min_contiguous_days` is the goal metric.

### Maintenance

**coordinator** — the durable maintenance scheduler (Ch. 17).

**unit / task** — one `(physical_table, source, project_id, slice, operation)`.

**slice** — a `[start, end)` time range. Mint width 10 min; the lattice is
10 min / 1 h / 6 h / 1 day.

**coarsen** — fusing adjacent sealed units into a wider one whose estimate still
fits the budget.

**subsume** — removing a fine unit already covered by a wider one.

**Superseded** — the terminal state meaning a wider unit now covers this one.

**quarantined (unit)** — a unit that has timed out `QUARANTINE_ATTEMPTS` times and
may only run under a small dedicated permit pool.

**operation cycle** — the ten-slot mix a worker rotates through.
`CYCLE_BALANCED` / `CYCLE_COVERAGE_SHORT`.

**derived operation** — one fully re-derivable from a storage scan, and therefore
deliberately not persisted.

**scheduling class** — the priority tier used by `claim_next`. A *hole* outranks a
*re-derive*.

**sealed reservation** — the share of claims reserved for sealed (non-frontier)
work, suspended when frontier lag exceeds budget.

**frontier / live frontier** — the leading edge of ingest.

**debt** — file-rewriting work that cannot advance rollup coverage:
`HotPacking`, `SealedConsolidation`, `Dedup`, `Repair`.

**wave** — a batch of staged units committed together against one table.

**staged intent** — output written but not yet committed. What
`TIMEFUSION_REPAIR_RESUME_ENABLED` resumes against.

**converged** — a file at or past 7/8 of target, which must not be re-selected
alone.

**hot-tail packing** — bin-packing today's small files.

**sealed consolidation** — packing sealed days to the cold target.

**footer repair** — rewriting a file whose Parquet footer lacks `sorting_columns`.

**recompress** — the only force-rewrite; the only path that can fix a single
at-target file with a poisoned footer.

**dirty bin** — a `(project, table, date, 10-min bin)` recorded after a Delta
append, feeding the dedup sweep.

**slice tags** — the Delta file tags (`timefusion.slice_start_micros`, etc.) that
let a republication compute its replace-set. An untagged file is immortal.

### Storage and caching

**Delta** — the transaction log over Parquet on object storage. Authoritative
after flush.

**Foyer** — the two-tier (memory + disk) object cache implementing `ObjectStore`
(Ch. 8).

**L1 / L2** — Foyer's memory and disk tiers.

**block (Foyer)** — the disk eviction unit and the hard cap on a cacheable entry.

**metadata cache** — Foyer's second instance, for `_delta_log` and Parquet
footers, kept separate so a wide scan cannot evict them.

**range alignment** — rounding Parquet data reads to 1 MiB so a sliding dashboard
window reuses cache entries.

**sidecar** — a best-effort local JSON file: certifications, dirty bins, delta
snapshots, cursor snapshot. Degrades to empty (Ch. 8).

**hot tier** — the local Arrow IPC cache of demoted buckets. Measured a net loss
and being removed (Ch. 20).

**demote** — writing a flushed bucket to the hot tier.

**covers_window** — the hot-tier claim that a file holds *every* row in its span at
or below its stamp. The precondition for excluding that window from Delta.

### Memory

**budget tree** — the derivation of every pool from one detected cgroup limit
(Ch. 7).

**untracked slack** — 15% reserved for consumers no pool tracks: Parquet decode,
pgwire parse ASTs, allocator overhead.

**memory brake** — the wave-boundary safety valve at 80% of the budgeted limit.

**spill threshold** — a `FairSpillPool` bound; exceeding it degrades to disk spill
rather than failing.

**unspillable merge** — a sort partition's merge operator, which cannot spill.
More partitions means more unspillable merges competing for one pool.

### Protocol and operations

**pgwire** — the PostgreSQL wire protocol layer.

**early bind** — the responder that occupies `:5432` during startup, answering
SQLSTATE `57P03` instead of `ECONNREFUSED` (Ch. 9).

**57P03** — "the database system is starting up". Transient to real clients.

**admin commands** — `OPTIMIZE`, `VACUUM`, `FLUSH`, `HANDOFF`, `DELTA HISTORY`,
`DELTA ACTIONS`, `DELTA RECOVERY AUDIT` (Ch. 13).

**HANDOFF** — the leased write fence that makes a start-first deploy exit in
constant time (Ch. 22).

**handoff readiness** — fenced, quiesced, and drained. What lets a replacement
trigger the predecessor's exit.

**giant statement** — one over 2 MiB, admitted through a 2-permit gate.

**timefusion_stats** — the in-process diagnostics table, queryable over SQL
(Ch. 27).

**start-first / stop-first** — deploy shapes. Start-first overlaps the
replacement with the predecessor and hands over the WAL lock.

### Project-specific names

**monoscope / APIToolkit** — the observability product TimeFusion backs. Its
enrichment pipeline is the source of the `UPDATE … FROM` workload, the
`hashes` mutable column, and the DML coalescer.

**whale** — a very large tenant whose partitions dominate maintenance cost.

**shipbubble** — a tenant that was quarantined after repeated staging failures.

**CapRover / Swarm** — the deployment platform.

**R2 / OVH / MinIO** — object-storage backends: Cloudflare R2 (historical), OVH
(current production), MinIO (local and CI).
# Part II — The spine: data model and core abstractions

## 6. The schema registry and the core data model

*After this chapter you will know exactly what a TimeFusion table is, how a table
is declared, how its Arrow and Delta schemas are derived from that declaration,
what the Variant type is and why it needs two schemas, and what the seven
"identity" columns mean. You will have read the definition of every type in the
data model.*

### There is no `CREATE TABLE`

TimeFusion tables are not created by SQL. They are declared as YAML files in
`schemas/`, compiled into the binary at build time, and materialized on object
storage on first write. `src/schema.rs:642` is the whole mechanism:

**Listing 6.1 — src/schema.rs:641–642**

```rust
// Include all YAML files from schemas directory at compile time
static SCHEMAS_DIR: Dir = include_dir!("$CARGO_MANIFEST_DIR/schemas");
```

Five files live there: `otel_logs_and_spans.yaml` (457 lines — the production
table), `otel_metrics.yaml`, and three fixtures used by tests (`mor_versioned`,
`mor_dormant`, `variant_bench`).

The registry parses, validates, and synthesizes exactly once, behind a
`OnceLock`:

**Listing 6.2 — src/schema.rs:736–762**

```rust
// Global registry instance.
//
// IMPORTANT: The registry is loaded once via `include_dir!` and `OnceLock`,
// so schemas are immutable for the lifetime of the process. Several
// downstream caches rely on this invariant for correctness (not just perf):
//   - `optimizers::indexed_columns_for` (per-table tokenizer map)
//   - `plan_cache::PlanCacheHook` (LogicalPlan embeds SchemaRef at parse time)
// If hot-reload of YAML schemas is ever added, those caches must gain a
// schema-version token in their key (or be flushed on reload).
static SCHEMA_REGISTRY: OnceLock<SchemaRegistry> = OnceLock::new();

pub fn registry() -> &'static SchemaRegistry {
    SCHEMA_REGISTRY.get_or_init(SchemaRegistry::new)
}

pub fn get_schema(table_name: &str) -> Option<&'static TableSchema> {
    registry().get(table_name)
}

pub fn get_default_schema() -> &'static TableSchema {
    registry().get_default().expect("No schemas available in registry")
}

/// `get_schema(table_name)`, falling back to the default schema — the common case at every call site.
pub fn schema_or_default(table_name: &str) -> &'static TableSchema {
    get_schema(table_name).unwrap_or_else(get_default_schema)
}
```

That comment is the kind that saves an afternoon: schema immutability is not a
convenience, it is a *correctness precondition* for two downstream caches. Adding
YAML hot-reload would silently break the plan cache, because a cached
`LogicalPlan` embeds the `SchemaRef` it was parsed against.

### `TableSchema`: the whole declaration

This is the central type of the data model. Read it once carefully; every later
chapter refers back to some field of it.

**Listing 6.3 — src/schema.rs:305–378**

```rust
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TableSchema {
    pub table_name: String,
    /// Continuous-aggregate rollups derived FROM this table. Declared here
    /// rather than as separate schema files so a rollup cannot drift from its
    /// source; the rollup's own `TableSchema` is synthesized at load
    /// (`RollupSpec::synthesize`) and registered under `{table}_rollup_{grain}`.
    #[serde(default)]
    pub rollups: Vec<RollupSpec>,
    pub partitions: Vec<String>,
    pub sorting_columns: Vec<SortingColumnDef>,
    pub z_order_columns: Vec<String>,
    pub fields: Vec<FieldDef>,
    /// Column the optimizer should rewrite into a `date` partition filter.
    /// Defaults to `"timestamp"` for back-compat with existing schemas.
    #[serde(default)]
    pub time_column: Option<String>,
    /// Composite key for last-write-wins dedup at flush time. Empty = no dedup
    /// (append-only). E.g. `[id, timestamp]`. Variant columns rejected at load.
    /// Only collapses dupes inside one bucket; cross-bucket dupes need the
    /// read-side row_number() rewrite.
    #[serde(default)]
    pub dedup_keys: Vec<String>,
    /// Tie-breaker column for dedup: when rows share `dedup_keys`, keep the one
    /// with the greatest value here (ties → last seen, the back-compat default;
    /// NULL sorts lowest, so an un-stamped legacy row always loses).
    /// On a [`Self::version_append`] table the tiebreak is TF-OWNED: every write
    /// stamps it from a per-table monotonic clock (`insert_coerce::stamp_version`),
    /// so the newest version of a row wins deterministically. Everywhere else it
    /// is client-supplied and TF never writes it. `None` = keep-last by position.
    #[serde(default)]
    pub dedup_tiebreak: Option<String>,
    /// Nullable `Boolean` column marking a row version as a DELETION of its
    /// `dedup_keys` tuple (merge-on-read). `None` = the table has no tombstones
    /// and every mechanism below is a no-op. NULL and `false` both mean live —
    /// only `true` is a tombstone — so a table can declare the column before a
    /// single tombstone exists (and before any backfill) with zero effect.
    ///
    /// Independent of [`Self::version_append`] on purpose: read-side filtering
    /// and the sweep's version collapse key off this column alone, so they come
    /// alive (as no-ops) without waiting for the write path.
    #[serde(default)]
    pub tombstone_column: Option<String>,
    /// Merge-on-read WRITE path: `UPDATE`/`DELETE` append a new row version
    /// (with a fresh `dedup_tiebreak`, and `tombstone_column = true` for a
    /// delete) instead of planning a Delta MERGE. Per-table opt-in; false =
    /// today's in-place mutation. Requires `dedup_keys`, `dedup_tiebreak` and
    /// `tombstone_column`.
    // ... elided: ~25 lines of rationale, quoted separately below
    #[serde(default)]
    pub version_append: bool,
}
```

Eleven fields, and every one of them is load-bearing somewhere in Part III:

- `partitions` → the Delta partition columns and the directory layout on S3.
- `sorting_columns` → the Parquet footer's `SortingColumn` list, which drives the
  sort-order pushdown and streaming TopK (Chapter 13).
- `time_column` → what the optimizer rewrites into a `date` partition filter, so
  a user's `WHERE timestamp >= …` prunes partitions without an explicit `date`
  predicate.
- `dedup_keys` + `dedup_tiebreak` + `tombstone_column` + `version_append` → the
  entire merge-on-read machinery (Chapter 15).
- `rollups` → the continuous aggregates (Chapter 18).

### The three merge-on-read flags, and the trap in each

These four fields interact in a way that has broken production twice, and the code
carries both scars.

**`version_append` is a *write-path* flag.** When true, `UPDATE`/`DELETE` append a
new row version rather than rewriting files. The doc comment records what happened
when it was first turned on:

**Listing 6.4 — src/schema.rs:361–377 (continuation of `version_append`'s doc)**

```rust
    /// [`Self::tombstones_possible`] deliberately does NOT gate on this flag:
    /// versions already written outlive the flag being turned off. Anything
    /// whose correctness depends on what is IN STORAGE must key off the column.
    ///
    /// 2026-08-02: enabling this on `otel_logs_and_spans` / `otel_metrics` first
    /// made recent-window reads unusable (1h ~13s, 3h timing out). Dedup becomes
    /// mandatory, bounded dedup needed ordered input, and the appended version
    /// carries the row's ORIGINAL timestamp into a NEW file — so Delta files
    /// overlap in time, no ordering can be declared, and the planner inserted a
    /// query-time `SortExec` that exhausted the 27.5GB query pool.
    ///
    /// Fixed in `read_dedup`, not by giving up the feature: keep-greatest now
    /// runs WITHOUT a bound (buffering to end-of-stream), so nothing has to
    /// manufacture an ordering. `mor_delta_leg_sorts` staying 0 is the signal
    /// that this is holding.
```

**`tombstones_possible()` is a *storage* predicate, not a flag read.** This is the
most instructive small function in the file:

**Listing 6.5 — src/schema.rs:400–427**

```rust
    /// Can a tombstone row EXIST in this table's storage? True as soon as the
    /// column is declared, regardless of the write path.
    ///
    /// The old rule was `column && version_append`, guarded by a warning to
    /// "never flip the flag off after tombstones were written". That warning was
    /// correct and the hazard was real: disabling merge-on-read on
    /// `otel_logs_and_spans` (2026-08-02) would have instantly re-enabled
    /// COUNT(*)-from-stats over files still holding tombstoned rows, counting
    /// deleted rows as live. Rather than leave a flag whose safety depends on
    /// never being turned off, this now tracks STORAGE: a declared column means
    /// a tombstone may be down there, whatever the writer is doing today.
    ///
    /// The cost is that a table which declares the column but has never
    /// tombstoned anything gives up the stats fast path. That is the right side
    /// to err on — the alternative silently over-counts. A table that genuinely
    /// wants the fast path simply must not declare a `tombstone_column`.
    pub fn tombstones_possible(&self) -> bool {
        // ... elided: the comment above, repeated inline
        self.tombstone_column.is_some()
    }
```

The generalizable lesson, and one worth carrying into your own code: *a flag whose
safety depends on never being turned off is a bug.* The fix was not better
documentation; it was re-deriving the predicate from something that cannot be
un-done — the presence of the column.

`validate()` enforces the coupling at load, so a malformed schema fails at process
start rather than at the first tombstoned read:

**Listing 6.6 — src/schema.rs:429–454**

```rust
    fn validate(&self) -> anyhow::Result<()> {
        let field = |role: &str, name: &str| {
            self.fields
                .iter()
                .find(|f| f.name == name)
                .ok_or_else(|| anyhow::anyhow!("schema `{}`: {role} references unknown field `{}`", self.table_name, name))
        };
        self.dedup_keys.iter().map(|k| ("dedup_keys", k)).chain(self.dedup_tiebreak.iter().map(|tb| ("dedup_tiebreak", tb))).try_for_each(
            |(role, name)| -> anyhow::Result<()> {
                anyhow::ensure!(field(role, name)?.data_type != "Variant", "schema `{}`: {role} cannot be a Variant column `{}`", self.table_name, name);
                Ok(())
            },
        )?;
        if let Some(tc) = &self.tombstone_column {
            let f = field("tombstone_column", tc)?;
            // Nullable Boolean is load-bearing: NULL must be a legal "live"
            // encoding so existing rows need no backfill.
            anyhow::ensure!(f.data_type == "Boolean" && f.nullable, "schema `{}`: tombstone_column `{}` must be a nullable Boolean field", self.table_name, tc);
        }
        anyhow::ensure!(
            !self.version_append || (!self.dedup_keys.is_empty() && self.dedup_tiebreak.is_some() && self.tombstone_column.is_some()),
            "schema `{}`: version_append requires dedup_keys, dedup_tiebreak and tombstone_column",
            self.table_name
        );
        Ok(())
    }
```

### `FieldDef`, and the `mutable` flag that cost an outage

**Listing 6.7 — src/schema.rs:464–506**

```rust
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FieldDef {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    #[serde(default)]
    pub tantivy: Option<TantivyFieldConfig>,
    /// Opt-out for dictionary encoding. Default on. Set false for high-entropy
    /// free-text columns (stacktraces, raw queries, full URLs) where dict just
    /// builds a useless 8MB before falling back to PLAIN — wasted writer pass.
    #[serde(default)]
    pub dictionary: Option<bool>,
    /// Per-column bloom filter opt-in. Default off. Enable for high-cardinality
    /// equality-lookup columns (ids, trace_ids, span_ids, session_ids).
    #[serde(default)]
    pub bloom_filter: bool,
    /// Declares that an UPDATE may change this column, so versions of one row
    /// can disagree on its value.
    ///
    /// **Columns are immutable by default.** That default is what makes point
    /// lookups cheap: a filter on an immutable column is safe to push BELOW the
    /// merge-on-read `DedupExec`, because every version of a key agrees, so the
    /// predicate keeps or drops a key group whole and dedup-then-filter equals
    /// filter-then-dedup. A filter on a MUTABLE column must stay above the
    /// dedup, or a stale version could match a predicate the winning version no
    /// longer satisfies.
    ///
    /// Getting this backwards is expensive. When every non-key column was
    /// treated as mutable, a single `context___trace_id` lookup over 24h had its
    /// predicate stranded above the dedup, so the engine coalesced 623 hot-tier
    /// files plus the Delta legs into one partition and materialised the ENTIRE
    /// window keep-greatest before discarding almost all of it — ~4.5 GB/s, 84%
    /// to 93% of the cgroup limit in ~1.5s, and the instance was killed
    /// (2026-08-20, exit 137).
    ///
    /// The declaration is ENFORCED, not trusted: `extract_dml_info` refuses at
    /// plan time any UPDATE that assigns a column this does not mark, so the
    /// read path's premise cannot be broken by a writer. The version tiebreak
    /// and tombstone columns are always treated as mutable without declaring it
    /// — they vary across versions by construction.
    #[serde(default)]
    pub mutable: bool,
}
```

This is the single most important field in the data model for query performance,
and it is worth stating the invariant in isolation because it recurs in Chapters 13
and 16:

> A filter on an **immutable** column commutes with merge-on-read dedup, so it can
> be pushed below `DedupExec` and evaluated at the Parquet scan (bloom filters,
> row-group stats, Tantivy). A filter on a **mutable** column does not commute and
> must stay above the dedup. Columns are immutable by default; `mutable: true` is
> a declaration, and the DML planner *refuses* an `UPDATE` that assigns a column
> not so declared.

On `otel_logs_and_spans` exactly one column is declared mutable — `hashes`, the
list the enrichment pipeline appends tags to:

**Listing 6.8 — schemas/otel_logs_and_spans.yaml (the `hashes` field)**

```yaml
  - name: hashes
    data_type: "List(Utf8)"
    nullable: true
    # The one column production UPDATEs on this table (monoscope's enrichment
    # appends tags). Columns are immutable by DEFAULT, because a filter on an
    # immutable column can be pushed below the merge-on-read dedup and pruned at
    # the scan; a filter on this one must stay above it, or an older version
    # could match a tag the winning version no longer carries.
    mutable: true
```

### From YAML strings to two schemas

`data_type` is a string, and it is parsed twice — once into Arrow, once into
Delta. The two functions are the type system of the whole database.

**Listing 6.9 — src/schema.rs:583–620**

```rust
fn parse_arrow_data_type(s: &str) -> anyhow::Result<ArrowDataType> {
    Ok(match s {
        // Use Utf8View for better performance with zero-copy string operations
        "Utf8" => ArrowDataType::Utf8View,
        "Date32" => ArrowDataType::Date32,
        "Boolean" => ArrowDataType::Boolean,
        "Int32" => ArrowDataType::Int32,
        "Int64" => ArrowDataType::Int64,
        "Float64" => ArrowDataType::Float64,
        "Binary" => ArrowDataType::Binary,
        "UInt32" => ArrowDataType::UInt32,
        "UInt64" => ArrowDataType::UInt64,
        "List(Utf8)" => ArrowDataType::List(Arc::new(Field::new("item", ArrowDataType::Utf8View, true))),
        "List(Int64)" => ArrowDataType::List(Arc::new(Field::new("item", ArrowDataType::Int64, true))),
        "List(Float64)" => ArrowDataType::List(Arc::new(Field::new("item", ArrowDataType::Float64, true))),
        "Timestamp(Microsecond, None)" => ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
        "Timestamp(Microsecond, Some(\"UTC\"))" => ArrowDataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        // Variant: declare the inner buffers as Binary to match
        // `delta_kernel::unshredded_variant()`. delta-rs's kernel rejects
        // schema mismatches at scan validation time even when no data
        // files exist (e.g. fresh DELETE on an empty table). Both
        // MemBuffer and Delta reads end up as Binary because:
        //   - the parquet reader honors `schema_force_view_types=false`
        //     (set in our session and in `delta_session_from` for DML);
        //   - `convert_variant_columns` casts VariantArrayBuilder's
        //     BinaryView output to Binary before MemBuffer ever sees it.
        // The ExtensionType marker (`VARIANT_EXT_KEY`) is added to the Field's
        // metadata in `fields()`.
        "Variant" => ArrowDataType::Struct(
            vec![
                Arc::new(Field::new(VARIANT_METADATA_FIELD, ArrowDataType::Binary, false)),
                Arc::new(Field::new(VARIANT_VALUE_FIELD, ArrowDataType::Binary, false)),
            ]
            .into(),
        ),
        _ => anyhow::bail!("Unknown type: {}", s),
    })
}
```

Two project-wide conventions are established right here. `Utf8` in YAML means
`Utf8View` in Arrow — every string column in this database is a view type. And
`Variant` is a `Struct { metadata: Binary, value: Binary }`, *not* `BinaryView`,
because delta-kernel validates against `unshredded_variant()` even when there is
no data to scan.

The Delta side is simpler and lossier — `UInt32` and `Int32` both become Delta
`Integer`, and every `Timestamp*` becomes `Timestamp`:

**Listing 6.10 — src/schema.rs:622–639**

```rust
fn parse_delta_data_type(s: &str) -> anyhow::Result<DeltaDataType> {
    use PrimitiveType::*;
    Ok(match s {
        "Utf8" => DeltaDataType::Primitive(String),
        "Date32" => DeltaDataType::Primitive(Date),
        "Boolean" => DeltaDataType::Primitive(Boolean),
        "Int32" | "UInt32" => DeltaDataType::Primitive(Integer),
        "Int64" | "UInt64" => DeltaDataType::Primitive(Long),
        "Float64" => DeltaDataType::Primitive(Double),
        "Binary" => DeltaDataType::Primitive(Binary),
        "List(Utf8)" => DeltaDataType::Array(Box::new(ArrayType::new(DeltaDataType::Primitive(String), true))),
        "List(Int64)" => DeltaDataType::Array(Box::new(ArrayType::new(DeltaDataType::Primitive(Long), true))),
        "List(Float64)" => DeltaDataType::Array(Box::new(ArrayType::new(DeltaDataType::Primitive(Double), true))),
        "Variant" => DeltaDataType::unshredded_variant(),
        _ if s.starts_with("Timestamp") => DeltaDataType::Primitive(Timestamp),
        _ => anyhow::bail!("Unknown type: {}", s),
    })
}
```

### `schema_ref()` and the partition-column reordering

Delta emits partition columns last. TimeFusion's Arrow schema must match that
order or every batch is misaligned:

**Listing 6.11 — src/schema.rs:527–558**

```rust
impl TableSchema {
    pub fn fields(&self) -> anyhow::Result<Vec<FieldRef>> {
        self.fields
            .iter()
            .map(|f| {
                let field = Field::new(&f.name, parse_arrow_data_type(&f.data_type)?, f.nullable);
                // Without the ExtensionType marker fresh tables (variant_bench)
                // crash on the first INSERT — see `VARIANT_EXT_KEY`.
                Ok(Arc::new(match f.data_type.as_str() {
                    "Variant" => field.with_metadata(HashMap::from([(VARIANT_EXT_KEY.to_string(), VARIANT_EXT_VALUE.to_string())])),
                    _ => field,
                }) as FieldRef)
            })
            .collect()
    }

    pub fn columns(&self) -> anyhow::Result<Vec<StructField>> {
        self.fields.iter().map(|f| Ok(StructField::new(&f.name, parse_delta_data_type(&f.data_type)?, f.nullable))).collect()
    }

    pub fn schema_ref(&self) -> SchemaRef {
        // Partition columns move to the end to match Delta Lake's output order,
        // order preserved within each group.
        let all_fields = self.fields().unwrap_or_else(|e| panic!("Failed to build schema for table {}: {e:?}", self.table_name));
        let partition_set = self.partition_set();
        let (partition_fields, data_fields): (Vec<_>, Vec<_>) = all_fields.into_iter().partition(|f| partition_set.contains(f.name().as_str()));
        Arc::new(Schema::new(data_fields.into_iter().chain(partition_fields).collect::<Vec<_>>()))
    }
```

The same reordering hazard bites the Parquet footer, and the fix is a good example
of a bug that is invisible until you look for it:

**Listing 6.12 — src/schema.rs:560–580**

```rust
    pub fn sorting_columns(&self) -> Vec<SortingColumn> {
        // Parquet data files omit partition columns (they live in the path), so
        // the `SortingColumn.column_idx` the footer records must be the column's
        // position among the *non-partition* fields — the physical parquet leaf
        // order the reader (`ordering_from_parquet_metadata`) indexes into. Using
        // the raw fields-list index over-counts by every partition column that
        // precedes a sort key (e.g. `date` at field 0), so the footer points at
        // the wrong column and the sort-order pushdown silently never fires.
        let partition_set = self.partition_set();
        let data_cols: Vec<&str> = self.fields.iter().map(|f| f.name.as_str()).filter(|n| !partition_set.contains(n)).collect();
        self.sorting_columns
            .iter()
            .filter_map(|col| {
                data_cols.iter().position(|n| *n == col.name).map(|idx| SortingColumn {
                    column_idx: idx as i32,
                    descending: col.descending,
                    nulls_first: col.nulls_first,
                })
            })
            .collect()
    }
```

`date` is field index 0 in the YAML and is a partition column, so the naive index
over-counts by exactly one, the footer advertises the wrong column, and *nothing
fails* — the sort-order pushdown just never fires and every `ORDER BY timestamp
DESC LIMIT n` becomes a blocking sort. The regression test
`sorting_columns_index_excludes_partitions` (`src/schema.rs:903–920`) pins it.

### Variant: one type, two schemas

Variant is TimeFusion's semi-structured column type — the OTel `body`,
`attributes`, `resource`, and `severity` columns are Variants. Physically it is a
`Struct { metadata: Binary, value: Binary }` carrying the Parquet Variant binary
encoding. Logically, SQL clients want to write JSON strings into it and read JSON
strings out.

DataFusion will not let you `INSERT INTO t (v) VALUES ('{"a":1}')` into a Struct
column, and there is no extension hook to make it. So TimeFusion keeps two views
of the schema and converts between them:

**Listing 6.13 — src/schema.rs:785–822**

```rust
/// Replaces Variant fields with Utf8View on a schema. This is the schema we hand to the
/// SQL planner via `TableProvider::schema()` whenever the table contains Variant columns.
///
/// Background: `INSERT INTO t (v) VALUES ('{"a":1}')` fails inside
/// `LogicalPlanBuilder::values` because `arrow_cast::can_cast_types(Utf8, Struct{Binary,Binary})`
/// is false. The check is hardcoded in datafusion-expr; there is no extension hook to
/// register a Utf8→Variant coercion (datafusion exposes `ExprPlanner` for binary ops,
/// field access, etc., but not for the values-type check). Patching arrow-cast or
/// datafusion-expr is the only "fundamental" fix and is out of scope.
///
/// So we keep two views of the schema:
/// - SQL-facing view (this function): Utf8View for variant cols → planner accepts JSON literals.
/// - Storage view (`real_schema()`): the actual Struct{Binary, Binary} variant type.
///
/// `DataSink::write_all` converts inbound Utf8/Utf8View → Variant struct (via
/// `parquet_variant_compute::VariantArrayBuilder`) before the Delta write.
pub fn create_insert_compatible_schema(schema: &SchemaRef) -> SchemaRef {
    // `tf.pg_type = jsonb`: pgwire Describe derives RowDescription from the
    // *unanalyzed* plan, where Variant cols carry this Utf8View view. Without
    // the tag, bare Variant columns surface text OID 25 and strict drivers
    // (hasql) reject the row (expected jsonb 3802). vendor/arrow-pg maps the
    // tag to OID 3802 + the 0x01 binary jsonb version byte.
    let fields: Vec<FieldRef> = schema
        .fields()
        .iter()
        .map(|f| {
            if is_variant_type(f.data_type()) {
                Arc::new(
                    Field::new(f.name(), ArrowDataType::Utf8View, f.is_nullable())
                        .with_metadata(HashMap::from([("tf.pg_type".to_string(), "jsonb".to_string())])),
                )
            } else {
                f.clone()
            }
        })
        .collect();
    Arc::new(Schema::new(fields))
}
```

Two consequences ripple through the rest of the book. First, because the
SQL-facing schema erases the Variant type, an analyzer rule
(`VariantTableScanSchemaPatch`) has to *restore* it on `TableScan` outputs so
intermediate UDFs see binary Variant, and a second rule
(`VariantPgwireRootWrap`) has to wrap the outermost projection in
`variant_to_json()` for the wire — but only on pgwire-facing sessions. Chapter 14
walks both. Second, the `tf.pg_type = jsonb` metadata tag is why a `hasql` client
does not reject the row: it maps to OID 3802 with the binary jsonb version byte,
instead of plain text OID 25.

Variant detection is structural, not nominal:

**Listing 6.14 — src/schema.rs:764–783**

```rust
/// Inner field names of the unshredded Variant struct
/// (`delta_kernel::unshredded_variant()`). Centralized here so any writer or
/// validator that constructs a Variant struct uses the same names; if
/// delta-kernel ever renames these, only this file changes.
pub const VARIANT_METADATA_FIELD: &str = "metadata";
pub const VARIANT_VALUE_FIELD: &str = "value";

/// Arrow ExtensionType marker every Variant field must carry, or
/// `Field::try_extension_type::<VariantType>()` (delta-rs, parquet-variant-compute)
/// panics with "Extension type name missing".
pub const VARIANT_EXT_KEY: &str = "ARROW:extension:name";
pub const VARIANT_EXT_VALUE: &str = "arrow.parquet.variant";

/// Returns true if the given Arrow DataType structurally matches a Variant
/// (Struct with `metadata` + `value` binary/binaryview fields).
pub fn is_variant_type(data_type: &ArrowDataType) -> bool {
    let ArrowDataType::Struct(fields) = data_type else { return false };
    let binary_named = |name: &str| fields.iter().any(|f| f.name() == name && matches!(f.data_type(), ArrowDataType::Binary | ArrowDataType::BinaryView));
    fields.len() == 2 && binary_named(VARIANT_METADATA_FIELD) && binary_named(VARIANT_VALUE_FIELD)
}
```

### The production table: `otel_logs_and_spans`

457 lines of YAML, ~95 columns. The header declares the tenancy, the dedup
identity, and the physical layout:

**Listing 6.15 — schemas/otel_logs_and_spans.yaml:1–20 (header)**

```yaml
table_name: otel_logs_and_spans
partitions:
  - project_id
  - date
# Last-write-wins dedup at flush time. Same retry from a client (same span
# id at the same timestamp) collapses to one row before Delta commit. Only
# covers dupes inside one 10-min bucket; cross-bucket dupes need the
# read-side row_number() rewrite.
dedup_keys:
  - timestamp
  - id
# Tie-breaker when rows share (id, timestamp): keep the greatest `updated_at`.
#
# MUST be a TF-OWNED column. `insert_coerce::stamp_version` OVERWRITES whatever
# column this names on every write to a `version_append` table, so pointing it
# at a client-supplied field (this was `observed_timestamp`) silently destroys
# that field's value on every row. `updated_at` exists for exactly this.
dedup_tiebreak: updated_at
```

The sorting-column block is where query performance is decided, and it explains
the DESC:

**Listing 6.16 — schemas/otel_logs_and_spans.yaml (sorting_columns and its rationale)**

```yaml
# Hot paths: point lookup by (timestamp, id), and service_name queries within
# a time range. Leading with `timestamp` keeps row-group min/max stats tight
# for any timestamp-bound query; sorting by service_name next clusters rows
# of the same service together within each row group, so service_name filters
# inside a time range prune at the page level.
#
# `timestamp` is sorted DESCENDING (newest-first): the dominant dashboard
# pattern is `ORDER BY timestamp DESC LIMIT n`. With DESC-sorted files the
# footer advertises `[timestamp DESC, …]`, so the parquet sort-order pushdown
# (delta-rs fork) + `ordered_union_for_topk` turn that query into a streaming
# TopK that reads the newest files/rows and stops, instead of a full blocking
# sort over the window. `nulls_first: true` matches DataFusion's DESC default
# so the advertised ordering satisfies the query even if a reader marks the
# (never-null) column nullable. Secondary keys stay ASC as tiebreakers.
sorting_columns:
  - name: timestamp
    descending: true
    nulls_first: true
  - name: resource___service___name
    descending: false
    nulls_first: false
  - name: id
    descending: false
    nulls_first: false
  - name: level
    descending: false
    nulls_first: false
  - name: status_code
    descending: false
    nulls_first: false
```

And Z-order is *deliberately empty*, which is unusual enough to deserve its
paragraph of reasoning:

**Listing 6.17 — schemas/otel_logs_and_spans.yaml (z_order_columns)**

```yaml
# No Z-ORDER. This is a time-dominant append log where ~all queries filter by
# `timestamp`; Z-order's space-filling curve interleaves timestamp bits across
# files/row-groups, loosening exactly the locality those queries prune on (it
# fights the timestamp-leading `sorting_columns` above). Point lookups on
# `id`/`trace_id`/`span_id` are served by bloom filters + tantivy, and
# `service_name` by the secondary sort column — none need Z-order. The hot
# compaction paths already bin-pack with plain Compact; the cold recompress
# path force-rewrites via a streaming `replace_where` overwrite (see
# `recompress_partition`), so nothing depends on this list anymore.
z_order_columns: []
```

The column naming convention is worth noting because it looks strange the first
time: OTel's nested attribute paths are flattened with triple underscores.
`attributes___http___response___status_code` is OTel's
`attributes.http.response.status_code`. Nesting is *also* available through the
Variant columns (`body`, `attributes`), so the same data is reachable two ways: as
a typed flat column with Parquet statistics and bloom filters, or as a Variant
path with `->`/`->>`. Flattening the hot ones is what makes them prunable.

Per-column indexing declarations look like this:

**Listing 6.18 — schemas/otel_logs_and_spans.yaml (representative fields)**

```yaml
  - name: id
    data_type: Utf8
    nullable: false
    bloom_filter: true
    tantivy: { indexed: true, tokenizer: raw }
  - name: name
    data_type: Utf8
    nullable: true
    bloom_filter: true
    tantivy: { indexed: true, tokenizer: ngram3 }
  - name: body
    data_type: Variant
    nullable: true
    tantivy: { indexed: true, tokenizer: ngram3, flatten: json }
```

`tokenizer: raw` means exact-match keyword indexing (ids); `ngram3` means
substring-searchable; `flatten: json` tells the indexer how to turn a Variant into
text. `TantivyFieldConfig` is the type:

**Listing 6.19 — src/schema.rs:508–525**

```rust
/// Per-column tantivy index configuration. Drives `tantivy_index::schema`.
///
/// `tokenizer`: "raw" (exact match keyword) or "default" (tokenized text).
/// `flatten`: for Variant columns — "json" (value-only text) or "kv" (key:value tokens).
///
/// User fields are always indexed-only — the real data lives in Delta/parquet.
/// Only the reserved `_timestamp` and `_id` reserved fields are stored, and only
/// because the reader needs them to produce `(timestamp, id)` prefilter hits for
/// the Delta-side join.
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct TantivyFieldConfig {
    #[serde(default)]
    pub indexed: bool,
    #[serde(default)]
    pub tokenizer: Option<String>,
    #[serde(default)]
    pub flatten: Option<String>,
}
```

### Rollup declarations: `RollupSpec` and `RollupMeasure`

A rollup is declared *on the source table*, and its table is synthesized. This is
the design decision that makes rollups a config change rather than a new schema
file plus a hardcoded constant.

**Listing 6.20 — src/schema.rs:14–59**

```rust
/// One continuous-aggregate rollup, declared on the SOURCE table.
///
/// TimescaleDB's continuous aggregates in TF's shape: you declare the grain,
/// the dimensions and the measures, and the rollup table is SYNTHESIZED from
/// this plus the source schema (`RollupSpec::synthesize`). Nothing is
/// hand-written, so a rollup column's type cannot drift from the source column
/// it aggregates, and adding a rollup is a config change rather than a new
/// YAML file plus a hardcoded constant.
///
/// The refresh policy is TF's own and is better than a timer: the build fires
/// at the point a partition is CERTIFIED duplicate-free, because a bucket
/// computed over a bin that is later deduped is simply wrong.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RollupSpec {
    /// Bucket width, e.g. `1m`, `1h`, `1d`. The default table-name suffix, so
    /// the name cannot disagree with the resolution.
    pub grain: String,
    /// Distinguishing suffix, for a SECOND rollup at the same grain.
    // ... elided: 8 lines explaining when a second rollup is warranted
    #[serde(default)]
    pub name: Option<String>,
    /// Columns a query may GROUP BY **or FILTER on** and still be answerable.
    /// Filters constrain the design exactly as hard as group-bys — rows for a
    /// non-dimension are already summed together and cannot be subtracted back
    /// out — which is the part a rollup design usually gets wrong.
    pub dimensions: Vec<String>,
    pub measures: Vec<RollupMeasure>,
    /// Build this rollup from ANOTHER rollup on the same source rather than from
    /// raw rows, naming that spec's `name`.
    // ... elided: 6 lines on why re-aggregating a fine tier is exact and cheap
    #[serde(default)]
    pub derive_from: Option<String>,
}
```

The measure type constrains what is expressible, and the constraint is principled:

**Listing 6.21 — src/schema.rs:61–78**

```rust
/// One stored measure. Only DECOMPOSABLE aggregates are expressible: count/sum/
/// min/max re-aggregate across buckets and across collapsed dimensions. `avg`
/// is admissible as sum/count and is expanded before it gets here; exact
/// percentiles and exact count(distinct) are not, and are refused rather than
/// answered approximately without being asked.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RollupMeasure {
    /// Column name in the rollup table.
    pub name: String,
    /// `count` | `sum` | `min` | `max`.
    pub agg: String,
    /// Source column to aggregate. Omitted for `count`.
    #[serde(default)]
    pub column: Option<String>,
    /// Optional `FILTER (WHERE …)` predicate, for things like an error count.
    #[serde(default)]
    pub filter: Option<String>,
}
```

The doc comment says `count | sum | min | max`, but `validate` accepts two more —
`tdigest` and `hll`, the sketch aggregates that make approximate percentiles and
approximate distinct counts *decomposable* and therefore rollup-able:

**Listing 6.22 — src/schema.rs:164–199 (measure validation, excerpt)**

```rust
        for measure in &self.measures {
            anyhow::ensure!(is_ident(&measure.name), "rollup {}: measure `{}` must be an SQL identifier", self.table_name(&source.table_name), measure.name);
            anyhow::ensure!(names.insert(&measure.name), "rollup {}: duplicate or colliding measure `{}`", self.table_name(&source.table_name), measure.name);
            anyhow::ensure!(
                matches!(measure.agg.as_str(), "count" | "sum" | "min" | "max" | "tdigest" | "hll"),
                "rollup {}: unsupported aggregate `{}`",
                self.table_name(&source.table_name),
                measure.agg
            );
            match (&measure.agg[..], measure.column.as_deref()) {
                ("count", None) => {}
                ("count", Some(column)) | ("sum" | "min" | "max" | "hll", Some(column)) => {
                    anyhow::ensure!(field(column).is_some(), "rollup {}: unknown column `{column}`", self.table_name(&source.table_name));
                }
                ("tdigest", Some(column)) => {
                    let Some(data_type) = field(column).map(|f| f.data_type.as_str()) else {
                        anyhow::bail!("rollup {}: unknown column `{column}`", self.table_name(&source.table_name))
                    };
                    anyhow::ensure!(
                        matches!(data_type, "Int32" | "Int64" | "UInt32" | "UInt64" | "Float64"),
                        "rollup {}: tdigest column `{column}` must be numeric",
                        self.table_name(&source.table_name)
                    );
                }
                // ... elided: two bail! arms for a measure with no source column
            }
```

⚠️ The `RollupMeasure::agg` doc comment (Listing 6.21) is stale: it lists four
aggregates where the validator accepts six. A `tdigest` measure is in production
use on `otel_logs_and_spans` (`server_duration_digest`).

`derive_from` validation is the interesting half, because deriving a coarse tier
from a fine one is only sound under conditions that must be checked:

**Listing 6.23 — src/schema.rs:123–163**

```rust
        if let Some(base) = &self.derive_from {
            let target = self.table_name(&source.table_name);
            let base_spec = source
                .rollups
                .iter()
                .find(|spec| spec.name.as_deref() == Some(base.as_str()))
                .ok_or_else(|| anyhow::anyhow!("rollup {target}: `derive_from: {base}` names no rollup on this table"))?;
            anyhow::ensure!(base_spec.derive_from.is_none(), "rollup {target}: `derive_from` may not chain — `{base}` is itself derived");
            let (Some(fine), Some(coarse)) = (base_spec.grain_micros(), self.grain_micros()) else {
                anyhow::bail!("rollup {target}: cannot compare grains with `{base}`")
            };
            // A base bucket must fall entirely inside one of ours, or its state
            // would have to be split across two output buckets — which no
            // aggregate state can do.
            anyhow::ensure!(
                coarse > fine && coarse % fine == 0,
                "rollup {target}: grain must be a whole multiple of `{base}`'s ({} vs {})",
                self.grain,
                base_spec.grain
            );
            for dimension in &self.dimensions {
                anyhow::ensure!(
                    base_spec.dimensions.contains(dimension),
                    "rollup {target}: dimension `{dimension}` is absent from `{base}`, so it cannot be derived from it"
                );
            }
            for measure in &self.measures {
                let base_measure = base_spec
                    .measures
                    .iter()
                    .find(|candidate| candidate.name == measure.name)
                    .ok_or_else(|| anyhow::anyhow!("rollup {target}: measure `{}` is absent from `{base}`", measure.name))?;
                // Re-aggregating states only works if both sides mean the same
                // thing; a `sum` folded out of a `min` is silently wrong.
                anyhow::ensure!(
                    base_measure.agg == measure.agg && base_measure.column == measure.column && base_measure.filter == measure.filter,
                    "rollup {target}: measure `{}` must match `{base}`'s definition exactly to be derived from it",
                    measure.name
                );
            }
        }
```

Four conditions: no chaining, the coarse grain is a whole multiple of the fine
one, every coarse dimension exists in the fine tier, and every measure matches
*exactly* in aggregate, column, and filter. Each rules out a specific way to get a
silently wrong answer.

### Synthesis: how a rollup table gets its schema

**Listing 6.24 — src/schema.rs:203–302 (`RollupSpec::synthesize`, with the long
comment on `dedup_keys` elided)**

```rust
    /// The rollup's `TableSchema`, derived from the source so column types
    /// cannot drift. Identity columns mirror what every table here carries;
    /// dimensions keep the source's own type and nullability; measures are
    /// Int64 (count) or the source column's type (sum/min/max).
    pub fn synthesize(&self, source: &TableSchema) -> anyhow::Result<TableSchema> {
        let src_field = |n: &str| {
            source
                .fields
                .iter()
                .find(|f| f.name == n)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("rollup {}: unknown column `{n}`", self.table_name(&source.table_name)))
        };
        let plain = |name: &str, data_type: &str, nullable: bool| FieldDef {
            name: name.to_string(),
            data_type: data_type.to_string(),
            nullable,
            tantivy: None,
            dictionary: None,
            bloom_filter: false,
            // A tier is rebuilt wholesale, never UPDATEd.
            mutable: false,
        };
        let mut fields = vec![
            plain("project_id", "Utf8", true),
            plain("timestamp", "Timestamp(Microsecond, Some(\"UTC\"))", false),
            plain("date", "Date32", false),
            plain("id", "Utf8", false),
            plain("updated_at", "Timestamp(Microsecond, Some(\"UTC\"))", false),
            plain("deleted", "Boolean", true),
            plain("rollup_generation", "Utf8", false),
        ];
        for d in &self.dimensions {
            let f = src_field(d)?;
            // Always nullable in the rollup: GROUP BY emits a NULL group for
            // rows missing the dimension, even when the source column is not.
            fields.push(FieldDef { nullable: true, ..f });
        }
        for m in &self.measures {
            let ty = match (m.agg.as_str(), &m.column) {
                ("count", _) => "Int64".to_string(),
                // `src_field` for its error, not its type: a sketch column is
                // always Binary, but naming a column that does not exist must
                // still fail HERE rather than synthesize a phantom field that
                // only breaks when the build SQL runs.
                ("tdigest" | "hll", Some(c)) => src_field(c).map(|_| "Binary".to_string())?,
                (_, Some(c)) => src_field(c)?.data_type,
                (a, None) => anyhow::bail!("rollup {}: `{a}` measure `{}` needs a source column", self.table_name(&source.table_name), m.name),
            };
            // A measure over an empty group is NULL, and count is never NULL.
            fields.push(plain(&m.name, &ty, m.agg != "count"));
        }
        Ok(TableSchema {
            table_name: self.table_name(&source.table_name),
            // Same partitioning as the source, so ProjectRoutingTable gives
            // multi-tenant isolation and date pruning for free.
            partitions: source.partitions.clone(),
            sorting_columns: vec![SortingColumnDef { name: "timestamp".into(), descending: true, nulls_first: true }],
            z_order_columns: vec![],
            fields,
            time_column: Some("timestamp".into()),
            // ... elided: 30 lines documenting the 2026-08-20 reversal of the
            // "no read-time dedup on a tier" decision — quoted below
            dedup_keys: vec!["timestamp".into(), "id".into()],
            dedup_tiebreak: Some("updated_at".into()),
            tombstone_column: Some("deleted".into()),
            // Not version_append: that is for UPDATE/DELETE appending versions in
            // place, which a tier never does — it is rebuilt wholesale.
            version_append: false,
            // A rollup of a rollup is a real design (1m -> 1h -> 1d) but it is
            // not this change: declaring it here would recurse at load.
            rollups: vec![],
        })
    }
```

Every synthesized rollup table carries the same seven **identity columns**, and
they are worth memorizing because they appear in every rollup query plan:

| Column | Type | Meaning |
|---|---|---|
| `project_id` | Utf8 | tenant; partition column |
| `timestamp` | Timestamp(µs, UTC) | the *bucket start*, not an event time |
| `date` | Date32 | partition column, derived from `timestamp` |
| `id` | Utf8 | synthetic bucket identity (bucket + dimension tuple) |
| `updated_at` | Timestamp(µs, UTC) | dedup tiebreak — which build wrote this row |
| `deleted` | Boolean (nullable) | tombstone |
| `rollup_generation` | Utf8 | which generation of the spec produced the row |

Then the declared dimensions (forced nullable, because `GROUP BY` emits a NULL
group), then the measures (`Int64` for `count`, `Binary` for sketches, otherwise
the source column's type; nullable unless it is a `count`).

The elided comment in `synthesize` is a 30-line reversal of an earlier decision,
and it is one of the most useful passages in the codebase for understanding how
this project reasons about its own past choices:

**Listing 6.25 — src/schema.rs:264–292 (the `dedup_keys` comment on a synthesized rollup)**

```rust
            // A tier declares its identity, so reads collapse superseded
            // versions. This REVERSES an earlier "no read-time dedup" decision;
            // both reasons it rested on were re-tested on 2026-08-20 and neither
            // survives.
            //
            // It claimed duplicates were impossible by construction, because
            // rollup waves removed every file in a (project, date) partition as
            // they wrote. That architecture is gone. The coordinator's
            // replace-set removes only files whose slice is CONTAINED in the one
            // being published, and (until `slice_retires`) untagged files never —
            // so one measured prod partition held 45,483 rows for 7,923 buckets,
            // 4 to 8 versions each, written by 320 separate passes.
            //
            // It also claimed declaring keys broke planning: "DedupExec key `id`
            // not in input schema", every routed query falling back to a failing
            // raw scan. That no longer reproduces anywhere in 1,031 tests, the
            // routed rollup tests included.
            //
            // Both tiers declare the SAME keys, deliberately. The asymmetry this
            // replaces — a derived tier protected by its maintenance-read
            // collapse while the base tier had no read-time defence at all — is
            // why one tier read 1.00 versions per id while the other read 5.64,
            // with nothing at the schema level to say why.
            //
            // Clean bytes remain the goal, not this: a `DedupExec` over a
            // partition holding one version per key is near-free, over one
            // holding eight it is not. This is the safety net under the repair,
            // not a substitute for it.
            dedup_keys: vec!["timestamp".into(), "id".into()],
```

### Registry construction, collisions, and legacy aliases

`SchemaRegistry::new` does four things in order: parse and validate every YAML,
check for rollups that would generate the same table name, synthesize and register
every rollup table, and register read-only aliases for retired rollup generations.

**Listing 6.26 — src/schema.rs:648–720**

```rust
impl SchemaRegistry {
    fn new() -> Self {
        let mut schemas: HashMap<String, TableSchema> = SCHEMAS_DIR
            .files()
            .filter(|f| f.path().extension().and_then(|s| s.to_str()) == Some("yaml"))
            .map(|file| {
                let content = file.contents_utf8().expect("Schema file should be UTF-8");
                let schema: TableSchema = serde_yaml::from_str(content).unwrap_or_else(|e| panic!("Failed to parse schema {:?}: {}", file.path(), e));
                schema.validate().unwrap_or_else(|e| panic!("Invalid schema {:?}: {}", file.path(), e));
                schema.rollups.iter().try_for_each(|rollup| rollup.validate(&schema)).unwrap_or_else(|e| panic!("Invalid rollup on {:?}: {}", file.path(), e));
                (schema.table_name.clone(), schema)
            })
            .collect();
        // ... elided: the duplicate-rollup-name check, quoted below
        let synthesized: Vec<TableSchema> = schemas
            .values()
            .flat_map(|src| src.rollups.iter().map(move |spec| (src, spec)))
            .map(|(src, spec)| {
                let rollup = spec.synthesize(src).unwrap_or_else(|e| panic!("Invalid rollup on {}: {e}", src.table_name));
                rollup.validate().unwrap_or_else(|e| panic!("Invalid synthesized rollup {}: {e}", rollup.table_name));
                rollup
            })
            .collect();
        for r in synthesized {
            let name = r.table_name.clone();
            if schemas.insert(name.clone(), r).is_some() {
                // A hand-written file under a generated name would silently win
                // or lose depending on iteration order. (Same-source rollup
                // collisions are already reported above, with a better message.)
                panic!("rollup table `{name}` collides with a hand-written schema file of the same name");
            }
        }
        // Migration aliases remain queryable while v3/v2 slice generations
        // shadow-build and canary. They are read-only schema aliases: source
        // rollup declarations point only at the new targets, so maintenance
        // cannot accidentally keep writing the retired generations.
        for (current, legacy) in [
            ("otel_logs_and_spans_rollup_dashboard_1m_v3", "otel_logs_and_spans_rollup_dashboard_1m_v2"),
            ("otel_logs_and_spans_rollup_dashboard_1h_v2", "otel_logs_and_spans_rollup_dashboard_1h_v1"),
            ("otel_metrics_rollup_metrics_1m_v2", "otel_metrics_rollup_metrics_1m_v1"),
            ("otel_metrics_rollup_metrics_1h_v2", "otel_metrics_rollup_metrics_1h_v1"),
        ] {
            if let Some(mut schema) = schemas.get(current).cloned() {
                schema.table_name = legacy.to_owned();
                schemas.entry(legacy.to_owned()).or_insert(schema);
            }
        }
        Self { schemas }
    }
```

Note that every failure here is a `panic!` at first registry access — which, given
the `OnceLock`, means process start. That is the right call: a schema that does
not validate cannot produce correct answers, and there is no partial-degradation
mode worth having.

The collision check earns its verbosity by diagnosing *which* mistake you made:

**Listing 6.27 — src/schema.rs:663–685**

```rust
        // Two rollups generating one name is a config error, and WHICH error it
        // is depends on whether they group the same way — so say so, rather
        // than making the operator infer it from a name collision.
        for src in schemas.values() {
            for (i, a) in src.rollups.iter().enumerate() {
                if let Some(b) = src.rollups[i + 1..].iter().find(|b| b.table_name(&src.table_name) == a.table_name(&src.table_name)) {
                    let name = a.table_name(&src.table_name);
                    assert!(
                        a.dimensions != b.dimensions,
                        "{}: two rollups both generate `{name}` with the SAME dimensions. Same grain + same dimensions is the same GROUP BY, so \
                         add the extra measures to the existing rollup instead of declaring a second one — a second table would duplicate every \
                         identity and dimension column and make a query wanting both measures read two tables.",
                        src.table_name
                    );
                    panic!(
                        "{}: two rollups both generate `{name}` but group differently ({:?} vs {:?}). Different dimensions ARE different tables; \
                         give one of them a `name:` to distinguish it.",
                        src.table_name, a.dimensions, b.dimensions
                    );
                }
            }
        }
```

### The production rollups

`otel_logs_and_spans` declares two, and the pair demonstrates the derived-tier
design:

**Listing 6.28 — schemas/otel_logs_and_spans.yaml (rollups, measures elided)**

```yaml
rollups:
  - grain: 1m
    name: dashboard_1m_v3
    dimensions: [resource___service___name, kind, status_code]
    measures:
      - { name: request_count, agg: count }
      - { name: duration_count, agg: count, column: duration }
      - { name: error_count, agg: count, filter: "status_code = 'ERROR' OR COALESCE(attributes___http___response___status_code, 0) >= 500" }
      - { name: duration_sum, agg: sum, column: duration }
      - { name: duration_min, agg: min, column: duration }
      - { name: duration_max, agg: max, column: duration }
      # ... elided: seven `server_*` measures with the same
      # "kind = 'server' OR name = 'apitoolkit-http-span' OR name = 'monoscope.http'"
      # filter, plus:
      - { name: server_duration_digest, agg: tdigest, column: duration, filter: "kind = 'server' OR ..." }
  # Coarse tier for wide windows. The matcher picks the coarsest grain that
  # divides the requested bucket width, so a 30-day chart (1d/6h buckets)
  # reads this and a 5-minute chart still reads the 1m tier. Derived from
  # that tier, so it re-aggregates its rows instead of scanning the raw
  # partition a second time — on this table that is the difference between
  # a few hundred thousand rows and hundreds of millions.
  - grain: 1h
    name: dashboard_1h_v2
    derive_from: dashboard_1m_v3
    dimensions: [resource___service___name, kind, status_code]
    measures: # identical set, byte-for-byte — required by `derive_from` validation
```

The dimension-cardinality constraint is stated in the YAML and is the thing most
likely to be got wrong when adding a rollup:

**Listing 6.29 — schemas/otel_logs_and_spans.yaml (the rollup design rule)**

```yaml
# Every dimension must be low-cardinality: rows per bucket is roughly the
# product of their distinct counts, and at 1m grain that product has to stay in
# the low thousands or the rollup is a slightly smaller copy of this table.
# A filter on any column NOT listed disqualifies a query from routing, because a
# filter cannot be applied after aggregation.
```

### The lifecycle of the payments span

Putting the model together, here is the recurring example as an actual row.
Written by the client as:

```sql
INSERT INTO otel_logs_and_spans
  (project_id, date, timestamp, id, name, kind, status_code,
   resource___service___name, duration, hashes, body)
VALUES
  ('prod-api-001', '2025-01-17', '2025-01-17 14:25:00Z',
   '550e8400-e29b-41d4-a716-446655440000',
   'POST /api/v1/payments', 'server', 'ERROR',
   'payments-api', 3421000000, ARRAY[]::text[],
   '{"error":{"type":"UpstreamTimeout"}}');
```

What the data model does to it:

1. `body` arrives as a `Utf8View` literal — legal only because
   `create_insert_compatible_schema` presented the column as `Utf8View`. The
   `DataSink` converts it to `Struct{metadata,value}` before the Delta write.
2. `updated_at` is *overwritten* by `insert_coerce::stamp_version` with a fresh
   monotonic value, because `otel_logs_and_spans` has `version_append: true` and
   names `updated_at` as its `dedup_tiebreak`.
3. `deleted` is left NULL, which reads as live.
4. The dedup identity of this row is `(timestamp, id)` — so a client retry of the
   same span collapses at flush time.
5. It lands under `project_id=prod-api-001/date=2025-01-17/` in a Parquet file
   whose footer advertises `[timestamp DESC, resource___service___name ASC,
   id ASC, level ASC, status_code ASC]`.
6. `id` and `name` get bloom filters; `id`, `name`, `kind`, `status_code`,
   `level`, `status_message`, and `body` get Tantivy postings.
7. Its `duration` contributes to `request_count`, `duration_count`,
   `duration_sum/min/max`, `error_count` (it is `status_code = 'ERROR'`), and
   every `server_*` measure including `server_duration_digest` — because
   `kind = 'server'` — in the 1-minute bucket `2025-01-17 14:25:00`, keyed by
   `(payments-api, server, ERROR)`.

### Life outside the YAML: schema migration

One more thing the model does *not* do: derive the stored Delta schema from the
YAML. They are two separate schemas, and a mismatch is fatal.

**Listing 6.30 — src/schema.rs:380–386**

```rust
// Existing tables need an explicit column migration: their Delta schema is not
// derived from YAML, and new-table tests cannot detect an upgrade mismatch.
//
// So: a column addition to a shipped table needs a migration that evolves the
// stored Delta schema of every live table (all projects, unified + custom)
// BEFORE the binary that writes the wider batch is deployed — verified against a
// pre-existing table, not a fresh one.
```

The migration tool is `timefusion migrate-columns` (Chapter 23), which writes a
zero-row batch at the widened schema with `SchemaMode::Merge` — metadata-only and
idempotent. The regression test that pins the ordering hazard is worth reading in
full, because it documents an actual production break:

**Listing 6.31 — src/schema.rs:936–974**

```rust
    /// The SHIPPED merge-on-read tables, post-migration (2026-08-02). This guard
    /// used to assert these tables had NO merge-on-read columns, because adding
    /// one to a table that already holds live Delta data broke prod (7d68f01):
    /// the stored transaction log kept the old column set while the write path
    /// built batches to the YAML's, giving `number of columns(94) must match
    /// number of fields(92)`.
    ///
    /// The migration has since been run and verified against prod
    /// (`migrate-columns`, commit edb2fd2), so the invariant is no longer
    /// "absent" but "declared in the SAME SHAPE AND ORDER the stored schema was
    /// widened in" — the two columns last, nullable, and never before any
    /// pre-existing field. Reordering them here re-creates 7d68f01 exactly.
    #[test]
    fn shipped_mor_tables_declare_the_migrated_columns_last() {
        for name in ["otel_logs_and_spans", "otel_metrics"] {
            let schema = get_schema(name).unwrap_or_else(|| panic!("{name} registered"));
            // ... elided: assertions that version_append is on and the tombstone
            // column is a nullable Boolean
            // ORDER is the load-bearing part: `migrate-columns` APPENDED these,
            // so they must be the final two fields, updated_at then deleted.
            let tail: Vec<&str> = schema.fields.iter().rev().take(2).map(|f| f.name.as_str()).collect();
            assert_eq!(tail, vec!["deleted", "updated_at"], "{name}: the migrated columns must be the LAST two fields, in migration order (7d68f01)");
        }
        // The tiebreak MUST be the TF-owned column, not the client's:
        // `insert_coerce::stamp_version` OVERWRITES whatever this names, so
        // pointing it back at `observed_timestamp` / `ingested_at` would destroy
        // client data on every write.
        for name in ["otel_logs_and_spans", "otel_metrics"] {
            assert_eq!(get_schema(name).unwrap().dedup_tiebreak.as_deref(), Some("updated_at"), "{name} must break ties on the TF-owned stamp");
        }
    }
```

⚠️ Note the tension inside that test: the assertion is `assert!(schema.version_append,
"{name} ships merge-on-read")`, while the comment two lines above says
"`version_append` is OFF since 2026-08-02". The YAML resolves it — `version_append:
true` with a comment saying it was **re-enabled** on 2026-08-02 after `read_dedup`
was fixed. The stale half is the test's comment, not the assertion.

**Key takeaways.** A TimeFusion table is a YAML declaration compiled into the
binary, parsed once, validated at process start, and projected into two type
systems (Arrow and Delta) plus a third SQL-facing view for Variant columns.
`dedup_keys` / `dedup_tiebreak` / `tombstone_column` / `version_append` define
merge-on-read; `mutable` (default false) decides whether a filter may be pushed
below the dedup and is the difference between a point lookup and an OOM; rollups
are declared on the source table and their tables are synthesized so types cannot
drift. The stored Delta schema is *not* derived from the YAML — widening a shipped
table is a migration, not an edit.
## 7. The core abstractions

*After this chapter you will know the six types that everything else in
TimeFusion is built from — `TableKey`, `AppConfig` / `DerivedBudget`, `Database`,
`ProjectRoutingTable`, `ScanMetrics`, and the caching types around Delta
snapshots — and you will have read each one's definition. You will also
understand the two structural patterns that recur everywhere: late-binding
`OnceLock` fields, and disjoint memory pools.*

### `TableKey`: the unit of everything

**Listing 7.1 — src/database/mod.rs:58–59, 355–357**

```rust
/// Delta tables shared by default projects and partitioned by `project_id`.
pub type UnifiedTables = Arc<RwLock<HashMap<String, Arc<RwLock<DeltaTable>>>>>;

// Custom project tables: projects with their own S3 bucket get isolated tables
// Key: (project_id, table_name) -> DeltaTable
pub type CustomProjectTables = Arc<RwLock<HashMap<(String, String), Arc<RwLock<DeltaTable>>>>>;
```

`(project_id, table_name)` is the composite key that identifies everything
addressable in the system: a Delta table, a WAL topic, a MemBuffer table, a
commit lock, a DML lock, a flush-waiter counter, a rollup coverage entry. In the
MemBuffer it is spelled `TableKey = (Arc<str>, Arc<str>)`; in `Database` it is
`(String, String)`. Two shapes of the same idea.

Unified tables are keyed by table name alone because the tenant lives in the
partition path, not in the table identity. Custom project tables carry the
project in the key because each has its own storage root. Every routing decision
in the codebase reduces to "which of those two maps do I look in", and the answer
comes from `resolve_table` / `try_fast_resolve`.

### `AppConfig`: nine structs and a derived tree

**Listing 7.2 — src/config.rs:625–651**

```rust
#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    #[serde(flatten)]
    pub aws: AwsConfig,
    #[serde(flatten)]
    pub core: CoreConfig,
    #[serde(flatten)]
    pub buffer: BufferConfig,
    #[serde(flatten)]
    pub cache: CacheConfig,
    #[serde(flatten)]
    pub parquet: ParquetConfig,
    #[serde(flatten)]
    pub maintenance: MaintenanceConfig,
    #[serde(flatten)]
    pub memory: MemoryConfig,
    #[serde(flatten)]
    pub telemetry: TelemetryConfig,
    #[serde(flatten)]
    pub tantivy: TantivyConfig,
    /// Self-sizing budget tree, derived (not deserialized) at construction
    /// from the cgroup limit, optionally lowered by
    /// `TIMEFUSION_MEMORY_BUDGET_GB`. NOT `timefusion_memory_fraction` — dead
    /// since the tree landed.
    #[serde(skip)]
    pub derived: DerivedBudget,
}
```

Loading is deliberately per-struct rather than one flattened `envy` call, because
`envy` mishandles `#[serde(flatten)]`:

**Listing 7.3 — src/config.rs:559–606**

```rust
/// Load config from environment variables.
pub fn load_config_from_env() -> Result<AppConfig, envy::Error> {
    // Load each sub-config separately to avoid #[serde(flatten)] issues with envy
    // See: https://github.com/softprops/envy/issues/26
    Ok(AppConfig {
        aws: envy::from_env()?,
        core: envy::from_env()?,
        buffer: envy::from_env()?,
        cache: envy::from_env()?,
        parquet: envy::from_env()?,
        maintenance: envy::from_env()?,
        memory: envy::from_env()?,
        telemetry: envy::from_env()?,
        tantivy: envy::from_env()?,
        derived: DerivedBudget::compute(),
    })
}

/// Initialize global config from environment (for production use).
pub fn init_config() -> Result<&'static AppConfig, envy::Error> {
    if let Some(cfg) = CONFIG.get() {
        return Ok(cfg);
    }
    // `&mut` is autotune's API (cross-module), so the mutation stays here.
    let mut cfg = load_config_from_env()?;
    crate::config::apply(&mut cfg);
    let _ = CONFIG.set(cfg);
    Ok(config())
}

/// Get global config. Panics if not initialized.
pub fn config() -> &'static AppConfig {
    CONFIG.get().expect("Config not initialized. Call init_config() first.")
}
```

There is a global `OnceLock<AppConfig>`, and there is also a per-`Database`
`Arc<AppConfig>`. The tests use the latter exclusively; production uses both.
`CLAUDE.md` states the rule that follows from this: **never** `std::env::set_var`
in a test. Under nextest's process-per-test model a process-global mutation
silently stops meaning what you think while still looking correct. Build the
`AppConfig` the test needs and pass it to `Database::with_config`.

### `DerivedBudget`: partitioning one cgroup limit

This is the type that makes single-process operation survivable, and it is worth
studying as a design in its own right. The premise: there is exactly one memory
limit (the cgroup's), and several independent consumers that will each happily
use all of it. Rather than expose a knob per consumer — which drifts — the whole
tree derives from one detected number.

**Listing 7.4 — src/config.rs:120–136**

```rust
/// Self-sizing memory/concurrency budget derived once at startup from the
/// container's cgroup limits. See docs/compaction-redesign-2026-07-29.md §4.
///
/// Fixed fractions are opinions pinned in code (no override — that's the
/// point of deleting the env vars): a workload needing a different split
/// changes the constants here, not a knob in production.
#[derive(Debug, Clone, Copy)]
pub struct DerivedBudget {
    pub memory_limit_bytes: usize,
    pub cores: usize,
    query_pool_bytes: usize,
    ingest_buffer_bytes: usize,
    foyer_memory_bytes: usize,
    writer_reserve_bytes: usize,
    maintenance_pool_bytes: usize,
    profile: BudgetProfile,
}
```

Note that every field except `memory_limit_bytes` and `cores` is *private*, with
accessor methods. That is not ceremony: several accessors are not simple getters
(`heavy_share_bytes`, `light_share_bytes`, `coordinator_share_bytes` all depend on
the profile), and letting a caller read the raw field would silently give the
wrong answer under `MaintenanceCli`.

The derivation itself is the interesting part:

**Listing 7.5 — src/config.rs:261–283 (the server profile)**

```rust
        // Fixed fraction, not the old TIMEFUSION_MEMORY_FRACTION knob: that
        // 0.75 was calibrated against a hand-set limit and, applied to the
        // real cgroup, would crush maintenance to K=1 — the drift-class bug
        // this tree exists to kill. 0.20 of the real limit is ~1.25x the old
        // effective pool.
        let query_pool_bytes = (memory_limit_bytes as f64 * QUERY_POOL_FRACTION) as usize;
        let ingest_buffer_bytes = (memory_limit_bytes as f64 * INGEST_BUFFER_FRACTION) as usize;
        let foyer_memory_bytes = (memory_limit_bytes as f64 * FOYER_MEMORY_FRACTION) as usize;
        // Capped at 10% of the limit: the full 6 GiB reserve on an 8 GiB dev
        // box budgeted 142% of the container — the drift class this tree kills.
        let writer_reserve_bytes = (HEAVY_REWRITE_PERMITS * OPTIMIZE_MERGE_TASKS * WRITER_RESERVE_PER_TASK_BYTES).min(memory_limit_bytes / 10);
        // UNTRACKED-CONSUMER SLACK, carved out BEFORE maintenance takes the
        // remainder. Without it the tree hands maintenance everything left,
        // and consumers no pool tracks (parquet decode, giant-INSERT parse
        // ASTs, allocator overhead) push the box over the cgroup limit —
        // every subsystem behaved "legally", the sum was the bug. 15% covers
        // the measured untracked peak; the resulting maintenance shrink was
        // proven harmless (512MB bins sort+spill fine on smaller pools).
        let untracked_slack_bytes = (memory_limit_bytes as f64 * UNTRACKED_SLACK_FRACTION) as usize;
        let reserved = query_pool_bytes + ingest_buffer_bytes + foyer_memory_bytes + writer_reserve_bytes + untracked_slack_bytes;
        let maintenance_pool_bytes = memory_limit_bytes.saturating_sub(reserved).max(MAINTENANCE_FLOOR_BYTES);
        Self { memory_limit_bytes, cores, query_pool_bytes, ingest_buffer_bytes, foyer_memory_bytes, writer_reserve_bytes, maintenance_pool_bytes, profile }
```

Read that carefully, because the sentence "every subsystem behaved legally, the
sum was the bug" is the single most transferable idea in this codebase. The
constants:

**Listing 7.6 — src/config.rs:152–208 (the fractions and permits)**

```rust
// 0.20, down from 0.25: sampled pool usage sat at 0 while the 70% memory
// brake chronically halted light-compaction waves. The freed headroom favors
// maintenance — a query past its pool spills (one slower scan), while a
// stopped hot-tail compaction backlogs the whole table.
const QUERY_POOL_FRACTION: f64 = 0.20;
/// Share reserved for consumers no pool tracks — parquet decode heap, pgwire
/// parse ASTs, allocator overhead (measured ~10-20 GiB on prod). Carved out
/// before maintenance takes the remainder so the tree never sanctions more
/// than the cgroup holds.
const UNTRACKED_SLACK_FRACTION: f64 = 0.15;
/// Ingest MemBuffer share of the limit. Reproduces today's working ratio
/// (24 GiB of a 120 GiB box).
const INGEST_BUFFER_FRACTION: f64 = 0.20;
/// Foyer read-cache share, deliberately larger than the previous ~3.3%: cache
/// hit-rate is what query latency lives on, and it was the most-starved
/// consumer relative to impact.
const FOYER_MEMORY_FRACTION: f64 = 0.10;
/// Per-(rewrite-permit × merge-task) delta-rs writer buffer. Previously
/// budgeted nowhere — the 06-11 OOM was exactly this gap.
const WRITER_RESERVE_PER_TASK_BYTES: usize = 3 * GIB / 2;
/// delta-rs concurrent merge tasks per optimize run (unchanged default).
const OPTIMIZE_MERGE_TASKS: usize = 2;
// ... elided: OPTIMIZE_MAX_FILES_PER_BIN (32) and its fan-in rationale
/// Concurrent heavy maintenance rewrites (dedup/optimize/recompress).
/// Formerly `TIMEFUSION_MAINTENANCE_REWRITE_CONCURRENCY`. 10, at the full
/// 4 GiB per-sort budget: at fewer permits, coordinator jobs spent most of
/// their wall clock BLOCKED on this semaphore rather than working, freezing
/// rollup coverage. Raising coordinator jobs alone doesn't help — it just
/// queues deeper here. `PER_SORT_BUDGET_BYTES` is a spill THRESHOLD on a
/// FairSpillPool, not a hard reservation, so extra permits buy
/// parallelism/spill rather than blowing the bound; the 85% memory brake is
/// the backstop. Watch `permit_wait_ms`, RSS against the brake, and
/// `occ_conflicts_total` before raising further.
const HEAVY_REWRITE_PERMITS: usize = 10;
/// Per-sort budget. A spill THRESHOLD on a FairSpillPool, not a hard
/// reservation — a sort that exceeds it degrades to bounded disk spill rather
/// than failing, so more spilling-capable parallel sorts beat fewer
/// comfortable ones. 2 GiB, HALVED from 4 GiB to pay for
/// `HEAVY_REWRITE_PERMITS` going 4 -> 10: leaving it at 4 GiB while permits
/// rose caused an OOM kill (anon-rss 124.9 GB, two minutes after 11.9 GB) —
/// halving keeps the extra concurrency while landing the fan-in envelope near
/// where it last ran clean (10 x 2 = 20 GiB vs the old 4 x 4 = 16 GiB).
const PER_SORT_BUDGET_BYTES: usize = 2 * GIB;
```

For a 120 GiB cgroup the tree comes out roughly: query pool 24 GiB, ingest buffer
24 GiB, Foyer memory 12 GiB, writer reserve 12 GiB (capped at limit/10), untracked
slack 18 GiB, maintenance pool the remaining ~30 GiB — which then splits again
into a coordinator share (jobs × 512 MiB, capped at a quarter), a heavy share
(40% of the rest), and a light share (the remainder).

Detection is layered and never panics:

**Listing 7.7 — src/config.rs:38–73**

```rust
/// Detect the effective memory limit in bytes: cgroup v2 → cgroup v1 →
/// `/proc/meminfo` total → a conservative 8 GiB floor. Never panics.
fn detect_memory_limit_bytes() -> usize {
    read_parsed("/sys/fs/cgroup/memory.max", parse_cgroup_v2_memory_max)
        .or_else(|| read_parsed("/sys/fs/cgroup/memory/memory.limit_in_bytes", parse_cgroup_v1_memory_limit))
        // No cgroup limit → unmanaged box: an explicit env override is safe HERE
        // only (off-box CLI / dev boxes) — prod always runs under a cgroup, so
        // the misconfigured-knob OOM loop can't recur through this path.
        .or_else(|| {
            env_memory_override_bytes().inspect(|v| tracing::warn!("budget tree: no cgroup limit; using TIMEFUSION_MEMORY_LIMIT_GB override ({} GiB)", v / GIB))
        })
        // Shared host: budget HALF the machine, loudly — sizing from full host
        // RAM inside a container caused a memcg OOM-loop, so stay conservative.
        .or_else(|| {
            read_parsed("/proc/meminfo", parse_meminfo_total_bytes)
                .map(|v| v / 2)
                .inspect(|v| tracing::warn!("budget tree: no cgroup memory limit; deriving from HALF of host RAM ({} GiB)", v / GIB))
        })
        // ... elided: the macOS branch, same half-the-machine rule via sysinfo
        .unwrap_or_else(|| {
            tracing::warn!("budget tree: could not detect memory limit from cgroup or /proc/meminfo; falling back to 8 GiB");
            8 * GIB
        })
}
```

The `TIMEFUSION_MEMORY_LIMIT_GB` override is consulted **only** when no cgroup
limit exists, and the comment explains why: a containerized deployment must never
be resizable by env var, because that is precisely the misconfiguration that
produced an OOM loop while the operator believed the limit was 26 GB and the
process was budgeting 120 GiB.

The one operator lever that always applies is `TIMEFUSION_MEMORY_BUDGET_GB`, and
it can only *lower*:

**Listing 7.8 — src/config.rs:217–222**

```rust
/// The number the whole tree derives from: the detected limit, LOWERED by an
/// operator request — budgeting above the cgroup is never valid, so an
/// over-large request is clamped rather than honoured.
fn effective_limit(detected: usize, requested: Option<usize>) -> usize {
    requested.map_or(detected, |b| b.min(detected))
}
```

Because the whole tree is invisible unless printed, it is printed — after
telemetry is initialized, which is itself a fix:

**Listing 7.9 — src/main.rs:355–361**

```rust
    observability::init_telemetry(&cfg.telemetry)?;
    // AFTER init_telemetry: config is built before the subscriber exists, so
    // logging the tree at derivation time is silently swallowed — which is why
    // prod could carry TIMEFUSION_MEMORY_LIMIT_GB=26 while actually budgeting
    // 120 GiB with nothing on the box revealing the gap (2026-07-31).
    config::log_derived_budget(&cfg.derived);
```

### `Database`: the god object, and why it is one

`Database` is a 250-line struct with roughly 70 fields, and it derives `Clone`.
That combination looks alarming until you understand the two properties it
encodes: every field is either an `Arc` of shared state or a per-clone *behavior
flag*, and the clone is how a scoped variant is made.

**Listing 7.10 — src/database/mod.rs:2046–2076 (the memory pools)**

```rust
#[derive(Debug, Clone)]
pub struct Database {
    config: Arc<AppConfig>,
    /// One RuntimeEnv/memory pool shared by every session context and clone —
    /// the pool only enforces a global cap if it's global.
    runtime_env: Arc<std::sync::OnceLock<Arc<datafusion::execution::runtime_env::RuntimeEnv>>>,
    // The next five runtime envs are disjoint pool slices (constant total
    // budget) so one workload's long sorts can never starve another's.
    /// Heavy maintenance (optimize/dedup/recompress): bounded FairSpill pool +
    /// spill dir so a Z-order global sort can always reserve its merge floor
    /// instead of losing the race for the saturated query pool.
    maintenance_runtime_env: Arc<std::sync::OnceLock<Arc<datafusion::execution::runtime_env::RuntimeEnv>>>,
    /// Hot-tail packing: keeps today's compaction reserve while heavy rewrites
    /// hold the maintenance pool for minutes.
    light_optimize_runtime_env: Arc<std::sync::OnceLock<Arc<datafusion::execution::runtime_env::RuntimeEnv>>>,
    /// Footer repair: sorts whole multi-hundred-MB files; sharing packing's
    /// pool meant stopping packing outright for the duration.
    repair_runtime_env: Arc<std::sync::OnceLock<Arc<datafusion::execution::runtime_env::RuntimeEnv>>>,
    /// Coordinator execution units: one process-wide 512 MiB spill pool. Reuse
    /// stops a fresh runtime treating another worker's live spill dir as an
    /// orphan; the pool is the hard aggregate ceiling across slice workers.
    coordinator_runtime_env: Arc<std::sync::OnceLock<Arc<datafusion::execution::runtime_env::RuntimeEnv>>>,
    /// Flush-path sorts for oversized buckets: flush is on the INGEST path and
    /// must not queue behind maintenance. Bounded + spillable because the
    /// in-process sort allocates outside every pool.
    flush_sort_runtime_env: Arc<std::sync::OnceLock<Arc<datafusion::execution::runtime_env::RuntimeEnv>>>,
    /// Caps concurrent spilling flush sorts. FairSpill gives each ~pool/N;
    /// below a viable slice the sort fails, the group is written unsorted, and
    /// a footer with no `sorting_columns` disables ordering for every scan of
    /// that partition — starvation shows up as slow queries, not errors.
    flush_sort_gate: Arc<tokio::sync::Semaphore>,
```

Six `RuntimeEnv`s, each with its own DataFusion memory pool, carved out of the
budget tree so that a maintenance sort cannot starve a query and vice versa. The
comment on `flush_sort_gate` names a failure mode worth internalizing: pool
starvation here does not produce an error, it produces a *file with no
`sorting_columns` footer*, which then makes every future scan of that partition
slower. Resource exhaustion that degrades data layout rather than failing loudly
is the hardest kind to notice.

The behavior flags — the per-clone part:

**Listing 7.11 — src/database/mod.rs:2254–2270 (late-binding cells and scoping flags)**

```rust
    /// Late-binding shared cell: boot creates the pgwire SessionContext before
    /// the layer exists, so the layer is published through a OnceLock visible
    /// to clones captured earlier (e.g. DmlQueryPlanner). A plain Option left
    /// those clones without the mem leg, losing updates to unflushed rows.
    buffered_layer: Arc<std::sync::OnceLock<Arc<crate::write::BufferedWriteLayer>>>,
    /// Per-clone override for `query_delta_only`: hides the shared layer so
    /// scans bypass the in-memory buffer.
    bypass_buffer: bool,
    /// Internal aggregate builds must never read the rollup they are rebuilding.
    bypass_rollup: bool,
    /// Plan the scan with maintenance parallelism instead of query parallelism:
    /// Parquet decode buffers are untracked by the memory pool, so planning a
    /// background rewrite at the full CPU quota fanned out decode into OOMs.
    maintenance_scan: bool,
    /// Same late-binding pattern as `buffered_layer`: attached by `with_*`
    /// builders after boot has already cloned Database into sessions/planners.
    tantivy_search: Arc<std::sync::OnceLock<Arc<crate::tantivy::search::TantivySearchService>>>,
    tantivy_indexer: Arc<std::sync::OnceLock<Arc<crate::tantivy::search::TantivyIndexService>>>,
    /// Same late-binding pattern; populated by `start_dml_coalescer` when
    /// `TIMEFUSION_DML_COALESCE_SECS > 0`.
    dml_coalescer: Arc<std::sync::OnceLock<Arc<crate::dml::DmlCoalescer>>>,
```

**The late-binding `OnceLock` pattern.** Bootstrap has a chicken-and-egg problem:
the `SessionContext` needs a `Database`, and the `BufferedWriteLayer` needs a
`Database` for its write callback, and the `Database` needs the layer for the
MemBuffer read leg. The resolution is to make the layer an
`Arc<OnceLock<Arc<BufferedWriteLayer>>>` — clones made *before* the layer exists
share the same cell and see it as soon as it is set. The comment records what
happened with the naive `Option`: clones captured early (in particular the
`DmlQueryPlanner`) had no mem leg, so `UPDATE`s silently missed unflushed rows.

**The scoping-flag pattern.** `bypass_buffer`, `bypass_rollup`, and
`maintenance_scan` are plain `bool`s, not `Arc`s, precisely so that a clone can
differ. `db.query_delta_only()` returns a clone with `bypass_buffer = true`; a
rollup build uses a clone with `bypass_rollup = true` so it cannot read the
rollup it is rebuilding. This is a cheap, explicit alternative to threading a
context object through every call.

The remaining fields fall into recognizable groups. Resolution and caching:

**Listing 7.12 — src/database/mod.rs:2085–2103**

```rust
    /// Unified tables: one Delta table per schema, partitioned by [project_id, date]
    unified_tables: UnifiedTables,
    /// Custom project tables: isolated tables for projects with their own S3 bucket
    custom_project_tables: CustomProjectTables,
    /// Lock-free (project, table) → resolved Delta table cache. The inner
    /// `Arc<RwLock<DeltaTable>>` is the same object held in the table maps
    /// above, so slow-path `update_state` is seen by hot-path callers.
    /// No eviction: grows with unique pairs seen since process start.
    fast_resolve_cache: FastResolveCache,
    /// Per-(project, table) sticky "Delta may hold matching files" bit — never
    /// falsely `false`. While false, scans skip Delta and MemBuffer is
    /// authoritative. `Arc<AtomicBool>` because `Database` derives `Clone`.
    delta_has_files: dashmap::DashMap<(String, String), Arc<std::sync::atomic::AtomicBool>>,
    /// Cached Delta-side `TableProvider` per (project, table) + snapshot
    /// version. Exact-version invalidation; concurrent misses single-flight
    /// via a per-key `OnceCell`. No drop eviction.
    delta_provider_cache: DeltaProviderCache,
    /// Cumulative scan-path counters, exported via `timefusion_stats`.
    pub scan_metrics: Arc<ScanMetrics>,
```

Note "No eviction: grows with unique pairs seen since process start" — twice. The
design target is "thousands of tenants", and there is a soft warning at ten times
that:

**Listing 7.13 — src/database/mod.rs:61–65**

```rust
/// Soft size at which the no-eviction table caches log a warning.
/// Picked at 10× the documented design target ("thousands of tenants").
/// Crossings are once-per-threshold-multiple, so a runaway tenant churn
/// surfaces as growing log frequency rather than a single quiet spike.
const CACHE_SOFT_LIMIT_WARN: usize = 10_000;
```

Locks and semaphores — there are eleven, and each one's doc comment says what it
is bounding and what happened without it:

**Listing 7.14 — src/database/mod.rs:2200–2245 (the semaphores)**

```rust
    /// Caps concurrent heavy rewrites (dedup staging, optimize, consolidate,
    /// recompress). Their Arrow footprint is invisible to the DataFusion memory
    /// pool, so aggregate concurrency, not the pool, is the real OOM bound.
    /// Hot-tail waves deliberately use [`Self::light_rewrite_sem`] instead.
    maintenance_rewrite_sem: Arc<tokio::sync::Semaphore>,
    /// Caps hot-tail wave staging. Separate from `maintenance_rewrite_sem` so a
    /// long dedup drain can't starve hot compaction (disjoint partitions, so
    /// serializing them is pure loss). Sized to the light pool's own K.
    light_rewrite_sem: Arc<tokio::sync::Semaphore>,
    /// Caps coordinator workers in debt work (dedup, packing, consolidation,
    /// repair), reserving the rest for rollup. Bounds occupancy, not memory:
    /// debt units hold a worker for minutes and would otherwise starve cheap
    /// rollup units regardless of attempt share.
    maintenance_debt_slots: Arc<tokio::sync::Semaphore>,
    /// Caps workers inside units already timed out under
    /// `TaskJournal::QUARANTINE_ATTEMPTS`: bounds the wall-clock cost of a
    /// doomed unit, not its frequency.
    maintenance_quarantine_slots: Arc<tokio::sync::Semaphore>,
    /// Reserves workers for `DerivedRollup` while coverage is short — slow
    /// sealed `BaseRollup` days would otherwise occupy every freed slot while
    /// cheap derived work (the actual coverage gap) starves.
    maintenance_derived_reserve: Arc<tokio::sync::Semaphore>,
    /// Caps concurrent user DML MERGE-UPDATEs; each scans the time-windowed
    /// target and ungated bursts starve reads. Permits =
    /// `timefusion_dml_merge_concurrency`.
    dml_merge_sem: Arc<tokio::sync::Semaphore>,
    /// Caps concurrent Parquet decodes for WIDE scans across all queries
    /// (`timefusion_max_concurrent_scan_readers`), so a burst of wide-window
    /// dashboards can't stack decode buffers into an OOM.
    heavy_scan_sem: Arc<tokio::sync::Semaphore>,
    /// Serializes the outer full and light maintenance jobs; rewrite permits
    /// alone let a waiting light job exhaust its table timeout before starting.
    maintenance_job_sem: Arc<tokio::sync::Semaphore>,
    /// Serializes in-process Delta commits per physical table (`table_lock_key`).
    /// delta-kernel's OCC checker can't evaluate `replace_where`'s timestamp
    /// predicate, so a dedup commit racing a concurrent append to the same log
    /// aborts; per-log serialization lets the rebase skip the checker.
    commit_locks: DmlLocks,
    /// Flush/ingest committers queued on each table's `commit_locks` entry.
    /// Durability outranks maintenance: `commit_wave` declines to enqueue while
    /// nonzero, bounding flush latency to one in-flight wave commit.
    flush_waiter_counts: FlushWaiterCounts,
    /// Per-table serialization for in-process DML (see `dml_lock`): concurrent
    /// merges would OCC-conflict and redo full rewrites. Queuing here leaves
    /// the table's RwLock free for readers and insert commits.
    dml_locks: DmlLocks,
```

`flush_waiter_counts` deserves a moment. It is a per-table count of ingest
committers *queued* on the commit lock, and maintenance consults it to decide
whether to enqueue at all — a priority inversion guard, expressed as a counter
rather than a priority queue. The RAII helper that maintains it is small and
correct in a way that is easy to get wrong:

**Listing 7.15 — src/database/mod.rs:412–424**

```rust
/// Per-physical-table count of flush/ingest committers QUEUED on the commit lock
/// — see `Database::flush_waiters` and the priority check in `commit_wave`.
type FlushWaiterCounts = Arc<dashmap::DashMap<(String, String), Arc<std::sync::atomic::AtomicUsize>>>;

/// RAII count of one flush/ingest committer waiting on a per-table commit lock.
/// Must decrement on lock acquisition AND on future cancellation — drop
/// covers both; a manual decrement leaks on cancellation and wedges maintenance.
fn flush_waiter(count: &Arc<std::sync::atomic::AtomicUsize>) -> impl Drop + use<> {
    count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    scopeguard::guard(Arc::clone(count), |count| {
        count.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    })
}
```

Twelve lines, and the doc comment states exactly why it is RAII rather than a
matched pair of `fetch_add`/`fetch_sub`: an async future can be *cancelled*
between them, and a leaked increment permanently blocks maintenance on that table.

### The Delta provider cache: a version ring

A subtle but instructive piece. Building a DataFusion `TableProvider` from a Delta
snapshot is expensive, so it is cached — but caching a *single* provider per
table thrashes under flush cadence, since every commit bumps the version and
concurrent readers may resolve different versions. The solution is a short ring
keyed by exact version:

**Listing 7.16 — src/database/mod.rs:67–124**

```rust
/// Per-key build de-duplicator for the cached Delta `TableProvider`. The inner
/// `OnceCell` is initialised exactly once per `(project, table, version)`; all
/// concurrent first-time misses share the same Arc and await the same build.
type DeltaProviderCell = tokio::sync::OnceCell<Arc<dyn datafusion::datasource::TableProvider>>;

#[derive(derive_more::Debug)]
#[debug("CachedDeltaProvider {{ version: {version}, age: {:?}, .. }}", created_at.elapsed())]
struct CachedDeltaProvider {
    version: u64,
    created_at: std::time::Instant,
    cell: Arc<DeltaProviderCell>,
}

/// Snapshot versions kept cached per `(project, table)`. A single slot thrashes
/// under flush cadence (every commit bumps the version; concurrent readers
/// observe non-monotonically); a short ring serves each query its exact version.
const PROVIDER_VERSION_RETENTION: usize = 3;

/// The recent-version ring for one `(project, table)`, newest first.
#[derive(Debug, Default)]
struct ProviderVersions {
    versions: Vec<CachedDeltaProvider>,
}

impl ProviderVersions {
    /// Cell for `version`, if cached and within TTL. Exact-version match — an
    /// older retained version is never handed to a query that resolved a newer
    /// snapshot.
    fn get(&self, version: u64, ttl: std::time::Duration) -> Option<Arc<DeltaProviderCell>> {
        self.versions.iter().find(|e| e.version == version && e.created_at.elapsed() <= ttl).map(|e| Arc::clone(&e.cell))
    }

    /// Install a fresh cell for `version` at the head, dropping any expired or
    /// same-version predecessor, and keep only `PROVIDER_VERSION_RETENTION`.
    fn install(&mut self, version: u64, ttl: std::time::Duration) -> Arc<DeltaProviderCell> {
        let cell = Arc::new(DeltaProviderCell::new());
        self.versions.retain(|e| e.version != version && e.created_at.elapsed() <= ttl);
        self.versions.insert(0, CachedDeltaProvider { version, created_at: std::time::Instant::now(), cell: Arc::clone(&cell) });
        self.versions.truncate(PROVIDER_VERSION_RETENTION);
        cell
    }
    // ... elided: prune(), len(), holds() — bookkeeping used by the eviction path
}
```

The `derive_more::Debug` attribute is a nice small touch: the `Debug` impl prints
the *age* rather than the raw `Instant`, which is what an operator reading a log
line actually wants.

`tokio::sync::OnceCell` as the value is the single-flight mechanism: N concurrent
misses on the same `(project, table, version)` all await the same build rather
than each doing it.

### Refreshing a snapshot without convoying

Related, and a good example of a lock-scope fix:

**Listing 7.17 — src/database/mod.rs:453–470**

```rust
/// Refresh `table`'s snapshot without holding the write lock across `update_state()`.
///
/// `update_state()` replays the Delta log and does object-store IO; every query refreshes after a
/// flush, so holding the write lock convoyed planning. Clone-update-swap instead: readers keep the
/// old snapshot while a clone refreshes, and the write lock is held only for the swap. The swap is
/// version-guarded against concurrent committers, so the shared handle never regresses.
pub(crate) async fn refresh_table_snapshot(table: &Arc<RwLock<DeltaTable>>, incremental: bool) -> std::result::Result<Option<u64>, deltalake::DeltaTableError> {
    // Staleness probe: versions are contiguous, so the snapshot is current iff
    // `{version+1}.json` doesn't exist — one GET/404 instead of update_state's
    // `_delta_log` LIST (LISTs bypass the Foyer cache). On probe *error* fall
    // through to the full refresh — never skip on uncertainty.
    {
        let guard = table.read().await;
        if let Some(v) = guard.version() {
            let log_store = guard.log_store();
            drop(guard);
            if matches!(log_store.read_commit_entry(v + 1).await, Ok(None)) {
                return Ok(Some(v));
            }
        }
    }
    // ... elided: the clone-update-swap and the append-only fast catch-up
```

Two ideas in eighteen lines. *Clone-update-swap* keeps the write lock's hold time
to the swap alone. And the *staleness probe* replaces a `_delta_log` LIST with a
single GET-or-404 on `{version+1}.json`, exploiting the fact that Delta versions
are contiguous — which matters because, as the comment says, LISTs bypass the
Foyer cache entirely.

The decision of whether to refresh at all is three lines with a scar:

**Listing 7.18 — src/database/mod.rs:436–446**

```rust
/// Should `resolve_*_table` call `update_state()` on the cached snapshot?
/// Biased toward refreshing: skip only when this process's own writes prove the
/// snapshot current — a `(Some(_), None) => false` shortcut broke buffer→Delta
/// visibility (a background flusher may have committed).
fn should_refresh_table(current_version: Option<u64>, last_written_version: Option<u64>) -> bool {
    match (current_version, last_written_version) {
        (Some(current), Some(last)) => current < last,
        (None, None) => false,
        _ => true,
    }
}
```

### `ProjectRoutingTable`: the `TableProvider`

Every user-visible table is one of these. It is small, because all the state lives
in `Database`:

**Listing 7.19 — src/database/mod.rs:7049–7075**

```rust
#[derive(Debug, Clone)]
pub struct ProjectRoutingTable {
    default_project: String,
    database: Arc<Database>,
    schema: SchemaRef,
    _batch_queue: Option<Arc<crate::write::BatchQueue>>,
    table_name: String,
    /// When true, INSERTs commit straight to Delta (`skip_queue=true`),
    /// bypassing the BufferedWriteLayer (WAL + MemBuffer). Backs the
    /// `{table}__bulk` alias for backfills / DLQ drains that must not pressure
    /// the live MemBuffer. Reads route to the same underlying Delta table.
    skip_queue: bool,
}

impl ProjectRoutingTable {
    pub fn new(
        default_project: String, database: Arc<Database>, schema: SchemaRef, batch_queue: Option<Arc<crate::write::BatchQueue>>, table_name: String,
    ) -> Self {
        Self { default_project, database, schema, _batch_queue: batch_queue, table_name, skip_queue: false }
    }

    /// Route this provider's INSERTs straight to Delta, bypassing the
    /// BufferedWriteLayer. Backs the `{table}__bulk` alias.
    pub fn with_skip_queue(mut self, skip_queue: bool) -> Self {
        self.skip_queue = skip_queue;
        self
    }
```

The `{table}__bulk` alias is worth knowing about: it is a second registered
provider over the same physical Delta table whose `INSERT`s bypass the WAL and
MemBuffer entirely. Backfills and dead-letter-queue drains use it so they do not
pressure the live ingest buffer.

`ProjectRoutingTable` implements three DataFusion traits, and the split is the
whole architecture in miniature:

- `TableProvider::scan` — the read path (Chapter 13).
- `TableProvider::insert_into` → `DataSinkExec` → `DataSink::write_all` — the
  write path (Chapter 10).
- `DisplayAs` — because `DataSink` requires it.

The two-schema trick from Chapter 6 surfaces here as a deliberate lie:

**Listing 7.20 — src/database/mod.rs:7150–7160**

```rust
    fn schema(&self) -> SchemaRef {
        // Present Variant cols as Utf8View at the table-provider boundary so the SQL planner's
        // INSERT VALUES type check accepts JSON string literals (arrow has no Utf8→Struct cast).
        // `write_all` converts these Utf8 columns back to Variant structs before the Delta write.
        create_insert_compatible_schema(&self.schema)
    }

    /// Real (Variant-typed) schema for internal use.
    pub fn real_schema(&self) -> SchemaRef {
        self.schema.clone()
    }
```

and the consequence is documented at the point where the check would normally
happen:

**Listing 7.21 — src/database/mod.rs:8439–8452**

```rust
    async fn insert_into(&self, _state: &dyn Session, input: Arc<dyn ExecutionPlan>, insert_op: InsertOp) -> DFResult<Arc<dyn ExecutionPlan>> {
        if insert_op != InsertOp::Append {
            error!("Unsupported insert operation: {:?}", insert_op);
            return not_impl_err!("{insert_op} not implemented for MemoryTable yet");
        }
        // No `logically_equivalent_names_and_types(&input.schema())` check here:
        // `self.schema()` returns the "insert-compatible" (lying) schema where
        // Variant columns appear as Utf8View so VALUES literals type-check.
        // Validating against that shape would reject the real downstream batches
        // (which carry Variant). `write_all` coerces back to Variant before
        // the Delta commit, so the type contract is enforced at the boundary
        // that matters.
        Ok(Arc::new(DataSinkExec::new(input, Arc::new(self.clone()), None)))
    }
```

### Filter pushdown: `Exact`, `Inexact`, `Unsupported`

DataFusion asks the provider what it can do with each filter, and TimeFusion's
answer encodes the merge-on-read invariant from Chapter 6. This is the function
that decides whether a point lookup is fast or fatal:

**Listing 7.22 — src/database/mod.rs:8454–8490**

```rust
    fn supports_filters_pushdown(&self, filter: &[&Expr]) -> DFResult<Vec<TableProviderFilterPushDown>> {
        // Variant columns are Struct(Binary, Binary); the delta-kernel scan cannot
        // evaluate predicates on them ("Predicate references unknown column: <col>").
        // Mark any filter that references a Variant column `Unsupported` so DataFusion
        // applies it via a FilterExec above the scan rather than pushing it into the
        // kernel. (Variant predicates can't prune row groups anyway.)
        let variant_cols: HashSet<String> = crate::schema::registry()
            .get(&self.table_name)
            .map(|s| s.schema_ref().fields().iter().filter(|f| crate::schema::is_variant_type(f.data_type())).map(|f| f.name().clone()).collect())
            .unwrap_or_default();
        let mutable = Self::version_mutable_columns(&self.table_name);
        Ok(filter
            .iter()
            .map(|f| {
                if Self::references_tombstone(&self.table_name, f)
                    || (!variant_cols.is_empty() && f.column_refs().iter().any(|c| variant_cols.contains(&c.name)))
                {
                    TableProviderFilterPushDown::Unsupported
                } else if mutable.as_ref().is_some_and(|m| f.column_refs().iter().any(|c| m.contains(&c.name))) {
                    // `Inexact`, not `Unsupported`: DataFusion keeps its own
                    // FilterExec above the scan either way, but `Unsupported`
                    // also withholds the predicate from `scan()`, and `scan()`
                    // is the only place that knows whether this window is
                    // sweep-certified. `scan()` re-strips it for every path that
                    // is not (see `leg_safe`), so this widens what `scan()` can
                    // SEE without widening what any leg is given. NEVER `Exact`:
                    // the above-dedup FilterExec is what makes a stale-version
                    // match impossible, and `Exact` would delete it.
                    TableProviderFilterPushDown::Inexact
                } else if Self::is_exact_pushdown_filter(f) {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Inexact
                }
            })
            .collect())
    }
```

Three-valued logic used precisely:

| Verdict | Meaning | Used for |
|---|---|---|
| `Unsupported` | provider will not see it; DataFusion filters above | tombstone column, Variant columns |
| `Inexact` | provider sees it *and* DataFusion keeps a `FilterExec` above | mutable columns, everything unclassified |
| `Exact` | provider fully applies it; DataFusion drops its filter | partition columns only |

And `Exact` is deliberately restricted to *partition columns alone*, for a reason
that is easy to get wrong:

**Listing 7.23 — src/database/mod.rs:7197–7206**

```rust
    /// True for columns that support exact pushdown — the table provider fully applies the
    /// filter and DataFusion can drop the `FilterExec`.
    ///
    /// Only true partition columns qualify: Delta's partition pruning is exact, and partition values
    /// are compared exactly inside MemBuffer. Other columns were previously listed, but MemBuffer's
    /// best-effort physical-expr compilation can silently fall back to "no filter"; with exact
    /// pushdown the filter is gone and rows leak through.
    fn is_pushdown_column(column_name: &str) -> bool {
        matches!(column_name, "project_id" | "date")
    }
```

"MemBuffer's best-effort physical-expr compilation can silently fall back to no
filter" — so claiming `Exact` for a column the MemBuffer might not actually filter
on produces *wrong results*, not slow ones. The union-of-heterogeneous-legs
architecture means `Exact` is only safe for predicates every leg can evaluate
exactly.

### `GatedScanExec`: bounding what no pool tracks

TimeFusion adds exactly one custom `ExecutionPlan` node to the read path, and its
purpose is to bound Parquet decode heap — memory that the DataFusion memory pool
does not see.

**Listing 7.24 — src/database/mod.rs:8112–8140**

```rust
/// Concurrency-gate a wide read scan.
///
/// Each output partition acquires a permit around every batch decode, bounding the number of
/// Parquet row groups decoded at once across all wide queries. Parquet decode heap is untracked by
/// the DataFusion memory pool, so unbounded parallelism can OOM the process. Acquisition is
/// per-batch, not per-stream: holding a permit for a partition's whole lifetime would deadlock
/// `SortPreservingMergeExec`, which needs one batch from every input before it can emit.
#[derive(Debug)]
struct GatedScanExec {
    input: Arc<dyn ExecutionPlan>,
    sem: Arc<tokio::sync::Semaphore>,
    properties: Arc<PlanProperties>,
    /// Decode accounting only — this operator never denies on memory.
    metrics: Option<Arc<ScanMetrics>>,
    /// Scan-resistant admission: a scan deep enough to be reading history — not
    /// the hot tail — must not evict the hot tail on its way through. Derived
    /// once, from the caller's filters (see `gate_if_wide`).
    bypass_cache: bool,
    /// Size of `sem`'s pool — `scan_pressure_permits` scales its claim off it
    /// (tokio semaphores don't expose their initial size).
    pool_size: u32,
}
```

The per-batch rather than per-stream acquisition is the load-bearing detail, and
the reason given — `SortPreservingMergeExec` needs one batch from every input
before it can emit, so a per-stream permit would deadlock — is exactly right and
not obvious.

The `execute` body is where the adaptive accounting happens:

**Listing 7.25 — src/database/mod.rs:8164–8212**

```rust
    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> DFResult<SendableRecordBatchStream> {
        let inner = self.input.execute(partition, context)?;
        let schema = inner.schema();
        let sem = self.sem.clone();
        let metrics = self.metrics.clone();
        let bypass = self.bypass_cache;
        let pool_size = self.pool_size;
        // Hold a permit only across each `poll_next` (one batch decode), then
        // release so other partitions/queries can proceed — see type docs.
        // The permit window is also exactly the decode window, which is what
        // makes this the honest place to measure decode heap.
        // `last_bytes` is this stream's most recent decoded batch size: the
        // claim adapts to what this scan actually produces instead of every
        // poll reserving the 145 MB worst case.
        let gated = futures::stream::unfold((inner, 0u64), move |(mut inner, last_bytes)| {
            let sem = sem.clone();
            let metrics = metrics.clone();
            async move {
                // Near the OOM line each poll claims more of the pool,
                // shrinking effective decode concurrency (see
                // `scan_pressure_permits`). `acquire_many` never exceeds the
                // pool size, so progress is guaranteed.
                // Pressure valve wins when it is engaged (it claims a quarter
                // or all of the pool); otherwise the heap-proportional claim does.
                let want = scan_pressure_permits(pool_size).max(decode_units(last_bytes)).min(pool_size);
                let _permit = sem.acquire_many_owned(want).await.ok()?;
                if let Some(m) = &metrics {
                    m.decode_begin();
                    if want > DECODE_UNITS_PER_READER {
                        metrics::counter!(scan_metric_names::DECODE_PRESSURE_THROTTLED).increment(1);
                    }
                }
                // The object-store fetches for this batch happen inside the
                // poll, so the bypass scope covers exactly them. Only paid for
                // when it's actually suppressing.
                let next = match bypass {
                    true => crate::storage::scan_bypass_scope(true, futures::StreamExt::next(&mut inner)).await,
                    false => futures::StreamExt::next(&mut inner).await,
                };
                let produced = next.as_ref().and_then(|r| r.as_ref().ok()).map_or(0, |b: &RecordBatch| b.get_array_memory_size() as u64);
                if let Some(m) = &metrics {
                    // Size the decoded Arrow, not the compressed parquet.
                    m.decode_end(produced);
                }
                next.map(|item| (item, (inner, produced)))
            }
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, gated)))
    }
```

Two mechanisms are multiplexed onto one semaphore. The first is
*heap-proportional claiming*: a reader slot is subdivided into 16 units, and a
poll claims units in proportion to the bytes its *previous* batch produced.

**Listing 7.26 — src/database/mod.rs:8063–8092**

```rust
/// Sub-divisions of one reader slot in the wide-scan gate.
///
/// The gate bounds Parquet decode heap, which is untracked by the DataFusion memory pool.
/// Counting raw polls sized each permit for the worst-case batch while real dashboard batches
/// are orders of magnitude smaller, so most of the decode budget sat unused while polls queued.
/// Splitting each reader slot into `K` units and charging a poll for the heap it actually
/// produced keeps the same ceiling — a full-size batch still claims all `K` units, while a tiny
/// batch claims one. This is why `timefusion_max_concurrent_scan_readers` is not raised: doing
/// so would raise the heap ceiling, which is the one thing the guard exists to hold down.
const DECODE_UNITS_PER_READER: u32 = 16;

/// Worst-case decoded batch size one reader slot is sized for.
///
/// 8192-row decode batches of wide OTel rows measured up to 145 MB; this is the number the
/// poll-counting gate was implicitly built around.
const NOMINAL_DECODE_BATCH_BYTES: u64 = 145 * 1024 * 1024;

/// Units a poll must claim to cover `last_batch_bytes` of decoded Arrow.
///
/// `0` means "not yet known" (a stream's first poll) and claims a whole reader
/// slot, i.e. exactly the pre-split behaviour; the claim adapts down once the
/// stream has shown what its batches actually weigh. Never exceeds one slot, so
/// the heap ceiling is unchanged, and never zero, so progress is guaranteed.
fn decode_units(last_batch_bytes: u64) -> u32 {
    let k = u64::from(DECODE_UNITS_PER_READER);
    match last_batch_bytes {
        0 => DECODE_UNITS_PER_READER,
        b => (b.saturating_mul(k).div_ceil(NOMINAL_DECODE_BATCH_BYTES)).clamp(1, k) as u32,
    }
}
```

The second is a *pressure valve* that escalates as RSS approaches the limit,
tiered on both absolute usage and a projected time-to-limit:

**Listing 7.27 — src/database/mod.rs:8094–8110**

```rust
/// Tier math for `scan_pressure_permits`, separated for testability.
///
/// Permits one decode poll must claim, given both current usage and its rate. Whichever tier is
/// reached first wins: absolute percentages backstop fast bursts, projections catch bursts early.
/// The projection is gated on [`VALVE_RATE_FLOOR_PCT`] of the limit so a process filling from cold
/// — which grows fast but plateaus far from the limit — is not throttled at a small fraction of
/// the limit.
fn pressure_permit_claim_at(usage_pct: u64, eta_secs: u64, total: u32) -> u32 {
    let projected = usage_pct >= VALVE_RATE_FLOOR_PCT;
    if usage_pct >= 95 || (projected && eta_secs <= VALVE_ETA_TIER2_SECS) {
        total
    } else if usage_pct >= 88 || (projected && eta_secs <= VALVE_ETA_TIER1_SECS) {
        (total / 4).max(1)
    } else {
        1
    }
}
```

At ≥95% usage, or a projected 30 seconds to the limit, one poll claims the *entire
pool* — decode is fully serialized. The tiers are sized against a measured
~450 MB/s burst rate (`VALVE_ETA_TIER1_SECS = 90`, the time to cross 40 GB of
headroom).

### `ScanMetrics` and the two-place-truth problem

The metric names are `const`s in a public module, not string literals at call
sites, and the doc comment says exactly why:

**Listing 7.28 — src/database/mod.rs:169–180**

```rust
/// Counter/gauge names for `ScanMetrics`, shared between the `metrics::counter!()`/
/// `gauge!()` call sites (below) and their `timefusion_stats` readback
/// (`server::pg_compat`) so the two can't drift apart into a silent typo. High-water-mark
/// fields (`decode_polls_inflight`/`_peak`, `decode_peak_batch_bytes`) stay hand-rolled
/// atomics below — `metrics::Gauge` has no `fetch_max`, so there's no clean equivalent.
pub mod scan_metric_names {
    pub const SCANS_TOTAL: &str = "timefusion.scan.scans_total";
    pub const SCANS_SKIPPED_DELTA: &str = "timefusion.scan.scans_skipped_delta";
    pub const SCANS_MEM_ONLY: &str = "timefusion.scan.scans_mem_only";
    pub const SCANS_DELTA_ONLY: &str = "timefusion.scan.scans_delta_only";
    pub const SCANS_MEM_PLUS_DELTA: &str = "timefusion.scan.scans_mem_plus_delta";
    // ... elided: 33 further names, covering dedup verdicts, provider cache,
    // fast resolve, plan timing, decode bytes, wide-scan rejections
}
```

`ScanMetrics` itself is nearly empty, because almost everything routes through the
global `metrics` recorder; only the high-water marks need real atomics:

**Listing 7.29 — src/database/mod.rs:214–249**

```rust
/// High-water-mark decode gauges surfaced via `timefusion_stats`. Separate from the
/// `metrics`-backed counters above because `metrics::Gauge` has no `fetch_max` —
/// `decode_begin`/`decode_end` need the atomic read-modify-write these give directly.
#[derive(Debug, Default)]
pub struct DecodeGauges {
    pub decode_peak_batch_bytes: std::sync::atomic::AtomicU64,
    pub decode_polls_inflight: std::sync::atomic::AtomicU64,
    pub decode_polls_inflight_peak: std::sync::atomic::AtomicU64,
}

/// Counters/gauges surfaced via `timefusion_stats` for production debugging.
// ... elided: 5 lines on the metrics-recorder fan-out
#[derive(Debug, Default)]
pub struct ScanMetrics {
    pub decode: DecodeGauges,
}

impl ScanMetrics {
    /// One gated decode entered: bump the in-flight gauge and its high-water
    /// mark. Returns nothing — the caller pairs it with `decode_end`.
    fn decode_begin(&self) {
        use std::sync::atomic::Ordering::Relaxed;
        let n = self.decode.decode_polls_inflight.fetch_add(1, Relaxed) + 1;
        self.decode.decode_polls_inflight_peak.fetch_max(n, Relaxed);
    }

    /// One gated decode finished, having produced `bytes` of Arrow.
    fn decode_end(&self, bytes: u64) {
        use std::sync::atomic::Ordering::Relaxed;
        self.decode.decode_polls_inflight.fetch_sub(1, Relaxed);
        metrics::counter!(scan_metric_names::DECODE_BYTES_TOTAL).increment(bytes);
        self.decode.decode_peak_batch_bytes.fetch_max(bytes, Relaxed);
    }
```

### `DedupSkipVerdict`: an enum that answers the *next* question

A small type that demonstrates a design habit worth stealing. The read path can
sometimes skip merge-on-read dedup entirely. When it cannot, a boolean would tell
you *that* it declined; this enum tells you what to do about it:

**Listing 7.30 — src/database/mod.rs:142–167**

```rust
/// Why the swept-partition dedup skip was granted or refused, decided once per
/// scan. The split answers "would persisting certifications convert the
/// denial?": `NeverCertified` = yes, `FpMoved` = no (written since certified).
/// See `docs/plans/2026-08-11-certification-survival.md`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DedupSkipVerdict {
    Granted,
    /// `timefusion_read_dedup_skip_swept` is off, or the table declares no dedup keys.
    Disabled,
    /// No usable time bound on the query (no timestamp predicate, or a span so
    /// wide `window_dates` refuses it), so there is no partition set to certify.
    NoWindow,
    /// The table was not already resolved in this process. The skip is an
    /// optimisation, so it declines rather than pay a resolve to decide.
    Unresolved,
    /// Some in-window partition has no `dedup_clean_fp` entry at all.
    NeverCertified,
    /// Some in-window partition was certified and has been committed to since.
    FpMoved,
}

impl DedupSkipVerdict {
    pub fn granted(self) -> bool {
        self == Self::Granted
    }
}
```

Each variant is a separate counter (`DEDUP_DENIED_NEVER_CERTIFIED`,
`DEDUP_DENIED_FP_MOVED`, …), so the production dashboards can answer "would
persisting certifications across restarts help?" directly from the ratio rather
than from a guess.

### Certification: proving a partition is duplicate-free

Two small types back the read-side dedup skip:

**Listing 7.31 — src/database/mod.rs:328–353**

```rust
/// One partition proved duplicate-free: the file fingerprint it was proved over,
/// and when. `since` exists purely to measure how long certifications survive —
/// the evidence that decides whether persisting them could ever pay.
#[derive(Clone, Copy, Debug)]
struct Certification {
    fp: u64,
    since: std::time::Instant,
}

/// Clean dedup slices accumulated toward certifying one (project, table, date)
/// partition: disjoint sorted `[start, end)` intervals, all proved over `fp`.
/// A unit that drops rows or a fingerprint move resets the accumulation —
/// evidence over a moved file set is void. Memory-only: a restart just
/// re-accumulates; certification itself persists.
#[derive(Clone, Debug)]
struct SliceCoverage {
    fp: u64,
    intervals: Vec<(i64, i64)>,
}

/// Merge `[start, end)` into a sorted vec of disjoint half-open intervals.
fn merge_clean_interval(intervals: &mut Vec<(i64, i64)>, interval: (i64, i64)) {
    intervals.push(interval);
    intervals.sort_unstable();
    *intervals = intervals.iter().copied().coalesce(|a, b| if b.0 <= a.1 { Ok((a.0, a.1.max(b.1))) } else { Err((a, b)) }).collect();
}
```

The certification protocol is: a dedup sweep proves a `(project, table, date)`
partition duplicate-free *over a specific set of live files*, and records that
set's fingerprint. Any later commit changes the file set, the fingerprint no
longer matches, and the certification is void. It is an optimistic proof with a
cheap invalidation check — the same idea as an ETag.

`merge_clean_interval` is a nice one-expression interval coalesce using
`itertools::coalesce`; the sort-then-coalesce is O(n log n) per insertion, which is
fine because the interval count per partition is small (144 ten-minute slices at
most).

### The pattern summary

Five structural habits recur throughout the codebase, and recognizing them makes
the remaining 70,000 lines much faster to read:

1. **Late-binding `Arc<OnceLock<T>>`** for anything constructed after `Database`
   but needed by clones taken before it (`buffered_layer`, `tantivy_search`,
   `tantivy_indexer`, `dml_coalescer`, every `RuntimeEnv`).
2. **Per-clone `bool` flags** for scoped behavior (`bypass_buffer`,
   `bypass_rollup`, `maintenance_scan`) instead of a threaded context.
3. **Disjoint pools and semaphores per workload class**, sized from one budget
   tree, because the process has exactly one memory limit and several
   independent consumers.
4. **Enums instead of booleans** wherever the caller (or a dashboard) would
   otherwise have to guess why (`DedupSkipVerdict`, `PrefilterDecision`,
   `RecompressOutcome`, `ResumeVerdict`).
5. **RAII guards for anything an async cancellation could leak**
   (`flush_waiter`, `scopeguard::guard`).

**Key takeaways.** `(project_id, table_name)` addresses everything.
`DerivedBudget` splits one cgroup limit into six named pools with pinned
fractions and no operator knobs, because per-consumer knobs drift and the sum is
what kills the process. `Database` is a large `Clone` type whose fields are
either shared `Arc`s or per-clone behavior flags. `ProjectRoutingTable` is the
`TableProvider`/`DataSink` and deliberately presents a *lying* schema so that
`INSERT ... VALUES` accepts JSON literals into Variant columns.
`supports_filters_pushdown` encodes the merge-on-read commutativity invariant in
three-valued logic, and `Exact` is restricted to partition columns because a
heterogeneous union of legs cannot promise more. `GatedScanExec` bounds the
memory DataFusion's pool cannot see.
## 8. State, storage, and persistence

*After this chapter you will know where every piece of TimeFusion's state lives,
which parts are authoritative and which are rebuildable, how the Foyer two-tier
cache sits underneath every object-store read, and what the four best-effort
local sidecar files are for.*

### The state inventory

| State | Lives in | Authoritative? | Lost on restart? |
|---|---|---|---|
| Committed rows | Delta on object storage | **yes** | no |
| Acknowledged-but-unflushed rows | WAL (local disk) + MemBuffer | **yes, until flushed** | no (WAL) |
| Rows in MemBuffer | RAM | no (WAL has them) | replayed |
| Object bytes | Foyer L1 (RAM) + L2 (local disk) | no | L2 survives; L1 does not |
| Delta table snapshots | `.timefusion_meta/delta_snapshots/` | no | falls back to full S3 load |
| Dedup certifications | `.timefusion_meta/dedup_certifications.json` | no | skip starts cold |
| Dirty dedup bins | `.timefusion_meta/dedup_dirty_bins.json` | no | re-derived |
| WAL cursors | `.timefusion_meta/cursor_snapshot.json` | no | Delta scan derives them |
| Maintenance task journal | `.timefusion_meta/maintenance_tasks.json` | **yes, for scheduling** | work is re-minted |
| Tantivy index blobs | object storage | no | rebuildable from Delta |
| Rollup tables | Delta on object storage | no | rebuildable from source |
| Plan cache, provider cache, resolve cache | RAM | no | rebuilt |

`docs/ARCHITECTURE.md:56–63` states the rule and its one exception: everything
derived is rebuildable, and "the WAL is not derived until its acknowledged rows
reach Delta."

### The object-store cache

`src/storage.rs` implements `object_store::ObjectStore` on top of Foyer, so every
Delta read — log JSON, checkpoints, Parquet footers, Parquet data — passes through
a two-tier cache without any caller knowing.

Two *separate* cache instances, because the access patterns are different:

**Listing 8.1 — src/storage.rs:452–462**

```rust
/// Shared Foyer cache that can be used across multiple object stores
#[derive(Debug)]
pub struct SharedFoyerCache {
    cache: FoyerCache,
    metadata_cache: FoyerCache,
    stats: StatsRef,
    metadata_stats: StatsRef,
    config: FoyerCacheConfig,
    evictions: Arc<AtomicU64>,
}
```

The **data** cache holds Parquet bodies (large, TTL'd, disk-heavy). The
**metadata** cache holds `_delta_log` JSON, `_last_checkpoint`, and Parquet
footers (small, hot, and re-read on *every* query plan). Keeping them separate
means a wide historical scan streaming gigabytes of Parquet cannot evict the
footers that every plan needs.

The configuration is a single `educe::Educe`-derived struct where each default is
written on the field it belongs to:

**Listing 8.2 — src/storage.rs:140–195**

```rust
/// Configuration for the foyer-based object store cache
#[derive(Debug, Clone, educe::Educe)]
#[educe(Default)]
pub struct FoyerCacheConfig {
    #[educe(Default = 134_217_728)] // 128MB
    pub memory_size_bytes: usize,
    #[educe(Default = 107_374_182_400)] // 100GB
    pub disk_size_bytes: usize,
    #[educe(Default(expression = Duration::from_secs(86_400)))] // 24h
    pub ttl: Duration,
    #[educe(Default(expression = PathBuf::from("/tmp/timefusion_cache")))]
    pub cache_dir: PathBuf,
    #[educe(Default = 8)]
    pub shards: usize,
    #[educe(Default = 16_777_216)] // 16MB - good for Parquet files
    pub file_size_bytes: usize,
    #[educe(Default = true)]
    pub enable_stats: bool,
    /// Size hint for reading parquet metadata from the end of files
    #[educe(Default = 1_048_576)] // 1MB - typical size for parquet metadata
    pub parquet_metadata_size_hint: usize,
    /// Memory size for metadata cache in bytes
    #[educe(Default = 67_108_864)] // 64MB
    pub metadata_memory_size_bytes: usize,
    /// Disk size for metadata cache in bytes
    #[educe(Default = 536_870_912)] // 512MB
    pub metadata_disk_size_bytes: usize,
    /// Number of shards for metadata cache — fewer than the data cache needs.
    #[educe(Default = 4)]
    pub metadata_shards: usize,
    /// Optional extra cap on bytes buffered to warm the cache inline from a
    /// multipart write (see `CachingMultipartUpload`). Always bounded by
    /// `block_size_bytes`; 0 = bound only by the block size (the default).
    pub warm_inline_max_bytes: usize,
    /// Per-upload cap on bytes teed into heap by `CachingMultipartUpload`.
    /// Sized for flush outputs; big compaction outputs skip the tee and are
    /// warmed post-commit via the read path. 0 = bounded only by the block size.
    #[educe(Default = 33_554_432)] // 32MB — flush-sized files only
    pub write_capture_max_bytes: usize,
    /// Process-wide budget for in-flight write-capture buffers. 0 = unbudgeted.
    #[educe(Default = 268_435_456)] // 256MB process-wide (8 x the per-upload cap)
    pub write_capture_budget_bytes: usize,
    /// Disk block size for the main data cache — foyer's eviction unit and the
    /// hard cap on the largest entry that can persist to disk. Must be >= the
    /// largest file we want cached (compaction target size).
    #[educe(Default = 268_435_456)] // 256MB — fits 128MB compaction outputs
    pub block_size_bytes: usize,
    /// Entries larger than this are inserted disk-only (`Location::OnDisk`) so
    /// they don't evict the hot L1 working set. 0 = always use L1.
    #[educe(Default = 16_777_216)] // 16MB
    pub l1_max_entry_bytes: usize,
    /// Don't admit writes whose `date=` partition is older than this many days.
    /// 0 = no age limit.
    #[educe(Default = 8)]
    pub cache_recent_days: usize,
}
```

This is the `educe` motivation from Chapter 4 in practice: `#[derive(Default)]`
cannot express a non-`Default::default()` field value, so every such struct
needed a hand-written `impl Default` — and the value would then sit 40 lines away
from the doc comment explaining it. `educe`'s `Default` takes the value *on the
field*, which is where the documentation already is.

Four of these fields form a coherent eviction policy worth understanding:

- **`block_size_bytes` (256 MB)** is Foyer's disk eviction unit and therefore the
  hard cap on the largest entry that can persist to disk. It must be ≥ the
  compaction target size, or compacted files can never be cached.
- **`l1_max_entry_bytes` (16 MB)** sends anything larger straight to disk,
  bypassing the RAM tier — so one 200 MB Parquet body cannot evict the whole hot
  working set.
- **`cache_recent_days` (8)** refuses to admit writes whose `date=` partition is
  older than eight days: a backfill or a historical rewrite should not displace
  the hot window.
- **`write_capture_max_bytes` (32 MB)** bounds the tee that warms the cache
  inline from a multipart write; big compaction outputs skip the tee entirely and
  are warmed later via the read path.

### The block-size trap

**Listing 8.3 — src/storage.rs:394–407**

```rust
/// Floor for the foyer disk block (region) size. Matches the legacy default
/// (`timefusion_foyer_file_size_mb`), small enough that even a modest disk
/// budget yields several regions.
const MIN_DISK_BLOCK_BYTES: usize = 4 * 1024 * 1024;

/// Cap a desired foyer disk block (region) size to the device. Foyer carves the
/// device into block-sized regions, so a block >= the device leaves zero usable
/// regions and every disk insert stalls (a 256MB block on a 50MB device wedged
/// CI). Keep several regions by capping at a quarter of the device, floored at
/// the legacy 4MB granularity and never above the device itself. Shared by both
/// cache builders so neither can silently wedge on a small disk.
fn capped_block_size(desired: usize, disk_size: usize) -> usize {
    desired.min(disk_size / 4).max(MIN_DISK_BLOCK_BYTES).min(disk_size)
}
```

A one-line clamp with three bounds and a CI incident behind it. The failure mode
is instructive: a block size ≥ the device leaves *zero usable regions*, so every
disk insert stalls — a cache that silently stops caching rather than erroring.

### The dedicated Foyer runtime

The most subtle piece of the file, and a genuinely nasty deadlock:

**Listing 8.4 — src/storage.rs:409–428**

```rust
/// Dedicated runtime for foyer's internal fetch/IO tasks, shared by every
/// cache instance in the process (2 threads, lives for the process).
///
/// Why not the caller's runtime: `RawCache::get_or_fetch_inner` holds its
/// inflight-manager mutex across `Spawner::spawn`. On a live runtime that's
/// fine, but on a runtime that is shutting down tokio cancels the spawned
/// task INLINE — `RawFetch::drop` then re-locks the same non-reentrant mutex
/// on the same thread and deadlocks. Any in-flight cache get racing runtime
/// teardown (test end, prod stop-grace) could hang forever; the e2e restart
/// tests hit it deterministically (3×600s timeouts, 2026-08-03). A dedicated
/// runtime never dies under foyer, so the inline-cancel path can't trigger.
fn foyer_spawner() -> foyer::Spawner {
    static SPAWNER: std::sync::OnceLock<foyer::Spawner> = std::sync::OnceLock::new();
    SPAWNER
        .get_or_init(|| {
            let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(2).thread_name("foyer").enable_all().build().expect("build foyer runtime");
            foyer::Spawner::from(rt)
        })
        .clone()
}
```

The bug chain: Foyer holds a non-reentrant mutex across `Spawner::spawn`; a Tokio
runtime that is *shutting down* cancels a spawned task **inline** rather than
scheduling it; the inline cancel runs `RawFetch::drop`, which re-locks the same
mutex on the same thread; deadlock. Any cache get racing runtime teardown hangs
forever. The fix is to give Foyer a runtime that never dies, so the inline-cancel
path is unreachable.

This is worth internalizing as a general hazard: **`tokio::spawn` during runtime
shutdown does not schedule, it cancels inline**, and inline cancellation runs
destructors on the calling thread. Any `Drop` that takes a lock the spawner is
holding will deadlock.

### Building the tiers

Both caches share one builder, differing only in sizes and the eviction listener:

**Listing 8.5 — src/storage.rs:430–451**

```rust
/// Build one hybrid (memory + disk) cache tier. The data and metadata caches
/// differ only in their sizes and in the eviction listener, so they share this.
async fn build_hybrid_cache(
    dir: &std::path::Path, memory_bytes: usize, shards: usize, disk_bytes: usize, block_size: usize,
    listener: Option<Arc<dyn foyer::EventListener<Key = String, Value = CacheValue>>>,
) -> anyhow::Result<FoyerCache> {
    let builder = HybridCacheBuilder::new().with_policy(HybridCachePolicy::WriteOnInsertion);
    let builder = listener.into_iter().fold(builder, |b, l| b.with_event_listener(l));
    Ok(Arc::new(
        builder
            .memory(memory_bytes)
            .with_shards(shards)
            .with_weighter(|_key: &String, value: &CacheValue| value.data.len())
            .storage()
            .with_spawner(foyer_spawner())
            .with_io_engine_config(PsyncIoEngineConfig::new())
            .with_engine_config(BlockEngineConfig::new(FsDeviceBuilder::new(dir).with_capacity(disk_bytes).build()?).with_block_size(block_size))
            .build()
            .await?,
    ))
}
```

`with_weighter(|_key, value| value.data.len())` is the "weigh the entries" lesson
from Chapter 10's demote queue, applied where it belongs: the LRU's budget is in
*bytes*, not entry count.

`listener.into_iter().fold(builder, ...)` is a tidy way to apply an
`Option<T>`-conditioned builder step without an `if let` and a rebind.

### Scan-resistant admission

A wide historical scan reads gigabytes it will never re-read. Admitting all of it
would evict the hot tail:

**Listing 8.6 — src/storage.rs:735–749**

```rust
/// Run `fut` with cache POPULATION suppressed (lookups still hit normally) when
/// `bypass` is set — scan-resistant admission, so a wide historical scan can't
/// evict the hot tail it will never re-read (ClickHouse's big-scan bypass).
///
/// Task-local, so it covers everything awaited inside `fut` but NOT work the
/// inner store hands to a separate task. That's the intended blast radius: the
/// gated scan's own fetches are what read GBs of cold parquet.
pub fn scan_bypass_scope<F: std::future::Future>(bypass: bool, fut: F) -> impl std::future::Future<Output = F::Output> {
    // ... elided: sets a tokio task-local flag around `fut`
}

/// Cap on [`FoyerObjectStoreCache::repeat_sighting`]'s key set. ~100k keys of
/// path-length strings is a few MB — small against a 4 GB L1, and large enough
/// that a whole dashboard's working set fits without a reset mid-refresh.
const BYPASS_SEEN_MAX: usize = 100_000;
```

Two mechanisms, and the pair is the interesting part. `scan_bypass_scope` is a
*task-local* suppression set by `GatedScanExec` (Chapter 7, Listing 7.25) around
exactly the poll that does the object-store fetches — so the blast radius is
precisely the deep scan's own reads. And `repeat_sighting` tracks keys seen more
than once, so a file that a *second* query also wants is admitted after all: the
bypass suppresses one-shot reads, not genuinely warm ones.

### Range alignment

A small optimization with an outsized effect on dashboard workloads:

**Listing 8.7 — src/storage.rs:30–33**

```rust
/// Align large Parquet data reads so sliding time predicates reuse the same
/// cache entry even when page/coalescing boundaries move slightly. At most two
/// edge blocks are extra, bounding amplification to <2 MiB per request.
const PARQUET_RANGE_ALIGNMENT_BYTES: u64 = 1024 * 1024;
```

A dashboard refreshing every 30 seconds issues *almost* the same query with a
slightly different time window each time. Without alignment, the row-group
coalescing produces byte ranges that differ by a few hundred bytes, so every
refresh misses the cache. Rounding ranges to 1 MiB boundaries makes them
identical, at a bounded cost of at most two extra edge blocks.

### The cache-key rules

Two predicates decide what the cache may do with a request:

**Listing 8.8 — src/storage.rs:647–670**

```rust
/// Strip the `scheme://` prefix and trailing slashes from a table URI, yielding
/// the bare table path used to build `_delta_log` cache keys.
// ... elided: two one-line helpers (table_path, last_checkpoint_key)

/// Whether a GET carries no precondition — the only shape the cache can serve,
/// since a cached body says nothing about the current etag/mtime.
// ... elided

/// Whether a cached object is a Parquet data file (vs. Delta log / checkpoint
/// metadata), which governs TTL and metadata-cache behavior.
```

"A cached body says nothing about the current etag/mtime" is the reason a
conditional GET can never be served from cache. It is obvious once stated and
easy to get wrong.

### Warming

Three warm helpers, all best-effort by construction:

**Listing 8.9 — src/storage.rs:671–733 (doc comments)**

```rust
/// Best-effort: warm the Parquet header and footer of `location` into the cache.
/// The header probe is deliberately metadata too: a cold `0..8` Parquet magic
/// read must not be classified as data and trigger a full-object fallback.

/// Best-effort: warm the Parquet footer of `location` into the cache by issuing
/// a ranged GET of the last `metadata_size_hint` bytes through `store`. When
/// `store` is a [`FoyerObjectStoreCache`], that ranged GET lands in the
/// metadata cache, so subsequent query planning (footer parse, row-group
/// stats, schema, pruning) pays zero S3 round-trips. The single `head` resolves
/// the file size needed to address the tail.
///
/// Strictly best-effort: every error is swallowed and reported via the return
/// value. Warming must never affect correctness or a caller's commit. Returns
/// `true` if the footer range was fetched.

/// HEAD + bounded-GET fallback for [`warm_footer`] when the store doesn't
/// support suffix ranges. Two round-trips, but always correct.

/// Best-effort: warm the full contents of `location` into the cache via a plain
/// GET through `store`. For a [`FoyerObjectStoreCache`] this populates the main
/// (full-file) cache so ranged data reads — DataFusion row-group scans — hit
/// Foyer instead of S3. Errors are swallowed; see [`warm_footer`].
```

The header-probe detail is the kind of thing that only surfaces under
measurement: a cold `0..8` read of the Parquet magic bytes, if classified as
*data*, triggers a full-object fallback fetch. Classifying it as metadata avoids
pulling an entire Parquet file to check eight bytes.

Warming happens after a flush commit (`timefusion_warm_after_compaction`,
`timefusion_warm_all_footers`, `timefusion_warm_concurrency` — Chapter 25) and is
bounded by `CACHE_CONFIRM_TIMEOUT` / `CACHE_CONFIRM_CONCURRENCY`:

**Listing 8.10 — src/config.rs:615–623**

```rust
/// Bound on the post-commit cache confirm. It is an optimization, never a
/// durability gate, so a slow warm must not stall the flush loop.
pub const CACHE_CONFIRM_TIMEOUT: Duration = Duration::from_secs(10);
/// Concurrency of the confirm's full-file fetches. Deliberately NOT the 16-way
/// `timefusion_warm_concurrency` (detached, off the flush path): each miss
/// buffers a whole flush-sized parquet body in transient heap no memory pool
/// tracks, ON the flush path — the untracked-consumer shape behind this box's
/// prior OOMs. Peak ≈ this × largest added file.
pub const CACHE_CONFIRM_CONCURRENCY: usize = 4;
```

The distinction between the *detached* 16-way warm and the *on-flush-path* 4-way
confirm is the untracked-consumer discipline from Chapter 7 applied at a call
site: work on the flush path buffers heap no pool tracks, so its concurrency must
be sized separately and smaller.

### Delta snapshot persistence

Rebuilding a Delta table's state from checkpoint plus log tail on S3 was the
dominant cold-start cost, so the state is persisted locally:

**Listing 8.11 — src/storage.rs:3255–3285**

```rust
// ===== snapshot_cache =====
// Local persistence of Delta table snapshots so a restart restores the last
// known state from disk and replays only commits made since, instead of
// rebuilding from checkpoint + log tail on S3 (prod boot replay was the
// dominant cold-start cost). Files live next to the WAL metadata under
// `TIMEFUSION_DATA_DIR/.timefusion_meta/delta_snapshots/` and are
// best-effort: any failure to write or read falls back to a full S3 load.
//
// Format: zstd-compressed JSON of `(FORMAT_VERSION, table_url, state)`.
// JSON (not bincode) because delta-rs's snapshot Serialize uses
// `serialize_seq(None)`, which non-self-describing formats reject.

use std::fs;

use deltalake::table::state::DeltaTableState;

/// Bump on incompatible layout changes (ours or delta-rs's snapshot serde);
/// old files then just miss and the table does a full load.
const FORMAT_VERSION: u32 = 1;

/// Snapshot files untouched for this long belong to dropped or long-idle
/// tables (active ones rewrite theirs every flush).
pub const SNAPSHOT_MAX_AGE: Duration = Duration::from_secs(7 * 24 * 3600);

fn path_for(dir: &std::path::Path, table_url: &str) -> std::path::PathBuf {
    use std::hash::{DefaultHasher, Hash, Hasher};
    let mut h = DefaultHasher::new();
    table_url.hash(&mut h);
    dir.join(format!("{:016x}.json.zst", h.finish()))
}
```

Note "JSON (not bincode) because delta-rs's snapshot `Serialize` uses
`serialize_seq(None)`, which non-self-describing formats reject" — a genuine
constraint from a dependency's serde impl, recorded so nobody tries to "optimize"
it to bincode.

Note also `DefaultHasher` used for the filename. Unlike the WAL's topic key
(Chapter 11), a hash collision here is *handled*, not catastrophic:

**Listing 8.12 — src/storage.rs:3287–3327**

```rust
/// Best-effort atomic persist (tmp + rename, same pattern as the WAL cursor
/// snapshot). Failures are logged, never propagated — persistence is an
/// optimization, not a correctness requirement.
pub fn store_snapshot(dir: &std::path::Path, table_url: &str, state: &DeltaTableState) {
    let path = path_for(dir, table_url);
    let write = || -> anyhow::Result<()> {
        fs::create_dir_all(dir)?;
        let tmp = path.with_extension("tmp");
        let mut enc = zstd::Encoder::new(fs::File::create(&tmp)?, 3)?;
        serde_json::to_writer(&mut enc, &(FORMAT_VERSION, table_url, state))?;
        enc.finish()?.sync_all()?;
        fs::rename(&tmp, &path)?;
        Ok(())
    };
    match write() {
        Ok(()) => debug!("Persisted delta snapshot for {table_url} to {path:?}"),
        Err(e) => warn!("Failed to persist delta snapshot for {table_url}: {e}"),
    }
}

/// Load a previously persisted snapshot. Any failure — missing file, corrupt
/// or incompatible payload, table-url mismatch (hash collision) — returns
/// `None` and the caller performs a full load.
pub fn load_snapshot(dir: &std::path::Path, table_url: &str) -> Option<DeltaTableState> {
    let path = path_for(dir, table_url);
    let reader = zstd::Decoder::new(fs::File::open(&path).ok()?).ok()?;
    match serde_json::from_reader::<_, (u32, String, DeltaTableState)>(reader) {
        Ok((FORMAT_VERSION, url, state)) if url == table_url => {
            debug!("Restored delta snapshot for {table_url} at version {}", state.version());
            Some(state)
        }
        Ok((version, url, _)) => {
            debug!("Ignoring delta snapshot {path:?}: version {version} / url {url} does not match {FORMAT_VERSION} / {table_url}");
            None
        }
        Err(e) => {
            warn!("Discarding unreadable delta snapshot {path:?}: {e}");
            let _ = fs::remove_file(&path);
            None
        }
    }
}
```

Three defences in the loader, and the pattern is worth copying: the version *and*
the table URL are stored *inside* the payload, so a format bump misses cleanly and
a hash collision is detected rather than silently loading another table's state.
An unreadable file is deleted, so a corruption cannot cost a warning on every
boot forever.

The `tmp` + `rename` write is the standard atomic-file idiom, and it is used
identically for the WAL cursor snapshot — a consistent house pattern.

### The sidecars

Four small JSON files under `.timefusion_meta/`, all best-effort, all with the
same load/store pair:

**Listing 8.13 — src/storage.rs:3515–3551**

```rust
/// Sidecar files in the WAL meta dir (certifications, dirty bins): best-effort,
/// never load-bearing. A missing, unreadable or corrupt file degrades to "empty"
/// with a warning rather than failing a boot, and a failed store is logged and
/// dropped — losing one costs re-derived work, never correctness.
pub fn load_sidecar<T: serde::de::DeserializeOwned>(data_dir: &std::path::Path, (file, what): (&str, &str)) -> Vec<T> {
    let path = crate::write::wal::meta_path(data_dir, file);
    match fs::read(&path).map(|data| serde_json::from_slice(&data)) {
        Ok(Ok(items)) => items,
        Ok(Err(error)) => {
            warn!(?path, %error, "discarding unreadable {what}");
            Vec::new()
        }
        Err(error) if error.kind() == ErrorKind::NotFound => Vec::new(),
        Err(error) => {
            warn!(?path, %error, "failed to load {what}");
            Vec::new()
        }
    }
}

pub fn store_sidecar<T: Serialize>(data_dir: &std::path::Path, (file, what): (&str, &str), items: &[T]) {
    use std::io::Write;
    let path = crate::write::wal::meta_path(data_dir, file);
    let result = path
        .parent()
        .map_or(Ok(()), fs::create_dir_all)
        .and_then(|()| serde_json::to_vec(items).map_err(std::io::Error::other))
        // Reuses the WAL's tmp+rename helper, which also cleans up the tmp file on failure.
        .and_then(|bytes| crate::write::wal::write_atomic_with(&path, false, |f| f.write_all(&bytes)));
    if let Err(error) = result {
        warn!(%error, "failed to persist {what}");
    }
}

/// Sidecar file names, paired with the label their warnings use.
pub const CERTIFICATIONS: (&str, &str) = ("dedup_certifications.json", "certification store");
pub const DIRTY_BINS: (&str, &str) = ("dedup_dirty_bins.json", "dirty-bin queue");
```

The `(file, what)` tuple constant is a small, effective pattern: the file name and
its human label travel together, so every warning message names the right thing
and the two cannot drift.

The certification store is the more interesting of the two, because it stores
*evidence*, and evidence stores are easy to get subtly wrong:

**Listing 8.14 — src/storage.rs:3456–3492**

```rust
// ===== certification_store =====
// Best-effort durable record of sweep certifications.
//
// `dedup_clean_fp` is process-local, so every restart begins with zero certified
// partitions and the read-side dedup skip starts from cold. TF deploys several
// times a day, which is why the skip was measured firing on 0.2–0.5% of
// Delta-reading scans (`docs/plans/2026-08-11-certification-survival.md`).
//
// What is stored is exactly what `record_certification` decided — the
// fingerprint it proved the partition clean over, never a verdict re-derived at
// a different strictness. Nothing here can widen certification: a loaded entry
// is subject to the same fingerprint-equality check against the live file list
// as an in-memory one, so a stale or corrupted record can only cost a skip, not
// grant a wrong one.

use std::io::ErrorKind;

/// Newest-first cap on what is written. Bounds the file for a process that has
/// certified a very large number of partitions; the tail it drops is the oldest,
/// which is also the likeliest to have been invalidated already.
pub const PERSIST_CAP: usize = 20_000;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct StoredCertification {
    pub project_id: String,
    pub table_name: String,
    pub date: String,
    /// `partition_file_fp` over the file set the certifying pass proved clean.
    pub fp: u64,
    /// Wall-clock ms since the epoch at which it was granted. Wall-clock rather
    /// than a monotonic instant precisely because it has to survive the process:
    /// it exists so dwell measures a certification's real lifetime instead of
    /// restarting at every deploy.
    pub granted_unix_ms: u64,
}
```

Two safety properties are stated and both matter:

1. **What is stored is the decision, not a re-derivable verdict.** The
   fingerprint the certifying pass proved the partition clean over is persisted
   verbatim. A later reader cannot re-derive it "at a different strictness".
2. **A loaded entry cannot widen certification.** It is subject to the same
   fingerprint-equality check against the live file list as an in-memory one, so a
   stale or corrupted record can only cost a *skip*, never grant a wrong one.

That is the right shape for any persisted cache of a proof: make the invalidation
check independent of the stored evidence, so corruption is conservative.

The clock handling is careful too:

**Listing 8.15 — src/storage.rs:3494–3502**

```rust
/// Wall-clock ms since the epoch, now.
pub fn now_unix_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_millis() as u64)
}

/// How long ago `granted_unix_ms` was, or `None` if it is in the future — which
/// a backwards clock jump or a hand-edited file can produce, and which must not
/// become a nonsense dwell.
pub fn age_since(granted_unix_ms: u64) -> Option<std::time::Duration> {
    now_unix_ms().checked_sub(granted_unix_ms).map(std::time::Duration::from_millis)
}
```

`checked_sub` returning `None` rather than saturating to zero is the right call
here: a future timestamp is *not* "zero age", it is "this measurement is
meaningless", and the caller should be able to tell the difference.

And the dirty-bin queue, four fields:

**Listing 8.16 — src/storage.rs:3504–3513**

```rust
// ===== dirty_bin_queue =====
// Best-effort durable metadata for sealed-bin dedup scheduling.

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DirtyBin {
    pub project_id: String,
    pub table_name: String,
    pub date: String,
    pub bin: i64,
}
```

### Where the bytes actually are

For the payments span, at rest, minutes after the flush:

```
s3://<bucket>/timefusion/default/otel_logs_and_spans/
  _delta_log/
    00000000000000462918.json          ← the commit that added our file
    _last_checkpoint                   ← metadata cache, hot
  project_id=prod-api-001/date=2025-01-17/
    part-00000-<uuid>.parquet          ← data cache, 256 MB block, disk tier

s3://<bucket>/timefusion/tantivy/
  otel_logs_and_spans/prod-api-001/... ← index blobs (tar + zstd)

<TIMEFUSION_DATA_DIR>/
  wal/                                 ← walrus blocks (entry consumed after flush)
  wal/.timefusion_meta/
    wal.lock  topics  cursor_snapshot.json
    dedup_certifications.json  dedup_dirty_bins.json
    delta_snapshots/<hash>.json.zst
  cache/                               ← Foyer L2
  hot_tier/                            ← Arrow IPC (disabled, Ch. 20)
```

For a project with its own storage the Delta path is
`s3://<bucket>/<prefix>/projects/<project_id>/<table>/` instead of
`.../default/<table>/` (`docs/MULTI_TABLE_ARCHITECTURE.md:24–33`).

### Consistency expectations

There is no multi-table transaction. Each Delta commit is atomic over one table;
a flush that spans several tables is several commits, and a crash between them
leaves the WAL holding the rest. That is exactly why the WAL cursor may only
advance per-topic and per-shard: the unit of "safely consumed" is one table's
commit, not the flush cycle.

Within one table, Delta's optimistic concurrency applies. TimeFusion serializes
in-process commits per physical table (`commit_locks`, Chapter 7) because
delta-kernel's OCC checker cannot evaluate `replace_where`'s timestamp predicate —
so a dedup commit racing a concurrent append would abort rather than merge. Two
*processes* committing to the same table is out of contract entirely, which is what
the WAL directory lock enforces.

### How to debug this flow

`timefusion_stats`, `component = 'cache'`: `hits`, `misses`, `range_hits`,
`range_misses`, `bytes_served`, `inner_bytes_read`, `range_bytes_read`,
`ttl_expirations`, `inner_gets`, `inner_puts` — separately for `main` and
`metadata`.

⚠️ A known measurement trap: the lock-free stats snapshot yields *default*
counters rather than blocking when contended (`src/storage.rs:387–388`), so a
busy cache can report zeros. A `hits = 0` reading on the busiest cache is a
statement about lock contention in the diagnostics path, not about the cache.

On disk: `du -sh <TIMEFUSION_DATA_DIR>/cache` versus the configured
`disk_size_bytes`. `ls -la <TIMEFUSION_DATA_DIR>/wal/.timefusion_meta/` shows
every sidecar with its mtime — an old `cursor_snapshot.json` mtime means flushes
are not completing.

Log lines: `"discarding unreadable <sidecar>"`, `"Discarding unreadable delta
snapshot"`, `"Failed to persist delta snapshot"`, `"Ignoring delta snapshot …:
version … / url … does not match"` (a format bump or a hash collision — both
benign).

**Key takeaways.** Delta on object storage is authoritative; the WAL is
authoritative until flushed; everything else is rebuildable. Every object-store
read goes through a two-tier Foyer cache split into separate data and metadata
instances so a wide scan cannot evict the footers every plan needs, with
scan-resistant admission, 1 MiB range alignment for sliding dashboard windows, and
size-based L1 bypass. Foyer runs on its own dedicated Tokio runtime because
`spawn` during runtime shutdown cancels inline and deadlocks its internal mutex.
Four best-effort local sidecars — Delta snapshots, certifications, dirty bins, WAL
cursors — accelerate boot and the read path; every one degrades to "empty" on
corruption, and the certification store is designed so a stale entry can only cost
a missed optimization, never grant a wrong skip.
# Part III — The flows

## 9. Startup: from `main` to serving

*After this chapter you will be able to follow process startup in order: the
allocator configuration that runs before `main`, the runtime build, the early
listener bind, the WAL directory lock, the four-way cursor reconciliation
decision, WAL replay, background-task start, and the listener handoff. You will
know what each `bootstrap.phase=` log line means and what a slow one implies.*

### Before `main`

Two things happen before the first line of `main` executes, both under
`--features profiling` on Linux:

**Listing 9.1 — src/main.rs:1–29**

```rust
// main.rs
#![recursion_limit = "512"]

// Production profiling (--features profiling, Linux): jemalloc as the global
// allocator with its heap profiler, plus a pprof CPU sampler (started in
// async_main). Deployed to attribute the prod OOM. See src/profiling.rs.
#[cfg(all(feature = "profiling", target_os = "linux"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

// jemalloc reads this symbol at startup — bakes the profiler config into the
// binary so no MALLOC_CONF env (host is read-only) is needed.
// prof_prefix points into the data-dir volume we can read off the host.
// Analyze: `jeprof --svg <binary> <prof_prefix>.*.heap`.
#[cfg(all(feature = "profiling", target_os = "linux"))]
#[unsafe(export_name = "malloc_conf")]
// `prof:true, prof_active:false`: sampling stays compiled in but off by default —
// re-arm at runtime via the `prof.active` mallctl (no rebuild) when heap
// attribution is next needed. lg_prof_sample:19 = ~512KiB sampling; keeping it
// off saves CPU/heap on this box, whose memory headroom gates compaction.
//
// `dirty_decay_ms:10000` (was 0): decay 0 madvise()s every freed page back to
// the kernel immediately, which under maintenance load (2026-08-18 perf trace)
// cost ~18% CPU in page-fault/TLB-shootdown churn from Arrow scan buffers being
// freed and re-faulted. 10s amortizes that while still returning idle memory;
// the 85% maintenance brake remains the OOM backstop. Don't drop this back to 0
// without re-measuring under maintenance load — it was set there in 2026-08-03
// to fight OOMs from since-fixed causes (unbounded scans, DedupExec).
pub static MALLOC_CONF: &[u8] = b"prof:true,prof_active:false,lg_prof_sample:19,lg_prof_interval:35,prof_prefix:/app/data/timefusion/profiles/jeprof,background_thread:true,dirty_decay_ms:10000,muzzy_decay_ms:10000\0";
```

Three techniques worth noting. `#[unsafe(export_name = "malloc_conf")]` exports a
symbol jemalloc looks up at *its* initialization — which is how the profiler is
configured on a host whose environment you cannot change. `prof:true,
prof_active:false` compiles sampling in but leaves it off, so heap attribution can
be armed at runtime through the `prof.active` mallctl without a rebuild. And
`dirty_decay_ms:10000` is a tuned value with a measurement attached: decay 0
`madvise()`s every freed page back to the kernel immediately, which under
maintenance load cost ~18% CPU in page-fault and TLB-shootdown churn as Arrow scan
buffers were freed and re-faulted.

### `main`: subcommands and the runtime

**Listing 9.2 — src/main.rs:58–115**

```rust
fn main() -> anyhow::Result<()> {
    dotenv().ok();
    // Before the runtime, so every worker thread/listener inherits the raised
    // limit. `bootstrap()` calls it too, for the e2e harness (skips main()).
    server::raise_file_limit();

    let subcommand = std::env::args().nth(1);
    if subcommand.as_deref() == Some("healthcheck") {
        return run_pgwire_healthcheck();
    }
    if subcommand.as_deref() == Some("encrypt-secret") {
        return config::run_cli();
    }
    // Replays a prod maintenance journal through the real scheduler on virtual
    // time — must stay config/bucket-free, that's what lets it answer
    // scheduler questions without a deploy.
    if subcommand.as_deref() == Some("sim") {
        return run_sim_cli();
    }

    // Maintenance CLIs get the maintenance-heavy budget shape (the server shape
    // strands cgroup memory in query/ingest slices a one-shot CLI never uses).
    // Must precede init_config, which snapshots the tree.
    //
    // `run-unit` excluded: it drives a coordinator unit whose pool comes from
    // `coordinator_share_bytes()`, which is a hard 0 under this profile
    // ("no coordinator runs under MaintenanceCli") — every invocation died at
    // `pool_size: 0.0 B`. Found 2026-08-20.
    //
    // SAFETY: no threads exist yet - we're before the Tokio runtime is built.
    if matches!(subcommand.as_deref(), Some("optimize" | "redrive-dml" | "migrate-columns")) {
        unsafe { std::env::set_var("TIMEFUSION_BUDGET_PROFILE", "maintenance-cli") };
    }

    let cfg = config::init_config().map_err(|e| anyhow::anyhow!("Failed to load config: {}", e))?;

    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().thread_stack_size(WORKER_STACK_BYTES).build()?;
    match subcommand.as_deref() {
        Some("redrive-dml") => rt.block_on(run_redrive_dml_cli(cfg)),
        Some("optimize") => rt.block_on(run_optimize_cli(cfg)),
        Some("migrate-columns") => rt.block_on(run_migrate_columns_cli(cfg)),
        Some("run-unit") => rt.block_on(run_unit_cli(cfg)),
        _ => {
            let result = rt.block_on(async_main(cfg));
            // Must END THE PROCESS here: dropping the runtime waits on
            // lingering blocking/detached threads, and that hang left a
            // zombie container blocking swarm's replacement (2026-08-06
            // pgwire outage). Everything durable is already on disk.
            match result {
                Ok(()) => std::process::exit(0),
                Err(e) => {
                    eprintln!("fatal: {e:#}");
                    std::process::exit(1)
                }
            }
        }
    }
}
```

Four things to notice.

**`raise_file_limit()` runs before the runtime**, so every worker thread and
listener inherits it. The implementation is one of the crate's two `unsafe`
blocks:

**Listing 9.3 — src/server/mod.rs:28–46**

```rust
/// Raise the open-file soft limit to the hard limit.
///
/// Best-effort raises the open-file soft limit to support mmap indexes and WAL.
pub fn raise_file_limit() {
    // SAFETY: both calls take a valid, fully-initialized `rlimit`, and neither
    // retains the pointer past the call.
    unsafe {
        let mut limit = std::mem::zeroed::<libc::rlimit>();
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) != 0 || limit.rlim_cur >= limit.rlim_max {
            return;
        }
        let (previous, target) = (limit.rlim_cur, limit.rlim_max);
        limit.rlim_cur = target;
        match libc::setrlimit(libc::RLIMIT_NOFILE, &limit) {
            0 => tracing::info!(previous, target, "raised the open-file soft limit to the hard limit"),
            _ => tracing::warn!(previous, target, error = %std::io::Error::last_os_error(), "could not raise the open-file soft limit"),
        }
    }
}
```

**The budget-profile `set_var` is the one sanctioned use of a process-global
mutation**, and its `SAFETY` comment says exactly why it is sound: no threads
exist yet. It must precede `init_config`, which snapshots the budget tree. The
excluded subcommand is documented with the incident: `run-unit` drives a
coordinator unit whose pool comes from `coordinator_share_bytes()`, which is a
hard 0 under `MaintenanceCli` — so every invocation died at `pool_size: 0.0 B`
until it was removed from the list.

**Workers get 32 MiB stacks**:

**Listing 9.4 — src/main.rs:45–56**

```rust
/// Stack size for every Tokio worker.
///
/// Tokio's default (2 MiB) overflowed planning a merge-on-read UPDATE on
/// 2026-08-16 (deep recursion over a wide schema + IN-list pushdown), which
/// aborts the whole process, not just the task — prod restart-looped on exit
/// 134. Plan depth follows schema width and predicate shape, not just pushdown
/// size, so this bounds the stack directly rather than the recursion. Reserved
/// lazily, so untouched pages cost address space, not RSS.
const WORKER_STACK_BYTES: usize = 32 * 1024 * 1024;
// Planning depth follows schema width and predicate shape, not just the
// pushdown cap, so keep real headroom over Tokio's 2 MiB default.
const _: () = assert!(WORKER_STACK_BYTES >= 8 * 2 * 1024 * 1024);
```

A stack overflow in a Tokio worker **aborts the process**, not the task, so this
was a restart loop rather than a failed query. Note the `const _: () = assert!(…)`
— a compile-time floor — and the test that pins the builder call:

**Listing 9.5 — src/main.rs:1070–1079**

```rust
    /// Workers must not run on Tokio's default stack.
    ///
    /// A stack overflow in a worker aborts the process, so this is a restart
    /// loop rather than a failed query — that is exactly how prod fell over on
    /// 2026-08-16 while planning a merge-on-read UPDATE. The builder call is one
    /// token to lose in a refactor and nothing else would notice, so pin it.
    #[test]
    fn workers_get_more_than_the_default_stack() {
        assert!(include_str!("main.rs").contains(".thread_stack_size(WORKER_STACK_BYTES)"), "the runtime must actually be built with WORKER_STACK_BYTES");
    }
```

Testing your own source text with `include_str!` is unusual, and justified here:
the builder call is one token that a refactor could drop, and nothing else in the
system would notice until the next deep plan.

**The process exits explicitly** rather than returning from `main`. Dropping the
Tokio runtime waits on lingering blocking and detached threads, and that hang left
a zombie container blocking Swarm's replacement during the 2026-08-06 pgwire
outage. Everything durable is already on disk by then, so `std::process::exit` is
the right call.

### `async_main`, phase by phase

#### Phase 0 — telemetry, then the budget log

**Listing 9.6 — src/main.rs:354–368**

```rust
async fn async_main(cfg: &'static AppConfig) -> anyhow::Result<()> {
    // Initialize OpenTelemetry with OTLP exporter
    observability::init_telemetry(&cfg.telemetry)?;
    // AFTER init_telemetry: config is built before the subscriber exists, so
    // logging the tree at derivation time is silently swallowed — which is why
    // prod could carry TIMEFUSION_MEMORY_LIMIT_GB=26 while actually budgeting
    // 120 GiB with nothing on the box revealing the gap (2026-07-31).
    config::log_derived_budget(&cfg.derived);
    support::init_from_env();

    // Start heap+CPU profiling (no-op unless --features profiling on Linux).
    // Early, so the profiles dir exists before jemalloc's first interval dump.
    timefusion::observability::start(cfg.core.timefusion_data_dir.clone());

    info!("Starting TimeFusion application");
```

Ordering is the content here. Config is constructed before the tracing subscriber
exists, so logging the derived budget at derivation time is silently swallowed —
which is how production ran with `TIMEFUSION_MEMORY_LIMIT_GB=26` set while
actually budgeting 120 GiB, with nothing on the box revealing the gap.

#### Phase 1 — bind the listener before doing anything slow

**Listing 9.7 — src/main.rs:370–388**

```rust
    // Create Arc<AppConfig> for passing to components
    let cfg_arc = Arc::new(cfg.clone());

    // Bind :5432 immediately, before the slow startup work (Database open,
    // WAL recovery — up to ~15 min when WAL has accumulated). Clients
    // connecting in this window get SQLSTATE 57P03 ("starting up") from
    // the early-bind responder instead of ECONNREFUSED, which is what
    // Hasql / pgjdbc / libpq expect during a backend restart and retry
    // on cleanly. See pgwire_early_bind for the responder.
    let pg_opts = ServerOptions::new().with_host("0.0.0.0".to_string()).with_port(cfg.core.pgwire_port);
    let pg_listener = datafusion_postgres::bind_listener(pg_opts.host(), *pg_opts.port(), *pg_opts.backlog()).await?;
    let early_shutdown = tokio_util::sync::CancellationToken::new();
    let early_task = tokio::spawn({
        let shutdown = early_shutdown.clone();
        async move {
            timefusion::server::run_until_ready(&pg_listener, shutdown).await;
            pg_listener
        }
    });
```

The distinction that makes this worth doing: `ECONNREFUSED` is a *hard* error to
libpq, pgjdbc and Hasql, while SQLSTATE `57P03` ("the database system is starting
up") is transient and they retry cleanly. Startup can take up to ~15 minutes with
an accumulated WAL, and during that window every reconnecting client would
otherwise fail hard.

The responder is a hand-written pgwire frame, because the real server does not
exist yet:

**Listing 9.8 — src/server/mod.rs:1524–1532**

```rust
/// Wire format: `Byte1('E') Int32(length) [Byte1(tag) String(value)]* Byte1(0)`
fn build_starting_up_response() -> Vec<u8> {
    let body: Vec<u8> = [(b'S', "FATAL"), (b'V', "FATAL"), (b'C', "57P03"), (b'M', "the database system is starting up")]
        .into_iter()
        .flat_map(|(tag, value)| [tag].into_iter().chain(value.bytes()).chain([0]))
        .chain([0])
        .collect();
    [b'E'].into_iter().chain(((body.len() + 4) as u32).to_be_bytes()).chain(body).collect()
}
```

The accept loop is bounded, and the bound has a specific shape:

**Listing 9.9 — src/server/mod.rs:1454–1490**

```rust
async fn accept_loop(listener: &TcpListener, shutdown: CancellationToken, max_handlers: usize) {
    let response: Arc<[u8]> = build_starting_up_response().into();
    let permits = Arc::new(tokio::sync::Semaphore::new(max_handlers));
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => return,
            res = listener.accept() => match res {
                Ok((sock, addr)) => {
                    // Over the cap (probable reconnect storm) we still send the canned
                    // 57P03 frame — dropping the socket unanswered would RST, which
                    // Hasql/libpq treat as ECONNREFUSED, the exact failure mode this
                    // responder exists to avoid — but skip the startup drain so the task
                    // is bounded by accept rate × write latency (~ms).
                    let permit = Arc::clone(&permits).try_acquire_owned().ok();
                    let (limit, drain) = match &permit {
                        Some(_) => (STARTUP_READ_TIMEOUT, true),
                        None => {
                            warn!("early-bind: at {max_handlers}-handler cap, fast-responding to {addr}");
                            (CAP_RESPONSE_TIMEOUT, false)
                        }
                    };
                    let resp = Arc::clone(&response);
                    tokio::spawn(async move {
                        let _permit = permit;
                        match tokio::time::timeout(limit, handle_one(sock, &resp, drain)).await {
                            Err(_) => debug!("early-bind: timeout waiting for startup from {addr}"),
                            Ok(Err(e)) => debug!("early-bind: short-circuit conn from {addr}: {e}"),
                            Ok(Ok(())) => {}
                        }
                    });
                }
                Err(e) => warn!("early-bind: accept failed: {e}"),
            },
        }
    }
}
```

The over-cap behaviour is the thoughtful part: it does *not* drop the socket,
because an unanswered socket RSTs and a RST is `ECONNREFUSED` to the very clients
this exists to protect. Instead it sends the canned frame and skips the startup
drain, so the task cost is bounded by accept rate × write latency.

The drain itself handles all three startup shapes:

**Listing 9.10 — src/server/mod.rs:1492–1522**

```rust
async fn handle_one(mut sock: TcpStream, response: &[u8], drain_startup: bool) -> io::Result<()> {
    // SSL/GSS negotiation precedes the real StartupMessage; both are 8 bytes
    // (length + magic). Drain whichever shape arrives, then send 57P03.
    // pg length fields include the 4-byte length itself. In the non-SSL branch
    // we've also consumed the 4-byte code → drain `len - 8`; in the SSL/GSS
    // branch we've consumed only the length of the *real* startup → drain
    // `real_len - 4`.
    if drain_startup {
        let len = sock.read_u32().await? as u64;
        let remaining = match sock.read_u32().await? {
            SSL_REQUEST_CODE | GSS_REQUEST_CODE => {
                sock.write_all(b"N").await?;
                (sock.read_u32().await? as u64).checked_sub(4)
            }
            _ => len.checked_sub(8),
        };
        drain_body(&mut sock, remaining).await?;
    }

    sock.write_all(response).await?;
    let _ = sock.shutdown().await;
    Ok(())
}

async fn drain_body(sock: &mut TcpStream, remaining: Option<u64>) -> io::Result<()> {
    let n = remaining.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "startup length below 8-byte header"))?;
    if n > MAX_STARTUP_BYTES {
        return Err(io::Error::new(io::ErrorKind::InvalidData, format!("startup body {n} exceeds {MAX_STARTUP_BYTES}-byte cap")));
    }
    tokio::io::copy(&mut sock.take(n), &mut tokio::io::sink()).await.map(drop)
}
```

`checked_sub` on the length arithmetic rather than a bare subtraction, and a
64 KiB cap on the drain — a malformed or hostile client can force at most that
much work.

#### Phase 2 — take the WAL lock

**Listing 9.11 — src/main.rs:390–401**

```rust
    // Take exclusive ownership of the WAL directory before ANY WAL access (boot
    // GC below, recovery, or writes). TimeFusion's WAL is single-writer with no
    // cross-process coordination; two live processes on the same dir fork it —
    // the newer one recovers only the prefix present at its start and orphans
    // the older's concurrent appends (silent loss on an overlapping redeploy).
    // Blocks until any previous process exits and releases the flock, serving
    // 57P03 via the early-bind responder above meanwhile. Held for the whole
    // process lifetime; released by the kernel even on SIGKILL. Under a
    // start-first deploy this self-resolves (readiness is a TCP check the early
    // responder already satisfies, so the orchestrator stops the old instance,
    // which releases the lock); stop-first shortens the handoff but isn't required.
    let _wal_dir_lock = timefusion::write::wal::WalDirLock::acquire(&cfg.core.wal_dir()).await?;
```

The ordering — early bind *then* lock — is what makes the wait tolerable: the
process serves `57P03` while blocked. Chapter 11 covers the lock and the takeover
protocol.

#### Phase 3 — open the database and build the write layer

**Listing 9.12 — src/main.rs:403–437**

```rust
    // Initialize database with explicit config
    let t_db = std::time::Instant::now();
    let mut db = Database::with_config(Arc::clone(&cfg_arc)).await?;
    info!("bootstrap.phase=database_init elapsed_ms={}", t_db.elapsed().as_millis());

    // ... elided: the BufferedWriteLayer config info! line

    // Create buffered layer with delta write callback
    let db_for_callback = db.clone();
    let delta_write_callback: timefusion::write::DeltaWriteCallback =
        Arc::new(move |project_id: String, table_name: String, batches: Vec<arrow::array::RecordBatch>, wal_watermark: timefusion::write::DeltaWatermark| {
            let db = db_for_callback.clone();
            Box::pin(async move {
                // insert_records_batch returns the URIs of files newly added by this
                // commit, derived from the post-write snapshot under the same write
                // lock — no second log scan. Watermark goes into Delta commit metadata
                // for crash-mid-flush recovery.
                // insert_records_batch warms the just-flushed files itself
                // (watermark-gated) — no warm here, or every flush would issue
                // the warm GETs twice.
                let added = db.insert_records_batch(&project_id, &table_name, batches, true, Some(&wal_watermark)).await?;
                // Unconditional on a successful commit — the flag means "this
                // (project, table) has Delta files", true as soon as the commit
                // lands even if file attribution came back empty. See the
                // coalesced callback in `bootstrap.rs` for the full rationale.
                db.mark_delta_has_files(&project_id, &table_name);
                Ok(added)
            })
        });
```

The callback closes the dependency cycle: the layer needs a way to commit to
Delta, and `Database` provides it, while `Database` will later be handed the layer
for its read leg. The `mark_delta_has_files` call is unconditional on a successful
commit — the flag means "this table has Delta files", which is true as soon as the
commit lands even if the added-file attribution came back empty.

#### Phase 4 — session, UDFs, and the replay registry

**Listing 9.13 — src/main.rs:439–452**

```rust
    // Register UDFs on the real SessionContext up front so its FunctionRegistry
    // doubles as the WAL-replay registry — no throwaway bootstrap context.
    // Table providers depend on buffered_layer and are registered after recovery.
    let mut session_context = Arc::new(db.clone()).create_session_context();
    db.setup_session_udfs(&mut session_context)?;
    let registry: Arc<timefusion::read::functions::FnRegistry> = Arc::new(session_context.state());

    // Tantivy sidecar indexes are always-on whenever at least one table has
    // `tantivy.indexed: true` fields in its YAML schema (or appears in the
    // optional `TIMEFUSION_TANTIVY_INDEXED_TABLES` override). The query layer
    // accelerates standard SQL predicates (`=`, `LIKE 'prefix%'`) via the
    // TantivyPredicateRewriter — callers don't need to know tantivy exists.
    // Pre-init WAL GC (gated + drained-flag consumption inside the helper).
    timefusion::write::wal::boot_wal_gc(&cfg.core.wal_dir());
```

The split is deliberate: **UDFs are registered now**, because WAL replay needs
them to plan a logged `UPDATE`'s SQL. **Table providers are registered after
recovery**, because they depend on the buffered layer.

#### Phase 5 — Tantivy wiring, and its failure mode

**Listing 9.14 — src/main.rs:459–476**

```rust
    let indexed_tables = cfg.tantivy.indexed_tables();
    let bucket = cfg.aws.aws_s3_bucket.as_deref().unwrap_or_default();
    let tantivy_svc_for_metrics = if indexed_tables.is_empty() {
        None
    } else if bucket.is_empty() {
        error!("Schema declares indexed columns but AWS_S3_BUCKET is unset — Tantivy disabled, queries will scan");
        None
    } else {
        let storage_uri = format!("s3://{bucket}/{}/tantivy", cfg.core.timefusion_table_prefix);
        let obj_store = db.create_object_store(&storage_uri, &cfg.aws.build_storage_options(None)).await?;
        let svc = Arc::new(timefusion::tantivy::search::TantivyIndexService::new(obj_store.clone(), Arc::new(cfg.tantivy.clone())));
        layer = layer.with_tantivy_indexer(svc.clone().callback());
        let search = Arc::new(timefusion::tantivy::search::TantivySearchService::new(obj_store, cfg.core.timefusion_data_dir.clone()));
        db = db.with_tantivy_search(search).with_tantivy_indexer(svc.clone());
        info!("Tantivy sidecar indexes active for tables: {:?}", indexed_tables);
        Some(svc)
    };
    let buffered_layer = Arc::new(layer);
```

A misconfiguration (schema declares indexed columns, bucket unset) is an
`error!` and a *degradation*, not a startup failure — queries scan instead of
prefiltering. That is the right call for a pure accelerator.

#### Phase 6 — the cursor reconciliation decision

This is the most consequential branch in startup, and it has four outcomes:

**Listing 9.15 — src/main.rs:496–539**

```rust
    // Fast-forward walrus cursors before WAL replay so we don't re-inject
    // entries Delta already has. Fast path: a `clean_shutdown=true` snapshot
    // on local disk lets us skip the ~6.5-min R2 scan entirely. Dirty/missing
    // snapshot still seeds positions, then falls through to the (env-tuned,
    // shorter) Delta verifier to catch commits made after the last snapshot.
    let wal_ref = buffered_layer.wal();
    let t_snap = std::time::Instant::now();
    let clean_snapshot = wal_ref.load_cursor_snapshot().is_some_and(|snap| {
        // age_secs is surfaced in the boot log only — not gating the skip. See
        // CursorSnapshot docs for the single-writer assumption and the `rm`
        // escape hatch. Backwards clock skew (NTP correction, snapshot ported
        // across hosts) is clamped to 0 by `saturating_sub`, not wrapped negative.
        let age_secs = timefusion::support::now_micros().saturating_sub(snap.written_at_micros) / 1_000_000;
        match wal_ref.restore_cursor_snapshot(&snap) {
            Ok(tables_advanced) => {
                info!(
                    "Cursor snapshot restored: {} table(s) seeded, {} table(s) advanced, clean_shutdown={}, age={}s",
                    snap.entries.len(),
                    tables_advanced,
                    snap.clean_shutdown,
                    age_secs
                );
                snap.clean_shutdown
            }
            Err(e) => {
                warn!("Cursor snapshot restore failed, falling back to Delta scan: {}", e);
                false
            }
        }
    });
    // A dirty/missing snapshot normally requires the expensive remote scan.
    // But when every durable cursor is already at its exact local WAL tail and
    // no interrupted-recovery marker exists, there is no payload whose cursor
    // Delta could advance. This covers the common deploy failure mode where
    // pre-deploy FLUSH drained successfully but the old container was killed
    // before it could write clean_shutdown=true.
    let local_wal_consumed = !clean_snapshot
        && match wal_ref.can_skip_delta_reconcile() {
            Ok(v) => v,
            Err(e) => {
                warn!("Local WAL tail/cursor proof failed, retaining Delta reconciliation: {e}");
                false
            }
        };
    let skip_delta_scan = clean_snapshot || local_wal_consumed;
```

The decision table:

| Snapshot | Cursors at tail | Outcome | Cost |
|---|---|---|---|
| clean | — | skip the Delta scan | ~0 |
| dirty/missing | yes, and no rewind marker | skip (`local_wal_consumed`) | one local check |
| dirty/missing | no | run the Delta cursor scan | ~6.5 min on R2 |
| restore failed | — | run the Delta cursor scan | ~6.5 min |

The `local_wal_consumed` path is worth its own note. It covers "pre-deploy `FLUSH`
drained successfully but the container was killed before writing
`clean_shutdown=true`" — a common deploy shape — and turns a 6.5-minute remote
scan into a local proof: if every durable cursor is already at its exact local WAL
tail and no interrupted-recovery marker exists, there is no payload whose cursor
Delta could advance.

Note also that `age_secs` is computed and logged but *not* used to gate the skip.
Age is not evidence: a clean snapshot from six hours ago is exactly as valid as
one from six seconds ago, provided this process is the only writer.

Both branches log:

**Listing 9.16 — src/main.rs:540–563**

```rust
    info!(
        "bootstrap.phase=cursor_snapshot skip_delta_scan={skip_delta_scan} clean_snapshot={clean_snapshot} local_wal_consumed={local_wal_consumed} elapsed_ms={}",
        t_snap.elapsed().as_millis()
    );
    if skip_delta_scan {
        info!(
            "Skipping Delta-derived cursor reconciliation ({})",
            if clean_snapshot { "cursor snapshot is clean" } else { "all local WAL cursors exactly match their tails" }
        );
    } else {
        info!(
            "Running Delta-derived cursor reconciliation (snapshot missing/dirty); scan_depth={}, concurrency={} \
             — set TIMEFUSION_DELTA_SCAN_DEPTH higher if a deployment lost more commits than that since its last clean state",
            cfg.buffer.delta_scan_depth(),
            cfg.buffer.delta_scan_concurrency()
        );
        let t_delta = std::time::Instant::now();
        match db.derive_wal_cursors_from_delta(wal_ref).await {
            Ok(0) => info!("Delta-derived cursor: no advancement needed"),
            Ok(n) => info!("Delta-derived cursor: advanced {} shard(s) past Delta watermark", n),
            Err(e) => warn!("Delta-derived cursor derivation failed (continuing with local cursor): {}", e),
        }
        info!("bootstrap.phase=delta_cursor_reconcile elapsed_ms={}", t_delta.elapsed().as_millis());
    }
```

Note the failure behaviour: a failed derivation *continues with the local
cursor*. That is the conservative direction — the local cursor is at or behind
Delta, so replay may re-inject entries Delta already has (duplicates, which dedup
collapses) rather than skipping entries it does not (loss).

#### Phase 7 — replay, then background tasks

**Listing 9.17 — src/main.rs:565–588**

```rust
    // Recover from WAL on startup
    let t_wal = std::time::Instant::now();
    let recovery_stats = buffered_layer.recover_from_wal().await?;
    info!("bootstrap.phase=wal_replay entries={} elapsed_ms={}", recovery_stats.entries_replayed, t_wal.elapsed().as_millis());

    // Start background tasks (flush and eviction)
    buffered_layer.start_background_tasks().await;
    info!("BufferedWriteLayer background tasks started");

    // Apply buffered layer to database
    db = db.with_buffered_layer(Arc::clone(&buffered_layer));
    db.start_dml_coalescer();

    // Start maintenance schedulers for regular optimize and vacuum
    db = db.start_maintenance_schedulers().await?;
    let db = Arc::new(db);
    db.setup_session_tables(&mut session_context)?;
    // Non-blocking: snapshot load + footer warm-up off the first query's path.
    db.preload_tables();
    // Config-gated background index maintenance: backfill uncovered files,
    // warm the local index cache with recent blobs.
    db.spawn_tantivy_backfill();
    db.spawn_tantivy_prefetch();
```

The order matters and each step depends on the previous: replay must finish before
background flushing starts (or a flush would race replay); the layer is published
into `Database`'s `OnceLock` only after replay; table providers are registered
after the layer exists; preloading is deliberately non-blocking so snapshot loads
and footer warm-up are off the first query's path.

The lag sampler starts *earlier* than you might expect, and the comment explains:

**Listing 9.18 — src/main.rs:487–493**

```rust
    // Starts here, not after WAL replay: replay is exactly the window where a
    // probe deadline gets missed, so the sampler has to already be running to
    // catch it. Its OWN token, not `early_shutdown` — that one is cancelled at
    // the early-bind handoff, which is precisely when the sampler starts being
    // interesting.
    let lag_shutdown = tokio_util::sync::CancellationToken::new();
    timefusion::observability::spawn_runtime_lag_sampler(lag_shutdown.clone());
```

#### Phase 8 — the listener handoff

**Listing 9.19 — src/main.rs:589–634**

```rust
    // Start PGWire server on the listener we pre-bound at the top of
    // async_main. First, hand control of that listener back from the
    // early-bind 57P03 responder.
    //
    // Ownership handoff: the listener was moved into early_task and is
    // returned as its final value, so `early_task.await?` hands back the
    // owned TcpListener — no Arc, no rebind, no ECONNREFUSED window.
    // handle_one tasks accepted just before shutdown may still be running;
    // they own only the accepted sockets and complete independently.
    info!("startup complete, transferring :5432 from early-bind 57P03 responder to real PGWire server");
    early_shutdown.cancel();
    let listener = early_task.await?;

    let auth_config = timefusion::server::AuthConfig::from_core(&cfg.core)?;

    // PGWire shutdown signal: when cancelled, the accept loop in
    // `serve_with_handlers` stops accepting new connections so the
    // BufferedWriteLayer flush isn't racing fresh inserts. Already-accepted
    // connections finish on their own spawned tasks.
    let pgwire_shutdown = tokio_util::sync::CancellationToken::new();
    // `mut` so the shutdown select! below can borrow it for early-failure
    // detection while leaving ownership for the drain phase.
    let mut pg_task = tokio::spawn({
        let shutdown = pgwire_shutdown.clone();
        let scan_metrics = Some(db.scan_metrics.clone());
        let db_for_pg = Arc::clone(&db);
        async move {
            if let Err(e) = timefusion::server::serve_with_listener(
                listener,
                Arc::new(session_context),
                &pg_opts,
                auth_config,
                scan_metrics,
                Some(db_for_pg),
                shutdown.cancelled_owned(),
            )
            .await
            {
                error!("PGWire server error: {}", e);
            }
        }
    });

    // PGWire is serving and WAL replay has returned; only now may recovery
    // relief files be indexed.
    db.spawn_deferred_tantivy_reindex(Arc::clone(&buffered_layer));
```

**Using the task's return value to transfer ownership** is the elegant part: the
`TcpListener` was *moved into* the early task, and the task returns it as its
final value, so `early_task.await?` hands back the owned listener. No `Arc`, no
re-bind, and therefore no window in which the port is unbound and connections get
`ECONNREFUSED`.

### Authentication

Configured at the handoff, and it fails closed:

**Listing 9.20 — src/server/mod.rs:193–221**

```rust
/// Auth configuration for PgWire server
#[derive(Debug, Clone, educe::Educe)]
#[educe(Default)]
pub struct AuthConfig {
    #[educe(Default = "postgres")]
    pub username: String,
    pub password: Option<String>,
}

impl AuthConfig {
    /// Construct from `CoreConfig`, requiring an explicit password unless
    /// `TIMEFUSION_ALLOW_INSECURE_AUTH=true` is set. We hard-fail the
    /// startup path rather than silently accept an empty password — the
    /// PG wire protocol's cleartext handler treats `None` as "accept any",
    /// which is an open ingest endpoint when bound to 0.0.0.0.
    pub fn from_core(core: &crate::config::CoreConfig) -> anyhow::Result<Self> {
        let allow_insecure = crate::config::is_insecure_auth_allowed();
        match (&core.pgwire_password, allow_insecure) {
            (Some(p), _) if !p.is_empty() => Ok(Self { username: core.pgwire_user.clone(), password: Some(p.clone()) }),
            (_, true) => {
                tracing::warn!(
                    "PGWIRE_PASSWORD unset and TIMEFUSION_ALLOW_INSECURE_AUTH=true — pgwire endpoint accepts any password. Acceptable for local dev ONLY; never in production."
                );
                Ok(Self { username: core.pgwire_user.clone(), password: None })
            }
            _ => anyhow::bail!("PGWIRE_PASSWORD is required (set TIMEFUSION_ALLOW_INSECURE_AUTH=true to opt into open auth for local dev)"),
        }
    }
}
```

The reasoning is exactly right for a fail-closed default: pgwire's cleartext
handler treats `None` as "accept any password", which on a `0.0.0.0` bind is an
open ingest endpoint. So an unset password is a *startup failure* unless the
operator explicitly opted in, and the opt-in logs a warning every boot.

### The shared bootstrap, and why tests use it

`server::bootstrap()` is a condensed version of everything above, used by the e2e
harness:

**Listing 9.21 — src/server/mod.rs:18–26, 48–56**

```rust
/// Fully initialized server state.
pub struct Bootstrapped {
    pub db: Arc<Database>,
    pub buffered_layer: Arc<BufferedWriteLayer>,
    /// Session context with providers and UDFs registered.
    pub session_ctx: Arc<SessionContext>,
    /// Cancellation signal for spawned tasks.
    pub shutdown: CancellationToken,
}
```

```rust
/// Initializes storage, recovery, background work, and query providers.
pub async fn bootstrap(cfg: Arc<AppConfig>) -> Result<Bootstrapped> {
    crate::support::init_from_env();
    raise_file_limit();

    let t_db = std::time::Instant::now();
    let mut db = Database::with_config(Arc::clone(&cfg)).await?;
    tracing::info!("bootstrap.phase=database_init elapsed_ms={}", t_db.elapsed().as_millis());
```

and it deliberately mirrors the cursor decision:

**Listing 9.22 — src/server/mod.rs:84–103**

```rust
    // Mirror main.rs: clean snapshot → skip the Delta cursor scan; dirty/missing
    // snapshot → derive cursors from Delta so WAL replay doesn't re-inject
    // entries Delta already has. Keeping this in the test-shared bootstrap
    // means e2e startup-time assertions exercise the same path as prod.
    // Per-phase timing is emitted at INFO so cold-start regressions surface
    // without needing trace-level enabled.
    let wal_ref = buffered_layer.wal();
    let t_snap = std::time::Instant::now();
    let clean_snapshot = wal_ref.load_cursor_snapshot().is_some_and(|snap| wal_ref.restore_cursor_snapshot(&snap).is_ok() && snap.clean_shutdown);
    let local_wal_consumed = !clean_snapshot && wal_ref.can_skip_delta_reconcile().unwrap_or(false);
    let skip_delta_scan = clean_snapshot || local_wal_consumed;
    tracing::info!(
        "bootstrap.phase=cursor_snapshot skip_delta_scan={skip_delta_scan} clean_snapshot={clean_snapshot} local_wal_consumed={local_wal_consumed} elapsed_ms={}",
        t_snap.elapsed().as_millis()
    );
    if !skip_delta_scan {
        let t_delta = std::time::Instant::now();
        let advanced = db.derive_wal_cursors_from_delta(wal_ref).await.unwrap_or(0);
        tracing::info!("bootstrap.phase=delta_cursor_reconcile shards_advanced={advanced} elapsed_ms={}", t_delta.elapsed().as_millis());
    }
```

⚠️ This is *duplicated* logic, not shared: `main.rs` and `server/mod.rs` each
implement the cursor decision, and the comment acknowledges it ("Mirror
main.rs"). The two can drift — `main.rs` logs richer diagnostics and handles
restore failure with a `warn!`, while `bootstrap` uses `unwrap_or(false)`. It is
recorded in Chapter 29.

### The health probe

Not startup exactly, but the thing that decides whether startup *counted*:

**Listing 9.23 — src/main.rs:117–137**

```rust
/// Docker liveness probe. The intentional early ErrorResponse with SQLSTATE
/// 57P03 is alive enough for Swarm to advance a start-first update; clients and
/// the deployment availability probe still treat it as unavailable. Any other
/// PGWire error remains unhealthy.
fn run_pgwire_healthcheck() -> anyhow::Result<()> {
    let port = std::env::var("TIMEFUSION_PGWIRE_PORT").or_else(|_| std::env::var("PGWIRE_PORT")).ok().and_then(|v| v.parse::<u16>().ok()).unwrap_or(5432);
    pgwire_ready_at(([127, 0, 0, 1], port).into())
}

/// Per-operation deadline for the readiness probe, so its worst case is 3x this
/// (connect + write + read) and must stay inside the Dockerfile's
/// `HEALTHCHECK --timeout` (pinned by `probe_worst_case_fits_the_docker_timeout`).
///
/// Was 750ms, and that was the actual killer (prod 2026-08-08): a probe measured
/// at 0.896s with no deploy in flight and the server perfectly healthy, three of
/// those in a row, and Swarm replaced the task — mid footer repair, discarding a
/// 40-minute rewrite. The handshake competes for the same runtime as ingest and
/// maintenance, so sub-second is not a budget a loaded database can hold. This
/// is a LIVENESS probe: the question is "is this still a database", not "is it
/// fast right now".
const PROBE_OP_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(1500);
```

"This is a LIVENESS probe: the question is *is this still a database*, not *is it
fast right now*" is the sentence to remember. A 750 ms budget on a loaded database
whose handshake shares a runtime with ingest and maintenance is not a health
check, it is a load test — and it killed a task mid-repair, discarding 40 minutes
of work.

The probe reports *which stage* was slow, which turned out to be the only usable
signal:

**Listing 9.24 — src/main.rs:139–205**

```rust
/// A probe verdict is useless without knowing WHICH stage was slow: a slow
/// `connect` is the accept loop (or the listen backlog) not getting scheduled,
/// a slow `auth` read is the handshake task losing its runtime slice behind
/// CPU-bound maintenance. On 2026-08-11 a probe timeout killed the task
/// mid-repair with CPU at 805%/4800% and 17.8 of 96 GiB — neither saturation
/// nor OOM, so the deadline was measuring something we could not see.
///
/// Printed on BOTH paths (Docker records healthcheck output either way), so a
/// `docker inspect` health log reads as a stage histogram over time rather than
/// a column of bare "unhealthy". Deliberately not widened — read the stages
/// first; widening the deadline destroys the only signal there is.
fn pgwire_ready_at(addr: std::net::SocketAddr) -> anyhow::Result<()> {
    use std::io::{Read, Write};

    let timeout = PROBE_OP_TIMEOUT;
    let t0 = std::time::Instant::now();
    let stage = |t: &mut std::time::Instant| {
        let d = t.elapsed();
        *t = std::time::Instant::now();
        d.as_millis()
    };
    let mut mark = t0;

    let connect = (|| {
        let s = std::net::TcpStream::connect_timeout(&addr, timeout)?;
        s.set_read_timeout(Some(timeout))?;
        s.set_write_timeout(Some(timeout))?;
        Ok::<_, std::io::Error>(s)
    })();
    let connect_ms = stage(&mut mark);
    let mut stream = connect.inspect_err(|e| println!("probe stage=connect ms={connect_ms} result=error err={e}"))?;

    let body = b"user\0timefusion_healthcheck\0database\0postgres\0\0";
    let mut startup = Vec::with_capacity(8 + body.len());
    startup.extend_from_slice(&((8 + body.len()) as u32).to_be_bytes());
    startup.extend_from_slice(&196_608u32.to_be_bytes()); // protocol 3.0
    startup.extend_from_slice(body);
    let wrote = stream.write_all(&startup);
    let write_ms = stage(&mut mark);
    wrote.inspect_err(|e| println!("probe stage=write connect_ms={connect_ms} ms={write_ms} result=error err={e}"))?;

    // Auth latency exposes server task starvation that connect latency misses.
    let mut tag = [0u8; 1];
    let read = stream.read_exact(&mut tag);
    let auth_ms = stage(&mut mark);
    let total_ms = t0.elapsed().as_millis();
    if let Err(e) = &read {
        println!("probe stage=auth connect_ms={connect_ms} write_ms={write_ms} ms={auth_ms} total_ms={total_ms} result=error err={e}");
    } else {
        println!("probe connect_ms={connect_ms} write_ms={write_ms} auth_ms={auth_ms} total_ms={total_ms} result=ok tag={}", tag[0] as char);
    }
    read?;
    if tag[0] == b'R' {
        return Ok(());
    }
    if tag[0] == b'E' {
        let mut length = [0u8; 4];
        stream.read_exact(&mut length)?;
        let payload_len = u32::from_be_bytes(length).saturating_sub(4) as usize;
        anyhow::ensure!(payload_len <= 64 * 1024, "PGWire ErrorResponse is unreasonably large");
        let mut payload = vec![0; payload_len];
        stream.read_exact(&mut payload)?;
        anyhow::ensure!(payload.windows(7).any(|field| field == b"C57P03\0"), "PGWire returned a non-startup error");
        return Ok(());
    }
    anyhow::bail!("PGWire returned unexpected response tag {:?}", tag[0] as char)
}
```

Three stages, each timed, printed on both the success and failure paths so
`docker inspect`'s health log reads as a stage histogram over time rather than a
column of bare "unhealthy". `connect` slow means the accept loop or listen backlog
is not getting scheduled; `auth` slow means the handshake task is losing its
runtime slice behind CPU-bound maintenance. And it deliberately accepts `57P03`
as healthy — that is what lets a start-first update advance while the replacement
is still replaying.

The budget is split across two files, and a test enforces the split:

**Listing 9.25 — src/main.rs:1046–1068**

```rust
    /// The probe and the Dockerfile are one budget split across two files, and
    /// the split is only correct in one direction: if Docker's `--timeout` is
    /// below the probe's own worst case, Docker kills the probe before it can
    /// report a verdict, and every slow-but-alive moment counts as a failure.
    /// That is the prod 2026-08-08 shape — a HEALTHY task replaced mid-repair.
    #[test]
    fn probe_worst_case_fits_the_docker_timeout() {
        let line = include_str!("../Dockerfile").lines().find(|l| l.starts_with("HEALTHCHECK ")).expect("Dockerfile must declare a HEALTHCHECK");
        let flag = |name: &str| -> u64 {
            line.split_whitespace()
                .find_map(|f| f.strip_prefix(name))
                .and_then(|v| v.strip_suffix('s').unwrap_or(v).parse().ok())
                .unwrap_or_else(|| panic!("HEALTHCHECK is missing {name}: {line}"))
        };
        let docker_timeout = std::time::Duration::from_secs(flag("--timeout="));
        // connect + write + read, each bounded by PROBE_OP_TIMEOUT.
        let worst_case = super::PROBE_OP_TIMEOUT * 3;
        assert!(
            worst_case <= docker_timeout,
            "the probe can take up to {worst_case:?} but Docker kills it at {docker_timeout:?} — raise --timeout or lower PROBE_OP_TIMEOUT"
        );
        assert!(flag("--retries=") >= 5, "3 consecutive misses inside 15s is 'busy', not 'dead' (prod 2026-08-08)");
    }
```

A unit test that parses the `Dockerfile` to check that a constant in Rust and a
flag in a container manifest remain consistent. This is a pattern worth stealing
whenever a budget is split across files in different languages.

### Reading the boot log

A healthy fast boot:

```
raised the open-file soft limit to the hard limit
self-sizing budget tree derived at startup profile=Server effective_limit_gb=120 …
Starting TimeFusion application
bootstrap.phase=database_init elapsed_ms=1200
bootstrap.phase=buffered_write_layer_init elapsed_ms=80
Tantivy sidecar indexes active for tables: ["otel_logs_and_spans", "otel_metrics"]
Cursor snapshot restored: 340 table(s) seeded, 12 table(s) advanced, clean_shutdown=true, age=41s
bootstrap.phase=cursor_snapshot skip_delta_scan=true clean_snapshot=true local_wal_consumed=false elapsed_ms=90
Skipping Delta-derived cursor reconciliation (cursor snapshot is clean)
bootstrap.phase=wal_replay entries=0 elapsed_ms=300
BufferedWriteLayer background tasks started
startup complete, transferring :5432 from early-bind 57P03 responder to real PGWire server
```

What each slow phase means:

| Slow phase | Cause | Fix |
|---|---|---|
| before `database_init` | waiting on the WAL lock | look for `"WAL dir … is locked"`; find the predecessor |
| `database_init` | opening Delta tables from S3 | check the snapshot cache dir; check S3 latency |
| `cursor_snapshot` | reading/restoring the snapshot | rare; a huge topic count |
| `delta_cursor_reconcile` | the ~6.5-min R2 scan | the previous life did not shut down cleanly |
| `wal_replay` | a real backlog | run `FLUSH` before planned restarts |

The three log lines that should worry you:

- `"Cursor snapshot restore failed, falling back to Delta scan"` — a corrupt or
  incompatible snapshot; boot will be minutes slower.
- `"Schema declares indexed columns but AWS_S3_BUCKET is unset — Tantivy
  disabled, queries will scan"` — a misconfiguration that degrades silently
  afterwards.
- `"PGWIRE_PASSWORD unset and TIMEFUSION_ALLOW_INSECURE_AUTH=true"` — an open
  endpoint.

**Key takeaways.** Startup binds the port *first* so clients get a retryable
`57P03` instead of `ECONNREFUSED`, then takes a process-lifetime `flock` on the
WAL directory, then decides — via four cases — whether it can skip a ~6.5-minute
Delta cursor scan, then replays only the WAL tail, then starts background work,
then hands the *owned* listener from the early responder to the real server with
no unbound window. The health probe is a liveness check with a 1.5-second
per-stage budget, reports which stage was slow, treats `57P03` as alive, and has
its budget pinned against the `Dockerfile` by a unit test.
## 10. The write path: from `INSERT` to acknowledged

*After this chapter you will be able to follow the payments span from the
`INSERT` statement to the moment the client is acknowledged: the `DataSink`, the
per-project row partitioning, the Variant conversion, the event-time bound, the
memory reservation with backpressure, the WAL pin lifecycle, and the MemBuffer
apply. You will also know every way this path can refuse a write and what the
client sees for each.*

### The flow

```
INSERT INTO otel_logs_and_spans (...) VALUES (...)
  │
  ├─ pgwire → DataFusion planning
  ├─ VariantInsertRewriter wraps JSON literals in json_to_variant()   [Ch. 14]
  ├─ plan cache may take the fast path (fast_insert_batch)            [Ch. 14]
  │
  ├─ TableProvider::insert_into → DataSinkExec
  │    └─ DataSink::write_all  (ProjectRoutingTable)
  │         ├─ normalize_timestamp_tz
  │         ├─ convert_variant_columns  (Utf8View → Struct{metadata,value})
  │         └─ partition_batch_by_project  → one sub-batch per tenant
  │
  └─ per project, concurrently:
       Database::insert_records_batch
         └─ BufferedWriteLayer::insert_bounded
              ├─ admit_write()            — the deploy fence
              ├─ WAL hard-backpressure check
              ├─ compact_batch            — drop inherited buffers
              ├─ bound_event_time         — reject absurd timestamps
              ├─ reserve_with_backpressure — CAS + relief flush
              └─ with_wal_pin
                   ├─ wal.append_batch    ← DURABILITY BOUNDARY (fsync ≤ 200ms)
                   └─ mem_buffer.insert_with_hold
              └─ release_reservation
  ← acknowledged
```

The durability boundary is `wal.append_batch`. Everything before it can reject
the write and nothing is lost (the client must retry); everything after it is
recoverable by replay.

### The sink

`ProjectRoutingTable` is both the `TableProvider` and its own `DataSink`.
`write_all` is where a heterogeneous batch becomes per-tenant writes:

**Listing 10.1 — src/database/mod.rs:8245–8291**

```rust
    async fn write_all(&self, mut data: SendableRecordBatchStream, _context: &Arc<TaskContext>) -> DFResult<u64> {
        let span = tracing::Span::current();
        let mut total_row_count = 0;
        let mut project_batches: HashMap<String, Vec<RecordBatch>> = HashMap::new();
        let target_schema = self.real_schema();
        // Collect batches, converting Utf8/Utf8View columns into Variant structs where the
        // target schema expects Variant (INSERT path: schema() presented Variant cols as
        // Utf8View, so inbound batches may carry strings), then partition each batch row-wise
        // by project_id — a single batch may carry rows for many projects, each of which
        // lands in its own Delta table.
        while let Some(batch) = data.next().await.transpose()? {
            let batch_rows = batch.num_rows();
            debug!("write_all: received batch with {} rows", batch_rows);
            total_row_count += batch_rows;
            let batch = normalize_timestamp_tz(batch)?;
            let converted = convert_variant_columns(batch, &target_schema)?;
            for (project_id, sub) in partition_batch_by_project(converted, &self.default_project)? {
                project_batches.entry(project_id).or_default().push(sub);
            }
        }

        span.record("rows.count", total_row_count);
        span.record("projects.count", project_batches.len());

        if project_batches.is_empty() {
            return Ok(0);
        }

        // Distinct projects → distinct Delta tables/WAL shards: insert them concurrently,
        // with no cross-project lock contention.
        let writes = project_batches.into_iter().map(|(project_id, batches)| {
            let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
            debug!("write_all: inserting {} batches with {} total rows for project {}", batches.len(), row_count, project_id);
            let insert_span = tracing::trace_span!(parent: &span, "delta_table.insert", project_id = %project_id, rows = row_count);
            async move {
                self.database
                    .insert_records_batch(&project_id, &self.table_name, batches, self.skip_queue, None)
                    .instrument(insert_span)
                    .await
                    .map_err(|e| DataFusionError::Execution(format!("Insert error for project {} table {}: {}", project_id, self.table_name, e)))
            }
        });
        futures::future::try_join_all(writes).await?;

        debug!("write_all: completed insertion of {} total rows", total_row_count);
        Ok(total_row_count as u64)
    }
```

Three transformations happen per batch, in order, and the order matters:

1. **`normalize_timestamp_tz`** — a client may send a naive timestamp; the schema
   declares `Timestamp(Microsecond, Some("UTC"))`, and a mismatched timezone
   annotation fails the Delta write.
2. **`convert_variant_columns`** — undoes the schema lie from Chapter 6, turning
   the `Utf8View` JSON strings the planner accepted into
   `Struct{metadata: Binary, value: Binary}`.
3. **`partition_batch_by_project`** — a *row-wise* split. One `INSERT` statement
   may carry rows for several tenants, and each goes to a different Delta table
   and a different WAL topic.

The per-project writes then run concurrently under `try_join_all`, because
distinct projects touch disjoint tables and WAL shards. One failure fails the
statement; there is no partial-success reporting at this level.

There is also a fast path that skips `DataSinkExec` and `ValuesExec` entirely,
used by the plan cache when it has already materialized the `VALUES` into a batch:

**Listing 10.2 — src/database/mod.rs:7119–7148**

```rust
    /// pgwire-INSERT fast path. Skips `DataSinkExec` + `ValuesExec` entirely:
    /// caller (the plan_cache hook) has already materialized the incoming
    /// VALUES into a RecordBatch from substituted literals, so we just run
    /// the per-batch fixups (`convert_variant_columns`, project-id routing,
    /// `normalize_timestamp_tz` is run inside `insert_records_batch`) and
    /// hand straight to `insert_records_batch` → `BufferedWriteLayer.insert`.
    /// Returns the inserted row count.
    pub async fn fast_insert_batch(&self, batch: RecordBatch) -> DFResult<u64> {
        let total_rows = batch.num_rows() as u64;
        if total_rows == 0 {
            return Ok(0);
        }
        let target_schema = self.real_schema();
        // Partition row-wise: one INSERT may carry rows for many projects, each
        // landing in its own Delta table. Distinct projects write concurrently.
        let writes = partition_batch_by_project(batch, &self.default_project)?
            .into_iter()
            .map(|(project_id, sub)| {
                let converted = convert_variant_columns(sub, &target_schema)?;
                Ok(async move {
                    self.database
                        .insert_records_batch(&project_id, &self.table_name, vec![converted], self.skip_queue, None)
                        .await
                        .map_err(|e| DataFusionError::Execution(format!("fast_insert_batch for project {} table {}: {}", project_id, self.table_name, e)))
                })
            })
            .collect::<DFResult<Vec<_>>>()?;
        futures::future::try_join_all(writes).await?;
        Ok(total_rows)
    }
```

### `BufferedWriteLayer`, the type

Before the insert function, the state it operates on. This struct is large, and
like `Database` it repays a slow read — nearly every field's comment is a
production finding.

**Listing 10.3 — src/write/mod.rs:466–530 (the first third)**

```rust
#[derive(derive_more::Debug)]
#[debug("BufferedWriteLayer {{ has_callback: {} }}", delta_write_callback.is_some())]
pub struct BufferedWriteLayer {
    config: Arc<AppConfig>,
    wal: Arc<WalManager>,
    mem_buffer: Arc<MemBuffer>,
    /// Local hot tier: drained buckets are demoted here post-commit and served
    /// as the scan's third leg. Always constructed — with
    /// `TIMEFUSION_HOT_TIER_RETENTION_HOURS=0` it demotes nothing and serves
    /// nothing, but still sweeps its own directory.
    hot_tier: Arc<crate::hot_tier::HotTier>,
    /// Demotion slots — see [`DEMOTE_CONCURRENCY`].
    // ... elided: 6 lines on why one slot serialized every table
    demote_permit: Arc<tokio::sync::Semaphore>,
    /// Bytes of drained batches queued for demotion, and the sets dropped for
    /// exceeding `demote_queue_limit`.
    ///
    /// PROD 2026-08-14: the bound was a COUNT (`MAX_PENDING_DEMOTIONS = 2`) and
    /// over it the WHOLE drained set was discarded — 84 buckets flushed, 9
    /// demoted, so **89% of the tier's writes were thrown away** and every later
    /// recent-window query paid R2 for them. The busiest project lost the most,
    /// because it produces the most buckets and so always lost the race.
    /// A count bounds nothing when one set can be a thousand times another; the
    /// stated concern was always heap, so the bound is now bytes. Same lesson as
    /// the plan cache (12ff764): weigh the entries.
    demote_queued_bytes: Arc<AtomicU64>,
    demote_skipped: Arc<AtomicU64>,
    demote_queue_limit: u64,
    shutdown: CancellationToken,
    /// Write-admission barrier for graceful handoff. Closing it before server
    /// connection drain prevents already-accepted PGWire sockets from appending
    /// after the shutdown flush/snapshot (the accept loop itself does not join
    /// its spawned connection tasks).
    accepting_writes: std::sync::atomic::AtomicBool,
    active_writes: AtomicU64,
    writes_drained: Notify,
    /// Invalidates leased pre-deploy write fences. Shutdown increments this
    /// again, so an old lease timer can never reopen admission after SIGTERM.
    handoff_generation: AtomicU64,
    /// True only after HANDOFF fenced admission, quiesced admitted writers,
    /// and flushed every WAL-backed hold. A start-first replacement may then
    /// ask this process to relinquish the single-writer WAL lock.
    deploy_handoff_ready: std::sync::atomic::AtomicBool,
    delta_write_callback: Option<DeltaWriteCallback>,
    /// Set alongside `delta_write_callback`; used instead of it when
    /// `TIMEFUSION_FLUSH_COALESCE_COMMITS` is on (see `flush_groups_coalesced`).
    coalesced_write_callback: Option<DeltaCoalescedWriteCallback>,
    tantivy_index_callback: Option<TantivyIndexCallback>,
    background_tasks: Mutex<Vec<JoinHandle<()>>>,
    flush_lock: Mutex<()>,
    // Single-flights insert-path backpressure relief: only the writer that wins
    // this try_lock drives a relief flush; the rest wait for it to free RAM.
    // Without it, every blocked writer ran its own flush cycle (the ~20s p99
    // herd). Distinct from `flush_lock` so relief never blocks behind a routine
    // background flush already holding `flush_lock`.
    relief_lock: Mutex<()>,
    reserved_bytes: AtomicUsize,  // Memory reserved for in-flight writes
    pressure_notify: Arc<Notify>, // Wakes flush task when pressure threshold crossed
```

"A count bounds nothing when one set can be a thousand times another" — the
demote-queue comment — is the same lesson as the plan cache's, and it is one of
the recurring themes of this codebase: **bound the resource you actually care
about, not a proxy for it.**

The most conceptually dense fields are the four WAL-hold maps, which together
implement the invariant "the WAL read cursor may not advance past any entry whose
rows are not yet in Delta":

**Listing 10.4 — src/write/mod.rs:595–622**

```rust
    /// WAL read-cursor holds for inserts whose entry is appended but whose
    /// MemBuffer bucket hasn't recorded its hold yet (the append→record
    /// window). Registered under the shard append lock BEFORE the entry
    /// exists — see `WalManager::append_batch` for the ordering argument.
    /// Keyed (project, table) → token → (shard, pre-append position).
    pending_wal_holds: dashmap::DashMap<(String, String), std::collections::HashMap<u64, (usize, walrus_rust::WalPosition)>>,
    /// Holds for buckets taken out of MemBuffer for an in-flight Delta
    /// commit: while airborne they're invisible to `MemBuffer::wal_holds`,
    /// but until the commit lands their WAL entries must still pin the
    /// cursor. Keyed (project, table) → token → per-shard holds.
    inflight_flush_holds: dashmap::DashMap<(String, String), std::collections::HashMap<u64, ShardHolds>>,
    /// Holds for buckets that could not be restored after a failed commit
    /// (evicted / incompatible schema): the rows exist only in the WAL, so
    /// the cursor must stay pinned until restart replays them. Kept apart
    /// from `inflight_flush_holds` so `await_inflight_flushes` (the DML
    /// Delta-leg ordering) doesn't treat a process-lifetime orphan as an
    /// airborne commit and stall every DML for the full watchdog budget.
    /// Per-topic orphaned cursor holds + the orphan's GC-floor pin
    /// (oldest WAL-append micros; i64::MAX = none). Process-lifetime — the
    /// rows exist only in the WAL until a restart replays them. Surfaced in
    /// `timefusion_stats` (orphaned_topics / orphan_pin_age) so an operator
    /// knows a restart is due before the pinned WAL fills the disk.
    orphaned_wal_holds: dashmap::DashMap<(String, String), (ShardHolds, i64)>,
    /// WAL GC floor legs for taken buckets while their commit is airborne:
    /// token → `first_wal_pin_micros`. Keeps `gc_wal_files` from deleting
    /// files their entries live in.
    inflight_wal_pins: dashmap::DashMap<u64, i64>,
    wal_hold_seq: AtomicU64,
```

Four maps, one invariant, four *lifecycle stages* of the same hold: pending
(append started, MemBuffer not yet updated), in MemBuffer (the normal state,
tracked by the bucket itself), airborne (bucket taken out for a commit), and
orphaned (commit failed and the rows could not be restored). Each stage needs its
own map because the queries differ — `await_inflight_flushes` must distinguish
"a commit is in flight" from "a process-lifetime orphan", or every DML would stall
for the full watchdog budget waiting on something that will never complete.

Finally the watermark that makes the read path's skip-Delta decision sound:

**Listing 10.5 — src/write/mod.rs:579–594**

```rust
    /// Per-(project, table) max row timestamp ever handed to a Delta commit
    /// this process lifetime, floored at `boot_micros`. Delta cannot hold
    /// rows newer than this, so a query whose lower time bound is above it
    /// can skip the Delta scan — the steady-state recent-window fast path.
    /// Unlike the old `query_min >= mem_buffer_oldest` heuristic this stays
    /// sound when Delta holds rows *inside* MemBuffer's range: force-flushed
    /// open buckets and out-of-order drains after a failed flush (2026-06-11
    /// visibility gap). Raised before the commit so a query can't race in
    /// between commit-visible and watermark-raise; a failed commit leaves it
    /// conservatively high.
    delta_flushed_watermark: dashmap::DashMap<crate::write::mem_buffer::TableKey, i64>,
    /// Recovery-time floor for the watermark: anything committed by earlier
    /// process lifetimes has row timestamps at/below roughly this (event
    /// timestamps drive bucketing; far-future-skewed pre-boot rows are the
    /// accepted residual exposure, same as the old heuristic).
    boot_micros: i64,
```

"Raised before the commit… a failed commit leaves it conservatively high" is the
correct direction for a watermark that gates a *skip*: too high means the read
path scans Delta unnecessarily (slow), too low means it skips Delta when Delta
holds relevant rows (wrong).

### The write-admission fence

Every insert begins by taking an admission guard. It is RAII, and the
close-versus-increment race is handled explicitly:

**Listing 10.6 — src/write/mod.rs:654–700**

```rust
struct WriteAdmission<'a> {
    layer: &'a BufferedWriteLayer,
}

impl Drop for WriteAdmission<'_> {
    fn drop(&mut self) {
        if self.layer.active_writes.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.layer.writes_drained.notify_waiters();
        }
    }
}

impl BufferedWriteLayer {
    fn admit_write(&self) -> Result<WriteAdmission<'_>, &'static str> {
        if !self.accepting_writes.load(Ordering::Acquire) {
            return Err("TimeFusion is draining for deployment; retry on the replacement");
        }
        self.active_writes.fetch_add(1, Ordering::AcqRel);
        // Close-vs-increment race: the shutdown thread stores false before it
        // waits for this counter. Recheck after increment so every admitted
        // writer is either visible to that wait or rejected here.
        if !self.accepting_writes.load(Ordering::Acquire) {
            if self.active_writes.fetch_sub(1, Ordering::AcqRel) == 1 {
                self.writes_drained.notify_waiters();
            }
            return Err("TimeFusion is draining for deployment; retry on the replacement");
        }
        Ok(WriteAdmission { layer: self })
    }

    /// Close write admission before network-server drain. Idempotent. Reads on
    /// existing PGWire sessions may finish; new INSERT/UPDATE/DELETE calls fail
    /// retryably instead of racing the final WAL cursor snapshot.
    pub fn stop_accepting_writes(&self) {
        self.handoff_generation.fetch_add(1, Ordering::AcqRel);
        self.deploy_handoff_ready.store(false, Ordering::Release);
        self.accepting_writes.store(false, Ordering::Release);
    }

    /// Whether a start-first replacement may safely trigger this process's
    /// graceful exit while reads are still being served.
    pub fn is_deploy_handoff_ready(&self) -> bool {
        self.deploy_handoff_ready.load(Ordering::Acquire)
            && !self.accepting_writes.load(Ordering::Acquire)
            && self.active_writes.load(Ordering::Acquire) == 0
            && self.is_drained()
    }
```

The pattern is check-increment-recheck: the shutdown path
stores `false` and *then* waits on `active_writes`, so a writer that incremented
before the store must be visible to that wait, and one that incremented after must
reject itself. Without the recheck a writer could slip between the store and the
wait.

### `insert_bounded`, in order

This is the function. We walk it in the order it executes.

**Listing 10.7 — src/write/mod.rs:1149–1206 (admission through event-time bound)**

```rust
    pub async fn insert(&self, project_id: &str, table_name: &str, batches: Vec<RecordBatch>) -> anyhow::Result<()> {
        self.insert_bounded(project_id, table_name, batches, true).await
    }

    /// `bound: false` skips the event-time admission bound — for DML
    /// re-appends only: tombstones/updates keep the original row's timestamp,
    /// which may legitimately lie outside the bound (2026-08-04: the bound ate
    /// the DELETE tombstones aimed at the `date=2238-12-31` garbage itself).
    #[instrument(skip(self, batches), fields(project_id, table_name, batch_count))]
    pub async fn insert_bounded(&self, project_id: &str, table_name: &str, batches: Vec<RecordBatch>, bound: bool) -> anyhow::Result<()> {
        let _admission = self.admit_write().map_err(anyhow::Error::msg)?;
        // Fail fast while the WAL backlog is over its HARD cap (see
        // `wal_hard_backpressure`): acking more work would only deepen an
        // unbounded, hours-to-recover backlog. The producer's DLQ replays.
        if self.wal_hard_backpressure.load(Ordering::Relaxed) {
            crate::observability::record_ingest_error(project_id, table_name);
            anyhow::bail!(
                "WAL backlog exceeds hard limit ({}GB); insert rejected under backpressure — retry later",
                self.config.buffer.timefusion_wal_hard_limit_gb
            );
        }
        // Memory pressure no longer triggers a synchronous flush_all_now in the
        // insert path — that violated the "inserts return fast, Delta happens on
        // a routine" invariant by stalling pgwire threads on S3 commits
        // (and worse, holding the global flush_lock so one slow tenant froze
        // ingest for everyone). The safety nets are: (a) `try_reserve_memory`
        // rejects inserts past the 120% hard limit, surfacing backpressure to
        // the client; (b) the post-CAS `pressure_notify.notify_one()` already
        // wakes the background flush task when reservations cross the
        // configured pressure threshold.
        if self.is_memory_pressure() {
            warn!(
                "Memory pressure (used={}MB / max={}MB) — notifying background flush; insert path will not block on Delta",
                self.effective_memory_bytes() / (1024 * 1024),
                self.config.buffer.max_memory_mb()
            );
            self.pressure_notify.notify_one();
        }

        let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();

        // Compact before reservation AND WAL serialization: scan-backed DML
        // batches and IPC-decoded inputs otherwise reserve at phantom size
        // and serialize entire inherited buffers into the WAL (2026-06-11:
        // fat UPDATE entries re-inflated the buffer to 772GB on every
        // replay). MemBuffer's insert re-runs this as a cheap no-op.
        let batches: Vec<RecordBatch> = batches.into_iter().map(crate::write::mem_buffer::compact_batch).collect();

        // Drop rows with absurd event timestamps before anything is reserved
        // or made durable: `date` is derived from `timestamp`, so a client
        // unit error (seconds where micros belong) mints garbage partitions
        // like `date=2238-12-31` that no query or retention pass ever visits
        // (prod 2026-08-03). Bounding at flush instead would wedge data that
        // was already acked into the WAL.
        let batches = if bound { bound_event_time(project_id, table_name, batches) } else { batches };
        if batches.is_empty() {
            return Ok(());
        }
```

Four things happen before any resource is committed, and each is placed where it
is for a reason stated in the comment:

**`compact_batch` runs before reservation and before WAL serialization.** An
Arrow `RecordBatch` produced by slicing a scan result *inherits the whole
underlying buffer*. Without compaction, a small DML batch reserves at phantom
size and serializes the entire inherited buffer into the WAL — which on
2026-06-11 re-inflated the buffer to 772 GB on every replay.

**`bound_event_time` runs before durability.** `date` is derived from
`timestamp`, so a client sending seconds where microseconds belong mints
partitions like `date=2238-12-31` that no query and no retention pass ever
visits. Bounding at flush time instead would mean the garbage was already
acknowledged and in the WAL.

**Listing 10.8 — src/write/mod.rs:96–140**

```rust
/// Event timestamps below this (2000-01-01T00:00:00Z) or more than
/// [`EVENT_TIME_MAX_FUTURE_MICROS`] past ingest time are client unit errors
/// (seconds/millis where micros belong), not data.
const EVENT_TIME_MIN_MICROS: i64 = 946_684_800_000_000;
const EVENT_TIME_MAX_FUTURE_MICROS: i64 = 48 * 3600 * 1_000_000;

/// Admission-time sanity bound on the table's event-time column: rows outside
/// [2000-01-01, now+48h] are dropped with a metric + warn. Null timestamps and
/// non-microsecond columns pass through untouched. See the call site in
/// [`BufferedWriteLayer::insert`] for why this must run before the WAL append.
fn bound_event_time(project_id: &str, table_name: &str, batches: Vec<RecordBatch>) -> Vec<RecordBatch> {
    use arrow::array::TimestampMicrosecondArray;
    let time_col = crate::dml::table_time_column(table_name);
    let hi = crate::support::now_micros() + EVENT_TIME_MAX_FUTURE_MICROS;
    let mut dropped = 0u64;
    let bounded: Vec<RecordBatch> = batches
        .into_iter()
        .filter_map(|batch| {
            let Some(ts) = batch.column_by_name(time_col).and_then(|c| c.as_any().downcast_ref::<TimestampMicrosecondArray>()) else {
                return Some(batch);
            };
            if let (Some(min), Some(max)) = (arrow::compute::min(ts), arrow::compute::max(ts))
                && min >= EVENT_TIME_MIN_MICROS
                && max <= hi
            {
                return Some(batch);
            }
            let mask: arrow::array::BooleanArray = ts.iter().map(|v| Some(v.is_none_or(|v| (EVENT_TIME_MIN_MICROS..=hi).contains(&v)))).collect();
            dropped += mask.iter().filter(|keep| *keep == Some(false)).count() as u64;
            match arrow::compute::filter_record_batch(&batch, &mask) {
                Ok(kept) if kept.num_rows() == 0 => None,
                Ok(kept) => Some(kept),
                Err(e) => {
                    error!("event-time bound filter failed, admitting batch unfiltered: {e}");
                    Some(batch)
                }
            }
        })
        .collect();
    if dropped > 0 {
        warn!("dropped {dropped} rows with event timestamps outside [2000-01-01, now+48h] for {project_id}/{table_name} — client timestamp unit error?");
        crate::observability::record_event_time_bounded(project_id, table_name, dropped);
    }
    bounded
}
```

Note the fast path: `arrow::compute::min`/`max` over the timestamp column first,
and only if the batch is out of range is a boolean mask built and applied. For the
overwhelmingly common in-range batch this is two vectorized aggregate passes and
no allocation. Also note the failure arm: if the filter itself fails, the batch is
admitted *unfiltered* with an `error!` — availability over strictness, consistent
with the rest of the ingest path.

⚠️ The `bound: false` variant exists for DML re-appends, and the comment records
the incident: on 2026-08-04 the bound ate the `DELETE` tombstones aimed at the
`date=2238-12-31` garbage that the bound itself had been added to prevent.
Bounds that reject the cleanup for the mess they exist to prevent are a genuinely
easy trap.

### Memory reservation: CAS, then backpressure

Reservation is the mechanism that keeps concurrent inserts from collectively
overshooting the ingest budget. It is a CAS loop, and every line of it has been
tuned:

**Listing 10.9 — src/write/mod.rs:930–991**

```rust
    /// Try to reserve memory atomically before a write.
    /// Returns estimated batch size on success, or error if hard limit exceeded.
    /// Uses exponential backoff to reduce CPU thrashing under contention.
    async fn try_reserve_memory(&self, batches: &[RecordBatch]) -> anyhow::Result<usize> {
        let estimated_size = estimate_reservation(batches);

        let max_bytes = self.max_memory_bytes();
        let hard_limit = hard_limit(max_bytes);
        let threshold = self.config.buffer.pressure_flush_pct();
        // Loop-invariant: only `reserved_bytes` is re-read per CAS attempt.
        // `estimated_memory_bytes` is a cached atomic now, but it was an
        // O(tables × buckets) sweep re-run on every retry — 10.3% of total
        // process CPU in prod (profile 2026-07-30). Keep it hoisted.
        let current_mem = self.mem_buffer.estimated_memory_bytes();

        for attempt in 0..MAX_CAS_RETRIES {
            let current_reserved = self.reserved_bytes.load(Ordering::Acquire);
            let new_total = current_mem + current_reserved + estimated_size;

            if new_total > hard_limit {
                anyhow::bail!(
                    "Memory limit exceeded: {}MB + {}MB reservation > {}MB hard limit",
                    (current_mem + current_reserved) / (1024 * 1024),
                    estimated_size / (1024 * 1024),
                    hard_limit / (1024 * 1024)
                );
            }

            if self.reserved_bytes.compare_exchange(current_reserved, current_reserved + estimated_size, Ordering::AcqRel, Ordering::Acquire).is_ok() {
                // If post-reservation we crossed the configured pressure threshold,
                // wake the flush task so it can drain completed buckets without
                // waiting for the next tick.
                if fill_pct(new_total, max_bytes) >= threshold {
                    self.pressure_notify.notify_one();
                }
                return Ok(estimated_size);
            }

            if attempt < CAS_SPIN_ATTEMPTS {
                std::hint::spin_loop();
            } else {
                let backoff_micros = CAS_BACKOFF_BASE_MICROS << attempt.min(CAS_BACKOFF_MAX_EXPONENT);
                tokio::time::sleep(std::time::Duration::from_micros(backoff_micros)).await;
            }
        }
        anyhow::bail!("Failed to reserve memory after {} retries due to contention", MAX_CAS_RETRIES)
    }
```

Five spins, then exponential backoff capped at ~1 ms, then 100 attempts total. The
hoisting comment is worth its own note: `estimated_memory_bytes()` used to be an
`O(tables × buckets)` sweep re-run on every CAS retry, and that alone was **10.3%
of total process CPU** in a production profile. Loop-invariant hoisting is not
usually a 10% win; it is here because the loop is contended and the invariant was
expensive.

The reservation size and the hard limit:

**Listing 10.10 — src/write/mod.rs:29–61**

```rust
// Safety margin over `estimate_batch_size()` for costs it can't see: Vec
// headers, DashMap node overhead, allocator fragmentation. The estimator's
// `get_array_memory_size()` already covers Arrow buffers (alignment, validity
// bitmaps), and fixed costs (walrus mmaps, Foyer, tantivy) are subtracted via
// `max_memory_bytes()`. Measured within ~10–15% of marginal heap growth
// (bench/multiplier_bench.py, 2026-05-17).
const MEMORY_OVERHEAD_MULTIPLIER: f64 = 1.15;

/// Estimated reserved bytes for a write: raw Arrow size × the overhead multiplier.
/// Single source of truth shared by `try_reserve_memory` and `force_reserve` so the
/// admit and force-admit paths can't drift apart.
fn estimate_reservation(batches: &[RecordBatch]) -> usize {
    let batch_size: usize = batches.iter().map(estimate_batch_size).sum();
    (batch_size as f64 * MEMORY_OVERHEAD_MULTIPLIER) as usize
}

/// Fill ratio (0..=100) of `used` against the budget, clamped.
fn fill_pct(used: usize, max_bytes: usize) -> u32 {
    ((used as u128 * 100 / max_bytes.max(1) as u128).min(100)) as u32
}

/// Hard limit = `max_bytes + max_bytes / N` = 120% of budget (`5` → +20%),
/// leaving headroom for in-flight writes without unbounded growth.
const HARD_LIMIT_HEADROOM_DIVISOR: usize = 5;
// ... elided: FLUSH_CHUNK_BUCKET_IDS
/// The reservation ceiling live writers are rejected at.
fn hard_limit(max_bytes: usize) -> usize {
    max_bytes.saturating_add(max_bytes / HARD_LIMIT_HEADROOM_DIVISOR)
}
```

⚠️ `CLAUDE.md` documents `MEMORY_OVERHEAD_MULTIPLIER = 1.2` and
`HARD_LIMIT_MULTIPLIER = 5`. The source says `1.15` and expresses the hard limit
as `max + max/5`. The 1.2 → 1.15 change is a real drift; the "5" is the same
number described differently. Trust the source.

**Backpressure instead of rejection.** When the CAS fails at the hard limit, the
correct move for a time-series database is not to reject — it is to spill to Delta
and retry:

**Listing 10.11 — src/write/mod.rs:993–1064**

```rust
    /// Reserve memory for a write, applying *backpressure* instead of dropping
    /// the write when the hard limit is hit. The rows are already destined for
    /// the durable WAL, and Delta/S3 is effectively unbounded "disk" — so when
    /// RAM is full the correct move is to flush MemBuffer → Delta to make room
    /// (the spill), not to reject. We retry the reservation after each drain
    /// and only fail after `write_backpressure_timeout` with no progress, which
    /// means Delta itself is unavailable.
    ///
    /// This deliberately reintroduces synchronous flushing into the insert path
    /// (previously removed to keep inserts non-blocking). The trade-off is
    /// intentional and now load-bearing: for a time-series DB a slow write is
    /// far better than a rejected one the producer must DLQ. Normal sub-limit
    /// inserts take the fast path and never block here.
    async fn reserve_with_backpressure(&self, batches: &[RecordBatch]) -> anyhow::Result<usize> {
        let first = self.try_reserve_memory(batches).await;
        let timeout = self.config.buffer.write_backpressure_timeout();
        if first.is_ok() || timeout.is_zero() {
            return first;
        }

        let deadline = std::time::Instant::now() + timeout;
        crate::observability::record_backpressure_engaged();
        self.backpressure_engaged_total.fetch_add(1, Ordering::Relaxed);
        warn!(
            "Write backpressure engaged: used={}MB ≥ hard limit; waking background flush to free RAM (not rejecting, not flushing on insert thread)",
            self.effective_memory_bytes() / (1024 * 1024)
        );
        loop {
            // Single-flight relief: only the writer that wins `relief_lock`
            // drives the synchronous flush; everyone else just nudges the
            // background flusher and waits. Previously every blocked writer ran
            // its own `flush_completed_buckets` + force-flush cycle, all queued
            // on `flush_lock` — with N writers the one at the back of the herd
            // waited O(N × commit), the source of the ~20s p99 tail. Now one
            // writer flushes (O(commit)) while the rest sleep below.
            if let Ok(_relief) = self.relief_lock.try_lock() {
                self.relieve_memory_pressure().await;
            } else {
                self.pressure_notify.notify_one();
            }

            match self.try_reserve_memory(batches).await {
                Ok(sz) => return Ok(sz),
                Err(e) => {
                    if std::time::Instant::now() >= deadline {
                        crate::observability::record_backpressure_rejected();
                        self.backpressure_rejected_total.fetch_add(1, Ordering::Relaxed);
                        // NOTE: this rejection happens in `insert()` BEFORE
                        // `wal.append_batch`, so the batch is NOT durable here —
                        // the old "data remains in WAL" wording was wrong. The
                        // batch is dropped from TF's side and recovery depends on
                        // the caller retrying / the upstream DLQ. Removing this
                        // loss seam is parity-plan Defect 1 (WAL-before-admit).
                        error!(
                            "Write backpressure exhausted after {:?}: used={}MB still over hard limit — Delta flush is not freeing memory; rejecting batch (NOT yet durable — WAL append happens only after admission; caller must retry or rely on the upstream DLQ)",
                            timeout,
                            self.effective_memory_bytes() / (1024 * 1024)
                        );
                        return Err(e);
                    }
                    // Wait for a flush to free RAM, then retry. Woken early by
                    // `flush_tick_notify` (the relief winner / background task
                    // signals it on every flush), capped at 25ms so a missed
                    // wakeup can't stall the retry.
                    tokio::select! {
                        _ = self.flush_tick_notify.notified() => {}
                        _ = tokio::time::sleep(Duration::from_millis(25)) => {}
                    }
                }
            }
        }
    }
```

**The single-flight relief pattern is the important part.** Without
`relief_lock`, every blocked writer ran its own flush cycle, all of them queued
on the global `flush_lock`, so the writer at the back of a herd of N waited
`O(N × commit)`. That was measured as a ~20-second p99 tail. With it, exactly one
writer flushes in `O(commit)` and the rest park on `flush_tick_notify` with a
25 ms poll as a missed-wakeup backstop.

The relief itself escalates in a specific order:

**Listing 10.12 — src/write/mod.rs:1066–1086**

```rust
    /// One pass of pressure relief: drain completed buckets, then — if still
    /// over the limit — force-flush the current open bucket(s). Order matters:
    /// `force_flush_current_buckets` self-gates while completed buckets remain
    /// (WAL-ordering invariant), so completed buckets must drain first. Shared
    /// by the insert backpressure path (single-flighted via `relief_lock`) and
    /// the background flush task; both warn-and-continue on flush errors so the
    /// caller's retry/no-progress logic decides when to give up.
    async fn relieve_memory_pressure(&self) {
        if let Err(e) = self.flush_completed_buckets().await {
            warn!("pressure: flush_completed_buckets failed: {}", e);
        }
        if self.is_memory_pressure()
            && let Err(e) = self.force_flush_current_buckets().await
        {
            warn!("pressure: force_flush_current_buckets failed: {}", e);
        }
        // Memory may now be below the limit — wake any backpressured writers
        // parked on `flush_tick_notify` so they retry their reservation
        // immediately instead of waiting out their 25ms poll.
        self.flush_tick_notify.notify_waiters();
    }
```

⚠️ **The known loss seam.** The `error!` in the rejection arm says it plainly:
this rejection happens *before* `wal.append_batch`, so the batch is **not**
durable, and recovery depends on the caller retrying or on the upstream
dead-letter queue. The comment names it as "parity-plan Defect 1
(WAL-before-admit)" — i.e. the fix is known (append to the WAL before admitting to
memory) and not yet done. This is one of the honest chapter's entries.

There is an opt-in that trades the loss seam for unbounded growth:

**Listing 10.13 — src/write/mod.rs:982–991**

```rust
    /// Reserve memory unconditionally — adds the estimated bytes even past the
    /// hard limit. Only used on the `wal_admit_decouple` path when backpressure
    /// is exhausted: we admit over-budget rather than drop, since the WAL already
    /// holds the batch durably. Wakes the flush task to drain the overage.
    fn force_reserve(&self, batches: &[RecordBatch]) -> usize {
        let estimated_size = estimate_reservation(batches);
        self.reserved_bytes.fetch_add(estimated_size, Ordering::AcqRel);
        self.pressure_notify.notify_one();
        estimated_size
    }
```

selected in `insert_bounded` by config:

**Listing 10.14 — src/write/mod.rs:1208–1225**

```rust
        // Reserve memory atomically before writing - prevents race condition.
        // Applies backpressure (synchronous flush-to-Delta + retry) instead of
        // rejecting when at the hard limit — see `reserve_with_backpressure`.
        let reserved_size = match self.reserve_with_backpressure(&batches).await {
            Ok(sz) => sz,
            // Decouple (parity plan Defect 1, default OFF): never DROP a write
            // whose backpressure budget is exhausted. The WAL append below is the
            // durability boundary, so admitting over-budget beats rejecting. The
            // batch is still admitted to MemBuffer + recorded, so the count-based
            // FIFO WAL advance stays correct (no skipped/un-admitted entry). Growth
            // is bounded by the relief flush + WAL replay on restart — soak before
            // prod enable.
            Err(e) if self.config.buffer.wal_admit_decouple() => {
                warn!("wal_admit_decouple: admitting over-budget instead of rejecting (WAL is durable): {}", e);
                self.force_reserve(&batches)
            }
            Err(e) => return Err(e),
        };
```

Default off, "soak before prod enable" — an honest label on a half-landed fix.

### The WAL pin lifecycle, and the append

The final act is the durability boundary. It is wrapped in `with_wal_pin`, which
manages the hold lifecycle across the append→apply window:

**Listing 10.15 — src/write/mod.rs:1227–1275**

```rust
        // No per-topic mutex needed: WAL now shards each (project, table)
        // across N walrus collections via `WalManager::pick_shard`, so
        // concurrent appends to the same topic land in different shards and
        // walrus's single-writer-per-collection invariant is never contended.
        // MemBuffer is DashMap-based and already concurrent-safe.
        // WAL append + MemBuffer apply under a single pin lifecycle (see
        // `with_wal_pin`): the pending hold covers the append→apply window,
        // then each destination bucket is pinned at the pre-append position
        // atomically with its batch (batches in one append all land on the
        // same shard, but may straddle bucket boundaries if their timestamps
        // differ; the shared pre-position is ≤ every entry of this append,
        // so it's a valid hold for all).
        let result: anyhow::Result<()> = self.with_wal_pin(
            project_id,
            table_name,
            "append_batch",
            |on_pre| self.wal.append_batch(project_id, table_name, &batches, on_pre),
            |hold| {
                let now = crate::support::now_micros();
                batches.iter().try_for_each(|batch| {
                    self.mem_buffer.insert_with_hold(
                        project_id,
                        table_name,
                        batch.clone(),
                        batch_timestamp_range(batch).map(|(min, _)| min).unwrap_or(now),
                        hold,
                    )
                })
            },
        );

        // Release reservation (memory is now tracked by MemBuffer)
        self.release_reservation(reserved_size);

        result
            .inspect(|()| {
                self.rows_ingested_total.fetch_add(row_count as u64, Ordering::Relaxed);
                crate::observability::record_insert(project_id, table_name, row_count as u64);
            })
            .inspect_err(|_| crate::observability::record_ingest_error(project_id, table_name))?;

        // Immediate flush mode: flush after every insert
        if self.config.buffer.flush_immediately() {
            self.flush_all_now().await?;
        }

        debug!("BufferedWriteLayer insert complete: project={}, table={}", project_id, table_name);
        Ok(())
    }
```

The hand-off of memory accounting is worth naming: `release_reservation` runs
*after* the MemBuffer apply, because from that point the bytes are counted by
`MemBuffer::estimated_memory_bytes()` instead. Reservation and MemBuffer
accounting are two halves of one number, and double-counting or gapping between
them would either over- or under-report pressure.

The `hold` argument is the pre-append WAL position, and it is what makes the WAL
cursor safe: the position is captured *before* the append (via the `on_pre`
callback, under the shard's append lock), so it is ≤ every entry this append
produces. Every destination bucket records that hold atomically with its batch.
Until a bucket's rows reach Delta, its hold pins the cursor. Chapter 11 develops
this in full.

### How the write path refuses

Seven distinct refusals, and the client sees a different thing for each:

| Refusal | Where | Durable? | Client sees |
|---|---|---|---|
| Draining for deployment | `admit_write` | n/a | retryable error naming the replacement |
| WAL backlog over hard cap | `insert_bounded` | n/a | "retry later", with the GB limit |
| Absurd event timestamps | `bound_event_time` | rows dropped | **success** (rows silently dropped, `warn!` + metric) |
| Memory hard limit, backpressure exhausted | `reserve_with_backpressure` | **no** | error; caller must retry / DLQ |
| CAS contention (100 retries) | `try_reserve_memory` | n/a | "failed to reserve memory … contention" |
| WAL append failure | `wal.append_batch` | no | error |
| MemBuffer schema incompatibility | `insert_with_hold` | **yes** (WAL has it) | error, but replay recovers |

The third row is the one to remember: **out-of-range timestamps are dropped, not
rejected**, and the statement still succeeds. The only signals are the
`warn!("dropped N rows with event timestamps outside …")` line and the
`event_time_bounded` metric.

### How to debug this flow

**`timefusion_stats`** is the first stop, and the field comments tell you how to
read the counters against each other:

```sql
SELECT component, key, value FROM timefusion_stats WHERE component = 'mem_buffer';
```

- `pressure_pct` at 100 with `rows_ingested_total` climbing faster than
  `rows_flushed_total` → the flush is *working* but ingest outpaces drain.
- `pressure_pct` at 100 with `flush_freed_bytes_total` flat while flushes commit →
  the drained buckets are near-empty; the memory is in buckets the flush path is
  not reaching (typically an open window needing force-flush).
- `flush_failed_total` climbing → a stuck flush, a different problem entirely.
- `backpressure_engaged_total` / `backpressure_rejected_total` /
  `backpressure_force_flush_total` → the three escalation tiers.
- `quarantine_files` / `quarantine_bytes` non-zero → **deferred data loss**; the
  field comment says to alert on it.
- `orphaned_topics` / `orphan_pin_age` → a failed commit left rows only in the
  WAL; a restart is due before the pinned WAL fills the disk.
- `mem_replay_dml_noops` growing → the loss-class canary that replaced the
  quarantine count.

**Log lines**, in rough order of severity:

- `"dropped N rows with event timestamps outside [2000-01-01, now+48h]"` — client
  unit error upstream.
- `"Memory pressure (used=…MB / max=…MB) — notifying background flush"` — normal
  under load.
- `"Write backpressure engaged: … waking background flush to free RAM"` — the
  spill path is active; inserts are slow but not failing.
- `"Write backpressure exhausted after …: rejecting batch (NOT yet durable …)"` —
  data loss unless the caller retries. Delta is not accepting commits.
- `"wal_admit_decouple: admitting over-budget instead of rejecting"` — the opt-in
  is on; watch RSS.
- `"Quarantined WAL entry to …"` — a WAL entry could not be decoded; parked for
  manual redrive (`timefusion redrive-dml`).

**Spans**: `datafusion.table.write` (`rows.count`, `projects.count`) and its child
`delta_table.insert` per project.

**Key takeaways.** An `INSERT` becomes one write per tenant, each of which is
compacted, timestamp-bounded, memory-reserved, WAL-appended, and MemBuffer-applied
before the client is acknowledged. The durability boundary is the WAL append;
everything before it can reject. Memory admission is a CAS loop against a 120%
hard limit, with single-flighted spill-to-Delta as backpressure rather than
rejection — the "slow write beats a rejected write" trade is explicit and
load-bearing. One known loss seam remains: a backpressure-exhausted write is
rejected before the WAL append and is not durable.
## 11. The write-ahead log

*After this chapter you will know the WAL's on-disk format, its topic and shard
model, the cursor-hold protocol that makes it safe to advance a read cursor, the
directory lock that enforces single-writer, the takeover protocol that makes
start-first deploys work, and the entry-splitting logic that bounds replay
memory.*

### What the WAL is, and is not

TimeFusion's WAL is a *durability buffer*, not a replication log and not a
transaction log. It exists to make an `INSERT` acknowledgeable in under a
millisecond while the Delta commit happens minutes later. Its entries are
consumed — permanently — once their rows reach Delta, and the machinery in this
chapter is almost entirely about deciding *when* that is true.

It is built on `walrus-rust`, vendored at `vendor/walrus-rust`: an append-only,
topic-partitioned, mmap-backed log with checkpointable read cursors and a
configurable fsync schedule.

### The on-disk format

**Listing 11.1 — src/write/wal.rs:67–81 and 1195–1216**

```rust
const WAL_MAGIC: [u8; 4] = [0x57, 0x41, 0x4C, 0x32];
/// Insert batches are stored as Arrow IPC stream bytes. Embeds the schema so
/// the reader doesn't need a separate registry lookup, and round-trips every
/// Arrow type (List/Struct/Variant/…) without the per-buffer bincode shuffle
/// the older CompactBatch format required.
///
/// Bump on any breaking change to the on-disk WAL format or the walrus key
/// derivation. The startup version-stamp check refuses to open a directory
/// written by a different version, so existing data must be wiped on bump.
const WAL_VERSION: u8 = 1;
const BINCODE_CONFIG: bincode::config::Configuration = bincode::config::standard();
```

```rust
fn serialize_wal_entry(entry: &WalEntry) -> Result<Vec<u8>, WalError> {
    let mut buffer = WAL_MAGIC.to_vec();
    buffer.push(WAL_VERSION);
    buffer.push(entry.operation as u8);
    buffer.extend(bincode::encode_to_vec(entry, BINCODE_CONFIG)?);
    Ok(buffer)
}

fn deserialize_wal_entry(data: &[u8]) -> Result<WalEntry, WalError> {
    let [m0, m1, m2, m3, version, operation, payload @ ..] = data else {
        return Err(WalError::TooShort { len: data.len() });
    };
    if [*m0, *m1, *m2, *m3] != WAL_MAGIC {
        return Err(WalError::UnsupportedVersion { version: *m0, expected: WAL_VERSION });
    }
    if *version != WAL_VERSION {
        return Err(WalError::UnsupportedVersion { version: *version, expected: WAL_VERSION });
    }
    WalOperation::try_from(*operation)?;
    let (entry, _): (WalEntry, _) = bincode::decode_from_slice(payload, BINCODE_CONFIG)?;
    Ok(entry)
}
```

Six bytes of header — `"WAL2"`, a version byte, an operation byte — then a
bincode-encoded `WalEntry`. The slice pattern `[m0, m1, m2, m3, version,
operation, payload @ ..]` is a nice use of Rust's slice patterns: length check,
field extraction, and rest-binding in one irrefutable-with-else destructuring.

⚠️ `CLAUDE.md` documents `WAL_VERSION = 128` with a note about distinguishing
from legacy operation bytes 0–2. The source says `WAL_VERSION: u8 = 1`. The
format has been rewritten since (INSERT payloads are now Arrow IPC rather than
the old `CompactBatch`), and the version stamp check refuses to open a directory
written by a different version, so the transition was a wipe rather than a
migration.

The entry itself, and the single attribute that made WAL replay 3× faster:

**Listing 11.2 — src/write/wal.rs:174–194**

```rust
#[derive(Debug, Encode, Decode)]
pub struct WalEntry {
    pub timestamp_micros: i64,
    pub project_id: String,
    pub table_name: String,
    pub operation: WalOperation,
    /// NOT `#[bincode(with_serde)]`. Serde encodes a `Vec<u8>` as a SEQUENCE,
    /// so decode ran a per-element loop: 2.7ms to decode one 86KB payload
    /// (~31 MB/s), which was 58% of all WAL replay wall-clock (2026-08-15).
    /// bincode's native impl decodes the same bytes in 3-4us — 700-880x — and
    /// `compare_vec_u8_encodings` asserts the two encodings are BYTE-IDENTICAL,
    /// so this is a pure speedup: no on-disk format change, no version bump,
    /// and logs written by either binary read back under the other.
    pub data: Vec<u8>,
}

impl WalEntry {
    fn new(project_id: &str, table_name: &str, operation: WalOperation, data: Vec<u8>) -> Self {
        Self { timestamp_micros: chrono::Utc::now().timestamp_micros(), project_id: project_id.into(), table_name: table_name.into(), operation, data }
    }
}
```

Read that comment twice. **58% of all WAL replay wall-clock** was one derive
attribute causing `Vec<u8>` to be decoded element-by-element as a sequence
instead of as a byte slice. 700–880× on that step, byte-identical output, no
format change. It is the single highest-leverage line in the file, and it is
invisible unless you profile.

Four operations, and a `strum::FromRepr` derive doing the `u8 → enum` work:

**Listing 11.3 — src/write/wal.rs:155–172**

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode, strum::FromRepr)]
#[repr(u8)]
pub enum WalOperation {
    Insert = 0,
    Delete = 1,
    Update = 2,
    /// `UPDATE ... FROM` with a materialized source RecordBatch serialized
    /// alongside the predicate/assignments. Added in V2 of the UPDATE shape;
    /// old binaries will reject these entries with `InvalidOperation(3)`.
    UpdateWithSource = 3,
}

impl TryFrom<u8> for WalOperation {
    type Error = WalError;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        Self::from_repr(value).ok_or(WalError::InvalidOperation(value))
    }
}
```

The three DML payloads:

**Listing 11.4 — src/write/wal.rs:196–223**

```rust
#[derive(Debug, Encode, Decode)]
pub struct DeletePayload {
    pub predicate_sql: Option<String>,
}

#[derive(Debug, Encode, Decode)]
pub struct UpdatePayload {
    pub predicate_sql: Option<String>,
    pub assignments: Vec<(String, String)>,
}

/// `UPDATE ... FROM` source side, persisted alongside the predicate +
/// assignments so WAL replay can reconstruct the join after a restart.
/// `batch_ipc` is an Arrow IPC stream of the source `RecordBatch`.
#[derive(Debug, Clone, Encode, Decode)]
pub struct SerializedSource {
    /// `(target_col, source_col)` pairs — bare column names.
    pub join_keys: Vec<(String, String)>,
    /// Arrow IPC stream bytes of the materialized source batch.
    pub batch_ipc: Vec<u8>,
}

#[derive(Debug, Encode, Decode)]
pub struct UpdateWithSourcePayload {
    pub predicate_sql: Option<String>,
    pub assignments: Vec<(String, String)>,
    pub source: SerializedSource,
}
```

Note that DML is logged as **SQL text plus assignments**, not as a row set. That
is what makes an `UPDATE` replayable at all — but it also means replay needs the
UDF registry (which is why `main.rs` builds the session context and hands
`Arc<FnRegistry>` to the layer before recovery). `UPDATE ... FROM` is the
exception: its *source* side is materialized to Arrow IPC, because a join source
cannot be re-derived at replay time.

### Topics, shards, and keys

The logical unit is a topic per `(project_id, table_name)`. The physical unit is
N walrus collections per topic:

**Listing 11.5 — src/write/wal.rs:225–241**

```rust
/// Number of walrus shards per logical (project_id, table_name) topic.
/// Walrus serializes appends within a single collection — the per-collection
/// `is_batch_writing` AtomicBool returns WouldBlock on concurrent batch
/// writes. Routing each write to one of N hash-distinguished shards lifts the
/// single-project ceiling near-linearly (different shards never contend on
/// the same walrus block/offset), at the cost of merging N streams in
/// timestamp order during recovery.
///
/// 4 is a defensible default for a developer/single-host workload; production
/// deployments override via `BufferConfig::timefusion_wal_shards_per_topic`.
const WAL_SHARDS_PER_TOPIC_DEFAULT: usize = 4;

/// Stripe count for the per-collection append locks (see
/// `WalManager::append_locks`). Far exceeds the realistic distinct-collection
/// count (topics × shards) so false sharing between unrelated collections is
/// negligible.
const WAL_APPEND_LOCK_STRIPES: usize = 256;
```

The key derivation is short and carries two non-obvious requirements:

**Listing 11.6 — src/write/wal.rs:374–418**

```rust
    /// Human-readable topic identifier for metadata/logging
    fn make_topic(project_id: &str, table_name: &str) -> String {
        format!("{}:{}", project_id, table_name)
    }

    /// Short hash for walrus topic key, scoped to a shard so we get N
    /// independent walrus collections per logical (project, table).
    /// Walrus's metadata budget is 62 bytes; 16 hex chars + a `-` + 2 digits
    /// shard suffix stays well under.
    fn walrus_topic_key(project_id: &str, table_name: &str, shard: usize) -> String {
        // Must be stable across compilations — the key indexes durable WAL
        // data. AHasher::default() seeds itself per build, which would silently
        // strand entries after an upgrade. FNV-1a is deterministic, fast, and
        // 64-bit-wide (the only width walrus's 62-byte key budget needs).
        //
        // Length-prefix each field so ("a:b","c") and ("a","b:c") (or any
        // pair that would concatenate to the same bytes) hash distinctly.
        // Don't rely on `str::hash`'s 0xff terminator for separation — that's
        // a stdlib implementation detail, not a contract.
        use std::hash::Hasher;

        use fnv::FnvHasher;
        let mut hasher = FnvHasher::default();
        hasher.write_u64(project_id.len() as u64);
        hasher.write(project_id.as_bytes());
        hasher.write_u64(table_name.len() as u64);
        hasher.write(table_name.as_bytes());
        format!("{:016x}-{:02}", hasher.finish(), shard)
    }

    /// Round-robin shard chooser for a topic. Bumps a per-topic counter so
    /// concurrent batches for the same topic spread across N walrus
    /// collections rather than serializing at walrus's per-collection write
    /// lock.
    fn pick_shard(&self, topic: &str) -> usize {
        use std::sync::atomic::{AtomicU64, Ordering};
        // get-first: `entry` would allocate the String key on every append.
        // Safe against dashmap's self-deadlock — the `get` guard is released
        // before the `entry` in the None arm.
        let ticket = match self.shard_counter.get(topic) {
            Some(counter) => counter.fetch_add(1, Ordering::Relaxed),
            None => self.shard_counter.entry(topic.to_string()).or_insert_with(|| AtomicU64::new(0)).fetch_add(1, Ordering::Relaxed),
        };
        (ticket as usize) % self.shards_per_topic
    }
```

Two hard requirements, both stated:

1. **The hash must be stable across compilations**, because the key indexes
   durable data on disk. `AHasher::default()` seeds itself per build, which would
   silently strand every existing entry after an upgrade. FNV-1a is
   deterministic.
2. **The fields must be length-prefixed**, so `("a:b", "c")` and `("a", "b:c")`
   hash distinctly. Relying on `str::hash`'s internal `0xff` terminator would be
   depending on a stdlib implementation detail.

`pick_shard`'s `get`-before-`entry` and `persist_topic`'s `contains`-before-`insert`
are the same micro-optimization: DashMap's `entry` API takes an owned key, so the
naive version allocates a `String` on *every append*.

**Listing 11.7 — src/write/wal.rs:361–372**

```rust
    fn persist_topic(&self, topic: &str) {
        // contains-first: `insert` alone would allocate a String on every append.
        if self.known_topics.contains(topic) || !self.known_topics.insert(topic.to_string()) {
            return;
        }
        use std::io::Write;
        let meta_dir = self.data_dir.join(META_DIR);
        let _ = std::fs::create_dir_all(&meta_dir)
            .and_then(|()| std::fs::OpenOptions::new().create(true).append(true).open(meta_dir.join("topics")))
            .and_then(|mut f| writeln!(f, "{}", topic))
            .inspect_err(|e| warn!("Failed to persist WAL topic '{}' to {:?}: {}", topic, meta_dir, e));
    }
```

The `topics` file under `.timefusion_meta/` exists because the walrus key is a
hash — there is no way to enumerate `(project, table)` pairs from the collection
names alone. The topic list is how a fresh process discovers what to replay.

### The append, and the hold-registration contract

This is the most important contract in the WAL, and it is expressed as a callback
that fires *under the append lock, before the entries exist*:

**Listing 11.8 — src/write/wal.rs:466–512**

```rust
    /// Returns the shard the entry was appended to.
    #[instrument(skip(self, batch))]
    pub fn append(&self, project_id: &str, table_name: &str, batch: &RecordBatch) -> Result<usize, WalError> {
        self.append_batch(project_id, table_name, std::slice::from_ref(batch), |_, _| {}).map(|(shard, _)| shard)
    }

    /// Returns `(shard, pre_append_position)` — every batch becomes one walrus
    /// entry on the chosen shard.
    ///
    /// `on_pre_append(shard, position)` fires under the shard's append lock
    /// BEFORE the entries exist, with the shard's write tail at that instant.
    /// Callers register a read-cursor *hold* there: because registration
    /// happens-before the append, a concurrent watermark computation that
    /// snapshots the tail first and then reads holds can never advance the
    /// cursor past an entry whose hold it hasn't seen (see
    /// `BufferedWriteLayer::compute_wal_watermark`).
    #[instrument(skip(self, batches, on_pre_append))]
    pub fn append_batch(
        &self, project_id: &str, table_name: &str, batches: &[RecordBatch], on_pre_append: impl FnOnce(usize, Option<WalPosition>),
    ) -> Result<(usize, Option<WalPosition>), WalError> {
        let topic = Self::make_topic(project_id, table_name);
        let shard = self.pick_shard(&topic);
        let walrus_key = Self::walrus_topic_key(project_id, table_name, shard);
        // Imperative on purpose: a `map(..).collect::<Result<Vec<_>,_>>()` over
        // the splits would hold every batch's split output alive alongside the
        // serialized entries, doubling the transient footprint of a big append.
        let mut payloads: Vec<Vec<u8>> = Vec::with_capacity(batches.len());
        for batch in batches {
            for data in split_to_wal_payloads(batch, WAL_SPLIT_TARGET, MAX_BATCH_SIZE)? {
                payloads.push(serialize_wal_entry(&WalEntry::new(project_id, table_name, WalOperation::Insert, data))?);
            }
        }

        let payload_refs: Vec<&[u8]> = payloads.iter().map(Vec::as_slice).collect();
        let pre_pos = {
            // Guard scoped tightly: dropped before persist_topic so the shard
            // lock never covers persist_topic's synchronous file I/O.
            let _guard = self.append_lock(&walrus_key);
            let pre_pos = self.wal.current_position(&walrus_key).ok();
            on_pre_append(shard, pre_pos);
            self.wal.batch_append_for_topic(&walrus_key, &payload_refs)?;
            pre_pos
        };
        self.persist_topic(&topic);
        debug!(%topic, shard, batches = batches.len(), "WAL batch append INSERT");
        Ok((shard, pre_pos))
    }
```

**The happens-before argument.** The cursor-advance computation
(`compute_wal_watermark`) works by snapshotting the shard tail and *then* reading
the set of registered holds. A writer registers its hold under the append lock,
before appending. So there are only two orderings:

- Writer registers hold → appends → watermark snapshots tail (sees the entry) and
  reads holds (sees the hold): the hold blocks the advance. Correct.
- Watermark snapshots tail (does not see the entry) → writer registers and
  appends: the watermark cannot advance past an entry it did not see. Correct.

There is no interleaving in which the watermark sees an entry but not its hold.
That is the whole safety property, and it is bought with one callback firing at
one specific point.

Note also the imperative loop over splits, with its stated reason (a
`collect::<Result<Vec<_>,_>>()` would hold every batch's split output alive
alongside the serialized entries) — a nice counter-example to the codebase's
general preference for iterator chains. Conciseness yields to a memory argument.

The single-entry variant does the same, plus an explicit fsync decision:

**Listing 11.9 — src/write/wal.rs:443–464**

```rust
    /// Serialize and append one entry under the shard's `append_lock` so
    /// concurrent same-shard appends queue instead of erroring. The guard
    /// drops when this returns, so callers' `persist_topic` file I/O runs
    /// outside the critical section — keep it after the call, not before.
    /// `on_pre` fires with the pre-append tail under the lock — same
    /// hold-registration contract as [`Self::append_batch`].
    fn locked_append(&self, walrus_key: &str, entry: &WalEntry, on_pre: impl FnOnce(Option<WalPosition>)) -> Result<(), WalError> {
        let entry_bytes = serialize_wal_entry(entry)?;
        let guard = self.append_lock(walrus_key);
        on_pre(self.wal.current_position(walrus_key).ok());
        self.wal.append_for_topic(walrus_key, &entry_bytes)?;
        // Sync OUTSIDE the stripe lock: the entry's bytes are already in the
        // mmap and `Writer::sync` flushes the whole active block, so
        // sync-before-ack holds — while an ms-scale msync under the stripe
        // would stall every same-stripe append (the lock's contract is
        // "fast, in-memory only").
        drop(guard);
        if self.ack_fsync {
            self.wal.sync_topic(walrus_key).map_err(WalError::Io)?;
        }
        Ok(())
    }
```

"Sync outside the stripe lock" is correct *because* `Writer::sync` flushes the
whole active block — so a sync issued after releasing the lock still covers the
entry this call appended, and possibly more. Correctness is preserved and the
lock's "fast, in-memory only" contract holds.

The striped locks themselves:

**Listing 11.10 — src/write/wal.rs:424–435**

```rust
    /// Acquire the append lock for a walrus collection so concurrent appends to
    /// it queue instead of hitting walrus's "another batch write already in
    /// progress". Held only across the (fast, in-memory) walrus write — never an
    /// `.await` — so blocking a worker is brief. The guard wraps `()`, so a
    /// poisoned lock carries no invalid state and is safe to recover.
    fn append_lock(&self, walrus_key: &str) -> std::sync::MutexGuard<'_, ()> {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        walrus_key.hash(&mut h);
        let idx = (h.finish() as usize) % self.append_locks.len();
        self.append_locks[idx].lock().unwrap_or_else(|e| e.into_inner())
    }
```

A `std::sync::Mutex` (not Tokio's) held across a synchronous section with no
`await` — the right choice, and the doc comment states both halves of why:
brevity of the critical section, and that a `Mutex<()>` cannot carry invalid
state, so `into_inner()` on poison is safe. Note that unlike `walrus_topic_key`,
this hash *may* use `DefaultHasher` — the stripe index is ephemeral, not durable.

### DML appends

All three DML appenders share one spine:

**Listing 11.11 — src/write/wal.rs:514–582**

```rust
    /// Encode a DML payload and append it as one entry; returns the chosen
    /// shard. Shared spine of the Delete/Update/UpdateWithSource appenders —
    /// they differ only in payload type and log line.
    fn append_dml<P: Encode>(
        &self, project_id: &str, table_name: &str, operation: WalOperation, payload: &P, on_pre_append: impl FnOnce(usize, Option<WalPosition>),
    ) -> Result<usize, WalError> {
        let topic = Self::make_topic(project_id, table_name);
        let shard = self.pick_shard(&topic);
        let walrus_key = Self::walrus_topic_key(project_id, table_name, shard);
        let entry = WalEntry::new(project_id, table_name, operation, bincode::encode_to_vec(payload, BINCODE_CONFIG)?);
        self.locked_append(&walrus_key, &entry, |pre| on_pre_append(shard, pre))?;
        self.persist_topic(&topic);
        Ok(shard)
    }

    // ... elided: append_delete and append_update, each three lines of payload
    // construction plus a debug! line

    /// Append an `UPDATE ... FROM` entry. Stores the source `RecordBatch`
    /// (already serialized to Arrow IPC bytes by the caller) alongside the
    /// predicate + assignments so WAL replay can reconstruct the join.
    /// `on_pre_append` — same hold-registration contract as [`Self::append_batch`].
    #[instrument(skip(self, assignments, source, on_pre_append), fields(source_ipc_bytes = source.batch_ipc.len()))]
    pub fn append_update_with_source(
        &self, project_id: &str, table_name: &str, predicate_sql: Option<&str>, assignments: &[(String, String)], source: &SerializedSource,
        on_pre_append: impl FnOnce(usize, Option<WalPosition>),
    ) -> Result<usize, WalError> {
        // The replay-side deserializer rejects over-cap source batches, so an
        // acked oversized entry would be silently dropped at the next boot —
        // fail the append instead so the client sees the error. (INSERTs are
        // split transparently; a JOIN source can't be split without changing
        // update semantics for non-unique keys.)
        if source.batch_ipc.len() > MAX_BATCH_SIZE {
            return Err(WalError::BatchTooLarge { size: source.batch_ipc.len(), max: MAX_BATCH_SIZE });
        }
        let payload = UpdateWithSourcePayload { predicate_sql: predicate_sql.map(String::from), assignments: assignments.to_vec(), source: source.clone() };
        let shard = self.append_dml(project_id, table_name, WalOperation::UpdateWithSource, &payload, on_pre_append)?;
        // ... elided: the debug! line
        Ok(shard)
    }
```

The `MAX_BATCH_SIZE` check on the source side is a *symmetry* fix: the replay
deserializer rejects over-cap payloads, so accepting one at append time would ack
a write that silently vanishes at the next boot. Fail at append so the client
learns about it. And the parenthetical explains why splitting is not an option
here: `INSERT`s split transparently because rows are independent, but splitting a
join source changes `UPDATE` semantics for non-unique keys.

### Splitting: bounding replay memory

`INSERT` payloads are split at append time so that replay decodes small units:

**Listing 11.12 — src/write/wal.rs:138–153**

```rust
/// Hard cap on a single WAL entry's batch payload (1GiB) — the replay
/// acceptance bound, guarding against unbounded allocation from a corrupted
/// entry, and the limit for unsplittable payloads (UPDATE...FROM sources,
/// single oversized rows). Ceiling is walrus's `MAX_ALLOC` (1GiB/block,
/// vendor/walrus-rust config.rs): entries can't physically exceed it, so
/// don't raise this without touching the vendored WAL engine.
const MAX_BATCH_SIZE: usize = 1024 * 1024 * 1024;
/// Append-side split target for INSERT batches — purely a replay-memory and
/// blast-radius knob, invisible to clients and to Delta (flush re-coalesces
/// per table into one commit regardless of WAL chunking). Each WAL entry is
/// read + Arrow-decoded whole during recovery inside the buffer budget, and a
/// corrupted entry quarantines whole — so keep the unit small even though
/// acceptance goes up to `MAX_BATCH_SIZE`.
const WAL_SPLIT_TARGET: usize = 100 * 1024 * 1024;
/// Fsync schedule interval in milliseconds - balances durability with performance
const FSYNC_SCHEDULE_MS: u64 = 200;
```

The splitter has to deal with a subtlety: Arrow dictionary columns are *shared
across rows*, so row-boundary slicing does not divide them.

**Listing 11.13 — src/write/wal.rs:1128–1186**

```rust
    // replicates values per row, so re-measure: the chunk math and the
    // shrink-bail below must use the size that will actually be sliced.
    let (batch, parent_len) = match flatten_dictionary_columns(batch)? {
        Some(flat) => {
            let len = serialize_record_batch(&flat)?.len();
            (flat, len)
        }
        None => (batch.clone(), data.len()),
    };
    drop(data);
    // +1 chunk of headroom absorbs row-size skew without a second pass.
    let chunks = parent_len.div_ceil(target) + 1;
    let rows_per = batch.num_rows().div_ceil(chunks).max(1);
    (0..batch.num_rows()).step_by(rows_per).try_fold(Vec::with_capacity(chunks), |mut out, start| {
        let len = rows_per.min(batch.num_rows() - start);
        let chunk = crate::write::mem_buffer::compact_batch(batch.slice(start, len));
        let chunk_data = serialize_record_batch(&chunk)?;
        if chunk_data.len() <= target || len <= 1 {
            if chunk_data.len() > hard_max {
                return Err(WalError::BatchTooLarge { size: chunk_data.len(), max: hard_max });
            }
            out.push(chunk_data);
        } else if chunk_data.len().saturating_mul(3) >= parent_len.saturating_mul(2) {
            // The chunk barely shrank despite holding a fraction of the rows:
            // some payload is shared across rows and row-slicing can't divide
            // it. Bail explicitly rather than recurse toward a per-row
            // explosion of near-full-size entries.
            return Err(WalError::BatchTooLarge { size: chunk_data.len(), max: target });
        } else {
            // Skewed rows left this chunk over target — re-split just it.
            drop(chunk_data);
            out.extend(split_to_wal_payloads(&chunk, target, hard_max)?);
        }
        Ok(out)
    })
}

/// Cast top-level dictionary columns to their value types (`None` when the
/// batch has no dictionary columns). Shared dictionaries defeat row-boundary
/// splitting; see `split_to_wal_payloads`.
fn flatten_dictionary_columns(batch: &RecordBatch) -> Result<Option<RecordBatch>, WalError> {
    use arrow::datatypes::{DataType, Field};
    if !batch.schema().fields().iter().any(|f| matches!(f.data_type(), DataType::Dictionary(_, _))) {
        return Ok(None);
    }
    let (fields, cols): (Vec<Field>, Vec<arrow::array::ArrayRef>) = batch
        .schema()
        .fields()
        .iter()
        .zip(batch.columns())
        .map(|(f, c)| match f.data_type() {
            DataType::Dictionary(_, value_type) => Ok((Field::new(f.name(), (**value_type).clone(), f.is_nullable()), arrow::compute::cast(c, value_type)?)),
            _ => Ok(((**f).clone(), c.clone())),
        })
        .collect::<Result<_, WalError>>()?;
    Ok(Some(RecordBatch::try_new(std::sync::Arc::new(arrow::datatypes::Schema::new(fields)), cols)?))
}
```

Three defences in one recursive fold: dictionaries are flattened up front (so
slicing actually divides the payload), a chunk that overshoots is re-split, and a
chunk that *barely shrank* despite holding a fraction of the rows bails out
explicitly rather than recursing toward a per-row explosion of near-full-size
entries. The `chunk_data.len() * 3 >= parent_len * 2` test is the
"shrinkage below ⅓ means something is shared" heuristic.

### Replay: bounded memory, timestamp-ordered

Replay is a pull iterator over a min-heap, and its invariant is stated precisely:

**Listing 11.14 — src/write/wal.rs:1224–1248**

```rust
/// See [`WalManager::replay_iter`]. Heap is keyed by `(timestamp, shard)` so
/// smaller timestamps come out first; shard index breaks ties
/// deterministically. The entry payload travels in a parallel Vec slot
/// indexed by shard, avoiding an `Ord` bound on `WalEntry`. Invariant: at
/// most one in-flight entry per shard is alive at a time → replay memory is
/// O(shards_per_topic), not O(total entries).
pub struct WalReplayIter<'a> {
    wal: &'a WalManager,
    topics: Vec<String>,
    topic_idx: usize,
    heap: std::collections::BinaryHeap<std::cmp::Reverse<(i64, usize)>>,
    shard_keys: Vec<String>,
    pending: Vec<Option<(WalEntry, WalPosition)>>,
    /// The (project, table) currently being drained (last topic primed).
    cur_topic: Option<(String, String)>,
    /// Entries yielded so far.
    pub total: u64,
    /// Corrupt/unreadable entries skipped so far.
    pub errors: usize,
    /// Wall-clock inside the walrus read (I/O + the copy out of the block).
    pub read_nanos: u128,
    /// Wall-clock decoding the WAL envelope — the bincode step that allocates
    /// and copies `WalEntry::data` before the Arrow decode ever sees it.
    pub envelope_nanos: u128,
}
```

`O(shards_per_topic)` replay memory rather than `O(total entries)` is what makes
recovery from a deep backlog possible at all — the previous `read_all_entries_raw`
materialized the whole slice, which at long retention is millions of entries and
gigabytes.

The heap is keyed by `(timestamp, shard)` with the payload in a parallel `Vec`
slot — a small but tidy trick that avoids requiring `Ord` on `WalEntry`.

The *frontier* is the concept that makes mid-replay watermarks safe:

**Listing 11.15 — src/write/wal.rs:1250–1261**

```rust
impl WalReplayIter<'_> {
    /// The topic currently being replayed and its per-shard *frontier* — the
    /// position of the next entry each shard will yield (`None` = that shard is
    /// exhausted). Everything strictly before `frontier[shard]` on that shard
    /// has already been yielded AND processed; the entry AT `frontier[shard]`
    /// (the prefetched `pending` entry) has not. This is the safe watermark
    /// baseline for the in-progress topic: the walrus read cursor sits one
    /// prefetched entry per shard *ahead* of it. `None` topic before the first
    /// `next_entry`.
    pub fn frontier(&self) -> (Option<(String, String)>, Vec<Option<WalPosition>>) {
        (self.cur_topic.clone(), self.pending.iter().map(|p| p.as_ref().map(|(_, pos)| *pos)).collect())
    }
```

Because the iterator prefetches one entry per shard, the walrus read cursor is
always *ahead* of what has actually been processed. The frontier is the honest
position, and mid-replay relief flushes use it rather than the cursor.

### The cursor snapshot

Every successful flush and every graceful shutdown writes a JSON snapshot of
every topic's per-shard cursor. Boot reads it to decide whether the expensive
Delta reconciliation scan can be skipped.

**Listing 11.16 — src/write/wal.rs:95–127**

```rust
/// Serialized form of every known topic's per-shard persisted-read cursor.
/// Written after every successful Delta flush + on graceful shutdown; read
/// on boot to skip the Delta scan when the cursor is known-current.
///
/// Correctness assumes this timefusion process is the **only** writer to its
/// Delta tables — `BufferedWriteLayer::flush` is the sole commit path. If you
/// ever run a parallel writer (manual `OPTIMIZE`, an external delta-rs
/// client, a sister process) between a clean-shutdown snapshot and the next
/// boot, delete `cursor_snapshot.json` to force a Delta reconciliation; the
/// `clean_shutdown` flag alone won't catch out-of-band commits.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CursorSnapshot {
    pub version: u32,
    /// Wall-clock micros (`support::now_micros`) at write time. Informational
    /// only — surfaced in the boot log so operators can spot a stale
    /// snapshot, but not enforced as a max-age gate.
    pub written_at_micros: i64,
    pub shards_per_topic: usize,
    /// True only when written by the graceful-shutdown path. Boot uses this
    /// flag to decide whether the Delta verifier can be skipped entirely.
    /// NOT a drain claim — shutdown writes it even after a partial/timed-out
    /// flush (the WAL holds the remainder); see `drained` for that.
    pub clean_shutdown: bool,
    /// True only when the shutdown flush left NOTHING un-flushed (no
    /// MemBuffer buckets, no airborne or orphaned holds). Sole authorizer of
    /// the pure-mtime boot WAL GC — with un-flushed data the old files may
    /// BE the backlog. `#[serde(default)]`: snapshots from older builds parse
    /// as drained=false, which just skips the boot sweep (safe direction).
    #[serde(default)]
    pub drained: bool,
    /// `"project_id:table_name"` → per-shard cursor (None = never written).
    pub entries: std::collections::BTreeMap<String, Vec<Option<SnapPos>>>,
}
```

The `clean_shutdown` versus `drained` distinction is exactly the kind of
two-similar-booleans situation that usually goes wrong, and here it is
disambiguated in the doc comments:

- **`clean_shutdown`** — "the shutdown path wrote this", true even after a
  partial or timed-out flush. Authorizes skipping the Delta cursor scan.
- **`drained`** — "nothing was left un-flushed": no MemBuffer buckets, no
  airborne holds, no orphaned holds. The *sole* authorizer of the boot WAL GC
  sweep, because with un-flushed data the old files may *be* the backlog.

`#[serde(default)]` on `drained` means an older snapshot parses as `false`, which
skips the sweep — the safe direction. Choosing which direction a missing field
defaults to is a real design decision and this one is annotated.

The stated single-writer assumption is worth flagging: if you ever run an
out-of-band delta-rs client against these tables, delete `cursor_snapshot.json`,
because `clean_shutdown` cannot detect commits this process did not make.

### The directory lock

Single-writer is enforced by an advisory `flock` held for the process lifetime:

**Listing 11.17 — src/write/wal.rs:1387–1410**

```rust
pub struct WalDirLock {
    // Held for the process lifetime; the flock releases when this drops (or the
    // process dies). Never read after construction — its liveness IS the lock.
    _file: std::fs::File,
}

impl WalDirLock {
    /// Acquire the exclusive WAL-dir lock, waiting with backoff until any other
    /// TimeFusion process holding it exits. Async so the early-bind 57P03
    /// responder keeps serving connections while we wait.
    pub async fn acquire(wal_dir: &std::path::Path) -> Result<Self, WalError> {
        let meta_dir = wal_dir.join(META_DIR);
        std::fs::create_dir_all(&meta_dir)?;
        let path = meta_dir.join("wal.lock");
        let file = std::fs::OpenOptions::new().create(true).read(true).write(true).truncate(false).open(&path)?;
        let mut waits = 0u64;
        loop {
            match file.try_lock_exclusive() {
                Ok(true) => {
                    clear_takeover_request(wal_dir);
                    if waits > 0 {
                        info!("WAL dir lock acquired after waiting for a previous process to exit");
                    }
                    return Ok(Self { _file: file });
                }
```

`_file` with the leading underscore and the comment "its liveness IS the lock" is
the idiomatic way to express a resource whose only purpose is its `Drop`. The
kernel releases a `flock` even on `SIGKILL`, which is why this is safe against a
hard kill.

The `Ok(false)` arm is where start-first deploys are made to work:

**Listing 11.18 — src/write/wal.rs:1412–1467**

```rust
                // Ok(false) = another live TimeFusion process owns the WAL.
                // Poll quickly during the brief start-first overlap so lock
                // transfer does not add a visible half-second outage. Log only
                // every ~10s (400 × 25ms). A normal handoff clears in
                // seconds; escalate to error past ~60s so a wedged predecessor
                // (readiness stays TCP-green, masking the stall) is loud, not a
                // silent hang. We still never steal — the orchestrator's
                // stop-grace SIGKILL is what bounds a truly stuck predecessor.
                Ok(false) => {
                    // In start-first mode the drained predecessor retains this
                    // lock while serving reads. This marker asks it to enter
                    // its normal graceful-exit path; it never authorizes this
                    // contender to touch WAL state before acquiring the lock.
                    if waits.is_multiple_of(400) {
                        let request = meta_dir.join(TAKEOVER_REQUEST_FILE);
                        // Written ONCE and never refreshed: the predecessor
                        // escalates on how long the request has been
                        // outstanding, and rewriting it every 10s reset that
                        // age to zero on every poll, so the escalation could
                        // never fire and an orphaned predecessor held the lock
                        // forever (2026-08-10: 47 minutes, six live containers).
                        if !request.is_file() {
                            let _ =
                                std::fs::write(&request, format!("pid={} requested_at_micros={}\n", std::process::id(), chrono::Utc::now().timestamp_micros()));
                        }
                        let secs = waits / 40;
                        if waits >= 2_400 {
                            error!(
                                "WAL dir {:?} still locked by another TimeFusion process after {secs}s — predecessor may be wedged (check for a stuck/duplicate instance)",
                                path
                            );
                        } else {
                            warn!("WAL dir {:?} is locked by another TimeFusion process; waiting for it to exit before recovery", path);
                        }
                    }
                    // Never spinning forever is the point. A predecessor that
                    // will never release — an orphaned container swarm has lost
                    // track of, so it is never sent SIGTERM — used to leave this
                    // process alive and half-started indefinitely, and every
                    // redeploy stacked another one onto the box until it ran out
                    // of memory. Exiting non-zero instead turns a silent
                    // permanent wedge into an ordinary crash-loop the
                    // orchestrator backs off and an operator can see.
                    if waits >= LOCK_WAIT_GIVE_UP.as_millis() as u64 / 25 {
                        return Err(WalError::LockContention(format!(
                            "WAL dir {path:?} still locked after {}s; giving up so this process restarts instead of \
                             occupying memory forever (look for an orphaned TimeFusion container holding the lock)",
                            LOCK_WAIT_GIVE_UP.as_secs()
                        )));
                    }
                    waits += 1;
                    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
                }
                Err(e) => return Err(WalError::Io(e)),
            }
```

Four distinct behaviours in one arm, each with a stated failure it prevents:

1. **A takeover request file**, written once. The predecessor polls for it and
   enters its graceful-exit path. This is what makes a *start-first* deploy work:
   the new container binds its listener and blocks on the lock, and the request
   tells the old one it may leave.
2. **Written ONCE, never refreshed.** The predecessor escalates on the request's
   *age*; rewriting it every 10 s reset the age to zero, so the escalation could
   never fire — and on 2026-08-10 an orphaned predecessor held the lock for 47
   minutes with six live containers stacked on the box.
3. **Escalating log level** at ~60 s, because readiness stays TCP-green (the
   early-bind responder answers) and would otherwise mask the stall entirely.
4. **A 900-second give-up** that exits non-zero. The reasoning is worth quoting
   because it inverts the usual instinct: a process that waits forever is *worse*
   than one that crashes, because a crash-loop is visible and the orchestrator
   backs off, while a permanent half-started wedge silently consumes memory and
   every redeploy stacks another one.

The corresponding predecessor-side logic lives in `main.rs`'s `takeover_signal`
(Chapter 22), and the age helper is here:

**Listing 11.19 — src/write/wal.rs:1471–1493**

```rust
pub fn takeover_requested(wal_dir: &std::path::Path) -> bool {
    wal_dir.join(META_DIR).join(TAKEOVER_REQUEST_FILE).is_file()
}

/// How long a takeover request has been outstanding, or `None` when none is.
///
/// The predecessor escalates on this: a request it keeps ignoring because it
/// never reaches handoff readiness is exactly the wedge this bounds.
pub fn takeover_request_age(wal_dir: &std::path::Path) -> Option<std::time::Duration> {
    let path = wal_dir.join(META_DIR).join(TAKEOVER_REQUEST_FILE);
    let requested_at =
        std::fs::read_to_string(&path).ok()?.split_whitespace().find_map(|field| field.strip_prefix("requested_at_micros=")?.parse::<i64>().ok())?;
    let elapsed = crate::support::now_micros().saturating_sub(requested_at).max(0);
    Some(std::time::Duration::from_micros(elapsed as u64))
}

pub fn clear_takeover_request(wal_dir: &std::path::Path) {
    match std::fs::remove_file(wal_dir.join(META_DIR).join(TAKEOVER_REQUEST_FILE)) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => warn!("could not clear WAL takeover request: {e}"),
    }
}
```

with the two timeouts stated together:

**Listing 11.20 — src/write/wal.rs:51–60**

```rust
/// How long a contender waits for the WAL lock before exiting non-zero. Far
/// beyond any real handoff (seconds), so only a predecessor that will NEVER
/// release trips it.
const LOCK_WAIT_GIVE_UP: std::time::Duration = std::time::Duration::from_secs(900);

/// How long the holder tolerates an outstanding takeover request before shutting
/// down even though it never reached handoff readiness. The graceful path is the
/// same one SIGTERM takes, and it is lossless — a measured `docker stop` of a
/// wedged instance drained in 23s.
pub const TAKEOVER_ESCALATE_AFTER: std::time::Duration = std::time::Duration::from_secs(180);
```

### Boot WAL garbage collection

**Listing 11.21 — src/write/wal.rs:1495–1504 (doc comment)**

```rust
/// Pre-walrus boot WAL GC, shared by `main.rs` and the e2e `bootstrap()`.
///
/// Deletes dead files before walrus enumerates the dir (accumulated leaks
/// dominated startup — 467 GB / 12-min boot, see `wal_bloat_startup.md`).
/// A complete sweep is sound ONLY when the previous life's shutdown flush
/// fully drained (snapshot `drained=true`): otherwise the old files may BE
/// the un-flushed backlog (2026-07-08 acked-write loss). After sweeping, the
/// drained claim is consumed (rewritten false): this life will accept new
/// acked writes, and if it crashes before its first successful flush the
/// stale claim must not authorize the NEXT boot's sweep. Dirty/undrained
```

"The drained claim is **consumed**" is the important idea: a boolean that
authorizes a destructive action must be single-use, or a crash after the sweep
would let the next boot sweep again on stale evidence.

### The metadata directory

Everything TimeFusion keeps alongside walrus's own data lives under
`.timefusion_meta/`, and the GC skips it:

**Listing 11.22 — src/write/wal.rs:44–65**

```rust
/// Magic bytes to identify the WAL format ("WAL2").
/// TimeFusion's own metadata directory, kept alongside the walrus data files
/// (topic list, WAL version stamp, cursor snapshot, dedup dirty bins, delta
/// snapshots). Skipped by WAL GC.
pub const META_DIR: &str = ".timefusion_meta";
const TAKEOVER_REQUEST_FILE: &str = "takeover.request";
// ... elided: LOCK_WAIT_GIVE_UP and TAKEOVER_ESCALATE_AFTER (Listing 11.20)

/// `<data_dir>/.timefusion_meta/<file>`.
pub fn meta_path(data_dir: &Path, file: &str) -> PathBuf {
    data_dir.join(META_DIR).join(file)
}
```

Contents, by file:

| File | Written by | Read by |
|---|---|---|
| `wal.lock` | `WalDirLock::acquire` | every process, as the flock target |
| `takeover.request` | a blocked contender | the lock holder's `takeover_signal` |
| `topics` | `persist_topic` (append) | replay, to enumerate topics |
| `cursor_snapshot.json` | flush + graceful shutdown | boot, to skip the Delta scan |
| `recovery_rewind.json` | `write_recovery_rewind_marker` | resumable replay after a crash |
| WAL version stamp | first open | `check_wal_version_stamp` |
| dedup dirty bins, delta snapshots | `storage.rs` sidecars | maintenance, boot |

⚠️ Note the doc-comment placement bug at `src/write/wal.rs:44–48`: the
`/// Magic bytes to identify the WAL format ("WAL2").` line is attached to
`META_DIR`, not to `WAL_MAGIC` (which is declared 20 lines later). Harmless, but
it means `META_DIR`'s rustdoc opens with a sentence about magic bytes.

### The error type

Eleven variants, and the one non-obvious annotation is on the lock:

**Listing 11.23 — src/write/wal.rs:16–42**

```rust
#[derive(Debug, Error)]
pub enum WalError {
    #[error("WAL entry too short: {len} bytes")]
    TooShort { len: usize },
    #[error("Batch too large: {size} bytes exceeds max {max}")]
    BatchTooLarge { size: usize, max: usize },
    #[error("Invalid WAL operation type: {0}")]
    InvalidOperation(u8),
    #[error("Unsupported WAL version: {version} (expected {expected})")]
    UnsupportedVersion { version: u8, expected: u8 },
    /// The WAL lock was never released. Fatal on purpose: a process that cannot
    /// own the WAL must exit rather than linger half-started.
    #[error("{0}")]
    LockContention(String),
    #[error("Bincode decode error: {0}")]
    BincodeDecode(#[from] bincode::error::DecodeError),
    #[error("Bincode encode error: {0}")]
    BincodeEncode(#[from] bincode::error::EncodeError),
    #[error("Arrow IPC error: {0}")]
    ArrowIpc(#[from] arrow::error::ArrowError),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("No record batch found in data")]
    EmptyBatch,
    #[error("Internal WAL invariant violated: {0}")]
    Internal(String),
}
```

### How to debug this flow

**On disk.** `<TIMEFUSION_DATA_DIR>/wal/` holds walrus's block files;
`<…>/wal/.timefusion_meta/` holds the table above. `cat .timefusion_meta/topics`
lists every `(project, table)` the process has ever written.
`cat .timefusion_meta/cursor_snapshot.json | jq '.clean_shutdown, .drained,
.written_at_micros'` answers "will the next boot skip the Delta scan, and will it
sweep?".

**Boot log lines**, in order:

```
bootstrap.phase=cursor_snapshot skip_delta_scan=… clean_snapshot=… local_wal_consumed=… elapsed_ms=…
Cursor snapshot restored: N table(s) seeded, M table(s) advanced, clean_shutdown=…, age=…s
bootstrap.phase=delta_cursor_reconcile elapsed_ms=…      (only when not skipped)
bootstrap.phase=wal_replay entries=… elapsed_ms=…
```

A long `wal_replay` means the previous life did not flush before exiting — run
`FLUSH` before a planned restart.

**Lock contention.**
`"WAL dir … is locked by another TimeFusion process"` every ~10 s is a normal
start-first overlap for a few seconds. The same line escalating to `error!` after
60 s means the predecessor is wedged. After 900 s the contender exits non-zero and
the orchestrator crash-loops it — deliberately.

**`timefusion_stats`** exposes `wal_files`, `wal_disk_bytes`, `quarantine_files`,
`quarantine_bytes`, `wal_shards_per_topic`, `wal_known_topics`,
`orphaned_topics`, `orphan_pin_age`. Non-zero `quarantine_files` means an entry
could not be decoded and is parked for `timefusion redrive-dml`; the field comment
says to treat it as deferred data loss. A growing `orphan_pin_age` means WAL files
are pinned by rows a failed commit could not restore — a restart is due before the
disk fills.

**Key takeaways.** The WAL is a six-byte header plus a bincode `WalEntry` whose
`data` is Arrow IPC (inserts) or a bincode DML payload. Topics are
`(project, table)`, sharded N ways over walrus collections to escape its
per-collection write lock, with FNV keys that must stay stable across builds. The
central safety property is the hold-registration callback firing under the append
lock *before* the entry exists, which makes it impossible for a cursor advance to
see an entry without seeing its hold. Single-writer is a process-lifetime
`flock`; start-first deploys work through a write-once takeover request whose
*age* drives the predecessor's escalation. Replay is a per-shard heap merge with
`O(shards)` memory, and the boot GC sweep is authorized only by a single-use
`drained` claim.
## 12. The MemBuffer and the flush to Delta

*After this chapter you will know the MemBuffer's three-level structure, why a
bucket is 5 or 10 minutes wide, how memory is accounted (and the two ways that
accounting has gone wrong), what a bucket's WAL holds are, and how the background
flush task turns buckets into Delta commits — including the pressure escalation
ladder and the emergency WAL drain.*

### The structure

Three levels, all keyed for lock-free reads:

```
MemBuffer
 └─ tables: DashMap<TableKey, Arc<TableBuffer>>          TableKey = (Arc<str>, Arc<str>)
     └─ buckets: DashMap<i64, TimeBucket>                bucket_id = ts / bucket_duration
         ├─ batches: Mutex<Vec<RecordBatch>>
         ├─ row_count / memory_bytes / min_ts / max_ts   (atomics)
         ├─ created_micros                               (flush-dwell signal)
         ├─ wal_shard_state                              (the cursor holds)
         ├─ flush_pinned_prefix / mutation_gen           (snapshot-flush safety)
         └─ last_wal_pin_micros / first_wal_pin_micros   (WAL GC floor)
```

**Listing 12.1 — src/write/mem_buffer.rs:248–253**

```rust
pub type TableKey = (Arc<str>, Arc<str>);

#[inline]
pub fn table_key(project_id: &str, table_name: &str) -> TableKey {
    (Arc::from(project_id), Arc::from(table_name))
}
```

`Arc<str>` rather than `String` because the key is cloned constantly — into the
text-index LRU, into `force_flushed`, into every hold registration — and an
`Arc<str>` clone is a refcount bump.

### Bucket width

**Listing 12.2 — src/write/mem_buffer.rs:30–74**

```rust
// Note: Timestamps before 1970 (negative microseconds) produce negative bucket IDs,
// which is supported but may result in unexpected ordering if mixed with post-1970 data.
// Fallback when `set_bucket_duration_micros` is never called (i.e. unit tests
// that build a MemBuffer directly). MUST track `d_bucket_duration_secs` in
// config.rs — prod always overrides via bootstrap, but keeping the two in sync
// avoids the test-only `BUCKET_DURATION_MICROS` const diverging from the
// process-global runtime value once any test pins the OnceLock.
const DEFAULT_BUCKET_DURATION_MICROS: i64 = 5 * 60 * 1_000_000;
#[cfg(test)]
const BUCKET_DURATION_MICROS: i64 = DEFAULT_BUCKET_DURATION_MICROS;

static BUCKET_DURATION_MICROS_CFG: std::sync::OnceLock<i64> = std::sync::OnceLock::new();

// ... elided: MAX_BATCH_COUNT_PER_BUCKET and MAX_BATCH_BYTES_FOR_COALESCE,
// quoted below

/// Configured bucket window in microseconds. Set once at startup via
/// `set_bucket_duration_micros`; defaults to 5 minutes when unset. Smaller
/// windows free MemBuffer memory sooner (because the previous bucket becomes
/// flushable sooner) at the cost of more, smaller Delta commits.
pub fn bucket_duration_micros() -> i64 {
    *BUCKET_DURATION_MICROS_CFG.get_or_init(|| DEFAULT_BUCKET_DURATION_MICROS)
}

/// Set the bucket window. No-op after the first call (OnceLock). Must be
/// invoked before any MemBuffer activity, e.g. from `init_config`.
pub fn set_bucket_duration_micros(micros: i64) {
    let _ = BUCKET_DURATION_MICROS_CFG.set(micros.max(1_000_000));
}
```

⚠️ `CLAUDE.md` states `BUCKET_DURATION_MICROS = 10 * 60 * 1_000_000` and describes
"10-minute time buckets" throughout. The source's compiled-in default is **5
minutes**, and the real value is a runtime `OnceLock` set from config at startup.
The trade the doc comment names is the whole story: a smaller window frees
MemBuffer memory sooner because the previous bucket becomes flushable sooner, at
the cost of more, smaller Delta commits. Read the boot log, not either document,
for what production is actually running.

### Insert-time coalescing

A bucket accumulates `RecordBatch`es. Too many, and every read pays per-batch
Arrow overhead; coalescing them costs a `concat_batches` under the bucket lock.
Two constants tune the trade:

**Listing 12.3 — src/write/mem_buffer.rs:43–60**

```rust
/// Hard cap on RecordBatch count per TimeBucket. Insert just pushes; when
/// the bucket crosses this threshold, one insert pays an amortized coalesce
/// (all batches → one). 8 is the sweet spot at prod scale: lower means more
/// concat work per insert (but each concat is cheap, since batches are
/// small), higher means each read scans more RecordBatches with per-batch
/// Arrow overhead. Empirically, dropping from 32→8 cut p95 at 200-project
/// load from 240ms to ~80ms.
const MAX_BATCH_COUNT_PER_BUCKET: usize = 8;
/// Skip the in-lock coalesce when the bucket's combined payload exceeds
/// this many bytes. The point of coalesce is to bound query-side
/// per-bucket batch fanout for sub-ms reads on bursty small-INSERT
/// workloads — once a bucket already holds a multi-megabyte payload the
/// per-query iteration overhead is already dwarfed by the data work, and
/// `concat_batches` on tens of MB would hold the bucket lock for
/// milliseconds, starving every concurrent reader of that bucket. 4 MB
/// matches one Arrow IPC default block; arrived at empirically — see
/// `tests/membuffer_concurrency_bench.rs`.
const MAX_BATCH_BYTES_FOR_COALESCE: usize = 4 * 1024 * 1024;
```

32 → 8 took p95 from 240 ms to ~80 ms at 200-project load. And the byte cap exists
because the *benefit* of coalescing (bounded per-query batch fanout) saturates
while the *cost* (lock hold time) grows linearly — so above 4 MB the coalesce is
pure loss.

### Memory accounting, and its two failure modes

The single most carefully documented field in the codebase:

**Listing 12.4 — src/write/mem_buffer.rs:255–298**

```rust
pub struct MemBuffer {
    /// Flattened structure: (project_id, table_name) → TableBuffer
    /// Reduces 3 hash lookups to 1 for table access.
    tables: DashMap<TableKey, Arc<TableBuffer>>,
    /// Running total of in-memory bytes across all live buckets — the value
    /// [`MemBuffer::estimated_memory_bytes`] returns, and therefore what the
    /// memory-reservation CAS, `pressure_pct`, and `unflushed_backlog_bytes`
    /// all read. Maintained by DELTA at every site that changes any bucket's
    /// `memory_bytes`, so it must stay in step with the sum of those atomics.
    ///
    /// **Exhaustive list of mutation sites** (the historical bug was a missed
    /// site — grep `memory_bytes` before adding one and update this list):
    /// 1. `MemBuffer::insert_with_hold` — adds the net delta returned by
    ///    `TableBuffer::insert_batch` (insert size *plus* the coalesce
    ///    shrinkage, see 2).
    /// 2. `TableBuffer::insert_batch` — `+= new_size` on push, then on
    ///    coalesce `+= combined_size - folded_size` (negative). Both are
    ///    folded into its returned net delta; the caller applies it. NOT
    ///    re-derived — this is exactly what the old cache got wrong.
    /// 3. `MemBuffer::insert_batches` — sums the net deltas of 2.
    /// 4. `finish_flushed_snapshot` prefix drain — `-= applied`, the value
    ///    actually subtracted from the bucket (clamped identically), and
    ///    `-= residual` of the bucket if the drain removes it.
    /// 5. `take_bucket_for_flush` — `-= swap(0)`; plus the residual of the
    ///    bucket if the empty shell is removed.
    /// 6. `restore_bucket` (failed-commit restore) — `+= added`.
    /// 7. `evict_old_data` — `-= sum(memory_bytes)` of removed buckets.
    /// 8. `reap_expired_empty_buckets` — `-= residual` of removed shells.
    /// 9. `delete` (DML mem leg) — `-= total_freed`.
    /// 10. `update` / `update_with_source` (DML mem legs) — signed delta.
    /// 11. `clear` — `store(0)` alongside `tables.clear()`.
    ///
    /// **Drift is bounded, never permanent.** Every subtraction goes through
    /// [`apply_signed_delta`], which saturates at 0: an earlier version used
    /// raw `fetch_sub` and a single mismatched subtraction wrapped the counter
    /// to ~948 GB against a 44 GB budget, after which *every*
    /// `try_reserve_memory` failed with "Memory limit exceeded" until restart.
    /// Saturation makes that failure mode under-report instead of wedging
    /// ingest, and [`MemBuffer::reconcile_estimated_bytes`] — called once per
    /// flush-task timer tick — recomputes the authoritative sum, stores it,
    /// and warns if the drift exceeded a few percent. So a future missed
    /// mutation site shows up as a warn log within a minute rather than as a
    /// permanent outage.
    estimated_bytes: AtomicUsize,
```

This is what a cached aggregate should look like when correctness matters:

1. **An exhaustive, numbered list of every mutation site**, with an instruction
   ("grep `memory_bytes` before adding one and update this list").
2. **Saturating arithmetic**, because the failure mode of unsaturated
   `fetch_sub` was catastrophic: one mismatched subtraction wrapped an unsigned
   counter to ~948 GB against a 44 GB budget, and *every subsequent reservation
   failed* until restart. Saturation converts a wedge into an under-report.
3. **Periodic reconciliation** against the authoritative sum, once per flush
   timer tick, which logs a warning if the drift exceeded a few percent — so a
   missed site surfaces within a minute instead of never.

The reconciliation call site, in the flush task:

**Listing 12.5 — src/write/mod.rs:2072–2081**

```rust
            } else {
                // Once per timer tick (NOT on pressure wakeups, which can be
                // frequent): re-derive MemBuffer's memory total from the bucket
                // atomics so any accounting drift is bounded by one interval
                // instead of accumulating. The hot path reads a cached atomic;
                // this is the O(tables × buckets) sweep that used to run on
                // every reservation attempt. Warns on non-trivial drift — see
                // `MemBuffer::reconcile_estimated_bytes`.
                self.mem_buffer.reconcile_estimated_bytes();
            }
```

Note the pairing with Chapter 10: this `O(tables × buckets)` sweep is the *same
computation* that used to run inside the reservation CAS loop and cost 10.3% of
process CPU. It was moved from "every attempt" to "once per timer tick", and a
cached atomic took its place on the hot path. The cache needed the mutation-site
discipline above to be safe; the reconciliation is the safety net under the cache.

The rest of `MemBuffer`'s fields, each solving a specific hazard:

**Listing 12.6 — src/write/mem_buffer.rs:299–341**

```rust
    /// Mirrors `WalManager::shards_per_topic` so `FlushableBucket.wal_first_positions`
    /// is always sized correctly when snapshotted at seal time.
    shards_per_topic: usize,
    /// LRU cache of per-bucket tantivy indexes. Lives at the MemBuffer
    /// level (not on individual TimeBuckets) so the LRU has a global view
    /// for byte-budget eviction. Entries are dropped:
    /// - when `text_index_max_bytes` is exceeded (LRU-evict tail)
    /// - when the bucket receives an insert (cache_invalidate by key)
    /// - when the bucket drains/evicts (cache_invalidate by key)
    text_index_cache: parking_lot::Mutex<lru::LruCache<BucketCacheKey, Arc<crate::tantivy::BucketTextIndex>>>,
    /// Sum of `size_bytes` across cached entries. Kept in an atomic so the
    /// hot insert path can do a single load to check "over budget?" without
    /// taking the LRU mutex.
    text_index_bytes: AtomicUsize,
    // ... elided: text_index_max_bytes (auto-tuned from buffer_max_memory_mb)
    /// (project_id, table_name) → bucket_ids whose rows were force-flushed
    /// to Delta while the bucket was open. Such a bucket's window holds rows
    /// legitimately in *both* stores (disjoint sets — force-flush removes
    /// rows from MemBuffer before committing), so it must stay exempt from
    /// the Delta-scan exclusion for its whole lifetime, not just while
    /// current (2026-06-11: the sealed exclusion + a 2h flush backlog masked
    /// force-flushed Delta rows for 2h). Kept at MemBuffer level — not on
    /// TableBuffer/TimeBucket — so the mark survives empty-bucket reclaim in
    /// `take_bucket_for_flush` and bucket/table re-creation by later inserts.
    /// Pruned on drain and eviction.
    force_flushed: DashMap<TableKey, std::collections::HashSet<i64>>,
    /// GC-floor pins of buckets mid-take: `take_bucket_for_flush` removes a
    /// bucket (and its `first_wal_pin_micros`) from the tables map before the
    /// flush path registers its inflight pin; a GC sweep sampling the floor
    /// in that gap would miss the airborne bucket and could delete its
    /// backing WAL file. The pin parks here from before the removal until
    /// [`Self::release_taking_pin`]. Keyed by `taking_seq`.
    taking_pins: DashMap<u64, i64>,
    taking_seq: AtomicU64,
    /// WAL-replay DML entries consumed as no-ops because their table had no
    /// buffered rows (already flushed / drained mid-replay). Surfaced in
    /// `timefusion_stats` — this replaced the quarantine file that used to be
    /// this loss-class's on-disk canary, so growth here is the re-drive signal.
    replay_dml_noops: AtomicU64,
}
```

Two of these are pure race-window plugs, and both have the same shape:
**something is removed from one place before being registered in another, and a
concurrent observer sampling in the gap draws a wrong conclusion.**
`taking_pins` covers the gap between removing a bucket and registering its
in-flight WAL pin. `force_flushed` lives at MemBuffer level rather than on the
bucket precisely so it survives the bucket being reclaimed.

### `TimeBucket`: the unit of flush

**Listing 12.7 — src/write/mem_buffer.rs:360–407**

```rust
pub struct TimeBucket {
    batches: Mutex<Vec<RecordBatch>>,
    row_count: AtomicUsize,
    memory_bytes: AtomicUsize,
    min_timestamp: AtomicI64,
    max_timestamp: AtomicI64,
    /// Wall-clock micros (via `crate::support`) when this bucket was created.
    /// Drives the flush-dwell staleness signal — how long the bucket has
    /// waited to flush — independent of its rows' event-time range, so
    /// backfilled/late data can't false-trip the "oldest bucket" alarm.
    created_micros: i64,
    /// Per-shard walrus positions captured BEFORE this bucket's first WAL
    /// entry on each shard (min-merged). These are the bucket's read-cursor
    /// *holds*: while the bucket is unflushed, the cursor must not advance
    /// past `first_positions[shard]`, or a crash would replay past this
    /// bucket's acked entries and lose them (prod 2026-07-03).
    wal_shard_state: Mutex<WalShardState>,
    /// While a flush snapshot is airborne, the first N batches are the
    /// snapshot's prefix: insert-time coalesce must not fold across this
    /// boundary or the post-commit prefix drain would remove late
    /// (unflushed) rows merged into a combined batch. 0 = no snapshot in
    /// flight.
    flush_pinned_prefix: AtomicUsize,
    /// Bumped by every in-place DML mutation of this bucket's batches (under
    /// the batches lock). A flush snapshot captures it; if it changed by
    /// commit time, the commit landed pre-DML row values and the prefix
    /// indices may have shifted (DELETE drops emptied batches), so
    /// `finish_flushed_snapshot` must NOT drain — the bucket re-flushes
    /// whole next cycle with the post-DML values.
    mutation_gen: AtomicU64,
    /// Wall-clock micros of the newest WAL entry pinned on this bucket
    /// (WalEntry timestamps are append-time, so this is ARRIVAL time).
    /// Drives [`MemBuffer::reap_expired_empty_buckets`]'s grace period —
    /// see its doc for the (pair-netting) soundness argument; replay itself
    /// no longer filters by age.
    last_wal_pin_micros: AtomicI64,
    /// Real-clock micros of the OLDEST WAL append this bucket's un-flushed
    /// data may depend on — the WAL GC floor: no WAL file whose mtime is at
    /// or after `min(first_wal_pin)` across live buckets may be deleted.
    /// Stamped `Utc::now()` on live inserts and DML pins (the append just
    /// happened); replay buckets are additionally floored at the entry's
    /// original append time via [`MemBuffer::record_replay_hold`]. Event
    /// time is deliberately NOT used: a backfill of old events would drag
    /// the floor days back and suspend WAL GC for the backfill's duration.
    /// Deliberately real-clock (chrono, not `crate::support`) — it is compared
    /// against file mtimes.
    first_wal_pin_micros: AtomicI64,
}
```

Four different clocks appear in this one struct, and getting them mixed up is how
you break things:

| Field | Clock | Why |
|---|---|---|
| `min/max_timestamp` | **event time** | what the row says happened |
| `created_micros` | virtual (`crate::support`) | flush *dwell*, immune to backfill |
| `last_wal_pin_micros` | virtual | WAL entry arrival time |
| `first_wal_pin_micros` | **real** (`chrono`) | compared against file mtimes |

The comment on `first_wal_pin_micros` states the anti-pattern explicitly: using
event time would let a backfill of old events drag the GC floor days back and
suspend WAL GC for the backfill's duration. And it is deliberately *real* clock
rather than the virtual test clock, because it is compared against filesystem
mtimes, which the virtual clock does not control.

The two snapshot-safety fields deserve their own paragraph. When a flush takes a
*snapshot* of a bucket (rather than taking the whole bucket), it captures the
first N batches and leaves the bucket live for continuing inserts. Two things can
then go wrong, and each has a guard:

- **Insert-time coalescing could fold across the snapshot boundary**, merging
  unflushed late rows into a batch the post-commit drain would then remove.
  `flush_pinned_prefix` pins the boundary.
- **A DML could mutate the bucket's batches in place** while the commit is
  airborne, so the commit lands pre-DML values *and* the prefix indices may have
  shifted (a `DELETE` can empty and drop a batch). `mutation_gen` is captured at
  snapshot time; if it moved, the drain is skipped and the bucket re-flushes whole
  next cycle.

### The WAL holds, min-merged

**Listing 12.8 — src/write/mem_buffer.rs:415–435**

```rust
#[derive(Debug, Default, Clone)]
struct WalShardState {
    first_positions: Vec<Option<walrus_rust::WalPosition>>,
}

impl WalShardState {
    /// Min-merge a hold for `shard`, growing the vec as needed. The hold is a
    /// *floor*, so the earliest position ever seen for a shard wins.
    fn merge(&mut self, shard: usize, pos: walrus_rust::WalPosition) {
        if self.first_positions.len() <= shard {
            self.first_positions.resize(shard + 1, None);
        }
        self.first_positions[shard] = Some(self.first_positions[shard].map_or(pos, |prev| prev.min(pos)));
    }
}

/// Per-shard holds re-shaped to exactly `shards` entries (truncating extras,
/// padding with `None`) so watermark indices always line up.
fn pad_positions(src: &[Option<walrus_rust::WalPosition>], shards: usize) -> Vec<Option<walrus_rust::WalPosition>> {
    (0..shards).map(|i| src.get(i).copied().flatten()).collect()
}
```

A hold is a *floor*, so min-merge is the only correct combination: a bucket that
receives entries at positions 100 and 50 (different appends, different shards or
the same) must hold at 50.

### `FlushableBucket`: what a flush carries

**Listing 12.9 — src/write/mem_buffer.rs:437–467**

```rust
#[derive(Debug, Clone)]
pub struct FlushableBucket {
    pub project_id: String,
    pub table_name: String,
    pub bucket_id: i64,
    pub batches: Vec<RecordBatch>,
    pub row_count: usize,
    /// Per-shard positions BEFORE this bucket's first WAL entry — the bucket's
    /// read-cursor holds. Registered as in-flight holds while the flush is
    /// airborne; restored to the bucket if the Delta commit fails.
    pub wal_first_positions: Vec<Option<walrus_rust::WalPosition>>,
    /// `mutation_gen` at snapshot time (snapshot-flush path only). If the
    /// bucket's gen moved by commit time, a DML mutated it mid-flight: the
    /// commit landed pre-DML values, so `finish_flushed_snapshot` must keep
    /// the rows and re-flush instead of draining.
    pub snapshot_gen: u64,
    /// Actual min/max timestamp of the taken rows, captured before the source
    /// bucket's atomics were reset. `restore_taken_bucket` replays these so a
    /// restored bucket keeps its true time range (and stays visible to
    /// time-range pruning) rather than collapsing to the bucket's start.
    pub min_timestamp: i64,
    pub max_timestamp: i64,
    /// Source bucket's `first_wal_pin_micros` (WAL GC floor) — carried so an
    /// airborne take/commit keeps flooring the GC, and a failed commit's
    /// restore re-applies it.
    pub first_wal_pin_micros: i64,
    /// Key of this take's entry in `MemBuffer::taking_pins`; the flush path
    /// releases it via [`MemBuffer::release_taking_pin`] once its own
    /// inflight pin is registered.
    pub taking_pin_seq: u64,
}
```

Everything needed to *undo* the take is carried alongside the data: the holds, the
GC floor, the true timestamp range, and the mutation generation. That is what
makes a failed Delta commit recoverable — `restore_taken_bucket` puts the rows
back with their real time range rather than collapsing them to the bucket start,
which would make them invisible to time-range pruning.

### Schema compatibility and nullability alignment

A `TableBuffer`'s schema is fixed at creation. Incoming batches must be
compatible:

**Listing 12.10 — src/write/mem_buffer.rs:76–94**

```rust
/// Check if two schemas are compatible for merge.
/// Compatible means: all existing fields must be present in incoming schema with same type,
/// incoming schema may have additional nullable fields.
fn schemas_compatible(existing: &SchemaRef, incoming: &SchemaRef) -> bool {
    // Every existing field must survive with a compatible type (nullability may loosen).
    if !existing.fields().iter().all(|f| incoming.field_with_name(f.name()).is_ok_and(|i| types_compatible(f.data_type(), i.data_type()))) {
        return false;
    }
    // New fields are OK only if nullable (SchemaMode::Merge) — a new NOT NULL
    // field would break the already-buffered rows.
    let new_fields = incoming.fields().iter().filter(|f| existing.field_with_name(f.name()).is_err()).collect::<Vec<_>>();
    if new_fields.iter().any(|f| !f.is_nullable()) {
        return false;
    }
    if !new_fields.is_empty() {
        info!("Schema evolution: {} new nullable field(s) added", new_fields.len());
    }
    true
}
```

There is a subtler problem than compatibility, though, and it is one of those
Arrow gotchas worth knowing:

**Listing 12.11 — src/write/mem_buffer.rs:96–115**

```rust
/// Take field nullability from the **declared** table schema wherever the data
/// honestly supports it, so one schema is authoritative end-to-end.
///
/// Arrow records nullability from how an array was *built*, not from whether it
/// holds nulls, so client and decode paths routinely hand us
/// `timestamp: nullable=true` for a column that is never null. That metadata
/// used to be pinned on the TableBuffer for the process lifetime (whatever the
/// first batch happened to say), ride into demoted hot-tier IPC files and the
/// flush path, and only surface far downstream as a mismatch against the
/// declared NOT NULL schema — as a hot-tier read miss, or as DataFusion's
/// `physical true vs logical false` aggregate error (prod 2026-07-31).
///
/// Only same-name, **exactly**-same-type fields are substituted, which leaves
/// variant columns (Utf8View SQL view vs Struct storage view) and any timezone
/// mismatch untouched. Tightening to NOT NULL additionally requires the column
/// to actually hold no nulls: a declared-NOT-NULL column that does contain
/// nulls stays nullable rather than asserting an invariant the data violates.
/// `null_count` is `None` when no data is available to check (schema-only
/// alignment), in which case the declared nullability is taken as given.
fn align_nullability(schema: &SchemaRef, declared: &SchemaRef, null_count: Option<&dyn Fn(usize) -> usize>) -> Option<SchemaRef> {
```

**Arrow nullability is a property of how the array was built, not of the data.**
A client library that always builds nullable arrays hands you
`timestamp: nullable=true` for a column that never holds a null — and if that
metadata gets pinned on the `TableBuffer` for the process lifetime (whichever
batch happened to arrive first), it rides into the hot-tier IPC files and the
flush path and only surfaces much later as DataFusion's `physical true vs logical
false` aggregate error. The fix makes the *declared* schema authoritative, but
only where the data honestly supports it: tightening to NOT NULL additionally
requires that the column actually holds no nulls.

### The flush task

One loop, four responsibilities:

**Listing 12.12 — src/write/mod.rs:2057–2143**

```rust
    async fn run_flush_task(&self) {
        let flush_interval = Duration::from_secs(self.config.buffer.flush_interval_secs());

        loop {
            let by_pressure = tokio::select! {
                _ = tokio::time::sleep(flush_interval) => false,
                _ = self.pressure_notify.notified() => true,
                _ = self.shutdown.cancelled() => {
                    info!("Flush task shutting down");
                    break;
                }
            };

            if by_pressure {
                debug!("Pressure-triggered flush at {}% (threshold {}%)", self.pressure_pct(), self.config.buffer.pressure_flush_pct());
            } else {
                // ... elided: the reconcile_estimated_bytes call, Listing 12.5
                self.mem_buffer.reconcile_estimated_bytes();
            }

            if let Err(e) = self.flush_completed_buckets().await {
                crate::observability::record_flush(false);
                self.note_flush_failure(1);
                error!("Flush task error: {}", e);
            }

            // Pressure escalation off the insert path: a single still-open
            // window can be the whole budget, which completed-bucket flushing
            // alone cannot reach. Drain until below the limit, until a round
            // commits nothing, or until the round cap. Gate on COMMIT
            // PROGRESS, not a byte delta: under
            // old-event-time backfill each flushed bucket is tiny, so the old
            // "<1% bytes freed → bail" quit at pressure=100 while hundreds of old
            // buckets were still draining slowly. As long as rounds keep
            // committing buckets (and ingest keeps adding flushable ones), keep
            // draining; only stop when a round commits nothing (completed buckets
            // gone or every commit failing/blocked — looping won't free RAM) so
            // we don't busy-spin when Delta is the bottleneck.
            const MAX_RELIEF_ROUNDS: u32 = 50;
            for _ in 0..MAX_RELIEF_ROUNDS {
                if !self.is_memory_pressure() {
                    break;
                }
                let before = self.flush_completed_total.load(Ordering::Relaxed);
                self.relieve_memory_pressure().await;
                if self.flush_completed_total.load(Ordering::Relaxed) == before {
                    error!(
                        "Pressure relief made no progress: used={}MB still over the limit — Delta flush committed nothing this round",
                        self.effective_memory_bytes() / (1024 * 1024)
                    );
                    break;
                }
            }
            // WAL monitoring: check file accumulation
            let (file_count, total_bytes) = self.wal.wal_stats();
            if !by_pressure {
                info!("WAL stats: {} files, {}MB", file_count, total_bytes / (1024 * 1024));
            }
            // Emergency drain if the WAL is over EITHER threshold: file count
            // (many small shards) or unflushed bytes (cursor-lag backlog the
            // memory-pressure valve misses — issue #83). flush_all_now advances
            // the read cursor so WAL GC can reclaim the backlog, keeping
            // restart replay bounded.
            if self.is_wal_over_threshold() {
                warn!(
                    "WAL over threshold (files {}/{}, unflushed {}MB/{}MB, disk {}MB), triggering emergency flush",
                    file_count,
                    self.config.effective_wal_max_files(),
                    self.unflushed_backlog_bytes() / (1024 * 1024),
                    self.config.effective_wal_max_unflushed_bytes() / (1024 * 1024),
                    total_bytes / (1024 * 1024)
                );
                if let Err(e) = self.flush_all_now().await {
                    error!("Emergency WAL flush failed: {}", e);
                }
            }
            // Test-hook signal: every iteration end (success or failure).
            // `notify_waiters` wakes all currently parked awaiters; if no
            // test is watching, the call is essentially free.
            self.flush_tick_notify.notify_waiters();
        }
    }
```

**Gate on commit progress, not on a byte delta.** The elided-then-quoted comment
records a real failure: the old rule was "if a round freed less than 1% of bytes,
bail", and under an old-event-time backfill each flushed bucket is tiny — so the
loop quit at `pressure=100` while hundreds of old buckets were still draining
slowly. Counting *commits* instead is the right invariant: as long as rounds keep
committing, keep going; stop only when a round commits nothing, because then
looping cannot free RAM.

`flush_tick_notify.notify_waiters()` at the end of every iteration is a
deliberate test hook — it lets the e2e harness `await_next_flush()` instead of
racing a wall-clock sleep. Building the observability of your own background loop
into the loop is what makes deterministic tests possible.

### What counts as "the backlog"

Two predicates gate maintenance and emergency flushing, and both had to be
rewritten after measuring the wrong thing:

**Listing 12.13 — src/write/mod.rs:2156–2183**

```rust
    /// Bytes accepted (already durable in the WAL) but NOT yet in Delta:
    /// MemBuffer's un-drained bucket bytes plus the reservation held for
    /// in-flight writes and airborne flush commits. This is the flush *backlog*.
    ///
    /// Deliberately NOT on-disk WAL size: that gauge is ingest-rate × trim
    /// retention (a workload property — ~30GB at 1GB/min healthy ingest), so it
    /// sits permanently above the 12GiB threshold and any brake reading it is
    /// permanently engaged (prod 2026-07-29: 105 projects planned, 0 completed,
    /// wal_yields on 10/10 ticks, while flush was provably healthy). Per-shard
    /// WAL cursor lag can't be measured cheaply (see `run_wal_gate_task`), so
    /// the unflushed-bytes accounting the memory valve already maintains is the
    /// honest proxy: it drops the moment flushes commit and only grows when
    /// drain genuinely falls behind ingest.
    pub fn unflushed_backlog_bytes(&self) -> u64 {
        self.effective_memory_bytes() as u64
    }

    /// The COMPACTION brake predicate: is durability genuinely behind?
    /// Compared against the same `effective_wal_max_unflushed_bytes` threshold
    /// the emergency flush uses — whose name already documented this intent.
    pub fn is_wal_backlog_over_threshold(&self) -> bool {
        wal_backlog_over_threshold(
            self.unflushed_backlog_bytes(),
            self.config.effective_wal_max_unflushed_bytes(),
            self.last_flush_failure_micros.load(Ordering::Relaxed),
            crate::support::now_micros(),
        )
    }
```

and:

**Listing 12.14 — src/write/mod.rs:2196–2213**

```rust
    /// The emergency-flush predicate: file sprawl OR a real unflushed backlog.
    /// The bytes leg compares UNFLUSHED bytes, not total on-disk size: on-disk
    /// residue is age/pin-bound (the GC floor), so a disk-based signal engages
    /// PERMANENTLY once residue exceeds the threshold — and here a permanently
    /// engaged signal means `flush_all_now` across all topics every ~60s,
    /// pounding the per-table commit locks and starving maintenance commits
    /// (2026-08-06 03:56Z: ~15GB flushed residue vs ~780MB unflushed; the
    /// storm froze the dedup drain for hours). "An extra flush is harmless"
    /// only holds when flushing can actually shrink the signal; it cannot
    /// shrink disk residue — only GC's age+floor can.
    pub fn is_wal_over_threshold(&self) -> bool {
        wal_emergency_flush_needed(
            self.wal.wal_stats().0,
            self.config.effective_wal_max_files(),
            self.unflushed_backlog_bytes(),
            self.config.effective_wal_max_unflushed_bytes(),
        )
    }
```

Both comments make the same argument from different angles, and it generalizes
well beyond this codebase:

> **A control signal must be something the control action can move.** On-disk WAL
> size cannot be reduced by flushing (only by GC, which is age- and pin-bound), so
> a brake reading it engages permanently. Unflushed bytes *do* drop the moment a
> flush commits, so they are the honest proxy.

The consequences of getting it wrong were measured twice: 105 projects planned and
zero completed with a permanently-engaged brake (2026-07-29), and a
flush-every-60s storm that froze the dedup drain for hours because 15 GB of
*flushed* residue kept a threshold tripped while only 780 MB was actually
unflushed (2026-08-06).

There is a third, harder signal:

**Listing 12.15 — src/write/mod.rs:2185–2194**

```rust
    /// Number of buckets that have already exceeded the configured hot-buffer
    /// retention and still have not landed in Delta.
    ///
    /// This is a stronger persistence-debt signal than bytes alone: a small,
    /// old bucket can sit below the WAL byte threshold indefinitely while
    /// maintenance rewrites keep winning shared S3/commit capacity. Scheduled
    /// optimization must yield until these buckets drain.
    pub fn stale_unflushed_bucket_count(&self) -> usize {
        self.mem_buffer.count_buckets_dwelling_since(self.retention_cutoff_micros())
    }
```

Bytes miss the case where a *small* bucket is starved indefinitely; counting
buckets past retention catches it.

### The WAL hard gate

The soft thresholds above only warn and flush. There is a separate hard breaker,
deliberately in its own task:

**Listing 12.16 — src/write/mod.rs:2215–2236**

```rust
    /// HARD WAL cap — a DISK-RUNAWAY breaker (2026-07-26 merge storm: WAL
    /// grew to 121GB while soft thresholds only WARNed). Own task, NOT the
    /// flush loop: the loop awaits flushes inline, so a stalled S3 flush —
    /// the exact overload mode this guards against — would delay engagement
    /// unboundedly. The gauge is total on-disk WAL bytes; that includes
    /// flushed segments the age-gated GC holds for ~90min plus every ACTIVE
    /// per-shard file, so the limit must sit far above the busy-hour residue
    /// (a catch-up burst measured ~56GB) — it exists to break unbounded
    /// growth, not to bound replay (recovery is cursor-bounded regardless).
    /// Per-shard cursor lag can't be measured cheaply: walrus block ids come
    /// from a shared pool (no per-column byte arithmetic) and active files
    /// always carry fresh mtimes. Rejected inserts land in the upstream DLQ
    /// and auto-replay once the gate clears.
    async fn run_wal_gate_task(&self) {
        let Some(hard) = self.config.buffer.wal_hard_limit_bytes() else { return };
        loop {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(15)) => {}
                _ = self.shutdown.cancelled() => return,
            }
            let (_, backlog) = self.wal.wal_stats();
            let over = backlog > hard;
            // ... elided: sets/clears `wal_hard_backpressure`, which `insert_bounded` reads
```

"Own task, NOT the flush loop" is the load-bearing sentence: the flush loop awaits
flushes inline, so a stalled S3 flush — precisely the overload this guards against
— would delay the breaker unboundedly. A safety mechanism must not depend on the
thing it protects against.

Note also that this breaker *does* read total on-disk bytes, which the previous
section argued against — and the comment explains why that is right here: this one
is not a control signal trying to shrink a backlog, it is a disk-runaway breaker.
Different job, different gauge.

### The payments span, flushed

By the time our example row leaves memory:

1. It was inserted into bucket `⌊1737123900000000 / 300000000⌋` for
   `("prod-api-001", "otel_logs_and_spans")`, with a hold registered at the
   pre-append position of whichever WAL shard the round-robin chose.
2. Up to seven more batches accumulated in that bucket; on the eighth, one insert
   paid an amortized `concat_batches` (unless the bucket already held over 4 MB).
3. Five minutes later the bucket sealed. The next flush tick (or a pressure
   wakeup) called `flush_completed_buckets`.
4. The bucket was taken (or snapshotted), its holds moved to
   `inflight_flush_holds`, its `taking_pin` parked in `taking_pins`, and its
   batches handed to the `DeltaWriteCallback` — which calls
   `Database::insert_records_batch`, sorts by the schema's sort keys, writes
   Parquet with a `sorting_columns` footer, and commits with the WAL watermark in
   the commit metadata.
5. `delta_flushed_watermark` for `("prod-api-001", "otel_logs_and_spans")` rose to
   at least `2025-01-17 14:25:00Z` **before** the commit, so no query can race
   between "the commit is visible" and "the watermark says so".
6. On success the bucket's memory was subtracted, the WAL holds released, the
   cursor snapshot rewritten, and (if the hot tier were enabled) the batches
   demoted to a local Arrow IPC file.
7. On failure the rows were restored with their true timestamp range, the holds
   went back onto the bucket, and the flush failure was noted — the WAL still
   holds them, and the next tick tries again.

### How to debug this flow

`timefusion_stats`, `component = 'mem_buffer'`:

- `mem_total_rows`, `mem_total_buckets`, `mem_estimated_bytes`, `pressure_pct`.
- `mem_buffer_oldest_bucket_age_seconds` — flush *dwell*, not event age. Alert
  above 2× the flush interval. Because it is derived from `created_micros`, a
  backfill of old events cannot false-trip it.
- `mem_replay_dml_noops` — the loss-class canary.

Log lines specific to this chapter:

- `"WAL stats: N files, M MB"` — once per timer tick (suppressed on pressure
  wakeups, so its cadence tells you whether flushes are timer- or
  pressure-driven).
- `"Pressure-triggered flush at N% (threshold M%)"` — at debug level.
- `"Pressure relief made no progress: … Delta flush committed nothing this
  round"` — Delta is the bottleneck; look at S3, not at memory.
- `"WAL over threshold (files …, unflushed …MB, disk …MB), triggering emergency
  flush"` — check *which* leg tripped, files or unflushed bytes.
- `"Schema evolution: N new nullable field(s) added"` — a client started sending a
  wider batch.
- A `reconcile_estimated_bytes` drift warning — someone added a `memory_bytes`
  mutation site without updating the accounting.

**Key takeaways.** The MemBuffer is a `DashMap` of tables of time buckets, with a
runtime-configurable bucket width (5 min compiled default, not the 10 min the docs
claim). Its memory total is a cached atomic maintained by delta at eleven
enumerated sites, saturating on subtraction and reconciled once per flush tick —
because an unsaturated `fetch_sub` once wrapped it to 948 GB and wedged all
ingest. A bucket carries four different clocks and two snapshot-safety
generations. The flush loop escalates on *commit progress* rather than bytes
freed, and every backlog predicate measures something a flush can actually
shrink — with the sole exception of the hard WAL breaker, which runs in its own
task precisely so a stalled flush cannot delay it.
## 13. The read path, part I: from socket to legs

*After this chapter you will be able to trace a `SELECT` from the TCP socket to
the union of execution-plan legs it becomes, function by function: the pgwire
handler, the admin-command interception, the statement timeout, the giant-statement
gate, and then all of `ProjectRoutingTable::scan` — the guard, the filter
stripping, the Tantivy prefilter, the dedup-skip decision, the projection
augmentation, the leg selection, and the union assembly.*

### The flow, before the code

Our query for this chapter is a realistic dashboard shape over the payments span:

```sql
SELECT timestamp, name, duration, status_code
FROM otel_logs_and_spans
WHERE project_id = 'prod-api-001'
  AND timestamp >= '2025-01-17 14:00:00' AND timestamp < '2025-01-17 15:00:00'
  AND resource___service___name = 'payments-api'
ORDER BY timestamp DESC
LIMIT 100;
```

The path it takes:

```
 1. TCP → pgwire startup → cleartext auth (ConfigAuthSource)
 2. Simple or extended query → LoggingSimpleQueryHandler::do_query
 3. rewrite_pg_synonyms  (ABORT→ROLLBACK, row_to_json AST rewrite)
 4. admin-command interception (OPTIMIZE/VACUUM/FLUSH/HANDOFF/DELTA *)  — no match
 5. giant_stmt_permit (statement < 2 MiB — no permit needed)
 6. statement timeout computed; applies (it is a SELECT, not DML)
 7. DfSessionService::do_query → hooks → DataFusion planning
 8.   analyzer rules (Variant, pg_catalog, tantivy, rollup routing…)  [Ch. 14]
 9.   optimizer rules; plan cache consulted                            [Ch. 14]
10. physical planning → ProjectRoutingTable::scan
11.   otel scan guard: is the query bounded?
12.   strip leg-unsafe filters (tombstone + mutable columns)
13.   timestamp → date partition filters
14.   extract project_id
15.   tantivy prefilter → id IN (...), file exclusions, row selections
16.   dedup-skip verdict (certification check)
17.   projection augmentation (dedup keys + tiebreak + tombstone ride in)
18.   limit suppression (dedup drops rows after the scan)
19.   leg selection: mem? hot? delta? skip-delta watermark?
20.   per-leg range exclusion so mem ∪ delta doesn't double-count
21.   union → SortPreservingMerge → DedupExec → tombstone filter
22. execution: GatedScanExec throttles decode                          [Ch. 7]
23. rows → DataRowEncoder → wire
```

Steps 1–7 are this section; 10–21 are the rest of the chapter; 8–9 are Chapter 14
and 21–22's dedup semantics are Chapter 15.

### The pgwire handler

`LoggingSimpleQueryHandler` wraps `datafusion-postgres`'s `DfSessionService`. Its
`do_query` is short and every line is a policy:

**Listing 13.1 — src/server/mod.rs:992–1047**

```rust
#[async_trait]
impl SimpleQueryHandler for LoggingSimpleQueryHandler {
    #[instrument(
        name = "postgres.query.simple",
        skip_all,
        fields(query.text = Empty, query.type = Empty, query.operation = Empty, db.system = "postgresql", db.operation = Empty)
    )]
    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let rewritten = rewrite_pg_synonyms(query);
        let query = rewritten.as_ref();

        // Admin commands, caught before DataFusion (whose parser rejects all
        // OPTIMIZE/VACUUM maintenance plus FLUSH and HANDOFF durability hooks.
        if let Some(cmd) = parse_optimize(query).map_err(admin_err)? {
            return self.run_optimize(cmd).await;
        }
        if let Some(cmd) = parse_vacuum(query).map_err(admin_err)? {
            return self.run_vacuum(cmd).await;
        }
        if let Some(cmd) = parse_delta_recovery_audit(query).map_err(admin_err)? {
            return self.run_delta_recovery_audit(cmd).await;
        }
        if let Some(cmd) = parse_delta_actions(query).map_err(admin_err)? {
            return self.run_delta_actions(cmd).await;
        }
        if let Some(cmd) = parse_delta_history(query).map_err(admin_err)? {
            return self.run_delta_history(cmd).await;
        }
        if parse_flush(query) {
            return self.run_flush().await;
        }
        if parse_handoff(query) {
            return self.run_handoff().await;
        }

        let span = tracing::Span::current();
        record_query_span(&span, query);

        let _giant = giant_stmt_permit(query.len()).await;
        let execute_span = tracing::trace_span!(parent: &span, "datafusion.execute");
        let t0 = std::time::Instant::now();
        let timeout = effective_statement_timeout(client_statement_timeout(client), self.max_statement_secs).filter(|_| statement_timeout_applies(query));
        let result =
            run_with_statement_timeout(timeout, <DfSessionService as SimpleQueryHandler>::do_query(&self.inner, client, query).instrument(execute_span))
                .await
                .map(|(responses, deadline)| with_response_deadlines(responses, deadline));
        record_statement_latency(self.scan_metrics.as_deref(), query, "simple", t0.elapsed().as_micros() as u64, result.is_ok());
        log_statement_failure("simple", &result);
        result
    }
}
```

Seven pieces of policy in fifty lines, and each is worth a paragraph.

**Postgres synonym rewriting.** DataFusion's SQL parser does not accept
`ABORT`, and Hasql's connection pool emits it defensively on every session
acquisition:

**Listing 13.2 — src/server/mod.rs:864–898**

```rust
/// Rewrites Postgres synonyms that DataFusion's SQL parser doesn't accept.
///
/// `ABORT [ WORK | TRANSACTION ]` is a Postgres alias for `ROLLBACK`. Hasql's
/// connection pool emits `ABORT` defensively on session acquisition to clear
/// any leftover transaction state; without this rewrite, every Hasql client
/// (e.g. monoscope) sees its first statement on each connection fail with
/// `sql parser error: Expected: an SQL statement, found: ABORT`, which then
/// poisons the whole session.
fn rewrite_pg_synonyms(query: &str) -> Cow<'_, str> {
    let query = strip_keyword(query.trim_start(), "ABORT", |c| c.is_whitespace() || c == ';')
        .map_or(Cow::Borrowed(query), |rest| Cow::Owned(format!("ROLLBACK{rest}")));
    rewrite_row_to_json_record(&query).map_or(query, Cow::Owned)
}

/// `row_to_json(t)` over a derived-table alias, which DataFusion rejects while
/// planning the SQL — before any analyzer rule could see it. pgAdmin's dashboard
/// polls that shape every 5s.
///
/// This is an AST rewrite, not a text substitution: the statement is parsed,
/// visited, and unparsed only if something actually changed. Anything that fails
/// to parse, or that the visitor declines to touch, is returned unchanged and
/// reaches DataFusion byte-for-byte as before — a malformed or unusual statement
/// can never be corrupted into a different valid statement by this path.
fn rewrite_row_to_json_record(query: &str) -> Option<String> {
    use datafusion::sql::sqlparser::{dialect::PostgreSqlDialect, parser::Parser};

    if !crate::read::optimizers::might_need_rewrite(query) {
        return None;
    }
    let mut statements = Parser::parse_sql(&PostgreSqlDialect {}, query).ok()?;
    let [statement] = statements.as_mut_slice() else {
        return None;
    };
    crate::read::optimizers::rewrite(statement).then(|| statement.to_string())
}
```

The `Cow` return is deliberate — the overwhelmingly common case borrows and
allocates nothing — and the `row_to_json` rewrite is careful in a way that
text-substitution rewrites usually are not: it parses, visits, and unparses only
if the visitor changed something, so a statement it does not understand reaches
DataFusion byte-for-byte.

The same rewrite must fire on the extended protocol too, and does, via a wrapper
`QueryParser`:

**Listing 13.3 — src/server/mod.rs:1082–1101**

```rust
/// Applies the same statement rewrites to the extended protocol that
/// `rewrite_pg_synonyms` applies to the simple one. pgAdmin's dashboard uses
/// simple queries, but a rewrite that fires on one protocol and not the other
/// would mean identical SQL succeeding or failing depending on how the client
/// sent it.
pub struct RewritingQueryParser {
    inner: Arc<<DfSessionService as ExtendedQueryHandler>::QueryParser>,
}

#[async_trait]
impl datafusion_postgres::pgwire::api::stmt::QueryParser for RewritingQueryParser {
    type Statement = <DfSessionService as ExtendedQueryHandler>::Statement;

    async fn parse_sql<C>(&self, client: &C, sql: &str, types: &[Option<datafusion_postgres::pgwire::api::Type>]) -> PgWireResult<Self::Statement>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let rewritten = rewrite_row_to_json_record(sql);
        self.inner.parse_sql(client, rewritten.as_deref().unwrap_or(sql), types).await
    }
    // ... elided: get_parameter_types / get_result_schema delegate to inner
}
```

**Admin commands.** TimeFusion extends SQL with seven statements DataFusion's
parser would reject outright. They are intercepted textually *before* planning.
Each parser returns `Result<Option<Cmd>, String>` with a deliberate three-way
meaning:

**Listing 13.4 — src/server/mod.rs:778–815**

```rust
/// Parse `OPTIMIZE <table> WHERE date = 'YYYY-MM-DD'`.
///
/// - `Ok(None)`: not an OPTIMIZE statement — fall through to DataFusion.
/// - `Ok(Some(_))`: valid, run it.
/// - `Err(msg)`: it *is* OPTIMIZE but malformed (no table, missing/non-`date`
///   filter, bad date). A bare `OPTIMIZE <table>` is rejected on purpose — an
///   unbounded in-process compaction can OOM the instance — and surfaced as a
///   clear error rather than a confusing DataFusion parser error.
pub(crate) fn parse_optimize(query: &str) -> Result<Option<OptimizeCmd>, String> {
    let Some(rest) = strip_command(query, "optimize") else { return Ok(None) };
    let (table, where_part) = rest.split_once(char::is_whitespace).map(|(t, w)| (t.trim(), w.trim())).unwrap_or((rest, ""));
    if table.is_empty() {
        return Err("OPTIMIZE requires a table and date: OPTIMIZE <table> WHERE date = 'YYYY-MM-DD'".to_string());
    }
    let Some(conds) = strip_keyword(where_part, "where", char::is_whitespace) else {
        return Err(format!(
            "OPTIMIZE {table} needs a date filter: OPTIMIZE {table} WHERE date = 'YYYY-MM-DD' (bare OPTIMIZE is disabled — it would compact all history in-process)"
        ));
    };
    // `WHERE date = '...'` optionally AND-ed (either order) with
    // `project_id = '...'`. Values are simple quoted literals, so splitting on
    // a top-level ` AND ` needs no nesting awareness.
    static AND: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?i)\s+and\s+").unwrap());
    let (date, project_id) = AND.split(conds.trim()).try_fold((None, None), |(date, project_id), cond| {
        let cond = cond.trim();
        let column = |name| strip_keyword(cond, name, |c: char| c.is_whitespace() || c == '=');
        match (column("date"), column("project_id")) {
            (Some(rest), _) => {
                let val = filter_value(rest)?;
                Ok((Some(val.parse::<chrono::NaiveDate>().map_err(|_| format!("invalid date '{val}', expected YYYY-MM-DD"))?), project_id))
            }
            (_, Some(rest)) => Ok((date, Some(filter_value(rest)?.to_string()))),
            _ => Err("OPTIMIZE supports only `date` and `project_id` filters".to_string()),
        }
    })?;
    let date = date.ok_or("OPTIMIZE requires a date filter: WHERE date = 'YYYY-MM-DD'")?;
    Ok(Some(OptimizeCmd { table: table.to_string(), date, project_id }))
}
```

Note that a *bare* `OPTIMIZE <table>` is a hard error rather than a whole-table
compaction, and the message says why. `OptimizeCmd`'s field doc carries the
incident:

**Listing 13.5 — src/server/mod.rs:737–747**

```rust
/// An intercepted `OPTIMIZE <table> WHERE date = 'YYYY-MM-DD'` admin command.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct OptimizeCmd {
    pub table: String,
    pub date: chrono::NaiveDate,
    /// Restrict the compaction to one tenant's partition. A whole-date
    /// optimize spans every project's files for that date — tens of GB on a
    /// busy day, which doesn't fit in-process next to serving load
    /// (2026-07-27: two OOMs). One (project, date) partition is a few GB.
    pub project_id: Option<String>,
}
```

The keyword matcher avoids the classic prefix-match bug:

**Listing 13.6 — src/server/mod.rs:759–776**

```rust
/// Remainder after a leading case-insensitive `keyword` that ends at end-of-input
/// or at a `boundary` char — so identifiers merely starting with it
/// (`optimizer_stats`, `aborted`) don't match. Remainder is returned untrimmed.
fn strip_keyword<'a>(s: &'a str, keyword: &str, boundary: fn(char) -> bool) -> Option<&'a str> {
    let (head, rest) = s.split_at_checked(keyword.len())?;
    (head.eq_ignore_ascii_case(keyword) && (rest.is_empty() || rest.starts_with(boundary))).then_some(rest)
}

/// Strip a leading admin keyword plus any trailing `;`, returning the trimmed
/// remainder. `None` when `query` isn't that command.
fn strip_command<'a>(query: &'a str, keyword: &str) -> Option<&'a str> {
    strip_keyword(query.trim().trim_end_matches(';').trim(), keyword, char::is_whitespace).map(str::trim)
}

/// `= '<value>'` → `<value>`, tolerating either quote style and loose spacing.
fn filter_value(rest: &str) -> Result<&str, String> {
    Ok(rest.trim().strip_prefix('=').ok_or("expected: <col> = '<value>'")?.trim().trim_matches(['\'', '"']).trim())
}
```

The seven admin commands, all read-only or explicitly operational:

| Statement | Effect | Guard |
|---|---|---|
| `OPTIMIZE t WHERE date = '…' [AND project_id = '…']` | compact one partition | date required |
| `VACUUM t [RETAIN n HOURS]` | delete tombstoned files | table required |
| `FLUSH` | drain the whole MemBuffer to Delta | 10 s rate limit |
| `HANDOFF` | lease a write fence + drain, for start-first deploys | — |
| `DELTA HISTORY t [LIMIT n]` | recent commit metadata | limit 1..10 000 |
| `DELTA ACTIONS t VERSION n` | every raw action in one commit | read-only |
| `DELTA RECOVERY AUDIT t VERSION n` | reconstruct Add actions a commit removed | fails unless every removal has a source |

`FLUSH` is the pre-deploy hook, and its rate limit exists because a looping client
would mint file-count explosion:

**Listing 13.7 — src/server/mod.rs:450–490**

```rust
    /// Execute an intercepted `FLUSH` — drain the whole MemBuffer to Delta.
    /// Ops pre-restart hook: run it right before a planned restart/deploy so
    /// the stop grace never bounds the shutdown flush and the boot's WAL
    /// replay is near-empty. Errors when any bucket fails so callers can
    /// gate on it.
    async fn run_flush(&self) -> PgWireResult<Vec<Response>> {
        let layer = require_available(self.db.as_ref().and_then(|d| d.buffered_layer()), "FLUSH")?;
        // Misuse guard: FLUSH commits the open window per table (tiny parquet
        // files + tantivy builds), so a looping client mints file-count
        // explosion and contends flush_lock with routine flushes. Operator
        // cadence is "once before a deploy" — enforce a floor between runs.
        // Frozen-clock (test) harnesses are exempt: their cadence is
        // script-driven and the frozen epoch resets between environments.
        use std::sync::atomic::{AtomicI64, Ordering};
        const FLUSH_MIN_INTERVAL_SECS: i64 = 10;
        static LAST_FLUSH_MICROS: AtomicI64 = AtomicI64::new(i64::MIN);
        if !crate::support::is_frozen() {
            let now = chrono::Utc::now().timestamp_micros();
            let since = now.saturating_sub(LAST_FLUSH_MICROS.load(Ordering::Acquire));
            if since < FLUSH_MIN_INTERVAL_SECS * 1_000_000 {
                return Err(admin_err(format!("FLUSH rate-limited: last ran {}s ago (min interval {FLUSH_MIN_INTERVAL_SECS}s)", since / 1_000_000)));
            }
            LAST_FLUSH_MICROS.store(now, Ordering::Release);
        }
        let stats = layer.flush_all_now().await.map_err(|e| admin_err(format!("FLUSH: {e}")))?;
        info!("pgwire FLUSH: {} bucket(s) flushed ({} rows), {} failed", stats.buckets_flushed, stats.total_rows, stats.buckets_failed);
        if stats.buckets_failed > 0 {
            return Err(admin_err(format!(
                "FLUSH: {} bucket(s) failed to flush ({} flushed) — data stays buffered/WAL-durable",
                stats.buckets_failed, stats.buckets_flushed
            )));
        }
        // Wake Walrus's safe reclaim worker and wait for its completion while
        // this instance is still serving, otherwise the replacement pays to
        // scan the outgoing instance's consumed WAL.
        // Frozen-clock test harnesses skip this operational handoff delay.
        if !crate::support::is_frozen() {
            layer.reclaim_wal_after_flush().await;
        }
        Ok(vec![Response::Execution(Tag::new(&format!("FLUSH {}", stats.total_rows)))])
    }
```

**The giant-statement gate.** Monoscope sends multi-megabyte `INSERT`s and
`unnest`-array enrichment `UPDATE`s. Planning and binding those materializes their
literals as `ScalarValue` arrays — transient heap that no pool tracks:

**Listing 13.8 — src/server/mod.rs:339–361**

```rust
/// Concurrent-giant-statement gate. A mega-statement (monoscope's multi-MB
/// INSERTs / unnest-array enrichment UPDATEs) materializes its literals and
/// bound parameters as ScalarValue arrays during plan + bind — tens to
/// hundreds of MB of transient heap per statement, bounded only by connection
/// concurrency. The 08:13Z 2026-08-03 OOM's pre-kill heap dumps were dominated
/// by exactly this (`ScalarValue::iter_to_array`/`make_run_array` under
/// pgwire). Two permits: one giant can always run while another queues, and
/// worst-case transient parse heap is 2x one statement instead of Nx.
const GIANT_STMT_BYTES: usize = 2 * 1024 * 1024;
static GIANT_STMT_SEM: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(2);

async fn giant_stmt_permit(len: usize) -> Option<tokio::sync::SemaphorePermit<'static>> {
    if len < GIANT_STMT_BYTES {
        return None;
    }
    let t0 = std::time::Instant::now();
    let permit = GIANT_STMT_SEM.acquire().await.expect("giant-stmt semaphore never closed");
    let waited = t0.elapsed();
    if waited.as_millis() > 50 {
        tracing::info!("giant statement ({len} B) queued {waited:?} behind the 2-permit parse gate");
    }
    Some(permit)
}
```

Two permits, not one — so a giant statement can always run while another queues,
and worst-case transient parse heap is 2× one statement rather than N×.

**The statement timeout, and why writes are exempt.** This is subtle and correct:

**Listing 13.9 — src/server/mod.rs:367–409**

```rust
/// Writes are exempt from the statement timeout.
///
/// `run_with_statement_timeout` enforces by DROPPING the in-flight future, and
/// the DML path runs its WAL append and Delta commit *inside* that future — so
/// cancelling a slow bulk INSERT or a MOR UPDATE reports failure to the client
/// for a write that is already partly durable, and it reappears on the next WAL
/// replay. PostgreSQL's `statement_timeout` aborts a statement transactionally;
/// dropping a future cannot, so the deadline only covers read-only statements.
/// `classify_query` errs toward matching DML, which errs toward no timeout.
fn statement_timeout_applies(query: &str) -> bool {
    let (kind, _) = classify_query(query);
    !matches!(kind, "DML" | "DDL")
}

async fn run_with_statement_timeout<T>(
    timeout: Option<std::time::Duration>, query: impl std::future::Future<Output = PgWireResult<T>>,
) -> PgWireResult<(T, Option<tokio::time::Instant>)> {
    let deadline = timeout.map(|timeout| tokio::time::Instant::now() + timeout);
    let result = match deadline {
        Some(deadline) => tokio::time::timeout_at(deadline, query).await.map_err(|_| statement_timeout_error())?,
        None => query.await,
    }?;
    Ok((result, deadline))
}

fn with_response_deadline(response: Response, deadline: Option<tokio::time::Instant>) -> Response {
    match (response, deadline) {
        (Response::Query(QueryResponse { command_tag, row_schema, data_rows, .. }), Some(deadline)) => {
            let data_rows = stream::unfold(Some(data_rows), move |rows| async move {
                let mut rows = rows?;
                match tokio::time::timeout_at(deadline, rows.next()).await {
                    Ok(Some(row)) => Some((row, Some(rows))),
                    Ok(None) => None,
                    Err(_) => Some((Err(statement_timeout_error()), None)),
                }
            });
            let mut response = QueryResponse::new(row_schema, data_rows);
            response.set_command_tag(&command_tag);
            Response::Query(response)
        }
        (response, _) => response,
    }
}
```

Two halves: the *planning and execution* future gets a `timeout_at`, and the
resulting *row stream* is re-wrapped so the same deadline also bounds streaming.
Without the second half a query that plans fast and streams slowly would run
forever.

`classify_query` is a first-match-wins keyword scan, and its ordering is
load-bearing:

**Listing 13.10 — src/server/mod.rs:900–918**

```rust
/// (keyword, space-padded keyword, `query.type`, operation). First match wins,
/// so order is significant.
const QUERY_KINDS: [(&str, &str, &str, &str); 7] = [
    ("select", " select ", "SELECT", "SELECT"),
    ("update", " update ", "DML", "UPDATE"),
    ("delete", " delete ", "DML", "DELETE"),
    ("insert", " insert ", "DML", "INSERT"),
    ("create", " create ", "DDL", "CREATE"),
    ("drop", " drop ", "DDL", "DROP"),
    ("alter", " alter ", "DDL", "ALTER"),
];

fn classify_query(query: &str) -> (&'static str, &'static str) {
    let q = query.trim().to_lowercase();
    QUERY_KINDS
        .iter()
        .find(|(kw, padded, ..)| q.starts_with(kw) || q.contains(padded))
        .map_or(("OTHER", "UNKNOWN"), |&(.., query_type, operation)| (query_type, operation))
}
```

⚠️ This classifier is heuristic and it knows it. `INSERT INTO t SELECT …` starts
with `insert` and is classified DML, correctly. But a `SELECT` whose text contains
` update ` inside a string literal would be misclassified as DML — which errs
toward *no timeout*, which the doc comment above calls the safe direction. Worth
knowing when reading `pgwire.slow_statement` events.

**Slow-statement logging, without leaking SQL.** A query that takes over a second
emits one bounded event. Literals are redacted and the fingerprint is stable
across parameter values:

**Listing 13.11 — src/server/mod.rs:920–990**

```rust
/// Redact literal values and comments so the result can safely be indexed and
/// used as a stable query fingerprint.
fn normalized_query(query: &str) -> String {
    static BLOCK_COMMENT: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?s)/\*.*?\*/").unwrap());
    static LINE_COMMENT: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"--[^\r\n]*").unwrap());
    static DOLLAR_STRING: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?s)\$[A-Za-z_0-9]*\$.*?\$[A-Za-z_0-9]*\$").unwrap());
    static STRING: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?is)(?:e|u&)?'(?:''|[^'])*'").unwrap());
    static NUMBER: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\b(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?\b").unwrap());
    static WHITESPACE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\s+").unwrap());

    let query = BLOCK_COMMENT.replace_all(query, " ");
    let query = LINE_COMMENT.replace_all(&query, " ");
    let query = DOLLAR_STRING.replace_all(&query, "?");
    let query = STRING.replace_all(&query, "?");
    let query = NUMBER.replace_all(&query, "?");
    WHITESPACE.replace_all(&query, " ").trim().to_ascii_lowercase()
}

// ... elided: query_template (512-char cap) and query_fingerprint (SHA-256)

/// Emit one bounded event for statements slow enough to affect the tail. Table
/// and project dimensions are extracted only for diagnosis; raw SQL is never
/// included in this event.
fn record_statement_latency(metrics: Option<&crate::database::ScanMetrics>, query: &str, protocol: &'static str, duration_us: u64, success: bool) {
    if let Some(metrics) = metrics {
        metrics.record_pgwire_query(duration_us);
    }
    const SLOW_QUERY_US: u64 = 1_000_000;
    if duration_us < SLOW_QUERY_US {
        return;
    }

    let (_, operation) = classify_query(query);
    let (tables, project_id) = query_dimensions(query);
    info!(
        event = "pgwire.slow_statement",
        query.class = operation,
        query.fingerprint = %query_fingerprint(query),
        query.template = %query_template(query),
        query.tables = %tables,
        project.id = %project_id,
        protocol,
        duration_us,
        success,
        "slow PostgreSQL statement"
    );
}
```

The test that pins the redaction is a good spec:

**Listing 13.12 — src/server/mod.rs:1381–1391**

```rust
    #[test]
    fn query_template_redacts_literals_and_has_a_stable_fingerprint() {
        let first = "SELECT * FROM logs WHERE project_id = 'project-123' AND body = 'secret' AND n = 42 -- do not log";
        let second = "select * from logs where project_id = 'project-456' and body = 'other' and n = 7";
        let first_template = query_template(first);
        assert_eq!(first_template, "select * from logs where project_id = ? and body = ? and n = ?");
        assert_eq!(first_template, query_template(second));
        assert_eq!(query_fingerprint(first), query_fingerprint(second));
        assert!(!first_template.contains("secret"));
        assert!(!first_template.contains("project-123"));
    }
```

Note that `project_id` *is* separately extracted (`query_dimensions`) and logged
as a dimension — the tenant id is operationally necessary, the payload literals
are not.

### `ProjectRoutingTable::scan`, step by step

Now the main event. This is one function, roughly 570 lines, and it is the single
most important function in the read path. We walk it in order.

#### Step 11 — the OTel scan guard

**Listing 13.13 — src/database/mod.rs:8510–8531**

```rust
    async fn scan(&self, state: &dyn Session, projection: Option<&Vec<usize>>, filters: &[Expr], limit: Option<usize>) -> DFResult<Arc<dyn ExecutionPlan>> {
        let span = tracing::Span::current();
        let scan_start = std::time::Instant::now();
        let scan_metrics = self.database.scan_metrics.clone();

        // Internal Delta-only reads (rollup builds, maintenance) are not the
        // unbounded client scans this guard exists to reject.
        if !self.database.bypass_rollup
            && let Some(reason) = self.bounded_otel_scan_reason(filters, limit)
        {
            match self.database.config.core.timefusion_otel_scan_guard {
                config::OtelScanGuard::Off => {}
                config::OtelScanGuard::Observe => {
                    metrics::counter!(scan_metric_names::BOUNDED_OTEL_SCAN_CANDIDATES).increment(1);
                    warn!(event = "otel_scan_guard_candidate", table.name = %self.table_name, reason, "raw OTel scan would be rejected");
                }
                config::OtelScanGuard::Enforce => {
                    metrics::counter!(scan_metric_names::BOUNDED_OTEL_SCAN_REJECTIONS).increment(1);
                    return Err(DataFusionError::Plan("raw OTel queries require project_id = <value> and a timestamp lower bound or LIMIT".to_string()));
                }
            }
        }
```

Three-state configuration (`Off` / `Observe` / `Enforce`) is the right shape for a
guard you intend to roll out: run it in `Observe`, watch
`BOUNDED_OTEL_SCAN_CANDIDATES`, then flip to `Enforce` when the count is zero.
`bypass_rollup` exempts TimeFusion's own internal reads.

The predicate itself must accept every shape the *rollup matcher* accepts, or
enforce mode would reject TimeFusion's own rollup build query:

**Listing 13.14 — src/database/mod.rs:7081–7117**

```rust
    fn bounded_otel_scan_reason(&self, filters: &[Expr], limit: Option<usize>) -> Option<&'static str> {
        let mut conjuncts = Vec::new();
        fn collect_conjuncts<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
            if let Expr::BinaryExpr(BinaryExpr { left, op: Operator::And, right }) = expr {
                collect_conjuncts(left, out);
                collect_conjuncts(right, out);
            } else {
                out.push(expr);
            }
        }
        for filter in filters {
            collect_conjuncts(filter, &mut conjuncts);
        }
        let bounded = conjuncts.iter().any(|expr| Self::is_bounding_predicate(expr))
            || self.extract_time_range_from_filters(&conjuncts.into_iter().cloned().collect::<Vec<_>>()).is_some_and(|(lower, _)| lower != i64::MIN);
        Self::raw_otel_scan_reason(&self.table_name, filters, limit, bounded)
    }

    /// Predicates that bound a scan but that `extract_time_range_from_filters`
    /// does not decompose: `date = '…'` restricts it to one partition, and
    /// `BETWEEN` is a timestamp range it does not see. Both are shapes
    /// `rollup::match_aggregate` accepts, so the guard must accept them too —
    /// otherwise enforce mode rejects TimeFusion's own rollup build query.
    fn is_bounding_predicate(expr: &Expr) -> bool {
        matches!(expr, Expr::Between(between) if !between.negated && matches!(between.expr.as_ref(), Expr::Column(c) if c.name == "timestamp"))
            || matches!(expr, Expr::BinaryExpr(BinaryExpr { left, op: Operator::Eq, .. }) if matches!(left.as_ref(), Expr::Column(c) if c.name == "date"))
    }

    fn raw_otel_scan_reason(table_name: &str, filters: &[Expr], limit: Option<usize>, lower_timestamp_bound: bool) -> Option<&'static str> {
        if !matches!(table_name, "otel_logs_and_spans" | "otel_metrics") {
            return None;
        }
        if !filters.iter().any(|filter| crate::read::optimizers::extract_project_id_from_expr(filter).is_some()) {
            return Some("missing exact project_id filter");
        }
        (limit.is_none() && !lower_timestamp_bound).then_some("missing timestamp lower bound or scan limit")
    }
```

Our example query passes: it has `project_id = 'prod-api-001'` and a
`timestamp >=` lower bound.

#### Step 12 — stripping leg-unsafe filters

This is where the Chapter 6 mutability invariant is *enforced* on the read side:

**Listing 13.15 — src/database/mod.rs:8533–8552**

```rust
        // Second line of defence behind `supports_filters_pushdown`: predicates
        // on the tombstone marker or any version-mutable column must not reach
        // a scan leg however they arrived (silent resurrection / stale-version
        // serving). For the tantivy prefilter the invariant is: leaf pruning
        // below DedupExec commutes with keep-greatest only when the predicate
        // evaluates identically on every version of a key — so file exclusion
        // and row selections stay OFF on mutable columns, while the id-set half
        // stays sound (`id` is a dedup key: `id IN (hits)` admits whole keys
        // atomically and the above-dedup filter rejects stale-only matches).
        let mutable = Self::version_mutable_columns(&self.table_name);
        let unstripped_filters = filters;
        let leg_safe = |f: &Expr| {
            !Self::references_tombstone(&self.table_name, f) && !mutable.as_ref().is_some_and(|m| f.column_refs().iter().any(|c| m.contains(&c.name)))
        };
        let filters: Vec<Expr> = filters.iter().filter(|f| leg_safe(f)).cloned().collect();
        let optimized_filters = self.apply_time_series_optimizations(&filters)?;

        // Get project_id from filters if possible, otherwise use default
        let project_id = self.extract_project_id_from_filters(&optimized_filters).unwrap_or_else(|| self.default_project.clone());
        span.record("table.project_id", project_id.as_str());
```

Note `unstripped_filters` is kept — it is needed later for the Tantivy id-set,
which *is* sound on mutable columns because `id` is a dedup key.

#### Step 13 — timestamp becomes a date partition filter

The optimization that makes `WHERE timestamp >= …` prune partitions without the
user writing a `date` predicate:

**Listing 13.16 — src/database/mod.rs:7208–7234**

```rust
    /// Apply time-series specific optimizations to filters
    fn apply_time_series_optimizations(&self, filters: &[Expr]) -> DFResult<Vec<Expr>> {
        use crate::read::optimizers::time_range_partition_pruner;

        // Resolve the schema-declared time column for this table; falls back to
        // "timestamp" when the schema isn't registered (custom/dynamic tables).
        let time_column = crate::schema::get_schema(&self.table_name).map(|s| s.time_column_name().to_string()).unwrap_or_else(|| "timestamp".to_string());

        let optimized_filters: Vec<Expr> = filters
            .iter()
            .cloned()
            .chain(filters.iter().flat_map(|filter| {
                let date_filters = time_range_partition_pruner::timestamp_to_date_filters(filter, &time_column);
                if !date_filters.is_empty() {
                    debug!("Added {} date partition filter(s) for {} on column {}", date_filters.len(), self.table_name, time_column);
                }
                date_filters
            }))
            .collect();

        // Check if project_id filter is present
        if !crate::read::optimizers::ProjectIdPushdown::has_project_id_filter(&optimized_filters) {
            debug!("Query missing project_id filter - may scan all partitions");
        }

        Ok(optimized_filters)
    }
```

Filters are *added*, never replaced — the original timestamp predicate still runs
for exactness within the day.

For our example, `timestamp >= '2025-01-17 14:00' AND timestamp < '2025-01-17 15:00'`
adds `date = '2025-01-17'`, which prunes the scan to one partition directory.

#### Step 15 — the Tantivy prefilter

If the query carries a `text_match` predicate (or an equality/LIKE the rewriter
converted into one — Chapter 21), the sidecar index is searched *at plan time* and
the result narrows the scan four different ways.

**Listing 13.17 — src/database/mod.rs:8554–8582**

```rust
        // Tantivy prefilter, two independent paths: the Delta side builds
        // `id IN (delta_ids)` for the Delta scan only (MemBuffer rows are never
        // in the index, so applying it there would drop valid rows); the
        // MemBuffer side prefilters atomically under its own bucket lock. On a
        // MOR table collect the tree from the UNSTRIPPED filters — the id-set
        // (the only output allowed below on such tables) is sound for them.
        let text_match_tree = match mutable.is_some() {
            false => crate::tantivy::udf::collect_text_match_tree(&optimized_filters),
            true => crate::tantivy::udf::collect_text_match_tree(&self.apply_time_series_optimizations(unstripped_filters)?),
        };
        // Query [lo,hi] timestamp window, shared by the tantivy prefilter (time-
        // prunes the sidecar search + scopes the coverage gate to a needle's
        // window, not every index the project built) and the skip-delta
        // watermark check below.
        let query_time_range = self.extract_time_range_from_filters(&optimized_filters);
        let mut tantivy_id_filter: Option<Expr> = None;
        // When index coverage is partial, the indexed and raw files are read
        // as separate Delta legs. Only the indexed leg receives the narrowing
        // id-set; uncovered files retain the original predicate and therefore
        // cannot lose rows. This turns coverage lag into bounded raw debt
        // instead of making one missing sidecar poison the whole query window.
        let mut tantivy_covered_files: Option<HashSet<String>> = None;
        // Files the prefilter proved hold no matches (zero-hit covering
        // index) — excluded from the Delta scan when file pruning is on.
        let mut tantivy_exclude: Option<HashSet<String>> = None;
        // Per-file matching row ordinals (row-selection pushdown), for files
        // whose covering index was built in parquet row order.
        let mut tantivy_row_selections: Option<HashMap<String, Vec<u64>>> = None;
```

The four outputs, in increasing order of narrowing power:

1. **`id IN (…)`** — an `InList` predicate over the dedup key. Sound everywhere,
   including merge-on-read tables, because `id` is a dedup key so the list admits
   or rejects whole key groups.
2. **`covered_files`** — the index's coverage set, used to split the Delta scan
   into an indexed leg (which gets the id-set) and a raw leg (which does not).
3. **`exclude_files`** — files whose covering index returned *zero* hits, dropped
   from the scan entirely.
4. **`row_selections`** — per-file matching row ordinals, turned into a
   `ParquetAccessPlan` so the reader decodes only matching rows.

The search itself is one pass, compiling the whole predicate tree into a single
tantivy `BooleanQuery` per index:

**Listing 13.18 — src/database/mod.rs:8598–8622**

```rust
            let mut abort_reason: Option<&'static str> = None;
            // ONE pass over the in-window index set: the routable predicate
            // tree compiles to a single tantivy BooleanQuery per index
            // (And→Must, Or→Should; `collect_text_match_tree` only emits OR
            // nodes whose every branch is completely covered), hits unioned
            // across indexes (they cover disjoint row sets).
            match svc.search_with_stats(&self.table_name, &project_id, tree, max_hits, query_time_range).await {
                Ok(Some(result)) => {
                    delta_any_usable = true;
                    delta_indexed_rows = result.indexed_rows;
                    delta_covered = result.covered_files;
                    delta_field_gap = result.field_coverage_gap;
                    delta_zero_hit = result.zero_hit_files;
                    delta_row_sel = result.row_selections;
                    delta_ids = Some(result.hits.into_iter().map(|h| h.id).collect());
                }
                Ok(None) => {
                    abort_reason = Some("delta_no_index_or_cap_exceeded");
                }
                Err(e) => {
                    warn!("tantivy search failed for {}/{}: {:#} — falling back to full scan", project_id, self.table_name, e);
                    crate::observability::record_tantivy_prefilter_error();
                    abort_reason = Some("delta_error");
                }
            }
```

Every failure mode falls back to a full scan. The index is an accelerator, never a
correctness input — a theme worth noting, since it is what makes the whole Tantivy
subsystem safe to have.

The decision of *whether* to use the result is a pure function, deliberately
extracted so it can be unit-tested without IO:

**Listing 13.19 — src/database/mod.rs:8294–8340**

```rust
/// Outcome of [`decide_prefilter`]: either why the tantivy prefilter was
/// skipped, or the narrowing it proved sound to apply.
enum PrefilterDecision {
    Skipped(&'static str),
    Used { ids: HashSet<String>, covered_files: HashSet<String>, exclude_files: Option<HashSet<String>>, row_selections: Option<HashMap<String, Vec<u64>>> },
}

/// Pure decision over an already-completed tantivy search's stats — no IO, mirrors the branches
/// in `ProjectRoutingTable::scan`'s prefilter block exactly (skip reasons in the same order).
///
/// `is_mutable`: whether the leg is on a `version_append` (merge-on-read) table. On such a
/// table a "hitless" file may hold the NEWEST version of a key whose match lives only in an
/// older version, and dropping it below `DedupExec` would serve the stale row — so file
/// exclusion/row-selection narrowing NEVER applies there; only the id-set does (sound at
/// whole-key granularity).
#[allow(clippy::too_many_arguments)]
fn decide_prefilter(
    ids: HashSet<String>, indexed_rows: u64, min_selectivity_pct: u64, field_gap: bool, covered_files: HashSet<String>, zero_hit_files: HashSet<String>,
    row_selections: HashMap<String, Vec<u64>>, is_mutable: bool, file_pruning_enabled: bool, row_selection_enabled: bool,
) -> PrefilterDecision {
    // No indexed rows = no useful prefilter. Without this guard we'd emit an
    // empty IN(...) list that zeros the Delta scan even when matching rows
    // exist there (e.g. data written directly without triggering an index build).
    if indexed_rows == 0 {
        return PrefilterDecision::Skipped("empty_index");
    }
    // Selectivity cutoff: if the hit set covers most of the indexed rows, the
    // IN-list won't prune enough to be worth its planning cost. Bail; the
    // original predicate re-runs as the correctness backstop.
    if (ids.len() as u64) * 100 >= indexed_rows * min_selectivity_pct {
        return PrefilterDecision::Skipped("low_selectivity");
    }
    // An in-window index lacked one of the queried fields (schema evolution
    // added a tantivy column after it was built). It can't answer that
    // predicate yet appears "covered", so the IN-list would drop its rows.
    if field_gap {
        return PrefilterDecision::Skipped("field_coverage_gap");
    }
    // A zero-hit covering index proves its own files hold no matches. Complete
    // coverage excludes those files from the single Delta leg; partial
    // coverage excludes them only from the indexed leg while uncovered files
    // scan raw.
    let (exclude_files, row_selections) = if is_mutable {
        (None, None)
    } else {
        // ... elided: the two config gates (file pruning, row selection)
```

Four skip reasons, each guarding a specific way to return wrong answers:
`empty_index` (an empty `IN` list would zero the scan), `low_selectivity` (not
worth the planning cost), `field_coverage_gap` (an index built before a column was
added looks covered but cannot answer), and `is_mutable` (a hitless file may hold
the *newest* version of a key whose match lives only in an older version).

#### Step 16 — the dedup-skip verdict

**Listing 13.20 — src/database/mod.rs:8669–8706**

```rust
        // Read-side dedup setup: collapse physical duplicates of dedup-key rows
        // over the routed/pruned union at query time, so COUNT(*) is correct
        // regardless of sweep timing. The pushed projection is augmented with
        // any dedup-key columns the query projected away; `output_projection`
        // restores the requested set. No-op without declared dedup_keys.
        let table_schema = crate::schema::get_schema(&self.table_name);
        let dedup_keys: Vec<String> = table_schema.as_ref().map(|s| s.dedup_keys.clone()).unwrap_or_default();
        // The tiebreak rides in with the keys ONLY for merge-on-read tables
        // (DedupExec keeps the greatest version per key and must see the
        // column). Gated on `version_append`: elsewhere keep-greatest can't
        // engage, so the column would be read and never used — measured at
        // 2x-6x slower counts when pulled in unconditionally.
        let dedup_tiebreak: Option<String> = table_schema.as_ref().filter(|s| s.version_append).and_then(|s| s.dedup_tiebreak.clone());
        // Merge-on-read DELETE: a tombstone version must reach the filter ABOVE
        // the dedup, so its marker column rides in with the keys and is stripped
        // again afterwards. `None` on every table that declares none.
        let tombstone: Option<String> = table_schema.and_then(|s| s.tombstone_column.clone());
        // `tombstone_keep` is the requested width when the marker rode in
        // purely for the filter (one trailing column the post-filter projection
        // removes). The dedup skip is decided BEFORE the projection is built:
        // deciding after, gated on `output_projection.is_none()`, meant
        // augmenting with the keys was exactly what disabled the skip — and
        // `id` is ~43% of an otel file's bytes, read purely to feed a DedupExec
        // this path then removes. A certified window now skips BOTH the dedup
        // and the widening; a fast-resolve miss simply declines (the skip is an
        // optimisation, never a correctness input).
        let skip_verdict = match dedup_keys.is_empty() {
            true => DedupSkipVerdict::Disabled,
            false => self
                .database
                .try_fast_resolve(&project_id, &self.table_name)
                .and_then(|t| t.try_read().ok().map(|table| self.dedup_skip_allowed(&table, &project_id, query_time_range, &dedup_keys)))
                // A resolve miss is its own denial reason, not an uncertified
                // partition: it says the provider cache was cold, which the
                // certification-survival question must not be charged for.
                .unwrap_or(DedupSkipVerdict::Unresolved),
        };
        let pre_skip_dedup = skip_verdict.granted();
```

The parenthetical "`id` is ~43% of an otel file's bytes, read purely to feed a
`DedupExec` this path then removes" quantifies why the ordering of these two
decisions matters: a certified window skips both the dedup *and* reading the key
columns.

`try_read()` (not `read().await`) is deliberate — the skip is an optimization, so
it declines rather than block.

#### Step 17 — projection augmentation

**Listing 13.21 — src/database/mod.rs:8707–8735**

```rust
        let (scan_projection, output_projection, tombstone_keep): (Option<Vec<usize>>, Option<Vec<usize>>, Option<usize>) = match projection {
            Some(p) if !dedup_keys.is_empty() || tombstone.is_some() => {
                let full_schema = self.schema();
                // The dedup keys ALWAYS ride in, even when `pre_skip_dedup` says
                // the window is certified: the skip is granted PER LEG below and
                // the mem ∪ delta union path never grants it, so dropping the
                // keys here built DedupExecs over scans that couldn't feed them
                // ("DedupExec key `id` not in input schema", 08-09..08-11).
                // Keeping them costs bytes; removing the DedupExec is the larger win.
                let augment: Vec<&String> = dedup_keys.iter().chain(dedup_tiebreak.iter()).collect();
                let missing: Vec<usize> =
                    augment.into_iter().chain(tombstone.iter()).filter_map(|k| full_schema.index_of(k).ok()).filter(|i| !p.contains(i)).collect();
                if missing.is_empty() {
                    (Some(p.clone()), None, None)
                } else {
                    let mut aug = p.clone();
                    aug.extend(&missing);
                    // Requested columns occupy the first p.len() positions of the augmented output.
                    let mut out: Vec<usize> = (0..p.len()).collect();
                    // The marker alone must survive DedupExec's projection restore.
                    let extra = tombstone.as_ref().and_then(|t| full_schema.index_of(t).ok()).filter(|i| !p.contains(i));
                    if let Some(ti) = extra {
                        out.push(aug.iter().position(|&i| i == ti).expect("just extended with it"));
                    }
                    (Some(aug), Some(out), extra.map(|_| p.len()))
                }
            }
            _ => (projection.cloned(), None, None),
        };
        let projection = scan_projection.as_ref();
```

Our example projects `(timestamp, name, duration, status_code)`. `timestamp` is
already a dedup key, but `id`, `updated_at`, and `deleted` are not projected — so
the *scan* projection becomes seven columns and `output_projection` records that
only the first four (plus the tombstone marker, for the filter) survive.

#### Step 18 — suppressing the pushed limit

A short block with a subtle correctness argument:

**Listing 13.22 — src/database/mod.rs:8736–8747**

```rust
        // When DedupExec is active it drops rows AFTER the scan, so a pushed
        // `limit` must NOT truncate the underlying scans — otherwise the deduped
        // result can yield < limit distinct rows even when more exist below the
        // cut, and the outer GlobalLimitExec (which DataFusion keeps) can't
        // recover them. Suppress the per-scan limit; the outer limit still caps.
        // `orig_limit` is restored on Delta-only paths that skip DedupExec. The
        // tombstone filter drops rows after the scan for exactly the same reason,
        // so it suppresses the pushed limit even where dedup doesn't.
        let orig_limit = limit;
        let post_scan_row_drop = !dedup_keys.is_empty() || tombstone.is_some();
        let limit = if post_scan_row_drop { None } else { limit };
```

`LIMIT 100` in our example is therefore *not* pushed into the Parquet scan — the
outer `GlobalLimitExec` still caps the result, but the scan must be free to
produce more than 100 rows so that dedup and tombstone filtering can still yield
100 distinct live ones.

#### Step 19 — which legs?

The leg selection has four outcomes: Delta-only (no buffered layer), Delta-only
(no memory data), mem-only (skip Delta), or the union. The first two share a
helper. The Delta-only-because-no-layer branch:

**Listing 13.23 — src/database/mod.rs:8856–8901**

```rust
        // Check if buffered layer is configured
        let has_layer = self.database.buffered_layer().is_some();
        debug!("ProjectRoutingTable::scan - buffered_layer present: {}, project_id: {}", has_layer, project_id);
        let Some(layer) = self.database.buffered_layer() else {
            // No buffered layer, query Delta directly
            debug!("No buffered layer, querying Delta only");
            // A sweep-certified window holds exactly one (winning) row per key,
            // so the stale-version hazard that keeps mutable predicates above
            // DedupExec has no instance — a scan that needs no dedup needs no
            // protection from pruning below it. Pushing those predicates down
            // is the half that matters for dashboards (445x row amplification
            // measured when only `timestamp >=` reached Parquet). Delta-only by
            // construction: the skip is never granted while the MemBuffer leg —
            // where uncertified versions live — is in play.
            let (skip_dedup, plans) = self
                .scan_delta_only(
                    state, projection, &optimized_filters, unstripped_filters, &project_id, query_time_range,
                    &dedup_keys, pre_skip_dedup, &tombstone, orig_limit, limit, true,
                    tantivy_id_filter.as_ref(), tantivy_covered_files.as_ref(),
                    tantivy_exclude.as_ref(), tantivy_row_selections.as_ref(),
                )
                .await?;
            if skip_dedup {
                scan_state.lock().skip_dedup = true;
            }
            // This leg IS the Delta read, and until now it said so to nobody:
            // `record_scan` gates every dedup counter on `has_delta`, so a
            // deployment without a buffered layer reported zero eligible scans
            // however many it served. Prod runs with the buffer, which is why the
            // 2026-08-11 measurement was unaffected — but `query_delta_only`
            // (bypass_buffer) takes this path, so tests measured nothing at all.
            scan_state.lock().has_delta = true;
            return wrap_result(plans.into_iter().map(|plan| (plan, crate::read::LegKind::Delta)).collect());
        };
```

"445× row amplification measured when only `timestamp >=` reached Parquet" is the
payoff for the certification machinery: on a certified window, the mutable-column
predicates that were stripped in step 12 are *re-admitted* and pushed all the way
into the Parquet scan.

The **skip-Delta watermark** is the next decision, and the comment records a
visibility bug that the current design fixes:

**Listing 13.24 — src/database/mod.rs:8905–8924**

```rust
        // Skip Delta when the query's lower bound is strictly above the
        // per-table flushed watermark (max row ts ever handed to a Delta
        // commit, floored at boot) — Delta provably holds nothing newer, so
        // MemBuffer alone serves open-ended `WHERE timestamp >= now() - 5m`
        // dashboards. The previous `query_min >= mem_oldest` heuristic was
        // unsound whenever Delta held rows inside MemBuffer's range —
        // force-flushed open buckets, or a newer bucket drained while an
        // older one was stuck after a failed flush — and silently hid those
        // rows (2026-06-11 visibility gap).
        let skip_delta = match query_time_range {
            Some((query_min, _)) => query_min > layer.delta_flushed_watermark(&project_id, &self.table_name),
            None => false,
        };
        // Sticky-empty short-circuit: if no flush has ever committed for this
        // (project, table), Delta is guaranteed empty and we can skip the
        // scan-plan-build cost. Flipped by the flush callback after a
        // successful commit; never flipped back (compaction reduces files but
        // doesn't go to zero in steady state).
        let skip_delta = skip_delta || self.database.delta_scan_can_be_skipped(&project_id, &self.table_name);
```

The distinction between the old heuristic and the new one is worth internalizing.
"The query starts after MemBuffer's oldest row" is *not* a proof that Delta holds
nothing relevant — a force-flushed bucket puts recent rows in Delta while
MemBuffer still covers that range. "The query starts after the highest timestamp
ever handed to a Delta commit" *is* a proof.

Then the MemBuffer leg, the hot leg, and the check for whether either exists:

**Listing 13.25 — src/database/mod.rs:8926–8965**

```rust
        // MemBuffer query. `query_partitioned_with_text_match` handles its
        // own atomic per-bucket prefilter inside the bucket lock — we must
        // NOT prepend `tantivy_id_filter` here (that filter is derived from
        // delta-side IDs only and would drop legitimate MemBuffer rows).
        // On a MOR table the per-bucket ROW prefilter is below DedupExec and
        // the tree may reference mutable columns (it was collected unstripped
        // for the delta id-set) — dropping a stale version's row here while
        // its match-bearing sibling sits in another leg breaks keep-greatest,
        // so the mem leg gets no tree.
        let mem_tree = text_match_tree.as_ref().filter(|_| mutable.is_none());
        let mem_plan_started = std::time::Instant::now();
        let mem_leg = layer.query_partitioned_with_text_match(&project_id, &self.table_name, &optimized_filters, mem_tree).unwrap_or_else(|e| {
            warn!("Failed to query mem buffer: {}", e);
            Default::default()
        });
        metrics::counter!(scan_metric_names::MEM_PLAN_TOTAL).increment(1);
        metrics::counter!(scan_metric_names::MEM_PLAN_US_TOTAL).increment(mem_plan_started.elapsed().as_micros() as u64);
        let mem_partitions = mem_leg.partitions;

        // Hot-tier third leg (P1) — see `HotTier::scan` for the coverage
        // contract. Consulted only when Delta is actually scanned (the tier
        // holds only flushed data) and only for shallow scans — past its
        // retention window the tier is a fraction of the answer, so a 7d scan
        // would open every file in it for nothing. DEDUP: a non-empty hot leg
        // forces the union path, which never sets `skip_dedup` (the hot leg
        // serves pre-dedup rows and relies on `DedupExec`).
        let too_deep = crate::hot_tier::skip_for_lookback(self.scan_lookback_micros(&optimized_filters));
        let mem_ranges = layer.get_bucket_ranges(&project_id, &self.table_name);
        let hot_plan_started = std::time::Instant::now();
        let hot: crate::hot_tier::HotLeg = match skip_delta || too_deep {
            true => Default::default(),
            false => layer.hot_tier().scan(&project_id, &self.table_name, query_time_range, &mem_ranges, &optimized_filters, &self.schema, projection),
        };
```

Note the MemBuffer query failure mode: `unwrap_or_else(|e| { warn!(...);
Default::default() })`. ⚠️ A MemBuffer query error degrades to *no memory leg*,
which silently omits unflushed rows from the answer rather than failing the query.
That is a deliberate availability-over-consistency choice, but it is worth
knowing; the only signal is the `warn!`.

#### Step 20 — range exclusion, and the merge-on-read weakening

The union must not double-count. MemBuffer and the hot tier are authoritative for
the time ranges they hold, so Delta's filters get those ranges excluded:

**Listing 13.26 — src/database/mod.rs:9015–9041**

```rust
        // Build Delta filters with per-bucket exclusion so the union doesn't
        // double-count: Delta excludes the merged (mem ∪ hot) row ranges where
        // those legs are authoritative (`get_bucket_ranges` skips open and
        // force-flushed buckets, whose windows legitimately straddle stores).
        // MERGE-ON-READ: an UPDATE appends a new version at the row's ORIGINAL
        // timestamp — inside an excluded range — so each conjunct is weakened
        // with `OR stamp > gate`. Weakening is safe in one direction only: an
        // over-admitted row is a duplicate DedupExec collapses, an
        // under-admitted one is a stale read; the union path never grants skip_dedup.
        let mut delta_filters = optimized_filters.clone();
        let ts_col = || Box::new(col("timestamp"));
        let ts_lit = |t: i64| Box::new(lit(ScalarValue::TimestampMicrosecond(Some(t), Some("UTC".into()))));
        let version_col = table_schema.as_ref().filter(|s| s.version_append).and_then(|s| s.dedup_tiebreak.clone());
        for (start, end) in crate::write::mem_buffer::merge_ranges([mem_ranges, hot_ranges].concat()) {
            // NOT (ts >= start AND ts < end)  ≡  (ts < start) OR (ts >= end)
            let below = Expr::BinaryExpr(BinaryExpr { left: ts_col(), op: Operator::Lt, right: ts_lit(start) });
            let at_or_above = Expr::BinaryExpr(BinaryExpr { left: ts_col(), op: Operator::GtEq, right: ts_lit(end) });
            let outside = Expr::BinaryExpr(BinaryExpr { left: Box::new(below), op: Operator::Or, right: Box::new(at_or_above) });
            delta_filters.push(match (&version_col, version_gate) {
                (Some(c), Some(g)) => Expr::BinaryExpr(BinaryExpr {
                    left: Box::new(outside),
                    op: Operator::Or,
                    right: Box::new(Expr::BinaryExpr(BinaryExpr { left: Box::new(col(c)), op: Operator::Gt, right: ts_lit(g) })),
                }),
                _ => outside,
            });
        }
```

The sentence to remember: *"an over-admitted row is a duplicate `DedupExec`
collapses, an under-admitted one is a stale read"*. That asymmetry is what makes
it safe to weaken the exclusion with `OR updated_at > gate` on a merge-on-read
table — where an `UPDATE` writes a new version carrying the row's *original*
timestamp, and therefore lands inside an excluded range.

Table resolution then takes the fast path when it can:

**Listing 13.27 — src/database/mod.rs:9042–9070**

```rust
        // Execute Delta query — fast path skips the 3 tokio RwLock `.await`s
        // when we've already resolved this (project, table) pair before.
        let resolve_span = tracing::trace_span!(parent: &span, "resolve_delta_table");
        let delta_table = match self.database.try_fast_resolve(&project_id, &self.table_name) {
            Some(t) => {
                scan_state.lock().fast_resolve_hit = Some(true);
                t
            }
            None => {
                scan_state.lock().fast_resolve_hit = Some(false);
                self.database.resolve_table(&project_id, &self.table_name).instrument(resolve_span).await?
            }
        };
        let table = delta_table.read().await;
        let delta_plans = self
            .scan_delta_with_tantivy(
                &table, state, projection, &delta_filters, limit,
                tantivy_id_filter.as_ref(), tantivy_covered_files.as_ref(),
                tantivy_exclude.as_ref(), tantivy_row_selections.as_ref(),
                query_time_range,
            )
            .await?;
        scan_state.lock().has_delta = true;
```

#### Step 21 — assembling the union

Legs are collected with their identity attached, in recency order:

**Listing 13.28 — src/database/mod.rs:9072–9084**

```rust
        // Union the legs in recency order — mem, then hot tier, then Delta — so
        // DedupExec's keep-first favours the freshest copy of a row. The hot leg
        // arrives already built (projected, filtered and ordering-declared by
        // `HotTier::scan`), so there is nothing left to assemble here.
        //
        // Identity travels WITH the plan, so the flatten cannot desynchronise it
        // from the sortability it implies (see `wrap_result`).
        use crate::read::LegKind;
        let mut legs: Vec<(Arc<dyn ExecutionPlan>, LegKind)> =
            [mem_plan.map(|p| (p, LegKind::Mem)), hot_plan.map(|p| (p, LegKind::Hot))].into_iter().flatten().collect();
        legs.extend(delta_plans.into_iter().map(|p| (p, LegKind::Delta)));
        wrap_result(legs)
    }
```

"Identity travels WITH the plan" is a small but real design lesson: the earlier
version kept a parallel `Vec<bool>` of sortability that had to stay index-aligned
with a list built by flattening three `Option`s. Pairing the identity with the
plan makes desynchronization impossible.

`wrap_result` is the closure defined earlier in the function; it does the final
five transformations. First, empty-leg elimination — which is not an optimization
but a correctness-of-plan-shape fix:

**Listing 13.29 — src/database/mod.rs:8755–8783**

```rust
        let wrap_result = |mut legs: Vec<(Arc<dyn ExecutionPlan>, crate::read::LegKind)>| -> DFResult<Arc<dyn ExecutionPlan>> {
            // A leg pruned to nothing (tantivy split with zero surviving files,
            // a date window outside the snapshot) bottoms out in an EmptyExec,
            // which declares no output ordering — and Delta legs are unsortable,
            // so ONE empty leg vetoed `merge_req` below: no SPM, DedupExec fell
            // to full-set over a coalesce, and the most selective point lookups
            // inherited the 2 GiB full-set ceiling (prod 2026-08-20). An empty
            // leg contributes no rows; drop it before the union. Keep one leg if
            // all are empty so the single-plan path stays valid.
            fn provably_empty(plan: &dyn ExecutionPlan) -> bool {
                plan.is::<datafusion::physical_plan::empty::EmptyExec>() || matches!(plan.children().as_slice(), [child] if provably_empty(child.as_ref()))
            }
            if legs.len() > 1 && legs.iter().any(|(p, _)| provably_empty(p.as_ref())) {
                match legs.iter().any(|(p, _)| !provably_empty(p.as_ref())) {
                    true => legs.retain(|(p, _)| !provably_empty(p.as_ref())),
                    false => legs.truncate(1),
                }
            }
            let leg_sortable: Vec<bool> = legs.iter().map(|(_, k)| k.sortable()).collect();
            let legs: Vec<Arc<dyn ExecutionPlan>> = legs
                .into_iter()
                .map(|(plan, kind)| match crate::read::ordering_probe_enabled() {
                    true => Arc::new(crate::read::OrderingProbeExec::new(plan, kind)) as Arc<dyn ExecutionPlan>,
                    false => plan,
                })
                .collect();
            let shape = *scan_state.lock();
            let us = scan_start.elapsed().as_micros() as u64;
            scan_metrics.record_scan(us, shape.skipped_delta, shape.has_mem, shape.has_delta, shape.fast_resolve_hit, shape.skip_dedup, skip_verdict);
```

This is a beautiful bug. An `EmptyExec` declares no output ordering. The
`SortPreservingMerge` above the union requires *every* input to satisfy the
ordering. So one empty leg — produced by, say, a Tantivy split that pruned every
file out of the raw side — vetoed the merge, which made `DedupExec` fall back to
its unbounded full-set mode over a coalesce, which gave *the most selective point
lookups in the system* the 2 GiB full-set ceiling. The fix is three lines: an
empty leg contributes no rows, so drop it before the union.

Then the merge-on-read ordering negotiation, which is the deepest part of the
function:

**Listing 13.30 — src/database/mod.rs:8784–8855**

```rust
            let dedup_on = !dedup_keys.is_empty() && !shape.skip_dedup;
            let mut plans = legs;
            // Merge-on-read prerequisite: keep-greatest only engages while the
            // input still declares an ordering on the leading dedup key, so the
            // in-memory legs are sorted up to the Delta leg's footer ordering
            // and merged explicitly. The SPM is built HERE, not left to
            // EnforceDistribution — DedupExec declares no required input
            // ordering, so EnforceSorting would delete the injected sorts.
            // Gated on `version_append`: ungated, it charged every scan a
            // blocking SortExec + k-way merge for a dormant feature (the
            // 2026-07-20 OOM shape).
            let mut merge_req = None;
            if dedup_on
                && table_schema.is_some_and(|t| t.version_append)
                && let Some(req) = table_schema.and_then(|t| Self::keep_greatest_ordering(t, &plans[0].schema()))
            {
                // Per-leg sortability: the DELTA leg is NEVER sortable — MOR
                // UPDATEs make files overlap, and a read-time SortExec over
                // them exhausted the query pool twice (2026-08-02, and again
                // via the 08-07 sort-only-the-unordered-branch attempt, whose
                // unspillable per-partition merges saturated 24 GB; reverted —
                // footer-less files need REPAIR, not read-time sorting).
                // `ordered_children` bails whenever an unsortable leg misses
                // `req`, so a Delta sort is structurally impossible here. The
                // in-memory legs ARE sortable: bounded, cheap, and exactly
                // where a fresh version append lives.
                match crate::read::optimizers::ordered_children(&plans, &req, None, &leg_sortable, false)? {
                    Some(ordered) => {
                        // Only in-memory legs can reach here (Delta is marked
                        // unsortable), so this is cheap — no metric alarm.
                        plans = ordered;
                        merge_req = Some(req);
                    }
                    // `None` is either "every leg already satisfies `req`" (merge
                    // anyway — the legs are still N partitions) or "an unsortable
                    // leg doesn't" (a Delta scan whose footer ordering isn't
                    // declared: bail, keep-greatest stays dormant, keep-first is
                    // still sound and the dedup sweep remains the authority).
                    None => {
                        let all = plans
                            .iter()
                            .map(|p| p.properties().equivalence_properties().ordering_satisfy(req.iter().cloned()))
                            .collect::<DFResult<Vec<_>>>()?;
                        merge_req = all.iter().all(|&s| s).then_some(req);
                    }
                }
            }
            let plan = if plans.len() == 1 { plans.remove(0) } else { UnionExec::try_new(plans)? };
            let plan = match merge_req.clone() {
                Some(req) => Arc::new(datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec::new(req, plan)),
                None => plan,
            };
            let plan = match dedup_on {
                true => Arc::new(
                    crate::read::DedupExec::with_tiebreak(plan, dedup_keys.clone(), dedup_tiebreak.clone(), output_projection.clone())?
                        // Declaring it REQUIRED is what stops EnforceSorting from
                        // deleting the merge above as unused — see the field docs.
                        .requiring(merge_req.clone()),
                ) as Arc<dyn ExecutionPlan>,
                // DedupExec is what restores the requested columns when it runs. Skipped,
                // that debt is still owed: the scan is carrying augmented key columns the
                // caller never asked for, and without this they leak into the result.
                false => match &output_projection {
                    Some(idxs) => Self::project_indices(plan, idxs)?,
                    None => plan,
                },
            };
            match &tombstone {
                Some(marker) => Self::filter_tombstones(plan, marker, tombstone_keep),
                None => Ok(plan),
            }
        };
```

Three things here are worth pulling out, because they are the kind of thing that
only becomes obvious after it has cost you a production incident.

**The SPM is built here, not left to the optimizer.** `DedupExec` declares no
required input ordering, so `EnforceSorting` would happily delete an injected
sort as unused. TimeFusion therefore constructs the `SortPreservingMergeExec`
itself *and* tells `DedupExec` to declare the ordering as required
(`.requiring(merge_req)`), purely so the optimizer will not remove it.

**The Delta leg is structurally unsortable.** `LegKind::sortable()` returns false
for Delta, and `ordered_children` bails whenever an unsortable leg misses the
requirement. That is not a heuristic — it is a hard guarantee that no read-time
`SortExec` can ever appear over the Delta leg, because that exhausted the query
pool twice. When the Delta leg's footer ordering is not declared, keep-greatest
simply stays dormant and the sweep remains the authority.

**Skipping the dedup still owes the projection.** `DedupExec` is what restores the
requested column set. If it is skipped, the augmented key columns would leak into
the result, so the `false` arm applies `output_projection` explicitly.

Finally, the tombstone filter goes *above* everything, which is exactly the point:
a tombstone version must survive to the filter so it can suppress its key.

### The final plan shape

For our example query, on a warm process where the window is not certified:

```
GlobalLimitExec: skip=0, fetch=100
  SortPreservingMergeExec: [timestamp@0 DESC]     ← the ORDER BY
    FilterExec: deleted IS NULL OR deleted = false ← tombstone filter
      DedupExec: keys=[timestamp, id] tiebreak=updated_at, project=[0,1,2,3]
        SortPreservingMergeExec: [timestamp DESC, id ASC]   ← keep-greatest input
          UnionExec
            DataSourceExec (MemorySourceConfig, ordering declared)   ← mem leg
            GatedScanExec                                            ← delta leg
              DeltaScan project_id=prod-api-001/date=2025-01-17
                predicate: timestamp >= … AND timestamp < …
                        AND resource___service___name = 'payments-api'
                        AND ((timestamp < mem_start) OR (timestamp >= mem_end)
                             OR updated_at > version_gate)
```

If the window *were* certified (a dedup sweep proved it clean and no commit has
landed since), the plan would collapse to a single Delta leg with no `DedupExec`,
no key-column augmentation, and the `resource___service___name` predicate pushed
all the way into the Parquet reader.

### How to debug this flow

The instrumentation is unusually good; here is what to reach for.

**Spans.** `datafusion.table.scan` carries `table.name`, `table.project_id`,
`scan.filters_count`, `scan.has_limit`, `scan.limit`, `scan.uses_mem_buffer`,
`scan.skipped_delta`, and — where the Delta scan reports them — `parquet.files`,
`parquet.bytes`, `parquet.file_ids`, `parquet.selected_row_groups`
(`src/database/mod.rs:8492–8509`). `postgres.query.simple` /
`postgres.query.extended` wrap the whole statement.

**The `pgwire.slow_statement` event**, for anything over one second: fingerprint,
redacted template, tables, project id, protocol, duration, success.

**`timefusion_stats`.** Every `scan_metric_names` constant is readable over SQL:

```sql
SELECT component, key, value FROM timefusion_stats
WHERE key LIKE 'scan%' OR key LIKE 'dedup%';
```

The diagnostic ratios worth knowing:

- `dedup_skipped / dedup_eligible_scans` — how often the certification pays.
- `dedup_denied_never_certified` vs `dedup_denied_fp_moved` — whether persisting
  certifications across restarts would help (`never_certified` = yes,
  `fp_moved` = no).
- `dedup_denied_by_leg` — the window *was* certified but a leg refused; means the
  MemBuffer or hot leg was in play.
- `fast_resolve_hits / misses` and `provider_cache_hits / misses` — planning cost.
- `decode_pressure_throttled` — the pressure valve is engaging, i.e. the process
  is near its memory limit during scans.
- `scans_mem_only` / `scans_delta_only` / `scans_mem_plus_delta` — the leg mix.
- `bounded_otel_scan_candidates` — unbounded queries that `Enforce` would reject.

**When a query is unexpectedly slow**, the questions in order: did it get a
`project_id` (span field)? did it get a `date` filter (debug log "Added N date
partition filter(s)")? was the dedup skipped, and if not which verdict? did the
Tantivy prefilter engage or skip, and with which reason (debug log "Tantivy
prefilter skipped … : <reason>")? is there a `SortExec` in the `EXPLAIN` over a
Delta leg (there must never be)?

**When a query returns wrong or missing rows**, the suspects in order: a
`warn!("Failed to query mem buffer")` (the memory leg was silently dropped); a
`skip_delta` decision based on a watermark that has not advanced; a Tantivy
prefilter applied where it should not have been (check `is_mutable` handling); a
filter marked `Exact` that a leg cannot actually evaluate.

**Key takeaways.** A `SELECT` passes through seven pgwire-level policies before
DataFusion sees it, then through one 570-line `scan` that makes eleven decisions:
guard, strip, prune, route, prefilter, certify, project, un-limit, select legs,
exclude ranges, assemble. The two recurring correctness principles are that
*over-admitting rows is recoverable and under-admitting is not*, and that
*predicates on mutable columns must stay above the dedup*. The two recurring
performance principles are that *the Delta leg must never be sorted at read time*
and that *an empty leg must be removed before it vetoes the merge*.
## 14. The read path, part II: rules, functions, and the plan cache

*After this chapter you will know every analyzer and optimizer rule TimeFusion
registers, why the Variant type system needs three of them, how the Tantivy
predicate rewriter turns ordinary SQL into index lookups, what the custom UDF
surface is, and how the two-level plan cache turns a never-repeating dashboard
query into a cache hit.*

### The rule inventory

`src/read/optimizers.rs` is 3,068 lines and registers roughly a dozen rules. They
fall into four groups:

| Group | Rules | Purpose |
|---|---|---|
| Variant | `VariantInsertRewriter`, `VariantSelectRewriter`, `VariantScanSchemaRestore`, `VariantPgwireRootWrap` | reconcile the SQL-facing and storage schemas |
| Postgres parity | `PgCoalesceUdf`, `PgArrayLiteralRewriter`, `row_to_json` AST rewrite | make real PG clients work |
| Pruning | `ProjectIdPushdown`, `time_range_partition_pruner` | tenant + date partition pruning |
| Acceleration | `TantivyPredicateRewriter`, `OrderedUnionForTopK`, rollup routing | turn a scan into an index lookup or a pre-aggregate |

The custom functions live in `src/read/functions.rs`, and the plan cache in
`src/read/plan_cache.rs`.

### Variant: three rules for one type

Recall from Chapter 6 that a Variant column has two schemas — `Utf8View` facing
SQL, `Struct{Binary,Binary}` in storage — because DataFusion's `VALUES` type check
rejects `Utf8 → Struct`. Keeping that fiction consistent takes three rules plus an
expression planner.

**Rule 1 — `VariantInsertRewriter`** turns JSON string literals into UDF calls so
the planner has a concrete coercion to type-check:

**Listing 14.1 — src/read/optimizers.rs:293–299**

```rust
/// AnalyzerRule that rewrites INSERT statements to wrap Utf8 expressions
/// going into Variant columns with `json_to_variant()`.
///
/// This is necessary because DataFusion's type checker rejects Utf8 -> Variant(Struct)
/// casts outright; `json_to_variant()` gives the planner a concrete UDF call instead.
#[derive(Debug)]
pub struct VariantInsertRewriter;
```

Its scope is deliberately narrow, and both bounds are documented:

**Listing 14.2 — src/read/optimizers.rs:341–345, 384–388**

```rust
/// Rewrite only the immediate child of the Dml node. `variant_indices` are
/// positions in `dml.input.schema()` (i.e. target table order) — they're only
/// valid for that single plan. Recursing into nested projections with the same
/// indices would mis-wrap unrelated columns whose positions happen to align.
```

```rust
/// Matches *literal* Utf8 only (and casts thereof). Column references — e.g.
/// `INSERT INTO t (payload) SELECT col FROM staging` — would need a type lookup
/// against the source plan's schema; today only the pgwire VALUES form needs
/// Variant coercion (other shapes are rejected in `rewrite_input_for_variant`).
```

Positional indices that are only valid for one plan node is exactly the kind of
thing that produces a silent, alignment-dependent bug; stating it and refusing to
recurse is the right defence.

**Rule 2 — `VariantSelectRewriter`** restores the real Variant type on `TableScan`
outputs, and lowers Variant expressions in text positions back to JSON:

**Listing 14.3 — src/read/optimizers.rs:480–490**

```rust
/// Restore the real Variant `Struct{Binary,Binary}` type on every TableScan's
/// `projected_schema` (see `patch_table_scan`), lower any Variant-in-text-
/// position expr back to `variant_to_json`, and recompute cached schemas
/// bottom-up so the restored type propagates. Returns `Transformed::yes` iff at
/// least one scan was actually re-typed.
///
/// Shared by the analyzer (`VariantSelectRewriter` Pass 1) and by
/// `VariantScanSchemaRestore`, the optimizer rule that re-applies it after
/// DataFusion's `optimize_projections` rebuilds each TableScan from the lying
/// `ProjectRoutingTable::schema()` and thereby reverts Variant → Utf8View.
```

**Rule 3 — `VariantScanSchemaRestore`** exists because rule 2's work gets undone:

**Listing 14.4 — src/read/optimizers.rs:511–524**

```rust
/// Re-applies `restore_variant_scan_types` after DataFusion's built-in
/// `optimize_projections` rebuilds each `TableScan` via `TableScan::try_new`,
/// which re-derives `projected_schema` from the lying `ProjectRoutingTable::schema()`
/// (Variant → Utf8View + `tf.pg_type=jsonb`) and so discards the analyzer's
/// Pass-1 Variant patch. The physical scan always emits the real Variant
/// struct, so the reverted logical scan disagrees with it. Most SELECTs never
/// notice, but DataFusion's Aggregate physical planner asserts
/// physical-input-schema == logical-input-schema — and `DISTINCT ON` lowers to
/// an Aggregate over `first_value` — so `SELECT DISTINCT ON (k) *` touching a
/// Variant column blew up with XX000 "Physical input schema should be the
/// same…" (2026-07-14 monoscope fetchEventExamples). Registered last so it runs
/// after `optimize_projections` in each optimizer pass.
#[derive(Debug)]
pub struct VariantScanSchemaRestore;
```

The bug chain: `optimize_projections` rebuilds each `TableScan` via
`TableScan::try_new`, which re-derives `projected_schema` from the provider's
(lying) `schema()`, reverting Variant to `Utf8View`. The *physical* scan still
emits real Variant. Most queries never notice — until an `Aggregate`, whose
physical planner asserts that the physical and logical input schemas match. And
`DISTINCT ON` lowers to an `Aggregate` over `first_value`, so
`SELECT DISTINCT ON (k) *` over a Variant column produced `XX000 "Physical input
schema should be the same…"`.

Registration order is load-bearing: this rule must run *after*
`optimize_projections` in each optimizer pass.

**The text-coercion lowering** is the fourth piece, and it is where Postgres
semantics get pinned:

**Listing 14.5 — src/read/optimizers.rs:591–604**

```rust
/// Value-position Variant → text coercion.
///
/// `patch_table_scan` restores the real Variant `Struct{Binary,Binary}` type on
/// the scan, which then propagates up. Any expression that had a Variant column
/// in a *scalar-text* position — a comparison/regex against a string, a
/// `LIKE`/`ILIKE`/`SIMILAR TO`, a `CAST(… AS text)`, or `IN (str, …)` — now
/// faces DataFusion with `Struct op Utf8`, which DF54 cannot coerce: bare
/// `body = 'x'` and `body LIKE …` error out, and `CAST(body AS text)` in a
/// projection silently yields empty strings. We lower the Variant side to
/// canonical JSON text via `variant_to_json` — exactly Postgres `jsonb::text`
/// semantics (scalar strings stay quoted, composites serialize), identical to
/// what the wire already returns for a bare `SELECT body`. The `->`/`->>`
/// accessors keep their own lowering (`VariantAwareExprPlanner`) and are not
/// touched here.
```

and the rewrite is idempotent by construction:

**Listing 14.6 — src/read/optimizers.rs:622–624**

```rust
/// Bottom-up rewrite of a single expression: wrap any Variant operand that sits
/// in a scalar-text position with `variant_to_json`. Idempotent — an
/// already-wrapped operand types as `Utf8` and `is_variant_expr` returns false.
```

"Idempotent because the output no longer matches the input predicate" is the
cheapest possible way to make a rewrite rule safe to run repeatedly, and it is
worth designing for.

**The wire wrap** is registered only on pgwire-facing sessions, and it peels
before it wraps:

**Listing 14.7 — src/read/optimizers.rs:691–694, 757–763**

```rust
/// Peel Sort / Limit / Distinct / SubqueryAlias from the root and wrap
/// the underlying Projection's Variant-typed expressions with
/// `variant_to_json()`. Returns the plan unchanged if no Projection sits
/// inside that peel.
```

```rust
/// Add a top-level Projection above `plan` that wraps every Variant-typed
/// output column with `variant_to_json`. Used for plan shapes that can't be
/// peeled into (Union/Aggregate/Join/Window/etc.) — the wrap is at the wire
/// only, so intermediate ops still operate on binary Variant.
///
/// Non-Variant columns pass through as bare `Expr::Column` so DataFusion's
/// schema accounting stays identical (same names, same qualifiers).
```

"Peel if you can, wrap on top if you cannot" is a good general strategy for
root-level plan rewrites: peeling preserves the plan shape (and therefore any
top-K or limit pushdown), while the added projection is the universal fallback.

### The expression planner: `->` and `->>`

DataFusion has no JSON operators; TimeFusion supplies them through an
`ExprPlanner`:

**Listing 14.8 — src/read/functions.rs:76–78, 192–194**

```rust
/// Rewrites Variant `->` and `->>` operators to `variant_get` calls.
#[derive(Debug)]
pub struct VariantAwareExprPlanner;
```

```rust
/// Build variant_get path string from components:
/// `["user", "name"]` → `user.name`, `["items", Index(0)]` → `items[0]`.
```

and one small UDF bridges a semantic gap between the two ecosystems:

**Listing 14.9 — src/read/functions.rs:227–236**

```rust
/// `json_to_pg_text(utf8) → utf8`: convert JSON-encoded text to Postgres `->>` text.
///
/// - JSON string `"Alice"` → `Alice` (parsed, so escape sequences resolve correctly)
/// - JSON null → SQL NULL
/// - JSON number / boolean → its literal text (`42`, `true`)
/// - JSON object / array → returned as-is (Postgres `->>` does the same)
///
/// Bridges `parquet_variant_compute::variant_get`'s NULL-on-non-string-cast
/// behavior to the Postgres `->>` contract.
```

The distinction between `->` (returns JSON) and `->>` (returns text) is exactly
where implementations usually diverge from Postgres; enumerating the four cases in
the doc comment is how you avoid that.

### The Variant extension-marker wrapper

A genuinely obscure failure, fixed with a generic newtype:

**Listing 14.10 — src/read/functions.rs:276–301**

```rust
/// `datafusion-variant`'s UDFs call `try_field_as_variant_array(field)` on
/// their first arg and bail with "Extension type name missing" when the
/// field lacks the `ARROW:extension:name = arrow.parquet.variant` marker.
/// That marker survives in the LogicalPlan's `projected_schema` (set by
/// `VariantSelectRewriter::patch_table_scan` and by `SchemaRegistry`'s
/// `fields()`), but is stripped on the way to the physical executor's
/// per-row Field — so any SELECT touching a Variant column would panic at
/// execution time. We re-stamp the marker here right before delegating.

/// Wrap a `datafusion-variant` UDF so its arg fields get the Variant
/// extension marker re-stamped before delegation. Generic over the inner
/// UDF type so `VariantToJsonUdf` and `VariantGetUdf` share one impl.
/// `JSONB_OUT` tags the output Field with `tf.pg_type = jsonb` so bare
/// Variant columns (wrapped by VariantPgwireRootWrap) surface PG OID 3802
/// over the wire instead of text — strict drivers (hasql) reject text.
#[derive(Debug, Default, Hash, PartialEq, Eq)]
pub struct VariantExtWrapper<U: ScalarUDFImpl + Default + Hash + PartialEq + Eq + 'static, const JSONB_OUT: bool = false> {
```

A const-generic bool on a wrapper type, used to make two UDFs (`variant_to_json`
with the jsonb tag, `variant_get` without) share one implementation. This is the
"advanced Rust to eliminate boilerplate" the project's stated philosophy asks for,
in a place where it genuinely pays.

### Postgres parity: coalesce and array literals

`coalesce(list_col, '{}')` is idiomatic Postgres and unrepresentable in Arrow —
there is no `Utf8 → List` cast. The fix is a two-part dance:

**Listing 14.11 — src/read/optimizers.rs:1482–1495**

```rust
/// `coalesce` wrapper whose coercion additionally unifies string args into a
/// sibling list type. Needed because the SQL planner computes projection
/// schemas (→ `coerce_types`) BEFORE analyzer rules run, so
/// `coalesce(List, Utf8)` must type-check up front; the analyzer rule below
/// then replaces the string literal with a real list literal so TypeCoercion
/// never has to cast Utf8 → List (unsupported in Arrow).
///
/// Registered under the built-in's name, shadowing it session-wide; every
/// trait method delegates to the inner built-in. On a DataFusion upgrade,
/// re-check `ScalarUDFImpl` for new methods whose defaults would diverge
/// from the built-in coalesce and forward them here too — the const assert
/// below fails the build on a version bump until that audit happens.
#[derive(Debug)]
pub struct PgCoalesceUdf {
```

The ordering problem is real: the SQL planner computes projection schemas *before*
analyzer rules run, so `coalesce(List, Utf8)` has to type-check up front — which
is what the shadowing UDF's widened coercion achieves — and only then does the
analyzer rule replace the string with a genuine list literal, so `TypeCoercion`
never faces the impossible cast.

The version tripwire is a technique worth stealing:

**Listing 14.12 — src/read/optimizers.rs:1499–1507**

```rust
/// DataFusion version `PgCoalesceUdf`'s method forwarding was last audited
/// against, compared at compile time to `datafusion::DATAFUSION_VERSION`.
/// A `cargo update` of datafusion breaks the build here on purpose:
/// re-audit `ScalarUDFImpl` for new methods, forward them above, then bump.
/// Maintenance invariant: DATAFUSION_VERSION resolves through the delta-rs
/// fork's patched graph — the fork must not bump it independently of the
/// workspace pin, or this check would pass against the wrong version. By the
/// same token, fork upgrades that keep the version string still need the
/// manual audit — the tripwire only catches plain `cargo update`s.
```

A compile-time assertion against a dependency's version string, deliberately
breaking the build on upgrade so a manual audit happens — plus an honest statement
of what the tripwire does *not* catch.

The array-literal parser is a small exercise in choosing leniency deliberately:

**Listing 14.13 — src/read/optimizers.rs:1664–1673**

```rust
/// Parse a PG array literal of strings: `{}`, `{a,b}`, `{"a,b",NULL}`.
/// Returns None if `s` isn't brace-wrapped (not an array literal), or if it
/// contains unquoted nested braces (multi-dimensional arrays like
/// `{{a},{b}}` — the schema is 1-D only, and misparsing the inner braces as
/// element text would be silently wrong; bail so the arg is left untouched).
/// Malformed quoting parses leniently rather than strictly: bailing would
/// only swap one error (this rewrite skipped → TypeCoercion's) for another,
/// while leniency keeps stable-but-sloppy client literals working.
/// Only the bare `NULL` keyword is a null element; `\N` is COPY text-format
/// syntax, not array-literal syntax, and stays literal text (as in PG).
```

Strict where being wrong would be *silent* (nested braces), lenient where being
wrong would merely swap one error message for another (malformed quoting). That is
the right axis on which to make the strictness decision.

### Partition pruning

Two rules do the work every multi-tenant time-series query depends on:

**Listing 14.14 — src/read/optimizers.rs:63, 124–136**

```rust
/// Converts timestamp filters to Delta date-partition filters.
```

```rust
/// Extracts the first positive `project_id = literal` AND-conjunct.
pub fn extract_project_id_from_expr(expr: &Expr) -> Option<String> {
```

```rust
pub struct ProjectIdPushdown;
```

The supporting helpers are small and worth reading because they define what
"a filter TimeFusion understands" means in practice:

**Listing 14.15 — src/read/optimizers.rs:10–61**

```rust
/// Avoids the competing `as_any` methods in this crate's trait scope.
pub fn downcast<T: 'static>(any: &dyn std::any::Any) -> Option<&T> {

/// Extracts any UTF-8 scalar representation.
pub fn extract_utf8_string(v: &ScalarValue) -> Option<String> {

/// Matches a column through coercion casts.
pub fn is_col_through_cast(expr: &Expr, name: &str) -> bool {

/// Removes coercion casts that otherwise hide literals from pruning.
pub fn unwrap_literal(expr: &Expr) -> Option<&ScalarValue> {

/// Timestamp scalar of any unit → microseconds since epoch.
pub fn scalar_micros(v: &ScalarValue) -> Option<i64> {

/// Reverses comparisons with swapped operands.
pub fn swap_comparison(op: Operator) -> Operator {
```

"Removes coercion casts that otherwise hide literals from pruning" is the theme:
DataFusion's type coercion wraps literals in `CAST` nodes, and a naive pattern
match on `Expr::Literal` then misses them entirely — so the query silently loses
its partition pruning. `is_col_through_cast` and `unwrap_literal` are the
see-through-the-cast pair every such rule needs.

### Tantivy predicate rewriting

The rule that makes ordinary SQL use the full-text index without the user knowing:

**Listing 14.16 — src/read/optimizers.rs:994–1004**

```rust
/// Per-column index facts the rewriter needs: the resolved tokenizer, and
/// whether the *stored* column is a plain string. The latter gates regex
/// routing: on a Variant/List column the tantivy index holds our own
/// canonical text rendering (`builder::variant_to_text`), which need not be
/// byte-identical to what a SQL `col::text` cast produces — routing there
/// could drop rows, so only text-typed columns are eligible.
#[derive(Debug)]
pub struct TantivyPredicateRewriter {
```

The gate is the interesting bit: the index for a Variant column holds
TimeFusion's *own* canonical text rendering, which need not be byte-identical to
what a SQL `::text` cast produces — so routing a regex there could drop rows. Only
text-typed columns are eligible.

The routing decision is per-column and tokenizer-dependent:

**Listing 14.17 — src/read/optimizers.rs:1096, 1122–1125, 1196–1199**

```rust
/// How a routed predicate reaches tantivy.
```

```rust
/// If `expr` is a rewritable predicate on an indexed column, return
/// `(column_name, route)`. Decision depends on the column's
/// tokenizer — raw can't do substring; ngram3 can do everything; default
/// is in between.
```

```rust
/// The column under zero or more string casts. Monoscope emits `col::text`,
/// which DataFusion may keep as a `Cast`/`TryCast` when the column is already
/// Utf8-ish; the cast is value-preserving for string types, so seeing through
/// it is safe (non-string sources are rejected by the `text_typed` gate).
```

IN-lists are expanded into OR-of-terms, with a length cap:

**Listing 14.18 — src/read/optimizers.rs:1046–1068**

```rust
/// Longest IN-list we'll expand into an OR of `text_match` calls. Beyond
/// this the per-item query cost outweighs the pruning (and the selectivity
/// cutoff would likely reject the hit set anyway).
```

```rust
/// `col IN ('a','b',...)` on a RAW-tokenized column → the per-item term
/// queries, under the same gates as exact `=` routing (raw tokenizer,
/// eq-term-safe literals, `route_equality` flag). Placeholder items defer to
/// scan-time classification. `NOT IN` is never routed.
```

`NOT IN` is never routed — negation over an index that may not cover every file is
unsound, and the simplest correct answer is to refuse.

The per-table indexed-column map is cached, with the same `OnceLock` caveat as the
schema registry:

**Listing 14.19 — src/read/optimizers.rs:1214–1245**

```rust
/// Indexed columns of the first TableScan below `plan` that has a tantivy
/// index. Stops at the first one (predicates above only see one scan in
/// practice; cross-table joins on indexed columns aren't supported in v1
/// — each filter is rewritten relative to its own subtree's scan).
```

```rust
/// Indexed columns for a table from the static schema registry — keyed by
/// column name, value is the resolved tokenizer (raw/default/ngram3).
/// Returns `None` when the table isn't in the registry.
///
/// The cache is populated *once* on first call. This is safe because
/// `schema_loader::registry()` is compiled-in YAML and immutable. If we ever
/// add runtime/hot-reload of schemas, this OnceLock must be replaced with an
/// invalidatable structure — newly-added Tantivy-indexed tables would
/// otherwise silently never accelerate.
```

That is the second of the two caches Chapter 6 warned about. Both name the same
precondition, in both directions.

### Ordered union for top-K

The rule that turns `ORDER BY timestamp DESC LIMIT n` into a streaming top-K over
a union of legs — and the shared helper that the read path (Chapter 13) also
calls:

**Listing 14.20 — src/read/optimizers.rs:1875–1913**

```rust
pub struct OrderedUnionForTopK;

/// Re-express `req` (sort keys, possibly indexed against a downstream projected
/// schema) as plain columns resolved by name against `schema`. Returns `None`
/// if any key is not a plain column or is absent from `schema` — in which case
/// the rule bails (correctness over cleverness).
```

```rust
/// The shared mechanism: `children` with every child that does not already
/// satisfy `req` wrapped in `SortExec(req)` (carrying `fetch`), so a union over
/// them advertises `req`.
///
/// Callers: this rule (top-K, `fetch` known) and `ProjectRoutingTable::scan`,
/// which uses it with no fetch so the mem ∪ hot ∪ delta union advertises the
/// table's lead sort key and `EnforceDistribution` satisfies `DedupExec`'s
/// `SinglePartition` with a `SortPreservingMergeExec` rather than a
/// `CoalescePartitionsExec` (which declares no ordering, leaving keep-greatest
/// dormant — see `docs/plans/2026-08-01-merge-on-read-dml.md` §3).
///
/// `Ok(None)` means "leave the plan alone":
/// - every child already satisfies `req` (nothing to inject), or
/// - a child that doesn't satisfy it is marked unsortable — `sortable[i] ==
///   false` says "this leg is a whole-window parquet scan; a blocking sort on
///   it costs far more than the ordering buys" (indices past `sortable`'s end
///   are sortable), or
/// - `require_ordered_child` and no child is ordered. The top-K rule sets this:
///   with the Delta footer pushdown off, neither branch is ordered and sorting
///   the mem branch alone buys nothing. `scan` does not — a MemBuffer-only scan
///   has no ordered leg by construction, yet is exactly where a fresh version
///   append lives, so it must still be ordered.
pub fn ordered_children(
```

This is the function Chapter 13's Listing 13.30 calls, and the doc comment
explains the parameter that differs between its two callers. The top-K rule sets
`require_ordered_child` because with the Delta footer pushdown off, sorting the
memory branch alone buys nothing. `scan` does not, because a MemBuffer-only scan
has no ordered leg by construction and yet is exactly where a fresh version append
lives.

`CoalescePartitionsExec` versus `SortPreservingMergeExec` is the whole game: the
former declares no ordering, which leaves `DedupExec`'s keep-greatest dormant.

### The custom function surface

One registration function, and reading it is the fastest way to learn what SQL
TimeFusion actually speaks:

**Listing 14.21 — src/read/functions.rs:394–455**

```rust
pub fn register_custom_functions(ctx: &mut datafusion::execution::context::SessionContext) -> Result<()> {
    // Register Variant-aware expr planner (must be before JSON planner for priority)
    datafusion::execution::FunctionRegistry::register_expr_planner(ctx, Arc::new(VariantAwareExprPlanner))?;

    // PgCoalesceUdf: PG parity coalesce that type-checks `coalesce(list_col, '{}')`,
    // replacing the built-in under the same name; see PgArrayLiteralRewriter.
    // JsonToPgTextUdf bridges variant -> Postgres ->> text semantics (numeric/bool/null → text/NULL).
    reg_from!(
        ctx,
        crate::read::optimizers::PgCoalesceUdf::default(),
        ToCharUDF::new(),
        AtTimeZoneUDF::new(),
        JsonBuildArrayUDF::new(),
        JsonbBuildArrayUDF::new(),
        ToJsonbUDF::new(),
        ToJsonUDF::new(),
        ExtractEpochUDF::new(),
        JsonToPgTextUdf::default(),
        datafusion_variant::JsonToVariantUdf::default(),
        VariantToJsonExtUdf::default(),
        VariantGetExtUdf::default(),
        datafusion_variant::CastToVariantUdf::default(),
        datafusion_variant::IsVariantNullUdf::default(),
        datafusion_variant::VariantPretty::default(),
        datafusion_variant::VariantListConstruct::default(),
        datafusion_variant::VariantListInsert::default(),
        datafusion_variant::VariantObjectConstruct::default(),
        datafusion_variant::VariantObjectInsert::default(),
        JsonbPathExistsUDF::new(),
        ApproxPercentileUDF::new(),
    );

    // create_udf-based UDFs that carry construction logic.
    ctx.register_udf(create_jsonb_array_elements_udf());
    ctx.register_udf(create_time_bucket_udf());
    ctx.register_udaf(create_percentile_agg_udaf());
    ctx.register_udaf(create_tdigest_merge_udaf());
    ctx.register_udaf(AggregateUDF::from(HllAggUDF::default()));
    ctx.register_udaf(create_hll_merge_udaf());
    ctx.register_udf(create_hll_count_udf());
    ctx.register_udf(hash_bucket_udf());

    // text_match(col, 'query') for tantivy-accelerated full-text search. Naive
    // substring fallback keeps correctness when tantivy is disabled or when
    // post-filtering MemBuffer rows; see [[tantivy_index/udf]].
    ctx.register_udf(crate::tantivy::udf::text_match_udf());

    // Test-only clock UDFs. Gated behind TIMEFUSION_ENABLE_TEST_UDFS so a
    // production deployment can't have its eviction/flush clock yanked by
    // a stray SQL session. Required by the long-duration bench harness in
    // `bench/timeseries_lifecycle.py` to simulate hours in seconds.
    if std::env::var("TIMEFUSION_ENABLE_TEST_UDFS").is_ok_and(|v| v == "true" || v == "1") {
        ctx.register_udf(create_set_clock_udf());
        ctx.register_udf(create_advance_clock_udf());
        ctx.register_udf(create_now_micros_udf());
        tracing::warn!("TIMEFUSION_ENABLE_TEST_UDFS=true; clock UDFs registered. Do NOT enable in production.");
    }

    Ok(())
}
```

The `reg_from!` macro collapses the repetitive registrations:

**Listing 14.22 — src/read/functions.rs:387–392**

```rust
/// Register all custom PostgreSQL-compatible functions
/// Collapse the repetitive `ctx.register_udf(ScalarUDF::from(T))` calls for
/// UDFs built straight from a unit/default struct.
macro_rules! reg_from {
    ($ctx:expr, $($udf:expr),+ $(,)?) => { $( $ctx.register_udf(ScalarUDF::from($udf)); )+ };
```

Function families, by purpose:

- **Postgres parity**: `to_char`, `at_time_zone`, `extract_epoch`,
  `json_build_array`, `jsonb_build_array`, `to_json`, `to_jsonb`,
  `jsonb_array_elements`, `jsonb_path_exists`, the shadowing `coalesce`.
- **Timescale parity**: `time_bucket`, `percentile_agg` + `approx_percentile`,
  `tdigest_merge`.
- **Variant**: `json_to_variant`, `variant_to_json`, `variant_get`,
  `cast_to_variant`, `is_variant_null`, `variant_pretty`, plus list/object
  construct/insert.
- **Sketches**: `hll_agg`, `hll_merge`, `hll_count` for approximate distinct
  counts.
- **Search**: `text_match`.
- **Routing**: `hash_bucket`.
- **Test-only clock control**, gated behind an env var with a `warn!` — because a
  stray SQL session must not be able to yank a production eviction clock.

And the registry is built once:

**Listing 14.23 — src/read/functions.rs:457–461**

```rust
pub type FnRegistry = dyn datafusion::execution::FunctionRegistry + Send + Sync;

/// Process-wide Arc'd FunctionRegistry pre-populated with all custom UDFs.
/// Lazy-init via OnceLock so test/bench harnesses that build many layers don't
/// re-register UDFs 20× per test. Production builds it once at startup either
/// way.
```

### The plan cache

The last piece of the read path, and the one with the most measured impact on the
dashboard workload.

The problem: dashboard SQL is *generated*, so it never repeats verbatim — each
refresh carries new timestamp and filter literals. A conventional plan cache keyed
by statement text would have a 0% hit rate.

The design is two caches plus a memo, all lock-free:

**Listing 14.24 — src/read/plan_cache.rs:445–482**

```rust
/// Lock-free plan cache.
///
/// The Mutex<LruCache> design was a serialization point on the hot read path:
/// every query — even on a cache hit — took the mutex to update LRU order.
/// At 50+ concurrent readers that became the dominant bottleneck.
///
/// OLAP workloads churn through a small set of templates (the harness's prod
/// replay sees ~5 unique canonical plans across millions of queries), so we
/// drop LRU entirely. DashMap gives us lock-free reads and a soft size cap
/// that just clears the cache once exceeded — cheap, correct, and never holds
/// a lock across the await in `handle_simple_query`.
pub struct PlanCacheHook {
    cache: WeighedMap<LogicalPlan>,
    hits: AtomicU64,
    misses: AtomicU64,
    /// Shape cache for LITERAL-bearing SELECTs (generated dashboard SQL that
    /// never repeats verbatim): keyed by the statement with every string
    /// literal replaced by `$N`, storing the pre-optimized placeholder plan +
    /// inferred parameter types. A hit clones the plan and substitutes the
    /// query's actual literals (cast to the inferred types) — skipping parse,
    /// analyze, AND optimize. `None` = negative entry: this shape failed to
    /// plan/parameterize once; don't retry it per query.
    /// Bounded by WEIGHT as well as count. It holds `LogicalPlan`s exactly like
    /// `cache` does, and was previously bounded by entry count alone — the same
    /// bug 12ff764 fixed for `cache` and never applied here.
    shapes: WeighedMap<Option<ShapeEntry>>,
    /// Canonical texts we served a pre-optimized substituted plan for, so
    /// `was_pre_optimized` can tell the handler to skip `state.optimize()`.
    /// Literal-bearing texts are one-shot (next dashboard refresh has new
    /// literals), so recency semantics with a soft cap are enough — a false
    /// `false` after eviction merely re-optimizes an optimized plan.
    served: DashMap<String, ()>,
    shape_hits: AtomicU64,
    shape_skips: AtomicU64,
    /// When true, `now()`-bearing SELECTs go through the shape cache with the
    /// time function parameterized (fresh instant substituted per query) instead
    /// of being bypassed. Off by default — it's the hot dashboard path, so enable
    /// deliberately (TIMEFUSION_PLAN_CACHE_TIME_FNS=1) after canarying.
    time_fn_shapes: bool,
}
```

Three observations.

**LRU was removed on purpose.** Every query, even a hit, took the mutex to update
LRU order, and at 50+ concurrent readers that was the dominant bottleneck. The
workload justifies it: ~5 unique canonical plans across millions of queries in a
production replay. A `DashMap` with a soft cap that clears wholesale is cheap and
correct for that distribution.

**"Bound by weight, not count" appears again**, with an explicit note that the fix
applied to `cache` was not applied to `shapes` for a while. Same lesson as the
demote queue (Chapter 10) and the Foyer weighter (Chapter 8): entries whose sizes
vary by orders of magnitude cannot be bounded by counting them.

**Negative entries are cached.** `None` in `shapes` means "this shape failed to
parameterize once; don't retry it per query" — the cheapest way to stop paying for
a known-failing analysis on every request.

The weight estimate:

**Listing 14.25 — src/read/plan_cache.rs:54–71**

```rust
/// Randomly halves a map once it reaches its soft cap.
// ... elided: the sweep helper
/// Approximate retained bytes per expression, calibrated against production.
const PLAN_BYTES_PER_EXPR: usize = 384;

/// Retained plan budget per cache slot.
const PLAN_CACHE_PLAN_BYTES_PER_SLOT: usize = 128 * 1024;

/// Sweep to half the budget to avoid repeated boundary crossings.
const SWEEP_LOW_WATER_NUM: usize = 1;
const SWEEP_LOW_WATER_DEN: usize = 2;

/// Estimated retained size of a plan, in bytes. See [`PLAN_BYTES_PER_EXPR`].
```

Sweeping to *half* the budget rather than to exactly the budget is the standard
hysteresis trick: sweeping to the boundary means the next insertion sweeps again.

### Time functions must not be cached

The subtlest correctness constraint in the cache:

**Listing 14.26 — src/read/plan_cache.rs:497–515**

```rust
/// Statements whose optimized plan embeds the QUERY START TIME must never be
/// cached: DataFusion const-folds these Stable functions during
/// `state.optimize()` (SimplifyExpressions reads query_execution_start_time),
/// so a cached plan would freeze `now()` at first-build time and serve stale
/// windows forever. Applies to BOTH the `$N` template cache and the shape
/// cache — such statements re-plan per query instead.
fn contains_plan_time_folded_fn(stmt: &Statement) -> bool {
    // Union of both classes; TIME_FNS == PARAMETERIZABLE ∪ UNPARAMETERIZABLE.
    stmt_uses_fn(stmt, PARAMETERIZABLE_TIME_FNS) || stmt_uses_fn(stmt, UNPARAMETERIZABLE_TIME_FNS)
}

/// The timestamp-returning time fns we can safely parameterize (replace the call
/// with a `$N` placeholder bound to the current instant, so the plan is reusable
/// and time stays fresh).
const PARAMETERIZABLE_TIME_FNS: &[&str] = &["now", "current_timestamp", "statement_timestamp", "transaction_timestamp", "clock_timestamp", "localtimestamp"];

/// Date/Time-returning time fns — different result type, riskier substitution —
/// so a query using any of these stays on the bypass path.
const UNPARAMETERIZABLE_TIME_FNS: &[&str] = &["current_date", "today", "current_time", "localtime"];
```

DataFusion const-folds `now()` during `optimize()`, reading
`query_execution_start_time`. A cached optimized plan therefore *freezes* `now()`
at first-build time and serves a stale window forever. The resolution splits the
time functions in two: the timestamp-returning ones can be *parameterized* (the
call is replaced by a `$N` placeholder bound to the current instant, so the plan is
reusable and time stays fresh), while the date/time-returning ones stay on the
bypass path because the substitution is riskier.

That parameterization is behind a flag — `TIMEFUSION_PLAN_CACHE_TIME_FNS` — which
`CLAUDE.md` records as canaried and turned on after 2026-07-19 flamegraphs put
~25% of CPU in `SessionState::optimize` on `now()`-bearing misses.

### The INSERT fast path

Two optimizations for the pgwire `INSERT` shape, both with measured costs:

**Listing 14.27 — src/read/plan_cache.rs:176–184**

```rust
/// Walk a plan and replace every `CAST(Literal(v), T)` with `Literal(cast(v, T))`.
///
/// After `replace_params_with_values` substitutes `$N → literal`, the `CAST`
/// wrappers `insert_coerce` puts around every placeholder turn into per-cell
/// `CAST(Literal, T)` exprs inside `ValuesExec`. Executing those casts at
/// query time, once per (row, column), is responsible for ~9–10 ms/row of
/// pgwire-INSERT overhead at the 88-col schema (measured). The cast values
/// are constant so we can fold them once, at substitution time, and let
/// `ValuesExec` see plain literals.
```

**Listing 14.28 — src/read/plan_cache.rs:223–231**

```rust
/// pgwire-INSERT bypass: recognise `Dml(Insert) → [Projection →] Values(literals)`
/// and short-circuit the whole DataFusion executor by building the RecordBatch
/// directly from the literals and calling `ProjectRoutingTable.fast_insert_batch`.
/// Skips `ValuesExec`, `DataSinkExec`, and the per-row `replace_params_with_values`
/// walk that together account for ~5-6 ms/row of overhead at the 88-col schema.
///
/// Returns `Ok(Some(rows))` on success, `Ok(None)` if the plan shape isn't
/// the supported fast-path INSERT (caller should fall back to the regular
/// `execute_logical_plan` path).
```

~15 ms/row of overhead removed between the two, at an 88-column schema. The
`Ok(None)` fallback shape is the right contract for a fast path: recognize the
supported shape, or decline and let the general path run.

### A vendored function that must be kept in sync

Not every dependency exposes what you need:

**Listing 14.29 — src/read/plan_cache.rs:356–366**

```rust
/// Mirror of `datafusion_postgres::handlers::dml_completion`,
/// which is `pub(super)` and so unreachable from outside the crate.
///
/// **Re-sync checklist.** When bumping the patched `datafusion-postgres` git dep
/// (apitoolkit/datafusion-postgres @ `timefusion-df54`, see the `[patch.crates-io]`
/// in Cargo.toml), diff its `handlers.rs::dml_completion` against this
/// implementation — upstream changes to the tag format ("INSERT 0 N" oid +
/// count), the `count` column name, or the count column's Arrow type are silent
/// divergence here (no compile error, wrong wire response). Search for the
/// `RE-SYNC-DML-COMPLETION` marker below and confirm parity.
```

A copied private function, with a named marker and a re-sync checklist, and an
explicit statement that divergence is *silent* — no compile error, just a wrong
wire response. When you must copy, this is how to leave the note.

### A narrow rewrite, and why narrowness is correctness

**Listing 14.30 — src/read/plan_cache.rs:521–536**

```rust
/// Rewrite `count(*)` to `count(1)` — but ONLY for a statement that DataFusion
/// rejects today, i.e. one whose `ORDER BY <ordinal>` points at a select item
/// that wraps `count(*)` in a larger expression. `None` for everything else.
///
/// The narrowness is the point, and it is a correctness requirement rather than
/// caution. `count(*)` and `count(1)` compute the same thing, but they do not
/// NAME the same thing: the wire-visible column for `SELECT count(*)` is
/// `count(*)`, and the shape cache lifts the injected `1` into a placeholder on
/// top of that, so a blanket rewrite renamed the column to `count($1)` for every
// ... elided: the remainder of the rationale and the implementation
```

Two semantically equivalent expressions that are not *nominally* equivalent, in a
system where the column name is wire-visible and the shape cache parameterizes
literals. A blanket rewrite renames the user's column to `count($1)`. Restricting
the rewrite to the statements DataFusion would otherwise reject is the only safe
scope.

### How to debug this flow

**`EXPLAIN` and `EXPLAIN ANALYZE`** work normally, but note two things: `EXPLAIN`
does *not* exercise rollup routing (the routing decision happens during scan), and
the plan you see reflects whichever session's rule set applied.

**Plan-cache counters** in `timefusion_stats`: `hits`, `misses`, `shape_hits`,
`shape_skips`. The ratio `shape_hits / (shape_hits + shape_skips)` is the
dashboard-path health metric; a collapse means either the SQL shape changed or a
negative entry is being hit.

**When a Variant query fails**, the error message usually names which rule did not
fire:

- `"Extension type name missing"` → the `VariantExtWrapper` marker was stripped.
- `XX000 "Physical input schema should be the same…"` → `VariantScanSchemaRestore`
  did not run after `optimize_projections`.
- `Struct op Utf8` coercion errors → the text-position lowering did not fire.
- Text OID 25 where a driver expected 3802 → the `tf.pg_type = jsonb` tag was
  lost, or the root wrap was not registered on this session.

**When the Tantivy prefilter does not engage**, the debug log names the reason
(Chapter 13): `empty_index`, `low_selectivity`, `field_coverage_gap`,
`delta_no_index_or_cap_exceeded`, `delta_error`.

**When partition pruning does not happen**, look for the `debug!` "Added N date
partition filter(s)"; zero usually means a `CAST` hid the literal, or the predicate
shape (a function call, a `BETWEEN` the pruner does not decompose) is not one the
rule recognizes.

**Key takeaways.** The Variant type needs three rules and a wrapper UDF because
its SQL-facing and storage schemas differ, and DataFusion's own
`optimize_projections` actively undoes one of them. Postgres parity is bought
with a shadowing `coalesce`, an array-literal rewriter, and an AST-level
`row_to_json` rewrite — each narrow, each justified. The Tantivy rewriter turns
`=`, `LIKE`, `IN` and regex into index lookups when the tokenizer and column type
allow, and refuses when they do not. The plan cache is deliberately lock-free and
LRU-free, keyed at two levels — exact text and literal-erased *shape* — and it
must never cache a plan whose `now()` DataFusion has already const-folded.
## 15. Merge-on-read: `DedupExec`, tombstones, and count pushdown

*After this chapter you will understand TimeFusion's merge-on-read model
end to end: why a row can exist in several versions, how `DedupExec` picks the
winner in both its bounded and unbounded modes, why the bound column stays in the
dedup key, how tombstones are filtered, and how `COUNT(*)` can be answered from
file statistics without reading a single row.*

### Why versions exist

On a `version_append` table, `UPDATE` and `DELETE` do not rewrite files. They
append a *new version* of the row, carrying:

- the same `dedup_keys` tuple — on `otel_logs_and_spans`, `(timestamp, id)`;
- a fresh `dedup_tiebreak` (`updated_at`), stamped by TimeFusion from a per-table
  monotonic clock;
- `deleted = true` if it is a delete.

Reads must therefore collapse versions. The rule is **keep-greatest by
tiebreak within each key group**, then drop the group if the winner is a
tombstone.

The consequence that shapes everything else: an appended version carries the
row's *original* timestamp into a *new* file, so Delta files overlap in time and
no file ordering can be declared. That is why the Delta leg is structurally
unsortable (Chapter 13) and why a read-time `SortExec` over it has twice
exhausted the query pool.

### `LegKind`: identity that carries its own sortability

**Listing 15.1 — src/read/mod.rs:111–145**

```rust
/// Which union leg a row came from. Also carries the sortability the plan
/// builder needs, so the two can no longer drift apart in parallel vectors —
/// the Delta leg is the one that must never be sorted at read time (an UPDATE
/// writes a row's ORIGINAL timestamp into a NEW file, so its files overlap and
/// the blocking sort that "fixes" that exhausted the query pool, prod
/// 2026-08-02).
#[derive(Clone, Copy, Debug, PartialEq, Eq, strum::IntoStaticStr)]
#[strum(serialize_all = "lowercase")]
pub enum LegKind {
    Mem,
    Hot,
    Delta,
}

impl LegKind {
    pub fn sortable(self) -> bool {
        !matches!(self, LegKind::Delta)
    }

    pub fn label(self) -> &'static str {
        self.into()
    }

    fn counter(self) -> &'static std::sync::atomic::AtomicU64 {
        match self {
            LegKind::Mem => &ORDERING_VIOLATIONS_MEM,
            LegKind::Hot => &ORDERING_VIOLATIONS_HOT,
            LegKind::Delta => &ORDERING_VIOLATIONS_DELTA,
        }
    }
}
```

Three responsibilities on one enum: the leg's identity, its sortability, and its
diagnostic counter. `strum::IntoStaticStr` supplies the label. The doc comment
explains why sortability lives *here* rather than in a parallel `Vec<bool>` — the
two used to drift.

### `DedupExec`

**Listing 15.2 — src/read/mod.rs:376–404**

```rust
pub struct DedupExec {
    input: Arc<dyn ExecutionPlan>,
    keys: Vec<String>,
    /// Indices of the key columns within `input.schema()`.
    key_idxs: Vec<usize>,
    /// Schema's `dedup_tiebreak` column name, when the table declares one.
    /// Keep-greatest engages only if it is also present in the input schema.
    tiebreak: Option<String>,
    /// Ordering keep-greatest DEPENDS on, declared as *required* so
    /// `EnforceSorting` preserves it.
    ///
    /// Without this the operator is silently correctness-fragile: the caller
    /// builds a `SortPreservingMergeExec` to supply the run property, but
    /// EnforceSorting deletes any ordering no parent requires — which is every
    /// aggregate. The plan then reaches `execute` unordered, `detect_bound`
    /// returns `None`, keep-greatest degrades to keep-FIRST, and a merge-on-read
    /// table answers `MAX(updated_at)` with the PRE-update row while the same
    /// data read by a plain projection answers correctly. Requiring the ordering
    /// is what makes version resolution a property of the operator rather than
    /// of what happens to sit above it.
    required_ordering: Option<datafusion::physical_expr::LexOrdering>,
    /// Indices into `input.schema()` to emit after dedup, restoring the
    /// originally-requested projection. `None` = emit the input schema as-is.
    output_projection: Option<Vec<usize>>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}
```

The `required_ordering` comment describes as clean a bug as you will find in a
query engine, so it is worth restating:

1. The scan builder constructs a `SortPreservingMergeExec` to give `DedupExec`
   the ordered input keep-greatest needs.
2. `EnforceSorting` deletes any ordering that no *parent* requires — and no
   aggregate requires one.
3. So the plan reaches `execute` unordered, `detect_bound` returns `None`, and
   keep-greatest degrades to keep-**first**.
4. Result: `SELECT MAX(updated_at) …` returns the **pre-update** row, while the
   same data read by a plain projection returns the correct one.

Two query shapes over the same rows disagreeing, with no error anywhere. The fix
— declaring the ordering *required* on the operator — is the general lesson:
**a property your operator depends on must be declared by your operator, not
assumed from what happens to sit above it.**

Construction:

**Listing 15.3 — src/read/mod.rs:410–444**

```rust
    pub fn with_tiebreak(input: Arc<dyn ExecutionPlan>, keys: Vec<String>, tiebreak: Option<String>, output_projection: Option<Vec<usize>>) -> DFResult<Self> {
        let in_schema = input.schema();
        let key_idxs = keys
            .iter()
            .map(|k| in_schema.index_of(k).map_err(|_| DataFusionError::Internal(format!("DedupExec key `{k}` not in input schema"))))
            .collect::<DFResult<Vec<_>>>()?;
        let schema = match &output_projection {
            Some(idxs) => Arc::new(in_schema.project(idxs)?),
            None => in_schema.clone(),
        };
        // Dedup preserves the input's row order (it only drops rows), so the
        // input's output ordering remains valid on the output (remapped through
        // the projection). Without this the sorted Delta scan's declared order
        // (fork sort-order pushdown) dies here and `ORDER BY timestamp LIMIT n`
        // re-sorts the whole window instead of early-terminating.
        let eq = match remap_ordering(&input, &output_projection, &schema) {
            Some(ordering) => datafusion::physical_expr::EquivalenceProperties::new_with_orderings(schema.clone(), [ordering]),
            None => datafusion::physical_expr::EquivalenceProperties::new(schema.clone()),
        };
        let properties =
            Arc::new(PlanProperties::new(eq, Partitioning::UnknownPartitioning(1), input.properties().emission_type, input.properties().boundedness));
        Ok(Self { input, keys, key_idxs, tiebreak, required_ordering: None, output_projection, schema, properties, metrics: ExecutionPlanMetricsSet::new() })
    }

    /// Declare the ordering keep-greatest needs (see `required_ordering`).
    /// `None` leaves the operator ordering-agnostic — the pre-merge-on-read
    /// behaviour every table without `version_append` keeps.
    pub fn requiring(mut self, ordering: Option<datafusion::physical_expr::LexOrdering>) -> Self {
        self.required_ordering = ordering;
        self
    }
```

The *output* side matters as much as the input: an operator that only drops rows
preserves ordering, and saying so is what keeps `ORDER BY timestamp DESC LIMIT n`
an early-terminating top-K instead of a full re-sort. Forgetting to propagate an
equivalence property is a silent performance cliff, not an error.

`remap_ordering` handles the projection:

**Listing 15.4 — src/read/mod.rs:350–355**

```rust
/// The input's output ordering, remapped through `output_projection` onto the
/// dedup output schema. Keeps the longest prefix of plain-column sort exprs
/// whose columns survive the projection (`map_while`: a non-column or
/// projected-away expr truncates it); `None` when nothing survives or the input
/// declares no ordering.
```

Keeping the longest *prefix* is exactly right: `[a, b, c]` with `b` projected away
leaves `[a]` valid, not `[a, c]`.

### Bounded versus unbounded: two modes, two budgets

`DedupExec` has two execution strategies.

**Bounded mode** exploits an ordered input: within a *run* of equal bound values
(equal timestamps), all versions of a key are adjacent, so once the bound advances
the run's state can be released and its winners emitted. This is what enables
`LIMIT` early termination.

**Unbounded mode** buffers to end-of-stream, because nothing guarantees where a
key's versions are.

Both are capped:

**Listing 15.5 — src/read/mod.rs:51–70**

```rust
/// Encoded Arrow keys, allocated only on first sight.
type SeenSet = HashSet<Box<[u8]>, ahash::RandomState>;

/// Maximum retained bytes for one ordered timestamp run.
const RUN_BUFFER_MAX_BYTES: usize = 64 * 1024 * 1024;

/// Per-query ceiling for unordered keep-greatest state.
const UNBOUNDED_GREATEST_MAX_BYTES: usize = 2 * 1024 * 1024 * 1024;

fn check_unbounded_growth(current: usize, additional: usize) -> DFResult<()> {
    let requested =
        current.checked_add(additional).ok_or_else(|| DataFusionError::ResourcesExhausted("unordered merge-on-read dedup buffer size overflow".to_string()))?;
    if requested > UNBOUNDED_GREATEST_MAX_BYTES {
        return Err(DataFusionError::ResourcesExhausted(format!(
            "unordered merge-on-read dedup exceeded its {} MiB per-query limit; narrow the time window or compact unsorted files",
            UNBOUNDED_GREATEST_MAX_BYTES / 1024 / 1024
        )));
    }
    Ok(())
}
```

64 MiB per run versus 2 GiB per query is a 32× difference, and it is the number
behind Chapter 13's empty-leg bug: one `EmptyExec` vetoing the merge dropped a
point lookup from bounded mode into the 2 GiB unbounded ceiling.

The error message is a good one — it names the two things an operator can
actually do about it: narrow the window, or compact the unsorted files.

### The `Bound`: tracking a run, and catching a lying footer

**Listing 15.6 — src/read/mod.rs:72–109**

```rust
/// Tracks an ordered timestamp run so its dedup state can be released promptly.
struct Bound {
    /// Bound column index within the input schema.
    idx: usize,
    /// True when the sort is descending (bound decreases down the stream).
    desc: bool,
    /// The current run's bound value; `None` until the first row.
    last: Option<i64>,
}

impl Bound {
    /// Counts ordering violations for a specific union leg.
    fn advance_counting(&mut self, t: i64, leg: LegKind) {
        if let Some(l) = self.last
            && if self.desc { t > l } else { t < l }
        {
            leg.counter().fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        if self.last.is_none_or(|l| if self.desc { t < l } else { t > l }) {
            self.last = Some(t);
        }
    }

    fn advance(&mut self, t: i64) -> bool {
        // A value moving AGAINST the declared direction proves this scan's
        // advertised ordering is false — a parquet footer's `sorting_columns` is
        // lying. Dedup stays sound either way (`dedup_key_idxs`), but this is the
        // only direct signal that the hot-tail footer repair still has work to
        // do; a zero here across prod would exonerate footers entirely and send
        // the 2026-08-07 under-count investigation elsewhere.
        if let Some(l) = self.last
            && if self.desc { t > l } else { t < l }
        {
            ORDERING_VIOLATIONS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        self.last.is_none_or(|l| if self.desc { t < l } else { t > l }) && self.last.replace(t).is_some()
    }
}
```

`advance` returns whether the run *closed*, and simultaneously counts violations.
A value moving against the declared direction is proof that a Parquet footer's
`sorting_columns` is lying — the operator stays correct regardless, but the
counter is the only direct evidence that footer repair still has work.

### The bound column must stay in the key

The single most instructive comment in the read path:

**Listing 15.7 — src/read/mod.rs:307–328**

```rust
/// The dedup key columns to hash, given the chosen bound.
///
/// The bound column is ALWAYS retained. It used to be filtered out here: within
/// a *genuinely* sorted run the bound is constant, so encoding it into every key
/// is redundant, and dropping it saved one timestamp encoding per physical row.
///
/// That reasoning holds only while the declared ordering is TRUE. A parquet
/// footer missing/misreporting `sorting_columns` makes a scan declare
/// `output_ordering=[timestamp DESC]` over data that is not in that order (see
/// the hot-tail footer repair). The bound then never advances across the
/// mis-ordered stretch, one "run" spans many timestamps, and a key reduced to
/// `id` alone collapses rows that differ only in `timestamp` — distinct rows.
/// Prod 2026-08-07: a single minute read 132 rows instead of 1620, surfacing as
/// multi-minute holes in customer dashboards.
///
/// Keeping the bound in the key makes bounded mode fail-SAFE: a false ordering
/// can now only under-dedup (emit a duplicate), never drop a distinct row.
```

The optimization was *locally valid*: within a genuinely sorted run the bound is
constant, so it contributes nothing to the key. It was globally wrong because the
premise — "the declared ordering is true" — is supplied by a Parquet footer that
can lie. When it lies, one "run" spans many timestamps, and a key reduced to `id`
alone collapses genuinely distinct rows. Production read 132 rows where 1,620
existed, surfacing as multi-minute holes in customer dashboards.

The fix is not "make footers honest" (that is the repair pass's job, Chapter 19)
but **make the failure mode safe**: keeping the bound in the key means a false
ordering can only under-dedup — emit a duplicate — never drop a distinct row.

That is the pattern to take away: when an optimization depends on an assumption
you cannot verify cheaply, arrange for the assumption's failure to be
*conservative*, not silent.

Bounded mode's entry condition, and its kill switch:

**Listing 15.8 — src/read/mod.rs:329–336, 277–289**

```rust
/// Enable bounded mode iff the input's leading sort column is a dedup key of an
/// i64-backed type AND `timefusion_read_dedup_bounded` is on.
///
/// The ordering here is *declared*, never verified — `output_ordering()` is only
/// as trustworthy as the parquet footer behind it. `dedup_key_idxs` keeps the
/// operator sound when that declaration lies; the flag is the kill switch for
/// when it lies badly enough to matter, and defaults OFF until the footer repair
/// has drained the poisoned files.
```

```rust
/// Emergency kill switch for bounded[timestamp] dedup. Defaults ON.
///
/// Correctness does NOT depend on this — `dedup_key_idxs` keeps the operator
/// sound under a lying footer. Turning it off is a big hammer with a real cost:
/// bounded mode is what lets keep-greatest emit per run instead of buffering to
/// end-of-stream, so disabling it also disables LIMIT early termination
/// (`keep_greatest_limit_terminates_early` runs unbounded and does not finish).
/// A "top 100" log-explorer query would scan the whole window. Reach for it only
/// if a bounded scan is proven to be serving wrong rows again.
```

⚠️ The two comments disagree about the default: `bounded_dedup_enabled`'s doc says
"Defaults ON", while `detect_bound`'s says the flag "defaults OFF until the footer
repair has drained the poisoned files". `src/config.rs:1928–1929` settles it —
`timefusion_read_dedup_bounded` is `#[serde_inline_default(true)]`, so it is **on**
by default, and `detect_bound`'s comment is stale.

### Keep-greatest internals

The run state is deliberately shaped around the common case:

**Listing 15.9 — src/read/mod.rs:601–612, 705–707, 748–751**

```rust
/// Winning row for a dedup key within the open run: where it sits in the
/// buffered batches, plus its order-encoded tiebreak for comparison.
```

```rust
/// Most telemetry timestamps identify one logical event, so a bounded run
/// normally has one winner (plus its physical MOR copies). Avoid hashing those
/// tiny runs; promote only an unusually wide equal-timestamp run to a map.
```

```rust
/// Owned winner tiebreak. Timestamp/int64 values stay primitive; other schema
/// types retain Arrow's generic order-preserving encoding.
```

```rust
/// Keep-greatest run state: the open run's batches (Arc clones) and its
/// per-key winners.
```

Two adaptations to the actual data distribution. **Small runs use a linear scan,
not a hash map**, because most telemetry timestamps identify one logical event and
a run typically holds one winner plus its physical copies; only an unusually wide
equal-timestamp run is promoted to a map. And **the tiebreak stays primitive for
i64-backed types**, falling back to Arrow's generic order-preserving encoding only
for other types — one comparison instead of an encode-and-compare.

The projection is applied late, and there is a measured reason:

**Listing 15.10 — src/read/mod.rs:972–990**

```rust
/// Filter only columns the caller will consume. COUNT and narrow projections
/// augment the scan with ID/tiebreak columns solely for winner selection;
/// filtering those variable-width arrays and then throwing them away was a
/// large avoidable copy on amplified merge-on-read scans.
```

```rust
/// Keep-first: drop rows whose key tuple was already emitted, then restore the
/// requested projection. Returns `None` when nothing survives (caller pulls the
/// next batch).
```

Recall from Chapter 13 that the scan projection is *augmented* with dedup keys,
the tiebreak, and the tombstone column. Those augmented columns are variable-width
(`id` is ~43% of an OTel file's bytes) and are discarded immediately after winner
selection — so filtering them is a large avoidable copy.

### The behaviours pinned by tests

The test names in `src/read/mod.rs` read as a specification of keep-greatest:

**Listing 15.11 — src/read/mod.rs (test function names, 1252–1581)**

```rust
    async fn keep_greatest_without_a_bound_still_picks_the_newest_version() {
    async fn keep_greatest_string_tiebreak_uses_generic_ordering() {
    async fn keep_greatest_picks_highest_tiebreak_across_batches() {
    async fn keep_greatest_across_run_boundary() {
    async fn keep_greatest_null_tiebreak_loses() {
    async fn keep_greatest_limit_terminates_early() {
    fn keep_greatest_run_state_is_bounded() {
```

Seven properties: it works without a bound; string tiebreaks use generic ordering;
winners are found across batch boundaries; runs close correctly; a NULL tiebreak
always loses (so an un-stamped legacy row can never beat a stamped one); `LIMIT`
terminates early; and run state is bounded. Naming tests after the property rather
than the function is what makes a test list readable as a spec.

### Tombstones

The tombstone filter sits *above* `DedupExec` (Chapter 13, Listing 13.30), which
is the only correct placement: a tombstone version must survive dedup in order to
*win* its key group and suppress it. Filtering below the dedup would drop the
tombstone and resurrect the row.

The encoding is three-valued and chosen so that no backfill was needed
(Chapter 6): `NULL` and `false` both mean live; only `true` is a tombstone. That
is why `validate()` requires the column to be a *nullable* `Boolean`.

`tombstones_possible()` (Listing 6.5) gates the `COUNT(*)` fast path below, and it
reads the column's *presence*, never the `version_append` flag — because rows
already written outlive the flag being turned off.

### Count pushdown

The most valuable optimization in the read path for dashboard workloads:
`COUNT(*)` over a time window, answered without reading a row.

**Listing 15.12 — src/read/mod.rs:1774, 1831–1847**

```rust
/// Predicate classification for one conjunct.
```

```rust
/// Peel one alias layer, so `count(*) AS n` matches like `count(*)`.
```

```rust
/// The matched query shape: table + project + inclusive window.
```

```rust
/// Match the COUNT(*) shape and extract the (table, project, window).
```

Window resolution is where the pragmatism is:

**Listing 15.13 — src/read/mod.rs:1923–1934**

```rust
/// Resolve the count window's bounds. A lower bound is required (an unbounded
/// count would scan everything). A one-sided `timestamp > cutoff` (no upper
/// bound) is the common dashboard/export shape: treat the missing upper bound as
/// `now`, keeping the window bounded so the dedup-clean date check stays cheap.
/// The downstream MemBuffer-flushed + dedup-clean gates keep the result exact —
/// an unflushed or dirty recent tail simply bails to a normal scan. Returns
/// `None` when there's no lower bound or the window is empty (`lo > hi`).
```

A missing upper bound is *synthesized as now* rather than refused, because the
downstream gates keep the result exact anyway: if the recent tail is unflushed or
dirty, the pushdown bails to a normal scan.

The summing rule is strict in exactly the right place:

**Listing 15.14 — src/read/mod.rs:1936–1941**

```rust
/// Pure summing logic over per-file `(min_ts, max_ts, num_records)` stats:
/// `Some(total)` when every window-overlapping file is FULLY inside `[lo,hi]`,
/// `None` when a boundary file straddles (or stats are missing → caller
/// passes `None` fields → bail).
```

A file that *straddles* the window boundary cannot contribute its whole
`numRecords`, and there is no way to know how many of its rows fall inside without
reading it. So the pushdown refuses. Note that this is a pure function over
`(min_ts, max_ts, num_records)` triples — no IO, trivially unit-testable, which is
the same seam discipline as `decide_prefilter` (Chapter 13) and
`pressure_permit_claim_at` (Chapter 7).

Two sources can answer, in order of preference:

**Listing 15.15 — src/read/mod.rs:1953–2018 (excerpts)**

```rust
/// Attempt the pushdown. `Ok(None)` = not applicable, plan normally.
pub async fn try_count_pushdown(plan: &LogicalPlan, database: &Arc<Database>) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
    if !database.config().maintenance.timefusion_count_pushdown {
        // ... elided: bail
    }
    // ... elided: shape match, window resolution
        debug!("count_pushdown: answered {}/{} [{}, {}] = {} from logical-count index", q.project_id, q.table_name, q.lo, q.hi, total);
        crate::observability::record_logical_count_pushdown_used();
    // ... elided
            debug!("count_pushdown: bailed for {}/{} (stats gaps or boundary files)", q.project_id, q.table_name);
    // ... elided
    debug!("count_pushdown: answered {}/{} [{}, {}] = {} from add-action stats", q.project_id, q.table_name, q.lo, q.hi, total);
    crate::observability::record_count_pushdown_used();
```

1. **The logical-count index** — an exact per-partition merge-on-read count,
   maintained in `Database::logical_count_cache` with single-flighted, bounded
   background builds (`logical_count_building`, `logical_count_build_sem` in
   Listing 7.12's neighbourhood). "Logical" because it counts *live* rows after
   version collapse and tombstones, which raw file stats cannot.
2. **Delta add-action statistics** — `numRecords` per file, summed when every
   overlapping file is fully inside the window. Only valid when
   `tombstones_possible()` is false, since a tombstoned row still counts in
   `numRecords`.

Two separate metrics (`record_logical_count_pushdown_used` versus
`record_count_pushdown_used`) so the dashboards can tell which source answered.

And the stats extraction:

**Listing 15.16 — src/read/mod.rs:2125–2126**

```rust
/// Extract `(min_ts, max_ts, numRecords)` for this project's files from the
// ... elided: the add-action iteration
```

### Putting it together: three query shapes

For the payments span's table, over the same one-hour window:

**`SELECT count(*) …`** — no rows read at all if the window is fully covered by
files whose min/max timestamps fall inside it and the logical-count index has the
partition. Otherwise it falls back to a scan with `DedupExec`, because a
tombstoned or superseded row must not be counted.

**`SELECT … ORDER BY timestamp DESC LIMIT 100`** — bounded mode. The Delta scan
declares `[timestamp DESC]` from the footer, the memory legs are sorted to match,
a `SortPreservingMergeExec` merges them, `DedupExec` emits per run, and the outer
limit terminates early. Nothing beyond the first ~100 distinct keys is decoded.

**`SELECT … WHERE 'trace_124' = ANY(hashes)`** — `hashes` is the one *mutable*
column, so the predicate cannot be pushed below the dedup. It stays in a
`FilterExec` above `DedupExec`, and the scan below is bounded only by the time
window and whatever Tantivy could prune. This is the shape that was killing the
process before the mutability model landed.

### How to debug this flow

**`ordering_violations_total`** — nonzero means some scan's declared ordering is
false, i.e. a Parquet footer is lying. Dedup is still correct (the bound is in the
key), but bounded mode's run boundaries are wrong and the footer repair has work.

**`ordering_violations_by_leg()`** — the attribution, exposed in
`timefusion_stats` as three counters. Only populated when
`TIMEFUSION_ORDERING_PROBE=true`, and the doc explains why it is off by default:

**Listing 15.17 — src/read/mod.rs:151–164**

```rust
/// Diagnostic wrapper that answers "which leg's declared ordering is false?".
///
/// `ORDERING_VIOLATIONS` is counted inside `DedupExec`, which is single-partition
/// and sits above the mem ∪ hot ∪ delta union — so by the time a violation is
/// seen the row's leg is gone, and the plan algebra alone cannot say which leg
/// lied (I tried; every leg looks honest on paper because a leg that declares
/// nothing stops the union from declaring either). This checks each leg against
/// its OWN declared ordering, so a nonzero counter names the culprit directly.
///
/// OFF by default (`TIMEFUSION_ORDERING_PROBE`): it costs one i64 compare per
/// row per leg, which is the same order as the bound check it duplicates. Turn
/// it on when `ordering_violations_total` is nonzero and you need attribution.
#[derive(Debug)]
pub struct OrderingProbeExec;
```

"I tried; every leg looks honest on paper" is an unusually candid note, and the
reasoning behind it is sound: a leg that declares *no* ordering prevents the union
from declaring one, so static analysis cannot distinguish "declared nothing" from
"declared falsely".

**`mor_delta_leg_sorts`** — must stay 0. A nonzero value means a read-time
`SortExec` appeared over the Delta leg, which is the shape that exhausted the
query pool twice.

**`ResourcesExhausted: unordered merge-on-read dedup exceeded its 2048 MiB
per-query limit`** — an unbounded dedup hit its ceiling. Either the input lost its
ordering (check for an empty leg, or `EnforceSorting` deleting the merge) or the
window genuinely holds too many versions and the partition needs a dedup sweep.

**`DedupExec key '<k>' not in input schema`** — the projection augmentation
(Chapter 13, Listing 13.21) did not include a key. Historically this happened when
the dedup skip was decided *after* the projection was built.

**Count-pushdown debug lines** name which source answered or why it bailed:
`"answered … from logical-count index"`, `"answered … from add-action stats"`,
`"bailed … (stats gaps or boundary files)"`.

**Key takeaways.** Merge-on-read makes an `UPDATE` an append, so reads must
collapse versions with keep-greatest by tiebreak and then filter tombstones above
the dedup. `DedupExec` declares the ordering it depends on as *required*, because
otherwise `EnforceSorting` deletes it and the operator silently degrades to
keep-first — making two query shapes over the same rows disagree. Bounded mode is
a 32× memory saving and the enabler of `LIMIT` early termination, and it keeps the
bound column in the dedup key so that a lying Parquet footer can only produce a
duplicate, never drop a distinct row. `COUNT(*)` can be answered from a logical
count index or from Delta file statistics, but only when no boundary file
straddles the window and no tombstone could exist.
## 16. `UPDATE` and `DELETE`: interception, version append, and coalescing

*After this chapter you will know how TimeFusion intercepts DML before
DataFusion can plan it, how `UPDATE … FROM` is decomposed into a target scan and
a materialized source, why the mutability declaration from Chapter 6 is enforced
here, what "version append" does instead of a Delta `MERGE`, and how the DML
coalescer batches deferred Delta legs — including what it does when one fails
terminally.*

### Why DML is intercepted

DataFusion can plan an `UPDATE` into a scan plus a write, but that is not what
TimeFusion needs. The target rows live in three places (MemBuffer, hot tier,
Delta), the Delta side may need a `MERGE` or a version append depending on the
table's schema, and the WAL must record the *statement* so replay can reapply it.
So `DmlQueryPlanner` intercepts:

**Listing 16.1 — src/dml.rs:126–144**

```rust
pub struct UpdateSourcePlan {
    // ... elided: the unmaterialized source plan + join keys
}

#[derive(Debug)]
pub struct DmlQueryPlanner {
    // ... elided: the Database handle and the inner planner it delegates to
}
```

and one of its jobs is to swap in a rollup rewrite without taking the plan apart:

**Listing 16.2 — src/dml.rs:145–172**

```rust
/// Give `plan`'s columns the qualifiers `target` carries, field for field.
///
/// The rollup SQL produces the right NAMES — its aliases are the aggregate's own
/// field names — but SELECT aliases are unqualified, while an aggregate's
/// group-by column keeps its source qualifier. A `Column` reference in an
/// untouched node above resolves on `(qualifier, name)`, so without this the
/// substitution would not resolve.
```

```rust
/// Swap `replacement` in for the `matched` node, leaving every other node as the
/// optimizer produced it.
///
/// This is the whole reassembly. It replaces peeling the plan apart and
/// rebuilding it, which could only ever accept a fixed grammar of parent nodes
/// and kept declining production shapes that had one layer more.
```

"It replaces peeling the plan apart and rebuilding it, which could only ever
accept a fixed grammar of parent nodes and kept declining production shapes that
had one layer more" is a general lesson about plan rewriting: **substitute a
subtree in place; do not reconstruct the tree.** The reconstruction approach fails
open-endedly, one production shape at a time.

### Extracting the DML shape

**Listing 16.3 — src/dml.rs:316–345**

```rust
/// Extract DML information from logical plan.
///
/// Walks the projection/filter/scan chain of `dml.input`. When a `Join` is
/// encountered (i.e. the user wrote `UPDATE t SET … FROM src WHERE t.k = src.k`),
/// it identifies which side scans the target table, extracts equi-join keys, and
/// stashes the *other* side's `LogicalPlan` for later async materialization. The
/// walk then continues down the target side as a plain `UPDATE`.
fn extract_dml_info(input: &LogicalPlan, table_name: &str, extract_assignments: bool) -> Result<DmlInfo> {
    // Imperative descent: each node kind updates a different slot of the state and
    // the walk is not a fixed-length iteration, so a fold would just thread the same
    // four fields by hand. Iterative (not recursive) — plan trees can be deep.
    let mut current_plan = input;
    let mut predicate: Option<Expr> = None;
    let mut assignments = None;
    let mut project_id = String::new();
    let mut source_plan: Option<UpdateSourcePlan> = None;

    loop {
        match current_plan {
            LogicalPlan::Projection(proj) if extract_assignments => {
                match &mut assignments {
                    // First Projection encountered: real UPDATE assignments.
                    None => assignments = Some(extract_assignments_from_projection(proj)),
                    // Nested Projection (DataFusion CSE introduces one that defines
                    // `__common_expr_*`). Inline its aliases into our assignments so
                    // references to those synthetic columns resolve when we evaluate
                    // physical exprs against the bare table schema below.
                    Some(existing) => inline_projection_aliases(proj, existing)?,
                }
                current_plan = proj.input.as_ref();
```

Two style notes justified inline. **Imperative, not a fold**, because each node
kind updates a different slot and the walk is not a fixed-length iteration — a
fold would thread four fields by hand for no gain. **Iterative, not recursive**,
because plan trees can be deep — the same concern that produced the 32 MiB worker
stacks (Chapter 9).

The nested-`Projection` case is a real DataFusion detail: common-subexpression
elimination introduces a projection defining `__common_expr_1` and friends, and
those synthetic references must be inlined or they survive into MemBuffer's
physical-expression evaluation against the bare table schema and fail with "Column
not found":

**Listing 16.4 — src/dml.rs:573–577**

```rust
/// Inline aliases from a nested (CSE) Projection into the existing UPDATE assignment
/// exprs. Without this, refs like `__common_expr_1` survive into mem_buffer's physical
/// expr evaluation against the bare table schema and fail with "Column not found".
```

What counts as an assignment is also subtler than it looks:

**Listing 16.5 — src/dml.rs:554–556**

```rust
/// Extract UPDATE assignments from a projection: aliased exprs that actually
/// change the column (a bare `col AS col` passthrough is not an assignment).
```

DataFusion's `UPDATE` plan projects *every* column, aliasing each to its own name;
only the ones whose expression differs from a bare column reference are real
assignments.

### `UPDATE … FROM`: splitting the join

`UPDATE … FROM` is the shape monoscope's enrichment pipeline uses, and it is why
one of the four dependency forks exists (Chapter 4: the `datafusion-sql` fork
removes a `not_impl_err!` guard so the statement reaches this planner at all).

**Listing 16.6 — src/dml.rs:443–460**

```rust
/// Walk a [`LogicalPlan`] tree until we hit a `TableScan`. Returns the matched
/// scan's qualified name or `None` if no scan is reachable.
```

```rust
/// Given a `Join` and the target table name, decide which child is the target
/// (the side that scans the target table) and extract equi-join key pairs in
/// `(target_col_name, source_col_name)` order.
```

Key extraction has to see through the casts DataFusion inserts:

**Listing 16.7 — src/dml.rs:494–501**

```rust
/// Pull a bare column name (drop any table qualifier) from an `Expr::Column`.
/// Unwraps `Alias`, `Cast`, and `TryCast` — DataFusion's logical planner often
/// inserts an implicit cast on join keys when the two sides have slightly
/// different types (e.g. `Utf8` vs `Utf8View`), which is irrelevant for the
/// purposes of identifying which target column the join key resolves to.
/// Returns `None` for any other expression shape, which propagates as a clean
/// "not supported" error to the caller.
```

The same see-through-the-cast discipline as the partition pruner (Chapter 14),
with the same failure mode if omitted: a silent non-match rather than an error.

The source side is materialized, capped, and carried:

**Listing 16.8 — src/dml.rs:96–111, 38–49**

```rust
/// Materialized RHS of an `UPDATE ... FROM` statement together with the
/// equi-join key spec that pairs target rows with source rows.
///
/// `batch` is the fully-materialized source side (capped at
/// [`MAX_UPDATE_SOURCE_ROWS`]). Assignment exprs reference its columns via
/// the `source` qualifier (e.g. `col("source.value")`); downstream code
/// expects those refs to resolve against `schema`.
#[derive(Debug, Clone)]
pub struct UpdateSource {
```

```rust
/// Reject larger `UPDATE ... FROM` sources before materializing them.
const MAX_UPDATE_SOURCE_ROWS: usize = 1_000_000;
const SLOW_DML_PHASE_US: u64 = 1_000_000;
/// Maximum source rows in one merge-on-read scan. Each chunk becomes bounded
/// IN-lists on the complete join key, so a large enrichment UPDATE never falls
/// back to decoding and deduplicating its whole target time window.
// ... elided: the intermediate constant
const MOR_KEY_PUSHDOWN_ROWS: usize = 256;
```

`MOR_KEY_PUSHDOWN_ROWS = 256` is the number that makes a large enrichment
`UPDATE` affordable: each chunk of 256 source rows becomes bounded `IN`-lists on
the complete join key, which engages the Parquet bloom filters, instead of
decoding and deduplicating the whole target time window.

### The mutability gate

Chapter 6 promised that the `mutable` declaration is *enforced, not trusted*.
This is where:

**Listing 16.9 — src/dml.rs:410–441**

```rust
    // below the merge-on-read dedup on that basis — sound only while every
    // version of a row agrees on their value. An UPDATE assigning an undeclared
    // column would break that silently and at read time, surfacing a stale
    // version that matches a predicate the winning version does not. Refuse at
    // plan time so the declaration is enforced rather than trusted.
    //
    // The tiebreak and tombstone are exempt: `stamp_version` rewrites the
    // tiebreak on every append and a delete appends a tombstone row, so both are
    // mutable by construction and already excluded from the pushdown.
    if let Some(assigned) = assignments.as_ref()
        && let Some(schema) = crate::schema::get_schema(table_name).filter(|schema| schema.version_append)
    {
        let allowed = |column: &String| {
            schema.fields.iter().any(|field| &field.name == column && field.mutable)
                || schema.dedup_tiebreak.as_ref() == Some(column)
                || schema.tombstone_column.as_ref() == Some(column)
        };
        if let Some((blocked, _)) = assigned.iter().find(|(column, _)| !allowed(column)) {
            return Err(DataFusionError::Plan(format!(
                "UPDATE cannot assign `{blocked}` on `{table_name}`: columns are immutable unless declared `mutable: true`, and read filters on \
                 immutable columns are pushed below the merge-on-read dedup on that basis"
            )));
        }
    }

    Ok(DmlInfo { table_name: table_name.to_string(), project_id, predicate, assignments, source_plan })
}
```

Twenty lines that close the loop opened in Chapter 6 and used in Chapters 13 and
15. The read path pushes filters on immutable columns *below* the merge-on-read
dedup, which is sound only while every version of a row agrees on their value. An
`UPDATE` assigning an undeclared column would break that premise **silently, at
read time**, surfacing a stale version that matches a predicate the winning
version does not. Refusing at plan time turns a schema annotation into an
enforced invariant.

The error message is worth noting too: it does not just say "not allowed", it says
*why* — the reader learns the read-path invariant from the message.

Two exemptions, both correct by construction: the tiebreak is rewritten on every
append by `stamp_version`, and a delete appends a tombstone row, so both columns
vary across versions by definition and are already excluded from the pushdown.

### Version append: the merge-on-read write path

**Listing 16.10 — src/dml.rs:930–958**

```rust
/// `perform_delta_merge_update`'s so [`requalify_for_merge`] serves both paths;
/// the target keeps the TABLE NAME because the statement's predicate and
/// assignments were planned against it and may still carry that qualifier.
const MOR_SOURCE: &str = "source";

/// Merge-on-read DML (`docs/plans/2026-08-01-merge-on-read-dml.md`). On a
/// `version_append` table an UPDATE/DELETE rewrites NOTHING: it resolves its
/// target rows through the normal routed read path — mem ∪ hot ∪ delta, already
/// version-collapsed by `DedupExec` — evaluates the `SET` expressions against
/// them, and appends the results as new row versions.
/// [`BufferedWriteLayer::insert`] stamps a fresh monotonic `dedup_tiebreak`
/// (`insert_coerce::stamp_version`) on the way through, so the appended version
/// outranks every older copy at read time. No Delta MERGE, no deletion vector,
/// no OCC retry, and — because nothing existing changes — no hot-tier
/// invalidation.
///
/// DELETE appends the same FULL row with the schema's `tombstone_column` set,
/// not a key-only stub: the row has just been read anyway, a stub could not
/// satisfy the table's NOT NULL columns, and the read side keys off the marker
/// alone.
///
/// STREAMED, not collected. Rows are appended in scan-sized chunks, so an
/// UPDATE over a wide window costs one batch of memory rather than the whole
/// match set — the 2026-07-04 `update_with_source` OOM shape. The trade is
/// statement atomicity: a failure partway leaves the versions already appended
/// in place. That is sound under merge-on-read (each is a COMPLETE row version,
/// never a half-written row) and the client sees the error and retries, which
/// re-appends them idempotently.
```

This is the design in one comment. Four properties are worth pulling out:

**The read path *is* the target resolution.** The routed provider unions
MemBuffer, hot tier and Delta, prunes, and runs `DedupExec` — so the rows the
`UPDATE` sees are already the current versions. No separate "find the rows"
implementation exists, and therefore no way for the two to disagree.

**A `DELETE` appends the whole row**, not a key-only stub, for three stated
reasons: the row has just been read anyway, a stub could not satisfy the table's
`NOT NULL` columns, and the read side keys off the marker alone.

**It streams.** A failure partway leaves already-appended versions in place — no
statement atomicity — and the comment argues why that is acceptable *under this
specific model*: each append is a complete row version, never a half-written row,
and a client retry re-appends idempotently. This is a trade that is only sound
because of merge-on-read; the same trade under in-place mutation would corrupt
data.

**No OCC retry, no deletion vector, no hot-tier invalidation**, because nothing
existing changes.

The implementation opens by validating the schema's own contract:

**Listing 16.11 — src/dml.rs:959–985**

```rust
#[allow(clippy::too_many_arguments)]
async fn perform_version_append(
    database: &Arc<Database>, layer: Option<&Arc<BufferedWriteLayer>>, table_name: &str, project_id: &str, predicate: Option<Expr>,
    assignments: &[(String, Expr)], source: Option<&UpdateSource>, tombstone: bool, session: &Arc<dyn Session>,
) -> Result<u64> {
    use datafusion::{
        datasource::{MemTable, provider_as_source},
        logical_expr::{LogicalPlanBuilder, col, lit},
    };

    let schema =
        crate::schema::get_schema(table_name).ok_or_else(|| DataFusionError::Execution(format!("merge-on-read: no registered schema for {table_name}")))?;
    let tombstone_col = schema.tombstone_column.clone();
    if tombstone && tombstone_col.is_none() {
        // `version_append` is documented to require all three columns; a DELETE
        // with nowhere to write the marker would silently delete nothing.
        return Err(DataFusionError::Execution(format!("merge-on-read: {table_name} sets version_append but declares no tombstone_column")));
    }
    let table_schema = schema.schema_ref();

    // The routing provider IS the logical table: it unions MemBuffer, the hot
    // tier and Delta, prunes by the predicate's project/time bounds, and runs
    // DedupExec — so the rows we read are already the current versions.
    let provider =
        Arc::new(crate::database::ProjectRoutingTable::new(project_id.to_string(), database.clone(), table_schema.clone(), None, table_name.to_string()));
```

"A DELETE with nowhere to write the marker would silently delete nothing" — the
schema validator already enforces this at load, and it is checked *again* here.
Redundant checks on a silent-failure path are cheap insurance.

### Tenant isolation is not routing

The single most important security-adjacent line in the DML path:

**Listing 16.12 — src/dml.rs:986–995**

```rust
    // `project_id` is STRIPPED from the DML predicate (it is routing
    // information, consumed by `extract_dml_info`), so it must be put back as a
    // row filter here. Routing alone is not tenant isolation: every default
    // project shares ONE unified Delta table, so without this conjunct an
    // UPDATE scoped to one tenant rewrites the matching rows of every tenant in
    // that table. It also prunes, which is why the in-place Delta leg
    // re-augments its own predicate the same way.
    let tenant = Expr::Column(Column::from_name("project_id")).eq(lit(project_id));
```

`extract_dml_info` *consumes* `project_id = '…'` as routing information — it uses
it to pick the table — and therefore strips it from the predicate. But default
projects all share one unified Delta table, so routing does not isolate them.
Without re-adding the conjunct as a row filter, an `UPDATE` scoped to one tenant
would rewrite the matching rows of *every* tenant in that table.

Worth stating as a general principle: **when a value serves two purposes — routing
and filtering — consuming it for one does not discharge the other.**

### Splitting the predicate at the join

**Listing 16.13 — src/dml.rs:996–1009**

```rust
    let source_cols: std::collections::HashSet<String> = source.map(|s| s.schema.fields().iter().map(|f| f.name().clone()).collect()).unwrap_or_default();
    // The predicate splits at the join: a conjunct referencing any source
    // column (the enrichment guard `NOT (hashes @> ARRAY[u.tag])`) can only be
    // evaluated on the joined row, while target-only conjuncts (tenant, time
    // bounds) belong on the scan where they prune. Dropping the source-side
    // conjuncts instead of deferring them un-guards the UPDATE — every pass
    // then re-appends versions for every matched row (prod 2026-08-03).
    let (pre_join, post_join): (Vec<Expr>, Vec<Expr>) = predicate
        .as_ref()
        .map(|p| split_conjunction(p).into_iter().cloned().partition(|c| !c.column_refs().iter().any(|col| source_cols.contains(&col.name))))
        .unwrap_or_default();
    let filter = pre_join.into_iter().fold(tenant, Expr::and);
    let mut builder = LogicalPlanBuilder::scan(table_name, provider_as_source(provider), None)?.filter(filter)?;
```

A `partition` on whether a conjunct references any source column, and the
consequence of getting it wrong is precise: the enrichment guard
`NOT (hashes @> ARRAY[u.tag])` is what stops a pass from re-tagging rows it
already tagged. Dropping it instead of deferring it means *every pass re-appends
versions for every matched row* — an unbounded write amplification that ran in
production on 2026-08-03.

And the key pushdown that makes the scan affordable:

**Listing 16.14 — src/dml.rs:1010–1017**

```rust
    if let Some(src) = source {
        // The join keys never prune the scan on their own — the equi-join
        // matches AFTER every row in the window is decoded. Pushing the
        // source's key values down as IN-lists engages the parquet bloom
        // filters on exactly these columns and shrinks the scan from "whole
        // window, all columns" to the matched pages. Sound: join-key target
        // ... elided: the IN-list construction, chunked at MOR_KEY_PUSHDOWN_ROWS
```

"The equi-join matches AFTER every row in the window is decoded" is the
observation that motivates the whole optimization. A join key is not a filter
until you *make* it one.

### The DML coalescer

Monoscope's enrichment pipeline issues many small `UPDATE … FROM` statements. Each
one's Delta leg is a `MERGE`, and a `MERGE` per statement is far more expensive
than one `MERGE` per batch of statements. The coalescer defers and batches them.

**Listing 16.15 — src/dml.rs:2655–2673**

```rust
/// Accumulates deferred `UPDATE ... FROM` Delta legs and drains them as
/// batched merges. One instance per `Database`, created when
/// `TIMEFUSION_DML_COALESCE_SECS > 0`.
/// Manual format: `PendingGroup` holds an `Arc<dyn Session>`, which has no `Debug`.
#[derive(derive_more::Debug)]
#[debug("DmlCoalescer {{ interval_secs: {interval_secs}, queued_rows: {}, .. }}", queued_rows.load(Ordering::Relaxed))]
pub struct DmlCoalescer {
    interval_secs: u64,
    /// See `fold_groups` — cross-project folding of same-shape groups.
    fold: bool,
    groups: std::sync::Mutex<HashMap<GroupKey, PendingGroup>>,
    queued_rows: AtomicUsize,
    drain_notify: Notify,
    /// Serializes drains (timer vs shutdown vs test-triggered).
    drain_lock: tokio::sync::Mutex<()>,
    /// Where terminally-failed groups are parked instead of dropped.
    quarantine_dir: std::path::PathBuf,
}
```

The contract it imposes on callers is unusual and must be understood before using
it:

**Listing 16.16 — src/dml.rs:1735–1756**

```rust
// rows with different payloads (e.g. two tags for one span) cannot share one
// MERGE (Delta forbids duplicate source matches), so the drained batch splits
// into ordered rounds — round N holds each key's Nth occurrence.
//
// Contract (see `d_dml_coalesce_secs`): deferred statements must be
// idempotent under re-application. A row flushed between the mem leg and the
// drain sees the assignment applied twice, and a failed drain retries whole
// groups (including rounds that already committed).
//
// Durability: the mem leg WAL-appends `UpdateWithSource` before enqueue, so
// buffer-resident rows survive a crash with their post-DML values. What a
// crash CAN lose is the deferred Delta leg for rows that were already in
// Delta when the statement ran — bounded by the drain interval.
//
// A group that exhausts `MAX_DRAIN_ATTEMPTS` is **parked**, not dropped: its
// rows go to `<wal_dir>/quarantine/dml` as Arrow IPC + a `.meta` sidecar
// (`timefusion.dml.coalesce_quarantined`). Dropping was unrecoverable — the
// Delta leg targets rows already flushed out of the buffer, so there is no
// newer copy to converge from and read-side dedup (first-seen-wins) cannot
// repair it. `timefusion.dml.coalesce_dropped` now means the *quarantine
// write itself* failed, i.e. genuine loss.
```

Four things, each load-bearing.

**Rounds.** Delta forbids duplicate source matches in a `MERGE`, so two source
rows targeting one key cannot share a merge. The drained batch splits into ordered
rounds, round N holding each key's Nth occurrence.

**Idempotence is a caller obligation.** A row flushed between the mem leg and the
drain sees the assignment applied twice, and a failed drain retries whole groups
including rounds that already committed. Enrichment appends a tag guarded by
`NOT (hashes @> …)`, which is idempotent; an `UPDATE SET n = n + 1` would not be
and must not be coalesced.

**The durability boundary is explicit.** The mem leg WAL-appends
`UpdateWithSource` before enqueue, so buffer-resident rows survive a crash with
their post-DML values. What a crash can lose is the deferred Delta leg for rows
already flushed to Delta — bounded by the drain interval.

**Terminal failure parks, it does not drop.** This is the important one. The Delta
leg targets rows already flushed out of the buffer, so there is no newer copy to
converge from, and read-side dedup (first-seen-wins) cannot repair it. Dropping
was unrecoverable, so a group that exhausts its attempts is written to
`<wal_dir>/quarantine/dml` as Arrow IPC plus a `.meta` sidecar. Note the metric
redefinition: `coalesce_dropped` now means *the quarantine write itself failed* —
genuine loss — while `coalesce_quarantined` means recoverable.

The bounds:

**Listing 16.17 — src/dml.rs:1779–1800**

```rust
/// Queue-size pressure threshold: total buffered source rows above which a
/// drain is triggered immediately instead of waiting for the timer. Matches
/// `MAX_UPDATE_SOURCE_ROWS` — a drained group must stay mergeable.
const MAX_QUEUED_SOURCE_ROWS: usize = 1_000_000;

/// Drain attempts per group before it is quarantined (each drain already
/// carries perform_delta_merge_update's 4-attempt OCC retry underneath).
const MAX_DRAIN_ATTEMPTS: u32 = 3;

/// Max source rows fed to a single Delta MERGE. `MAX_QUEUED_SOURCE_ROWS` only
/// *notifies* a drain, so a group can grow past it unbounded — on 2026-07-27
/// one reached 1_252_311 rows (7457 statements) and every MERGE attempt died
/// with "Resources exhausted", costing the whole group. Rounds are therefore
/// chunked: many bounded merges instead of one unbounded one. Each chunk is an
/// independent commit, which the idempotence contract already permits.
const MAX_MERGE_ROWS: usize = 100_000;

/// Zero-copy slices of `batch` of at most `max` rows, covering every row once.
fn chunk_rows(batch: &RecordBatch, max: usize) -> impl Iterator<Item = RecordBatch> + '_ {
    (0..batch.num_rows()).step_by(max).map(move |off| batch.slice(off, max.min(batch.num_rows() - off)))
}
```

The distinction between `MAX_QUEUED_SOURCE_ROWS` (a *notify* threshold) and
`MAX_MERGE_ROWS` (a *hard* chunk size) is the fix for a specific failure: the
notify threshold does not bound growth, so a group reached 1,252,311 rows across
7,457 statements and every merge attempt died with "Resources exhausted", costing
the whole group. Chunking converts one unbounded merge into many bounded ones,
which the idempotence contract already permits.

`chunk_rows` is a three-line zero-copy slicer — `RecordBatch::slice` shares
buffers — and the total effective retry budget is worth noting:
3 drain attempts × 4 OCC retries inside each merge = 12 attempts before a group is
parked.

### Quarantine, and the loss it still admits

**Listing 16.18 — src/dml.rs:1812–1880 (excerpts)**

```rust
fn quarantine_group(dir: &std::path::Path, key: &GroupKey, group: &PendingGroup, batches: &[RecordBatch], reason: &str) -> bool {
    // ... elided: create_dir_all with an error! on failure
    // Schema drift is itself a quarantine reason (concat failure), so keep
    // ... elided: pick the batches matching the group schema
            "dml quarantine: no batch matches the group schema for {}/{} — {} rows LOST: {reason}",
    // ... elided: unique-path selection, IPC write, meta write
    error!("dml quarantine: parked {}/{} ({rows} rows) at {path:?}: {reason}", key.project_id, key.table_name);
    crate::observability::record_dml_coalesce_quarantined();
    // ... elided
        // other copy. Page on it, or the recoverable-looking quarantine metric
        // hides real loss.
        error!("dml quarantine: {skipped} schema-mismatched batch(es) for {}/{} could NOT be parked — {lost} rows LOST", key.project_id, key.table_name);
```

⚠️ The quarantine path itself can lose rows: batches whose schema does not match
the group's cannot be parked, and the comment says to **page on it**, because the
recoverable-looking `quarantined` metric would otherwise hide real loss. The `LOST`
token in both messages is deliberate and greppable.

Recovery is the `redrive-dml` CLI:

**Listing 16.19 — src/dml.rs:2239–2246, 2389–2409**

```rust
/// Re-drive parked `quarantine/dml/*` groups (hash-enrichment shape only; see
/// [`parse_quarantine_meta`]). Rebuilds the merge from the sidecar, replays it
// ... elided
pub async fn redrive_dml_quarantine(db: &Arc<crate::database::Database>, dir: &std::path::Path, dry_run: bool) -> (usize, usize) {
    // ... elided
        info!("dml redrive: no quarantine dir at {dir:?}");
```

```rust
/// Parsed `quarantine/dml/*.meta` sidecar for the known hash-enrichment shape.
// ... elided: the struct
pub(crate) fn parse_quarantine_meta(meta: &str) -> Option<QuarantineMeta> {
```

⚠️ Note the scope: **hash-enrichment shape only**. A quarantined group of any
other shape is parked but not automatically redrivable — the sidecar's parser
recognizes one statement form. That is honest (the parser refuses rather than
mis-reconstructing) but it means an operator must hand-reconstruct anything else.

### A session without the interceptor

One small piece of plumbing that prevents infinite regress:

**Listing 16.20 — src/dml.rs:58–60**

```rust
/// Build a clean SessionState with config + runtime from the given session but with
/// delta-rs's DeltaPlanner instead of our custom DmlQueryPlanner.
```

The Delta `MERGE` path plans its own internal queries; if those went through
`DmlQueryPlanner` again the interception would recurse. This builds a session that
keeps the config and runtime (so pool budgets and settings are inherited) but
swaps the planner.

### The full DML decision tree

```
UPDATE / DELETE arrives
  │
  ├─ DmlQueryPlanner::create_physical_plan intercepts
  ├─ extract_dml_info walks the plan:
  │    ├─ Projection(s)  → assignments (CSE aliases inlined)
  │    ├─ Filter         → predicate
  │    ├─ Join           → target side + equi-join keys + source plan
  │    └─ TableScan      → table name; project_id consumed from the predicate
  │
  ├─ MUTABILITY GATE: refuse any assignment to an undeclared column
  ├─ materialize the source plan (≤ 1,000,000 rows)
  │
  └─ DmlExec::execute
       │
       ├─ table has version_append?
       │    YES → perform_version_append
       │           ├─ scan via ProjectRoutingTable (mem ∪ hot ∪ delta, deduped)
       │           ├─ re-add the tenant conjunct  ← isolation
       │           ├─ split the predicate at the join (pre/post)
       │           ├─ push source keys as IN-lists (256 at a time)
       │           ├─ evaluate SET / set the tombstone
       │           └─ append via BufferedWriteLayer (stamps a fresh tiebreak)
       │
       │    NO  → in-place path
       │           ├─ mem leg: mutate buffered batches, WAL-append the statement
       │           └─ Delta leg: MERGE (or defer to DmlCoalescer)
       │
       └─ coalescer drain (timer / pressure / shutdown)
            ├─ group by (project, table, statement shape)
            ├─ split into rounds (Delta forbids duplicate source matches)
            ├─ chunk each round at 100,000 rows
            ├─ MERGE each chunk (4 OCC retries)
            └─ after 3 failed drains → quarantine to <wal_dir>/quarantine/dml
```

### How to debug this flow

**`timefusion_stats`**: `dml.coalesce_queued_rows`,
`timefusion.dml.coalesce_quarantined`, `timefusion.dml.coalesce_dropped`. The last
one non-zero means genuine loss and should page.

**Log lines**, by severity:

- `"dml quarantine: parked <project>/<table> (N rows) at <path>: <reason>"` —
  recoverable; run `timefusion redrive-dml`.
- `"dml quarantine: … could NOT be parked — N rows LOST"` — genuine loss.
- `"dml quarantine: no batch matches the group schema … N rows LOST"` — schema
  drift inside a group.
- `"dml redrive: no quarantine dir at …"` — nothing to redrive.

**Error messages that name their cause**:

- `"UPDATE cannot assign '<col>' on '<table>': columns are immutable unless
  declared mutable: true …"` — add `mutable: true` to the YAML *and* understand
  that you are removing the column from the pushdown-eligible set.
- `"merge-on-read: <table> sets version_append but declares no
  tombstone_column"` — a schema that passed load validation but not this check.
- `"merge-on-read: no registered schema for <table>"` — DML against an
  unregistered table.

**`SLOW_DML_PHASE_US`** (1 second) gates per-phase DML timing logs, which is the
first thing to look at when an `UPDATE` is slow: the phases separate target
resolution from source materialization from commit.

**Key takeaways.** DML is intercepted before DataFusion can plan it, decomposed by
an iterative walk that handles CSE projections and join splits, and gated by a
plan-time mutability check that turns the schema's `mutable` annotation into an
enforced read-path invariant. On a `version_append` table an `UPDATE` resolves its
targets through the *ordinary read path* and appends new row versions — streaming,
non-atomic, and idempotent-by-contract. `project_id` must be re-added as a row
filter after routing consumes it, or one tenant's `UPDATE` rewrites every tenant's
rows. The coalescer batches deferred Delta merges under an explicit idempotence
contract, chunks them so one oversized group cannot fail wholesale, and parks
terminal failures on disk rather than dropping them — while still admitting one
narrow loss path that is deliberately logged as `LOST`.
## 17. The maintenance coordinator

*After this chapter you will understand TimeFusion's durable maintenance
scheduler: how work is identified and minted, how the journal survives restarts,
how units are split and fused to fit a memory budget, how `claim_next` picks the
next unit under six competing pressures, and how a doomed unit is quarantined
rather than allowed to burn the fleet.*

This is the subsystem with the highest density of "we measured this in production
and it was not what we assumed" comments in the codebase, and it is worth reading
for that alone.

### The problem

A streaming ingest path writing every few minutes produces thousands of small
Parquet files. Merge-on-read accumulates row versions. Rollups need building for
every `(project, date)` and rebuilding when their source changes. Footers get
written without `sorting_columns` and poison every future scan of their partition.

All of that is *background* work competing for one process's memory, CPU and S3
capacity with the ingest and query paths that actually serve users. The
coordinator's job is to decide, repeatedly, which single unit of work to run next.

### The unit of work

**Listing 17.1 — src/maintenance_coordinator.rs:120–143**

```rust
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Operation {
    Dedup,
    BaseRollup,
    DerivedRollup,
    HotPacking,
    SealedConsolidation,
    Repair,
}

impl Operation {
    const fn priority(self) -> u8 {
        match self {
            Self::Dedup => 0,
            Self::BaseRollup => 1,
            Self::DerivedRollup => 2,
            Self::HotPacking => 3,
            Self::SealedConsolidation => 4,
            Self::Repair => 5,
        }
    }
}
```

Six operations, covered in detail in Chapters 18 (`BaseRollup`, `DerivedRollup`)
and 19 (`Dedup`, `HotPacking`, `SealedConsolidation`, `Repair`).

A task is identified by five things:

**Listing 17.2 — src/maintenance_coordinator.rs:189–247**

```rust
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Deserialize, Serialize)]
pub struct TimeSlice {
    pub start_micros: i64,
    pub end_micros: i64,
}

impl TimeSlice {
    pub fn new(start_micros: i64, end_micros: i64) -> anyhow::Result<Self> {
        anyhow::ensure!(start_micros < end_micros, "maintenance slice must be non-empty");
        Ok(Self { start_micros, end_micros })
    }
    // ... elided: width(), containment and alignment helpers
}
```

```rust
pub struct TaskKey {
    pub physical_table: String,
    pub source: String,
    pub project_id: String,
    pub slice: TimeSlice,
    pub operation: Operation,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskState {
    Pending,
    Running,
    Retry,
    Complete,
    Superseded,
}
```

Five states, and `Superseded` is the one that has caused the most trouble — a
terminal state meaning "a wider unit now covers this", which is correct but which
also means the *key* can never be re-minted.

And the task itself:

**Listing 17.3 — src/maintenance_coordinator.rs:248–292**

```rust
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct MaintenanceTask {
    pub key: TaskKey,
    pub state: TaskState,
    pub deadline_micros: i64,
    pub estimated_decoded_bytes: u64,
    pub hash_shard: u32,
    pub hash_shards: u32,
    pub attempts: u32,
    pub created_unix_ms: u64,
    #[serde(default)]
    pub retry_reason: Option<String>,
    #[serde(default)]
    pub publication: Option<Publication>,
    /// The base tier this derived unit aggregates is ALREADY PRESENT, proven
    /// from real rollup coverage by `plan_rollup_backfill` rather than from
    /// journal bookkeeping.
    ///
    /// `dependencies_complete` otherwise requires COMPLETE `BaseRollup` TASKS
    /// contiguously covering the slice. For a frontier hour that is right. For a
    /// historical day whose 1m tier was built weeks ago — possibly by an older
    /// code path, possibly with its journal records long since collapsed — no
    /// such task exists, so the unit is unclaimable forever and `claim_next`
    /// skips it with no counter and no log.
    ///
    /// Prod 2026-08-18 22:30 UTC is that shape exactly: the 1m base tier is 33
    /// days deep on most projects while the 1h derived tier it feeds sits at
    /// 9-17, `pending_derived_rollup` did not move by ONE task across two 240s
    /// windows with workers free, and all 35 derived units claimed in 20 minutes
    /// were frontier slices whose base had completed minutes earlier.
    ///
    /// Only ever set from positive evidence — the planner computes `missing`
    /// tiers from actual coverage, so a derived tier missing while no base tier
    /// is missing means the base data is there. That is strictly better evidence
    /// than the journal's, which is why this overrides rather than supplements.
    #[serde(default)]
    pub base_tier_present: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct Publication {
    pub source_fingerprint: u64,
    pub generation: String,
    pub rows: u64,
}
```

That `base_tier_present` comment is the archetype of this file's failure mode:
**a unit that is unclaimable forever, skipped with no counter and no log**. The
gauges said work was pending, workers were free, and nothing moved — because a
dependency check was asking the journal a question only the *storage* could
answer.

### The journal

**Listing 17.4 — src/maintenance_coordinator.rs:365–380**

```rust
/// Crash-safe task journal. `checkpoint` uses the same fsync + atomic rename
/// primitive as WAL metadata; a failed completion checkpoint therefore causes
/// redundant work, never missing work.
#[derive(Debug)]
pub struct TaskJournal {
    path: PathBuf,
    wal_path: PathBuf,
    snapshot: Snapshot,
    /// Stable indices into `snapshot.tasks`. Tasks are never removed, so point
    /// updates and WAL replay stay O(1) even with a production-sized backlog.
    task_indices: HashMap<TaskKey, usize>,
    dirty_tasks: HashSet<TaskKey>,
    /// Keys removed since the last write, pending a `Removed` tombstone.
    removed_tasks: HashSet<TaskKey>,
    dirty_cursors: HashSet<String>,
    fair_cursors: HashMap<Operation, String>,
```

"A failed completion checkpoint therefore causes redundant work, never missing
work" is the correct bias for a maintenance journal, and it is the same bias as
the WAL's.

Note the tension in two adjacent comments: `task_indices`'s doc says "Tasks are
never removed", while `removed_tasks` exists for exactly that. The resolution is
that removal is a *later* addition — and the record type that enables it carries
one of the most expensive lessons in the file:

**Listing 17.5 — src/maintenance_coordinator.rs:339–362**

```rust
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum JournalRecord {
    Task(MaintenanceTask),
    SourceCursor {
        source: String,
        delta_version: u64,
    },
    /// This task no longer exists.
    ///
    /// Without it the WAL is upsert-only, and a pass that removes tasks can
    /// persist its work ONLY by rewriting the whole snapshot. Prod 2026-08-19
    /// shows what that costs when a caller forgets: coarsening took
    /// `pending_base_rollup` 88,618 -> 2,294, every gauge agreed, and the
    /// on-disk journal was still byte-identical at 84,734,124 bytes with all
    /// 173,901 tasks. The collapse existed only in memory and the next restart
    /// undid it.
    ///
    /// `compact` remains correct and is still what a large migration should
    /// use; this exists so that the CHEAP path can express a deletion at all,
    /// and so forgetting is no longer possible — `retain_tasks` records the
    /// tombstones for every caller.
    Removed(TaskKey),
}
```

An append-only WAL with no deletion record cannot express "this is gone" — so a
pass that collapsed 88,618 pending tasks into 2,294 changed only memory. Every
gauge agreed. The on-disk journal was byte-identical. The next restart undid all
of it. The fix is a tombstone record *plus* an API (`retain_tasks`) that records
the tombstone for every caller, so forgetting is structurally impossible.

Checkpointing is the cheap path; compaction is the expensive one:

**Listing 17.6 — src/maintenance_coordinator.rs:1830–1871**

```rust
    pub fn checkpoint(&mut self) -> anyhow::Result<()> {
        if let Some(parent) = self.wal_path.parent() {
            fs::create_dir_all(parent)?;
        }
        if !self.dirty_tasks.is_empty() || !self.dirty_cursors.is_empty() || !self.removed_tasks.is_empty() {
            let mut wal = OpenOptions::new().create(true).append(true).open(&self.wal_path)?;
            let mut records = Vec::new();
            for key in self.dirty_tasks.drain() {
                if is_derived_operation(key.operation) {
                    continue;
                }
                if let Some(index) = self.task_indices.get(&key).copied() {
                    let task = &self.snapshot.tasks[index];
                    serde_json::to_writer(&mut records, &JournalRecord::Task(task.clone()))?;
                    records.push(b'\n');
                }
            }
            for source in self.dirty_cursors.drain() {
                if let Some(delta_version) = self.snapshot.source_cursors.get(&source).copied() {
                    serde_json::to_writer(&mut records, &JournalRecord::SourceCursor { source, delta_version })?;
                    records.push(b'\n');
                }
            }
            // AFTER the upserts, so a key removed and re-created in the same
            // window keeps the re-creation. `retain_tasks` already drops such a
            // key from `removed_tasks` when it reappears, but ordering makes the
            // record stream correct on its own terms rather than by convention.
            for key in self.removed_tasks.drain() {
                if is_derived_operation(key.operation) {
                    continue;
                }
                serde_json::to_writer(&mut records, &JournalRecord::Removed(key))?;
                records.push(b'\n');
            }
            wal.write_all(&records)?;
            wal.sync_all()?;
        }
        if fs::metadata(&self.wal_path).is_ok_and(|metadata| metadata.len() >= JOURNAL_COMPACT_BYTES) {
            self.compact()?;
        }
        self.publish_statistics();
        Ok(())
    }
```

Tombstones written *after* upserts, so a key removed and re-created in the same
window keeps the re-creation — and the comment notes that `retain_tasks` already
handles this, but that ordering makes the record stream correct "on its own terms
rather than by convention". Defence in depth on an ordering invariant.

**Listing 17.7 — src/maintenance_coordinator.rs:1873–1900**

```rust
    /// Rewrite the authoritative snapshot even when the WAL is below its
    /// normal size threshold. Migrations that remove tasks cannot represent
    /// those deletions as append-only WAL records, so they must force this
    /// compaction before startup continues.
    pub fn compact(&mut self) -> anyhow::Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }
        // Derived work is left out of the authoritative snapshot too, so a
        // reload starts with none of it and `plan_compaction_debt` re-derives
        // exactly what storage says is needed within its next 60 s pass.
        let durable = Snapshot {
            version: self.snapshot.version,
            tasks: self.snapshot.tasks.iter().filter(|task| !is_derived_operation(task.key.operation)).cloned().collect(),
            source_cursors: self.snapshot.source_cursors.clone(),
        };
        let bytes = serde_json::to_vec(&durable)?;
        crate::write::wal::write_atomic_with(&self.path, true, |file| file.write_all(&bytes))?;
        let wal = OpenOptions::new().create(true).write(true).truncate(true).open(&self.wal_path)?;
        wal.sync_all()?;
        self.dirty_tasks.clear();
        self.removed_tasks.clear();
        self.dirty_cursors.clear();
        Ok(())
    }
```

### Do not persist what you can re-derive

Both `checkpoint` and `compact` filter on `is_derived_operation`, and that
predicate carries the second-most-expensive lesson here:

**Listing 17.8 — src/maintenance_coordinator.rs:49–70**

```rust
/// Is this operation fully re-derivable from a storage scan, and therefore not
/// worth persisting?
///
/// `plan_compaction_debt` scans the real file list of every (project, date)
/// every 60 s and mints HotPacking for today and SealedConsolidation once the
/// day seals, from the files themselves — `small.len() >= 2 || any !sorted`. The
/// scan is authoritative; a durable record of it is a second, weaker copy that
/// can only disagree.
///
/// And it did. Prod 2026-08-19 carried `pending_sealed_consolidation = 2,218`
/// while an audit of object storage found 877 of 1,033 partitions already
/// compliant and only 108 sealed ones out of policy — a queue 20x inflated with
/// work already done, draining at -0.27/min forever. Persisting derived state
/// buys nothing and costs staleness.
///
/// Repair is deliberately NOT here. Its units are day-wide rewrites that run for
/// 12-15 minutes and stage output before committing, so a durable record is what
/// `TIMEFUSION_REPAIR_RESUME_ENABLED` resumes against rather than redoing the
/// work.
pub const fn is_derived_operation(operation: Operation) -> bool {
```

The general principle: **if a scan of the authoritative source can re-derive the
work item, do not persist it** — the persisted copy is a second, weaker source of
truth that can only disagree. Production carried 2,218 pending consolidation units
while an audit of object storage found only 108 partitions actually out of policy:
a queue 20× inflated with work already done, draining at −0.27 units per minute
forever.

`Repair` is the deliberate exception, and the reason is stated: its units run for
12–15 minutes and *stage output before committing*, so the durable record is what
resume works against.

### Fitting the memory budget: splitting and fusing

Every unit must fit `MAX_DECODED_BYTES`:

**Listing 17.9 — src/maintenance_coordinator.rs:18–28, 72–75**

```rust
pub const NORMAL_SLICE_MICROS: i64 = 10 * 60 * 1_000_000;
pub const DAY_MICROS: i64 = 24 * 60 * 60 * 1_000_000;
/// Widths `coarsen_sealed_slices` fuses sealed units to, widest first. It takes
/// the widest whose summed estimate fits `MAX_DECODED_BYTES`; below the finest,
/// the mint width stands. Each divides the one above, so an aligned unit at any
/// width sits inside exactly one bucket at every coarser width.
pub const COARSEN_WIDTHS: [i64; 3] = [DAY_MICROS, 6 * 60 * 60 * 1_000_000, 60 * 60 * 1_000_000];
/// Widths a unit can be SUBSUMED at, finest first — the mint width plus every
/// fusion width. Each divides the next, so an aligned unit at any of them sits
/// wholly inside one bucket of every coarser one.
```

```rust
pub const SUBSUME_WIDTHS: [i64; 4] = [NORMAL_SLICE_MICROS, 60 * 60 * 1_000_000, 6 * 60 * 60 * 1_000_000, DAY_MICROS];
pub const MIN_SLICE_MICROS: i64 = 60 * 1_000_000;
pub const DERIVED_SLICE_MICROS: i64 = 60 * 60 * 1_000_000;
pub const MAX_DECODED_BYTES: u64 = 512 * 1024 * 1024;
```

The width lattice — 10 min, 1 h, 6 h, 1 day — is chosen so each divides the next.
That is what makes the containment arithmetic exact: an aligned unit at any width
sits wholly inside exactly one bucket at every coarser width, so fusing and
subsuming are both total and unambiguous.

Splitting handles the other direction:

**Listing 17.10 — src/maintenance_coordinator.rs:294–302**

```rust
/// Split a unit until each estimated reservation fits.  A one-minute whale is
/// divided by a stable hash of the complete dedup key; callers must apply
/// `hash(key) % hash_shards == hash_shard` before deduplication.
pub fn byte_bounded_units(task: &MaintenanceTask, observed_or_estimated_bytes: u64) -> Vec<MaintenanceTask> {
    if observed_or_estimated_bytes <= MAX_DECODED_BYTES {
        let mut task = task.clone();
        task.estimated_decoded_bytes = observed_or_estimated_bytes;
        // ... elided: the recursive time-split, then the hash-shard fallback
```

Two-stage: split by *time* while the slice is wider than the minimum, then — for a
"one-minute whale", a single tenant-minute too large to process — split by a
stable **hash of the complete dedup key**. Hash sharding works where time
splitting has run out because the dedup key space is always divisible, and using
the *complete* key means each shard can dedup independently.

Fusion runs the other way and reports what it did:

**Listing 17.11 — src/maintenance_coordinator.rs:28–47, 703–760**

```rust
/// What one `coarsen_sealed_slices` pass actually did, per stage.
#[derive(Debug, Default, Clone, Copy)]
pub struct CoarsenReport {
    // ... elided: per-stage counters
}
```

```rust
    pub fn coarsen_sealed_slices(&mut self, now_micros: i64) -> usize {
    pub fn coarsen_sealed_slices_reporting(&mut self, now_micros: i64) -> CoarsenReport {
    fn subsume_covered_units(&mut self, now_micros: i64) -> usize {
```

*Coarsening* fuses adjacent sealed units into the widest width whose summed
estimate still fits the budget. *Subsuming* removes a fine unit already covered by
a wider one. Both are pure queue-shape operations that reduce the per-unit fixed
cost — which, per `CLAUDE.md`, is dominated by the Delta commit rather than the
scan, making *width* the lever.

The reporting variant exists because a fusion pass that silently does nothing is
indistinguishable from one that is working.

### `claim_next`: six pressures, one decision

The scheduler's core. Its doc comment is a production post-mortem:

**Listing 17.12 — src/maintenance_coordinator.rs:1396–1418**

```rust
    /// out [`Self::QUARANTINE_ATTEMPTS`] times; the caller gates it on a small
    /// occupancy permit so proven-unfittable work cannot hold the whole pool.
    ///
    /// Measured on prod 2026-08-18 21:00 UTC over 60 minutes of logs: 47 units
    /// timed out (BaseRollup 24, HotPacking 12, SealedConsolidation 11) against
    /// a 900s deadline each. That is 42,300 of 57,600 available worker-seconds —
    /// 73% of ALL maintenance capacity — spent committing nothing, while
    /// ~40,000 pending BaseRollup units could not get a slot. Over 180s
    /// `tasks_complete` rose by 2 with `tasks_running` pinned at 16, and the one
    /// rollup that completed took 812ms.
    ///
    /// Neither existing lever reaches this. `abandon_running`'s backoff decides
    /// how OFTEN a doomed unit runs, never what it costs when it does; and its
    /// bisection makes the total worse, because halving a slice cannot halve a
    /// per-file cost — measured at ~3.2s per parquet file the same day — it only
    /// doubles the number of units paying it. #176's cap bounds occupancy, which
    /// is the right lever, but exempts BaseRollup, and BaseRollup is half the
    /// timeouts: the exemption assumed rollup units advance coverage, which is
    /// true only of one that COMPLETES.
    pub fn claim_next(&mut self, operation: Operation, now_micros: i64, allow_quarantined: bool) -> Option<MaintenanceTask> {
```

73% of all maintenance capacity spent committing nothing. The analysis of *why the
existing levers did not reach it* is the valuable part:

- **Retry backoff** decides how *often* a doomed unit runs, never what it costs
  when it does.
- **Bisection** makes the total *worse*: halving a slice cannot halve a per-file
  cost (~3.2 s per Parquet file), it only doubles the number of units paying it.
- **The occupancy cap** was the right lever but exempted `BaseRollup`, which was
  half the timeouts — the exemption assumed rollup units advance coverage, which
  is true only of one that *completes*.

The fix is `QUARANTINE_ATTEMPTS`: a unit that has timed out twice is only
claimable under a small dedicated permit pool
(`maintenance_quarantine_slots`, Chapter 7, Listing 7.14), bounding the
*wall-clock cost* of doomed work rather than its frequency.

The implementation opens with a performance rewrite whose reasoning is worth
following:

**Listing 17.13 — src/maintenance_coordinator.rs:1419–1450**

```rust
        // The winning scheduling class, as a streaming minimum.
        //
        // This used to call `fair_ready_tasks`, which builds a BTreeMap of every
        // ready task plus a fully materialised Vec, and then took `.find()` —
        // discarding all of it to keep one `(class, order_key)` tuple. That is
        // O(n log n) and two large allocations PER CLAIM, on every worker.
        // Tolerable at 18k tasks; at 128k (2026-08-17, after the rollup backfill
        // queued real history) it started showing up as live-frontier lag, and
        // it scales the wrong way for the 10x-projects target.
        //
        // The minimum over the same predicate is identical: groups are ordered
        // by `(class, operation.priority(), order_key)` and the operation is
        // fixed here, so the first match `find` returned was exactly the
        // smallest `(class, order_key)` among eligible tasks for this operation.
        //
        // `dependencies_complete` is itself a scan, so it is evaluated ONLY when
        // a task would actually improve the current best — otherwise this would
        // be quadratic for `DerivedRollup`.
        //
        // One claim in three is also RESERVED for sealed work. Class is strict
        // priority and ingest generates live-frontier work continuously, so
        // without a reservation class 1 never runs at all. Prod 2026-08-17, over
        // 278 consecutive task starts: every one was today or yesterday, not a
        // single sealed day. Rollup coverage for a live tenant stayed pinned at
        // two days for hours while the frontier was perfectly healthy
        // (`eligible_watermark_lag_seconds` 0) — and a 7d/14d/30d query needs
        // exactly the sealed days that never ran. Falls back to any class when
        // there is no sealed work, so quiet history never idles a worker.
        self.claim_tick = self.claim_tick.wrapping_add(1);
        // Every OTHER claim, not every third. Measured after the reservation
        // shipped: sealed work went from 0 of 278 task starts to 8 of 131
        // (6.1%), far short of the intended third, because a sealed turn falls
        // ... elided: the remainder of the reservation arithmetic and the scan
```

Three separate lessons in one comment block.

**A sort-then-find that keeps one element is a streaming minimum.** The old code
built a `BTreeMap` of every ready task plus a materialized `Vec` and then took
`.find()` — `O(n log n)` and two large allocations *per claim, per worker*.
Tolerable at 18k tasks; at 128k it surfaced as live-frontier lag.

**Evaluate the expensive predicate last.** `dependencies_complete` is itself a
scan, so it runs only when a task would actually improve the current best —
otherwise the whole claim is quadratic for `DerivedRollup`.

**Strict priority starves the lower class completely.** Over 278 consecutive task
starts, every single one was today or yesterday; not one sealed day ran, while
rollup coverage for a live tenant stayed pinned at two days for hours with the
frontier perfectly healthy. A 7-day, 14-day or 30-day query needs exactly the
sealed days that never ran. The fix is a *reservation*: one claim in N is
restricted to sealed work, falling back to any class when there is none so quiet
history never idles a worker.

And then the follow-up measurement, which is the part most teams skip: after the
reservation shipped, sealed work went from 0 of 278 to 8 of 131 (6.1%) — "far
short of the intended third" — so the reservation was retuned from every third
claim to every other claim.

The reservation is itself conditional on the frontier being healthy:

**Listing 17.14 — src/maintenance_coordinator.rs:77–84**

```rust
/// Frontier lag the sealed reservation is still affordable at.
///
/// Above this, `claim_next` stops reserving a share for sealed work until the
/// live frontier catches up. Ten minutes is one `NORMAL_SLICE_MICROS`: a
/// frontier that is a whole slice behind is not keeping up, and every hybrid
/// query is paying for it through `raw_tail_duration_secs`
/// (`FINALIZATION_DELAY + lag`).
pub const FRONTIER_LAG_BUDGET_SECS: u64 = 600;
```

A reservation that would starve the frontier is suspended until the frontier
recovers — a two-way guard rather than a fixed split.

### Dependencies: ask storage, not the journal

**Listing 17.15 — src/maintenance_coordinator.rs:1594–1620**

```rust
    fn dependencies_complete(&self, task: &MaintenanceTask) -> bool {
        let required = match task.key.operation {
            // Base publication performs its own bounded complete-key/tiebreak
            // dedup before aggregation. Physical source consolidation is
            // independent debt and must not block exact rollup coverage.
            Operation::BaseRollup => None,
            Operation::DerivedRollup => Some(Operation::BaseRollup),
            _ => None,
        };
        // Proven from real tier coverage, which is strictly better evidence than
        // the journal's own record of who built what. See the field's comment.
        //
        // The day-keyed set is checked first and is the one that actually works:
        // the per-task flag has to be set on exactly the right `TaskKey`, and
        // prod measured 674 of 674 pending derived tasks without it. Keying the
        // fact on the DAY it is a fact about cannot miss a task.
        if task.base_tier_present {
            return true;
        }
        if let Some(date) = chrono::DateTime::from_timestamp_micros(task.key.slice.start_micros).map(|time| time.date_naive().to_string())
            && self.base_tier_ready.contains(&(task.key.source.clone(), task.key.project_id.clone(), date))
        {
            return true;
        }
        required.is_none_or(|required| {
            let mut intervals = self
                .snapshot
                // ... elided: the contiguous-coverage interval merge over COMPLETE BaseRollup tasks
```

Only one real dependency exists — `DerivedRollup` needs `BaseRollup` — and
`BaseRollup` depends on nothing, because base publication does its own bounded
dedup before aggregating and physical consolidation is independent debt.

The evidence hierarchy is the design: a per-task flag first, then a *day-keyed*
set, then the journal's own interval coverage. The day-keyed set is described as
"the one that actually works", and the field comment explains why three attempts
at the per-task flag failed:

**Listing 17.16 — src/maintenance_coordinator.rs:381–397**

```rust
    /// `(source, project_id, date)` whose BASE tier is already built, as read
    /// from real rollup coverage by `plan_rollup_backfill` every 60s.
    ///
    /// `dependencies_complete` consults this instead of requiring COMPLETE
    /// `BaseRollup` TASKS, which a historical day does not have. Three attempts
    /// to carry the same fact as a per-task flag failed (#184, #186, #195),
    /// because the flag had to land on exactly the right `TaskKey` and the
    /// queued work is not the width the planner assumes: prod 2026-08-19 06:30
    /// measured `derived_unproven=674` out of `derived_pending=674` — the flag
    /// had never been set on ONE pending task.
    ///
    /// A day is the right key because that is what the fact is about, so it
    /// cannot miss a task whatever slice that task covers. Runtime only, never
    /// journalled: the planner rebuilds it from coverage each pass, so a restart
    /// costs one pass and it self-heals if coverage changes underneath it.
    base_tier_ready: HashSet<(String, String, String)>,
```

**"A day is the right key because that is what the fact is about."** Three
implementations failed because they keyed a fact on a `TaskKey` — a
`(table, source, project, slice, operation)` tuple — when the fact was about a
*day*. Any mismatch between the planner's assumed slice width and the queued
work's actual width meant the flag never landed. 674 of 674 pending derived tasks
lacked it.

The corollary — runtime only, never journalled — follows from the same reasoning:
if the planner rebuilds it from coverage every 60 seconds, persisting it would
reintroduce the derived-state staleness problem from Listing 17.8.

### Contiguity versus freshness

The second runtime-only set exists because "newest first" is the wrong ordering
for a coverage goal:

**Listing 17.17 — src/maintenance_coordinator.rs:398–412**

```rust
    /// `(source, project_id, physical_table, date)` the tier is MISSING, from
    /// the same coverage read that fills `base_tier_ready`.
    ///
    /// `scheduling_class` ranks a hole ahead of a re-derive. Without that,
    /// sealed rollup work is strictly newest-first, and recent days are
    /// continuously re-invalidated by ongoing publication — so the claim never
    /// walks backwards far enough to reach an old hole. Prod 2026-08-19 09:00:
    /// `94c5dc1f` had 1h-tier dates jumping 2026-07-31 -> 08-14 for a second
    /// day running while day-wide derived units for 08-17 were claimed over and
    /// over. Newest-first is right for freshness and wrong for CONTIGUITY, and
    /// 30d coverage is a contiguity goal.
    ///
    /// Runtime only, rebuilt from coverage every 60s, same as `base_tier_ready`.
    tier_holes: HashSet<(String, String, String, String)>,
```

**"Newest-first is right for freshness and wrong for CONTIGUITY, and 30d coverage
is a contiguity goal."** Recent days are continuously re-invalidated by ongoing
publication, so a newest-first scheduler re-derives them forever and never walks
back to a hole from two weeks ago. Ranking a *hole* ahead of a *re-derive* is what
makes 30-day coverage reachable.

### The operation cycle

Workers rotate through a fixed mix, and there are two mixes:

**Listing 17.18 — src/maintenance_coordinator.rs:144–187**

```rust
/// The operation mix a maintenance worker rotates through. One definition for
/// the server loop (`run_coordinator_maintenance_once`) and the journal-replay
/// simulator (`maintenance_sim`) — the sim exists to evaluate changes to this
/// mix, so the two must never be able to drift apart.
///
/// BALANCED interleaves dependent publication with dedup: dedup/base receive
/// three slots each; derived and file work each receive one. `claim_next`
/// still applies deadline, recent-slice, dependency, and project fairness.
///
/// COVERAGE_SHORT gives the rollup chain the slots while
/// `rollup_min_contiguous_days` is below goal: `dependencies_complete` makes
/// BaseRollup depend on NOTHING, so of the balanced cycle six slots in ten go
/// to work that cannot advance the metric governing 14d/30d latency (measured
/// 2026-08-18). Every operation keeps at least one slot — file debt left at
/// zero is how file counts ran to 2-3k and degraded every query (2026-08-01).
pub const CYCLE_BALANCED: [Operation; 10] = [
    Operation::Dedup,
    Operation::BaseRollup,
    Operation::DerivedRollup,
    Operation::HotPacking,
    Operation::Dedup,
    Operation::BaseRollup,
    Operation::SealedConsolidation,
    Operation::Dedup,
    Operation::BaseRollup,
    Operation::Repair,
];
pub const CYCLE_COVERAGE_SHORT: [Operation; 10] = [
    Operation::BaseRollup,
    Operation::DerivedRollup,
    Operation::BaseRollup,
    Operation::Dedup,
    Operation::BaseRollup,
    Operation::DerivedRollup,
    Operation::HotPacking,
    Operation::BaseRollup,
    Operation::SealedConsolidation,
    Operation::Repair,
];

pub fn operation_cycle(coverage_short: bool) -> &'static [Operation; 10] {
    if coverage_short { &CYCLE_COVERAGE_SHORT } else { &CYCLE_BALANCED }
}
```

A ten-slot cycle expressed as a literal array is about as legible as a scheduling
policy gets — the mix is visible at a glance and diffs cleanly.

Two properties are load-bearing. **One definition, shared with the simulator**:
the sim exists to evaluate changes to this mix, so if the two could drift the sim
would evaluate a policy production does not run. And **every operation keeps at
least one slot**, because file debt left at zero is how file counts ran to 2–3k
and degraded every query.

The switch between the mixes is itself a measured decision: under the balanced
cycle, six of ten slots go to work that cannot advance `rollup_min_contiguous_days`
— the metric governing 14-day and 30-day query latency.

### Deadlines, and what a failure means

**Listing 17.19 — src/maintenance_coordinator.rs:86–105**

```rust
/// Whether a failure means "this did not fit" rather than "this went wrong".
///
/// Matched on the message, not the type: these errors originate in DataFusion,
/// cross the delta-rs and `anyhow` boundaries on the way back, and arrive
/// type-erased. The two strings are DataFusion's own — `ResourcesExhausted`'s
/// `Display` and the `ExternalSorter`'s message — and both are asserted against
/// verbatim prod text in `capacity_failures_are_recognised_from_prod_text`.
/// Operation deadlines also bound retry backoff so oversized units cannot
/// monopolize a worker.
pub const fn operation_deadline_secs(operation: Operation) -> u64 {
    // ... elided: per-operation deadlines
}

pub fn is_capacity_failure(message: &str) -> bool {
```

String matching on error messages is normally a smell, and this is the case where
it is not: the errors originate in DataFusion, cross `delta-rs` and `anyhow`
boundaries, and arrive type-erased. The mitigation is honest — the strings are
DataFusion's own, and a test asserts them against *verbatim production text*.

The distinction matters because "this did not fit" leads to splitting while "this
went wrong" leads to retry-then-quarantine.

### Publication tags

Rollup and consolidation outputs carry Delta file tags:

**Listing 17.20 — src/maintenance_coordinator.rs:106–117**

```rust
pub const FINALIZATION_DELAY_MICROS: i64 = 15 * 60 * 1_000_000;
pub const INVALIDATION_DEADLINE_BUCKET_MICROS: i64 = 30 * 1_000_000;
pub const LIVE_FRONTIER_WINDOW_MICROS: i64 = 24 * 60 * 60 * 1_000_000;
const PRIORITY_BUCKET_MICROS: i64 = 60 * 1_000_000;
pub const TAG_SOURCE: &str = "timefusion.source";
pub const TAG_PROJECT: &str = "timefusion.project";
pub const TAG_SLICE_START: &str = "timefusion.slice_start_micros";
pub const TAG_SLICE_END: &str = "timefusion.slice_end_micros";
pub const TAG_SOURCE_FINGERPRINT: &str = "timefusion.source_fingerprint";
pub const TAG_GENERATION: &str = "timefusion.generation";
const JOURNAL_VERSION: u32 = 1;
const JOURNAL_COMPACT_BYTES: u64 = 64 * 1024 * 1024;
```

These six tags are how a rollup tier knows which files a republication should
*replace*: the replace-set removes files whose slice is contained in the one being
published. ⚠️ This is also the mechanism a stock delta-rs `OPTIMIZE` breaks — it
rewrites files without preserving tags, and an untagged file can never be matched
by a replace-set, so it accumulates beside its replacements forever. Chapter 29
records it.

`FINALIZATION_DELAY_MICROS` (15 minutes) is the settle time before a slice is
considered eligible; combined with frontier lag it is what a hybrid query pays as
`raw_tail_duration_secs`.

### The admission controller

**Listing 17.21 — src/maintenance_coordinator.rs:2268–2270**

```rust
pub struct AdmissionController(Arc<Mutex<AdmissionState>>);

impl AdmissionController {
    // ... elided: token accounting for cpu / reads / writes, and publish_utilization
```

A newtype over `Arc<Mutex<AdmissionState>>` tracking CPU, read and write tokens.
`CLAUDE.md` records that it was once `(cpu 1, reads 1, writes 1)` with
`coordinator_jobs` hard-coded at 1 — serializing maintenance so hard that the
queue grew unbounded at ~99% idle decode budget. That, per the config comment in
Chapter 7, was the root cause of dead rollups: no dedup commits means no
certification, which keeps `DedupExec` in every plan and denies rollup routing its
certified prefix.

### The recurring pattern

Every incident in this chapter has the same shape, and the codebase names it:

> **Queued, plus gauges saying eligible, plus a predicate that refuses.**

- `base_tier_present` — a dependency check asking the journal a question only
  storage could answer. 674 of 674 tasks unclaimable.
- `tier_holes` — newest-first ordering never reaching an old hole.
- `is_derived_operation` — a persisted copy of derived state, 20× inflated.
- `Removed(TaskKey)` — a collapse that existed only in memory.
- `QUARANTINE_ATTEMPTS` — 73% of capacity spent on units that would never fit.
- The sealed reservation — 0 of 278 starts, then 8 of 131, then retuned.

In every case the counters looked healthy, workers were free, and nothing moved.
The operational lesson `CLAUDE.md` draws from six of these in one night is the
right one: **measure, fix ONE, re-measure** — because they queue behind each
other, and fixing two at once tells you nothing about either.

### How to debug this flow

**`timefusion_stats`**, maintenance section: `pending_<operation>` per operation,
`tasks_running`, `tasks_complete` (a *gauge*, not a counter — do not treat it as
monotonic), `eligible_watermark_lag_seconds`, `rollup_min_contiguous_days`,
`occ_conflicts_total`, `permit_wait_ms`.

The diagnostic questions, in order:

1. **Is anything running?** `tasks_running` at the worker cap with
   `tasks_complete` flat means units are timing out, not working.
2. **Is the queue real?** Compare `pending_sealed_consolidation` against an actual
   audit of file counts. A derived-operation queue that disagrees with storage is
   the storage that is right.
3. **Is something unclaimable?** Pending count flat with free workers means a
   predicate is refusing — check `dependencies_complete`, the deadline, and the
   quarantine state.
4. **Is a class starving?** All claims on today's slices means the sealed
   reservation is suspended (frontier lag over budget) or not firing.
5. **Did a collapse persist?** Compare the on-disk journal size before and after a
   coarsening pass. Byte-identical means the tombstones were not written.

**`timefusion sim <journal.json>`** (Chapter 23) replays a real production journal
through this exact scheduler on virtual time. It shares `operation_cycle` with the
server precisely so that a policy evaluated in the sim is the policy production
runs. Use it before deploying any change to the mix, the widths, or the
reservation.

**Key takeaways.** Maintenance work is `(table, source, project, slice,
operation)`, journaled crash-safely with a bias toward redundant work over missing
work, and re-derivable operations are deliberately *not* persisted because a
second copy of derived state can only go stale. Units are split by time and then
by hash to fit a 512 MiB decode budget, and fused up a divisibility lattice
because the per-unit cost is dominated by the commit. `claim_next` balances six
pressures — class, priority, dependency, project fairness, a sealed reservation
suspended under frontier lag, and quarantine for repeatedly-timing-out units — and
almost every line of it exists because a production measurement contradicted an
assumption. Facts should be keyed on what they are facts *about*: a day, not a
task.
## 18. Rollups: continuous aggregates and query routing

*After this chapter you will know how a rollup table is built from its
declaration, how a derived tier re-aggregates a finer one, how a query is matched
against a rollup and rewritten, what "hybrid" routing means and when it is
refused, and how the fifteen distinct reasons a query cannot use a rollup are
surfaced as telemetry.*

Chapter 6 covered the *declaration* (`RollupSpec`) and the synthesized schema.
This chapter covers the build and the read.

### The build SQL

A rollup partition is built by generating SQL and executing it. That is a
deliberate choice: the aggregate is expressible in the engine's own language, so
there is no second implementation to keep in step.

**Listing 18.1 — src/rollup.rs:77–84**

```rust
/// SQL that builds one source `(project_id, date)` partition.
///
/// Aggregate filters belong on each aggregate rather than in the row `WHERE`
/// clause. Moving them would make unrelated measures observe the wrong rows.
pub fn build_partition_sql(spec: &RollupSpec, source: &str, project_id: &str, date: &str) -> anyhow::Result<String> {
    build_partition_sql_from(spec, source, source, project_id, date)
}
```

The rule in that comment is the one most likely to be got wrong by hand: a
measure's `filter` becomes a SQL `FILTER (WHERE …)` clause **on that aggregate**,
never a row-level `WHERE`. `error_count` filters on `status_code = 'ERROR'`, but
`request_count` in the same rollup must still count every row. Hoisting the filter
to the row level would make every measure observe the error subset.

The measure expressions:

**Listing 18.2 — src/rollup.rs:164–200**

```rust
    let measures = spec
        .measures
        .iter()
        .map(|measure| {
            if derived {
                let expression = match measure.agg.as_str() {
                    "min" => format!("MIN({})", measure.name),
                    "max" => format!("MAX({})", measure.name),
                    "tdigest" => format!("tdigest_merge(CAST({} AS BYTEA))", measure.name),
                    "hll" => format!("hll_merge(CAST({} AS BYTEA))", measure.name),
                    _ => format!("SUM({})", measure.name),
                };
                return Ok(format!("{expression} AS {}", measure.name));
            }
            let expression = match (measure.agg.as_str(), measure.column.as_deref()) {
                ("count", None) => "COUNT(*)".to_string(),
                ("count", Some(column)) => format!("COUNT({column})"),
                ("tdigest", Some(column)) => format!("percentile_agg(CAST({column} AS DOUBLE))"),
                ("hll", Some(column)) => format!("hll_agg({column})"),
                (aggregate, Some(column)) => format!("{}({column})", aggregate.to_uppercase()),
                (aggregate, None) => return Err(anyhow::anyhow!("{} measure `{}` needs a source column", aggregate, measure.name)),
            };
            Ok(match &measure.filter {
                Some(filter) => format!("{expression} FILTER (WHERE {filter}) AS {}", measure.name),
                None => format!("{expression} AS {}", measure.name),
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?
        .join(", ");
```

Two different generators for the same measure list. Over raw rows, a `count`
becomes `COUNT(*)` and a `tdigest` becomes `percentile_agg(...)`. Over a finer
tier, *every* measure becomes a **state merge** — `SUM` of counts and sums, `MIN`
/ `MAX` of extrema, `tdigest_merge` / `hll_merge` of sketches.

**Listing 18.3 — src/rollup.rs:85–91**

```rust
/// `build_partition_sql`, but reading `from` instead of the raw source.
///
/// When `from` is a finer rollup the measures are re-aggregated as STATES —
/// `SUM` of counts and sums, `MIN`/`MAX` of extrema, `tdigest_merge` of digests —
/// and each measure's declared `filter` is deliberately NOT re-applied: the base
/// row already had it applied when it was built, and the filter's columns do not
/// even exist on the base table.
```

The filter is *not* re-applied on the derived path, for two independent reasons —
it was already applied when the base row was built, and the columns it references
do not exist on the base table. Either alone would be sufficient; together they
make the rule unambiguous. This is also why `RollupSpec::validate` requires a
derived measure to match its base *exactly* in aggregate, column, **and filter**
(Chapter 6, Listing 6.23): the derived build cannot re-apply a filter, so a
mismatch is unrepresentable rather than merely wrong.

### Incremental rebuild

A day whose source changed in one hour should not be rebuilt from scratch:

**Listing 18.4 — src/rollup.rs:158–166**

```rust
/// The partition's rows, rebuilt over `ranges` only and carried forward from
/// `target` everywhere else. Empty `ranges` means the whole day, from scratch.
///
/// The carried-forward rows are re-emitted verbatim: they were aggregated from
/// source rows that have not changed since, so re-aggregating them would produce
/// the same numbers at the cost of scanning the raw partition again — which is
/// the entire expense this exists to avoid.
pub(crate) fn build_partition_sql_ranges(
    spec: &RollupSpec, source: &str, from: &str, target: &str, project_id: &str, date: &str, ranges: &[(i64, i64)],
) -> anyhow::Result<String> {
```

Rows outside the changed ranges are read from the *existing rollup table* and
re-emitted verbatim. The argument is airtight — they were aggregated from source
rows that have not changed, so recomputing them yields identical numbers at the
cost of the scan this exists to avoid.

Which hours changed is tracked as a 24-bit mask:

**Listing 18.5 — src/rollup.rs:92–105**

```rust
/// One bit per hour of a UTC day. `ALL_HOURS` is the conservative value: every
/// invalidation means it unless the caller can prove a narrower set.
pub(crate) const ALL_HOURS: u32 = (1 << 24) - 1;

/// The hours of a partition-day a committed file can hold rows for, from its
/// Delta stats JSON (`minValues.timestamp` / `maxValues.timestamp`). `None`
/// when stats or timestamp bounds are absent — the caller falls back to
/// `ALL_HOURS`, never to skipping work. A computed mask of zero (bounds
/// entirely outside the partition day) is likewise treated as absent.
///
/// This is what lets a boot reconcile invalidate the ONE hour a downtime
/// commit actually touched instead of all 24 (`enqueue_maintenance_hours`
/// with `ALL_HOURS` was ~312 durable tasks per active project per restart,
/// prod 2026-08-18 — the queue's dominant growth source under deploy churn).
```

A 32-bit integer as a 24-bit hour set, with `ALL_HOURS` as the conservative
default: absent statistics fall back to "everything", **never to skipping work**.
The measured payoff is large — invalidating all 24 hours per commit produced ~312
durable tasks per active project per restart, which under deploy churn was the
dominant source of queue growth.

The stats parser accepts both spellings writers use:

**Listing 18.6 — src/rollup.rs:106–110, 135–140**

```rust
/// The inclusive timestamp bounds a file's Delta statistics claim.
///
/// Writers spell them either as epoch micros or RFC 3339, so both are accepted.
/// `None` means the file makes no claim — never treat that as an empty range.
pub(crate) fn stats_time_range(stats: &str) -> Option<(i64, i64)> {
```

```rust
const HOUR_MICROS: i64 = 3_600_000_000;

/// The `[start, end)` ranges `hours` marks on the day beginning at `day_start`,
/// with adjacent hours merged so a contiguous span costs one predicate.
```

"`None` means the file makes no claim — never treat that as an empty range" is
the same fail-safe direction as `ALL_HOURS`. Merging adjacent hours into ranges is
what keeps the generated predicate small.

### Generation identity

Every rollup row carries a `rollup_generation`, and how that value is computed
matters more than it looks:

**Listing 18.7 — src/rollup.rs:59–76**

```rust
/// Deterministic identity for one rollup generation.
///
/// It was a random UUID, which made the rollup rows on S3 unreadable after a
/// restart. It deliberately does not include the source fingerprint: independently
/// replaceable slices of one date must share a generation so a query can merge
/// them. The fingerprint remains in each Add tag and in the read ticket, where
/// it is a validity check rather than a row-selection key.
///
/// The spec participates because adding a measure without bumping the table
/// name would otherwise serve rows built under the old spec as if current.
pub fn generation_id(spec: &RollupSpec, source: &str, project_id: &str, date: &str, _source_fp: u64) -> String {
    use std::hash::{Hash, Hasher};
    let mut hasher = fnv::FnvHasher::default();
    format!("{spec:?}").hash(&mut hasher);
    (source, project_id, date).hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}
```

Three decisions, each with a stated consequence:

**Deterministic, not a UUID.** A random generation made the rollup rows on S3
unreadable after a restart — the process could no longer tell which generation was
current.

**The source fingerprint is deliberately excluded**, and the parameter is named
`_source_fp` to make that visible. Independently replaceable slices of one date
must share a generation so a query can merge them; the fingerprint lives in each
Add tag and in the read ticket, where it is a *validity check* rather than a
row-selection key.

**The spec participates**, via `format!("{spec:?}")` — so adding a measure without
bumping the table name produces a new generation, rather than serving rows built
under the old spec as if current. Hashing a `Debug` rendering is unusual but
correct here: it changes whenever any field of the spec changes.

### The read side: matching a query to a rollup

**Listing 18.8 — src/rollup.rs:1282, 1467**

```rust
pub(crate) async fn match_aggregates(
    // ... elided: walks the optimized plan for Aggregate nodes and tries each spec
```

```rust
async fn route_with_spec(
    // ... elided: per-spec matching, coverage check, and SQL generation
```

The output is the `RollupRewrite` from Chapter 7:

**Listing 18.9 — src/database/mod.rs:396–411**

```rust
/// A matched query's rollup substitute: the SQL to plan, the `Aggregate` node it
/// is substituted for, and the coverage ticket to re-check before it is used.
#[derive(Debug)]
pub(crate) struct RollupRewrite {
    pub sql: String,
    pub grain: String,
    /// `"full"` when the rollup answered the whole window, `"hybrid"` when raw
    /// fringes or a live tail were unioned in. Reported on the hit metric: the
    /// two have very different cost profiles and conflating them hides which one
    /// production actually gets.
    pub mode: &'static str,
    /// The `Aggregate` this rewrite replaces, verbatim, so the caller can swap it
    /// in place instead of taking the plan apart and rebuilding it.
    pub matched: datafusion::logical_expr::LogicalPlan,
    pub ticket: RollupReadTicket,
}
```

Two things worth noting. `mode` distinguishes `"full"` from `"hybrid"` on the hit
metric, because the two have very different cost profiles and conflating them
hides which one production actually gets — a hit rate of 90% means nothing if all
of it is hybrid. And `matched` carries the `Aggregate` node *verbatim* so the
caller can swap it in place, which is the substitution technique from Chapter 16
(Listing 16.2).

### Coverage tickets

A rollup may only answer if its rows are still valid for the source they
aggregate. That is checked twice — once when matching, once before use:

**Listing 18.10 — src/database/mod.rs:369–394**

```rust
#[derive(Debug, Clone)]
struct RollupCoverage {
    source_fp: u64,
    source_epoch: u64,
    generation: String,
    rows: u64,
    /// Exclusive upper bound on the source timestamps this build actually
    /// aggregated. `day_start + DAY_MICROS` for a sealed day — the whole
    /// partition — and less than that only for a day still being written.
    ///
    /// Stored rather than recomputed because it is TIME-VARYING for today: the
    /// build, the read and the ticket re-check all have to agree on the same
    /// bound, and two of them computing it a minute apart would disagree and
    /// invalidate perfectly good coverage on every query.
    covered_through: i64,
}

#[derive(Debug)]
/// (coverage key, source fingerprint, source epoch, generation, the BOUND the
/// fingerprint was taken to). The bound travels with the ticket because it is
/// the build's, not something the re-check may recompute — see
/// `RollupCoverage::covered_through`.
pub(crate) struct RollupReadTicket {
    dates: Vec<(RollupCoverageKey, u64, u64, String, i64)>,
    slices: Vec<(RollupSliceCoverageKey, u64, String)>,
}
```

**`covered_through` is stored, not recomputed**, and the reason generalizes: it is
*time-varying* for today, so the build, the read, and the ticket re-check must all
agree on the same value. Two of them computing it a minute apart would disagree,
and perfectly good coverage would be invalidated on every query. The bound
therefore *travels with the ticket*.

This is a good rule for any validity check over a moving boundary: **capture the
boundary at the point of proof and carry it, rather than recomputing it at the
point of use.**

### Hybrid routing

A rollup rarely covers a whole query window. The recent tail is still being
written; some interior dates may be uncovered. Hybrid routing unions the rollup's
*certified interior* with raw scans over the fringes:

**Listing 18.11 — src/rollup.rs:406–430**

```rust
/// How a measure's per-leg partial states combine into the query's answer.
///
/// The same combinator serves both shapes: over measure columns when the rollup
/// answers alone, and over the union's state aliases when a raw leg is present.
/// That is only sound because every variant is associative over a *partition* of
/// the row set — which is exactly what [`interior`] guarantees.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Merge {
    Count,
    Sum,
    Min,
    Max,
    Avg,
    TDigest,
    /// A distinct-count sketch. Like `TDigest`, the query's output IS the folded
    /// state: `distinct_count` reads the number out of it in the projection
    /// above the aggregate, which the rewrite never touches.
    Hll,
}

impl Merge {
    /// State columns consumed, in order. `Avg` is the only multi-state merge: an
    /// average is not a state, so the legs must carry sum and count apart or the
    /// union would average two averages.
    const fn arity(self) -> usize {
```

The soundness argument is stated precisely: every merge variant is associative
*over a partition of the row set*, and `interior` is what guarantees the legs
partition rather than overlap. Overlapping legs would double-count.

`Avg`'s arity is the illustrative case. An average is not an aggregate *state* —
you cannot merge two averages — so the legs must carry sum and count separately
and divide at the end. Modelling arity explicitly is what makes that
representable.

The branch count is bounded:

**Listing 18.12 — src/rollup.rs:569**

```rust
pub(crate) fn hybrid_branch_count(lo: i64, hi: i64, ranges: &[(i64, i64)]) -> usize {
```

with a test that pins the arithmetic:

**Listing 18.13 — src/rollup.rs:2731–2734**

```rust
    fn hybrid_branch_count_includes_rollup_and_raw_ranges() {
        // ... elided: `ranges` fixture
        assert_eq!(hybrid_branch_count(0, 70, &ranges), 7);
        assert_eq!(hybrid_branch_count(10, 60, &ranges), 5);
    }
```

Too many branches is a *refusal*, not a slow plan — `MissReason::TooManyBranches`
below.

### The fifteen ways a query cannot use a rollup

This enum is the single most useful diagnostic surface in the routing layer:

**Listing 18.14 — src/rollup.rs:10–52**

```rust
/// Why a query cannot use a rollup. Variant names ARE the `rollup_misses`
/// telemetry labels (snake_case); the two `serialize` overrides are historical
/// names prod dashboards already query on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::EnumIter, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum MissReason {
    UnsupportedShape,
    MissingProject,
    UnboundedTime,
    UnknownGroupBy,
    UnknownFilter,
    /// The residual row filter constrains columns no declared measure filters
    /// on, so no rollup could ever have answered it. Separated from
    /// `UnknownFilter` so that counter means "a filter we should have matched
    /// and didn't" — see the decline site for the prod evidence.
    FilterNotEligible,
    MissingMeasure,
    #[strum(serialize = "non_decomposable")]
    NonDecomposableAggregate,
    /// Names the ONLY thing this reason still means: a `time_bucket` width that
    /// is not a multiple of the grain. The window's own alignment stopped
    /// mattering once raw fringes were added.
    #[strum(serialize = "unaligned_bucket_width")]
    PartialBucket,
    /// No rollup was ever built for a date in the window.
    NotBuilt,
    /// A rollup exists for the date but the source has moved under it.
    StaleCoverage,
    /// Coverage cannot be established at all: buffered rows, or a window whose
    /// dates cannot be enumerated.
    IncompleteCoverage,
    /// The certified interior is too small a slice of the window to be worth the
    /// union's second scan.
    TinyInterior,
    /// Hybrid routing would create too many disjoint raw/rollup predicates.
    TooManyBranches,
    RewriteSchemaMismatch,
}

impl MissReason {
    pub fn label(self) -> &'static str {
        self.into()
    }
}
```

Four design notes.

**The variant names *are* the telemetry labels**, via `strum`. There is no
separate mapping table to drift, and the two `serialize` overrides exist purely so
that dashboards already querying the historical names keep working — a
compatibility shim placed exactly where it belongs.

**`FilterNotEligible` was split out of `UnknownFilter`** so that `UnknownFilter`
means "a filter we *should* have matched and didn't" — an actionable bug — while
`FilterNotEligible` means "no rollup could ever have answered this", which is a
design fact about the query. Splitting a counter so that one half becomes
actionable is worth doing whenever a metric is being used to decide whether to
investigate.

**`PartialBucket`'s comment narrows what it still means.** Once raw fringes were
added, window *alignment* stopped mattering; the only remaining cause is a
`time_bucket` width that is not a multiple of the grain. Updating a variant's doc
comment when its meaning narrows is how you stop a stale name from misleading
future readers.

**`strum::EnumIter`** exists so every reason can be enumerated and exported as a
zero-valued counter at startup — an absent label and a zero label look identical
in most dashboards otherwise.

Mapping the reasons to what an operator should do:

| Reason | Actionable? | What to do |
|---|---|---|
| `UnsupportedShape`, `NonDecomposableAggregate` | design | the query cannot be rolled up |
| `MissingProject`, `UnboundedTime` | query | add `project_id` / a time bound |
| `UnknownGroupBy`, `FilterNotEligible` | design | the column is not a declared dimension |
| `UnknownFilter`, `MissingMeasure`, `RewriteSchemaMismatch` | **bug** | should have matched; investigate |
| `PartialBucket` | query | bucket width is not a multiple of the grain |
| `NotBuilt`, `IncompleteCoverage` | maintenance | the rollup has not been built for these dates |
| `StaleCoverage` | maintenance | the source moved; rebuild is queued |
| `TinyInterior`, `TooManyBranches` | expected | hybrid would cost more than it saves |

`CLAUDE.md` records that a production investigation into rollup misses found they
were **one query shape**, and that `rollup_miss_sampled` — which renders the
refused plan — is the thing to reach for first.

### Cohort builds

One more build-side detail worth knowing:

**Listing 18.15 — src/rollup.rs:370–375**

```rust
/// Split a cohort aggregate by its output `project_id` and shape each project
/// with its own generation. The aggregate must retain `project_id` as a group
/// key; synthesizing it from the cohort request would mix tenant identities.
```

Several small projects can be aggregated in one pass — a *cohort* — but the
aggregate must keep `project_id` as a group key and the split must use the
*output* value. Synthesizing it from the request would mix tenant identities, i.e.
attribute one tenant's rows to another. The same class of hazard as Chapter 16's
stripped tenant conjunct.

And the batch conversion:

**Listing 18.16 — src/rollup.rs:291–298**

```rust
/// Convert aggregate batches into rows for the generated rollup schema.
///
/// The aggregate output contains `timestamp`, then configured dimensions and
/// configured measures. All remaining target fields are internal identity or
/// partition fields. The conversion deliberately copies each configured Arrow
/// array and casts it only at the generated target boundary, so binary digest
/// state and non-string dimensions retain their types.
pub fn to_rollup_batches(
```

Casting only at the target boundary is what keeps a `tdigest` state binary rather
than round-tripping it through a string.

### The payments span, rolled up

Our example row contributes to the 1-minute bucket `2025-01-17 14:25:00` keyed
`(payments-api, server, ERROR)` in
`otel_logs_and_spans_rollup_dashboard_1m_v3`. Its measure contributions:

| Measure | Contribution |
|---|---|
| `request_count` | +1 |
| `duration_count` | +1 (`duration` is non-null) |
| `error_count` | +1 (`status_code = 'ERROR'`) |
| `duration_sum` | +3,421,000,000 |
| `duration_min` / `duration_max` | candidate |
| `server_*` (all seven) | +1 / candidate — `kind = 'server'` |
| `server_duration_digest` | one sample at 3,421,000,000 |

That bucket is then re-aggregated into `…_rollup_dashboard_1h_v2`'s
`2025-01-17 14:00:00` bucket by `SUM`ing the counts and sums, `MIN`/`MAX`ing the
extrema, and `tdigest_merge`ing the digest — never by re-reading the raw
partition.

A dashboard asking for 5-minute buckets over one hour routes to the 1m tier
(5 min is a multiple of 1 min). A dashboard asking for 6-hour buckets over 30 days
routes to the 1h tier. A query filtering on `resource___service___name`,
`kind`, or `status_code` still routes — those are declared dimensions. A query
filtering on `context___trace_id` does not, and the miss is recorded as
`unknown_group_by` or `filter_not_eligible`.

### How to debug this flow

**`rollup_misses`**, labelled by the fifteen reasons above. The useful ratio is
misses-by-reason over total, and the useful action depends entirely on which
reason dominates (see the table).

**`rollup_hits`**, labelled `full` versus `hybrid`. A high hit rate that is all
hybrid means the tier is not covering the window and the second raw scan is being
paid on every query.

**`rollup_miss_sampled`** renders the refused plan. Reach for it first — it
answers "which query shape is missing" directly rather than by inference.

**`rollup_min_contiguous_days`** is the coverage goal metric, and it is what
switches the coordinator between `CYCLE_BALANCED` and `CYCLE_COVERAGE_SHORT`
(Chapter 17).

**`rollup_tier_untagged`** counts live tier files with no slice tags — files a
replace-set can never remove. A rising value means something rewrote tier files
without preserving tags.

**`raw_tail_duration_secs`** is `FINALIZATION_DELAY + frontier lag` — how much of
a hybrid query's window has to be answered from raw rows.

**Reading the generated SQL.** `build_partition_sql` and `hybrid_sql` produce
plain text, so the fastest way to understand a routing decision is to log the
generated statement and run it by hand. The tests do exactly this
(`src/rollup.rs:2163–2200`), comparing generated SQL against expected shapes.

**Key takeaways.** A rollup is built by generating SQL, with measure filters
attached to each aggregate rather than to the row filter, and a derived tier
re-aggregates *states* — sums of counts, merges of sketches — never re-applying
the base filter. Rebuilds are incremental over a 24-bit hour mask that falls back
to "all hours" whenever statistics are absent. A generation id is deterministic,
excludes the source fingerprint (so slices merge) and includes the spec (so a
changed measure cannot be served as current). Coverage is proved with a ticket
that *carries* the time bound it was proved to, because recomputing a moving
boundary at the point of use invalidates good coverage. And the fifteen
`MissReason` variants are the telemetry labels themselves, split so that one of
them means "a bug" rather than "a design fact".
## 19. Compaction, dedup, footer repair, and vacuum

*After this chapter you will know the four kinds of file rewriting TimeFusion
does, why each exists, why they are split rather than unified, how a rewrite is
staged and committed, what a "converged" file is, and why footer repair is the
subsystem most likely to consume its own budget producing nothing.*

### Four jobs, deliberately separate

| Job | Scope | Unit | Why |
|---|---|---|---|
| **Hot-tail packing** | today only | bin of small files | ingest produces ~1 MB files; queries open hundreds |
| **Sealed consolidation** | sealed days | leveled bins to target | cold data should be at the 256–512 MB target |
| **Dedup** | any `(project, date)` | slice or bin | merge-on-read versions and cross-bucket duplicates |
| **Footer repair** | sealed days | one whole file | a file with no `sorting_columns` poisons its partition |

Splitting packing from repair is not organizational tidiness — it is a scheduling
requirement:

**Listing 19.1 — src/database/mod.rs:6395–6410**

```rust
/// The hot tail runs two jobs that were one, and merging them broke both.
///
/// Packing is continuous (today's small files, small units). Footer repair is finite (a fixed
/// backlog of large sealed files, each a whole-file global sort). Sharing the same short cron
/// gave repair a slice of a budget it cannot fit in, and `stage_hot_bin` discards a bin that
/// outruns the budget and re-selects the identical file next time. Splitting them lets each get
/// the budget its unit size actually needs.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum TailPass {
    /// Today's partition only: bin-pack small files. Never touches sealed dates.
    Pack,
    /// Sealed dates only: rewrite ONE footer-less file per project so the
    /// reader's all-or-nothing ordering claim survives. Never touches today.
    Repair,
}
```

The failure mode when they shared a budget is worth naming because it recurs
throughout maintenance: **a unit that outruns its budget is discarded at the
deadline, and the next pass re-selects the identical unit.** That is not slow
progress, it is *zero* progress at full cost. Any scheduler that discards
unfinished work must guarantee its units fit.

### Why compaction matters at all

`CLAUDE.md` records the measurement: ~1 MB files against a 256 MB target meant
**408 file opens per second** on the query path. Compaction and query latency are
not two problems; they are one.

### Ownership: who compacts what

`optimize_table` carves the calendar so no two passes fight over the same
partition:

**Listing 19.2 — src/database/compact.rs:1–30**

```rust
//! OPTIMIZE / compaction: Z-order, hot-tail, sealed-partition, and dedup rewrites.
use super::*;

impl Database {
    /// Optimizes recent Delta partitions for time-range reads.
    pub async fn optimize_table(&self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, _target_size: Option<i64>) -> Result<()> {
        let start_time = std::time::Instant::now();
        let window_hours = self.config.maintenance.timefusion_optimize_window_hours.max(1);

        let table_clone = {
            let table = table_ref.read().await;
            table.clone()
        };

        let now = Utc::now();
        let today = now.date_naive();
        let num_days = (window_hours / 24).max(1);
        // Cold consolidation (daily) owns sealed partitions older than
        // `cold_optimize_after_days` and bin-packs them to the 512MB target.
        // Exclude them from the 30-min warm Z-order so it can't fragment those
        // cold files back to the warm target every cycle (oscillation = wasted
        // S3 I/O). With after_days=1 this leaves warm processing only today —
        // the partition still taking writes.
        let after_days = self.config.parquet.cold_optimize_after_days();
        // Light optimization owns today's event-time-disjoint runs.
        let skip_today = self.config.maintenance.timefusion_light_optimize_enabled;
        let window_dates: Vec<chrono::NaiveDate> = (0..=num_days)
            .map(|days_ago| (now - chrono::Duration::days(days_ago as i64)).date_naive())
            .filter(|d| !(Self::date_is_cold(today, *d, after_days) || skip_today && *d == today))
            .collect();
```

Two exclusions with two different justifications. **Cold partitions are excluded**
because the daily cold pass packs to a 512 MB target and a 30-minute warm pass
would fragment them back to the warm target every cycle — an oscillation that
costs S3 I/O and produces nothing. **Today is excluded** because light
optimization owns it.

With `after_days = 1`, the two exclusions leave `optimize_table` with *nothing* on
most days. That is intentional: it is the fallback for the window between "today"
and "cold", not the main path.

### The Z-order idempotence guard

**Listing 19.3 — src/database/compact.rs:51–60**

```rust
        // delta-rs ZOrder has NO idempotence guard (unlike Compact it does no
        // size / single-file / already-sorted check): it rewrites every file in
        // the selected partitions on every run, even sealed days that didn't
        // change — and PR #39 then has to re-warm all those cold rewrites. Skip
        // any partition whose live file set is identical to the last successful
        // optimize. `today` is always processed (growing leading edge).
        let kept_dates: Vec<chrono::NaiveDate> = {
            let guard = self.zorder_filesets.read().await;
            let prev = guard.get(&table_url);
            window_dates
```

delta-rs's `Compact` skips files already at target and drops single-file bins;
`ZOrder` does neither and rewrites everything, every run. TimeFusion supplies the
missing guard by remembering each partition's live file set after a successful
optimize (`zorder_filesets`, Chapter 7). In-memory only — a restart re-Z-orders
each partition once, harmlessly.

Recall from Chapter 6 that `otel_logs_and_spans` declares `z_order_columns: []`,
so on the production table this path is inert. The guard matters for tables that
do use Z-order.

The target size follows the partition's role:

**Listing 19.4 — src/database/compact.rs:41–49**

```rust
        // Keep the active partition at the light-compaction target. A single
        // day-sized file would make 1h and 3h predicates select the same file
        // even when timestamp ordering makes their row groups disjoint.
        let target_size = if window_dates.contains(&today) {
            self.config.maintenance.timefusion_light_optimize_target_size
        } else {
            self.config.parquet.timefusion_optimize_target_size
        };
```

Bigger is not always better. A single day-sized file makes a 1-hour and a 3-hour
predicate select the *same* file even though timestamp ordering makes their row
groups disjoint — so file-level pruning stops working and every recent-window
query reads the whole day.

### Convergence: when to stop rewriting

The most important predicate in packing:

**Listing 19.5 — src/database/mod.rs:6411–6415**

```rust
impl HotBinPolicy<'_> {
    /// A file at or past 7/8 of target is "converged" — re-selecting it alone
    /// would rewrite it 1→1 forever.
    fn converged(&self) -> i64 {
        let cap = self.target_size.max(1);
```

Without a convergence threshold, a bin-packer re-selects an almost-target file
forever, rewriting it 1→1 at full cost. `CLAUDE.md` records the production version
of this bug: packing rewrote *converged half-gigabyte files* because the size skip
sat behind a tag that only `OPTIMIZE` writes — "a right scheduler on a wrong
eligibility test looks broken".

### Sizing a unit to the time it has

Three related functions turn a wall-clock budget into a byte cap:

**Listing 19.6 — src/database/mod.rs:6295–6340**

```rust
/// Largest file a repair pass will admit, derived from the time it actually has.
///
/// A repair pass may admit roughly `budget * INPUT_BYTES_PER_SEC` and still finish. Never smaller
/// than the configured value, so raising the knob still widens the reach.
pub(crate) fn repair_reach_bytes(configured: i64, budget: std::time::Duration) -> i64 {
    configured.max(budget_bytes(budget))
}

/// Conservative input bytes one hot-tail rewrite sustains per second.
///
/// Measured on an uncontended 1.19 GB repair rewrite. Packing bins ran faster under concurrency,
/// so this stays the conservative floor for both.
const INPUT_BYTES_PER_SEC: i64 = 460_000;

fn budget_bytes(budget: std::time::Duration) -> i64 {
    (budget.as_secs().min(i64::MAX as u64) as i64).saturating_mul(INPUT_BYTES_PER_SEC)
}

/// Fraction of the tick a packing bin may consume, leaving the rest as margin.
///
/// Sizing a bin to fill the whole budget leaves nothing for the commit, and any dip below the
/// assumed rate turns the tick's output to zero. A single global rate estimate cannot be tight
/// because rewrite rate varies widely by table; it can only be safe. Half the budget puts the
/// slowest table safely inside the tick while letting fast tables run several rounds.
const PACK_BUDGET_FRACTION: u32 = 2;

/// Largest bin a packing pass will assemble, derived from the time it has.
///
/// A bin that cannot be rewritten inside the tick is discarded at the deadline and the next
/// tick re-selects the same files, so an oversized target produces nothing while burning the
/// whole budget. Capping at [`PACK_BUDGET_FRACTION`] of the budget removes stragglers. The
/// whole policy shrinks together with this value, so a freshly packed run still lands above
/// `sorted_run_cap` and `converged`; otherwise smaller outputs would be re-selected forever.
pub(crate) fn pack_target_bytes(configured: i64, budget: std::time::Duration) -> i64 {
    configured.min(budget_bytes(budget / PACK_BUDGET_FRACTION)).max(1)
}
```

Three ideas that generalize well beyond this codebase.

**A single global rate estimate can only be safe, never tight.** Rewrite rate
varies widely by table, so 460 kB/s is the conservative floor measured on an
uncontended 1.19 GB rewrite — not an average.

**Leave margin for the commit.** Sizing a bin to fill the whole budget leaves
nothing for the commit, and any dip below the assumed rate turns the tick's output
to zero. Half the budget puts the slowest table safely inside the tick while
letting fast tables run several rounds.

**The whole policy must shrink together.** If `pack_target_bytes` shrinks but
`converged` and `sorted_run_cap` do not, a freshly packed run lands *below* the
convergence threshold and is re-selected forever — the exact loop the sizing was
meant to avoid.

And the wave cap, whose two values differ by 40×:

**Listing 19.7 — src/database/mod.rs:6104–6119**

```rust
/// Wave cap for one tail pass. A wave serves each project at most one bin, so this is also
/// the per-project file ceiling.
///
/// Packing keeps 12: bins are minutes apart on `today` and run frequently; a never-ending
/// pack tick would hold the light pool against the next flush. Repair uses a much larger cap
/// because its 8640s budget is meant to chew through a backlog; capping it at 12 files per
/// pass would make a large backlog converge at only 12 files per hour and fail to outrun
/// deploy churn. The real bounds are the deadline and memory brake enforced inside
/// `round_robin_bins`; this constant only stops a runaway loop.
const fn max_waves(pass: TailPass) -> usize {
    match pass {
        TailPass::Pack => 12,
        TailPass::Repair => 512,
    }
}
```

"Capping it at 12 files per pass would make a large backlog converge at only 12
files per hour and **fail to outrun deploy churn**" — the rate a repair backlog
must beat is the rate at which new damage is created, and deploys create damage
(Chapter 12: a flush that cannot get a sort slice writes an unsorted file).

### Footer repair

A Parquet file written without `sorting_columns` in its footer does not merely
fail to help — it **disables the reader's all-or-nothing ordering claim for its
entire partition** (Chapter 15). One bad file makes every scan of that partition
slower, and can push merge-on-read dedup from bounded into unbounded mode.

Repair rewrites one such file per project per pass, as a whole-file global sort.
That is an expensive unit, and almost every constant around it exists to stop it
consuming its budget for nothing.

**Listing 19.8 — src/database/mod.rs:5644–5650**

```rust
/// A whole-file sort does not fit the light pool, so the unit must shrink. 256 MB is the only
/// value with an observed successful completion. A 64 MB target was tried and reverted: the
/// apparent throughput cliff was concurrent-bin contention, not size. If retuning, measure
/// completion time for one bin of a known size on an idle box.
const REPAIR_SLICE_TARGET_BYTES: i64 = 256 * 1024 * 1024;
```

"The apparent throughput cliff was concurrent-bin contention, not size" — a tuning
change that appeared to help and did not, with the instruction for how to retune
correctly (measure one bin of a known size on an *idle* box). Recording a rejected
hypothesis is as useful as recording an accepted one.

Slicing needs a time range, and the probe declines rather than risking data:

**Listing 19.9 — src/database/mod.rs:5652–5660**

```rust
/// Run the min/max/null probe for slice planning. `None` declines slicing —
/// on any error, a non-i64 sort column, or a single NULL, because a NULL would
/// fall outside every range and be dropped.
///
/// The probe is an aggregate with no ORDER BY, so it costs a scan and no sort
/// memory. Slicing then costs one scan per slice; for a repair that otherwise
/// never completes at all, re-reading is the cheap side of the trade.
async fn bin_time_range(ctx: &datafusion::prelude::SessionContext, probe: &str) -> Option<(i64, i64)> {
```

**A single NULL declines the whole optimization**, because a NULL falls outside
every range and would be silently dropped. And the cost argument is explicit:
re-reading once per slice is cheap compared to a repair that never completes.

The escalation ladder is the cleverest piece here:

**Listing 19.10 — src/database/mod.rs:6343–6353**

```rust
/// Consecutive staging failures after which a repair candidate stops being offered for the rest
/// of the process.
///
/// A repair bin is one whole-file sort. If its working set does not fit, the failure is
/// deterministic, not transient. Without quarantine that file would be re-selected identically
/// on every pass and consume the wave. Three attempts, not one, because staging can also fail for
/// transient reasons (OCC race, concurrent rewrite, restart mid-stage). A success clears the
/// count.
const REPAIR_QUARANTINE_AFTER: u32 = 3;

/// Sort parallelism ladder a repair bin is retried at before its pool exhaustion is believed.
///
/// `REPAIR_SORT_PARTITIONS` (16) is the fast setting, but exhaustion there used to be treated
/// as proof a file is impossible. The working set is a function of both file size and
/// parallelism: each partition has an unspillable merge operator, so 16 partitions means 16
/// unspillable merges competing for the pool. A single-partition sort has no merge and spills
/// within its fair share. Exhaustion at 16 says nothing about 1, so the ladder descends
/// through 16, 4, 1. Exhaustion at the floor is believed.
const REPAIR_SORT_PARTITION_LADDER: [usize; 3] = [REPAIR_SORT_PARTITIONS, 4, 1];
```

The insight is precise: **the working set is a function of both file size and
parallelism.** Each sort partition carries an *unspillable* merge operator, so 16
partitions means 16 unspillable merges competing for one pool. A single-partition
sort has no merge at all and spills within its fair share. So "it did not fit at
16" says nothing about 1 — and treating it as proof of impossibility was
abandoning repairable files.

`CLAUDE.md` records the same physics from the other direction: a repair sort died
25.3 MB short, and "partitions scale the UNSPILLABLE merge".

The result is a two-dimensional escalation — descend the parallelism ladder, then
count strikes — and the decision is factored out so it can be tested without a
pool to exhaust:

**Listing 19.11 — src/database/mod.rs:6355–6362**

```rust
/// What a failed repair staging costs the candidate: the parallelism to retry
/// at next (`None` = nothing cheaper left) and the strike step to charge.
///
/// Split out from the failure arm so the escalation rule is testable without a
/// pool to exhaust. A pool exhaustion is only "deterministic" once it happens
/// at the bottom of [`REPAIR_SORT_PARTITION_LADDER`]; above the bottom it buys
// ... elided: the returned (next_parallelism, strike) pair
```

Candidate selection avoids re-probing files it has already cleared:

**Listing 19.12 — src/database/mod.rs:6096–6125**

```rust
/// adds a fresh batch of untagged suspects; newest-first walks them first. `repair_verified_sorted`
/// is in-process only, so a restart resets to the newest candidate.
const REPAIR_RESELECT_ROUNDS: usize = 64;
```

```rust
const REPAIR_VERIFY_CONCURRENCY: usize = 16;

/// How many verified-sorted paths survive a restart. One path is ~150 bytes, so
/// 200k is ~30 MB on disk and bounds boot-time load. Newest wins, because the
/// walk is newest-first and the recent tail is what the next pass will re-probe.
const REPAIR_VERIFIED_PERSIST_CAP: usize = 200_000;
```

The `repair_verified_sorted` set (Chapter 7, Listing 7.14's neighbourhood) exists
because *untagged does not mean unsorted*:

> The `delta-rs.optimize.sort_by` tag ≠ a `sorting_columns` footer (flush sorts
> without the tag), so untagged means suspect, not unsorted — admission by tag
> would rewrite healthy files.

So each candidate costs one footer read to *prove* it needs repair, and the proof
is cached (in memory, and persisted up to 200k paths).

**Listing 19.13 — src/database/mod.rs:6131–6135**

```rust
/// Compressed input admitted to one coordinator L0 sort. Production zstd
/// expansion is about 17x, so 16 MiB leaves room in the 512 MiB decoded pool
/// without turning every flush-file rewrite into a multi-gigabyte spill.
/// Sorted runs are merged separately toward the 256/512 MiB physical targets.
const COORDINATOR_L0_SORT_TARGET_BYTES: i64 = 16 * 1024 * 1024;

const SORTED_RUN_TAG: &str = "delta-rs.optimize.sort_by";
```

**A 17× zstd expansion factor** is the number that connects "compressed bytes on
S3" to "decoded bytes in the pool". 16 MiB in becomes ~272 MiB decoded, which fits
the 512 MiB coordinator pool with room for the merge.

### The flush-side of footer damage

Repair exists because flushes sometimes write unsorted files, and that path has
its own sizing:

**Listing 19.14 — src/database/mod.rs:6280–6295**

```rust
/// 512 MB was too small: a single sort's merge phase exceeded it and a second sort was admitted
/// alongside, exhausting the whole pool. Every such refusal writes the group unsorted, and one
/// unsorted file disables the reader's all-or-nothing footer ordering for its entire partition.
const MIN_SPILL_SORT_BYTES: usize = 1 << 30;

/// How many escalated flush sorts may run at once on a pool of `pool_bytes`.
///
/// At the 1 GB default this is ONE: escalated sorts serialize. That is the trade
/// the gate was always meant to make — "queueing costs latency on an already
/// oversized group; losing the slice costs the partition's footer ordering on
/// every later scan" — and concurrency is bought back by raising
/// `TIMEFUSION_FLUSH_SORT_POOL_MB`, not by pretending a sort fits in half a
/// gigabyte.
pub(crate) fn flush_sort_permits(pool_bytes: usize) -> usize {
    (pool_bytes / MIN_SPILL_SORT_BYTES).max(1)
}
```

The trade is stated in one sentence: *queueing costs latency on an already
oversized group; losing the slice costs the partition's footer ordering on every
later scan.* Serializing escalated flush sorts is the right side of that trade,
and the way to buy concurrency back is to raise the pool — "not by pretending a
sort fits in half a gigabyte".

This closes a loop that spans four chapters: a flush sort that cannot get a slice
(Ch. 12) writes a file with no `sorting_columns` footer, which makes every scan of
that partition slower and pushes dedup into unbounded mode (Ch. 15), which the
repair pass then has to fix at ~460 kB/s (this chapter) — and the only signal that
it is happening is `ordering_violations_total`.

### Dedup

Two shapes of duplicate exist. Flush-time dedup (Chapter 12) collapses retries
inside one bucket. This pass handles the rest:

**Listing 19.15 — src/database/compact.rs:959–992**

```rust
    /// Cross-flush dedup: collapse a `(project_id, date)` partition by `dedup_keys` and write back
    /// via `replace_where`. No-op on no dedup_keys or no duplicates.
    ///
    /// Returns `(rows_dropped, complete)`. `complete=false` means duplicate-bearing work was skipped
    /// (unsealed chunks, rewrite budget, vanished snapshot rows) — the partition must not be
    /// fingerprinted clean, or the read-side dedup skip would serve duplicates.
    ///
    /// Stage-and-commit the whole partition as a SINGLE wave. Used by the fallback
    /// sweep, which has no queue to batch across; the dirty-bin path stages with
    /// [`Self::stage_dedup_partition_range`] directly so one wave can span many bins.
    pub async fn dedup_partition(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, project_id: &str, date: chrono::NaiveDate,
    ) -> Result<(u64, bool)> {
        self.dedup_partition_range_limited(table_ref, table_name, project_id, date, None, None).await
    }

    pub(crate) async fn dedup_partition_range_limited(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, project_id: &str, date: chrono::NaiveDate,
        slice: Option<crate::maintenance_coordinator::TimeSlice>, limits: Option<DedupExecutionLimits>,
    ) -> Result<(u64, bool)> {
        let options = DedupRangeOptions { slice, dirty_key: None, limits };
        let (units, complete) = self.stage_dedup_partition_range(table_ref, table_name, project_id, date, options).await?;
        if units.is_empty() {
            return Ok((0, complete));
        }
        let markers = vec![format!("date={date}/")];
        let result = self.commit_wave(table_ref, table_name, &markers, true, units, 0).await;
        let dropped = wave_dropped_rows(&result.landed);
        for bin in &result.landed {
            if let Some(d) = &bin.dedup {
                info!("dedup rewrite: table={} chunk=[{}] dropped={} (before={} after={})", table_name, d.label, d.dropped(), d.before, d.after);
            }
        }
        // A unit that didn't land left its duplicates in place — the partition
        // must NOT be certified clean (2026-07-05 review).
        Ok((dropped, complete && result.failed.is_empty()))
    }
```

The `complete` flag is the load-bearing return value, and it is computed
conservatively in two places: `complete` from staging (was any duplicate-bearing
work skipped?) **and** `result.failed.is_empty()` from the commit. Either failing
means the partition must not be certified clean — because certification is what
lets the read path skip `DedupExec` entirely (Chapter 13), and certifying a
partition that still holds duplicates serves wrong counts.

Three reasons staging reports incomplete: unsealed chunks, exhausted rewrite
budget, and rows that vanished from the snapshot mid-pass. All three mean "some
duplicates were not examined", not "there were none".

The dedup budget constants come from configuration and encode the same
compressed-to-decoded reasoning as the repair sort:

| Constant | Default | Meaning |
|---|---|---|
| `timefusion_dedup_max_rewrite_bytes` | 512 MiB | compressed input per rewrite |
| `timefusion_dedup_max_decoded_bytes` | 512 MiB | decoded ceiling |
| `timefusion_dedup_decode_inflation` | 12 | assumed compressed→decoded ratio |
| `timefusion_dedup_bytes_per_row` | 4096 | row-size estimate for planning |
| `timefusion_dedup_lookback_days` | 35 | how far back the sweep reaches |

⚠️ Note the inflation factor here is **12**, while the coordinator's L0 sort
comment (Listing 19.13) states production zstd expansion is about **17×**. The two
are used in different places and the dedup one is the older estimate; a
12× assumption against 17× reality under-estimates decoded bytes by ~40%.

`CLAUDE.md` records the byte-estimate bug that dominated the queue: the estimate
counted **whole files** for any overlap, so a 10-minute slice estimated what a
whole day would cost — producing a 427× over-split and an 88,000-unit queue. The
fix is `slice_share_of_file` (Chapter 7's neighbourhood, `src/database/mod.rs:2321`),
which scales a file's contribution by the share of its time span the slice covers.

### Recompression

The one force-rewrite path, and the reason it exists is subtle:

**Listing 19.16 — src/main.rs:912–924**

```rust
    // `--recompress` is the ONLY force-rewrite. Bin-packing (`Compact`/`SortBy`,
    // and `consolidate`'s leveled variant) skips files already at target AND
    // drops single-file bins, so a lone file can never be rewritten by them —
    // which is exactly the shape of a partition poisoned by ONE file with no
    // `sorting_columns` footer. On prod 2026-08-07 that was 448 of 501 poisoned
    // partitions: `optimize` and `--consolidate` both reported success having
    // changed nothing (`removed=0 added=0`, file bytes identical).
    //
    // `recompress_partition` rewrites the partition through `replace_where`
    // with the schema ORDER BY, regardless of file count or size, so the output
    // carries an honest sorted footer. `--project` narrows the overwrite
    // predicate to `date = '...' AND project_id = '...'`, which is what makes
    // the job small enough to run on an ordinary runner.
```

**448 of 501 poisoned partitions could not be fixed by any bin-packing path**,
because a single-file bin is dropped and a file already at target is skipped — and
"one file with no footer" is exactly a single-file, at-target case. Both
`optimize` and `--consolidate` reported success having changed nothing.

The implementation:

**Listing 19.17 — src/database/compact.rs:620–658**

```rust
    /// z-order columns).
    ///
    /// Skips partitions whose probe file already advertises a tier >= `target_level` via Parquet
    /// footer metadata. Probes only one file per partition: every file in a successfully
    /// recompressed partition shares the same tier. A partial rewrite can leave mixed tiers; the
    /// next sweep may skip based on the probe, but the partition is re-evaluated the next day.
    /// `project` scopes the rewrite to one `project_id=` partition, which is the honest unit of
    /// repair.
    pub async fn recompress_partition(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, date: chrono::NaiveDate, target_level: i32, project: Option<&str>,
    ) -> Result<RecompressOutcome> {
        // ... elided: imports
        let date_str = date.to_string();
        if project.is_some() {
            // Scoped `replace_where` can deadlock; reject it before reading the table.
            anyhow::bail!("recompress --project is disabled: scoped replace_where deadlocks; re-run without --project");
        }
        let date_marker = format!("date={date_str}");
        // ... elided: URI collection under the read lock
        if uris.is_empty() {
            debug!("recompress: no files in partition date={} for table={}", date_str, table_name);
            return Ok(RecompressOutcome::Skipped("no files in partition"));
        }

        // Recompress rewrites whole partitions — same pool-invisible Arrow
        // materialization as dedup/optimize; hold a maintenance-rewrite permit.
        // Acquired after the empty-partition early-out so no-op calls are free.
        let _rewrite_permit = self.maintenance_rewrite_sem.acquire().await.map_err(|e| anyhow::anyhow!("maintenance rewrite semaphore closed: {e}"))?;
```

⚠️ **Contradiction, in the source.** The doc comment describes `--project` as
"the honest unit of repair", `main.rs`'s comment says `--project` "is what makes
the job small enough to run on an ordinary runner", and the very first statement
of the function **rejects `--project` outright** because scoped `replace_where`
deadlocks. The rejection is what actually runs; the two comments are stale. This
is Chapter 29 material.

The compression-tier probe is a nice idempotence trick: the chosen zstd level is
written into the Parquet footer's key-value metadata
(`COMPRESSION_TIER_KEY = "timefusion.compression_tier"`,
`src/database/mod.rs:2020`), so a later sweep can skip a partition by reading *one*
file's footer.

**Listing 19.18 — src/database/mod.rs:2007–2021**

```rust
const ZSTD_COMPRESSION_LEVEL: i32 = 3;

// ... elided: RecompressOutcome's variants
pub enum RecompressOutcome {
    // ... elided: Rewritten { files } | Skipped(&'static str)
}

const COMPRESSION_TIER_KEY: &str = "timefusion.compression_tier";
```

`RecompressOutcome::Skipped(&'static str)` carrying its reason as a string is the
same enum-instead-of-bool discipline as `DedupSkipVerdict` — the CLI prints
`SKIPPED — <why>` rather than leaving the operator to guess.

### Writer properties

All four rewrite paths converge on one function:

**Listing 19.19 — src/database/mod.rs:2430–2441**

```rust
    /// Creates writer properties for a Delta write at a compression tier.
    ///
    /// Hot writes use zstd level 3 for fast ingest; cold rewrites use 9/15/19 for storage
    /// savings. The chosen level is stored in footer metadata so re-sweeps can skip
    /// already-target-tier files. Encoding is tuned per column: delta packing for
    /// timestamps/ints, delta byte arrays for sorted strings, dictionary by default for other
    /// strings with per-field opt-out, and bloom filters opt-in for point-lookup columns.
    /// `declare_sorted` is `true` only for paths that sort rows by the schema's sort keys
    /// (flush, dedup); optimize/compact pass `false`.
    fn create_writer_properties(&self, schema: &crate::schema::TableSchema, zstd_level: i32, declare_sorted: bool) -> WriterProperties {
        build_writer_properties(&self.config.parquet, schema, zstd_level, declare_sorted)
    }
```

`declare_sorted` is the parameter that connects this chapter to Chapters 6 and 15.
Only paths that *actually sort* by the schema's sort keys — flush and dedup — pass
`true`; optimize and compact pass `false`. A path that declared sorted output
without sorting would write the lying footer that `Bound::advance` exists to
detect.

The per-column encoding choices trace back to the `FieldDef` flags from Chapter 6:
`dictionary: false` for high-entropy free text, `bloom_filter: true` for
point-lookup columns.

### Staging and committing: waves

Every rewrite path shares one commit mechanism. `stage_*` produces units without
committing; `commit_wave` commits many units against one table.

**Listing 19.20 — src/database/mod.rs:1618–1650**

```rust
pub struct CoalescedWriteUnit {
    // ... elided: project_id, table_name, batches, watermark
}

struct StagedUnit {
    // ... elided: the staged output files and their replace-set
}

const INCONCLUSIVE_COMMIT_MARKER: &str = "landing-unconfirmed";

/// ... elided
const COMMIT_LOCK_OP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(600);

struct CommitFailure {
```

`INCONCLUSIVE_COMMIT_MARKER = "landing-unconfirmed"` is the interesting one: a
commit whose outcome could not be determined (a timeout on the conditional PUT) is
neither success nor failure, and the landed-probe decides afterwards. Recall from
Chapter 7's config that this is *why* the commit-log request timeout can be
bounded tightly — delta-rs's conditional commit PUT is not marked idempotent in
`object_store`, so a timeout is never silently re-sent.

Staged output that was never committed is what `TIMEFUSION_REPAIR_RESUME_ENABLED`
resumes against:

**Listing 19.21 — src/database/mod.rs:5828–5846**

```rust
const STAGED_INTENT_MIN_AGE_SECS: u64 = 30 * 60;

// ... elided: the staged-intent manifest helpers

enum ResumeVerdict {
    // ... elided: Commit | Restage | Discard
}
```

`CLAUDE.md` describes the failure this fixes: a repair pass that staged 40+
minutes of work, timed out, and re-staged from scratch — and, in an earlier
version, `round_robin_bins` discarded every recovered rewrite. The 30-minute
minimum age exists so resume never races a stage still in flight.

### Vacuum

The simplest of the five, exposed over pgwire (Chapter 13):

**Listing 19.22 — src/server/mod.rs:442–448**

```rust
    /// Execute an intercepted `VACUUM <table> [RETAIN <n> HOURS]`.
    async fn run_vacuum(&self, cmd: VacuumCmd) -> PgWireResult<Vec<Response>> {
        let db = require_available(self.db.as_ref(), "VACUUM")?;
        let deleted = db.vacuum_named(&cmd.table, cmd.retention_hours).await.map_err(|e| admin_err(format!("VACUUM '{}': {e}", cmd.table)))?;
        info!("pgwire VACUUM {} retention={:?}: {deleted} files deleted", cmd.table, cmd.retention_hours);
        Ok(vec![Response::Execution(Tag::new(&format!("VACUUM {deleted}")))])
    }
```

Vacuum deletes files no longer referenced by any retained Delta version. The
retention window must exceed the longest-running query, or a scan can have its
files deleted underneath it. `timefusion_vacuum_schedule` defaults to
`0 15 */6 * * *` — every six hours.

### The schedule

All of it is cron-driven, with `croner` parsing and TimeFusion driving the
wall-clock loop itself (Chapter 4):

| Job | Default schedule | Config key |
|---|---|---|
| Hot-tail light optimize | every 5 min | `timefusion_light_optimize_schedule` |
| Dedup sweep | every 5 min | `timefusion_dedup_schedule` |
| Checkpoint | every 2 min | `timefusion_checkpoint_schedule` |
| Optimize | every 30 min | `timefusion_optimize_schedule` |
| Footer repair | hourly at :30 | `timefusion_footer_repair_schedule` |
| Reconcile | hourly | `timefusion_reconcile_schedule` |
| Vacuum | every 6 h at :15 | `timefusion_vacuum_schedule` |
| Consolidate | daily 02:30 | `timefusion_consolidate_schedule` |
| Recompress | daily 03:00 | `timefusion_recompress_schedule` |
| Tantivy reconcile | daily 03:30 | `timefusion_tantivy_reconcile_schedule` |
| Tantivy cache reap | every 10 min | `timefusion_tantivy_cache_reap_schedule` |

Budgets are 80% of the cron period (`DerivedBudget::tick_budget`, Chapter 7), with
one exception: footer repair gets `timefusion_footer_repair_budget_secs = 8640`
(2.4 hours) against an hourly cron, because its unit is a whole-file sort.

⚠️ `CLAUDE.md` records that the 03:30 Tantivy reconcile cron — the *only* drain for
the index backlog — did not fire while the process was alive, and that a no-op
pass logs nothing. A daily cron that silently does not run is indistinguishable
from one that runs and finds nothing.

### How to debug this flow

**File counts first.** Compaction health is a file count against a target size.
`DELTA HISTORY <table> LIMIT n` over pgwire shows recent commits and their
operations without needing object-store credentials; `DELTA ACTIONS <table>
VERSION n` shows exactly which files one commit added and removed.

**`timefusion_stats`** maintenance counters: `pending_<operation>`,
`tasks_running`, `tasks_complete`, `occ_conflicts_total`, `permit_wait_ms`,
`rollup_tier_untagged`.

**`ordering_violations_total`** (Chapter 15) is the footer-damage signal, and
`ordering_violations_by_leg()` attributes it once
`TIMEFUSION_ORDERING_PROBE=true`.

**Log lines**:

- `"dedup rewrite: table=… chunk=[…] dropped=N (before=X after=Y)"` — a dedup
  wave landed.
- `"recompress date=…: rewritten from N file(s) (sorted footer restored)"` /
  `"SKIPPED — <why>"` — the CLI's per-partition outcome.
- `"recompress: no files in partition date=… for table=…"` — at debug level.

**The three ways a pass produces nothing at full cost**, in order of likelihood:

1. **Discarded at the deadline.** The unit did not fit the budget; the next pass
   re-selects it identically. Check `pack_target_bytes` against the observed
   rewrite rate.
2. **Skipped by an eligibility test.** The file is already at target, or the bin
   is a single file, or the tag says converged. Check `converged()` and the
   `SORTED_RUN_TAG` logic — "a right scheduler on a wrong eligibility test looks
   broken".
3. **Staged but not committed.** Check for staged-intent manifest entries and
   whether `TIMEFUSION_REPAIR_RESUME_ENABLED` is on.

**Key takeaways.** Four rewrite jobs, split because their unit sizes differ by
orders of magnitude and a shared budget starves the large one. Every unit is sized
from the wall-clock budget it actually has, at a deliberately conservative
460 kB/s, with margin left for the commit — because a unit discarded at the
deadline produces zero progress at full cost and is re-selected identically next
pass. Convergence thresholds stop 1→1 rewrites; a parallelism ladder distinguishes
"this file cannot be sorted" from "16 unspillable merges cannot share one pool";
and dedup refuses to certify a partition clean unless *every* duplicate-bearing
unit both staged and landed. Recompression exists because bin-packing structurally
cannot fix a single at-target file with a poisoned footer — which was 448 of 501
poisoned partitions.
## 20. The hot tier — and the case for deleting it

*After this chapter you will know what the local hot tier was, the coverage
contract that made it safe to serve reads from, the adaptive suppression that
stopped it wasting writes, why it was measured to be a net loss, and what its
removal teaches about adding a caching layer to a system that already has two.*

> **⚠️ This subsystem is being removed as this book is written.** The book is
> pinned to commit `69bdfcd` (2026-08-21), where `src/hot_tier.rs` is 2,118 lines
> and fully wired. While Part III was being written, a concurrent branch —
> `remove/hot-tier` — deleted `src/hot_tier.rs`, `tests/e2e/hot_tier.rs`, and the
> `pub mod hot_tier;` line from `src/lib.rs`. Every listing below is verbatim from
> the pinned commit, read back via `git show 69bdfcd:src/hot_tier.rs`. If your
> checkout has no `src/hot_tier.rs`, the removal has landed and this chapter is
> history — worth reading anyway, because the *reasoning* is the point.

### What it was

**Listing 20.1 — src/hot_tier.rs:1–9**

```rust
//! Local cache of committed MemBuffer buckets.
//!
//! Files are immutable, uncompressed Arrow IPC written only after Delta commits.
//! DataFusion decodes them at execution time inside the query memory pool. Disk
//! pressure evicts the oldest files; there is no time-based retention.
//!
//! This tier is never a durability boundary. Writes are best effort, and an
//! unreadable file is absent so reads fall through to Delta. GC only removes
//! cache-named `*.arrow` files beneath its own root.
```

A third read leg between MemBuffer and Delta. When a bucket was flushed and its
Delta commit landed, the same batches were *demoted* to an uncompressed Arrow IPC
file on local disk. A recent-window query then read the hot leg instead of
fetching Parquet from object storage.

Three properties made it defensible in principle: it is never a durability
boundary (an unreadable file is simply absent), it holds only *committed* data, and
its GC touches only files it named itself.

The last point is not paranoia. `CLAUDE.md` records that WAL GC once ate the
quarantine directory, which is why `CoreConfig::hot_tier_dir()` carries this
comment (Chapter 7's config listing):

**Listing 20.2 — src/config.rs:926–930**

```rust
    /// Own root for the local hot tier — never share a dir with a generic
    /// recursive deleter (WAL GC once ate the quarantine dir this way).
    pub fn hot_tier_dir(&self) -> PathBuf {
        self.timefusion_data_dir.join("hot_tier")
    }
```

### Disk, not time

**Listing 20.3 — src/hot_tier.rs:33–55**

```rust
const EXT: &str = "arrow";
const ARROW_MAGIC: &[u8; 6] = b"ARROW1";
/// magic + minimal footer + trailer; anything shorter is definitionally torn.
const MIN_FILE_LEN: usize = 6 + 10;
/// The tier's only ceiling — an operator knob
/// (`TIMEFUSION_HOT_TIER_MAX_DISK_GB`) because it bounds the WAL/data volume
/// this box has already been killed by. Query heap needs no knob of its own
/// since `HotTier::scan` streams inside the query's memory pool.
#[derive(Clone, Copy, Debug, educe::Educe)]
#[educe(Default)]
pub struct HotTierLimits {
    /// Directory cap; over it GC unlinks oldest-first.
    #[educe(Default = 68_719_476_736)] // 64 GiB
    pub max_disk_bytes: u64,
    /// Merge-on-demote: a versioned bucket that already has demoted files is
    /// rewritten (old stack + new drain, keep-greatest) into ONE covering file
    /// whose gate is the newest stamp. Kill switch falls back to STACKED files
    // ... elided: the merge-on-demote flag and its rationale
```

The corresponding config field states the design change plainly:

**Listing 20.4 — src/config.rs:953–975**

```rust
    /// Local hot tier: instead of dropping a drained bucket, demote it to an
    /// uncompressed Arrow IPC file on local disk and serve recent-window
    /// reads via zero-copy mmap. This is the tier's main switch; **0 turns
    /// demotion off** (GC still sweeps). Past this age a demoted file is
    /// unlinked and its window falls back to Delta.
    // The local hot tier: demoted sealed buckets served as the scan's third leg.
    //
    // Holds WHATEVER FITS ON DISK — no time retention. Used to keep a fixed
    // number of hours, which made the tier's value depend on guessing the right
    // number and left disk unused. GC now unlinks oldest-first purely to stay
    // under `timefusion_hot_tier_max_disk_gb`, so buying disk buys coverage directly, and
    // `skip_for_lookback` reads the tier's MEASURED span rather than a setting.
    #[serde_inline_default(true)]
    pub timefusion_hot_tier_enabled: bool,
    /// Hard cap on the tier's directory; over it, GC unlinks oldest-first.
    /// The tier shares the WAL/data volume, which has twice been eaten by an
    /// unbounded consumer, so this one is a real dial.
    // Files are UNCOMPRESSED (~4x the bytes of the LZ4 era), so this holds
    // roughly the coverage the old compressed cap did — bought with disk that was
    // sitting idle instead of decompression CPU and anon heap. Raise it to buy
    // more history; that is now the only knob that changes how far back the tier
    // reaches.
    #[serde_inline_default(600)]
    pub timefusion_hot_tier_max_disk_gb: u64,
```

"Holds whatever fits on disk — no time retention" replaced a fixed hours setting,
because a time-based cap made the tier's value depend on guessing a number and
left disk unused. Two doc comments here have gone stale relative to the code they
describe (the "past this age a demoted file is unlinked" sentence, and the
`skip_for_lookback` claim contradicted by Listing 20.6) — Chapter 29 records both.

### The file-name-as-metadata trick

Everything the read side needs about a demoted file is encoded in its *name*:

**Listing 20.5 — src/hot_tier.rs:131–178**

```rust
/// One demoted bucket file. `[min_ts, end_ts)` is the file's ACTUAL row range,
/// not its bucket window, and is HALF-OPEN — the same convention as
/// `MemBuffer::get_bucket_ranges`, so both feed the Delta exclusion unchanged.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HotBucketMeta {
    pub bucket_id: i64,
    pub seq: u64,
    pub min_ts: i64,
    pub end_ts: i64,
    pub bytes: u64,
    /// This file's rows are ordered by the table's declared `sorting_columns`.
    /// Recorded per FILE, in the name (an `s` on the seq component), because
    /// the read side turns it into a `try_with_sort_information` claim: a file
    /// left by an older binary, or one whose batches would not sort, must be
    /// able to say so rather than be assumed ordered. Any unsorted file in a
    /// leg retracts the claim for the whole leg.
    pub sorted: bool,
    /// Greatest `dedup_tiebreak` (version stamp) among the file's rows, on a
    /// `version_append` table. It is what lets the file keep excluding its
    /// window from Delta while merge-on-read keeps appending newer versions
    /// INTO that window: everything at or below this stamp is already in this
    /// file, so only strictly-greater stamps need reading from Delta (see
    /// `version_gate`). `None` = unknown (file demoted before the table opted
    /// in, or by an older binary) — the gate then admits every stamped row,
    /// which is conservative, never wrong.
    pub max_stamp: Option<i64>,
    /// True only when this file holds EVERY row in `[min_ts, end_ts]` at or
    /// below `max_stamp` — the precondition the Delta-scan range exclusion
    /// silently assumed and nothing enforced (prod 2026-08-07: a window read
    /// 4322 rows against a true 11349, because the file claimed its whole span
    /// while holding only the rows one drain happened to carry).
    ///
    /// Set by the writer when a FULL bucket drain produced the file. Absent on
    /// files from an older binary, which is the safe default: no claim, so the
    /// window falls through to Delta. Necessary but NOT sufficient — a bucket
    /// demoted more than once holds its span across several files, and
    /// `plan_leg` grants the window only when EVERY file of the bucket is
    /// served and every one carries this mark (merge-on-demote collapses such
    /// stacks back to one file when it can).
    pub covers_window: bool,
    pub path: PathBuf,
}

impl HotBucketMeta {
    fn range(&self) -> (i64, i64) {
        (self.min_ts, self.end_ts)
    }
}
```

Encoding metadata in the filename means the planner can decide which files to read
by walking a directory — no index, no sidecar, no consistency problem between the
two. And `sorted` recorded *per file* (an `s` on the seq component) means a file
written by an older binary can say "I make no ordering claim" rather than being
assumed ordered — with the leg-level rule that any unsorted file retracts the
claim for the whole leg.

`covers_window` is the field that fixed a real correctness bug, and it is the
clearest statement of the tier's actual contract:

> **A file may only exclude its window from the Delta scan if it holds *every*
> row in that window.**

Production on 2026-08-07 read **4,322 rows against a true 11,349** because a file
claimed its whole span while holding only the rows one drain happened to carry.
The Delta-scan range exclusion had silently assumed a precondition nothing
enforced.

Three properties of the fix are worth copying. Absent on old files means *no
claim*, which falls through to Delta — the safe default. It is *necessary but not
sufficient*: a bucket demoted more than once spreads its span across several
files, so the window is granted only when **every** file of the bucket is served
and every one carries the mark. And merge-on-demote exists to collapse such stacks
back into one file when it can.

`max_stamp` is the merge-on-read counterpart, and it is the `version_gate` that
Chapter 13's Listing 13.26 weakens the Delta exclusion with: everything at or
below this stamp is already in this file, so only strictly-greater stamps need
reading from Delta. `None` admits every stamped row — conservative, never wrong.

### A depth proxy that could deadlock itself

**Listing 20.6 — src/hot_tier.rs:112–130**

```rust
/// Must a scan reaching `lookback` micros into the past skip the hot leg
/// entirely? Only an UNBOUNDED scan must: with no lower time bound the query
/// reaches all of history, of which the tier holds a vanishing fraction, and it
/// would still pay to list every file the tier has.
///
/// This used to also reject anything deeper than 2x the retention window,
/// because the leg was materialized EAGERLY at plan time — a 7d/14d scan pulled
/// the whole tier into un-pooled heap to shave a handful of files off a scan
/// dominated by thousands. `HotTier::scan` streams now (2026-08-14), so a deep
/// scan costs only the metadata walk (footer validation is memoized per file),
/// and every window the tier covers is one that does NOT go to R2. Serving that
/// is the entire point of the tier.
///
/// Keying the gate on the tier's MEASURED span was tried and is strictly worse:
/// the span is ~0 while the tier is young, so a freshly-started process refuses
/// to use its own tier — and the tier can never become useful, because nothing
/// reads it. A depth proxy must not be able to deadlock the thing it guards.
pub fn skip_for_lookback(lookback: Option<i64>) -> bool {
    lookback.is_none()
}
```

**"A depth proxy must not be able to deadlock the thing it guards."** Gating on
the tier's measured span is superficially the *most* accurate heuristic available
— and it is self-defeating, because the span is ~0 on a fresh process, so the
process refuses to read its own tier, so nothing populates the span, so it never
becomes useful. A negative feedback loop in a guard.

The function that survives all that reasoning is one line, and the reasoning is
eighteen. That ratio is characteristic of this codebase and is the right way
round.

Note also `CLAUDE.md`'s record of the earlier eager-materialization bug: the hot
leg "MATERIALIZED where every other leg PLANS", costing 954 ms per scan. Making it
stream is what allowed the depth gate to relax to "unbounded only".

### Adaptive suppression: a cache that notices it is useless

The most interesting idea in the file, and the one that most directly foreshadows
its removal:

**Listing 20.7 — src/hot_tier.rs:56–110**

```rust
/// Demotions a table must accumulate before its first conviction — one unlucky
/// DML must not cost a healthy table its tier.
const PROBE_DEMOTES: u64 = 4;
/// After the first conviction the probe is a single file: the cooldown only
/// buys anything if re-testing is cheap.
const REPROBE_DEMOTES: u64 = 1;
/// Share of a probe's files that must have been invalidated WITHOUT ever being
/// decoded by a query to convict. Not 100%: GC may legitimately have reaped one
/// of them first.
const WASTE_PCT: u64 = 75;
/// How long a convicted table stops demoting. Long relative to the flush
/// interval (so the wasted-write rate drops from one file per flush to one per
/// cooldown), short enough that a table whose enrichment stopped is back inside
/// the hot window.
const SUPPRESSION_COOLDOWN: Duration = Duration::from_secs(30 * 60);
/// Per-table suppression rows exposed in `timefusion_stats`; the count is
/// always exact, only the enumeration is capped.
const MAX_SUPPRESSED_ROWS: usize = 32;

/// Per-table demotion payoff, the input to adaptive suppression.
///
/// A demoted file is an immutable pre-DML snapshot, so any UPDATE/DELETE on the
/// table drops every file it holds. Under a workload that rewrites the whole
/// table continuously (monoscope's enrichment jobs on `otel_logs_and_spans`)
/// that is forever: the tier writes IPC, burns NVMe and page cache, and serves
/// nothing — invisibly, because files/bytes/writes all look healthy. So each
/// table is judged on its own files and stops demoting when they don't pay off.
#[derive(Debug)]
struct DemotionHealth {
    /// Files written in the current probe window...
    demoted: AtomicU64,
    /// ...and how many of them a DML dropped before any query decoded them.
    wasted: AtomicU64,
    /// Micros at which the current suppression lifts; 0 = demoting.
    until: AtomicI64,
    episodes: AtomicU64,
}

impl DemotionHealth {
    /// True = skip this demotion. Lifting the cooldown resets the window, so
    /// the re-probe is judged only on what the table does from here — a table
    /// that stopped being mutated recovers with no restart and no config.
    fn suppressed(&self) -> bool {
        match self.until.load(Relaxed) {
            0 => false,
            until if crate::support::now_micros() < until => true,
            _ => {
                self.until.store(0, Relaxed);
                self.demoted.store(0, Relaxed);
                self.wasted.store(0, Relaxed);
                false
            }
        }
    }
}
```

The failure this detects is stated exactly: **a demoted file is an immutable
pre-DML snapshot, so any `UPDATE`/`DELETE` on the table drops every file it
holds.** Under monoscope's continuous enrichment of `otel_logs_and_spans`, that is
*forever* — the tier writes IPC, burns NVMe and page cache, and serves nothing.

And the crucial adjective: **invisibly**. Files, bytes and writes all look
healthy. The tier is doing exactly what it was built to do, at full cost, for zero
benefit, and every gauge agrees it is working.

The mechanism is a per-table hypothesis test: over a probe window of 4 demotions
(1 after the first conviction), if ≥75% of files were invalidated *without ever
being decoded by a query*, the table stops demoting for 30 minutes. Four
sub-decisions, each justified:

- **4 demotions before the first conviction**, so one unlucky DML does not cost a
  healthy table its tier.
- **1 after**, because a cooldown only buys anything if re-testing is cheap.
- **75%, not 100%**, because GC may legitimately have reaped one file first.
- **30 minutes**, long relative to the flush interval so the wasted-write rate
  drops from one file per flush to one per cooldown, short enough that a table
  whose enrichment stopped is back inside the hot window.

And the recovery is automatic: lifting the cooldown resets the counters, so the
re-probe judges only what the table does from *here*. No restart, no config.

`MAX_SUPPRESSED_ROWS = 32` caps the *enumeration* in `timefusion_stats` while
keeping the count exact — a small but correct distinction for a diagnostic
surface.

### The leg

**Listing 20.8 — src/hot_tier.rs:184–201, 755–780**

```rust
pub struct HotLeg {
    // ... elided: plan and ranges
    pub version_gate: Option<i64>,
}
```

```rust
    pub fn scan(
        // ... elided: (project, table, query range, mem ranges, filters, schema, projection)
                HotLeg { plan: Some(exec), ranges: plan.ranges, version_gate: plan.version_gate }
```

`HotLeg` returns three things to `ProjectRoutingTable::scan` (Chapter 13, Listings
13.25–13.26): the execution plan, the time ranges the tier is authoritative for
(which get excluded from the Delta scan), and the `version_gate` that weakens that
exclusion for merge-on-read tables.

The coverage decision lives in `plan_leg`, and its rule follows directly from
`covers_window`: a bucket's window is granted only when every file of that bucket
is served and every one carries the coverage mark.

### Why it was removed

Chapter 2 called the hot tier "derived data": rebuildable, never authoritative.
That framing is what made removing it a *performance* decision rather than a
correctness one.

The measured verdict, recorded on 2026-08-21 — the same day this book was written
— is blunt: unindexed Arrow IPC made recent-window queries **6× slower** than
reading Delta from object storage, *while starving Foyer of disk*. The tier was
disabled, and a Delta-only control window barely moved.

Both halves matter.

**The tier had no index.** A demoted file is uncompressed Arrow IPC with no
footer statistics, no row-group pruning, no bloom filters, and no
`sorting_columns` ordering claim beyond the per-file `sorted` bit. A Parquet file
in Delta has all of those, plus the Foyer cache in front of it, plus a Tantivy
sidecar. "Local disk beats remote object storage" is true only when the local copy
is at least as *prunable*, and this one was strictly less so. The in-flight plan
`docs/plans/2026-08-21-hot-leg-pruning.md` was an attempt to add per-file blooms
and enum sets to close exactly that gap; the branch that deletes the tier deletes
that plan too.

**It competed with the layer that was working.** The tier and the Foyer L2 disk
cache share one volume. Every gigabyte the tier held was a gigabyte Foyer could
not use for cached Parquet — and Foyer's copies *are* indexed. A cache that
displaces a better cache is worse than no cache.

**The read path already had three ways to avoid object storage**, all cheaper: the
MemBuffer leg (for data not yet flushed), the skip-Delta watermark (Chapter 13,
Listing 13.24 — which proves Delta holds nothing newer and skips the scan
entirely), and Foyer. The tier's window was the narrow band between "still in
memory" and "cached Parquet", and that band turned out not to be worth a third
leg.

### What it cost while it existed

Reading Part III as a whole, the hot tier appears in the machinery of five other
subsystems:

- `LegKind::Hot` and a third set of ordering-violation counters (Ch. 15).
- A third leg in the union, its own sortability rule, and its participation in
  the empty-leg veto bug (Ch. 13).
- `version_gate`, weakening the Delta range exclusion (Ch. 13).
- `demote_permit`, `demote_queued_bytes`, `demote_queue_limit`,
  `demote_skipped`, `DEMOTE_CONCURRENCY`, and `finish_hot_tier_open` /
  `hot_tier_gc` in `BufferedWriteLayer` (Ch. 10, Ch. 12).
- `hot_compact_table_cursor` and `HotPacking`'s naming overlap in the coordinator
  (Ch. 17).

Plus its own 2,118 lines, its e2e test file, and two production incidents
(`covers_window`'s 4,322-of-11,349 under-read, and the count-bounded demote queue
that discarded 89% of the tier's writes — Chapter 10, Listing 10.3).

### The lessons

**A cache is not free because it is derived.** Its cost is the code that has to
know about it: every union, every ordering rule, every exclusion predicate, every
invalidation path. The hot tier's *correctness* machinery — `covers_window`,
`max_stamp`, per-file `sorted` — was the majority of the file, and all of it
existed to let one more leg into a union.

**Measure the layer against the layer it displaces, not against nothing.** "Local
disk is faster than S3" was true and irrelevant. The comparison that mattered was
against cached, indexed Parquet — and against the disk the cache lost.

**A subsystem that can be invisibly useless needs a self-check.** `DemotionHealth`
is the right instinct: judge each table on whether its files are actually read
before they are invalidated. That it had to exist at all was the strongest
available evidence that the tier's value was conditional on workload — and
monoscope's workload was the one it ran under.

**Build the disable switch first.** `timefusion_hot_tier_enabled` and the
merge-on-demote kill switch are what made the 2026-08-21 experiment a
configuration change rather than a deploy, and what made the removal branch a
deletion rather than a migration.

### How to debug this flow (while it exists)

**`timefusion_stats`**: hot-tier files, bytes, writes, `demote_skipped`, and up to
32 rows of per-table suppression state with an exact suppressed count.

**The metric trap**: files, bytes and writes can all look healthy while the tier
serves nothing. The signal that matters is *reads* — whether demoted files are
decoded before a DML invalidates them — which is precisely what `DemotionHealth`
tracks and what the raw counters hide.

**`ordering_violations_by_leg()`** attributes a false ordering claim to `hot`
specifically (Chapter 15) — the per-file `sorted` bit is a claim, and a file whose
batches were not actually sorted lies exactly like a bad Parquet footer.

**`scans_mem_plus_delta` versus hot-leg participation**: a non-empty hot leg
forces the union path, which never grants the read-side dedup skip. So an active
hot tier costs every recent-window query its `DedupExec`.

**On disk**: `<TIMEFUSION_DATA_DIR>/hot_tier/` holds `*.arrow` files whose names
encode bucket id, sequence, sorted bit, timestamp range, stamp and coverage mark.
`du -sh` it against `<TIMEFUSION_DATA_DIR>/cache/` — that ratio is the trade the
removal decided.

**Key takeaways.** The hot tier demoted committed MemBuffer buckets to local Arrow
IPC and served them as a third read leg, under a coverage contract encoded in
filenames: a file may exclude its window from the Delta scan only if it provably
holds every row in that window at or below its version stamp. It carried an
adaptive per-table suppression mechanism because a workload that continuously
rewrites its table invalidates every demoted file before any query reads it — at
full cost and invisibly. It was measured at 6× slower than indexed Parquet through
Foyer, while taking disk from Foyer, and is being deleted. The transferable
lesson is that a derived cache must be measured against the cache it displaces,
and that the machinery required to make an extra read leg *correct* is usually
larger than the leg itself.
## 21. Tantivy: per-file full-text sidecar indexes

*After this chapter you will know how TimeFusion builds one Tantivy index per
Parquet file, how those indexes are packaged, stored, cached, and searched, how a
plain SQL predicate becomes an index query, and — most importantly — the coverage
discipline that keeps an index a pure accelerator that can never change an
answer.*

### The shape

**Listing 21.1 — src/tantivy/mod.rs:1–7**

```rust
//! Per-parquet-file Tantivy index: parallel sidecar indexes that pre-filter
//! `(timestamp, id)` candidates so Delta/MemBuffer scans stay narrow.
//!
//! Layout: one tantivy index per Delta parquet file, scoped per `project_id`.
//! Schema is derived from the YAML `TableSchema` via `schema::build_for_table`.
//! Indexes always store `_timestamp` (i64, fast) and `_id` (text raw); user
//! columns are indexed-only unless explicitly marked `stored: true`.
```

**One index per Parquet file** is the decision everything else follows from. It
means an index is immutable (its file is), it can be built independently, its
coverage is exactly one file, and a missing index degrades exactly one file's
worth of pruning rather than the whole query.

User columns are **indexed-only** — the index holds postings, not values. Only the
two reserved fields are stored, and Chapter 6's `TantivyFieldConfig` comment says
why: `_timestamp` and `_id` are what the reader needs to produce `(timestamp, id)`
prefilter hits for the Delta-side join. The real data lives in Parquet.

### Building an index

Three entry points, and the third one exists because of an OOM:

**Listing 21.2 — src/tantivy/mod.rs:44–102**

```rust
/// Heap reserved per writer and charged against the MemBuffer budget.
pub const WRITER_HEAP_BYTES: usize = 64 * 1024 * 1024;

/// Deferred builds merge past this cap to bound per-query segment cost.
pub const MAX_DEFERRED_SEGMENTS: usize = 32;

/// When a build is allowed to spend CPU on segment merges.
///
/// Merging is logically invisible but expensive during ingestion.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MergeMode {
    // ... elided: the variants
}

// ... elided: IndexBuildStats

/// Build an in-memory tantivy `Index` from `batches`. Returns the index and
/// row-level stats. Caller serializes the index (via `store::pack_index`) to
/// bytes for upload.
pub fn build_in_memory(table: &TableSchema, batches: &[RecordBatch]) -> Result<(Index, BuiltSchema, IndexBuildStats)> {

/// Append `batches` to an existing tantivy `Index` (created in RAM or on disk).
/// Used by `store::build_to_dir` to write directly to a `MmapDirectory`.
pub fn index_to_writer(built: &BuiltSchema, index: &Index, batches: &[RecordBatch], merge: MergeMode) -> Result<IndexBuildStats> {

/// Build a committed-file index from a bounded channel of decoded parquet
/// batches. The reader and Tantivy writer run concurrently, so only the
/// channel's small window remains live; the old committed-file path collected
/// the entire wide parquet into Arrow before indexing and a sub-512 MiB file
/// OOM-killed a 12 GiB repair cgroup in production.
///
/// Must run on a blocking thread: `IndexWriter` is CPU/blocking work and
/// `blocking_recv` intentionally keeps it off Tokio's async workers.
pub fn build_stream_to_dir(
```

The streaming builder's comment is the important one: the old path collected an
entire wide Parquet file into Arrow before indexing, and **a sub-512 MiB file
OOM-killed a 12 GiB cgroup**. A 24× amplification from compressed Parquet to
decoded Arrow is not intuitive until you have measured it — the same
compressed-to-decoded reasoning as Chapter 19's zstd factor.

Two smaller notes. `WRITER_HEAP_BYTES` is *charged against the MemBuffer budget*,
which is the untracked-consumer discipline from Chapter 7 applied to a third-party
library's arena. And `build_stream_to_dir` must run on a blocking thread —
`IndexWriter` is CPU work and `blocking_recv` deliberately keeps it off Tokio's
async workers.

`MergeMode` exists because segment merging is "logically invisible but expensive
during ingestion" — invisible in that it changes no answer, expensive in that it
costs CPU on the flush path.

### Tokenizers

**Listing 21.3 — src/tantivy/mod.rs:450–456**

```rust
/// Tokenizer name we use for n-gram indexing. Combined with `LowerCaser` so
/// `ILIKE` semantics fall out automatically.
pub const NGRAM3_TOKENIZER: &str = "tf_ngram3";
/// Tokenizer name we use for word-level indexing (lowercase + word split +
/// ASCII folding + max-length cap). Same name as tantivy's default so
// ... elided
```

Three tokenizers, matching the YAML's `tantivy.tokenizer` values from Chapter 6:

| Tokenizer | Indexes | Serves |
|---|---|---|
| `raw` | the whole value as one term | `=`, `IN`, prefix `LIKE 'foo%'` |
| `default` | lowercased, word-split, ASCII-folded | word-level match |
| `ngram3` | overlapping trigrams, lowercased | substring `LIKE '%foo%'`, `ILIKE`, literal regex |

Combining n-grams with `LowerCaser` is what makes `ILIKE` semantics "fall out
automatically" — case-insensitivity is a property of the analyzer rather than a
special code path.

Variant columns are rendered to text before indexing, and the rendering must agree
with everything else:

**Listing 21.4 — src/tantivy/mod.rs:242–246**

```rust
/// Render one Variant row to text. `kv=false` → canonical JSON (the same
/// `parquet_variant_json` serializer used everywhere: the wire, the coercion
/// path, and `text_match`'s row-eval), so all three agree byte-for-byte.
```

"So all three agree byte-for-byte" is the requirement. If the index rendered a
Variant differently from what `text_match`'s row evaluation produces, the index and
the fallback would disagree — and Chapter 14's `text_typed` gate exists precisely
because that agreement cannot be guaranteed for a SQL `::text` cast.

### The `text_match` UDF and its superset rule

**Listing 21.5 — src/tantivy/udf.rs:1–4**

```rust
//! `text_match(col, 'query')` — returns BOOLEAN.
//!
//! Case-insensitive AND-token fallback for rows not covered by an index.
//! Its matches must remain a superset of the Tantivy prefilter.
```

Four lines that make the whole subsystem safe. `text_match` has *two*
implementations: a Tantivy index lookup, and a naive row-by-row evaluation for
rows no index covers. The invariant is that the **fallback's matches are a
superset of the index's**.

That direction is the only safe one. If the fallback matched *fewer* rows than the
index, a row the index found could be rejected on re-evaluation and lost. A
superset means the index only ever *narrows* what the fallback would have found,
and the original predicate always re-runs as a backstop.

### Turning SQL into an index query

The classifier that decides what is accelerable:

**Listing 21.6 — src/tantivy/udf.rs:19–44**

```rust
/// Minimum literal length accepted by the trigram index.
// ... elided: the constant
/// Accepts only characters whose analyzer behavior is safe for routing.
pub fn is_tantivy_safe_term_char(c: char) -> bool {

/// Accepts exact terms that pass through the raw query parser unchanged.
pub fn is_eq_term_safe(c: char) -> bool {

/// Decide which Tantivy query form a SQL LIKE pattern maps to.
///
/// `allow_substring=false` (raw/default tokenizer):
///   - `'foo'`     → term `foo`
///   - `'foo%'`    → prefix `foo*`
///   - `'%foo'`, `'%foo%'`, embedded `%` → unsupported (None)
///
/// `allow_substring=true` (ngram3 tokenizer):
///   - `'%foo'` / `'%foo%'` → term `foo` (n-gram match by tantivy)
///   - Embedded `%` between literal chars (e.g. `'a%b'`) → unsupported
///
/// `_` (single-char wildcard) is never accelerable. Returns None.
pub fn classify_like_pattern(pat: &str, escape: Option<char>, allow_substring: bool) -> Option<String> {
```

A complete truth table in a doc comment, with the tokenizer as the axis. `_` is
never accelerable because a single-character wildcard has no term or n-gram
representation. An embedded `%` between literal characters is unsupported even on
n-grams, because `'a%b'` is not a substring query.

The two character predicates guard a subtler hazard: a literal containing a
character the query parser treats specially would be *reinterpreted* rather than
matched. `is_eq_term_safe` accepts only terms that "pass through the raw query
parser unchanged" — so the routing decision is made against what the parser will
actually do, not against what the SQL said.

Regex gets the same treatment:

**Listing 21.7 — src/tantivy/udf.rs:83–88**

```rust
/// Regex metacharacters escaped by Monoscope's literal-query encoder.
// ... elided: the set
/// Decode a `~` / `~*` pattern that is a PLAIN LITERAL SUBSTRING into that
/// substring, or `None` when the pattern uses any regex feature.
```

A `~` regex that a client's query encoder produced by escaping a literal string is
just a substring search wearing a costume. Recognizing that shape — and refusing
anything that uses an actual regex feature — turns a large class of production
queries into n-gram lookups.

### Manifests: what an index covers

**Listing 21.8 — src/tantivy/search.rs:1–10**

```rust
//! Searches in-window sidecar indexes and unions their hits.
//!
//! Disk cache layout (under `cache_root`):
//!   tantivy_cache/{table}/{project_id}/{file_uuid}/  (extracted index dir)
//!
//! Missing blobs are downloaded, unpacked, atomically installed, memory-mapped,
//! and retained in a process LRU. Immutable blob paths need only eviction.
//!
//! [`TantivySearchService::reap_disk_cache`] bounds extracted indexes because
//! object-store GC cannot see them and the cache shares disk with the WAL.
```

The cache-bounding note is the hot-tier lesson again from a different direction:
an extracted index directory is invisible to object-store GC and shares disk with
the WAL, so it needs its own reaper. Two subsystems have now been recorded as
having eaten that volume.

A **manifest** per `(table, project)` records which Parquet files have indexes:

**Listing 21.9 — src/tantivy/search.rs:37–66**

```rust
/// Open (Index, IndexReader) pairs kept hot across queries.
// ... elided: the LRU
/// Stale manifests degrade to full scans, never incorrect rows.
// ... elided: MANIFEST_CACHE_TTL
/// Concurrent per-index download+search tasks within one query.
// ... elided: the concurrency cap

#[derive(Debug, Default)]
pub struct SearchResult {
    // ... elided: hits
    /// Rows covered by queried manifest entries.
    // ... elided: indexed_rows, covered_files, zero_hit_files, row_selections
    pub field_coverage_gap: bool,
}

#[derive(Debug)]
pub struct TantivySearchService {
    // ... elided: object store, cache root
    /// TTL cache of parsed manifests, keyed (table, project). Per-service
    // ... elided
    manifests: DashMap<(String, String), (Instant, Arc<Manifest>)>,
```

**"Stale manifests degrade to full scans, never incorrect rows."** That single
sentence is what licenses a TTL cache over the manifest at all. A stale manifest
can only be wrong in two ways: it lists an index for a file that no longer exists
(the search finds nothing and the file scans raw), or it omits an index that does
exist (the file scans raw). Both cost performance; neither changes an answer.

The search, and where the coverage gap is detected:

**Listing 21.10 — src/tantivy/search.rs:125–212**

```rust
        let m = self.load_manifest_cached(table, project_id).await?;
        // ... elided: window filtering, per-index concurrent search
        // `None` hits = the index lacks a queried field (coverage gap).
        // ... elided
        let mut field_coverage_gap = false;
        // ... elided
                // built before the column was indexed) is a coverage hole the
        // ... elided
                field_coverage_gap = true;
        // ... elided
        Ok(Some(SearchResult { hits: all_hits, indexed_rows, covered_files, zero_hit_files, row_selections, field_coverage_gap }))
```

An index built before a column was added to the schema looks *covered* — its
manifest entry exists and its file is in range — but cannot answer a predicate on
that column. Returning `None` hits rather than zero hits is the distinction that
matters: zero hits would let the caller exclude the file, and Chapter 13's
`decide_prefilter` bails on `field_coverage_gap` for exactly this reason.

The manifest cache:

**Listing 21.11 — src/tantivy/search.rs:247–256**

```rust
    /// TTL-cached manifest read (see `MANIFEST_CACHE_TTL` for the staleness
    /// ... elided
    async fn load_manifest_cached(&self, table: &str, project_id: &str) -> Result<Arc<Manifest>> {
        // ... elided: key construction
        if let Some(m) = self.manifests.get(&key).filter(|e| e.0.elapsed() < MANIFEST_CACHE_TTL).map(|e| e.1.clone()) {
            // ... elided: return the cached Arc
        }
        let m = Arc::new(load_manifest(self.object_store.as_ref(), table, project_id).await?);
        self.manifests.insert(key, (Instant::now(), m.clone()));
```

### The four narrowings, revisited

Chapter 13 listed what a prefilter produces. With the index model in hand, each
one's soundness argument is now visible:

**`id IN (…)`** — sound everywhere, including merge-on-read tables, because `id` is
a dedup key: the list admits or rejects *whole key groups*, so it commutes with
keep-greatest.

**`covered_files`** — splits the Delta scan into an indexed leg (which gets the
id-set) and a raw leg (which keeps the original predicate). This is what turns
index-coverage lag into *bounded raw debt* instead of letting one missing sidecar
poison the whole query window.

**`exclude_files`** — a zero-hit covering index proves its own file holds no
matches. Never applied on a merge-on-read table, because a "hitless" file may hold
the newest version of a key whose match lives only in an older version.

**`row_selections`** — per-file matching row ordinals become a
`ParquetAccessPlan`, so the reader decodes only matching rows. Requires the index
to have been built in Parquet row order, and is subject to the same merge-on-read
restriction.

Both of the narrowings that are *unsound* under merge-on-read operate below the
`DedupExec`; the two that are sound operate at whole-key granularity or above it.
That is the same commutativity rule as Chapter 6's `mutable` flag, arrived at from
a different direction.

### Coverage lifecycle

An index's life:

```
flush commits a Parquet file
  └─ TantivyIndexService callback builds an index for it (bounded concurrency)
       └─ pack_index → tar + zstd → object storage
            └─ manifest upsert: (file, index blob, indexed row count, fields)

compaction rewrites files
  └─ post-optimize reindex + GC: new files indexed, dead manifest entries removed

startup
  ├─ spawn_tantivy_backfill   — index live files no manifest covers (off by default)
  ├─ spawn_tantivy_prefetch   — warm the local cache with recent blobs
  └─ spawn_deferred_tantivy_reindex — index files written by replay relief flushes

daily 03:30 cron
  └─ tantivy_reconcile_table — index uncovered files, GC dead entries and blobs
```

Two ordering constraints appear in `main.rs`. The deferred reindex runs only
*after* pgwire is serving and WAL replay has returned (`src/main.rs:632–634`),
because indexing replay-relief files earlier would publish a partial replayed
state. And the CLI attaches the index service explicitly before compacting
(`src/main.rs:867–878`) because without it "the post-optimize reindex/GC hooks
silently no-op: every CLI compaction orphans the rewritten files' index entries
and leaves its outputs unindexed until a server backfill".

### The gates

Every stage is bounded, and Chapter 25 lists the full configuration. The ones that
change behaviour most:

| Knob | Default | Effect |
|---|---|---|
| `timefusion_tantivy_prefilter_max_hits` | 100,000 | above this the `IN`-list is the bottleneck; skip |
| `timefusion_tantivy_prefilter_min_selectivity_pct` | 50 | a hit set covering ≥50% of indexed rows is not worth the round trip |
| `timefusion_tantivy_min_files_for_pushdown` | 2 | not worth it for a single file |
| `timefusion_tantivy_route_equality` | true | route exact `=` on raw columns, not just `LIKE` |
| `timefusion_tantivy_file_pruning` | true | exclude zero-hit files |
| `timefusion_tantivy_row_selection` | true | per-file row ordinals |
| `timefusion_tantivy_build_concurrency` | 2 | safe alongside prod query load |
| `timefusion_tantivy_backfill` | false | reads every uncovered file back from S3 |
| `timefusion_tantivy_cache_disk_gb` | 64 | measured working set is ~65 GB across ~6,500 leaf dirs |
| `timefusion_tantivy_compression_level` | 3 | level 19 cost ~88% of a CPU window per flush for 10–15% smaller output |

Two of those comments are worth repeating. The cache size is "sized against a
working set, not a wish": the reaper only evicts what no query has opened
recently, and every eviction re-downloads a blob on the next hit, so 4 GB would
thrash the hot window at production scale. And the compression level is on the
*flush hot path*, where level 19 bought 10–15% smaller blobs for 88% of a CPU
window.

### The payments span, indexed

Our row lands in a Parquet file under
`project_id=prod-api-001/date=2025-01-17/`. Post-commit, one Tantivy index is
built for that file containing:

- `_timestamp` = 1737123900000000 (i64, fast field)
- `_id` = `550e8400-…` (raw term)
- `id`, `parent_id`, `kind`, `status_code`, `level` — raw terms
- `name` = `POST /api/v1/payments` — trigrams
- `status_message`, `body` — trigrams; `body` flattened as canonical JSON

A later query `WHERE name LIKE '%payments%'` becomes an n-gram term query for
`payments`; `WHERE id = '550e8400-…'` becomes a raw term query (because
`route_equality` is on); `WHERE context___trace_id = 'abc'` becomes a raw term
query on a bloom-filtered, Tantivy-indexed column — which, combined with the
`mutable`-column rule from Chapter 6, is the difference between a point lookup and
the 2026-08-20 OOM.

### How to debug this flow

**Prefilter skip reasons** (debug log, Chapter 13): `empty_index`,
`low_selectivity`, `field_coverage_gap`, `delta_no_index_or_cap_exceeded`,
`delta_error`.

**Counters**: `record_tantivy_prefilter_attempt` / `_used` / `_skipped` /
`_error`. The `used / attempt` ratio is the health metric; a collapse usually
means coverage regressed, not that the rewriter stopped firing.

**Coverage.** ⚠️ `CLAUDE.md` records a live divergence: uncovered files grew at
~85 per hour, and the only backlog drain is the 03:30 reconcile cron — which did
not fire while the process was alive, and **a no-op pass logs nothing**. Two
lessons: a daily cron is not a drain if the process restarts daily, and a
maintenance pass should log that it ran even when it found nothing, or "did not
run" and "found nothing" are indistinguishable.

**Disk.** `<TIMEFUSION_DATA_DIR>/tantivy_cache/{table}/{project}/{file_uuid}/`
against `timefusion_tantivy_cache_disk_gb`. The reaper is the only thing that
deletes from that tree, and it shares a volume with the WAL — a full volume also
fails WAL appends.

**When results look wrong**, check in this order: is the predicate on a *mutable*
column (then file pruning and row selection must be off)? Is there a
`field_coverage_gap` (an index built before the column existed)? Does the Variant
text rendering match `text_match`'s row evaluation? Is the fallback still a
superset of the index?

**Key takeaways.** One immutable Tantivy index per Parquet file, per project,
listed in a per-`(table, project)` manifest, packaged as tar+zstd in object
storage and cached as extracted mmap directories under a self-reaped local budget.
Three tokenizers map SQL `=`, `IN`, `LIKE`, `ILIKE`, and literal regex onto term,
prefix, and n-gram queries, with explicit refusal for anything the analyzer would
reinterpret. The whole subsystem is a pure accelerator: a stale manifest degrades
to a full scan, a missing index degrades one file, a coverage gap is detected and
declines the narrowing, and the row-level fallback is required to be a *superset*
of what the index finds — so no index state can ever change an answer.
## 22. Shutdown and the deploy handoff

*After this chapter you will know the four ways TimeFusion can be asked to stop,
the exact order of the drain, how one stop-grace budget is split across serial
phases, why the process calls `exit()` instead of returning, and how a
start-first deploy hands the WAL lock from one process to its replacement.*

### Four shutdown triggers

**Listing 22.1 — src/main.rs:636–705**

```rust
    // Catch SIGTERM (k8s rolling restart) in addition to SIGINT (Ctrl-C).
    // Without SIGTERM handling, k8s sends SIGKILL after the grace period
    // and in-flight writes are dropped.
    let term_signal = async {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            let mut sigterm = signal(SignalKind::terminate()).expect("install SIGTERM handler");
            sigterm.recv().await;
        }
        #[cfg(not(unix))]
        {
            std::future::pending::<()>().await;
        }
    };

    // In a start-first rollout the replacement binds its isolated listener,
    // then blocks on the shared WAL flock and writes a takeover request. A
    // successful HANDOFF has already fenced writes and drained every hold, so
    // the predecessor can exit at that exact moment while continuing to serve
    // reads until the replacement actually exists. Without HANDOFF readiness,
    // requests are ignored and SIGTERM remains the only shutdown authority.
    let takeover_signal = async {
        loop {
            tokio::time::sleep(Duration::from_millis(25)).await;
            let wal_dir = cfg.core.wal_dir();
            if !timefusion::write::wal::takeover_requested(&wal_dir) {
                continue;
            }
            if buffered_layer.is_deploy_handoff_ready() {
                break;
            }
            // Escalation. Handoff readiness is the FAST path, not the only one:
            // an instance the orchestrator has lost track of is never sent
            // SIGTERM, so if readiness is the sole authority it holds the WAL
            // lock forever and every replacement starves behind it — measured
            // 2026-08-10 at 47 minutes with six live containers stacking up on
            // one box. A request nobody has satisfied for this long means the
            // predecessor is that instance, so take the ordinary graceful path
            // anyway; it fences writes and flushes exactly like SIGTERM does.
            if timefusion::write::wal::takeover_request_age(&wal_dir).is_some_and(|age| age >= timefusion::write::wal::TAKEOVER_ESCALATE_AFTER) {
                warn!(
                    "WAL takeover requested {}s ago and this instance never reached handoff readiness; shutting down anyway so the replacement can start",
                    timefusion::write::wal::TAKEOVER_ESCALATE_AFTER.as_secs()
                );
                break;
            }
        }
    };

    // Wait for shutdown signal. Borrow `pg_task` so we can still await it
    // in the drain phase below — the select! only watches it for early
    // failure, not for ownership.
    tokio::select! {
        res = &mut pg_task => {
            match res {
                Ok(()) => error!("PGWire server task ended unexpectedly"),
                Err(e) => error!("PGWire server task panicked: {}", e),
            }
        },
        _ = tokio::signal::ctrl_c() => {
            info!("Received SIGINT, initiating graceful shutdown");
        }
        _ = term_signal => {
            info!("Received SIGTERM, initiating graceful shutdown");
        }
        _ = takeover_signal => {
            info!("Start-first replacement requested drained WAL ownership; initiating graceful handoff");
        }
    }
```

Four arms: the pgwire task ending unexpectedly (an error, not a normal shutdown),
SIGINT, SIGTERM, and the takeover signal. All four converge on the same drain.

The `&mut pg_task` borrow is deliberate — the `select!` watches it for early
failure without taking ownership, so the drain phase below can still `await` it.

**The escalation is the interesting part.** Handoff readiness is the *fast* path,
not the only one. An instance the orchestrator has lost track of is never sent
SIGTERM, so if readiness were the sole authority it would hold the WAL lock
forever and every replacement would starve behind it — measured at **47 minutes
with six live containers stacked on one box**. After `TAKEOVER_ESCALATE_AFTER`
(180 s, Chapter 11), the predecessor takes the ordinary graceful path anyway, which
is lossless: "a measured `docker stop` of a wedged instance drained in 23s".

Two failure modes, two mechanisms, and neither can wedge indefinitely: the
*contender* gives up after 900 s and crash-loops (Chapter 11), and the *holder*
escalates after 180 s and exits.

### The drain, in order

**Listing 22.2 — src/main.rs:707–748**

```rust
    // Fence writes immediately. datafusion-postgres stops its accept loop but
    // does not join per-connection tasks; without this barrier an already-
    // accepted INSERT could append after the final flush/snapshot, forcing the
    // replacement back onto dirty recovery (or making a clean claim stale).
    buffered_layer.stop_accepting_writes();
    let preflushed_handoff = buffered_layer.is_drained();

    // Stop maintenance first: an in-flight light-optimize/dedup sweep must bail
    // before the buffered-layer flush, not compete with it and then outlive the
    // Foyer cache (a running sweep hitting a closed cache previously hung
    // shutdown until the orchestrator SIGKILLed us after the stop grace).
    db.cancel_maintenance();

    // Drain order matters:
    // 0. Stop PGWire from accepting new connections. Without this, the
    //    BufferedWriteLayer flush below races fresh inserts that pile back
    //    into MemBuffer + WAL, defeating the whole point of a graceful
    //    shutdown.
    // 1. Flush and checkpoint the fenced buffered layer.
    // 2. Shut down database (cache, foyer, log store).
    // One shutdown budget shared by all serial phases (TIMEFUSION_STOP_GRACE_SECS,
    // sized to fit the orchestrator's SIGTERM→SIGKILL grace). The drain phases
    // get small caps so a hung connection can't starve the buffer flush +
    // cursor snapshot — the phase that determines next-boot cost; their unused
    // slack flows forward automatically because the buffered layer works off
    // the same absolute deadline.
    let configured_grace = cfg.buffer.stop_grace();
    // Only a layer that is STILL drained after the admission fence can use the
    // constant-time handoff. At production ingest rates tens of thousands of
    // rows can arrive during an online FLUSH, so a recent FLUSH marker alone is
    // not evidence that replay is small. A post-FLUSH tail gets the normal
    // correctness-first budget and is flushed after the fence.
    let grace = if preflushed_handoff { configured_grace.min(Duration::from_secs(1)) } else { configured_grace };
    let deadline = tokio::time::Instant::now() + grace;
    pgwire_shutdown.cancel();
    lag_shutdown.cancel();
    let pg_drain_budget = if preflushed_handoff { Duration::from_millis(50) } else { grace.mul_f32(0.2) };
    match tokio::time::timeout(pg_drain_budget, pg_task).await {
        Ok(Ok(())) => info!("PGWire drained cleanly"),
        Ok(Err(e)) => error!("PGWire task panicked during drain: {}", e),
        Err(_) => warn!("PGWire drain exceeded its slice of the stop grace — proceeding; in-flight queries may be reset"),
    }
```

The order and each step's reason:

1. **`stop_accepting_writes()` first.** `datafusion-postgres` stops its accept
   loop but does *not* join per-connection tasks, so an already-accepted `INSERT`
   could append after the final flush and snapshot — forcing the replacement back
   onto dirty recovery, or making a clean claim stale.
2. **Cancel maintenance second.** An in-flight sweep must bail before the flush
   rather than compete with it and then outlive the Foyer cache — a running sweep
   hitting a *closed* cache previously hung shutdown until the orchestrator
   SIGKILLed the process.
3. **Drain pgwire**, bounded by 20% of the grace.
4. **Flush the buffered layer** against the absolute deadline.
5. **Shut down the database** against the same deadline.

**One absolute deadline shared by all serial phases** is the key structural
choice. Each phase gets a *cap* rather than a slice, so a phase that finishes
early passes its unused slack forward automatically — and the phase that matters
most (the buffer flush plus cursor snapshot, which determines next-boot cost)
cannot be starved by a hung connection.

### The constant-time handoff

`preflushed_handoff` shortens the whole grace from the configured value to one
second, and the 50 ms pgwire drain from 20% of it. The precondition is stated
carefully:

> Only a layer that is STILL drained *after* the admission fence can use the
> constant-time handoff. At production ingest rates tens of thousands of rows can
> arrive during an online `FLUSH`, so a recent `FLUSH` marker alone is not
> evidence that replay is small.

That is the difference between "we flushed recently" and "there is nothing left".
The check runs after `stop_accepting_writes()`, so no further rows can arrive
between the check and the flush.

The operational workflow this enables:

```sql
FLUSH;      -- drain MemBuffer to Delta while still serving (rate-limited to 10s)
HANDOFF;    -- fence writes, drain the finite tail, stay up serving reads
```

then the orchestrator starts the replacement, which blocks on the WAL lock and
writes a takeover request; the predecessor sees `is_deploy_handoff_ready()` and
exits within 25 ms.

**Listing 22.3 — src/server/mod.rs:492–501**

```rust
    /// Execute `HANDOFF`: lease a write-admission fence, drain the finite tail,
    /// and keep serving reads until the orchestrator replaces this task.
    async fn run_handoff(&self) -> PgWireResult<Vec<Response>> {
        let layer = require_available(self.db.as_ref().and_then(|d| d.buffered_layer()), "HANDOFF")?;
        let stats = layer.prepare_deploy_handoff().await.map_err(|e| admin_err(format!("HANDOFF: {e}")))?;
        if !crate::support::is_frozen() {
            layer.reclaim_wal_after_flush().await;
        }
        Ok(vec![Response::Execution(Tag::new(&format!("HANDOFF {}", stats.total_rows)))])
    }
```

`HANDOFF` is a *lease*, not a permanent fence — the `handoff_generation` counter
(Chapter 10, Listing 10.3) invalidates it, and shutdown increments it again "so an
old lease timer can never reopen admission after SIGTERM".

The readiness predicate requires all four conditions:

**Listing 22.4 — src/write/mod.rs:693–700**

```rust
    /// Whether a start-first replacement may safely trigger this process's
    /// graceful exit while reads are still being served.
    pub fn is_deploy_handoff_ready(&self) -> bool {
        self.deploy_handoff_ready.load(Ordering::Acquire)
            && !self.accepting_writes.load(Ordering::Acquire)
            && self.active_writes.load(Ordering::Acquire) == 0
            && self.is_drained()
    }
```

A `HANDOFF` ran, admission is closed, no writer is in flight, and nothing is
un-flushed. Any one of those failing means the replacement must wait or the
predecessor must escalate.

### The final phases, and not flushing telemetry

**Listing 22.5 — src/main.rs:750–769**

```rust
    if let Err(e) = buffered_layer.shutdown_by(deadline).await {
        error!("Error during buffered layer shutdown: {}", e);
    }
    // Share the same absolute `deadline` as the buffered-layer flush above so
    // the whole serial shutdown fits one stop-grace budget — every phase that
    // can block on a slow Delta/S3 backend (DML drain, foyer `close()`) is
    // bounded by it, so process exit and `wal.lock` release stay inside the
    // orchestrator's SIGTERM→SIGKILL window (issue #82).
    if let Err(e) = db.shutdown_by(deadline).await {
        error!("Error during database shutdown: {}", e);
    }

    info!("Shutdown complete.");
    // Do not synchronously flush OTLP here. Its exporter has a 10-second
    // network timeout, and `_wal_dir_lock` must remain held until this future
    // returns so no detached runtime work can overlap the replacement's WAL
    // access. Losing the final telemetry batch is preferable to extending a
    // planned database outage; normal batches are exported continuously.

    Ok(())
}
```

The OTLP note is the sharpest trade in the file. Flushing telemetry on exit is the
obvious, well-intentioned thing to do — and here it would hold the WAL lock for up
to ten more seconds on a network timeout, extending a *planned database outage* to
save one batch of metrics that are exported continuously anyway.

The general principle: **anything that can block on the network must not sit
between "we are done" and "we release the resource the replacement needs."**

Errors in both phases are logged, not propagated. There is nothing useful to do
with a shutdown error except record it and keep exiting.

### Why `exit()` and not `return`

**Listing 22.6 — src/main.rs:100–113**

```rust
        _ => {
            let result = rt.block_on(async_main(cfg));
            // Must END THE PROCESS here: dropping the runtime waits on
            // lingering blocking/detached threads, and that hang left a
            // zombie container blocking swarm's replacement (2026-08-06
            // pgwire outage). Everything durable is already on disk.
            match result {
                Ok(()) => std::process::exit(0),
                Err(e) => {
                    eprintln!("fatal: {e:#}");
                    std::process::exit(1)
                }
            }
        }
```

Dropping a Tokio runtime waits on lingering blocking and detached threads.
TimeFusion has several — Foyer's dedicated runtime (Chapter 8), detached Tantivy
uploads (whose handles are deliberately not stored), the `spawn_blocking` index
writers. Waiting for all of them produced a zombie container that blocked Swarm's
replacement during the 2026-08-06 pgwire outage.

The justification for the abruptness is the important half: **everything durable
is already on disk.** The WAL is fsynced, the cursor snapshot is written, the
Delta commits have landed. There is nothing left for those threads to protect, so
`exit()` is not a shortcut — it is correct.

The consequence, acknowledged in `BufferedWriteLayer`'s field docs
(Chapter 10, Listing 10.3): in-flight Tantivy uploads are not awaited. Acceptable
because the sidecar is best-effort and rebuildable from Delta on demand.

### What the next boot inherits

The shutdown's whole purpose is to shape the next startup:

| Shutdown outcome | Snapshot | Next boot |
|---|---|---|
| Graceful, everything flushed | `clean_shutdown=true`, `drained=true` | skip Delta scan; empty replay; WAL GC sweeps |
| Graceful, partial flush (deadline hit) | `clean_shutdown=true`, `drained=false` | skip Delta scan; replay the tail; **no** sweep |
| SIGKILL after grace | stale or absent snapshot | `can_skip_delta_reconcile()` may still save it |
| Crash | absent | full Delta cursor reconciliation (~6.5 min) + replay |

Chapter 11's `drained` flag is what authorizes the boot WAL GC sweep, and it is
*consumed* on use — so a crash after a sweep cannot let the next boot sweep on
stale evidence.

The `local_wal_consumed` path (Chapter 9) is what rescues the common deploy
failure: pre-deploy `FLUSH` drained successfully, but the container was killed
before it could write `clean_shutdown=true`. If every durable cursor sits at its
exact local WAL tail and no rewind marker exists, the expensive scan is skipped on
local evidence alone.

### The three deploy shapes

**Stop-first.** SIGTERM → drain → exit → replacement starts. Simple, and the
outage is the whole drain plus the whole boot. Fine when `FLUSH` ran first.

**Start-first.** The replacement binds its own listener, blocks on the WAL flock,
and writes a takeover request. The predecessor keeps serving *reads* until it sees
the request, then exits. The read outage is the boot gap only. This is what the
early-bind responder (Chapter 9) and the takeover protocol (Chapter 11) exist for.

**Start-first with `HANDOFF`.** The operator runs `FLUSH` then `HANDOFF` before
triggering the deploy. The predecessor is already fenced and drained, so it exits
within 25 ms of seeing the request, and the replacement's boot skips both the
Delta scan and the replay. This is the constant-time path.

The failure mode all three guard against is the one Chapter 9 opens with: **two
live processes on one WAL directory fork it.** The newer one recovers only the
prefix present at its start and orphans the older's concurrent appends — silent
loss on an overlapping redeploy.

### How to debug this flow

**Log sequence for a clean shutdown:**

```
Received SIGTERM, initiating graceful shutdown
PGWire drained cleanly
Shutdown complete.
```

or, for a handoff:

```
Start-first replacement requested drained WAL ownership; initiating graceful handoff
PGWire drained cleanly
Shutdown complete.
```

**Lines that mean the grace was too short:**

- `"PGWire drain exceeded its slice of the stop grace — proceeding; in-flight
  queries may be reset"` — usually benign.
- `"Error during buffered layer shutdown: …"` — the flush did not finish;
  next boot replays more.
- No `"Shutdown complete."` at all — SIGKILL arrived first. Check
  `TIMEFUSION_STOP_GRACE_SECS` against the orchestrator's grace.

**Lines that mean a deploy is wedged:**

- `"WAL dir … is locked by another TimeFusion process"` repeating past 60 s at
  `error!` level — the predecessor is not exiting.
- `"WAL takeover requested 180s ago and this instance never reached handoff
  readiness; shutting down anyway"` — the escalation fired; the predecessor was
  the orphan.

**`timefusion_stats`** before a planned restart: `mem_total_rows` and
`pressure_pct` say how much a shutdown flush will have to move; `orphaned_topics`
says whether a previous failed commit is pinning WAL files.

**The pre-deploy checklist**, in order: `FLUSH;` (wait for the tag), confirm
`mem_total_rows` is near zero, `HANDOFF;`, then trigger the deploy.

**Key takeaways.** Four triggers converge on one drain whose order is
fence-writes → cancel-maintenance → drain-pgwire → flush → close, all against a
*single absolute deadline* so unused slack flows to the phase that determines
next-boot cost. A pre-flushed, still-drained layer collapses the whole grace to one
second. The process calls `exit()` rather than returning, because dropping the
runtime waits on detached threads and once produced a zombie container — and it is
safe precisely because everything durable is already on disk. The WAL lock is
released last and nothing that can block on the network is allowed to sit in front
of it, which is why the final telemetry batch is deliberately dropped.
## 23. The CLI subcommands

*After this chapter you will know all eight subcommands the `timefusion` binary
exposes, what each is for, which are safe to run against production, and — in the
most detail — how `sim` replays a real production journal through the real
scheduler on virtual time.*

### The eight subcommands

`main.rs` dispatches on `argv[1]` (Chapter 9, Listing 9.2):

| Subcommand | Runtime | Purpose |
|---|---|---|
| *(none)* | full server | the database |
| `healthcheck` | none | Docker liveness probe (Chapter 9) |
| `encrypt-secret` | none | encrypt a config secret |
| `sim` | none | replay a maintenance journal on virtual time |
| `optimize` | maintenance-cli budget | off-box compaction / consolidation / dedup / recompress |
| `redrive-dml` | maintenance-cli budget | replay parked DML quarantine groups |
| `migrate-columns` | maintenance-cli budget | widen a live table's stored Delta schema |
| `run-unit` | server budget | execute ONE maintenance unit with phase timings |

Three of them get a different memory budget shape, and one is deliberately
excluded from that list (Chapter 9, Listing 9.2): `run-unit` drives a coordinator
unit whose pool comes from `coordinator_share_bytes()`, which is a hard **0** under
`MaintenanceCli` — so every invocation died at `pool_size: 0.0 B` until it was
removed.

### `sim` — the fastest way to evaluate a scheduling change

Deploying a maintenance change to production costs about half a day per hypothesis
(`CLAUDE.md`): the restart kills in-flight units and resets the rollup coverage
map, then needs ~2 hours of quiet before any throughput number means anything. One
night produced ~25 deploys and mostly invalid measurements. `sim` exists to
collapse that loop.

**Listing 23.1 — src/maintenance_sim.rs:1–6**

```rust
//! Replays a production `TaskJournal` through the real scheduler on virtual
//! time, using measured duration distributions.
//!
//! Task selection, timeout handling, cycle switching, and invalidation are
//! real. Durations and ingest cadence are modeled. Byte-driven splits, memory
//! admission, and intra-call operation order are outside the model.
```

Six lines that draw the model's boundary precisely: **what is real** (task
selection, timeout handling, cycle switching, invalidation — i.e. every scheduling
decision), **what is modeled** (durations, ingest cadence), and **what is outside
the model entirely** (byte-driven splits, memory admission, intra-call operation
order).

A simulator without that third list is a trap. Stating it up front is what lets a
reader know when the sim's answer is not evidence.

The durations are measured, not invented:

**Listing 23.2 — src/maintenance_sim.rs:79–110**

```rust
/// Measured duration ranges in seconds, per operation and width class.
/// Sources: rollup phase timing (#174, prod 2026-08-18): 174 rollup starts,
/// NOT ONE over 60s; the rollup counters put e2e at ~3s/unit. Debt units are
/// the slow ones (#176): HotPacking 320-895s, SealedConsolidation 294-767s.
/// Dedup is BIMODAL (#175/#177): quick, or past its 300s deadline — nothing
/// finished between 75s and 300s in the measured window.
// ... elided: the per-operation duration table

/// Debt work = file rewrites that cannot advance rollup coverage
/// (`dependencies_complete`): the operations #176's occupancy cap applies to.
```

Every range cites the issue and the production date it came from. And the *shape*
of a distribution is captured, not just its mean: **dedup is bimodal** — quick, or
past its 300 s deadline, with nothing finishing between 75 s and 300 s. A model
that averaged those two modes would predict a middle behaviour that never happens.

The rollup number is the one that reframes the problem: 174 rollup starts, not one
over 60 s, at ~3 s end-to-end per unit. Rollup units are *cheap*; the debt units
(320–895 s) are what occupy workers. That is the measurement behind the occupancy
cap and `maintenance_debt_slots` (Chapter 17).

Determinism is a design requirement:

**Listing 23.3 — src/maintenance_sim.rs:111–127**

```rust
/// Deterministic SplitMix64 generator for simulation.
struct Rng(u64);

impl Rng {
    // ... elided: SplitMix64's next()
}
```

A seeded SplitMix64 rather than `fastrand` or the system RNG, so `--seed` makes a
run byte-reproducible. Comparing two policies is only meaningful if the same
random draws are made in both.

The workload model:

**Listing 23.4 — src/maintenance_sim.rs:128–213**

```rust
/// One ingest stream: the tables a project invalidates per flush.
#[derive(Clone, Debug)]
struct Stream {
    // ... elided
}

/// Contiguity model mirroring `min_contiguous_days`: a day counts once its
/// base tier AND its derived tier each cover a full day's width; contiguity
/// counts back from yesterday; the gauge is the MIN over active (source,
/// project) pairs — including pairs with NO completed coverage, which count 0.
struct Coverage {
    // ... elided
}

// ... elided: Coverage's impl

/// One ingest flush (or one restart reconciliation) for one stream: the base
/// invalidation mints Dedup + BaseRollup over the range's 10-minute slices;
/// the derived invalidation mints the hour-aligned DerivedRollup units.
/// `observed_at` sets the finalization deadline — the flush time for minting,
/// the boot time for a restart reconcile.
```

The `Coverage` comment reproduces the production gauge's definition exactly,
**including the part that once produced a false reading**: pairs with *no*
completed coverage count 0. `CLAUDE.md` records the incident — `min_contiguous_days = 0`
was one sparse tenant dragging the minimum down, and the real backfill was 375
units rather than the 17,430 the gauge implied. A sim that modeled the metric
without that quirk would not reproduce the artifact.

Restarts are modeled too (`--restarts-every-hours`, `--restart-at-hours`), because
a restart reconciliation mints work — and under deploy churn that was the queue's
dominant growth source (Chapter 18, Listing 18.5).

### Running it

**Listing 23.5 — src/main.rs:213–222**

```rust
/// `timefusion sim <journal.json | data-dir> [--hours N] [--workers N]
/// [--streams N] [--scale F] [--seed N] [--no-mint] [--json]`
///
/// Replay a copied-out prod maintenance journal through the real scheduler on
/// virtual time (`timefusion::maintenance_sim`). The answer to "does this
/// policy keep up" without a deploy. Fetch the input with e.g.
/// `ssh ubuntu@captain.s.past3.tech 'docker cp <container>:/data/.timefusion_meta/maintenance_tasks.json -'`.
fn run_sim_cli() -> anyhow::Result<()> {
```

The doc comment includes the command to fetch the input. Small thing, large
effect: the tool is unusable without a real journal, and the reader now has one.

`main.rs` also states the constraint that keeps `sim` useful (Listing 9.2): it
"must stay config/bucket-free, that's what lets it answer scheduler questions
without a deploy". It runs before `init_config`, touches no object storage, and
needs no credentials.

The report:

**Listing 23.6 — src/main.rs:254–281**

```rust
    println!(
        "sim: {:.1}h virtual | {} workers | scale {:.2} | {} streams | seed {:#x}",
        report.hours,
        cfg.workers,
        cfg.duration_scale,
        cfg.streams.map_or("journal".to_owned(), |n| n.to_string()),
        cfg.seed
    );
    println!("pending: {} -> {} | executions: {} | splits: {}", report.pending_start, report.pending_end, report.executions, report.splits);
    let mut completions = report.completions.iter().collect::<Vec<_>>();
    completions.sort();
    println!("completions: {}", completions.iter().map(|(op, n)| format!("{op}={n}")).collect::<Vec<_>>().join(" "));
    if !report.timeouts.is_empty() {
        let mut timeouts = report.timeouts.iter().collect::<Vec<_>>();
        timeouts.sort();
        println!("timeouts:    {}", timeouts.iter().map(|(op, n)| format!("{op}={n}")).collect::<Vec<_>>().join(" "));
    }
    println!("frontier lag max: {}s", report.frontier_lag_secs_max);
    println!(
        "min contiguous days at end: {} (14d at {}, 30d at {})",
        report.min_contiguous_days_end,
        report.hours_to_contiguous_14.map_or("never".to_owned(), |h| format!("{h:.1}h")),
        report.hours_to_contiguous_30.map_or("never".to_owned(), |h| format!("{h:.1}h"))
    );
    for sample in &report.samples {
        println!("  h={:5.1} pending={:>7} lag={:>6}s contiguous={}", sample.hour, sample.pending, sample.frontier_lag_secs, sample.min_contiguous_days);
    }
    Ok(())
}
```

The header line echoes every parameter, so a pasted result is self-describing.
`hours_to_contiguous_14` and `_30` print **`never`** rather than a sentinel number
— which is the answer that actually matters: does this policy ever reach 30-day
coverage, or does it asymptote below it? And the periodic samples let you see
whether pending is draining or oscillating rather than just where it ended.

`--json` emits the whole report for scripted comparison of policy variants.

The workflow `CLAUDE.md` prescribes is a ladder, fastest first:

1. **`sim`** — divergence, lag and policy questions in seconds. Backtest against a
   known night's queue shape before trusting a prediction.
2. **`run-unit`** — one unit against real storage with phase timers.
3. **Staging** — real S3 latency, because per-unit cost is object-store round
   trips and MinIO validates correctness, not cost.
4. **Production, only after 1–3 agree** — one change per deploy, behind an env
   kill switch, ≥2 h quiet before trusting numbers.

### `run-unit` — where one unit's time goes

**Listing 23.7 — src/main.rs:284–292**

```rust
/// `timefusion run-unit --project ID [--source TABLE] [--date YYYY-MM-DD]
/// [--op base|derived|dedup|hot|sealed|repair] [--slice-hours N] [--offset-hours N]`
///
/// Execute ONE maintenance unit against the configured storage and print where
/// its time went (scan/stage/commit/end-to-end deltas + wall). The per-unit
/// cost decomposition as a command, not a fleet-counter inference. Point
/// TIMEFUSION_DATA_DIR at a scratch dir so the journal holds no other
/// claimable work.
async fn run_unit_cli(cfg: &'static AppConfig) -> anyhow::Result<()> {
```

"The per-unit cost decomposition as a command, not a fleet-counter inference" is
the point. Fleet counters tell you throughput; they cannot tell you that 3.74 s of
a 5.21 s unit is the *commit* — which is the measurement that made width, not
scan efficiency, the lever (Chapter 17).

The report type formats itself:

**Listing 23.8 — src/database/mod.rs:2348–2368**

```rust
/// What a single `run-unit` execution produced. The phase counters are deltas
/// of the global `maintenance_stats` atomics over the unit's run; the CLI owns
/// the process, so nothing else is moving them.
#[derive(derive_more::Display)]
#[display(
    "run-unit: {operation:?} {project_id} {date} | wall {wall_ms}ms | scan {scan_ms}ms staging {staging_ms}ms commit {commit_ms}ms e2e {end_to_end_ms}ms | cohorts {cohorts} | state {state:?}{}",
    retry_reason.as_deref().map_or_else(String::new, |reason| format!(" | retry_reason {reason}"))
)]
pub struct UnitRunReport {
    pub operation: crate::maintenance_coordinator::Operation,
    pub project_id: String,
    pub date: chrono::NaiveDate,
    pub wall_ms: u64,
    pub scan_ms: u64,
    pub staging_ms: u64,
    pub commit_ms: u64,
    pub end_to_end_ms: u64,
    pub cohorts: u64,
    pub state: Option<crate::maintenance_coordinator::TaskState>,
    pub retry_reason: Option<String>,
}
```

`derive_more::Display` with a format string on the type keeps the rendering with
the data. The measurement technique is stated too: the phase counters are *deltas*
of the global maintenance atomics over the unit's run, valid only because the CLI
owns the process and nothing else is moving them.

⚠️ `CLAUDE.md` records that `run-unit` "never worked, its profile zeroed its own
pool" — the `MaintenanceCli` bug from Listing 9.2 — and separately that it once
"ran the WRONG unit (28 of 100)". Both are fixed at the pinned commit, but the
tool has less production mileage than the others.

### `optimize` — off-box compaction

**Listing 23.9 — src/main.rs:819–824**

```rust
/// One-off compaction CLI (`timefusion optimize [...]`): compacts old `date=`
/// partitions outside the scheduled 48h Z-order window via `Database::compact_date`
/// per partition. Meant to run off-box against prod storage so it doesn't load
/// the live server's memory; commits use the same S3/R2 conditional-put
/// coordination as the live server, so concurrent commits OCC-retry safely.
async fn run_optimize_cli(cfg: &'static AppConfig) -> anyhow::Result<()> {
```

**"Meant to run off-box against prod storage"** — a maintenance CLI that competes
with the live server for memory is worse than useless. Delta's conditional-put
commit coordination is what makes it safe to run concurrently: the live server and
the CLI OCC-retry against each other rather than corrupting.

Five modes, in the order the code checks them:

- **`--dry-run`** — list candidate partitions and file counts, mutate nothing
  (`src/main.rs:895–910`).
- **`--recompress`** — the only force-rewrite (Chapter 19, Listing 19.16).
- **`--consolidate [--target-size-mb N]`** — leveled event-time-disjoint
  consolidation, per project.
- **`--dedup`** — a dedup pass per project.
- **default** — `compact_date_concurrent` per partition.

The consolidation loop's retry is worth showing, because it is resumable by
construction:

**Listing 23.10 — src/main.rs:939–969**

```rust
    if consolidate || dedup {
        // Leveled event-time-disjoint consolidation (the cold sweep's engine,
        // pointed at any date/target) and/or a dedup pass, per project so a
        // busy day's tens of GB never sit in one merge. Oldest event-time
        // slices rewrite first; incremental per-run commits make an
        // interrupted run resumable.
        const MAX_ATTEMPTS: u64 = 5;
        for d in &dates {
            let projects = match &project {
                Some(p) => vec![p.clone()],
                None => db.partition_projects(&table_ref, *d).await?,
            };
            if consolidate {
                let target = target_size_mb.map_or(cfg.parquet.timefusion_cold_optimize_target_size, |mb| mb * 1024 * 1024);
                for p in &projects {
                    println!("  consolidate date={d} project={p} target={}MB", target / (1024 * 1024));
                    // Committed runs persist across attempts (excluded from
                    // re-selection), so retrying after a transient S3/OCC error
                    // resumes at the next slice rather than restarting.
                    for attempt in 1..=MAX_ATTEMPTS {
                        match db.consolidate_date_binned(&table_ref, &table, *d, target, Some(p), usize::MAX).await {
                            Ok(()) => break,
                            Err(e) if attempt < MAX_ATTEMPTS => {
                                eprintln!("  consolidate date={d} project={p}: attempt {attempt} failed, retrying: {e}");
                                sleep(Duration::from_secs(5 * attempt)).await;
                            }
                            Err(e) => eprintln!("  consolidate date={d} project={p}: FAILED after {attempt} attempts: {e}"),
                        }
                    }
                }
            }
            // ... elided: the dedup arm, same per-project shape
        }
        reconcile_tantivy(&db, &table).await;
        return db.shutdown().await;
    }
```

Per project rather than per date, "so a busy day's tens of GB never sit in one
merge". Linear backoff (5 s × attempt). And retry is cheap because committed runs
are excluded from re-selection — so an interrupted run resumes at the next slice
rather than restarting.

Note that a failure `eprintln!`s and continues to the next partition rather than
aborting the whole run. For a bulk maintenance tool that is right: one bad
partition should not cost the other ninety-nine.

The Tantivy attachment is not optional:

**Listing 23.11 — src/main.rs:866–878**

```rust
    let db = Database::with_config(Arc::new(cfg.clone())).await?;
    // Attach the tantivy sidecar service exactly like the server bootstrap.
    // Without it `tantivy_indexer()` is None, so the post-optimize reindex/GC
    // hooks silently no-op: every CLI compaction orphans the rewritten files'
    // index entries and leaves its outputs unindexed until a server backfill.
    let db = match (cfg.tantivy.indexed_tables().is_empty(), cfg.aws.aws_s3_bucket.as_deref().unwrap_or_default()) {
        (false, bucket) if !bucket.is_empty() => {
            let storage_uri = format!("s3://{bucket}/{}/tantivy", cfg.core.timefusion_table_prefix);
            let obj_store = db.create_object_store(&storage_uri, &cfg.aws.build_storage_options(None)).await?;
            db.with_tantivy_indexer(Arc::new(timefusion::tantivy::search::TantivyIndexService::new(obj_store, Arc::new(cfg.tantivy.clone()))))
        }
        _ => db,
    };
```

A hook that silently no-ops when a dependency is absent is a good way to leave
production with orphaned index entries and unindexed files. Every exit path in
this CLI therefore also runs a reconcile:

**Listing 23.12 — src/main.rs:999–1008**

```rust
/// Post-run index reconcile: index uncovered live files (incl. leftovers from
/// earlier runs that compacted without the service attached), GC dead entries.
/// Best-effort — the coverage gate keeps queries correct either way.
async fn reconcile_tantivy(db: &Database, table: &str) {
    match db.tantivy_reconcile_table(table).await {
        Ok((0, 0, 0)) => {}
        Ok((built, removed, blobs)) => println!("tantivy reconcile: built={built} manifest_entries_removed={removed} blobs_deleted={blobs}"),
        Err(e) => eprintln!("tantivy reconcile FAILED (indexes stale until the next reconcile or server backfill): {e}"),
    }
}
```

"Incl. leftovers from earlier runs that compacted without the service attached" —
the reconcile repairs the damage the missing-hook bug caused. And the `Ok((0,0,0))`
arm prints nothing, which is the *opposite* of the lesson from Chapter 21's silent
reconcile cron. Here it is fine because the operator is watching the command run;
there it was not, because nobody was.

### `migrate-columns` — widening a shipped table

**Listing 23.13 — src/main.rs:772–786**

```rust
/// Adds nullable columns to a live table's STORED Delta schema, without
/// touching the YAML.
///
/// A shipped table can't gain a column via YAML alone — the YAML and the
/// Delta transaction log are two separate schemas, and a mismatch produces
/// batch/field count errors and rejected INSERTs (see 7d68f01, and the doc
/// block atop `schema_loader.rs`). Run this against prod first; only once
/// every live table has the columns may the YAML declare them.
///
/// Writes a ZERO-ROW batch at the widened schema (`SchemaMode::Merge`), so
/// it's metadata-only and idempotent.
///
///   timefusion migrate-columns --table otel_logs_and_spans \
///       --add updated_at:timestamp --add deleted:boolean [--dry-run]
async fn run_migrate_columns_cli(cfg: &'static AppConfig) -> anyhow::Result<()> {
```

This is the tool Chapter 6 promised. The **order** is the whole procedure: run the
migration against production first, and only once every live table has the columns
may the YAML declare them. Doing it the other way round is commit `7d68f01` —
`number of columns(94) must match number of fields(92)` and rejected `INSERT`s.

A zero-row batch at the widened schema with `SchemaMode::Merge` is metadata-only
and idempotent, which is what makes it safe to re-run.

The output distinguishes three outcomes:

**Listing 23.14 — src/main.rs:805–817**

```rust
    let db = Database::with_config(Arc::new(cfg.clone())).await?;
    let report = db.migrate_add_columns(&table, &adds, dry_run).await?;
    println!("table='{}' stored_columns={} requested={} missing={}", table, report.stored_before, adds.len(), report.added.len());
    for n in &report.added {
        println!("  + {n}");
    }
    match (report.added.is_empty(), dry_run) {
        (true, _) => println!("nothing to migrate — every requested column is already in the stored schema"),
        (_, true) => println!("--dry-run: no commit written"),
        _ => println!("migrated: stored schema now has {} columns", report.stored_after),
    }
    Ok(())
}
```

A two-field match producing three distinct messages, so "already done" and
"dry run" are never confused with "migrated".

### `redrive-dml` — recovering parked DML

**Listing 23.15 — src/main.rs:333–352**

```rust
/// `timefusion redrive-dml [--dir PATH] [--dry-run]` — replay parked quarantine/dml
/// enrichment groups (see [`timefusion::dml::redrive_dml_quarantine`]).
async fn run_redrive_dml_cli(cfg: &'static AppConfig) -> anyhow::Result<()> {
    init_cli_tracing();
    // Two-token flags need lookahead into the arg iterator, so this stays a loop.
    let mut dir = cfg.core.wal_dir().join(timefusion::write::wal::QUARANTINE_DIR_NAME).join("dml");
    let mut dry_run = false;
    let mut it = std::env::args().skip(2);
    while let Some(a) = it.next() {
        match a.as_str() {
            "--dir" => dir = it.next().map(std::path::PathBuf::from).context("--dir needs a value")?,
            "--dry-run" => dry_run = true,
            other => anyhow::bail!("unknown argument: {other} (usage: timefusion redrive-dml [--dir PATH] [--dry-run])"),
        }
    }
    let db = Arc::new(Database::with_config(Arc::new(cfg.clone())).await?);
    let (ok, skipped) = timefusion::dml::redrive_dml_quarantine(&db, &dir, dry_run).await;
    println!("redrive-dml: {ok} recovered, {skipped} left parked (dir {dir:?})");
    db.shutdown().await
}
```

This is the other half of Chapter 16's quarantine. `--dry-run` first, always.
⚠️ Only the hash-enrichment shape is recognized; anything else is reported as
`left parked` and needs hand reconstruction.

### Argument parsing, by hand

Every subcommand parses `std::env::args()` with a `while let` loop rather than
`clap`. The reason is stated once and applies throughout:

**Listing 23.16 — src/main.rs:838–839, 226–229**

```rust
    // Two-token flags need lookahead into the arg iterator, so this stays a loop.
    let mut it = std::env::args().skip(2);
```

```rust
    let mut it = std::env::args().skip(2);
    let usage = "usage: timefusion sim <journal.json|data-dir> [--hours N] [--workers N] [--streams N] [--scale F] [--seed N] [--no-mint] [--json]";
    let input = it.next().context(usage)?;
    let mut cfg = SimConfig::default();
```

The pattern throughout: a `usage` string as the `.context()` for a missing
argument, `.context("--flag must be an integer")` for a parse failure, and
`anyhow::bail!("unknown argument: {other} (usage: …)")` for the catch-all. Every
error message carries the usage line. For a handful of operator tools, no
dependency is a defensible trade — though it is also the kind of thing a project
outgrows.

### Safety notes

**Safe against production storage**: `sim` (touches nothing), `healthcheck`,
`encrypt-secret`, `migrate-columns --dry-run`, `optimize --dry-run`,
`redrive-dml --dry-run`, and the `DELTA HISTORY` / `DELTA ACTIONS` /
`DELTA RECOVERY AUDIT` pgwire commands (Chapter 13).

**Mutating, but designed for concurrent use with a live server**: `optimize` in
all its modes — Delta's conditional-put OCC is what makes that true.

**Run off-box**: `optimize` in particular. The `maintenance-cli` budget profile
exists so a one-shot CLI does not strand cgroup memory in query and ingest slices
it will never use.

**Needs a scratch data directory**: `run-unit` — point `TIMEFUSION_DATA_DIR` at
one "so the journal holds no other claimable work", or it may claim something the
live server was working on.

**Key takeaways.** Eight subcommands, three of which take a maintenance-shaped
memory budget. `sim` is the highest-leverage tool in the repository: it replays a
real production journal through the *real* scheduler on virtual time with measured
duration distributions, states exactly what it does and does not model, is
deterministic under a seed, and answers "does this policy ever reach 30-day
coverage" with `never` when the answer is never. `run-unit` gives the per-unit cost
decomposition that fleet counters cannot. `optimize` is the off-box compaction
tool and is safe alongside a live server because Delta commits are
conditional-put coordinated. `migrate-columns` is the only correct way to widen a
shipped table, and its ordering — migrate first, declare in YAML second — is not
optional.
# Part IV — Module reference

## 24. The modules the flow chapters did not reach

*This part is deliberately thinner than Part III, as Chapter 1's triage promised.
It covers, at reference depth, the five modules that Part III used but did not
open: the virtual clock, the observability plumbing, the `pg_catalog`
compatibility layer, the rollup journal, and the test suites.*

### 24.1 `src/support.rs` — the virtual clock (263 lines)

The smallest module in the crate and one of the most load-bearing: every
time-dependent decision in TimeFusion reads it, which is what makes the e2e suite
deterministic.

**Listing 24.1 — src/support.rs:1–46**

```rust
//! Process-wide wall or frozen clock used by eviction and flush.
//!
//! Tests can control it through SQL UDFs or the compatible
//! `TIMEFUSION_FROZEN_TIME` environment variable.

use std::sync::atomic::{AtomicI64, Ordering};

/// An impossible epoch value marks wall-clock mode.
const WALL_SENTINEL: i64 = i64::MIN;

static FROZEN_NOW: AtomicI64 = AtomicI64::new(WALL_SENTINEL);

fn frozen_micros() -> Option<i64> {
    Some(FROZEN_NOW.load(Ordering::Acquire)).filter(|&v| v != WALL_SENTINEL)
}

pub fn init_from_env() {
    let Ok(s) = std::env::var("TIMEFUSION_FROZEN_TIME") else { return };
    let t = chrono::DateTime::parse_from_rfc3339(&s).unwrap_or_else(|e| panic!("TIMEFUSION_FROZEN_TIME must be RFC3339 ({s:?}): {e}")).timestamp_micros();
    set_micros(t);
    tracing::warn!(frozen_at = %s, "TIMEFUSION_FROZEN_TIME set; clock is frozen (test mode)");
}

#[inline]
pub fn now_micros() -> i64 {
    frozen_micros().unwrap_or_else(|| chrono::Utc::now().timestamp_micros())
}

/// Wall-clock seconds since epoch, honoring the frozen-clock test seam (see `now_micros`).
#[inline]
pub fn now_secs() -> u64 {
    (now_micros() / 1_000_000).max(0) as u64
}

/// Today's UTC date on the (possibly frozen) clock. Maintenance that decides
/// which partitions are sealed must read this rather than `Utc::now`, or a
/// frozen-clock test sees a date its fixture data never lands in.
pub fn today_utc() -> chrono::NaiveDate {
    chrono::DateTime::from_timestamp_micros(now_micros()).unwrap_or_default().date_naive()
}

/// True when the clock is currently pinned (test mode).
pub fn is_frozen() -> bool {
    frozen_micros().is_some()
}
```

A single `AtomicI64` with `i64::MIN` as the "not frozen" sentinel. `now_micros()`
is one relaxed-ish atomic load plus a branch on the common path — cheap enough to
call everywhere, which is the point: a clock seam only works if nothing is tempted
to bypass it.

**Listing 24.2 — src/support.rs:47–60**

```rust
/// Install or replace the frozen time (test mode). Returns the new value.
pub fn set_micros(t: i64) -> i64 {
    FROZEN_NOW.store(t, Ordering::Release);
    t
}

/// Advance the frozen time by `delta_micros`. If the clock is *not* frozen,
/// this freezes it at `wall_now + delta_micros` so the first call from an
/// unprimed test harness has predictable behavior. Returns new value.
pub fn advance_micros(delta_micros: i64) -> i64 {
    set_micros(now_micros().saturating_add(delta_micros))
}
```

`advance_micros` freezing an unfrozen clock is the right ergonomics for a harness:
`env.advance(Duration::from_secs(600))` works whether or not the test primed the
clock first.

**Where the seam deliberately does *not* apply.** Chapter 12 listed four clocks in
one struct; the distinction matters here. `TimeBucket::first_wal_pin_micros` uses
`chrono::Utc::now()` directly, "deliberately real-clock … it is compared against
file mtimes" — a virtual clock cannot move the filesystem. Similarly
`storage.rs`'s certification timestamps use `SystemTime` because they must survive
the process.

**Where forgetting the seam breaks a test.** `today_utc()`'s comment names it:
maintenance deciding which partitions are sealed must read this rather than
`Utc::now`, or a frozen-clock test sees a date its fixture data never lands in.

`is_frozen()` also gates *operational* behaviour, not just time: the `FLUSH`
rate limit and the post-flush WAL reclaim are both skipped under a frozen clock
(Chapter 13, Listing 13.7), because a test harness's cadence is script-driven.

The clock UDFs (`timefusion_set_clock`, `timefusion_advance_clock`,
`timefusion_now_micros`) are registered only behind `TIMEFUSION_ENABLE_TEST_UDFS`
with a `warn!` (Chapter 14, Listing 14.21), so a stray SQL session cannot yank
production's eviction clock.

### 24.2 `src/observability.rs` — metrics, traces, profiling (1,468 lines)

**Listing 24.3 — src/observability.rs:1–17**

```rust
//! OpenTelemetry metrics export.
//!
//! Sits next to `telemetry.rs` (which owns traces). On `init_metrics()` we
//! create a `SdkMeterProvider` with the OTLP exporter, register a few
//! observable gauges that read from the `BufferedWriteLayer` once per export
//! cycle, and install it as the global meter provider.
//!
//! Why observables (not synchronous counters): the stats we care about
//! (memory pressure, oldest bucket age, WAL bytes) live inside the
//! `BufferedWriteLayer` and are already computed by `snapshot_stats()` for
//! the SQL `timefusion.stats()` view. Polling on each export keeps the hot
//! path untouched.
//!
//! Counters (insert success/failure, corruption events) are exposed through
//! `MetricsRegistry::record_*` so they can be incremented inline. They live
//! in a process-global `OnceLock`; if init isn't called (tests, embedded
//! use), the helpers no-op.
```

⚠️ The module doc references a sibling `telemetry.rs` "which owns traces". No such
file exists; `init_telemetry` lives in this same module. Stale, and recorded in
Chapter 29.

The **observable-versus-synchronous** distinction is the design: state that already
exists inside a subsystem is *polled* once per export cycle rather than pushed on
every change, so the hot path is untouched. Events that have no natural home
(insert success, corruption) are incremented inline.

The counter registry is macro-generated from a single list:

**Listing 24.4 — src/observability.rs:54–57**

```rust
/// Declares the counter registry struct and its `new()` builder from a single
/// list of `field => "metric.id": "description"` entries, so adding a counter
/// is a one-line change with no risk of the field and registration drifting.
```

The same anti-drift instinct as `scan_metric_names` (Chapter 7) and
`MissReason`'s strum labels (Chapter 18): one declaration, two uses, no
possibility of divergence.

The most interesting piece is the local readback path, which exists because the
OTel bridge is push-only:

**Listing 24.5 — src/observability.rs:154–196**

```rust
/// Local, in-process side of `metrics::histogram!()` calls, for readback (e.g.
/// `timefusion_stats` percentiles). The OTel bridge (`metrics_exporter_opentelemetry`)
/// is push-only — no snapshot API — so the two are fanned out from one global
/// `metrics::Recorder` via `metrics_util::layers::Fanout`; see `init_metrics()`.
///
/// One `Summary` (DDSketch, relative-error quantiles) per metric name, keyed
/// lazily on first `record()` — replaces hand-rolled power-of-two bucket arrays.
// ... elided: LocalHistograms

/// Local recorder: histograms go through `LocalHistograms` (Summary/DDSketch,
/// above); counters and gauges go through `metrics_util`'s own `Registry` +
/// `AtomicStorage` — ready-made `Arc<AtomicU64>`-backed storage, so this needs
/// no bespoke counter/gauge type of its own. Wrapping both in one newtype
/// (rather than implementing `Recorder` on `Arc<LocalHistograms>` directly) is
/// required by the orphan rule — `Arc` isn't a fundamental type.
```

Every call site writes once through `metrics::counter!()` / `histogram!()`, and a
`Fanout` layer sends it to *both* the OTel exporter and a local registry. That is
what makes `timefusion_stats` readable over SQL without a collector, and what
guarantees the two views cannot disagree. The orphan-rule note explains an
otherwise-mysterious newtype.

**Listing 24.6 — src/observability.rs:218–240**

```rust
/// Read back a quantile (0.0-1.0) for a name recorded via `metrics::histogram!()`.
/// `None` if metrics weren't initialized or the name has never recorded a value.
pub fn histogram_quantile(name: &str, p: f64) -> Option<f64> {

/// Read back the current value of a name recorded via `metrics::counter!()`.
/// 0 if metrics weren't initialized or the name has never recorded a value —
/// matches how these were read before migration (`AtomicU64::load` on an
/// unused field is also 0), so callers don't need an `Option`.
pub fn counter_value(name: &'static str) -> u64 {

/// Read back the current value of a name recorded via `metrics::gauge!()`. See `counter_value`.
pub fn gauge_value(name: &'static str) -> f64 {

/// Test helper: installs just the local (non-OTel) side of `init_metrics()`'s recorder, so a
/// unit/integration test can assert on `metrics::counter!()`/`histogram!()` values via
/// `counter_value()`/`gauge_value()`/`histogram_quantile()` without a running OTLP collector.
```

Note the deliberate asymmetry: `histogram_quantile` returns `Option` (a quantile
over no samples is meaningless) while `counter_value` returns `0` (which matches
how an unused `AtomicU64` field read before the migration). Both choices are
justified rather than defaulted.

Traces are set up in `init_telemetry`, with one subtlety worth extracting:

**Listing 24.7 — src/observability.rs:1140–1149**

```rust
    // tonic/hyper events inside an export would recurse into another export.
    let log_exporter = opentelemetry_otlp::LogExporter::builder().with_tonic().with_endpoint(otlp_endpoint).with_timeout(EXPORT_TIMEOUT).build()?;
    // ... elided: provider construction
        .with_filter(tracing_subscriber::filter::filter_fn(|meta| !["opentelemetry", "tonic", "h2", "hyper"].iter().any(|p| meta.target().starts_with(p))));
```

Exporting logs over gRPC means the gRPC client's own `tracing` events would be
exported, which emits more events, which… The filter cuts the loop by target
prefix.

The module also owns the profiling entry points (`observability::start`), the
runtime-lag sampler (`spawn_runtime_lag_sampler`, started deliberately early —
Chapter 9, Listing 9.18), and the maintenance retry-reason slot
(`set_maintenance_retry_reason` / `maintenance_retry_reason`) that surfaces in
`UnitRunReport`.

### 24.3 `src/server/pg_compat.rs` — Postgres client compatibility (1,431 lines)

This module exists entirely because real Postgres clients probe things before they
will talk to you.

**Listing 24.8 — src/server/pg_compat.rs:39–46**

```rust
pub const PG_COMPAT_VERSION: &str = "16.6";
pub const PG_COMPAT_VERSION_NUM: &str = "160006";
pub const PG_COMPAT_DATABASE: &str = "postgres";
pub const PG_COMPAT_SCHEMA: &str = "public";
pub const DEFAULT_MAX_STATEMENT_SECS: u64 = 60;

#[derive(Debug, Default)]
pub struct PgCatalogContext {
```

TimeFusion presents as PostgreSQL 16.6. ⚠️ Note the tension with `README.md:15–19`,
which says `SHOW server_version` "reflects the embedded PostgreSQL-protocol layer,
not TimeFusion's own" — these constants are that layer's answer.

**Listing 24.9 — src/server/pg_compat.rs:77–83, 143–146**

```rust
pub fn setup_catalog(ctx: &SessionContext, role: &str, max_statement_secs: u64) -> DFResult<()> {
// ... elided
/// Empty PostgreSQL 16 runtime views needed by pgAdmin clients.
```

```rust
/// Delegates to the pg_catalog crate's schema provider, adding tables it does
/// not ship. It hardcodes its table list and does not implement
/// `register_table`, so wrapping is the only way to extend it from here.
```

Wrapping rather than forking, because the upstream provider hardcodes its table
list and does not implement `register_table`. The same "extend, don't fork"
instinct as `PgCoalesceUdf` (Chapter 14).

Three fixes that only a real client discovers:

**Listing 24.10 — src/server/pg_compat.rs:214, 359–380**

```rust
/// Alias that identifies pgAdmin's role probe. No other client query uses it.
```

```rust
/// A tautology over every `$n` the original statement bound, so the substitute
/// plan declares the same parameters. Without it the client's Bind is rejected
/// with "expected 0 parameters but got 1" — pgAdmin binds the role name it is
/// testing for. The type is Utf8 because that is what pgAdmin sends; the
/// predicate is always true, so the value is never actually consulted.
```

```rust
/// A plan that *produces* the row, rather than an empty relation shaped like it:
/// the extended protocol executes this plan, and only the real executor encodes
/// columns in the result format the client requested.
```

Each is a small, specific accommodation. The last one is the most generalizable:
under the extended protocol, an *empty relation of the right shape* is not
equivalent to a plan that produces a row, because only the real executor encodes
columns in the format the client asked for.

**Listing 24.11 — src/server/pg_compat.rs:508–512, 546–550**

```rust
/// Every setting `current_setting()` answers — also the row set of
/// `pg_show_all_settings()`, so the two can never disagree.
```

```rust
/// `pg_settings` is a view over this set-returning function; pgAdmin calls the
/// function directly on connect, and the upstream pg_catalog crate only ships
/// the view — without this, every pgAdmin connection fails at planning.
```

One list serving both `current_setting()` and `pg_show_all_settings()` — the same
single-source discipline again. And a genuinely load-bearing gap: pgAdmin calls
the *function* on connect while upstream ships only the *view*, so without this
every pgAdmin connection failed at planning.

The module also owns `timefusion_stats`, referenced throughout this book:

**Listing 24.12 — src/server/pg_compat.rs:717**

```rust
//     SELECT * FROM timefusion_stats;
```

It is a table provider that renders `(component, key, value)` rows by reading
`BufferedWriteLayer::snapshot_stats()`, the `ScanMetrics` atomics, and the local
metrics registry (§24.2). Exposing in-process diagnostics through the query
protocol means an operator needs no second port, no scrape endpoint, and no
credentials beyond the ones they already have — which, on a memory-tight box where
ad-hoc `SELECT`s over `otel_logs_and_spans` can themselves cause an OOM, is the
difference between diagnosable and not.

`docs/pg-client-compat.md` and `tests/slt/pg_catalog.slt` are the reference and the
regression guard. `CLAUDE.md` records the rule learned the hard way:
**`pg_catalog` compatibility must be tested through `.slt`/pgwire, not unit
tests** — the failures live in the protocol layer, not in the plan.

### 24.4 `src/rollup_journal.rs` — the dirty-range journal (118 lines)

**Listing 24.13 — src/rollup_journal.rs:1–31**

```rust
//! Crash-safe dirty-range journal for rollup maintenance.
//!
//! This is scheduling state, not the read-side correctness boundary. Missing or
//! unreadable state deliberately loads as empty; an absent dirty entry already
//! means "full rebuild required" to the builder.

use std::{
    fs,
    io::{ErrorKind, Write},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use tracing::warn;

const VERSION: u32 = 1;

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct RollupInvalidation {
    pub project_id: String,
    pub source: String,
    pub date: String,
    pub epoch: u64,
    pub dirty_hours: u32,
    pub unknown: bool,
    /// Wall-clock time when this partition first became dirty. A zero value
    /// comes from journals written before this field existed and is treated as
    /// unknown rather than manufacturing an inaccurate age.
    #[serde(default)]
    pub invalidated_unix_ms: u64,
}
```

118 lines, and the module doc carries the whole safety argument: **missing or
unreadable state deliberately loads as empty, because an absent dirty entry
already means "full rebuild required" to the builder.** The degraded mode is more
work, never wrong work — the same property as every sidecar in Chapter 8.

`dirty_hours` is the 24-bit mask from Chapter 18. `unknown` is the explicit
"we cannot say" state, distinct from a zero mask. And `invalidated_unix_ms`
defaulting to zero is treated as *unknown* rather than as "invalidated at the
epoch" — refusing to manufacture an inaccurate age is the same instinct as
`age_since`'s `checked_sub` (Chapter 8, Listing 8.15).

### 24.5 The test suites

Three targets, three purposes, covered here at reference depth; Chapter 26 covers
how to run them.

**`tests/suite/` — 27 modules, one binary.** Integration tests against real
dependencies. The largest is `dedup_compaction_test.rs` at 2,938 lines. Notable
modules and what they pin:

| Module | Pins |
|---|---|
| `dedup_compaction_test` | the dedup/compaction interaction, the largest surface |
| `test_dml_operations` | `UPDATE`/`DELETE` shapes including `UPDATE … FROM` |
| `kill_recovery` | spawns real server processes and kills them |
| `sqllogictest` | drives all 15 `.slt` files |
| `tantivy_*` (5 modules) | index build, storage, search, e2e, transparency |
| `pg_client_compat`, `jsonb_oid_test`, `pgwire_dml_tag_test` | wire-level client behaviour |
| `connection_pressure_test`, `listen_backlog_test` | accept-path behaviour under load |
| `proptest_invariants` | property tests |
| `plan_cache_shape_repro`, `unnest_optimizer_regression_test` | named regressions |

**`tests/e2e/` — 28 modules behind the `e2e` feature.** The full production path
on virtual time, using `server::bootstrap()` so a failure mirrors a production
failure. `harness.rs` (715 lines) owns MinIO resolution and per-test isolation.

The isolation model is worth stating because it is what makes ~10-way parallelism
safe: **per-test uniqueness comes from configuration, not from serialization.**
A unique `e2e-<uuid>` bucket, a unique data directory, a unique table-name prefix,
and an OS-assigned port (`127.0.0.1:0`). No `#[serial]`, no fixed port windows, no
`set_var`.

The test names read as a feature list: `restart_recovery`, `staged_commit`,
`repair_resume`, `merge_on_read`, `deletion_vectors`, `wide_scan_gate`,
`ordering_pushdown`, `recent_window_pruning`, `partition_pruning`,
`pressure_flush`, `flush_sort_escalation`, `hot_tail_sorted_footer`,
`consolidate_catchup`, `postcommit_hooks`, `zorder_idempotence`,
`multi_tenant_isolation`, `bulk_load`, `hash_enrichment`, `cache_warmth`.
Each corresponds to a mechanism in Part III.

**`tests/slt/` — 15 sqllogictest files.** SQL-surface behaviour: aggregations,
filtering, JSON functions, percentile functions, `pg_catalog`, Variant columns and
functions, `DISTINCT ON` over Variant, merge-on-read, partition pruning, edge
cases. Each file becomes its own `#[test]` via `slt_files!`, and
`every_slt_file_has_a_test` fails if a file is not listed — so a `.slt` cannot be
silently unrun.

**`benches/` — 5 criterion benches**: `core_benchmarks`, `dedup_benchmarks`,
`sort_layout_benchmarks`, `sort_strategy_benchmarks`, `tantivy_benchmarks`. These
are what the sort and dedup constants in Chapters 15 and 19 were tuned against.

**`bench/` — the load generators** (not Cargo): `run_insert_bench.sh`,
`run_select_bench.sh`, `query_under_ingest.py`, `timeseries_lifecycle.py`,
`replay_prod_load.py`, plus audit tooling. `timeseries_lifecycle.py` is the one
that needs `TIMEFUSION_ENABLE_TEST_UDFS` — it simulates hours in seconds by
driving the clock over SQL.

**What the tests reveal about intent.** Reading test *names* is the cheapest way to
learn a subsystem's invariants in this codebase, because they are written as
property statements rather than as function names:
`keep_greatest_null_tiebreak_loses`, `flush_advance_must_not_consume_open_bucket_entries`,
`shipped_mor_tables_declare_the_migrated_columns_last`,
`probe_worst_case_fits_the_docker_timeout`,
`superseded_vetoes_backfill_enqueue`, `wal_admit_decouple_on_never_drops_over_budget`.
Each names a rule; the body checks it.

**Key takeaways.** `support.rs` is a two-state atomic clock that everything
time-dependent reads, with deliberate exceptions where a virtual clock cannot help
(file mtimes, cross-process timestamps). `observability.rs` fans one
`metrics::Recorder` out to both OTLP and a local registry, which is why
`timefusion_stats` can answer over SQL without a collector and cannot disagree with
what is exported. `pg_compat.rs` is a catalogue of specific accommodations that
only real clients discover, wrapping the upstream provider rather than forking it.
`rollup_journal.rs` is 118 lines whose entire safety argument is that missing state
degrades to more work, never wrong work. And the three test targets are isolated by
*configuration* rather than by serialization, which is what makes the suite
parallel.
# Part V — Operations and evolution

## 25. Configuration

*After this chapter you will know every configuration surface TimeFusion has: the
166 environment-deserialized fields, the handful of env vars read directly, what
is deliberately **not** configurable, and how a secret is encrypted.*

### Three kinds of configuration

**1. Deserialized fields (166).** Nine structs, each loaded by `envy` from the
environment, each field's default written on the field itself via
`serde-inline-default` (Chapter 7, Listing 7.3). The env var name is the field
name upper-cased.

**2. Directly-read env vars (11).** Read by `std::env::var` at specific points,
usually because they must be consulted before `AppConfig` exists or because they
are test/debug seams.

**3. Pinned constants (not configurable, on purpose).** The budget tree's
fractions, the rewrite permits, the sort budgets. Chapter 7 quotes the rationale:
"Fixed fractions are opinions pinned in code (no override — that's the point of
deleting the env vars): a workload needing a different split changes the constants
here, not a knob in production."

That third category is a deliberate reversal. Per-consumer memory knobs existed
and were removed because they drift out of proportion with each other — and the
sum, not any individual value, is what kills the process.

### The one number that sizes everything

**Listing 25.1 — src/config.rs:82–96**

```rust
/// `TIMEFUSION_MEMORY_BUDGET_GB`: sizes the whole tree BELOW the cgroup limit.
///
/// A single input — every budget derives from it, so shares can't drift out of
/// proportion (the failure mode that got the old per-consumer knobs removed).
/// Needed because the tree otherwise budgets 100% of the cgroup, which
/// oversubscribes a shared host (TF's container sits alongside other services
/// on the same box, and growing into its entitlement gets it OOM-killed).
/// Lowering the container limit fixes it too but needs an orchestrator change
/// and a redeploy; this lets the process size itself down instead.
///
/// Only ever LOWERS the effective limit — an over-large value is clamped,
/// never honoured.
fn env_memory_budget_bytes() -> Option<usize> {
    std::env::var("TIMEFUSION_MEMORY_BUDGET_GB").ok()?.parse::<f64>().ok().filter(|gb| *gb > 0.0).map(|gb| (gb * GIB as f64) as usize)
}
```

If you change one thing about a TimeFusion deployment's memory behaviour, change
this. Everything else derives from it (Chapter 7).

### The minimum viable configuration

```bash
AWS_S3_BUCKET=my-bucket
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
PGWIRE_PASSWORD=...            # or TIMEFUSION_ALLOW_INSECURE_AUTH=true for local dev
```

Everything else has a default. For a non-AWS endpoint add `AWS_S3_ENDPOINT` and,
for plain HTTP MinIO, `AWS_ALLOW_HTTP=true`.

### Directly-read environment variables

These bypass `AppConfig` entirely.

| Var | Read at | Effect |
|---|---|---|
| `TIMEFUSION_MEMORY_BUDGET_GB` | `DerivedBudget::compute` | lowers (never raises) the whole budget tree |
| `TIMEFUSION_MEMORY_LIMIT_GB` | `detect_memory_limit_bytes` | **only** honoured when no cgroup limit exists |
| `TIMEFUSION_BUDGET_PROFILE` | `profile_from_env` | `server` (default) or `maintenance-cli` |
| `TIMEFUSION_COORDINATOR_JOB_WORKERS` | `coordinator_jobs` | overrides the derived worker count; `1` restores the old serialized behaviour |
| `TIMEFUSION_ALLOW_INSECURE_AUTH` | `is_insecure_auth_allowed` | permits an unset pgwire password (dev only) |
| `TIMEFUSION_ENABLE_TEST_UDFS` | `register_custom_functions` | registers the clock UDFs; logs a `warn!` |
| `TIMEFUSION_FROZEN_TIME` | `support::init_from_env` | pins the process clock to an RFC 3339 instant |
| `TIMEFUSION_ORDERING_PROBE` | `ordering_probe_enabled` | per-leg ordering attribution (Chapter 15) |
| `TIMEFUSION_CPU_PROFILE` | `observability::start` | default on; `false`/`0` skips the pprof sampler |
| `TIMEFUSION_TANTIVY_INDEXED_TABLES` | `TantivyConfig` | optional override of the schema-derived indexed-table set |
| `TIMEFUSION_TEST_S3_ENDPOINT` | test harnesses | reuse an already-running MinIO |
| `TIMEFUSION_PGWIRE_PORT` | `run_pgwire_healthcheck` | the probe's port, falling back to `PGWIRE_PORT` |

⚠️ `TIMEFUSION_CPU_PROFILE` is the one whose *default* is worth knowing before an
incident. `CLAUDE.md` records that the signal-handler and libunwind code runs at
boot, and that production crash-looped on `exit 139` on 2026-08-11 with
`starting cpu profiler` as the last line of every attempt. Setting it to `false` is
the first thing to try on a boot SIGSEGV with no Rust panic.

### The full field reference

Grouped by struct, in load order. Every name below is the environment variable;
the field name is the same, lower-cased.

#### Core

| Env var | Type | Default |
|---|---|---|
| `TIMEFUSION_DATA_DIR` | `PathBuf` | `"./data"` |
| `PGWIRE_PORT` | `u16` | `5432` |
| `TIMEFUSION_TABLE_PREFIX` | `String` | `"timefusion"` |
| `TIMEFUSION_CONFIG_DATABASE_URL` | `Option<String>` | `—` |
| `ENABLE_BATCH_QUEUE` | `bool` | `true` |
| `TIMEFUSION_BATCH_QUEUE_CAPACITY` | `usize` | `100_000_000` |
| `PGWIRE_USER` | `String` | `"postgres"` |
| `PGWIRE_PASSWORD` | `Option<String>` | `—` |
| `TIMEFUSION_PGWIRE_MAX_STATEMENT_SECS` | `u64` | `60` |
| `TIMEFUSION_OTEL_SCAN_GUARD` | `OtelScanGuard` | `—` |

#### Object storage (`AwsConfig`)

| Env var | Type | Default |
|---|---|---|
| `AWS_ACCESS_KEY_ID` | `Option<String>` | `—` |
| `AWS_SECRET_ACCESS_KEY` | `Option<String>` | `—` |
| `AWS_DEFAULT_REGION` | `Option<String>` | `—` |
| `AWS_S3_ENDPOINT` | `String` | `"https://s3.amazonaws.com"` |
| `AWS_S3_BUCKET` | `Option<String>` | `—` |
| `AWS_ALLOW_HTTP` | `Option<String>` | `—` |
| `TIMEFUSION_S3_CONNECT_TIMEOUT` | `Option<String>` | `—` |
| `TIMEFUSION_S3_REQUEST_TIMEOUT` | `Option<String>` | `—` |
| `TIMEFUSION_S3_LOG_REQUEST_TIMEOUT` | `Option<String>` | `—` |

#### Write buffer, WAL and flush (`BufferConfig`)

| Env var | Type | Default |
|---|---|---|
| `TIMEFUSION_FLUSH_INTERVAL_SECS` | `u64` | `60` |
| `TIMEFUSION_FLUSH_DWELL_SECS` | `i64` | `-1` |
| `TIMEFUSION_BUFFER_RETENTION_MINS` | `u64` | `70` |
| `TIMEFUSION_EVICTION_INTERVAL_SECS` | `u64` | `60` |
| `TIMEFUSION_BUFFER_MAX_MEMORY_MB` | `usize` | `4096` |
| `TIMEFUSION_STOP_GRACE_SECS` | `u64` | `70` |
| `TIMEFUSION_WAL_CORRUPTION_THRESHOLD` | `usize` | `10` |
| `TIMEFUSION_FLUSH_PARALLELISM` | `usize` | `8` |
| `TIMEFUSION_FLUSH_COALESCE_COMMITS` | `bool` | `false` |
| `TIMEFUSION_FLUSH_IMMEDIATELY` | `bool` | `—` |
| `TIMEFUSION_WAL_ADMIT_DECOUPLE` | `bool` | `—` |
| `TIMEFUSION_WAL_FSYNC_MS` | `u64` | `200` |
| `TIMEFUSION_WAL_FSYNC_MODE` | `String` | `"sync_each"` |
| `TIMEFUSION_WAL_ACK_FSYNC` | `bool` | `true` |
| `TIMEFUSION_WAL_MAX_FILE_COUNT` | `usize` | `0` |
| `TIMEFUSION_WAL_MAX_UNFLUSHED_MB` | `usize` | `—` |
| `TIMEFUSION_WAL_HARD_LIMIT_GB` | `u64` | `192` |
| `TIMEFUSION_BUCKET_DURATION_SECS` | `u64` | `300` |
| `TIMEFUSION_PRESSURE_FLUSH_PCT` | `u32` | `75` |
| `TIMEFUSION_WRITE_BACKPRESSURE_SECS` | `u64` | `60` |
| `TIMEFUSION_DML_COALESCE_SECS` | `u64` | `3` |
| `TIMEFUSION_DML_COALESCE_FOLD` | `bool` | `true` |
| `TIMEFUSION_FLUSH_BUCKET_TIMEOUT_SECS` | `u64` | `600` |
| `TIMEFUSION_WAL_SHARDS_PER_TOPIC` | `usize` | `4` |
| `TIMEFUSION_DELTA_SCAN_CONCURRENCY` | `usize` | `64` |
| `TIMEFUSION_DELTA_SCAN_DEPTH` | `usize` | `8` |

#### Foyer cache (`CacheConfig`)

| Env var | Type | Default |
|---|---|---|
| `TIMEFUSION_FOYER_MEMORY_MB` | `usize` | `1024` |
| `TIMEFUSION_FOYER_DISK_MB` | `Option<usize>` | `—` |
| `TIMEFUSION_FOYER_DISK_GB` | `usize` | `500` |
| `TIMEFUSION_FOYER_TTL_SECONDS` | `u64` | `604_800` |
| `TIMEFUSION_PROVIDER_CACHE_TTL_SECONDS` | `u64` | `300` |
| `TIMEFUSION_PROVIDER_CACHE_CAPACITY` | `usize` | `4_096` |
| `TIMEFUSION_FOYER_SHARDS` | `usize` | `8` |
| `TIMEFUSION_FOYER_FILE_SIZE_MB` | `usize` | `32` |
| `TIMEFUSION_FOYER_STATS` | `String` | `"true"` |
| `TIMEFUSION_PARQUET_METADATA_SIZE_HINT` | `usize` | `MiB` |
| `TIMEFUSION_DF_METADATA_CACHE_MB` | `usize` | `512` |
| `TIMEFUSION_FOYER_METADATA_MEMORY_MB` | `usize` | `512` |
| `TIMEFUSION_FOYER_METADATA_DISK_MB` | `Option<usize>` | `—` |
| `TIMEFUSION_FOYER_METADATA_DISK_GB` | `usize` | `5` |
| `TIMEFUSION_FOYER_METADATA_SHARDS` | `usize` | `4` |
| `TIMEFUSION_FOYER_BLOCK_SIZE_MB` | `usize` | `256` |
| `TIMEFUSION_FOYER_L1_MAX_ENTRY_MB` | `usize` | `16` |
| `TIMEFUSION_CACHE_RECENT_DAYS` | `usize` | `8` |
| `TIMEFUSION_WARM_INLINE_MAX_MB` | `usize` | `0` |
| `TIMEFUSION_WRITE_CAPTURE_MAX_MB` | `usize` | `32` |
| `TIMEFUSION_WRITE_CAPTURE_BUDGET_MB` | `usize` | `256` |
| `TIMEFUSION_FOYER_DISABLED` | `bool` | `—` |
| `TIMEFUSION_CACHE_BYPASS_SCAN_HOURS` | `u64` | `24` |

#### Parquet and compaction targets (`ParquetConfig`)

| Env var | Type | Default |
|---|---|---|
| `TIMEFUSION_PAGE_ROW_COUNT_LIMIT` | `usize` | `20_000` |
| `TIMEFUSION_ZSTD_COMPRESSION_LEVEL` | `i32` | `3` |
| `TIMEFUSION_ZSTD_LEVEL_INTERMEDIATE` | `i32` | `1` |
| `TIMEFUSION_ZSTD_LEVEL_WARM` | `i32` | `9` |
| `TIMEFUSION_ZSTD_LEVEL_COLD` | `i32` | `19` |
| `TIMEFUSION_COLD_CUTOFF_DAYS` | `u64` | `14` |
| `TIMEFUSION_MAX_ROW_GROUP_SIZE` | `usize` | `128 * MiB` |
| `TIMEFUSION_CHECKPOINT_INTERVAL` | `u64` | `10` |
| `TIMEFUSION_OPTIMIZE_TARGET_SIZE` | `i64` | `256 * MiB` |
| `TIMEFUSION_COLD_OPTIMIZE_TARGET_SIZE` | `i64` | `512 * MiB` |
| `TIMEFUSION_COLD_OPTIMIZE_AFTER_DAYS` | `u64` | `1` |
| `TIMEFUSION_STATS_CACHE_SIZE` | `usize` | `50` |
| `TIMEFUSION_BLOOM_FILTER_DISABLED` | `bool` | `—` |

#### Maintenance (`MaintenanceConfig`)

| Env var | Type | Default |
|---|---|---|
| `TIMEFUSION_VACUUM_RETENTION_HOURS` | `u64` | `72` |
| `TIMEFUSION_LOG_RETENTION_HOURS` | `u64` | `6` |
| `TIMEFUSION_OPTIMIZE_WINDOW_HOURS` | `u64` | `48` |
| `TIMEFUSION_OPTIMIZE_USE_ZORDER` | `bool` | `—` |
| `TIMEFUSION_OPTIMIZE_SORT_BY` | `bool` | `true` |
| `TIMEFUSION_SORT_SKIP_BYTES` | `usize` | `2 * GiB` |
| `TIMEFUSION_FLUSH_SORT_POOL_MB` | `u64` | `1024` |
| `TIMEFUSION_COMPACT_MIN_FILES` | `usize` | `5` |
| `TIMEFUSION_LIGHT_OPTIMIZE_ENABLED` | `bool` | `true` |
| `TIMEFUSION_LIGHT_OPTIMIZE_TARGET_SIZE` | `i64` | `256 * MiB` |
| `TIMEFUSION_WRITER_MAX_FILE_BYTES` | `usize` | `512 * MiB` |
| `TIMEFUSION_REPAIR_MAX_FILE_BYTES` | `usize` | `512 * MiB` |
| `TIMEFUSION_LIGHT_OPTIMIZE_REPAIR_DAYS` | `u64` | `31` |
| `TIMEFUSION_LIGHT_OPTIMIZE_SCHEDULE` | `String` | `"0 */5 * * * *"` |
| `TIMEFUSION_FOOTER_REPAIR_SCHEDULE` | `String` | `"0 30 * * * *"` |
| `TIMEFUSION_FOOTER_REPAIR_BUDGET_SECS` | `u64` | `8640` |
| `TIMEFUSION_DEDUP_SCHEDULE` | `String` | `"0 */5 * * * *"` |
| `TIMEFUSION_DIRTY_BIN_DEDUP_ENABLED` | `bool` | `true` |
| `TIMEFUSION_OPTIMIZE_SCHEDULE` | `String` | `"0 */30 * * * *"` |
| `TIMEFUSION_CONSOLIDATE_SCHEDULE` | `String` | `"0 30 2 * * *"` |
| `TIMEFUSION_CONSOLIDATE_CATCHUP_PASSES` | `usize` | `4` |
| `TIMEFUSION_VACUUM_SCHEDULE` | `String` | `"0 15 */6 * * *"` |
| `TIMEFUSION_RECOMPRESS_SCHEDULE` | `String` | `"0 0 3 * * *"` |
| `TIMEFUSION_CHECKPOINT_SCHEDULE` | `String` | `"0 */2 * * * *"` |
| `TIMEFUSION_RECONCILE_SCHEDULE` | `String` | `"0 0 * * * *"` |
| `TIMEFUSION_TANTIVY_RECONCILE_SCHEDULE` | `String` | `"0 30 3 * * *"` |
| `TIMEFUSION_WARM_AFTER_COMPACTION` | `bool` | `true` |
| `TIMEFUSION_WARM_FULL_FILES` | `bool` | `—` |
| `TIMEFUSION_WARM_RECENCY_DAYS` | `u64` | `1` |
| `TIMEFUSION_WARM_ALL_FOOTERS` | `bool` | `true` |
| `TIMEFUSION_WARM_CONCURRENCY` | `usize` | `16` |
| `TIMEFUSION_EVICT_AFTER_COMPACTION` | `bool` | `true` |
| `TIMEFUSION_INCREMENTAL_SNAPSHOT` | `bool` | `true` |
| `TIMEFUSION_SNAPSHOT_RECONCILE_COMMITS` | `u64` | `500` |
| `TIMEFUSION_REPAIR_RESUME_ENABLED` | `bool` | `true` |
| `TIMEFUSION_DEDUP_LOOKBACK_DAYS` | `u64` | `35` |
| `TIMEFUSION_DEDUP_SWEEP_FALLBACK` | `bool` | `—` |
| `TIMEFUSION_ROLLUP_ENABLED` | `bool` | `—` |
| `TIMEFUSION_ROLLUP_READ_ENABLED` | `bool` | `—` |
| `TIMEFUSION_ROLLUP_REALTIME_TAIL` | `bool` | `—` |
| `TIMEFUSION_ROLLUP_READ_PROJECTS` | `Option<String>` | `—` |
| `TIMEFUSION_ROLLUP_BACKFILL_DAYS` | `u16` | `35` |
| `TIMEFUSION_ROLLUP_BACKFILL_SCHEDULE` | `String` | `"0 */10 * * * *"` |
| `TIMEFUSION_READ_DEDUP_SKIP_SWEPT` | `bool` | `true` |
| `TIMEFUSION_COMPACT_DEDUP_MERGE` | `bool` | `—` |
| `TIMEFUSION_DEDUP_CERTIFICATION_PERSIST` | `bool` | `true` |
| `TIMEFUSION_READ_DEDUP_BOUNDED` | `bool` | `true` |
| `TIMEFUSION_COUNT_PUSHDOWN` | `bool` | `true` |
| `TIMEFUSION_DEDUP_MAX_REWRITE_BYTES` | `u64` | `GiB / 2` |
| `TIMEFUSION_DEDUP_MAX_DECODED_BYTES` | `u64` | `GiB / 2` |
| `TIMEFUSION_DEDUP_DECODE_INFLATION` | `u64` | `12` |
| `TIMEFUSION_DEDUP_BYTES_PER_ROW` | `u64` | `4096` |
| `TIMEFUSION_DML_MERGE_CONCURRENCY` | `usize` | `1` |
| `TIMEFUSION_USE_DELETION_VECTORS` | `bool` | `true` |
| `TIMEFUSION_DML_MERGE_APPEND_REBASE` | `bool` | `true` |
| `TIMEFUSION_DML_MERGE_KEY_PRUNE` | `bool` | `true` |

#### Query memory and scan admission (`MemoryConfig`)

| Env var | Type | Default |
|---|---|---|
| `TIMEFUSION_SORT_SPILL_RESERVATION_BYTES` | `Option<usize>` | `—` |
| `TIMEFUSION_MEMORY_POOL` | `MemoryPoolKind` | `—` |
| `TIMEFUSION_TRACING_RECORD_METRICS` | `bool` | `true` |
| `TIMEFUSION_QUERY_PARTITIONS` | `usize` | `0` |
| `TIMEFUSION_MAX_CONCURRENT_SCAN_READERS` | `usize` | `16` |
| `TIMEFUSION_WIDE_SCAN_LOOKBACK_HOURS` | `u64` | `2` |
| `TIMEFUSION_WIDE_SCAN_MAX_FILES` | `usize` | `256` |
| `TIMEFUSION_WIDE_SCAN_MAX_MB` | `u64` | `64` |
| `TIMEFUSION_PLAN_CACHE_CAPACITY` | `usize` | `2048` |
| `TIMEFUSION_PLAN_CACHE_TIME_FNS` | `bool` | `true` |

#### Tantivy sidecar (`TantivyConfig`)

| Env var | Type | Default |
|---|---|---|
| `TIMEFUSION_TANTIVY_MAX_INDEX_SIZE_MB` | `u64` | `64` |
| `TIMEFUSION_TANTIVY_CACHE_DISK_GB` | `u64` | `64` |
| `TIMEFUSION_TANTIVY_CACHE_REAP_SCHEDULE` | `String` | `"0 */10 * * * *"` |
| `TIMEFUSION_TANTIVY_COMPRESSION_LEVEL` | `i32` | `3` |
| `TIMEFUSION_TANTIVY_MIN_FILES_FOR_PUSHDOWN` | `usize` | `2` |
| `TIMEFUSION_TANTIVY_PREFILTER_MAX_HITS` | `usize` | `100_000` |
| `TIMEFUSION_TANTIVY_PREFILTER_MIN_SELECTIVITY_PCT` | `u32` | `50` |
| `TIMEFUSION_TANTIVY_ROUTE_EQUALITY` | `bool` | `true` |
| `TIMEFUSION_TANTIVY_BACKFILL` | `bool` | `—` |
| `TIMEFUSION_TANTIVY_BUILD_CONCURRENCY` | `usize` | `2` |
| `TIMEFUSION_TANTIVY_BACKFILL_MAX_FILE_MB` | `u64` | `4096` |
| `TIMEFUSION_TANTIVY_FILE_PRUNING` | `bool` | `true` |
| `TIMEFUSION_TANTIVY_PREFETCH_DAYS` | `u32` | `—` |
| `TIMEFUSION_TANTIVY_ROW_SELECTION` | `bool` | `true` |

#### Telemetry (`TelemetryConfig`)

| Env var | Type | Default |
|---|---|---|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `String` | `"http://localhost:4317"` |
| `OTEL_SERVICE_NAME` | `String` | `"timefusion"` |
| `OTEL_SERVICE_VERSION` | `String` | `crate version` |
| `LOG_FORMAT` | `Option<String>` | `—` |
| `OTEL_TRACES_EXPORTER` | `Option<String>` | `—` |

### Encrypted secrets

Per-project storage credentials live in a Postgres `timefusion_projects` table
(`TIMEFUSION_CONFIG_DATABASE_URL`), and may be encrypted at rest.

**Listing 25.2 — src/config.rs:2914–2933**

```rust
// AES-256-GCM two-way encryption for at-rest secrets (S3 creds in
// `timefusion_projects`). Key is supplied via the
// `TIMEFUSION_CONFIG_ENCRYPTION_KEY` env var as a base64-encoded 32-byte
// value. Ciphertext is stored as `enc:v1:<base64(nonce||ct||tag)>`.
//
// Plaintext (un-prefixed) rows are still accepted on read so the feature
// can be rolled out without a forced backfill — re-encrypt with
// `timefusion encrypt-secret <value>` and UPDATE the row.

use aes_gcm::{
    AeadCore, Aes256Gcm, Key, Nonce,
    aead::{Aead, KeyInit, OsRng},
};
use anyhow::{Context, Result, anyhow};
use base64::{Engine, engine::general_purpose::STANDARD as B64};

pub const ENC_PREFIX: &str = "enc:v1:";
const KEY_ENV: &str = "TIMEFUSION_CONFIG_ENCRYPTION_KEY";
const NONCE_LEN: usize = 12;
```

A versioned prefix (`enc:v1:`) makes a future algorithm change a
recognize-and-branch rather than a guess, and accepting un-prefixed rows on read
is what allows incremental rollout with no forced backfill.

**Listing 25.3 — src/config.rs:2935–2953**

```rust
static CIPHER: OnceLock<Option<Aes256Gcm>> = OnceLock::new();

fn cipher() -> Option<&'static Aes256Gcm> {
    CIPHER
        .get_or_init(|| {
            let raw = std::env::var(KEY_ENV).ok().filter(|s| !s.is_empty())?;
            B64.decode(raw.trim())
                .map_err(|e| anyhow!("is not valid base64 ({e})"))
                .and_then(|b| <[u8; 32]>::try_from(b).map_err(|_| anyhow!("is not 32 bytes after base64 decode")))
                .map(|b| Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&b)))
                .inspect_err(|e| tracing::error!("{KEY_ENV} {e}; encryption disabled"))
                .ok()
        })
        .as_ref()
}
```

⚠️ A malformed key is an `error!` and **encryption disabled**, not a startup
failure. That means a typo in the key produces a running server that cannot
decrypt its per-project credentials — the error is logged once, at first use.

Decryption's failure messages name both plausible causes:

**Listing 25.4 — src/config.rs:2964–2981**

```rust
pub fn decrypt_or_passthrough(value: &str) -> Result<String> {
    // ... elided: the un-prefixed passthrough
    let c = cipher().ok_or_else(|| anyhow!("row is encrypted ({ENC_PREFIX}…) but {KEY_ENV} is not set"))?;
    let bytes = B64.decode(rest).context("encrypted secret is not valid base64")?;
    let (nonce, ct) = bytes.split_at_checked(NONCE_LEN).filter(|(_, ct)| !ct.is_empty()).context("encrypted secret payload too short")?;
    let pt = c.decrypt(Nonce::from_slice(nonce), ct).map_err(|e| anyhow!("AES-GCM decrypt failed (key mismatch or tampered ciphertext): {e}"))?;
    String::from_utf8(pt).context("decrypted secret is not valid UTF-8")
}

/// CLI helper: `timefusion encrypt-secret <plaintext>` — encrypts the
// ... elided
pub fn run_cli() -> Result<()> {
    // skip binary + "encrypt-secret"
    let plaintext = std::env::args().nth(2).ok_or_else(|| anyhow!("usage: timefusion encrypt-secret <plaintext>"))?;
    println!("{}", encrypt(&plaintext)?);
```

Rolling out encryption: set `TIMEFUSION_CONFIG_ENCRYPTION_KEY` (base64 of 32
random bytes), run `timefusion encrypt-secret <value>` per credential, `UPDATE`
the rows. Plaintext rows keep working throughout.

`StorageConfig` also redacts on both serde and `Debug` paths (Chapter 7,
Listing 7.11's neighbourhood at `src/database/mod.rs:2029–2044`), so credentials
cannot leak through a metrics dump or a `{:?}` log line.

### Configuration hazards

**Do not `set_var` in tests.** Chapter 7 and `CLAUDE.md` both state it: under
nextest's process-per-test model a process-global mutation silently stops meaning
what you think while still looking correct. Build the `AppConfig` and pass it to
`Database::with_config`. The one sanctioned exception is `main.rs`'s budget-profile
line, which runs before any thread exists (Chapter 9, Listing 9.2).

**`TIMEFUSION_MEMORY_LIMIT_GB` does nothing under a cgroup.** By design — that is
the misconfiguration that produced an OOM loop while an operator believed the limit
was 26 GB and the process was budgeting 120 GiB. Use
`TIMEFUSION_MEMORY_BUDGET_GB`, which always applies and can only lower.

**Two fields set the WAL fsync behaviour.** `timefusion_wal_fsync_mode`
(`sync_each` by default) and `timefusion_wal_fsync_ms` (200). The mode is what
decides whether the interval is used at all.

**`timefusion_flush_interval_secs` defaults to 60, not 600.** `CLAUDE.md` and
`README.md` both say 600/300. The field comment explains the change: a shorter
interval bounds how much un-flushed WAL a restart must replay, at the cost of ~5×
more Delta commits, which compaction absorbs.

**`timefusion_bucket_duration_secs` defaults to 300**, matching the MemBuffer's
compiled-in 5-minute fallback and *not* the 10 minutes the documentation claims
(Chapter 12).

**Schedules are six-field cron** (`croner`): second, minute, hour, day, month,
weekday. `"0 */5 * * * *"` is every five minutes, not every five seconds.

**An empty schedule disables its job.** `timefusion_tantivy_cache_reap_schedule`
says so explicitly: "empty disables the reap (and the bound)".

**Key takeaways.** 166 deserialized fields with defaults written on the field, 11
directly-read env vars, and a deliberately non-configurable memory budget tree
whose only operator lever is `TIMEFUSION_MEMORY_BUDGET_GB` — which can lower the
whole tree but never raise it. Four required variables get a working server;
everything else has a considered default with a doc comment explaining the value.
Secrets are AES-256-GCM with a versioned prefix and plaintext passthrough for
incremental rollout, and a malformed key disables encryption rather than failing
startup.
## 26. Build, test, deploy

*After this chapter you will be able to build TimeFusion, run every test target,
reproduce CI locally, and follow an image from a commit to a running production
container.*

### Build

```bash
cargo build                       # debug — 5–15× faster than release; the dev loop
cargo build --release             # production
cargo build --profile release-iter  # release + symbols, for profilers
make build-prod                   # release with .env.prod loaded
```

Requires a recent stable toolchain (edition 2024 → Rust 1.85+), pinned by
`rust-toolchain.toml`. On macOS arm64 it also requires `ld64.lld`:

**Listing 26.1 — .cargo/config.toml:1–7**

```toml
# Faster linking on macOS arm64. The system linker spends a large slice of every
# leaf recompile linking a ~100MB binary with 1200+ deps' symbols. ld64.lld is
# substantially faster. Full path is required — clang rejects the bare
# `-fuse-ld=ld64.lld` shortname (only `lld`/`gold`/etc. are recognized).
# Install: `brew install llvm@15` (or any llvm providing ld64.lld).
[target.aarch64-apple-darwin]
rustflags = ["-C", "link-arg=-fuse-ld=/opt/homebrew/opt/llvm@15/bin/ld64.lld"]
```

Without it the build fails with `invalid linker name`. Adjust the path or delete
the file to fall back to the system linker. Combined with
`[profile.dev] debug = "line-tables-only"` this roughly halves the warm recompile
of the large `database` module (~22 s → ~12 s).

### Linting: one definition, no second copy

**Listing 26.2 — .cargo/config.toml:9–28**

```toml
# THE lint definition. CI (`.github/workflows/ci.yml`) and the autofix workflow
# both invoke these aliases, so `cargo lint` locally is byte-for-byte what CI
# runs — there is no second copy of the flags to drift.
#
# Why the flags matter (2026-08-01: a 52-file sweep passed a bare
# `cargo clippy --all-targets` and still failed CI on six lints):
#   --all-features  the `e2e`/`profiling` cfgs compile code the default build
#                   never sees, and lints fire inside it
#   --locked        a stale Cargo.lock changes what is linted
#   -D warnings     CI treats every lint as an error; a local run that only
#                   prints them tells you nothing about whether CI will pass
#
# Deliberately NOT a `[lints]` table in Cargo.toml: that would apply
# `deny(warnings)` to `cargo build`/`cargo test` too, so a stray unused import
# would break the dev loop this repo optimizes hard for.
[alias]
lint = "clippy --all-targets --all-features --locked -- -D warnings"
lint-fix = "clippy --fix --all-targets --all-features --allow-dirty --allow-staged -- -D warnings"
```

Two decisions worth copying. **The flags live in exactly one place** and both CI
and the local command invoke the same alias, so "it passed locally" means
something. And a `[lints]` table was *rejected* because it would apply
`deny(warnings)` to `cargo build` and `cargo test` too, breaking the dev loop over
a stray unused import — the strictness is scoped to the gate, not to the inner
loop.

### Testing

**Always `cargo nextest run`, never `cargo test`.** The Makefile explains:

**Listing 26.3 — Makefile:3–23**

```make
# THE inner-loop command: the whole suite, every time you change something.
#
# `cargo nextest` (not `cargo test`) because nextest runs one process per test
# from a single global pool. `cargo test` runs the test *binaries* one after
# another and `#[serial]` re-serializes most tests inside each one, so it uses
# roughly one core no matter how many the box has. Process-per-test also gives
# each test its own copy of the process-global state (`PGWIRE_PORT`, the
# `OnceLock` config) that `#[serial]` exists to protect.
#
# Needs cargo-nextest:
#   curl -LsSf https://get.nexte.st/latest/mac | tar zxf - -C ~/.cargo/bin
#
# Filter with ARGS (substring match on the test name):
#   make test ARGS=dedup_compaction
test:
	cargo nextest run $${ARGS}

# Lib-only: skips the integration binary entirely. Use when iterating on a
# pure-logic change; `make test` is cheap enough to be the default otherwise.
test-unit:
	cargo nextest run --lib $${ARGS}
```

Process-per-test is not only about parallelism — it gives each test its own copy
of the process-global state that `#[serial]` existed to protect. 553 s becomes
~74 s for the same 617 tests.

Note the tension with `CLAUDE.md`, which says to "default to TARGETED tests while
iterating" and reserve the full suite for the pre-push gate, while the Makefile
calls `make test` "THE inner-loop command". Both are defensible; the Makefile's
claim assumes a warm build.

The targets:

| Command | Runs |
|---|---|
| `make test` | the whole suite |
| `make test-unit` | `--lib` only |
| `make test-all` | including `#[ignore]`d tests, with `.env` exported |
| `make test-e2e` | the `e2e` feature target |
| `make test-integration` | the integration binary |
| `make test-minio` / `-all` | against a local MinIO |
| `make test-ovh` / `test-prod` | against non-local storage (explicit opt-in) |
| `cargo nextest run <substring>` | one test, or one `.slt` file by its name |
| `make prepush` | the gate |

### The pre-push gate

**Listing 26.4 — Makefile:36–53**

```make
# Pre-push gate: CI's checks in CI's order — formatting, then lint (both fail
# fast and cheap; neither should cost a full test run), then the whole suite. No
# hand-picked subset any more: with nextest the full run is short enough that
# skipping targets only buys a surprise in CI. Set TIMEFUSION_TEST_S3_ENDPOINT
# to reuse a persistent MinIO.
#
# `fmt` is here because `lint` does NOT cover it and CI runs both: 2026-08-15 a
# push passed local `cargo lint` and the full suite, then failed CI on two
# rustfmt line-break diffs alone. A red CI on master blocks the deploy queue for
# everyone, which is far more expensive than the second this costs.
prepush: fmt lint
	RUST_LOG=off cargo nextest run $${ARGS}

# CI's exact formatting gate (ci.yml "Format"). rust-toolchain.toml pins the
# channel, so this is the same rustfmt CI runs.
fmt:
	cargo fmt --all --check
```

**`cargo lint` does not cover `cargo fmt`.** A push passed local lint and the full
suite and still failed CI on two rustfmt line-break diffs. The justification for
adding a redundant-feeling step is economic: a red CI on master blocks the deploy
queue for everyone, which costs far more than the second `fmt` takes.

`RUST_LOG=off` because the suite's log output is noise at the gate.

### Local storage

Tests default to local MinIO. Reaching non-local storage takes explicit effort:
`make run-prod` / `build-prod` / `test-prod`, or exporting real `AWS_*` credentials
yourself. `.env` points at `127.0.0.1:9000`; production credentials live only in
`.env.prod`.

The `sqllogictest` harness resolves MinIO **local-first**:

1. `TIMEFUSION_TEST_S3_ENDPOINT` if set,
2. an already-running MinIO on `127.0.0.1:9000`,
3. the local `minio` binary, spawned on `:9000` and killed when the test ends,
4. Docker via testcontainers — **only** when no `minio` binary is on PATH.

Docker is the fallback, not the default. `make minio-start` / `minio-stop` run a
persistent one for iteration.

⚠️ A known gotcha: ~15 database unit tests need a `timefusion-tests` bucket that
the e2e/sqllogictest MinIO resets silently delete. On `NoSuchBucket`:

```bash
AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
  aws s3 mb s3://timefusion-tests --endpoint-url http://localhost:9000
```

### CI

**Listing 26.5 — .github/workflows/ci.yml:14–17, 27–35, 48–66**

```yaml
concurrency:
  group: ci-${{ github.workflow }}-${{ github.ref }}
  cancel-in-progress: true
```

```yaml
  fmt:
    name: Format
    runs-on: blacksmith-4vcpu-ubuntu-2404
      # channel, so plain `cargo fmt` here uses the exact same rustfmt as every
      - run: cargo fmt --all --check
```

```yaml
  clippy:
    name: Clippy
    runs-on: blacksmith-4vcpu-ubuntu-2404
      # `cargo lint` is the alias in `.cargo/config.toml`; `make lint`, the
        run: cargo lint
```

Five jobs: `fmt`, `clippy`, `build-test` (sharded), a pgwire smoke test, and
`e2e`. All on 4-vCPU Blacksmith runners, with in-progress runs cancelled per ref.

**Listing 26.6 — .github/workflows/ci.yml:70–72, 173, 213–237**

```yaml
  build-test:
    name: Clippy & Test (shard ${{ matrix.shard }})
    runs-on: blacksmith-4vcpu-ubuntu-2404
```

```yaml
        run: cargo nextest run --profile ci --locked --partition hash:${{ matrix.shard }}/2
```

```yaml
    name: E2E
    runs-on: blacksmith-4vcpu-ubuntu-2404
        run: cargo nextest run --profile ci --features e2e --locked -E 'binary(e2e)' --test-threads 2
```

The suite is split with `--partition hash:N/2`, so each shard runs a
deterministic half. E2E is capped at `--test-threads 2` because each test can spawn
its own MinIO.

The smoke test carries a lesson about backgrounding:

**Listing 26.7 — .github/workflows/ci.yml:183–190**

```yaml
          # Build BEFORE backgrounding. `cargo run &` compiles inside the
          # ... elided
          # what the code did — the step's own log was cargo warnings and
          # ... elided
          cargo build --locked --quiet --bin timefusion
            cargo run --locked --quiet --bin timefusion >"$log" 2>&1 &
```

`cargo run &` compiles *inside* the background job, so the captured log is cargo's
output rather than the program's. Build first, then background.

⚠️ **The local-CI attestation system described at length in `CLAUDE.md` — `make ci`,
`ci/checks.tsv`, `scripts/ci/ci.sh`, `make ci-status`, `make ci-selftest`, and the
`refs/ci-attest/v1/*` attestations — does not exist on this branch.** Neither file
is present and the `Makefile` has no `ci` target. `make prepush` is the real
pre-push gate. Chapter 29 records it.

### The image

**Listing 26.8 — Dockerfile:6, 14, 22, 47, 58–59, 82–83, 91, 115, 122**

```dockerfile
FROM rust:1.91-slim-bookworm AS chef
RUN cargo install cargo-chef --version 0.1.77 --locked
FROM chef AS planner
FROM chef AS builder
# so cargo-chef's cached dep layer matches the final build's profile.
ENV CARGO_PROFILE_RELEASE_STRIP=none
# identical RUSTFLAGS or cargo-chef's cached dep layer is silently discarded.
ENV RUSTFLAGS="-C force-frame-pointers=yes -C target-cpu=${TARGET_CPU}"
ENV JEMALLOC_SYS_PROF_BACKTRACE=libunwind
FROM gcr.io/distroless/cc-debian12:nonroot
ENV LD_LIBRARY_PATH=/usr/local/lib
```

A four-stage build. `cargo-chef` caches the dependency layer, and **two settings
must match between the planner and the builder or that cache is silently
discarded**: `CARGO_PROFILE_RELEASE_STRIP` and `RUSTFLAGS`. Silently is the
operative word — a mismatch costs a full dependency rebuild with no error.

`force-frame-pointers=yes` keeps profiler stacks resolvable.
`JEMALLOC_SYS_PROF_BACKTRACE=libunwind` is the vendored `tikv-jemalloc-sys` patch
from Chapter 4 — upstream passes `--enable-prof` alone and the resulting libgcc
unwinder returned zero frames, so every production heap dump was one anonymous
frame.

`target-cpu` is a build argument. ⚠️ `CLAUDE.md` records that production shipped a
*baseline* x86-64 (SSE2) binary for months on an AVX-512 EPYC — the `simd-bench`
workflow exists to keep that visible.

The runtime image is `distroless/cc-debian12:nonroot` — no shell, non-root by
default.

**Listing 26.9 — Dockerfile:149, 155, 167, 170**

```dockerfile
HEALTHCHECK --interval=5s --timeout=5s --start-period=10s --start-interval=250ms --retries=5 \
ENV OTEL_EXPORTER_OTLP_ENDPOINT=http://srv-captain--otelcol:4317 \
ENV MALLOC_ARENA_MAX=2 \
ENTRYPOINT ["/usr/local/bin/timefusion"]
```

The `HEALTHCHECK` line is the one pinned by
`probe_worst_case_fits_the_docker_timeout` (Chapter 9, Listing 9.25): `--timeout=5s`
must exceed the probe's 3 × 1.5 s worst case, and `--retries=5` because "3
consecutive misses inside 15s is 'busy', not 'dead'". `--start-interval=250ms`
makes a fresh container reach healthy quickly without shortening the steady-state
interval.

`MALLOC_ARENA_MAX=2` bounds glibc's per-thread arenas — relevant even under
jemalloc, since not every allocation path goes through it.

### Deploy

`deploy.yml` runs on push and does: build and push to GHCR tagged with the git
short SHA → **smoke-test the pushed image** → prepare the production handoff →
deploy to CapRover → record the boot.

**Listing 26.10 — .github/workflows/deploy.yml:85–106**

```yaml
      - name: Smoke test pushed image
          docker pull "$IMAGE_URL"
          docker run -d \
          trap 'docker rm -f tf-smoke >/dev/null 2>&1 || true' EXIT
            if docker logs tf-smoke 2>&1 | grep -Eq \
          docker logs tf-smoke 2>&1 | tail -40
```

The *pushed* image is pulled back and run before anything is deployed — so a
broken image is caught in CI rather than in production. The `trap` guarantees
cleanup even on failure.

**Listing 26.11 — .github/workflows/deploy.yml:109–120, 162, 249–262**

```yaml
      # Install the same CLI used by caprover/deploy-from-github@v1.1.2 before
      - name: Install CapRover deploy client
        run: npm install --global caprover@2.3.1
      - name: Prepare production handoff
```

```yaml
      - name: Record production boot
```

```yaml
          caprover deploy \
            --caproverUrl "$CAPROVER_SERVER" \
            -i "$IMAGE_URL" >caprover-deploy.log 2>&1 &
          # otherwise stopping it here reports a false 0ms outage.
```

"Prepare production handoff" is the `FLUSH` / `HANDOFF` sequence from Chapter 22,
run *before* the deploy so the predecessor exits in constant time. "Record
production boot" measures the outage — and the comment on the backgrounded deploy
notes that stopping the measurement at the wrong moment "reports a false 0ms
outage", which is the kind of self-measurement bug that makes a deploy look
better than it is.

The CLI is pinned to an exact version rather than using the action, so a
transitive action update cannot change deploy behaviour.

**Deployment target.** A Docker Swarm service `srv-captain--timefusion` on a
CapRover host, image tagged with the git short SHA:

```bash
ssh ubuntu@captain.s.past3.tech 'docker service ls | grep timefusion'   # what is live
ssh ubuntu@captain.s.past3.tech 'docker service logs srv-captain--timefusion --since 30m'
ssh ubuntu@captain.s.past3.tech 'docker service ps srv-captain--timefusion --no-trunc'  # restarts/OOMs
```

⚠️ **Read-only.** Never restart, scale, redeploy, `exec`-mutate, or touch volumes
from that shell. And heavy ad-hoc `SELECT`s against `otel_logs_and_spans` can
themselves push the memory-tight instance into an OOM — prefer `timefusion_stats`
and tightly time-bounded queries.

⚠️ **Any non-docs push restarts production.** `CLAUDE.md` records it, and it is the
single most important operational fact about this repository's workflow: merging
to master *is* deploying.

### The other workflows

| Workflow | Purpose |
|---|---|
| `ci.yml` | fmt, clippy, sharded test, pgwire smoke, e2e |
| `deploy.yml` | build → push → smoke → handoff → CapRover → record boot |
| `build-image.yml` | image build without deploy |
| `autoformat.yml` | applies `cargo lint-fix` |
| `emergency-rollback.yml` | redeploy a previous tag |
| `simd-bench.yml` | guards the `target-cpu` regression |
| `claude.yml`, `claude-code-review.yml` | assistant integrations |

### Running locally

```bash
docker compose up                      # MinIO + bucket + TimeFusion, zero config
docker compose up -d minio createbucket  # MinIO only, then `cargo run`
psql "postgresql://postgres:postgres@localhost:5432/postgres"
```

**Iteration tips**, from `CLAUDE.md` and measured:

- Debug builds compile 5–15× faster; use them for correctness work.
- `RUSTFLAGS="-C debuginfo=0"` drops minutes off a release LTO/codegen cycle.
- MinIO can stay running across iterations; per-run isolation via
  `TIMEFUSION_DATA_DIR=./data/run-N` beats wiping the MinIO data directory.
- **Never `cargo clean`** unless truly necessary — it forces every dependency to
  rebuild.
- ⚠️ `target/` has reached 200 GB+. Check `df -h` before trusting a suite failure;
  a full disk produces TLS and S3 errors that look like network problems.

**Key takeaways.** One lint definition shared by CI and the local alias, with
`--all-features --locked -D warnings` because a bare `cargo clippy` passes on code
CI rejects. Always nextest, because process-per-test is both 7× faster and what
makes the process-global state safe. `make prepush` runs fmt then lint then the
whole suite, in CI's order. The image is a four-stage `cargo-chef` build into
distroless whose dependency cache silently invalidates if two env vars drift, and
whose `HEALTHCHECK` flags are pinned by a Rust unit test. Deploying is a push to
master, so the smoke test runs against the *pushed image* before CapRover ever
sees it — and the production host is strictly read-only.
## 27. Observability of TimeFusion itself

*After this chapter you will know every signal TimeFusion emits about its own
behaviour — logs, spans, metrics, `timefusion_stats`, and profiles — where each
comes from, and which one to reach for first for a given class of problem.*

### Five surfaces

| Surface | Transport | Best for |
|---|---|---|
| **Structured logs** | stdout → `docker service logs` | what happened, once |
| **Spans** | OTLP traces | where a statement's time went |
| **Metrics** | OTLP | rates and trends over time |
| **`timefusion_stats`** | the SQL protocol | *right now*, without a collector |
| **Profiles** | files in the data volume | where heap or CPU went |

`timefusion_stats` is the distinctive one: in-process diagnostics exposed through
the database's own query protocol, so an operator needs no second port, no scrape
endpoint, and no credentials beyond the ones they already have.

### `timefusion_stats`

```sql
SELECT component, key, value FROM timefusion_stats;
SELECT component, key, value FROM timefusion_stats WHERE component = 'mem_buffer';
```

Three columns, one row per counter. It is a `TableProvider` in
`src/server/pg_compat.rs` (Chapter 24) that renders
`BufferedWriteLayer::snapshot_stats()`, the `ScanMetrics` atomics, and the local
metrics registry.

The components and what each answers:

**`mem_buffer` / write path.** `mem_project_count`, `mem_total_buckets`,
`mem_total_rows`, `mem_total_batches`, `mem_estimated_bytes`, `pressure_pct`,
`reserved_bytes`, `max_memory_bytes`, `rows_ingested_total`, `rows_flushed_total`,
`flush_freed_bytes_total`, `flush_completed_total`, `flush_failed_total`,
`backpressure_engaged_total`, `backpressure_rejected_total`,
`backpressure_force_flush_total`, `mem_buffer_oldest_bucket_age_seconds`,
`mem_replay_dml_noops`.

**`wal`.** `wal_files`, `wal_disk_bytes`, `quarantine_files`, `quarantine_bytes`,
`wal_shards_per_topic`, `wal_known_topics`, `orphaned_topics`, `orphan_pin_age`,
`bucket_duration_micros`.

**`scan`.** Every constant in `scan_metric_names` (Chapter 7): scan counts by leg
mix, the six dedup verdicts, certification grants and dwell, fast-resolve and
provider-cache hit rates, plan timing, decode bytes and throttling, wide-scan
rejections, and `pgwire.queries_total`.

**`maintenance`.** `pending_<operation>` per operation, `tasks_running`,
`tasks_complete`, `eligible_watermark_lag_seconds`, `rollup_min_contiguous_days`,
`rollup_tier_untagged`, `occ_conflicts_total`, `permit_wait_ms`.

**`rollup`.** `rollup_hits` (labelled `full`/`hybrid`), `rollup_misses` (labelled
by the fifteen `MissReason` variants), `raw_tail_duration_secs`.

**`cache`.** Per tier (`main`, `metadata`): `hits`, `misses`, `range_hits`,
`range_misses`, `bytes_served`, `inner_bytes_read`, `range_bytes_read`,
`ttl_expirations`, `inner_gets`, `inner_puts`, plus evictions.

**`dedup` / `read`.** `ordering_violations_total` and, under
`TIMEFUSION_ORDERING_PROBE`, the three per-leg counters.

`pgwire.queries_total` **resets on restart**, which — as `CLAUDE.md` notes — makes
it double as an OOM-restart detector: a counter that went backwards means the
process died.

### How one recorder serves both

Chapter 24 covered the mechanism; the operational consequence is worth stating
plainly. Every call site writes once through `metrics::counter!()` or
`histogram!()`. A `Fanout` layer sends it to the OTLP exporter *and* a local
registry. So:

- `timefusion_stats` and your dashboards **cannot disagree** — same source.
- `timefusion_stats` works with **no collector configured**, which is exactly the
  situation during an incident on a box you have read-only SSH to.
- Percentiles are DDSketch-backed (`histogram_quantile`), so `latency_percentile_us`
  and `pgwire_percentile_us` are real quantiles rather than bucket midpoints.

### Spans

| Span | Fields |
|---|---|
| `postgres.query.simple` / `.extended` | `query.text` (redacted template), `query.type`, `query.operation`, `db.system`, `db.operation` |
| `datafusion.execute` | child of the above |
| `datafusion.table.scan` | `table.name`, `table.project_id`, `scan.filters_count`, `scan.has_limit`, `scan.limit`, `scan.has_projection`, `scan.uses_mem_buffer`, `scan.skipped_delta`, `parquet.files`, `parquet.bytes`, `parquet.file_ids`, `parquet.selected_row_groups` |
| `datafusion.table.write` | `table.name`, `operation`, `rows.count`, `projects.count` |
| `delta_table.insert` | `project_id`, `rows` |
| `resolve_delta_table` | table resolution |

`datafusion-tracing` and `instrumented-object-store` add engine-level and
store-level spans beneath these.

`OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_SERVICE_NAME`, `OTEL_SERVICE_VERSION`, and
`OTEL_TRACES_EXPORTER` configure the exporter; the Docker image defaults the
endpoint to `http://srv-captain--otelcol:4317`.

### Logs

`LOG_FORMAT` selects plain or JSON. `RUST_LOG` sets the filter (default `info`).

The log lines worth alerting on, by severity:

**Data loss or imminent loss**

- `"dml quarantine: … rows LOST"` — the quarantine write itself failed.
- `"Write backpressure exhausted after …: rejecting batch (NOT yet durable …)"` —
  a write was dropped; the caller must retry.
- `"Quarantined WAL entry to …"` — an entry could not be decoded; parked.
- Non-zero `quarantine_files` — deferred loss.

**Degradation that will compound**

- `"WAL over threshold (files …, unflushed …MB, disk …MB), triggering emergency flush"`
- `"Pressure relief made no progress: … Delta flush committed nothing this round"`
- `"Schema declares indexed columns but AWS_S3_BUCKET is unset — Tantivy disabled, queries will scan"`
- `"tantivy reconcile FAILED (indexes stale until the next reconcile or server backfill)"`
- A `reconcile_estimated_bytes` drift warning.

**Deploy problems**

- `"WAL dir … is locked by another TimeFusion process"` at `error!` (past ~60 s).
- `"WAL takeover requested 180s ago and this instance never reached handoff readiness"`
- `"PGWire drain exceeded its slice of the stop grace"`
- No `"Shutdown complete."` — SIGKILL arrived first.

**Client-caused**

- `"dropped N rows with event timestamps outside [2000-01-01, now+48h]"` — a unit
  error upstream; the statement still succeeded.
- `"giant statement (N B) queued … behind the 2-permit parse gate"`
- `event = "pgwire.slow_statement"` — over one second.

**Boot**

- The `bootstrap.phase=` sequence (Chapter 9) and
  `"self-sizing budget tree derived at startup"`.

### The slow-statement event

One bounded event per statement over one second, with literals redacted and a
stable fingerprint (Chapter 13, Listings 13.11–13.12):

```
event=pgwire.slow_statement
  query.class=SELECT
  query.fingerprint=<sha256 of the normalized text>
  query.template="select * from logs where project_id = ? and body = ? and n = ?"
  query.tables="otel_logs_and_spans"
  project.id="prod-api-001"
  protocol=simple
  duration_us=2145000
  success=true
```

The fingerprint is stable across parameter values, so grouping by it gives you
per-shape latency without a trace backend. `project.id` *is* logged — the tenant is
operationally necessary — while the payload literals are not.

### Profiling

Under `--features profiling` on Linux (which the production image enables):

**Heap.** jemalloc's own profiler, configured through the exported `malloc_conf`
symbol (Chapter 9, Listing 9.1). Sampling is compiled in but **off** by default;
arm it at runtime via the `prof.active` mallctl. Dumps land at
`/app/data/timefusion/profiles/jeprof.*.heap` and are analyzed with
`jeprof --svg <binary> <prefix>.*.heap`.

The vendored `tikv-jemalloc-sys` patch (Chapter 4) is what makes those dumps
useful — with upstream's libgcc unwinder, every production heap dump was one
anonymous frame.

**CPU.** A `pprof` sampler, started in `observability::start`, on by default.
⚠️ Set `TIMEFUSION_CPU_PROFILE=false` if the process crash-loops on `exit 139` with
`starting cpu profiler` as the last log line — signal-handler plus libunwind code
running at boot is exactly the shape of a SIGSEGV with no Rust panic, and it
happened in production on 2026-08-11.

**Runtime lag.** `spawn_runtime_lag_sampler` measures Tokio scheduling delay, and
is started *before* WAL replay because replay is precisely the window where a
health probe deadline gets missed.

### Choosing a surface

| Symptom | Start with |
|---|---|
| Queries slow | `timefusion_stats` scan counters, then `EXPLAIN`, then the scan span |
| Queries wrong | dedup verdicts, `ordering_violations_total`, the mem-buffer `warn!` |
| Writes rejected | mem_buffer pressure counters, then the backpressure log lines |
| Memory climbing | `pressure_pct` + `decode_pressure_throttled`, then a heap profile |
| Maintenance stalled | `pending_*` vs `tasks_running` vs `tasks_complete`, then `sim` |
| Rollups not used | `rollup_misses` by reason, then `rollup_miss_sampled` |
| Deploy stuck | the WAL lock log lines |
| Boot slow | the `bootstrap.phase=` sequence |

### Measurement traps

This codebase has an unusual number of documented cases where the *instrument* was
wrong, and they are worth internalizing before quoting a number.

**`tasks_complete` is a gauge, not a counter.** Do not treat it as monotonic.

**`pgwire.queries_total` resets on restart.** Useful as a restart detector,
misleading as a rate.

**Foyer's stats snapshot yields defaults under contention.** `src/storage.rs:387–388`:
the lock-free snapshot returns default counters rather than blocking, so a
*busy* cache can report zeros. `hits = 0` on the busiest cache is a statement about
the diagnostics path, not the cache.

**A derived-operation queue disagrees with storage.** Chapter 17: 2,218 pending
consolidation units against 108 partitions actually out of policy. When a derived
queue and an audit disagree, the audit is right.

**A probe returning the same extreme on every row is a bug in the probe.**
`CLAUDE.md` records three such scripts in one day.

**Single-pass measurement ladders inflate every number.** The recorded technique
is 10 interleaved passes plus a control query, because single-pass ladders
inflated results 2–6×.

**A no-op maintenance pass logs nothing**, so "did not run" and "found nothing"
are indistinguishable (Chapter 21). If you need to know a cron fired, make it say
so.

### The dashboards

`docs/dashboards/` holds two standalone HTML dashboards — a historical-compaction
chart (per-project, per-day file counts) and an intra-day maintenance page
(ingest/flush, hot-tail compaction, dedup queue, crons, read path, OOM banner).
Both are built from `timefusion_stats` and prod queries rather than from a metrics
backend, which is consistent with the rest of the design: the database is its own
diagnostic surface.

**Key takeaways.** Five surfaces, one recorder. `timefusion_stats` is the first
stop because it needs no collector and cannot disagree with the exported metrics.
Logs are structured and the high-severity ones are greppable by design (`LOST`,
`NOT yet durable`, `made no progress`). Heap and CPU profiling are compiled in and
armable at runtime, with the CPU sampler being the first thing to disable on a
boot SIGSEGV. And several of this system's instruments have been wrong in
documented ways — check the trap list before quoting a number.
## 28. Security and multi-tenancy boundaries

*After this chapter you will know exactly what TimeFusion's authentication model
is, what its tenancy boundary is and is not, where secrets live, and — stated
plainly — what the threat model does not cover.*

### Authentication: one user, cleartext, fail-closed

There is **one** account. `PGWIRE_USER` (default `postgres`) and
`PGWIRE_PASSWORD`, validated by `ConfigAuthSource`:

**Listing 28.1 — src/server/mod.rs:223–243**

```rust
/// AuthSource that validates against configured credentials
#[derive(Debug, Clone)]
pub struct ConfigAuthSource {
    config: AuthConfig,
}

impl ConfigAuthSource {
    pub fn new(config: AuthConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl AuthSource for ConfigAuthSource {
    async fn get_password(&self, login: &LoginInfo) -> PgWireResult<Password> {
        let username = login.user().unwrap_or("");
        (username == self.config.username).then(|| Password::new(None, self.config.password.clone().unwrap_or_default().into_bytes())).ok_or_else(|| {
            PgWireError::UserError(Box::new(ErrorInfo::new("FATAL".into(), "28P01".into(), format!("password authentication failed for user \"{username}\""))))
        })
    }
}
```

No roles, no per-tenant credentials, no `GRANT`. Anyone who authenticates can
query and write any project's data. **Tenant isolation is the application's job,
not TimeFusion's** — the client is trusted to send the right `project_id`.

The password is **required** unless explicitly waived, and the reasoning is stated
where the config is built (Chapter 9, Listing 9.20): pgwire's cleartext handler
treats `None` as "accept any password", which on a `0.0.0.0` bind is an open
ingest endpoint. So an unset password is a *startup failure*, and
`TIMEFUSION_ALLOW_INSECURE_AUTH=true` logs a warning on every boot.

⚠️ **Cleartext.** `CleartextPasswordAuthStartupHandler` sends the password in the
clear unless TLS is negotiated. `ServerOptions` carries TLS configuration
(`serve_with_listener`'s doc notes that `options` contributes "TLS config and
connection-limit settings"), and `tokio-rustls` is a dependency — but nothing in
the production `Dockerfile` or `.env` configures a certificate. Treat the pgwire
port as needing network-level protection.

The production endpoint is `timefusion.s.past3.tech:5432`, and
`deploy/timefusion-pgwire-proxy.cfg` suggests a proxy in front of it.

### Tenancy: a partition key, not a boundary

`project_id` is:

- a Delta **partition column**, so a query filtering on it reads only that
  tenant's directory;
- the **routing key** that selects a unified table or a custom project table;
- the WAL **topic** component;
- the key for every per-tenant map in `Database`.

`project_id` is **not**:

- an authorization boundary — there are no per-tenant credentials;
- automatically applied — a query without it scans across tenants
  (`README.md:80–81`).

Two isolation levels exist:

| Level | Path | Isolation |
|---|---|---|
| Unified (default) | `s3://bucket/prefix/default/{table}/project_id=.../` | partition only |
| Custom project | `s3://bucket/prefix/projects/{project_id}/{table}/` | separate table, own storage config |

A custom project can carry its own bucket, endpoint and credentials via the
`timefusion_projects` table (`TIMEFUSION_CONFIG_DATABASE_URL`), which is genuine
storage-level separation. Default projects share one Delta table and are separated
by the partition column alone.

### Where tenancy is *enforced* in code

Chapter 16 covered the important one, and it deserves repeating here because it is
the closest thing to a security control in the codebase:

**Listing 28.2 — src/dml.rs:986–995**

```rust
    // `project_id` is STRIPPED from the DML predicate (it is routing
    // information, consumed by `extract_dml_info`), so it must be put back as a
    // row filter here. Routing alone is not tenant isolation: every default
    // project shares ONE unified Delta table, so without this conjunct an
    // UPDATE scoped to one tenant rewrites the matching rows of every tenant in
    // that table. It also prunes, which is why the in-place Delta leg
    // re-augments its own predicate the same way.
    let tenant = Expr::Column(Column::from_name("project_id")).eq(lit(project_id));
```

The generalizable rule: **a value that serves both routing and filtering must be
re-applied as a filter after routing consumes it.**

The same hazard appears in cohort rollup builds (Chapter 18, Listing 18.15): the
split must use the aggregate's *output* `project_id`, because synthesizing it from
the request "would mix tenant identities".

And the scan guard (Chapter 13, Listing 13.13) refuses unbounded raw OTel scans
in `Enforce` mode with the message: *"raw OTel queries require project_id = <value>
and a timestamp lower bound or LIMIT"*. That is a resource guard rather than an
authorization control — but it does make a cross-tenant scan hard to issue by
accident. ⚠️ Its default is `Off`.

### Secrets

**Object-storage credentials** come from `AWS_ACCESS_KEY_ID` /
`AWS_SECRET_ACCESS_KEY` (process-wide) or per-project rows in
`timefusion_projects`.

**Per-project credentials may be encrypted at rest** with AES-256-GCM under
`TIMEFUSION_CONFIG_ENCRYPTION_KEY` (Chapter 25, Listings 25.2–25.4), stored as
`enc:v1:<base64(nonce||ct||tag)>`, with plaintext passthrough for incremental
rollout.

**They are redacted on both output paths:**

**Listing 28.3 — src/database/mod.rs:2023–2044**

```rust
struct StorageConfig {
    project_id: String,
    table_name: String,
    s3_bucket: String,
    s3_prefix: String,
    s3_region: String,
    /// Skipped on serialize so credentials never leak through serde-based dumps
    /// (debug endpoints, metrics serialization, etc.). sqlx::FromRow bypasses
    /// serde so DB-row loading is unaffected. `#[debug("[redacted]")]` keeps
    /// them out of `{:?}` log lines.
    #[serde(serialize_with = "redact_str")]
    #[debug("[redacted]")]
    s3_access_key_id: String,
    #[serde(serialize_with = "redact_str")]
    #[debug("[redacted]")]
    s3_secret_access_key: String,
    s3_endpoint: Option<String>,
}

fn redact_str<S: serde::Serializer>(_: &str, ser: S) -> std::result::Result<S::Ok, S::Error> {
    ser.serialize_str("[redacted]")
}
```

Both paths covered — serde (for any dump or metrics serialization) and `Debug`
(for `{:?}` in a log line) — and the comment notes that `sqlx::FromRow` bypasses
serde, so *loading* still works. Covering only one of the two paths is the usual
way credentials leak.

Storage options are also filtered before being logged:

**Listing 28.4 — src/database/mod.rs:2419–2428**

```rust
    /// Build storage options with consistent configuration for S3.
    fn build_storage_options(&self) -> HashMap<String, String> {
        let storage_options = self.config.aws.build_storage_options(self.default_s3_endpoint.as_deref());

        // debug! (not info!) because this is called on every insert path —
        // info-level logging here would flood production logs.
        let safe_options: HashMap<_, _> = storage_options.iter().filter(|(k, _)| !k.contains("secret") && !k.contains("password")).collect();
        debug!("Storage options configured: {:?}", safe_options);
        storage_options
    }
```

⚠️ The filter is a substring match on `"secret"` and `"password"`. It catches
`AWS_SECRET_ACCESS_KEY` but would not catch a differently-named credential key —
`AWS_ACCESS_KEY_ID` is logged. An allow-list would be safer than a deny-list here.

### What is not logged

Query *literals* are redacted from the slow-statement event (Chapter 13,
Listing 13.11): strings, dollar-quoted strings, numbers and comments all become
`?`, and the test asserts that a literal `'secret'` does not survive. Tenant ids
*are* logged, deliberately.

⚠️ Full SQL **is** captured in span fields as `query.text` — but that field carries
the redacted `query_template`, not the raw statement. Debug-level logs may still
include more.

### Files and permissions

WAL quarantine files hold raw user data that failed to deserialize, and are
written owner-only:

**Listing 28.5 — src/write/mod.rs:72–94**

```rust
/// Create `path` for writing with owner-only (0600) permissions on Unix.
/// `exclusive` fails if the file already exists — use it where an existing file
/// means a name collision that must not silently overwrite user data.
pub(crate) fn create_owner_only(path: &std::path::Path, exclusive: bool) -> std::io::Result<std::fs::File> {
    let mut opts = std::fs::OpenOptions::new();
    // `create_new` supersedes create/truncate, so the flags are just !exclusive.
    opts.write(true).create_new(exclusive).create(!exclusive).truncate(!exclusive);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    opts.open(path)
}

/// Write raw bytes with owner-only (0600) permissions on Unix; plain write
/// elsewhere.
pub(crate) fn write_owner_only(path: &std::path::Path, contents: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    let mut f = create_owner_only(path, false)?;
    f.write_all(contents)?;
    f.sync_all()
}
```

with the call site stating why:

**Listing 28.6 — src/write/mod.rs:152–156**

```rust
    // Quarantine files contain raw user data that failed to deserialize —
    // write with mode 0600 so they're not world-readable on shared hosts.
    if let Err(e) = write_owner_only(&path, &entry.data) {
```

The `exclusive` flag prevents a name collision from silently overwriting user
data — a small correctness property riding along with the permission one.

⚠️ **File ownership has caused an outage.** `CLAUDE.md` records a root-owned WAL
directory after a health-kill, which made replay SEGV. The container runs as
`nonroot`; anything that writes into the data volume as root breaks the next boot.

### The container

`gcr.io/distroless/cc-debian12:nonroot` — no shell, non-root by default, minimal
attack surface. `ENTRYPOINT` is the binary directly.

### Input handling

**Bounded parsing.** The early-bind responder caps a startup message at 64 KiB
(Chapter 9, Listing 9.10) and uses `checked_sub` on the length arithmetic. The
health probe caps an `ErrorResponse` at 64 KiB. WAL entries are capped at
`MAX_BATCH_SIZE` (1 GiB) on both append and replay. `DELTA HISTORY`'s limit is
clamped to 1..10 000.

**Bounded concurrency.** The early-bind acceptor caps handlers at 512; the
giant-statement gate caps concurrent multi-megabyte parses at 2; `heavy_scan_sem`
caps concurrent wide-scan decodes.

**Admin commands** (`OPTIMIZE`, `VACUUM`, `FLUSH`, `HANDOFF`, `DELTA *`) have no
separate authorization — any authenticated client can run them. `FLUSH` is
rate-limited to once per 10 s specifically as a misuse guard. `OPTIMIZE` requires a
date filter. `DELTA HISTORY` / `ACTIONS` / `RECOVERY AUDIT` are read-only by
construction.

**Test-only UDFs** that can move the process clock are gated behind
`TIMEFUSION_ENABLE_TEST_UDFS` with a `warn!`, precisely "so a production
deployment can't have its eviction/flush clock yanked by a stray SQL session"
(Chapter 14, Listing 14.21).

### `unsafe`

Three uses in the crate, all justified inline:

1. `raise_file_limit` — two `libc` rlimit calls with a `SAFETY` comment
   (Chapter 9, Listing 9.3).
2. `#[unsafe(export_name = "malloc_conf")]` — exporting a symbol jemalloc reads at
   its own initialization (Chapter 9, Listing 9.1).
3. `std::env::set_var` for the budget profile — with a `SAFETY` comment stating
   that no threads exist yet (Chapter 9, Listing 9.2).

The codebase's own guidance is to prefer `ArrayData::try_new` over
`new_unchecked` for Arrow construction, so that corrupted buffers produce an error
rather than undefined behaviour.

### The honest threat model

**What TimeFusion defends against:**

- An unauthenticated network client (password required, fail-closed).
- Malformed or hostile protocol input (bounded parsing, bounded concurrency).
- Corrupted WAL entries (magic, version, size caps, quarantine).
- Resource exhaustion by a single expensive statement (giant-statement gate,
  statement timeout, scan admission, decode pressure valve).
- Credential leakage through logs and serialization.
- A second process corrupting the WAL (exclusive `flock`).

**What it does not defend against:**

- **A tenant reading another tenant's data.** There is one credential. Any
  authenticated client can query any `project_id`. This is by design — TimeFusion
  is a backend for a multi-tenant application that does its own authorization.
- **Network eavesdropping**, unless TLS is configured; the default handler is
  cleartext.
- **A malicious authenticated client.** It can `VACUUM`, `OPTIMIZE`, `FLUSH`,
  `DELETE` any tenant's rows, or issue queries designed to exhaust memory.
- **Denial of service from a single tenant.** The guards are process-wide, not
  per-tenant; there is no per-tenant quota or rate limit.
- **Data at rest.** Parquet in the bucket is unencrypted by TimeFusion; use
  bucket-level encryption.

If you are deploying this, the checklist is: keep 5432 off the public internet,
put TLS in front of it, set `PGWIRE_PASSWORD` (never
`TIMEFUSION_ALLOW_INSECURE_AUTH`), set
`TIMEFUSION_CONFIG_ENCRYPTION_KEY` if you use per-project credentials, enable
bucket-level encryption, and do your tenant authorization in the application layer
above.

**Key takeaways.** One credential, cleartext by default, fail-closed on an unset
password. `project_id` is a partition and routing key, *not* an authorization
boundary — and the one place it is genuinely enforced is the DML tenant conjunct
that must be re-added after routing consumes it. Secrets are AES-256-GCM at rest
and redacted on both the serde and `Debug` paths, though the storage-options log
filter is a deny-list rather than an allow-list. Parsing and concurrency are
bounded throughout. The model assumes a trusted authenticated client and a network
boundary you provide.
## 29. The honest chapter

*Everything a maintainer would whisper to a new hire. This is my assessment,
built only from evidence in the repository — doc comments, `TODO`s, contradictions
between files, and code whose behaviour disagrees with its own documentation. I
name the evidence for each so you can check it.*

The headline first: **this codebase is in unusually good shape for a pre-1.0
single-team database.** Its comments are forensic, its constants are justified,
and it records rejected hypotheses alongside accepted ones. Most of what follows
is documentation drift and half-landed work, not rot.

### 1. `CLAUDE.md` has drifted from the tree

`CLAUDE.md` is 39 KB of architectural and operational instruction, and it is the
first thing a new contributor reads. Several of its claims no longer hold.

| `CLAUDE.md` says | The tree has |
|---|---|
| `src/maintenance/{mod,rollup,hot_tier}.rs` | `src/maintenance_coordinator.rs`, `src/rollup.rs`, `src/hot_tier.rs` at top level |
| `src/database/{rollup,scan}.rs` | neither exists; `database/` has 4 files |
| `proto/`, `build.rs`, gRPC ingest on `:50051`, `GRPC_TOKEN` | no `proto/`, no `build.rs`; `grep -rn "grpc\|tonic" src/` matches only the OTLP exporter |
| `ci/checks.tsv`, `scripts/ci/ci.sh`, `make ci`, `make ci-status`, `make ci-selftest` | none exist; no `ci` target in the `Makefile` |
| `BUCKET_DURATION_MICROS = 10 min` | 5 min compiled default, runtime-configurable |
| `MEMORY_OVERHEAD_MULTIPLIER = 1.2` | `1.15` (`src/write/mod.rs:35`) |
| `WAL_VERSION = 128` | `1` (`src/write/wal.rs:76`) |
| `TIMEFUSION_BUFFER_FLUSH_INTERVAL_SECS` default 600 | `timefusion_flush_interval_secs` default 60 |
| `TIMEFUSION_PLAN_CACHE_CAPACITY` default 1024 | 2048 (`src/config.rs:2162`) |
| `TIMEFUSION_MAINTENANCE_REWRITE_CONCURRENCY` (env) | pinned constant `HEAVY_REWRITE_PERMITS = 10` |
| `HEAVY_REWRITE_PERMITS = 4` (in one passage) | 10 |

**Assessment.** The gRPC and local-CI sections are the costly ones — a
contributor could spend an afternoon looking for `ci/checks.tsv`. The constant
drift is minor but corrosive: once a reader finds two wrong numbers, they stop
trusting the file, which is worse than the numbers.

**Fix:** delete the absent sections, and consider generating the constants table.

### 2. The known write-loss seam

The clearest defect in the codebase, and it is labelled as such:

**Listing 29.1 — src/write/mod.rs:1040–1051**

```rust
                        // NOTE: this rejection happens in `insert()` BEFORE
                        // `wal.append_batch`, so the batch is NOT durable here —
                        // the old "data remains in WAL" wording was wrong. The
                        // batch is dropped from TF's side and recovery depends on
                        // the caller retrying / the upstream DLQ. Removing this
                        // loss seam is parity-plan Defect 1 (WAL-before-admit).
```

A write whose backpressure budget is exhausted is rejected *before* the WAL
append, so it is not durable. The mitigation (`wal_admit_decouple`) exists, is
default-off, and is marked "soak before prod enable"
(`src/write/mod.rs:1163–1168`). Two tests pin both sides
(`src/write/mod.rs:5713`, `:5728`).

**Assessment.** Correctly identified, correctly labelled, mitigated behind a flag,
not yet fixed. The right fix — append to the WAL before admitting to memory — is
named. Until it lands, the upstream DLQ is load-bearing.

There is a second, narrower loss path in the DML quarantine (Chapter 16): batches
whose schema does not match their group cannot be parked, and the code says to
**page on it** because the recoverable-looking `quarantined` metric would
otherwise hide real loss (`src/dml.rs:1874–1878`).

### 3. A subsystem being deleted mid-book

`src/hot_tier.rs` (2,118 lines) is fully wired at the pinned commit `69bdfcd` and
is being removed on the `remove/hot-tier` branch, which also deletes
`tests/e2e/hot_tier.rs` and `docs/plans/2026-08-21-hot-leg-pruning.md`.

**Assessment.** This is the codebase working correctly — a derived subsystem was
measured, found to be a net loss (6× slower than indexed Parquet through Foyer,
while taking disk *from* Foyer), and removed. Chapter 20 covers it in full.

The residue to expect after the removal lands: `LegKind::Hot` and its ordering
counters, `version_gate` on the Delta exclusion, the `demote_*` fields on
`BufferedWriteLayer`, `CoreConfig::hot_tier_dir()`, and the `timefusion_hot_tier_*`
config fields. Grep for `hot_tier` and `LegKind::Hot` after pulling.

### 4. Comments that contradict the code beside them

Each of these is one stale sentence, and each would mislead a reader.

**`recompress --project`.** The function doc calls `--project` "the honest unit of
repair" and `main.rs:922–924` says it "is what makes the job small enough to run
on an ordinary runner". The first statement of the function rejects it:

**Listing 29.2 — src/database/compact.rs:637–641**

```rust
        let date_str = date.to_string();
        if project.is_some() {
            // Scoped `replace_where` can deadlock; reject it before reading the table.
            anyhow::bail!("recompress --project is disabled: scoped replace_where deadlocks; re-run without --project");
        }
```

**Bounded dedup's default.** `bounded_dedup_enabled`'s doc says "Defaults ON";
`detect_bound`'s says the flag "defaults OFF until the footer repair has drained
the poisoned files". `src/config.rs:1928–1929` settles it: `true`.

**`version_append`'s state.** `src/schema.rs:951–953` says "`version_append` is OFF
since 2026-08-02" two lines above `assert!(schema.version_append, "{name} ships
merge-on-read")`. The YAML has `version_append: true` with a comment saying it was
*re-enabled* on 2026-08-02. The test comment is the stale half.

**`RollupMeasure::agg`.** The doc lists `count | sum | min | max`; `validate`
accepts six, including `tdigest` and `hll`, both in production use
(`src/schema.rs:168`).

**A sibling module that does not exist.** `src/observability.rs:3` says it "sits
next to `telemetry.rs` (which owns traces)". There is no `telemetry.rs`;
`init_telemetry` is in the same file.

**`META_DIR`'s doc comment** opens with "Magic bytes to identify the WAL format
(\"WAL2\")" — the sentence belongs to `WAL_MAGIC`, declared 20 lines later
(`src/write/wal.rs:44–48`).

**Hot-tier retention.** `timefusion_hot_tier_enabled`'s doc says "Past this age a
demoted file is unlinked" and that `skip_for_lookback` "reads the tier's MEASURED
span"; the code below has no time retention, and `skip_for_lookback` explicitly
*rejects* keying on the measured span (`src/hot_tier.rs:124–130`).

**Assessment.** Seven stale sentences in ~78,000 lines is a good ratio. The
`recompress --project` one is the only operationally dangerous member — an
operator following the comment gets a hard error rather than a wrong result.

### 5. Duplicated logic that can drift

**The cursor-reconciliation decision** is implemented twice, in `main.rs:496–563`
and `src/server/mod.rs:84–103`, and the second says "Mirror main.rs". They already
differ: `main.rs` logs richer diagnostics and handles restore failure with a
`warn!`; `bootstrap` uses `unwrap_or(false)`.

**`dml_completion`** is a hand-copy of a `pub(super)` function from
`datafusion-postgres`, with a `RE-SYNC-DML-COMPLETION` marker and a checklist
noting that divergence is *silent* (`src/read/plan_cache.rs:356–366`).

**Assessment.** The second is handled about as well as copying can be. The first
is a real drift risk on a startup path that decides between a 0-second and a
6.5-minute boot.

### 6. Numbers that disagree with each other

**Decode inflation.** `timefusion_dedup_decode_inflation` is 12;
`COORDINATOR_L0_SORT_TARGET_BYTES`'s comment states production zstd expansion is
"about 17x" (`src/database/mod.rs:6131–6134`). A 12× assumption against 17× reality
under-estimates decoded bytes by ~40%.

**`MIN_DISK_BLOCK_BYTES`** is described as matching "the legacy default
(`timefusion_foyer_file_size_mb`)" at 4 MB; that field's default is 32 MB.

**Assessment.** The inflation factor is worth re-measuring; it feeds dedup unit
sizing, which feeds the queue shape.

### 7. Config knobs whose defaults tell a story

Several defaults were flipped after an incident and left in the "safe" position:

| Knob | Default | What it means |
|---|---|---|
| `timefusion_otel_scan_guard` | `Off` | the unbounded-scan guard is not enforcing |
| `timefusion_tantivy_backfill` | `false` | index coverage does not self-heal at boot |
| `timefusion_flush_coalesce_commits` | `false` | "needs a soak before default-on" |
| `timefusion_wal_admit_decouple` | `false` | the loss-seam mitigation |
| `timefusion_rollup_enabled` / `_read_enabled` | `false` | rollups are opt-in |
| `timefusion_compact_dedup_merge` | `false` | |
| `timefusion_dedup_sweep_fallback` | `false` | |
| `timefusion_hot_tier_merge_demote` | `false` | reverted "pending pooled decode" |
| `timefusion_optimize_use_zorder` | `false` | |
| `timefusion_warm_full_files` | `false` | |

**Assessment.** Ten off-by-default flags is a lot of latent surface. Each was
presumably right at the time; collectively they mean the shipped configuration and
the *designed* configuration are different systems. Worth an audit asking, per
flag: is this ever going to be turned on, or is it dead?

`timefusion_rollup_enabled` defaulting to false while Chapters 17–18 describe
rollups as a central subsystem is the most striking.

### 8. Unbounded in-memory maps

Several `Database` fields grow with unique keys seen since process start and never
evict. `fast_resolve_cache` and `delta_provider_cache` say so explicitly ("No
eviction: grows with unique pairs seen since process start"), guarded only by a
soft log warning at 10,000 (`CACHE_SOFT_LIMIT_WARN`). And there is a `TODO`:

**Listing 29.3 — src/database/maintain.rs:3055**

```rust
        // TODO: same unbounded-growth caveat as `last_written_versions`.
```

**Assessment.** Bounded in practice by tenant count, and the design target is
"thousands of tenants" against a 10,000 warning. It becomes a real problem under
tenant churn — many short-lived project ids — which the warning would surface as
growing log frequency rather than a spike. Acceptable, documented, worth watching.

### 9. Explicit follow-ups

**Listing 29.4 — src/write/mod.rs:530–533**

```rust
    /// FOLLOW-UP: handles aren't stored; graceful shutdown does not await
    /// in-flight tantivy uploads. Acceptable for now because the sidecar is
    /// best-effort and the index can be rebuilt from Delta on demand.
    tantivy_spawn_sem: Arc<tokio::sync::Semaphore>,
```

**Listing 29.5 — src/read/functions.rs:801**

```rust
        Arc::new(move |_: &[ColumnarValue]| not_impl_err!("jsonb_array_elements is not yet fully implemented - requires table function support"));
```

**Assessment.** Both correctly scoped. The first is genuinely acceptable given
rebuildability. The second is a registered function that always errors — better
than silently wrong, but a client discovers it at runtime.

### 10. Surprising couplings

**A flush sort that cannot get a memory slice writes an unsorted file**, which
disables the reader's all-or-nothing footer ordering for the *entire partition*,
which can push merge-on-read dedup from bounded (64 MiB/run) into unbounded
(2 GiB/query) mode. So an ingest-side memory shortage becomes a read-side memory
blow-up, hours later, in a different subsystem. The only signal is
`ordering_violations_total`. Chapters 12, 15, 19.

**One `EmptyExec` leg vetoes the `SortPreservingMerge`**, which drops `DedupExec`
to full-set mode, which gave *the most selective point lookups in the system* a
2 GiB ceiling (`src/database/mod.rs:8756–8763`).

**A delta-rs `OPTIMIZE` over a rollup tier strips the slice tags**, and an
untagged file can never be matched by a replace-set — so rebuilds stack versions
beside it forever. Tags are load-bearing metadata that a stock operation destroys.

**Certification couples maintenance to read latency.** If dedup never commits,
`record_certification` never fires, so every plan keeps its `DedupExec` and rollup
routing loses its certified prefix. A maintenance stall presents as a query
regression.

**Assessment.** These are inherent to a system where storage layout is shared
state between ingest, maintenance and reads. They are documented at each site; the
cross-references are what this book adds.

### 11. Style inconsistencies

**Argument parsing** is hand-rolled `while let` loops in six subcommands, with the
usage string duplicated. Justified once ("two-token flags need lookahead"), but
`clap` would remove ~150 lines.

**Error types are mixed.** `anyhow` in the write path and CLIs, `thiserror`
(`WalError`) in the WAL, `DataFusionError` on query paths, `deltalake::DeltaTableError`
underneath. Conversions cross those boundaries, which is why
`is_capacity_failure` has to match on *message strings*
(`src/maintenance_coordinator.rs:86–103`).

**Assessment.** The string matching is honest about itself and pinned by a test
against verbatim production text. It is still fragile in a way a typed error path
would not be.

**Two config paths.** A global `OnceLock<AppConfig>` and a per-`Database`
`Arc<AppConfig>`. Tests use the second exclusively; production uses both. `try_config()`
exists for construction paths that may run before init. This works, but "which
config am I reading" is a real question at some call sites.

### 12. Test-shaped observations

**One integration binary** means a single test's compile error blocks the whole
suite, and adding a file requires editing `main.rs`. Deliberate — the alternative
was 26 links per edit — but it surprises newcomers.

**`cold_start_under_five_seconds` asserts 30 s**, because ~10-way suite
parallelism inflates wall-clock ~3×. A latency benchmark whose assertion is 6× its
target measures very little.

**`dedup_compaction_test.rs` is 2,938 lines**, ~23% of the integration suite in one
file.

**Two source-text tests** (`workers_get_more_than_the_default_stack`,
`probe_worst_case_fits_the_docker_timeout`) assert on `include_str!` of source and
the `Dockerfile`. Unusual, and correct for pinning a cross-file budget — but they
break on formatting changes.

### 13. What I could not determine

⚠️ **Whether `timefusion_rollup_enabled` is true in production.** The default is
`false` and the chapters describe rollups as central. `.env.prod` was not read.

⚠️ **Whether TLS is configured in production.** `ServerOptions` supports it,
`tokio-rustls` is a dependency, nothing in the `Dockerfile` or `.env` sets a
certificate.

⚠️ **Whether the 03:30 Tantivy reconcile cron currently fires.** `CLAUDE.md`
records that it did not while the process was alive, and that a no-op pass logs
nothing.

⚠️ **The current production `target-cpu`.** A baseline SSE2 binary shipped for
months on an AVX-512 host; `simd-bench.yml` exists to keep it visible, but the
current value is a build argument.

### The overall assessment

**What is strong.** The comment culture — forensic, dated, with measurements and
rejected hypotheses. Enums instead of booleans wherever a caller would otherwise
guess. Failure modes deliberately made conservative (the bound in the dedup key,
`ALL_HOURS`, absent-means-no-claim). Single-source definitions with anti-drift
notes. A simulator that shares its policy definition with the server. Tests named
as property statements.

**What is weak.** Documentation drift in the file newcomers read first. Ten
off-by-default flags whose collective meaning is unclear. One acknowledged loss
seam. Duplicated startup logic on the path that decides a 6.5-minute boot. A few
numbers that disagree.

**What would worry me most.** Not any single item above — it is that the *density*
of "we measured this and it was not what we assumed" comments in
`maintenance_coordinator.rs` suggests the maintenance scheduler is still being
discovered rather than understood. Six silent refusals in one night, each queued
behind the others, is a system whose behaviour is emergent. The `sim` tool is the
right response, and the discipline `CLAUDE.md` records — measure, fix *one*,
re-measure — is the right process. But that subsystem is where the next surprise
will come from.

**Key takeaways.** The code is in good shape; the *documentation about* the code
has drifted, most consequentially in `CLAUDE.md`. One write-loss seam is known,
labelled, and mitigated behind a default-off flag. Ten feature flags default off,
which means the shipped and designed systems differ. The subtle couplings — a
flush-time memory shortage causing a read-time OOM hours later, a stock `OPTIMIZE`
destroying load-bearing tags, a maintenance stall presenting as a query regression
— are the ones to hold in your head, because none of them is visible from the
subsystem where the symptom appears.
## 30. Where to go next

*A suggested path for your first five contributions, and the ten questions to ask
the team.*

### Before anything: get the loop running

```bash
git clone … && cd timefusion
brew install llvm@15                      # ld64.lld, or delete .cargo/config.toml
curl -LsSf https://get.nexte.st/latest/mac | tar zxf - -C ~/.cargo/bin
docker compose up -d minio createbucket
make test                                  # ~74s warm
make prepush                               # the gate
```

Then read, in this order (Chapter 3's reading order, restated):
`src/lib.rs` → `src/main.rs` → `src/support.rs` → `src/schema.rs` +
`schemas/otel_logs_and_spans.yaml` → `src/config.rs`'s `DerivedBudget` →
`ProjectRoutingTable::scan`.

Then, before touching anything: `git log --oneline -50` and skim
`docs/plans/`. The plans carry status headers and post-hoc results, which makes
them the best record of *why* recent subsystems look the way they do.

### Contribution 1 — Fix the documentation drift

**Why first.** It is the highest value-per-risk change available, it forces you to
verify claims against the tree, and it removes the trap that will otherwise catch
the next person.

**What.** From Chapter 29 §1 and §4: delete `CLAUDE.md`'s gRPC and local-CI
sections, correct the module layout and the eight wrong constants, and fix the
seven stale doc comments — especially `recompress --project`, which currently
documents a feature the function rejects.

**How to verify.** Every number you write should come from a `grep` you ran.

**What you learn.** The module layout, the constants that matter, and the habit of
checking documentation against source.

### Contribution 2 — Add a test that pins something currently unpinned

**Why second.** It teaches the test infrastructure without requiring you to change
behaviour, and this project's bug-fix workflow is *test first* (`CLAUDE.md`, and it
is mandatory).

**What.** Pick one property from Chapter 29 that no test currently guards:

- `is_capacity_failure` against a fresh capture of production error text (there is
  a test; extend it).
- The `timefusion_dedup_decode_inflation` = 12 versus the observed 17× — write a
  test that computes the expansion from a real fixture file and fails if the
  constant is optimistic.
- A `.slt` case for a `pg_catalog` shape a client uses that has no coverage.

**Read first.** `tests/suite/main.rs` (the `mod` list — Cargo will not discover
your file otherwise), the `rs-minimal-tests` skill's ladder (doctest → property →
case table → snapshot → real-dependency integration → golden), and a few test
*names* to absorb the property-statement convention.

**Rules.** Never `set_var`. Never mock Delta, `object_store`, or Postgres — use the
real thing in a tempdir or MinIO. Everything a test needs to be unique comes from
its own config.

### Contribution 3 — Land a bounded diagnostic

**Why third.** Chapter 29's deepest concern is that the maintenance scheduler is
still being discovered. Diagnostics are what convert that into understanding, and
this codebase already treats them as first-class.

**What.** One of:

- **Make a no-op maintenance pass log that it ran.** Chapter 21's finding — "a
  no-op pass logs nothing", so "did not run" and "found nothing" are
  indistinguishable — is a two-line fix with real operational value.
- **Add the missing counter to a silent refusal.** Chapter 17 lists several
  predicates that decline with no counter and no log. `DedupSkipVerdict` is the
  model: an enum whose variants are the metric labels.
- **Surface a `timefusion_stats` row for something you had to infer.**

**Read first.** `src/observability.rs`'s registry macro, `scan_metric_names`,
`MissReason`'s strum labels, and `pg_compat.rs`'s `timefusion_stats` provider.

**What you learn.** The one-recorder fan-out, and why the metric names live in
`const`s.

### Contribution 4 — Fix a real defect end to end

**Why fourth.** By now you know the flows, the tests, and the diagnostics.

**Candidates, easiest first:**

- **De-duplicate the cursor-reconciliation decision** (Chapter 29 §5). Extract the
  four-case logic from `main.rs` and `server/mod.rs` into one function that both
  call. Contained, well-tested by the e2e restart tests, and removes a drift risk
  on the path that decides a 6.5-minute boot.
- **Audit the ten off-by-default flags** (Chapter 29 §7). For each: is it going to
  be turned on, or is it dead? Delete the dead ones. This is the kind of change
  that needs a conversation first — see the questions below.
- **Close the write-loss seam** (Chapter 29 §2). The right fix is named:
  WAL-before-admit. This is the highest-value change in the list and also the
  riskiest — the WAL hold protocol (Chapter 11) is the subtlest invariant in the
  system, and reordering the append relative to admission moves it. Do not start
  here.

**Process.** Reproduce as a failing test first, at the level closest to where the
bug manifests. `CLAUDE.md` is explicit that "obvious" fixes shipped without a
failing test have missed the actually-broken path more than once.

### Contribution 5 — Change a policy, using the tools

**Why last.** It requires everything above plus the measurement discipline.

**What.** A change to the maintenance scheduler: the operation cycle, a width in
the coarsen lattice, the sealed reservation ratio, a budget fraction.

**How, in order (`CLAUDE.md`'s ladder):**

1. **`timefusion sim`** against a real production journal. Backtest a known
   night's queue shape *first*, so you know the sim reproduces reality before you
   trust its prediction about a change.
2. **`timefusion run-unit`** for the per-unit cost decomposition.
3. **Staging**, because MinIO validates correctness and not cost — per-unit cost is
   object-store round trips.
4. **Production**, only after 1–3 agree. One change per deploy, behind an env kill
   switch, ≥2 hours quiet before trusting numbers, and name the `timefusion_stats`
   metric your change should move.

**The rule that matters most:** measure, fix **one**, re-measure. Six defects in
one night queued behind each other; fixing two at once tells you nothing about
either.

### Things to avoid early

- **`cargo clean`.** It rebuilds every dependency.
- **Adding a Cargo test target.** Each is a full link of a ~100 MB binary.
- **A new env knob** for something the budget tree derives. That is the drift class
  the tree was built to kill.
- **Widening a `caps` string, an `Exact` pushdown, or a memory bound** to make
  something pass. Chapter 7's three-valued pushdown logic and Chapter 19's
  eligibility tests are load-bearing.
- **Turning off `bounded_dedup` or the mutability enforcement** to fix a symptom.
  Both are the safe side of a correctness trade.
- **Pushing to master casually.** Any non-docs push restarts production.

### The ten questions to ask the team

1. **Is `timefusion_rollup_enabled` on in production?** The default is `false` and
   two chapters describe rollups as central. If it is on, where does the true
   configuration live?
2. **What is the plan for the write-loss seam?** Is `wal_admit_decouple` going to
   be enabled, or is WAL-before-admit scheduled?
3. **Which of the ten off-by-default flags are dead?** Each is latent surface with
   an untested path.
4. **Is TLS terminated in front of pgwire?** The default handler is cleartext.
5. **What is `target-cpu` on the production image right now?** A baseline SSE2
   binary shipped for months on an AVX-512 host.
6. **Did the local-CI attestation system exist and get removed, or was it planned
   and never landed?** `CLAUDE.md` documents it in detail.
7. **What replaced the hot tier's role?** Foyer plus the skip-Delta watermark, or
   is the recent-window path expected to be slower now?
8. **Is the 03:30 Tantivy reconcile cron firing?** It is the only backlog drain,
   and a no-op pass logs nothing.
9. **What is the actual decode inflation factor?** 12 and 17 are both in the
   codebase and it feeds dedup unit sizing.
10. **What is the intended scaling story?** Single-process is a stated directive.
    At 10× projects, which subsystem breaks first — the unbounded resolve caches,
    the coordinator queue, or the memory tree?

### Reading list, in order of value

1. `docs/plans/` — current design work with status headers and results.
2. `CLAUDE.md`'s "Throughput/scheduler iteration" section — the measurement ladder.
3. `RUNBOOK.md` — operating procedures.
4. `docs/WAL.md`, `docs/buffered-write-layer.md` — the write path in the project's
   own words.
5. `docs/VARIANT_TYPE_SYSTEM.md` — the two-schema trick.
6. `docs/CACHING.md`, `docs/DELTA_CHECKPOINT_HANDLING.md`.
7. `git log --oneline` on any file whose comment surprised you. The commit that
   introduced the comment usually has the measurement.

### A closing note on how to read this codebase

The comments are the design documents. When you find a constant that looks
arbitrary, read the comment above it — it will name a date, a measurement, and
usually a rejected alternative. When you find code that looks over-complicated,
the comment will name what the simple version broke.

The habit worth adopting: when you change one of these values, **add to the
comment rather than replacing it**. The record of what was tried and rejected is
worth as much as the current value, and this codebase's comments are the reason a
book like this one could be written from it at all.
# Appendices

## Appendix A — Annotated directory tree

Pinned to commit `69bdfcd`. Line counts are `wc -l`.

```
timefusion/
├── src/                                78,180 lines, 29 files
│   ├── lib.rs                              18  the module list
│   ├── main.rs                          1,080  entry point, CLI, startup, shutdown   [Ch. 9, 22, 23]
│   ├── support.rs                         263  virtual clock                          [Ch. 24]
│   ├── schema.rs                        1,021  YAML registry, rollup specs, Variant   [Ch. 6]
│   ├── config.rs                        3,053  AppConfig, budget tree, secrets        [Ch. 7, 25]
│   ├── storage.rs                       3,551  Foyer ObjectStore, sidecars            [Ch. 8]
│   ├── observability.rs                 1,468  metrics, traces, profiling             [Ch. 24, 27]
│   ├── dml.rs                           3,356  UPDATE/DELETE + coalescer              [Ch. 16]
│   ├── maintenance_coordinator.rs       4,240  journal + scheduler                    [Ch. 17]
│   ├── maintenance_sim.rs                 655  the IO-free simulator                  [Ch. 23]
│   ├── rollup.rs                        3,039  build SQL + query routing              [Ch. 18]
│   ├── rollup_journal.rs                  118  dirty-range journal                    [Ch. 24]
│   ├── hot_tier.rs                      2,118  local Arrow IPC tier (being removed)   [Ch. 20]
│   ├── write/
│   │   ├── mod.rs                       6,838  BufferedWriteLayer                     [Ch. 10, 12]
│   │   ├── wal.rs                       2,663  WAL, dir lock, takeover                [Ch. 11]
│   │   └── mem_buffer.rs                4,544  MemBuffer, TimeBucket                  [Ch. 12]
│   ├── database/
│   │   ├── mod.rs                      15,172  Database, ProjectRoutingTable, scan    [Ch. 7, 13]
│   │   ├── write.rs                     1,372  insert path, coalesced commits         [Ch. 10]
│   │   ├── compact.rs                   1,727  OPTIMIZE, consolidate, dedup           [Ch. 19]
│   │   └── maintain.rs                  5,865  sweeps, repair, vacuum, shutdown       [Ch. 19]
│   ├── read/
│   │   ├── mod.rs                       3,594  DedupExec, count pushdown, HLL         [Ch. 15]
│   │   ├── optimizers.rs                3,068  every analyzer/optimizer rule          [Ch. 14]
│   │   ├── functions.rs                 2,205  UDFs + VariantAwareExprPlanner         [Ch. 14]
│   │   └── plan_cache.rs                1,537  cross-connection plan cache            [Ch. 14]
│   ├── server/
│   │   ├── mod.rs                       1,691  bootstrap, pgwire handlers, early bind [Ch. 9, 13]
│   │   └── pg_compat.rs                 1,431  pg_catalog + timefusion_stats          [Ch. 24]
│   └── tantivy/
│       ├── mod.rs                         968  index build, schema, tokenizers        [Ch. 21]
│       ├── search.rs                    1,075  search, manifests, disk cache          [Ch. 21]
│       └── udf.rs                         450  text_match + predicate classification  [Ch. 21]
│
├── schemas/                                    compiled in via include_dir!           [Ch. 6]
│   ├── otel_logs_and_spans.yaml           457  the production table + 2 rollups
│   ├── otel_metrics.yaml                  150
│   ├── mor_versioned.yaml                  61  merge-on-read fixture
│   ├── mor_dormant.yaml                    72  declared-but-dormant fixture
│   └── variant_bench.yaml                  45
│
├── tests/
│   ├── suite/       main.rs + 27 modules       the integration target                 [Ch. 24, 26]
│   ├── e2e/         main.rs + harness.rs + 28  full prod path, virtual time
│   └── slt/         15 .slt files              SQL surface
│
├── benches/         5 criterion benches
├── bench/           python/shell load generators (NOT cargo)
├── vendor/          pgwire, walrus-rust, tikv-jemalloc-sys                            [Ch. 4]
├── docs/            13 md + plans/ (3) + dashboards/ (2)
├── deploy/          CapRover override + pgwire proxy config
├── scripts/         prod benchmarking and probes
├── tasks/           older task briefs, superseded by docs/plans
├── .github/workflows/  ci · deploy · build-image · autoformat · rollback · simd-bench [Ch. 26]
├── .cargo/config.toml  THE cargo lint definition + ld64.lld                           [Ch. 26]
├── Makefile         every test/lint/run entry point
├── Dockerfile       4-stage cargo-chef → distroless
├── docker-compose.yml  local MinIO + TimeFusion
├── rust-toolchain.toml pinned toolchain
├── rustfmt.toml
├── CLAUDE.md        39 KB of instructions — drifted, see Ch. 29
├── README.md · RUNBOOK.md · CONTRIBUTING.md · DELTA_CONFIG.md
└── data/ minio/ target/  gitignored (target/ has reached 200 GB+)
```

**Absent but documented:** `proto/`, `build.rs`, `ci/checks.tsv`,
`scripts/ci/ci.sh`. See Chapter 29 §1.

## Appendix B — Public entry points

### Binaries

One: `timefusion`. `Cargo.toml` declares no `[[bin]]`, so it is `src/main.rs`.

### CLI subcommands

| Command | One line | Chapter |
|---|---|---|
| `timefusion` | run the server | 9 |
| `timefusion healthcheck` | Docker liveness probe; prints a stage histogram | 9 |
| `timefusion encrypt-secret <plaintext>` | AES-256-GCM encrypt a config secret | 25 |
| `timefusion sim <journal.json\|data-dir> [flags]` | replay a maintenance journal on virtual time | 23 |
| `timefusion run-unit --project ID [flags]` | execute ONE maintenance unit with phase timings | 23 |
| `timefusion optimize [flags]` | off-box compaction / consolidate / dedup / recompress | 23 |
| `timefusion redrive-dml [--dir P] [--dry-run]` | replay parked DML quarantine groups | 23 |
| `timefusion migrate-columns --table T --add N:T` | widen a live table's stored Delta schema | 23 |

`sim` flags: `--hours`, `--workers`, `--streams`, `--scale`, `--seed`,
`--restarts-every-hours`, `--restart-at-hours`, `--no-mint`, `--json`.

`run-unit` flags: `--source`, `--project`, `--date`, `--op`
(`base|derived|dedup|hot|sealed|repair`), `--slice-hours`, `--offset-hours`.

`optimize` flags: `--table`, `--date`, `--older-than-hours`, `--all`,
`--project`, `--concurrency`, `--consolidate`, `--target-size-mb`, `--dedup`,
`--recompress`, `--dry-run`.

### Network listeners

| Port | Protocol | Notes |
|---|---|---|
| `PGWIRE_PORT` (5432) | PostgreSQL wire | the only ingress |

Outbound: object storage (HTTPS), OTLP (gRPC), optionally Postgres for
per-project storage config.

### SQL admin commands (intercepted before DataFusion)

| Statement | Effect | Chapter |
|---|---|---|
| `OPTIMIZE <t> WHERE date = '…' [AND project_id = '…']` | compact one partition | 13, 19 |
| `VACUUM <t> [RETAIN <n> HOURS]` | delete unreferenced files | 13, 19 |
| `FLUSH` | drain the whole MemBuffer to Delta (10 s rate limit) | 13, 22 |
| `HANDOFF` | lease a write fence and drain, for start-first deploys | 13, 22 |
| `DELTA HISTORY <t> [LIMIT n]` | recent commit metadata (read-only) | 13 |
| `DELTA ACTIONS <t> VERSION n` | every raw action in one commit (read-only) | 13 |
| `DELTA RECOVERY AUDIT <t> VERSION n` | reconstruct Add actions a commit removed | 13 |

### SQL surface beyond standard DataFusion

**Tables.** Every table in `schemas/`, plus each synthesized
`{source}_rollup_{name}` and its legacy aliases, plus `{table}__bulk` variants,
plus `timefusion_stats`, plus the `pg_catalog` surface.

**Postgres-parity functions.** `to_char`, `at_time_zone`, `extract_epoch`,
`json_build_array`, `jsonb_build_array`, `to_json`, `to_jsonb`,
`jsonb_array_elements` (⚠️ errors — needs table-function support),
`jsonb_path_exists`, and a shadowing `coalesce`.

**Timescale-parity.** `time_bucket`, `percentile_agg`, `approx_percentile`,
`tdigest_merge`.

**Variant.** `json_to_variant`, `variant_to_json`, `variant_get`,
`cast_to_variant`, `is_variant_null`, `variant_pretty`, list/object
construct/insert, and the `->` / `->>` operators.

**Sketches.** `hll_agg`, `hll_merge`, `hll_count`.

**Search.** `text_match(col, 'query')`.

**Routing.** `hash_bucket`.

**Test-only** (behind `TIMEFUSION_ENABLE_TEST_UDFS`): `timefusion_set_clock`,
`timefusion_advance_clock`, `timefusion_now_micros`.

### Rust library surface

`src/lib.rs` exports 16 public modules. The types most likely to be used from
outside:

| Item | Purpose | Chapter |
|---|---|---|
| `server::bootstrap(cfg) -> Bootstrapped` | full wiring, shared with the e2e harness | 9 |
| `server::serve_with_listener(...)` | run pgwire on a pre-bound listener | 9 |
| `server::run_until_ready(...)` | the 57P03 early-bind responder | 9 |
| `server::AuthConfig::from_core(...)` | fail-closed auth config | 9, 28 |
| `database::Database::with_config(cfg)` | open the database | 7 |
| `write::BufferedWriteLayer::with_config(...)` | the write layer | 10 |
| `write::wal::WalDirLock::acquire(dir)` | the single-writer lock | 11 |
| `config::init_config()` / `config()` / `try_config()` | the global config | 25 |
| `schema::registry()` / `get_schema(name)` | the schema registry | 6 |
| `support::{now_micros, set_micros, advance_micros}` | the clock | 24 |
| `maintenance_sim::run(journal, cfg, now)` | the simulator | 23 |

## Appendix C — Error catalog

TimeFusion mixes four error families. Chapter 29 §11 notes the cost: conversions
cross those boundaries and arrive type-erased, which is why capacity failures are
matched on *message strings*.

### `WalError` — `src/write/wal.rs:16–42`

| Variant | Triggered by |
|---|---|
| `TooShort { len }` | an entry under the 6-byte header |
| `BatchTooLarge { size, max }` | a payload over `MAX_BATCH_SIZE` (1 GiB), or an unsplittable chunk over the split target |
| `InvalidOperation(u8)` | an unknown operation byte — e.g. an old binary reading `UpdateWithSource` (3) |
| `UnsupportedVersion { version, expected }` | wrong magic or wrong `WAL_VERSION` |
| `LockContention(String)` | the WAL dir lock was not released within 900 s. **Fatal on purpose** |
| `BincodeDecode` / `BincodeEncode` | corrupt or unserializable entry |
| `ArrowIpc` | corrupt Arrow IPC payload |
| `Io` | filesystem failure |
| `EmptyBatch` | an IPC stream containing no batch |
| `Internal(String)` | an invariant violation |

### `DataFusionError` variants TimeFusion raises

| Message shape | Meaning | Chapter |
|---|---|---|
| `Plan("raw OTel queries require project_id = <value> and a timestamp lower bound or LIMIT")` | the scan guard in `Enforce` mode | 13 |
| `Plan("UPDATE cannot assign '<col>' on '<table>': columns are immutable unless declared mutable: true …")` | the mutability gate | 16 |
| `Execution("merge-on-read: no registered schema for <table>")` | DML against an unregistered table | 16 |
| `Execution("merge-on-read: <table> sets version_append but declares no tombstone_column")` | a schema that passed load but not this check | 16 |
| `Execution("Insert error for project <p> table <t>: …")` | a per-project insert failed | 10 |
| `Execution("fast_insert_batch for project <p> table <t>: …")` | the plan-cache INSERT fast path failed | 10 |
| `Internal("DedupExec key '<k>' not in input schema")` | the projection augmentation missed a key | 15 |
| `ResourcesExhausted("unordered merge-on-read dedup exceeded its 2048 MiB per-query limit; narrow the time window or compact unsorted files")` | unbounded dedup hit its ceiling | 15 |
| `ResourcesExhausted("unordered merge-on-read dedup buffer size overflow")` | integer overflow in the same accounting | 15 |
| `NotImplemented("<op> not implemented for MemoryTable yet")` | a non-`Append` insert | 7 |
| `NotImplemented("jsonb_array_elements is not yet fully implemented …")` | ⚠️ a registered function that always errors | 29 |

### `PgWireError` — wire-level

| SQLSTATE | Meaning |
|---|---|
| `28P01` | password authentication failed |
| `57P03` | the database system is starting up (early-bind responder) |
| `42601` | every admin-command error (`admin_err`) |

Admin-command messages, verbatim shapes:

- `"OPTIMIZE <t> needs a date filter: … (bare OPTIMIZE is disabled — it would compact all history in-process)"`
- `"OPTIMIZE supports only \`date\` and \`project_id\` filters"`
- `"VACUUM requires a table: VACUUM <table> [RETAIN <n> HOURS] (bare VACUUM is disabled — name the table)"`
- `"FLUSH rate-limited: last ran <n>s ago (min interval 10s)"`
- `"FLUSH: <n> bucket(s) failed to flush (<m> flushed) — data stays buffered/WAL-durable"`
- `"DELTA HISTORY limit must be between 1 and 10000"`
- `"DELTA RECOVERY AUDIT '<t>' VERSION <n>: reconstructed <a> of <b> removed files"`
- `"<CMD> is not available on this server"`
- the statement-timeout error (`statement_timeout_error()`)

### `anyhow` errors on the write path

| Message shape | Meaning |
|---|---|
| `"Memory limit exceeded: <a>MB + <b>MB reservation > <c>MB hard limit"` | the reservation CAS refused |
| `"Failed to reserve memory after 100 retries due to contention"` | CAS contention |
| `"WAL backlog exceeds hard limit (<n>GB); insert rejected under backpressure — retry later"` | the hard WAL gate |
| `"TimeFusion is draining for deployment; retry on the replacement"` | the write-admission fence |
| `"PGWIRE_PASSWORD is required (set TIMEFUSION_ALLOW_INSECURE_AUTH=true …)"` | fail-closed auth |
| `"recompress --project is disabled: scoped replace_where deadlocks; re-run without --project"` | ⚠️ documented as supported |

### Schema-load panics (process start)

Every schema failure is a `panic!` at first registry access — deliberately, since
an invalid schema cannot produce correct answers:

- `"Failed to parse schema <path>: <err>"`
- `"Invalid schema <path>: <err>"` — unknown field, non-nullable tombstone,
  `version_append` without its three columns, a Variant dedup key
- `"Invalid rollup on <path>: <err>"` — bad grain, unknown dimension/column,
  unsupported aggregate, chained or mismatched `derive_from`
- `"<table>: two rollups both generate \`<name>\` with the SAME dimensions …"`
- `"<table>: two rollups both generate \`<name>\` but group differently …"`
- `"rollup table \`<name>\` collides with a hand-written schema file of the same name"`
- `"Unknown type: <s>"` — an unrecognized `data_type`

### Capacity versus failure

`is_capacity_failure(message)` (`src/maintenance_coordinator.rs:103`) distinguishes
"this did not fit" from "this went wrong" by matching two DataFusion strings —
`ResourcesExhausted`'s `Display` and the `ExternalSorter`'s message — both pinned
against verbatim production text by
`capacity_failures_are_recognised_from_prod_text`. A capacity failure leads to
splitting; anything else leads to retry and eventually quarantine.

## Appendix D — Dependency inventory

Versions resolved from `Cargo.lock` at commit `69bdfcd`. Chapter 4 covers why
each significant one is here.

### Forked or patched (5)

| Crate | Source | Why |
|---|---|---|
| `deltalake` | `tonyalaribe/delta-rs-timefusion` @ `e2e2c65e` | Variant DML fixes + parquet sort-order pushdown. Pinned to a rev, not a branch, so a fork push cannot bust cargo-chef's recipe |
| `pgwire` | `vendor/pgwire` | libpq 18 requires a `ProtocolVersionNegotiation` response listing unsupported `_pq_.*` options |
| `datafusion-postgres` | `apitoolkit/datafusion-postgres` @ `timefusion-df54` | bind-parameter, cursor, catalog and client-compat fixes. Pulls `arrow-pg` / `datafusion-pg-catalog` from the same workspace |
| `datafusion-sql` | `tonyalaribe/datafusion` @ `timefusion-update-from-54` | removes the `not_impl_err!("UPDATE ... FROM is not supported")` guard |
| `tikv-jemalloc-sys` | `vendor/tikv-jemalloc-sys` | honours `JEMALLOC_SYS_PROF_BACKTRACE` so the heap profiler gets a working unwinder |

Plus `datafusion-variant`, pinned to an upstream rev with no local changes, and
`walrus-rust`, vendored at `vendor/walrus-rust`.

### Build profiles

| Profile | Settings |
|---|---|
| `dev` | `debug = "line-tables-only"` |
| `release` | `lto = "thin"`, `codegen-units = 16`, `strip = "symbols"` |
| `release-iter` | inherits `release`, `debug = 0`, `strip = "none"` |

### Features

| Feature | Default | Effect |
|---|---|---|
| `test` | off | test-only paths |
| `e2e` | off | gates the dynamic-MinIO E2E target |
| `profiling` | off | jemalloc heap + pprof CPU (Linux only); the Docker image opts in |

### Runtime dependencies (87)

| Crate | Resolved version |
|---|---|
| `tokio` | 1.52.3 |
| `libc` | 0.2.186 |
| `datafusion` | 54.0.0 |
| `datafusion-datasource` | 54.0.0 |
| `arrow` | 58.3.0 |
| `arrow-ipc` | 58.3.0 |
| `arrow-json` | 58.3.0 |
| `uuid` | 1.23.3 |
| `serde` | 1.0.228 |
| `serde_arrow` | 0.14.1 |
| `serde_json` | 1.0.149 |
| `serde_with` | 3.16.1 |
| `serde_yaml` | 0.9.34+deprecated |
| `async-trait` | 0.1.89 |
| `log` | 0.4.29 |
| `color-eyre` | 0.6.5 |
| `arrow-schema` | 58.3.0 |
| `regex` | 1.12.3 |
| `deltalake` | 1.0.0 |
| `buoyant_kernel` | 0.24.0 |
| `chrono` | 0.4.44 |
| `chrono-tz` | 0.10.4 |
| `sqlx` | 0.8.6 |
| `futures` | 0.3.32 |
| `bytes` | 1.11.1 |
| `tokio-rustls` | 0.26.4 |
| `datafusion-postgres` | 0.17.0 |
| `datafusion-functions-json` | 0.54.2 |
| `anyhow` | 1.0.101 |
| `sha2` | 0.11.0 |
| `fastrand` | 2.3.0 |
| `fnv` | 1.0.7 |
| `fs4` | 0.13.1 |
| `tokio-util` | 0.7.18 |
| `tokio-stream` | 0.1.18 |
| `tracing-subscriber` | 0.3.22 |
| `tracing` | 0.1.44 |
| `tracing-opentelemetry` | 0.32.1 |
| `opentelemetry` | 0.31.0 |
| `opentelemetry-otlp` | 0.31.0 |
| `opentelemetry_sdk` | 0.31.0 |
| `opentelemetry-appender-tracing` | 0.31.1 |
| `metrics` | 0.24.6 |
| `metrics-exporter-opentelemetry` | 0.2.1 |
| `metrics-util` | 0.20.4 |
| `datafusion-tracing` | 54.0.0 |
| `instrumented-object-store` | 54.0.0 |
| `dotenv` | 0.15.0 |
| `include_dir` | 0.7.4 |
| `aws-config` | 1.8.13 |
| `aws-types` | 1.3.15 |
| `aws-sdk-s3` | 1.119.0 |
| `url` | 2.5.8 |
| `croner` | 3.0.1 |
| `object_store` | 0.13.2 |
| `foyer` | 0.22.3 |
| `ahash` | 0.8.12 |
| `lru` | 0.16.3 |
| `dashmap` | 6.1.0 |
| `itertools` | 0.14.0 |
| `tap` | 1.0.1 |
| `parking_lot` | 0.12.5 |
| `envy` | 0.4.2 |
| `tdigests` | 1.0.1 |
| `bincode` | 2.0.1 |
| `walrus-rust` | 0.2.0 |
| `thiserror` | 2.0.18 |
| `strum` | 0.27.2 |
| `derive_more` | 2.1.1 |
| `datafusion-variant` | 0.1.0 |
| `parquet-variant-compute` | 58.3.0 |
| `parquet-variant-json` | 58.3.0 |
| `parquet-variant` | 58.3.0 |
| `sql-json-path` | 0.1.1 |
| `base64` | 0.22.1 |
| `aes-gcm` | 0.10.3 |
| `tantivy` | 0.22.1 |
| `tar` | 0.4.45 |
| `zstd` | 0.13.3 |
| `tempfile` | 3.25.0 |
| `sysinfo` | 0.32.1 |
| `num_cpus` | 1.17.0 |
| `scopeguard` | 1.2.0 |
| `memmap2` | 0.9.10 |
| `educe` | 0.7.6 |
| `serde-inline-default` | 1.0.2 |
| `bon` | 3.9.3 |

### Linux-only, `profiling` feature (2)

| Crate | Resolved version |
|---|---|
| `tikv-jemallocator` | 0.6.1 |
| `pprof` | 0.14.1 |

### Dev dependencies (15)

| Crate | Resolved version |
|---|---|
| `sqllogictest` | 0.29.1 |
| `serial_test` | 3.3.1 |
| `tokio` | 1.52.3 |
| `datafusion-common` | 54.0.0 |
| `tokio-postgres` | 0.7.16 |
| `rand` | 0.10.0 |
| `tempfile` | 3.25.0 |
| `test-case` | 3.3.1 |
| `criterion` | 0.8.2 |
| `testcontainers` | 0.23.3 |
| `testcontainers-modules` | 0.11.6 |
| `tokio-test` | 0.4.5 |
| `tracing-test` | 0.2.6 |
| `insta` | 1.47.2 |
| `proptest` | 1.11.0 |
