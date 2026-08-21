# Architecture

TimeFusion exposes one client interface: the PostgreSQL wire protocol. PGWire
accepts SQL statements and sends them to DataFusion.

## Data path

```text
PostgreSQL client
       |
       v
PGWire server (`src/server`)
       |
       v
DataFusion planner and executor
       |
       +---- write ----> WAL ----> memory buffer ----> Delta Lake on S3
       |
       `---- read -----> memory buffer + hot tier + Delta Lake
                                      |
                                      `---- Foyer object cache
```

An `INSERT` reaches the WAL before TimeFusion acknowledges it. TimeFusion also
adds the row to the memory buffer. A background task flushes buffered rows to
Delta Lake.

A read combines recent rows with Delta files. The read path applies merge-on-read
rules for updates and deletes. It also removes duplicate event versions.

## Storage and tenancy

Delta Lake on S3-compatible storage is the authoritative store. TimeFusion
partitions shared tables by `project_id` and `date`.

Every data query must contain an equality filter for `project_id`. This filter
selects the tenant and limits the storage scan.

The WAL is a local durability layer. One process owns each WAL directory. For
recovery and deployment rules, read [WAL.md](WAL.md) and [the runbook](../RUNBOOK.md).

## Main modules

| Path | Responsibility |
|---|---|
| `src/server` | PGWire authentication, compatibility, statement handling, and result encoding |
| `src/database` | Delta reads, writes, compaction, deduplication, and maintenance |
| `src/read` | Read planning, optimizer rules, caches, and custom functions |
| `src/write` | WAL, memory buffer, backpressure, recovery, and flush control |
| `src/dml.rs` | `UPDATE`, `DELETE`, and merge-on-read behavior |
| `src/maintenance_coordinator.rs` | Durable scheduling for maintenance work |
| `src/rollup.rs` | Rollup creation and query routing |
| `src/storage.rs` | Object-store access and cache integration |
| `src/tantivy` | Derived search indexes |
| `src/config.rs` | Environment configuration and derived defaults |

## Derived data

Foyer cache entries, Tantivy indexes, rollups, and maintenance indexes are
derived data. TimeFusion can rebuild them from Delta data.

The WAL is not derived until its acknowledged rows reach Delta. Do not delete a
WAL directory unless its durable cursors prove that Delta contains every entry.

## Schema behavior

Schemas are in `schemas/`. TimeFusion loads them at startup. The planner converts
Variant values for PostgreSQL clients and implements the supported JSON operators.

Read [VARIANT_TYPE_SYSTEM.md](VARIANT_TYPE_SYSTEM.md) for Variant behavior. Read
[MULTI_TABLE_ARCHITECTURE.md](MULTI_TABLE_ARCHITECTURE.md) for table routing.

## Design rules

- Keep PGWire behavior compatible with supported PostgreSQL clients.
- Treat Delta Lake as the source of truth after a flush.
- Preserve acknowledged WAL entries until Delta contains them.
- Bound background work by memory, time, and input size.
- Make maintenance retry-safe and persistent across restarts.
- Keep derived indexes optional for correctness.
