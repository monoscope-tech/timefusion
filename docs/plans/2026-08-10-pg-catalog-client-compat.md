# PostgreSQL catalog and client compatibility

**Status:** reviewed and ready for implementation, 2026-08-10
**Target clients:** pgAdmin 8, DBeaver 25.1 PostgreSQL driver, and psql/libpq 18
**PostgreSQL compatibility version:** 16.6 (`server_version_num = 160006`)

## 1. Problem

TimeFusion speaks the PostgreSQL wire protocol but does not register a
`pg_catalog` schema. Catalog-driven clients fail before users can browse tables
or open a useful query editor. `information_schema` works, but it is not enough
for pgAdmin, DBeaver, or the psql `\d` commands.

The main catalog implementation already exists in the patched
`datafusion-postgres` workspace. TimeFusion must register it and fill the gaps
that the target clients expose.

GUI access also increases operational and confidentiality risks. A data preview
can issue a broad query without `project_id` or a time bound. The compatibility
work must not expose production until the query guard is enforced.

## 2. Blocking assumption

Production GUI credentials are for trusted operators only. This plan does not
add project-scoped authentication or row-level security.

If untrusted tenant users need GUI access, stop this work. Design project-scoped
authentication and enforced tenant routing first.

## 3. Goals

The implementation is complete when all these statements are true:

1. psql 18 connects without `max_protocol_version=3.0`.
2. `\dt`, `\d otel_logs_and_spans`, `\l`, `\du`, and `\dn` complete without errors.
3. pgAdmin can expand `Databases → postgres → Schemas → public → Tables → Columns`.
4. DBeaver's PostgreSQL driver shows the `public` schema, tables, and columns.
5. `version()`, `SHOW server_version`, and startup parameters report PostgreSQL 16.6 consistently.
6. Catalog queries do not call `ProjectRoutingTable::scan` or read object storage.
7. Unsafe raw OTel queries fail before Delta provider creation or object-store access.
8. Client compatibility tests run in CI with pinned client and driver versions.

A bounded user query must contain an exact `project_id` predicate. It must also
contain a lower `timestamp` bound or a pushed scan limit.

## 4. Non-goals

This work does not add these PostgreSQL features:

- writable system catalogs
- `CREATE ROLE`, `GRANT`, or per-object privileges
- project-scoped roles or row-level security
- accurate `pg_stat_*` data or row estimates
- sequences, triggers, stored procedures, or extension management
- stable OIDs across TimeFusion restarts
- full PostgreSQL GUC semantics
- support for every pgAdmin or DBeaver administration tab

The catalog is read-only. TimeFusion's existing data DML behavior does not
change.

## 5. Findings that change the original plan

### 5.1 The catalog is available through a re-export

`datafusion-postgres` re-exports `datafusion_pg_catalog`. TimeFusion can call:

```rust
datafusion_postgres::datafusion_pg_catalog::setup_pg_catalog(
    ctx,
    "datafusion",
    provider,
)
```

Do not add a direct `datafusion-pg-catalog` dependency. The existing
`datafusion-postgres` patch selects the sibling crate from the same workspace.

The `"datafusion"` argument selects the DataFusion catalog that receives the
`pg_catalog` schema. It does not define the PostgreSQL database name shown to
clients.

### 5.2 The bundled catalog is useful but not complete

It provides dynamic `pg_class`, `pg_namespace`, `pg_attribute`, `pg_database`,
`pg_roles`, `pg_tables`, `pg_views`, and `pg_settings` providers. It also
provides static catalog tables and many compatibility UDFs.

Important limits remain:

- `current_database()` returns `datafusion`.
- `session_user()` returns the literal `postgres`.
- `version()` exists in source but is not registered.
- `pg_catalog.pg_settings` has 17 columns but only one row.
- `pg_roles.oid` is currently `0` for every role.
- relation and namespace OIDs use an in-memory process-local cache.

TimeFusion must override the identity UDFs after `setup_pg_catalog` runs.

### 5.3 The current-user rewrite already exists

`PostgresCompatibilityParser` already rewrites bare `CURRENT_USER` to
`session_user`. Do not add a duplicate rewrite in TimeFusion.

Add failing tests for bare `CURRENT_USER`, `SESSION_USER`, and `USER`. Patch the
existing fork rule only for forms that the tests prove are still broken.

### 5.4 The existing DBeaver fixture is ignored

The `datafusion-postgres` fork already contains pgAdmin, DBeaver, Metabase, and
ADBC query fixtures. The DBeaver fixture is ignored because DataFusion 54 still
fails on `::regproc` and `array_upper`.

Catalog registration alone cannot satisfy the DBeaver goal. The implementation
must reproduce and correct these two failures, then unignore the fork test.

### 5.5 The existing statement timeout does not bound query execution

The current timeout wraps planning and DataFrame creation. The returned
`QueryResponse` contains a lazy row stream. Delta reads and Arrow row encoding
occur later, outside the timeout.

Changing the default at `plan_cache.rs` alone does not protect production. The
server cap must also wrap row-stream polling for simple and extended queries.

### 5.6 Session settings are connection-local, but the SessionContext is shared

`SetShowHook` stores settings in pgwire client metadata. SQL UDFs execute in one
shared `SessionContext` and cannot read that connection metadata.

Therefore, a TimeFusion `current_setting()` UDF cannot truthfully reflect every
per-connection `SET` without a larger upstream design change. This plan uses a
small static compatibility registry for the target clients. `set_config()`
continues to echo its value and does not mutate the registry.

### 5.7 The scan guard belongs at the table-provider boundary

`gate_if_wide` returns an `ExecutionPlan` and does not receive the pushed limit.
It is not the correct rejection point.

Apply the guard at the start of `ProjectRoutingTable::scan`, where the filters,
table name, and pushed limit are available. Reject before any Delta table or
object-store operation begins.

## 6. Design decisions

### 6.1 One compatibility module owns the PostgreSQL contract

Add `src/pg_compat.rs` and export it from `src/lib.rs`. Keep these values and
behaviors in that module:

- `PG_COMPAT_VERSION = "16.6"`
- `PG_COMPAT_VERSION_NUM = "160006"`
- canonical database name `postgres`
- canonical schema name `public`
- the configured role provider
- startup server parameters
- identity and setting UDF registration
- the TimeFusion `SHOW` hook
- the static compatibility setting lookup

This module prevents version and setting values from drifting across catalog,
startup, `SHOW`, and UDF surfaces.

### 6.2 Identity is static for the shared server session

Use `AppConfig.core.pgwire_user` for `session_user()`, `CURRENT_USER`, and the
single `pg_roles` row. Use `postgres` for `current_database()`.

Register these overrides immediately after `setup_pg_catalog`. This ordering is
required because the bundled setup function registers hard-coded UDFs.

Use this format for `version()`:

```text
PostgreSQL 16.6 (TimeFusion <crate-version>) on <arch>-<os>
```

Do not add git-SHA build plumbing only for this string.

### 6.3 The setting registry is intentionally small

Add only settings required by captured target-client queries. The initial set is:

- `server_version`
- `server_version_num`
- `search_path`
- `is_superuser`
- `standard_conforming_strings`
- `client_encoding`
- `TimeZone`
- `DateStyle`
- `IntervalStyle`
- `statement_timeout`

Names use PostgreSQL's expected spelling. Lookup is case-insensitive where
PostgreSQL treats the name as case-insensitive.

Implement both signatures:

```sql
current_setting(name)
current_setting(name, missing_ok)
```

For an unknown name, the one-argument form returns an error. The two-argument
form returns `NULL` when `missing_ok` is true.

Do not claim that `current_setting()` reflects a prior connection-local `SET`.
If a pinned client requires that behavior, move the setting lookup into the
`datafusion-postgres` query path with access to `ClientInfo`.

### 6.4 The server timeout is a hard cap

Add `TIMEFUSION_PGWIRE_MAX_STATEMENT_SECS`, with a 60-second default. A value of
`0` disables the server cap.

The effective timeout is the smaller nonzero value from these sources:

1. the client `statement_timeout`
2. the TimeFusion server cap

A client value of `0` cannot disable a nonzero server cap. This differs from
PostgreSQL and is an intentional production safety rule.

The timeout starts when execution begins. It covers planning, DataFrame
creation, stream creation, row-stream polling, and row encoding. Timeout errors
use SQLSTATE `57014` and the existing PostgreSQL message.

Wrap query streams in the TimeFusion pgwire handlers. Do not add timeout logic
to catalog providers or object-store implementations.

### 6.5 The raw OTel guard enforces tenant and scan bounds

Add `TIMEFUSION_OTEL_SCAN_GUARD` with `off`, `observe`, and `enforce` modes.
Start with a code default of `off`. Use `observe` for the production audit, then
use `enforce` before GUI access. Make `enforce` the default after existing
callers pass the audit. Reject unknown mode values during configuration load.

When enforcement is on, a raw OTel scan must satisfy both rules:

1. It has one exact `project_id = <scalar>` filter.
2. It has a finite lower `timestamp` bound or a pushed scan limit.

Apply the guard to `otel_logs_and_spans` and `otel_metrics`. Do not apply it to
catalog tables, `information_schema`, `timefusion_stats`, or rollup tables.

A syntactic limit above an aggregate must not bypass the guard. For example,
`SELECT count(*) FROM ... LIMIT 1` must still fail because the scan has no
pushed limit.

An upper time bound alone is not safe because it can read all history. A lower
time bound can still select a wide range. The existing wide-scan semaphore
continues to control that case.

Return one stable error message that names the required predicates. Add a
rejection counter and a structured log with table and reason. Do not log SQL or
project values in this event.

### 6.6 Internal aliases remain visible in the first release

`__bulk` aliases and `timefusion_stats` remain visible because the bundled
catalog enumerates the DataFusion catalog directly. Filtering them requires an
upstream catalog-filter API or a schema-layout change.

Document `__bulk` as an internal write alias. Hiding internal objects is a
follow-up and is not a release blocker.

## 7. Implementation phases

Follow the repository's bug-fix rule in every phase. Add a failing regression
test first, run it, implement the change, and run the test again.

### Phase A: Pin evidence and failing tests

#### A1. Capture the client matrix

Record exact versions in test comments and the pull request:

- pgAdmin 8 image tag and Python package version
- DBeaver version and PostgreSQL JDBC driver version
- psql and libpq 18 minor version

Capture startup and tree-expansion SQL from server debug logs. Redact
credentials and literal project IDs. Compare the capture with the existing fork
fixtures before adding duplicate queries.

#### A2. Add SQL catalog tests

Create `tests/slt/pg_catalog.slt` and add `pg_catalog` to `slt_files!` in
`tests/suite/sqllogictest.rs`.

The initial failing file must cover:

- one row for database `postgres`
- one row for the configured role
- `public` in `pg_namespace`
- `otel_logs_and_spans` in `pg_class` and `pg_tables`
- expected columns in `pg_attribute`
- the 17-column `pg_catalog.pg_settings` shape
- `format_type(23, NULL) = 'integer'`
- bare and qualified `::regclass` cases
- `current_database()` and `current_schema()`
- bare and called user forms
- both `current_setting()` signatures
- `version()`

Do not assert exact dynamic OID values. Assert joins and nonzero relation OIDs.

#### A3. Add client-query fixtures

Create `tests/suite/pg_client_compat.rs` and add its module to
`tests/suite/main.rs`.

Start from the existing fork fixtures. Add the full pgAdmin tree-expansion
capture and the psql 18 catalog SQL. Keep each query separate so a failure names
the exact statement.

For every fixture query, assert these conditions:

- planning succeeds
- execution succeeds
- catalog row streams complete
- `ScanMetrics.provider_scan_total` does not increase

The first run must expose the missing catalog, version, DBeaver, and setting
failures.

### Phase B: Add safety before exposure

#### B1. Add observation and enforcement modes

Add the bounded-scan configuration and guard to
`ProjectRoutingTable::scan`. Keep `gate_if_wide` unchanged.

In observation mode, log and count queries that enforcement will reject. Do not
change their result. In enforcement mode, return before Delta provider creation.

Add focused unit tests near `ProjectRoutingTable` for:

- no project filter
- project filter without a lower time bound or pushed limit
- lower time bound without a project filter
- upper time bound only
- exact project plus lower time bound
- exact project plus pushed limit
- aggregate with a top-level `LIMIT 1`
- project predicates joined with `OR`
- a non-OTel table

Add one integration test that proves a rejected query does not increment
`provider_build_total` or `provider_scan_total`.

#### B2. Add the end-to-end server timeout

Add the max-statement configuration and effective-timeout helper. Apply it in
both `LoggingSimpleQueryHandler` and `LoggingExtendedQueryHandler`.

Wrap the handler future and each returned `QueryResponse.data_rows` stream with
one absolute deadline. Dropping the timed-out stream must cancel DataFusion
work.

Add deterministic tests with a delayed test stream. Cover these cases:

- server cap with no client timeout
- lower client timeout
- higher client timeout capped by the server
- client timeout `0` with an active server cap
- simple protocol
- extended protocol
- SQLSTATE `57014`

Do not use a real slow S3 query for timeout tests.

#### B3. Audit before enforcement

Deploy observation mode without GUI access. Observe at least one normal traffic
cycle. Classify every rejection candidate by caller and intended use.

Then complete these steps:

1. Add missing `project_id` and time predicates to valid callers.
2. Move intentional cross-project operations to a documented operator path.
3. Enable enforcement in production.
4. Confirm that rejection counts match the expected callers.
5. Expose GUI credentials only after this confirmation.

### Phase C: Register the catalog and identity surfaces

#### C1. Register `pg_catalog`

In `Database::setup_session_tables`, register routing tables and
`timefusion_stats` first. Then call `setup_pg_catalog` through the
`datafusion-postgres` re-export.

Pass a cloneable single-role context provider built from
`self.config.core.pgwire_user`. Return one role with these values:

- `is_superuser = true`
- `can_login = true`
- all create and replication flags set to `false`
- no grants or inherited roles

After catalog setup, register the TimeFusion identity and setting UDF overrides.
This order must work in both production's split setup and
`setup_session_context` used by tests.

#### C2. Remove the compatibility table in `public`

Delete `register_pg_settings_table` and its call. Search for internal readers
first. The expected result is no reader outside its registration code.

This table does not replace `pg_catalog.pg_settings`. It only creates an
unqualified two-column table in `public` and can produce incorrect resolution.

#### C3. Restore cursor support

Add `CursorStatementHook` to `LoggingHandlerFactory::hooks()`. Preserve the
shared `PlanCacheHook` instance.

Use this hook order unless a failing test proves another order is required:

1. `CursorStatementHook`
2. TimeFusion compatibility `SHOW` hook
3. shared `PlanCacheHook`
4. upstream `SetShowHook`
5. `TransactionStatementHook`

Add a test for `DECLARE`, repeated `FETCH`, and `CLOSE` over a catalog query.

#### C4. Make version reporting consistent

Implement a `ServerParameterProvider` wrapper around
`DefaultServerParameterProvider`. The default provider is non-exhaustive and
cannot add `server_version_num`, so field mutation alone is insufficient.

The wrapper must replace `server_version` and insert `server_version_num` while
preserving all other default startup parameters.

The TimeFusion `SHOW` hook must handle these names before `SetShowHook`:

- `server_version`
- `server_version_num`
- `is_superuser`
- `search_path`
- `statement_timeout`

Register `version()`, `current_database()`, `session_user()`, and
`current_setting()` after the bundled catalog UDFs.

Add a raw startup test that inspects `ParameterStatus` messages. SQLLogicTest
cannot verify startup messages.

### Phase D: Close target-client SQL gaps

#### D1. Fix the existing DBeaver failures

Run the ignored `datafusion-postgres` DBeaver fixture first. Record the exact
failure for `::regproc` and `array_upper`.

Prefer fixes in the existing fork because these are PostgreSQL parser and
catalog functions, not TimeFusion storage behavior.

For `::regproc`, extend the existing unsupported-type rewrite only as much as
the failing query requires. Preserve the expression's comparison type.

For `array_upper(array, 1)`, return the PostgreSQL upper bound for one-dimensional
arrays. Add tests for `NULL`, an empty array, and an unsupported dimension.

Unignore the fork's DBeaver test after both failures pass. Then run the same
queries through TimeFusion's exact hook list.

#### D2. Extend settings only from evidence

Run the pinned client fixtures. If a client requests another setting, add it to
the compatibility registry and `pg_catalog.pg_settings` fixture data.

Do not implement a general PostgreSQL configuration system in this phase.

#### D3. Test psql meta-command SQL

Capture SQL emitted by psql 18 for `\dt`, `\d`, `\l`, `\du`, and `\dn`. Add
those statements to the compatibility fixture.

Also run the real psql commands in a Linux CI smoke job. Meta-command expansion
is client behavior and cannot be fully tested by replaying SQL alone.

### Phase E: Support libpq 18 protocol negotiation

#### E1. Check for an upstream release first

Before creating another long-lived fork, check the newest compatible `pgwire`
release for unsupported `_pq_.` option reporting. Prefer a released fix when it
does not require an unrelated dependency upgrade.

#### E2. Patch pgwire if the release is still affected

In pgwire 0.40.3, `protocol_negotiation` accepts protocol 3.2 but sends no
`NegotiateProtocolVersion` message when the version matches. libpq 18 sends a
probe option and requires the server to report it as unsupported.

Update protocol negotiation to:

1. Collect every startup parameter whose name starts with `_pq_.`.
2. Negotiate the protocol version as it does now.
3. Send `NegotiateProtocolVersion` when the version changes or the unsupported list is not empty.
4. Include every collected option name in that message.
5. Continue normal authentication after the message.

Add pgwire unit tests for:

- protocol 3.2 with no `_pq_.` options
- protocol 3.2 with one unsupported option
- protocol 3.2 with multiple unsupported options
- a future minor version with unsupported options
- an unsupported major version

If a fork is required, patch crates.io with a pinned git revision. Do not use an
unpinned branch in the new patch entry. Submit the change upstream.

#### E3. Add TimeFusion protocol tests

Add a raw startup-packet test that sends protocol 3.2 with
`_pq_.test_protocol_negotiation`. Assert that the server returns a
`NegotiateProtocolVersion` message containing that exact name before auth
completes.

Run a real psql 18 connection without `max_protocol_version`. Keep the
connection-string workaround in the operator document until this test passes in
the deployed image.

## 8. Test matrix

| Area | Test | Required result |
| --- | --- | --- |
| Catalog SQL | `tests/slt/pg_catalog.slt` | Catalog rows, columns, UDFs, and rewrites pass |
| Client SQL | `tests/suite/pg_client_compat.rs` | All pinned fixture queries complete |
| Catalog cost | client fixture metrics assertion | No routing-table scan or object-store read |
| Cursor flow | pgwire handler test | `DECLARE`, `FETCH`, and `CLOSE` pass |
| Startup identity | raw startup test | Version parameters are exact and consistent |
| Query safety | routing-table unit and integration tests | Unsafe scans fail before provider creation |
| Timeout | delayed-stream tests | Simple and extended streams stop with `57014` |
| Protocol 3.2 | pgwire unit and TimeFusion raw-wire tests | Unsupported `_pq_.` options are reported |
| psql 18 | Linux CI smoke | Connect and all required meta-commands pass |
| pgAdmin | fixture plus manual smoke | Full table and column tree expands |
| DBeaver | unignored fork fixture plus manual smoke | PostgreSQL driver tree expands |

Use targeted commands while implementing:

```bash
cargo nextest run pg_catalog
cargo nextest run pg_client_compat
cargo nextest run bounded_otel
cargo nextest run statement_timeout
cargo check --lib
```

Run the repository gate before push:

```bash
cargo lint
cargo nextest run
```

Never use `cargo test` in this repository.

## 9. Pull-request sequence

### PR 1: Safety and observation

- add bounded-scan observation and enforcement
- add the end-to-end pgwire timeout
- add counters, logs, and focused tests
- deploy in observation mode

This PR can ship independently and has the highest operational value.

### PR 2: Catalog and client SQL

- add failing catalog and client fixtures
- register `pg_catalog`
- add role, identity, settings, version, and cursor support
- remove `public.pg_settings`
- correct the DBeaver fork gaps
- enable bounded-scan enforcement before GUI access

Do not call DBeaver supported while its fork fixture remains ignored.

### PR 3: libpq 18 negotiation

- upgrade or patch pgwire
- add raw protocol tests
- add the psql 18 CI smoke
- remove the documented connection workaround after deployment

This PR can follow PR 2 because protocol 3.0 remains a temporary workaround.
The overall plan is not complete until PR 3 lands.

## 10. Rollout and rollback

### Rollout

1. Deploy PR 1 with bounded-scan observation enabled.
2. Audit rejection-candidate events and correct callers.
3. Enable bounded-scan enforcement.
4. Deploy PR 2 without sharing new credentials.
5. Run the catalog SQL and raw startup smoke against the deployed server.
6. Run pgAdmin and DBeaver manual smoke tests with bounded sample queries.
7. Deploy PR 3 and run psql 18 without the workaround.
8. Share GUI access only with trusted operators.

Treat production as read-only during smoke tests. Use `timefusion_stats` and
small catalog queries. Do not run broad aggregates to prove that the guard
works.

### Rollback

- Revoke or rotate GUI credentials first.
- Set the scan guard to `observe` only if enforcement blocks a required caller.
- Restore `max_protocol_version=3.0` if protocol 3.2 regresses.
- Roll back to the prior TimeFusion image for catalog or handler regressions.

A rollback does not require catalog data cleanup because all catalog state is
in memory.

## 11. Documentation

Add `docs/pg-client-compat.md` with:

- tested client versions
- connection examples
- the required `project_id` and time-bound query shape
- the server timeout cap
- supported browsing operations
- unsupported administration tabs and DDL
- visible `__bulk` aliases
- process-local OIDs and the reconnect-after-deploy rule
- the psql 18 workaround while PR 3 is not deployed

Add a short link from `CLAUDE.md` instead of copying the full matrix there.

## 12. Known limits after completion

- Relation, schema, and database OIDs can change after restart.
- `pg_roles.oid` remains a placeholder until the upstream provider changes.
- `has_*_privilege` reports compatibility answers, not real object grants.
- `current_setting()` exposes static compatibility values.
- `set_config()` does not change session behavior.
- The catalog shows unified table names, not project-specific virtual schemas.
- `__bulk` aliases remain visible.
- GUI data preview without `project_id` fails by design.
- DDL and administration actions can fail even when their objects appear in the tree.
- Long-lived GUI sessions must reconnect after a TimeFusion deployment.
