# PostgreSQL client compatibility

TimeFusion supports catalog browsing with pgAdmin 8, DBeaver 25.1, and psql/libpq 18.

```sh
psql "postgresql://postgres:<password>@<host>:5432/postgres"
```

CI runs `psql 18.4` against `\dt`, `\d otel_logs_and_spans`, `\l`, `\du`, and `\dn`.

The server reports PostgreSQL 16.6. The catalog is read-only. DDL, role
management, grants, and administration views are not supported.

## Query requirements

Raw queries of `otel_logs_and_spans` and `otel_metrics` can require these
predicates when `TIMEFUSION_OTEL_SCAN_GUARD=enforce`:

```sql
SELECT *
FROM otel_logs_and_spans
WHERE project_id = 'project-id'
  AND timestamp >= now() - INTERVAL '1 hour';
```

A pushed `LIMIT` can replace the lower timestamp bound. A top-level `LIMIT` on
an aggregate does not make its scan bounded.

`TIMEFUSION_PGWIRE_MAX_STATEMENT_SECS` defaults to 60. The server uses the
smaller nonzero value of this limit and a client `statement_timeout`.

## Limits

- OIDs can change after a TimeFusion restart. Reconnect after deployment.
- `current_setting()` exposes compatibility values. It does not reflect prior
  connection-local `SET` statements.
- `__bulk` names are internal direct-to-Delta write aliases and remain visible.
- Catalog browsing shows unified table names. It does not create virtual
  project schemas.
- Data previews without a `project_id` predicate fail when scan enforcement is on.
