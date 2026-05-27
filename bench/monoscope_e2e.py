#!/usr/bin/env python3
"""
End-to-end smoke test of the monoscope → TimeFusion write path.

What this proves:
  - TimeFusion is reachable on PGWIRE_PORT (default 12345)
  - The `otel_logs_and_spans` schema accepts the exact column list monoscope's
    `bulkInsertOtelLogsAndSpansTF` writes (so the prepared statement monoscope
    constructs lines up with what TF expects)
  - Inserted rows are queryable back through PGWire, including Variant
    extraction (`severity->>'severity_text'`, etc.)
  - Multi-row INSERT (monoscope's actual ingestion shape) succeeds

This is intentionally narrower than the full monoscope integration suite
(which needs a Postgres with timescaledb_toolkit installed and is largely
unrelated to TF). It exercises the boundary that matters for dual-write.
"""
from __future__ import annotations
import json
import os
import sys
import uuid
from datetime import datetime, timezone

import psycopg


def conn_str() -> str:
    port = os.environ.get("PGWIRE_PORT", "12345")
    return f"host=127.0.0.1 port={port} user=postgres password=postgres dbname=postgres"


# Monoscope's exact 88-column list from src/Models/Telemetry/Telemetry.hs::otelColumns
COLUMNS = [
    "timestamp", "observed_timestamp", "id", "parent_id", "hashes", "name", "kind",
    "status_code", "status_message", "level", "severity",
    "severity___severity_text", "severity___severity_number", "body", "duration",
    "start_time", "end_time", "context", "context___trace_id", "context___span_id",
    "context___trace_state", "context___trace_flags", "context___is_remote",
    "events", "links", "attributes", "attributes___client___address",
    "attributes___client___port", "attributes___server___address",
    "attributes___server___port", "attributes___network___local__address",
    "attributes___network___local__port", "attributes___network___peer___address",
    "attributes___network___peer__port", "attributes___network___protocol___name",
    "attributes___network___protocol___version", "attributes___network___transport",
    "attributes___network___type", "attributes___code___number",
    "attributes___code___file___path", "attributes___code___function___name",
    "attributes___code___line___number", "attributes___code___stacktrace",
    "attributes___log__record___original", "attributes___log__record___uid",
    "attributes___error___type", "attributes___exception___type",
    "attributes___exception___message", "attributes___exception___stacktrace",
    "attributes___url___fragment", "attributes___url___full", "attributes___url___path",
    "attributes___url___query", "attributes___url___scheme",
    "attributes___user_agent___original", "attributes___http___request___method",
    "attributes___http___request___method_original",
    "attributes___http___response___status_code",
    "attributes___http___request___resend_count",
    "attributes___http___request___body___size", "attributes___session___id",
    "attributes___session___previous___id", "attributes___db___system___name",
    "attributes___db___collection___name", "attributes___db___namespace",
    "attributes___db___operation___name", "attributes___db___response___status_code",
    "attributes___db___operation___batch___size", "attributes___db___query___summary",
    "attributes___db___query___text", "attributes___user___id", "attributes___user___email",
    "attributes___user___full_name", "attributes___user___name", "attributes___user___hash",
    "resource", "resource___service___name", "resource___service___version",
    "resource___service___instance___id", "resource___service___namespace",
    "resource___telemetry___sdk___language", "resource___telemetry___sdk___name",
    "resource___telemetry___sdk___version", "resource___user_agent___original",
    "project_id", "summary", "date", "message_size_bytes",
]
assert len(COLUMNS) == 88, len(COLUMNS)


def row(project_id: str, *, name: str, level: str, status_code: str, severity_text: str) -> dict:
    now = datetime.now(timezone.utc)
    today = now.date().isoformat()
    return {
        "timestamp": now,
        "observed_timestamp": now,
        "id": str(uuid.uuid4()),
        "name": name,
        "kind": "SPAN_KIND_SERVER",
        "status_code": status_code,
        "level": level,
        "severity": json.dumps({"severity_text": severity_text, "severity_number": 9}),
        "severity___severity_text": severity_text,
        "severity___severity_number": 9,
        "body": json.dumps({"msg": f"hello from {name}"}),
        "duration": 12345,
        "start_time": now,
        "end_time": now,
        "context": json.dumps({"trace_id": uuid.uuid4().hex, "span_id": uuid.uuid4().hex[:16]}),
        "context___trace_id": uuid.uuid4().hex,
        "context___span_id": uuid.uuid4().hex[:16],
        "context___is_remote": False,
        "attributes": json.dumps({"http.request.method": "GET"}),
        "attributes___http___request___method": "GET",
        "attributes___http___response___status_code": 200,
        "resource": json.dumps({"service.name": "monoscope-e2e"}),
        "resource___service___name": "monoscope-e2e",
        "project_id": project_id,
        "date": today,
        "hashes": [],
        "summary": [],
        "message_size_bytes": 512,
    }


def insert_rows(cur, rows: list[dict]) -> int:
    placeholders = "(" + ", ".join(["%s"] * len(COLUMNS)) + ")"
    sql = f"INSERT INTO otel_logs_and_spans ({', '.join(COLUMNS)}) VALUES " + ", ".join([placeholders] * len(rows))
    values = []
    for r in rows:
        for col in COLUMNS:
            values.append(r.get(col))
    cur.execute(sql, values)
    return cur.rowcount


def main():
    pid = f"monoscope-e2e-{uuid.uuid4().hex[:8]}"
    rows = [
        row(pid, name="orders.create", level="INFO", status_code="OK", severity_text="INFO"),
        row(pid, name="orders.read",   level="INFO", status_code="OK", severity_text="INFO"),
        row(pid, name="orders.fail",   level="ERROR", status_code="ERROR", severity_text="ERROR"),
    ]
    with psycopg.connect(conn_str(), autocommit=True) as conn:
        with conn.cursor() as cur:
            inserted = insert_rows(cur, rows)
            print(f"insert rowcount: {inserted}")

            cur.execute(
                "SELECT count(*) FROM otel_logs_and_spans WHERE project_id = %s",
                (pid,),
            )
            (total,) = cur.fetchone()
            print(f"select count: {total}")

            cur.execute(
                "SELECT name, level, status_code, severity___severity_text "
                "FROM otel_logs_and_spans WHERE project_id = %s ORDER BY name",
                (pid,),
            )
            seen = cur.fetchall()
            print("rows back:")
            for r in seen:
                print(f"  {r}")

            # Variant extraction via TF's variant_get UDF (the kernel-native
            # path; monoscope's queries today still hit main PG, but TF
            # readers need this to work for downstream querying).
            cur.execute(
                "SELECT count(*) FROM otel_logs_and_spans "
                "WHERE project_id = %s AND variant_get(severity, 'severity_text', 'Utf8') = 'ERROR'",
                (pid,),
            )
            (errs,) = cur.fetchone()
            print(f"variant filter (severity_text=ERROR) count: {errs}")

    failures = []
    # Note: TF's pgwire returns TuplesOk instead of CommandOk for multi-row
    # INSERT under the extended query protocol, so libpq surfaces rowcount=0.
    # Monoscope swallows this known wire-mismatch (see Telemetry.hs:911) —
    # verify landing via SELECT count instead.
    if total != len(rows):
        failures.append(f"select count {total} != {len(rows)}")
    if len(seen) != len(rows):
        failures.append(f"detail rows {len(seen)} != {len(rows)}")
    if errs != 1:
        failures.append(f"variant ERROR count {errs} != 1")
    if failures:
        for f in failures:
            print(f"FAIL: {f}", file=sys.stderr)
        sys.exit(1)
    print("PASS")


if __name__ == "__main__":
    main()
