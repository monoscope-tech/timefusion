#!/usr/bin/env python3
"""Reproduce prod-shaped MemBuffer fragmentation: ingest small batches into one
project, then time the monoscope span-list query against the buffered rows.
Usage: python3 bench/query_under_ingest.py [total_rows] [rows_per_batch]
"""
import os, sys, time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from concurrent_load import COLUMNS, _to_pg, load_rows, shift_for_project

URL = f"host=127.0.0.1 port={os.environ.get('PGWIRE_PORT','12345')} user=postgres password=postgres dbname=postgres"
PID = os.environ.get("QUI_PROJECT", "qlat-frag-p0")
Q = """SELECT jsonb_build_array(id, to_char(timestamp at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'), context___trace_id, name, duration, resource___service___name, parent_id, CAST(EXTRACT(EPOCH FROM (start_time)) * 1000000000 AS BIGINT), errors is not null, to_jsonb(summary), context___span_id, kind)
FROM otel_logs_and_spans
WHERE project_id = %s AND timestamp BETWEEN now() - interval '1 hour' AND now() AND (TRUE)
ORDER BY timestamp DESC LIMIT 501"""


def main():
    total = int(sys.argv[1]) if len(sys.argv) > 1 else 30000
    per_batch = int(sys.argv[2]) if len(sys.argv) > 2 else 30
    rows = shift_for_project(load_rows(per_batch), PID, timedelta(0))
    placeholders = "(" + ", ".join(["%s"] * len(COLUMNS)) + ")"
    sql = f"INSERT INTO otel_logs_and_spans ({', '.join(COLUMNS)}) VALUES " + ", ".join([placeholders] * len(rows))
    t0 = time.perf_counter()
    with psycopg.connect(URL, autocommit=True) as c, c.cursor() as cur:
        sent = 0
        while sent < total:
            now = datetime.now(timezone.utc)
            for r in rows:
                r["timestamp"] = now.isoformat()
                r["date"] = now.date().isoformat()
            cur.execute(sql, [_to_pg(col, r.get(col)) for r in rows for col in COLUMNS])
            sent += len(rows)
        print(f"ingested {sent} rows in {per_batch}-row batches ({time.perf_counter()-t0:.0f}s)")
        lats = []
        for i in range(7):
            t = time.perf_counter()
            cur.execute(Q, (PID,))
            n = len(cur.fetchall())
            ms = (time.perf_counter() - t) * 1000
            lats.append(ms)
            print(f"query iter{i}: {ms:7.1f}ms rows={n}")
        lats.sort()
        print(f"p50={lats[len(lats) // 2]:.0f}ms min={lats[0]:.0f}ms max={lats[-1]:.0f}ms")


if __name__ == "__main__":
    main()
