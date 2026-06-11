#!/usr/bin/env python3
"""Reproduce the prod stall: time the monoscope span-list query while the same
project receives continuous ingest. Usage:
  python3 bench/query_under_ingest.py [duration_s] [rows_per_batch] [batches_per_sec]
"""
import os, sys, threading, time
from datetime import datetime, timezone
from pathlib import Path

import psycopg

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from concurrent_load import COLUMNS, _to_pg, load_rows, shift_for_project

URL = f"host=127.0.0.1 port={os.environ.get('PGWIRE_PORT','12345')} user=postgres password=postgres dbname=postgres"
PID = "qlat-scale-p0"
Q = """SELECT jsonb_build_array(id, to_char(timestamp at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'), context___trace_id, name, duration, resource___service___name, parent_id, CAST(EXTRACT(EPOCH FROM (start_time)) * 1000000000 AS BIGINT), errors is not null, to_jsonb(summary), context___span_id, kind)
FROM otel_logs_and_spans
WHERE project_id = %s AND timestamp BETWEEN now() - interval '4 days' AND now() AND (TRUE)
ORDER BY timestamp DESC LIMIT 501"""

stop = threading.Event()
inserted = [0]


def writer(rows_per_batch: int, batches_per_sec: float):
    rows = shift_for_project(load_rows(rows_per_batch), PID, __import__("datetime").timedelta(0))
    placeholders = "(" + ", ".join(["%s"] * len(COLUMNS)) + ")"
    sql = f"INSERT INTO otel_logs_and_spans ({', '.join(COLUMNS)}) VALUES " + ", ".join([placeholders] * len(rows))
    with psycopg.connect(URL, autocommit=True) as c, c.cursor() as cur:
        while not stop.is_set():
            t0 = time.perf_counter()
            now = datetime.now(timezone.utc)
            for r in rows:
                r["timestamp"] = now.isoformat()
                r["date"] = now.date().isoformat()
            cur.execute(sql, [_to_pg(col, r.get(col)) for r in rows for col in COLUMNS])
            inserted[0] += len(rows)
            stop.wait(max(0.0, 1.0 / batches_per_sec - (time.perf_counter() - t0)))


def main():
    duration = int(sys.argv[1]) if len(sys.argv) > 1 else 60
    rows_per_batch = int(sys.argv[2]) if len(sys.argv) > 2 else 100
    batches_per_sec = float(sys.argv[3]) if len(sys.argv) > 3 else 2.0
    t = threading.Thread(target=writer, args=(rows_per_batch, batches_per_sec), daemon=True)
    t.start()
    lats = []
    t_end = time.time() + duration
    with psycopg.connect(URL, autocommit=True) as c, c.cursor() as cur:
        while time.time() < t_end:
            t0 = time.perf_counter()
            cur.execute(Q, (PID,))
            cur.fetchall()
            ms = (time.perf_counter() - t0) * 1000
            lats.append(ms)
            print(f"query: {ms:7.1f}ms  (ingested so far: {inserted[0]})", flush=True)
            time.sleep(1)
    stop.set()
    t.join(timeout=5)
    lats.sort()
    n = len(lats)
    print(f"\nn={n} p50={lats[n // 2]:.0f}ms p95={lats[int(n * 0.95)]:.0f}ms max={lats[-1]:.0f}ms rate={inserted[0] / duration:.0f} rows/s")


if __name__ == "__main__":
    main()
