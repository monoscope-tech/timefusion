#!/usr/bin/env python3
"""Measure per-INSERT latency at the server with the same row shape as
concurrent_load.py — but synchronously, one batch at a time, to pin
TF's per-query cost vs client-side overhead."""
import gzip, json, os, sys, time, uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path
import psycopg

ROOT = Path(__file__).resolve().parent
DUMP = ROOT / "data" / "sample.jsonl.gz"
URL = "host=127.0.0.1 port=12345 user=postgres password=postgres dbname=postgres"

# Same columns as concurrent_load.py
sys.path.insert(0, str(ROOT))
from concurrent_load import COLUMNS, JSON_COLS, LIST_COLS, _to_pg, load_rows, shift_for_project

def main():
    rows = load_rows(2000)
    shift = datetime.now(timezone.utc) - max(datetime.fromisoformat(r["timestamp"]) for r in rows)
    pid = f"latprobe-{uuid.uuid4().hex[:6]}"
    rows = shift_for_project(rows, pid, shift)

    placeholders = "(" + ", ".join(["%s"] * len(COLUMNS)) + ")"
    base = f"INSERT INTO otel_logs_and_spans ({', '.join(COLUMNS)}) VALUES "

    for batch_size in (1, 10, 30, 100, 500, 1000):
        with psycopg.connect(URL, autocommit=True) as c, c.cursor() as cur:
            times = []
            for i in range(8):
                batch = rows[i * batch_size : (i + 1) * batch_size]
                if len(batch) < batch_size: break
                flat = []
                for r in batch:
                    for col in COLUMNS:
                        flat.append(_to_pg(col, r.get(col)))
                t = time.perf_counter()
                cur.execute(base + ", ".join([placeholders] * len(batch)), flat)
                times.append((time.perf_counter() - t) * 1000)
            if not times: continue
            warm = times[2:]  # drop first 2 cold runs
            avg = sum(warm) / len(warm) if warm else times[-1]
            per_row = avg / batch_size
            r_s = 1000 / per_row if per_row > 0 else 0
            print(f"batch={batch_size:>4}  warm_avg={avg:7.1f}ms  per_row={per_row:6.3f}ms  → {r_s:7.0f} r/s (1 conn)")

if __name__ == "__main__":
    main()
