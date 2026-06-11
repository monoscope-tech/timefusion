#!/usr/bin/env python3
"""Measure SELECT latency for the two monoscope hot paths:
  1. "list" queries (log explorer page load: recent rows, KPI percentiles UNION)
  2. "random access" by (timestamp BETWEEN t±1s, project_id, id) LIMIT 1
     — monoscope's lookupOtelRecord, sent with uuid + timestamptz params.

Usage:
  python3 bench/query_latency.py seed     # insert rows, spread over 12h, flush to Delta
  python3 bench/query_latency.py run      # run latency suite against seeded data
  python3 bench/query_latency.py explain  # EXPLAIN ANALYZE each query, dump to /tmp
"""
from __future__ import annotations
import json, os, statistics, sys, time, uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from concurrent_load import COLUMNS, _to_pg, load_rows, shift_for_project

URL = f"host=127.0.0.1 port={os.environ.get('PGWIRE_PORT','12345')} user=postgres password=postgres dbname=postgres"
STATE = ROOT / "data" / "query_latency_state.json"

PID = "qlat-bench-project"
N_ROWS = 20000
WINDOW_H = 12  # spread rows over the last 12 hours


def seed():
    rows = load_rows(N_ROWS)
    # Spread evenly across the window: re-stamp timestamps over the last 12h.
    now = datetime.now(timezone.utc)
    base = max(datetime.fromisoformat(r["timestamp"]) for r in rows)
    rows = shift_for_project(rows, PID, now - base - timedelta(minutes=2))
    step = timedelta(hours=WINDOW_H) / len(rows)
    for i, r in enumerate(rows):
        ts = now - timedelta(minutes=2) - step * i
        r["timestamp"] = ts.isoformat()
        r["date"] = ts.date().isoformat()

    placeholders = "(" + ", ".join(["%s"] * len(COLUMNS)) + ")"
    base_sql = f"INSERT INTO otel_logs_and_spans ({', '.join(COLUMNS)}) VALUES "
    t0 = time.perf_counter()
    with psycopg.connect(URL, autocommit=True) as c, c.cursor() as cur:
        B = 700  # 88 cols × 700 = 61.6k params < 65535 pgwire limit
        for i in range(0, len(rows), B):
            batch = rows[i : i + B]
            flat = [_to_pg(col, r.get(col)) for r in batch for col in COLUMNS]
            cur.execute(base_sql + ", ".join([placeholders] * len(batch)), flat)
            print(f"  inserted {i+len(batch)}/{len(rows)}")
    print(f"seed done in {time.perf_counter()-t0:.1f}s")

    # Save a few target rows (id+timestamp) for random-access queries.
    targets = [
        {"id": rows[k]["id"], "timestamp": rows[k]["timestamp"]}
        for k in (50, len(rows) // 2, len(rows) - 50)
    ]
    STATE.write_text(json.dumps({"project_id": PID, "targets": targets}))
    print(f"targets saved to {STATE}")


Q_LIST = """SELECT id, timestamp, name, duration, status_code, body
FROM otel_logs_and_spans
WHERE project_id = %s AND timestamp >= %s
ORDER BY timestamp DESC LIMIT 50"""

Q_RANDOM = """SELECT project_id, id, timestamp, observed_timestamp, context, level, severity, body, attributes, resource,
  hashes, kind, status_code, status_message, COALESCE(start_time, timestamp), end_time, events, links,
  duration, name, parent_id, summary, date, errors
FROM otel_logs_and_spans
WHERE timestamp BETWEEN %s AND %s AND project_id = %s AND id = %s LIMIT 1"""

Q_COUNT = """SELECT count(*) FROM otel_logs_and_spans WHERE project_id = %s AND timestamp >= %s"""

Q_UNION_PCT = """
SELECT 'p50' AS pct, approx_percentile_cont(duration, 0.50) AS v FROM otel_logs_and_spans WHERE project_id = %s AND timestamp >= %s
UNION ALL
SELECT 'p75', approx_percentile_cont(duration, 0.75) FROM otel_logs_and_spans WHERE project_id = %s AND timestamp >= %s
UNION ALL
SELECT 'p90', approx_percentile_cont(duration, 0.90) FROM otel_logs_and_spans WHERE project_id = %s AND timestamp >= %s
UNION ALL
SELECT 'p95', approx_percentile_cont(duration, 0.95) FROM otel_logs_and_spans WHERE project_id = %s AND timestamp >= %s"""


def seed_scale():
    """Prod-shaped dataset: N_PROJECTS x N_DAYS, rows spread per day.
    Saves random-access targets at several ages (recent / day-old / deep)."""
    n_projects = int(os.environ.get("QLAT_PROJECTS", "5"))
    n_days = int(os.environ.get("QLAT_DAYS", "30"))
    rows_per_day = int(os.environ.get("QLAT_ROWS_PER_DAY", "700"))
    base_rows = load_rows(rows_per_day)
    now = datetime.now(timezone.utc)
    placeholders = "(" + ", ".join(["%s"] * len(COLUMNS)) + ")"
    base_sql = f"INSERT INTO otel_logs_and_spans ({', '.join(COLUMNS)}) VALUES "
    targets = {}
    t0 = time.perf_counter()
    with psycopg.connect(URL, autocommit=True) as c, c.cursor() as cur:
        for p in range(n_projects):
            pid = f"qlat-scale-p{p}"
            for d in range(n_days):
                day_end = now - timedelta(days=d, minutes=3)
                rows = shift_for_project(base_rows, pid, timedelta(0))
                step = timedelta(hours=20) / len(rows)
                for i, r in enumerate(rows):
                    ts = day_end - step * i
                    r["timestamp"] = ts.isoformat()
                    r["date"] = ts.date().isoformat()
                B = 700
                for i in range(0, len(rows), B):
                    batch = rows[i : i + B]
                    flat = [_to_pg(col, r.get(col)) for r in batch for col in COLUMNS]
                    cur.execute(base_sql + ", ".join([placeholders] * len(batch)), flat)
                if p == 0 and d in (0, 1, 20):
                    k = {0: "recent", 1: "day_old", 20: "deep"}[d]
                    targets[k] = {"id": rows[10]["id"], "timestamp": rows[10]["timestamp"]}
            print(f"  project {pid}: {n_days}d x {len(base_rows)} rows done ({time.perf_counter()-t0:.0f}s)")
    STATE.write_text(json.dumps({"project_id": "qlat-scale-p0", "targets": [targets.get("day_old", targets["recent"])], "targets_by_age": targets}))
    print(f"seed_scale done in {time.perf_counter()-t0:.0f}s → {STATE}")


def run_ages(n_iter=8):
    """Random access at several partition ages — exposes cold/stale costs."""
    st = json.loads(STATE.read_text())
    pid = st["project_id"]
    with psycopg.connect(URL, autocommit=True) as c, c.cursor() as cur:
        for age, tgt in st.get("targets_by_age", {}).items():
            ts = datetime.fromisoformat(tgt["timestamp"])
            lo, hi = ts - timedelta(seconds=1), ts + timedelta(seconds=1)
            times = []
            for _ in range(n_iter):
                t = time.perf_counter()
                cur.execute(Q_RANDOM, (lo, hi, pid, tgt["id"]))
                rows = cur.fetchall()
                times.append((time.perf_counter() - t) * 1000)
            ts_str = " ".join(f"{x:.0f}" for x in times)
            print(f"random_access[{age:>7}] rows={len(rows)} ms: {ts_str}")


def queries():
    st = json.loads(STATE.read_text())
    pid = st["project_id"]
    since = datetime.now(timezone.utc) - timedelta(hours=WINDOW_H + 1)
    tgt = st["targets"][-1]
    ts = datetime.fromisoformat(tgt["timestamp"])
    lo, hi = ts - timedelta(seconds=1), ts + timedelta(seconds=1)
    return [
        ("simple_select_1", "SELECT 1", ()),
        ("list_recent_50", Q_LIST, (pid, since)),
        ("count_12h", Q_COUNT, (pid, since)),
        ("union_percentiles_x4", Q_UNION_PCT, (pid, since) * 4),
        ("random_access_ts_id", Q_RANDOM, (lo, hi, pid, tgt["id"])),
    ]


def run(n_iter=15):
    qs = queries()
    print(f"{'query':<24} {'p50':>8} {'p95':>8} {'min':>8} {'max':>8}  (ms, {n_iter} iters, fresh conn each)")
    for name, sql, params in qs:
        times = []
        for i in range(n_iter):
            with psycopg.connect(URL, autocommit=True) as c, c.cursor() as cur:
                t = time.perf_counter()
                cur.execute(sql, params or None)
                cur.fetchall()
                times.append((time.perf_counter() - t) * 1000)
        times_w = sorted(times[2:])  # drop 2 cold
        p50 = statistics.median(times_w)
        p95 = times_w[int(len(times_w) * 0.95) - 1]
        print(f"{name:<24} {p50:8.1f} {p95:8.1f} {times_w[0]:8.1f} {times_w[-1]:8.1f}")

    # Same but on a single reused connection (separates conn setup from query cost).
    # plan-only = `EXPLAIN <sql>` wall time: parse + logical plan + analyzers +
    # optimizers + physical plan, no execution. exec ≈ full − plan-only.
    print("\n-- reused connection --")
    print(f"{'query':<24} {'p50':>8} {'max':>8} {'plan-only p50':>14}")
    with psycopg.connect(URL, autocommit=True) as c, c.cursor() as cur:
        for name, sql, params in qs:
            times, plan_times = [], []
            for i in range(n_iter):
                t = time.perf_counter()
                cur.execute(sql, params or None)
                cur.fetchall()
                times.append((time.perf_counter() - t) * 1000)
                if name != "simple_select_1":
                    t = time.perf_counter()
                    cur.execute("EXPLAIN " + sql, params or None)
                    cur.fetchall()
                    plan_times.append((time.perf_counter() - t) * 1000)
            times_w = sorted(times[2:])
            p50 = statistics.median(times_w)
            plan_p50 = statistics.median(sorted(plan_times[2:])) if len(plan_times) > 4 else 0.0
            print(f"{name:<24} {p50:8.1f} {times_w[-1]:8.1f} {plan_p50:14.1f}")


def explain():
    qs = queries()
    with psycopg.connect(URL, autocommit=True) as c, c.cursor() as cur:
        for name, sql, params in qs:
            if name == "simple_select_1":
                continue
            # inline params: EXPLAIN with binds works over extended proto, but inline keeps it simple
            t = time.perf_counter()
            cur.execute("EXPLAIN ANALYZE " + sql, params or None)
            out = "\n".join(str(r[0 if len(r) == 1 else 1]) for r in cur.fetchall())
            ms = (time.perf_counter() - t) * 1000
            p = Path(f"/tmp/qlat-explain-{name}.txt")
            p.write_text(out)
            print(f"{name}: {ms:.1f}ms → {p}")


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "run"
    {"seed": seed, "seed_scale": seed_scale, "run": run, "run_ages": run_ages, "explain": explain}[cmd]()
