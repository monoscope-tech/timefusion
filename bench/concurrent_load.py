#!/usr/bin/env python3
"""
Multi-tenant concurrent ingest + query benchmark.

Spawns N writer threads (each owning one project_id, replaying a slice of
bench/data/sample.jsonl.gz with shifted timestamps) and M reader threads
issuing the canonical log-explorer queries on a 100ms budget.

Reports:
  - ingest: rows/sec, batches/sec, total rows
  - reads:  p50/p95/p99 per query type, % under 100ms budget
  - state:  start/end timefusion_stats deltas (rows, batches, mem_mb, oldest_age)

Usage:
  python3 bench/concurrent_load.py --writers 5 --readers 4 --duration 90
"""
from __future__ import annotations
import argparse, gzip, json, os, random, statistics, sys, threading, time, uuid
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, date, timezone, timedelta
from pathlib import Path
from typing import Any

import psycopg

ROOT = Path(__file__).resolve().parent
DUMP = ROOT / "data" / "sample.jsonl.gz"

LOCAL_HOST = os.environ.get("PGWIRE_HOST", "127.0.0.1")
LOCAL_PORT = int(os.environ.get("PGWIRE_PORT", "12345"))
LOCAL_URL = f"host={LOCAL_HOST} port={LOCAL_PORT} user=postgres password=postgres dbname=postgres"

# Same shape as replay_prod_load.py — kept duplicated so this script is
# self-contained.
COLUMNS = [
    "timestamp","observed_timestamp","id","parent_id","hashes","name","kind",
    "status_code","status_message","level","severity",
    "severity___severity_text","severity___severity_number","body","duration",
    "start_time","end_time","context","context___trace_id","context___span_id",
    "context___trace_state","context___trace_flags","context___is_remote",
    "events","links","attributes","attributes___client___address",
    "attributes___client___port","attributes___server___address",
    "attributes___server___port","attributes___network___local__address",
    "attributes___network___local__port","attributes___network___peer___address",
    "attributes___network___peer__port","attributes___network___protocol___name",
    "attributes___network___protocol___version","attributes___network___transport",
    "attributes___network___type","attributes___code___number",
    "attributes___code___file___path","attributes___code___function___name",
    "attributes___code___line___number","attributes___code___stacktrace",
    "attributes___log__record___original","attributes___log__record___uid",
    "attributes___error___type","attributes___exception___type",
    "attributes___exception___message","attributes___exception___stacktrace",
    "attributes___url___fragment","attributes___url___full","attributes___url___path",
    "attributes___url___query","attributes___url___scheme",
    "attributes___user_agent___original","attributes___http___request___method",
    "attributes___http___request___method_original",
    "attributes___http___response___status_code",
    "attributes___http___request___resend_count",
    "attributes___http___request___body___size","attributes___session___id",
    "attributes___session___previous___id","attributes___db___system___name",
    "attributes___db___collection___name","attributes___db___namespace",
    "attributes___db___operation___name","attributes___db___response___status_code",
    "attributes___db___operation___batch___size","attributes___db___query___summary",
    "attributes___db___query___text","attributes___user___id","attributes___user___email",
    "attributes___user___full_name","attributes___user___name","attributes___user___hash",
    "resource","resource___service___name","resource___service___version",
    "resource___service___instance___id","resource___service___namespace",
    "resource___telemetry___sdk___language","resource___telemetry___sdk___name",
    "resource___telemetry___sdk___version","resource___user_agent___original",
    "project_id","summary","date","message_size_bytes",
]
JSON_COLS = {"severity","body","context","attributes","resource","errors","events","links"}
LIST_COLS = {"hashes","summary"}

QUERIES = [
    ("list_1h", "SELECT id, timestamp FROM otel_logs_and_spans "
                "WHERE project_id=%(pid)s AND timestamp >= now() - INTERVAL '1 hour' "
                "ORDER BY timestamp DESC LIMIT 251"),
    ("list_2h", "SELECT id, timestamp, name, duration FROM otel_logs_and_spans "
                "WHERE project_id=%(pid)s AND timestamp >= now() - INTERVAL '2 hours' "
                "ORDER BY timestamp DESC LIMIT 251"),
    ("hist_1h","SELECT time_bucket('60 seconds', timestamp) tb, count(*) FROM otel_logs_and_spans "
               "WHERE project_id=%(pid)s AND timestamp >= now() - INTERVAL '1 hour' "
               "GROUP BY tb ORDER BY tb DESC LIMIT 500"),
    ("hist_2h","SELECT time_bucket('5 minutes', timestamp) tb, count(*) FROM otel_logs_and_spans "
               "WHERE project_id=%(pid)s AND timestamp >= now() - INTERVAL '2 hours' "
               "GROUP BY tb ORDER BY tb DESC LIMIT 500"),
]


def _to_pg(col: str, v: Any) -> Any:
    if v is None: return None
    if col in {"timestamp","observed_timestamp","start_time","end_time"} and isinstance(v, str):
        return datetime.fromisoformat(v)
    if col in JSON_COLS and not isinstance(v, str):
        return json.dumps(v, default=str)
    if col in LIST_COLS and isinstance(v, list):
        return [e if isinstance(e, (str,int,float,bool)) else json.dumps(e, default=str) for e in v]
    return v


def load_rows(limit: int | None = None) -> list[dict]:
    """Read the dump once, return raw rows (no shift)."""
    rows: list[dict] = []
    with gzip.open(DUMP, "rt") as f:
        for line in f:
            rows.append(json.loads(line))
            if limit and len(rows) >= limit: break
    return rows


def shift_for_project(rows: list[dict], project_id: str, shift: timedelta) -> list[dict]:
    """Re-stamp rows for a synthetic project. Returns shallow copies (cheap)."""
    out: list[dict] = []
    for r in rows:
        nr = dict(r)
        nr["project_id"] = project_id
        nr["id"] = str(uuid.uuid4())
        for k in ("timestamp","observed_timestamp","start_time","end_time"):
            if nr.get(k):
                nr[k] = (datetime.fromisoformat(nr[k]) + shift).isoformat()
        nr["date"] = datetime.fromisoformat(nr["timestamp"]).date().isoformat()
        out.append(nr)
    return out


# ───── workers ─────

class Stats:
    def __init__(self):
        self.lock = threading.Lock()
        self.inserted = 0
        self.batches = 0
        self.errors: list[str] = []
        self.lat: dict[str, list[float]] = defaultdict(list)
        self.query_errors: list[str] = []


def writer(stop: threading.Event, pid: str, rows: list[dict], batch_size: int, stats: Stats):
    placeholders = "(" + ", ".join(["%s"] * len(COLUMNS)) + ")"
    base_sql = f"INSERT INTO otel_logs_and_spans ({', '.join(COLUMNS)}) VALUES "

    with psycopg.connect(LOCAL_URL, autocommit=True) as conn, conn.cursor() as cur:
        idx = 0
        while not stop.is_set():
            batch = rows[idx:idx+batch_size]
            if not batch:
                # loop back to start with re-shifted timestamps (newer "now")
                idx = 0
                continue
            idx += batch_size
            flat: list[Any] = []
            for r in batch:
                for c in COLUMNS:
                    flat.append(_to_pg(c, r.get(c)))
            try:
                cur.execute(base_sql + ", ".join([placeholders]*len(batch)), flat)
                with stats.lock:
                    stats.inserted += len(batch)
                    stats.batches += 1
            except Exception as e:
                with stats.lock:
                    stats.errors.append(repr(e)[:200])


def reader(stop: threading.Event, project_ids: list[str], stats: Stats):
    with psycopg.connect(LOCAL_URL, autocommit=True) as conn, conn.cursor() as cur:
        while not stop.is_set():
            name, sql = random.choice(QUERIES)
            pid = random.choice(project_ids)
            t0 = time.perf_counter()
            try:
                cur.execute(sql, {"pid": pid})
                cur.fetchall()
            except Exception as e:
                with stats.lock:
                    stats.query_errors.append(f"{name}: {repr(e)[:120]}")
                continue
            dt = time.perf_counter() - t0
            with stats.lock:
                stats.lat[name].append(dt)


def read_tf_stats() -> dict[str, float]:
    with psycopg.connect(LOCAL_URL, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("SELECT component, key, value FROM timefusion_stats")
        out: dict[str, float] = {}
        for c, k, v in cur.fetchall():
            try: out[f"{c}.{k}"] = float(v)
            except (TypeError, ValueError): pass
        return out


# ───── main ─────

def pct(xs: list[float], p: float) -> float:
    if not xs: return float("nan")
    s = sorted(xs)
    k = max(0, min(len(s)-1, int(round((p/100) * (len(s)-1)))))
    return s[k]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--writers", type=int, default=5, help="concurrent writer threads / projects")
    ap.add_argument("--readers", type=int, default=4, help="concurrent reader threads")
    ap.add_argument("--duration", type=int, default=60, help="seconds to run")
    ap.add_argument("--batch", type=int, default=30, help="rows per INSERT (prod-shape: ~30)")
    ap.add_argument("--row-limit", type=int, default=80000, help="max rows preloaded per project")
    ap.add_argument("--budget-ms", type=float, default=100.0, help="read latency budget for PASS")
    args = ap.parse_args()

    if not DUMP.exists():
        sys.exit(f"missing dump at {DUMP} — run replay_prod_load.py download first")

    print(f"loading dump (limit {args.row_limit} rows per writer)…", flush=True)
    raw = load_rows(args.row_limit)
    if not raw: sys.exit("dump is empty")

    # Shift so the LATEST original row lands at now() for each project
    max_ts = max(datetime.fromisoformat(r["timestamp"]) for r in raw)
    base_shift = datetime.now(timezone.utc) - max_ts

    pids = [f"bench-{i:02d}-{uuid.uuid4().hex[:8]}" for i in range(args.writers)]
    per_project: dict[str, list[dict]] = {}
    for pid in pids:
        # Tiny per-project jitter on the shift so projects aren't synchronised
        jitter = timedelta(seconds=random.uniform(-30, 30))
        per_project[pid] = shift_for_project(raw, pid, base_shift + jitter)

    print(f"projects={len(pids)} rows/project={len(raw)} duration={args.duration}s "
          f"readers={args.readers} batch={args.batch}", flush=True)

    stats = Stats()
    stop = threading.Event()
    pre = read_tf_stats()
    t0 = time.perf_counter()

    with ThreadPoolExecutor(max_workers=args.writers + args.readers) as ex:
        for pid in pids:
            ex.submit(writer, stop, pid, per_project[pid], args.batch, stats)
        for _ in range(args.readers):
            ex.submit(reader, stop, pids, stats)
        # Snapshot stats every 15s so it's visible mid-run
        next_snap = t0 + 15
        while time.perf_counter() - t0 < args.duration:
            time.sleep(0.5)
            if time.perf_counter() >= next_snap:
                next_snap += 15
                s = read_tf_stats()
                with stats.lock:
                    queries_so_far = sum(len(v) for v in stats.lat.values())
                    ins = stats.inserted
                print(f"  t={time.perf_counter()-t0:5.1f}s ins={ins:,} q={queries_so_far} "
                      f"mb_rows={int(s.get('mem_buffer.total_rows',0)):,} "
                      f"mb_batches={int(s.get('mem_buffer.total_batches',0))} "
                      f"mb_mb={s.get('mem_buffer.estimated_mb',0):.0f} "
                      f"buckets={int(s.get('mem_buffer.total_buckets',0))} "
                      f"oldest={int(s.get('mem_buffer.oldest_bucket_age_secs',0))}s "
                      f"press={int(s.get('buffered_layer.pressure_pct',0))}%", flush=True)
        stop.set()

    elapsed = time.perf_counter() - t0
    post = read_tf_stats()

    print("\n# ingest")
    print(f"  rows inserted  : {stats.inserted:,}")
    print(f"  batches sent   : {stats.batches:,}")
    print(f"  throughput     : {stats.inserted/elapsed:,.0f} rows/s")
    print(f"  insert errors  : {len(stats.errors)}")
    if stats.errors[:3]:
        for e in stats.errors[:3]: print(f"    - {e}")

    print("\n# reads (budget {:.0f} ms)".format(args.budget_ms))
    overall_pass = len(stats.errors) == 0
    for name, _ in QUERIES:
        xs = stats.lat[name]
        if not xs:
            print(f"  {name:8s} no samples"); continue
        p50 = pct(xs, 50)*1000; p95 = pct(xs, 95)*1000; p99 = pct(xs, 99)*1000
        max_ms = max(xs)*1000
        under_budget = sum(1 for x in xs if x*1000 <= args.budget_ms) / len(xs)
        ok = p95*1 <= args.budget_ms
        overall_pass &= ok
        print(f"  [{'PASS' if ok else 'FAIL'}] {name:8s} "
              f"n={len(xs):4d}  p50={p50:6.1f}ms  p95={p95:7.1f}ms  p99={p99:7.1f}ms  "
              f"max={max_ms:7.1f}ms  under_budget={under_budget*100:5.1f}%")

    if stats.query_errors:
        print(f"  query errors: {len(stats.query_errors)}")
        for e in stats.query_errors[:3]: print(f"    - {e}")

    print("\n# timefusion_stats delta")
    def diff(k): return post.get(k,0) - pre.get(k,0)
    print(f"  mem_buffer.total_rows     {int(pre.get('mem_buffer.total_rows',0)):>10,} -> {int(post.get('mem_buffer.total_rows',0)):>10,}")
    print(f"  mem_buffer.total_batches  {int(pre.get('mem_buffer.total_batches',0)):>10,} -> {int(post.get('mem_buffer.total_batches',0)):>10,}")
    print(f"  mem_buffer.total_buckets  {int(pre.get('mem_buffer.total_buckets',0)):>10,} -> {int(post.get('mem_buffer.total_buckets',0)):>10,}")
    print(f"  mem_buffer.estimated_mb   {pre.get('mem_buffer.estimated_mb',0):>10.1f} -> {post.get('mem_buffer.estimated_mb',0):>10.1f}")
    print(f"  buffered_layer.pressure   {pre.get('buffered_layer.pressure_pct',0):>10.0f}%-> {post.get('buffered_layer.pressure_pct',0):>10.0f}%")
    print(f"  wal.disk_mb               {pre.get('wal.disk_mb',0):>10.0f}  -> {post.get('wal.disk_mb',0):>10.0f}")
    if post.get('mem_buffer.total_batches', 0) > 0:
        rpb = post['mem_buffer.total_rows'] / post['mem_buffer.total_batches']
        print(f"  rows/batch (end)          {rpb:>10.1f}   target >5000")
    if post.get('mem_buffer.total_rows', 0) > 0:
        bpr = post['mem_buffer.estimated_bytes'] / post['mem_buffer.total_rows']
        print(f"  bytes/row (end)           {bpr:>10.0f}   target <2048")

    print(f"\n# overall: {'PASS' if overall_pass else 'FAIL'}")
    sys.exit(0 if overall_pass else 1)


if __name__ == "__main__":
    main()
