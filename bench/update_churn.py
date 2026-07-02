#!/usr/bin/env python3
"""
UPDATE-churn benchmark: reproduces monoscope's UPDATE ... FROM load pattern
concurrently with ingest + reads, to measure how much the DML Delta leg
convoys inserts and SELECTs.

Threads:
  - insert threads: batched INSERTs, ack latency recorded
  - update threads: monoscope-shaped `UPDATE ... FROM (VALUES ...)` joining on
    id against recently-inserted rows, latency recorded
  - reader thread: time-bounded COUNT, latency recorded

Reports p50/p95/p99/max per op class + error counts.

Usage: python3 bench/update_churn.py --duration 60 --inserters 4 --updaters 2
"""
from __future__ import annotations
import argparse, collections, os, random, statistics, threading, time, uuid
from datetime import datetime, timezone

import psycopg

HOST = os.environ.get("PGWIRE_HOST", "127.0.0.1")
PORT = int(os.environ.get("PGWIRE_PORT", "12345"))
URL = f"host={HOST} port={PORT} user=postgres password=postgres dbname=postgres"
PROJECT = os.environ.get("BENCH_PROJECT", "bench_upd")
TABLE = "otel_logs_and_spans"

stop = threading.Event()
lat: dict[str, list[float]] = {"insert": [], "update": [], "select": []}
errs: dict[str, int] = collections.defaultdict(int)
rows_updated_total = 0
recent_ids: collections.deque[str] = collections.deque(maxlen=50_000)
lock = threading.Lock()


def now_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")


def insert_worker(batch: int):
    conn = psycopg.connect(URL, autocommit=True)
    today = datetime.now(timezone.utc).date().isoformat()
    while not stop.is_set():
        ids = [uuid.uuid4().hex for _ in range(batch)]
        vals = ",".join(
            f"('{now_ts()}','{i}','span_{random.randint(0, 500)}','{PROJECT}','{today}',ARRAY[]::text[],ARRAY[]::text[],{random.randint(1, 10**6)},'INFO','OK')"
            for i in ids
        )
        sql = (
            f"INSERT INTO {TABLE} (timestamp,id,name,project_id,date,hashes,summary,duration,level,status_code) VALUES {vals}"
        )
        t0 = time.perf_counter()
        try:
            conn.execute(sql)
            lat["insert"].append(time.perf_counter() - t0)
            with lock:
                recent_ids.extend(ids)
        except Exception as e:
            errs["insert"] += 1
            if errs["insert"] <= 3:
                print(f"insert error: {e}")
        time.sleep(0.05)


def update_worker(source_rows: int, every: float):
    global rows_updated_total
    conn = psycopg.connect(URL, autocommit=True)
    while not stop.is_set():
        with lock:
            if len(recent_ids) < source_rows:
                pass_ids = None
            else:
                # Mix of ages: some fresh (buffer), some old (likely flushed).
                pool = list(recent_ids)
                pass_ids = random.sample(pool, source_rows)
        if not pass_ids:
            time.sleep(0.2)
            continue
        vals = ",".join(f"('{i}','pat:{random.randint(0,99)}')" for i in pass_ids)
        sql = (
            f"UPDATE {TABLE} o SET hashes = COALESCE(o.hashes,ARRAY[]::text[]) || ARRAY[u.tag] "
            f"FROM (VALUES {vals}) AS u(id, tag) "
            f"WHERE o.project_id = '{PROJECT}' AND o.id = u.id"
        )
        t0 = time.perf_counter()
        try:
            cur = conn.execute(sql)
            lat["update"].append(time.perf_counter() - t0)
            # pgwire returns a command tag ("UPDATE <n>"), not a result set.
            tag = cur.statusmessage or ""
            if tag.startswith("UPDATE"):
                with lock:
                    rows_updated_total += int(tag.split()[-1])
        except Exception as e:
            errs["update"] += 1
            if errs["update"] <= 3:
                print(f"update error: {e}")
        time.sleep(every)


def reader_worker():
    conn = psycopg.connect(URL, autocommit=True)
    while not stop.is_set():
        t0 = time.perf_counter()
        try:
            conn.execute(
                f"SELECT COUNT(*) FROM {TABLE} WHERE project_id = '{PROJECT}' AND timestamp >= NOW() - INTERVAL '5 minutes'"
            ).fetchone()
            lat["select"].append(time.perf_counter() - t0)
        except Exception as e:
            errs["select"] += 1
            if errs["select"] <= 3:
                print(f"select error: {e}")
        time.sleep(0.25)


def pct(xs: list[float], p: float) -> float:
    if not xs:
        return float("nan")
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(p / 100 * len(xs)))]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--duration", type=int, default=60)
    ap.add_argument("--inserters", type=int, default=4)
    ap.add_argument("--updaters", type=int, default=2)
    ap.add_argument("--batch", type=int, default=200)
    ap.add_argument("--source-rows", type=int, default=24)
    ap.add_argument("--update-every", type=float, default=0.3)
    args = ap.parse_args()

    threads = (
        [threading.Thread(target=insert_worker, args=(args.batch,)) for _ in range(args.inserters)]
        + [threading.Thread(target=update_worker, args=(args.source_rows, args.update_every)) for _ in range(args.updaters)]
        + [threading.Thread(target=reader_worker)]
    )
    for t in threads:
        t.start()
    time.sleep(args.duration)
    stop.set()
    for t in threads:
        t.join()

    print(f"\n===== update_churn results ({args.duration}s) =====")
    for k in ("insert", "update", "select"):
        xs = lat[k]
        if not xs:
            print(f"{k:7s}: no samples (errors={errs[k]})")
            continue
        print(
            f"{k:7s}: n={len(xs):5d} p50={pct(xs,50)*1000:8.1f}ms p95={pct(xs,95)*1000:8.1f}ms "
            f"p99={pct(xs,99)*1000:8.1f}ms max={max(xs)*1000:8.1f}ms mean={statistics.mean(xs)*1000:8.1f}ms errors={errs[k]}"
        )
    total_rows = len(lat["insert"]) * args.batch
    print(f"rows inserted ≈ {total_rows}, rows updated = {rows_updated_total}, updates run = {len(lat['update'])}")


if __name__ == "__main__":
    main()
