#!/usr/bin/env python3
"""Does a 440 GB scan starve everything else?

THE acceptance test for "make the queries work without starving everything else".
Runs p1's 30d aggregate on one connection while a second connection opens a FRESH
connection every 2s and times `SELECT 1`. Mode C was never about the slow query —
it was that new connections timed out while it ran, so one tenant's dashboard took
the box away from everyone. A fresh connect per probe is the point: a pre-opened
one would not show an accept-loop that has stopped accepting.
"""
import pathlib, statistics, sys, threading, time
sys.path.insert(0, str(pathlib.Path(__file__).parent))
import psycopg
from probe import T

URL = (pathlib.Path(__file__).parent / "tfurl").read_text().strip()
P1 = "87576849-4941-49d3-a15d-680fef88a1a8"
stop = threading.Event()
probes, failures = [], []


def prober():
    while not stop.is_set():
        t = time.perf_counter()
        try:
            with psycopg.connect(URL, connect_timeout=10) as c, c.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchall()
            probes.append((time.perf_counter() - t) * 1000)
        except Exception as e:
            failures.append(str(e).splitlines()[0][:70])
        stop.wait(0.5)


th = threading.Thread(target=prober, daemon=True)
th.start()
time.sleep(12)
base = list(probes)
print(f"baseline: {len(base)} probes, median {statistics.median(base):.0f} ms", flush=True)

sql = (f"SELECT extract(epoch from time_bucket('6 hours', timestamp))::integer, count(*)::float FROM {T} "
       f"WHERE project_id = '{P1}' AND timestamp BETWEEN now() - interval '30 days' AND now() "
       f"GROUP BY time_bucket('6 hours', timestamp)")
t0 = time.perf_counter()
status = "ok"
try:
    with psycopg.connect(URL, connect_timeout=30) as c:
        c.autocommit = True
        with c.cursor() as cur:
            cur.execute("SET statement_timeout = '70s'")
            cur.execute(sql)
            cur.fetchall()
except Exception as e:
    status = str(e).splitlines()[0][:70]
heavy_ms = (time.perf_counter() - t0) * 1000
during = probes[len(base):]
time.sleep(6)
stop.set()
th.join(timeout=5)

print(f"heavy query: {heavy_ms:.0f} ms -> {status}", flush=True)
print(f"probes DURING: n={len(during)} median={statistics.median(during):.0f} ms max={max(during):.0f} ms" if during else "probes DURING: NONE", flush=True)
print(f"connect FAILURES during the whole run: {len(failures)}" + (f" -> {failures[:3]}" if failures else ""), flush=True)
print("VERDICT:", "REACHABLE" if failures == [] and during else "STARVED", flush=True)
