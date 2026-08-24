#!/usr/bin/env python3
"""Where do the 2.3 s of "opening a connection" actually go?

Decomposed, because on a box at load 39 every number looks like a connection
problem. Five stages, 6 reps each, medians reported:
  dns     — resolve the host
  tcp     — socket connect only, no protocol
  connect — full psycopg connect (startup packet, auth, ReadyForQuery)
  query   — SELECT 1 on an ALREADY-OPEN connection
  reuse   — second SELECT 1 on that same connection
If `query` on a warm connection is also ~seconds, this is CPU saturation and not
a connection-path defect at all.
"""
import pathlib, re, socket, statistics, time, urllib.parse
import psycopg

URL = (pathlib.Path(__file__).parent / "tfurl").read_text().strip()
u = urllib.parse.urlparse(URL)
HOST, PORT = u.hostname, u.port or 5432
N = 6
res = {k: [] for k in ("dns", "tcp", "connect", "query", "reuse")}

for _ in range(N):
    t = time.perf_counter(); ip = socket.gethostbyname(HOST); res["dns"].append((time.perf_counter()-t)*1000)
    t = time.perf_counter()
    s = socket.create_connection((ip, PORT), timeout=30); s.close()
    res["tcp"].append((time.perf_counter()-t)*1000)
    t = time.perf_counter()
    c = psycopg.connect(URL, connect_timeout=30)
    res["connect"].append((time.perf_counter()-t)*1000)
    with c.cursor() as cur:
        t = time.perf_counter(); cur.execute("SELECT 1"); cur.fetchall(); res["query"].append((time.perf_counter()-t)*1000)
        t = time.perf_counter(); cur.execute("SELECT 1"); cur.fetchall(); res["reuse"].append((time.perf_counter()-t)*1000)
    c.close()

for k in ("dns", "tcp", "connect", "query", "reuse"):
    v = res[k]
    print(f"{k:8} median={statistics.median(v):8.1f} ms   min={min(v):7.1f}  max={max(v):7.1f}", flush=True)
