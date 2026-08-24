#!/usr/bin/env python3
"""Phase 0 of the "SELECT 1 costs seconds on an old process" plan: deconfound
process AGE from host LOAD.

The two samples that opened that plan differ in both (5 h / load 39 vs 4 min /
load 22), so neither is attributable. This runs the `conncost.py` decomposition
on a schedule and records, with EVERY sample, the things that were missing:
process uptime, runtime scheduling lag, blocking-section hold times, host load.
One CSV row per sample; `--analyze` reads it back.

    python3 conncost_watch.py                 # sample every 30 min, append to conncost_watch.csv
    python3 conncost_watch.py --once          # one sample
    python3 conncost_watch.py --analyze       # correlate cost against uptime and load

Read-only against prod: SELECT 1 and timefusion_stats, plus `uptime` over the
documented read-only SSH. URL from ./tfurl, $TIMEFUSION_PG_URL or monoscope's .env.

`runtime.*` and `block.*` are empty until a build carrying them is deployed —
before that, nothing in-process reported its own age.

Two clean outcomes, per the plan:
  cost tracks UPTIME, not load -> state that grows in-process; a scheduled
      restart is the mitigation while the real cause is found.
  cost tracks LOAD, not uptime -> contention; a restart buys nothing.
A run is only worth analyzing if it spans a process nobody redeploys —
`uptime_s` resetting mid-run splits the series, and `--analyze` says so.
"""
import argparse, csv, os, pathlib, socket, statistics, subprocess, sys, time, urllib.parse
import psycopg

HERE = pathlib.Path(__file__).parent


def _url():
    """./tfurl, else $TIMEFUSION_PG_URL, else monoscope's .env (which always
    carries the current prod URL because it dual-writes to TF)."""
    if (f := HERE / "tfurl").exists():
        return f.read_text().strip()
    if (env := os.environ.get("TIMEFUSION_PG_URL")):
        return env.strip()
    for candidate in (HERE / "../../../monoscope/.env", HERE / "../../../../monoscope/.env"):
        if candidate.exists():
            for line in candidate.read_text().splitlines():
                if line.startswith("TIMEFUSION_PG_URL="):
                    return line.split("=", 1)[1].strip()
    sys.exit("no connection URL: create ./tfurl or set TIMEFUSION_PG_URL")


URL = _url()
CSV = HERE / "conncost_watch.csv"
SSH_HOST = "ubuntu@captain.s.past3.tech"
N = 6  # reps per sample, same as conncost.py

# (component, key) pulled from timefusion_stats with every sample. Each is a
# candidate from the plan's hypothesis table: runtime.* is "workers blocked
# while cores idle", block.* names WHICH section blocked them, and the rest are
# the state that grows with uptime (mem buffer, plan cache, provider caches).
STATS = [
    ("runtime", "uptime_seconds"),
    ("runtime", "scheduling_lag_ms"),
    ("runtime", "scheduling_lag_max_ms"),
    ("block", "journal_lock_wait.max_ms"),
    ("block", "journal_hold.max_ms"),
    ("block", "journal_hold.total_ms"),
    # Hypothesis 3, directly: cost that scales with the active file list rather
    # than with what the query reads. Every query on the resolve path pays it.
    ("section", "delta_snapshot_refresh.avg_us"),
    ("section", "delta_snapshot_refresh.max_ms"),
    ("section", "delta_snapshot_refresh.count"),
    ("mem_buffer", "total_rows"),
    ("mem_buffer", "estimated_bytes_approx"),
    ("mem_buffer", "total_buckets"),
    ("plan_cache", "hits"),
    ("plan_cache", "misses"),
    ("memory", "charged_bytes"),
    ("scan", "provider_cache_entries"),
    ("scan", "fast_resolve_cache_entries"),
    ("pgwire", "queries_total"),
    ("maintenance", "tasks_pending"),
    ("maintenance", "tasks_running"),
]
STAGES = ("dns", "tcp", "connect", "query", "reuse")
FIELDS = ["ts", "load1", *STAGES, *(f"{c}.{k}" for c, k in STATS)]


def stages():
    """Medians of the five conncost stages, ms."""
    u = urllib.parse.urlparse(URL)
    host, port = u.hostname, u.port or 5432
    res = {k: [] for k in STAGES}
    for _ in range(N):
        t = time.perf_counter(); ip = socket.gethostbyname(host); res["dns"].append((time.perf_counter() - t) * 1000)
        t = time.perf_counter()
        socket.create_connection((ip, port), timeout=30).close()
        res["tcp"].append((time.perf_counter() - t) * 1000)
        t = time.perf_counter(); c = psycopg.connect(URL, connect_timeout=30); res["connect"].append((time.perf_counter() - t) * 1000)
        with c.cursor() as cur:
            for stage in ("query", "reuse"):
                t = time.perf_counter(); cur.execute("SELECT 1"); cur.fetchall(); res[stage].append((time.perf_counter() - t) * 1000)
        c.close()
    return {k: round(statistics.median(v), 1) for k, v in res.items()}


def stats():
    """The (component, key) pairs above, as strings. Missing keys read ''."""
    with psycopg.connect(URL, connect_timeout=30) as c, c.cursor() as cur:
        cur.execute("SELECT component, key, value FROM timefusion_stats")
        got = {(r[0], r[1]): r[2] for r in cur.fetchall()}
    return {f"{c}.{k}": got.get((c, k), "") for c, k in STATS}


def host_load1():
    """1-minute load average from the host, or '' if SSH is unavailable."""
    try:
        out = subprocess.run(["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", SSH_HOST, "uptime"], capture_output=True, text=True, timeout=30)
        return out.stdout.rsplit("load average:", 1)[1].split(",")[0].strip()
    except Exception:
        return ""


def sample():
    row = {"ts": int(time.time()), "load1": host_load1(), **stages(), **stats()}
    new = not CSV.exists()
    with CSV.open("a", newline="") as f:
        w = csv.DictWriter(f, FIELDS)
        if new:
            w.writeheader()
        w.writerow(row)
    print(f"{time.strftime('%H:%M:%S')} uptime={row['runtime.uptime_seconds']}s load={row['load1']} "
          f"connect={row['connect']}ms reuse={row['reuse']}ms lag={row['runtime.scheduling_lag_ms']}ms", flush=True)
    return row


def analyze():
    rows = list(csv.DictReader(CSV.open()))
    if len(rows) < 3:
        sys.exit(f"{len(rows)} samples — not enough to attribute anything.")

    def nums(key):
        return [float(r[key]) for r in rows if r.get(key) not in (None, "")]

    # A restart resets uptime, which SPLITS the series: samples either side are
    # different processes, so a correlation across the break is meaningless.
    # Correlate only inside the longest uninterrupted run — warning about the
    # break and then correlating across it anyway is the same mistake with a
    # disclaimer attached.
    all_rows = rows
    runs = [[all_rows[0]]]
    for prev, row in zip(all_rows, all_rows[1:]):
        restarted = float(row["runtime.uptime_seconds"] or 0) < float(prev["runtime.uptime_seconds"] or 0)
        runs.append([row]) if restarted else runs[-1].append(row)
    rows = max(runs, key=len)  # `nums` reads this name — late binding is deliberate

    ups = nums("runtime.uptime_seconds")
    span_h = (int(all_rows[-1]["ts"]) - int(all_rows[0]["ts"])) / 3600
    print(f"{len(all_rows)} samples over {span_h:.1f} h in {len(runs)} process run(s)")
    if len(runs) > 1:
        print(f"  ! restarts split the series; analyzing the longest run only ({len(rows)} samples)")
    if len(rows) < 3:
        sys.exit("  the longest uninterrupted run is too short — Phase 0 needs a process nobody redeploys")
    print(f"  uptime spanned {min(ups)/3600:.1f}–{max(ups)/3600:.1f} h")

    for cost in ("connect", "query", "reuse"):
        line = [f"{cost:8}"]
        for driver in ("runtime.uptime_seconds", "load1"):
            pairs = [(float(r[driver]), float(r[cost])) for r in rows if r.get(driver) not in (None, "")]
            if len(pairs) < 3 or len({p[0] for p in pairs}) < 2:
                line.append(f"{driver.split('.')[-1]}=n/a")
                continue
            x, y = zip(*pairs)
            line.append(f"{driver.split('.')[-1]}: r={statistics.correlation(x, y):+.2f}")
        print("  " + "   ".join(line))
    print("\n  r near +1 for exactly one driver is the answer; both high means they are still confounded.")
    for c, k in STATS:
        v = nums(f"{c}.{k}")
        if v and max(v) != min(v):
            print(f"  {c}.{k:32} {min(v):.0f} -> {max(v):.0f}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--every", type=int, default=1800, help="seconds between samples")
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--analyze", action="store_true")
    a = ap.parse_args()
    if a.analyze:
        analyze()
    elif a.once:
        sample()
    else:
        while True:
            try:
                sample()
            except Exception as e:  # a refused connection IS a data point; keep the series going
                print(f"{time.strftime('%H:%M:%S')} sample failed: {e}", flush=True)
            time.sleep(a.every)
