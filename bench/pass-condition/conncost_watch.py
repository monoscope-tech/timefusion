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
    for parent in HERE.resolve().parents:  # a checkout may be a worktree, so search upward
        env = parent / "monoscope" / ".env"
        if env.exists():
            for line in env.read_text().splitlines():
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
    # Host load rising and TimeFusion's OWN flush/maintenance burst are not
    # distinguishable from `load1` alone — a flush drives load up itself. These
    # are the counters that separate "the box got busy" from "we got busy".
    ("buffered_layer", "flush_completed_total"),
    ("buffered_layer", "flush_failed_total"),
    ("buffered_layer", "pressure_pct"),
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
FIELDS = ["ts", "load1", "kcompactd_cpu", "swap_free_mb", "iowait_pct", *STAGES, *(f"{c}.{k}" for c, k in STATS)]


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


def host():
    """`(load1, kcompactd_cpu, swap_free_mb, iowait_pct)`, or blanks if SSH fails.

    `load1` alone was not enough: load 43 cost 852 ms in one window and load 44
    cost 178 ms an hour earlier. `kcompactd` pinned at 100 % and a full swap are
    the box-wide allocation-pressure signal that separates those two windows,
    and neither is visible from inside the process.
    """
    try:
        out = subprocess.run(
            ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", SSH_HOST, "top -bn1 | head -20; free -m | grep -i swap"],
            capture_output=True, text=True, timeout=45,
        ).stdout
        load = out.rsplit("load average:", 1)[1].split(",")[0].strip()
        kcomp = next((l.split()[8] for l in out.splitlines() if "kcompact" in l), "0")
        swap_free = next((l.split()[3] for l in out.splitlines() if l.lower().startswith("swap")), "")
        wa = next((l.split("wa")[0].rsplit(",", 1)[1].strip() for l in out.splitlines() if "%Cpu(s)" in l and "wa" in l), "")
        return load, kcomp, swap_free, wa
    except Exception:
        return "", "", "", ""


def sample():
    load1, kcomp, swap_free, wa = host()
    row = {"ts": int(time.time()), "load1": load1, "kcompactd_cpu": kcomp, "swap_free_mb": swap_free, "iowait_pct": wa, **stages(), **stats()}
    new = not CSV.exists()
    with CSV.open("a", newline="") as f:
        w = csv.DictWriter(f, FIELDS)
        if new:
            w.writeheader()
        w.writerow(row)
    print(f"{time.strftime('%H:%M:%S')} uptime={row['runtime.uptime_seconds']}s load={row['load1']} "
          f"connect={row['connect']}ms reuse={row['reuse']}ms lag={row['runtime.scheduling_lag_ms']}ms kcompactd={kcomp}%", flush=True)
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

    def up(row):
        """Uptime, or None when the process could not report it — a row with no
        uptime has no known process identity, so it can neither extend a run nor
        be correlated against age. Treat it as a break, not as zero."""
        v = row.get("runtime.uptime_seconds")
        return float(v) if v not in (None, "") else None

    runs = [[]]
    for prev, row in zip([None, *all_rows], all_rows):
        same_process = up(row) is not None and prev is not None and up(prev) is not None and up(row) >= up(prev)
        runs[-1].append(row) if same_process else runs.append([row])
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

    # The spikes are the phenomenon, and a correlation over 5 samples hides
    # them: one sample went 270 -> 2,960 -> 252 ms while uptime rose
    # monotonically. So print the worst sample beside its neighbours — what
    # moved WITH it is the lead, and what didn't move is eliminated.
    # Only rows carrying in-process context can implicate anything, so a
    # pre-instrumentation row is never "the worst" here however slow it was.
    with_ctx = [r for r in all_rows if up(r) is not None]
    worst = max(with_ctx, key=lambda r: float(r["connect"] or 0))
    i = all_rows.index(worst)
    print(f"\n  worst `connect` sample and its neighbours (row {i} of {len(all_rows)}):")
    for r in all_rows[max(0, i - 1) : i + 2]:
        mark = "->" if r is worst else "  "
        print(f"  {mark} up={r['runtime.uptime_seconds'] or '?':>6}s load={r['load1'] or '?':>6} connect={r['connect']:>8}ms reuse={r['reuse']:>7}ms "
              f"lag={r['runtime.scheduling_lag_ms'] or '?'}ms refresh_avg_us={r['section.delta_snapshot_refresh.avg_us'] or '?'} "
              f"hold_max={r['block.journal_hold.max_ms'] or '?'}ms")
    for c, k in STATS:
        v = nums(f"{c}.{k}")
        if v and max(v) != min(v):
            print(f"  {c}.{k:32} {min(v):.0f} -> {max(v):.0f}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    # 5 min, not 30: prod is redeployed every ~30-50 min by whoever is working
    # that day, and a 30-min cadence yields ONE sample per process life while
    # `--analyze` needs three in a run. The age axis has to be sampled faster
    # than the thing that resets it.
    ap.add_argument("--every", type=int, default=300, help="seconds between samples")
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
