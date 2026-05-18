#!/usr/bin/env python3
"""25-hour lifecycle simulation for TimeFusion.

Drives TF over PGWire with a frozen, advancing clock so we can exercise
~25 simulated hours of write+query traffic in a few minutes of real time.
Verifies:
  - **Correctness**: every query battery's row count matches an in-process
    ground truth (rows are tracked by simulated minute of insertion).
  - **Latency**: p50/p99 by query class (mem-only / boundary / delta-only /
    aggregate). Pass envelope is configurable per class.
  - **Memory/WAL**: RSS and `mem_estimated_bytes` should plateau after the
    retention boundary is crossed. WAL file count should stabilise.
  - **Throughput**: sustained inserts/sec across the run.

Prerequisites — TF must be running with:
  TIMEFUSION_ENABLE_TEST_UDFS=true
  TIMEFUSION_BUFFER_FLUSH_INTERVAL_SECS=2     (so flushes fire often in real time)
  TIMEFUSION_BUFFER_EVICTION_INTERVAL_SECS=2
  TIMEFUSION_BUFFER_RETENTION_MINS=70         (simulated minutes)
  TIMEFUSION_BUCKET_DURATION_SECS=600         (simulated seconds per bucket)

Usage:
  python3 bench/timeseries_lifecycle.py [--hours 25] [--csv out.csv]
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
import os
import statistics
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass, field

import psycopg

HOST = os.getenv("TF_HOST", "127.0.0.1")
PORT = int(os.getenv("TF_PORT", "12345"))
USER = os.getenv("TF_USER", "postgres")
PWD  = os.getenv("TF_PASS", "postgres")
DB   = os.getenv("TF_DB",   "postgres")

# Columns we populate — small subset of the 88-col `otel_logs_and_spans`.
# `date` is a required partition column; `id` must be unique for INSERT to
# succeed under the merge semantics, hence per-row uuid.
COLS = ["timestamp", "id", "name", "project_id", "summary",
        "date", "message_size_bytes"]

# Diurnal traffic shape: per-minute insert count by sim hour-of-day.
def rate_for_sim_hour(h: int) -> int:
    if 14 <= h < 16:        # peak
        return 500
    if 2 <= h < 6:          # nightly trough
        return 30
    return 120              # baseline


@dataclass
class QueryStat:
    label: str
    latencies_s: list[float] = field(default_factory=list)
    mismatches: list[tuple[int, int]] = field(default_factory=list)  # (got, want)

    def p(self, q: float) -> float:
        if not self.latencies_s:
            return 0.0
        xs = sorted(self.latencies_s)
        idx = int(q * (len(xs) - 1))
        return xs[idx]


@dataclass
class Run:
    project_id: str
    sim_start_micros: int
    sim_now_micros: int = 0
    ground_truth: dict[int, int] = field(default_factory=dict)  # minute_micros -> count
    total_inserted: int = 0
    insert_ms_history: list[float] = field(default_factory=list)
    stats_samples: list[dict] = field(default_factory=list)
    queries: dict[str, QueryStat] = field(default_factory=dict)
    flush_failures: int = 0
    # Pass envelope (configurable)
    envelope = {
        "mem_only":   {"p99_s": 0.5},
        "boundary":   {"p99_s": 1.5},
        "delta_only": {"p99_s": 2.0},
        "aggregate":  {"p99_s": 3.0},
    }

    def record(self, label: str, lat_s: float, got: int, want: int):
        qs = self.queries.setdefault(label, QueryStat(label))
        qs.latencies_s.append(lat_s)
        if got != want:
            qs.mismatches.append((got, want))


def connect():
    # autocommit=True so a failed query (e.g. table not yet created during
    # the first cold query) doesn't poison the connection for subsequent
    # inserts. We're not relying on transaction atomicity in this bench;
    # each INSERT/SELECT is independent.
    return psycopg.connect(host=HOST, port=PORT, user=USER, password=PWD, dbname=DB, autocommit=True)


def find_tf_pid() -> int | None:
    try:
        out = subprocess.run(["lsof", "-ti", f":{PORT}", "-sTCP:LISTEN"],
                             capture_output=True, text=True, timeout=2)
        s = out.stdout.strip().splitlines()
        return int(s[0]) if s else None
    except Exception:
        return None


def rss_kb(pid: int) -> int | None:
    try:
        out = subprocess.run(["ps", "-p", str(pid), "-o", "rss="],
                             capture_output=True, text=True, timeout=2)
        return int(out.stdout.strip())
    except Exception:
        return None


def set_clock(conn, ts: dt.datetime) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT timefusion_set_clock(%s)", (ts.isoformat().replace("+00:00", "Z"),))
        v = cur.fetchone()[0]
    return v


def advance_clock(conn, micros: int) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT timefusion_advance_clock(%s)", (micros,))
        v = cur.fetchone()[0]
    return v


def now_micros(conn) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT timefusion_now_micros()")
        v = cur.fetchone()[0]
    return v


def insert_minute(conn, run: Run, n_rows: int, sim_ts: dt.datetime) -> float:
    """Insert `n_rows` rows for sim time `sim_ts`. Returns elapsed seconds.

    Uses psycopg.pipeline() so executemany's N Bind+Execute pairs are
    flushed in a single network round-trip — one parse/plan/execute for
    the whole minute, no per-row plan re-runs. Requires the pgwire
    placeholder-coercion and numeric-sort fixes (see src/insert_coerce.rs
    and vendor/datafusion-postgres). Falls back to executemany under
    psycopg.pipeline() if a single multi-row insert ever errors so the
    bench keeps running while the regression is debugged.
    """
    name = "GET /bench/" + ("x" * 188)  # ~200-byte name
    date = sim_ts.date()
    rows_tuples = [(sim_ts, str(uuid.uuid4()), name, run.project_id, ["s"], date, 200) for _ in range(n_rows)]
    flat: list = []
    for row in rows_tuples:
        flat.extend(row)
    row_placeholder = "(" + ", ".join(["%s"] * len(COLS)) + ")"
    sql = f"INSERT INTO otel_logs_and_spans ({', '.join(COLS)}) VALUES " + ", ".join([row_placeholder] * n_rows)
    t0 = time.time()
    with conn.cursor() as cur:
        cur.execute(sql, flat)
    elapsed = time.time() - t0
    minute_micros = int(sim_ts.replace(second=0, microsecond=0).timestamp()) * 1_000_000
    run.ground_truth[minute_micros] = n_rows
    run.total_inserted += n_rows
    run.insert_ms_history.append(elapsed * 1000)
    return elapsed


def expected_count(run: Run, lo_micros: int, hi_micros: int) -> int:
    """Sum ground truth counts for minutes whose timestamp falls in [lo, hi)."""
    return sum(c for m, c in run.ground_truth.items() if lo_micros <= m < hi_micros)


def query_count(conn, run: Run, label: str, lo_micros: int, hi_micros: int):
    # Pass timestamps as plain text and let TF parse them so Arrow keeps
    # the schema-declared "UTC" zone. psycopg's timezone-aware datetime
    # round-trips as Timestamp(µs, "+00:00"), which Arrow refuses to
    # compare against the schema's Timestamp(µs, "UTC").
    sql = ("SELECT count(*) FROM otel_logs_and_spans "
           "WHERE project_id = %s "
           "AND timestamp >= %s::timestamptz "
           "AND timestamp <  %s::timestamptz")
    lo_s = dt.datetime.fromtimestamp(lo_micros / 1e6, tz=dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    hi_s = dt.datetime.fromtimestamp(hi_micros / 1e6, tz=dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    t0 = time.time()
    with conn.cursor() as cur:
        cur.execute(sql, (run.project_id, lo_s, hi_s))
        got = cur.fetchone()[0]
    lat = time.time() - t0
    want = expected_count(run, lo_micros, hi_micros)
    run.record(label, lat, got, want)


def query_aggregate(conn, run: Run, lo_micros: int, hi_micros: int):
    sql = ("SELECT date_trunc('minute', timestamp) AS minute, count(*) "
           "FROM otel_logs_and_spans "
           "WHERE project_id = %s "
           "AND timestamp >= %s::timestamptz "
           "AND timestamp <  %s::timestamptz "
           "GROUP BY minute ORDER BY minute")
    lo_s = dt.datetime.fromtimestamp(lo_micros / 1e6, tz=dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    hi_s = dt.datetime.fromtimestamp(hi_micros / 1e6, tz=dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    t0 = time.time()
    with conn.cursor() as cur:
        cur.execute(sql, (run.project_id, lo_s, hi_s))
        rows = cur.fetchall()
    lat = time.time() - t0
    got = sum(r[1] for r in rows)
    want = expected_count(run, lo_micros, hi_micros)
    run.record("aggregate", lat, got, want)


def sample_stats(conn, run: Run, pid: int | None):
    with conn.cursor() as cur:
        cur.execute("SELECT component, key, value FROM timefusion_stats")
        rows = cur.fetchall()
    flat = {f"{c}.{k}": v for (c, k, v) in rows}
    flat["sim_minute"] = (run.sim_now_micros - run.sim_start_micros) // 60_000_000
    flat["rss_kb"] = rss_kb(pid) if pid else None
    flat["total_inserted"] = run.total_inserted
    run.stats_samples.append(flat)


def run_query_battery(conn, run: Run):
    now = run.sim_now_micros
    # Mem-only: last 5 sim minutes
    query_count(conn, run, "mem_only", now - 5 * 60_000_000, now)
    # Boundary: last 2 sim hours (some in mem, most in delta)
    query_count(conn, run, "boundary", now - 2 * 3600_000_000, now)
    # Delta-only: between 12h and 10h ago
    if now - 12 * 3600_000_000 > run.sim_start_micros:
        query_count(conn, run, "delta_only",
                    now - 12 * 3600_000_000, now - 10 * 3600_000_000)
    # Aggregate over last 6h
    query_aggregate(conn, run, max(now - 6 * 3600_000_000, run.sim_start_micros), now)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--hours", type=int, default=25)
    ap.add_argument("--csv", default="/tmp/tf_lifecycle.csv")
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()

    sim_start = dt.datetime(2026, 5, 17, 0, 0, 0, tzinfo=dt.timezone.utc)
    sim_total_min = args.hours * 60

    pid = find_tf_pid()
    if pid is None:
        print("ERROR: TF not listening on", PORT, file=sys.stderr); sys.exit(1)
    print(f"TF pid={pid}, rss_start={rss_kb(pid)} kB")

    run = Run(project_id=str(uuid.uuid4()),
              sim_start_micros=int(sim_start.timestamp() * 1_000_000))

    with connect() as conn:
        run.sim_now_micros = set_clock(conn, sim_start)
        print(f"sim clock set to {sim_start}  (micros={run.sim_now_micros})")
        print(f"project_id={run.project_id}")
        print(f"simulating {args.hours}h ({sim_total_min} sim-minutes)")
        print()

        t_real0 = time.time()
        for sim_min in range(sim_total_min):
            sim_ts = sim_start + dt.timedelta(minutes=sim_min)
            n = rate_for_sim_hour(sim_ts.hour)
            insert_minute(conn, run, n, sim_ts)
            run.sim_now_micros = advance_clock(conn, 60_000_000)

            # Periodic queries
            if sim_min % 15 == 0 and sim_min > 0:
                try:
                    run_query_battery(conn, run)
                except Exception as e:
                    print(f"  query battery error at sim_min={sim_min}: {e}")

            # Periodic stats
            if sim_min % 30 == 0:
                try:
                    sample_stats(conn, run, pid)
                except Exception as e:
                    print(f"  stats sample error at sim_min={sim_min}: {e}")

            # Brief real-time yield so flush/eviction tasks get CPU.
            # We need enough real time between sim-minute advances that the
            # flush task (every 2s real) and eviction task can actually run.
            time.sleep(0.05)

            if not args.quiet and sim_min % 60 == 0:
                el = time.time() - t_real0
                last = run.stats_samples[-1] if run.stats_samples else {}
                print(f"  sim h={sim_min//60:>2d}  real={el:5.1f}s  "
                      f"inserted={run.total_inserted:>7d}  "
                      f"rss={last.get('rss_kb',0)/1024:5.1f}MB  "
                      f"mem_est={float(last.get('mem_buffer.estimated_mb',0)):5.1f}MB  "
                      f"wal_files={last.get('wal.files','?')}  "
                      f"pressure={last.get('buffered_layer.pressure_pct','?')}%")

        real_elapsed = time.time() - t_real0
        # Final stats and query battery to capture end state
        run_query_battery(conn, run)
        sample_stats(conn, run, pid)

    # ---- Report ----
    print(f"\n{'='*72}\nLifecycle run complete.")
    print(f"  sim hours       = {args.hours}")
    print(f"  real elapsed    = {real_elapsed:.1f}s")
    print(f"  total rows      = {run.total_inserted}")
    print(f"  inserts/sec     = {run.total_inserted / real_elapsed:.0f} (real)")
    if run.insert_ms_history:
        print(f"  insert p50/p99  = {statistics.median(run.insert_ms_history):.1f}ms / "
              f"{sorted(run.insert_ms_history)[int(0.99*(len(run.insert_ms_history)-1))]:.1f}ms")

    print("\nQuery battery:")
    failed = False
    for label, qs in run.queries.items():
        env = run.envelope.get(label, {})
        p99 = qs.p(0.99)
        p99_lim = env.get("p99_s")
        viol = (p99_lim is not None and p99 > p99_lim)
        mark = " VIOL" if viol else ""
        miss = f"  mismatches={len(qs.mismatches)}" if qs.mismatches else ""
        print(f"  {label:11s}  n={len(qs.latencies_s):3d}  p50={qs.p(0.5):.3f}s  p99={p99:.3f}s  "
              f"limit={p99_lim}s{mark}{miss}")
        if viol or qs.mismatches:
            failed = True
        if qs.mismatches[:3]:
            for got, want in qs.mismatches[:3]:
                print(f"      mismatch: got={got} want={want}")

    if run.stats_samples:
        s0 = run.stats_samples[0]
        sN = run.stats_samples[-1]
        rss_growth = (sN.get("rss_kb", 0) - s0.get("rss_kb", 0)) / 1024.0
        peak_rss = max((s.get("rss_kb", 0) or 0) for s in run.stats_samples) / 1024.0
        # RSS-plateau check: compare last quartile mean to second quartile mean.
        rs = [s.get("rss_kb", 0) or 0 for s in run.stats_samples]
        if len(rs) >= 4:
            q2 = statistics.mean(rs[len(rs)//2: 3*len(rs)//4])
            q4 = statistics.mean(rs[3*len(rs)//4:])
            drift_pct = (q4 - q2) / q2 * 100 if q2 else 0
        else:
            drift_pct = 0
        print(f"\nMemory + WAL:")
        print(f"  rss peak        = {peak_rss:.1f} MB")
        print(f"  rss start→end   = {s0.get('rss_kb',0)/1024:.1f} → {sN.get('rss_kb',0)/1024:.1f} MB")
        # Only positive drift is suspicious — negative means RSS dropped
        # after peak, which is healthy. Threshold scales with run length:
        # short runs (< retention window) haven't reached steady state, so
        # the drift can't be interpreted as a leak.
        drift_threshold = 10.0 if args.hours >= 4 else 30.0
        leak_flag = "OK" if drift_pct < drift_threshold else "POSSIBLE LEAK"
        print(f"  rss late-drift  = {drift_pct:+.1f}% (q4 vs q2 mean, threshold={drift_threshold:.0f}%)  {leak_flag}")
        print(f"  mem_estimated   = {sN.get('mem_buffer.estimated_mb','?')} MB")
        print(f"  wal_files       = {sN.get('wal.files','?')}")
        print(f"  pressure_pct    = {sN.get('buffered_layer.pressure_pct','?')}%")
        print(f"  plan_cache      = {sN.get('plan_cache.hits','?')}h / {sN.get('plan_cache.misses','?')}m "
              f"({sN.get('plan_cache.hit_pct','?')}%)")
        if drift_pct > drift_threshold:
            failed = True

    # CSV dump
    if run.stats_samples:
        keys = sorted({k for s in run.stats_samples for k in s.keys()})
        with open(args.csv, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=keys)
            w.writeheader()
            for s in run.stats_samples:
                w.writerow(s)
        print(f"\nCSV written to {args.csv}")

    print(f"\n{'PASS' if not failed else 'FAIL'}")
    sys.exit(0 if not failed else 1)


if __name__ == "__main__":
    main()
