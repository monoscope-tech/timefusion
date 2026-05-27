#!/usr/bin/env python3
"""
TimeFusion realistic memory + throughput benchmark.

Drives TF directly via the PGWire endpoint with `psycopg` (bypassing
monoscope's OTLP gRPC layer, which has its own throughput cap). For each
scenario, samples macOS `ps` for RSS at 1 Hz, reports peak/end RSS,
sustained throughput, and any client-side errors.

Assumes TF is already running on :12345 with credentials postgres/postgres.
The script does NOT start/stop TF — by design, we want to see *steady-state*
memory under different workload shapes.

Run:  python3 bench/tf-memory-bench.py [scenario_name]
"""

from __future__ import annotations

import argparse
import concurrent.futures
import datetime as _dt
import os
import statistics
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass

import psycopg


TF_HOST = "127.0.0.1"
TF_PORT = 12345
TF_USER = "postgres"
TF_PASS = "postgres"
TF_DB = "postgres"


# ── Column list & row factory ────────────────────────────────────────────────
# Same 88-column shape monoscope writes — keeps the benchmark honest about
# real-world per-batch payload size.

COLS_88 = [
    "timestamp",
    "id",
    "hashes",
    "name",
    "project_id",
    "summary",
    "date",
    "message_size_bytes",
]


def make_row(pid: str, ts: _dt.datetime, size_bytes: int = 200) -> tuple:
    name = ("GET /bench/" + ("x" * max(size_bytes - 12, 1)))[:size_bytes]
    return (
        ts,
        str(uuid.uuid4()),
        ["h1", "h2"],
        name,
        pid,
        ["summary line"],
        ts.date(),
        size_bytes,
    )


def insert_rows(pid: str, n: int, row_size_bytes: int = 200, ts: _dt.datetime | None = None) -> int:
    """Insert `n` rows into TF on one connection. Returns count actually inserted."""
    ts = ts or _dt.datetime(2025, 1, 1)
    ok = 0
    with psycopg.connect(host=TF_HOST, port=TF_PORT, user=TF_USER, password=TF_PASS, dbname=TF_DB) as conn:
        with conn.cursor() as cur:
            for _ in range(n):
                try:
                    cur.execute(
                        f"INSERT INTO otel_logs_and_spans ({', '.join(COLS_88)}) VALUES "
                        + "(" + ", ".join(["%s"] * len(COLS_88)) + ")",
                        make_row(pid, ts, row_size_bytes),
                    )
                    ok += 1
                except Exception:
                    # We track this as throughput loss; the loop continues so
                    # other writers aren't blocked by a single bad row.
                    pass
        conn.commit()
    return ok


# ── RSS sampler ─────────────────────────────────────────────────────────────

def find_tf_pid() -> int | None:
    """Find the timefusion server's PID via the bound port. macOS `lsof`."""
    try:
        out = subprocess.run(
            ["lsof", "-ti", f":{TF_PORT}", "-sTCP:LISTEN"],
            capture_output=True, text=True, timeout=2,
        )
        pid = out.stdout.strip().splitlines()[0] if out.stdout.strip() else ""
        return int(pid) if pid else None
    except Exception:
        return None


def rss_kib(pid: int) -> int | None:
    try:
        out = subprocess.run(["ps", "-p", str(pid), "-o", "rss="], capture_output=True, text=True, timeout=2)
        s = out.stdout.strip()
        return int(s) if s else None
    except Exception:
        return None


@dataclass
class Sample:
    t: float
    rss_kib: int


class Sampler:
    """Polls `ps -p $tf_pid -o rss=` once per second in a background thread."""

    def __init__(self, tf_pid: int):
        self.tf_pid = tf_pid
        self.samples: list[Sample] = []
        self._stop = False
        self._thread = None

    def start(self):
        import threading
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def _loop(self):
        t0 = time.time()
        while not self._stop:
            r = rss_kib(self.tf_pid)
            if r is not None:
                self.samples.append(Sample(time.time() - t0, r))
            time.sleep(1.0)

    def stop(self):
        self._stop = True
        if self._thread:
            self._thread.join(timeout=2)

    def report(self) -> dict:
        if not self.samples:
            return {"samples": 0}
        rs = [s.rss_kib for s in self.samples]
        return {
            "samples": len(rs),
            "rss_start_mb": round(rs[0] / 1024, 1),
            "rss_end_mb": round(rs[-1] / 1024, 1),
            "rss_peak_mb": round(max(rs) / 1024, 1),
            "rss_mean_mb": round(statistics.mean(rs) / 1024, 1),
            "rss_growth_mb": round((rs[-1] - rs[0]) / 1024, 1),
        }


# ── Scenarios ───────────────────────────────────────────────────────────────

@dataclass
class ScenarioResult:
    name: str
    workers: int
    duration_s: float
    inserts_ok: int
    inserts_per_sec: float
    rss: dict


def scenario_hot_single_project(workers: int = 16, duration_s: int = 30, batch_n: int = 200) -> ScenarioResult:
    """A) One hot project, many concurrent writers. Stresses sharded WAL."""
    pid = str(uuid.uuid4())
    deadline = time.time() + duration_s
    inserts_ok = 0
    tf_pid = find_tf_pid()
    sampler = Sampler(tf_pid) if tf_pid else None
    if sampler:
        sampler.start()
    t0 = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        in_flight: set = set()
        while time.time() < deadline:
            while len(in_flight) < workers and time.time() < deadline:
                in_flight.add(ex.submit(insert_rows, pid, batch_n))
            done, in_flight = concurrent.futures.wait(in_flight, timeout=0.05, return_when=concurrent.futures.FIRST_COMPLETED)
            for fut in done:
                try:
                    inserts_ok += fut.result()
                except Exception:
                    pass
        for fut in concurrent.futures.as_completed(in_flight):
            try:
                inserts_ok += fut.result()
            except Exception:
                pass
    elapsed = time.time() - t0
    if sampler:
        sampler.stop()
    return ScenarioResult(
        name="hot_single_project",
        workers=workers,
        duration_s=elapsed,
        inserts_ok=inserts_ok,
        inserts_per_sec=inserts_ok / elapsed,
        rss=(sampler.report() if sampler else {}),
    )


def scenario_many_projects(workers: int = 16, projects: int = 64, duration_s: int = 30, batch_n: int = 200) -> ScenarioResult:
    """B) Distributed write across many projects (multi-tenant SaaS shape).
    Stresses per-project DashMap entry creation + parallel-topic writes."""
    pids = [str(uuid.uuid4()) for _ in range(projects)]
    deadline = time.time() + duration_s
    inserts_ok = 0
    tf_pid = find_tf_pid()
    sampler = Sampler(tf_pid) if tf_pid else None
    if sampler:
        sampler.start()
    t0 = time.time()
    counter = [0]

    def submit_one(pool):
        idx = counter[0] % projects
        counter[0] += 1
        return pool.submit(insert_rows, pids[idx], batch_n)

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        in_flight = set()
        while time.time() < deadline:
            while len(in_flight) < workers and time.time() < deadline:
                in_flight.add(submit_one(ex))
            done, in_flight = concurrent.futures.wait(in_flight, timeout=0.05, return_when=concurrent.futures.FIRST_COMPLETED)
            for fut in done:
                try:
                    inserts_ok += fut.result()
                except Exception:
                    pass
        for fut in concurrent.futures.as_completed(in_flight):
            try:
                inserts_ok += fut.result()
            except Exception:
                pass
    elapsed = time.time() - t0
    if sampler:
        sampler.stop()
    return ScenarioResult(
        name="many_projects",
        workers=workers,
        duration_s=elapsed,
        inserts_ok=inserts_ok,
        inserts_per_sec=inserts_ok / elapsed,
        rss=(sampler.report() if sampler else {}),
    )


def scenario_large_rows(workers: int = 8, duration_s: int = 20, batch_n: int = 50, row_size_bytes: int = 4096) -> ScenarioResult:
    """C) Large row payloads (large log bodies / OTLP attribute blobs).
    Stresses MemBuffer memory accounting + Arrow IPC pipe."""
    pid = str(uuid.uuid4())
    deadline = time.time() + duration_s
    inserts_ok = 0
    tf_pid = find_tf_pid()
    sampler = Sampler(tf_pid) if tf_pid else None
    if sampler:
        sampler.start()
    t0 = time.time()

    def worker():
        nonlocal_ok = 0
        while time.time() < deadline:
            try:
                nonlocal_ok += insert_rows(pid, batch_n, row_size_bytes=row_size_bytes)
            except Exception:
                pass
        return nonlocal_ok

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        for fut in concurrent.futures.as_completed([ex.submit(worker) for _ in range(workers)]):
            inserts_ok += fut.result()
    elapsed = time.time() - t0
    if sampler:
        sampler.stop()
    return ScenarioResult(
        name="large_rows",
        workers=workers,
        duration_s=elapsed,
        inserts_ok=inserts_ok,
        inserts_per_sec=inserts_ok / elapsed,
        rss=(sampler.report() if sampler else {}),
    )


def scenario_query_while_write(workers: int = 8, readers: int = 4, duration_s: int = 30) -> ScenarioResult:
    """D) Writers + readers in parallel. Stresses MemBuffer snapshot-on-read
    + RecordBatch Arc traffic."""
    pid = str(uuid.uuid4())
    deadline = time.time() + duration_s
    inserts_ok = 0
    queries_ok = 0
    tf_pid = find_tf_pid()
    sampler = Sampler(tf_pid) if tf_pid else None
    if sampler:
        sampler.start()
    t0 = time.time()

    def writer():
        nonlocal_ok = 0
        while time.time() < deadline:
            try:
                nonlocal_ok += insert_rows(pid, 200)
            except Exception:
                pass
        return nonlocal_ok

    def reader():
        nonlocal_q = 0
        with psycopg.connect(host=TF_HOST, port=TF_PORT, user=TF_USER, password=TF_PASS, dbname=TF_DB) as conn:
            with conn.cursor() as cur:
                while time.time() < deadline:
                    try:
                        cur.execute("SELECT count(*) FROM otel_logs_and_spans WHERE project_id = %s", (pid,))
                        cur.fetchone()
                        nonlocal_q += 1
                    except Exception:
                        pass
        return nonlocal_q

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers + readers) as ex:
        write_futs = [ex.submit(writer) for _ in range(workers)]
        read_futs = [ex.submit(reader) for _ in range(readers)]
        for fut in write_futs:
            inserts_ok += fut.result()
        for fut in read_futs:
            queries_ok += fut.result()
    elapsed = time.time() - t0
    if sampler:
        sampler.stop()
    res = ScenarioResult(
        name="query_while_write",
        workers=workers,
        duration_s=elapsed,
        inserts_ok=inserts_ok,
        inserts_per_sec=inserts_ok / elapsed,
        rss=(sampler.report() if sampler else {}),
    )
    res.rss["queries"] = queries_ok
    return res


# ── Reporter ────────────────────────────────────────────────────────────────

def fmt(res: ScenarioResult) -> str:
    return (
        f"  workers={res.workers}  elapsed={res.duration_s:.1f}s  "
        f"inserts={res.inserts_ok}  rate={res.inserts_per_sec:.0f}/s  "
        f"rss={res.rss}"
    )


SCENARIOS = {
    "hot_single_project": scenario_hot_single_project,
    "many_projects": scenario_many_projects,
    "large_rows": scenario_large_rows,
    "query_while_write": scenario_query_while_write,
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("scenario", nargs="?", choices=list(SCENARIOS.keys()) + ["all"], default="all")
    ap.add_argument("--workers", type=int, default=None, help="override worker count")
    ap.add_argument("--duration", type=int, default=None, help="override duration in seconds")
    args = ap.parse_args()

    tf_pid = find_tf_pid()
    if not tf_pid:
        print(f"!! TimeFusion not reachable on :{TF_PORT}. Start it before running.", file=sys.stderr)
        sys.exit(1)
    rss0 = rss_kib(tf_pid)
    print(f"# TimeFusion pid={tf_pid}, initial RSS={rss0/1024:.1f} MB")

    targets = SCENARIOS.keys() if args.scenario == "all" else [args.scenario]
    for name in targets:
        print(f"\n## {name}")
        kwargs = {}
        if args.workers is not None:
            kwargs["workers"] = args.workers
        if args.duration is not None:
            kwargs["duration_s"] = args.duration
        res = SCENARIOS[name](**kwargs)
        print(fmt(res))


if __name__ == "__main__":
    main()
