#!/usr/bin/env python3
"""Variant column read/write benchmark.

Why. OpenTelemetry payloads carry the bulk of real traffic in semi-structured
fields (`attributes`, `resource`, `events`, `links`). TimeFusion stores
those as the new Arrow Variant type, plumbed through delta-rs main. We need
empirical numbers on whether Variant is winning vs the obvious Utf8+JSON
fallback, across the shapes real workloads produce.

What this harness does. For each shape — small flat, large flat, deep
nested, mixed array of objects — write N rows into `variant_bench`. The
same JSON object lands in two columns: `payload` (Variant) and
`payload_json` (Utf8). Then run four query patterns against each column:

  1. variant_get on a hot field
  2. variant_to_json for wire output
  3. WHERE filter on a Variant field (e.g. status_code = 500)
  4. Range aggregate (GROUP BY minute, count WHERE field = X)

For Utf8 baseline, every query first wraps the column in
`json_to_variant(payload_json)` so we measure parse-on-read vs Variant's
parse-on-write trade-off honestly.

Output. Prints a comparison table (write throughput, p50/p99 read latency
per shape and query class, ratio Utf8/Variant). CSV row per measurement
goes to /tmp/variant_bench.csv for later plotting.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import random
import statistics
import time
import uuid
from typing import Callable

import psycopg

HOST = os.getenv("TF_HOST", "127.0.0.1")
PORT = int(os.getenv("TF_PORT", "12345"))
USER = os.getenv("TF_USER", "postgres")
PWD = os.getenv("TF_PASS", "postgres")
DB = os.getenv("TF_DB", "postgres")

PROJECT_ID = str(uuid.uuid4())
COLS = ["timestamp", "id", "project_id", "shape", "payload", "payload_json", "date"]


# ── Shape generators ────────────────────────────────────────────────────────
HTTP_METHODS = ["GET", "POST", "PUT", "DELETE", "PATCH"]
STATUS_CODES = ["200", "201", "204", "400", "404", "500", "502"]

def shape_small(rnd: random.Random) -> dict:
    return {
        "http": {
            "method": rnd.choice(HTTP_METHODS),
            "status_code": rnd.choice(STATUS_CODES),
            "host": "api.example.com",
        },
        "user_id": str(uuid.uuid4()),
        "request_id": str(uuid.uuid4()),
        "duration_ms": rnd.randint(1, 5000),
        "bytes_in": rnd.randint(100, 10000),
        "bytes_out": rnd.randint(100, 50000),
        "client_ip": f"10.{rnd.randint(0,255)}.{rnd.randint(0,255)}.{rnd.randint(0,255)}",
    }


def shape_large(rnd: random.Random) -> dict:
    base = shape_small(rnd)
    # Add ~90 extra keys to push to ~5 kB
    for i in range(90):
        base[f"tag_{i}"] = f"value_{rnd.randint(0,1000)}_{uuid.uuid4().hex[:8]}"
    return base


def shape_nested(rnd: random.Random) -> dict:
    return {
        "request": {
            "http": {
                "method": rnd.choice(HTTP_METHODS),
                "status_code": rnd.choice(STATUS_CODES),
                "headers": {
                    "user_agent": "Mozilla/5.0",
                    "x_forwarded": {
                        "for": f"10.{rnd.randint(0,255)}.0.1",
                        "proto": "https",
                        "host": {
                            "name": "api.example.com",
                            "port": 443,
                        },
                    },
                },
            },
        },
        "trace": {
            "id": uuid.uuid4().hex,
            "span": {"id": uuid.uuid4().hex[:16], "parent_id": uuid.uuid4().hex[:16]},
        },
    }


def shape_array(rnd: random.Random) -> dict:
    n_events = rnd.randint(5, 20)
    return {
        "service": "checkout",
        "events": [
            {
                "name": f"event_{i}",
                "ts": int(time.time() * 1000) + i,
                "attrs": {
                    "status_code": rnd.choice(STATUS_CODES),
                    "method": rnd.choice(HTTP_METHODS),
                    "bytes": rnd.randint(1, 10000),
                },
            }
            for i in range(n_events)
        ],
        "links": [{"trace_id": uuid.uuid4().hex} for _ in range(rnd.randint(1, 5))],
    }


SHAPES = {
    "small":  shape_small,
    "large":  shape_large,
    "nested": shape_nested,
    "array":  shape_array,
}


def jsonpath_for(shape: str) -> str:
    """RFC 9535 JSONPath that picks one hot field per shape, used both for
    `jsonb_path_exists` filters and (eventually) value extraction."""
    return {
        "small":  "$.http.method",
        "large":  "$.http.method",
        "nested": "$.request.http.headers.x_forwarded.host.name",
        "array":  "$.events[0].attrs.method",
    }[shape]


# ── Bench plumbing ───────────────────────────────────────────────────────────
def connect():
    return psycopg.connect(host=HOST, port=PORT, user=USER, password=PWD, dbname=DB, autocommit=True)


def insert_batch(conn, shape: str, n: int, rnd: random.Random, mode: str) -> float:
    """Insert n rows of `shape` into one of (variant, utf8, both). Returns
    elapsed seconds. Modes:
      - variant: only `payload` is set; `payload_json` is NULL.
      - utf8: only `payload_json` is set; `payload` is NULL.
      - both:    both columns set to the same JSON (used by read-phase
        bench rows so each query has data in either column).
    """
    ts = dt.datetime(2026, 5, 17, tzinfo=dt.timezone.utc)
    date = ts.date()
    rows = []
    for _ in range(n):
        payload = SHAPES[shape](rnd)
        j = json.dumps(payload, separators=(",", ":"))
        if mode == "variant":
            rows.append((ts, str(uuid.uuid4()), PROJECT_ID, shape, j, None, date))
        elif mode == "utf8":
            rows.append((ts, str(uuid.uuid4()), PROJECT_ID, shape, None, j, date))
        else:
            rows.append((ts, str(uuid.uuid4()), PROJECT_ID, shape, j, j, date))
    flat: list = [v for r in rows for v in r]
    row_ph = "(" + ", ".join(["%s"] * len(COLS)) + ")"
    sql = f"INSERT INTO variant_bench ({', '.join(COLS)}) VALUES " + ", ".join([row_ph] * n)
    t0 = time.time()
    with conn.cursor() as cur:
        cur.execute(sql, flat)
    return time.time() - t0


def time_query(conn, sql: str) -> tuple[float, int]:
    """Run `sql`, return (elapsed_s, rowcount)."""
    t0 = time.time()
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    return time.time() - t0, len(rows)


def percentiles(xs: list[float]) -> tuple[float, float]:
    if not xs:
        return 0.0, 0.0
    xs = sorted(xs)
    return xs[len(xs) // 2], xs[int(0.99 * (len(xs) - 1))]


# ── Read-path queries ────────────────────────────────────────────────────────
def _col(mode: str) -> str:
    return "payload" if mode == "variant" else "payload_json"


def q_raw(shape: str, mode: str) -> str:
    """Read the entire payload column. For Variant this triggers
    VariantToJsonExec at the scan boundary (Variant binary → JSON text);
    for Utf8 it's a direct passthrough. Latency difference here isolates
    decode cost from JSON parse cost."""
    return (f"SELECT {_col(mode)} FROM variant_bench "
            f"WHERE project_id = '{PROJECT_ID}' AND shape = '{shape}' LIMIT 200")


def q_path_exists(shape: str, mode: str) -> str:
    """Project a JSONPath presence check across all matching rows. Hits
    `jsonb_path_exists` against the column, which handles both Variant
    and Utf8 transparently (`evaluate_jsonpath_on_variant` vs
    `evaluate_jsonpath_on_json_string`)."""
    return (f"SELECT jsonb_path_exists({_col(mode)}, '{jsonpath_for(shape)}') "
            f"FROM variant_bench "
            f"WHERE project_id = '{PROJECT_ID}' AND shape = '{shape}' LIMIT 200")


def q_filter(shape: str, mode: str) -> str:
    """Count rows where the hot JSON field exists. Push-downable to a
    full-scan with row-level filter."""
    return (f"SELECT count(*) FROM variant_bench "
            f"WHERE project_id = '{PROJECT_ID}' AND shape = '{shape}' "
            f"AND jsonb_path_exists({_col(mode)}, '{jsonpath_for(shape)}')")


def q_agg(shape: str, mode: str) -> str:
    """Realistic dashboard query: per-minute count of rows whose payload
    contains the hot path. Tests jsonb_path_exists inside an aggregate +
    GROUP BY."""
    return (f"SELECT date_trunc('minute', timestamp), count(*) "
            f"FROM variant_bench WHERE project_id = '{PROJECT_ID}' AND shape = '{shape}' "
            f"AND jsonb_path_exists({_col(mode)}, '{jsonpath_for(shape)}') "
            f"GROUP BY 1 ORDER BY 1")


READ_QUERIES: dict[str, Callable[[str, str], str]] = {
    "raw":         q_raw,
    "path_exists": q_path_exists,
    "filter":      q_filter,
    "agg":         q_agg,
}


# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows-per-shape", type=int, default=10_000)
    ap.add_argument("--batch", type=int, default=200, help="rows per multi-row INSERT")
    ap.add_argument("--query-iters", type=int, default=20, help="repetitions per query for p50/p99")
    ap.add_argument("--csv", default="/tmp/variant_bench.csv")
    ap.add_argument("--shapes", nargs="+", default=list(SHAPES.keys()))
    args = ap.parse_args()

    rnd = random.Random(42)
    results: list[dict] = []

    with connect() as conn:
        # Force the table to exist by writing one row of each shape (the
        # otel_logs_and_spans rewriter logic handles Utf8 → Variant on insert).
        for shape in args.shapes:
            insert_batch(conn, shape, 1, rnd, "both")

        # ── Write phase ──────────────────────────────────────────────────
        # Per shape, write rows_per_shape into the variant column AND
        # rows_per_shape into the utf8 column separately so per-mode
        # throughput is independent. Also write a small "both" batch used
        # by the read phase so neither column is empty.
        print("== WRITE (rows/s; ratio = utf8/variant) ==")
        print(f"  {'shape':7s}  {'variant rate':>14s}  {'utf8 rate':>14s}  {'ratio':>6s}")
        for shape in args.shapes:
            batches = max(1, args.rows_per_shape // args.batch)
            mode_rates: dict[str, float] = {}
            for mode in ("variant", "utf8"):
                t0 = time.time()
                for _ in range(batches):
                    insert_batch(conn, shape, args.batch, rnd, mode)
                elapsed = time.time() - t0
                rows = batches * args.batch
                rate = rows / elapsed if elapsed > 0 else 0.0
                mode_rates[mode] = rate
                results.append({
                    "phase": "write", "shape": shape, "mode": mode,
                    "rows": rows, "elapsed_s": round(elapsed, 3), "rate": round(rate, 1),
                    "p50_ms": "", "p99_ms": "", "rowcount": "",
                })
            ratio = mode_rates["utf8"] / mode_rates["variant"] if mode_rates["variant"] else 0
            print(f"  {shape:7s}  {mode_rates['variant']:>11,.0f}  {mode_rates['utf8']:>11,.0f}  {ratio:>5.2f}x")

        # ── Read phase ───────────────────────────────────────────────────
        print("\n== READ (p50 / p99 in ms, ratio = utf8/variant; <1 = variant faster) ==")
        header = f"  {'shape':7s}  {'query':10s}  {'variant p50':>11s}  {'variant p99':>11s}  {'utf8 p50':>10s}  {'utf8 p99':>10s}  {'ratio_p50':>9s}  {'ratio_p99':>9s}"
        print(header)
        for shape in args.shapes:
            for qname, qbuilder in READ_QUERIES.items():
                latencies: dict[str, list[float]] = {"variant": [], "utf8": []}
                rowcount: dict[str, int] = {}
                for mode in ("variant", "utf8"):
                    sql = qbuilder(shape, mode)
                    # Warmup once (drops cold-cache effects)
                    try:
                        _, rc = time_query(conn, sql)
                        rowcount[mode] = rc
                    except Exception as e:
                        print(f"    {shape} {qname} {mode} ERR: {type(e).__name__}: {e}")
                        latencies[mode].append(float("inf"))
                        continue
                    for _ in range(args.query_iters):
                        try:
                            t, _ = time_query(conn, sql)
                            latencies[mode].append(t)
                        except Exception as e:
                            latencies[mode].append(float("inf"))
                v_p50, v_p99 = percentiles(latencies["variant"])
                u_p50, u_p99 = percentiles(latencies["utf8"])
                ratio_p50 = (u_p50 / v_p50) if v_p50 else 0
                ratio_p99 = (u_p99 / v_p99) if v_p99 else 0
                print(f"  {shape:7s}  {qname:10s}  {v_p50*1000:>10.2f}ms  {v_p99*1000:>10.2f}ms  {u_p50*1000:>9.2f}ms  {u_p99*1000:>9.2f}ms  {ratio_p50:>8.2f}x  {ratio_p99:>8.2f}x")
                for mode, lats in latencies.items():
                    p50, p99 = percentiles(lats)
                    results.append({
                        "phase": "read", "shape": shape, "mode": mode, "query": qname,
                        "p50_ms": round(p50 * 1000, 3), "p99_ms": round(p99 * 1000, 3),
                        "rowcount": rowcount.get(mode, ""),
                        "rows": "", "elapsed_s": "", "rate": "",
                    })

    # ── CSV dump ─────────────────────────────────────────────────────────
    if results:
        keys = ["phase", "shape", "mode", "query", "rows", "elapsed_s", "rate",
                "p50_ms", "p99_ms", "rowcount"]
        with open(args.csv, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=keys)
            w.writeheader()
            for r in results:
                w.writerow({k: r.get(k, "") for k in keys})
        print(f"\nCSV: {args.csv}")


if __name__ == "__main__":
    main()
