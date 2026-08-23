#!/usr/bin/env python3
"""Re-run the monoscope query matrix: 6 projects x 7 windows x real emitted SQL.

Same shapes and method as the 2026-08-22 baseline so the numbers are comparable.
Serial, warm rep quoted, resumable, skip-wider-on-timeout.
"""
import csv, pathlib, sys, time
import psycopg

D = pathlib.Path(__file__).parent
URL = (D / "tfurl").read_text().strip()
OUT = D / "matrix3.csv"
T = "otel_logs_and_spans"
WHERE = "project_id = %(pid)s AND timestamp BETWEEN %(lo)s AND %(hi)s"

PROJECTS = {
    "p1": "87576849-4941-49d3-a15d-680fef88a1a8",
    "p2": "edb04135-1ee1-435e-8b01-2f969eb01c2b",
    "p3": "00000000-0000-0000-0000-000000000000",
    "p4": "dcad860a-9a98-4c9e-9e69-20d52dcf90e2",
    "p5": "be87ebc1-08b9-4293-a390-283460fa6202",
    "p6": "28f62f01-46a1-400e-8195-da7bc3505b5b",
}
WINDOWS = [("1h", "1 hour", "1 minutes"), ("3h", "3 hours", "5 minutes"),
           ("24h", "24 hours", "5 minutes"), ("3d", "3 days", "1 hours"),
           ("7d", "7 days", "1 hours"), ("14d", "14 days", "6 hours"),
           ("30d", "30 days", "6 hours")]


def shapes(bucket):
    B = f"time_bucket('{bucket}', timestamp)"
    return {
        "throughput": f"SELECT extract(epoch from {B})::integer, 'value', count(*)::float FROM {T} WHERE {WHERE} GROUP BY {B} ORDER BY {B} DESC",
        "group_by_service": f"""SELECT extract(epoch from {B})::integer, COALESCE(resource___service___name,'null'), count(*)::float
            FROM {T} WHERE {WHERE} GROUP BY {B}, COALESCE(resource___service___name,'null') ORDER BY {B} DESC LIMIT 10000""",
        "p95_latency": f"""SELECT extract(epoch from {B})::integer, 'value',
                (COALESCE(approx_percentile(0.95, percentile_agg(CAST(duration AS DOUBLE PRECISION))),0)::float/1000000)
            FROM {T} WHERE {WHERE} AND duration IS NOT NULL GROUP BY {B}""",
        "error_rate": f"""SELECT extract(epoch from {B})::integer, 'value',
                ROUND((COUNT(*) FILTER (WHERE status_code='ERROR' OR COALESCE(attributes___http___response___status_code,0)>=500)::float
                  *100.0/count(*)::float)::numeric,2)::float FROM {T} WHERE {WHERE} GROUP BY {B}""",
        "log_list": f"""SELECT id, timestamp, name, kind, status_code, level, duration, context___trace_id,
                context___span_id, resource___service___name, body, attributes___http___request___method, attributes___url___path
            FROM {T} WHERE {WHERE} ORDER BY timestamp DESC LIMIT 251""",
    }


def wide(bucket):
    return {
        "dcount_service": f"SELECT distinct_count(approx_count_distinct(resource___service___name))::float FROM {T} WHERE {WHERE}",
        "facet_service": f"""SELECT (resource___service___name)::text, COUNT(*)::bigint FROM {T} WHERE {WHERE}
            AND resource___service___name IS NOT NULL GROUP BY 1 ORDER BY 2 DESC LIMIT 50""",
    }


def connect(tries=8):
    for i in range(tries):
        try:
            c = psycopg.connect(URL, connect_timeout=30)
            c.autocommit = True
            with c.cursor() as cur:
                cur.execute("SET statement_timeout = '70s'")
            return c
        except Exception as e:
            print(f"connect {i+1}: {e}", file=sys.stderr, flush=True)
            time.sleep(min(60, 5 * 2**i))
    raise SystemExit("no connection")


def run(conn, sql, p):
    t = time.perf_counter()
    with conn.cursor() as cur:
        cur.execute(sql, p)
        rows = cur.fetchall() if cur.description else []
    return (time.perf_counter() - t) * 1000, len(rows)


def healthy(conn):
    try:
        if conn.closed or conn.broken:
            return False
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        return True
    except Exception:
        return False


def main():
    new = not OUT.exists()
    fh = OUT.open("a", newline="")
    w = csv.writer(fh)
    if new:
        w.writerow(["project", "window", "shape", "rep1_ms", "rep2_ms", "rows", "status"])
        fh.flush()
    done, prior_dead = set(), {}
    if not new:
        with OUT.open() as f:
            for r in csv.DictReader(f):
                done.add((r["project"], r["window"], r["shape"]))
                if r["status"].startswith(("FAIL", "skip")):
                    prior_dead.setdefault(r["project"], set()).add(r["shape"])
    conn = connect()
    for pk, pid in PROJECTS.items():
        dead = set(prior_dead.get(pk, ()))
        for win, iv, bucket in WINDOWS:
            allsh = dict(shapes(bucket))
            if win in ("24h", "7d", "30d"):
                allsh.update(wide(bucket))
            for shape, sql in allsh.items():
                if (pk, win, shape) in done:
                    continue
                if shape in dead:
                    w.writerow([pk, win, shape, "", "", "", "skip_implied"]); fh.flush(); continue
                s = sql.replace("%(lo)s", f"now() - interval '{iv}'").replace("%(hi)s", "now()")
                try:
                    r1, rows = run(conn, s, {"pid": pid})
                except Exception as e:
                    m = str(e).strip().splitlines()[0][:90]
                    w.writerow([pk, win, shape, "", "", "", f"FAIL: {m}"]); fh.flush()
                    print(f"{pk} {win} {shape} FAIL {m}", flush=True)
                    if "timeout" in m.lower() or "cancel" in m.lower():
                        dead.add(shape)
                    if not healthy(conn):
                        conn = connect()
                    continue
                r2 = ""
                if r1 <= 15000:
                    try:
                        r2, _ = run(conn, s, {"pid": pid}); r2 = round(r2, 1)
                    except Exception:
                        r2 = ""
                w.writerow([pk, win, shape, round(r1, 1), r2, rows, "ok"]); fh.flush()
                print(f"{pk} {win} {shape} rep1={r1:.0f} rep2={r2} rows={rows}", flush=True)
    fh.close()
    print("SWEEP_DONE", flush=True)


if __name__ == "__main__":
    main()
