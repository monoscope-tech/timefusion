#!/usr/bin/env python3
"""Latest-window acceptance bench: monoscope's Overview SQL shapes, run one at a time against prod.

    bench/latest_bench.py                      # 15m 1h 6h 24h x shipbubble demo whale, 3 rounds
    bench/latest_bench.py --windows 1h --projects shipbubble --rounds 5
    bench/latest_bench.py --only events_by_service,traffic

Sequential on purpose: concurrent raw load has OOMed prod (memory tf_my_synthetic_load_oomed_prod).
Bins follow monoscope's `calculateAutoBinWidth` (range / 150 bars or / 300 line points, nearest ladder rung).
Prints p50/p95 per window and query, and the server counters that moved over the run.
"""
import argparse, json, re, statistics, time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg

DSN = re.search(r'^TIMEFUSION_PG_URL=(.*)$', (Path(__file__).resolve().parents[2] / "monoscope/.env").read_text(), re.M)[1].strip().strip('"')
PROJECTS = {"shipbubble": "28f62f01-46a1-400e-8195-da7bc3505b5b", "demo": "00000000-0000-0000-0000-000000000000",
            "whale": "87576849-4941-49d3-a15d-680fef88a1a8"}
WINDOWS = {"15m": 900, "1h": 3600, "6h": 6 * 3600, "24h": 86400}
LADDER = [(1, "1 second"), (2, "2 seconds"), (5, "5 seconds"), (10, "10 seconds"), (15, "15 seconds"), (30, "30 seconds"),
          (60, "1 minute"), (120, "2 minutes"), (300, "5 minutes"), (600, "10 minutes"), (900, "15 minutes"), (1800, "30 minutes"),
          (3600, "1 hour"), (7200, "2 hours"), (10800, "3 hours"), (21600, "6 hours"), (43200, "12 hours"), (86400, "1 day")]
COUNTERS = ("scan.heavy_query_admitted", "scan.heavy_query_queued", "scan.heavy_query_queue_timeout",
            "maintenance.rollup_hits_full_total", "maintenance.rollup_hits_hybrid_total", "maintenance.rollup_misses_total")


def bin_auto(secs: int, points: int) -> str:
    ideal = max(1, secs / points)
    return min(LADDER, key=lambda r: max(r[0] / ideal, ideal / r[0]))[1]


HTTP = "(kind = 'server' or name = 'apitoolkit-http-span' or name = 'monoscope.http')"
B, T = "timestamp between '{s}' and '{e}'", "(timestamp >= '{s}' and timestamp <= '{e}')"
FROM = "from otel_logs_and_spans where project_id='{p}'"
P95 = "coalesce(approx_percentile(0.95, percentile_agg(cast(duration as double precision))), 0)::float"
ts = lambda b: f"extract(epoch from time_bucket('{b}', timestamp))::integer"  # noqa: E731
grp = lambda b, extra="": f"group by time_bucket('{b}', timestamp){extra} order by time_bucket('{b}', timestamp) desc"  # noqa: E731


def queries(bar: str, line: str) -> dict[str, str]:
    svc, status = "coalesce(resource___service___name::text, 'null')", "coalesce(cast(attributes___http___response___status_code as text), 'unknown')"
    by_svc = lambda where: f"select {ts(bar)}, {svc}, count(*)::float as count_ {FROM} and {B} and (({where})) {grp(bar, ', ' + svc)} limit 10000"  # noqa: E731
    return {
        "var_service": f"select distinct resource___service___name {FROM} and resource___service___name is not null and {T} limit 100",
        "top_resources": f"select name {FROM} and name is not null and kind = 'server' and {T} group by name order by count(*) desc limit 20",
        "traffic": f"select {ts(bar)}, 'value', count(*)::float as count_ {FROM} and {B} and (({HTTP})) {grp(bar)}",
        "p95_latency": f"select {ts(bar)}, 'value', (coalesce(({P95} / nullif(1000000, 0)), 0))::float {FROM} and {B} and (({HTTP} and duration is not null)) {grp(bar)}",
        "error_rate": f"select {ts(bar)}, 'value', round((coalesce(((count(*) filter (where status_code = 'ERROR' or coalesce(attributes___http___response___status_code, 0) >= 500)::float * 100.0) / nullif(count(*)::float, 0)), 0))::numeric, 2)::float {FROM} and {B} and (({HTTP})) {grp(bar)}",
        "apdex": f"select round((sum(case when duration <= 500000000 then 1.0 when duration <= 2000000000 then 0.5 else 0 end)) / greatest(1, count(*))::numeric, 2)::float {FROM} and {HTTP} and duration is not null and {T}",
        "total_requests": f"select count(*)::float {FROM} and {B} and (({HTTP}))",
        "events_by_service": by_svc("resource___service___name is not null"),
        "errors_by_service": by_svc("status_code = 'ERROR' and resource___service___name is not null"),
        "http_by_service": by_svc("resource___service___name is not null and kind = 'server'"),
        "http_by_status": f"select {ts(bar)}, {status}, count(*)::float as count_ {FROM} and {B} and (({HTTP} and attributes___http___response___status_code is not null)) {grp(bar, ', ' + status)} limit 10000",
        "latency_percentiles": f"select {ts(line)}, " + ", ".join(f"coalesce(approx_percentile({q}, percentile_agg(cast(duration as double precision))), 0)::float" for q in (0.5, 0.9, 0.99))
        + f" {FROM} and {B} and (({HTTP} and duration is not null)) {grp(line)}",
    }


def counters(c) -> dict[str, float]:
    rows = c.execute("select component || '.' || key, value from timefusion_stats").fetchall()
    return {k: float(v) for k, v in rows if k in COUNTERS and re.fullmatch(r"-?[\d.]+", v)}


def pct(xs: list[float], q: float) -> float:
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(q * len(xs)))]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--windows", default=",".join(WINDOWS))
    ap.add_argument("--projects", default=",".join(PROJECTS))
    ap.add_argument("--only", default="")
    ap.add_argument("--rounds", type=int, default=3)
    ap.add_argument("--jsonl", default="")
    a = ap.parse_args()
    out = open(a.jsonl, "a") if a.jsonl else None
    samples: dict[tuple[str, str], list[float]] = {}
    with psycopg.connect(DSN, autocommit=True, prepare_threshold=None) as c:
        before, t0 = counters(c), time.monotonic()
        for r in range(a.rounds):
            # Alternate order each round so neither end of the list always runs warm.
            ws = a.windows.split(",")[:: 1 if r % 2 == 0 else -1]
            for w in ws:
                end = datetime.now(timezone.utc).replace(microsecond=0)
                s, e = (end - timedelta(seconds=WINDOWS[w])).isoformat(), end.isoformat()
                qs = queries(bin_auto(WINDOWS[w], 150), bin_auto(WINDOWS[w], 300))
                for pn in a.projects.split(","):
                    for qn, sql in qs.items():
                        if a.only and qn not in a.only.split(","):
                            continue
                        t = time.monotonic()
                        err = None
                        try:
                            c.execute(sql.format(p=PROJECTS[pn], s=s, e=e)).fetchall()
                        except Exception as ex:  # noqa: BLE001
                            err = str(ex).splitlines()[0][:160]
                        ms = (time.monotonic() - t) * 1000
                        samples.setdefault((w, qn), []).append(ms)
                        if out:
                            out.write(json.dumps(dict(round=r, window=w, project=pn, query=qn, ms=round(ms), error=err, at=e)) + "\n")
                        if err or ms > 5000:
                            print(f"  slow/err r{r} {w:4} {pn:10} {qn:20} {ms:8.0f}ms {err or ''}", flush=True)
        after, secs = counters(c), time.monotonic() - t0
    print(f"\n{'window':6} {'query':20} {'n':>3} {'p50':>8} {'p95':>8} {'max':>8}")
    for w in a.windows.split(","):
        allw = [x for (ww, _), xs in samples.items() if ww == w for x in xs]
        for (ww, qn), xs in samples.items():
            if ww == w:
                print(f"{w:6} {qn:20} {len(xs):3} {pct(xs, .5):8.0f} {pct(xs, .95):8.0f} {max(xs):8.0f}")
        if allw:
            print(f"{w:6} {'ALL':20} {len(allw):3} {pct(allw, .5):8.0f} {pct(allw, .95):8.0f} {max(allw):8.0f}\n")
    print(f"counter deltas over {secs:.0f}s (includes other traffic):")
    for k in COUNTERS:
        if k in before and k in after:
            print(f"  {k:45} +{after[k] - before[k]:,.0f}")


if __name__ == "__main__":
    main()
