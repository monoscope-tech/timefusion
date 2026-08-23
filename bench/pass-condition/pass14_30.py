#!/usr/bin/env python3
"""THE pass condition: zero fail cells at 14d and 30d for all six projects.

This measures COMPLETION, not routing — a routing miss still answers from the raw
path, so `rollup_min_contiguous_days` does NOT have to be 30 for this to be valid.
That distinction is why this can run while prod is restarting; only the routing
attribution needs a quiet process.

Serial, one connection, newest-first per project so a project that is going to
fail does so on its cheapest cell. Appends after every cell — a killed run keeps
what it measured. Re-running skips cells already in the CSV.
"""
import csv, pathlib, sys, time
sys.path.insert(0, str(pathlib.Path(__file__).parent))
from probe import connect, healthy, run, shapes, T, WHERE

D = pathlib.Path(__file__).parent
OUT = D / "pass14_30.csv"

PROJECTS = {
    "p1": "87576849-4941-49d3-a15d-680fef88a1a8",
    "p2": "edb04135-1ee1-435e-8b01-2f969eb01c2b",
    "p3": "00000000-0000-0000-0000-000000000000",
    "p4": "dcad860a-9a98-4c9e-9e69-20d52dcf90e2",
    "p5": "be87ebc1-08b9-4293-a390-283460fa6202",
    "p6": "28f62f01-46a1-400e-8195-da7bc3505b5b",
}
# 14d first: if a project fails at 14d it will fail at 30d, and the cheap cell
# tells us that for a third of the wall clock.
WINDOWS = [("14d", 14, "6 hours"), ("30d", 30, "6 hours")]


def done():
    if not OUT.exists():
        return set()
    with OUT.open() as f:
        return {(r["project"], r["window"], r["shape"]) for r in csv.DictReader(f)}


def main():
    already = done()
    new = not OUT.exists()
    out = OUT.open("a", newline="")
    w = csv.writer(out)
    if new:
        w.writerow(["project", "window", "shape", "rep1_ms", "rep2_ms", "rows", "status"])
        out.flush()

    conn = connect()
    for name, pid in PROJECTS.items():
        for label, days, bucket in WINDOWS:
            for shape, sql in shapes(bucket).items():
                if (name, label, shape) in already:
                    continue
                params = {"pid": pid, "lo": f"now() - interval '{days} days'", "hi": "now()"}
                # probe.py's WHERE uses bound params for lo/hi; inline them instead so
                # the interval arithmetic happens server-side like monoscope's does.
                q = sql.replace("%(pid)s", f"'{pid}'").replace("%(lo)s", f"now() - interval '{days} days'").replace("%(hi)s", "now()")
                reps, rows, status = [], 0, "ok"
                for rep in range(2):
                    if not healthy(conn):
                        conn = connect()
                    try:
                        ms, rows = run(conn, q, None)
                        reps.append(round(ms))
                    except Exception as e:
                        msg = str(e).splitlines()[0]
                        status = "fail" if "timeout" in msg.lower() else "err"
                        reps.append(msg[:60])
                        if not healthy(conn):
                            conn = connect()
                        break
                    # A cell over 15s does not get a second rep — the first already
                    # answered the only question this page asks.
                    if ms > 15_000:
                        break
                w.writerow([name, label, shape, reps[0], reps[1] if len(reps) > 1 else "", rows, status])
                out.flush()
                print(f"{name:3} {label:4} {shape:18} {reps} rows={rows} {status}", flush=True)
                if status == "fail":
                    # Skip the wider window's remaining shapes for this project only
                    # when the NARROWER one already failed — monotone by construction.
                    pass
    out.close()
    print("PASS_SWEEP_DONE", flush=True)


if __name__ == "__main__":
    main()
