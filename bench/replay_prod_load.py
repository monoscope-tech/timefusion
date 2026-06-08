#!/usr/bin/env python3
"""
Pull a realistic slice of prod otel_logs_and_spans, replay it into a local
TimeFusion, and validate against the success criteria from
docs/membuffer_flush_fix_plan.md.

Three subcommands:

  download  pull rows from prod -> bench/data/sample.jsonl.gz
  replay    read sample -> INSERT into local TF (PGWIRE_PORT, default 12345)
  validate  run the four canonical log-explorer queries + check timefusion_stats
            against the plan's target table

Typical loop:

  python3 bench/replay_prod_load.py download \\
      --project 87576849-4941-49d3-a15d-680fef88a1a8 --hours 2
  # in another shell: cargo run --release
  python3 bench/replay_prod_load.py replay   --rate 5000
  python3 bench/replay_prod_load.py validate
  # change code, restart TF, re-run replay+validate
"""
from __future__ import annotations
import argparse, gzip, json, os, re, sys, time, uuid
from contextlib import contextmanager
from datetime import datetime, date, timezone, timedelta
from pathlib import Path
from typing import Any, Iterable

import psycopg

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"
DUMP = DATA / "sample.jsonl.gz"

def prod_url() -> str:
    """Read TF_PROD_URL on demand. Lazy so importing this module doesn't
    exit when the env var is unset — tooling that only invokes the
    `validate` subcommand doesn't need a prod URL at all."""
    url = os.environ.get("TF_PROD_URL")
    if not url:
        sys.exit("TF_PROD_URL is required — set it to your TimeFusion connection string")
    return url
LOCAL_HOST = os.environ.get("PGWIRE_HOST", "127.0.0.1")
LOCAL_PORT = int(os.environ.get("PGWIRE_PORT", "12345"))
LOCAL_URL = f"host={LOCAL_HOST} port={LOCAL_PORT} user=postgres password=postgres dbname=postgres"

# Same 88-column list monoscope writes (see bench/monoscope_e2e.py).
# Kept identical here so replay exercises the exact INSERT path that produced
# the fragmented MemBuffer in prod.
COLUMNS = [
    "timestamp","observed_timestamp","id","parent_id","hashes","name","kind",
    "status_code","status_message","level","severity",
    "severity___severity_text","severity___severity_number","body","duration",
    "start_time","end_time","context","context___trace_id","context___span_id",
    "context___trace_state","context___trace_flags","context___is_remote",
    "events","links","attributes","attributes___client___address",
    "attributes___client___port","attributes___server___address",
    "attributes___server___port","attributes___network___local__address",
    "attributes___network___local__port","attributes___network___peer___address",
    "attributes___network___peer__port","attributes___network___protocol___name",
    "attributes___network___protocol___version","attributes___network___transport",
    "attributes___network___type","attributes___code___number",
    "attributes___code___file___path","attributes___code___function___name",
    "attributes___code___line___number","attributes___code___stacktrace",
    "attributes___log__record___original","attributes___log__record___uid",
    "attributes___error___type","attributes___exception___type",
    "attributes___exception___message","attributes___exception___stacktrace",
    "attributes___url___fragment","attributes___url___full","attributes___url___path",
    "attributes___url___query","attributes___url___scheme",
    "attributes___user_agent___original","attributes___http___request___method",
    "attributes___http___request___method_original",
    "attributes___http___response___status_code",
    "attributes___http___request___resend_count",
    "attributes___http___request___body___size","attributes___session___id",
    "attributes___session___previous___id","attributes___db___system___name",
    "attributes___db___collection___name","attributes___db___namespace",
    "attributes___db___operation___name","attributes___db___response___status_code",
    "attributes___db___operation___batch___size","attributes___db___query___summary",
    "attributes___db___query___text","attributes___user___id","attributes___user___email",
    "attributes___user___full_name","attributes___user___name","attributes___user___hash",
    "resource","resource___service___name","resource___service___version",
    "resource___service___instance___id","resource___service___namespace",
    "resource___telemetry___sdk___language","resource___telemetry___sdk___name",
    "resource___telemetry___sdk___version","resource___user_agent___original",
    "project_id","summary","date","message_size_bytes",
]
assert len(COLUMNS) == 88, len(COLUMNS)

# Columns that come back as JSON / list / Variant strings on the wire — psycopg
# round-trips these as Python values, so we re-serialize for the JSONL dump.
JSON_COLS = {"severity", "body", "context", "attributes", "resource", "errors", "events", "links"}
LIST_COLS = {"hashes", "summary"}


# ───────────────────────── download ─────────────────────────

def download(args):
    DATA.mkdir(parents=True, exist_ok=True)
    # TF pgwire doesn't support DECLARE CURSOR, so we paginate by (timestamp, id)
    # to avoid loading all rows into memory at once. Hours/limit are ints — safe
    # to inline (TF rejects parameterised INTERVAL anyway).
    cols_sql = ", ".join(COLUMNS)
    # Guard against SQL injection — UUID/hex-only chars.
    if not re.fullmatch(r"[0-9a-fA-F-]{1,64}", args.project):
        sys.exit(f"refusing unsafe --project value: {args.project!r}")
    where_base = f"project_id = '{args.project}'"
    if args.hours:
        where_base += f" AND timestamp >= now() - INTERVAL '{int(args.hours)} hours'"

    print(f"[download] {prod_url().split('@')[-1]}  project={args.project}  hours={args.hours}  limit={args.limit}")
    t0 = time.time()
    n = 0
    page = min(args.limit, 25_000)
    cursor_ts: str | None = None
    cursor_id: str | None = None

    with psycopg.connect(prod_url()) as c, c.cursor() as cur, gzip.open(DUMP, "wt") as out:
        while n < args.limit:
            where = where_base
            if cursor_ts is not None:
                # (timestamp, id) keyset — strictly greater than last row
                where += (f" AND (timestamp > TIMESTAMP '{cursor_ts}' OR "
                          f"(timestamp = TIMESTAMP '{cursor_ts}' AND id > '{cursor_id}'))")
            sql = (f"SELECT {cols_sql} FROM otel_logs_and_spans WHERE {where} "
                   f"ORDER BY timestamp, id LIMIT {min(page, args.limit - n)}")
            cur.execute(sql)
            rows = cur.fetchall()
            if not rows: break
            for r in rows:
                row = {col: _to_jsonable(col, v) for col, v in zip(COLUMNS, r)}
                out.write(json.dumps(row, separators=(",", ":")) + "\n")
            n += len(rows)
            last = dict(zip(COLUMNS, rows[-1]))
            cursor_ts = last["timestamp"].isoformat(sep=" ") if isinstance(last["timestamp"], datetime) else str(last["timestamp"])
            cursor_id = last["id"]
            print(f"  ... {n} rows in {time.time()-t0:.1f}s")
            if len(rows) < page: break
    print(f"[download] wrote {n} rows -> {DUMP} ({DUMP.stat().st_size/1e6:.1f} MB) in {time.time()-t0:.1f}s")


def _to_jsonable(col: str, v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, (bytes, bytearray, memoryview)):
        return bytes(v).hex()
    if col in JSON_COLS and not isinstance(v, str):
        return json.dumps(v, default=str)
    if col in LIST_COLS and v is not None and not isinstance(v, list):
        # Defensive: TF sometimes returns lists as JSON-encoded text.
        try: return json.loads(v)
        except Exception: return [v]
    return v


# ───────────────────────── replay ─────────────────────────

def replay(args):
    if not DUMP.exists():
        sys.exit(f"no dump at {DUMP} — run `download` first")

    # Optionally shift timestamps so the LATEST row lands at "now" — otherwise
    # prod timestamps may be in the past (1h queries miss) or, if we anchored
    # the oldest row to now, in the future (queries with <= now() miss).
    shift = None
    if args.shift_to_now:
        # Two-pass: read once to find max timestamp, set shift so max -> now.
        max_ts: datetime | None = None
        with gzip.open(DUMP, "rt") as f:
            for line in f:
                ts = datetime.fromisoformat(json.loads(line)["timestamp"])
                if max_ts is None or ts > max_ts: max_ts = ts
        assert max_ts is not None
        shift = datetime.now(timezone.utc) - max_ts
        print(f"[replay] shifting timestamps by {shift} so latest row lands at now (was {max_ts.isoformat()})")

    print(f"[replay] target {LOCAL_HOST}:{LOCAL_PORT}  batch={args.batch}  rate={args.rate}/s")
    placeholders = "(" + ", ".join(["%s"] * len(COLUMNS)) + ")"

    t0 = time.time()
    inserted = 0; errors = 0
    next_send = t0
    per_batch_delay = args.batch / args.rate if args.rate > 0 else 0

    with psycopg.connect(LOCAL_URL, autocommit=True) as conn, conn.cursor() as cur:
        for batch in _batches(_iter_dump(shift), args.batch):
            sql = (f"INSERT INTO otel_logs_and_spans ({', '.join(COLUMNS)}) VALUES "
                   + ", ".join([placeholders] * len(batch)))
            flat: list[Any] = []
            for row in batch:
                for col in COLUMNS:
                    flat.append(_to_pg(col, row.get(col)))
            try:
                cur.execute(sql, flat)
                inserted += len(batch)
            except Exception as e:
                errors += 1
                if errors <= 3:
                    print(f"  insert err: {e}")
            if per_batch_delay:
                next_send += per_batch_delay
                sleep = next_send - time.time()
                if sleep > 0: time.sleep(sleep)
            if inserted % 20_000 == 0:
                rate = inserted / max(time.time() - t0, 1e-3)
                print(f"  ... inserted={inserted} errs={errors} rate={rate:,.0f}/s")

    dur = time.time() - t0
    print(f"[replay] done: inserted={inserted} errs={errors} duration={dur:.1f}s rate={inserted/max(dur,1e-3):,.0f}/s")


def _iter_dump(shift: timedelta | None) -> Iterable[dict]:
    with gzip.open(DUMP, "rt") as f:
        for line in f:
            row = json.loads(line)
            if shift is not None:
                for k in ("timestamp", "observed_timestamp", "start_time", "end_time"):
                    if row.get(k):
                        row[k] = (datetime.fromisoformat(row[k]) + shift).isoformat()
                row["date"] = datetime.fromisoformat(row["timestamp"]).date().isoformat()
            # Force a fresh id so retried replays don't dedupe.
            row["id"] = str(uuid.uuid4())
            yield row


def _batches(it: Iterable[dict], size: int) -> Iterable[list[dict]]:
    batch: list[dict] = []
    for row in it:
        batch.append(row)
        if len(batch) >= size:
            yield batch; batch = []
    if batch: yield batch


def _to_pg(col: str, v: Any) -> Any:
    if v is None: return None
    if col in {"timestamp", "observed_timestamp", "start_time", "end_time"} and isinstance(v, str):
        return datetime.fromisoformat(v)
    if col in JSON_COLS and not isinstance(v, str):
        return json.dumps(v, default=str)
    if col in LIST_COLS and isinstance(v, list):
        # psycopg adapts list[str] -> PG text[] cleanly, but list[dict] errors.
        # Stringify any non-primitive element so the whole list lands as text[].
        return [e if isinstance(e, (str, int, float, bool)) else json.dumps(e, default=str) for e in v]
    return v


# ───────────────────────── validate ─────────────────────────

TARGETS = {
    # name -> (extractor(stats_dict) -> float, predicate, description)
    "rows_per_batch":        (lambda s: s["mem_buffer.total_rows"] / max(s["mem_buffer.total_batches"], 1),
                              lambda v: v > 5000,
                              ">5000 rows/batch (now 65)"),
    "bytes_per_row":         (lambda s: s["mem_buffer.estimated_bytes_approx"] / max(s["mem_buffer.total_rows"], 1),
                              lambda v: v < 2048,
                              "<2 KB/row (now 9 KB)"),
    "oldest_bucket_age_secs":(lambda s: s["mem_buffer.oldest_bucket_age_secs"],
                              lambda v: v < 7200,
                              "<7200s (within 2h retention)"),
    "wal_disk_mb":           (lambda s: s["wal.disk_mb"],
                              lambda v: v < 5000,
                              "<5000 MB"),
}

QUERIES = {
    "list_1h":     ("SELECT id, timestamp FROM otel_logs_and_spans "
                    "WHERE project_id=%(pid)s AND timestamp >= now() - INTERVAL '1 hour' "
                    "ORDER BY timestamp DESC LIMIT 251",
                    0.2),
    "hist_1h":     ("SELECT time_bucket('60 seconds', timestamp) tb, count(*) FROM otel_logs_and_spans "
                    "WHERE project_id=%(pid)s AND timestamp >= now() - INTERVAL '1 hour' "
                    "GROUP BY tb ORDER BY tb DESC LIMIT 500",
                    1.0),
    "list_24h":    ("SELECT id, timestamp FROM otel_logs_and_spans "
                    "WHERE project_id=%(pid)s AND timestamp >= now() - INTERVAL '24 hours' "
                    "ORDER BY timestamp DESC LIMIT 251",
                    5.0),
    "hist_24h":    ("SELECT time_bucket('5 minutes', timestamp) tb, count(*) FROM otel_logs_and_spans "
                    "WHERE project_id=%(pid)s AND timestamp >= now() - INTERVAL '24 hours' "
                    "GROUP BY tb ORDER BY tb DESC LIMIT 500",
                    5.0),
}


def validate(args):
    pid = args.project or _guess_pid()
    with psycopg.connect(LOCAL_URL, autocommit=True) as conn, conn.cursor() as cur:
        stats = _read_stats(cur)
        print(f"\n# timefusion_stats (project filter not applied — these are global)")
        for k in ("mem_buffer.total_rows","mem_buffer.total_batches","mem_buffer.estimated_mb_approx",
                  "mem_buffer.oldest_bucket_age_secs","buffered_layer.pressure_pct","wal.disk_mb"):
            print(f"  {k:40s} {stats.get(k)}")

        print(f"\n# success-criteria checks")
        passed = True
        for name, (extract, ok_pred, desc) in TARGETS.items():
            v = extract(stats)
            ok = ok_pred(v)
            passed &= ok
            print(f"  [{'PASS' if ok else 'FAIL'}] {name:24s} = {v:,.1f}   target: {desc}")

        print(f"\n# query latency (pid={pid})")
        for name, (q, budget) in QUERIES.items():
            t = _time_query(cur, q, {"pid": pid})
            ok = t <= budget
            passed &= ok
            print(f"  [{'PASS' if ok else 'FAIL'}] {name:10s} {t*1000:8.1f} ms   budget: {budget*1000:.0f} ms")

    sys.exit(0 if passed else 1)


def _read_stats(cur) -> dict[str, float]:
    cur.execute("SELECT component, key, value FROM timefusion_stats")
    out: dict[str, float] = {}
    for component, key, value in cur.fetchall():
        try: out[f"{component}.{key}"] = float(value)
        except (TypeError, ValueError): pass
    return out


def _time_query(cur, sql: str, params: dict) -> float:
    t0 = time.time()
    cur.execute(sql, params)
    cur.fetchall()
    return time.time() - t0


def _guess_pid() -> str:
    if not DUMP.exists():
        sys.exit("no project_id given and no dump to read it from")
    with gzip.open(DUMP, "rt") as f:
        return json.loads(f.readline())["project_id"]


# ───────────────────────── main ─────────────────────────

def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    d = sub.add_parser("download", help="pull rows from prod TF")
    d.add_argument("--project", required=True, help="project_id to sample")
    d.add_argument("--hours", type=int, default=2, help="lookback window in hours (default 2)")
    d.add_argument("--limit", type=int, default=500_000, help="max rows to dump (default 500k)")
    d.set_defaults(func=download)

    r = sub.add_parser("replay", help="INSERT dump into local TF")
    r.add_argument("--batch", type=int, default=500, help="rows per INSERT (default 500)")
    r.add_argument("--rate", type=float, default=0, help="cap inserts/sec (default unlimited)")
    r.add_argument("--shift-to-now", action="store_true",
                   help="shift timestamps so first row lands at current time (default off)")
    r.set_defaults(func=replay)

    v = sub.add_parser("validate", help="run diagnostic queries + check targets")
    v.add_argument("--project", help="project_id (defaults to first row of dump)")
    v.set_defaults(func=validate)

    args = ap.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
