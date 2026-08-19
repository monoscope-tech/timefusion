#!/usr/bin/env python3
"""Per-(project, day) maintenance state: compacted, sorted, deduped, rolled up.

Answers "what is actually done and what is not" from the STORAGE LAYER, not from
the process's own counters. That distinction is the whole point of this script.
On 2026-08-19 the counters said `pending_sealed_consolidation = 2,218` and the
class draining at -0.27/min forever; reading object storage said 877 of 1,033
partitions were already compliant and only 108 sealed ones were out of policy.
The queue was 20x inflated with work already done. When a queue and the world
disagree, the world is right.

Four dimensions, each from the cheapest source that is actually authoritative:

  compacted  active Adds in the Delta snapshot, per partition.
             NOT `aws s3 ls` — a raw listing counts tombstones that have not been
             vacuumed and overstates by an order of magnitude (24,637 objects
             against 1,033 real partitions on 2026-08-19).

  sorted     the parquet FOOTER's `sorting_columns`, optionally (--footers).
             NOT the `delta-rs.optimize.sort_by` tag: only the OPTIMIZE path
             writes it, 1,593 of 1,648 adds carry no tags at all, and the flush
             path sorts and stamps a correct footer WITHOUT the tag. Reading the
             tag as the property is what made every partition look permanently
             out of policy. The tag column is still shown, to make that gap
             visible rather than hide it.

  deduped    whether the plan still contains `DedupExec` for that day.
             A certified partition skips it. The scan.dedup_* counters are
             process-global, so concurrent traffic contaminates any attempt to
             attribute them per day; the plan shape does not.

  rollups    the tier tables' own partitions, per date. A derived tier cannot
             advance past its base, so the two are shown side by side.

EXPLAIN only for the dedup probe — it never executes the day query. Running a
wide query to learn a plan property is what times out and risks OOM-ing the
memory-tight prod box.

Usage:
  scripts/maintenance_state.py                      # fleet, per day, last 30d
  scripts/maintenance_state.py --project 28f62f01   # one project, all dimensions
  scripts/maintenance_state.py --project 28f62f01 --footers   # + read footers
  scripts/maintenance_state.py --since 2026-08-01 --source otel_metrics

Env: AWS_* from .env.prod (or exported); TIMEFUSION_PG_URL, or MONOSCOPE_ENV
pointing at monoscope's .env. --no-pg skips everything needing the database.
"""

from __future__ import annotations

import argparse
import collections
import datetime as dt
import os
import re
import subprocess
import sys

GOOD, WARN, BAD, DIM, OFF = "\033[32m", "\033[33m", "\033[31m", "\033[2m", "\033[0m"


def _color(s: str, c: str) -> str:
    return s if not sys.stdout.isatty() else f"{c}{s}{OFF}"


def load_env(path: str) -> None:
    """Source KEY=VALUE lines without overriding anything already exported."""
    try:
        with open(path) as handle:
            for line in handle:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                os.environ.setdefault(key.strip(), value.strip().strip("'\""))
    except OSError:
        pass


def pg_url() -> str | None:
    if os.environ.get("TIMEFUSION_PG_URL"):
        return os.environ["TIMEFUSION_PG_URL"]
    env = os.environ.get("MONOSCOPE_ENV", "../monoscope/.env")
    try:
        with open(env) as handle:
            for line in handle:
                if line.startswith("TIMEFUSION_PG_URL="):
                    return line.split("=", 1)[1].strip()
    except OSError:
        return None
    return None


def psql(url: str, sql: str, timeout: int = 90) -> list[str]:
    """One-column rows, or [] on any failure — this is a reporting tool."""
    try:
        out = subprocess.run(
            ["psql", url, "-t", "-A", "-c", sql],
            capture_output=True,
            text=True,
            timeout=timeout,
            env={**os.environ, "PGMAXPROTOCOLVERSION": "3.0"},
        )
        return [line for line in out.stdout.splitlines() if line.strip()]
    except (subprocess.SubprocessError, OSError):
        return []


def partitions(source: str, since: str) -> dict[tuple[str, str], dict]:
    """Active Adds per (project, date) from the Delta snapshot."""
    from deltalake import DeltaTable

    storage = {
        "AWS_ACCESS_KEY_ID": os.environ["AWS_ACCESS_KEY_ID"],
        "AWS_SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"],
        "AWS_ENDPOINT_URL": os.environ["AWS_S3_ENDPOINT"],
        # OVH rejects "auto"; the region is part of the signature.
        "AWS_REGION": os.environ.get("AWS_DEFAULT_REGION") or "de",
    }
    uri = f"s3://{os.environ['AWS_S3_BUCKET']}/timefusion/{source}"
    table = DeltaTable(uri, storage_options=storage)
    cells: dict[tuple[str, str], dict] = collections.defaultdict(
        lambda: {"files": 0, "bytes": 0, "rows": 0, "tagged": 0, "paths": []}
    )
    for add in table.get_add_actions(flatten=True).to_struct_array().to_pylist():
        date = str(add.get("partition.date"))
        if date < since or date == "None":
            continue
        project = add.get("partition.project_id") or "default"
        cell = cells[(project, date)]
        cell["files"] += 1
        cell["bytes"] += add.get("size_bytes") or 0
        cell["rows"] += add.get("num_records") or 0
        tags = add.get("tags")
        if isinstance(tags, dict) and tags.get("delta-rs.optimize.sort_by"):
            cell["tagged"] += 1
        if len(cell["paths"]) < 1:
            cell["paths"].append(add.get("path"))
    return dict(cells), table.version()


def footer_sorted(source: str, path: str) -> bool | None:
    """Does this file's footer declare a sort order? None if unreadable."""
    import pyarrow.parquet as pq
    from pyarrow import fs as pafs

    endpoint = os.environ["AWS_S3_ENDPOINT"]
    try:
        s3 = pafs.S3FileSystem(
            access_key=os.environ["AWS_ACCESS_KEY_ID"],
            secret_key=os.environ["AWS_SECRET_ACCESS_KEY"],
            endpoint_override=re.sub(r"^https?://", "", endpoint).rstrip("/"),
            scheme="https" if endpoint.startswith("https") else "http",
            region=os.environ.get("AWS_DEFAULT_REGION") or "de",
        )
        key = f"{os.environ['AWS_S3_BUCKET']}/timefusion/{source}/{path}"
        with s3.open_input_file(key) as handle:
            meta = pq.ParquetFile(handle).metadata
            return bool(meta.num_row_groups and meta.row_group(0).sorting_columns)
    except Exception:
        return None


def tier_dates(url: str, source: str, project: str) -> dict[str, set[str]]:
    """Dates present in each rollup tier of `source`, for one project."""
    tiers = {
        "1m": f"{source}_rollup_dashboard_1m_v3",
        "1h": f"{source}_rollup_dashboard_1h_v2",
    }
    if source == "otel_metrics":
        tiers = {"1m": "otel_metrics_rollup_metrics_1m_v2", "1h": "otel_metrics_rollup_metrics_1h_v2"}
    out = {}
    for label, table in tiers.items():
        rows = psql(url, f"SELECT date::text AS d FROM {table} WHERE project_id = '{project}' GROUP BY date;")
        out[label] = {r.strip() for r in rows if re.match(r"^\d{4}-\d{2}-\d{2}$", r.strip())}
    return out


def certified(url: str, source: str, project: str, date: str) -> bool | None:
    """Certified == the plan no longer needs DedupExec for that day.

    `project_id` must be the FULL id and the predicate must be equality. A
    `LIKE 'prefix%'` pins nothing the router recognises, and the plan comes back
    without a DedupExec for reasons that have nothing to do with certification —
    measured 2026-08-19: LIKE reported 0 for every day of a project where
    equality reports 1 for every day. The prefix is a CLI convenience only; it is
    resolved to a full id before any SQL is built.

    EXPLAIN only. None when the plan could not be obtained.
    """
    sql = (
        f"EXPLAIN SELECT count(*) FROM {source} WHERE project_id = '{project}' "
        f"AND timestamp >= timestamp '{date} 00:00:00' AND timestamp < timestamp '{date} 23:59:59';"
    )
    plan = psql(url, sql, timeout=60)
    if not plan:
        return None
    return not any("DedupExec" in line for line in plan)


def mark(ok: bool | None, good: str = "yes", bad: str = "NO") -> str:
    if ok is None:
        return _color(" ? ", DIM)
    return _color(f"{good:>3}", GOOD) if ok else _color(f"{bad:>3}", BAD)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--project", help="8-char prefix; omit for the fleet summary")
    ap.add_argument("--source", default="otel_logs_and_spans")
    ap.add_argument("--since", default=(dt.date.today() - dt.timedelta(days=30)).isoformat())
    ap.add_argument("--target-mb", type=int, default=256, help="per-file target; a day is compacted at <=2 files")
    ap.add_argument("--footers", action="store_true", help="read one parquet footer per day (slow, authoritative)")
    ap.add_argument("--no-pg", action="store_true", help="storage only; skip rollups and the dedup probe")
    args = ap.parse_args()

    for candidate in (".env.prod", ".env"):
        load_env(candidate)
    if not os.environ.get("AWS_S3_BUCKET"):
        print("no AWS_S3_BUCKET — run from the repo root with .env.prod present", file=sys.stderr)
        return 2

    cells, version = partitions(args.source, args.since)
    if not cells:
        print(f"no partitions in {args.source} since {args.since}")
        return 0
    url = None if args.no_pg else pg_url()
    today = dt.date.today().isoformat()

    if not args.project:
        # Fleet: one row per day. Sealed days should sit at ~1 file per project.
        by_day: dict[str, dict] = collections.defaultdict(lambda: {"files": 0, "bytes": 0, "projects": 0, "bad": 0})
        for (_project, date), cell in cells.items():
            day = by_day[date]
            day["files"] += cell["files"]
            day["bytes"] += cell["bytes"]
            day["projects"] += 1
            day["bad"] += 1 if cell["files"] > 2 else 0
        print(f"\n{args.source} · snapshot v{version} · active Adds since {args.since}\n")
        print(f"{'date':<12}{'files':>7}{'GB':>8}{'projects':>10}{'out of policy':>15}")
        for date in sorted(by_day):
            day = by_day[date]
            note = "  (open)" if date >= today else ""
            flag = _color(f"{day['bad']:>15}", BAD if day["bad"] else GOOD)
            print(f"{date:<12}{day['files']:>7}{day['bytes'] / 2**30:>8.2f}{day['projects']:>10}{flag}{note}")
        worst = max((d for d in by_day if d < today), key=lambda d: by_day[d]["files"], default=None)
        if worst:
            print(f"\nworst sealed day: {worst} at {by_day[worst]['files']} files / {by_day[worst]['bytes'] / 2**30:.1f} GB")
            floor = min((by_day[d]["files"] for d in by_day if d < today), default=0)
            print(f"converged floor : {floor} files — a day at the floor is ~1 file per project")
        print("\nper-day detail for one project: --project <8-char prefix>")
        return 0

    matches = sorted({proj for (proj, _date) in cells if proj.startswith(args.project)})
    if len(matches) != 1:
        print(f"--project {args.project!r} matched {len(matches)} projects: {', '.join(m[:8] for m in matches) or 'none'}")
        return 1
    project = matches[0]
    days = sorted({date for (proj, date) in cells if proj == project})

    tiers = tier_dates(url, args.source, project) if url else {"1m": set(), "1h": set()}

    print(f"\n{args.source} · project {project[:8]} · snapshot v{version} · since {args.since}")
    print(f"compacted = <=2 active files · sorted = footer sorting_columns · deduped = no DedupExec in plan\n")
    head = f"{'date':<12}{'files':>6}{'GB':>7}{'rows':>11}  {'compact':>7} {'tag':>5} {'sorted':>6} {'dedup':>5} {'1m':>3} {'1h':>3}"
    print(head)
    print("-" * len(re.sub(r"\033\[[0-9;]*m", "", head)))

    totals = collections.Counter()
    for date in days:
        cell = cells[(project, date)]
        open_day = date >= today
        compact = None if open_day else cell["files"] <= 2
        srt = footer_sorted(args.source, cell["paths"][0]) if (args.footers and cell["paths"]) else None
        ded = certified(url, args.source, project, date) if url and not open_day else None
        has_1m, has_1h = date in tiers["1m"], date in tiers["1h"]
        tag = f"{cell['tagged']}/{cell['files']}"
        print(
            f"{date:<12}{cell['files']:>6}{cell['bytes'] / 2**30:>7.2f}{cell['rows']:>11}  "
            f"{mark(compact):>7} {_color(f'{tag:>5}', DIM)} {mark(srt):>6} {mark(ded):>5} "
            f"{mark(has_1m, 'yes', 'NO')} {mark(has_1h, 'yes', 'NO')}"
            + ("  (open)" if open_day else "")
        )
        if open_day:
            continue
        totals["days"] += 1
        totals["compacted"] += bool(compact)
        totals["deduped"] += bool(ded)
        totals["1m"] += has_1m
        totals["1h"] += has_1h
        totals["sorted"] += bool(srt)

    n = totals["days"]
    print(f"\nsealed days: {n}")
    for label, key in (("compacted", "compacted"), ("deduped", "deduped"), ("1m rollup", "1m"), ("1h rollup", "1h")):
        got = totals[key]
        colour = GOOD if got == n else (WARN if got else BAD)
        print(f"  {label:<11} {_color(f'{got}/{n}', colour)}")
    if args.footers:
        print(f"  {'sorted':<11} {totals['sorted']}/{n}")
    else:
        print(f"  {'sorted':<11} not checked — pass --footers (the tag column is not the property)")

    # The derived tier cannot advance past its base; say so rather than leaving
    # it to be re-derived from the columns every time.
    missing_base = [d for d in days if d < today and d not in tiers["1m"]]
    if missing_base:
        span = f"{missing_base[0]} … {missing_base[-1]}" if len(missing_base) > 1 else missing_base[0]
        print(f"\n1m base tier missing {len(missing_base)} day(s): {span}")
        print("  the 1h tier cannot cover these until the base does — base rollup backfill is the critical path")
    return 0


if __name__ == "__main__":
    sys.exit(main())
