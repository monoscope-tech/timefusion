#!/usr/bin/env python3
"""One-command read-only health report for production TimeFusion.

Sources (each optional; a failed source is reported, never fatal):
  * `timefusion_stats` over pgwire (URL = monoscope/.env TIMEFUSION_PG_URL)
  * CapRover host over read-only ssh: deployed image, uptime, container mem/CPU
  * Delta logs on object storage (.env.prod creds): checkpoint + JSON replay per
    table -> active files/bytes/rows, deletion-vector files and masked rows,
    retained tombstones, and the commit lane mix over the retained log window

Counters reset at every boot, so every `*_total` is also shown per hour of
uptime; `--window N` samples the stats twice N seconds apart and reports rates
over that window instead, which is the only honest number on a young process.

    bench/prod_report.py                     # text report
    bench/prod_report.py --window 120        # plus 2-minute rates
    bench/prod_report.py --json > snap.json  # machine-readable, for dashboards
    bench/prod_report.py --no-delta --no-ssh # stats only (~2 s)
"""

from __future__ import annotations

import argparse
import io
import json
import math
import os
import re
import subprocess
import sys
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from delta_work_ledger import commit_actions, delta_version, lane_name, stats_rows  # noqa: E402

REPO = Path(__file__).resolve().parent.parent
HOST = "ubuntu@captain.s.past3.tech"
SERVICE = "srv-captain--timefusion"
MAIN = "otel_logs_and_spans"
# Config defaults (src/config.rs); prod does not override them.
VACUUM = {"schedule": "0 15 */6 * * *", "retention_h": 72, "log_retention_h": 6}


def env_file(path: Path) -> dict[str, str]:
    out = {}
    for line in path.read_text().splitlines() if path.exists() else []:
        m = re.match(r"\s*(?:export\s+)?([A-Z_][A-Z0-9_]*)=(.*)", line)
        if m:
            out[m[1]] = m[2].strip().strip("'\"")
    return out


def run(cmd: list[str], timeout: int) -> str:
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, check=True).stdout


# ---------- sources ----------

def pg_url() -> str:
    url = os.environ.get("TIMEFUSION_PG_URL") or env_file(REPO.parent / "monoscope" / ".env").get("TIMEFUSION_PG_URL")
    if not url:
        raise RuntimeError("TIMEFUSION_PG_URL not in env or ../monoscope/.env")
    return url


def fetch_stats() -> dict[str, str]:
    out = run(["psql", pg_url(), "-Atc", "SELECT component,key,value FROM timefusion_stats"], 60)
    return {f"{c}.{k}": v for c, k, v in (line.split("|", 2) for line in out.splitlines() if line.count("|") >= 2)}


def fetch_host() -> dict:
    fmt = '{{.Image}}|{{.CurrentState}}|{{.Error}}'
    script = (
        f"docker service ps {SERVICE} --no-trunc --format '{fmt}' | head -4; echo ===; "
        f"docker stats --no-stream --format '{{{{.MemUsage}}}}|{{{{.CPUPerc}}}}' $(docker ps -q -f name={SERVICE}.1) 2>/dev/null"
    )
    tasks, stats = run(["ssh", "-o", "ConnectTimeout=10", "-o", "BatchMode=yes", HOST, script], 45).split("===")
    rows = [line.split("|") for line in tasks.strip().splitlines()]
    digest = rows[0][0].rsplit(":", 1)[-1][:12] if rows else "?"
    mem, cpu = (stats.strip().splitlines() or ["?|?"])[0].split("|")
    return {"digest": digest, "commit": deployed_commit(), "state": rows[0][1] if rows else "?",
            "recent_tasks": [" ".join(r[1:]).strip() for r in rows[1:]], "mem": mem.strip(), "cpu": cpu.strip()}


def deployed_commit() -> str:
    """The image carries only a digest, so name the commit the last successful deploy run built."""
    try:
        return run(["gh", "run", "list", "-R", "monoscope-tech/timefusion", "-L", "40", "--json",  # --status is served stale
                    "workflowName,conclusion,headSha,updatedAt", "-q",
                    '[.[] | select(.workflowName == "Build and Deploy" and .conclusion == "success")][0] '
                    '| .headSha[:8] + " (deployed " + .updatedAt + ")"'], 30).strip() or "?"
    except Exception:  # noqa: BLE001
        return "?"


def s3_client():
    import boto3
    from botocore.config import Config

    # .env.prod is gitignored, so a fresh worktree falls back to the main checkout's.
    e = next((env_file(p) for p in (REPO / ".env.prod", REPO.parent / "timefusion" / ".env.prod") if p.exists()), {})
    os.environ.setdefault("AWS_REQUEST_CHECKSUM_CALCULATION", "when_required")
    os.environ.setdefault("AWS_RESPONSE_CHECKSUM_VALIDATION", "when_required")
    # OVH rejects region `auto` (AuthorizationHeaderMalformed ... expecting 'de').
    client = boto3.client("s3", endpoint_url=e["AWS_S3_ENDPOINT"], region_name="de",
                          aws_access_key_id=e["AWS_ACCESS_KEY_ID"], aws_secret_access_key=e["AWS_SECRET_ACCESS_KEY"],
                          config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 5}, max_pool_connections=64))
    return client, e["AWS_S3_BUCKET"]


def list_tables(client, bucket: str) -> list[str]:
    page = client.list_objects_v2(Bucket=bucket, Prefix="timefusion/", Delimiter="/")
    return [p["Prefix"].split("/")[1] for p in page.get("CommonPrefixes", []) if not p["Prefix"].split("/")[1].startswith("_")]


def replay_table(client, bucket: str, table: str, days: int) -> dict:
    """Checkpoint + JSON commits -> the live snapshot, with DVs and tombstones."""
    import pyarrow.parquet as pq

    prefix = f"timefusion/{table}/_delta_log/"
    keys = [(o["Key"], o["LastModified"]) for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
            for o in page.get("Contents", [])]
    commits = sorted((v, k, m) for k, m in keys if (v := delta_version(k)) is not None)
    if not commits:
        return {"table": table, "error": "no commits"}
    last_commit_age_s = (datetime.now(timezone.utc) - commits[-1][2]).total_seconds()
    active: dict[str, dict] = {}
    tombs: dict[str, dict] = {}
    cp_version = -1
    try:
        cp = json.loads(client.get_object(Bucket=bucket, Key=prefix + "_last_checkpoint")["Body"].read())
        cp_version = cp["version"]
        parts = cp.get("parts")
        names = ([f"{cp_version:020d}.checkpoint.{i:010d}.{parts:010d}.parquet" for i in range(1, parts + 1)]
                 if parts else [f"{cp_version:020d}.checkpoint.parquet"])
        for name in names:
            t = pq.read_table(io.BytesIO(client.get_object(Bucket=bucket, Key=prefix + name)["Body"].read()), columns=["add", "remove"])
            for rec in t.column("add").to_pylist():
                if rec:
                    pv = rec.get("partitionValues")
                    rec["partitionValues"] = dict(pv) if isinstance(pv, list) else (pv or {})
                    active[rec["path"]] = rec
            for rec in t.column("remove").to_pylist():
                if rec:
                    tombs[rec["path"]] = rec
    except client.exceptions.NoSuchKey:
        pass
    after = [(v, k) for v, k, _ in commits if v > cp_version]
    with ThreadPoolExecutor(32) as pool:
        texts = list(pool.map(lambda vk: client.get_object(Bucket=bucket, Key=vk[1])["Body"].read().decode(), after))
    lanes: Counter = Counter()
    for text in texts:
        info, adds, removes = commit_actions(text)
        lanes[lane_name(info)] += 1
        for r in removes:  # removes first: a DV update is remove(path) + add(path) in one commit
            active.pop(r["path"], None)
            tombs[r["path"]] = r
        for a in adds:
            active[a["path"]] = a
            tombs.pop(a["path"], None)
    # Lanes over every retained commit, not only those after the checkpoint.
    if len(after) < len(commits):
        with ThreadPoolExecutor(32) as pool:
            older = pool.map(lambda vk: client.get_object(Bucket=bucket, Key=vk[1])["Body"].read().decode(),
                             [(v, k) for v, k, _ in commits if v <= cp_version])
            for text in older:
                lanes[lane_name(commit_actions(text)[0])] += 1

    since = (date.today() - timedelta(days=days - 1)).isoformat()
    per_date: dict[str, dict] = defaultdict(lambda: Counter())
    per_project: dict[str, dict] = defaultdict(lambda: defaultdict(lambda: [0, 0.0]))
    for a in active.values():
        d = str(a["partitionValues"].get("date") or "none")
        if d >= since:
            cell = per_project[str(a["partitionValues"].get("project_id") or "NULL")[:8]][d]
            cell[0] += 1
            cell[1] += int(a.get("size") or 0) / 1e9
        dv = int((a.get("deletionVector") or {}).get("cardinality") or 0)
        c = per_date[d]
        c["files"] += 1
        c["bytes"] += int(a.get("size") or 0)
        c["rows"] += stats_rows(a)
        c["dv_files"] += bool(dv)
        c["masked_rows"] += dv
    now_ms = time.time() * 1000
    tomb_ages = [(now_ms - int(t.get("deletionTimestamp") or now_ms)) / 3.6e6 for t in tombs.values()]
    total = sum(per_date.values(), Counter())
    return {
        "table": table, "version": commits[-1][0], "last_commit_age_s": round(last_commit_age_s),
        "retained_commits": len(commits), "log_window_h": round((commits[-1][2] - commits[0][2]).total_seconds() / 3600, 1),
        "lanes": dict(lanes.most_common()), "totals": dict(total),
        "per_date": {d: dict(c) for d, c in sorted(per_date.items())},
        # {project8: {date: [files, GB]}} — the compaction chart's input.
        "per_project_date": {p: {d: [n, round(gb, 2)] for d, (n, gb) in sorted(ds.items())} for p, ds in sorted(per_project.items())},
        "tombstones": len(tombs), "tombstone_bytes": sum(int(t.get("size") or 0) for t in tombs.values()),
        "oldest_tombstone_h": round(max(tomb_ages), 1) if tomb_ages else 0,
        "tombstones_past_retention": sum(h > VACUUM["retention_h"] + 6 for h in tomb_ages),
    }


def fetch_delta(days: int, all_tables: bool) -> dict:
    client, bucket = s3_client()
    tables = list_tables(client, bucket)
    live = [t for t in tables if t == MAIN or (all_tables or "rollup" in t)]

    def one(t):
        try:
            return replay_table(client, bucket, t, days if t == MAIN else 2)
        except Exception as e:  # noqa: BLE001 - a broken table must not hide the others
            return {"table": t, "error": f"{type(e).__name__}: {e}"}

    with ThreadPoolExecutor(8) as pool:
        return {r["table"]: r for r in pool.map(one, live)}


# ---------- hourly history (monoscope OTel metrics) ----------

MONOSCOPE_PROJECT = "87576849-4941-49d3-a15d-680fef88a1a8"  # TF exports its own metrics into this project
# name -> (metric, extra filter, kind). Gauges sum across series, "max" takes the
# worst series (latency); counters are
# cumulative per process, so they are differenced per series, a drop = restart.
HISTORY_METRICS = {
    **{k: (f"timefusion.maintenance.{k}", "", "gauge") for k in (
        "pending_base_rollup", "pending_dedup", "pending_repair", "due_nonquarantined_tasks",
        "sealed_compaction_debt_bytes", "cpu_tokens_used", "cpu_tokens_capacity")},
    **{k: (f"timefusion.maintenance.{k}", "", "counter") for k in (
        "admission_refused_cpu", "admission_refused_state_bytes", "dedup_bins_committed", "light_optimize_bins_committed")},
    "p99_ms": ("timefusion.pgwire.query_latency_p99_ms", "", "max"),
    "hits": ("timefusion.rollup.hits", "", "counter"),
    "misses": ("timefusion.rollup.misses", "", "counter"),
    "dedup_skipped": ("timefusion.scan.dedup_skipped", "", "counter"),
    "dedup_eligible_scans": ("timefusion.scan.dedup_eligible_scans", "", "counter"),
    "cert_slice_files_proved": ("timefusion.scan.cert_slice_files_proved", "", "counter"),
    "dedup_dropped_rows": ("timefusion.compaction.dedup_dropped_rows", "", "counter"),
    # ns of CPU, cumulative per container: differenced per hour / 3600e9 = cores.
    "cpu_cores": ("container.cpu.usage.total", 'and resource contains "monoscope-tech/timefusion"', "counter"),
}
CACHE = Path.home() / ".cache" / "tf_prod_report"


def monoscope_day(metric: str, extra: str, day: date) -> list[dict]:
    """One UTC day of hourly max per series, as [{series: {ts: value}}]. Closed days are cached:
    a 7-day query times out server-side (3 min) while a day returns in under a second."""
    cache = CACHE / f"{metric}{hash(extra) & 0xffff:04x}_{day}.json"
    if day < date.today() and cache.exists():
        return json.loads(cache.read_text())
    lo = datetime.combine(day, datetime.min.time(), timezone.utc)
    q = f'where metric_name == "{metric}" {extra} | summarize max(value) by bin(timestamp, 1h), {"resource" if extra else "attributes"}'
    d = json.loads(run(["monoscope", "--json", "-p", MONOSCOPE_PROJECT, "chart", q, "--source", "metrics",
                        "--from", lo.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "--to", min(lo + timedelta(days=1), datetime.now(timezone.utc)).strftime("%Y-%m-%dT%H:%M:%SZ")], 170))
    if d.get("error") or d.get("code"):
        raise RuntimeError(f"{metric} {day}: {d.get('error') or d.get('message')}")
    cols = d.get("headers") or []
    series = {c: {int(row[0]): row[i] for row in d.get("dataset") or [] if row[i] is not None} for i, c in enumerate(cols) if i}
    if day < date.today():
        CACHE.mkdir(parents=True, exist_ok=True)
        cache.write_text(json.dumps(series))
    return series


def fetch_history(days: int) -> dict:
    """Hourly series + per-day rows for the dashboard's maintenance section."""
    span = [date.today() - timedelta(days=i) for i in reversed(range(days))]
    with ThreadPoolExecutor(16) as pool:
        raw = {(k, d): pool.submit(monoscope_day, m, extra, d) for k, (m, extra, _) in HISTORY_METRICS.items() for d in span}
        raw = {key: f.result() for key, f in raw.items()}
    hourly: dict[str, dict[int, float]] = {}
    for k, (_, _, kind) in HISTORY_METRICS.items():
        merged: dict[str, dict[int, float]] = defaultdict(dict)
        for d in span:
            for col, pts in raw[(k, d)].items():
                merged[col].update({int(t): v for t, v in pts.items()})
        out: dict[int, float] = defaultdict(float)
        for pts in merged.values():
            prev = None
            for t in sorted(pts):
                v = pts[t]
                if kind == "gauge":
                    out[t] += v
                elif kind == "max":
                    out[t] = max(out.get(t, v), v)
                elif prev is not None:
                    out[t] += v - prev if v >= prev else v
                prev = v
        hourly[k] = {t: v / 3600e9 if k == "cpu_cores" else v for t, v in out.items()}
    ts = sorted(set().union(*hourly.values()))
    day_of = lambda t: datetime.fromtimestamp(t / 1000, timezone.utc).strftime("%m-%d")  # noqa: E731
    mdays = [d.strftime("%m-%d") for d in span]

    def per_day(k: str, f) -> list:
        by: dict[str, list] = defaultdict(list)
        for t, v in hourly[k].items():
            by[day_of(t)].append(v)
        return [f(by[d]) if by.get(d) else None for d in mdays]

    ratio = lambda a, b: [None if x is None or y is None or x + y == 0 else 100 * x / (x + y) for x, y in zip(a, b)]  # noqa: E731
    scaled = lambda a, k: [None if v is None else v * k for v in a]  # noqa: E731
    mx, sm = max, sum
    skipped, eligible = per_day("dedup_skipped", sm), per_day("dedup_eligible_scans", sm)
    rows = [
        ("Due tasks (peak)", per_day("due_nonquarantined_tasks", mx), 0),
        ("Pending base rollup (peak)", per_day("pending_base_rollup", mx), 0),
        ("Pending dedup (peak)", per_day("pending_dedup", mx), 0),
        ("Pending repair (peak)", per_day("pending_repair", mx), 0),
        ("Sealed compaction debt, GB decoded (peak)", scaled(per_day("sealed_compaction_debt_bytes", mx), 1e-9), 0),
        ("Hot-packing bins committed", per_day("light_optimize_bins_committed", sm), 0),
        ("Dedup bins committed", per_day("dedup_bins_committed", sm), 0),
        ("Duplicate rows dropped by compaction (M)", scaled(per_day("dedup_dropped_rows", sm), 1e-6), 1),
        ("Rollup hit rate %", ratio(per_day("hits", sm), per_day("misses", sm)), 1),
        ("Scans that skipped dedup via certification %",
         [None if a is None or not e else 100 * a / e for a, e in zip(skipped, eligible)], 1),
        ("Files proved by certification", per_day("cert_slice_files_proved", sm), 0),
        ("CPU admission refusals (k)", scaled(per_day("admission_refused_cpu", sm), 1e-3), 1),
        ("State-memory admission refusals (k)", scaled(per_day("admission_refused_state_bytes", sm), 1e-3), 1),
        ("pgwire p99, median hour's worst minute (s)", scaled(per_day("p99_ms", lambda v: sorted(v)[len(v) // 2]), 1e-3), 2),
    ]
    series = {k: [None if (v := hourly[k].get(t)) is None else round(v, 2) for t in ts] for k in hourly}
    return {"ts": ts, "series": series, "mdays": mdays,
            "rows": [[n, [None if v is None else round(v, d) for v in a], d] for n, a, d in rows]}


# ---------- presentation ----------

def num(stats: dict, key: str) -> float | None:
    v = stats.get(key)
    if v is None:  # accept a bare key if exactly one component carries it
        hits = [s for k, s in stats.items() if k.split(".", 1)[1] == key]
        v = hits[0] if len(hits) == 1 else None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def human(v: float | None, unit: str = "") -> str:
    if v is None:
        return "—"
    if unit == "B":
        for u in ("B", "KB", "MB", "GB", "TB"):
            if abs(v) < 1000 or u == "TB":
                return f"{v:.1f}{u}" if u != "B" else f"{v:.0f}B"
            v /= 1000
    if unit == "us":
        return f"{v/1e6:.2f}s" if v >= 1e6 else f"{v/1e3:.0f}ms"
    if unit == "s":
        return f"{v/86400:.1f}d" if v >= 86400 else f"{v/3600:.1f}h" if v >= 3600 else f"{v:.0f}s"
    if unit == "%":
        return f"{v:.1f}%"
    return f"{v:,.0f}" if abs(v) >= 100 or v == int(v) else f"{v:.2f}"


# (label, key, unit). A trailing "/h" shows value and per-uptime-hour rate.
SECTIONS: list[tuple[str, list[tuple[str, str, str]]]] = [
    ("Query latency (pgwire, since boot)", [
        ("p50 / p95", "pgwire.lat_p50_us_approx|pgwire.lat_p95_us_approx", "us"),
        ("p99 / p999", "pgwire.lat_p99_us_approx|pgwire.lat_p999_us_approx", "us"),
        ("scan p50 / p99 / p999", "scan.lat_p50_us_approx|scan.lat_p99_us_approx|scan.lat_p999_us_approx", "us"),
        ("queries", "pgwire.queries_total", "/h"),
        ("heavy admitted / queued / timed out", "scan.heavy_query_admitted|scan.heavy_query_queued|scan.heavy_query_queue_timeout", ""),
        ("plan cache hit", "plan_cache.hit_pct", "%"),
        ("delta skipped (served from mem)", "scan.skipped_delta_pct", "%"),
        ("tantivy prefilter used / skipped", "scan.prefilter_used|scan.prefilter_skipped", ""),
        ("foyer hits / misses / bypassed", "foyer.hits|foyer.misses|foyer.insert_bypassed", ""),
        ("runtime sched lag max", "runtime.scheduling_lag_max_ms", "ms"),
    ]),
    ("Ingest & flush", [
        ("rows ingested", "buffered_layer.rows_ingested_total", "/h"),
        ("rows in buffer (lag)", "buffered_layer.rows_in_buffer_lag", ""),
        ("flushes ok / failed", "buffered_layer.flush_completed_total|buffered_layer.flush_failed_total", ""),
        ("dirty re-flush rows drained / reflushed", "flush.dirty_reflush_rows_drained|flush.dirty_reflush_rows_reflushed", ""),
        ("buffer pressure", "buffered_layer.pressure_pct", "%"),
        ("backpressure rejected", "buffered_layer.backpressure_rejected_total", ""),
        ("WAL disk / quarantine files", "wal.disk_mb|wal.quarantine_files", ""),
    ]),
    ("Maintenance backlog", [
        ("tasks pending / running / retry / quarantined", "maintenance.tasks_pending|maintenance.tasks_running|maintenance.tasks_retry|maintenance.tasks_quarantined", ""),
        ("claimable / due", "maintenance.claimable_tasks|maintenance.tasks_due_nonquarantined", ""),
        ("backlog bytes", "maintenance.backlog_bytes", "B"),
        ("oldest task age", "maintenance.oldest_task_age_seconds", "s"),
        ("tasks completed (journal lifetime)", "maintenance.tasks_complete", ""),
        ("processed bytes", "maintenance.processed_bytes_total", "B/h"),
        ("pending base / derived rollup", "maintenance.pending_base_rollup|maintenance.pending_derived_rollup", ""),
        ("pending dedup / hot packing", "maintenance.pending_dedup|maintenance.pending_hot_packing", ""),
        ("pending sealed consolidation / repair", "maintenance.pending_sealed_consolidation|maintenance.pending_repair", ""),
        ("admission refused cpu / state", "maintenance.admission_refused_cpu_total|maintenance.admission_refused_state_bytes_total", ""),
        ("cpu tokens used / cap", "maintenance.cpu_tokens_used|maintenance.cpu_tokens_capacity", ""),
        ("state bytes used / cap", "maintenance.state_bytes_used|maintenance.state_bytes_capacity", "B"),
        ("coordinator errors / invariant violations", "maintenance.maintenance_coordinator_errors|maintenance.compaction_invariant_violations", ""),
        ("claim avg / journal lock wait avg", "block.coordinator_claim.avg_us|block.journal_lock_wait.avg_us", "us"),
    ]),
    ("Rollups", [
        ("hits full / hybrid / misses", "maintenance.rollup_hits_full_total|maintenance.rollup_hits_hybrid_total|maintenance.rollup_misses_total", ""),
        ("dirty partitions", "maintenance.rollup_dirty_partitions", ""),
        ("contiguous days min / median", "maintenance.rollup_min_contiguous_days|maintenance.rollup_median_contiguous_days", ""),
        ("oldest invalidation age", "maintenance.rollup_oldest_invalidation_age_seconds", "s"),
        ("rebuilds full / incremental", "maintenance.rollup_rebuilds_full_total|maintenance.rollup_rebuilds_incremental_total", ""),
        ("witness carried / unverifiable", "maintenance.rollup_witness_carried_total|maintenance.rollup_unverifiable_total", ""),
        ("stale: fp moved / shrank / grew", "scan.rollup_stale_fp_moved|scan.rollup_stale_shrank|scan.rollup_stale_grew", ""),
    ]),
    ("Dedup & deletion vectors", [
        ("dedup waves committed / failed", "maintenance.dedup_waves_committed_total|maintenance.dedup_failed_total", ""),
        ("dedup rows dropped", "maintenance.work.Dedup.rows_dropped", "/h"),
        ("DV-dedup bins staged", "maintenance.dv_dedup_bins_staged_total", ""),
        ("DV strips landed", "maintenance.dv_rewrites_landed_total", "/h"),
        ("DV strip rows retired", "maintenance.dv_rewrite_rows_retired_total", "/h"),
        ("DV strip plan sorts / lossy refusals", "maintenance.dv_strip_plan_sorts_total|maintenance.lossy_rewrite_refusals_total", ""),
        ("MoR versions appended / retracted", "dml.mor_version_rows_appended_total|dml.mor_versions_retracted_total", ""),
        ("read-side dedup skipped", "scan.dedup_skipped_pct", "%"),
        ("read-side denied: never certified", "scan.dedup_denied_never_certified_pct", "%"),
    ]),
    ("Certification", [
        ("granted", "scan.cert_granted_total", "/h"),
        ("dwell p50 / p90", "scan.cert_dwell_p50_secs|scan.cert_dwell_p90_secs", "s"),
        ("slice files proved / unproven", "scan.cert_slice_files_proved|scan.cert_slice_files_unproven", ""),
        ("declined dirty bins / refused fp moved", "scan.cert_declined_dirty_bins|scan.cert_refused_fp_moved", ""),
    ]),
    ("Light compaction (hot tail)", [
        ("waves / bins committed", "maintenance.light_optimize_waves_committed_total|maintenance.light_optimize_bins_committed_total", ""),
        ("failed / timed out", "maintenance.light_optimize_failed_total|maintenance.light_optimize_timed_out_total", ""),
        ("permits available / total", "maintenance.light_rewrite_permits_available|maintenance.light_rewrite_permits_total", ""),
        ("hot packing reserve unavailable", "maintenance.hot_packing_reserve_unavailable", ""),
    ]),
    ("Major compaction (sealed days)", [
        ("sealed debt", "maintenance.sealed_compaction_debt_bytes", "B"),
        ("eligible sealed", "maintenance.eligible_sealed_total", ""),
        ("repair in flight / sorted at write", "maintenance.repair_bins_in_flight|maintenance.repair_sorted_at_write_total", ""),
        ("units that selected nothing", "maintenance.compaction_units_selected_nothing", ""),
    ]),
    ("Vacuum & log", [
        ("checkpoints created / failed / lag", "maintenance.checkpoints_created|maintenance.checkpoint_failed|maintenance.checkpoint_lag_versions", ""),
        ("log files cleaned / cleanup failed", "maintenance.log_files_cleaned|maintenance.log_cleanup_failed", ""),
    ]),
    ("Memory", [
        ("process RSS", "buffered_layer.process_rss_mb", "MB"),
        ("charged of limit", "memory.charged_pct", "%"),
        ("jemalloc allocated / resident", "jemalloc.allocated_mb|jemalloc.resident_mb", "MB"),
        ("pools: query / maintenance / coordinator", "memory.query_pool_pct|memory.maintenance_pool_pct|memory.coordinator_pool_pct", "%"),
    ]),
]


def fmt_row(stats: dict, keys: str, unit: str, uptime_h: float, rates: dict | None) -> str:
    vals = [num(stats, k) for k in keys.split("|")]
    base = unit.split("/")[0]
    if base == "ms":
        text = " / ".join(human(v and v * 1000, "us") for v in vals)
    elif base == "MB":
        text = " / ".join(human(v and v * 1e6, "B") for v in vals)
    else:
        text = " / ".join(human(v, base) for v in vals)
    if unit.endswith("/h") and vals[0] is not None:
        per = rates.get(keys) if rates else None
        text += f"   ({human(per if per is not None else vals[0] / max(uptime_h, 1e-9), base)}/h{' window' if per is not None else ' since boot'})"
    return text


def top(stats: dict, pattern: str, n: int = 5) -> list[tuple[str, float]]:
    rx = re.compile(pattern)
    hits = [(m[1], float(v)) for k, v in stats.items() if (m := rx.fullmatch(k)) and re.fullmatch(r"-?[\d.]+", v)]
    return sorted((h for h in hits if h[1] > 0), key=lambda h: -h[1])[:n]


def flags(stats: dict, host: dict | None, delta: dict | None) -> list[tuple[str, str]]:
    n = lambda k: num(stats, k) or 0  # noqa: E731
    out = []
    def chk(level, cond, msg):
        if cond:
            out.append((level, msg))
    chk("CRIT", n("buffered_layer.flush_failed_total") > 0, "flushes failing")
    chk("CRIT", n("wal.quarantine_files") > 0, "WAL quarantine non-empty: acked data not in the store")
    chk("CRIT", n("maintenance.lossy_rewrite_refusals_total") > 0, "lossy rewrite refusals > 0")
    chk("CRIT", n("buffered_layer.backpressure_rejected_total") > 0, "ingest rejected by backpressure")
    chk("WARN", n("pgwire.lat_p99_us_approx") > 2e6, f"pgwire p99 {human(n('pgwire.lat_p99_us_approx'), 'us')} > 2s")
    chk("WARN", n("maintenance.tasks_quarantined") > 0, "maintenance tasks quarantined")
    chk("WARN", n("maintenance.maintenance_coordinator_errors") > 0, "coordinator errors")
    chk("WARN", n("maintenance.oldest_task_age_seconds") > 86400, f"oldest maintenance task {human(n('maintenance.oldest_task_age_seconds'), 's')} old")
    chk("WARN", n("memory.charged_pct") > 80, "memory charged > 80% of limit")
    chk("WARN", n("maintenance.checkpoint_failed") > 0, "checkpoint failures")
    chk("INFO", n("runtime.uptime_seconds") < 3600, "process < 1h old: counters and latencies are not yet representative")
    # "No such container" is the old task at a normal deploy handoff, not a crash.
    bad = [t for t in (host or {}).get("recent_tasks", [])[:2] if re.search(r"Failed|Rejected", t) and "No such container" not in t]
    chk("WARN", bool(bad), f"recent task failure (OOM/crash?): {bad[0] if bad else ''}")
    main = (delta or {}).get(MAIN) or {}
    chk("WARN", main.get("tombstones_past_retention", 0) > 0, f"{main.get('tombstones_past_retention')} tombstones older than retention+6h: vacuum lagging")
    return out


def render(snap: dict) -> str:
    stats, host, delta, rates = snap.get("stats") or {}, snap.get("host"), snap.get("delta"), snap.get("rates")
    uptime_h = (num(stats, "runtime.uptime_seconds") or 0) / 3600
    L = [f"# TimeFusion prod report  {snap['taken_at']}"]
    if host:
        L.append(f"commit {host['commit']}  image {host['digest']}  {host['state']}  mem {host['mem']}  cpu {host['cpu']}")
    elif "host_error" in snap:
        L.append(f"host: unavailable ({snap['host_error']})")
    if not stats:
        L.append(f"stats: unavailable ({snap.get('stats_error')})")
    else:
        L.append(f"uptime {uptime_h:.2f}h — counters below are since boot" + (f"; rates over a {snap['window_s']}s window" if rates else ""))
    for level, msg in snap.get("flags", []):
        L.append(f"  [{level}] {msg}")
    for title, rows in SECTIONS if stats else []:
        L.append(f"\n## {title}")
        L += [f"  {label:<46} {fmt_row(stats, keys, unit, uptime_h, rates)}" for label, keys, unit in rows]
        if title == "Maintenance backlog":
            L.append("  top retry reasons: " + ", ".join(f"{k}={v:,.0f}" for k, v in top(stats, r"maintenance\.retry\.(.+)")))
        if title == "Rollups":
            h = sum(num(stats, f"maintenance.rollup_{k}") or 0 for k in ("hits_full_total", "hits_hybrid_total"))
            m = num(stats, "maintenance.rollup_misses_total") or 0
            L.append(f"  {'hit rate':<46} {human(100 * h / (h + m) if h + m else None, '%')}")
            L.append("  top miss reasons: " + ", ".join(f"{k}={v:,.0f}" for k, v in top(stats, r"maintenance\.rollup_miss_(.+)_total")))
        if title == "Vacuum & log":
            L.append(f"  {'vacuum schedule / retention':<46} {VACUUM['schedule']} / {VACUUM['retention_h']}h (config default)")
            if (m := (delta or {}).get(MAIN)) and "error" not in m:
                L.append(f"  {'tombstones retained (main)':<46} {m['tombstones']:,} ({human(m['tombstone_bytes'], 'B')}), oldest {m['oldest_tombstone_h']}h, past retention {m['tombstones_past_retention']}")
    if delta:
        L.append("\n## Delta snapshot")
        dormant = [t for t, d in delta.items() if d.get("last_commit_age_s", 0) > 2 * 86400]
        for t, d in delta.items():
            if t in dormant:
                continue
            if "error" in d:
                L.append(f"  {t}: ERROR {d['error']}")
                continue
            tot = d["totals"]
            L.append(f"  {t:<52} v{d['version']}  files {tot.get('files', 0):,}  {human(tot.get('bytes', 0), 'B')}  "
                     f"DV files {tot.get('dv_files', 0):,} masked {tot.get('masked_rows', 0):,}  last commit {human(d['last_commit_age_s'], 's')} ago")
        if dormant:
            L.append(f"  dormant (no commit in 2d): {', '.join(dormant)}")
        m = delta.get(MAIN)
        if m and "error" not in m:
            lanes = ", ".join(f"{k}={v}" for k, v in list(m["lanes"].items())[:6])
            L.append(f"\n  {MAIN} commits over the retained {m['log_window_h']}h log ({m['retained_commits']}): {lanes}")
            L.append(f"  {'date':<11}{'files':>7}{'GB':>8}{'ideal':>7}{'rows(M)':>9}{'DV files':>9}{'masked%':>9}")
            for d, c in sorted(m["per_date"].items(), reverse=True)[: snap.get("days", 8)]:
                gb = c["bytes"] / 1e9
                L.append(f"  {d:<11}{c['files']:>7}{gb:>8.1f}{math.ceil(gb) or 1:>7}{c['rows']/1e6:>9.1f}{c['dv_files']:>9}"
                         f"{100 * c['masked_rows'] / c['rows'] if c['rows'] else 0:>8.1f}%")
    for src in ("delta_error",):
        if src in snap:
            L.append(f"\n{src}: {snap[src]}")
    return "\n".join(L)


# ---------- dashboard (docs/dashboards/compaction-chart.html) ----------

TAGS = {"87576849": "whale", "28f62f01": "shipbubble"}
CHART_DAYS, THEN_DAYS = 14, [f"2026-08-{d}" for d in range(16, 29)]  # the 08-28 baseline row stays static


def tile(k: str, v: str, dim: str, d: str, cls: str = "") -> str:
    return f'  <div class="tile"><div class="k">{k}</div><div class="v{cls}">{v}<span class="dim">{dim}</span></div><div class="d">{d}</div></div>'


def tiles(*ts: str) -> str:
    return '<div class="tiles">\n' + "\n".join(ts) + "\n</div>\n"


def span(ds: list[str]) -> str:
    return f"{ds[0][5:]}…{ds[-1][5:]}" if ds else "—"


def html_compaction(snap: dict) -> tuple[dict, str, str]:
    m, stats, host = snap["delta"][MAIN], snap.get("stats") or {}, snap.get("host") or {}
    today = datetime.now(timezone.utc).date().isoformat()
    per_date, ppd = m["per_date"], m["per_project_date"]
    days = [(date.fromisoformat(today) - timedelta(days=i)).isoformat() for i in reversed(range(CHART_DAYS))]
    rows = [[p, TAGS.get(p, ""), [ds.get(d) for d in days]] for p, ds in ppd.items() if any(ds.get(d) for d in days)]
    tnow = [[per_date[d]["files"], round(per_date[d]["bytes"] / 1e9, 2)] if d in per_date else [0, 0] for d in THEN_DAYS]
    sealed = [d for d in per_date if "2000" < d < today][-30:]
    files = [per_date[d]["files"] for d in sealed]
    dv_dates = [d for d in per_date if "2000" < d < today and per_date[d]["dv_files"]]
    cells = [(p, d, n, gb) for p, ds in ppd.items() for d, (n, gb) in ds.items() if d < today and d in days]
    worst = max(cells, key=lambda c: (c[2], c[3]), default=None)
    t, tot = per_date.get(today, {}), m["totals"]
    sealed_dv = sum(per_date[d]["dv_files"] for d in dv_dates)
    up = (num(stats, "runtime.uptime_seconds") or 0) / 60
    sub = (f'<p class="sub">unified table on OVH · snapshot <b>v{m["version"]}</b> · {snap["taken_at"][:16].replace("T", " ")} UTC · '
           f'<code>{host.get("commit", "?").split()[0]}</code> up {up:.0f} min · {today[5:]} is the OPEN partition · '
           f'generated by <code>bench/prod_report.py --html</code></p>')
    body = tiles(
        tile("Sealed days", f"{min(files, default=0)}–{max(files, default=0)}", f"files per day, {span(sealed)}",
             f"last 30 sealed days · <b>{sealed_dv:,}</b> deletion-vector files left on any sealed day"
             + (f" ({', '.join(d[5:] for d in dv_dates)})" if dv_dates else ""), " win" if sealed_dv == 0 else ""),
        tile("Table-wide", f"{tot.get('files', 0):,}", f"files over {tot.get('bytes', 0) / 1e9:,.0f} GB",
             f"<b>{sum(c[2] <= 2 for c in cells)} of {len(cells)}</b> sealed project×day cells in the grid hold two files or fewer. "
             f"{t.get('files', 0):,} files are today's open partition"),
        tile("Today's deletion vectors", f"{t.get('dv_files', 0):,}", f"DV files · {t.get('masked_rows', 0) / 1e6:.1f} M masked rows",
             f"of {t.get('rows', 0) / 1e6:.1f} M rows today. Strips landed since boot: <b>{num(stats, 'maintenance.dv_rewrites_landed_total') or 0:,.0f}</b> "
             f"({num(stats, 'maintenance.lossy_rewrite_refusals_total') or 0:,.0f} lossy refusals)"),
        tile("Worst sealed cell", f"{worst[2] if worst else '—'}", f"files · {worst[0]} / {worst[1][5:]}" if worst else "",
             f"It holds <b>{worst[3]:.1f} GB</b>, so its floor at 0.5 GB/file is {max(1, math.ceil(worst[3] / 0.5))}" if worst else ""),
    )
    data = {"days": [d[5:] for d in days], "OPEN": CHART_DAYS - 1, "SEALED": CHART_DAYS - 2, "rows": rows, "tnow": tnow}
    return data, sub, body


def html_live(snap: dict) -> str:
    st = snap.get("stats") or {}
    n = lambda k: num(st, k) or 0  # noqa: E731
    up, main = n("runtime.uptime_seconds") / 60, (snap.get("delta") or {}).get(MAIN, {})
    hits, miss = n("maintenance.rollup_hits_full_total") + n("maintenance.rollup_hits_hybrid_total"), n("maintenance.rollup_misses_total")
    refused = {"state memory": n("maintenance.admission_refused_state_bytes_total"), "CPU tokens": n("maintenance.admission_refused_cpu_total")}
    binding = max(refused, key=refused.get)
    return (f'<h2 style="font-size:14px;font-weight:650;margin:30px 0 2px">Maintenance now — live at {snap["taken_at"][11:16]} UTC</h2>\n'
            f'<p class="sub" style="margin-bottom:12px">timefusion_stats on a {up:.0f}-minute-old process · counters reset at boot'
            f'{" — too young to judge rates" if up < 60 else ""}</p>\n' + tiles(
        tile("Due / running", f"{n('maintenance.tasks_due_nonquarantined'):,.0f} / {n('maintenance.tasks_running'):,.0f}",
             f"tasks · oldest {human(n('maintenance.oldest_task_age_seconds'), 's')}",
             f"Pending base rollup <b>{n('maintenance.pending_base_rollup'):,.0f}</b>, dedup <b>{n('maintenance.pending_dedup'):,.0f}</b>, "
             f"repair <b>{n('maintenance.pending_repair'):,.0f}</b> · quarantined {n('maintenance.tasks_quarantined'):,.0f}"),
        tile("What binds now", binding, f"{refused['state memory'] / 1e3:,.1f} k state vs {refused['CPU tokens'] / 1e3:,.1f} k CPU refusals",
             f"State {human(n('maintenance.state_bytes_used'), 'B')} of {human(n('maintenance.state_bytes_capacity'), 'B')}; "
             f"CPU tokens {n('maintenance.cpu_tokens_used'):.0f} of {n('maintenance.cpu_tokens_capacity'):.0f}"),
        tile("Rollup hit rate", human(100 * hits / (hits + miss) if hits + miss else None, "%"), f"{hits:,.0f} hits · {miss:,.0f} misses since boot",
             "Top miss reasons: " + ", ".join(f"{k.replace('_', ' ')} {v:,.0f}" for k, v in top(st, r"maintenance\.rollup_miss_(.+)_total", 3))),
        tile("Query latency", human(n("pgwire.lat_p99_us_approx"), "us"), "pgwire p99 since boot",
             f"p50 {human(n('pgwire.lat_p50_us_approx'), 'us')} · p95 {human(n('pgwire.lat_p95_us_approx'), 'us')} · "
             f"p999 {human(n('pgwire.lat_p999_us_approx'), 'us')}"),
        tile("Vacuum", f"{main.get('tombstones_past_retention', '—')}", "tombstones past retention",
             f"{main.get('tombstones', 0):,} retained ({human(main.get('tombstone_bytes', 0), 'B')}), oldest {main.get('oldest_tombstone_h', 0)} h; "
             f"schedule {VACUUM['schedule']}, retention {VACUUM['retention_h']} h", " win" if main.get("tombstones_past_retention") == 0 else ""),
    ))


def html_history(hist: dict) -> tuple[dict, str]:
    se, rows = hist["series"], {name: vals for name, vals, _ in hist["rows"]}
    busy = sorted(c for c, d in zip(se["cpu_cores"], se["due_nonquarantined_tasks"]) if c is not None and d is not None and d >= 300)
    tok = [v for v in se["cpu_tokens_used"] if v is not None]
    cap = next((v for v in reversed(se["cpu_tokens_capacity"]) if v), 66)
    due, base, dd = rows["Due tasks (peak)"], rows["Pending base rollup (peak)"], rows["Pending dedup (peak)"]
    first = next((i for i, v in enumerate(due) if v is not None), 0)
    peak = lambda name: max(((v, d) for v, d in zip(rows[name], hist["mdays"]) if v is not None), default=(0, "—"))  # noqa: E731
    cpu_pk, st_pk = peak("CPU admission refusals (k)"), peak("State-memory admission refusals (k)")
    md = hist["mdays"]
    html = (f'<h2 style="font-size:14px;font-weight:650;margin:30px 0 2px">Maintenance, {md[0]}…{md[-1]} — is the box working as hard as the backlog asks?</h2>\n'
            f'<p class="sub" style="margin-bottom:12px">monoscope OTel metrics (project 87576849), hourly · counters differenced per process with restarts handled · '
            f'{md[-1]} is a partial day</p>\n' + tiles(
        tile("CPU with ≥300 tasks due", f"{busy[len(busy) // 2]:.0f} / 48" if busy else "—", f"cores, median over {len(busy)} such hours",
             f"Peak hour <b>{busy[-1]:.0f} cores</b>. In <b>{sum(c < 30 for c in busy)} of {len(busy)}</b> backlogged hours the container used under 30 cores"
             if busy else "No hour in the window had 300 or more tasks due"),
        tile("Maintenance CPU tokens", f"{sorted(tok)[len(tok) // 2]:.0f} / {cap:.0f}" if tok else "—", "median hourly peak in use",
             f"At the ceiling in <b>{sum(v >= cap for v in tok)} of {len(tok)}</b> hours. A unit costs 1 token per 64 MiB decoded, so tokens count bytes in flight, not cores"),
        tile("Admission refusals, peak day", f"{cpu_pk[0]:,.0f} k", f"CPU on {cpu_pk[1]} · state memory {st_pk[0]:,.0f} k on {st_pk[1]}",
             "State-memory refusals are exported only since 09-28; before that the series is empty, not zero"),
        tile("Backlog, first → last day", f"{due[first] or 0:,.0f} → {due[-1] or 0:,.0f}", f"peak due tasks, {md[first]} → {md[-1]}",
             f"Base rollup <b>{base[first] or 0:,.0f} → {base[-1] or 0:,.0f}</b>, dedup <b>{dd[first] or 0:,.0f} → {dd[-1] or 0:,.0f}</b>. "
             "A restart re-inflates pending counts, so read the trend, not one day"),
    ))
    data = {"H0": hist["ts"][0] if hist["ts"] else 0, "hCores": se["cpu_cores"], "hTok": se["cpu_tokens_used"],
            "hDue": se["due_nonquarantined_tasks"], "hRef": se["admission_refused_cpu"],
            "hP99": [None if v is None else round(v / 1000, 2) for v in se["p99_ms"]], "mdays": md, "mrows": hist["rows"]}
    return data, html


def write_html(snap: dict, path: Path) -> None:
    """Regenerate every data block of the dashboard in place; prose outside the markers is left alone."""
    h = path.read_text()
    blocks, data = {}, {}
    if "delta" in snap:
        d, blocks["sub"], blocks["tiles"] = html_compaction(snap)
        data |= d
    if snap.get("stats"):
        blocks["live"] = html_live(snap)
    if "history" in snap:
        d, blocks["hist"] = html_history(snap["history"])
        data |= d
    for name, body in blocks.items():
        h, n = re.subn(rf"<!--gen:{name}-->.*?<!--/gen:{name}-->", lambda _: f"<!--gen:{name}-->{body}<!--/gen:{name}-->", h, flags=re.S)
        if n != 1:
            raise SystemExit(f"{path}: marker gen:{name} missing")
    # Keep blocks a failed source could not refresh; replace only what we have.
    old = dict(re.findall(r"^const (\w+(?:, SEALED)?)=(.*?);$", re.search(r"/\*gen:data\*/(.*?)/\*/gen:data\*/", h, re.S)[1], re.M))
    merged = {**{k: v for k, v in old.items()}, **{k: json.dumps(v) for k, v in data.items()}}
    js = "".join(f"const {k}={v};\n" for k, v in merged.items())
    h = re.sub(r"/\*gen:data\*/.*?/\*/gen:data\*/", lambda _: f"/*gen:data*/\n{js}/*/gen:data*/", h, flags=re.S)
    path.write_text(h)


def window_rates(s1: dict, s2: dict, secs: float) -> dict:
    out = {}
    for _, rows in SECTIONS:
        for _, keys, unit in rows:
            if unit.endswith("/h"):
                k = keys.split("|")[0]
                a, b = num(s1, k), num(s2, k)
                if a is not None and b is not None and b >= a:
                    out[keys] = (b - a) * 3600 / secs
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--json", action="store_true", help="emit the full snapshot as JSON")
    ap.add_argument("--days", type=int, default=8, help="dates shown for the main table (default 8)")
    ap.add_argument("--window", type=int, default=0, help="sample stats twice this many seconds apart for rates")
    ap.add_argument("--no-delta", action="store_true")
    ap.add_argument("--no-ssh", action="store_true")
    ap.add_argument("--all-tables", action="store_true", help="replay every table, not just main + rollups")
    ap.add_argument("--history", type=int, default=0, metavar="DAYS", help="hourly maintenance history from monoscope (the dashboard uses 8)")
    ap.add_argument("--html", type=Path, help="regenerate this dashboard's data blocks in place (implies --history 8)")
    args = ap.parse_args()
    args.history = args.history or (8 if args.html else 0)

    snap: dict = {"taken_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), "days": args.days}
    with ThreadPoolExecutor(4) as pool:
        jobs = {"stats": pool.submit(fetch_stats)}
        if not args.no_ssh:
            jobs["host"] = pool.submit(fetch_host)
        if not args.no_delta:
            jobs["delta"] = pool.submit(fetch_delta, max(args.days, CHART_DAYS if args.html else 0), args.all_tables)
        if args.history:
            jobs["history"] = pool.submit(fetch_history, args.history)
        if args.window:
            try:
                s1 = jobs["stats"].result()
                time.sleep(args.window)
                s2 = fetch_stats()
                snap["rates"], snap["window_s"], jobs["stats"] = window_rates(s1, s2, args.window), args.window, None
                snap["stats"] = s2
            except Exception as e:  # noqa: BLE001
                snap["stats_error"] = f"{type(e).__name__}: {e}"
        for name, job in jobs.items():
            if job is None:
                continue
            try:
                snap[name] = job.result()
            except Exception as e:  # noqa: BLE001 - report every source that failed, keep the rest
                snap[f"{name}_error"] = f"{type(e).__name__}: {str(e).strip()[:300]}"
    snap["flags"] = flags(snap.get("stats") or {}, snap.get("host"), snap.get("delta"))
    if args.html:
        write_html(snap, args.html)
    print(json.dumps(snap, indent=1, default=str) if args.json else render(snap))


if __name__ == "__main__":
    main()
