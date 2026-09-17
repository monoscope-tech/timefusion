#!/usr/bin/env python3
"""Run a bounded cold/warm latency and rollup-routing matrix.

The manifest contains literal client SELECT shapes with psycopg named
parameters: ``%(project_id)s``, ``%(start)s``, and ``%(end)s``.  The runner
records the physical plan, result digest, process identity, latency, and all
numeric ``timefusion_stats`` deltas.  An optional second DSN is the local raw
oracle; production runs should omit it rather than disabling rollups or
purging caches on the live service.

The ladder stops for a shape after an error, a process/image change, a result
mismatch, a statement deadline, a 7d/30d rollup miss, or physical reads above
the configured budget.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import time
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable

import psycopg


WINDOWS = {"1h": timedelta(hours=1), "1d": timedelta(days=1), "7d": timedelta(days=7), "30d": timedelta(days=30)}
LONG_WINDOWS = {"7d", "30d"}
READ_PREFIXES = ("select", "with")
MUTATING_TOKENS = re.compile(r"\b(insert|update|delete|merge|drop|alter|truncate|copy|call|create|replace|grant|revoke)\b", re.IGNORECASE)
DEPLOYMENT_MARKERS = ("57p03", "draining for deployment", "database is starting up")


@dataclass
class Run:
    query: str
    window: str
    period: str
    iteration: str
    start: str
    end: str
    executed_at: str | None = None
    process_age_seconds: float | None = None
    latency_ms: float | None = None
    rows: int | None = None
    result_sha256: str | None = None
    oracle_sha256: str | None = None
    result_equal: bool | None = None
    oracle_comparable: bool = False
    planned_route: str | None = None
    oracle_planned_route: str | None = None
    ambient_miss_reasons: list[str] | None = None
    ambient_physical_bytes_read_upper_bound: int | None = None
    plan: list[str] | None = None
    oracle_plan: list[str] | None = None
    stats_delta: dict[str, int | float] | None = None
    error: str | None = None
    stop: str | None = None


def validate_sql(sql: str) -> str:
    normalized = sql.strip()
    first = re.match(r"[A-Za-z]+", normalized)
    if not first or first.group(0).lower() not in READ_PREFIXES:
        raise ValueError("matrix queries must be SELECT or WITH statements")
    if ";" in normalized.rstrip(";"):
        raise ValueError("matrix queries must contain exactly one statement")
    # Manifests are reviewed files, but enforce a second guard before a query is
    # ever pointed at production. Remove quoted literals before checking so a
    # chart label such as 'update' is not mistaken for a statement.
    tokens = re.sub(r"'(?:''|[^'])*'", "''", normalized)
    if MUTATING_TOKENS.search(tokens):
        raise ValueError("matrix queries must be read-only")
    return normalized.rstrip(";")


def json_value(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return "0" if value == 0 else str(value.normalize())
    if isinstance(value, bytes):
        return {"bytes_hex": value.hex()}
    if isinstance(value, tuple):
        return [json_value(item) for item in value]
    if isinstance(value, list):
        return [json_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): json_value(item) for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))}
    return value


def result_digest(rows: Iterable[tuple[Any, ...]], ordered: bool = True) -> str:
    normalized = json_value(list(rows))
    if not ordered:
        normalized.sort(key=lambda row: json.dumps(row, sort_keys=True, separators=(",", ":"), ensure_ascii=False))
    payload = json.dumps(normalized, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode()).hexdigest()


def numeric(value: str) -> int | float | None:
    try:
        return int(value)
    except ValueError:
        try:
            return float(value)
        except ValueError:
            return None


def stats_snapshot(cursor: psycopg.Cursor[Any]) -> dict[str, str]:
    cursor.execute("SELECT component, key, value FROM timefusion_stats")
    return {f"{component}.{key}": str(value) for component, key, value in cursor.fetchall()}


def stats_delta(before: dict[str, str], after: dict[str, str]) -> dict[str, int | float]:
    delta: dict[str, int | float] = {}
    for key in before.keys() & after.keys():
        first, last = numeric(before[key]), numeric(after[key])
        if first is not None and last is not None and last != first:
            delta[key] = last - first
    return dict(sorted(delta.items()))


def ambient_miss_reasons(delta: dict[str, int | float]) -> list[str]:
    prefix = "maintenance.rollup_"
    return sorted(
        key.removeprefix(f"{prefix}miss_").removesuffix("_total")
        for key, value in delta.items()
        if key.startswith(f"{prefix}miss_") and key != f"{prefix}misses_total" and value > 0
    )


def planned_route(plan: Iterable[str]) -> str:
    joined = " ".join(plan).lower()
    rollup = bool(re.search(r"\botel_(?:logs_and_spans|metrics)_rollup_[a-z0-9_]+\b", joined))
    raw = bool(re.search(r"\botel_(?:logs_and_spans|metrics)\b", joined))
    if rollup and raw:
        return "hybrid"
    if rollup:
        return "full"
    if raw:
        return "raw"
    return "unknown"


def ambient_physical_bytes(delta: dict[str, int | float]) -> int | None:
    value = delta.get("parquet.bytes_read")
    return None if value is None else max(0, int(value))


def window_bounds(now: datetime, window: str, period: str, sealed_lag_days: int) -> tuple[datetime, datetime]:
    end = now if period == "today" else datetime.combine((now - timedelta(days=sealed_lag_days)).date(), datetime.min.time(), timezone.utc)
    return end - WINDOWS[window], end


def stop_reason(run: Run, scan_budget: int) -> str | None:
    if run.error:
        return "query_error"
    if run.result_equal is False:
        return "oracle_mismatch"
    if run.ambient_physical_bytes_read_upper_bound is not None and run.ambient_physical_bytes_read_upper_bound > scan_budget:
        return "ambient_scan_budget_exceeded"
    if run.window in LONG_WINDOWS and run.planned_route not in {"full", "hybrid"}:
        return "long_window_route_unproven"
    return None


def explain(cursor: psycopg.Cursor[Any], sql: str, params: dict[str, Any]) -> list[str]:
    cursor.execute("EXPLAIN " + sql, params)
    return [str(row[0]) for row in cursor.fetchall()]


def execute_once(
    cursor: psycopg.Cursor[Any], oracle: psycopg.Cursor[Any] | None, query: dict[str, Any], window: str, period: str,
    iteration: str, start: datetime, end: datetime, expected_boot: str, scan_budget: int,
) -> Run:
    executed_at = datetime.now(timezone.utc)
    run = Run(
        query=query["name"], window=window, period=period, iteration=iteration, start=start.isoformat(), end=end.isoformat(),
        executed_at=executed_at.isoformat(), process_age_seconds=max(0, executed_at.timestamp() - int(expected_boot) / 1_000_000),
    )
    params = {**query.get("params", {}), "project_id": query["project_id"], "start": start, "end": end}
    try:
        sql = validate_sql(query["sql"])
        plan = explain(cursor, sql, params)
        before = stats_snapshot(cursor)
        if before.get("buffered_layer.boot_micros") != expected_boot:
            raise RuntimeError("TimeFusion boot changed before query")
        began = time.perf_counter()
        cursor.execute(sql, params)
        rows = cursor.fetchall()
        run.latency_ms = round((time.perf_counter() - began) * 1000, 3)
        after = stats_snapshot(cursor)
        if after.get("buffered_layer.boot_micros") != expected_boot:
            raise RuntimeError("TimeFusion boot changed during query")
        delta = stats_delta(before, after)
        ordered = query.get("comparison", "ordered") == "ordered"
        run.rows, run.result_sha256 = len(rows), result_digest(rows, ordered)
        run.plan, run.stats_delta = plan, delta
        run.planned_route = planned_route(plan)
        run.ambient_miss_reasons = ambient_miss_reasons(delta)
        run.ambient_physical_bytes_read_upper_bound = ambient_physical_bytes(delta)
        if oracle is not None:
            run.oracle_plan = explain(oracle, sql, params)
            run.oracle_planned_route = planned_route(run.oracle_plan)
            oracle.execute(sql, params)
            run.oracle_sha256 = result_digest(oracle.fetchall(), ordered)
            run.oracle_comparable = run.planned_route in {"full", "hybrid"} and run.oracle_planned_route == "raw"
            if run.oracle_comparable:
                run.result_equal = run.result_sha256 == run.oracle_sha256
        run.stop = stop_reason(run, scan_budget)
    except Exception as error:  # Preserve the partial matrix and stop this shape.
        message = f"{type(error).__name__}: {error}"
        run.error = message
        if "boot changed" in message.lower():
            run.stop = "identity_changed"
        elif any(marker in message.lower() for marker in DEPLOYMENT_MARKERS):
            run.stop = "deployment_response"
        else:
            run.stop = "query_error"
    return run


def load_manifest(path: Path) -> dict[str, Any]:
    manifest = json.loads(path.read_text())
    if not isinstance(manifest.get("queries"), list) or not manifest["queries"]:
        raise ValueError("manifest must contain a non-empty queries array")
    names: set[str] = set()
    for query in manifest["queries"]:
        if not isinstance(query, dict) or not isinstance(query.get("name"), str) or not isinstance(query.get("sql"), str):
            raise ValueError("each query requires string name and sql fields")
        validate_sql(query["sql"])
        if query["name"] in names:
            raise ValueError(f"duplicate query name: {query['name']}")
        names.add(query["name"])
        if not query.get("project_id"):
            raise ValueError(f"{query['name']}: project_id is required")
        windows = query.get("windows", list(WINDOWS))
        periods = query.get("periods", ["today", "sealed"])
        if not windows or not periods:
            raise ValueError(f"{query['name']}: windows and periods must be non-empty")
        unknown = set(windows) - WINDOWS.keys()
        if unknown:
            raise ValueError(f"{query['name']}: unknown windows {sorted(unknown)}")
        unknown_periods = set(periods) - {"today", "sealed"}
        if unknown_periods:
            raise ValueError(f"{query['name']}: unknown periods {sorted(unknown_periods)}")
        if query.get("comparison", "ordered") not in {"ordered", "multiset"}:
            raise ValueError(f"{query['name']}: comparison must be ordered or multiset")
    return manifest


def write_report(path: Path, report: dict[str, Any]) -> None:
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--dsn", required=True, help="routed TimeFusion PostgreSQL DSN")
    parser.add_argument("--oracle-dsn", help="local raw-path TimeFusion DSN with identical seeded data")
    parser.add_argument("--image", required=True, help="immutable image digest or commit under test")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--deadline-seconds", type=int, default=10)
    parser.add_argument("--scan-budget-bytes", type=int, default=1 << 30)
    parser.add_argument("--sealed-lag-days", type=int, default=2)
    args = parser.parse_args()
    if args.deadline_seconds <= 0 or args.scan_budget_bytes <= 0 or args.sealed_lag_days < 1:
        parser.error("deadline and scan budget must be positive; sealed lag must be at least one day")

    manifest = load_manifest(args.manifest)
    report: dict[str, Any] = {"expected_image": args.image, "captured_at": datetime.now(timezone.utc).isoformat(), "runs": []}
    with psycopg.connect(args.dsn, autocommit=True) as connection, connection.cursor() as cursor:
        cursor.execute(f"SET statement_timeout = '{args.deadline_seconds}s'")
        initial = stats_snapshot(cursor)
        cursor.execute("SELECT version()")
        report["server_version"] = str(cursor.fetchone()[0])
        boot = initial.get("buffered_layer.boot_micros")
        if not boot:
            raise SystemExit("timefusion_stats did not expose buffered_layer.boot_micros")
        report.update(boot_micros=boot, process_age_seconds=max(0, time.time() - int(boot) / 1_000_000))
        oracle_connection = psycopg.connect(args.oracle_dsn, autocommit=True) if args.oracle_dsn else None
        try:
            oracle = oracle_connection.cursor() if oracle_connection else None
            if oracle:
                oracle.execute(f"SET statement_timeout = '{args.deadline_seconds}s'")
                oracle.execute("SELECT version()")
                report["oracle_server_version"] = str(oracle.fetchone()[0])
            now = datetime.now(timezone.utc)
            abort_matrix = False
            for query in manifest["queries"]:
                if abort_matrix:
                    break
                stopped = False
                for window in query.get("windows", list(WINDOWS)):
                    if stopped:
                        break
                    for period in query.get("periods", ["today", "sealed"]):
                        start, end = window_bounds(now, window, period, args.sealed_lag_days)
                        for iteration in ("first", "warm"):
                            run = execute_once(cursor, oracle, query, window, period, iteration, start, end, boot, args.scan_budget_bytes)
                            report["runs"].append(asdict(run))
                            write_report(args.output, report)
                            if run.stop in {"deployment_response", "identity_changed"}:
                                abort_matrix = True
                            if run.stop:
                                stopped = True
                                break
                        if stopped:
                            break
        finally:
            if oracle_connection:
                oracle_connection.close()
    write_report(args.output, report)
    print(f"wrote {len(report['runs'])} runs to {args.output}")


if __name__ == "__main__":
    main()
