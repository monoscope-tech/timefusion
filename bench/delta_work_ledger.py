#!/usr/bin/env python3
"""Build a bounded physical-write ledger from Delta JSON commits.

Unlike a sum of every ``add.stats.numRecords``, this ledger does not count a
same-path deletion-vector update as a Parquet rewrite.  It reports the new DV
sidecar separately and attributes work with ``timefusion.lane``.

Examples:

    bench/delta_work_ledger.py s3://bucket/table --last 5000
    bench/delta_work_ledger.py file:///tmp/table --from-version 120 --to-version 180
    bench/delta_work_ledger.py s3://bucket/table --last 10000 \
        --from-timestamp 2026-09-17T02:02:50Z --group-by lane,project,date --json

S3 requires the standard AWS credentials plus AWS_S3_ENDPOINT or
AWS_ENDPOINT_URL for an S3-compatible service.  A bound is mandatory so an
investigation cannot accidentally download the table's complete log history.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable
from urllib.parse import urlparse


LANE_KEY = "timefusion.lane"


@dataclass(frozen=True)
class LogObject:
    version: int
    name: str
    read: Callable[[], str]


def delta_version(name: str) -> int | None:
    leaf = name.rsplit("/", 1)[-1]
    if not leaf.endswith(".json") or not leaf[:-5].isdigit():
        return None
    return int(leaf[:-5])


def iso_timestamp(value: str) -> int:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1000)


def commit_actions(text: str) -> tuple[dict, list[dict], list[dict]]:
    info: dict = {}
    adds: list[dict] = []
    removes: list[dict] = []
    for line in text.splitlines():
        if not line.strip():
            continue
        action = json.loads(line)
        if "commitInfo" in action:
            info = action["commitInfo"]
        elif "add" in action:
            adds.append(action["add"])
        elif "remove" in action:
            removes.append(action["remove"])
    return info, adds, removes


def stats_rows(add: dict) -> int:
    stats = add_stats(add)
    return int(stats.get("numRecords") or 0)


def add_stats(add: dict) -> dict:
    stats = add.get("stats") or {}
    if isinstance(stats, str):
        try:
            stats = json.loads(stats)
        except json.JSONDecodeError:
            return {}
    return stats


def dv_identity(action: dict) -> tuple | None:
    dv = action.get("deletionVector")
    if not dv:
        return None
    return (
        dv.get("storageType"),
        dv.get("pathOrInlineDv"),
        dv.get("offset"),
        dv.get("sizeInBytes"),
        dv.get("cardinality"),
    )


def dimensions(info: dict, action: dict, fields: tuple[str, ...]) -> tuple[str, ...]:
    partitions = action.get("partitionValues") or {}
    values = {
        "lane": lane_name(info),
        "operation": str(info.get("operation") or "unknown"),
        "project": str(partitions.get("project_id") or "unknown"),
        "date": str(partitions.get("date") or "unknown"),
    }
    return tuple(values[field] for field in fields)


def lane_name(info: dict) -> str:
    lane = info.get(LANE_KEY)
    if not lane and ("timefusion.wal_watermark" in info or "timefusion.landed_digests" in info):
        lane = "flush_commit"
    return str(lane or f"unattributed:{info.get('operation') or 'unknown'}")


def event_micros(value) -> int | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1_000_000)


def pack_trace(commits: Iterable[tuple[int, str]], start_ms: int | None = None, end_ms: int | None = None) -> list[dict]:
    trace = []
    for version, text in commits:
        info, adds, _ = commit_actions(text)
        timestamp = info.get("timestamp")
        if timestamp is None or lane_name(info) != "flush_commit":
            continue
        timestamp = int(timestamp)
        if start_ms is not None and timestamp < start_ms:
            continue
        if end_ms is not None and timestamp >= end_ms:
            continue
        for index, add in enumerate(adds):
            stats = add_stats(add)
            lo = event_micros((stats.get("minValues") or {}).get("timestamp"))
            hi = event_micros((stats.get("maxValues") or {}).get("timestamp"))
            if lo is None or hi is None:
                continue
            partitions = add.get("partitionValues") or {}
            project = str(partitions.get("project_id") or "unknown")
            tags = add.get("tags") or {}
            trace.append(
                {
                    "commit_ms": timestamp,
                    "project": hashlib.sha256(project.encode()).hexdigest()[:16],
                    "date": str(partitions.get("date") or "unknown"),
                    "path": f"v{version}-a{index}",
                    "size": int(add.get("size") or 0),
                    "rows": int(stats.get("numRecords") or 0),
                    "min_event": lo,
                    "max_event": hi,
                    "sorted": tags.get("delta-rs.optimize.sort_by") == "true",
                    "has_dv": bool(add.get("deletionVector")),
                }
            )
    return sorted(trace, key=lambda row: (row["commit_ms"], row["path"]))


def empty_totals() -> dict[str, int | set[int]]:
    return {
        "versions": set(),
        "add_actions": 0,
        "remove_actions": 0,
        "parquet_files_written": 0,
        "parquet_rows_written": 0,
        "parquet_bytes_written": 0,
        "parquet_files_removed": 0,
        "parquet_bytes_removed": 0,
        "same_path_readds": 0,
        "dv_sidecars_written": 0,
        "dv_bytes_written": 0,
        "dv_sidecars_removed": 0,
        "dv_bytes_removed": 0,
    }


def add_amount(group: dict, key: str, value: int = 1) -> None:
    group[key] = int(group[key]) + value


def ledger(commits: Iterable[tuple[int, str]], fields: tuple[str, ...], start_ms: int | None = None, end_ms: int | None = None) -> list[dict]:
    groups: dict[tuple[str, ...], dict] = defaultdict(empty_totals)
    for version, text in commits:
        info, adds, removes = commit_actions(text)
        timestamp = info.get("timestamp")
        if start_ms is not None and (timestamp is None or int(timestamp) < start_ms):
            continue
        if end_ms is not None and (timestamp is None or int(timestamp) >= end_ms):
            continue

        removed_by_path = {remove.get("path"): remove for remove in removes}
        added_paths = {add.get("path") for add in adds}

        for add in adds:
            group = groups[dimensions(info, add, fields)]
            group["versions"].add(version)
            add_amount(group, "add_actions")
            old = removed_by_path.get(add.get("path"))
            new_dv = dv_identity(add)
            old_dv = dv_identity(old or {})
            if old is not None:
                add_amount(group, "same_path_readds")
            else:
                add_amount(group, "parquet_files_written")
                add_amount(group, "parquet_rows_written", stats_rows(add))
                add_amount(group, "parquet_bytes_written", int(add.get("size") or 0))
            if new_dv is not None and new_dv != old_dv:
                add_amount(group, "dv_sidecars_written")
                add_amount(group, "dv_bytes_written", int((add.get("deletionVector") or {}).get("sizeInBytes") or 0))

        for remove in removes:
            group = groups[dimensions(info, remove, fields)]
            group["versions"].add(version)
            add_amount(group, "remove_actions")
            if remove.get("path") not in added_paths:
                add_amount(group, "parquet_files_removed")
                add_amount(group, "parquet_bytes_removed", int(remove.get("size") or 0))
            old_dv = dv_identity(remove)
            replacement = next((add for add in adds if add.get("path") == remove.get("path")), None)
            if old_dv is not None and old_dv != dv_identity(replacement or {}):
                add_amount(group, "dv_sidecars_removed")
                add_amount(group, "dv_bytes_removed", int((remove.get("deletionVector") or {}).get("sizeInBytes") or 0))

        if not adds and not removes:
            group = groups[dimensions(info, {}, fields)]
            group["versions"].add(version)

    result = []
    for key, totals in sorted(groups.items()):
        record = dict(zip(fields, key, strict=True))
        record["commits"] = len(totals.pop("versions"))
        record.update(totals)
        result.append(record)
    return result


def local_logs(path: Path) -> list[LogObject]:
    directory = path / "_delta_log"
    if not directory.is_dir():
        raise SystemExit(f"no _delta_log at {directory}")
    objects = []
    for entry in directory.iterdir():
        version = delta_version(entry.name)
        if version is not None:
            objects.append(LogObject(version, str(entry), entry.read_text))
    return sorted(objects, key=lambda item: item.version)


def s3_logs(bucket: str, prefix: str) -> list[LogObject]:
    import boto3
    from botocore.config import Config

    endpoint = os.environ.get("AWS_S3_ENDPOINT") or os.environ.get("AWS_ENDPOINT_URL")
    client = boto3.client("s3", endpoint_url=endpoint, config=Config(s3={"addressing_style": "path"}))
    log_prefix = f"{prefix.rstrip('/')}/_delta_log/"
    objects = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=log_prefix):
        for item in page.get("Contents", []):
            key = item["Key"]
            version = delta_version(key)
            if version is None:
                continue
            objects.append(
                LogObject(
                    version,
                    key,
                    lambda key=key: client.get_object(Bucket=bucket, Key=key)["Body"].read().decode(),
                )
            )
    return sorted(objects, key=lambda item: item.version)


def bounded(objects: list[LogObject], args: argparse.Namespace) -> list[LogObject]:
    selected = [
        item
        for item in objects
        if (args.from_version is None or item.version >= args.from_version)
        and (args.to_version is None or item.version <= args.to_version)
    ]
    if args.last is not None:
        selected = selected[-args.last :]
    return selected


def read_commits(objects: list[LogObject], workers: int) -> list[tuple[int, str]]:
    if workers == 1:
        return [(item.version, item.read()) for item in objects]
    with ThreadPoolExecutor(max_workers=workers) as pool:
        texts = pool.map(lambda item: item.read(), objects)
        return [(item.version, text) for item, text in zip(objects, texts, strict=True)]


def human_bytes(value: int) -> str:
    amount = float(value)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if amount < 1024 or unit == "TiB":
            return f"{amount:.2f} {unit}"
        amount /= 1024
    raise AssertionError("unreachable")


def print_table(records: list[dict], fields: tuple[str, ...]) -> None:
    metrics = (
        "commits",
        "parquet_files_written",
        "parquet_rows_written",
        "parquet_bytes_written",
        "same_path_readds",
        "dv_sidecars_written",
        "dv_bytes_written",
    )
    headings = (*fields, *metrics)
    rows = []
    for record in records:
        rows.append(
            [
                *(str(record[field]) for field in fields),
                *(human_bytes(record[name]) if name.endswith("bytes_written") else str(record[name]) for name in metrics),
            ]
        )
    widths = [max(len(headings[i]), *(len(row[i]) for row in rows)) for i in range(len(headings))]
    print("  ".join(headings[i].ljust(widths[i]) for i in range(len(headings))))
    print("  ".join("-" * width for width in widths))
    for row in rows:
        print("  ".join(value.ljust(widths[i]) for i, value in enumerate(row)))


def matching_window(commits: list[tuple[int, str]], start_ms: int | None, end_ms: int | None) -> dict:
    matched = []
    for version, text in commits:
        info, _, _ = commit_actions(text)
        timestamp = info.get("timestamp")
        if timestamp is None:
            continue
        timestamp = int(timestamp)
        if start_ms is not None and timestamp < start_ms:
            continue
        if end_ms is not None and timestamp >= end_ms:
            continue
        matched.append((version, timestamp))
    if not matched:
        return {"commits": 0}
    return {
        "commits": len(matched),
        "first_version": min(version for version, _ in matched),
        "last_version": max(version for version, _ in matched),
        "first_timestamp": datetime.fromtimestamp(min(timestamp for _, timestamp in matched) / 1000, timezone.utc).isoformat(),
        "last_timestamp": datetime.fromtimestamp(max(timestamp for _, timestamp in matched) / 1000, timezone.utc).isoformat(),
    }


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("target", help="s3://bucket/table or file:///absolute/table")
    parser.add_argument("--last", type=int, help="read only the newest N JSON commits")
    parser.add_argument("--from-version", type=int)
    parser.add_argument("--to-version", type=int)
    parser.add_argument("--from-timestamp", help="inclusive ISO-8601 commit timestamp")
    parser.add_argument("--to-timestamp", help="exclusive ISO-8601 commit timestamp")
    parser.add_argument("--group-by", default="lane", help="comma-separated lane,operation,project,date")
    parser.add_argument("--json", action="store_true", help="emit machine-readable records")
    parser.add_argument("--pack-trace", action="store_true", help="emit sanitized flush Add events as JSONL for the ignored pack replay")
    parser.add_argument("--workers", type=int, default=16, help="parallel log reads (default: 16)")
    parser.add_argument("--all", action="store_true", help="explicitly permit the complete JSON log history")
    args = parser.parse_args()
    if not args.all and args.last is None and args.from_version is None:
        parser.error("choose --last or --from-version (or explicitly use --all)")
    if args.last is not None and args.last <= 0:
        parser.error("--last must be positive")
    if args.workers <= 0:
        parser.error("--workers must be positive")
    fields = tuple(part.strip() for part in args.group_by.split(",") if part.strip())
    allowed = {"lane", "operation", "project", "date"}
    if not fields or set(fields) - allowed:
        parser.error("--group-by fields must be lane, operation, project, or date")
    args.group_fields = fields
    return args


def main() -> None:
    args = arguments()
    target = urlparse(args.target)
    if target.scheme == "s3":
        objects = s3_logs(target.netloc, target.path.lstrip("/"))
    elif target.scheme in ("file", ""):
        objects = local_logs(Path(target.path or args.target))
    else:
        raise SystemExit(f"unsupported scheme: {target.scheme}")
    objects = bounded(objects, args)
    commits = read_commits(objects, args.workers)
    start_ms = iso_timestamp(args.from_timestamp) if args.from_timestamp else None
    end_ms = iso_timestamp(args.to_timestamp) if args.to_timestamp else None
    records = ledger(
        commits,
        args.group_fields,
        start_ms,
        end_ms,
    )
    window = matching_window(commits, start_ms, end_ms)
    if args.pack_trace:
        for record in pack_trace(commits, start_ms, end_ms):
            print(json.dumps(record, separators=(",", ":"), sort_keys=True))
    elif args.json:
        print(json.dumps({"logs_read": len(objects), "window": window, "records": records}, indent=2, sort_keys=True))
    elif records:
        print_table(records, args.group_fields)
        print(f"\nlogs read: {len(objects)}; commits matched: {window['commits']}")
    else:
        print(f"no matching commits (logs read: {len(objects)})")


if __name__ == "__main__":
    main()
