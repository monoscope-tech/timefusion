#!/usr/bin/env python3
"""Audit physical hash coverage from production metadata, without reading events.

Requires boto3, deltalake, and pyarrow. Run with --project UUID --output report.json.
Credentials come from .env.prod by default and are never included in the report.
"""

import argparse
import collections
import datetime
import json
from pathlib import Path
import re
import uuid

import boto3
from deltalake import DeltaTable
import pyarrow as pa


def audit(project, env_file):
    env = {}
    for line in env_file.read_text().splitlines():
        key, separator, value = line.partition("=")
        if separator and not key.lstrip().startswith("#"):
            env[key.strip()] = value.strip().strip('"').strip("'")
    bucket = env["AWS_S3_BUCKET"]
    table_name = "otel_logs_and_spans"
    table_uri = f"s3://{bucket}/timefusion/{table_name}"
    client = boto3.client(
        "s3", endpoint_url=env["AWS_S3_ENDPOINT"], region_name=env["AWS_REGION"],
        aws_access_key_id=env["AWS_ACCESS_KEY_ID"], aws_secret_access_key=env["AWS_SECRET_ACCESS_KEY"],
    )
    manifest = json.loads(client.get_object(
        Bucket=bucket, Key=f"index_manifests/{table_name}/{project}/manifest.json",
    )["Body"].read())
    if manifest.get("version") != 1:
        raise ValueError("Unsupported manifest version")
    options = {key: env[key] for key in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_REGION")}
    options["AWS_ENDPOINT_URL"] = env["AWS_S3_ENDPOINT"]
    table = DeltaTable(table_uri, storage_options=options, skip_stats=True)

    # With this metadata-only load, file_uris(partition_filters=...) returned
    # other projects too. Filter explicitly and verify against add actions.
    live = {uri for uri in table.file_uris() if f"/project_id={project}/" in uri}
    adds = pa.table(table.get_add_actions(flatten=True)).select([
        "path", "size_bytes", "partition.project_id",
    ]).to_pylist()
    sizes = {
        f"{table_uri}/{row['path']}": row["size_bytes"]
        for row in adds if row["partition.project_id"] == project
    }
    if live != sizes.keys():
        raise ValueError("Project URI selection disagrees with Delta partition metadata")
    entries = manifest["entries"]
    covered = {
        entry["covered_files"][0] for entry in entries.values()
        if entry.get("schema_version") == 1 and entry.get("index") and not entry.get("error")
        and entry.get("ordinals_valid") and "hashes" in entry.get("element_fields", [])
        and len(entry.get("covered_files", [])) == 1
    }
    days = collections.defaultdict(lambda: dict(live_files=0, physical_hash_candidates=0, missing_compressed_bytes=0))
    for uri in live:
        match = re.search(r"/date=(\d{4}-\d{2}-\d{2})/", uri)
        if not match:
            raise ValueError("Project file lacks a date partition")
        day = days[match[1]]
        day["live_files"] += 1
        day["physical_hash_candidates"] += uri in covered
        if uri not in covered:
            day["missing_compressed_bytes"] += sizes[uri]
    return {
        "at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "project": project, "delta_version": table.version(), "live_project_files": len(live),
        "manifest_entries": len(entries),
        "manifest_hash_entries": sum("hashes" in entry.get("element_fields", []) for entry in entries.values()),
        "by_date": dict(sorted(days.items())),
        "limitations": "Delta and manifest read independently. Project URI selection cross-checked against add actions. Exact one-file URI/schema/ordinal/element metadata checked; index contents not decoded. Compressed file bytes are not projected I/O or build cost.",
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", type=uuid.UUID, required=True)
    parser.add_argument("--env-file", type=Path, default=Path(__file__).resolve().parents[1] / ".env.prod")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = audit(str(args.project), args.env_file)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({key: value for key, value in report.items() if key != "by_date"}))
