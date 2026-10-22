#!/usr/bin/env python3
"""Audit physical hash/body coverage from metadata, without reading events.

Requires boto3, deltalake, and pyarrow. Run with --project UUID --output report.json.
Credentials come from .env.prod by default and are never included in the report.
"""

import argparse
import collections
from concurrent.futures import ThreadPoolExecutor
import datetime
import json
from pathlib import Path
import re
import uuid

import boto3
from botocore.exceptions import ClientError
from deltalake import DeltaTable
import pyarrow as pa


def load_manifest(client, bucket, prefix):
    def get_json(key):
        try:
            return json.loads(client.get_object(Bucket=bucket, Key=key)["Body"].read())
        except ClientError as error:
            if error.response["Error"]["Code"] in ("NoSuchKey", "404", "NotFound"):
                return None
            raise

    root = get_json(f"{prefix}/shards/_root.json")
    if root is None:
        return get_json(f"{prefix}/manifest.json"), "legacy"
    entries = {}
    with ThreadPoolExecutor(max_workers=8) as pool:
        shards = pool.map(lambda date: get_json(f"{prefix}/shards/{date}.json"), sorted(root["shards"]))
        for shard in shards:
            if shard is not None:
                entries.update(shard["entries"])
    return {"version": root["version"], "entries": entries}, "sharded"


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
    manifest, manifest_format = load_manifest(client, bucket, f"index_manifests/{table_name}/{project}")
    if manifest is None or manifest.get("version") != 1:
        raise ValueError("Missing or unsupported manifest version")
    options = {key: env[key] for key in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_REGION")}
    options["AWS_ENDPOINT_URL"] = env["AWS_S3_ENDPOINT"]
    table = DeltaTable(table_uri, storage_options=options, skip_stats=True)

    # With this metadata-only load, file_uris(partition_filters=...) returned
    # other projects too. Filter explicitly and verify against add actions.
    live = {uri for uri in table.file_uris() if f"/project_id={project}/" in uri}
    adds = pa.table(table.get_add_actions(flatten=True)).select([
        "path", "size_bytes", "modification_time", "partition.project_id",
    ]).to_pylist()
    files = {
        f"{table_uri}/{row['path']}": row
        for row in adds if row["partition.project_id"] == project
    }
    if live != files.keys():
        raise ValueError("Project URI selection disagrees with Delta partition metadata")
    entries = manifest["entries"]
    usable = [
        entry for entry in entries.values()
        if entry.get("schema_version") == 1 and entry.get("index") and not entry.get("error")
    ]
    physical = [entry for entry in usable if entry.get("ordinals_valid") and len(entry.get("covered_files", [])) == 1]
    covered = {entry["covered_files"][0] for entry in physical if "hashes" in entry.get("element_fields", [])}
    body_covered = {entry["covered_files"][0] for entry in physical if "body" in entry.get("fields", [])}
    body_indexed = {uri for entry in usable if "body" in entry.get("fields", []) for uri in entry.get("covered_files", [])}
    days = collections.defaultdict(lambda: dict(
        live_files=0, physical_hash_candidates=0, missing_compressed_bytes=0,
        physical_body_candidates=0, body_physical_missing_compressed_bytes=0,
        body_index_candidates=0, body_uncovered_compressed_bytes=0, compressed_bytes=0,
    ))
    file_details = []
    for uri in sorted(live):
        match = re.search(r"/date=(\d{4}-\d{2}-\d{2})/", uri)
        if not match:
            raise ValueError("Project file lacks a date partition")
        day = days[match[1]]
        day["live_files"] += 1
        size = files[uri]["size_bytes"]
        day["compressed_bytes"] += size
        day["physical_hash_candidates"] += uri in covered
        day["physical_body_candidates"] += uri in body_covered
        day["body_index_candidates"] += uri in body_indexed
        if uri not in covered:
            day["missing_compressed_bytes"] += size
        if uri not in body_covered:
            day["body_physical_missing_compressed_bytes"] += size
        if uri not in body_indexed:
            day["body_uncovered_compressed_bytes"] += size
        file_details.append({
            "uri": uri, "date": match[1], "size_bytes": size,
            "modified_ms": files[uri]["modification_time"],
            "physical_body_candidate": uri in body_covered,
            "physical_hash_candidate": uri in covered,
            "body_index_candidate": uri in body_indexed,
        })
    at = datetime.datetime.now(datetime.timezone.utc)
    today = at.date().isoformat()
    for date, day in days.items():
        day["age_class"] = "future" if date > today else "today" if date == today else "sealed"
    return {
        "at": at.isoformat(), "manifest_format": manifest_format,
        "project": project, "delta_version": table.version(), "live_project_files": len(live),
        "manifest_entries": len(entries),
        "manifest_hash_entries": sum("hashes" in entry.get("element_fields", []) for entry in entries.values()),
        "by_date": dict(sorted(days.items())),
        "files": file_details,
        "limitations": "Delta and manifest read independently. Project URIs are cross-checked against add actions. Physical candidates require one-file URI/schema/ordinal metadata plus exact hash elements or a declared body field. Body index candidates also include carried coverage without physical ordinals. Index contents are not decoded. Compressed file bytes are not projected I/O, query latency, or build cost.",
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", type=uuid.UUID, required=True)
    parser.add_argument("--env-file", type=Path, default=Path(__file__).resolve().parents[1] / ".env.prod")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = audit(str(args.project), args.env_file)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({key: value for key, value in report.items() if key not in ("by_date", "files")}))
