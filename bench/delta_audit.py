#!/usr/bin/env python3
"""
Audit a Delta table's _delta_log to attribute Add/Remove actions per commit.

Background: "delta-rs checkpoint silently tombstones files" can be alarming
when the checkpoint shows many Remove records but no DELETE/UPDATE was issued.
In TF the usual culprit is light-optimize compactions, which legitimately
emit Remove (old small file) + Add (new compacted file). This script makes
that provenance visible — every Remove gets attributed to a specific commit
operation, so anything truly silent stands out.

Usage:
    bench/delta_audit.py s3://bucket/path/to/table
    bench/delta_audit.py file:///abs/path/to/local/table

Requires env: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_S3_ENDPOINT (for S3).
"""
import json
import os
import sys
from collections import Counter
from urllib.parse import urlparse

import boto3
from botocore.config import Config


def parse_commit(text):
    adds = removes = 0
    op = parameters = engine_info = None
    for line in text.splitlines():
        if not line.strip():
            continue
        rec = json.loads(line)
        if "add" in rec:
            adds += 1
        elif "remove" in rec:
            removes += 1
        elif "commitInfo" in rec:
            ci = rec["commitInfo"]
            op = ci.get("operation")
            parameters = ci.get("operationParameters") or {}
            engine_info = ci.get("engineInfo")
    return op, parameters, engine_info, adds, removes


def audit_s3(bucket, prefix):
    cfg = Config(s3={"addressing_style": "path"})
    endpoint = os.environ.get("AWS_S3_ENDPOINT") or os.environ.get("AWS_ENDPOINT_URL")
    s3 = boto3.client("s3", endpoint_url=endpoint, config=cfg)
    log_prefix = f"{prefix.rstrip('/')}/_delta_log/"
    paginator = s3.get_paginator("list_objects_v2")
    commits = []
    for page in paginator.paginate(Bucket=bucket, Prefix=log_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".json"):
                commits.append(key)
    commits.sort()
    return [(c, s3.get_object(Bucket=bucket, Key=c)["Body"].read().decode()) for c in commits]


def audit_local(path):
    log = os.path.join(path, "_delta_log")
    if not os.path.isdir(log):
        sys.exit(f"no _delta_log at {log}")
    out = []
    for name in sorted(os.listdir(log)):
        if name.endswith(".json"):
            with open(os.path.join(log, name)) as f:
                out.append((name, f.read()))
    return out


def main():
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    target = sys.argv[1]
    u = urlparse(target)
    if u.scheme == "s3":
        commits = audit_s3(u.netloc, u.path.lstrip("/"))
    elif u.scheme in ("file", ""):
        commits = audit_local(u.path or target)
    else:
        sys.exit(f"unsupported scheme: {u.scheme}")

    print(f"{'version':>7}  {'op':<18}  {'add':>5}  {'rem':>5}  details")
    print(f"{'-' * 70}")
    total_add = total_rem = silent_rem = 0
    op_totals = Counter()
    for key, text in commits:
        version = int(os.path.basename(key).split(".")[0])
        op, params, engine, adds, removes = parse_commit(text)
        op_label = op or "?"
        # A Remove without an attributable operation (or a WRITE op claiming
        # Remove records) is the "silent" case we want to flag.
        silent = (removes > 0 and op_label in {"?", "WRITE", "MERGE"} and adds <= removes)
        flag = " ← silent" if silent else ""
        if silent:
            silent_rem += removes
        total_add += adds
        total_rem += removes
        op_totals[op_label] += 1
        detail = ""
        if params:
            interesting = {k: v for k, v in params.items() if k in ("predicate", "target_size", "zOrderBy")}
            if interesting:
                detail = json.dumps(interesting, separators=(",", ":"))
        print(f"{version:>7}  {op_label:<18}  {adds:>5}  {removes:>5}  {detail}{flag}")

    print()
    print(f"totals: add={total_add}  remove={total_rem}  silent_remove={silent_rem}")
    print(f"ops: {dict(op_totals)}")
    if silent_rem > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
