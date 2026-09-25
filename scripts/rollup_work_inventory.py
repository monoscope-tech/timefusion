#!/usr/bin/env python3
"""Summarize rollup publication logs from stdin, without emitting tenant data.

Accepts tracing JSON or ANSI-colored text logs. Counts publication observations,
not unique tasks: repeated publications are real work and remain separate.
Recorded hash_shards take precedence. For older records, the fallback formula
matches run_coordinator_rollup_selected at baseline 08d34190.
Estimates describe successful scans only, not CPU, decoded bytes, or query use.
No production connection, payload persistence, or mutation is performed here.
"""

import argparse
import collections
import json
import re
import sys

ANSI = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
FIELD = re.compile(r'(\w+)=("(?:\\.|[^"\\])*"|[^\s]+)')
EVENT = "maintenance_rollup_published"
MAX_DECODED_BYTES = 512 * 1024 * 1024


def publication(line):
    """Return publication fields, ignoring other records and Docker prefixes."""
    text = ANSI.sub("", line).strip()
    if EVENT not in text:
        return None
    try:
        record = json.loads(text)
    except json.JSONDecodeError:
        fields = {}
        for name, value in FIELD.findall(text):
            fields[name] = json.loads(value) if value.startswith('"') else value
        return fields if fields.get("event") == EVENT else None
    if not isinstance(record, dict):
        return None
    fields = record.get("fields", record)
    return fields if isinstance(fields, dict) and fields.get("event") == EVENT else None


def nonnegative_integer(value):
    # Reject booleans and floats rather than truncating malformed measurements.
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise ValueError("expected a nonnegative integer")
    if isinstance(value, str) and not value.isdecimal():
        raise ValueError("expected a nonnegative integer")
    number = int(value)
    if number < 0:
        raise ValueError("expected a nonnegative integer")
    return number


def inventory(lines, max_decoded_bytes=MAX_DECODED_BYTES):
    if max_decoded_bytes <= 0:
        raise ValueError("max decoded bytes must be positive")
    groups = collections.defaultdict(collections.Counter)
    missing = observations = input_lines = 0
    for line in lines:
        input_lines += 1
        fields = publication(line)
        if fields is None:
            continue
        observations += 1
        table = fields.get("table")
        operation = fields.get("operation")
        try:
            size = nonnegative_integer(fields.get("estimated_decoded_bytes"))
            if not isinstance(table, str) or not table or operation not in ("BaseRollup", "DerivedRollup"):
                raise ValueError("missing publication identity")
            evidence = "recorded" if "hash_shards" in fields else "estimated"
            shards = (nonnegative_integer(fields["hash_shards"]) if evidence == "recorded"
                      else max(1, (size + max_decoded_bytes - 1) // max_decoded_bytes))
            if shards == 0:
                raise ValueError("recorded shard count must be positive")
        except ValueError:
            missing += 1
            continue
        counts = groups[table, operation, evidence, shards]
        counts["publications"] += 1
        counts["estimated_input_bytes"] += size
        counts["estimated_input_bytes_times_shards"] += size * shards
    bands = collections.defaultdict(collections.Counter)
    exposure = collections.defaultdict(collections.Counter)
    band_names = ("1", "2", "3–4", "5–8", ">8")
    for (_, _, evidence, shards), counts in groups.items():
        band = sum(shards > boundary for boundary in (1, 2, 4, 8))
        bands[evidence, band].update(counts)
        exposure[evidence].update(counts)
        exposure[evidence]["multishard_estimated_input_bytes"] += counts["estimated_input_bytes"] if shards > 1 else 0
    return {
        "input_lines": input_lines,
        "publication_observations": observations,
        "publications_missing_evidence": missing,
        "max_decoded_bytes": max_decoded_bytes,
        "scope": "successful publication observations only; input completeness is not established",
        "limitations": [
            "CPU attribution and query consumers are not measured",
            "input bytes and shard-weighted bytes are estimates, not physical read measurements",
            "recorded shard counts and baseline-formula estimates remain separate groups",
            "failed scans, in-flight work, metadata-only skips, and unlogged publications are excluded",
            "duplicate log delivery cannot be distinguished from repeated publications",
            "pass multiplier prices repeated projected input, not actual decode amplification or attainable CPU savings",
        ],
        "groups": [
            {"table": table, "operation": operation, "shard_evidence": evidence,
             "recorded_shards" if evidence == "recorded" else "inferred_shards": shards, **counts}
            for (table, operation, evidence, shards), counts in sorted(groups.items())
        ],
        "shard_bands": [
            {"shard_evidence": evidence, "shard_band": band_names[band], **counts}
            for (evidence, band), counts in sorted(bands.items())
        ],
        "exposure": [
            {"shard_evidence": evidence, **counts,
             "multishard_input_fraction": counts["multishard_estimated_input_bytes"] / size if size else None,
             "estimated_pass_multiplier": counts["estimated_input_bytes_times_shards"] / size if size else None}
            for evidence, counts in sorted(exposure.items())
            for size in [counts["estimated_input_bytes"]]
        ],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-decoded-bytes", type=int, default=MAX_DECODED_BYTES)
    args = parser.parse_args()
    if args.max_decoded_bytes <= 0:
        parser.error("--max-decoded-bytes must be positive")
    json.dump(inventory(sys.stdin, args.max_decoded_bytes), sys.stdout, indent=2)
    print()


if __name__ == "__main__":
    main()
