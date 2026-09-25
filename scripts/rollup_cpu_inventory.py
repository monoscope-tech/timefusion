#!/usr/bin/env python3
"""Summarize inclusive sample coverage in Inferno SVGs, not CPU-seconds.

Categories overlap. Their shares must not be added. Detached async execution
can omit its caller's frame, so function names do not prove complete lane share.
"""

import argparse
import hashlib
import json
from pathlib import Path
import xml.etree.ElementTree as ET


SVG = "{http://www.w3.org/2000/svg}"
FG = "{http://github.com/jonhoo/inferno}"
CATEGORIES = {
    "rollup_caller": "run_coordinator_rollup_selected",
    "dedup_caller": "run_coordinator_dedup_once",
    "partition_stats": "partition_stats_bounded",
    "sort_batch_assembly": "datafusion_physical_plan::sorts::builder::BatchBuilder::build_record_batch",
    "sort_operators": "datafusion_physical_plan::sorts::",
    "aggregate_operators": "datafusion_physical_plan::aggregates::",
    "parquet": "parquet::",
    "maintenance_threads": "maintenance-wor",
}


def covered_samples(intervals):
    end = count = 0
    for lo, hi in sorted(intervals):
        count += max(0, hi - max(lo, end))
        end = max(end, hi)
    return count


def summarize(data):
    root = ET.fromstring(data)
    frames = []
    for group in root.iter(SVG + "g"):
        title, rect = group.find(SVG + "title"), group.find(SVG + "rect")
        if title is None or rect is None:
            continue
        name = (title.text or "").rsplit(" (", 1)[0]
        lo, width = int(rect.attrib[FG + "x"]), int(rect.attrib[FG + "w"])
        if lo < 0 or width < 0:
            raise ValueError("negative sample interval")
        frames.append((name, lo, lo + width))
    roots = [(lo, hi) for name, lo, hi in frames if name == "all"]
    if len(roots) != 1 or roots[0][0] != 0 or roots[0][1] <= 0:
        raise ValueError("expected one nonempty all-samples root")
    total = roots[0][1]
    if any(hi > total for _, _, hi in frames):
        raise ValueError("frame exceeds all-samples root")
    return {
        "samples": total,
        "inclusive_samples": {
            category: covered_samples((lo, hi) for name, lo, hi in frames if pattern in name)
            for category, pattern in CATEGORIES.items()
        },
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("profiles", type=Path, nargs="+")
    args = parser.parse_args()
    profiles = []
    for path in args.profiles:
        data = path.read_bytes()
        profiles.append({"file": path.name, "sha256": hashlib.sha256(data).hexdigest(), **summarize(data)})
    total = sum(profile["samples"] for profile in profiles)
    counts = {key: sum(profile["inclusive_samples"][key] for profile in profiles) for key in CATEGORIES}
    print(json.dumps({
        "categories_overlap": True,
        "samples": total,
        "inclusive_samples": counts,
        "inclusive_percent": {key: 100 * count / total for key, count in counts.items()},
        "profiles": profiles,
    }, indent=2))


if __name__ == "__main__":
    main()
