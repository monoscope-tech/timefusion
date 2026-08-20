"""Enumerate rollup-tier partitions holding UNTAGGED (immortal) files.

A tier file carries `timefusion.slice_*` identity tags; one that does not could
never be retired by the publish path's replace-set before `slice_retires` grew
its untagged arm, so every rebuild stacked another version of every `id` beside
it. This lists the (tier, project, date) cells a day-wide rebuild must visit.

Two traps this deliberately avoids, both of which produced confident wrong
numbers on 2026-08-20:

- `DeltaTable.get_add_actions(flatten=True)` exposes NO `tags.*` columns, so a
  tags check against it reports EVERY live file as untagged (1,939 vs the true
  352). A probe returning the same verdict for every row is a bug report about
  the probe.
- pyarrow cannot read this bucket over S3 directly — OVH rejects its
  `x-amz-checksum-mode` header and every GetObject fails. Download the
  checkpoint with the AWS CLI and read it locally.

Usage:
    set -a; source .env.prod; set +a
    python3 scripts/rollup_untagged_cells.py <dir-with-downloaded-_delta_log> [--commands]

Give it a directory holding, per tier, the newest checkpoint parquet AND every
.json commit — both are needed; see live_files().
"""

import collections
import glob
import json
import os
import re
import sys

import pyarrow.parquet as pq

TIERS = ["otel_logs_and_spans_rollup_dashboard_1m_v3", "otel_logs_and_spans_rollup_dashboard_1h_v2"]
OPS = {TIERS[0]: "base", TIERS[1]: "derived"}
SOURCE = "otel_logs_and_spans"

root = sys.argv[1]
UNTAGGED = "timefusion.slice_start_micros"


def live_files(tier_dir):
    """Replay the checkpoint plus every later commit, yielding the CURRENT adds.

    The checkpoint alone is not the table. Delta checkpoints here every 20
    commits, so a repair of 26 units leaves its last commits outside the newest
    checkpoint — reading only the parquet reported a day as untouched on
    2026-08-20 when it had in fact been repaired.
    """
    live = {}
    checkpoint_version = -1
    for path in sorted(glob.glob(os.path.join(tier_dir, "*.checkpoint*.parquet"))):
        checkpoint_version = max(checkpoint_version, int(re.search(r"(\d{20})\.", os.path.basename(path)).group(1)))
        for add in pq.read_table(path).column("add").to_pylist():
            if add is not None:
                live[add["path"]] = add
    for path in sorted(glob.glob(os.path.join(tier_dir, "*.json"))):
        match = re.search(r"(\d{20})\.json", os.path.basename(path))
        if not match or int(match.group(1)) <= checkpoint_version:
            continue
        for line in open(path):
            action = json.loads(line)
            if "remove" in action:
                live.pop(action["remove"]["path"], None)
            if "add" in action:
                live[action["add"]["path"]] = action["add"]
    return live.values()


cells = collections.Counter()
for tier in TIERS:
    adds = list(live_files(os.path.join(root, tier)))
    for add in adds:
        # Partially tagged is untagged for coverage: recover_rollup_coverage
        # requires every identity tag before it will read a file.
        if not dict(add.get("tags") or {}).get(UNTAGGED):
            partition = dict(add.get("partitionValues") or {})
            cells[(tier, partition.get("project_id", "default"), partition.get("date", "?"))] += 1
    if "--commands" not in sys.argv:
        untagged = sum(n for (t, _, _), n in cells.items() if t == tier)
        print(f"{tier}: {len(adds)} live, {untagged} untagged in {len([1 for t, _, _ in cells if t == tier])} cells")

if "--commands" in sys.argv:
    # Oldest first: settled days are cheap to check a mistake against before
    # touching the recent ones dashboards actually read.
    for tier, project, date in sorted(cells, key=lambda cell: (cell[2], cell[1])):
        print(f"timefusion run-unit --project {project} --source {SOURCE} --date {date} --op {OPS[tier]}")
else:
    for date in sorted({d for _, _, d in cells}):
        print(f"  {date}  files={sum(n for (_, _, d), n in cells.items() if d == date):4d}  cells={len([1 for _, _, d in cells if d == date])}")
    print(f"total: {sum(cells.values())} files in {len(cells)} cells")
