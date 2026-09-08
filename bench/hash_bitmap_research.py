#!/usr/bin/env python3
"""Index-resident histogram prototype and version-visibility counterexamples.

Requires pyroaring. Uses the same synthetic distribution as hash_index_research.py.
This measures in-memory Roaring operations, not deployed TimeFusion performance.
"""
import argparse
import itertools
import json
import math
import statistics
import time
from pathlib import Path

from pyroaring import BitMap


def visibility_cases():
    tested, failures = 0, []
    values = [(), ("a",), ("b",), ("a", "b"), ("a", "a")]
    for old, new, deleted, old_leg, new_leg in itertools.product(
            values, values, (False, True), ("indexed", "raw", "mem"), ("indexed", "raw", "mem")):
        rows = [(1, old, False, old_leg), (2, new, deleted, new_leg)]
        winner = rows[-1]
        expected = "a" in winner[1] and not winner[2]
        indexed_candidate = any("a" in r[1] and r[3] == "indexed" for r in rows)
        # Model the current split: candidate ID narrows indexed files only.
        admitted = [r for r in rows if r[3] != "indexed" or indexed_candidate]
        chosen = max(admitted, default=None)
        actual = chosen is not None and "a" in chosen[1] and not chosen[2]
        if actual != expected:
            failures.append({"old": old, "new": new, "deleted": deleted,
                             "old_leg": old_leg, "new_leg": new_leg,
                             "expected": expected, "split_prefilter": actual})
        # Global discovery includes all legs; then fetch every candidate's versions.
        global_candidate = any("a" in r[1] for r in rows)
        safe = global_candidate and "a" in winner[1] and not winner[2]
        assert safe == expected
        # Equivalent winner bitmap: match postings intersect current live versions.
        matches = BitMap(i for i, r in enumerate(rows) if "a" in r[1])
        live = BitMap([1]) if not winner[2] else BitMap()
        assert bool(matches & live) == expected
        tested += 1
    assert failures, "The known partial-coverage counterexample must reproduce"
    return {"cases": tested, "split_failures": len(failures),
            "counterexamples": failures[:8], "global_candidates_pass": tested,
            "winner_bitmap_pass": tested,
            "scope": "Algorithm model; not an end-to-end TimeFusion regression test"}


def benchmark(n, scattered):
    assert math.gcd(n, 104729) == 1
    tags = {t: BitMap() for t in ("common", "rare", "medium", "overlap")}
    hours = [BitMap() for _ in range(720)]
    start = time.perf_counter()
    for i in range(1, n+1):
        doc = ((i-1)*104729) % n if scattered else i-1
        hours[(i-1)*720//n].add(doc)
        if i % 10:
            tags["common"].add(doc)
        if i % 1000 == 0:
            tags["rare"].add(doc)
        if i % 100 == 0:
            tags["medium"].add(doc)
        if i % 200 == 0:
            tags["overlap"].add(doc)
    live = BitMap(range(n))
    for b in [*tags.values(), *hours, live]:
        b.run_optimize()
    build_s = time.perf_counter()-start
    # Counts are precomputed for the comparison, without recording union counts.
    counts = {tag: [b.intersection_cardinality(h) for h in hours] for tag, b in tags.items()}
    cases = {"absent": BitMap(), **tags, "or_overlap": tags["medium"] | tags["overlap"]}
    out = []
    for days in (3, 7, 30):
        first_hour = 720-days*24
        # First included i-1 is ceil(n*(30-days)/30).
        start_i = (n*(30-days)+29)//30+1
        multiples = lambda divisor: n//divisor-(start_i-1)//divisor
        expected = {"absent": 0, "common": n-start_i+1-multiples(10),
                    "rare": multiples(1000), "medium": multiples(100),
                    "overlap": multiples(200), "or_overlap": multiples(100)}
        for case, posting in cases.items():
            times = []
            for _ in range(7):
                t = time.perf_counter()
                visible_matches = posting & live
                result = [visible_matches.intersection_cardinality(h) for h in hours[first_hour:]]
                times.append((time.perf_counter()-t)*1000)
            assert sum(result) == expected[case], (case, days, sum(result), expected[case])
            out.append({"case": case, "days": days, "count": sum(result),
                        "bitmap_ms": times, "bitmap_median_ms": statistics.median(times)})
    return {"rows": n, "scattered_doc_ids": scattered, "build_seconds": build_s,
            "serialized_postings_bytes": sum(len(b.serialize()) for b in tags.values()),
            "serialized_hour_bitmaps_bytes": sum(len(b.serialize()) for b in hours),
            "serialized_live_bitmap_bytes": len(live.serialize()),
            "query_results": out,
            "overlap_30d_naive_sum": sum(counts["medium"])+sum(counts["overlap"]),
            "overlap_30d_exact": len(cases["or_overlap"])}


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--rows", type=int, default=1_000_000)
    p.add_argument("--output", type=Path, required=True)
    args = p.parse_args()
    result = {"visibility": visibility_cases(), "benchmarks": []}
    for scattered in (False, True):
        item = benchmark(args.rows, scattered)
        result["benchmarks"].append(item)
        print("completed", args.rows, "scattered", scattered, "build seconds", item["build_seconds"], flush=True)
    args.output.write_text(json.dumps(result, indent=2)+"\n")
