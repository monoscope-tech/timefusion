# The sorted dedup path is LINEAR; the unsorted one is not — and that closes the loop

**Local benchmark, 2026-09-03** (`cargo bench --bench dedup_benchmarks`, criterion
mean of 10 samples). Production dedup runs **keep-greatest** (merge-on-read), so
that is the column that matters.

## The measurement

| rows | dup % | shuffled | sorted | **shuffled / sorted** |
| --- | --- | --- | --- | --- |
| 10M | 0 | 19,579 ms | 455 ms | **43.0x** |
| 10M | 50 | 14,656 ms | 397 ms | **36.9x** |
| 10M | 90 | 5,621 ms | 274 ms | **20.5x** |
| 10M | 99 | 1,011 ms | 197 ms | 5.1x |
| 1M | 0 | 434 ms | 46 ms | 9.4x |
| 1M | 50 | 366 ms | 40 ms | 9.2x |
| 1M | 90 | 120 ms | 30 ms | 3.9x |

## The finding that matters for 10x

Scale the SAME case from 1M to 10M rows — ten times the data:

| path | 1M | 10M | growth |
| --- | --- | --- | --- |
| **sorted, keep-greatest** | 46 ms | 455 ms | **9.9x — LINEAR** |
| **shuffled, keep-greatest** | 434 ms | 19,579 ms | **45x — SUPERLINEAR** |

**On sorted input, dedup cost scales linearly with data.** That is the property
10x readiness requires, and we have it — the 2026-09-02 work (dedup keys as a
sort prefix + `RunCollapse`) is what buys it, and this quantifies the win at
**37-43x** on a 10M-row unit.

**On unsorted input it degrades superlinearly**, because the operator must sort
before it can collapse: O(n log n) plus spill pressure, which is exactly the
`ExternalSorter` + unspillable `ExternalSorterMerge` shape seen in the failing
production queries.

## This closes the loop on tonight's synthesis — as a measured feedback LOOP

`2026-09-03-why-the-frozen-mass-is-a-read-path-bug.md` traced:

> `version_append` breaks time-disjointness -> the Delta leg declares no ordering
> -> a full SortExec appears

Put that together with the numbers above and it is not a chain, it is a **cycle**:

```
partition not deduped
  -> files not time-disjoint
  -> scan cannot declare ordering
  -> dedup of that partition runs the SHUFFLED path  (43x at 10M rows)
  -> the unit is far likelier to exhaust its budget / deadline
  -> partition STAYS not deduped
```

**A partition that falls behind gets 43x more expensive to catch up on.** That is
a positive feedback loop with a measured gain, and it explains why the frozen mass
stayed frozen for 18 days rather than draining slowly: the units guarding it are
the most expensive ones in the fleet, precisely because they are stale.

It also re-prices tonight's `dd4a557f`. Making the (14 d, 31 d] band reachable is
not merely fairness — **each unit it unblocks is one that would otherwise keep
compounding**, and every partition returned to a sorted, disjoint state moves its
future dedup cost from the 45x curve onto the 9.9x one.

## What this says about the remaining 10x gap

The gap is unit cost, and the largest single term is **whether the input is
sorted** — worth 37-43x at 10M rows, far more than any scheduling change.
Therefore the highest-value remaining work is not making units smaller or the
pool bigger, but **keeping partitions on the sorted path**:

1. Ensure every rewrite output declares honest footer ordering (prior work exists;
   `tf_footer_sort_index_is_a_leaf_2026-09-02` is the trap).
2. Prioritise partitions that have LOST ordering, because their cost is the one
   that compounds.
3. Treat "fraction of scanned files declaring ordering" as a first-class health
   metric — it is a *cost multiplier*, not a nicety.

**Caveat:** synthetic batches, in-process, no object storage — so these are
operator costs, not whole-unit costs, and real units also pay S3 round trips.
The RATIO and the scaling exponent are the transferable results, not the absolute
milliseconds.
