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

---

## PROD CONFIRMATION: 98.8% of dedup rewrites are on the expensive path

`dedup_plan_shape` over 96 minutes of production, 83 dedup rewrites:

| | |
| --- | --- |
| **`ordered_scan=false`** | **82 of 83 — 98.8%** |
| `ordered_scan=true` | 1 |
| `sorts=1` (a SortExec is present) | 82 |

**Essentially every production dedup unit pays the sort** — the path the benchmark
above prices at **20-43x**. This is the concrete answer to "is dedup breaking a
sweat": yes, and by a measurable factor.

### It is NOT a footer-metadata bug

Sampling the newest 25 files on five dates (2026-08-20 through 2026-09-03):
**every file declares 5 sorting columns.** The footers are honest, on old and new
files alike, so the 2026-09-02 leaf-index fix is holding and this is not legacy
damage.

### It is OVERLAP, measured

On the 201 Delta-live files of one whale day:

| | |
| --- | --- |
| files disjoint from ALL others | **8 of 201 — 4.0%** |
| max files overlapping at one instant | **10** |
| file time-span | p50 **300 s**, p90 958 s, max **66,603 s (18.5 h)** |

Each file is internally sorted; the file SET has no global order, so DataFusion
must sort. That is exactly the mechanism `tf_mor_breaks_time_disjoint_files`
predicted, now measured on the file set rather than inferred from a plan.

### Sub-hypothesis TESTED AND REFUTED: "the wide files are the poison"

An 18.5-hour file overlaps everything, so eliminating the wide ones looked like a
cheap, targeted fix. **Modelled, it is not:**

- wide files (span > 1 h): **5 of 201 (2.5%)**, holding 23% of rows — and three of
  the five are tiny (4 to 14k rows), i.e. scattered enrichment updates
- **removing them lifts disjointness only 4.0% -> 18.4%** (max overlap 10 -> 7)

**81.6% of files would still overlap.** The overlap is pervasive among ordinary
~5-minute files, because a time bucket accumulates SEVERAL files — the original
flush plus every later enrichment append that lands in it.

**So the lever is merging files WITHIN a bucket — which is precisely what hot-tail
packing does, and `pending_hot_packing` has sat at 16-17 all night.** Not a new
mechanism: an existing lane whose throughput now has a measured 20-43x
consequence on dedup, and a read-path consequence on top.

---

## Following the lever: hot packing is CLAIMED but cannot aim

If overlap is fixed by merging files within a bucket, hot packing is the lane
that matters. It is **not** starved of claims — 100 minutes of prod:

| operation | task starts |
| --- | --- |
| Dedup | 108 |
| BaseRollup | 107 |
| SealedConsolidation | 36 |
| Repair | 36 |
| **HotPacking** | **35** |
| DerivedRollup | 35 |

But it consumes **1,106 worker-seconds of ~96,800 available (1.1%)** — ~31 s per
unit, with 7 units reporting `compaction_unit_selected_nothing`. It runs often and
does little.

**`maintenance_hygiene_debt_unclaimed` fired 50 times and names why:**

```
operation=HotPacking refusal="outranked_by:6297304f:2026-09-03:00000000:2026-09-03:files=37"
```

Format is `outranked_by:<winner>:<winner_date>:<worst>:<worst_date>:files=N`. Both
cells are TODAY, so they tie on `starved` and fall through to `benefit`:

```rust
const BENEFIT_BUCKET_FILES: u32 = 64;
benefit = -(input.files / BENEFIT_BUCKET_FILES)
```

**37 / 64 = 0.** The most indebted hot-packing cell scores identically to a
one-file cell. Measured across the journal's hygiene-style lane:

| lane | pending with file counts | under 64 files | file p50 / max |
| --- | --- | --- | --- |
| **repair** | 24 | **24 = 100%** | **1 / 27** |

Every one lands in bucket 0. HotPacking's worst cell (37) does too. Only
SealedConsolidation's 338-file cell reaches a non-zero bucket (5).

**So the debt term is switched OFF precisely in the small-file lanes — and merging
small files is what hot packing IS.** With `benefit` tied at 0, ordering falls
through to recency and the per-project cursor, i.e. effectively blind to debt.

### Why the bucket exists — a fix must respect this

The coarse bucket is deliberate: `claim_next` matches the winning rank tuple
EXACTLY, so a continuous key would make one unit the sole winner of every claim
and defeat the per-project rotation in `fair_cursors`. **A fix cannot simply use
raw file counts.** A smaller bucket (e.g. 8) keeps quantisation while restoring
discrimination at the sizes these lanes actually see; the sim's
`claims_*` counters plus a per-lane debt metric are the acceptance test.

### NOT SHIPPED TONIGHT, deliberately

This is a second selection/ordering change, the category with the worst incident
history in this repo, and one such change (`dd4a557f`) already shipped tonight and
is still being verified. **One change per deploy.** The evidence is recorded here;
the fix belongs to a session that can sim it, ship it alone, and watch it.
