# Scale readiness: what actually breaks at 10x and 100x

**Measured 2026-09-02** with `timefusion sim synth:whale --hours 24`, the
IO-free coordinator on virtual time. `--scale F` multiplies per-unit duration,
which is the right proxy for "F times the data per unit". Queue: 813 pending.
`--workers` is prod's `HEAVY_REWRITE_PERMITS`, **pinned at 10**.

No deploy, no prod load, reproducible in seconds. This is the answer to "can we
handle 10x" that the counters cannot give, because prod counters need a quiet
process and prod is never quiet.

## The headline

| load | workers = 10 (prod) | verdict |
| --- | --- | --- |
| **1x** (today) | 813 → **0**, 0 timeouts, lag 0s | idle |
| **10x** | 813 → **0**, 0 timeouts, lag 3600s | **keeps up** |
| **20x** | 813 → **0**, 53 timeouts, lag 3h | keeps up, straining |
| **30x** | 813 → **0**, 138 timeouts, lag 5h | keeps up, straining |
| **50x** | 813 → 53 left, 337 timeouts, lag 9.5h | **does not drain** |
| **100x** | 813 → 218 left, 865 timeouts, lag 10h | does not drain |

**10x is fine today, unchanged.** The queue still fully drains on the pinned
10 workers, with zero timeouts and no splits — the only cost is frontier lag
rising from 0 to one hour. The knee is between **30x and 50x**.

## The constraint CHANGES CHARACTER between 50x and 100x — this is the finding

**At 50x it is concurrency-bound.** More workers fixes it outright:

| workers at 50x | pending end | lag max |
| --- | --- | --- |
| 10 | 53 | 34200s |
| 20 | **0** | 16200s |
| 40 | **0** | 9000s |
| 80 | **0** | 3600s |

**At 100x more workers stops working.** Pending plateaus and timeouts get
*worse*, not better:

| workers at 100x | pending end | timeouts | lag max |
| --- | --- | --- | --- |
| 40 | 139 | 2036 | 26298s |
| 80 | 52 | 2328 | 29397s |
| 160 | 44 | **2758** | 29498s |

Quadrupling the workers buys ~8 units of pending and *adds* 700 timeouts. That
is the signature of a **per-unit deadline** limit, not a throughput limit: each
unit is now too big to finish in its budget, so it times out, splits, and
retries — and running more of them concurrently just produces more timeouts.
The coordinator does split (644–988 splits), but at 100x splitting-after-failure
does not converge.

### Both findings hold across seeds

Single-sample conclusions have been wrong repeatedly in this codebase, so both
were re-run on four seeds (pending remaining after 24h, from 813):

| seed | 50x w10 | 50x w20 | 100x w20 | 100x w160 |
| --- | --- | --- | --- | --- |
| `0x5eed` | 55 | **0** | 198 | 59 |
| `0xa11ce` | 21 | **0** | 201 | 74 |
| `0xb0b` | 44 | **0** | 219 | 54 |
| `0x1234` | 37 | **0** | 236 | 53 |

4/4: at 50x doubling the workers drains it completely; at 100x **eight times**
the workers moves pending only ~210 → ~60 and never reaches zero.

### The cleanest proof it is per-unit, not concurrency

If units were merely contending, the per-execution timeout rate would fall as
workers rise. It does not move at all across an **8x** range:

| workers at 100x | executions | timeouts | timeout rate |
| --- | --- | --- | --- |
| 20 | 2006 | 1060 | **52.8%** |
| 40 | 4051 | 2043 | **50.4%** |
| 80 | 4694 | 2404 | **51.2%** |
| 160 | 5178 | 2679 | **51.7%** |

A flat ~51% is the signature of a property of the UNIT (it does not fit its
deadline), independent of how many run at once. Adding workers just runs more
coin-flips at the same odds — and at 100x **about half of all executions are
wasted work**.

### It is not the split guard

The obvious suspect for "splitting does not converge" is the split guard, so it
was tested directly (`--guard-off`, 100x, 40 workers, pending remaining):

| seed | guard on | guard off |
| --- | --- | --- |
| `0x5eed` | 134 | **287** |
| `0xb0b` | 123 | **287** |

Turning the guard off makes it **twice as bad**. The guard is doing its job; the
limit is upstream of it, in how large a unit is allowed to be in the first place.

## What this means for the roadmap

1. **10x needs nothing.** Do not spend effort there. The subsystems the goal
   names are not the constraint at 10x — the queue drains with zero timeouts.
2. **50x is bought with concurrency, and concurrency is pinned by MEMORY.**
   `HEAVY_REWRITE_PERMITS` is a *pinned constant* whose stated purpose is to
   "guard against an uncapped-rewrite OOM", and peak transient heap is roughly
   `block_size x permits`. So the 50x lever is not a scheduler change, it is
   **memory headroom per rewrite**. This is the same ceiling that causes the
   OOM restarts that manufacture duplicates
   (`2026-09-02-stop-manufacturing-duplicates.md`) — the memory work and the
   throughput work are one problem, and this is the measurement that shows it.
   **The 50x change is concrete, and it costs no extra memory.** The binding
   constraint is not the cgroup — heavy sorts run inside `heavy_share_bytes()`
   (~4.98 GiB on prod), and each concurrent sort needs a slice ≥ **182 MB**
   (a 150 MB widest indivisible otel batch + `ExternalSorterMerge`'s 32 MB
   unspillable floor) or it *fails* instead of spilling. That sets a hard
   ceiling on permits, independent of the cgroup:

   | permits | per-sort slice of the 4.98 GiB heavy share | vs the 182 MB floor |
   | --- | --- | --- |
   | 10 (today) | 510 MB | OK |
   | 20 | 255 MB | OK |
   | 28 | 182 MB | exactly at the floor |
   | 32 | 159 MB | **fails — a sort cannot spill below its floor** |

   So **permits can double to 20 — which is exactly what the 50x sim needs — by
   re-slicing the pool we already have**, not by buying memory. `config.rs` has
   a test pinning the fan-in envelope (`permits × PER_SORT_BUDGET_BYTES`) at
   exactly 20 GiB and demanding that any change "state the memory headroom that
   pays for it": halving `PER_SORT_BUDGET_BYTES` from 2 GiB to 1 GiB alongside
   the permit doubling keeps that envelope unchanged. `PER_SORT_BUDGET_BYTES` is
   a spill *threshold*, not a reservation, which is why this is a re-slice rather
   than an allocation.

   **Not done tonight**: it is the OOM path, the envelope is deliberately
   guarded, and at 1x–10x nothing needs it. It is a proposal with arithmetic
   attached, for a human to weigh.

3. **100x needs units to be SIZED, not split after they fail.** More workers
   cannot fix a unit that cannot finish. The lever is unit *width* chosen up
   front from a cost estimate, so a unit fits its budget by construction —
   consistent with the earlier finding that day-wide sealed units are
   unsatisfiable against a 900s deadline, and that unit cost is dominated by the
   commit, so width is the lever.

## Honest limits of this measurement

- **The ARRIVAL axis is untested, and that is a real gap.** `--scale` models
  "each unit costs F times more" — 10x the data per customer. It does NOT model
  10x the *customers*. `--streams` exists for exactly that ("260 streams at 130
  projects"), but it is **inert for `synth:whale`**: stream minting is gated on
  `cfg.mint_frontier`, and the synthetic queue forces that to `false`. Verified
  empirically — 26, 130, 260 and 1300 streams all produce identical output
  (389 executions, 813 → 0, lag 0s). So *"10x keeps up"* is proven for
  data-per-unit growth and **unproven for customer-count growth**, which is the
  axis the goal actually names ("more concurrent customers and users"). Closing
  it needs a real journal, because minting derives its streams from one.
- The sim is IO-free: it models scheduling, deadlines, splitting and coarsening,
  not object-store latency. It answers "does the policy keep up", not "how many
  milliseconds".
- **Every completion in these runs is `BaseRollup`.** `synth:whale` is a
  rollup-shaped queue, so this measures the ROLLUP path's scale behaviour, not
  dedup or hot-tail packing. Those two were measured separately against prod
  tonight (see `approaches-and-decisions.md`: dedup's expensive path has zero
  staging timeouts, and its cheap probe backlog is draining 61 -> 7 groups per
  phase). A real prod journal would exercise all ops in one run and is the
  obvious next input.
- `synth:whale` is a reproducible synthetic queue, not tonight's prod journal —
  the prod journal lives in object storage, not on the CapRover host's disk, so
  it could not be fetched under the read-only constraint.
- Virtual time, single seed (`0x5eed`) for every run above, so the comparisons
  are like-for-like; absolute numbers would move with a different seed.

## Reproduce

```bash
cargo build
./target/debug/timefusion sim synth:whale --hours 24 --scale 10 --workers 10
./target/debug/timefusion sim synth:whale --hours 24 --scale 100 --workers 160
```

## Prior art for the 100x lever: nobody splits after failure

Our 100x failure is that a unit is too big to finish, times out, splits, and
retries — and adding workers makes it *worse* because more oversized units run
concurrently. That is a solved problem, and the solution is the opposite of what
we do: **cap the unit size before selecting it, and shrink the cap as the pool
fills.**

**ClickHouse MergeTree** has exactly these knobs:

| ClickHouse setting | what it does | our equivalent |
| --- | --- | --- |
| `max_bytes_to_merge_at_max_space_in_pool` (default 150 GB) | hard ceiling on the output size of any background merge — **a merge that would exceed it is never selected** | we have no size ceiling on unit selection; we discover the problem via a 900s deadline |
| `number_of_free_entries_in_pool_to_lower_max_size_of_merge` | **as free pool slots run out, the maximum merge size is scaled DOWN** | we hold unit size constant and raise concurrency, which is what our 100x runs show failing |
| `max_bytes_to_merge_at_min_space_in_pool` | a much smaller ceiling when resources are scarce | — |

The second row is the important one. It is the direct answer to the measurement
above: under pressure ClickHouse makes units **smaller**, not more numerous. Our
100x runs are the empirical demonstration of why — 40 → 160 workers moved
pending 139 → 44 while timeouts rose 2036 → 2758.

ClickHouse also treats "take available memory into account when selecting parts
to merge" as an open/handled concern rather than an afterthought
([ClickHouse#16838](https://github.com/ClickHouse/ClickHouse/issues/16838)), and
has a documented failure mode where a part grown beyond
`max_bytes_to_merge_at_max_space_in_pool` becomes effectively unmergeable
([#80681](https://github.com/ClickHouse/ClickHouse/issues/80681)) — worth
knowing before we add a cap, because a cap without a way to handle
already-oversized units creates immortal units. We have met that shape before
(`tf_optimize_stripped_tags_make_files_immortal`).

The LSM literature frames the same choice as compaction *granularity* and *data
movement policy* being first-class design dimensions rather than constants
([Sarkar et al., *Constructing and Analyzing the LSM Compaction Design Space*,
VLDB 2021](http://vldb.org/pvldb/vol14/p2216-sarkar.pdf)), and compaction memory
is normally budgeted explicitly alongside the memtable rather than hoped for
([Luo & Carey, *Adaptive Memory Management in LSM-based Storage Systems*,
VLDB 2021](https://vldb.org/pvldb/vol14/p241-luo.pdf)).

**What this suggests for us, concretely:**

1. Give unit selection a **byte ceiling**, so a unit is sized to fit its budget
   rather than discovering the budget by timing out. The byte preflight the sim
   already exercises is the natural place — today it can refuse a unit; this
   would let it *size* one.
2. Make that ceiling a **function of free permits**, so the system degrades to
   smaller units under load instead of to more timeouts.
3. Keep an escape hatch for units already larger than the ceiling, or they
   become immortal.

None of this is needed below 50x, and none of it was built tonight.

## The real prod journal: the tail IS the workload

The synthetic runs above are rollup-shaped. The real journal (78,741 tasks,
63.8 MB, `docker cp`'d read-only from the running container) says something the
synthetic queue could not, and it is the most important number of the night.

**Queue composition** — 17 projects:

| operation | tasks | share |
| --- | --- | --- |
| `base_rollup` | 44,088 | 56% |
| `dedup` | 22,329 | 28% |
| `derived_rollup` | 9,493 | 12% |
| `repair` | 2,831 | 4% |

States: 57,197 complete, **19,159 superseded (24%)**, 1,248 pending, 1,121 retry.

Note the reconciliation: dedup is only 28% of tasks but ~96% of worker *time*,
so a dedup unit is roughly an order of magnitude costlier than a rollup unit.

### Unit size is heavy-tailed to an extreme degree

`estimated_decoded_bytes`, straight from the journal:

| operation | median | p90 | p99 | max |
| --- | --- | --- | --- | --- |
| `base_rollup` | 0.35G | 1.33G | 8.19G | 45.9G |
| `dedup` | 0.30G | 1.36G | **22.7G** | **1150G** |
| `derived_rollup` | 0.25G | 0.50G | 0.50G | 15.1G |
| `repair` | 0.10G | 3.33G | 15.7G | **1044G** |

A dedup unit's median is 0.30 GiB and its maximum is **1.1 TiB — a ~3,800x
spread**, with p99 at 76x the median.

### The number that matters

Against the real budget — each concurrent heavy sort gets a **510 MB** slice of
the 4.98 GiB heavy share, and `PER_SORT_BUDGET_BYTES` is **2 GiB**:

| operation | units over the 510 MB slice | units over the 2 GiB budget | share of that op's BYTES in those units |
| --- | --- | --- | --- |
| `base_rollup` | 17.0% | 6.9% | 49.7% |
| `dedup` | 27.6% | 6.0% | **76.9%** |
| `derived_rollup` | 0.9% | 0.6% | 13.8% |
| `repair` | 21.9% | 15.5% | **95.3%** |

> **6.4% of all units exceed the 2 GiB per-sort budget, and those units carry
> 67.1% of all queued bytes.**

Two thirds of the maintenance workload sits in units that individually do not
fit the budget they run against. That reframes everything above:

- It explains why adding workers fails at 100x — the tail units still do not
  fit, so the ~51% timeout rate is invariant to concurrency.
- **It means unit sizing is not a 100x concern. It is a TODAY concern.** The
  ClickHouse-style cap (size the unit before selecting it; shrink the cap as the
  pool fills) is addressing the majority of our current bytes, not a
  hypothetical future.
- It also explains the 24% superseded rate: work planned against a tail unit has
  a long window in which to be invalidated.

The cheapest expression of this is the one prior art already gives us: refuse to
*select* a unit whose estimated bytes exceed what a sort slice can absorb, and
split it at planning time instead of discovering it at the deadline. The
estimate is already in the journal — `estimated_decoded_bytes` is what every row
of the tables above was computed from, so the input for the decision exists and
is simply not used as a selection bound.
