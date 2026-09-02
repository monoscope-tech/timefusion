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

### Production already pays for this, and the journal says so directly

The sim argues that units are discovered-too-big rather than sized. The journal
proves it happens, without any simulation:

- **25.4%** of all tasks (19,961 of 78,741) carry a `retry_reason`.
- **80.3% of those retries are `split_into_smaller_slices`** — 16,036 tasks.
  That is **one in five tasks in the whole journal** claimed, found not to fit,
  and split. Discovering size after selection is not a theoretical cost; it is
  the single largest source of retry in production.
- **338** carry `dedup: Not enough memory to continue external sort` — the
  per-sort budget failing outright in production, which is the concrete form of
  the 182 MB unspillable floor analysis above.
- The rest are small: `migrated_to_aligned_hour_slice` 2,790,
  `resource_admission` 273, `admission_busy` 171, `worker_error` 159.

And the superseded population is **biased toward big units**:

| population | n | median | mean |
| --- | --- | --- | --- |
| superseded | 17,296 | 0.31G | **1.50G** |
| not superseded | 21,447 | 0.31G | 0.81G |

Identical medians, but the superseded mean is **1.85x** larger — so it is the
*tail* that gets invalidated before it can run, exactly as expected when a large
unit sits claimed for a long window. That is 19,159 tasks of planned work thrown
away, and it is size-correlated.

**Taken together:** one in five tasks is split after claim, the tail is what gets
superseded, and 6.4% of units carry 67.1% of the bytes. Sizing units at
selection time addresses all three at once.

## The heavy tail IS a single tenant — which is the 100x customer question in miniature

Maintenance work is not spread across the 17 projects. **One project is 83.4% of
all queued bytes** (35.3 TiB of 42.3) while being only 33% of tasks; the top 3
are 88.9%.

| population | n | median | p90 | p99 | max |
| --- | --- | --- | --- | --- | --- |
| the whale | 22,302 | 0.37G | 1.85G | 12.2G | **1150G** |
| everyone else | 16,441 | 0.25G | 0.50G | 5.19G | 39.2G |

| population | units > 2 GiB | share of that group's bytes they hold |
| --- | --- | --- |
| the whale | **9.3%** | **73.9%** |
| everyone else | 2.4% | 32.7% |

- **83.8% of ALL oversized units belong to the whale.**
- **61.7% of the ENTIRE queue's bytes are whale units that individually exceed
  the per-sort budget.**
- The whale's worst-case unit is **29x** everyone else's worst case (1150G vs
  39.2G).

This is the most decision-relevant finding for the "customer with 100x our total
volume" question, because **we are already running the experiment at small
scale.** One tenant already generates five sixths of the oversized units, and it
is exactly that population which cannot fit a sort slice, gets split after
claim, and is preferentially superseded.

The implication is not "we cannot take the customer". It is that the thing to
fix first is **per-tenant unit sizing** — the budget a unit is selected against
has to be a property of the unit, not of the fleet — because a second, larger
whale multiplies the population that already accounts for two thirds of our
bytes. Onboarding 100x volume without it would land entirely in the failure mode
we can already measure.

## CORRECTION: on the real journal we do not keep up at 1x — and it is a FLOW problem

Everything above this section used `synth:whale`. Running the **real journal**
changes the headline, and the earlier "10x keeps up" claim must be read narrowly.

24 virtual hours, 10 workers, real journal (21,544 pending at start):

| run | pending after 24h | executions | frontier lag max |
| --- | --- | --- | --- |
| `--no-mint` (no new arrivals) | 21,544 → **18** | 1,594 | 84,543s |
| default (ongoing ingest modelled) | 21,544 → **16,431** | 21,088 | 84,789s |

**The standing backlog is not the problem.** Without new arrivals it drains to
essentially zero using only 1,594 executions. With arrivals modelled, **21,088
executions — 13x more work done — still ends further behind than it started.**

That is a flow problem, not a stock problem, and it explains the prod
observation that `pending_dedup` never durably shrinks. It also explains why
`synth:whale` was so optimistic: `synth:whale` forces `mint_frontier = false`,
so it models a backlog with *no ongoing ingest at all*. Every scale number
earlier in this document is therefore a **backlog-drain** measurement, not a
keep-up measurement. They are still valid for what they measure — how unit cost
and concurrency interact — but "10x keeps up" means *"a 10x-costlier backlog
still drains"*, NOT *"we keep up with 10x the traffic"*.

The honest current-state answer to "are we breaking a sweat at 1x": **on the
real queue with ingest modelled, yes.**

### And coarsening — the mechanism that would help — is blocked 81% of the time

Identical in both runs, so it is not an artifact of the arrival model:

```
coarsen: candidates 114,802  blocked 92,670 (80.7%)  over_budget 22,074 (19.2%)
         subsumed 978        fused 58
```

Only **58 fusions out of 114,802 candidates.** Coarsening is what turns many
small arriving invalidations into one efficient unit — exactly what a flow
problem needs — and it is refused for four fifths of candidates, with a further
fifth refused on budget. That budget refusal is the same size ceiling this
document has been circling: units that *should* be fused cannot be, because the
fused result would not fit.

**This is the sharpest open lead of the night.** The tension is real and worth
stating: arrival pressure wants units fused *larger*, while the per-sort budget
wants them *smaller*. Resolving it is the same "size the unit deliberately"
change — a fused unit has to be sized to the budget, not abandoned when it
would exceed it.

## Root cause of the flow problem: the fusion ceiling is 1.62x the median unit

Chasing the coarsening numbers to the bottom. First, a correction to the obvious
reading: `blocked` (80.7%) is mostly **benign** — it means a Pending/Retry unit
at or wider than the fusion width already covers that bucket, so fusion is
unnecessary there. (The old superseded-blocks-everything trap is genuinely fixed;
`TaskState::Superseded => false` with a long comment explaining why.)

The signal is what happens to candidates that *could* fuse:

| | count | share of unblocked |
| --- | --- | --- |
| unblocked candidates | 22,132 | — |
| **refused over budget** | 22,074 | **99.7%** |
| actually fused | 58 | 0.3% |

**99.7% of everything that could fuse is refused on size.** Why becomes obvious
next to the real unit sizes — `MAX_DECODED_BYTES` is a hard constant of
**512 MiB**, and the median unit in the journal is **316 MiB**:

| how many median-ish units fit under the 512 MiB ceiling | share of units small enough |
| --- | --- |
| 1 | 82.8% |
| **2** | **21.5%** |
| 3 | 4.4% |
| 12 | 4.0% |
| 144 | 2.7% |

- The ceiling is **1.62x the median unit**.
- **17.2% of units already exceed it alone** — they can never fuse with anything.
- Coarsening exists so that "one unit does one scan where 144 slices each did the
  same scan". Only **2.7%** of units are small enough for 144 of them to fit.

**Coarsening — the only mechanism that reduces unit COUNT — is structurally
disabled at current unit sizes.** That is the flow problem's root: nothing
consolidates the arriving invalidations, so unit count tracks arrival rate
forever.

### The design tension this exposes, which someone should decide deliberately

Note what 512 MiB is close to: the **510 MB** per-sort slice (4.98 GiB heavy
share / 10 permits). Whether or not that was intentional, it is the right frame —
a fused unit must fit a sort slice. Which puts two of tonight's findings in
direct opposition:

- **Backlog drain wants MORE permits.** 50x only drains at 20 workers (4/4 seeds).
- **Flow/arrival wants BIGGER slices, i.e. FEWER permits.** More permits ⇒
  smaller slice ⇒ lower fusion ceiling ⇒ less consolidation ⇒ more units.

More concurrency and more consolidation are bought with the same memory, in
opposite directions. That is a genuine tradeoff, not an oversight, and it is the
thing to decide deliberately rather than by leaving one constant at 512 MiB. The
sim can price both sides before anything ships.

## CORRECTION: raising the fusion ceiling does NOT fix the flow problem — measured

The section above concluded the 512 MiB ceiling was the root cause. **I tested it
rather than recommending it, and it is wrong.** `MAX_DECODED_BYTES` was raised
512 MiB → 4 GiB (8x), rebuilt, and the real-journal sim re-run identically:

| | 512 MiB (today) | 4 GiB |
| --- | --- | --- |
| pending after 24h | 16,431 | **16,320** |
| over_budget | 22,074 | **5,816** |
| blocked | 92,670 | **99,063** |
| fused | 58 | 65 |

The ceiling *was* genuinely binding on budget refusals — they fell **74%**. But
the candidates it freed did not fuse; they became **blocked** instead
(+6,393, almost exactly the number that left `over_budget`). Net effect on the
thing that matters: **0.7%**.

**Why:** once size stops refusing them, they are refused because a Pending/Retry
unit at or wider than the fusion width already holds the bucket. The binding
constraint is bucket occupancy and group population, not bytes. The static
analysis said the same thing and I under-weighted it — the live queue holds only
**1,117 sized units across 585 (project, source, op, day) groups, ~1.9 per
group**. Greedy-packing those units by ceiling:

| ceiling | resulting units | reduction |
| --- | --- | --- |
| 512 MiB | 979 | 12.4% |
| 2 GiB | 722 | 35.4% |
| 16 GiB | 660 | **40.9%** |

Even an *infinite* ceiling saturates near 41%, because you cannot fuse units that
are not there. A 35–41% unit-count reduction is worth having, but it is not the
answer to an arrival rate that outruns 21,000 executions a day.

**What this rules out, which is the value of having run it:** "raise
`MAX_DECODED_BYTES`" is a plausible, cheap-looking, one-constant change that a
reasonable person would ship on the strength of the 99.7%-refused-on-budget
number. It buys 0.7%. The 99.7% was real and the inference from it was wrong.

**Where that leaves the flow problem.** The arrival side is now the only
remaining explanation, and it is where the next work belongs: what *mints* the
invalidations, and how many are avoidable. Two concrete leads already on the
table — the restart-driven re-invalidation the sim models explicitly (~13 tasks
per stream per restart), and the self-inflicted duplicate work in
`2026-09-02-stop-manufacturing-duplicates.md`. Both reduce arrivals rather than
trying to consolidate them after the fact.

## The arrival side, quantified: maintenance scales with STREAMS, not bytes

Having ruled out the fusion ceiling, the arrival side is where the flow problem
lives. Its model is simple and structural (`mint_stream`, `MINT_INTERVAL_MICROS`):

> **Every 10 minutes, every stream mints 2 invalidations** (base + derived
> rollup).

The journal has **124 distinct `(source, project)` streams**. So the nominal
mint rate is:

```
124 streams x 2 tasks x 144 ten-minute windows = 35,712 tasks/day
```

Against that, the sim's 24 hours did **21,088 executions** and moved pending
only 21,544 → 16,431 — a drop of 5,113. So roughly **16,000 tasks of new work
were absorbed during the run** (the gap between nominal 35,712 and observed
~16,000 is subsumption and invalidation-merging doing their job: 975 subsumed,
plus `invalidate` extending an existing task instead of creating one).

**The important property is the shape, not the exact number:**

> Maintenance arrival is **linear in stream count and independent of data
> volume.** A stream ingesting one row per 10 minutes mints exactly as many
> tasks as the whale.

That is the direct answer to "more concurrent customers and users":

| | maintenance tasks/day (nominal) |
| --- | --- |
| today, 124 streams | 35,712 |
| 10x customers | **357,120** |

Note this is a *different* axis from everything earlier in this document.
`--scale` models bytes-per-unit — 10x the data from the *same* customers. Stream
growth multiplies the unit COUNT instead, and unit count is precisely what
coarsening was supposed to control and provably cannot (previous section).

So the two scaling axes decompose cleanly:

- **the whale determines per-unit COST** (6.4% of units, 67.1% of bytes)
- **stream count determines unit COUNT** (2 per stream per 10 minutes)
- the flow problem is `count x cost > capacity`, and **only the count side grows
  with customers**

### Which makes mint granularity the lever the data actually points at

`MINT_INTERVAL_MICROS` is `NORMAL_SLICE_MICROS` = **10 minutes**. It is a
constant, and it multiplies the arrival rate directly: minting hourly instead
would cut nominal arrivals **6x** (35,712 → 5,952/day), which is below the
measured execution rate rather than above it.

That is not a free change and should not be made casually — coarser
invalidation means rollups refresh less often, i.e. dashboard staleness, and
`NORMAL_SLICE_MICROS` is load-bearing elsewhere (it is also `SUBSUME_WIDTHS[0]`).
But it is the first lever found tonight that acts on the quantity that actually
outruns capacity, and unlike the fusion ceiling it has not been refuted — it
should be priced in the sim before anything else.

**Restart cadence is NOT a significant contributor**, tested: restarts every 24h
gave pending 16,264 vs 16,320 with none — within noise. The ~13-tasks-per-stream
re-invalidation a restart causes is real but small next to 144 mint cycles a day.

## CORRECTION #2: the sim's arrival model is ~6x prod's real one

The previous section derived the arrival rate from the sim's `mint_stream`
model — every stream, every 10 minutes. **Prod does not work that way.**
Invalidation happens on actual writes (`invalidate_rollup_batches` on the
inbound path), so a stream with no traffic in a window costs nothing.

Measured from the journal's own `created_unix_ms`, over its 408-hour span:

| | tasks/day |
| --- | --- |
| sim nominal model (124 streams x 2 x 144) | 35,712 |
| **real prod arrival** | **5,782** (241/hour, min 211 / max 279 over 24 buckets) |

And **only 21 of the 124 streams created any work in the last 24 hours.** The
other 103 are idle, and the sim mints for all of them.

**This undermines the "we do not keep up at 1x" conclusion**, which was drawn
from a run whose arrival rate is ~6x reality. Against the sim's own measured
throughput of 21,088 executions/24h (879/hour), the real arrival rate of
241/hour has substantial headroom — roughly **3.6x**.

So the honest current state is: **the earlier flow-problem finding is an
artifact of the sim's uniform minting, not a proven property of production.**
What survives is narrower and still useful:

- The `--no-mint` control (backlog drains to 18 in 1,594 executions) stands —
  the standing backlog genuinely is not the constraint.
- Arrival being **linear in ACTIVE stream count** stands as a shape. What
  changes is the coefficient: today only 21 streams are active, not 124, so
  today's arrival is ~1/6 of what the model assumed.
- The mint-granularity lever is **not needed** at the real arrival rate, and
  proposing it would have been solving a problem prod does not have.

**The right next measurement** is the sim with a realistic arrival model:
`--streams 21` mints 21 x 2 x 144 = 6,048/day, within 5% of the measured 5,782.
That is the run that answers "do we keep up at 1x", and by extension what 10x
customers (210 active streams) actually costs.

**Method note, twice-learned tonight:** `synth:whale` models zero arrivals and
flattered us; the real-journal default models 6x too many and alarmed us. Both
times the fix was to check the model against production data rather than to
trust the number it printed. Any keep-up claim needs its arrival rate validated
against `created_unix_ms` first.

## What the real queue is actually doing: not flow, not backlog — STARVATION

Both sim-derived stories were wrong in different directions. The journal
answers the question directly, and this is the finding that matters.

**The queue is small but STUCK.** 2,369 tasks queued (pending + retry):

| | |
| --- | --- |
| median age | **34.8 hours** |
| older than 48h | **49%** |
| p90 / p99 / max age | **407.6h — the entire journal span** |
| younger than 1h | 10% |
| composition | dedup 1,708 (72%), repair 328, base_rollup 297, derived 36 |

A cohort has been queued for ~17 days and never run. At the real arrival rate
(241/h) this queue is under 10 hours of work, so it is **not** a capacity
problem — the work simply never executes.

### The stuck cohort is defined by ADMISSION REFUSAL

| | stuck (>48h, n=1,168) | fresh (<6h, n=331) |
| --- | --- | --- |
| top retry reasons | **`resource_admission` 272, `admission_busy` 169** | memory 41, worker_error 25 |
| never claimed (0 attempts) | **355** | — |
| whale share | **46%** | 14% |
| repair tasks | **328 — every repair task in the journal** | 0 |

`resource_admission` is documented in `maintenance_coordinator.rs` as *"the
unit's ESTIMATE exceeds what admission can ever grant, so it fails identically
every pass"*, with the prod history attached: *"a 1.1TB-estimate day-wide Repair
looped at its 1s admission-retry delay for DAYS (prod 2026-08-21, attempts
140-211): never claimed a worker, never timed out, so neither abandon_running's
split nor its backoff floor ever fired."*

**The largest unit in this journal is 1,150 GiB. That exact shape is still
present, and 328 of 328 repair tasks are starved.**

### This validates unit sizing — by a mechanism I had not identified

Everything earlier in this document argued units are too big for their budget,
and inferred the cost was *timeouts at 100x*. The real cost is worse and it is
being paid **today**: a unit whose estimate exceeds the admission ceiling is
**never admitted at all**. It does not time out. It does not fail. It waits
forever, and 46% of that population is the whale.

So the recommendation stands and strengthens, with the mechanism corrected:

- **not** "raise `MAX_DECODED_BYTES`" — measured, buys 0.7%
- **not** "coarsen mint granularity" — the arrival rate is 6x lower than the sim modelled and is not the constraint
- **yes**: bound unit size at PLANNING time so no unit is ever created that
  admission cannot grant. The permanently-starved population is exactly the
  units that exceed what admission can ever give them, and today nothing
  prevents such a unit from being created.

### Ranked by evidence, for the morning

1. **Starving units (production-verified, today).** 1,168 tasks >48h old, 328 of
   them every repair task in the system, driven by `resource_admission`.
2. **The whale is over-represented 3x** in that population (46% vs 14%), which
   is the 100x-customer question arriving early.
3. Unit sizing at planning time addresses both, and is the same change the 100x
   sim work pointed at from the other direction.

## The admission band is mis-calibrated against real unit sizes — and it is the same constant

`occupancy_scaled_ceiling` (borrowed deliberately from ClickHouse, rule 2 of the
prior-art survey) is:

```
ceiling = MAX_DECODED_BYTES x (available / capacity),  clamped to
          [MAX_DECODED_BYTES/16, MAX_DECODED_BYTES]  =  [32 MiB, 512 MiB]
```

Set that band against the real queue's unit sizes:

| operation | queued | NEVER admissible (>512 MiB) | refused on a busy pool (>32 MiB) |
| --- | --- | --- | --- |
| `base_rollup` | 164 | **57.9%** | 100% |
| `dedup` | 620 | 10.0% | 94.0% |
| `repair` | 328 | 0.0% | **100%** |
| `derived_rollup` | 5 | 0.0% | 100% |
| **all queued** | 1,117 | **14.1%** | **96.7%** |

- **14.1% of queued work can never be admitted at ANY occupancy.** That is the
  `resource_admission` population — permanently starved by construction.
- **96.7% is refused whenever the pool is busy.** The floor exists so "a busy
  pool must still admit work this small, or hygiene starves" — but it is
  **32 MiB against a median unit of 316 MiB**, so the floor admits almost
  nothing real. With the pool rarely idle, that is the `admission_busy`
  population.

**The whole scaling band sits below the working set.** The ceiling is 1.6x the
median unit and the floor is one tenth of it, so occupancy scaling spends its
entire range in territory where almost no real unit fits.

### IMPORTANT correction to this document's own earlier refutation

Earlier I raised `MAX_DECODED_BYTES` 512 MiB → 4 GiB, measured a 0.7% change,
and concluded the constant was not the lever. **That conclusion was scoped too
broadly.** The sim's module documentation states plainly:

> *"Memory admission and intra-call operation order are outside the model."*

So that experiment measured the **fusion** effect only. `MAX_DECODED_BYTES` also
sets the admission band, and raising it 8x would move that band from
`[32 MiB, 512 MiB]` to `[256 MiB, 4 GiB]` — taking never-admissible from 14.1%
toward ~0 and lifting the busy-pool floor from one tenth of the median unit to
close to it. **The sim cannot show that effect, and did not.**

So the honest status of the leading candidate:

- raising `MAX_DECODED_BYTES` does little for **fusion** — measured, 0.7%
- its effect on **admission starvation** is **unmeasured and potentially large**
- and the two uses are the *same constant*, which is itself worth questioning:
  the largest unit worth FUSING and the largest unit admission can GRANT are
  different questions that happen to share a number

**Next measurement, and it cannot be the sim:** an admission-aware test — either
a unit test over `occupancy_scaled_ceiling` against the real journal's size
distribution (cheap, offline, no deploy), or `run-unit` against seeded local
storage. That is the one experiment that would confirm or kill the leading
candidate, and it is where I would start.

## Pricing the candidate offline: what raising the admission band would buy, and whether it is safe

The admission-aware measurement I said was needed does not require the sim or a
deploy — the band is a closed-form function and the journal has the sizes.

**What it buys.** Share of the queued backlog admissible at a given pool
occupancy:

| `MAX_DECODED_BYTES` | band | 100% free | 50% free | 25% free | 10% free | never-admissible |
| --- | --- | --- | --- | --- | --- | --- |
| **512 MiB (today)** | [32M, 512M] | 86% | **33%** | **7%** | 3% | 14.1% |
| 1 GiB | [64M, 1G] | 88% | 86% | 33% | 6% | 11.9% |
| **2 GiB** | [128M, 2G] | 90% | 88% | **86%** | 14% | 10.5% |
| 4 GiB | [256M, 4G] | 91% | 90% | 88% | 57% | 9.5% |

Today's band is the outlier: **at half occupancy only a third of the queue can
be admitted, and at 25% free only 7%.** That is a self-sustaining stall — the
queue cannot drain while the pool is busy, and the pool stays busy because the
queue cannot drain. 2 GiB holds 86% admissible down to 25% free.

**Whether it is safe.** Admission capacity is `memory_limit x 3/4` =
**60 GiB** of decoded bytes (80 GiB budget). The worst case is every permit
holding a maximum-size unit:

| `MAX_DECODED_BYTES` | % of capacity, one unit | worst case (x10 permits) |
| --- | --- | --- |
| 512 MiB (today) | 0.8% | 5 GiB = **8%** of capacity |
| **2 GiB** | 3.3% | 20 GiB = **33%** |
| 4 GiB | 6.7% | 40 GiB = 67% |
| 6 GiB | 10.0% | 60 GiB = 100% — too far |

**Today's ceiling lets a single unit use under 1% of the pool it is being
admitted into**, and the whole fleet at full occupancy uses 8% of the decode
capacity that exists. That is not a safety margin, it is a mis-calibration: the
guard is so tight it starves the thing it protects.

### The recommendation, with its evidence

**Raise `MAX_DECODED_BYTES` from 512 MiB to 2 GiB.** It is one constant.

- Admissibility at 25% free pool: **7% → 86%**
- Never-admissible share: 14.1% → 10.5%
- Worst-case memory: 8% → **33%** of the 60 GiB decode capacity — still a 3x margin
- Fusion effect: measured, negligible (0.7%) — this change is about admission

**Caveats, stated honestly:**

1. The same constant governs fusion and admission. Those are different questions
   ("largest unit worth fusing" vs "largest unit admission can grant") and
   splitting them is the cleaner change; raising the shared value is the smaller
   one.
2. `PER_SORT_BUDGET_BYTES` (2 GiB) and the 510 MB per-sort slice are a separate
   budget. A unit admitted at 2 GiB decoded still sorts inside its slice and
   spills — which is normal — but the interaction deserves a look before shipping.
3. This is an offline calculation over a journal snapshot, not a live
   experiment. It is strong enough to justify the change and not strong enough
   to skip a canary.

## CONFIRMED ON LIVE CODE: the starvation is current behaviour, not 09-01 residue

The admission machinery churned on 2026-09-01 (occupancy-scaled ceiling, the
honest-request fix, repair slicing), so the stuck cohort could have been residue
of bugs already fixed. Checked, two ways.

**The wrong way first, recorded because the method is the lesson.** I grepped
12h of prod logs for `resource_admission` / `admission_busy` and found **zero**,
and briefly took that as "not live". It proves nothing: those strings are
**retry reasons stored on the task, never logged as events** — outside tests the
coordinator never emits them. *Absence from logs is not absence of the event
when the event is not logged.*

**The right way: re-fetch the journal and diff the cohort by task key.**
Snapshot 1 at 12:45, snapshot 2 at 17:07, 4.4 hours apart, same running binary:

| | |
| --- | --- |
| queued, snapshot 1 → 2 | 2,369 → 2,222 |
| of the 1,168 tasks stuck >48h at 12:45 | **1,144 (98%) still queued** at 17:07 |
| drained in 4.4h | **24** |
| survivors whose `attempts` INCREMENTED | **359** |
| survivors by op | dedup 671, repair 311, base_rollup 158 |
| survivors' current retry reasons | `compaction_incomplete` 194, `admission_busy` 166, `resource_admission` 146 |

**98% of the stuck cohort is still stuck, and 359 of them were actively retried
and refused during the window.** They are not idle debris; they are being picked
up and rejected on the current binary, repeatedly. The finding upgrades from
"true of a journal snapshot" to **confirmed against live code**.

## The counter-signal to the recommendation, quantified

Before raising `MAX_DECODED_BYTES`, note what the journal says about the units
that already fail their SORT (`dedup: Not enough memory to continue external
sort`, 338 tasks, 41 of them in the FRESH cohort — i.e. current behaviour):

| bucket | share of sized sort-failures |
| --- | --- |
| < 32 MiB (busy floor) | 16.7% |
| **32 MiB – 512 MiB (today's band)** | **83.3%** |
| > 512 MiB | 0% |

Median 256 MiB. **Every sized sort failure is at or below today's admission
ceiling** — units around a quarter of a gigabyte are already exhausting the
510 MB per-sort slice, because a sort's working set is a multiple of the data it
decodes.

So raising admission to 2 GiB *alone* risks converting starved units into
sort-failing units — trading `admission_busy` retries for hard failures on the
OOM-adjacent path, while the landed-skip flag is OFF (so any OOM-driven unclean
restart manufactures exactly the duplicates the first half of this night was
about).

**Revised recommendation, and it is a different ask:**

> Admission and the per-sort slice must be raised **together**, or neither. The
> admission ceiling governs what may enter; the sort slice governs what can
> actually finish. Today they disagree — admission permits 512 MiB while the
> sort slice fails at 256 MiB — and moving only one moves the failure, not the
> outcome.

That is a two-constant change on the OOM path with a real interaction, not the
one-liner it looked like an hour ago. It wants a canary and someone awake.

## A SECOND, distinct defect: repair converges one file per attempt

The largest retry reason among the stuck survivors is not admission at all —
it is `compaction_incomplete` (194–197), and it is a different failure.

Profile of that population (snapshot 2):

| | |
| --- | --- |
| operation | **repair, 197 of 197** |
| state | 195 retry, 2 running |
| attempts | median **14**, max **875** |
| age | **412.0 hours (17.2 days) — median AND max**, i.e. one cohort |
| estimated bytes | median **0.25 GiB**, max 0.3 GiB |
| whale share | **80%** |

**These units are small.** At 0.25 GiB they sit comfortably inside the 512 MiB
admission ceiling, so this is *not* the admission problem — they are admitted,
they run, and they do not finish. One has been attempted **875 times**.

**Mechanism.** Repair's candidate selection is:

```rust
// coordinator_compaction_files, Operation::Repair
candidates.filter(|add| !self.repair_verified_sorted.contains(&add.path)).take(1)
```

**`.take(1)` — one candidate file per attempt.** Admission picks a file on the
*absence* of a sort tag, which is only a suspicion; `stage_hot_bin` then reads
the footer, finds it already sorted, records it in `repair_verified_sorted`, and
returns `BinOutcome::Retry` → `completed = false` →
`journal.retry(key, "compaction_incomplete", now + 30s)`.

So each attempt clears exactly one false suspect. The code comment predicts this
outcome in as many words:

> *"could take days to reach the file that actually poisons its scans — while
> looking busy the whole time."*

It is taking **weeks**. `repair_verified_sorted.txt` on the prod host is
**12.76 MB** and still being written (16:30 today) — on Delta path lengths that
is on the order of **100,000 files already verified**, one attempt at a time.

**Why this matters independently of the admission finding:**

- It is the single largest stuck population, and it is **not** fixed by moving
  admission or sort constants — those units already fit.
- Every attempt is real work (a footer read plus scheduling) that retires one
  file, so the cost scales with the size of the suspect set, not the damage.
- It is **80% whale**, so it grows with exactly the tenant profile the 100x
  customer question is about.

**The shape of a fix** (not implemented, not validated): the suspicion is
per-FILE but the unit is per-CELL, so a repair attempt could verify the whole
candidate set in one pass rather than `take(1)` — the footer reads are
independent and already cached. That turns weeks of one-at-a-time clearing into
a single pass, and it does not touch the memory constants at all, which makes it
the lowest-risk of tonight's three candidates.

### CORRECTION to the section above: claims are NOT scarce — and that makes it worse

Two things in the profile above were misread, both caught by re-checking rather
than reasoning:

**1. `attempts` is not cumulative.** I read "median 14 attempts over 412 hours"
as one claim per ~29 hours, i.e. a starved lane. Wrong — `attempts` is reset
(`reset_repair_attempts`). Measuring the real rate from the two snapshots
4.4 hours apart:

| | |
| --- | --- |
| cohort tasks present in both | 197 |
| **gained ≥1 attempt** | **197 (100%)** |
| total attempts gained | **625 in 4.4h** |
| **claim rate** | **142/hour = ~3,409/day**, ~17 per task per day |

The lane is not starved. Every one of these tasks is being claimed roughly
hourly, running, and not completing. **The cost is ~3,409 maintenance claims per
day that each yield exactly one footer verification.**

**2. The cohort was bulk-enqueued, not organically aged.** All 197 share one
identical `created_unix_ms` = **2026-08-16T11:09:57.595Z** — a single
enqueue event, which is why median age equalled max age exactly. They have
genuinely been queued 17 days; they did not accumulate over 17 days.

**The arithmetic now closes on itself**, which is the strongest evidence the
mechanism is understood:

```
17 days elapsed x 3,409 claims/day = 57,953 files cleared, one per claim
repair_verified_sorted.txt = 12,758,889 bytes
   -> ~58,000 paths at ~220 B per Delta path
```

The verified-sorted list is exactly the size the claim rate predicts. Three
independent quantities — elapsed time, measured claim rate, and the on-disk
listing — agree.

**This strengthens the fix rather than weakening it.** Because claims are
plentiful and yield is the bottleneck, per-attempt yield is precisely the lever:
verifying the whole candidate set per attempt instead of `take(1)` converts
~3,400 claims/day of one-file grinding into a small number of passes. Had claims
been scarce (my original misreading), `take(1)` would have been secondary and
the real fix would have been upstream in claim scheduling.

## CORRECTION #3: it is not `take(1)` — repair bins price at the ENTIRE byte budget

I was about to write a test for the `take(1)` hypothesis. Checking which `Retry`
branch actually fires in prod killed it first. Three hours of logs:

| event | count |
| --- | --- |
| **`repair_rewrite_permit_busy`** | **243** (≈81/hour) |
| `light_optimize_tail_selected` | **0** |
| `light_optimize_bin_vanished` | 0 |
| `resumed_bin_committed_early` | 0 |

**The already-sorted path is never reached.** Repair units do not get far enough
to verify a footer, so `take(1)` — however wasteful in principle — is not the
operative mechanism. Had I written that fix, it would have changed nothing.

**What actually happens.** Repair rewrites are gated by a byte-priced semaphore:

```rust
let budget_mib = self.config.derived.repair_rewrite_budget_mib();   // 1,280 MiB
let want_mib = decoded(targets).clamp(1, budget_mib);
match self.repair_rewrite_sem.try_acquire_many_owned(want_mib) { ... }
```

`repair_rewrite_budget_bytes` is exactly `COORDINATOR_PER_SORT_BUDGET_BYTES`
= `5 GiB / 4` = **1.25 GiB = 1,280 MiB**. And prod logs the request:

```
want_mib=1280  budget_mib=1280   event="repair_rewrite_permit_busy"
```

**Every observed repair unit asks for the entire budget.** Its real decoded size
is at or above 1.25 GiB, so the clamp pins it to the whole semaphore — which
means **repair is serialized: one rewrite at a time, each 40+ minutes, and every
other repair unit bounces.** 243 bounces in three hours.

**The design intent was the opposite**, and the comment says so:

> *"Bins below the budget now share it, which is the only thing that changes — a
> count of 1 priced that worst case onto all 358 pending units and held repair to
> ~2/hour."*

The move from count-pricing to byte-pricing was meant to let small bins share the
budget. **In production no bin is below the budget, so the sharing never
happens** and repair sits in the same serialized state the change was written to
escape.

**One more discrepancy worth noting:** these tasks' stored
`estimated_decoded_bytes` is a median of **0.25 GiB**, while runtime pricing of
the same work asks for **≥1.25 GiB** — a ~5x disagreement between the journal's
estimate and what the rewrite actually prices. Estimates were bulk-cleared once
before because they "were all measured with a broken ruler"
(`clear_stale_estimates`); this looks like the same class of problem and it
matters, because admission and coarsening both make decisions on the stored
number.

**Revised Decision 1.** Not `take(1)`. Either:

- raise `repair_rewrite_budget_bytes` above a real bin's decoded size so the
  intended sharing can occur, or
- size repair bins below the budget — the same unit-sizing theme as everything
  else tonight.

Both are the same shape as Decision 2, which is now the strongest signal of the
night: **every stuck lane is a unit that does not fit the budget it must pass
through**, and the budgets are calibrated well below real unit sizes in three
independent places (admission 512 MiB, per-sort slice 510 MB, repair 1,280 MiB).

### The complete repair mechanism: two gates, two prices, no re-measurement

Chasing the 5x estimate disagreement to the bottom gives the whole picture.

**Gate 1 — admission** (`maintain.rs:3298`) prices the unit on its **stored**
estimate:

```rust
let request = Resources { decoded_bytes: task.estimated_decoded_bytes.clamp(1, MAX_DECODED_BYTES), .. };
```

Stored median for these units: **0.25 GiB**. It passes.

**Gate 2 — the repair rewrite semaphore** (`maintain.rs:~6857`) prices the same
work on the **actual files**:

```rust
let want_mib = estimated_decoded_bytes(targets.iter().map(|a| a.size).sum()).clamp(1, budget_mib);
```

Real value: **≥1.25 GiB**, which is the entire 1,280 MiB budget. It bounces.

**And nothing ever reconciles the two.** The byte preflight that re-measures a
unit and splits it (`split_time_task`) has exactly two call sites, and **both are
dedup paths** — repair never passes through it. So a repair unit is:

1. created with an estimate that is ~5x below what its rewrite will cost,
2. admitted on that cheap estimate,
3. refused by the semaphore on the real cost,
4. requeued unchanged — never re-measured, never split,
5. repeat ~17 times a day, for 17 days.

**That is the whole 3,409-claims-per-day waste**, and it is not a tuning problem:
no value of the repair budget fixes a unit that is priced one way to get in and
another way to run. The two gates have to agree, or the unit has to be
re-measured between them.

**Fix shapes, cheapest first:**

1. **Price admission on the same number the semaphore uses.** The unit is
   admitted on a figure that has no bearing on whether it can run.
2. **Re-measure and split repair like dedup does.** `split_time_task` already
   exists and repair simply never calls it. This is the structural fix, and it
   is the same "size the unit to the budget" theme as the rest of this document.
3. Raising `repair_rewrite_budget_bytes` alone treats the symptom and still
   leaves the two gates disagreeing.

**Caveat:** repair rewrites WHOLE FILES by design (it is fixing a file's sort
order), so "split the unit" may not be expressible for repair the way it is for
a time-sliced dedup unit. If so, option 1 is the real fix and option 2 is not
available — that distinction needs a read of the repair rewrite before anyone
commits to a direction.

### RESOLVED: repair is serialized BY CONSTRUCTION — three constants that cannot all be true

The open question above ("can a repair unit be split?") has a definite answer:
**no — a repair unit is exactly one file** (`coordinator_compaction_files`
returns `.take(1)` for Repair), so there is nothing to split. Which makes the
rest pure arithmetic:

| constant | value | location |
| --- | --- | --- |
| `COORDINATOR_HOT_TARGET_BYTES` / `COORDINATOR_SEALED_TARGET_BYTES` | **256 MiB** compressed | `database/mod.rs:1378-1379` |
| `DECODED_BYTES_PER_COMPRESSED` | **x12** | `database/maintain.rs:150` |
| repair budget = `COORDINATOR_PER_SORT_BUDGET_BYTES` | **1,280 MiB** decoded | `config.rs:222` |

```
one target-sized file = 256 MiB x 12 = 3,072 MiB decoded
repair budget                        = 1,280 MiB
                                     -> 2.4x OVER BUDGET
```

**A file that compaction produced exactly as intended is 2.4x the entire repair
byte budget.** A file fits only if it is under **107 MiB** compressed — i.e. only
files that compaction would consider *too small*. So:

- every repair unit clamps to the whole semaphore,
- repair runs strictly one rewrite at a time,
- the "bins below the budget share it" case the byte-pricing change was written
  for **cannot occur for any correctly-sized file**,
- and no re-measurement or splitting can help, because the unit is one file.

This is not a tuning miss, it is three constants in three files that cannot all
be satisfied at once. The fix is derivable rather than guessed: to let `N` repair
rewrites share, the budget must be at least
`N x target_file_bytes x DECODED_BYTES_PER_COMPRESSED` — **3,072 MiB for one**,
6,144 MiB for two.

**Which also explains the code comment** that priced this as acceptable —
*"prod's worst bin is ~28 GB decoded, far over budget, so it still takes
everything and runs alone. Bins below the budget now share it"* — the author was
reasoning about a pathological 28 GB bin, and did not notice that the ORDINARY
bin is also over budget, by 2.4x. The change was correct in intent and inert in
practice.

**Recommended framing for the morning:** this is the cheapest of the three
decisions to reason about, because it needs no new measurement — the numbers are
constants in the source. The judgement required is only how much memory repair
may hold, given that admission (Decision 2) draws on the same pool.

### Confirmed from three directions, and the stale-estimate nuance

The journal settles both halves. All **312** queued repair tasks share ONE
creation stamp (2026-08-16T11:09:57) and their estimates cluster tightly:

```
312 tasks   median 256 MiB   max 262 MiB
```

**256 MiB is exactly `COORDINATOR_HOT_TARGET_BYTES`** — the compaction target, in
*compressed* bytes. Two things follow, and they are independent:

**1. Each repair unit is exactly one target-sized file.** The tight clustering
proves it. So its real decoded cost is `256 x 12 = 3,072 MiB` against a
**1,280 MiB** budget — the 2.4x over-budget arithmetic above, now confirmed by
production data rather than derived from constants alone.

**2. The stored estimates are missing the x12** — they are compressed bytes in a
field named `estimated_decoded_bytes`. But this is **stale data, not a live
planner bug**: today's planner computes it correctly —

```rust
let estimate = suspects.iter().fold(0u64, |b, f| b.saturating_add(estimated_decoded_bytes(f.size)));
```

so this cohort was stamped before that path (or by another one). It matters
anyway, because **admission decides on the stored number**: 256 MiB passes the
512 MiB ceiling comfortably, while the work it authorises costs 3,072 MiB. That
is precisely how a unit gets admitted and then bounces forever.

There is precedent for exactly this class — `clear_stale_estimates` exists
because earlier estimates "were all measured with a broken ruler" and a
correction that only applies to NEW measurements cannot repair a durable queue
full of old ones. **This cohort appears to have escaped that migration**, which
is worth checking on its own: a one-off re-estimate would at least make
admission's decision honest.

**Final state of Decision 1 — verified three ways:**

| direction | evidence |
| --- | --- |
| source constants | 256 MiB x 12 = 3,072 MiB vs a 1,280 MiB budget |
| prod logs | `want_mib=1280 budget_mib=1280`, 243 bounces in 3h |
| journal data | 312 units, median 256 MiB, one creation stamp |

The budget must exceed one target-sized file's decoded size, because a repair
unit is one file and cannot be split. Everything else — `take(1)`, the estimate
gap, the admission mismatch — sits downstream of that single inequality.

## OPERATIONAL INCIDENT (mine): I published another session's work under my commits

Recording this because it affects what is on master and what prod is running,
and because the mechanism is a trap anyone in this checkout can hit.

**What happened.** A concurrent session was working in the same checkout on
branch `tf-monoscope-compat`. Several of my commits used `git add -A`, which in a
shared working tree stages *their* files too. Four of my "docs:" commits
therefore carried their work, and I pushed it to master:

| my commit | foreign content it carried |
| --- | --- |
| `671be119` | `tests/slt/monoscope_query_shapes.slt` (526 lines, new), `tests/suite/sqllogictest.rs` |
| `6671e4af` | further edits to both |
| `223285e7` | further `.slt` edits |
| `e68106d9` | **`src/rollup.rs` (88 lines)** + `.slt` |

**Consequences.**

1. Their in-progress work was published to master before they chose to publish it.
2. **It triggered a prod deploy.** I had believed my docs pushes were inert
   (`paths-ignore` does cover `docs/**` and `**/*.md`) — and they would have
   been, had they contained only docs. Prod now runs image `e07421d`, which is
   my docs commit as tip carrying their `src/rollup.rs` change.
3. Every "no deploys since X" statement I made tonight is therefore wrong for
   the windows spanning those pushes.

**Verified after the fact:** master builds clean (`cargo check --all-targets`),
and their `monoscope_query_shapes` test passes. Prod is serving normally
(23,465 queries, dedup committing at ~37/hour). **No damage — but that is luck,
not process.**

**Not reverting.** Their work is intentional and may already be built upon;
unpublishing it would be a second uninvited change to someone else's branch.

**The rule that would have prevented it**, and which the existing
`tf_shared_checkout_git_discipline` memory already states: in a shared checkout,
never `git add -A`. Stage explicit paths (`git add docs/`), which is what the
later commits in this session did — and those carried nothing foreign.

## MEASUREMENT CAVEAT: the journal JSON is a periodic checkpoint, not live state

Worth stating precisely, because every journal number in this document depends
on it. `.timefusion_meta/` holds two files:

| file | size | mtime when checked | role |
| --- | --- | --- | --- |
| `maintenance_tasks.wal` | **27.7 MB** | **19:04 — current** | live state, actively appended |
| `maintenance_tasks.json` | 59.2 MB | 17:07 (15:07 UTC) — **~2h stale** | periodic checkpoint |

**I fetched only the JSON.** A third fetch returned a file byte-identical to the
second (same SHA-256), which is what first drew attention: the checkpoint had not
been rewritten in ~2 hours even though the running process had committed 47 dedup
bins. That is the design working, not a fault — `checkpoint()` appends to the WAL
and the JSON is rewritten periodically.

**What this does and does not invalidate:**

- **Stands:** the 4.4-hour cohort diff. Snapshots 1 and 2 were two *genuine*
  checkpoints (different sizes, 63.8 MB → 59.2 MB), so "1,144 of 1,168 stuck
  tasks still queued, 359 with incremented attempts" is a real comparison
  between two real points in time.
- **Stands:** every size/composition statistic — those describe the queue as of
  a checkpoint, which is what they claim.
- **Qualified:** any statement of the form "*right now* N tasks are queued". The
  checkpoint can lag live state by hours, so the true current figure may differ.
- **Unaffected:** the constants arithmetic (256 MiB x 12 vs 1,280 MiB) and the
  prod-log evidence (243 `repair_rewrite_permit_busy` in 3h), neither of which
  comes from the journal.

**For anyone repeating this:** `load_sandboxed` copies **both** files, so a
faithful replay needs the `.wal` too — `docker cp` it alongside the `.json`.
Fetching only the JSON gives a coherent but potentially hours-old queue.

## MEASUREMENT CAVEAT 2: the sim's `pending` counts SUPERSEDED tasks

Fetching both journal files (`.json` + `.wal`) and replaying them gives a live
view, and it exposes an interpretation error running through the sim sections
above.

The sim reports `pending_start: 21,544` for snapshot 1. The journal's own state
counts for that snapshot are:

```
superseded 19,159 + pending 1,248 + retry 1,121 + running 16 = 21,544
```

**Exactly the sim's number.** So `pending` in the sim report means *all
non-complete tasks*, and **~89% of it is superseded bookkeeping, not work.**

Confirmed against the live replay (both files, `--no-mint`, 6 virtual minutes):

```
pending: 21,367 -> 1,968 | executions: 134
```

19,399 tasks disappeared in six virtual minutes on 134 executions — they were
never work; they were retired as superseded/subsumed. The actionable queue is
**~2,000**, which matches counting `pending + retry` directly (2,222).

**What this corrects:**

- The flow-problem section's headline — "21,544 → 16,431, so 21,088 executions
  lost ground" — was counting superseded records. The *actionable* backlog is an
  order of magnitude smaller than that framing implies, which makes "the queue
  grows" a much weaker statement than it appeared.
- It reinforces, rather than undermines, the finding that replaced it: the real
  problem was never queue *size*. It is that **specific units never execute** —
  the ~2,000 actionable tasks, half of them older than 48 hours, with 312 repair
  units bouncing off a byte budget they cannot fit.

**Rule:** in this codebase "pending" means different things in different places.
`timefusion_stats`' `pending_dedup`, the sim's `pending_start`, and
`state == Pending` in the journal are three different populations. Say which one
you mean, and check the arithmetic against the state counts before quoting it.

### Decision 1 confirmed against LIVE state (both journal files replayed)

The checkpoint-staleness caveat above raises the obvious question: is the stuck
repair cohort an artifact of a 2-hour-old snapshot? **No.** Fetching both
`.json` and `.wal` and replaying them gives live state:

| operation | Pending | Retry | Complete | Superseded |
| --- | --- | --- | --- | --- |
| Dedup | 1,009 | 458 | 15,696 | 5,531 |
| BaseRollup | 190 | 170 | 36,530 | 7,880 |
| **Repair** | 0 | **310** | 2,188 | 333 |
| DerivedRollup | 29 | 25 | 4,169 | 5,385 |

**310 repair tasks are in Retry right now** — against 311 survivors measured in
the stale checkpoint. The cohort is not a snapshot artifact and is not draining.

Two further things this settles:

- **Actionable queue = 2,191** (Pending + Retry across all ops), against the
  ~21,000 the sim's `pending` reports. The order-of-magnitude gap is superseded
  bookkeeping, as the previous section established.
- **Repair is not universally broken** — 2,188 repair tasks have completed over
  the journal's life. So the byte budget is passable *sometimes* (when the
  semaphore happens to be free and the file happens to be small enough). What
  the 310 share is that they cannot fit, and nothing re-measures or splits them.
  That is consistent with the constants arithmetic rather than in tension with
  it: files below ~107 MiB compressed fit and complete; target-sized 256 MiB
  files never do.

This is the strongest form of the evidence available without shipping a change:
live state, replayed from the same two files the coordinator itself loads.
