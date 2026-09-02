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
