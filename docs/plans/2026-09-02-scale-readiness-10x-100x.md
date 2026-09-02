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
3. **100x needs units to be SIZED, not split after they fail.** More workers
   cannot fix a unit that cannot finish. The lever is unit *width* chosen up
   front from a cost estimate, so a unit fits its budget by construction —
   consistent with the earlier finding that day-wide sealed units are
   unsatisfiable against a 900s deadline, and that unit cost is dominated by the
   commit, so width is the lever.

## Honest limits of this measurement

- `--scale` models "each unit costs F times more". Real 100x traffic also means
  more units, more streams, and more projects; `--streams` models that arrival
  side separately and is not varied here.
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
