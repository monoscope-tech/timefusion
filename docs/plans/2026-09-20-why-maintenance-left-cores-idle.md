# Why maintenance left cores idle with a backlog

Status: the first I/O-slot fix is deployed. Its production result exposed a
second, independent memory-accounting/quarantine failure. The follow-up fixes
described below are implemented locally and still require deployment.

## Conclusion

Maintenance did not fill the 44-core cgroup for two consecutive reasons. The
original build capped the entire coordinator at 28 in-flight units because
object-read and object-write admission were both 28. After that was raised to
66, the startup wave overran a coordinator memory pool still sized for only 14
jobs (7 GiB), and its allocation failures quarantined most of the apparently
eligible rollup queue behind one permit.

The live signature was exact: CPU, read, and write reservations repeatedly
moved together and reached `28 / 28 / 28`. Runtime scheduling lag stayed near
zero, decoded reservations were about 9.1 GB, and container RSS was only
21.5-29 GiB of 120 GiB. Neither foreground starvation nor memory pressure was
asking maintenance to back off.

Thus both deployed states violated work conservation: first no 29th unit could
pass I/O admission; then admission allowed far more decoded work than the real
DataFusion pool could hold and converted the resulting transient failures into
long-lived scheduler exclusion.

## Production snapshot

The measurements came from the production image started at
2026-09-20T19:01:44Z. That image does not include this patch.

| Signal | Observed |
|---|---:|
| cgroup CPU quota | 44 logical cores |
| coordinator worker slots | 66 |
| maintenance runtime threads | 44 |
| adaptive CPU-token maximum | 66 |
| object-read capacity | 28 |
| object-write capacity | 28 |
| sampled CPU/read/write reservations | equal; repeatedly 28/28/28 |
| foreground runtime lag | usually 0-2 ms |
| decoded reservations | about 9.1 GB |
| container memory | about 21.5-29 GiB / 120 GiB |
| sampled total CPU | 21.5-39.0 cores; mean about 32.8 |
| estimated maintenance backlog | 2,018,872,899,886 decoded bytes |
| sealed consolidation debt | 1,862,957,274,084 decoded bytes |
| oldest unit | about 31.9 days |

The backlog number is an estimate of decoded bytes, not compressed object
bytes, and it falls only when tasks retire. It must not be divided by an
instantaneous object-byte rate to manufacture a precise ETA.

## Lost work beyond the hard cap

A ten-minute completion sample also showed large amounts of zero-work churn:

| Operation | Useful completions | Retries | Zero-second retries |
|---|---:|---:|---:|
| BaseRollup | 351 | 2,268 | 2,268 |
| Dedup | 44 | 318 | 300 |
| DerivedRollup | 20 | 102 | 102 |
| HotPacking | 2 | 19 | 15 |
| SealedConsolidation | 0 | 10 | 9 |

Since process start, `source_not_flushed` accounted for 49,771 BaseRollup and
9,859 Dedup retries. A sealed slice whose source remained buffered was retried
at a flat five-second cadence. Those claims acquire the journal lock, rank the
queue, append another retry record, and release the worker without reducing
debt.

The retry delay now uses the larger of:

- the time until the slice can be finalized;
- a five-second first retry; and
- exponential transient backoff, capped at 64 seconds.

This preserves a prompt first retry while preventing a persistent prerequisite
from consuming coordinator and journal capacity every five seconds forever.

## Implemented changes

1. Object-read and object-write admission capacities now equal
   `coordinator_job_slots()`. On the measured 44-core box this changes both from
   28 to 66, making the existing CPU ceiling reachable.
2. Startup logs now publish coordinator jobs, worker slots, I/O slots, and
   runtime threads together.
3. Metrics now publish capacity as well as utilization for CPU, decoded memory,
   object reads, and object writes.
4. Admission refusals are counted independently for CPU, decoded memory,
   object reads, and object writes. If two dimensions bind, both counters rise.
5. Persistent `source_not_flushed` retries now back off to at most 64 seconds.
6. `admission_busy` retries use a fixed five-second delay instead of the task's
   lifetime attempt count. Capacity can refill within five seconds after a lag
   spike instead of waiting as long as 64 seconds; prerequisite failures retain
   exponential backoff. At the measured worst floor, 52 rejected workers imply
   at most about 10.4 retries/second. At the observed 5.5 ms average claim cost,
   that is roughly 57 ms/second of serialized claim work—close to the existing
   retry rate, while cutting worst-case refill latency by about 59 seconds.
7. While sealed consolidation is pending, current-day packing is capped at one
   third of the light rewrite pool (at least one lane). On the observed
   five-permit production shape this leaves four lanes available to sealed work
   and one to the open day. The cap is bypassed when sealed debt reaches zero,
   so it does not strand capacity after catch-up. Coordinator refusals and
   cron-wave waits at this gate are exported separately.
8. Sealed hygiene now folds file-count benefit into its age score. Previously,
   crossing the three-day starvation threshold was a strict ordering dimension:
   a three-file old cell could outrank a newly sealed cell with hundreds of
   files. Benefit now improves the same bounded score while age continues to
   gain one step per day past the 31-day horizon, so large fragmentation is
   attacked promptly without making small old cells permanently unreachable.

The maintenance runtime remains isolated and lower-priority. Its 500 ms
scheduling-lag feedback comes from the foreground Tokio runtime. With lag at or
below 25 ms, maintenance may use the full ceiling; from 25-250 ms the ceiling
interpolates downward; at 250 ms it returns to the previous conservative floor.
This lets idle cores work while retaining a direct query-starvation brake.

## What this does not change

Sealed consolidation had all five light rewrite permits occupied. Those permits
are derived from a fixed memory pool and a 1.25 GiB per-sort envelope. Increasing
them merely because RSS looked low would give each concurrent DataFusion sort
less pool memory, increase spill, and risks repeating the prior
`Resources exhausted` failures. The I/O-cap fix therefore does not alter the
sort permit count or per-sort budget.

This distinction matters: the patch should raise total runnable maintenance
and fill otherwise idle cores, but sealed-debt throughput can remain bounded by
five long-running sorts. Core saturation is evidence that global scheduling is
work-conserving; it is not, by itself, proof that the dominant sealed lane is
efficient.

## Deployment experiment and acceptance gates

Deploy this change before estimating the new drain time. Compare equal mature
windows, not the restart burst.

1. Confirm the startup line reports 66 job workers, 44 runtime workers, and 66
   I/O slots on the current production shape.
2. During an eligible backlog, verify object read/write utilization can exceed
   28 and their refusal counters do not rise while capacity remains.
3. Measure cgroup `usage_usec`, `nr_throttled`, and `throttled_usec` over fixed
   intervals. Success is sustained CPU close to the 44-core quota, or a named
   downstream limiter visible in the new refusal metrics.
4. Require foreground scheduling lag and query latency to remain inside their
   existing budgets. The adaptive ceiling should retreat if they do not.
5. Compare useful completions and committed input bytes by operation. Do not use
   task starts, retries, or the stock backlog estimate as throughput.
6. Verify `source_not_flushed` retry rate falls sharply without increasing the
   age of newly eligible sealed work.
7. Recompute ETA only after at least one stable multi-hour window. Use net debt
   retirement: useful completion bytes minus newly planned debt bytes.

If CPU is still materially below quota after I/O utilization exceeds 28, the
next decision follows the counters:

- decoded-memory refusals: reduce unit size or improve phase-aware memory
  pricing;
- CPU refusals with low runtime lag: revisit token lifetime because a unit
  currently holds its CPU token while parked on I/O;
- no admission refusals and five busy light permits: optimize sealed-bin work or
  its spill behavior, not global worker count;
- no refusals and idle workers: inspect claim eligibility and dependency state;
- high foreground lag: the controller is intentionally protecting queries.

Until the patched build produces that window, there is no defensible new ETA.
The previous state was not converging reliably, and extrapolating it would only
estimate how long the known 28-slot bottleneck persists.

## Storage truth and the sealed lane

The coordinator's decoded-byte backlog gauge is not the physical backlog. A
read-only Delta snapshot census at versions 623217, 623236, 623252, and 623382 found
the actual fragmentation concentrated in the newest sealed dates:

| Date | Active files | Compressed bytes | Projects | Projects over the <=2-file policy |
|---|---:|---:|---:|---:|
| 2026-09-18 | 304 | 3.85 GiB | 12 | 9 |
| 2026-09-19 | 1,250 | 3.28 GiB | 11 | 10 |
| 2026-09-20 (open) | 199-202 | 2.51-2.60 GiB | 12 | 8-9 |

After the UTC date boundary, version 623382 showed 2026-09-20 newly sealed at
166 files / 2.65 GiB with six projects out of policy. Hot work had reduced that
date from 202 files, while 2026-09-19 still had not moved.

That split outcome identified an ordering problem in addition to permit
competition. Sealed ranking compared the three-day age band before file-count
benefit. Therefore every older out-of-policy cell, however small, could outrank
the 1,250-file 2026-09-19 day. The new combined age/benefit score directly
targets the physical debt that dominates scan and metadata cost.

Older dates were generally 10-35 files fleet-wide. Across all four censuses,
the 2026-09-19 row remained exactly 1,250 files and 3.28 GiB even though 165
Delta versions committed. Thus the worst sealed day had zero physical drain in
that interval. The 1.85 TiB `sealed_consolidation` gauge is an
estimated decoded task cost and is inflated by durable queue state; it is not
1.85 TiB of live parquet waiting to be compacted.

A fifth census at Delta version 623487 made the result worse, not merely flat:
2026-09-19 increased from 1,250 to 1,252 active files while 105 more Delta
versions committed. The newly sealed 2026-09-20 day moved only 166 -> 163 files.
The deployed build is therefore still not draining the worst sealed debt; at
the observed net rate its ETA is infinite. The lane-fairness and benefit-rank
fixes in this worktree are not present in that build.

All five light-rewrite permits were occupied during the same investigation.
Permit watchdogs reported sealed staging at 300, 900, 2,700, 4,500, and 6,300
seconds and hot packing at 300, 900, and 1,800 seconds. Observed bins ranged
from tiny multi-file rewrites to 200-525 MB inputs that took roughly 110-240
seconds to stage. A one-file Pack is intentional only for an unsorted L0 file:
it creates a sorted run but does not reduce file count. Two near-target inputs
may also legitimately exceed the 256 MiB packing target because output files
are capped at 512 MiB; such a pair can still collapse to one output.

The shared semaphore previously had no lane fairness. Hot packing could take
all five permits even while sealed work was pending. Rotation of task claims is
not sufficient fairness once a long-running unit owns a permit for minutes or
hours. The conditional one-third hot cap makes the scarce stage work-conserving
for catch-up: it preserves today's progress but prevents it from excluding the
sealed lane whose physical file count was not moving.

The staging events previously exposed only `pass=Pack`, so hot-tail work and
sealed consolidation were indistinguishable. They now include the coordinator
operation and partition date. This makes permit allocation and useful drain by
date measurable after deployment instead of inferred from snapshots.

## Secondary control-loop effect

One foreground-runtime lag sample reached 326 ms. The admission ceiling uses
the latest sample, so that event temporarily reduced the CPU ceiling from 66
to the conservative floor of 14. Existing work was not cancelled, but rejected
tasks used attempt-based backoff capped at 64 seconds. Utilization subsequently
moved from 14 toward 18 rather than refilling instantly. This is secondary to
the old 28-slot I/O cap, but it explains short under-filled periods after a lag
spike. Admission retries now use a fixed five-second delay to remove that refill
tail without making persistent prerequisites spin. The new per-dimension refusal
counters are required before changing the lag controller: a high foreground lag
is a valid reason not to saturate cores.

## Post-deploy finding: the 7 GiB pool poisoned the runnable queue

The 66-slot I/O fix deployed at 2026-09-20 21:48 UTC and proved that I/O
admission had been a real cap: startup reported 44 runtime threads, 66 workers,
and 66 read/write slots. It did not produce sustained core saturation. After
the startup burst, only 12-16 CPU/read/write reservations were normally held
despite a live CPU limit of 66 and hundreds of queued units.

The reason was a second configuration invariant that the slot change had not
updated:

| Quantity | Deployed value |
|---|---:|
| worker and I/O slots | 66 |
| admission decoded capacity | about 90 GiB |
| actual coordinator `FairSpillPool` | 7.0 GiB |
| coordinator errors in the post-start burst | 159 |
| BaseRollup `worker_error` retries accumulated | 113 |
| quarantine permits | 1 |

The errors were not ambiguous. They were DataFusion allocation failures such
as `Failed to allocate additional 16.1 MB for ExternalSorter` with only a few
MiB left in `fair(pool_size: 7.0 GB)`. They clustered immediately after the
new process admitted its startup wave and stopped after the wave collapsed.
Admission reported no decoded-memory refusals because it was comparing the
requests with 75% of the whole cgroup, not with the pool the queries actually
allocate from.

This also explains why the system stayed under-filled after memory recovered.
The allocation wording was absent from `is_capacity_failure`, so `TaskLease`
classified the failures as generic `worker_error`. After two attempts those
units are quarantined and can use only the single quarantine permit. The
existing `eligible_base_rollup` gauge counts a passed deadline even when a unit
is quarantined, so its value of 77 overstated the work available to ordinary
workers. Twelve running tasks plus one quarantine lane matches the observed
13 CPU tokens much more closely than the headline queue depth does.

The local follow-up restores the invariant end to end:

1. The coordinator pool scales from the 66 worker slots, capped by the existing
   maintenance share and by floors for the heavy and six-sort light lanes. On
   the 120 GiB / 44-core production shape it grows from 7 GiB to about 13.5 GiB
   without increasing total process memory or shrinking either sibling below
   its established envelope.
2. Admission's decoded capacity is derived from that coordinator pool, not 75%
   of the entire cgroup. It uses the existing measured-safe conversion of 1.79
   decoded input bytes per pool working byte, so it does not confuse input size
   with resident sort state. A burst is refused before DataFusion runs out of
   pool without needlessly serializing spillable work.
3. Heavy compaction remains capped at the measured safe optimum of six sorts.
   The larger pool is for narrow rollups and their S3 waits, not permission to
   multiply memory-heavy sort fan-out.
4. DataFusion's production `Failed to allocate additional ...` wording is now
   a capacity failure. It retries/splits through the capacity path and never
   marks a healthy unit as `worker_error` quarantine.
5. New `tasks_quarantined` and `tasks_due_nonquarantined` gauges expose the
   difference between queue depth and work ordinary workers can actually take.

This is deliberately memory-bounded, not an unconditional promise of exactly
44 busy cores. The 13.5 GiB working pool represents about 24 GiB of decoded
input at the measured safe conversion (47 maximum-sized units in aggregate),
although the occupancy rule deliberately stops admitting maximum-sized units
before the pool fills; smaller units can fill all 44 runtime threads. That is
the correct work-conserving behavior: saturate CPU when the admitted workload
fits, and publish decoded-memory refusal when memory—not an invisible pool OOM
and quarantine—is the reason it cannot.

## Current ETA

There is still no responsible catch-up ETA. The authoritative worst-day sample
showed zero net file retirement, and the patched scheduler has not run in
production. After deployment, estimate ETA from at least a stable multi-hour
window using the change in active files/bytes for sealed dates, subtracting new
arrivals. If the 2026-09-19 file count still does not fall while CPU is saturated,
the next bottleneck is the five-permit staging lane rather than global admission.
