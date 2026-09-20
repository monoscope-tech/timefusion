# Why maintenance left cores idle with a backlog

Status: locally implemented and validated; the production image measured below
predates these changes. Production verification therefore remains required.

## Conclusion

Maintenance did not fill the 44-core cgroup because an unrelated admission
dimension capped the entire coordinator at 28 in-flight units. The server had
66 coordinator workers, 44 maintenance runtime threads, and a lag-scaled CPU
ceiling of 66, but object-read and object-write capacity were each initialized
from `coordinator_jobs * 2`, or 28. Every unit reserves one read and one write
token for its whole lifetime, including time parked on object storage.

The live signature was exact: CPU, read, and write reservations repeatedly
moved together and reached `28 / 28 / 28`. Runtime scheduling lag stayed near
zero, decoded reservations were about 9.1 GB, and container RSS was only
21.5-29 GiB of 120 GiB. Neither foreground starvation nor memory pressure was
asking maintenance to back off.

This is why the expected work-conserving behavior did not occur. The CPU
controller could raise its limit to 66, but no 29th unit could pass the I/O
checks.

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
