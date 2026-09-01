# What 100x actually requires

The prompt that started the 2026-08-31 night named a real constraint: *"we have
a potential customer who has 100x current total volume."* The throughput work is
written up in `2026-08-31-maintenance-throughput-night.md`. This is the separate
question — is 100x a tuning problem or an architecture problem? — answered with
the same measurements.

**Short answer: 10x is a tuning problem and is now covered. 100x is not a tuning
problem. It is a horizontal-scale problem, and it collides with the standing
single-process directive.** That is a business/architecture decision, not
something to discover during an onboarding.

## The measurements this rests on

All from the live process, 2026-09-01:

| quantity | measured |
|---|---|
| ingest | 1,302 rows/s |
| ingest, decoded | 4.55 MB/s (`flush_freed_bytes_total` / uptime) |
| ingest, compressed | ~0.38 MB/s (~12x ratio) |
| table today | 6,615 files, 1,063 GB, 3.66 B rows, 1,176 (project,date) cells |
| maintenance demand | 3-5 passes over ingest = **14-23 MB/s decoded** |
| maintenance capacity | **19-25 MB/s decoded** at 4 concurrent rewrites, cache-served (`benches/rewrite_throughput.rs`) |
| box | 120 GB RAM (80 GB effective), 48 cores, 16.9 GB maintenance pool, 600 GB foyer disk |

## Scaling the demand

| | ingest rows/s | decoded MB/s | compressed/day | maintenance demand (decoded) |
|---|---|---|---|---|
| 1x (today) | 1,302 | 4.55 | ~33 GB | 14-23 MB/s |
| 10x | 13,020 | 45.5 | ~330 GB | 137-228 MB/s |
| **100x** | **130,200** | **455** | **~3.3 TB** | **1,365-2,275 MB/s** |

Against a measured capacity of 19-25 MB/s, 100x needs **roughly 60-100x more
maintenance throughput than the box delivers today.**

## Why 10x is reachable and 100x is not, on one box

**10x (137-228 MB/s demand).** The concurrency curve says throughput peaks at 4
workers on an 8 GB pool and *collapses* at 8 — but the collapse is a pool limit,
not CPU: at 16 GB, 8 workers run clean. So the lever exists. 48 cores can carry
~12-16 concurrent sorts if each is given memory, and the maintenance pool is
16.9 GB of an 80 GB effective limit. Combined with the per-unit gains already
shipped (batch sizing, one-pass repair, byte-capped row groups) this is a
tuning exercise with headroom, and the honest bound is ~4x more concurrency
times ~2x per-unit efficiency — enough for 10x, not for 100x.

**100x fails on three independent limits, and only one is tunable:**

1. **Memory.** 1,365-2,275 MB/s of decoded throughput at a ~2 GB working set per
   concurrent sort implies dozens of concurrent rewrites. The whole box is
   80 GB effective with 16 GB for maintenance. Even spending the entire box on
   maintenance does not close a 60-100x gap.
2. **Cache.** 3.3 TB/day compressed against **600 GB of foyer disk**. The
   working set for a day would not fit, so the read tier degrades from
   local-disk (8.81-24.66 MB/s per worker measured) to network
   (0.94-3.52 MB/s measured) — the wrong direction by ~7x, exactly when more is
   needed. Cache sizing is not a knob here; 3.3 TB/day needs a different disk
   budget than one host has.
3. **Object storage.** 455 MB/s decoded sustained, plus rewrite amplification,
   is multi-GB/s against OVH from a single host. Nothing in this codebase makes
   one host's NIC bigger.

## What this means, concretely

- **10x: proceed.** The levers are pool size and per-unit memory, both already
  identified, both measured. The occupancy-scaled admission ceiling is now
  always on, so saturation degrades by admitting small work rather than by
  killing large work — the failure mode that cost the whole 2026-08-31 night.
- **100x: do not sell it as a tuning change.** It needs either (a) that tenant
  on its own host — which the multi-tenant storage model already supports via
  custom project tables at their own S3 path — or (b) relaxing the
  single-process directive (`tf_single_process_directive_2026-08-03`) for
  maintenance specifically. Option (a) is much cheaper and is the shape the
  codebase is already built for: a whale tenant on dedicated storage with its
  own coordinator is a configuration, not a rewrite.
- **The measurement to take before committing to anything:** ingest-side, not
  maintenance-side. Everything above assumes 100x ingest lands the same way
  today's does. The WAL, MemBuffer and flush path have their own ceilings
  (`TIMEFUSION_BUFFER_MAX_MEMORY_MB`, WAL fsync-per-append) and none of them were
  exercised tonight. **Maintenance was the bottleneck at 1x; it will not be the
  first thing to break at 100x.**

## The prior art agrees on the shape

From `2026-08-31-how-other-systems-schedule-maintenance.md`: every system
surveyed bounds background work by bytes/rows/files and reserves capacity for
the lanes that gate ingest (SILK's 50-of-200 MB/s floor for flush + L0→L1;
Druid's guaranteed-one-task floor). None of them scale a single node to 100x by
tuning; they shard. ClickHouse's occupancy-scaled cap and RocksDB's write stalls
exist precisely so a node that *cannot* keep up degrades predictably instead of
collapsing — which is the property to want for a 100x tenant on shared
infrastructure, whatever the capacity decision.
