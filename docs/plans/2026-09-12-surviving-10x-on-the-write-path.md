# Surviving 10x on the write path

2026-09-12. Follow-on to
[`2026-09-11-write-latency-self-inflicted-disk-flood.md`](2026-09-11-write-latency-self-inflicted-disk-flood.md),
which took p99 writes from 47.9 s to ~7 s. That fix bought headroom. It does not
survive 10x, and this note is about why and what would.

Every number here was measured on prod at 8 h uptime on `e99e11b1` unless it is
marked as an estimate. Estimates are marked because two sizing arguments in the
previous round were confidently wrong.

## Lead with the divergence, not the latency

**The maintenance backlog grows at 1x.** `backlog_bytes` went 1.061 TB →
1.131 TB over roughly seven hours of this session, on an unchanged workload.
`oldest_task_age_seconds` is 2,217,023 — 25 days.

That reframes the whole question. 10x is not "the disk gets slower and p99
drifts up". At 1x the queue is already draining slower than it fills, so at 10x
it **diverges immediately** and the only question is how fast. Latency is the
symptom that made this visible; the backlog is the thing that decides whether
10x is possible at all.

## What is actually on the local disk — measured, not inferred

The previous round left ~300 MB/s unattributed and guessed at DataFusion spill.
**That guess was wrong.** Listing the directories directly through `docker cp`:

| directory | entries |
| --- | --- |
| `maintenance_spill` | 2 (empty) |
| `light_optimize_spill` | 2 (empty) |
| `repair_spill` | 2 (empty) |
| `coordinator_spill` | 2 (empty) |
| `flush_sort_spill` | 2 (empty) |
| **`tantivy_scratch`** | **109,797** |

Every spill pool is empty. Combined with foyer population measured at
**10.8 MB/s** (`inner_bytes_read` 312 GB / 8 h), essentially **all** of the
~340-794 MB/s of local write traffic is Tantivy index construction.

This is the first attribution in this whole investigation that comes from a
direct measurement of the thing itself rather than arithmetic. Three earlier
candidates — `persist_rollup_journal`, foyer, DataFusion spill — were each
plausible from a code read and each refuted by measurement. Do not re-propose
them.

## The multiplier nobody costed: `md3` is a 4-way MIRROR

```
md3 : active raid1 nvme3n1p3[3] nvme1n1p3[2] nvme0n1p3[0] nvme2n1p3[1]
      1873142784 blocks super 1.2 [4/4] [UUUU]
```

`raid1` with four members. **Every logical byte written to `/` is physically
written to all four NVMe devices.** iostat confirms it directly: md3 at
406 MB/s while each of nvme0-3 independently reports ~408 MB/s — **1.6 GB/s of
device traffic to absorb 406 MB/s of logical writes.**

Two consequences, both load-bearing for 10x:

1. **Four drives buy zero write bandwidth.** The array's write ceiling is one
   drive's, and the observed 95-100% util at ~750 MB/s says we are at it.
2. **1.8 TB usable out of ~6.8 TB raw.** 75% of the capacity is spent on
   redundancy — for a volume whose dominant occupant is *scratch files that are
   deleted minutes later*.

The data on this array is almost entirely reconstructible: Tantivy scratch is
temporary, the foyer cache re-fetches from R2, spill is temporary. Only the WAL
and the maintenance journals genuinely need durability, and they are a rounding
error by volume. **We are paying 4x write amplification to mirror garbage.**

## What binds at 10x

Assumption, stated explicitly because the exponent matters: index and
maintenance volume scale **~linearly with ingested bytes**. Tantivy is built once
per flush commit (`batch_callback` → `build_and_publish`, keyed to the parquet
file), so 10x ingest means ~10x flush files and ~10x index builds. Rollup and
dedup scale with data volume rather than request rate, so they scale at least
linearly too.

| | now (1x) | 10x (projected) |
| --- | --- | --- |
| ingest reaching Delta | 5.45 MB/s | 55 MB/s |
| logical local writes | ~500 MB/s | ~5 GB/s |
| **device writes (×4 mirror)** | **~2 GB/s** | **~20 GB/s** |
| array ceiling | ~1.6-2 GB/s (measured, at 100% util) | unchanged |

**10x is roughly an order of magnitude past the array's write ceiling.** No
amount of scheduling fixes that; scheduling changes who waits, not how many
bytes must land. The byte count and the mirror are the two terms that have to
move.

## Solutions, cheapest first, with what each is worth

### Phase 0 (BLOCKING): per-channel byte counters

`tantivy_scratch_bytes_written`, `spill_bytes_written`,
`journal_bytes_written`, `foyer_admit_bytes` in `timefusion_stats`.

This has now been deferred three times, and each deferral cost a wrong
attribution. The table above rests on a directory listing and arithmetic, not on
a counter; nothing below can be validated post-deploy without them. They are
behaviour-neutral, so they ship with whatever goes next.

### Tier A — do less work (worth ~2-4x, no infra change)

**A1. Stop indexing files that are about to be compacted away.** This is the
biggest single lever and it is pure waste. Indexes are built per flush commit,
on small 10-minute-bucket files that hot-packing and sealed consolidation
rewrite within minutes. Carry-forward already preserves indexes across a rewrite
when every input was covered (`carried=3 rebuilding=0` in the logs — it works),
but the *initial* index on a short-lived file is still built, written, packed,
uploaded, and then superseded. Index at the **sealed/compacted** tier and serve
the recent tail from the mem-buffer and raw scan, which is what the hot path
already does for uncovered intervals. This is the standard "don't index L0"
shape from LSM search engines.

**A2. Build small indexes in RAM.** `verify_blob` was changed in the last round
to stage into a `RamDirectory` instead of materialising to disk; the same
applies to the *build* when an index fits a bounded budget. A `MmapDirectory` on
a mirrored array is the worst possible destination for a file that lives for
seconds.

**A3. Bypass the cache for maintenance scans.** RocksDB's `ReadOptions::
fill_cache=false` exists precisely because compaction reads pollute the cache.
TimeFusion **already has the mechanism** — `scan_bypass_scope` /
`bypass_active()`, plus a `repeat_sighting` second-sighting admission filter in
the TinyLFU family — but it is wired at exactly one production call site
(`src/database/mod.rs:10831`), and `cache_insert_bypassed` is not exposed in
`timefusion_stats`. Small win by volume (foyer is only 10.8 MB/s), listed
because the cost is nearly zero and the mechanism is already built.

### Tier B — schedule the work (worth ~2-3x on latency, ~0 on volume)

**B1. A SILK-style I/O scheduler for maintenance.** SILK (USENIX ATC'19) attacks
exactly this failure — client latency spikes caused by interference from flushes
and compactions — with three techniques: opportunistically give background work
more bandwidth when client load is low, prioritise the lower levels of the tree,
and **preempt** long-running compactions. It reports up to two orders of
magnitude better p99.

TimeFusion has the pools and tokens but no client-latency feedback: maintenance
yields on flush debt, never on the thing we actually care about. The 2026-09-11
memory already prescribed this — "throttle maintenance on a client-latency
signal, not just flush debt". Preemption is the part TF most lacks: a repair bin
is a 40+ minute unit that cannot currently step aside for a latency spike.

**Explicitly NOT cgroup `io.latency`.** cgroup v2's io controllers are
cgroup-granular, and TF's WAL fsync, Tantivy scratch, and spill all issue from
one process in one cgroup — so the controller cannot separate them. It can only
protect TF from co-tenants, which were measured at sub-MB/s and are irrelevant
here. The applicable form of this prior art is in-process.

### Tier C — re-lay the disk (worth ~4x on device traffic)

Split the volume by durability requirement rather than mirroring everything:

- keep the WAL and journals on a **2-way** mirror (still redundant)
- put Tantivy scratch, foyer L2 and spill on a **RAID0** volume built from the
  two freed legs — reconstructible data does not need redundancy

That removes ~4x device amplification from the dominant traffic *and* gives the
ephemeral tier ~2x a single drive's bandwidth, so on the order of 10x effective
headroom for the bytes that actually matter. It also reclaims capacity on a
volume that has been sitting at 82-89% full.

**This is a live-host operation on a box we are read-only on, with real failure
modes mid-migration (`mdadm --fail/--remove` on a degraded array, on a
production ingest path). It is presented as an option with its arithmetic, not
as a recommendation to execute.**

### Tier D — move the work off the box (the only tier that removes the ceiling)

Tiers A-C multiply through to maybe 8-12x of *device* headroom, which makes 10x
survivable but with no margin. Removing the ceiling means maintenance stops
competing with ingest for one machine's disk.

Prior art is mature and points one way: **CaaS-LSM** (SIGMOD 2024) decouples
compaction from the KV store and runs it as a stateless service; **O3-LSM**
(SIGMOD 2026) extends offloading to memtable and flush and reports ~15.5% lower
total write amplification against disaggregated RocksDB; **D2Comp** offloads
compaction to DPUs. The recurring warning in all of them is network
amplification — measured at ~26x in one study, ~23x of it from compaction — so
an offload design has to be costed in network bytes, not just CPU.

TimeFusion already has most of the seam: `timefusion run-unit --source --project
--date --op` runs one maintenance unit standalone against real storage, and
Delta OCC commits make concurrent writers safe. What is missing is that the task
journal is process-local — it would have to become the shared queue.

**This conflicts with the standing single-process, no-k8s directive
(2026-08-03).** That directive is the user's to revisit; the honest statement is
that 10x is reachable without it only with zero margin, and not reachable at all
at 20x.

## Recommended order

1. **Phase 0 counters** — blocking, behaviour-neutral, ship with anything.
2. **A1** (don't index doomed flush files) — biggest volume lever in software.
3. **B1** (latency-fed maintenance scheduling + preemption) — protects p99 while
   A1 lands.
4. **A2/A3** — small, cheap, mechanisms mostly exist.
5. **C** — decide deliberately; large win, real operational risk.
6. **D** — only if 10x is a commitment rather than a projection.

## Sources

- [SILK: Preventing Latency Spikes in Log-Structured Merge Key-Value Stores](https://www.usenix.org/conference/atc19/presentation/balmau) (USENIX ATC '19) — [PDF](https://www.usenix.org/system/files/atc19-balmau.pdf)
- [SILK+ (ACM TOCS)](https://dl.acm.org/doi/10.1145/3380905) — heterogeneous workloads
- [CaaS-LSM: Compaction-as-a-Service for LSM-based Key-Value Stores](https://dl.acm.org/doi/10.1145/3654927) (SIGMOD 2024)
- [O3-LSM: Maximizing Disaggregated LSM Write Performance via Three-Layer Offloading](https://dl.acm.org/doi/10.1145/3802093) (SIGMOD 2026) — [PDF](https://cs.purdue.edu/homes/csjgwang/pubs/SIGMOD26_O3LSM.pdf)
- [D2Comp: Efficient Offload of LSM-tree Compaction with DPUs](https://dl.acm.org/doi/10.1145/3656584) (ACM TACO)
- [RocksDB Block Cache](https://github.com/facebook/rocksdb/wiki/Block-Cache) — `fill_cache` and compaction cache pollution
- [PrismDB](https://arxiv.org/pdf/2008.02352) — compaction pollutes the DRAM cache
- [cgroup2 IO controller](https://facebookmicrosites.github.io/cgroup2/docs/io-controller.html) — why `io.latency` protects *between* cgroups, not within one
