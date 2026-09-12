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

Spill was re-checked six times over 50 s *while the device sustained 507 MB/s*
(cgroup `wbytes` delta) and was empty in every sample — so this is not a
snapshot artefact of transient files. **Spill is exonerated.**

**But Tantivy is exonerated too, and that killed this note's first conclusion.**
`tantivy_index_built` logs `index_bytes`. Over 60 minutes: **203 builds,
3,033 MB of index output total = 0.84 MB/s**, largest single index 324 MB. Even
at a generous 3x build-time amplification that is ~2.5 MB/s. Tantivy cannot be
500 MB/s, and `tantivy_scratch` holding 109,797 files turned out to be just
**3 live build directories**, not a leak.

### The leading candidate: write-capture churn in a permanently full foyer L2

Read-side foyer counters are small (`inner_bytes_read` 10.8 MB/s), which is what
made foyer look innocent twice. **The write side is not counted there at all.**
`CachingMultipartUpload` tees every completed upload into the cache — so every
parquet file maintenance writes to R2 is *also* written to local L2.

The arithmetic is consistent for the first time:

| observation | value |
| --- | --- |
| device write rate (cgroup `wbytes`, 50 s) | **507 MB/s** |
| foyer evictions (45 s delta) | 191 → **4.2/s** |
| `l2_used_bytes` | **638.66 GB, pinned** (cap is 600 GB) |
| mean entry size (638 GB / 2,067 entries) | ~309 MB |
| 4.2/s × ~120 MB | **~504 MB/s** |

L2 is permanently over cap, so every admission forces an eviction: each
maintenance output costs a full-size local write plus a full-size eviction,
forever, for data that is mostly never read again. `cache_recent_days = 35`
admits essentially everything maintenance produces.

**Confidence: this is the leading hypothesis with consistent arithmetic, not an
established fact.** Four candidates have now been killed by measurement
(`persist_rollup_journal`, foyer *reads*, DataFusion spill, Tantivy) and two of
those looked equally solid before they fell. The counters in Phase 0 are what
settle it — specifically a `foyer_admit_bytes` split by admission source
(write-capture vs read-miss).

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

## The current 500 MB/s is NOT all steady state — a backfill is running

Two distinct Tantivy workloads are running, and conflating them would badly
misprice 10x:

1. **Steady state** — one index per flush commit (`batch_callback` →
   `build_and_publish`, keyed to the parquet file). Scales ~linearly with ingest.
2. **Catch-up backfill** — `tantivy_coverage_census` reports
   **`uncovered=783` (today=207, week=184, older=392)**, and
   `tantivy_backfill_unit` fires steadily. Backfill runs
   `build_parquet_and_pack` over *whole committed historical files*, which are
   far larger than a 10-minute flush bucket.

**The naive extrapolation "500 MB/s × 10" is therefore wrong**, and an earlier
draft of this note made it. A large but currently unmeasured share of today's
local write traffic is one-time catch-up over 783 files, not a per-ingested-byte
cost. Splitting these two is the single most important number nobody has, and it
is precisely what the Phase 0 counters below exist to produce.

What can be said without that split:

- **Steady-state indexing scales linearly with ingest.** 10x ingest → ~10x flush
  files → ~10x index builds. Whatever fraction *f* of today's 500 MB/s is
  steady state becomes `10 × f × 500 MB/s`.
- **Backfill inflow is ongoing, not a retiring debt.** The census breaks down as
  `today=207` — 207 of the 783 uncovered files were created *today*, so files
  keep entering the uncovered set rather than the set simply draining. Whether
  it converges depends on whether backfill throughput exceeds that inflow, which
  is exactly what the counters must measure.
- **The mirror multiplies whatever the answer is by 4.**

Stated in **logical** terms throughout, since the ×4 applies only to device
traffic:

| | now (1x, measured) | 10x |
| --- | --- | --- |
| ingest reaching Delta | 5.45 MB/s | 55 MB/s |
| logical local writes | **~507 MB/s** | ~10× whatever is ingest-proportional |
| **logical ceiling** | **~0.8 GB/s** — one drive's worth, because RAID1 | unchanged |
| device traffic at that ceiling | ~3.2 GB/s across 4 drives | unchanged |

We are already at **~63% of the logical ceiling**, and the peak sample (794 MB/s
at 100% util, `w_await` in the hundreds of ms) is essentially *at* it. Any
ingest-proportional term multiplied by 10 blows straight through.

Scheduling cannot fix this, because scheduling changes who waits, not how many
bytes must land. **The byte count and the mirror are the two terms that have to
move.**

## Solutions, cheapest first, with what each is worth

### Phase 0 (BLOCKING): per-channel byte counters

`tantivy_scratch_bytes_written` **split by build kind (flush vs backfill)**,
plus `spill_bytes_written`, `journal_bytes_written`, `foyer_admit_bytes` in
`timefusion_stats`.

The flush/backfill split is the load-bearing one: it produces *f*, and without
*f* the 10x model has an unbounded term in it. Everything else below is sized
against a directory listing and arithmetic, not a counter.

This has now been deferred three times and each deferral bought a wrong
attribution — `persist_rollup_journal`, then foyer, then spill, and in this very
note an extrapolation from a backfill-contaminated number. The counters are
behaviour-neutral; they ship with whatever goes next.

### Tier A — do less work (worth ~2-4x, no infra change)

**A0. Stop write-capturing maintenance output into L2.** If the attribution
above holds, this is the whole problem and the cheapest fix in this document.
Maintenance rewrites files constantly; teeing each rewrite into a cache that is
already over capacity buys a cache entry that is evicted almost immediately,
at the cost of a full-size local write *and* a full-size eviction.

RocksDB is the precedent in both directions: compaction **reads** take
`fill_cache=false`, and compaction **output** is not pushed into the block cache
on the theory that freshly-written data is not necessarily hot. TimeFusion does
the opposite by default.

Three dials, cheapest first: exclude maintenance-authored uploads from the tee
(the writer knows it is maintenance); tighten `cache_recent_days` from **35** so
the window actually filters; and stop running L2 **38 GB over its 600 GB cap**,
which guarantees an eviction per admission. Note the last one alone is not
enough — a smaller cache still writes every admission.

**A1. Coarsen index granularity above the flush file.** Indexes are built per
flush commit, on small 10-minute-bucket files.

*A correction worth recording, because the obvious version of this lever is
wrong:* those indexes are **not** thrown away when compaction rewrites the file.
`carry_forward_after_compaction` (`search.rs:1516`) extends `covered_files` on
the existing entries to include the compacted outputs — no rebuild
(`carried=3 rebuilding=0` in the logs). So "stop indexing doomed files" is not
the lever; the flush index is retained and reused.

The real cost of flush-file granularity is twofold. Each build pays fixed
per-index overhead (schema, dictionaries, segment metadata, tar+zstd, upload)
that multiplies with index *count* rather than data volume. And the indexes
accumulate: `indexes_per_query` is **10**, and every carry-forward sets
`ordinals_valid = false`, giving up the row-ordinal optimisation. At 10x there
are 10x as many small indexes — both build volume and query fanout degrade.

Building at a coarser unit (per project-day, or at the sealed tier) and serving
the recent tail from the mem-buffer and raw scan — which the hot path already
does for uncovered intervals — attacks count-proportional overhead and fanout
together. This is the "don't index L0" shape from LSM search engines.

**A1b. Size and bound the backfill.** 783 uncovered files are being indexed
concurrently with ingest, against a disk that is the binding constraint. Whether
this is a debt that retires or a permanent standing queue is unknown and
decides whether it belongs in the 10x model at all.

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

1. **Phase 0 counters** — blocking, behaviour-neutral, ship with anything. The
   one that matters most is `foyer_admit_bytes` split by source, because it
   confirms or kills A0 in a single reading.
2. **A0** (stop write-capturing maintenance output) — if the attribution holds,
   this is most of the 507 MB/s, and it is a config-and-predicate change rather
   than an architecture change.
3. **B1** (latency-fed maintenance scheduling + preemption) — protects p99
   regardless of which attribution wins, since it is agnostic to *which*
   background work is flooding.
4. **A1/A1b/A2/A3** — index granularity, backfill bounding, RAM builds, scan
   bypass. Real but second-order next to A0.
5. **C** (RAID split) — decide deliberately; large win, real operational risk.
6. **D** (offload) — only if 10x is a commitment rather than a projection.

**A caution this document earned.** Four attributions have now been killed by
measurement in two days: `persist_rollup_journal` (file was 105 KB, not 15 MB),
foyer *reads* (240x too small), DataFusion spill (empty across six samples under
load), and Tantivy (0.84 MB/s of output). Each was plausible from a code read;
two were asserted confidently in a committed document before being refuted. **Do
not implement A0 on the strength of its arithmetic — ship the counter first and
read it.** That ordering is the whole lesson.

## Sources

- [SILK: Preventing Latency Spikes in Log-Structured Merge Key-Value Stores](https://www.usenix.org/conference/atc19/presentation/balmau) (USENIX ATC '19) — [PDF](https://www.usenix.org/system/files/atc19-balmau.pdf)
- [SILK+ (ACM TOCS)](https://dl.acm.org/doi/10.1145/3380905) — heterogeneous workloads
- [CaaS-LSM: Compaction-as-a-Service for LSM-based Key-Value Stores](https://dl.acm.org/doi/10.1145/3654927) (SIGMOD 2024)
- [O3-LSM: Maximizing Disaggregated LSM Write Performance via Three-Layer Offloading](https://dl.acm.org/doi/10.1145/3802093) (SIGMOD 2026) — [PDF](https://cs.purdue.edu/homes/csjgwang/pubs/SIGMOD26_O3LSM.pdf)
- [D2Comp: Efficient Offload of LSM-tree Compaction with DPUs](https://dl.acm.org/doi/10.1145/3656584) (ACM TACO)
- [RocksDB Block Cache](https://github.com/facebook/rocksdb/wiki/Block-Cache) — `fill_cache` and compaction cache pollution
- [PrismDB](https://arxiv.org/pdf/2008.02352) — compaction pollutes the DRAM cache
- [cgroup2 IO controller](https://facebookmicrosites.github.io/cgroup2/docs/io-controller.html) — why `io.latency` protects *between* cgroups, not within one
