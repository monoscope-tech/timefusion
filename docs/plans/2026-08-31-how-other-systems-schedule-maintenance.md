# How other systems schedule background maintenance — and what it says about ours

Prior-art survey run on 2026-08-31 while diagnosing the maintenance backlog
(see `2026-08-31-maintenance-throughput-night.md`). Sources are primary: source
code, official docs, and papers. Only the conclusions that transfer are kept.

## The headline: our 900s deadline is the one thing nobody does

Every system surveyed sizes a unit of background work by **bytes, rows, files,
or a memory pool — never by seconds.** Wall clocks do exist, but only in two
roles, and neither is ours:

| role | mechanism | who |
|---|---|---|
| **sizing** — make the unit finish | bytes / rows / files / pool caps | RocksDB `max_compaction_bytes` (~1.6 GB); ClickHouse free-slot size cap; IOx `max_compact_size` = 3x target; Iceberg `max-file-group-size-bytes` = 100 GB; Druid & Pinot 5M rows |
| **liveness** — detect a wedged worker | a clock that fires only on **zero progress**, then quarantines | IOx `--compaction-partition-timeout-secs=1800`, progress-checked; Pinot `timeoutMs=3600000` **with `maxAttemptsPerTask=1`** + watermark re-derivation |
| **responsiveness** — big work must not hurt latency | preempt/pause-resume, reserved pools, occupancy-scaled caps | SILK; Luo & Carey; RocksDB BOTTOM pool; ClickHouse free-entry thresholds; Druid priority preemption |

**Nobody abandons partial work on a timer and re-runs the identical slice.**
That is exactly what `operation_deadline_secs` + `abandon_running` does, and it
is why all 432 active repair units carry `retry_reason = worker_error`.

## The ten rules worth taking

1. **Size a unit by two caps, whichever trips first, both read from metadata.**
   IOx: 20 files **or** 300 MB, decided from catalog rows with **no file
   opens**. RocksDB: `max_compaction_bytes = 25 x target_file_size_base`.
   Iceberg: `max-file-group-size-bytes`. Druid/Pinot: 5M rows per output
   segment, chosen because a segment is a unit of query parallelism.

2. **Scale the admission cap by pool occupancy, so a busy pool admits only
   small work.** ClickHouse computes
   `max_merge_bytes = 1 MB * (150 GB / 1 MB) ^ (free_slots / 8)` — 150 GB with
   8+ free slots, ~4 MB with one — *"to allow small merges to process, not
   filling the pool with long running merges."* This **subsumes a deadline**: an
   oversized unit is never admitted where it would be killed.
   <https://github.com/ClickHouse/ClickHouse/blob/master/src/Storages/MergeTree/Compaction/CompactionStatistics.cpp>

3. **A wall clock is a liveness detector and must check progress.** IOx:
   `NoWorkTimeOut` → quarantine; `SomeWorkTryAgain` → `Ok(())`, keep going.
   Pinot's 1h timeout is only safe because `maxAttemptsPerTask=1` plus a
   ZooKeeper watermark makes abandonment cheap.

4. **Quarantine on the failure's CLASS, not an attempt count, and record the
   budget that was exceeded.** IOx: `OutOfMemory | Timeout | Unknown → skip`;
   **`ObjectStore → never skip`**, so transient infrastructure cannot poison a
   partition. Its `skipped_compactions` row carries
   `(num_files, limit_num_files, estimated_bytes, limit_bytes)`. Iceberg bounds
   the treadmill with `partial-progress.max-failed-commits` — a cap on *failed
   commits*, not attempts.

5. **Answer an oversized unit by splitting its INPUT and committing each piece,
   never by extending its clock.** RocksDB subcompactions cut on sampled anchors
   at `max(total/N, output_target_file_size)` and install one atomic commit. IOx
   vertical-splits L0 by time using a uniform-distribution model plus L1
   boundary hints. Iceberg's `partial-progress.enabled` commits per file group.

6. **Rank by benefit-per-byte-written, not by age — and make it a switchable
   policy.** RocksDB's default `kMinOverlappingRatio` is
   `overlapping_next_level_bytes / file_size`, and it *replaced* age ordering as
   the default in 6.0. Iceberg exposes `rewrite-job-order` =
   `bytes-asc | bytes-desc | files-asc | files-desc`. ClickHouse scores
   `sum_size / (count - 1.9)`, an explicit model of time-averaged part count.
   Age belongs as an escalation floor, not the primary key.

7. **Reserve capacity for the small, latency-critical work, and cap total
   background work as a fraction of capacity with a guaranteed >= 1 floor.**
   RocksDB's `Env::Priority::BOTTOM` pool; ClickHouse's
   `number_of_free_entries_in_pool_to_lower_max_size_of_merge = 8`; SILK's
   reserved 50 of 200 MB/s for flush + L0→L1; Druid's
   `compactionTaskSlotRatio = 0.1` **with "at least one task is always
   submitted."**

8. **Preempt and resume; never abandon on a timer.** SILK pauses and resumes
   higher-level compactions and promotes L0→L1 into the flush pool. Luo & Carey's
   greedy scheduler "pauses the previous active merge and activates the new
   one." Druid preempts by lock **except inside the segment-publishing critical
   section** — i.e. commit is the one non-preemptible phase.

9. **Derive the queue from durable state so retry is free and needs no
   counter.** Druid re-derives eligibility from `lastCompactionState` every 30
   min; a segment already matching the current spec is skipped forever. Pinot
   resumes from a watermark advanced only on success. IOx writes to object store
   then commits one catalog transaction, with a 14-day GC. Delta OPTIMIZE
   bin-packing is explicitly idempotent.

10. **Judge the system by BACKLOG TRAJECTORY, not completion counters.** Luo &
    Carey (PVLDB 13:449): a scheduler that starves large merges *"would report a
    higher but unsustainable write throughput"* — the only tell is a
    monotonically growing backlog while every counter looks healthy. Fixing
    LevelDB's cut its measured maximum throughput by a third.

## The two papers that speak directly to us

**SILK (USENIX ATC '19, best paper)** — <https://www.usenix.org/system/files/atc19-balmau.pdf>
Lessons 2 and 3, verbatim, are an argument against our deadline:

> "Simply limiting bandwidth for internal operations does not solve the problem
> … and can in fact **exacerbate it in the long run**. This approach effectively
> **postpones compactions**, and therefore increases the likelihood that at some
> later point **many compactions occur at the same time**."

> "…being selective about starting compactions or only performing compactions at
> the highest level, **avoid latency spikes in the short run, but aggravate the
> problem in the long run**."

PebblesDB looked healthy and then halted ~8 hours in when its highest-level
compaction landed — hence their advice to run tests long enough for that to
surface. SILK's own numbers: 2-3 orders of magnitude lower p99 than RocksDB;
write stalls 178s / 100s / 18s / **0s** across RocksDB / TRIAD / auto-tuned /
SILK.

**Lethe (SIGMOD '20)** — <https://arxiv.org/pdf/2006.04777> — the one legitimate
use of a deadline in this literature, and it is on the *data's* SLA, not the
job. Note its correction, which applies to any deadline we keep: a **uniform**
per-file TTL `D_th/L` "leads to increased compaction time and resource
starvation as larger levels have exponentially more files, hence a large number
of files may exhaust their TTL simultaneously." Deadlines must be staged
non-uniformly or they become a correlated-burst generator.

**Sarkar et al. (PVLDB 14:2216)** — <https://vldb.org/pvldb/vol14/p2216-sarkar.pdf> —
granularity is the tail lever: tail write stall ~25 ms (tiering) vs 1.3 ms
(partial leveling), a 19x gap; but partial compaction means **4x more jobs**, so
per-unit coordinator overhead must be cheap. Also: CPU is ~50% of compaction
time regardless of strategy, dominated by the in-memory sort-merge.

## What this changes in our plan

The night's measured defects and the prior art agree, which is reassuring:

- **Repair's 900s deadline → progress-checked liveness clock** (IOx rule 3).
  A unit that is staging rows is making progress and must not be killed.
- **Repair's 13-slice rescan → split the input and COMMIT each piece**
  (rule 5 + Iceberg partial progress). Our own
  `TIMEFUSION_REPAIR_RESUME_ENABLED` is the same idea approached from the wrong
  end: the cleaner form is never to have an uncommitted 40-minute unit.
- **`resource_admission`'s flat `MAX_DECODED_BYTES` → occupancy-scaled cap**
  (rule 2). We already estimate decoded bytes; gate on `f(free_slots)`.
- **`QUARANTINE_ATTEMPTS`-by-count → quarantine by failure class** (rule 4),
  recording the budget that was exceeded. Our own comments already record that
  attempt-counting quarantined 109 of 162 sealed derived units on stale
  evidence.
- **`split_time_task` for Repair is unsound** and the literature says why: a
  per-file cost cannot be shed by time-bisection, so the split only multiplies
  the number of units paying it (mirror of IOx's anti-oscillation guards).
- **The scheduling tuple ranks `starved` (age) above `benefit`** — RocksDB
  deliberately went the other way (rule 6). Worth revisiting, but not tonight.
- **Measure by backlog trajectory** (rule 10) — which is exactly the
  `pending_*` / `backlog_bytes` slope this investigation started from.

One caution worth keeping: IOx gets acceptable throughput with **no ranking at
all** (`shuffle()` over partitions with new files), purely from correct per-unit
sizing plus 100-way concurrency. Our seven-term ranking tuple may be solving a
problem that correct sizing dissolves.
