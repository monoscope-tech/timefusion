# Lever 2: the dirty-bin drain is throughput-bound, not stalled

2026-08-23, after lever 1 collapsed the rollup queue 85%. Lever 2 is "compact the
fragmented partitions" — the thing that makes a one-minute slice cost 4.5 GB in
the first place. This is the first measurement of why it is not happening.

## What it is NOT

Two hypotheses ruled out by counters, both worth recording because each was
plausible and each has bitten this system before:

- **Not the flush health gate.** `dedup_passes_flush_yields_total = 0` and zero
  `dedup_drain_flush_yield` events in the window. `dedup_flush_healthy` is
  returning true.
- **Not the memory brake or WAL backlog.** `light_optimize_memory_brakes_total`
  and `light_optimize_wal_yields_total` are both 0, and light optimize
  independently committed 6 bins in the same window.
- **Not a missing caller.** "Running scheduled dedup on sealed partitions"
  appears twice in 12 minutes, exactly the 5-minute cadence.

## What it IS

```
dirty_bin_queue_depth          19,891
dedup_bins_committed_total          2     (in ~12 min / 2 cron passes)
dedup_waves_committed_total         2
dedup_bin_stage_timeouts_total      0
dedup_bins_deferred_cold_total      0
```

Roughly **one bin per pass**, about 12 bins/hour, against a queue of ~20,000.
That is ~1,650 hours to drain — the same order-of-magnitude shortfall lever 1 had
before the ceiling, but from a different cause: here the units are not
duplicated, they are simply expensive.

`DIRTY_BIN_STAGE_BATCH` is 64 and `DIRTY_BIN_DRAIN_BATCH` is 16,384, so admission
is not the limit. With no stage timeouts, the pass is spending its whole
~240 s budget on a very small number of bins.

## Caveat, stated because the sample is small

Two cron passes on an 11-minute-old container is a thin basis for a rate. The
counters are process-scoped and prod restarts every 20-40 minutes, so this needs
confirmation on a longer-lived container before the 12/hr figure is quoted. What
is NOT sample-dependent is the queue depth (persisted, ~20k) and the exclusion of
the gate hypotheses above.

## Why it matters for the goal

The rollup side is now fixed: sealed cells are one day-wide unit each. But each
of those units still scans a fragmented partition — project 87576849 selects
1,447 files / 460 GB for 30 days. Compaction is what makes each of those units
cheap, and at 12 bins/hour the fragmentation is not being retired.

The next question is where a pass's 240 s actually goes: `stage_dedup_chunk`'s
rewrite, the batch probe, or the commit. `dedup_bin_rewrite_duration_ms_total`
exists and read 0 on this container, so it wants a longer window rather than more
hypotheses.

## Correction and the real shape of lever 2

Two earlier readings on this page were taken on 2-minute-old containers and were
wrong. The compaction planner needs a scan cycle before its numbers mean
anything:

```
17:46  eligible_sealed=1235  pending_sealed_consolidation=0   <- planner mid-scan
17:48  eligible_sealed=1305  pending_sealed_consolidation=72  pending_hot_packing=20
17:51  ...                   pending_sealed_consolidation=75
```

So partitions genuinely out of policy DO exist, and the number is small:
**~75 sealed partitions**, consistent with the 2026-08-19 object-storage audit
("108 sealed ones genuinely out of policy" against 2,130 queued tasks). Lever 2
is not a 20,000-item backlog. The 19,891 `dirty_bin_queue_depth` is the DEDUP
queue, a different mechanism.

## What is actually blocking it

Watched over five minutes with the coordinator healthy:

```
17:51  pending_sealed_consolidation=75  tasks_running=16
17:53  pending_sealed_consolidation=75  tasks_running=16
17:56  pending_sealed_consolidation=75  tasks_running=16
17:57  everything 0                                    <- restart
```

Not draining, while all 16 workers are busy on rollup work. Both operation
cycles give `SealedConsolidation` one slot in ten, so the cycle is not starving
it by construction — the claim is failing for some other reason, and that is the
open question. Note also that these units are DERIVED (`is_derived_operation`),
so a restart discards them and the planner re-mints from the file list; with prod
restarting every 20-40 minutes, a unit has a narrow window to be claimed at all.

## The part that matters for the 14d/30d goal, stated carefully

Lever 2 was motivated as "what makes a 1-minute slice cost 4.5 GB". After lever 1
there are no 1-minute sealed slices, so that specific cost is already gone.

What remains is the READ side: project 87576849 selects 1,447 files / 460 GB for
30 days and is refused. Whether compaction can help that is NOT established here.
1,447 files over 460 GB averages ~318 MB, above the 256 MB sealed target — but a
mean does not describe a population, and this repo has already been burned by
exactly that inference (the tantivy size distribution, 2026-08-23: one 718 MB
whale among eight builds under 40 MB). The honest statement is that p1's file
size DISTRIBUTION has not been measured, and until it is, "compact p1" is a
hypothesis rather than a plan.

If p1's files really are mostly at target, then its 460 GB is data volume, not
fragmentation, and no compaction will make a 30-day raw scan cheap — only routing
to the rollup tier will, which is lever 1's territory and now unblocked.

## Measured from object storage — and it overturns the section above

Delta snapshot v498924, 4,547 add actions, sealed days only:

```
out-of-policy sealed cells (>=2 files under the 256 MB target):   48
small files in those cells:                                    1,784

  6297304f  2026-08-17   275 files /  0.90 GB   <- 3.3 MB per file
  87576849  2026-08-19   238 files /  1.93 GB
  28f62f01  2026-08-18   230 files /  1.48 GB
  6297304f  2026-08-18   114 files /  0.54 GB
  87576849  2026-08-18    62 files /  1.00 GB

small files by project: 87576849=631  6297304f=500  28f62f01=459  dcad860a=141
```

**Both projects whose 14d/30d cells still fail are among the three worst.**
`87576849` and `28f62f01` carry 631 and 459 small files respectively.

### The inference I got wrong, and why

Earlier on this page I reasoned that p1's 1,447 files over 460 GB averages
~318 MB, above the 256 MB target, and therefore "p1 is not fragmented, it is just
big". That is wrong. The distribution:

```
p1 sealed files (1,339 sampled)
  p10 = 0 MB    p50 = 340 MB    p90 = 797 MB    max = 1,829 MB
  under 256 MB: 588 / 1,339  = 44%
```

Nearly half of p1's sealed files are under target and the 10th percentile rounds
to zero. The mean was pulled up by a long tail of large files and said nothing
about the small ones — the same mistake this repo recorded hours earlier about
tantivy build sizes ("one 718 MB whale among eight builds under 40 MB; a mean
does not describe this population"). I had even written the caveat down and then
reasoned past it. A mean is not a distribution; measure the percentiles.

### Status of the mechanism

Consolidation IS running — 23 consolidation log events in a 20-minute window, and
the planner mints ~75 units (48 cells x tiers). The backlog is bounded and being
worked, not stalled. What slows it is the restart cadence: `SealedConsolidation`
is a DERIVED operation, so every deploy discards the queued units and the planner
re-mints them from the file list, giving each unit a narrow window to be claimed.

So lever 2 does not need a new mechanism. It needs the 48 cells to drain, and the
thing that most slows that is the same deploy cadence that has truncated every
measurement in this session.

## Compaction attempted by hand: one cell done, and why the rest cannot go that way

`OPTIMIZE <table> WHERE date = '…' AND project_id = '…'` is partition-bounded and
works. Verified end to end against the Delta snapshot:

```
6297304f / 2026-08-19    15 files -> 1 file      (OPTIMIZE 15 1, ~2 min)
out-of-policy cells      48 -> 47
small files              1,784 -> 1,769
```

Three other attempts FAILED, all the same way:

```
p1 / 2026-08-19  (238 files, 1.93 GB)  Failed to allocate 298.4 MB … 264.8 MB remain
p1 / 2026-08-21  ( 58 files, 0.82 GB)  Failed to allocate  98.8 MB …  27.7 MB remain
6297304f / 08-17 (275 files, 0.90 GB)  Failed to allocate  19.6 MB …  19.2 MB remain
                                       (retry)              22.7 MB …  22.3 MB remain
                                       (retry)               9.4 MB …   9.0 MB remain
```

Interactive `OPTIMIZE` sorts the whole partition inside the **shared 5 GB query
pool**, so it competes with live dashboard traffic. That alone makes it unusable
for the cells that matter — the 238- and 275-file ones.

**And the failures appear to LEAK the reservation.** `RepartitionExec[1]#107738`
appears at exactly 77.3 MB in three separate failed statements, and the pool's
free space trends down across attempts — 264.8 -> 27.7 -> 19.2 -> 22.3 -> 9.0 MB.
The same operator id surviving into later statements is not contention, it is a
reservation that was never released. Worth its own investigation; it means a
failed OPTIMIZE degrades the pool for everything after it.

Live queries stayed healthy throughout (a 1-hour `count(*)` returned in 1.7 s,
no restart), so the damage is confined to that pool's headroom — but I stopped
attempting further OPTIMIZEs rather than keep draining it.

## Conclusion for lever 2

The remaining 47 cells must go through the SYSTEM's consolidation path, not by
hand. That path bin-packs, runs on the maintenance pool rather than the query
pool, and commits incrementally — which is exactly why it exists — and it is
running (23 consolidation events in a 20-minute window). Lever 2 is therefore a
matter of that queue draining, with two things slowing it:

1. `SealedConsolidation` is a DERIVED operation, so every deploy discards the
   queued units and the planner re-mints them; prod restarts every 20-40 min.
2. The pool leak above, if confirmed, reduces headroom for everything sharing
   the query pool.
