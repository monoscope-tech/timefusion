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
