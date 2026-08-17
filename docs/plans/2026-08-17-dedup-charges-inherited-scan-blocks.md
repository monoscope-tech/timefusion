# DedupExec charges the parquet reader's column chunks, not its own rows

Status: **root-caused from prod logs, fix scoped below.**

## Symptom the user reported

"With each spill or any random thing queries get slow cos maintenance lags."

## What prod actually shows

monoscope's enrichment UPDATE, 2026-08-17 15:42:37, ran **16.9 s and then
failed**:

```
duration_us=16861662 success=false
Error: Failed to allocate additional 15.2 GB for DedupExec[keep-greatest]
  with 0.0 B already allocated for this reservation
  - 14.2 GB remain available for the total memory pool:
    greedy(used: 1866.2 MB, pool_size: 16.0 GB)
```

Three facts make the 15.2 GB a lie, not a measurement:

1. The pool had **1.8 GB used of 16 GB**. The process was not holding 15.2 GB;
   RSS would have shown it and the cgroup would have killed us.
2. The scans feeding this plan read `active_add_files_bytes=847116` (847 KB)
   and `5243336` (5 MB). Megabytes in, 15.2 GB claimed.
3. `0.0 B already allocated for this reservation` — this is the **first**
   `try_grow` on a fresh reservation, i.e. **one batch** allegedly weighing
   15.2 GB.

Observed sizes over 45 minutes: 15.2 GB, 13.9 GB, 5.8 GB, 5.5 GB, 4.8 GB —
three occurrences each, i.e. ~5 distinct statements each retried 3x.

## Root cause

`read_dedup.rs` sizes the keep-greatest run buffer with
`batch.get_array_memory_size()`, which sums buffer **capacities**. Batches
arriving from the DML UPDATE path are view arrays that reference the parquet
reader's full column-chunk blocks, so each batch is charged the whole block
rather than the rows it owns.

This codebase already diagnosed this exact mechanism, for a different consumer.
`mem_buffer.rs:531`:

> Inherited scan blocks: rows read back by the DML UPDATE path arrive as view
> arrays referencing the parquet reader's full column-chunk data blocks
> (`capacity == len`, so slack detection can't see it) — ~250-row UPDATE
> batches charged ~135MB each (29.9GB for 54k rows).

MemBuffer fixed it by compacting; `DedupExec` never got the same treatment.

**It is not only an accounting error.** The keep-greatest run buffer *retains*
the batches it holds, so an inherited block stays alive for as long as the run
does. Compacting makes the charge honest *and* actually releases the block.

## Why this is the user's "maintenance lags → queries slow"

The chain is real and now fully instrumented:

```
maintenance lags  →  partitions never certified  →  DedupExec stays in every plan
                  →  DedupExec charges inherited scan blocks
                  →  query fails ResourcesExhausted (or crawls) after ~17 s
```

`DedupExec` is only in the plan because certification hasn't happened. So
maintenance lag doesn't merely make queries scan more data — it inserts an
operator whose memory accounting is wrong by three orders of magnitude.

## Fix

Reuse `mem_buffer::compact_batch` (already `pub(crate)`, already the "full
charge-honesty pass": view `gc()` for block slack/inheritance, then
`privatize_sliced` for IPC-style shared buffers) on the buffering path in
`Dedup`, before `try_grow`.

It is free when there is nothing to compact — `compact_batch` returns the same
`RecordBatch` when every column pointer is unchanged.

Scope: the unbounded/keep-greatest run buffer only. Do not touch the bounded
path's emit logic, the survivor rule, or `UNBOUNDED_GREATEST_MAX_BYTES`.

## Verification

1. Failing test first: a batch whose columns are slices of a large parent, run
   through unbounded keep-greatest under a pool sized between the real bytes and
   the inherited-capacity charge. Must fail before the fix, pass after.
2. Full lib suite.
3. Prod: `Failed to allocate additional <GB> for DedupExec[keep-greatest]`
   should stop appearing, and the enrichment UPDATE should stop returning
   `success=false`.

## Explicitly NOT in scope

The heavy-share sizing question (`HEAVY_REWRITE_PERMITS` 4→10 against a
4.98 GiB heavy pool, with `PER_SORT_BUDGET_BYTES` halved to pay for it) is a
separate, real concern — 10 x 2 GiB of declared per-sort budget against a
4.98 GiB pool. It is tracked separately; this document is only the query-path
failure.
