# The admission boundary is the only blocker — measured, and my own fix direction was wrong

2026-09-03. Supersedes the "admission + sort slice are a pair" framing in
`2026-09-02-dedup-units-sit-exactly-on-the-admission-boundary.md`. Two local
benchmarks and a journal read changed the answer.

## Lane survey: two of four lanes are already fine

From the prod journal, live units (not complete/superseded):

| lane | n | age median | age p90 | states |
|---|---|---|---|---|
| dedup | 1,552 | 22.3 h | 386 h | 1,023 pending, 517 retry, 12 running |
| base_rollup | 320 | **169 h (7 d)** | 240 h | 138 pending, **180 retry**, 2 running |
| repair | 314 | **412 h (17 d)** | 412 h | **312 retry**, 2 running |
| derived_rollup | 52 | 1.1 h | 13.7 h | 33 pending, 19 retry |
| **HotPacking** | **0** | — | — | **no live units at all** |

**Hot-tail packing is keeping up completely** — zero backlog. `derived_rollup`
is healthy at a 1.1 h median. The problem is dedup, base_rollup and repair, and
below they turn out to be the same problem.

## The corrected causal chain

I previously wrote that oversized units "should have been hash-sharded and were
not". That reads the design backwards. `split_time_task` **deliberately declines
to shard**, and says why:

```rust
if children.len() <= 1 || children.iter().any(|child| child.hash_shards > 1) {
    split_declined_no_width += 1;
    return false;   // "let the unit RUN — the runner already hash-shards
}                   //  internally at any width, which bounds memory without
                    //  minting a single journal unit"
```

That is the right design. Journal-level sharding mints units that each re-pay
the partition scan; runner-level sharding does not. So the intended life of an
oversized unit is: **be admitted at the clamped `MAX_DECODED_BYTES`, then shard
itself internally to stay inside that reservation.**

It never gets to. The clamp produces a request of *exactly* MAX, and

```rust
occupancy_scaled_ceiling = (MAX * available / capacity).clamp(MAX/16, MAX)
```

is **strictly below MAX whenever `available < capacity`** — i.e. whenever
anything at all is reserved. So the request that the design intends as
"reserve the maximum, then self-shard" is the one request the gate can never
grant.

Evidence, from the journal:

- dedup ≥MAX: **365 units, every one `hash_shards = 1`**, median size exactly
  512.0 MiB, median age 14.6 days.
- base_rollup ≥MAX: **95 units, 94 at exactly 1.0-minute slice width**
  (`MIN_SLICE_MICROS` — time-bisection has bottomed out), estimating **7.5 GiB**
  each, `hash_shards = 1`, attempts to **715**, reason `admission_busy` ×159.

`admission_busy` is deliberately *not* a capacity failure, so `retry_or_split`
neither splits nor applies its escalating backoff — the unit hot-loops. That
classification is correct for units that fit and merely lost a race. For units
whose estimate exceeds MAX it is wrong: their refusal is structural, not
transient.

## Benchmark 1 — the sort slice is NOT the second jaw

`TF_BENCH_SLICE=1`, real 204 MB prod file, 2,451 MB decoded, dedup Sort shape:

```
pool MB   secs   pool/decoded  outcome
2048      14.3       0.84      ok
1024      22.5       0.42      ok
768       21.8       0.31      ok
512       30.6       0.21      ok      <- prod's nominal per-job slice
384       FAIL       0.16      Resources exhausted: ExternalSorterMerge[0]
256       FAIL       0.10      Not enough memory to continue external sort
```

**A 512 MB pool sorts 2,451 MB of decoded data — 4.8x its own size.** The floor
is ~0.16–0.21x decoded, not >=1x. So the arithmetic I reasoned from
("per job = 512 MiB = exactly the admitted bytes, therefore zero headroom") was
wrong about what a spilling sort needs. Cost is throughput, not correctness:
512 MB is 2.1x slower than 2 GB.

**This de-risks the admission fix substantially** — admitting these units does
not obviously convert a starving queue into a failing one.

## Benchmark 2 — but concurrency does not divide the way the config assumes

The older fleet ladder, same file and same shape, one shared 8 GiB pool:

```
6 workers  33.32 MB/s  0 failed   <- best
8 workers  15.07 MB/s  4 FAILED   <- cliff
```

8 workers sharing 8 GiB is **1 GiB nominal each** — twice what a lone worker
needs — and half of them fail. So **`pool / jobs` is not the share a worker
gets**; FairSpill contention makes the effective share much smaller than the
nominal one.

That matters because `coordinator_jobs` reaches **16** on the prod box against
that same 8 GiB pool — a rung the ladder never measured, two doublings past the
measured cliff. The 247 dedup retries in the journal carrying `Not enough memory
to continue external sort` are consistent with running there.

### The extended ladder run was INVALID — record it so it is not re-quoted

Extending to 10/12/16 produced `8 workers: 7 failed`, `10: 10 failed`,
`12: 12 failed` — and then the process died with

```
IoError(StorageFull … "No space left on device")
```

The local disk had gone from 65 GiB free to **1.0 GiB** (leaked spill and stale
worktrees). The 10- and 12-worker rungs "failed" in 8.7 s and 0.9 s — far too
fast to be sorts at all. **Those are disk exhaustion wearing a memory failure's
clothes**, exactly the class in `tf_saturation_makes_tests_lie_2026-08-25`, and
the 8-worker rung in the same run is contaminated too. Discarded, not reported.
Disk reclaimed to 49 GiB.

### And the fleet ladder over-states prod's per-worker load ~4.8x

More important than the invalid run: the ladder gives each worker the **whole
2,451 MB-decoded file**. A prod coordinator job is admitted for at most
`MAX_DECODED_BYTES` = **512 MiB**. So

```
ladder, 8 workers : 8 x 2,451 MB = 19.6 GB of decoded work in an 8 GiB pool
prod,  16 jobs    : 16 x 512 MiB =  8.0 GB of decoded work in an 8 GiB pool
```

Prod's configuration is **2.4x lighter per byte of pool** than the rung that
failed. So "8 workers fail at 8 GiB" does **not** establish that prod's 16 jobs
fail — the two are different workloads, and I nearly reported the ladder as if
they were the same.

The experiment that would settle it is 16 workers each sorting ~512 MiB decoded
(a ~43 MB compressed slice) against one 8 GiB pool. Until that exists, the job
COUNT is **open**, not condemned.

## What this means for the fix

The admission boundary is the single blocker for ~460 units across two lanes.
The minimal correct change is to make MAX attainable at healthy occupancy —
ClickHouse grants its full cap at **8 free pool entries of 16, half free, not
idle**, while ours grants it only at 100% free.

Note the curve is otherwise nearly inert: `maintenance_admission` is built with
`memory_limit_bytes` (**80 GiB**, measured from `limit_bytes`, not the 120 GB I
assumed), so capacity is 60 GiB and 16 jobs x 512 MiB = 8 GiB is ~13%. The
ceiling lives in roughly [446, 512) MiB and never approaches its 32 MiB floor.
So this is a **boundary** fix, not a curve reshape.

**Not yet shipped.** Two things gate it: the 16-worker fleet rung, and a window
of pool sampling from the instrument deployed tonight. One reading is not a
measurement.
