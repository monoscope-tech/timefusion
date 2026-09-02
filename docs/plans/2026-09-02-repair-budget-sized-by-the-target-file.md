# Repair's budget must fit one target-sized file

Shipped 2026-09-02. Applies rule 1 of
`2026-08-31-how-other-systems-schedule-maintenance.md` to the one lane that
cannot be split.

## The defect

`repair_rewrite_budget_bytes()` returned `COORDINATOR_PER_SORT_BUDGET_BYTES`
(1,280 MiB). A repair unit is exactly one file — `coordinator_compaction_files`
takes 1 for Repair — and compaction deliberately produces
`COORDINATOR_HOT_TARGET_BYTES` = 256 MiB files, which decode at
`DECODED_BYTES_PER_COMPRESSED` = 12x to **3,072 MiB**.

So the permit request

```rust
want_mib = estimated_decoded_bytes(sum of sizes) / MiB
           .clamp(1, budget_mib)
```

clamped to the whole semaphore for every correctly-sized file. Only files under
~107 MiB compressed — the ones compaction considers too small — could ever share.

| | value |
|---|---|
| target file, decoded | 3,072 MiB |
| repair budget | 1,280 MiB |
| ratio | **0.42x** |
| observed rate | ~1.2 large rewrites/hour |
| serialization ceiling | 60/40min = 1.50/hour |
| queued units | 310 (~11 days to drain) |

Prod logged `want_mib=1280 budget_mib=1280` 243 times in three hours. The lane
looked alive — 2,188 completions — because those were the small files.

The byte-pricing change that introduced this budget intended *"bins below the
budget now share it"*. At these constants no correctly-sized bin is ever below
it, so the sharing case could not occur: correct in intent, inert in practice.

## Why this shape of fix

The prior-art survey's rule 1 is that a compaction size cap is a **multiple of
the target file size**, read from metadata:

| system | cap | as a multiple of target |
|---|---|---|
| RocksDB `max_compaction_bytes` | 25x `target_file_size_base` | 25x |
| IOx `max_compact_size` | 3x target | 3x |
| Iceberg `max-file-group-size-bytes` | 100 GB | large |
| **ours, before** | 1,280 MiB | **0.42x** |
| **ours, after** | 6,144 MiB | **2x** |

Two things follow:

1. **The budget is now derived, not written.** It is
   `REPAIR_REWRITE_TARGET_FILES * COORDINATOR_HOT_TARGET_BYTES *
   DECODED_BYTES_PER_COMPRESSED`, so a change to what compaction produces cannot
   silently make repair unrunnable again — which is exactly how it broke.
2. **It is repair's own constant.** It used to be
   `COORDINATOR_PER_SORT_BUDGET_BYTES`, which is also `light_optimize_k`'s
   divisor; raising *that* to fix repair would cut hot-tail packing concurrency
   2.4x — the 2026-09-01 outage in the opposite direction (K 3 -> 1, zero
   HotPacking units claimed in 45 minutes with 17 pending).

## Why 2x and not RocksDB's 25x

The constraint is not the rule, it is the pool. `benches/rewrite_throughput.rs`
measured the 8 GiB coordinator pool holding **six** concurrent sorts of a real
204 MB prod file, with the cliff at eight:

```
4 workers  29.16 MB/s  0 failed
5 workers  29.31 MB/s  0 failed
6 workers  33.32 MB/s  0 failed   <- best
8 workers  15.07 MB/s  4 FAILED   <- cliff
```

2x keeps the total at six and moves only the split: **5 light + 1 repair ->
4 light + 2 repair**. The `light_optimize_k` holdback moves with it, which is
what pins the total; three assertions encode that invariant and were updated
together. Going to 3x would need K=3 light — approaching the HotPacking
starvation zone — and ~8.3 GiB of true demand against an 8 GiB share.

Expected: ~1.2 -> ~3 large rewrites/hour, 310 units in ~4.3 days instead of ~11.
Raise it again only alongside a bench that moves the cliff.

## What was deliberately NOT changed

**The 12x pricing.** The same bench implies a true footprint of ~1.33 GiB for a
204 MB file — about half what 12x prices — so repricing `want` at measured
footprint is tempting. It was rejected for this deploy: on 2026-09-01 two
~2.3 GB-compressed rewrites exhausted the pool, which contradicts a
constant-footprint model. Footprint is sublinear in input, not constant, and two
data points do not fit one line. The conservative 12x is also what keeps the
whale-runs-alone clamp meaningful. Repricing needs a cost model and its own
bench.

**The occupancy ceiling's range** (Decision 2). Ours is linear over
[32 MiB, 512 MiB] — a **16x** span where ClickHouse's geometric rule spans
**37,500x** (4 MB -> 150 GB). That is a real finding and probably the next
change, but it is gated on the pool-usage instrument
(branch `maintenance-pool-stats`), and one change per deploy.

## Verification

- `config::tests::repair_budget_must_fit_one_target_sized_file` — failed on
  master with the diagnosis in its message, passes now.
- `repair_serialises_on_its_own_permit` — gains the case that was never small:
  two target-sized rewrites (3,072 MiB each) must both acquire. The
  oversized-bin-runs-alone property is unchanged and still asserted.

## Baseline, taken before the deploy

Image `ca3c413`, process uptime **174 min** at the time of reading — quiet
enough for the cumulative counters to mean something (uptime computed from
`boot_micros`, not estimated).

```
maintenance.pending_repair                  302
maintenance.pending_dedup                  1304
maintenance.compaction_permits_unavailable  276   (over 174 min = 1.59/min)
maintenance.repair_bins_in_flight             0
maintenance.repair_sorted_at_write_total    935
scan.dedup_full_set_pct                     8.7
```

Note `dedup_full_set_pct` reads **8.7%**, up from the 4.4% measured earlier
today. It is a file property, not process state, so this is the unsorted
population growing while repair cannot drain it — the cost this change exists to
stop. It is also the cleanest single number to judge the deploy by.

## What to watch after deploy

Two hours of quiet before any number means anything.

| signal | expectation | where |
|---|---|---|
| large single-file rewrites | ~1.2/hr -> ~3/hr | Delta log (a large repair commits 1 remove -> **2** adds; a filter requiring 1-add-1-remove hides it) |
| `repair_rewrite_permit_busy` | falling | service logs |
| `Not enough memory to continue external sort` in repair staging | **zero** | **abort signal** — revert one constant |
| HotPacking claims at K=4 | non-zero units per 45 min | the 09-01 freeze tell |
| `scan.dedup_full_set_pct` | 4.4% trending down | `timefusion_stats` |
