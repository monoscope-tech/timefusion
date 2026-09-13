# What it actually takes to drain the 1.17 TB sealed backlog

2026-09-13, after #271 reached production (permit acquisition went from
**0.036% to 46%**; the lane had been wedged, not slow).

## FIRST: the "1 TB backlog" is ~100 GB on disk

`sealed_compaction_debt_bytes` sums **`task.estimated_decoded_bytes`**
(`maintenance_coordinator.rs:3433`) — DECODED bytes, not bytes stored. At this
codebase's own `DECODED_BYTES_PER_COMPRESSED` = 12, the **1.19 TB reads as
~99 GB of actual stored data** across 196 pending tasks, i.e. ~507 MB
compressed each.

This is the same compressed-vs-decoded confusion #271 exists to fix, showing up
one level higher — in the number everyone quotes as "the backlog". Any plan
sized against 1.19 TB of *files* is wrong by ~12x. **I made exactly that error
first**, estimating 11,000 bins and ~7 days before reading the accumulator.

Corrected: ~99 GB / ~107 MB per bin ≈ **950 bins**, which at the observed
24-105 bins/hour is **hours, not days**.

## The lane works now, but the debt is not falling at anything like that rate

Observed over 100 minutes on the fixed build: `sealed_compaction_debt_bytes`
moved **0.5 GB** and `pending_sealed_consolidation` held flat at **196** — while
SealedConsolidation completed ~48 units/hour. Two mechanisms are visible and
neither is the sort stall #271 fixed:

- **Tasks retry rather than retire.** `retry.HotPacking.compaction_debt_remaining`
  = 54 and `retry.SealedConsolidation.compaction_debt_remaining` = 7: a unit
  does a few bins and re-queues, so the pending COUNT is not a work-remaining
  gauge.
- **The bins being staged are tiny.** Every staged bin measured 42-47 files at
  ~1.4 MB each, ~63 MB total. That is correct hygiene (file-count reduction) but
  moves almost no bytes.

### A refusal that looks like a #271 side effect and is NOT

`pack_value_refused` reads **433 in 2 hours against 6 in the previous 9**, which
invites the story that capping bins pushed them under
`refuse_low_value_bin`'s 3-file threshold. **The rate refutes it**: rows per
refusal is **898,650,262 / 433 = 2.08 M**, against the old process's
13,863,476 / 6 = **2.31 M**. Same bin shape, not a smaller one.

The raw counts never compared — the lane was *dead* for the older window, so it
was not attempting bins to refuse. **Refusals per attempt is the only comparable
figure, and it is unchanged.** Worth keeping as the template: whenever a lane
goes from stalled to running, every one of its counters jumps, and none of those
jumps is evidence by itself.

What remains true is that ~2 M-row, sub-3-file bins are being refused and their
bytes cannot consolidate — but that is pre-existing, not caused by #271.

### Where the dose-response came from

Measured live in prod, every sealed/hot bin in a 3-hour window, against the
1.25 GiB decoded sort budget:

| bin | compressed | decoded (x12) | ratio | outcome |
|---|---:|---:|---:|---|
| 18 files | 65.7 MB | 0.73 GiB | 0.59x | staged in **67 s** |
| 24 files | 153 MB | 1.71 GiB | 1.37x | staged in **29.2 min** |
| 40 files | 249 MB | 2.78 GiB | 2.22x | **never staged** |
| 45 files | 227 MB | 2.53 GiB | 2.02x | **never staged** |

Monotonic, with an intermediate point 26,000x slower than the under-budget bin.
This is the strongest evidence yet for the compressed-vs-decoded confusion
described in `2026-09-12`'s hygiene-lane note.

## K = 2, and it cannot be configured

`light_optimize_k` is `mem_bound.min(cpu_bound).min(hot_project_count).max(1)`:

- `cpu_bound` = cores/4 = **7** — not binding.
- `mem_bound` = `slices - min(holdback 3, slices - LIGHT_MIN_SLICES)` where
  `slices` = `coordinator_share_bytes / 1.25 GiB`.

Tabulating the actual arithmetic:

| slices | holdback applied | **K** |
|---:|---:|---:|
| 3 (28 cores, today) | 1 | **2** |
| 4 | 2 | **2** |
| 5 | 3 | **2** |
| 6 | 3 | 3 |
| 7 | 3 | 4 |

`slices` = `jobs x 512 MiB / 1.25 GiB` and `jobs` = cores/3, so **K stays at 2
from 28 cores all the way to ~48**, and reaching the 4 light permits the
config's own comment calls the bench optimum ("6 concurrent sorts... 4 light +
2 repair") would need **~53 cores**. The LIGHT_MIN_SLICES fix shipped on 09-12
stopped K collapsing to 1; it did not make the holdback yield above the floor.

**There is no env knob.** `TIMEFUSION_LIGHT_OPTIMIZE_CONCURRENCY` is marked
*formerly* in `config.rs` — the field is gone, so setting it parses as nothing.
CLAUDE.md still documents it as live. Same for
`TIMEFUSION_MAINTENANCE_REWRITE_CONCURRENCY`, which **prod's container actually
sets** (`=2`) and which is likewise dead.

## The candidate change, and the reason it is not shipped unattended

The holdback reserves up to 3 slices for the repair lane. Over the 9 hours
measured, repair did **zero** work: `pending_repair = 0` and
`work.Repair.worker_secs = 0` throughout — the comment at `config.rs:687` even
records "`pending_repair` was 0 throughout". So the lane that owns 1.17 TB of
debt is capped at 2 concurrent sorts to reserve budget for a lane that ran
nothing.

Making the holdback demand-aware — yield it when no repair is pending — takes
K from 2 to 3 at today's core count, a ~50% drain improvement (7 days → ~4.7).
`light_optimize_k` already takes a runtime argument (`hot_project_count`), so
there is a clean place to put it.

**Not shipped in this pass** because over-committing this exact pool is the
documented cause of three separate "Resources exhausted" incidents, the
transition (repair becomes pending while three light sorts are in flight) needs
a bounded over-commit argument, and it cannot be validated against real memory
pressure on MinIO. It wants a staging run, and it is a 1.5x lever on a problem
whose shape is now understood — not the 10x one.

## The bigger lever is CPU, and it is not in this lane

The box runs at **25.0 of 28 cores**. Tantivy holds **~10 of them (39%)** while
indexing 2-7x the ingest row rate, for reasons not yet attributed
(`2026-09-13-where-the-28-cores-actually-go.md`). Reclaiming that is the
largest single source of headroom available — but note it does **not** directly
drain this backlog, because the sealed lane is permit-bound, not CPU-starved.
Raising the container cap 28 → 32 (already in
`deploy/caprover-service-override.yml`, still needs applying in CapRover) adds
~14% CPU and, per the table above, **changes K not at all**.

## Prior art: InfluxDB 3 bounds compaction by ROWS, not compressed bytes

InfluxDB 3 (IOx) is the closest architectural relative TimeFusion has — Rust,
DataFusion, Parquet, object storage — so its compactor is the most directly
transferable prior art, and it differs from ours in exactly the place that hurt.

- **Levels, not a single "sealed" pass.** L0 is newly ingested and uncompacted,
  L1 is consolidated, L2 is compacted and non-overlapping. Promotion between
  levels is gated on size boundaries (`--l1-consolidation-target-size`: a run set
  is consumed at or below it, and promotion requires at least two run sets above
  it). That is the same similar-size idea `bin_breaks_size_ratio` implements,
  expressed as an explicit ladder.
- **A separate, smaller target for the hot tail.** `--l1-hot-tail-target-size`
  (default 250 MB) caps live tail rewrites during snapshot compaction, and the
  larger tail is handed to L1 consolidation to seal. TimeFusion's split between
  hot-tail packing and sealed consolidation is the same shape.
- **The one that matters: `compactionRowLimit`, a soft limit of ~1,000,000 ROWS
  per file the compactor writes.** The bound is on rows, not on compressed
  bytes.

**That last point is the lesson for #271.** TimeFusion caps a bin at
`COORDINATOR_PER_SORT_BUDGET_BYTES / DECODED_BYTES_PER_COMPRESSED` — compressed
bytes converted through a *fixed 12x estimate*. But the thing the sort actually
costs is decoded rows, and the compression ratio is not 12x uniformly: it varies
by column mix, by tenant, and by how well-sorted a file already is. A bin of
identically-sized compressed input can decode to wildly different working sets.

**Rows are exact, and TimeFusion already has them**: `TailAdd::rows` comes from
Delta `numRecords` and `refuse_low_value_bin` already sums it to price a bin.
Pricing the cap in rows rather than estimated-decoded-bytes removes the 12x
estimate from the one decision that stalls the lane when it is wrong — which is
the same class of error as the gauge correction above, and as #271 itself.

This is a concrete follow-up, not a speculative one: the bound already exists,
the input is already summed one function away, and the failure mode of the
current estimate has now been measured twice.

Sources: [InfluxDB 3 storage engine internals](https://docs.influxdata.com/influxdb3/clustered/reference/internals/storage-engine/),
[InfluxDB 3 Enterprise configuration options](https://docs.influxdata.com/influxdb3/enterprise/reference/config-options/),
[Timestream for InfluxDB v3 parameters](https://docs.aws.amazon.com/ts-influxdb/latest/ts-influxdb-api/API_InfluxDBv3EnterpriseParameters.html)
