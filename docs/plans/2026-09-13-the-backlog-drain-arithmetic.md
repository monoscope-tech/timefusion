# What it actually takes to drain the 1.17 TB sealed backlog

2026-09-13, after #271 reached production (permit acquisition went from
**0.036% to 46%**; the lane had been wedged, not slow).

## The lane works now. It still cannot finish today.

`sealed_compaction_debt_bytes` = **1.17 TB**. #271 caps a packing bin at what
one sort can decode: `COORDINATOR_PER_SORT_BUDGET_BYTES` (1.25 GiB) /
`DECODED_BYTES_PER_COMPRESSED` (12) = **~107 MB compressed**.

```
1.17 TB / 107 MB           ≈ 11,000 bins
observed: a 65.7 MB bin staged in 67 s -> ~110 s at the 107 MB cap
K = 2 permits
  => 2 x (3600/110) ≈ 65 bins/hour
  => 11,000 / 65    ≈ 170 hours ≈ 7 days
```

**Draining this backlog "today" is not physically available at K=2**, whatever
else is fixed. The honest framing is: the lane has gone from *stalled forever*
to *finishing in about a week*, and the remaining question is what compresses
that week.

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
