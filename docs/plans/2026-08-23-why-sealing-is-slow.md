# Why sealing is slow on a 48-core, 188 GB box

2026-08-23. The hardware is not the constraint and neither is concurrency. Each
sealed unit re-reads a whole partition to emit a dozen rows.

## The evidence

`maintenance_rollup_published`, consecutive units, prod:

```
project 87576849  slice 1785345660..1785345720 (60s)  rows=18  bytes=4,466,185,462
project 87576849  slice 1785345720..1785345780 (60s)  rows=15  bytes=4,466,185,462
project 87576849  slice 1785345780..1785345840 (60s)  rows=16  bytes=4,466,185,462
project 87576849  slice 1785345840..1785345900 (60s)  rows=18  bytes=4,466,185,462
project 87576849  slice 1785345900..1785345960 (60s)  rows=18  bytes=4,466,185,462

project 6297304f  slice ..600s (10 min)               rows=25  bytes=    2,784,374
project 6297304f  slice ..600s (10 min)               rows=36  bytes=    3,463,223
project 6297304f  slice ..600s (10 min)               rows=43  bytes=    6,346,684
```

Two facts, and the second is the one that matters:

1. **4.47 GB to produce 18 rows.** Three orders of magnitude more bytes per unit
   than a healthy project on the same tier and the same grain.
2. **The byte figure is IDENTICAL across consecutive slices.** Each 1-minute unit
   is reading the same file set as its neighbour — a 1-minute slice cannot prune
   files that each span the whole day, so every unit re-reads the whole
   partition.

## Why this explains everything measured today

The queue for this project is 24.6 units per sealed cell. If each of those units
re-reads the same ~4.5 GB, the day costs ~24.6 x 4.5 GB of reading to produce one
day of rollup rows that a single day-wide unit would produce from one pass.

That is why:

- 48 cores and 188 GB do not help. The work is object-store reads and decode of
  the same bytes over and over, not CPU or memory pressure.
- Raising `coordinator_jobs` does not help. More workers means more concurrent
  re-reads of the same files.
- Sealed drain is ~41 units/hr while frontier drain is ~450/hr. Frontier slices
  sit in small, recent, well-pruned partitions (the healthy project above);
  sealed slices sit in old fragmented ones.
- `87576849` is exactly the project whose 30-day query selects **1,447 files /
  460 GB** and is refused by the scan guard. Same fragmentation, seen from the
  read side.

## The two levers, in order

1. **Coarsen sealed units to one per (project, date).** 448 cells instead of
   11,020 units, each reading the partition ONCE instead of ~24.6 times. Same
   pool, ~15x less wall clock, and it removes the redundant re-reads rather than
   parallelising them.
2. **Compact the fragmented partitions.** 1,447 files for one project's 30 days
   is what makes a 1-minute slice cost 4.5 GB in the first place. Fewer, larger,
   time-ordered files make both the rollup build and the read path prunable.

Neither is a query-side change, which is consistent with everything else measured
today: the routing rules are fixed and the remaining cost is all in how the data
is laid out and how the work is sliced.

## Caveat

`estimated_decoded_bytes` is an ESTIMATE and prior work found it counts whole
files rather than the selected range (2026-08-19). So 4.47 GB may overstate the
bytes actually decoded. It does not overstate the problem: the estimate is
identical across consecutive slices, which is itself the evidence that the same
files are being opened per unit, and file opens are the dominant per-unit cost on
object storage.
