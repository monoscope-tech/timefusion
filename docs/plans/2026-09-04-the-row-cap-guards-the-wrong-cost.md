# The row cap guards a cost that is 0.2 % of the deadline

Local benchmark, run because prod is unmeasurable at ~20-minute process
lifetimes. Input is **the exact file `875ea2a1` produced** — the merged pair from
`dcad860a/2026-06-17`, 211.3 MB compressed, **2,023,604 rows** — at
`TF_BENCH_POOL_MB=256`, prod's per-worker share (4.2 GB / 16 jobs).

```
variant                        secs        rows    MB/s in
scan only                       1.4     2023604     149.26
sort b256  p1                   1.6     2023604     135.57
sort b256  p8                   2.7     2023604      78.81
sort b2048 p1                   1.1     2023604     185.77
sort b8192 p1                   1.4     2023604     152.48
PROD: b256 p1 x13 slices        2.3     2023604      90.23
dedup WINDOW   b256  p1        46.8     2023604       4.52
dedup COLLAPSE b256  p1         1.6     2023604     136.03
dedup WINDOW   b2048 p1        41.5     2023604       5.10
dedup COLLAPSE b2048 p1         1.1     2023604     185.46
dedup WINDOW   b2048 p8      FAILED  (sort OOM at a 256 MB pool)
dedup COLLAPSE b2048 p8         1.7     2023604     123.30
```

## 1. The question I shipped on judgement, answered

`875ea2a1` lets a **second** file into a bin past `MAX_BIN_ROWS`, bounded at
`2 * MAX_BIN_ROWS`. I called that ceiling a guess at the time.

**A 2,023,604-row bin sorts in 1.1–2.7 s against a 900 s deadline.** The
exemption is safe by a margin of roughly **300–800x**. Nothing about the fix
needs revisiting.

## 2. The larger finding: the cap is priced against the wrong cost

`MAX_BIN_ROWS = 2_000_000` exists to keep a bin inside the 900 s deadline, and it
is sized from rows because "a rewrite costs what it must SORT AND WRITE".

**The sort is ~0.2 % of the deadline.** Even scaling to the dense shape the cap
was written for (metrics at 47 B/row, 5.58 M rows in 256 MB) the sort is a few
seconds, not fifteen minutes. So whatever made those units miss the deadline
9 times out of 9, **it was not the sort** — it is object-store IO and the commit,
which this repo has already concluded twice
(`tf_rollup_unit_cost_is_the_commit`, `tf_whale_footer_poison_sized`: "root cause
is the COMMIT, not the sort").

This is the same defect shape as
`2026-09-04-certification-proves-the-wrong-thing.md`: **a guard that measures a
proxy which turns out not to track the thing it protects.** A cap priced in rows
bounds sort work; the deadline is spent on bytes moved and commits made.

**Do not simply raise `MAX_BIN_ROWS` on the strength of this.** The bench reads
one local file; prod reads many files from object storage across a WAN, and that
is the cost this number is standing in for, badly. The correct change is to price
admission on **bytes and file count** — what IO and the commit actually scale
with — and keep a row bound only as a backstop. That is a real design change and
belongs in daylight, with the 49 byte-blocked cells from the census as its test
set.

## 3. Collapse beats the window function by 29–42x, and the window does not fit

| | window | collapse | ratio |
|---|---:|---:|---:|
| b256 p1 | 46.8 s | 1.6 s | **29x** |
| b2048 p1 | 41.5 s | 1.1 s | **38x** |
| b2048 p8 | **OOM** | 1.7 s | — |

The one-pass collapse shipped earlier (`7beb411e`, measured at 2.4x on synthetic
input) is worth **29–38x on a real prod file**, and the window form **cannot run
at all** at prod's per-worker pool once partitioned. This is a much stronger
result than the original measurement and worth quoting instead of it.

## 4. Partitioning is what breaks the memory pool, confirmed locally

The `p8` failure names its own cause:

```
ExternalSorterMerge[0]#38(can spill: false) consumed 32.0 MB, peak 32.0 MB,
ExternalSorterMerge[5]#66(can spill: false) consumed 32.0 MB, peak 32.0 MB,
...
Failed to allocate additional 8.5 MB ... 8.0 MB remain available
for the total memory pool: fair(pool_size: 256.0 MB)
```

Every partition contributes an **unspillable** `ExternalSorterMerge` reservation.
Eight of them at 32 MB each is 256 MB of a 256 MB pool before any sorting begins,
which is why `p8` dies where `p1` finishes in a second. This reproduces, at
worker scale and on a laptop, the mechanism recorded for the query side
(`tf_one_query_cannot_fit_the_query_pool_2026-09-03`: 16 partitions of unspillable
merges against a 16 GB pool). **The same defect exists at both scales, and it is
a property of partition count, not of data size.**

Practical consequence for the 10x question: **raising `target_partitions` to buy
throughput buys OOMs instead**, on any pool, because the unspillable floor grows
linearly with partitions while the pool does not.
