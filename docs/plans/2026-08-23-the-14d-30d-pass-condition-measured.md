# The 14d/30d pass condition, measured: 39 of 60 cells pass

2026-08-23, prod, read-only. First complete measurement of the condition
`2026-08-22-make-14d-30d-complete.md` sets: **zero fail cells at 14d and 30d for
all six projects.** Script `pass14_30.py`, results `pass14_30.csv` (six projects
× two windows × five monoscope shapes, serial, ≥2 warm reps under 15 s).

## The correction that made this runnable

I had recorded this as blocked on a quiet prod. That was wrong, and the error is
worth naming: **the pass condition asks whether a query COMPLETES, not whether it
routes.** A routing miss still answers from the raw path. `rollup_min_contiguous_days`
only has to be 30 for the routing *attribution* to mean anything — the pass/fail
measurement is valid at any coverage. Conflating the two cost several hours of
waiting for a window that never came.

## Result

| | 14d | 30d |
|---|---|---|
| p1 `87576849` | 2/5 | **0/5** |
| p2 `edb04135` | 5/5 | 5/5 |
| p3 `00000000` | 5/5 | 2/5 |
| p4 `dcad860a` | 5/5 | 4/5 |
| p5 `be87ebc1` | 5/5 | 4/5 |
| p6 `28f62f01` | 2/5 | **0/5** |

**24/30 at 14d, 15/30 at 30d.** The condition is not met.

## Two failures, not one, and they need different work

**1. `p95_latency` is the single worst shape.** At 30 d it fails on p3, p4 and p5
— and on p4 and p5 it is the *only* failure. Every other shape those two run
completes.

```
30d   throughput        p1=err p2=ok p3=ok   p4=ok   p5=ok   p6=err
      group_by_service  p1=err p2=ok p3=fail p4=ok   p5=ok   p6=err
      p95_latency       p1=err p2=ok p3=fail p4=fail p5=fail p6=err
      error_rate        p1=err p2=ok p3=fail p4=ok   p5=ok   p6=err
      log_list          p1=err p2=ok p3=ok   p4=ok   p5=ok   p6=err
```

This is exactly the shape the `duration IS NOT NULL` routing fix (91030f9)
targets, and `duration_digest` exists precisely to answer it. **Fixing p95
routing alone takes 30 d from 15/30 to 18/30 and clears p4 and p5 completely.**
It is blocked only on the tier rebuilding — no further code.

**2. p1 and p6 are a volume problem, not a query-shape problem.** Every one of
their 30 d cells is refused by the new per-scan limit:

```
scan selected 1447 files / 450603 MiB, over the 16384 MiB per-scan limit
```

**440 GB across 1,447 files** — ~310 MB each, so this is not fragmentation, it is
data volume. No routing or dedup change touches it. Checked that the number is
real and not double-counted: p1's plan at 3 d holds a single `DeltaScanExec` over
one `DataSourceExec`, so `selected_file_work` is not summing overlapping legs.

## The refusal, checked for regression

Every p1/p6 30 d cell was ALREADY failing before this landed — timeout or the
2 GiB dedup OOM. They now fail in milliseconds with a message naming the size and
the remedy, which is the stated goal for mode C. And the shape that *worked*
still works: **p4's 30 d `log_list` returns 251 rows in 2.0 s.** That was the
specific risk of a default-ON refusal and it did not materialise.

One defect found and fixed while measuring: refusing inside `scan()` also refused
`EXPLAIN`, removing the one tool that explains a refusal. The refusal now rides on
`GatedScanExec`, prints in its display line, and fires at execute.

## What else the numbers say

- **`log_list` is fast everywhere it is not refused** — 0.4–3.8 s at 30 d across
  four projects. The TopK path is fine; p1's is a casualty of volume.
- **The passing 30 d aggregates sit at 27–52 s against a 60 s cap.** p2 is 30.9 s,
  p4 27.1 s, p5 28.2 s. These are *passing* cells one growth spurt from failing,
  so "15/30" overstates the health of the half that works.
- **p2 is the planning-floor control and it holds.** 0.5–1.9 s at 14 d with zero
  rows, confirming the floor found earlier.
