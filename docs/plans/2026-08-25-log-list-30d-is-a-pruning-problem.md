# The p1 30d `log_list` failure is not an OOM — it is a scan-guard refusal

**Status:** premise corrected 2026-08-25 from `bench/local/matrix3.csv`, no code
written. Supersedes the framing in `tasks/05` and in item P2-8 of
`tasks/01-NEXT-2026-08-25.md`.

## What the item said, and what the data says

`tasks/05` is titled "p1 30d `log_list` OOM" and its done-when is "251 rows
instead of exhausting the budget". The matrix row is:

```
p1,30d,log_list,,,,"FAIL: Resources exhausted: scan selected 1404 files /
                    431629 MiB, over the 16384 MiB per-scan limit"
```

**That is the scan guard doing its job, not an out-of-memory.** The query is
refused at admission, before execution — so it never reaches `DedupExec`, and
**no amount of bounded top-N dedup work can change this outcome.** The retention
fix merged as `6a401dc` was correct on its own terms and is simply not the lever
here.

## The decisive detail: the refusal is shape-independent

`log_list` and `dcount_service` fail with a **byte-identical** selection —
1,404 files / 431,629 MiB. Two different query shapes selecting exactly the same
file set means **the selection is decided before the shape matters**: nothing in
the 30-day window is pruned for p1. 431 GB over 1,404 files is ~307 MB/file, i.e.
properly compacted files — this is not fragmentation, it is simply thirty days of
the whale's data with no pruning applied.

## Why this is the interesting question

`log_list` returns **251 rows** and does so in 4.4-8.0 s at every window from 1 h
to 14 d. It is a newest-first listing with a small limit. A limit-aware scan
should read the newest files and stop; instead the planner selects the entire
window and the guard refuses it. **The bug is that a 251-row query selects 431 GB.**

Corroborating: **p2 at 30 d `log_list` succeeds** (4.66 s, 251 rows). So this is
not "30 days is too much" in general — it is specific to the tenant whose 30-day
file set exceeds the guard.

## The failure population, for scale

25 failures in the matrix:

| count | failure |
|---|---|
| 17 | statement timeout |
| 5 | scan guard, 597 files / 26,418 MiB |
| 2 | scan guard, 1,404 files / 431,629 MiB |
| 1 | connection closed |

**The dominant failure mode is the statement timeout, not the guard.** Anyone
optimising for the guard first is working the smaller half.

## What to do

1. **Do NOT close `tasks/05` and do not build more dedup bounding for it.** Its
   stated done-when cannot be reached through the dedup path.
2. Establish why a `LIMIT`-bearing newest-first query does not prune to the
   newest files — is the limit pushed to the scan at all, and does file selection
   consult it? That is the fix, and it likely helps every windowed listing.
3. Re-target the item at the 17 timeouts, which are the larger population.

**Done when:** p1 30 d `log_list` returns 251 rows, and the selected-file count in
the refusal message drops by an order of magnitude — the count is the metric,
because it is what the guard measures and it is printed on every failure.
