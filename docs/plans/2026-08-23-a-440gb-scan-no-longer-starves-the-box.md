# A 440 GB scan no longer starves the box — measured, on prod

2026-08-23, prod `7d9b59a`, read-only. The acceptance test for the direction
"make the queries work without starving everything else or OOMing", replacing the
per-scan size limit that was removed in the same commit.

## What was actually wrong

Mode C — "a single 30-day aggregate makes the box unreachable" — was never about
the query being slow. It was that **new connections timed out while it ran**, so
one tenant's dashboard took the database away from every other tenant. That is
what made a per-scan refusal look necessary.

The real cause was diagnosed elsewhere the same day: unbounded keep-greatest
never yielded, so one `poll_next` ran for minutes of pure CPU, and
`tokio::time::timeout_at` can only fire when a poll returns `Pending`. Prod
2026-08-22 had a 7-day aggregate run **>20 minutes against a 60 s cap that never
fired**. `make_cooperative` fixes that, and a statement timeout that actually
fires is the real bound on one query's damage — not a size limit.

## The test

`bench/pass-condition/reachability.py`. One connection runs p1's 30-day
`throughput` aggregate — the 440 GB scan. A second thread opens a **fresh**
connection every 0.5 s and times `SELECT 1`. Fresh, not pre-opened: a pooled
connection would not reveal an accept loop that has stopped accepting, which is
exactly the symptom.

## Result

```
baseline:    3 probes, median 2314 ms
heavy query: 61985 ms -> canceling statement due to statement timeout
probes DURING: n=23  median=1065 ms  max=8184 ms
connect FAILURES during the whole run: 0
VERDICT: REACHABLE
```

Two things worth separating:

1. **The heavy query is bounded now.** It stopped at the 60 s statement timeout
   instead of running for 20 minutes. That is the cancellability fix working on
   the exact shape that motivated it.
2. **Nothing else was starved.** 23 fresh connections landed while it ran, none
   failed, and the median was *lower* than the idle baseline — the baseline
   sample is small and warm-up dominated, so treat them as equal rather than as
   an improvement.

So the size limit was compensating for a bug that no longer exists. Removing it
restores the honest behaviour: a query too big to answer in 60 s fails as a
timeout, on its own connection, without taking anyone else down.

## What this does NOT claim

p1's 30-day aggregate still does not **answer**. Reachability is not the same as
working, and the goal is months of data, so the remaining work is unchanged — it
just no longer competes with an availability problem:

- **Aggregates** route to the rollup tier and read pre-aggregated rows instead of
  440 GB. Blocked only on `not_built`; the rebuild queue has drained
  12,350 → 1,812.
- **`log_list`/TopK** needs `DedupExec` in `bounded[timestamp]` so `LIMIT 251`
  terminates early instead of buffering the window — the footer-repair backlog,
  visible as `dedup_full_set_pct`.
- **Needles** already work: 75–330 ms at 30 d via bloom/tantivy pruning.

No shape needs to read 440 GB. p1 and p6 hit it because all three mechanisms are
currently missing for them at once.

## Worth its own look

The **idle** baseline for opening a connection was **2.3 s**, and an earlier run
measured 5.5 s. That is not starvation under load — it is what connecting costs
when nothing is happening, and it is a large fraction of a dashboard's budget
before a single row is read.
