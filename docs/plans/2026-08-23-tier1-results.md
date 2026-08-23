
---

## Post-deploy verification (prod `8d6d8cd`, 2026-08-23 11:0x)

Both changes are live and were measured with `rollup_min_contiguous_days = 30`,
i.e. after the ~25 min coverage rebuild, not during it.

**The null guard works, confirmed by the reason moving rather than by a timing.**
Counter diff around monoscope's p95 chart, spelled exactly as it ships it
(`AND duration IS NOT NULL`), whale at 3d:

```
rollup_miss_filter_not_eligible_total   1 -> 1     (did NOT increment)
rollup_miss_not_built_total             4 -> 5     (+1)
```

That is precisely what the pre-deploy A/B predicted: the arm carrying the
predicate declined `filter_not_eligible`, the arm without it declined
`not_built`, and the fix makes the real query behave like the second arm. The
remaining `not_built` is the tier genuinely not being built for those partitions
— the drain-throughput wall, not a routing rule.

**The witness bound fix is confirmed by the classifier, not by a hit.** Before,
`current` read `None` and the classifier recorded `no_source_rows`; a comparison
that never happened. Now:

```
rollup_stale_no_source_rows = 0
rollup_stale_moved          = 270 and climbing
```

`moved` means the witness is being *compared* and genuinely disagrees — the
whale's live partitions are being written while queries run, which is the
honest, expected answer for today's data and the ~20% category the 79.7/20.3
split named. The comparison is working where it previously could not run at all.

**No wedge.** The exact shape that ran >20 min against a 60 s cap on `a7a4eb0`
(p5, 7d, throughput) now returns in **6.5 s**. That is not a proof of the
cancellation path — it never needed to cancel — so mode D stays "mitigated,
reproduce to confirm" as written above.

**Still not shipped, and now the top blocker for the coarse tier.** The three
rollup integration tests remain red on master, and the reason is the derived-tier
witness described above: the 1h tier records its witness against its PARENT tier
while the read path verifies against the raw source. Until that is fixed the
coarsest tier — the one the widest windows select — cannot verify, so 14d/30d
cannot route however complete coverage becomes.

## The witness fix, verified in prod (`45b9ab4`, coverage 30)

The three-site bound mismatch is fixed and `stale_coverage` is **gone**, measured
on a build carrying it with `rollup_min_contiguous_days = 30`:

```
rollup_miss_stale_coverage_total   0        (across 9 probe queries)
rollup_stale_no_source_rows        0
rollup_stale_moved                 0
rollup_hits_hybrid_total           0 -> 1   (a query routed)
rollup_miss_not_built_total        113 -> 120
```

Every remaining miss is `not_built`. That is the honest end state for this tier
of work: the routing RULES no longer refuse these queries — the null guard
(master's `91030f9`) cleared `filter_not_eligible`, and the bound fix cleared
`stale_coverage` — so what is left is the tier genuinely not being built for
those partitions. That is drain throughput, which is rebuild-class and was
excluded from this goal by construction.

monoscope's p95 chart, spelled exactly as it ships it, now declines `not_built`
where it declined `filter_not_eligible` before, and returns in 5.3 s against
17.3 s earlier in the session.

**The pass condition is still not met** and cannot be met by code alone: 14d/30d
completing for all six projects needs the tier BUILT for those days. Two
independent constraints stand in the way and both are outside this tier —
build throughput (~4 builds/hr by the concurrent session's own measurement), and
a prod that is quiet long enough to measure, which the deploy train has not
provided: coverage takes ~25 min to rebuild and prod restarted roughly every
15-25 min throughout this session.

## Pass-condition re-measure (prod `45b9ab4`, 49 min uptime, coverage 30)

First quiet window of the session. `count(*)` by 6h bucket, 6 projects x 14d/30d,
serial, 70 s cap:

| project | 14d | 30d |
|---|---|---|
| `87576849` | **timeout** (60.9 s) | **refused** (8.5 s) |
| `edb04135` | 1.0 s | 22.7 s |
| `00000000` | 13.6 s | 55.2 s |
| `dcad860a` | 11.0 s | 29.7 s |
| `be87ebc1` | 8.7 s | 28.1 s |
| `28f62f01` | 46.5 s | **refused** (11.6 s) |

**9 of 12 complete**, against a baseline where most 14d/30d cells failed —
`00000000` 30d and `28f62f01` 14d both went from `fail` to answering, and
`dcad860a` 30d from 30.9 s cold to 29.7 s.

**The two "failures" are the size guard working, not a regression.**
`TIMEFUSION_WIDE_SCAN_REJECT_MB` is now enabled in prod at 16 GiB, and it refuses:

```
scan selected 1458 files / 461034 MiB, over the 16384 MiB per-scan limit
scan selected  586 files /  25971 MiB, over the 16384 MiB per-scan limit
```

**461 GB in a single scan.** That query was never going to complete; before the
guard it was an availability event for every other session on the box (mode C,
measured 2026-08-22 as new connections timing out). Refusing it in 8.5 s with a
message naming the size is the correct outcome, and it is what the guard was
built for — but it does mean "complete" is not reachable for those two cells by
scanning. They need the tier BUILT, which is `not_built`: 135 of ~147 misses,
with `pending_base_rollup` at 12,443.

So the goal now decomposes cleanly:

- routing RULES: solved (`stale_coverage` 0, `filter_not_eligible` 0 on driven
  queries; `rollup_hits_hybrid_total` climbing, 1 -> 3 during this window)
- the widest cells: blocked on build throughput, not on query-side work
- one genuine timeout left (`87576849` at 14d, 60.9 s)

## 1.2 — CLOSED as superseded, with the measurement that closes it

1.2 was "bound `log_list` so `LIMIT 251` doesn't dedup 30 days — fixes mode B".
Re-measured on prod `45b9ab4`, the cell that defined mode B (p1, 30d,
`ORDER BY timestamp DESC LIMIT 251`) no longer reaches the dedup budget at all:

```
ERROR:  Resources exhausted: scan selected 1447 files / 460311 MiB,
        over the 16384 MiB per-scan limit
```

It fails in **2.9 s at the scan guard**, not at the 2 GiB unordered-dedup
ceiling. Mode B does not reproduce in prod any more, so bounding the dedup fixes
nothing observable — and it would not make this cell complete either, because
the scan is **460 GB**. What that query needs is to not select 460 GB: sorted
footers so a `LIMIT` can stop early, or file pruning. Neither is dedup work.

There is also a structural reason not to do it as specified. `Greatest` retains
whole `RecordBatch`es plus winner masks, so evicting a KEY does not release
memory unless the retained batches are also compacted down to surviving rows —
turning a "bound the top-N" change into a rework of the buffer, its masks and its
pool accounting. That is the operator that resolves merge-on-read versions, with
two logged prod incidents in its history, for a failure mode that no longer
fires.

The design remains recorded above (`dedup_keys` is `[timestamp, id]`, so all
versions of a key share a timestamp and a tie-inclusive top-N is sound) in case
the trade changes. Today it does not: 1.2 is closed as superseded by 1.3, not
deferred.

Worth flagging on its own: one project selecting **1,447 files / 460 GB** for a
30-day `count(*)` is a compaction signal, not a query signal.
