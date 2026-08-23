
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
