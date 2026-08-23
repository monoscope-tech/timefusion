# Making 14d and 30d *complete* for every project — a checklist

2026-08-22. Goal, deliberately lower than "sub-second": **every project's 14d
and 30d dashboard queries should return an answer instead of failing.** Today
most of them do not. This page lists what we can do, ordered by
(evidence it will work) × (how soon it lands), with the measurement behind each
item.

Baseline and method: `2026-08-22-seven-window-six-project-matrix.md` — six
projects × seven windows × nine of monoscope's real query shapes, prod,
read-only, warm rep quoted.

## What "fails" actually means — three distinct failure modes

Not one problem. The checklist below is organised around this split, because
the fixes do not overlap.

| mode | how it shows up | measured on |
|---|---|---|
| **A. Statement timeout** | `canceling statement due to statement timeout` at the 60 s `DEFAULT_MAX_STATEMENT_SECS` cap (`min(client, server)`, so a client cannot raise it) | most aggregates at 14d/30d, every project with data |
| **B. Per-query memory budget** | `Resources exhausted: unordered merge-on-read dedup exceeded its 2048 MiB per-query budget` | p1 `log_list` at 30d — while p4's same shape at 30d returns in 879 ms |
| **C. Server-wide unavailability** | new connections time out entirely while one wide scan runs; killed the harness once and blanked a `timefusion_stats` poll | during 30d aggregates |
| **D. Wedged query — no completion, no cancel** | a query ran **>20 min with `statement_timeout = 70s` set and the timeout never fired**, while the server kept answering `SELECT 1` on new connections normally | p5 7d `throughput`, prod `a7a4eb0` |

Mode C is the one to be loudest about: a single 30-day query is currently an
**availability** event, not just a slow query.

Mode D is worse than a timeout and deserves its own investigation. A cancelled
query returns an error the dashboard can render; a wedged one holds the
connection forever. It also means **the 60 s cap is not a reliable backstop** —
any plan for 14d/30d that assumes "worst case, it times out at 60 s" is
assuming something that did not hold here. Reproduce it before trusting any
completion guarantee.

## The two root causes both modes A and B trace to

**1. The rollup tier answers nothing.** Across ~216 routing decisions covering
every monoscope dashboard shape, six projects and seven windows:

```
rollup_hits_full_total    = 0
rollup_hits_hybrid_total  = 0
rollup_misses_total       = 37 -> 307
rollup_min_contiguous_days = 30      <- coverage was VALID throughout
```

This was measured with coverage at 30, not during a post-restart rebuild, so it
is not the "absence of a miss is not evidence of a hit" artifact. Reason
breakdown diffed across the sweep: `stale_coverage` +25, `filter_not_eligible`
+10, `not_built` +8. Every wide query therefore runs the raw path.

**2. Every scan pays full merge-on-read dedup.**

```
dedup_denied_never_certified_pct = 100.0
cert_granted_total               = 0
```

`DedupExec` + its `SortPreservingMerge` are ~55% of the raw path (prior
measurement, over 23% real duplicates). Its required ordering is
**correctness** — removing the merge degrades keep-greatest to keep-first and a
merge-on-read table answers with the pre-update row. So this is a cost to
*avoid paying* via certification, never a cost to delete.

## The checklist

### Tier 1 — makes queries complete, no spec change, no rebuild

- [ ] **1.1 Fix `stale_coverage` so the rollup tier actually answers.**
      Single highest-value item: routing removes scan *and* dedup for every
      aggregate shape, which is modes A and B at once. Work is already in
      flight from a concurrent session (per-slice source-row witness). The
      split counters `rollup_stale_no_witness` / `_moved` / `_no_source_rows`
      now exist in prod — read them before choosing a fix, because
      `no_witness` is a *throughput* problem (republish clears it) and `moved`
      is not (the partition really is churning). Opposite fixes, same miss.

      **READ, 2026-08-23: `no_witness` 2,448 (79.7%) vs `moved` 624 (20.3%),
      `no_source_rows` 0, coverage valid at 30.** The `source_fp` fallback that
      would have rescued the 79.7% without a republish was removed in `7e5bb5a`
      — the slice and partition fingerprints are computed by different hashers
      over different file sets, so with the flag on a witness-less slice routed
      NOTHING, measured end-to-end. There is no lever here short of republish
      throughput, which is rebuild-class and outside this tier. Closed, not
      deferred — see `2026-08-23-tier1-results.md` §1.1.
- [ ] **1.2 Bound `log_list` so TopK never deduplicates the whole window.**
      `ORDER BY timestamp DESC LIMIT 251` should resolve versions for ~251
      rows, not 30 days of them. p4 answers this shape at 30d in 879 ms and p1
      OOMs on it — so the blowup tracks duplicate density, not window width.
      Fixes mode B directly and is the narrowest change on this page.
- [ ] **1.3 Add an admission guard so one wide scan cannot make the box
      unreachable.** Mode C. Today the failure is silent and global; at minimum
      it should be a rejected query with a clear error, not refused
      connections. (Note a scan-pressure valve / `GatedScanExec` already exists
      in `database/scan.rs` — establish why it did not fire here before
      building anything new.)
- [ ] **1.4 Make the statement timeout actually fire.** Mode D: a 7d query ran
      >20 min against `statement_timeout = 70s` and never cancelled, on a
      server that was otherwise healthy. Until this is understood, "it will at
      worst time out" is not a property we have. Find where the cancellation
      check is not reached — a tight loop with no yield point, or a blocking
      object-store call outside the cancel path, are the obvious candidates.

### Tier 2 — reduces the raw-path cost that remains

- [ ] **2.1 Get certification working so `dedup_skipped` is non-zero.**
      100% of scans currently denied, `cert_granted_total = 0`. Prior sessions
      found certification itself works and the blocker is *contiguity* (the
      read path wants every in-window date certified). Per-date dedup skip
      shipped default-OFF — evaluate enabling it, since partial certification
      only pays once that lands.
- [ ] **2.2 Investigate the 7-day cold-needle cliff.** Non-monotonic and
      reproduced on two independent projects:

      | project | 3d | 7d | 14d | 30d |
      |---|---|---|---|---|
      | p3 | 395 ms | **43,401 ms** | 1,155 ms | — |
      | p4 | 258 ms | **35,106 ms** | 1,032 ms | 6,012 ms |

      7d is 30-160x its neighbours on both sides. That is a coverage-band
      boundary (foyer warm window, or tantivy's newest-first backfill starving
      a band), not a smooth degradation — and it is invisible if you only
      sample 3d/7d/30d without the neighbours.
- [ ] **2.3 Compaction / file count.** Fewer, larger files reduce both planning
      and open cost. Already the subject of its own workstream; listed so the
      dependency is explicit rather than because it needs new analysis.

### Tier 3 — needs a spec decision or a rebuild (do not start first)

- [ ] **3.1 Declare the missing rollup measures.** Measured gaps against the
      shapes monoscope actually sends:
      - `dcount` (§5) — no `hll` measure declared, so it can never route.
      - `kind IN ('server','client')` — the specs declare `request_count`,
        `error_count` and three `server_*` measures; nothing covers this
        conjunction, so it declines `unknown_filter` even after the
        text_match-hint fix landed.
      - `level` / `status_code` filters — `filter_not_eligible`; `level` is
        not a declared dimension.
      Each is a spec change plus a 30-day rebuild, which is wall-clock physics.
- [ ] **3.2 Verify whether `AND duration IS NOT NULL` disqualifies the p95
      widget.** monoscope emits it on every latency chart (§3a). `duration` is
      not a declared dimension and the spec's own rule is "a filter on any
      column NOT listed disqualifies a query from routing" — yet
      `duration_digest` exists precisely to answer that widget. If confirmed,
      the fix is to recognise `col IS NOT NULL` as a no-op against a measure
      that already skips nulls, and it would unblock every latency chart at
      every window for free. **Unverified** — the counter-diff A/B is written
      (`routing_ab.py`, arms 2 vs 3 isolate exactly this predicate) but has not
      been run.

### Explicitly NOT on this list, with the measurement that says so

- **Removing `DedupExec`'s `SortPreservingMerge`** (~502 ms, 930 MB). Its
  ordering is what keeps keep-greatest from degrading to keep-first under
  merge-on-read. Correctness, not overhead.
- **A provider cache for file-pruned scans** — `pruned_build_us` is 0.10 ms,
  0.1% of the pruned-scan path. Keeps looking attractive; keeps being noise.
- **Raising the 60 s statement cap.** It converts mode A into mode C. The cap
  is deliberate and tested; the queries genuinely exceed it.
- **"Mirror the Delta log into a second database" to fix planning.** The
  ghost-project control shows planning cost tracks the *window asked for*, not
  the files found, and delta-rs already keeps the snapshot resident.

## How to tell it worked

The pass condition is binary and cheap to re-measure: re-run the sweep and
require **zero `fail` cells at 14d and 30d** for all six projects. Sub-second is
a separate, later goal — today three of six projects cannot answer a 30-day
`count(*)` at all.

Two traps when re-measuring, both hit during this work:

1. **Read `rollup_min_contiguous_days` first.** It resets to 0 on restart and
   takes ~25 min to rebuild; with it at 0 the router may not attempt at all, so
   shapes record neither hit nor miss and it reads exactly like "the fix did
   nothing".
2. **A deploy mid-run splits the dataset.** Prod restarted twice during this
   session (once from a concurrent session's push), each time zeroing coverage
   and the caches. Stamp every cell with the image it ran on.
