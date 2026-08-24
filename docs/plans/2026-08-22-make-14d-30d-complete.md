# Making 14d and 30d *complete* for every project — a checklist

2026-08-22. Goal, deliberately lower than "sub-second": **every project's 14d
and 30d dashboard queries should return an answer instead of failing.** Today
most of them do not. This page lists what we can do, ordered by
(evidence it will work) × (how soon it lands), with the measurement behind each
item.

Baseline and method: `2026-08-22-seven-window-six-project-matrix.md` — six
projects × seven windows × nine of monoscope's real query shapes, prod,
read-only, warm rep quoted.

## What "fails" actually means — four distinct failure modes

Not one problem. The checklist below is organised around this split, because
the fixes do not overlap.

| mode | how it shows up | measured on |
|---|---|---|
| **A. Statement timeout** | `canceling statement due to statement timeout` at the 60 s `DEFAULT_MAX_STATEMENT_SECS` cap (`min(client, server)`, so a client cannot raise it) | most aggregates at 14d/30d, every project with data |
| **B. Per-query memory budget** | `Resources exhausted: unordered merge-on-read dedup exceeded its 2048 MiB per-query budget` | p1 `log_list` at 30d — while p4's same shape at 30d returns in 879 ms |
| **C. Server-wide unavailability** | new connections time out entirely while one wide scan runs; killed the harness once and blanked a `timefusion_stats` poll | during 30d aggregates |
| **D. Wedged query — no completion, no cancel** | a query ran **>20 min and the effective 60 s cap never fired** (client asked 70 s, server caps at 60 s, `min()` applies), while the server kept answering `SELECT 1` on new connections normally and the container had 2 h uptime | p5 7d `throughput`, prod `a7a4eb0` |

Mode C is the one to be loudest about: a single 30-day query is currently an
**availability** event, not just a slow query.

Mode D is worse than a timeout and deserves its own investigation. A cancelled
query returns an error the dashboard can render; a wedged one holds the
connection forever. It also means **the 60 s cap may not be a reliable
backstop** — any plan for 14d/30d that assumes "worst case, it times out at
60 s" is assuming something that did not hold in the p5 case.

**Evidence quality, stated because it cuts both ways.** The p5 instance is
clean: container up 2 h, `SELECT 1` instant on a new connection, no deploy in
the window. A second apparent instance — p6 7d `group_by_service` returning 169
rows after **189,775 ms** — is *confounded*: prod deployed mid-sweep
(`a7a4eb0` → `7e5bb5a` → `11cd17a`), and two `server closed the connection
unexpectedly` errors in the same p6 run were those restarts, **not** query
crashes as first read. So: one clean instance, one suggestive but contaminated.
Reproduce on a quiet build before treating mode D as characterised.

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
      **First action is a flag flip, not a wait.** The build currently deployed
      (`a7a4eb0`, "fingerprint fallback for witness-less slices, default OFF")
      already carries the fallback for the `no_witness` sub-case, gated behind
      `TIMEFUSION_ROLLUP_SLICE_FP_FALLBACK`. If `rollup_stale_no_witness`
      dominates `rollup_stale_moved` when read — prior measurement put
      `no_witness` at ~95% — then enabling that flag is the cheapest available
      test of item 1.1, with a kill switch. Read the two counters first; the
      flag does nothing for `moved`.
- [x] **1.2 `log_list@30d` is a FOOTER-REPAIR problem — rescoped 2026-08-23.**
      Not a read-path bug. 38 of p1's 86 file groups carry no footer
      `sorting_columns`; they form their own branch of the `DeltaScanExec` union,
      that union declares no ordering, so the delta leg declares none,
      `ordered_children` bails, no `SortPreservingMergeExec` is built,
      `detect_bound` returns `None`, and `DedupExec` runs `full-set` — the only
      mode with no LIMIT early termination and the only one charging the 2 GiB
      budget. The whale answers the identical shape at 30d in `bounded[timestamp]`.
      Both available read-path fixes are wrong (see the write-up: sorting the
      unordered branch is the reverted 2026-08-07 attempt; a top-K watermark is
      defeated by the `deleted IS DISTINCT FROM true` FilterExec between the sort
      and the dedup). Shipped `dedup_full_set_pct` instead, which reads **3.0** on
      prod — the backlog is now a number. Real fix is 2.3.
      `2026-08-23-log-list-30d-is-a-footer-repair-problem.md`.

- [x] **1.3 Wide scans can now be refused — shipped 2026-08-23.** `GatedScanExec`
      never failed to fire; it fires and refuses NOTHING by design, bounding how
      many wide scans decode at once and never how much any one of them decodes
      (`wide_scan_oversize_total` was pure observation, and read 4 within a minute
      of a restart). Shipped the refusal plus the distribution needed to size it,
      default 0; a day later `wide_scan_selected_mb_p99` read **3,506 MB** and the
      default became **16,384 MB** — 4.7x p99, about half the 32.8 GB scan that
      took the box down. `TIMEFUSION_WIDE_SCAN_REFUSE_MB=0` is the kill switch.

- [x] **1.4 `col IS NOT NULL` now routes — shipped 2026-08-23 (91030f9).**
      Dropped onto a `HAVING sum(count(col)) > 0` guard, which reads as one
      widening of the existing promotion rule: a measure
      `{agg: count, column: col, filter: F}` IS
      `count(*) FILTER (WHERE F AND col IS NOT NULL)`, so a single lookup covers
      the bare predicate, the promoted filter and — the case that matters — their
      CONJUNCTION. Two fail-closed disqualifiers: two different guard columns, and
      any aggregate not null-skipping over that same column (`count(*)` beside
      p95, which the top-K tables really send). Gate test fails without the fix on
      its routing assertion and passes with it.

- [ ] **1.5 Make the statement timeout actually fire.** Mode D: a 7d query ran
      >20 min against an effective 60 s cap and never cancelled, on a
      server that was otherwise healthy. Until this is understood, "it will at
      worst time out" is not a property we have. Find where the cancellation
      check is not reached — a tight loop with no yield point, or a blocking
      object-store call outside the cancel path, are the obvious candidates.

### Tier 2 — reduces the raw-path cost that remains

- [ ] **2.1 Certification needs a PARTIAL-GRANT producer — rescoped 2026-08-23.**
      Not an enablement task. The prod sidecar holds 106 certifications reaching
      2026-08-22, so contiguity is no longer the blocker; the blocker is that
      every recent entry belongs to a SMALL project and none to `dcad860a`,
      `87576849` or `00000000` — the three whose dashboards are slow.
      `record_certification` grants only when the whole partition's file
      fingerprint held still across the sweep, which a churning tenant's day
      never does. Additive per-file certification (4b91f8c) cannot rescue it
      because it resolves its file list FROM a granted `Certification`, so it
      sits downstream of the same gate. See
      `2026-08-23-certification-only-works-where-it-is-not-needed.md`.

- [x] **2.2 The 7-day cliff was a cold first touch — closed 2026-08-23.** Three
      reps per window on both projects: p3 7d = 33489 / 2600 / 1679 ms, p4 7d =
      46127 / 4477 / 1102 ms. Warm, 7d beats BOTH neighbours; counter diffs are
      identical in shape at every window and `tantivy_raw_files_total` rises
      smoothly. 14d only looked fine because the sweep ran narrowest-first and
      reused 7d's warm-in. No band, no plan difference. What remains is the
      LEVEL — 1.1–3.2 s warm for a point lookup, 15–40x cold — which is
      cold-start cost, tracked with the rest of the scan work.

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
- [x] ~~**3.2 Verify whether `AND duration IS NOT NULL` disqualifies the p95
      widget.**~~ **Done — confirmed.** Promoted to item 1.4; it needs no spec
      change and no rebuild. Evidence below.

## The p95 finding — measured, and it is a planner bug not a coverage problem

Counter-diff A/B on the whale at 3d, 3 reps per arm, requiring the reason to
increment ≥3 so a coincidence with live monoscope traffic is implausible.
Arms 2 and 3 are **character-identical except for `AND duration IS NOT NULL`**:

| arm | miss reason, ×3 |
|---|---|
| 1 `count(*)` by bucket, bare | `not_built` |
| **2 p95 exactly as monoscope emits it** | **`filter_not_eligible`** |
| **3 the same p95, minus `IS NOT NULL`** | **`not_built`** |
| 4 group-by `resource___service___name` | `not_built` |
| 5 error-rate with the verbatim declared predicate | `not_built` |
| 6 `dcount` | `missing_measure` |

Every other arm reports `not_built`, which is what a rebuilding tier reports
(`rollup_min_contiguous_days` was 0 on this young container). Arm 2 reports
`filter_not_eligible` **instead** — so the eligibility check runs *before*
coverage is consulted, and the conclusion is independent of the coverage state
this happened to be measured in:

> **The latency charts cannot route at any window, however complete the rollup
> tier becomes.** `duration_digest` is declared and unfiltered specifically to
> answer this widget, and a null-guard the widget always emits is what rejects
> it.

The spec rule doing this is deliberate and correct in general — "a filter on
any column NOT listed disqualifies a query, because a filter cannot be applied
after aggregation". `IS NOT NULL` against a measure that already skips nulls is
the case where it is over-strict: `count`/`sum`/`tdigest` over `duration`
ignore null rows anyway, so the predicate cannot change the answer.

Arm 6 independently confirms the `dcount` gap in item 3.1 — `missing_measure`,
not a filter problem.

### Confirmed in the code, not only in the counters

`src/rollup.rs:1741` — the residual survives promotion, and the guard asks
whether any column it constrains is mentioned by *any declared filter*:

```rust
if !promotable
    .iter()
    .flat_map(|expr| expr.column_refs())
    .any(|column| configured_filters.iter().any(|(_, filter)| filter.contains(column.name.as_str())))
{
    return MissReason::FilterNotEligible;
}
```

The declared filters mention `kind`, `name`, `status_code` and
`attributes___http___response___status_code`. They never mention `duration` —
`duration` is a measure *column*, not a filter column — so a residual of
`duration IS NOT NULL` matches nothing and declines. The counter measurement
and the code agree.

The guard itself is sound and was added for a good reason (2026-08-12: 84
declines in 3 h, all log-explorer/facet residuals, drowning the real
near-misses). The narrow gap is that it tests against *filter* columns only,
while a null-guard on a *measure* column can be a no-op — `count`, `sum`,
`min`, `max` and `tdigest` over `duration` all skip nulls. So the fix is to
drop `col IS NOT NULL` from the residual before this guard runs — not to weaken
the guard.

**The drop condition must be ALL measures, not the matched one — getting this
wrong is a silent wrong-answer bug.** A mixed query is the common case, and the
§6 top-K shapes are exactly it:

```sql
SELECT count(*), approx_percentile(0.95, percentile_agg(duration)) …
WHERE … AND duration IS NOT NULL GROUP BY bucket
```

`count(*)` maps to `request_count`, which counts rows **including** those with
a null `duration`. The predicate is therefore load-bearing for that measure —
drop it and `count(*)` silently returns a larger number than the raw path.
`p95` maps to `duration_digest`, for which the same predicate is a no-op.

So the rule is: strip `col IS NOT NULL` only when **every** measure the query
resolves to null-skips on `col` (i.e. aggregates `col` itself). If any resolved
measure is null-insensitive to `col` — `count(*)`, or an aggregate over a
different column — the predicate stays and the query declines exactly as it
does today. Conservative in the mixed case, correct in the pure-latency case,
which is the one that actually fails at 30d.

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

## The pass/fail baseline this goal is defined against

Completion only — ignore latency. `✓` = returned rows, `✗` = failed.
p1–p4 measured on `5062a7d`, p5–p6 on `a7a4eb0`+ (see the deploy caveat).

| project | 14d | 30d | notes |
|---|---|---|---|
| p1 `87576849` | 1 of 5 | 0 of 5 | only `log_list`; 30d `log_list` **OOMs** (mode B) |
| p2 `edb04135` | 5 of 5 | 3 of 5 | 14d is empty-window (no data newer than 14d) |
| p3 `00000000` | 3 of 6 | 1 of 6 | shared unified-default table, worst case |
| p4 `dcad860a` | 4 of 6 | 3 of 6 | whale; 30d `log_list` fine at 879 ms |
| p5 `be87ebc1` | **5 of 5** | 3 of 5 | best result; 30d p95 + error_rate time out |
| p6 `28f62f01` | 3 of 5 | 1 of 5 | one cell confounded by a deploy restart |

Two things this table says that the latency numbers hide:

- **`log_list` completes at 30d for five of six projects**, and the one
  exception fails on *memory*, not time. Item 1.2 alone moves p1 from 0/5 to
  1/5 at 30d.
- **`p95_latency` and `error_rate` are the two shapes that fail most often at
  30d.** Both are rollup-routable in principle — `duration_digest` and
  `error_count` are declared measures — so items 1.1 and 1.4 target exactly the
  cells that fail. Item 1.4 is confirmed to be what blocks `p95_latency`
  specifically, independent of coverage.

## The pass condition could not be run on 2026-08-23, and why

Attempted and abandoned with evidence. Over one working session prod was
observed running `fa5d2f7`, `6a5975a`, `b12b4ac` and `70f4a9b` — **a restart
roughly every 10-15 minutes**, against a `rollup_min_contiguous_days` rebuild
that takes ~25. It never reached 30. Two automated watchers polled for a valid
window and neither found one.

With coverage at 0 the router does not attempt, so every cell records neither a
hit nor a miss and the sweep reads exactly like "the fixes did nothing" — trap 1
below, in its most expensive form. Every other process-scoped counter is equally
unreadable: `wide_scan_selected_mb_p99` went 3,506 → 1,057 → 554 → 0 purely with
process age, and `rollup_witnessless_slices` reads 0 until the hourly recovery
pass has run once.

**So the precondition for this sweep is not a code state, it is a quiet prod.**
Nothing measured inside a 15-minute process is worth quoting. Before re-running:
confirm uptime exceeds the coverage rebuild, stamp every cell with
`docker service ps` (not `ls`), and reduce scope to the 14d and 30d cells — the
pass condition names only those, and a 30d aggregate is itself the mode-C
availability risk.

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

## The condition, last measured 2026-08-23: 39 of 60 cells

`24/30 at 14d, 15/30 at 30d` (six projects x five monoscope shapes x two
windows). Transplanted from `2026-08-23-the-14d-30d-pass-condition-measured.md`,
deleted 2026-08-24; the full per-cell grid is in git history. **Two failures that
need different work**, and conflating them wasted time before they were split:

1. **`p95_latency` is the single worst shape** — at 30d it fails on p3, p4 and
   p5, and on p4 and p5 it is the ONLY failure. This is what the
   `duration IS NOT NULL` routing fix (`91030f9`) and the `duration_digest`
   measure exist for. Fixing p95 routing alone takes 30d from 15/30 to 18/30 and
   clears p4 and p5 completely — blocked on the tier rebuilding, not on code.
2. **p1 and p6 are a volume problem, not a query-shape problem** — every one of
   their 30d cells is refused by the per-scan limit at
   `1,447 files / 450,603 MiB`. At ~310 MB per file that is data volume, not
   fragmentation, and no routing or dedup change touches it. Both were ALREADY
   failing before the limit existed (timeout, or the 2 GiB dedup OOM); they now
   fail in milliseconds with a message naming the size. p4's 30d `log_list` still
   returns 251 rows in 2.0 s, which was the specific risk of a default-ON refusal.

## 2026-08-24 — one blocker removed from every FILTERED cell

A filtered dashboard chart was paying a tantivy prefilter on the rollup leg it
routed to, because rollup dimensions had accidentally inherited
`tantivy: { indexed: true }` from the spans schema through a struct update. On
prod, over identical rows: `kind = 'server'` on a tier took **8,206 ms against
283 ms** for an opaque control, and the routed 7d chart went **7,240 ms ->
14,484 ms** purely by adding a `kind` filter. Fixed in `8698271`; after it the
direct arm is 211 ms and the routed arm records `prefilter_attempts +1` (the raw
leg) instead of `+2`.

This matters to the pass condition because the sweep's shapes are filtered ones.
Cells measured before 2026-08-24 afternoon carry that cost and are not comparable
with cells measured after — stamp the image, as trap 2 already says, and treat
`8698271` as the boundary.

Reading the third trap into the list above: **a mean is not a distribution, and a
single sample is not a mean.** The unfiltered 7d control ranged 5,447-10,715 ms
across two reps minutes apart on the same process. Where a counter can answer the
question instead of a timing, use the counter.
