# A 251-row query reads 431 GB — the ordering claim, not the scan guard

**Status:** root-caused and fixed 2026-08-28 (`next/prune`). The 08-25 framing of
this document — "refused at the scan guard" — is **FALSIFIED and rewritten
below**. What survives from it: the byte-identical selection, the 17/25 timeouts,
and the question it asked.

## Correction 1 — there is no scan-guard refusal any more

The 08-25 edition read `bench/local/matrix3.csv` (Aug 23):

```
p1,30d,log_list,,,,"FAIL: Resources exhausted: scan selected 1404 files /
                    431629 MiB, over the 16384 MiB per-scan limit"
```

and concluded the query is *refused at admission*. **That refusal no longer
exists.** It shipped as `5280d44a` ("refuse a single scan past a configured size,
default OFF") and has since been removed; `grep -rn "per-scan limit" src/` finds
only comments, and `src/database/mod.rs` states the policy outright: *"a per-scan
limit was tried and removed: users must be able to query months of data, so the
job is to make a 440 GB scan survivable, not to reject it."*

Prod (image `a90f881`, `timefusion_stats`) confirms scans of this size RUN:

```
wide_scan_oversize_total    24     (warning only, threshold 1 GiB)
wide_scan_selected_mb_p50   881
wide_scan_selected_mb_p90   4230
wide_scan_selected_mb_p99   48189   <- 48 GB selected by one scan
```

So the failure mode is a **statement timeout** (17 of the matrix's 25 failures),
not a refusal. Anyone optimising for the guard is working a population of zero.

## Correction 2 — "selected" is a PLAN-TIME number, and nothing was mispruned

`selected_file_work` (`src/database/mod.rs:9812`) folds
`FileScanConfig.file_groups`. It counts what the plan **enumerated**, not what
execution **read**. And the enumeration is decided entirely inside
`provider.scan(state, projection, filters, limit)` — in the delta-rs fork,
`KernelScanPlan::try_new_with_contract(...)` with the file-skipping predicate
(`crates/core/src/delta_datafusion/table_provider/next/mod.rs:762`). That is
partition values plus add-action statistics. Nothing else.

The `limit` argument never participates: it becomes `FileScanConfig::with_limit`,
a per-partition row cap, not a file selector. And under `ORDER BY … LIMIT n`
DataFusion keeps the fetch on the Sort (a TopK) anyway.

**This fully explains the 08-25 "decisive clue".** `log_list` and
`dcount_service` selected a byte-identical 1,404 files / 431,629 MiB because file
selection is a function of `(project_id, 30-day date range)` alone. With the
predicate being only `project_id = … AND timestamp BETWEEN lo AND hi`, selecting
every file in thirty date partitions is not a pruning bug — **it is the correct
answer**. 431 GB *is* thirty days of the whale's data. The clue was real; the
inference "nothing is pruning it" was the wrong reading of it.

**So hypothesis "files lack min/max timestamp statistics" is not the cause here.**
Stats can only trim the two boundary days of a 30-day BETWEEN. They cannot
account for three orders of magnitude.

## The actual mechanism — a cliff in the ORDERING claim

Reproduced locally, plan captured, in
`tests/e2e/ordering_pushdown.rs::one_unsorted_file_does_not_cost_the_majority_its_ordering`.

The delta-rs fork deliberately **isolates** files whose parquet footer does not
declare the scan's common ordering ("isolate, don't surrender",
`.../next/scan/mod.rs:815-885`): conforming files keep the `[timestamp DESC]`
claim in one `DataSourceExec`, the rest become a sibling `DataSourceExec` with no
claim, and the two are `UnionExec`'d. This was written because *"prod ran with 55%
of active files unsorted"* and one bad file used to void the claim for everything.

**But a `UnionExec` advertises an ordering only when EVERY child does.** So the
Delta leg as a whole still advertises none, and the isolation buys nothing at
TimeFusion's layer. Both consumers degrade at once:

* `ProjectRoutingTable::scan` cannot build its `SortPreservingMergeExec`, so
  `DedupExec` drops from bounded keep-greatest to the unbounded `full-set` mode;
* `OrderedUnionForTopK` finds no ordered child and no-ops, so `ORDER BY timestamp
  DESC LIMIT n` stays a **blocking `SortExec` over the whole window**.

The captured plan, one non-conforming file among conforming ones:

```
SortExec: TopK(fetch=3), expr=[timestamp@1 DESC]        <- blocking, reads everything
  FilterExec: deleted@2 IS DISTINCT FROM true
    DedupExec: keys=[timestamp, id], mode=full-set/greatest   <- unbounded
      CoalescePartitionsExec                                   <- not an SPM
        DeltaScanExec
          UnionExec
            DataSourceExec: 1 group, output_ordering=[timestamp@0 DESC, id@1 ASC]
            DataSourceExec: 1 group                            <- no claim
```

**It is a cliff, not a gradient: ONE non-conforming file in the window flips the
whole query from a streaming top-N into a full-window blocking sort.** That is
exactly the shape of the observation this document was written to explain —
`log_list` returns 251 rows in 1.6-8.0 s at every window from 1 h to 14 d, where
every file conforms, and dies at 30 d, where the sorting backlog reaches back far
enough to include a footer-less file.

So the honest statement of the bug is **not** "a 251-row query selects 431 GB" —
selection is correct — but **"a 251-row query READS the 431 GB it selected,
because a single unsorted file cost the scan its early-termination"**.

## The fix

`repair_isolated_scan_ordering` (`src/read/optimizers.rs`), called on the Delta
leg as it leaves `provider.scan`: sort each isolated unordered child of a mixed
scan union so the union advertises the ordering again.

Two constraints it respects, both bought with prior outages:

* **A byte budget, not a heuristic.** Sorting a whole-window parquet leg is the
  2026-08-02 and 2026-08-07 OOMs; size is the only thing separating those from
  this. `timefusion_read_sort_unordered_leg_max_mb` (default **64**, **0 disables the
  repair**) is the admission test, and `ordered_children` bails on the whole union
  as soon as one over-budget child misses the ordering — so the bad case is
  structurally unreachable rather than merely unlikely. 64 MB is COMPRESSED
  selected bytes, ~0.8 GB decoded: the same number and the same reason as
  `timefusion_wide_scan_max_mb`, and the repair runs on every Delta-reading query,
  so the ceiling is paid concurrently rather than once.
* **In the scan path, not as a `PhysicalOptimizerRule`.** `DedupExec` declares no
  required input ordering, so a sort injected before `EnforceSorting` is deleted
  as unused — the first attempt fired and was silently undone. Doing it where
  `ProjectRoutingTable::scan` receives the leg puts it under the
  `SortPreservingMergeExec` that `scan` builds and pins. No fetch is pushed:
  sorting a child changes no rows, so it stays sound beneath keep-greatest, where
  a top-n cut on a leg would truncate row versions.

Verified: the regression test was witnessed FAILING first, and now asserts **both**
halves of the repair (`SortPreservingMergeExec` present, `DedupExec` not in
`full-set` mode) **and** the decline at budget 0 — which returns the un-repaired
plan and the same rows. Budget 0 is not a separate early return, so pinning the
decline also pins the comparison's direction. `cargo lint` 0; `make test`
1230/1230; `make test-e2e` 62/62.

## What this does NOT fix, and what to measure next

1. **The budget may not admit prod's legs.** A compacted whale file is ~307 MB
   compressed, so a single non-conforming file already exceeds the 64 MB
   default by ~5x. The repair lands the common case (a handful of freshly-concatenated
   small files among thousands of sorted ones); it does not rescue a window whose
   unsorted side is itself whale-sized. **The lever for that remains REPAIR of
   the footers, not read-time sorting** — which is what the maintenance backlog
   work has been saying all along. Before tuning the budget up, measure the
   distribution of the isolated leg's bytes per query.
2. **A second, independent claim-loss was observed in the same fixture:** with
   TWO conforming files in one file group, `FileScanConfig`'s stats validation
   dropped the ordering for the *conforming* leg as well (`regroup` was false
   because `stats_backed_prefix_len` came back 0 — no footer sort stats for
   `timestamp`). That is a separate defect on the same path and is untouched
   here; it means the claim can be lost even with zero non-conforming files.
   Reproduce it by putting two flushed files in one group and checking for
   `output_ordering=` on the `DataSourceExec`.
3. **The 17 timeouts are still the larger population** and are not all this bug.
   Re-run `bench/local/query_matrix.py` against a build carrying the fix before
   attributing any of them.
4. **`tasks/05` still cannot be closed through the dedup path.** Its stated
   done-when was never reachable there; it belongs to this mechanism.

**Done when:** p1 30 d `log_list` returns 251 rows within the statement timeout,
and its plan shows a `SortPreservingMergeExec` with a fetch rather than a blocking
`SortExec`. Note the *selected*-file count is NOT the metric — it was never the
problem, and it will not move.
