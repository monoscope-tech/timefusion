# Adding sort-order pushdown to the delta-rs "next" DeltaScan

**Status:** implemented in the fork on branch `timefusion-variant-dml`
(commit `5daa461`, 2026-06-19) — verified: fork lib unit test
`remap_ordering_keeps_satisfiable_prefix` passes; TF builds end-to-end against
it via a local path patch. Remaining to land: push the fork commit and bump the
rev in TF `Cargo.toml`/`Cargo.lock` (currently `2f473fe`). Note: the plan's
assumption that `compute_all_files_statistics` already has each file's
`ParquetMetaData` was wrong — it aggregates Delta-log stats, so the
implementation reads footers explicitly at plan time via `DFParquetMetadata`
(cache-backed, front-loading reads the scan does anyway).
**Date:** 2026-06-17 (implemented 2026-06-19).
**Depends on:** Option A (honest `set_sorting_columns`, shipped 2026-06-17 — flush/dedup declare
the sort order, optimize/compact declare `None`). See
[2026-06-17-parquet-sorting-columns-inconsistency.md](./2026-06-17-parquet-sorting-columns-inconsistency.md).

---

## Goal

Make TimeFusion's reads *exploit* the fact that flushed/deduped files are physically sorted by
`[timestamp, resource___service___name, id, level, status_code]`, so the query engine can:

- skip the `SortExec` for `ORDER BY timestamp [, …]`,
- satisfy `ORDER BY timestamp … LIMIT n` by reading the first n rows of the first file(s)
  (`TopK`/limit short-circuit) instead of scanning + sorting the partition,
- feed sort-preserving merges/joins without a re-sort.

Today none of this happens: the `DeltaScanExec` advertises **no** output ordering, so DataFusion
assumes the scan is unordered and always inserts a sort.

This is the natural payoff of Option A — once the footer honestly says "sorted," the reader can
trust it. (Per-file footer-derived ordering means we only ever claim an order that's real.)

---

## Current state — the two gaps (delta-rs fork `2f473fe`)

Read path: `ProjectRoutingTable::scan` (TF) → delta-rs "next" `DeltaScan` → `get_read_plan`
(`crates/core/src/delta_datafusion/table_provider/next/scan/mod.rs`).

**Gap 1 — the inner parquet scan never advertises ordering.** `get_read_plan` builds the
`FileScanConfig` without `.with_output_ordering(...)`:

```
# scan/mod.rs ~546
let config = FileScanConfigBuilder::new(store_url, Arc::new(file_source))
    .with_file_groups(file_groups)
    .with_statistics(statistics)
    .with_limit(limit)
    .with_expr_adapter(Some(adapter_factory.clone() as _))
    .build();                              // <- no with_output_ordering
plans.push(DataSourceExec::from_data_source(config) ...);
```

**Gap 2 — `DeltaScanExec` publishes empty equivalence properties.** Even if the inner scan were
ordered, the wrapping exec discards it (`scan/exec.rs:161`):

```
let properties = Arc::new(PlanProperties::new(
    EquivalenceProperties::new(Arc::clone(&scan_plan.contract.output_schema)),  // <- no orderings
    ...
));
```

So the `DeltaScanExec` tells DataFusion "unordered," and `EnforceSorting` adds a `SortExec`.

DataFusion has everything needed; delta-rs just doesn't wire it:
- `FileScanConfigBuilder::with_output_ordering(Vec<LexOrdering>)`
  (`datafusion-datasource-53.1.0/src/file_scan_config.rs:457`). Setting it auto-enables
  `preserve_order` (`:534`), which makes `EnforceDistribution` keep each file group's order and
  insert a `SortPreservingMergeExec` across groups rather than concatenating.
- `EquivalenceProperties::new_with_orderings(schema, &[LexOrdering])`
  (`datafusion-physical-expr-53.1.0/src/equivalence/properties/mod.rs:221`).
- `ordering_from_parquet_metadata(&ParquetMetaData, &SchemaRef) -> Result<Option<LexOrdering>>`
  (`datafusion-datasource-parquet-53.1.0/src/metadata.rs:700`) — converts a file's footer
  `SortingColumn`s into a `LexOrdering` over a given schema. This is the honest-by-construction
  source: after Option A only actually-sorted files carry the metadata.

Precedent: DataFusion's own `ListingTable` does exactly this —
`try_create_output_ordering` (`datafusion-catalog-listing-53.1.0/src/table.rs:346`) →
`.with_output_ordering(...)` (`:584`).

---

## Why it's safe to advertise ordering here

1. **Honest footers (Option A).** Only flush/dedup files declare the sort order; Z-order/Compact
   declare `None`. Deriving the ordering from each file's footer therefore never over-claims.
2. **The kernel transform layer is order-preserving.** `finalize_transformed_batch`
   (`scan/mod.rs` ~563) only does `project` / `cast` / append-`file_id` — no row reordering,
   no filtering that permutes. Selection vectors drop rows but keep order. So the order the
   parquet reader emits survives to `DeltaScanExec`'s output.
3. **Multi-file & overlap handled by DataFusion.** `preserve_order` makes DataFusion merge the
   per-file-sorted groups with a `SortPreservingMergeExec`; it will not split one file across
   groups (which would break the per-group order). Files from different `(project_id, date)`
   partitions still merge correctly because the ordering is on data columns
   (`timestamp`, …), not partition columns.
4. **Prefix matching is automatic.** Advertising `[timestamp, service_name, id, level,
   status_code]` satisfies `ORDER BY timestamp`, `ORDER BY timestamp, service_name`, etc.
   (DataFusion uses the longest matching prefix).

---

## The one hard constraint: scan-wide ordering vs mixed files

`FileScanConfig.output_ordering` is **one ordering for the whole scan** (DF 53.1.0 has no
per-file ordering). It is valid only if **every file group** in the scan is sorted the same way.

A partition in steady state is **mixed**: recent files are flush-sorted (footer present), older
files have been Z-ordered by optimize (footer `None`). If any file lacks the ordering, we must
**not** set `output_ordering` for that scan (else we'd claim order the union doesn't have).

Two ways to live with this:

- **Conservative (correct anytime):** set `output_ordering` only when *all* files in the scan
  report the same footer ordering. Benefit accrues to fully-flushed-not-yet-optimized partitions
  (the hot, most-queried recent data) and to any table whose optimize also sorts.
- **Full benefit:** make optimize/compact **sort by the lead key** instead of Z-order
  (Option B in the inconsistency doc). Then every file in a partition is timestamp-sorted, the
  footers all agree, and the ordering pushdown fires on historical partitions too. This is the
  combination that makes the headline point-lookup / `ORDER BY timestamp LIMIT` fast on old data.

So: this pushdown + Option B together are what fully pay off; the pushdown alone helps the
recent (un-optimized) window. Decide Option B from the page-pruning numbers (inconsistency doc,
investigation step 3).

---

## Implementation plan (in the delta-rs fork `tonyalaribe/delta-rs-timefusion`)

The fork already carries TF-specific patches (see the `deltars_fork` memory), so this lands there.

1. **Derive a scan-wide `LexOrdering`** in `get_read_plan` (`scan/mod.rs`), before building each
   `FileScanConfig`:
   - The `ParquetMetaData` for each file is already loaded for stats/pruning
     (`compute_all_files_statistics`). For each file call
     `ordering_from_parquet_metadata(&meta, &parquet_read_schema)`.
   - Fold across files in the group: keep the ordering only if **all** files return the **same**
     non-empty ordering; otherwise `None`. (Helper: compare the resulting `LexOrdering`s.)
   - Map the ordering's column indices to the **scan output schema** (the parquet read schema
     is pre-`file_id`-append; the sort columns are data columns and keep their positions, but
     re-resolve by name against `scan_plan.contract.output_schema` to be safe).
2. **Wire it into the inner scan:** when present,
   `FileScanConfigBuilder::…​.with_output_ordering(vec![lex])`. This flips `preserve_order=true`
   and DataFusion handles the merge.
3. **Propagate to `DeltaScanExec`:** replace `EquivalenceProperties::new(schema)` at
   `scan/exec.rs:161` with `EquivalenceProperties::new_with_orderings(schema, &[lex])` when an
   ordering was derived (thread it through `KernelScanPlan`/the exec constructor). Re-express the
   `LexOrdering` over `output_schema` (post-transform) — same columns, resolve by name.
4. **Guard:** if `output_schema` doesn't contain a sort column (projection pruned it), drop that
   trailing part of the ordering (keep the satisfiable prefix) or skip — never advertise an
   ordering referencing a column not in the output.

Surface area: ~1 helper + 2 call sites (`get_read_plan`, `DeltaScanExec::new`). No DataFusion
changes. TF itself needs no change beyond bumping the fork rev.

### Interaction with the 2026-06-17 fixes
- **Fix #2 (target_partitions):** `preserve_order` stops DataFusion from splitting a single file
  into byte-range groups for ordered scans, which *also* removes the within-file fan-out for
  these queries — complementary to capping `target_partitions`.
- **Fix #4 (flush sort) + Option A:** are the prerequisites — they make the footer honest so this
  pushdown is correct.

---

## Query wins unlocked

- `... WHERE timestamp BETWEEN a AND b AND project_id=… ORDER BY timestamp LIMIT n` → no
  `SortExec`; `TopK`/limit reads the front of the merged stream and stops. This is the dashboard
  "latest N events in window" pattern.
- Range scans that currently sort a whole partition to satisfy an `ORDER BY` collapse to a
  `SortPreservingMergeExec` over already-sorted files (cheap, streaming, bounded memory — relevant
  to the TopK-projection OOM history, see `topk_projection_oom` memory).
- Any downstream operator that benefits from sorted input (merge join, dedup `row_number()`
  window over `(id ORDER BY timestamp)`).

---

## Validation

1. `EXPLAIN` an `ORDER BY timestamp LIMIT 50` over a recent (flush-only) partition: the plan
   should show **no `SortExec`** above `DeltaScanExec`, and a `SortPreservingMergeExec` if
   multiple files. Compare against an optimized partition (still has `SortExec` until Option B).
2. Correctness: assert results are actually ordered and the `LIMIT` returns the true first-n
   (a wrong ordering claim would surface as out-of-order / wrong rows here — make this a
   regression test in the fork against a partition with overlapping-range files).
3. Perf: EXPLAIN ANALYZE the dashboard query before/after; expect the sort + its memory to vanish
   and latency to drop on the recent window.

---

## Key references

delta-rs fork (`crates/core/src/delta_datafusion/table_provider/next/`):
- `scan/mod.rs ~546` — `FileScanConfigBuilder` build (add `with_output_ordering`).
- `scan/mod.rs ~563` — `finalize_transformed_batch` (order-preserving transform; confirms safety).
- `scan/exec.rs:161` — `PlanProperties::new(EquivalenceProperties::new(...))` (add orderings).

DataFusion 53.1.0:
- `datafusion-datasource/src/file_scan_config.rs:457` `with_output_ordering`; `:534` auto
  `preserve_order`.
- `datafusion-physical-expr/src/equivalence/properties/mod.rs:221` `new_with_orderings`.
- `datafusion-datasource-parquet/src/metadata.rs:700` `ordering_from_parquet_metadata`.
- `datafusion-catalog-listing/src/table.rs:346,584` — ListingTable precedent.

TimeFusion:
- `src/database.rs build_writer_properties` — `declare_sorted` (Option A) is what makes the
  footer honest and this pushdown sound.
- Schema sort order: `schemas/otel_logs_and_spans.yaml` `sorting_columns`.
