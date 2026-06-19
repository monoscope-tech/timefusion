# Parquet `sorting_columns` declared-vs-actual inconsistency on optimize/compact/recompress

**Status:** investigation handoff — not yet fixed.
**Date:** 2026-06-17.
**Related work shipped same day:** the four point-lookup fixes (metadata-cache limit wiring,
`target_partitions` from cgroup quota, per-column page stats, and **flush-path sort** —
see `metadata_cache_limit_dead` memory + git log). This doc is the deferred 5th item.

---

## TL;DR

Every parquet file TimeFusion writes declares a `SortingColumn` footer (via
`set_sorting_columns(...)` in `build_writer_properties`), claiming rows are ordered by
the schema's `sorting_columns` = `[timestamp, resource___service___name, id, level, status_code]`.

That claim is only **true on the flush/append path** (after the 2026-06-17 fix, which sorts
batches before write) and on the **dedup-rewrite path** (also fixed). It is **false** on the
three optimize-family paths, which rewrite those same files in a different order but keep
declaring the schema sort order:

| Write path | `database.rs` | Declares `sorting_columns`? | Actual row order written |
|---|---|---|---|
| Flush / append (`insert_records_batch`) | ~2685 | yes | **lexicographic by sort keys** ✅ (fixed 2026-06-17) |
| Dedup rewrite (`dedup_*` replace_where) | ~3358/3402 | yes | **lexicographic by sort keys** ✅ (fixed 2026-06-17) |
| Full Z-order optimize (`optimize_table`) | 2958/2964/2973 | yes | **Z-order interleave** of `z_order_columns` ❌ |
| Recompress tier (`recompress_partition`) | 3164/3171/3183 | yes | Z-order interleave (or Compact if no z_order cols) ❌ |
| Light optimize (`optimize_table_light_inner`) | 3523/3564/3569 | yes | **Compact** = concatenation of input files, no re-sort ❌ |

**Severity today: latent, not a live correctness bug.** TimeFusion reads through delta-rs's
"next" `DeltaScan`, which does **not** read the footer `sorting_columns` back to advertise a
scan output ordering. So nothing in the current query path trusts the (false) declared order.
It becomes a real bug the moment anything *does* trust it (see "When this bites").

**Performance angle (more immediately relevant):** the 2026-06-17 flush sort makes freshly
flushed files timestamp-sorted, so a `WHERE timestamp BETWEEN ...` point lookup prunes to ~1
page. But the optimize job (every 30 min over a 48h window) and recompress **rewrite those
files into Z-order**, which interleaves timestamp bits with id/service_name — so timestamp page
locality is partially lost on every partition that has been optimized (i.e. everything older
than the current 10-min flush bucket). The headline point-lookup query hits optimized data.

---

## Background: what `sorting_columns` is

Parquet lets a writer record, per row group, which columns the rows are sorted by
(`SortingColumn { column_idx, descending, nulls_first }`). It is **metadata only** — parquet
does not verify it. Readers may use it to:

- skip a sort they'd otherwise do (`ORDER BY timestamp` becomes a no-op),
- do binary-search / tighter page pruning,
- feed a merge that assumes ordered inputs.

If the declared order doesn't match the actual row order, any reader that *trusts* it can
return wrong/out-of-order results or wrong `LIMIT` rows.

In TF the declaration is unconditional:

```
# src/database.rs  build_writer_properties()
.set_sorting_columns(if sorting_columns_pq.is_empty() { None } else { Some(sorting_columns_pq) })
```
`sorting_columns_pq = schema.sorting_columns()`. For `otel_logs_and_spans`
(`schemas/otel_logs_and_spans.yaml`):

```
sorting_columns:  [timestamp, resource___service___name, id, level, status_code]
z_order_columns:  [timestamp, id, resource___service___name]
```
Note the two lists differ in both membership and order — so even the Z-order dimensions are not
the declared sort order.

---

## What each optimize path actually does (delta-rs evidence)

delta-rs checkout: `~/.cargo/git/checkouts/delta-rs-timefusion-2cbec12b0fe8ecae/2f473fe`.

- **delta-rs passes our `WriterProperties` through verbatim** — it never recomputes or clears
  `sorting_columns`. `crates/core/src/operations/optimize.rs:348` (`with_writer_properties`),
  used at `:693` and `:1057`. So whatever `set_sorting_columns` we hand it lands in the footer
  of the rewritten files unchanged.

- **Z-order sorts by the interleaved `zorder_key`, not lexicographically:**
  `optimize.rs:794` → `let df = df.sort(vec![expr.sort(true, true)])?;` where `expr` is the
  `zorder_key` UDF over `z_order_columns`. `zorder_key` truncates+interleaves bytes of each
  column (`optimize.rs:1880 zorder_key`). Result: rows ordered on the Z-curve, *not* by
  `[timestamp, service_name, id, level, status_code]`.

- **Compact does not re-sort:** `rewrite_files` (`optimize.rs:~598-720`) streams input batches
  through and writes them out merged; there is no sort step. Order = concatenation of the input
  files' existing order. After the flush fix each *input* file is timestamp-sorted, but
  concatenating N sorted files is only piecewise-sorted, not globally sorted — yet the footer
  still claims a global sort.

TF triggers (all in `src/database.rs`):
- `optimize_table` (full Z-order, 48h window, ~30 min cadence): `:2964` with
  `OptimizeType::ZOrder(schema.z_order_columns)` when `z_order_columns` non-empty (it is).
- `recompress_partition` (tier bump, cold path): `:3171`, ZOrder or Compact.
- `optimize_table_light_inner`: `:3564` `OptimizeType::Compact`.

All three use `create_writer_properties(schema, ...)` → `build_writer_properties` → the
unconditional `set_sorting_columns`.

---

## Why it's currently inert for *correctness*

The declared footer order only matters if a reader reads it back and advertises it as an
output ordering. In DataFusion 53.1.0 that read-back lives in `ordering_from_parquet_metadata`
(`datafusion-datasource-parquet-53.1.0/src/metadata.rs:700`), and it is called **only** from
the **native `ParquetFormat`/ListingTable** path:
`.../src/file_format.rs:470` and `:496` (schema/stats inference).

TimeFusion does **not** use ListingTable for reads — it goes through delta-rs's "next"
`DeltaScan` (`crates/core/src/delta_datafusion/table_provider/next/`). A full grep of that
module for `output_ordering`, `with_output_ordering`, `ordering_from_parquet_metadata`,
`sorting_columns`, `LexOrdering`, `eq_properties` returns **nothing**. So:

> The `DeltaScanExec` never claims an output ordering derived from the parquet footer.
> DataFusion therefore never elides a `Sort`, never shortcuts a `LIMIT`, and never feeds an
> order-assuming merge based on the (false) declared order. No query result depends on it today.

Confirmed by the EXPLAIN plan node being `DeltaScanExec` (the "next" provider), not a
DataFusion-native `ParquetExec`/ListingTable scan.

---

## When this bites (turning latent → live)

Any one of these makes the false declaration a real correctness bug:

1. A future delta-rs version starts honoring footer `sorting_columns` to set
   `DeltaScan` output ordering (plausible — upstream is actively wiring eq-properties).
2. TF adds a code path that reads these files via DataFusion's native `ParquetFormat`/
   ListingTable (e.g. an export/bench tool, a `COPY`, an external reader).
3. Any external consumer (Spark, DuckDB, pyarrow, Athena) reads the files and trusts
   `sorting_columns` for ordered scans / merge.

Failure mode: order-dependent operators (`ORDER BY ... LIMIT`, merge joins,
`SortPreservingMerge`) return wrong rows or wrong order, silently.

---

## The performance interaction with the flush-sort fix (#4)

This is the part worth acting on regardless of the correctness question.

- Flush (10-min buckets) now writes **timestamp-sorted** files → a 2s `timestamp BETWEEN`
  window prunes to ~1 page. Great for the most-recent data.
- Optimize (Z-order, 48h window, every 30 min) rewrites those same partitions into
  **Z-order**. Z-order on `[timestamp, id, service_name]` keeps timestamp only as one
  interleaved dimension, so a narrow timestamp window no longer maps to a contiguous page
  range — page pruning for the point lookup degrades on optimized partitions.
- Net: the point-lookup query (which targets data old enough to have been flushed *and*
  optimized) does **not** fully benefit from the flush sort, because optimize has since
  re-interleaved it.

So the flush sort helps the freshest bucket; everything the optimizer has touched reverts to
Z-order locality. If point-lookup-by-`(timestamp,id)` is the SLA-critical query, the optimize
strategy itself (Z-order vs timestamp-sort) is the lever, not just the flush write.

---

## How to investigate / reproduce

1. **Confirm the false declaration on an optimized file.** Pick an optimized partition
   (`date` older than the current flush window), pull one parquet file, and compare declared
   vs actual order:
   ```
   python - <<'PY'
   import pyarrow.parquet as pq
   f = pq.ParquetFile("part-....parquet")
   md = f.metadata.row_group(0)
   print("declared sorting_columns:", f.metadata.row_group(0).sorting_columns)  # via thrift; or use parquet-tools meta
   t = f.read(columns=["timestamp"]).column("timestamp").to_pylist()
   print("actually sorted by timestamp?", t == sorted(t))
   PY
   ```
   Expect: declared `[timestamp, ...]` but `actually sorted == False` on Z-ordered files.
   (`parquet-tools meta <file>` also prints the SortingColumn list.)

2. **Confirm it's inert in TF reads.** `EXPLAIN` any `ORDER BY timestamp` query against an
   optimized partition; verify a `SortExec`/`SortPreservingMergeExec` is still present (TF does
   not trust the footer to skip it). If a future change removes that Sort, this becomes live.

3. **Quantify the perf regression** from optimize: run the point-lookup EXPLAIN ANALYZE
   (`page_index_pages_pruned`) against (a) a just-flushed, not-yet-optimized partition vs
   (b) an optimized partition. Compare `pages matched`.

---

## Options (for whoever picks this up)

Pick based on whether the goal is *correctness hygiene* or *point-lookup performance*:

- **A. Make the declaration honest per path (cheapest correctness fix).**
  Only call `set_sorting_columns(...)` when the data is actually written in that order.
  Concretely: thread a flag into `build_writer_properties` (or a variant) so the flush/dedup
  paths declare sort keys, while the Z-order/Compact paths declare **either** nothing
  (`None`) **or** the true Z-order key. Z-order has no lexicographic SortingColumn
  representation, so `None` is the correct, safe value there. Low risk, removes the landmine.

- **B. Sort optimize output by the declared sort keys instead of Z-order.**
  Use `OptimizeType::Compact` + a real sort, or stop Z-ordering and lexicographically sort by
  `[timestamp, ...]` on rewrite. Makes the declaration honest *and* preserves timestamp page
  locality through optimize — directly helps the point lookup. Trades away Z-order's
  cross-dimensional file pruning for `service_name`. Decide via the query mix
  (point-lookup-by-time vs service_name-scan).

- **C. Leave Z-order, accept B's loss isn't wanted, and instead lean on the `id` bloom +
   `timestamp` chunk stats.** I.e. accept that optimized partitions prune at row-group (chunk)
   granularity for timestamp and rely on the per-`id` bloom filter for the point lookup. Then
   fix only the correctness declaration (option A). This is the "do nothing structural, just
   stop lying" path.

Recommendation: **A now** (removes the silent-wrong-results landmine for ~a few lines), then
evaluate **B vs C** with the perf numbers from investigation step 3. Whatever is chosen, the
declaration and the actual order must be made to agree on all five write paths.

---

## Key references

Code (this repo, `src/database.rs`):
- `build_writer_properties` (unconditional `set_sorting_columns`): search `set_sorting_columns`.
- `create_writer_properties`: ~878.
- Flush/append write (now sorted): `insert_records_batch`, sort call right after schema fetch (~2685); write ~2722.
- Dedup write (now sorted): ~3376–3402.
- Full Z-order optimize: `optimize_table` ~2958–2978.
- Recompress: `recompress_partition` ~3164–3185.
- Light optimize (Compact): `optimize_table_light_inner` ~3564–3575.
- The flush sort helper: `sort_batches_by_schema`.

delta-rs (`.../delta-rs-timefusion-2cbec12b0fe8ecae/2f473fe/crates/core/src`):
- `operations/optimize.rs:348` `with_writer_properties` (props passed through verbatim).
- `operations/optimize.rs:794` Z-order `df.sort(zorder_key)`.
- `operations/optimize.rs:1880` `zorder_key` interleave.
- `operations/optimize.rs:~598-720` `rewrite_files` (Compact, no sort).
- `delta_datafusion/table_provider/next/` — no `output_ordering`/`sorting_columns` read-back.

DataFusion 53.1.0:
- `datafusion-datasource-parquet-53.1.0/src/metadata.rs:700` `ordering_from_parquet_metadata`.
- `.../src/file_format.rs:470,496` — only caller (native ListingTable, unused by TF).
