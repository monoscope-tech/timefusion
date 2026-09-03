# `otel_metrics` never got the collapse — and it may be why `MAX_BIN_ROWS` exists

A fifth instance of tonight's pattern, found by acting on the pool sweep's own
recommendation ("make sure nothing silently falls back to the window form").

## The finding

`compact.rs:1624` chooses the dedup form:

```rust
let streaming_collapse = dedup_keys_lead_the_sort(schema) && !order_by.is_empty();
```

and `dedup_keys_lead_the_sort` (`compact.rs:1943`) requires the schema's
`dedup_keys` to be a **prefix of its `sorting_columns`**. Checking every schema:

| table | `dedup_keys` | `sorting_columns` prefix | collapse? |
|---|---|---|---|
| `otel_logs_and_spans` | `timestamp, resource___service___name, id` | `timestamp, resource___service___name, id` | ✅ |
| `mor_versioned` | `timestamp, id` | `timestamp, id` | ✅ |
| **`otel_metrics`** | **`timestamp, id`** | **`timestamp, metric_name, series_id`** | ❌ **window** |
| `mor_dormant` | `timestamp, id` | `timestamp` (only) | ❌ (fixture) |

**`id` does not appear in `otel_metrics.sorting_columns` at all.** So
`otel_metrics` takes the `ROW_NUMBER() OVER (PARTITION BY …)` path on **every**
dedup rewrite, always.

## Why that is serious, given tonight's sweep

Measured on a real prod file at four pool sizes:

- the window form is **20–58x slower** than the collapse;
- it **fails at 8 partitions at every pool size tested, up to 1 GB** — it cannot
  be bought off with memory;
- it fails even at **1 partition** at 128 MB.

So the metrics table is permanently on the slow, memory-fragile path, while the
logs table was moved off it on 2026-09-02 (`7beb411e`, "widen `dedup_keys` to the
sort prefix + one-pass collapse").

## The hypothesis worth testing first

`MAX_BIN_ROWS = 2_000_000` was introduced because the dense table "ran past its
900 s deadline **9 times out of 9**", and it was priced in rows on the reasoning
that metrics carry 5.58 M rows in a 256 MB bin against logs' 1.73 M.

**But tonight's §2 shows the sort is ~0.2 % of the deadline, so row count cannot
plausibly be what blew it.** The alternative explanation is right here: that table
is the one on the window path, and the window path is 20–58x slower and OOMs at
partition counts the collapse path handles in 1.7 s.

If that is right, then **`MAX_BIN_ROWS` is a workaround for the window fallback,
not for row density** — and converting `otel_metrics` to the collapse would
address the cause instead of the symptom, in the lane that has been the worst
offender.

This is a hypothesis, not a conclusion. It is testable offline: run the bench
against a real `otel_metrics` parquet file and compare window vs collapse at the
prod-representative 512 MB pool.

## Why I did not change it tonight

Converting means making `dedup_keys` a prefix of `sorting_columns`, and both
directions are correctness-critical:

- **Reorder `sorting_columns` to `timestamp, id, …`** — destroys the
  `metric_name, series_id` locality that metrics queries and range pruning rely
  on. Almost certainly wrong.
- **Widen `dedup_keys` to `timestamp, metric_name, series_id`** — the same shape
  as the logs fix, and probably right, but only if that tuple identifies a row at
  least as finely as `timestamp, id` does. If two distinct `id`s can share a
  `(timestamp, metric_name, series_id)`, widening **silently drops rows**.

It also has coupled consequences already recorded in this repo: the logical-count
index key must equal the dedup key, and a schema's sort order is baked into every
existing file's footer. That is a daylight change with a correctness argument to
write, not a 2 am edit — and I had already committed to stopping deploys so the
running process could age enough to measure the four fixes already shipped.

## The pattern, now five for five

| mechanism | applied to | missing from |
|---|---|---|
| shared capacity classifier | coordinator | hot-tail staging |
| both packer budgets | bytes | rows |
| fair position under a shared deadline | claim ordering | probe ordering |
| liveness watcher | repair, dedup | rollups |
| **one-pass collapse** | **`otel_logs_and_spans`** | **`otel_metrics`** |

The coverage matrix (`2026-09-04-lane-coverage-matrix.md`) should gain a row for
"dedup form", and the guard is cheap: a test asserting every `version_append`
schema satisfies `dedup_keys_lead_the_sort`, with an explicit allow-list for any
table deliberately left on the window path.
