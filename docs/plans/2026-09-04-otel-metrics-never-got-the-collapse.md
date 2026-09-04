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

## The correctness precondition — MEASURED, and it holds exactly

The blocking question above was whether widening `dedup_keys` to
`(timestamp, metric_name, series_id)` is safe: if that tuple were COARSER than
`(timestamp, id)`, widening would silently collapse rows that are currently
distinct.

**It is not coarser. The two keys are exactly equivalent.**

First through pgwire, three projects, 20-minute windows — zero groups with more
than one `id`, across 621,365 groups, and identical key cardinality
(128,126 vs 128,126 on `87576849`).

That query reads through `DedupExec` and therefore sees **winners only**, while
the condition that matters is over **every version the dedup will group**. So it
was re-run on the raw Delta-live files (`scratchpad/metrics_key_equiv.py`), one
full prod partition-day:

```
project_id=87576849…/date=2026-09-03
  LIVE files 65    rows 7,098,560
  (timestamp, id) keys                      : 7,097,386
  (timestamp, metric_name, series_id) keys  : 7,097,386
  groups where the proposed key is COARSER  : 0
```

**Identical cardinality over 7.1 M raw rows, and not one group where the proposed
key merges two `id`s.** (The 1,174-row gap between rows and keys is real
multi-version work, so this is not a partition with nothing to dedup.)

So the widening is a **relabelling of the same grouping**: the same rows collapse,
nothing is lost, nothing is left uncollapsed — and `dedup_keys` becomes a prefix
of `sorting_columns`, which flips `otel_metrics` onto the streaming collapse.

### Confirmed on a second project

```
project_id=6297304f…/date=2026-09-03
  LIVE files 53    rows 2,315,168
  (timestamp, id) keys                      : 2,315,078
  (timestamp, metric_name, series_id) keys  : 2,315,078
  groups where the proposed key is COARSER  : 0
```

Exactly equal again, independently, on a different tenant. And the third and
largest:

```
project_id=00000000…/date=2026-09-03
  LIVE files 68    rows 13,360,165
  (timestamp, id) keys                      : 13,347,334
  (timestamp, metric_name, series_id) keys  : 13,347,334
  groups where the proposed key is COARSER  : 0
```

**All three heavy projects verified on raw files: 22,773,893 rows across 186
files, three tenants, ZERO coarsening, identical cardinality every time.** The
row/key gaps (1,174 / 90 / 12,831) are real multi-version work, so none of these
is a partition with nothing to dedup.

### What still stands between this and shipping

1. ~~Verify the key equivalence on real data.~~ **DONE** — all three heavy
   projects, on raw files, over every live version. This was the blocking
   correctness question and it is answered.
2. **Equivalence in today's data is not a schema guarantee.** If it is adopted,
   the invariant deserves an assertion — the natural place is the same audit that
   already checks immutable columns during compaction.
3. **Coupled consequences already recorded in this repo:** the logical-count index
   key must equal the dedup key, and existing files' footers encode the current
   sort order. Both need checking against a `dedup_keys` change.
4. **The hypothesis that this explains `MAX_BIN_ROWS` is still untested** — that
   needs the bench run against a real `otel_metrics` file, window vs collapse, at
   the 512 MB prod-representative pool.

None of that is 2 am work, but the hard part — the correctness argument that
would otherwise have blocked it — is now measured rather than assumed.
