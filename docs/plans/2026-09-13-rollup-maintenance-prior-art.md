# Rollup maintenance: what every other system does, and what we do instead

2026-09-13. Prompted by the measured 10x verdict: BaseRollup alone would want
**25-39 of 48 cores** at ten times today's writes. That is not a tuning problem.

## The number that names the defect

`work.BaseRollup.progress_rows / rows_ingested_total` ≈ **1,490:1**. Every
invalidation re-aggregates a whole partition **from raw rows**. A day-wide unit
reads ~19 M source rows to emit ~28 k rollup rows, and it does that again on the
next invalidation — the 09-11 finding was that 72.6% of those rebuilds produced
byte-identical output.

The no-op skip (#267) removed the *repeated* rebuilds. It did nothing about the
cost of a *legitimate* one, and that cost is the 10x wall.

## What the field does: never re-read raw

Four independent lines of prior art converge on one rule.

### 1. ClickHouse — `AggregatingMergeTree` with `-State` / `-Merge`

Aggregate functions can emit their **intermediate binary state** instead of a
final value. "When ClickHouse merges parts, it combines (merges) these states
rather than re-reading the original rows." A daily-rollup table answers "unique
users last month" by merging 30 daily states rather than rescanning raw events.

This is the whole idea: a rollup is maintained by the pass that is *already*
rewriting files, at O(parts), not by a separate pass at O(rows).

### 2. Druid — roll up at ingest, perfect it at compaction

Druid distinguishes **perfect rollup** (all duplicates aggregated at ingest,
requires a shuffle) from **best-effort rollup** (streaming ingest cannot see the
future of a time chunk, so segments may contain duplicate dimension tuples).
Streaming indexers deliberately take best-effort, and **auto-compaction
re-rolls-up** the interval afterwards.

The trade is explicit and accepted: cheap and slightly imperfect now, exact once
compaction has run.

### 3. TimescaleDB — continuous aggregates + invalidation log

Dirty ranges are recorded on write; a refresh policy recomputes **only those
buckets**. TimeFusion already has this half (`rollup_dirty`, `dirty_hours`,
`invalidate_rollup_hours`) — which is why the remaining cost is per-rebuild, not
per-invalidation.

### 4. The IVM literature — self-maintainable aggregates

The change-table technique, CReaM, and viewlet transforms all rest on the same
algebra: an aggregate is incrementally maintainable when it is a **mergeable
monoid**, and `avg` becomes so once stored as `(sum, count)`.

**TimeFusion's measures already satisfy this.** `count`, `sum`, `min`, `max` are
monoids; the duration percentiles already use a mergeable digest. There is no
measure in `dashboard_1m_v3` that would block per-file partial aggregation.

## So why does TimeFusion re-read raw? The honest answer

**Dedup.** A rollup must aggregate the deduplicated row set, and duplicates span
files. A partial aggregate computed per file cannot subtract a duplicate that
lives in a different file, so the current design re-scans and dedups first. That
is a real constraint, not an oversight.

Druid's answer applies directly, and TimeFusion is unusually well placed to take
it, because the machinery already exists:

- **Per-file partial aggregates are best-effort** — exact within the file, and
  over-counting only across files that still hold duplicates of each other.
- **Dedup and compaction already rewrite those files.** When they do, they
  recompute the partial for the merged output — perfecting the rollup as a side
  effect of work already scheduled. This is precisely Druid's
  ingest-then-compact contract.
- **Certification already says when a partition is provably dedup-clean**
  (`cert_slice_files_proved`, the dedup certification lane). That is the exact
  predicate for "this merged partial is now perfect," and it is the thing
  `dedup_skipped_pct` has been trying to spend for weeks.

## The proposal, and its arithmetic

Write a partial aggregate alongside every raw file, keyed by
`(rollup spec, bucket, dimensions)`. Then:

```
BaseRollup(day) = MERGE(partials of the day's files)      -- O(files)
        instead of = AGGREGATE(DEDUP(rows of the day's files))  -- O(rows)
```

A day partition holds hundreds of files and tens of millions of rows. The unit
goes from ~19 M row-reads to ~10² partial-reads plus a merge — **three to four
orders of magnitude**, which is what makes 10x fit in the CPU budget instead of
wanting 25-39 cores.

Cost moves to the write path: each flush computes its own partial over data
already in memory, which is the cheapest possible place to do it, and the output
is small (one row per bucket × dimension tuple).

### What to settle before building it

1. **Where the partial lives.** A sidecar parquet per raw file is the least
   invasive and matches how bloom sidecars already work. Delta file tags are too
   small for a real aggregate.
2. **Cardinality.** The saving is only real if partial rows ≪ raw rows. Measure
   `(bucket × dimension) distinct count per file` on a prod partition first — if
   a 1-minute grain × 3 dimensions approaches the row count on a small file, the
   sidecar is not worth it for that file and the unit should fall back to
   scanning. **Do not build before this number is in hand.**
3. **Staged, not big-bang.** The merge path can be added behind the existing
   `rollup_noop_skip` style switch, with the raw path kept as the oracle: run
   both on a sample and assert equality before trusting the merge. The 09-07
   ingest-dedup lesson — shipped inert, passed every gate, did nothing in prod —
   was exactly a missing shadow phase.

## Sealed consolidation: the policy is already right

Researched separately and the answer is **do not re-architect this one**.
`2026-09-06-merge-policy-prior-art.md` already transferred the two rules that
matter, and they are implemented:

- ClickHouse's `SimpleMergeSelector` **similar-size preference** and RocksDB
  universal compaction's `size_ratio` → `bin_breaks_size_ratio`.
- **Age escalation** → the `starved` ranking.
- Fan-in value floor → `refuse_low_value_bin`.

Druid's auto-compaction adds target segment size, newest-to-oldest search, and
worker slots — all of which TimeFusion has equivalents for.

**The sealed lane's problem is not policy, it is the execution envelope**, and
that is what today's measurements show:

- one permit for two operations, because a fixed 3-slice repair holdback
  consumed a 3-slice coordinator share (fixed in this branch);
- a single unit holding that permit for **2 h 04 m** — a 61-file, 255 MB bin
  over the shared project that started staging twice and completed neither time;
- `run_until_idle` being an idle window, so nothing bounds the hold, and the
  absolute cap added in #268 could not preempt it because a future that never
  reaches an await point cannot be cancelled by a timeout.

Druid's answer to a compaction task that runs too long is to **bound the unit**:
target segment size and interval splitting, so no single task owns an unbounded
interval. TimeFusion has `split_time_task` for rollups and byte-preflight
splitting for dedup; the sealed lane takes a **day-wide** unit and has no
equivalent guard.

**But the evidence does not support size as the cause here, and that matters.**
The unit that ran 2 h 04 m selected **61 files and 255 MB** — small. 255 MB in
two hours is ~35 KB/s, which is not a sort and not CPU: it is a STALL. Splitting
a 255 MB unit into smaller ones would produce several stalled units instead of
one.

So the sealed lane needs, in this order:

1. **Diagnose the stall.** Staging emits `wave_bin_staging_started` and
   `wave_bin_staged`; this bin logged the first twice and the second never. The
   missing instrument is where inside staging it went — object-store read, sort,
   or commit. A phase timer there is cheap and is the only thing that can name it.
2. **Make the unit interruptible.** The absolute cap from #268 is correct in
   principle and could not fire, because a future that does not reach an await
   point cannot be cancelled. Whatever blocks for two hours must either yield or
   carry its own IO deadline.
3. **Only then** consider Druid-style interval bounding, which is right for a
   genuinely oversized unit and irrelevant to a stalled small one.

Bounding the unit first would have looked like a fix and changed nothing —
the same mistake as capping the lifetime of a unit that cannot be preempted.

## Sources

- [ClickHouse: AggregateFunction type](https://clickhouse.com/docs/sql-reference/data-types/aggregatefunction)
- [ClickHouse: aggregate function combinators](https://clickhouse.com/docs/sql-reference/aggregate-functions/combinators)
- [ClickHouse: using aggregate combinators](https://clickhouse.com/blog/aggregate-functions-combinators-in-clickhouse-for-arrays-maps-and-states)
- [Apache Druid: data rollup (perfect vs best-effort)](https://druid.apache.org/docs/latest/ingestion/rollup/)
- [Apache Druid: compaction](https://druid.apache.org/docs/latest/data-management/compaction/)
- [ViewDF: declarative IVM for streaming data](https://cs.uwaterloo.ca/~tozsu/publications/stream/elsarticle.pdf)
- [Incremental maintenance of aggregate views (CReaM)](https://web.stanford.edu/~abhijeet/papers/abhijeetFOIKS14.pdf)
- [Incremental maintenance of aggregate and outerjoin expressions](https://www3.cs.stonybrook.edu/~hgupta/ps/aggr-is.pdf)
- [Everything you need to know about incremental view maintenance](https://materializedview.io/p/everything-to-know-incremental-view-maintenance)
