# The row witness counts physical rows, and the rollup aggregates logical ones

2026-08-23. Found while chasing three rollup tests that went from green to red
with no code change between them. The tests are the symptom; the finding is about
the witness itself.

## The symptom

`dedup_compaction_test::{a_coalesced_dimension_folds_null_and_the_literal_identically_through_the_rollup,
a_partly_covered_window_unions_the_rollup_with_raw_and_matches_the_raw_answer,
an_all_null_duration_bucket_is_eliminated_identically_through_the_rollup}` all
passed at 23:xx UTC on 2026-08-22 and all fail now, on identical code. Each fails
on its **routing** assertion, never on its equality assertion — the rollup answer
is still right, the query just stops using it.

Instrumenting the miss counters gives one reason, and the same one at `ddfa910`
(before this session's rollup commits): **`stale_coverage`**. So it is the row
witness refusing, not a matcher regression.

Ruled out first, each with a measurement: disk (246 GB free), accumulated local
MinIO state (wiped the bucket, still red), time-of-day (hypothesised a 00:00–02:00
window from the fixture's `midnight + 2h` horizon; still red at 07:16 UTC), and
this session's own commits (reproduced at `ddfa910`).

## The finding

The witness (`TAG_SOURCE_ROWS` / `Publication::source_rows`) records the source
date partition's **`num_records` sum**, and the read path re-computes that sum and
demands equality. `num_records` is a count of **physical** rows: it includes
tombstoned rows and every merge-on-read version of an updated row.

The rollup build does not aggregate physical rows. It reads through the normal
scan path, which applies `DedupExec` — so it aggregates **logical** rows, one per
`(timestamp, id)`.

Those two quantities move independently. A dedup sweep or a compaction rewrite
that collapses duplicate versions lowers `num_records` **without changing the set
of logical rows**, so the aggregate the tier holds is still exactly correct — and
the witness invalidates it anyway. In the tests this is visible because
`run_maintenance_units(1024)` runs dedup units over the same partition the rollup
was just built from; whether it lands before or after the build is a scheduling
detail, which is precisely why the tests flip between runs rather than failing
deterministically.

## Why this matters beyond the tests

Prod's `stale_coverage` splits into `rollup_stale_no_witness` (~80%, the slices
that predate the tag — being republished, see 147fb00) and
`rollup_stale_moved` (~20%, measured at 126 and 156 per three reps). The "moved"
half has been read as "the partition really is churning". This finding says an
unknown share of it is instead **benign maintenance churn**: dedup and compaction
moving `num_records` on partitions whose logical content never changed. Every one
of those is a rollup that is correct and refused.

That share is worth measuring before anything is built, because the two readings
imply opposite work: real churn needs a rebuild, benign churn needs a better
witness.

## What a correct witness would compare

The witness has to be invariant under any rewrite that preserves the logical row
set. Candidates, cheapest first:

1. **Deduplicated row count** for the partition. Same shape as today, right
   quantity — but it costs a dedup pass to compute, where `num_records` is free
   from the Delta log.
2. **A logical-content fingerprint** carried forward by the rewriting operations
   themselves: compaction and dedup already know they preserved logical content,
   so they could re-stamp the witness instead of invalidating it. This is the same
   move as "carry `covered_files` forward across a compaction" in the tantivy
   plan, and it keeps the read path free.
3. **Keep `num_records` but stamp it at rewrite time** — the narrowest version of
   (2): whenever a maintenance rewrite commits, update the witness on the tier
   files that cover that partition to the new sum. Sound because the operation
   that moved the number is the one that knows the move was content-preserving.

(3) is the smallest change that removes the false alarms without weakening the
rule: a write that genuinely adds rows still moves `num_records` and still
invalidates, because no rewrite claims it.

## Deliberately not changed here

Weakening the witness is how a stale aggregate reaches a dashboard, and the
current rule fails **closed** — it costs a raw scan, never a wrong number. It
stays as it is until the benign-versus-real split has been measured on prod.
