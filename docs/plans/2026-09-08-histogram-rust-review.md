# Histogram Rust review

Scope: the hash histogram feature relative to `ed6e56b4`. Review sections below
are partial until the complete feature has two recorded passes.

## Pass 1: proof builder and admission

Reviewed `src/database/histogram.rs`: `HistogramProofBuilds`, its default,
`histogram_timestamps`, `seed_missing_proof`, and `build_histogram_count_proof`.
Checked `PartitionCountProof` construction and matching in
`src/tantivy/visibility.rs`, the maintenance runtime, and the pinned
DataFusion `SessionContext::read_table` implementation.

### src/database/histogram.rs

**Reuse** — The proof builder registers a temporary table, then looks it up.
Use `context.read_table(provider)?`; the pinned DataFusion version supports it.
This removes catalog mutation and the temporary table name. Applied in the
isolated worktree `/tmp/timefusion-histogram-review` while CI finishes against
the unchanged primary worktree.

**Algebraic** — No change requested. `Result<Option<u64>>` separates build
errors from partitions that cannot provide a uniqueness proof. Neither outcome
authorizes an indexed count. The caller logs declined and failed builds.

**Functional** — Keep the stream loop: it selects cancellation and the host
memory brake while waiting for data. Keep adjacent-key checks: equality exits
the enclosing builder without publishing; descending order returns an error.
Neither loop collects the partition.

**Combinators** — No additional change requested in this scope.

**Derives** — Manual Default is required to create one semaphore permit; a
derived default would not preserve admission behavior.

**Consolidate / Bloat** — Remove the temporary catalog plumbing above.

**Lines saved:** 2.

### rs-evasion-review findings

No blocking type evasion found in this scoped review. Proof publication binds
the complete file/DV map and visibility schema. Admission holds its permit
through worker execution; it does not hold the attempts mutex across await.
The streamed builder does not populate the resident winner cache.

Note: allocation accounting follows each decoded batch and Arrow row encoding.
A passing reservation is not evidence of a measured process RSS limit. Large
partition and spill measurements remain required.

The whole-diff mechanical scan found no newly added allow attributes, unsafe
blocks, ignored tests, todo macros, or unimplemented macros. This scan alone
does not establish a clean semantic review of the full diff.

| File | Lines saved | Highest-value change |
| --- | ---: | --- |
| src/database/histogram.rs | 2 | Read the captured provider directly |

## Pass 2: direct provider cleanup

Reviewed the resulting proof builder again with both skills. The provider
continues to bind the captured Delta snapshot and exact file selection.
`read_table` constructs a scan with the same provider, without catalog lookup.
Projection, full-key ordering, deletion vectors, memory accounting, shutdown,
partition revalidation, and publication are unchanged. No further distillation
or type-evasion finding in this scoped change. The focused existing regression
test passed: nextest run `14dc8b09-ce49-4654-9ab7-2fbdaf4a1ebe`, one test
in 1.642 seconds. The cleanup is integrated as `d05f161a`; no new test
duplicates its coverage.

This is a second pass over the proof section, not a second complete review of
the feature. Full-feature reviews and the hot-day performance gap remain open.

## Stream extraction review

### src/tantivy/visibility.rs

**Reuse** — `read_file_rows` now collects the same `stream_file_rows` used by
future streaming consumers. Parquet projection, captured partition constants,
casts, required-column checks, and physical-row validation remain in one place.

**Algebraic** — The stream and DV mask have distinct types. No sentinel batch
or synthetic user field carries completion or deletion metadata.

**Functional** — `try_unfold` carries reader state and checked physical-row
count between batches. Keep the collecting wrapper's loop: it checks the
aggregate decoded budget before retaining each output batch.

**Combinators / Derives** — No additional finding.

**Consolidate / Bloat** — No duplicate decoder was added. Extraction adds a
stream boundary and preserves the collecting API's budget contract.

**Lines saved:** none; this introduces a reusable streaming boundary.

rs-evasion-review: physical rows remain unfiltered; DV visibility stays in a
separate mask. End-of-stream validates complete decoding; excess rows fail
earlier. Errors propagate through `Result` without suppression. Documentation
was tightened to avoid claiming that the Parquet reader retains only one
batch internally; the guarantee is that this adapter does not collect output.
A second reading of the resulting extraction found no further scoped issue.
The full-feature review remains incomplete.

## Streaming winner-mask review, in progress

Scope: `winner_masks`, the new `stream_winner_masks`, their callers, and
canonical `DedupExec`/`Dedup`/`Greatest` behavior in `src/read/mod.rs`.

Reuse: use the existing spill-capable `SortExec`, canonical deduplication, and
`execute_stream`. Do not collect all winning batches before building masks.
Coalesce input partitions before the global sort. Sort physical source and
ordinal after keys to preserve equal-version source priority.

Correctness finding: canonical bounded deduplication closes a run early above
64 MiB, recording emitted keys as seen. Ordering only keys and lineage can
therefore emit an older version before its newer replacement. A large-run
regression now targets this exact boundary. It must fail for that symptom
before fixing the new path to sort greatest version first within each key,
with null versions last. Source/ordinal still breaks equal-version ties.

Compatibility finding: generic `winner_masks` previously accepted complete
keys without timestamp. Its extracted consumer must preserve that contract;
prefer timestamp as the leading sort key when present, otherwise use the
complete declared key order. Histogram capture independently requires timestamp.

Mask buffers are charged to the query pool during execution, including actual
builder capacity. The returned masks still need ownership accounting by their
consumer; this change does not claim a complete process RSS bound.

The large-run regression failed with 8,192 old winners instead of 8,191,
confirming loss of the newer version. Applied descending version order with
nulls last before lineage ordering. Also restored support for complete keys
without timestamp. The expanded regression set passed: 136 tests in 11.479 seconds, nextest
`ae1fc38e-fce0-4379-9946-e94153b4fd94`, including the large-run guard.
A second scoped review with both skills found no further change: the sort
retains complete keys, descending versions, and ascending physical lineage;
mask allocation is checked and charged; errors propagate. Full-feature reviews
and real captured-stream integration remain open.
