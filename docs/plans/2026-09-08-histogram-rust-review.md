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

**Estimated lines saved:** 3.

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

| File | Lines saved, pending | Highest-value change |
| --- | ---: | --- |
| src/database/histogram.rs | 3 | Read the captured provider directly |

## Pass 2: direct provider cleanup

Reviewed the resulting proof builder again with both skills. The provider
continues to bind the captured Delta snapshot and exact file selection.
`read_table` constructs a scan with the same provider, without catalog lookup.
Projection, full-key ordering, deletion vectors, memory accounting, shutdown,
partition revalidation, and publication are unchanged. No further distillation
or type-evasion finding in this scoped change. The focused existing regression
test is running; no new test duplicates its coverage.

This is a second pass over the proof section, not a second complete review of
the feature. Full-feature reviews and the hot-day performance gap remain open.
