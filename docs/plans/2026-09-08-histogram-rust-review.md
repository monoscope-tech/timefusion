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

## Prepared Parquet source review

Scope: `stream_file_rows`, `PreparedFileRows::open`, `PreparedFileRows::stream`,
and the extended real Delta DV test.

rs-distill pass 1: reuse Parquet 58.3.0's cloneable `ArrowReaderMetadata` and
`new_with_metadata`. Each scan owns a fresh reader. Projection, partition
reconstruction, casts, and physical-row validation remain in one decoder.
No mutex or one-shot execution state is required. The test extends the
existing DV scenario with repeat scans and a second projection.

rs-evasion pass 1: the prepared DV mask must be private. Exposing mutation
would let a caller change the physical-row count used by the stream checker.
Made it private; construction validates it against Parquet metadata.

Pass 2 with both skills: no further scoped finding. The prepared source holds
immutable snapshot metadata, each execution has independent reader state, and
errors propagate. Metadata and DV ownership still need query-pool accounting
in the forthcoming histogram source-plan owner. No whole-process memory
bound is claimed by this reader API.

All 136 selected tests passed in 9.317 seconds (nextest
`be958064-0dd1-4562-9701-53c98848a3d8`). Final lint is running. This remains a
scoped review, not completion of the required full-feature reviews.


### SQL streaming integration checkpoint — 2026-09-09

Scoped rs-distill review: collecting and streaming visibility now share
`lineage_schema` and `lineage_batch`. File execution is sequential, so adding
files does not add simultaneously active decoder streams. Bucket accumulation
still repeats across collecting and streaming paths; consolidation is open.

Scoped rs-evasion review: the collecting API retains its total decoded-data
contract. The new streaming API explicitly documents a per-batch limit and
uses query-pool reservations. Prepared DV state remains private. No new
unsafe code, lint suppression, ignored tests, or weakened assertions appear.
Metadata accounting is an estimate of retained allocations, not an RSS limit.
The 8 MiB trial exposed DataFusion's 10 MiB minimum sort reservation; the test
uses 32 MiB while retaining a hash fixture larger than 64 MiB. Full-feature
reviews remain open; this checkpoint does not claim the entire branch clean.


Final integration validation: `make ci-signoff CHECKS="fmt clippy test e2e"`
exited 0. Formatting, Clippy, the full nextest suite, all eight doctests, and
end-to-end tests passed. All four requested checks were attested for the final
source, including the 32 MiB streaming regression. Final gate status requires
only canonical `pg-smoke` on GitHub due to the documented macOS networking
limitation. No failed check was attested.


### Bucket accumulation follow-up — 2026-09-09

rs-distill scoped pass: seven copies of checked bucket accumulation in
`database/histogram.rs` and `tantivy/search.rs` now reuse `merge_counts` in
`tantivy/histogram.rs`. `HistogramSnapshotResult` derives its empty default.
This consolidates the overflow rule across indexed and captured-row paths.

rs-evasion scoped pass: the helper retains `checked_add` and propagates errors
through `try_for_each`. No saturating/wrapping substitution, new unsafe code,
ignored tests, or lint suppression was introduced. Overflow errors now share
one diagnostic, `histogram count overflow`; no caller was found parsing the
previous per-path strings. Source masks, query budgets, and routing are intact.
Targeted regression validation is running. This is not a full-feature clean
bill of health; remaining whole-path reviews and performance measurements
continue with production deployment.


Second scoped pass after implementation: no further rs-distill or
rs-evasion finding in the count-merging change. The helper owns incoming
counts, preserves ordered bucket keys, checks every addition, and propagates
errors. Existing source counters and captured-mask behavior are untouched.
All 136 targeted regressions passed in 11.710s (nextest
`2b7522fb-0d73-49d2-909b-2048c9247684`). Full local signoff follows.


Full local `make ci-signoff` passed all checks for the cleanup: fmt, Clippy,
1,474 tests, eight doctests, PostgreSQL smoke, and 63 e2e tests. E2E nextest
`12ac8937-f007-4785-af19-e6d953234d67` passed in 264.468s. All checks are
attested locally; no failed check was attested.


### No-coverage SQL admission — 2026-09-09

Scoped rs-distill pass 1: the planner reuses `Manifest::histogram_entries`
to select physical-ordinal coverage from the captured manifest. It uses the
existing membership column set, `try_fold`, `flatten`, and `all`; no new
configuration or index-selection abstraction is needed. The regression
extends the existing real SQL/Delta test and preserves its later partial
coverage, mutable-version, and routing assertions. No consolidation finding.

Scoped rs-evasion pass 1: `histogram_plan` returns its existing `Ok(None)`
when no captured file has coverage for all predicate columns. The caller
then performs ordinary SQL planning. Invalid manifest errors still propagate
to the existing logged planner fallback. The direct captured histogram APIs
retain their fallback semantics and budgets. No unsafe code, suppression,
sentinel, or weakened version mask was added. The new regression failed
before the fix: histogram execution count 1 versus expected 0.

This is an admission fix for zero usable coverage. Partial coverage can
still incur daily visibility work, and capture still loads the manifest.
Production latency validation and further narrow-window work remain open.


Second scoped pass found two fixture limitations. `build_db` did not call
`TantivyIndexService::with_reader`, unlike production startup, so it could
retain an empty cached manifest after publication. Adding that wiring alone
did not restore the routing assertion: flush indexes deliberately have
`ordinals_valid: false`. The previous assertion proved histogram fallback
execution, not use of an ordinal index.

The test now asserts ordinary SQL for the flush-only state, then uses the
real Parquet index builder to backfill only the newer file. It retains the
uncovered older file and all existing mutable-version and routing assertions.
No assertion was relaxed, and no sleep or cache-TTL workaround was added.

rs-distill: the fixture reuses production reader wiring and the existing
Parquet builder; no new cache or index-building mechanism. rs-evasion:
publication still uses real Parquet and object storage. The reader remains
a weak reference, and the fixture never marks flush ordinals valid by fiat.
Targeted verification is pending.


Final targeted run passed: `cargo nextest run --test suite
mutable_index_filter_cannot_resurrect_an_uncovered_version`, nextest
`8fe46d7a-ddb8-48ab-813c-5a0698a1bf56`, 3.015s. The test verifies no
usable coverage, flush-only unusable ordinals, then real partial Parquet
index coverage with mutable versions, memory rows, and SQL predicate forms.
A final reread found no additional scoped rs-distill or rs-evasion finding.
The complete local signoff remains required before pushing.


Complete local signoff passed for no-coverage admission: `make ci-signoff`
exited 0. Formatting, Clippy, 1,474 tests, eight doctests, PostgreSQL smoke,
and 63 e2e tests passed. Every check was attested locally; final status
leaves nothing for GitHub. Nextest runs: `e6066424-a057-4119-9366-997afd76a457`
(full suite) and `59ace074-928a-4245-ac40-4b7774e96168` (e2e).


### Narrow-window visibility — 2026-09-09

rs-distill pass 1: use DataFusion's existing `FilterExec` before the sort.
This avoids a new filtering field or a custom stream adapter. A small bound
expression closure reuses scalar casts and binary comparisons for the two
endpoints. Whole-day windows retain the existing plan without an extra filter.
The regression extends the real captured-histogram fixture.

rs-evasion pass 1: capture requires timestamp in the immutable key. Thus an
excluded timestamp cannot compete with an included version. The filter
preserves the complete schema and the already attached source/row ordinals.
File-date validation, memory authority, DVs, greatest-version ordering, and
tombstone removal remain intact. The test disables disk spill in its query
runtime to prove that unrelated same-day keys do not require sort storage;
this adds no production configuration.

The regression adds 20,000 unique out-of-window keys, each over 2 KiB, to
the same day as a one-microsecond query with partial index coverage. Before
the filter, it failed with `Memory Exhausted while Sorting (DiskManager is
disabled)`. After the filter, it returned the expected count 63 under the
same 32 MiB pool and released all reservations. The full targeted fixture
passed in 11.253s, nextest `fcb19cf2-f9f5-40d3-8472-088ae28495d6`. This
is a resource regression, not a production latency measurement.

Second scoped rs-distill and rs-evasion pass: no additional finding. Bounds
are cast to the existing timestamp field type, errors propagate, and the
filter uses inclusive start/exclusive end. No masks, assertions, or budget
checks were weakened. Further file/row-group pruning and production timing
remain open. The complete combined change now includes upstream read-path
refactor `a56f0be8` and requires a fresh local signoff before merge.


The combined tree, including upstream `a56f0be8`, passed full local
`make ci-signoff`: formatting, Clippy, 1,483 tests, ten doctests, PostgreSQL
smoke, and 63 e2e tests. All checks are attested; none remains for GitHub.
Full-suite nextest: `0db7b705-7f3b-46d1-8001-84369ca1084b`; e2e:
`9b29b1d3-b5b7-4a0c-9482-049da39bc1ca`.
