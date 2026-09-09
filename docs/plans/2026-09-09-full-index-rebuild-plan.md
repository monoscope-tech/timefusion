# Faster full-index rebuilds

User constraint: optimize the normal full index for the 30-day retention
window. Do not ship a separate historical hash index or another maintenance
obligation. The abandoned artifact experiment is preserved separately.

Initial change: project only the full index's configured fields plus timestamp
and id from Parquet. Preserve full schema, exact array terms, all physical
rows, blob format, manifests, and existing worker admission. This removes
unneeded decoding without creating new recurring work. Not yet tested.

Next measure full-builder phase costs (read/decode, indexing, final merge,
pack/verify/upload) on representative production-shaped files. Assess the
existing forced final merge with multi-segment query benchmarks before changing
it: avoiding build CPU is not success if it shifts excessive cost to queries.
Do not increase worker concurrency to hide per-file cost.

Validate full-text and exact hash results, physical ordinals, bounded memory,
actual rebuild CPU/time/I/O, and query latency. Then run the Rust reviews,
local signoff, deploy, and measure steady-state maintenance/backlog in prod.
Cache release validation proceeds independently from commit 0ec937e8.

16:03 UTC progress: full-index column projection is implemented. Saved
production footers estimate selected fractions of 57.4%, 69.7%, and 71.2%.
This is distinct from the abandoned hash-only byte estimate. VariantArray
construction previously repeated schema canonicalization per row. The
optimized standalone comparison on the pinned 58.3.0 library measured
390.46 ms per-row versus 331.00 ms per-batch preparation for 262144 rows
including JSON serialization (15.2% lower elapsed time). Alternating
order and equal byte counts are recorded; full value parity and end-to-end
rebuild benefits are not established by this microbenchmark.

Builder variants now retain prepared arrays per batch and share the existing
canonical JSON/KV renderer. The query UDF keeps its existing wrapper and
semantics. Added a full-build variant benchmark using both flatten modes
and the existing merge modes. Sources are frozen for cargo lint in
/tmp/timefusion-rebuild-target (APFS copy of the now-idle local check cache).
No full-builder changes are deployed or signed off yet.

## Rust review pass 1 (lint/tests still running)

rs-distill: the normal builder and blob lifecycle are reused. Prepared
variant arrays live in the existing column-kind enum; no second cache,
worker pool, artifact map, or enable flag remains. Both the builder and
existing row UDF share one canonical renderer. The source projection uses
borrowed schema names and selects actual Parquet root columns. Batch
preparation retains shared Arrow ownership; it does not clone each row.
Existing ordered writer and streaming I/O loops remain appropriate.

rs-evasion-review: reviewed index_batch, ColKind::detect/extract,
variant_to_text, prepared_variant_to_text, build_parquet_and_pack, and the
new benchmark. No added allow/unsafe/ignored tests, public constructor,
weakened ordinal checks, manifest-format changes, string-encoded state, or
new runtime knobs. Error propagation remains Result and no downstream
logic branches on the new context string. Unchanged UDF behavior was
checked as a second renderer consumer.

One behavior to validate explicitly: variant schema validation now happens
when preparing a column, including a fully null batch. Valid null variants
must still produce no text. Full-query parity, multiple batches, projection
coverage, and memory bounds remain test obligations. Benchmark timing is
not correctness evidence. Do not call this review clean before lint and
relevant integration tests have passed.

### Goal audit — 2026-09-09 16:11:48 UTC

The full goal remains active and unproven. User constraints now explicitly
exclude a special historical hash index and new maintenance obligations.
The active rebuild source changes only full-index projection and per-batch
variant preparation. First Rust review and cargo lint passed (7m20s);
subsequently extended the existing test for three batches including all-null
variants. Full local tests are compiling; rerun lint for final test source
before signoff. The complete build benchmark baseline is compiling.

Cache release local checks and 240 SQL cases passed; candidate Linux image
is building through make ci-signoff. No new image is deployed, so production
query latency acceptance remains open. Fresh metadata confirms historical
backfill progress but Sep6 remains 0/33 covered in the saved project.
Full build-time speedup, production maintenance cost, query latency, and
later other-column CPU/memory profiles are still outstanding.

Measurement protocol: the baseline benchmark source was copied into the
primary worktree, whose full-index builder remains unchanged. Its optimized
build runs in the primary release target; the candidate's debug tests use
a separate APFS-cloned target. Record both builder and benchmark hashes
before comparing. Use only the tantivy_variant_full_build Criterion group,
with the same source row count and compression level. Record final segment
counts before inferring a merge benefit: if a case produces one segment,
Now versus Deferred does not measure merge cost. Run timed comparisons when
heavy compilation has stopped, and capture concurrent host load.

Pinned Tantivy 0.22.1 selects up to eight writer threads, reducing the count
to fit its per-thread minimum within the existing 64 MiB total writer budget.
This partitions the arena and can affect segment count. It is a measurement
candidate, not justification to change worker concurrency or memory budgets.

Baseline full-builder benchmark compiled successfully in 15m10s. Its
binary/source identities are recorded in full-index-baseline-build.json.
The optimized release dependency cache has been APFS-copied to the candidate
target. The candidate benchmark build will start after its active test build
finishes; no candidate release compilation or timed comparison is claimed yet.

## Rust review pass 2

All 1508 nextest tests passed without retries in 220.038 seconds, including
the expanded variant JSON case across three batches and valid all-null
variant columns. All ten doctests passed. Test attestation is recorded in
full-index-rebuild-tests.json. The optimized candidate benchmark build is
now running against the same benchmark source as the completed baseline.

rs-distill: prepared column ownership remains in the existing enum; shared
rendering avoids duplicate JSON/KV code. Projection borrows declared names
and preserves all file rows. The added regression extends an existing test
instead of adding another fixture. Benchmark setup reuses the existing table
and batch fixture; positional field access is confined to that fixed fixture.

rs-evasion-review: no source-review blockers found. The all-null preparation
concern is now exercised by the passing integration test. Physical row
ordinals, exact list terms, publication, failure propagation, worker count,
and memory budget contracts remain intact. No suppression, unsafe, opt-out,
extra artifact, or fallback-only replacement was added.

This is not final release signoff: the test edit postdates the first lint
run. Remaining local checks and end-to-end performance/production evidence
are still required. No full-index speedup claim is established yet.

### Goal audit — 2026-09-09 16:41:57 UTC

Goal remains active. Cache release passed local correctness checks and 240
SQL benchmark cases. Its Linux dependency cook finished; the final profiling
application image build is now running. Image smoke, publication, push/merge,
local rollout, production exact-result comparisons, and latency acceptance
remain unproven. No new release has been deployed.

Full-index rebuild changes passed 1508 tests, 10 doctests, final formatting,
Clippy, and PGWire smoke. E2e is compiling. Both requested Rust reviews
are recorded. Baseline benchmark compiled successfully; candidate compilation
and a separate merge/query probe are active. Neither an end-to-end rebuild
speedup nor acceptable multi-segment query cost has been measured yet.

No special historical artifact or extra maintenance pool remains in active
source. Source changes preserve the normal full-index format and physical
row validity. Full production historical coverage and fast day/week queries
remain incomplete. Subsequent popular-column CPU and memory profiling and
optimization are still outstanding.

Merge/query diagnostic completed successfully: 64 exact histogram comparisons
passed over 300000 rows, duplicated common hashes, 30-day buckets, and a
physical mask excluding every seventh row. Immediate merge produced one
segment and took 7.155/7.837s; deferred merge retained 16 segments and took
2.476/2.206s. Warm query medians were rare 1.333/1.312ms and common
30.232/9.174ms for merged/deferred respectively. The probe uses the normal
full index schema with a synthetic fixture omitting unrelated text fields;
these build times do not establish full production reindex throughput.
It ran under concurrent compilation and is diagnostic evidence only.

The open_index path does not set a multithread executor, and pinned Tantivy
0.22.1 opens with Executor::single_thread(). Thus this probe did not obtain
its unmerged query result by adding a query worker pool. Whole-process peak
footprint was 169838080 bytes, including setup and all four builds; no
per-query heap/CPU attribution is claimed. Existing production merge behavior
is unchanged pending full-workload cost and query acceptance.
