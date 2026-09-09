# Streamed histogram visibility cache

The 3-million-row benchmark showed warm partial-coverage histograms taking
about 736–1,012 ms with zero index fetches or opens. The sampled stacks include
streamed visibility sorting and deduplication. Production still has sparse
physical hash coverage, so this optimization alone cannot close the goal.

Work starts from master 5f56c874 in the isolated build worktree. The initial
regression extends the real MinIO histogram test: repeat a partial-index
histogram, compare exact buckets, and require a Delta visibility cache hit.
The baseline nextest run is compiling. No passing regression is claimed yet.

Implementation constraints:

- Cache only Delta masks when current memory rows and covered ranges cannot
  affect that day. Do not retain current memory winners in resident entries.
- Bind exact ordered SnapshotFile metadata, deletion vectors, root, visibility
  schema, dedup keys, tie-break, tombstone semantics, and clipped query bounds.
- Keep the memory mask separate so unrelated memory batch sizes cannot reuse
  an incorrectly sized mask. Recompute current-memory visibility on every query.
- Share the existing bounded resident cache and query memory pool. Pinned
  queries retain reservations after eviction. No new runtime opt-in setting.
- Preserve exact fallback scans of unindexed files and all source validation.
- Verify changed files, DVs, bounds, tombstones, and fresh memory rows before
  running the complete SQL benchmark and production rollout.

Run rs-distill and rs-evasion reviews twice after implementation, fix findings,
then freeze inputs for local signoff, cross-build, deployment, and monitoring.
The existing global delta-cache hit counter can cover both decoded-row reuse
and streamed Delta-mask reuse; do not claim it is per-query attribution.

Progress audit 2026-09-09 14:15:09 UTC: the baseline MinIO regression failed
at the intended reuse assertion (4.907 seconds). The first implementation
passed both the integration test and existing cache ownership test (8.735
seconds; nextest 155718b3-199f-492f-a93a-3d849319935e). It adds exact Delta-mask
identity, shares the resident LRU, rejects overlapping memory/ranges, and
charges metadata and bitmap ownership. Nothing from this branch is deployed.

First rs-distill/rs-evasion review: cargo lint found a large enum variant.
Boxed its key; no lint suppression was added. Made owned-enum matches
exhaustive and paired prepared files with reservation owners. Replaced the
pure dual-vector source-building loop with a fallible iterator. The remaining
preparation loop performs ordered I/O; the checked metadata-size loop stays
explicit because a nested fallible fold would obscure its overflow checks.
Added bitmap-vector/header accounting and reserved cache-hit memory masks
before allocation. Existing memory-pool eviction assertions remain explicit.

Expanded overlay tests are compiling. They cover inserts, newer versions,
tombstones, covered ranges with no surviving memory rows, and unrelated memory
with changing batch sizes. Compilation rejected a test attempt to clone
MemSnapshot. That type intentionally lacks Clone; move it out with mem::take
and restore it after the cases instead. The build is still finishing its other
job; do not restart while it remains live. Second review, invalidation coverage,
benchmarks, signoff, deployment, and production acceptance remain open.
Next progress audit is due by 14:45:09 UTC.

## Review pass 2 (validation pending)

### src/database/histogram.rs

**Reuse** — one existing resident LRU serves rows and visibility masks. The
new lookup helper returns typed Arc ownership; no second cache budget or knob.

**Algebraic** — rows and masks are separate enum variants. The large key is
boxed; matching is exhaustive. Prepared files are paired with reservations,
removing the parallel-vector cardinality assumption. MemSnapshot is moved
with mem::take in the test and restored; its public type remains unchanged.

**Functional** — source records use a fallible iterator. Ordered file I/O
stays a loop. The checked metadata accumulator stays explicit for readable
overflow handling; replacing it with a large nested closure adds no clarity.

**Combinators / derives** — metadata identity uses derived PartialEq, with
no string serialization or hashes standing in for exact equality. Lookup
uses find_map and then; admission preserves the Option for non-cacheable days.

**Consolidate / bloat** — removed the redundant files argument from mask
streaming. Bitmap allocation headers and retained keys are charged alongside
bitmap storage. New memory masks reserve before allocation on a cache hit.

**Evasion (block)** — none found in the current source review. No allow,
unsafe, ignored test, new public constructor, or weakened read-view type was
introduced. The original regression failed before the implementation. Final
acceptance still needs expanded tests, lint, benchmark parity/latency, signoff,
and production evidence. This review does not claim those checks passed.

| File | Lines reduced in pass | Highest-value corrections |
|---|---:|---|
| src/database/histogram.rs | about 10 | paired ownership; exhaustive variants; reused LRU |

Expanded validation passed: nextest ce3b3cf9-2924-4a82-b74c-da1f846a172e ran
both cache tests successfully in 8.806 seconds. Memory insert/update/delete,
empty covered ranges, unrelated memory row-count changes, bounds, tombstone
semantics, and DV identity checks passed. The final cargo lint passed with
no source warning suppressions. The implementation was copied to the primary
worktree for its optimized SQL benchmark; the primary and isolated Cargo
output directories remain separate. Benchmark compilation is now running.


### Goal audit — 2026-09-09 14:44 UTC

The goal remains active. Production is healthy on the verified local image,
but saved day/week hash queries have not passed latency acceptance. Historical
backfill is active and slow. The corrected per-project census leaves 208
Sep 2–8 files unindexed, totaling about 7.4 GB compressed. Three actual file
footers show selected hash/identity columns occupy 11–16% of compressed
column data, supporting a separately covered element-index implementation.
This requires reader, manifest, and GC changes; it is not implemented yet.

The visibility cache passed its red/green regression, memory-overlay and
identity invalidation checks, and two source reviews. Full local fmt and
Clippy now passed and are attested. Full tests are still compiling in session
23512; the optimized benchmark is compiling in session 75825. Both handles
were confirmed live. Sources remain frozen. Complete the tests and measure
the exact-query benchmark before image signoff and production deployment.
All required production correctness/performance checks, coverage acceleration,
and subsequent other-column CPU/heap profiling remain open.
Next goal audit is due by 15:14 UTC.


Full local nextest run 56564711-2c5f-45d5-87c5-2c6d0c45bfb9 passed all
1,508 tests in 223.819 seconds, with two slow tests and no retries. Seventeen
tests were skipped by this main-suite configuration. All ten
doctests also passed. fmt, Clippy, and test attestations were published for
the checked source. PostgreSQL smoke and e2e remain running in the same
make ci command. Optimized benchmark compilation passed in 23m29s; timing
awaits the end of competing validation work. No production image is signed
off or deployed for this cache change yet.


### Optimized cache benchmark — 2026-09-09 15:02 UTC

The 3-million-row SQL benchmark exited 0 with all 240 exact hourly bucket,
route, and cache-hit assertions passing. Every repeated warm partial query
reused all queried daily masks. Thirty-day partial warm medians were
208.1 ms (rare), 213.0 ms (medium), 595.0 ms (common), and 259.6 ms (overlap).
The prior uncached run measured 730.1, 757.4, 980.7, and 767.1 ms. These
are historical-run comparisons under different host load, not isolation of
every latency change. Within the new run, ordinary scans were still faster
for those partial cases (173.1, 166.5, 305.3, and 177.1 ms).

Complete warm histograms stayed at 9.3–147.3 ms across these predicates.
Partial application-cold queries remained at 1.33–2.08 seconds. Warm partial
queries had zero blob fetches and index opens, confirming the remaining
cost is elsewhere. Whole-process peak footprint was 594,500,544 bytes,
including setup and all queries; it is not a per-query heap profile.
The benchmark supports deploying the cache as an improvement, while
physical coverage, cold I/O, and residual partial counting costs remain open.

Full local checks passed under the configured retry policy, but e2e reported
one first-attempt checkpoint failure. Investigation found table creation
explicitly enables post-commit hooks and property reconciliation uses defaults,
contrary to the out-of-band checkpoint rule. A strengthened existing in-memory
property test is compiling against unchanged implementation for a red run.
New changes will need renewed checks before image signoff and deployment.


### Follow-up fixes — 2026-09-09 15:09 UTC

The deterministic property-reconciliation regression failed on the original
implementation: nextest 69723f0b-902c-4077-8a50-d47692dc3860 exited 100 in
0.367 seconds because checkpoint files existed after property updates with
checkpoint interval 1. Both table creation and property reconciliation now
reuse base_commit_properties, keeping checkpoint and expired-log hooks out
of the foreground operation. Assertions and scheduler behavior are unchanged.

The first cache benchmark still reopened every Parquet footer before cache
lookup. Preparation now happens after a miss, or lazily for a source that
must scan because its index is absent or fails. A cache hit retains the exact
file/DV/window identity already validated by its captured masks. Prepared
metadata and its memory reservation remain paired. A new operation counter
exposes histogram_parquet_prepares in timefusion_stats and benchmark output.
The existing real partial-index regression now requires only the unindexed
replacement to prepare Parquet on a repeat. The SQL benchmark asserts the
same per-day behavior over all warm partial cases.

These changes are not validated yet. Targeted green tests are running in
session 40708; the next optimized build is running in session 82809 in the primary worktree. Source
is frozen in both worktrees. The earlier full-check attestations predate
these changes and cannot sign off this final source. Re-run the affected
checks and repeated source reviews before publishing or deploying an image.


### Goal audit — 2026-09-09 15:13 UTC

The goal remains active and production performance acceptance is incomplete.
The first optimized cache run passed 240 exact SQL cases and reduced measured
warm partial medians, but still trailed ordinary scans and did not solve cold
latency. Source review identified redundant Parquet metadata reads on hits;
these are now deferred until a source needs fallback. A public statistics
counter and existing integration assertions verify the number of prepares.

A real e2e failure exposed foreground table-creation/property checkpoint
hooks. The strengthened property test failed before the fix (nextest
69723f0b-902c-4077-8a50-d47692dc3860). After both paths reused the existing
base commit settings, targeted nextest ed7196c6-826d-469e-ac36-555c1d61687b
passed all three tests in 9.650 seconds, including lazy metadata reads,
visibility invalidation, and reservation ownership. The new full local gate
is running; the optimized build remains live in session 82809. Both source
trees are frozen. The first cache executable is preserved for comparison.

Still required: final full checks and repeated reviews, optimized comparison,
image signoff, deployment, and production correctness/latency monitoring.
Dedicated element indexes, historical coverage convergence, and subsequent
other-column CPU/heap optimization remain in scope. The corrected project
backlog and actual footer/CPU evidence supersede earlier broad estimates.
Next goal audit is due by 15:43 UTC.


Coverage correction: the original Python URI partition filter returned files
from other projects. The corrected audit explicitly filters project paths
and cross-checks against Delta add-action partition values. For the saved
project, Sep 2–8 has 225 files, 17 covered and 208 missing (about 7.4 GB
compressed). Sep 6 has 33 files and zero covered. Earlier claims of about
1,400 missing project files and 166 Sep 6 files are superseded. The checked
audit tool and corrected metadata report are included with this change.

## Review pass 3 — revised cache and checkpoint hooks

Mechanical gate: fmt passed. cargo lint passed in 12m05s with no added
warning suppressions. Full tests and the optimized benchmark remain active.

Reuse: prepare_file owns the one metadata-open/reservation path used by both
visibility misses and scan fallback. Table creation and property updates
reuse base_commit_properties instead of maintaining different hook settings.

Algebraic: Option<Vec<prepared source>> records whether visibility resolution
already prepared the sources. Each source stays paired with its reservation;
there is no second parallel vector or dummy prepared record on a cache hit.
The visibility key still uses exact ordered Add metadata, DV, schema, key,
tiebreak, tombstone, and clipped bounds. Relevant memory prevents admission.

Functional: ordered file preparation remains a loop because source order is
part of equal-version tie handling. Cache hits avoid that work. An absent or
failed index prepares only its own fallback source. Indexed reads can skip
Parquet metadata because the cache retains masks for the same immutable Add
identity; fallback still validates the physical object's size and metadata.

Tests and observability: the existing real partial-index case checks one
Parquet prepare on a repeat, exact buckets, overlapping memory, and bounded
reservations. The SQL benchmark checks prepares and cache hits per day.
The counter counts preparation attempts and is exposed as a statistic, not
an enable switch. The checkpoint regression failed before the two hook fixes
and passed afterwards; no assertion or retry setting was weakened.

Evasion review: no new allow, unsafe, ignored test, serialized key substitute,
public read-view escape, or opt-out was introduced. Lazy metadata reads do
not bypass snapshot capture, membership filtering, tombstones, or fallback.
The isolated element-index branch is unfinished and is outside this release
review. No additional source issue was identified in this pass. Full gate,
final timing, production image signoff, and production acceptance are still
required; this review does not establish those outcomes.

### 2026-09-09 15:45 UTC goal audit

The goal remains fast exact hash queries in production. Revised cache checks
have passed formatting, clippy, and all 1508 nextest tests; the remaining
local gate and optimized benchmark build are still in progress. No cache
release has been deployed. Dedicated element-index implementation remains
isolated and uncompiled; publication, GC, and scheduling integration must
precede enablement. The measured 11–16% projected column-byte fraction is
not a measured indexing speedup. Production acceptance and subsequent
other-column CPU/memory profiling remain open.

Revised full local gate completed successfully: fmt, Clippy, 1508 tests,
10 doctests, PGWire smoke, and all 63 e2e tests. E2e run
682c4dfa-4f0e-4e97-a5b6-d93ebbc60fee passed in 194.284 seconds without
retries. Attestation references are recorded in local-visibility-cache-final-checks.json.
The optimized lazy-metadata benchmark completed compilation in 34m33s and
is now running the full 240-query matrix. Image signoff and deployment remain
pending. User rejected a special historical index: full-index rebuild cost
will be optimized separately, without a second artifact or worker pool.

Revised optimized benchmark passed all 240 exact-bucket and execution-route
checks over 3 million rows. Repeated warm partial queries reused every
day's mask and prepared only one unindexed replacement per day (30 for
30 days). The 30-day partial medians were rare 326.5 ms, medium 272.5 ms,
common 632.0 ms, and overlap 263.1 ms. Ordinary-query medians were 370.9,
369.4, 306.0, and 287.5 ms respectively. The common case remains slower
than ordinary SQL. This run does not establish an incremental latency
speedup over the first cache build: both native and ordinary controls
changed with host conditions. It proves exact results and eliminated
redundant preparations. Whole-process peak footprint was 595172288 bytes,
including setup and every query; this is not a query heap profile.

Production latency acceptance remains open. The candidate is ready for
local image signoff based on the completed correctness gate and benchmark;
no claim is made that cache optimization alone completes the hash goal.
