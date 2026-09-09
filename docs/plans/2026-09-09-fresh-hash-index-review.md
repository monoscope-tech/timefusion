# Fresh hash indexes and queue shutdown

The fresh-index regression failed because flush-created manifests did not
claim physical row ordinals. The queue regression failed with two owners
of the database after shutdown, where the test requires one. Both failures
were observed before the implementation changes.

The server now builds each committed Parquet file through the existing
streamed index builder. Production startup and the real e2e fixture share
the callback factory. Pre-sort batches no longer determine row ordinals.
Low-level batch indexing remains explicit as batch_callback for search
fixtures and does not claim physical row order. File builds remain detached
from commit under the existing write-layer semaphore. Multi-file callbacks
build one file at a time. This adds Parquet reads to fresh indexing; ingestion
throughput and index readiness still need measurement before acceptance.

Queue shutdown cancels and waits for its tracked worker. TaskTracker avoids
an optional join handle behind a mutex and supports the existing shared
shutdown receiver. Its required tokio-util rt dependency feature is always
enabled, with no runtime option. The regression proves worker lifetime;
it does not by itself prove that every observed executor crash is fixed.

First rs-distill pass: reuse the committed-file builder, schema gate, table
resolver, callback factories, and task tracker. Keep the file loop because
each iteration performs sequential asynchronous I/O under the memory bound.
No new index format, task semaphore, or configuration option is introduced.

First rs-evasion pass: no physical-ordinal flag is forged from batch order.
The database clone resolves the actual table store; published entries come
from decoded files and remain one-file entries. Both production bootstrap
paths use the new factory. Errors propagate to existing sidecar-failure
reporting. No ignored assertion, suppression, unsafe block, or sentinel was
added. Remove the unused Context import left by deleting manual backfill.

Second scoped review: the index service still seeds its paired reader via
the existing weak reference. The callback clone does not acquire the newly
constructed buffered layer, so this wiring introduces no ownership cycle.
TaskTracker is closed after its single spawn and waited only after
cancellation. No further source finding. Compilation initially showed the
missing rt feature; that dependency declaration is corrected. Targeted green
verification, broader e2e coverage, and full local signoff remain pending.

Targeted verification now passes: both regressions completed in 2.308s,
nextest 3f5f2726-8486-48b8-87d3-1ec1395602e8. The broader real
Tantivy/search/bloom selection passed 35 tests in 10.675s, nextest
1425be4c-ba39-4a18-b950-278f24f04668, with one leaky-process
classification. A rerun with per-test output and a multi-file callback
case is pending. The added case commits two date-partitioned files and
requires a separate valid physical index for each.

The multi-file case passed. The full 35-test selection passed again in
11.146s without a leaky classification; the earlier classification did not
reproduce. The follow-up is committed as a17593ec (shutdown) and d72a4fa5
(physical flush indexes), then integrated with master 03cc1ae at e787f2a7.
Full local signoff is running against that combined tree using the isolated
cache. No follow-up push or deployment is claimed yet.

Full signoff caught the remaining callback rename in benches/tantivy_benchmarks.rs.
The real buffered-write benchmark now uses the same physical-file callback
and paired reader configuration as production. This missed consumer blocked
Clippy; no Clippy/test/pgwire/e2e attestation was published for that run.
The full signoff is being rerun after correcting it.

Full signoff rerun passed formatting, Clippy, 1,506 tests in 125.389s,
ten doctests, and PostgreSQL smoke tests. E2E failed with SIGSEGV during
bootstrap; no E2E pass is attested. Crash reports show libunwind decodeFDE
at a null address while anyhow captures the expected missing-table error.
The isolated cache_warmth::second_read_after_flush_hits_foyer test passed
without RUST_BACKTRACE and reproduced the crash with RUST_BACKTRACE=1
in 0.877s. Backtrace capture remains enabled for acceptance.

A first attempted system-linker experiment was invalid: LC_BUILD_VERSION
still reported LLVM 15.0.7 because Cargo's configured flag took precedence.
A private wrapper now selects Apple's linker; the resulting binary reports
1115.7.3. Its targeted run is pending. No repository linker configuration
has changed. Rust issue https://github.com/rust-lang/rust/issues/104388
reports similar symptoms, but that does not establish this failure's cause.

The verified Apple-linked binary passed the same test with backtraces enabled
in 4.035s. The configured LLVM 15.0.7 binary failed in 0.877s. Removed the
macOS-specific linker override, restoring the platform default. Full local
signoff is running against that actual configuration, rebuilding dependencies
as needed. The experimental metadata run was diagnostic only and was not
used to attest E2E. Review: removing the override adds no suppression or knob,
and the failing existing E2E remains part of the standard check.

Default-linker full signoff progress: Clippy passed; 1,506 tests passed in
133.752s (nextest b830f683-df5a-4736-a925-2382fea33dd8); ten doctests
passed; PostgreSQL smoke passed and was attested. E2E run
91a7df12-8991-4f33-9947-2ab019fafce3 is executing 63 tests. The source
inputs remain frozen. Root's empty-query and DML-range fixes are separate
until this diagnostic signoff finishes, then will receive combined signoff.

Default-linker full signoff completed successfully. All 63 E2E tests passed
in 222.964s, nextest 91a7df12-8991-4f33-9947-2ab019fafce3, with no retry
or leak classification. All five checks are attested; no checks remain for
GitHub on this tree. This establishes the standard E2E path works with
backtraces enabled after removing the LLVM 15 linker override. Combined
source will receive its own full signoff before push.
