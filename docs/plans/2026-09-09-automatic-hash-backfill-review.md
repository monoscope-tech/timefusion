# Automatic startup hash backfill

Production dd5647f deployed successfully through run 34343939580. Its saved
SQL smoke probe completed eight of sixteen comparison pairs with no mismatch;
the day/week cases timed out. Histogram completion counters remained zero.
That result does not establish fast native execution.

The new task logged active Tantivy indexing and a coverage census of 2,281
uncovered files (218 today, 1,339 in the last week, 724 older). It did not log
a startup backfill. Inspection found a remaining boolean opt-in:
timefusion_tantivy_backfill defaults to false, and the production service has
no corresponding environment entry. Scheduled reconciliation still runs.
The startup gate conflicts with the user's instruction to avoid unnecessary
knobs and let indexing run automatically.

The regression uses deserialized production Tantivy defaults, real MinIO,
a committed historical Parquet file, and a fresh empty index manifest.
It requires startup to publish physical element coverage and then requires
an actual SQL hash histogram to return one event through the native route.
The two identical array elements must count once.

The red run failed in 10.340 seconds with: default startup must publish a
physical hash index without an opt-in. The run exited 100; no passing
attestation was published. Log: /tmp/timefusion-startup-backfill-red.log.

Implementation removes the boolean field and its conditional return.
An attached indexer now always starts the existing bounded backfill pass.
A shared per-table semaphore prevents startup and scheduled passes from
building the same table concurrently. Other tables keep their independent
budgets and can still progress. Skipped duplicate passes emit a named event.
File-count, byte, age-selection, and build-concurrency bounds are retained.

First rs-distill pass: reuse the existing backfill path and Semaphore;
there is no new scheduler, index format, configuration option, or copy of
the indexing implementation. The admission map has one entry per table.

First rs-evasion pass: the semaphore is an internal resource guard, not an
opt-out. Its OwnedSemaphorePermit spans the pass; the DashMap entry guard is
dropped before I/O. Errors and cancellation release admission automatically.
Skipping duplicate work does not claim coverage; the next census still
checks manifests. The failed real regression remains enabled.

Second scoped review: startup and cron share the same Database admission
map through Arc clones. The check sits inside backfill_table_indexes so
both callers are covered. Existing manifest publication and index-reader
seeding remain unchanged. No warnings, tests, or type contracts are weakened.
Green regression, full local signoff, and production verification remain
pending. This change alone does not prove all historical coverage or the
release latency targets have been met.

The regression now passes in 0.663 seconds, nextest run
d60c3e3b-20c5-4a9f-a880-c42ded26e2bf. The default-startup index is physical,
contains the required element field, and answers the exact SQL count.
Full local signoff remains required before push. The known production
smoke failure and broader performance gaps remain open.

Local signoff follow-up: formatting, Clippy, the 1,508-test suite, ten
doctests, and PostgreSQL smoke have passed and published matching attestations.
The suite reported one flaky test: shutdown_writes_clean_snapshot_under_deadline
failed on its first attempt because the cursor snapshot was absent, then
passed on the configured retry. A separate nextest run with --stress-count 10
and --retries 0 passed all ten iterations in 8.883 seconds. The first failure
is retained as evidence; this does not establish that the timing issue is fixed.
The shutdown code is unchanged by this patch. Its one-second test grace gives
the snapshot at most 200 ms after the deliberately hung flush; runtime and
filesystem scheduling can exhaust that budget. Production shutdown uses the
configured grace and falls back to durable WAL replay when no snapshot exists.
No deadline, assertion, retry policy, or source contract was weakened.

Commands used the isolated /tmp/timefusion-isolated-target cache:
- make ci-signoff (full check still running through end-to-end tests).
- cargo nextest run --locked -E 'test(shutdown_writes_clean_snapshot_under_deadline)' --stress-count 10 --retries 0.

The current production chart handler still exceeds a 12-second client
deadline on the saved issue day/week windows. Automatic startup backfill
is a required coverage fix, not proof that those latency gaps are resolved.

Final local signoff completed with exit 0. All five checks have matching
published attestations; no checks remain for GitHub. End-to-end nextest run
830909b0-4b71-4ce0-a3c1-7574152c14cc passed all 63 tests in 255.558 seconds
(four slow, no retry). Full log: /tmp/timefusion-automatic-backfill-signoff.log.
The main-suite flaky result above remains part of the signoff record.
