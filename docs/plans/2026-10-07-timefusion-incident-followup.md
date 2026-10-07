# TimeFusion incident follow-up

Work is in progress. Local fixes have not been deployed. Production host inspection
is read-only; the proposed image cleanup still needs the previously requested approval.

The first resource-safety candidate is superseded by a required data-loss fix.
An additional real Delta/WAL regression showed that restoring a timed-out
snapshot's holds also released its coalescing fence. Seventeen later writes then
crossed the coalescing threshold; retiring the original detached commit left
only one late row instead of seventeen. The red result is preserved in
`/tmp/timefusion-snapshot-coalesce-red.log`.

Restoration now retains the prefix while the topic has an unresolved commit or a
confirmed receipt awaiting retirement. Known terminal failure still releases the
fence. Restoring holds and their prefix state is synchronized with the batch lock.
All six initial runtime receipt cases passed in the isolated worktree after the
fix (`/tmp/timefusion-snapshot-coalesce-green.log`). The same surgical change is
now in the main resource-safety batch, with a seventh case for the deduplicated
schema. All 241 targeted CI-feature write/WAL/buffer and detached-DML regressions
passed (`/tmp/timefusion-snapshot-coalesce-main-regressions.log`). Renewed code
checks passed: formatting, lint, 2,665 nextest tests, 16 doctests, PGWire smoke and
69 end-to-end tests. Fifteen non-e2e and two e2e tests are intentionally skipped.
Nextest marked three passing tests as leaky; this does not prove their child
processes were cleanly reaped. Inputs still match the pinned source hashes;
results and exact check fingerprints are recorded in
`/tmp/timefusion-snapshot-coalesce-validation.json`. The complete log is
`/tmp/timefusion-snapshot-coalesce-ci-validation.log`.

The new main image-input fingerprint is
`ed8ffb9679d445547df6052a9669fb076a00027aa846a3db2d3ae7daca66fa47`.
The earlier five code-check attestations and local image smoke remain evidence
for their original inputs only. They do not validate this changed batch, which
passed the renewed code checks above. Its new local Linux amd64 image also built
and passed the real PGWire smoke probe. The image label and current source
fingerprint both match `ed8ffb9679d445547df6052a9669fb076a00027aa846a3db2d3ae7daca66fa47`.
The result, local image ID and log hash are recorded in
`/tmp/timefusion-snapshot-coalesce-validation.json`; build/smoke output is
`/tmp/timefusion-snapshot-coalesce-image-prebuild.log`. Nothing was published or
deployed. The full `make ci-signoff` publication step remains outstanding.

## Integrated resource-safety validation

The first frozen-tree signoff ran 2,663 non-e2e tests: 2,662 passed and one failed.
The existing multi-partition admission test still required one permit, while the
new weighted admission charged eight. The test now verifies a single coalesced
admission root, a multi-permit charge for the parallel sort, and return of all
permits when the client stream is dropped. It passes with the CI feature set and
profile; no production code or timeout was changed for this gate failure.

The first attempt's formatting and lint passes remain valid for their original
fingerprints. Logs and inputs are preserved in
`/tmp/timefusion-incident-ci-first-validation.json`. The revised frozen tree passed
all five checks: 2,663 nextest tests, 16 doctests, PGWire smoke, 69 end-to-end tests,
formatting and lint. CI intentionally skipped 15 non-e2e tests and two e2e tests.
All helper tests passed, and all five exact input fingerprints were attested;
`make ci-status` reported all checks reusable for that earlier tree. Its inputs,
fingerprints and results are recorded in
`/tmp/timefusion-incident-ci-final-validation.json`; these are historical evidence
and were superseded by the snapshot coalescing fix above.

The Makefile's `ci-signoff` target has an unconditional image publication step,
even with explicit `CHECKS`. That step was stopped during its local Docker build,
before image publication; the wrapper exited 2. The passing checks and their
attestations remain valid. A subsequent local-only `production-image.py prebuild`
completed the Linux amd64 build and passed its PGWire smoke probe. The source
fingerprint matched that earlier tree:
`bb84e84176d4db85ffbdd519d9ab7b55aefc19fee1807cb9b6bed295e6409cee`.
It does not include the subsequent coalescing fix; a new image is required.
The candidate and log hashes are recorded in
`/tmp/timefusion-incident-local-image-validation.json`; the build log is
`/tmp/timefusion-incident-image-prebuild.log`. Publication remains outstanding,
and the complete signoff command is not green. No commit, master push or production
deployment occurred. The current worktree includes pre-existing user edits. The
full incident scope, including production disk recovery and rollup coverage,
remains open.

## Acceptance and remaining work

| Requirement | Current evidence | Remaining acceptance |
|---|---|---|
| Restore disk headroom | Latest read-only snapshot: **zero available bytes, 100% root usage**; 1,886,839,795,712 bytes total and 1,792,164,810,752 used. WAL decreased to **99.6 GiB across 104 files**, and completed flushes reached 14,855. Ten root-owned, fully allocated 10 GiB ballast files remain under `/home/ubuntu/disk-reserve`; current TimeFusion mounts exclude it, but their intended purpose is unconfirmed. An earlier inventory identified 44 unused TimeFusion images, approximately 13.4 GB, excluding active service references and three rollback images. | Obtain the pending disposable-file confirmation and explicit host read-only exception before cleanup, or expand storage. Revalidate candidates before any deletion. Verify a safe free-space margin and stable WAL growth; the observed decline alone proves neither. Never remove unflushed WAL. Current snapshot: `/tmp/timefusion-recovery-release-live-observation.json`. |
| Explain flush stalls and retained WAL | The morning upload failed after 600 seconds. Local fixes preserve snapshot pins, schedule independent per-topic retries, retain ownership through detached publication, and reconcile durable publication receipts before replay. Real MinIO fault cases reproduced and fixed an accepted PUT followed by HTTP 503/412 advancing to another Delta version. | The corrected recovery batch passed all five local gates. Its local Linux image probe also passed. Complete production verification; identify the actual oldest production WAL pin using the new diagnostics. Verify its progress and allocated WAL reclamation. The local retry reproduction does not establish the cause of the earlier production WAL growth. |
| Query sort-memory exhaustion | Admission charges actual sort fan-out and multiple sort stages; a real per-series window query exercises the physical plan. Small pools admit one heavy query rather than forcing a minimum of four. The resource/recovery candidate passed all five local gates and its local Linux image probe. | Verify production resource errors, spill behavior and queueing after the validated candidate is approved and deployed. |
| Overdue maintenance and invalidations | Copied journal: oldest overdue derived task is an October 9 future slice with an October 1 deadline and zero attempts. Its required parent has five pending ten-minute slices and only one completed slice; pending parent deadlines are October 9. The invalidate deadline policy explains how a derived task can be due before its future slice. Nine retries report `source_not_flushed`. Current host logs also establish ENOSPC failures in maintenance lease checkpointing. New writes clamp future invalidations; cleanup now preserves repair staging eligible for resumption before an early table-resolution sweep can discard it. Five targeted e2e cases pass. | Restore headroom, verify overdue eligible work after the locally validated batch is approved and deployed and dirty partitions progress. Confirm the archive-derived dependency and early-deadline explanation against live task diagnostics; distinguish policy-eligible cells from historical debt before altering concurrency or deleting existing tasks. |
| Compaction alert | A reset-aware `increase(value)` rule was applied in place of the lifetime-counter comparison. Recent readbacks return HTTP 500, so current saved configuration and scheduled evaluations are **unverified**. | Verify the current rule and actual scheduled evaluation after API access recovers; confirm the previous false positive clears. Evidence: `/tmp/timefusion-compaction-monitor-current-readback-error.json`. |
| Numeric cast failures | Monoscope empty predicates cast operands to text. The fix is included in externally committed `90fbc00bc` and merged PR #652. Its successful GitHub gate reused exact-input build, doctest, unit and integration attestations; the doctest fingerprint was `89ea08d05f6503ac84f3784532c1650b01b1234e8e77f4f274f66046a951d005`. Six TimeFusion predicate cases and the earlier 1,743 Monoscope examples passed. Actual incident parameter values remain unavailable. | Recover actual SQL plus parameters without treating scrubbed placeholders as evidence; verify recurrence after the observed rollout. The running service uses the image labeled revision `908cbb9c206968763ac94c2d9e707da65026f1c9`; its verified source tree contains the numeric empty-predicate fix. |
| Dashboard rollup coverage | Frequent slow statements were ranked and expected fallbacks classified. An isolated nested-CSE routing fix covers eligible cached one-minute charts; **233** rollup tests and lint passed with real raw-versus-rollup schema/result equivalence. Observed thirty-second requests remain expected grain-mismatch fallbacks. The refreshed routing candidate passed all five local gates: 2,708 main tests, 16 doctests, PGWire smoke, 70 e2e tests, format and Clippy. | Keep this broader routing batch separate from the first resource-safety release, verify production coverage and latency after approved rollout. Its local Linux image and PGWire probe passed. Do not claim the classified raw event/trace queries are aggregate routing opportunities. |
| Monitoring and issues | 502 issue records inventoried; 34 exact-title groups contain 86 extra records. Two current issues acknowledged; none archived without recurrence proof. Flush-stall, overdue-task and resource-error monitors were applied. Local lag reads actual buffered rows; latest deployed lag is 38,434,607 against 1,281 actual rows. The filesystem monitor is prepared but inactive. | Verify monitor readbacks and recurrence by pattern before closure. Deploy and sample filesystem/WAL gauges, then activate the disk monitor. Confirm corrected lag and resource alerts reflect runtime behavior. |

## Retry acceptance

Latest production diagnostics (read-only `timefusion_stats`, approximately
00:43 UTC October 7): WAL reports 111,149,059,772 logical bytes (103.5 GiB) across
108 files, down from the earlier 168-file snapshot. This proves reclamation has
progressed; it does not identify the oldest live retention pin or prove a stable
free-space margin. Root still reports zero available bytes. The buffer holds
1,110 rows across 30 buckets, while the old `rows_in_buffer_lag` counter reports
38,033,334. The process has no orphaned topics and reports `drained=false`.
Evidence: `/tmp/timefusion-full-disk-live-stats.tsv`. New allocated-byte and
pin-owner diagnostics are still local and must be deployed before relying on
them for production attribution.

- Preserve bounded transport retries and generous Parquet upload timeouts.
- Terminal upload failure preserves rows and WAL holds; per-topic backoff schedules
  fresh callback requests at roughly 5, 10, 20, 40 and 60 seconds with jitter.
- A detached commit blocks further attempts for its topic while other topics proceed.
- A successful commit releases only its own snapshot and retention holds.
- A staged flush now stays in reconciliation after an inconclusive probe, retaining
  its original uploaded files. It probes outside the commit lock with 10–15 seconds
  of jitter between attempts, without publishing another commit.
- The detached ceiling is now an alarm, not cancellation: the watcher retains the
  original callback and topic marker until a known outcome. A terminal upload
  failure still schedules fresh requests after backoff. Late success drains the
  landed rows without another publication. Other topics continue progressing.
- Individual S3 requests remain bounded. If an abandoned publication never becomes
  provably landed, its topic deliberately stays in reconciliation and needs operator
  investigation. Absence alone cannot prove that an abandoned request failed.
- The schema-evolution fallback now observes the exact `write_commit_entry`
  boundary of the combined writer. A failure before that boundary retries normally.
  After publication begins, it probes that exact immutable version for its unique
  commit identity. Its own record confirms success; another writer owning the
  version proves its delayed conditional write cannot land. An absent version
  after response loss remains uncertain. The probe runs outside the commit lock.
  WAL watermark, landed digest and publication identity are written together so
  none is lost when Delta replaces the metadata map. The observer is removed from
  the shared table after success.
- Callback panics now retain unresolved topic ownership instead of scheduling a
  new publication, including when the watchdog is disabled. The callback is
  constructed inside its task so construction panics cannot unwind the caller.
  A caller cancellation transfers the original task to the detached watcher;
  guards restore all active and queued snapshots before releasing their WAL pins.
  Confirmed terminal failures retain normal backoff; a task panic requires
  investigation because its storage outcome is unknown. `airborne_topics` now
  describes detached or unresolved commits.
- Destructive force-flush cancellation now restores taken rows with a drop guard
  before releasing their in-flight WAL holds. Failure to restore retains the
  existing orphan protection. Confirmed success disarms the guard and advances
  normally. Real Delta/WAL cases reproduced zero readable rows after cancellation
  before the fix, then verified immediate restoration, unchanged retention floor,
  healthy-topic progress and eventual release without another publication.
- Successful detached snapshot flushes now enqueue receipts for their exact
  source snapshots, with committed file URIs and index batches. Under the flush
  lock, the next cycle reuses the existing prefix-drain/mutation checks, advances
  WAL and saves the cursor snapshot. This works without a replay landed digest:
  append-only tables and a disabled replay optimization do not republish the
  original snapshot. An identical later write remains distinct.
- Detached destructive force flushes now use the same runtime receipts. Failed
  or unresolved takes restore their original batches as a protected prefix and
  capture its mutation generation under the batch lock. Late success consumes
  that prefix without touching arrivals before or after restoration, including
  batches subsequently coalesced in the tail. Append-only tables do not require
  a landed digest for this runtime completion proof.
- Restart recovery still needs review before claiming the complete uncertainty
  invariant. Aborting a local task does not prove that a remote commit failed.
- In-place DML now waits for both active holds and detached/unresolved topic
  ownership before mutating memory. It retires confirmed receipts first and
  keeps the existing flush lock through the synchronous memory mutation so a
  new snapshot sees post-DML rows. The deadline returns an execution error;
  the rejected statement does not apply its memory mutation. The lock is
  released before the Delta leg. This can also queue DML behind another topic's
  active flush cycle; the lock is not held while polling detached outcomes.

Five real in-memory Delta/WAL interruption cases pass, covering a panic after
publication with no watchdog, before expiry and after detaching, and successful
original publication following caller cancellation with/without the watchdog.
They verify no second publication, healthy-topic progress, readable uncertain
rows, unchanged WAL cursors after panic, and release after confirmed success.
Cancellation cases use parallelism one with a smaller second topic already
snapshotted and queued, proving that its unpolled snapshot releases its pins too.
Logs: `/tmp/timefusion-flush-panic-red.log` (duplicate publication reproduced) and
`/tmp/timefusion-flush-interruption-tests.log` (five passed, including the
strengthened queued-snapshot cases). The broader write/buffer/WAL run passed all
224 tests in `/tmp/timefusion-flush-interruption-regressions.log` on the final
inputs, including the queued-snapshot cases and respecting a disabled watchdog
after cancellation. Formatting passes. Input hashes and commands are recorded in
`/tmp/timefusion-flush-interruption-validation.json`.
`cargo lint` passes for the final inputs;
`/tmp/timefusion-flush-interruption-lint.log`. These are targeted checks, not full
release signoff or production verification.

Detached snapshot receipt validation:

- Real Delta/WAL cases reproduced retained committed rows with replay skipping
  disabled and without dedup keys: `/tmp/timefusion-detached-receipt-red.log`.
- Both fixed cases pass: `/tmp/timefusion-detached-receipt-tests.log`. Each inserts
  an identical late arrival into the same bucket, confirms only the original
  prefix drains, then verifies two physical rows and two publications in Delta.
- Broader write, invalidation and commit checks: 235 passed, with no handle-leak
  warning this run; `/tmp/timefusion-detached-receipt-regressions.log`.
- Formatting and `cargo lint` pass on the final receipt-retirement inputs;
  `/tmp/timefusion-detached-receipt-lint.log`. Receipt retirement owns a fresh WAL
  retention token because the original caller already released its token after
  restoring holds. Checked inputs and commands:
  `/tmp/timefusion-detached-receipt-validation.json`.

Detached force-flush receipt validation:

- Three real Delta/WAL force cases failed before receipt support, retaining
  committed rows: `/tmp/timefusion-force-receipt-red.log`.
- Five normal/force receipt cases pass in the broader run. The force case with
  an arrival before restoration also inserts sixteen later rows, crossing the
  batch-coalescing threshold; all seventeen late rows remain buffered, and Delta
  ultimately contains eighteen physical rows in exactly two publications.
- Broader write, WAL, invalidation and commit checks: 238 passed, no handle-leak
  warning; `/tmp/timefusion-force-receipt-regressions.log`.
- Formatting, diff whitespace checks and `cargo lint` pass. Checked input hashes
  and commands are recorded in `/tmp/timefusion-force-receipt-validation.json`.
  These are targeted checks, not release signoff or production verification.

Detached publication / in-place DML validation:

- Real SQL UPDATE and DELETE originally completed before the gated original
  Delta publication: `/tmp/timefusion-detached-dml-red.log` (two failed cases).
- Eight cases now cover UPDATE/DELETE, snapshot/taken flushes and both late
  success and expiry. Expiry leaves the original row and Delta version unchanged;
  after the original resolves, a fresh SQL retry succeeds. Delta-only reads
  verify the old publication cannot resurrect the deleted or pre-update row.
- Broader DML, write, WAL, invalidation and commit checks: 292 passed;
  `/tmp/timefusion-detached-dml-regressions.log`.
- All 28 local MinIO in-place DML integration checks pass; `cargo lint`, formatting
  and diff whitespace checks also pass. Commands, logs and input hashes are
  recorded in `/tmp/timefusion-detached-dml-validation.json`. This is still
  targeted validation, not full release signoff or production deployment.
- Receipts are runtime proof. This does not establish crash recovery for an
  append-only commit whose response was lost, nor settle the restored destructive
  take's original batches. Keep those acceptance items open.

Force-cancellation validation on the final formatted inputs:

- Seven interruption cases passed, including force-flush cancellation with and
  without the watchdog: `/tmp/timefusion-force-cancellation-tests.log`.
- Broader write/buffer/WAL run: 226 passed;
  `/tmp/timefusion-force-cancellation-regressions.log`.
- `cargo lint` and `cargo fmt --check` passed;
  `/tmp/timefusion-force-cancellation-lint.log` and
  `/tmp/timefusion-force-cancellation-fmt.log`.
- Checked input hashes: `/tmp/timefusion-force-cancellation-validation.json`.

Five targeted retry/reconciliation tests pass. The Delta regression stages real
Parquet in an in-memory object store, verifies that absent Adds after timeout keep
the reconciliation future pending, then commits the original Adds through a second
table handle and observes that single publication. The two detached-alarm cases
verify retained WAL protection and healthy-topic progress before late success or
terminal failure. Log: `/tmp/timefusion-uncertain-flush-tests.log`.

Broader validation: 72 write and commit regressions pass in
`/tmp/timefusion-uncertain-flush-regressions-detailed.log`; `cargo lint` passes in
`/tmp/timefusion-uncertain-flush-lint.log`. The first broad run reported one leaked
process handle; a repeat with detailed final status passed without that report.
The existing live flush-stall monitor now includes
`flush_commit_landing_unconfirmed`; its query was validated and exactly one
monitor with the original title and updated query was verified after apply.

Returned HTTP failures now retain their transport classification through the
Delta/object-store error chain. A connection-establishment failure permits the
existing probe/retry path. Timeout, interruption, request, decoding and unknown
HTTP failures mark publication uncertain; a negative snapshot probe cannot
authorize republishing or deleting staged Parquet. This covers the S3 client's
own response deadline, not only the outer 600-second deadline. No error-message
matching is used to classify these failures.

Transport validation: 79 write and commit regressions pass in
`/tmp/timefusion-transport-commit-regressions.log`; `cargo lint` passes in
`/tmp/timefusion-transport-commit-lint.log`. The real transport test sends a
conditional Delta-log PUT through `AmazonS3Builder` to a loopback TCP listener;
the server receives the PUT and withholds its response. The S3 client's 100 ms
deadline fires before the two-second outer deadline, and the returned error
retains uncertain publication. Six parameterized typed-error cases verify the
other transport classifications. All test requests use explicit dummy credentials
and local endpoints.

Schema-evolution validation: four targeted tests and 84 write/commit regressions
pass in `/tmp/timefusion-schema-flush-tests.log` and
`/tmp/timefusion-schema-flush-regressions.log`. They exercise the actual database
fallback on a memory-backed legacy schema, confirm all recovery metadata survives,
and verify the per-attempt observer does not become the shared table's log store.
The publication cases use real Delta writes for both possible owners of the target
version. A local filesystem test rejects an upload with directory permissions,
verifies no publication was attempted, restores storage access, and succeeds on a
fresh write. Full release validation remains outstanding.
`cargo lint` also passes for this batch; log
`/tmp/timefusion-schema-flush-lint.log`. The earlier compile-only check passed
before the final metadata-builder adjustment; the current targeted regressions
and lint cover that adjustment.

## Future-dated data and retention

The copied invalidation journal has one future-date entry: project
`6297304f-89c0-48a9-9b5c-20bcac61f54e`, logs, October 9 at hour 11, invalidated
October 1 at 11:19:03.861 UTC. This is consistent with the oldest blocked derived
task's future slice; it is evidence of future-dated source invalidation, not merely
an old scheduler deadline.

Normal ingestion bounds timestamps to at most 48 hours ahead. The journal's
October 9 date therefore cannot by itself prove that a future source row was
admitted on October 1. DML re-appends bypass the bound, and invalidation before
rejection provides another explanation for an ordinary incoming batch.

The real database/buffer/WAL path reproduced a second defect:
`insert_records_batch_bounded` persisted invalidations before the buffered layer
filtered event time. On an October 1 clock, rejected October 9/10 rows created
durable dirty partitions; a mixed batch also dirtied both rejected dates. Those
rows never entered WAL. This can explain an early future invalidation without
proving any future row was buffered, but does not establish the origin of the
historical production entry.

For buffered writes, the existing admission filter now runs before invalidation
and is applied only once. Invalidation still precedes WAL durability; direct
Delta writes and the DML timestamp bypass retain their behavior. Three cases
cover all-rejected rows, a mixed batch and DML re-appends, comparing persisted
dirty dates, maintenance task dates, buffered rows and decoded WAL contents.

- Red reproduction: `/tmp/timefusion-admission-invalidation-red.log`, two failures
  showing rejected dirty dates, with the DML control passing.
- Fixed cases: `/tmp/timefusion-admission-invalidation-tests.log`, three passed.
- Broader write, invalidation and commit regressions: 233 passed, with one nextest
  process-handle leak warning; `/tmp/timefusion-admission-invalidation-regressions.log`.
  The detailed repeat also passed 233 and identified the warning as the existing
  `write::coerce_tests::every_placeholder_in_every_row_is_typed` test:
  `/tmp/timefusion-admission-invalidation-detailed.log`. That test passes without
  the warning in isolation: `/tmp/timefusion-placeholder-leak-isolated.log`.
  Its behavior under parallel load remains a release-validation follow-up; no
  timeout or check was weakened to hide it.
- `cargo lint` passes: `/tmp/timefusion-admission-invalidation-lint.log`.
- Formatting and diff whitespace checks pass. Inputs and commands:
  `/tmp/timefusion-admission-invalidation-validation.json`.

Do not delete historical future tasks merely because this defect was reproduced:
verify source and pending WAL evidence before retiring them.

A real in-memory Delta and WAL regression reproduced retention for an accepted
row one day ahead: regular flushing left it buffered after arrival dwell, with
its WAL pin still held. Periodic selection now includes future buckets after the
existing dwell while keeping the current bucket open. The snapshot pipeline
already marks these early flushes to preserve read visibility. The regression
passes and verifies pin release, reopened future-bucket batching, and exactly
three durable rows after three acknowledged inserts. This confirms a code defect,
not ownership of the current production retention pin.

- Red reproduction: `/tmp/timefusion-future-dwell-red.log` (Delta version remained
  zero at the expired dwell instead of advancing to one).
- Fixed regression: `/tmp/timefusion-future-dwell-green.log`, one passed.
- Broader write/buffer/WAL regressions: 219 passed;
  `/tmp/timefusion-future-dwell-write-regressions.log`. Afterwards only callback
  formatting changed; `cargo fmt --check` passes. Input hashes are recorded in
  `/tmp/timefusion-future-dwell-validation.json`.
- `cargo lint` passes for the formatted inputs;
  `/tmp/timefusion-future-dwell-lint.log`.

## Recent rollup misses

An earlier follow-up could not rank these candidates by latency: Monoscope's
event and schema endpoints returned HTTP 500, and a bounded earlier Docker-log
window returned no retained samples. Event search subsequently recovered and
returned three timestamp-cast CSE projection refusals around 01:00:56–01:00:58 UTC
October 7. The summarized plan is a count grouped by project with a lifted
`CAST(timestamp AS Timestamp(ns, "UTC")) AS __common_expr_5` below a filter.
The current CSE matcher only handles a projection directly beneath the aggregate;
these samples identify another routing candidate, but not its frequency or cost.
The full-event lookup eventually returned HTTP 500. A bounded read-only host log
capture recovered the full plan and SQL template instead. These grouped count
queries match `tfTopProjectCounts` in Monoscope's `src/BackgroundJobs.hs`, called
by the hourly ingest continuity and read-consistency checks. They are background
health checks rather than dashboard queries. Evidence:
`/tmp/timefusion-rollup-cse-and-latency-host.txt` and
`/tmp/timefusion-rollup-current-recheck.json`. A subsequent bounded host log
sample confirms that the distinct-name facet still contains the zero project UUID
and unresolved service variable:
`/tmp/timefusion-rollup-cse-host-current.txt`. The existing facet example alone
does not justify broadening projection routing. Resource-safety code checks and
the local image smoke have passed on the frozen code; rollup optimization remains
a separate, open acceptance item.

The captured facet also requires both `name` and `resource___service___name`.
Inspection of all five tiers in `schemas/otel_logs_and_spans.yaml` confirms that
none contains both: the dashboard and session tiers omit `name`; the endpoint
and hash tiers omit service name. An alias-walking change alone therefore cannot
answer this query. The schema-derived dimension inventory is recorded in
`/tmp/timefusion-facet-rollup-coverage.csv`. Its captured parameters include a
zero project UUID and an unresolved `{{var-service}}` placeholder, so it is not
yet evidence of an expensive valid dashboard request. Obtain frequency and
latency evidence before adding dimensions or a new tier, including a measured
cardinality and maintenance-cost assessment.

### Cost ranking from retained statements

A subsequent read-only capture selected `pgwire.slow_statement` from the last
30,000 service log lines in the preceding hour. It contains 289 statements,
including 120 SELECTs. These are lower-bound counts: successful statements below
one second are omitted, and older lines may have fallen outside the retained
tail. Templates and durations are grouped in
`/tmp/timefusion-dashboard-slow-statement-ranking.json`; individual records are
in `/tmp/timefusion-dashboard-slow-statement-records.json`. This is a cost ranking,
not proof that each query fell back to raw storage.

| Shape / fingerprint | Slow executions | Total / maximum duration | Classification and next action |
|---|---:|---|---|
| Event list, `00f95f89a3fc92493e3e98f4adcfd012` | 15 | 159.7 / 43.0 s | Individual event fields ordered by timestamp; existing aggregate rollups cannot answer it. Investigate raw scan/pruning separately. |
| Similar-trace lookup, `60a6b8593d7f9c53fa1934bf38fa4b82` | 1 | 70.1 / 70.1 s | Individual trace IDs ranked by row predicates; not an aggregate coverage candidate. |
| Two trace graph shapes, `1738520fea44aa31f3e8ce045b832b9a` and `02e10c9305baff38df512ee5d0b0380f` | 44 | 99.5 / 3.8 s | Trace/span topology CTEs; inspect full plans before proposing a tier. |
| Hash/hour HTTP-success count, `652d4028b10fadc5c538db5d0b48c104` and `224e01d96f283cdbd5e77d1337b34115` | 7 | 77.3 / 45.0 s | Source matches Monoscope's proven-endpoint background job (`status < 400`). Host scan logs already reference `otel_logs_and_spans_rollup_hashes_30m`; investigate residual raw boundaries and coverage rather than treating latency as a routing miss. |
| Project health count, `04c0de68d05bb466f321ab85875a6e63` | 1 | 33.6 / 33.6 s | Hourly infrastructure check; CSE timestamp-cast refusal confirmed. Keep separate from dashboard prioritization. |
| Service/error and service/kind charts, `0dedd4ad975821c5c5a8c3096969da47` and `1361e4f255fad37e77ac31854549c9fb` | 6 | 12.3 / 2.3 s | Actual chart aggregates; correlate exact bucket widths and certified coverage before selecting a safe routing fix. |

Related retained chart warnings include nested CSE projections, but their shown
width is **30 seconds**, below the existing **one-minute** tier. Fixing the
projection walker would not make those samples eligible. These warnings also
show the zero project UUID. Evidence:
`/tmp/timefusion-rollup-ranked-shape-outcomes.txt`. The HTTP-success hash scans
are captured separately in `/tmp/timefusion-hash-coverage-outcomes.txt`.

A follow-up capture for the exact service-chart templates found 46 raw-table
scan contexts and no rollup-table scan contexts. It contains 45 Delta metadata
selection records, nine of which selected all files. These are metadata records,
not measured bytes fetched or completed scans. Bucket widths and refusal reasons
were not present in this capture, so the routing change is still unselected.
Evidence: `/tmp/timefusion-service-chart-correlated-clean.txt` and
`/tmp/timefusion-service-chart-scan-metadata.json`. The compaction monitor readback
also returned HTTP 500; scheduled evaluation remains unverified.

The resource-safety tree remains frozen. A separate detached worktree at
`../timefusion-incident-recovery` contains the frozen source plus a new targeted
restart regression. It uses a persistent local Delta table and the production
write callback, reopens Delta and WAL before a detached receipt is retired, and
checks both deduplicated and append-only fixtures. This exercises persisted
recovery state; it does not simulate SIGKILL. The first compile found that WAL
recovery requires an Arc receiver; the fixture was corrected. The targeted run
then completed: **one passed, one failed**. The deduplicated
`otel_logs_and_spans` fixture recovered without another publication. The
append-only `variant_bench` fixture recovered its WAL entry and published it
again, producing **two durable rows instead of one**. The new regression remains
isolated and failing; it has not been merged into the release candidate.
The regression now also requires a byte-identical later append to remain a
distinct durable write after recovery; that assertion is not yet reached by the
failing append-only case. At that checkpoint, the strengthened run again passed the deduplicated case
and failed the append-only case. The later durable-publication implementation
below resolves this regression in the isolated recovery worktree. Current output is at
`/tmp/timefusion-detached-restart-identity-guard.log`. Input hashes, command
and the failed result are recorded in
`/tmp/timefusion-detached-restart-validation.json`.

The code explains the difference: detached receipts live only in the process,
and boot-time landed-content skipping deliberately applies only to tables with
deduplication keys. The conservative Delta watermark cannot retire the original
entry by itself. Applying the content digest to append-only tables would lose
legitimate identical later writes, so it is not a valid fix. Durable recovery
proof must identify the original WAL records and the committed subset, including
split entries and buckets with concurrent arrivals. A minimum/maximum cursor
range cannot establish coverage for interleaved unflushed entries. Existing
best-effort recovery is at-least-once; do not claim exactly-once recovery for
append-only tables from the passing runtime receipt tests.

The vendor recovery implementation also assigns synthetic block IDs while
scanning surviving WAL files (`wal/runtime/walrus.rs`), so a proposed durable
record identity must not assume that a bare `WalPosition.block_id` stays attached
to the same physical record after file reclamation. This is an implementation
constraint for the new proof; a cursor-recovery data-loss defect has not been
reproduced by this inspection.

The isolated recovery worktree now has typed logical batch identities encoded in
reserved Arrow IPC schema metadata, without changing the outer WAL version or
adding a user column. The identity factory always replaces a supplied identity
with a fresh UUID; row splitting retains that UUID and records each fragment's
offset and count. Dictionary flattening preserves the identity metadata too.
The first regression caught Arrow's schema-superset check rejecting replacement
of an existing identity; rebuilding the batch with identical arrays and updated
metadata fixes it. Two real IPC split/round-trip cases pass and require fragments
to tile the original write without gaps or overlap. Legacy untagged payloads
establish no identity proof.

- Identity tests: two passed; `/tmp/timefusion-wal-identity-green.log`.
- WAL regressions on the formatted inputs: 37 passed;
  `/tmp/timefusion-wal-identity-regressions.log`.
- Formatting and diff whitespace checks: passed.
- Inputs and commands: `/tmp/timefusion-wal-identity-validation.json`.

That checkpoint completed the identity transport prototype only. It had not yet
been wired into admission, buffer lineage, Delta commit metadata or startup
reconciliation, and the append-only restart regression was still failing. The
later integrations below supersede that state. The complete path must retain the
identities through coalescing and snapshot restoration, invalidate unsupported
mutation proof, persist publication intent before sending a commit, and resolve
that intent on startup before permitting another publication. The frozen main
release candidate has not changed.

Read-only Monoscope queries on project `87576849-4941-49d3-a15d-680fef88a1a8`,
one-hour windows ending approximately 2026-10-06 23:09 UTC. Each query filters
`timefusion.rollup.misses` by `attributes.reason` and uses `increase(value)`.
Windows differ by a few seconds, so these are approximate comparisons, not an
exact partition of the separately queried total of 2,149.

| Reason | Increase | Interpretation |
|---|---:|---|
| `tiny_interior` | 670 | Deliberate cost threshold: certified interior too small for a second scan. |
| `unaligned_bucket_width` | 553 | Query width is not a multiple of the rollup grain; assess requested widths before proposing finer tiers. |
| `multi_scan_source` | 240 | Multiple scans cannot be answered by one source rollup under current routing. |
| `not_built` | 198 | Coverage-building candidate; correlate project/date and policy eligibility. |
| `filter_multiple_null_guards` | 149 | Existing measures cannot express the combined population; inspect frequent predicates. |
| `filter_not_eligible` | 119 | Residual predicate constrains columns absent from declared measure filters. |
| `stale_coverage` | 104 | Maintenance freshness candidate; correlate invalidations and dependencies. |
| `unknown_filter` | 86 | Inspect concrete predicates before changing measures. |
| `unwalkable_source` | 35 | Sample is a distinct facet projection with renamed columns, not sufficient evidence of an expensive chart fallback. |

Other currently declared miss reasons returned zero increases in this window.
Approximately 1,463 misses are explained by the interior, width and multiple-scan
guards. Prioritize fresh eligible coverage and common filter populations before
broadening projection matching.

## Disk monitor validation

`timefusion.wal.filesystem_available_bytes` and
`timefusion.wal.filesystem_free_pct` query the filesystem containing the WAL
directory directly with the existing `fs4` dependency. Failed probes emit no
sample. The prepared monitor alerts below 5%, warns below 10%, and recovers at
8% and 12%; it remains disabled until deployed samples are verified.

- `cargo lint`: passed; log `/tmp/timefusion-disk-headroom-lint.log`.
- Targeted configuration tests: six passed; log
  `/tmp/timefusion-disk-headroom-config-tests.log`.
- Monitor YAML parsing and threshold ordering: passed using Ruby's YAML parser.
- `git diff --check`: passed.

These checks are not full release signoff. A master push requires
`make ci-signoff` for the exact release tree.

## Isolated admitted-write lineage integration

In the separate `timefusion-incident-recovery` worktree, admission now stamps a
fresh write UUID before WAL append, including byte-identical repeated appends.
WAL IPC row splits retain the UUID and their original row ranges. MemBuffer
extracts the reserved metadata before schema canonicalization and carries typed
ranges separately from batches, so coalescing preserves proof without exposing
internal metadata through query schemas. Snapshot/take restoration carries the
ranges; receipt retirement removes only the committed ranges. DML mutation
invalidates the old ranges, and repeated failed-snapshot restoration is
idempotent. Malformed metadata produces no publication proof and is stripped,
while unrelated schema metadata is preserved.

Thirteen targeted lineage/coalescing/detached-retirement cases and all 237
write/WAL regressions passed. Commands, input hashes and log hashes are recorded
in `/tmp/timefusion-wal-lineage-buffer-validation.json`; the isolated diff
relative to the main batch is `/tmp/timefusion-wal-lineage-recovery-only.patch`.
Those results cover the intermediate lineage implementation. The durable
publication evidence and startup consumer added later are described below;
their targeted restart tests now pass. This code is not part of the validated
main candidate.

The latest read-only root filesystem check has 498,786,304 available bytes
(about 476 MiB) and still reports 100% usage. TimeFusion remains 1/1 at the same
production digest. Evidence: `/tmp/timefusion-lineage-turn-host-headroom.txt`
and `/tmp/timefusion-lineage-turn-service-state.txt`. This is not a safe margin;
the disk recovery requirement remains open.

## Isolated durable publication and restart reconciliation

The recovery worktree now uses a typed `FlushCommitContext` alongside the
conservative watermark. Each identified flush durably writes its write UUID/row
ranges before invoking the writer. The observer synchronously persists every
attempted immutable Delta version, physical table URI and table ID before sending
its conditional log write. Staged and schema-evolution paths share this boundary;
the commit metadata carries the journal's UUID. A confirmed response checkpoints
success. If that checkpoint fails, the previously synced version intent remains
sufficient for exact-version reconciliation. A known terminal callback failure
checkpoints `Aborted`; a task panic leaves publication unresolved.

Startup reconciles intents independently of the recent-history scan. Confirmed
proof must match the resolved physical table and be visible in its refreshed
snapshot. Pending proof reads its exact versions and matches the commit UUID.
WAL replay skips only covered fragments of the same admitted write; byte-identical
later appends have new UUIDs. Missing versions or a changed physical table leave
WAL cursors unchanged and retain topic ownership. This currently fails startup
conservatively; per-topic availability during an unresolved startup publication
needs review before this recovery batch can ship. No SIGKILL test has yet proved
the actual process-crash windows; the tests reopen persisted state.

Finished intents are reclaimed only once their topic's durable cursors equal its
write tails. A replay rewind marker vetoes reclamation. Real file tests cover
reopen, failed journal writes, unobserved-version rejection, success/failure
retirement and corrupt JSON retention. Empty appends do not mint zero-row intents.

With explicit local MinIO environment settings, all 254 write/WAL/DML/recovery
regressions passed (`/tmp/timefusion-durable-flush-local-regressions.log`). Four
restart cases cover deduplicated and append-only tables, lost confirmation
checkpoints, a one-entry history scan that excludes the publication, later
identical appends and eventual intent reclamation. Two startup cases cover a
missing version and a different physical table, then resolve the original
publication and recover without another commit. Initial lint found a complex
nested-map type; a named alias preserves its type without suppressing the lint.
The final visibility-check change passed all six startup cases
(`/tmp/timefusion-durable-flush-startup-final.log`). Final `cargo lint` passed
all targets and features with the lockfile and warnings gate unchanged
(`/tmp/timefusion-durable-flush-lint-final.log`). Current source hashes and reuse
scope are recorded in `/tmp/timefusion-durable-flush-validation.json`; the
isolated diff is `/tmp/timefusion-durable-flush-recovery-only.patch`.
The earlier 254-case inputs are preserved in
`/tmp/timefusion-durable-flush-validation-before-lint-fix.json`.

The first broad run passed 251 tests and timed out in one existing batch-queue
case. The isolated worktree has no `.env`; rerunning that case with explicit
local MinIO settings passed in 0.802 seconds. The corrected broad run uses those
settings. No test timeout or test assertion was weakened.

At 02:05:33 UTC October 7, read-only host inspection again reported **zero
available bytes, 100% root usage**. TimeFusion remained 1/1 on the unchanged
production digest. Evidence: `/tmp/timefusion-durable-flush-host-headroom.txt`.
Disk recovery and the pending host read-only exception remain outstanding.


### Real SIGKILL validation and physical WAL proof retention

The isolated recovery worktree now has a real server-process regression in
`tests/suite/kill_recovery.rs`: two identical append-only writes are acknowledged,
then the process is SIGKILLed and restarted; a third identical write is admitted
and a second SIGKILL/restart must still return exactly three physical rows.
The case runs with both unflushed and immediately committed writes against local
MinIO. This supplements the persistent-state reopen tests; it does not claim to
hit the exact conditional-PUT/intent-fsync crash window deterministically.

The committed case failed twice before the fix: the second restart returned four
rows instead of three. Its pre-kill metadata contained only a cursor snapshot,
so publication evidence had already been reclaimed. The boot log showed the
third entry reconstructed with block ID 3 despite the old cursor using block ID
1. Cursor equality therefore cannot authorize deleting a stable publication
identity while the physical WAL records remain replayable.

The journal now records the physical WAL filenames present before publication.
Finished intents are retained until those files are gone and the topic is
consumed. Cleanup loads the intents before its one directory inventory, fsyncs
the WAL directory to persist observed unlinks, then removes eligible intents and
fsyncs the metadata directory. Older records without a file inventory remain
conservatively retained. The inventory includes unrelated files present at the
cut, so an older global WAL pin can delay journal cleanup too; metadata retention
and startup cost still need assessment before release.

The first fixed run passed eight focused journal, detached-restart, and actual
SIGKILL cases (`/tmp/timefusion-durable-flush-physical-retention.log`). Three existing
SIGKILL cases also passed before the fix (baseline, during-flush workload, and
memory pressure), but their deduplicated table had not exposed this duplicate.
Red evidence is in `/tmp/timefusion-durable-flush-append-sigkill.log` and
`/tmp/timefusion-durable-flush-append-sigkill-debug.log`. Broader verification of
the batched directory scan passed 259 targeted regressions, including five real
SIGKILL cases (`/tmp/timefusion-durable-flush-physical-retention-regressions.log`).
Current input hashes and limitations are recorded in
`/tmp/timefusion-durable-flush-physical-retention-validation.json`. All-target/all-feature `cargo lint` also passed
(`/tmp/timefusion-durable-flush-physical-retention-lint.log`); the checked hashes
were reverified afterwards. No recovery release or deployment is claimed. The earlier recovery validation record remains evidence for its earlier
inputs, not this new physical-retention change.


### Retained-journal startup cost and directory-sync errors

Startup now shares its explicit Delta refresh across records targeting the same
physical URI and table UUID. Each record still verifies the resolved physical
table identity and visible committed version; uncertain versions still take
exact-version reconciliation. The per-call cache does not survive a failed
startup reconciliation or suppress a refresh on the next attempt.

Two new tests use real Delta writes for eight tenants per physical table, then
measure reconciliation against the real InMemory store wrapped by ThrottledStore.
With a 100 ms GET delay, the previous implementation paid 900 ms for one table
and 1.8 s for two. Both tests failed before the change. Afterward they enforce at
most two probes per physical table (the resolver may do an initial refresh),
independent of the number of retained receipts. Red evidence:
`/tmp/timefusion-durable-flush-grouped-refresh-red.log`.

The durable atomic-write helper now returns parent-directory open/fsync failures,
instead of silently accepting them. A durable intent write that fails this step
cannot authorize the subsequent Delta request. This also preserves the existing
rewind-marker durability contract. The successful sync path and existing local
I/O-failure regressions ran; this validation did not inject a directory fsync
failure into the OS.

All 261 targeted regressions and all-target/all-feature `cargo lint` passed on
unchanged checked source hashes. Commands, logs and hashes:
`/tmp/timefusion-durable-flush-grouped-recovery-validation.json`. This supersedes
the 259-test input record for the isolated recovery tree. The main resource batch
remains unchanged, and neither batch has been published or deployed.

The compaction monitor GET was retried and again returned HTTP 500. The JSON
body was empty; it is not evidence that the saved rule or scheduled evaluation
passed. Error evidence:
`/tmp/timefusion-compaction-monitor-latest-readback-error.json`.


### Cached dashboard chart routing through nested common expressions

A separate detached worktree, `../timefusion-incident-rollups`, now contains a
surgical matcher change in `src/rollup.rs`. It keeps the first resource-safety
batch and the durable-recovery worktree unchanged.

Literal SQL and directly bound Utf8/Utf8View values already routed. The actual
failure required the plan-cache lifecycle: optimize a placeholder template,
bind its parameters, then optimize the resulting plan. That creates two
adjacent common-expression projections. The old matcher expanded only the top
alias, leaving a reference to the lower alias and declining the aggregate.
Both preoptimized string cases failed before the change, with the same nested
cast/CASE projection shape found in the production logs. Red evidence:
`/tmp/timefusion-preoptimized-nested-chart-rollup-red.log`.

The matcher now walks only rename-free column projections and DataFusion's
common-expression aliases, expanding each layer against its own input. It still
refuses other computed projections, unresolved common-expression references,
and changes to output field names or types. Rollup coverage, generation,
measure availability and timestamp-grain gates remain intact.

Five parameterized cases exercise string representation and cached/uncached
plans against populated real MemTables. The rollup states are computed from the
same source data. Their rewritten and raw outputs match, including null status
values, repeated events, another tenant, the alternate server-name predicate,
and events outside the time window. A cached 30-second chart explicitly
continues to decline with `PartialBucket`; the observed subminute production
samples are not claimed as newly served. Supported one-minute cached charts now
route to the existing endpoint tier without adding another rollup tier.

All 233 rollup tests and all-target/all-feature `cargo lint` passed. Checked
source hashes were unchanged after both checks. Inputs and verification scope:
`/tmp/timefusion-nested-chart-rollup-validation.json`; isolated patch relative to
the frozen main tree: `/tmp/timefusion-nested-chart-rollup-only.patch`.
No production latency benefit or release validation is claimed yet.

The latest read-only disk check at 02:32:43 UTC still reports zero available
bytes on the production root filesystem. Evidence:
`/tmp/timefusion-rollup-work-host-headroom.txt`. The pending disk-cleanup approval
has not been answered and no host mutation has been made.


### Detached publication keeps exclusive WAL ownership

The CLI now shares its existing `WalDirLock` with the buffered layer. Each
spawned Delta callback retains that guard until it finishes or unwinds. Dropping
the flush caller or the layer cannot release ownership while the old client
could still publish or retry. The lock is not stolen and process death still
releases it through the kernel. Embedded callers retain their existing external
ownership contract; this change wires the production CLI explicitly.

The existing four real-Delta restart cases now exercise that cut: a publication
lands, its callback is held active, the old layer is dropped, and a replacement
tries to acquire the same real WAL lock. All four failed without task-level lock
retention. With retention, the replacement publishes a takeover request, stays
blocked until the callback is released, then acquires ownership and reconciles
without duplicate publication. Cases include append-only and deduplicated
storage, with both confirmed and lost-confirmation journal states.
Red evidence: `/tmp/timefusion-detached-wal-owner-red.log`.

The broader 261 targeted regressions passed (one passing leaky case), including
the five real server SIGKILL cases. All-target/all-feature `cargo lint` also
passed, and source hashes were unchanged after both checks. Current checked inputs and
limits: `/tmp/timefusion-detached-wal-owner-validation.json`.

An exact-payload recovery path is still under investigation, not implemented.
The pinned S3 adapter uses `DefaultLogStore` with conditional PUT bytes. Recording
those exact bytes before publication could allow a replacement to resume the
same immutable version, instead of treating an absent attempted version as a
permanent startup block. That requires verifying backend compatibility,
referenced staged files, and the no-live-predecessor condition. The ownership fix
above establishes that prerequisite for CLI handoff; it does not by itself make
unknown startup commits recoverable. No journal format, resubmission behavior,
production image or deployment was changed in this turn.

### Exact conditional commit payloads survive restart

The isolated recovery worktree now records each conditional Delta commit's exact
UTF-8 JSON payload in the same atomic, fsynced journal update as its attempted
version, before forwarding the request. Existing versions cannot acquire different
bytes. Invalid UTF-8 and malformed Delta actions fail before publication; failed
persistence does not update the in-memory record. Temporary-file backends and
legacy journals retain conservative version-only evidence. Older binaries reject
the added journal field rather than silently skipping recovery, so rollback must
account for the pending journal format.

Four existing real-Delta detached-restart cases now compare reopened journal
payloads byte-for-byte with the actual published commit, covering append-only and
deduplicated writes with confirmed and lost confirmations. The two journal lifecycle
cases also cover Unicode/newline preservation, immutable payloads, malformed input,
and failed persistence. Ten focused tests passed (one passing leaky case).
The broader WAL/recovery/SIGKILL run passed 260 cases but failed
`resumable_replay_after_crash_skips_drained_prefix`: its simulated-crash hook was
not reached. That test passed on isolated rerun. Its hook is checked during the
iterator loop, while a final outstanding drain is awaited after the loop, making
late drain completion a plausible timing-sensitive explanation; this has not yet
been fixed or deterministically reproduced. The broad run is **not** recorded as
green. All-target/all-feature `cargo lint` passed. Checked source hashes remained
unchanged; logs and exact results are in
`/tmp/timefusion-exact-commit-payload-validation.json`.

No startup resubmission is enabled. Missing attempted versions still preserve WAL
and stop recovery conservatively. The next implementation step must validate the
physical table/backend, exclusive ownership, referenced staged files, and original
flush identity before resuming only the same conditional version with these exact
bytes. Retained payload metadata memory and startup cost still need assessment.
The frozen main resource batch was not expanded, and nothing was published or
deployed.

### Startup resumes the original conditional version

The isolated recovery batch can now resume a missing immutable Delta version with
its already-durable exact payload. It reads every attempted version for an existing
publication before considering any resubmission. Resumption requires an attached
exclusive lock for the actual canonical WAL directory, a recorded and current
`DefaultLogStore` conditional-PUT backend, the immediately next visible version,
the original flush UUID, and every staged Add file present with its recorded size.
Adds with deletion vectors decline until their external objects can be verified.
The recovery PUT has no OCC loop and never advances to another version. Every
response, including an error, is followed by checking the immutable version; an
own commit refreshes the snapshot before WAL replay can skip its stable identities.
Missing or unverified outcomes retain WAL and startup ownership conservatively.
Legacy version-only journals and unsupported backends remain conservative failures.

Eleven real-Delta persistent-state cuts cover append-only and deduplicated success,
missing/foreign WAL locks, missing or changed files, backend/identity mismatch,
legacy evidence, a future version gap, and another writer owning the original
version. Positive cases compare the published JSON bytes exactly and count raw
physical rows; negative cases inspect the actual remote log for no publication
and verify WAL cursors remain unchanged. These reconstruct a prepublication cut
by retaining real staged Parquet while removing the original commit entry, rather
than claiming a deterministic process kill during that window.

The first broader run exposed two manually created fixtures without backend
identity; they now record their known backend. A separate legacy case verifies
that genuinely absent evidence remains blocked. A batch-queue case timed out in
two runs; diagnostic investigation found this isolated worktree has no `.env`,
and the harness-only `TIMEFUSION_TEST_S3_ENDPOINT` does not configure the fixture's
`Database::new`. Final validation explicitly supplied the local AWS S3 endpoint,
HTTP allowance, region, and MinIO credentials. All **272** targeted regressions
then passed (one passing leaky case), including five real SIGKILL cases. All-target,
all-feature `cargo lint` passed. Checked source hashes remained unchanged.
Exact commands, earlier failed runs and current evidence are recorded in
`/tmp/timefusion-same-version-flush-recovery-validation.json`.

A new read-only production `df` check still reports zero available bytes on root,
with 1,791,782,420,480 bytes used:
`/tmp/timefusion-same-version-recovery-host-headroom.txt`.
No host mutations, image publication, master push, or deployment occurred. The
main frozen resource-safety batch remains separate. Next recovery validation must
exercise response loss and a real race at this resumed conditional PUT, assess
retained payload metadata cost, and complete the recovery release checks before
publication. Embedded callers without attached WAL ownership remain conservative.

### Real S3 publication faults validate startup resumption

Four new cases use `AmazonS3Builder` against local MinIO through a TCP fault
proxy. The original conditional version-one request is captured only after its
real journal intent is fsynced; its writer task is cancelled and its WAL ownership
is released before a replacement acquires that same directory. This exercises
actual S3 request cancellation and delayed server requests, rather than manually
reconstructing the journal state. It is still task cancellation, not an exact-window
SIGKILL; five separate actual process-kill cases remain covered by the prior run.

The proxy forwards actual MinIO storage semantics. Cases cover:
- A resumed PUT accepted by MinIO while its response is withheld: the client's
  real response deadline fires, and recovery confirms the immutable version.
- The old server-side request arriving after the replacement's negative probe:
  the original wins, the resumed PUT receives HTTP 412, and its own UUID confirms
  one committed copy without version advancement.
- A foreign writer committing after the replacement's negative probe: the resumed
  PUT receives HTTP 412, recognizes the foreign commit, and WAL replay later makes
  a fresh safe commit of the acknowledged row.
- A resumed request withheld until its client times out: an absent version retains
  cursors, airborne ownership and the replay prohibition; the same request then
  arrives late and is confirmed on the next recovery pass.

All cases compare the captured original and resumed payloads exactly, assert that
resumption sends one conditional attempt and publishes no next version, and count
one raw physical row after recovery. All four passed in 2.018 seconds. All-target,
all-feature `cargo lint` passed. The initial fixture run failed because replacing
S3 client options reset its HTTP allowance; the test builder now applies the
explicit local HTTP allowance after those options. No production code changed.

The prior 272 regressions are reused: all production, dependency, schema and config
hashes match. The prior test source was reconstructed from its saved patch, its
validated hash checked, and its contents compared with the current file after
removing only the newly added fault cases and their helper. Existing cases are
unchanged. Evidence and current checked inputs:
`/tmp/timefusion-flush-recovery-s3-publication-faults-validation.json`.

Retained journal memory/startup cost and full recovery release checks remain
unfinished. No production storage writes, host mutations, image publication,
master push or deployment occurred. The main resource-safety batch remains frozen.

### Stream retained publication journals and finish late replay drains

Recovery now snapshots journal filenames and decodes one payload at a time.
Startup validates every record before publication, holds affected topics, then
reopens each record for reconciliation. Reclamation uses the same lazy reader
and tolerates a proof already removed by another concurrent reclamation pass;
startup still fails closed on missing or corrupt records. Payload memory scales
with the largest record, plus filenames, topics and retained replay identities.
This is not an RSS benchmark or a constant-total-memory claim. Startup performs
two validation passes, and disk-resident proofs remain until physical WAL files
are reclaimed. Prepared-intent retirement still needs review.

Two real-filesystem cases, with one and 64 one-MiB journal payloads, prove the
reader captures names without preloading their contents: corruption after reader
creation is observed on iteration, and a later intent is excluded from that
snapshot. Existing physical-proof retention guards remain covered.

The broader run reproduced an existing replay crash-hook failure: a relief drain
finishing after iterator exhaustion bypassed the rewind-marker update and test
hook. Both completion paths now share that handling before cursor parking. An
initial helper type mismatch was corrected. Final targeted replay/journal tests
passed **33/33**; the broader write/recovery run passed **278/278**, including the
real S3 publication fault and actual SIGKILL cases. `cargo lint` passed. Exact
source hashes, commands, logs and earlier failures are recorded in
`/tmp/timefusion-streaming-flush-journal-final-validation.json`. The isolated
recovery patch is refreshed at `/tmp/timefusion-durable-flush-recovery-only.patch`.

Full recovery release checks and deployment verification remain unfinished. No
production storage writes, host mutations, image publication, master push or
deployment occurred. The main resource-safety batch remains frozen. Production
headroom remains unresolved pending the previously requested disposable-file
confirmation under the host read-only rule.

### Reconcile an accepted S3 request before Delta conflict retries

A new real-MinIO fault case exposed an additional publication boundary bug:
MinIO accepted version one, the proxy returned HTTP 503 instead of its successful
response, and the S3 client retried the same conditional request. HTTP 412 from
that retry was treated as a foreign Delta conflict, and the OCC loop published
version two. The local assertion proves extra log publication; it does not prove
this sequence caused the earlier production WAL growth or produced extra physical
rows in production.

The flush publication observer now reconciles an occupied immutable version when
it has a durable journal and exact original commit bytes. Equal bytes confirm
success before the OCC loop advances. A verified foreign payload retains normal
conflict handling. An absent or unreadable version carries the existing typed
uncertain-publication marker into the outer reconciliation path, outside the
commit lock. A second red case showed that a temporarily absent GET response
otherwise allowed version advancement; it is now covered by the fix.

Eight real S3 fault cases cover the original accepted-request retry, schema-change
fallback, a genuine foreign conflict, temporary probe absence, and the four prior
startup-resumption races. MinIO retains the actual immutable storage semantics;
the proxy injects request delays and HTTP response faults. Fifteen focused
fault/transport/deadline cases passed (one passing leaky case), followed by **289**
passing write/recovery regressions, including actual SIGKILL cases. Exact checked
inputs, commands, logs and both red reproductions are recorded in
`/tmp/timefusion-accepted-commit-retry-validation.json`.

The recovery batch is now frozen for local release validation. Format and Clippy
checks are running through the repository CI wrapper; full test, PG smoke and e2e
checks remain to be completed. The isolated recovery patch has been refreshed and
now also includes `src/database/mod.rs` for typed uncertainty propagation. No
production host/storage mutation, registry image publication, master push or
deployment occurred. Production headroom remains unresolved.


Recovery release validation checkpoint: format and Clippy passed through
`./scripts/ci/ci.sh local fmt clippy` (terminal exit zero; Clippy 88 seconds).
Their exact fingerprints are stored in the accepted-commit validation record.
`./scripts/ci/ci.sh local test pg-smoke e2e` has started as session **15183**,
with output at `/tmp/timefusion-recovery-release-runtime-checks.log`. That run
remains pending and must be resumed rather than restarted. Checked source hashes
were unchanged at the transition between the checks.


Read-only production observation during recovery release checks: root still has
zero available bytes (1,792,164,810,752 bytes used). WAL is now
106,954,755,772 bytes (99.6 GiB), across 104 files; completed flushes increased to
14,855, while failed flushes remain two. Actual memory-buffer rows are 1,281,
versus the deployed cumulative `rows_in_buffer_lag` value 38,434,607. The deployed
image digest and boot counter are unchanged. These snapshots show flushing and
reclamation progress, not stabilized WAL growth or restored headroom. Exact
observation evidence is at `/tmp/timefusion-recovery-release-live-observation.json`.
A compaction monitor readback again returned HTTP 500; its current configuration
and scheduled evaluation remain unverified. No production mutations occurred.


### Release e2e failures expose a repair cleanup ordering bug

The first full recovery release run passed **2,702 nextest tests**, **16 doctests**,
and PG smoke, then failed the e2e gate: **67 passed, two failed, two skipped**.
The wrapper terminated with exit one. Those four passing gates are preserved as
previous-input evidence in the accepted-commit validation record; they are not
claimed for the corrected production tree.

One failure expected committed WAL rows to re-enter memory. The end-to-end test
now covers both publication receipts and the legacy history-digest path: receipts
skip replay, while deliberately removing local receipts reconstructs the prior
boundary without changing acknowledged WAL bytes or actual Delta history. Both
paths assert no new Delta version, exactly five physical rows with known record
counts, and five query-visible rows. The existing legacy assertions are retained
rather than simply weakening the test to accept either outcome.

The second failure was a real ordering bug. Durable WAL publication recovery
resolves tables earlier during bootstrap; the after-readiness orphan sweep can
now see and delete an aged staged repair before the repair pass adopts it.
Cleanup now retains entries the existing repair resume classifier considers
eligible, when repair resumption is enabled. Row-count inspection is restricted
to the recorded input/output paths, uses logical counts for deletion-vector
inputs, and leaves stale-output reclamation and the kill switch intact. The
cleanup log reports the retained count. It does not publish repair work itself;
the existing resume path still verifies staged objects and commit-time identities.
Rollup staging cleanup is unchanged.

All **five** focused real e2e cases passed: retained repair staging, stale twins,
kill switch, receipt-based recovery, and legacy history-based recovery. The initial
manual command lacked the required e2e feature, and a physical-row assertion's
integer type was corrected; final validation used `--features e2e` and fails on
missing record-count evidence. Current checked inputs and log evidence are in
`/tmp/timefusion-recovery-repair-gate-fix-validation.json`. The isolated patch now
includes eleven files, adding the cleanup change and e2e assertions.

The corrected batch is frozen. Format and Clippy are running as session **40593**
at `/tmp/timefusion-recovery-repair-gate-fix-fmt-clippy.log`; full runtime release
checks must follow for these changed inputs. No production mutation, image
publication, master push or deployment occurred. Host headroom remains unresolved.


Corrected-batch checkpoint: format and Clippy terminated successfully (exit zero;
Clippy 15 seconds). Current source hashes were unchanged. Full test, PG smoke
and e2e checks are now running as session **62621**, with output at
`/tmp/timefusion-recovery-repair-gate-fix-runtime-checks.log`. Resume that handle;
do not restart a live run. Runtime checks for this corrected tree are pending.


Corrected recovery release gates are now complete: **2,702** nextest tests and
**16** doctests passed, PG smoke passed, and all **70** e2e tests passed (two
skipped). Both CI wrapper calls terminated with exit zero. Source hashes remained
unchanged. Exact fingerprints and logs are recorded in
`/tmp/timefusion-recovery-repair-gate-fix-validation.json`.

The Linux amd64 production image is being built and probed locally with
`python3 scripts/production-image.py prebuild`, session **60003**, output at
`/tmp/timefusion-recovery-repair-gate-fix-image-prebuild.log`. Its frozen input
fingerprint is `4181ae8696e7aee539645905427cff54e425dcf195d0742fca5f88a1e15babce`.
This invokes no image push or deployment. The image probe remains pending.

The oldest copied future task's early deadline is consistent with the current
invalidation policy: a touched slice uses an observation-time quiet-period deadline,
while an untouched slice floors that deadline at its slice end. Its October 1
11:54:30 UTC deadline maps to an October 1 observation in the interval
(11:39:00, 11:39:30] UTC under the fifteen-minute/30-second rounding rule, despite
its October 9 11:00–12:00 UTC slice. This is an inference from the captured state
and current code, not proof of the historical minting caller. Derived claims also
require dependency coverage, so deadline age alone does not establish scheduler
starvation. Evidence: `/tmp/timefusion-future-maintenance-deadline-explanation.json`.


### Validated recovery changes promoted into the main workspace

The eleven-file recovery patch has been applied to the main TimeFusion checkout.
Before application, every affected main file was checked against the recorded
pre-promotion hash and every recovery file against the validated candidate hash.
The patch passed `git apply --check`; afterwards all tracked files in the main
and recovery checkouts matched exactly. Existing main changes were preserved.
Evidence: `/tmp/timefusion-recovery-candidate-main-promotion.json`.

All five CI fingerprints computed in the main checkout match the passing recovery
checks exactly, and its production image fingerprint matches the in-progress
Linux build (`4181ae8696e7aee539645905427cff54e425dcf195d0742fca5f88a1e15babce`).
The checked source hashes also match. Those valid results are reused; no redundant
Cargo checks were started. The production image probe is still running as session
60003. No commit, master push, image publication or deployment occurred. The
separate nested-CSE rollup candidate remains outside this first resource batch.


The local Linux amd64 image build and real PGWire probe have now terminated
successfully (exit zero). The server stayed running, and the probe authenticated
in 25 ms. Docker inspection confirms the expected source-fingerprint label and
Linux amd64 platform. Local image ID:
`sha256:00c5530144671bdfe8536e55dfa2e697b49a6cc88b554dce26fd4e1d1163fd9c`.
This is a local image ID, not a published registry manifest digest. Inspection
and exact validation evidence are recorded in the corrected-batch validation
record. No registry image push, `make ci-signoff`, master push or deployment occurred.

The separate rollup worktree has been refreshed onto this validated recovery
base by applying the same eleven-file patch. Its routing source hash was unchanged,
and all tracked files now match the main candidate except `src/rollup.rs`. Prior
233 targeted routing results remain earlier-base evidence; integrated release
gates for this refreshed routing candidate are still required. Evidence:
`/tmp/timefusion-rollup-refreshed-base-validation.json`. The first resource/recovery
candidate remains frozen and fully checked locally.


The refreshed, separate rollup candidate is frozen for integrated release checks.
Format and Clippy are running as session **2444**, output at
`/tmp/timefusion-rollup-refreshed-base-fmt-clippy.log`. Checked source hashes and
that live handle are stored in the refreshed-base validation record. The main
resource/recovery candidate and its completed image probe remain unchanged.


## Refreshed rollup validation and numeric fix readback

The separate routing candidate passed format and Clippy (1s and 89s), with
all 51 recorded source hashes unchanged. Runtime release checks (`test`,
`pg-smoke`, `e2e`) are running sequentially against local MinIO as session
32515; output: `/tmp/timefusion-rollup-refreshed-base-runtime-checks.log`.
The validation record contains the two passing fingerprints and live handle.

Monoscope PR [#652](https://github.com/monoscope-tech/monoscope/pull/652)
is merged and includes the numeric empty-predicate fix in `90fbc00bc`.
The clean sibling checkout is at `e04de2320`. The successful GitHub gate
reused exact-input attestations, including build, doctests, unit tests and
integration tests; evidence: `/tmp/timefusion-monoscope-652-gate.log`.
An ancillary Claude review job failed; the PR gate and release regression
jobs succeeded. No new sibling source edits or builds were needed.
Deployment of this fix and the actual incident SQL parameters remain unverified.

A fresh compaction-monitor readback still returns HTTP 500. No saved
configuration or scheduled evaluation success is claimed from this response.


A read-only host inspection now confirms the numeric fix is in the running
Monoscope service image (3/3 replicas). The immutable service digest is
`sha256:92bf3f073eb90b1e9f8b40d3a9eca129a08aa469dab3870a432561c9dfdc5b80`.
Its revision label is `908cbb9c206968763ac94c2d9e707da65026f1c9`; the local
git tree exactly matches the image's source-tree label, and that revision's
`shared/src/Pkg/Parser/Expr.hs` contains both corrected text comparisons.
Evidence: `/tmp/timefusion-numeric-fix-deployed-verification.json`. This
rollout was external; no deployment was performed by this investigation.
Actual original parameters and post-rollout recurrence remain unverified.
The fresh root filesystem check still reports zero available bytes.


A fresh read-only TimeFusion service-log scan examined 32,821 lines from the
last hour. It found 8,952 lines containing `ENOSPC` or `No space left on device`;
the latest matching timestamp was `2026-10-07T04:33:32.779990587Z`.
The regex scan found zero numeric-cast, sort-pool-exhaustion or detached-flush
failure matches. These are line counts, not deduplicated incidents, and
absence in a one-hour host-log window does not prove remediation or justify
closing historical issues. Evidence:
`/tmp/timefusion-post-rollout-host-log-recurrence.json`. The schema API also
returned HTTP 500, preventing the stronger telemetry recurrence readback.
No host data or service state was changed.


## Oldest overdue task: required parent evidence

Replaying task updates from the copied maintenance snapshot and WAL yields
167,222 task records. The oldest due derived task requires
`otel_logs_and_spans_rollup_dashboard_1m_v4` (the schema's `derive_from`).
For its October 9 11:00–12:00 UTC slice, the required parent has six overlapping
ten-minute tasks: five pending and one complete (11:10–11:20). The pending
parent deadlines are October 9 11:25, 11:45, 11:55, 12:05 and 12:15 UTC.
No contiguous completed parent coverage exists in the copied journal.
An overlapping completed sessions rollup is a different physical table and
cannot satisfy this dependency.

The current scheduler indexes Pending/Retry/Running base units independently
of their deadline; `dependencies_complete` refuses a derived unit when an
overlapping required parent remains in that claimable index. Therefore this
copied task state is consistent with a dependency block despite the derived
unit's October 1 deadline. This is a code-and-archive inference, not a live
scheduler trace: runtime `base_tier_ready` is not persisted, and the archive
may have changed since copying. Do not increase concurrency or delete these
units based on overdue age alone. Evidence:
`/tmp/timefusion-oldest-maintenance-parent-evidence.json`.

The copied rollup policy allows the required dashboard parent from
`1787961600000000` micros onward, before the October 9 slice, so that copied
policy does not itself exclude the five pending parent units. Source-cursor
WAL updates do not change task identity or state; the archive replay encountered
no task-removal or rollup-policy update records.


The refreshed separate rollup candidate now passed the main runtime gate:
2,708 nextest tests (15 skipped), all 16 doctests, and PGWire smoke (8s).
The wrapper continues with 70 end-to-end tests (2 skipped). Format and
Clippy already passed; full end-to-end completion is not yet claimed.
All 51 recorded source hashes still match the frozen candidate.
The preserved passing prefix is
`/tmp/timefusion-rollup-refreshed-base-runtime-verified-prefix.log`; test and
PG smoke fingerprints are recorded in the refreshed-base validation JSON.


The refreshed separate rollup candidate's runtime wrapper terminated with
exit zero. All five local gates are green: format, Clippy, 2,708 main
nextest tests plus 16 doctests, PGWire smoke, and all 70 e2e tests (2 skipped).
The post-run source hashes match all 51 frozen inputs. Final gate
fingerprints, log hash and terminal status are in
`/tmp/timefusion-rollup-refreshed-base-validation.json`.
No routing change was promoted into the first resource/recovery batch, and
no registry image publication, master push or production deployment occurred.


The refreshed routing candidate's local Linux image build and PGWire probe
are running as session 11133, log:
`/tmp/timefusion-rollup-refreshed-base-image-prebuild.log`. The command is
`python3 scripts/production-image.py prebuild`; it prepares and probes a local
image without registry publication or deployment.


The full-scope acceptance audit is recorded in
`/tmp/timefusion-incident-full-scope-acceptance-audit.json`: all eight incident
followups and all four retry invariants are retained. The four retry invariants
are locally validated; production rollout is outstanding. No original
requirement is marked complete from local tests alone. Reinspection of the
warm flush path confirms uncertain publication stays inside the reconciliation
loop until its original immutable version has an authoritative outcome; the
terminal-error watcher cannot re-drive that callback while it is probing.
The broader candidate's Linux build remains live as session 11133.


The separate routing candidate's Linux amd64 image build and real PGWire
probe terminated successfully (exit zero); authentication took 24ms and
the server stayed running. The image label matches source fingerprint
`e41391f4c9103b747db31ef2f28797cf7650e5f6704f24ec24d747d5958063cc`.
Local image ID:
`sha256:a94569f5da13d35c0f5ea96e9b072dada6af2263d4a218a140a85f858915a975`.
The validated routing-only patch relative to the unchanged first candidate is
`/tmp/timefusion-validated-rollup-routing-only.patch`; it is prepared for
review and remains outside main. Both candidates have passing local release
gates and Linux image probes. No source push, image publication or production
mutation occurred. Full-scope production acceptance remains unresolved.


## Recovery performed by the operator

The operator confirmed deleting ballast files. TimescaleDB logs at
07:08:22–07:08:39 UTC show repeated FATAL failures to write `postmaster.pid`
with `No space left on device`. At 07:08:46 UTC its replacement became ready
to accept connections. The original project's authenticated schema and
monitor reads now succeed. No host mutation was performed by this agent.
Evidence: `/tmp/monoscope-outage-database-recovery-readback.json` and
`/tmp/monoscope-outage-original-project-monitor.json`.

The CLI default project had changed externally to
`877fc81e-a319-4b5a-a4bd-49a80fc22c1d`; the first post-recovery monitor GET
returned 404 in that project. Explicitly using the original project
`87576849-4941-49d3-a15d-680fef88a1a8` succeeds. This does not explain the
earlier 500 failures caused while the database was unavailable.

Disk headroom remains unsafe: available bytes fell from 21,933,621,248 to
17,275,215,872 during recovery checks. Eight 10GiB ballast files (01–08)
remain in the root-owned disk-reserve directory, outside both TimeFusion's
and TimescaleDB's data mounts. A concrete exception request to remove those
eight remaining files is pending.

The compaction monitor readback confirms the reset-aware increase query,
threshold 5000, 60-minute window, and a successful scheduled evaluation at
07:08:53 UTC with normal status. A zero reading just after database recovery
is insufficient to establish complete recent telemetry coverage. Other
monitor configurations also read back; recurrence-based issue closure and
production TimeFusion rollout remain outstanding.


## Authorized ballast cleanup and required restoration

The user explicitly authorized removing the remaining ballast, conditional
on recreating it after TimeFusion releases its retained disk. During preflight,
files 05–08 disappeared concurrently. The agent revalidated and removed only
the four remaining regular, root-owned, fully allocated 10GiB files 01–04,
then fsynced the directory. No WAL, database files, volumes or service state
were changed. Evidence: `/tmp/timefusion-authorized-ballast-removal-result.json`.

Before this deletion the filesystem already had 863,161,761,792 free bytes;
a much larger concurrent recovery had occurred externally. After deleting
40GiB it reported 906,111,483,904 free bytes, then 901,371,891,712 in the
readback (50% usage). The cause of the concurrent release is unverified.
An unprivileged `du` failed with permission denied; its 4096-byte output is
NOT a valid TimeFusion directory-size measurement.

Read-only process stats show WAL 79,691,779,772 bytes across 77 files, down
from the earlier 106,954,755,772 bytes/104 files. Boot identity is unchanged,
completed flushes 14,869, failures 2, orphaned topics 0 and drained=false.
This proves ongoing reclamation, not that all retained WAL is released.
The ballast restoration is an outstanding acceptance requirement. Exact
paths, sizes and metadata are recorded in
`/tmp/timefusion-ballast-restoration-followup.json`; do not overwrite existing
files or allocate ballast while the retention problem remains unresolved.
A conservative restoration trigger is WAL below its 60GiB alert threshold,
stable reclamation, and at least 10% filesystem headroom AFTER restoration.
