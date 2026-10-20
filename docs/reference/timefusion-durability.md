# Durability, recovery, and operational lessons

Reviewed: 2026-10-20. This reference synthesizes the August–October incident and write-path plans through revision `ad1ee10b`.
Historical observations and local test results do not establish current production acceptance.
The [current backlog](../plans/2026-10-19-next-work.md) owns unresolved recovery and operational checks.
The [review inventory](../reviews/2026-10-19-timefusion-triage.md#plan-inventory) identifies the source notes.

## Accepted writes and ownership

INSERT acknowledgement follows durable WAL append and successful buffered insertion.
A flush is not complete until its Delta outcome is authoritative.
MemBuffer snapshots, WAL cursor holds, staged files, and publication receipts represent different parts of that ownership.
Removing one does not imply the others are safe to release.

A timed-out callback can still commit later.
A request error does not prove the object-store operation failed.
Do not release WAL pins, restore an independently mutable prefix, or start another publication merely because the caller stopped waiting.
Other topics can progress while one topic reconciles an uncertain commit.

## Snapshot restoration and late writes

The flush path snapshots a prefix without consuming later writes.
After an uncertain callback, restoration must retain the prefix and its coalescing fence.
Otherwise, later writes can cross the coalescing threshold and disappear when the original receipt retires.
The October regression reproduced seventeen late writes collapsing to one surviving late row before the correction.

Synchronize restored holds and prefix state with the batch lock.
A successful receipt drains only its original source identities and snapshot prefix.
A known terminal failure can release the fence and schedule a fresh retry.
An airborne or confirmed-but-not-retired commit still owns its prefix.
Mutation-generation checks protect concurrent DML from a stale snapshot drain.

## Authoritative conditional publication

Persist the exact commit version, immutable payload, commit identity, target lineage, and publication boundary needed for recovery.
WAL watermark, landed digest, and publication identity must survive metadata-map replacement together.

Before publication begins, an ordinary staging failure can retry normally.
After the conditional PUT begins, reconcile that exact original version:

- The expected identity at that version establishes self-landing.
- Another writer at that immutable version establishes that the original conditional write cannot land there.
- An absent version after response loss remains uncertain.

A delayed accepted request must not race a second callback into a different Delta version.
Probe outside the commit lock with bounded requests and backoff.
Keep uploaded output and the intent on an ambiguous outcome.
An alarm deadline does not authorize cancellation of uncertain durable work.

The real MinIO fault cases included an accepted PUT followed by HTTP 503/412 and a process kill before reconciliation.
Startup must resume the original payload/version and preserve physical WAL proof until the outcome and retirement are durable.
Do not infer proof from rows that merely happen to look equal after replay.

## Retry and startup recovery

Per-topic backoff avoids a stalled topic blocking every other topic.
The historical retry ladder grew approximately 5, 10, 20, 40, then 60 seconds with jitter.
Individual storage requests remain bounded even when publication reconciliation must continue.
The exact deployed configuration still needs inspection before applying these values operationally.

Retained publication journals can become a startup memory and latency problem.
Snapshot filenames, stream one payload at a time, and preserve directory-sync errors rather than silently declaring durable progress.
Late replay drains need an explicit finish path when their original publication becomes confirmed.
Shutdown snapshots are an optimization over durable replay, not a replacement for WAL correctness.
A missing clean snapshot after a short test grace does not establish data loss.

Recovery validation must cover:

- Timeout followed by late success or known terminal failure.
- Concurrent late writes, coalescing, and snapshot restoration.
- Accepted request followed by response loss and immediate process death.
- Restart before and after publication receipt persistence and source retirement.
- Independent progress for unaffected topics.
- Real physical WAL proof retention and eventual allocated-byte reclamation.

`75455c49` contains the main resource-safety/recovery batch.
The October 9 read record used that image as its deployed baseline.
Its earlier local test counts and source fingerprints apply only to their checked inputs.
They do not prove that every later production failure recovered correctly.

## Duplicate producers and prevention

WAL replay after an unclean exit can recreate rows already committed to Delta.
Landed-skip uses durable commit identities to decline a provably redundant flush.
The historical sample attributed 58% of duplicate groups in one sampled file to this producer.
That percentage is not a fleet-wide invariant.
The source default is true. A clean deploy does not exercise dirty-boot history loading.

Client retries are a separate producer.
TimeFusion stamps its version tiebreak, so a retry can receive a newer stamp despite identical client content.
A comparison against the incoming stamped version cannot establish retry identity.
The ingest-dedup design therefore compares dedup-key identity and canonical client-visible content, excluding the TimeFusion-owned stamp.

The historical design used two bounded epochs with a 256 MiB per-table cap and a six-hour window.
Populate durable identities only after confirmed commit, including confirmed landed-skip output.
Canonical serialization needs length prefixes and complete schema coverage.
A hash collision or probabilistic false positive must not silently discard a unique row.
A bounded cache miss can permit a duplicate for the existing dedup path to resolve.
Do not equate “exact equality over stored hashes” with a mathematical proof that collisions cannot occur.

No-op UPDATE suppression drops assignments that do not change values.
Eager retraction removes buffered predecessor versions after a stamped append.
They reduce different producers and neither replaces durable retry identity or read-time winner selection.
The September 16 retraction reading reached approximately 95%; remaining physical maintenance work still needed measurement.

## Future timestamps and retention

Future-dated rows can create invalidation tasks whose source interval has not closed or flushed.
Clamp future invalidation scheduling without silently changing the stored event timestamp.
Distinguish event-time retention, routing spans, WAL durability, and maintenance eligibility.
A future derived slice with an old deadline can still be correctly blocked on its required parent.
Retention or task cleanup must not remove source proof for an unresolved accepted write.

## WAL files, handles, and GC

A file removed from the directory can still consume disk through an open descriptor.
Logical WAL size, allocated directory bytes, and deleted-open bytes therefore need separate diagnostics.
The October 7 incident recorded about 920 GB pinned by deleted-open handles.
Closing handles on segment retirement addresses a different failure from incorrect cursor retention.

Age-based GC also encountered an idle writer retaining a block in a deleted segment.
The writer must roll away before GC removes that segment.
Segment mtime is not a stable age when a slow writer keeps refreshing an old segment.
`9168baa3` rolled writers by segment birth time after the earlier idle-roll fix.
Never remove unflushed WAL to satisfy a disk threshold.

WAL replay allocation bounds and append chunking defend against both corrupted frames and ordinary huge entries.
Limits must apply before allocation. A resource ceiling does not justify truncating a durable logical write.

## Storage layout and resource accounting

Keep durable WAL and journals on durable storage.
Cache, index scratch, and spill are reconstructible but still require bounded allocation and cleanup.
The September storage work moved the large Foyer cache from the durable RAID1 volume to ephemeral RAID0.
That was an executed migration, not a future task to repeat.

Container overlay writes from index scratch and unmeasured spill once obscured the source of disk traffic.
RAID mirroring also multiplies physical-device writes.
Directory growth undercounts temporary files created and deleted between samples.
A process I/O counter can include setup, buffered writeback, or overlapping phases.
Attribute bytes by channel and time window before naming a culprit.

The October 19 read-only snapshot reported root usage of 54% with approximately 776 GiB available.
It supersedes the old full-disk snapshot as current evidence for that review.
It does not identify the cause of a concurrent external disk release or close every WAL acceptance gate.

The incident's ballast cleanup carried a restoration condition.
Retain its follow-up until the current operator inventory establishes whether restoration is still required.
The earlier conservative trigger required stable WAL reclamation and at least 10% free space after restoration.
A planning document does not authorize allocating ballast or mutating production volumes.

## Spill ownership and upgrades

A spill reader must own the underlying file and accounting reservation until EOF, error, or cancellation permits cleanup.
A quota rejection must return its charge so a later query can acquire capacity.
Schema mismatch, partial consumption, concurrent writers, and outstanding filesystem operations need independent coverage.
Immediate deletion at EOF and cleanup after stream drop are different lifecycle assertions.

DataFusion 54 and 55 exposed different writer APIs.
Port the resource contract and failing regression rather than mechanically copying the older method.
Preserve fork behavior for Variant evaluation, schema transforms, field IDs, null propagation, and selected-file/DV scans.
A matching function name or pointer cache does not establish equivalent semantics.

The [DataFusion patch record](../../patches/datafusion/README.md) retains the old-stack porting evidence.
Dependency upgrades need integrated real-I/O, cancellation, memory, spill, and rollback checks on their exact pins.
Historical candidate-only passes are not acceptance for the current dependency tree.

## Query admission

Heavy-query admission originally covered unbounded sorts but missed wide ordered merges below dedup.
Later admission included that class and charged actual sort fan-out and multiple stages.
Small pools must not force an unsafe minimum number of heavy queries.
Aggregate-result sorting over a small set of groups is different from sorting raw rows.

A blanket admission gate can create a full queue-wait penalty for cheap charts.
A gate that never binds cannot explain the observed tail.
Record queueing, remaining statement deadline, held permits, spill usage, and execution phase before changing concurrency.
Dropping a client stream must release permits and owned resources.

## Deployment identity and recovery

A valid token for the wrong CapRover application once submitted the database image to the Monoscope app.
The shared deployment path now validates the expected app and token before fencing writes or announcing handoff.
The empty-upload identity probe reaches payload validation without scheduling a build.
Wider service/image/mount assertions remain a distinct review.

A missed availability SLO must not interrupt replacement recovery, soak, receipt, or lease cleanup.
An already-running verified image can finish reconciliation without another restart.
The image digest, source fingerprint, boot identity, recovery state, and receipt provide different evidence.
Local HEAD alone does not identify the running image.

Draining errors need a bounded client retry path across the replacement interval.
The September handoff sample recorded 327 failed statements across eight projects over approximately 22 seconds.
A DLQ is a backstop, not proof that every refused batch replayed.
Establish an identity-bearing retry outcome before closing those failures.

## Monitoring and acceptance

Separate failed statements, malformed input, deployment draining, transport errors, and missing data.
A generic ERROR or a generated issue title is insufficient root-cause evidence.
Issue severity can disagree with the recurring runtime signal.
`occurrence_count=1`, a stale list timestamp, or a short quiet window does not prove resolution.

Use reset-aware deltas for cumulative counters.
Separate hot and sealed progress so a healthy hot lane cannot hide a stopped sealed lane.
An idle lane can be healthy; “time since commit” must account for an empty eligible queue.
Observe buffer pressure, bucket age, flush failures, unresolved publication, WAL allocation, deleted-open bytes, and actual disk headroom.
A sealed-index metric deliberately excludes today's files and cannot detect today's raw-search cost.

The October 19 review found all 16 monitors normal while 819 of 820 error events named a missing `service` field.
That observation motivates query-error coverage, not another resource-threshold adjustment.
The current backlog requires caller attribution, safe error diagnostics, and explicit no-data/recovery behavior.
Keep production host access read-only unless the user separately authorizes a concrete operational change.
