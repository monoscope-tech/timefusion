# Maintenance, rollup proofs, and conditional designs

Reviewed: 2026-10-20. This reference synthesizes the August–October maintenance plans through revision `ad1ee10b`.
It preserves design constraints and decisions, not an independent work queue.
The [current backlog](../plans/2026-10-19-next-work.md) owns remaining acceptance and conditional triggers.
Source-note provenance is in the [review inventory](../reviews/2026-10-19-timefusion-triage.md#plan-inventory).

## The optimization objective

Reduce total CPU, memory, and I/O per accepted event on the same server.
More concurrency, shorter individual scans, or deferred debt alone do not establish a saving.
A lower visible load with an increasing required backlog fails the objective.

Measure original ingest, physical Parquet output, DV bytes, and useful query coverage separately.
A same-path Add with a deletion vector is not a full-file rewrite.
An estimated decoded-byte queue is not physical storage consumption.
A completed unit is not necessarily a physical commit or new usable coverage.

## Runnable claims and admission

Historical starvation occurred when a worker claimed a task and then waited for a resource that made it unrunnable.
Keep resource availability in claim eligibility where possible.
Per-class worker pools were a proposed architecture for this bug class, not a remedy for every slow lane.
A lane receiving slots but accumulating more arrivals needs lower work or more measured throughput.

The admission model must match real decoded state, row width, sort fan-out, and I/O slots.
Pricing an entire oversized file at the whole budget can serialize or starve it behind small work.
Blindly increasing workers cannot make an inadmissible unit runnable.
Conversely, an I/O-slot cap can leave cores idle even when memory permits work.

The September 20 investigation first found an I/O cap, then a large state-pool admission problem.
Those were distinct limits. The first fix did not prove the second was solved.
Do not widen a capability or shrink a price merely to pass a gate.
Protect flush progress and account for simultaneous sort/spill consumers.

## Splitting, escalation, and horizons

A split needs a cost model, not only elapsed time or compressed bytes.
A tiny time interval can still touch a large overlapping file.
Recursive bisection inside one call can cross a floor intended only between measurements.
The preflight-floor experiment therefore moved one level per measured attempt.

Preserve the distinction between pre-claim and post-claim attempt counters.
Escalating from the wrong interpretation can skip the intended sort-partition ladder.
An exhausted ladder needs a recoverable refusal, not an immortal retry loop.

Sealed work beyond the configured horizon must not masquerade as eligible starvation.
Avoid minting work that policy will never admit. Keep historical debt visible separately.
A due derived unit can still wait for required parent coverage or a future source interval.
Task age alone does not justify increasing concurrency or deleting tasks.

## Compaction and packing

The planner and executor must agree on what constitutes packable work.
A minimum-value rule that vetoes every selected pair can wedge both hot and sealed hygiene.
Count actual refused cells, rather than transient “selected nothing” outcomes with another valid operation available.

A pair's total size matters. Counting any two sub-target files overstates packable cells.
A packed output under target is not automatically another unit of useful debt.
Benefit-ranked sealed work, fairness, and bounded admission solve different parts of selection.

Dedup units operate on bins but can rewrite whole files spanning several bins.
Repeatedly reading those files can dominate work even when individual units are cheap.
A timestamp predicate is unsound when it excludes versions needed to resolve winners.
Wider bins, bounded file sets, and time-range selection therefore need proof and cost checks together.
A simulation once refuted the proposed time-ranged policy; do not present it as an accepted design.

The September 16 size-ratio experiment rejected ratio 4.
Rewritten bytes fell only 13.7%, while waves rose 66 to 186 and live files rose 74 to 260.
The gates required at least 25% fewer rewrite bytes and at most 10% more live files.
Its isolated arrival cohort lacked a shared initial file set and a measured duration model.
It rejects that candidate under its gates; it does not predict production capacity.

## Dedup and deletion vectors

Sort order must cover the dedup key before a rewrite can collapse adjacent runs.
A schema-only key change is insufficient when a maintenance path injects another sort or trusts the wrong footer field.
Preserve greatest-version and tombstone semantics in both read and maintenance operators.
A tie-break must be explicit before treating a dedup rewrite as logically unchanged.

DV dedup masks losing physical rows instead of rewriting their entire Parquet files.
The file remains live under a changed DV identity. URI-only fingerprints cannot represent that mutation.
Concurrent DML masks need version/OCC protection so one mask cannot overwrite another.
All readers, row ordinals, count proofs, and index visibility must honor DVs.

DV strip removes the accumulated physical overhead later.
Its fleet cap prevents a per-partition allowance from multiplying across tenants.
Refused lossy rewrites retain their parks across restart and retry when the relevant state or deadline changes.
The historical per-partition rate limit caused foreground latency and was replaced by a fleet bound.

## Certification is a uniqueness proof

The original whole-day grant required a no-drop pass under an unchanged file fingerprint.
That made live-day proof accumulation effectively unreachable and delayed proof after a successful dedup pass.
Later work introduced slice/file evidence, same-pass completion certification, and retention of unaffected coverage.
Consumers and producers must share the same proof scope.

A clean slice can certify only files whose entire relevant span it proves.
It cannot certify a multi-hour file from a ten-minute observation.
Missing timestamp statistics, overlap with unproven files, or possible MemBuffer versions require a safe decline.
Partial grants must not become a stronger whole-day claim accidentally.

The proof must bind file and DV identity and account for concurrent mutation.
A URI-only fingerprint remains unchanged under same-path DV updates.
A file-level consumer can still be inert when every proven file overlaps an unproven version-bearing source.
Counters need distinct refusal reasons before another producer change is justified.

False orderings remain a separate correctness risk.
Certification cannot legalize a streaming collapse over input whose claimed ordering is false.
A whole-day proof and accumulated per-file proofs are different mechanisms with different update rules.

## Rollup source witnesses

A rollup aggregates logical rows. A physical row-count witness includes obsolete versions and can falsely reject valid output.
Row counts alone also cannot prove content identity.
A file-list fingerprint changes under benign rewrites while missing same-path DV changes.
A current proof needs the specific content, version semantics, and scope that the tier consumed.

Keep the physical proof as a correctness backstop when adding a logical witness.
A dedup rewrite can carry a witness only if it preserves the tier's logical winner set and measures.
The September witness-carry experiment had a small measured benefit and remained off after its recorded review.
Do not infer a broad saving from a safe-looking carry rule.

Version-only append evidence is per tier.
A hashes-only enrichment can leave dashboard aggregates unchanged while changing a hash tier's counts.
The DML classifier, row/content witnesses, build proof, and carry logic must use the same tier read-set.
If the predecessor was still buffered, its later flush is genuinely new Delta content, not an ignorable version-only append.

New writes must invalidate only intersecting coverage.
A physical rewrite outside a certified span must not erase unrelated proof.
Dropping the entire date entry once turned proof destruction into a misleading `not_built` counter.
Later bounded/content witnesses can rescue stale-looking output only when exact inputs still justify it.

## Hybrid coverage and publication

A hybrid query reads proven rollup interiors plus the raw fringe.
The raw leg must retain source predicates, project isolation, version resolution, and matching result types.
A tier's materialized dimension name is not necessarily a valid source-column predicate.
Empty intervals and absent output need explicit proof; a generation tag cannot prove its own coverage.

Captured input footprints prevent a wide unit from repeatedly discovering the same raw gaps.
Packed repairs must preserve the uncovered remainder when replacing shared files.
Wider coverage can retire contained work only after the covering result is authoritative.
Cleanup must retain staged rewrites eligible for resumption before early table-resolution sweeps run.

Coverage ledgers and file tags have different ownership and recovery semantics.
A lagging held/proved counter relationship can be a publication-order artifact rather than a read-path bug.
The August ledger comparison found benign one-generation lag and an unreachable zero-disagreement gate.
Union-tag compaction was deferred with explicit revisit conditions, not blocked solely by that counter.
Before changing ledger authority, establish live callers, crash recovery, stale-proof refusal, and independent parity.

## Deadlines and reactivation

A continuously touched slice can starve when each invalidation restarts its quiet period.
Ended pending slices need a maximum wait from first dirtying.
Open slices and different task states can retain different debounce rules.
Reopening a completed task must use the new deadline, rather than its historical deadline.

Today's closed slices can become raw holes when revalidation only requeues sealed dates.
`63dd0510` requeued closed today slices whose witness moved.
The recorded 24h histogram fell from 1.2–1.4 seconds to 0.36–0.51 seconds without a continuing requeue storm.
Retain both the correctness proof and the repeated-work measurement.

## Durable pause and resume

Pause/resume policy applies before enqueue, invalidation, claims, backfill, and forced-repair cursor advancement.
A blocked parent prevents derived work even when the child has no local override.
Resume requires an explicit, grain-aligned boundary. A partial parent hour does not authorize a whole derived hour.

A failed persistence operation must not leave the in-memory policy executable.
Pending policy writes block admission until durable persistence succeeds.
A command error after a durable WAL record does not establish that the old policy remains active.
Inspect pending versus durable status before retrying.

Exclude paused non-running work from eligibility, frontier-lag pressure, and oldest-due gauges.
Its historical bytes can remain visible without reducing useful worker allocation.
Policy-aware journal formats constrain binary rollback. Deleting policy metadata can resume unwanted history.
Session v1 was retired, while sessions v2 later acquired real consumers.
The old “sessions has no consumer” premise cannot justify pausing current session work.

## Conditional fixed-server stages

These are retained designs. None is a standing implementation order.

| Stage | Preserved concept | Required trigger or gate |
| --- | --- | --- |
| 0 | Shared coverage/range proof and recovery reconciliation | Exact raw/rollup parity and race checks |
| 1A | Bypass winner resolution for certified-clean input | Proof remains valid for the complete consumed input |
| 1B | Avoid repeated hash-shard preparation | Measure repeated raw work and bounded execution cost |
| 1C | Share preparation across compatible useful aggregates | Material duplicate preparation after consumer inventory |
| 1D | Ignore changes outside a tier's read-set | Exact per-tier invalidation semantics |
| 2 | Capture a shared source view or establish logical-only routing | Reproduced hybrid race or required logical-only consumer |
| 3 | Batch staged publication | Material publication cost or an approved stage-4 dependency |
| 4 | Reuse buffered rows for aggregation | Source visibility, durable ownership, and flush independence |
| 5 | Fuse dedup and aggregation | A measured material remaining lane with winner/tombstone parity |
| 6 | Minute revisions and hierarchical repair | At least 30% saving on at least 20% of the relevant resource |

Append-and-merge aggregate states can avoid rebuilding raw windows.
They do not solve retries, late updates, deletes, or cross-file duplicate correction automatically.
Retractions need invertible state or a complete repair path. Some sketches cannot subtract a replaced contribution.
Cardinality and serialized state size must be measured before selecting a finer grain.
Physical and logical source generations must remain independently auditable.
A single oversized minute still needs bounded work and recoverable progress.
Splitting its state must preserve complete winner/tombstone semantics and the published interval contract.
Do not discard an indivisible group or weaken admission to force completion.

## Conditional publication queue

The September design separated staging from publication without changing ingest WAL entries.
A queued entry carries lease ownership, staged Add metadata, retire targets, source/output proofs, and a resumable intent.
It holds no Arrow batches or decode permits.
Queue identity follows the target table's commit-lock identity.

The proposed bounds were 32 entries, 10-second linger, and a 1 GiB global staged-byte ceiling.
These were experiment parameters, not current configuration promises.
An overflow publishes inline as a singleton. Excess pending bytes defer optional new rollup claims with hysteresis.
Keep the worker's lease through publication. Fire-and-forget otherwise changes abandonment and recovery semantics.
The drainer uses asynchronous coordination rather than blocking a runtime thread on a synchronous condition variable.

Under one refreshed target snapshot, check entries against a working partition view:

1. Require the task's live Running state and an unchanged applicable generation.
2. Recompute retire targets and coverage from the working view.
3. Permit disjoint sibling publications without falsely invalidating a staged slice.
4. Allow only explicitly proved contained-target growth. Refuse missing or modified required targets.
5. Reject overlapping retire sets and invalid Add/Remove combinations.
6. Let a validated containing entry supersede a narrower pending entry only through the established coverage path.
7. Refresh resumable intents before committing the accepted batch.

After commit, publish coverage before checkpointing. Clear intents only after the checkpoint.
Base publication reopens affected derived work under the same journal ownership boundary.
An ambiguous commit retains its staged files and intent for authoritative reconciliation.
Cancellation or shutdown must settle every waiter and retain resumable work.
The queue must preserve independent INSERT acknowledgement and flush durability ownership.

The measured publication share was below 1% of journal checkpoints.
Stale retries were much cheaper than the mean completed build in that sample.
The design therefore remained conditional; batching was not a justified cure for insert-journal contention.
A singleton refactor, bounded queue, crash matrix, contention test, and paired resource comparison remain separate acceptance steps.

## Accounting and reproduction

The September 17 task-flow sample reconciled queue movement to zero arithmetic residual.
Durable tasks increased by about 121.7 GB of estimates while outside-journal estimates fell by about 36.6 GB.
The live total rose by about 85.1 GB while physical Delta commits continued.
This demonstrates accounting domains, not incorrect task creation.
Reactivated parents need lineage to invalidation/planning events before being called redundant work.

Use [the task-flow tool](../../bench/maintenance_task_flow.py) with paired snapshots, WAL tails, and a matched physical ledger.
Use [the physical ledger](../../bench/delta_work_ledger.py) to distinguish new Parquet from same-path DV actions.
A longer healthy trace must reconcile staged/landed wave identities to committed outputs.
Simulations establish scheduling properties only for their modeled resources and arrival process.
Real-I/O staging remains required for memory, spill, object-store delays, and durability.
