# Histogram capture during SQL updates

Production 03cc1ae logs at 10:16:18, 10:16:21, and 10:16:27 UTC show the
native histogram query shape declining because capture overlaps active SQL
DML, at the probe times. This is direct routing evidence, unlike EXPLAIN or canceled-query
completion counters. The guard currently covers a whole project/table,
including queued coalescer work, so recent updates can block historical
queries continuously. The 10:14:50 census reports 2,263 uncovered files:
162 today, 1,360 in the last week, and 741 older. Both issues remain relevant.

The proposed fence registers each capture's timestamp interval and each
active update's proven interval. Starting an overlapping update invalidates
registered captures. A capture cannot start while an overlapping update is
active. Finished guards expire automatically; completed writes that started
during capture must still invalidate its ticket. Unbounded predicates,
unknown scalar precision, and timestamp assignments retain table-wide fences.
Captured immutable sources require validation only across source capture.

Coalescer admission must retain each statement's guard when appending to an
existing group. Existing folding, splitting, and retry paths already retain
all guards; their lifetime regression will be updated to require every
statement's fence. Recovery remains table-wide unless its entire mutation
range is independently proved. No configuration flag is proposed.

First regression runs the actual absent-hash SQL during a bounded update to
the following day. The query must still use the histogram path. Existing
active/queued/canceled-write invalidation guards remain required. The red run
is currently building; implementation is pending that observed failure.

The SQL regression failed in 1.550s with `SQL must use the indexed histogram`,
matching the production decline. The implementation now registers captures
and active statement ranges under one short mutex, with no lock across I/O.
Weak references retain neither completed statements nor finished captures.
Starting an overlap permanently invalidates its capture ticket, including
when the writer finishes before validation. Non-overlapping statements do
not invalidate the ticket. Source capture still pins file metadata and Arrow
batches before releasing the fence.

First rs-distill pass: reuse DecomposedPredicate and scalar_micros for bound
extraction, RangeInclusive for overlap, and the existing coalescer guard
ownership through fold/split/retry. First rs-evasion pass found the source
column ambiguity: a joined source with a timestamp field must decline range
narrowing. Unknown bounds, fractional-microsecond literals, arithmetic
overflow, reversed bounds, and timestamp assignments also retain All.
The explicit All/Timestamps enum carries the protection scope; no sentinel
interval or assumed timestamp is used. Guards are retained for every queued
statement, including occupied groups. Recovery retains All.

Tests cover adjacent and overlapping ranges, active/queued/canceled writers,
completed overlaps during capture, inclusive/exclusive bounds, OR and missing
bounds, timestamp assignments, and source-column ambiguity. Initial compile
found a missing TimeUnit qualification in the test; corrected. Green
verification and the second review pass remain pending.

Initial green run: six targeted histogram tests passed in 9.410s, nextest
ad256b62-cfbd-4a53-8113-8a46c2561fc2. This includes the formerly red SQL
route assertion and the coalescer fence-lifetime regression with four
statement guards instead of two project guards.

Second rs-evasion review retained the original sequentially consistent
ordering for invalidation stores and checks. Range precision changes the
scope, not the synchronization strength. No lock spans an await; guards and
capture tickets are owned independently of the registry's weak references.
Second rs-distill review uses field iteration for source-column presence,
without constructing a missing-field error. Further review of widened
execution ranges follows below.
A final targeted rerun checks these review changes. Full combined signoff
and production acceptance are still required.

After fresh-ingest signoff completes, merge the root branch into that worktree
so committed-file indexing, empty-result avoidance, and range-aware DML
capture can ship together after full local signoff. This avoids deploying a
known project-wide capture bottleneck between the related fixes.

Further review found that coalescing widens predicates, including gaps between
statements; cross-project folding also shares the final window. Original
statement fences alone cannot cover that wider mutation. The drain must add
fences for its final predicate for every folded project before merging, and
retain those guards through retry. A real capture regression now checks a gap
between queued windows before and after drain fencing; its red run is building.
The six-test reviewed run passed in 9.466s but reported one leaky-process
classification. The next run will report every test status to identify it;
this is not claimed clean. Implementation of final drain fencing is pending.

The widened-range guard failed before implementation in 0.910s with
`folded execution must fence gaps in the widened range for every project`.
PendingGroup::fence_histogram now registers the reconstructed execution
predicate for every folded project before the first merge and retains the
new guards through requeue. The regression performs actual capture against
local Delta/MinIO, requires the specific active-DML error in the gap, and
checks capture succeeds again after all queued work is dropped.

Scope extraction now borrows only the source Arrow schema rather than a
whole UpdateSource batch. Review of drain setup confirms no target mutation
precedes the new fence; prep failures keep the existing quarantine behavior.
A missing anyhow::Context import in the stricter assertion caused a compile
failure; corrected, and the obsolete failed compile was stopped. The current
six-test run reports every status. Do not claim full signoff from this run.

Final targeted run: all six tests passed in 8.765s, nextest 9813e261-b35c-442b-8ef6-33694a836a9c,
with no leak classification. The widened coalescer test passed in 0.695s;
the real SQL/visibility fixture passed in 8.759s. The earlier single leak
classification did not reproduce and remains unidentified; do not claim a
specific leak fix from this result. Formatting and diff checks pass.

Final scoped review: execution fences are installed after predicate folding,
bucketing, and watermark clamping but before any merge; they remain in the
retried group. The API borrows only the source schema. Bound uncertainty
retains All, atomics retain SeqCst, and no live source is reloaded during
counting. No remaining finding in this change; combined full signoff and
production routing/latency validation remain pending.
