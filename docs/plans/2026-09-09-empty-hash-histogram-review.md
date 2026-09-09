# Empty hash histogram validation

An absent hash should not require decoding all version keys when every
physical source has a complete index. The new regression uses four indexed
rows and a 64-byte decoded budget, before any uniqueness proof exists.
Before the change it failed with visibility source exceeds its decoded
memory budget. After the first implementation it passed in 7.710s, nextest
734484f7-8877-4eb2-8239-34f90028bf2e. This is a cost/correctness guard,
not a production timing result.

The shared index reader validates format, error state, physical single-file
coverage, document count, and ordinal bounds. Candidate queries reuse the
histogram time-range builder and return only a boolean. A possible physical
match never supplies a logical count. For an empty candidate set, the caller
also validates captured file size and footer row count through PreparedFileRows.
Every captured file must be covered with current element fields and timestamps
inside its partition. Any memory row in that day declines the shortcut.
The snapshot remains pinned; no live manifest refresh replaces its entries.

First rs-distill pass: share index validation and time-range query construction
with existing histogram reads; hoist membership columns out of the file loop.
Keep sequential I/O so only one file's metadata is retained at a time.
First rs-evasion pass: no fabricated all-visible mask, uniqueness witness,
logical count, or feature flag. Empty results increment histogram completion
but not unique-partition counters. Missing coverage retains ordinary resolution.

Second review found an avoidable cost on proved partitions. Prefer the existing
uniqueness route and probe absence only when it declines. An additional guard
adds an actual matching Arrow row to the captured memory source and requires
count 1; clearing index coverage must still fail the tiny decoded budget.
Those changes are under targeted verification. No lint or test suppression
was introduced; full local signoff remains required.

Remaining work: SQL currently starts background uniqueness seeding before
counting. Avoid redundant background proof scans for certified-empty SQL
without losing proof seeding for slow nonempty queries. Also validate full
SQL routing, corrupted/missing coverage, and production timing. EXPLAIN
shows the ordinary plan because the matcher does not match its wrapper;
it cannot establish the route of a timed-out query.

SQL cost regression: an actual session query for the absent hash returned
zero rows through the histogram route, but failed the assertion that no
background daily proof was scheduled. Red run took 1.535s. The streaming
path now invokes maintenance admission only before visibility resolution,
and targets that specific day. It does not seed already-proved or empty
partitions. The join handle still detaches on query completion or cancellation;
at most one admitted proof is requested per query. Targeted verification
is running. This preserves progress for expensive positive queries even if
the foreground scan later times out.

The SQL regression now passes together with the real-file visibility scenarios
in 8.717s, nextest 2ec2d573-8241-41f3-8d74-b46c23c13df1. The absent SQL
query increments histogram completion, returns no rows, and records no
proof attempt. Existing admission, cooldown, exact count, duplicate-version,
deletion-vector, memory-row, and missing-coverage assertions remain enabled.
A broader histogram selection is running; full signoff is still pending.

Follow-up rs-distill review: the private callback moves admission into the
single existing day loop; it does not duplicate the index probe. Targeting
a day removes the reverse partition scan from proof admission. Follow-up
rs-evasion review: the callback carries the real partition day; no placeholder
proof, forced success, suppression, or public setting was added. Errors still
enter canonical visibility resolution, with a warning for invalid indexes.
