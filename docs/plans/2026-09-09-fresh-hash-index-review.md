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
