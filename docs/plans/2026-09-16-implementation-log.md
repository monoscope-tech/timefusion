# September 16 implementation log

This log records execution against [the next-days work plan](2026-09-16-next-days-work-plan.md). It separates shipped behavior, local work, measurements, and rejected changes so a restart does not erase the decision state.

## Current state

| Plan items | State | Evidence and next action |
| --- | --- | --- |
| 01 | Fix in review | Fingerprint `455e7ae0bbfa7d80ac7e1515f077270b`, Monoscope's endpoint auto-ack evidence query, produced at least 50 failures in the six hours ending around 04:00 UTC on September 17. Several were 30-second admission queue timeouts. Monoscope PR #572 replaces its one seven-day aggregate with seven sequential daily slices and merges duplicate `(hash, UTC hour)` buckets before applying the existing proof rule. Deploy and verify that the old fingerprint stops recurring. |
| 02 | Deployed and verified | Late row-stream failures now carry a normalized fingerprint and template, table and project dimensions, protocol, effective deadline, duration, and failure class. The existing failure counter remains the aggregate signal. A controlled timeout/resource test covers the event, and production emitted the fields after deployment. |
| 03 | Deployed and verified | Admission previously wrapped unbounded `SortExec` only. PR #308 makes the observed multi-partition `SortPreservingMergeExec` below ordered `DedupExec` share the heavy-query gate. The live process has admitted 4,672 ordered merge-on-read queries, proving the production shape reaches the new guard. |
| 04 | Deployed and measuring | Multi-partition ordered merge-on-read fan-ins take one heavy-query permit for the stream lifetime and expose `class=ordered_mor_merge` plus fan-in in `EXPLAIN`. At the latest snapshot, the live process had admitted 7,240 heavy queries, queued 1,120, and returned 66 bounded queue timeouts. Memory charge was 14%; continue comparing completion and timeout rates before widening coverage. |
| 05 | Historical failure triaged | The 64 GB spill-limit issue has one recorded occurrence, at `2026-09-14T16:10:28Z`, during an external-sort saturation period under the old 22 GiB query pool. Bounded daily event searches for the limit message returned no matches on September 15–17. No evidence supports raising the cap. Keep it as a bounded safety limit and use daily recurrence slices if the shape returns. |
| 06 | Handoff measured; fixes merging | A normal September 17 handoff made writes unavailable for 38.470 seconds and produced 327 failed write statements. A later rollout continued after `HANDOFF` failed and left clients unready for 233.946 seconds, producing 368 `TF_WRITE_FAILED` events and 369 DLQ-route events. Monoscope PR #575 is merged with bounded retries for startup SQLSTATE `57P03` and the canonical TimeFusion drain response. TimeFusion PR #311 makes a single successful `HANDOFF` mandatory before replacement. The durable DLQ tiers remain the backstop; current logs do not carry an identity that proves every refused batch's eventual replay. |
| 07–08 | Recovery verified | The two named issues are transport failures: one S3 GET failed while sending the request, and one vacuum/list operation failed while reading the response body. A strict September 11–17 search found no event carrying both `NoSuchKey` and the generic S3 error marker. The failed September 12 vacuum belonged to a sweep whose later operations deleted 2,799 objects, and current sweeps continue completing. Both production Delta roots have fresh checkpoints and contiguous retained JSON versions. Continue daily recurrence measurement; do not treat these transport failures as evidence of lost objects. |
| 11–12 | Baseline captured | The pre-deploy process was 3.91 hours old and matched the deployment receipt for commit `a2e69c59`. Selected runtime evidence is below. A complete sanitized CapRover environment record remains open. |
| 13 | Physical-write ledger verified in production | `bench/delta_work_ledger.py` reads an explicitly bounded Delta-log window, attributes work by lane/project/date, and separates new Parquet output from same-path deletion-vector re-adds. A post-deploy 22-minute audit read 1,000 recent log objects and 112 in-window commits. It measured 376.3 MB of flush Parquet and 1.856 GB of wave Parquet while separating 41 same-path re-adds and 754.7 KB of new deletion-vector sidecars. |
| 14 | Event reconciliation verified | The first production audit joined all 29 staged wave IDs to all 29 landed IDs, with no unmatched staged IDs and no extra landed IDs. Both sides total 3,890,950 rows and 287,046,441 bytes. The durable Delta ledger works at the single partitioned table root, `timefusion/otel_logs_and_spans`; its earlier zero result came from local test-bucket credentials. Process byte counters remain deferred because they reset at boot and would misclassify same-path DV Add metadata as newly written Parquet. |
| 15–16 | Post-retraction wave census captured | Eager retraction remained at 94.86%. In the retained `02:52–08:53Z` Delta history, spans `wave_commit` wrote 297.24M physical rows / 39.26 GB against 18.47M flush rows / 1.79 GB; metrics wrote 239.76M rows / 8.61 GB against 16.65M flush rows / 745 MB. Retraction did not remove the planner problem. |
| 17 | First policy candidate rejected | A read-only isolated arrival-cohort replay feeds 1,218 sanitized post-retraction flush files through the real `select_tail_bin` at production ticks and limits. Size ratio 4 cut rewrite bytes only 13.7%, while waves rose 66→186 and live files 74→260. Sensitivity runs at 0.9×, 0.99×, and 1.1× output size also failed the 25% rewrite reduction and ≤10% file-count gates. Keep the ratio disabled. Pass/rows/bytes/wave-id fields were added locally so the next trace can separate Pack from Repair and prove which staged bins landed. |
| 19 | Short-window queue movement reconciled | An 82-second paired live-gauge and durable-journal capture closed with a zero residual. The live estimate rose 85.14 GB while durable tasks rose 121.71 GB and work outside the durable journal fell 36.57 GB. Most durable growth was BaseRollup work. The 213 `superseded -> pending` and nine `complete -> pending` transitions are supported lifecycle events, so this sample shows task manufacture and reopened work without proving duplication. PR #313 adds the offline accounting tool and report. Use a healthy four-to-six-hour trace before changing planner policy. |
| 25–29 | Blocked on product semantics | The proposed sessions additions were removed from the deploy candidate. They collided with a dimension name, lacked legacy-cell refusal, omitted the real `browserScope`, changed latest-page to first-page semantics, and dropped URL and resource-UA fallbacks. |
| 40 | Remediated and verified across replacement | The 600 GiB Foyer cache now uses a nested bind mount from the 3.5 TiB ephemeral RAID0 volume. The latest replacement retained that mount and recovered 103,878 entries in 54 ms. The cache currently allocates about 249 GiB there; ephemeral use is 10% with 3.2 TiB free. The durable RAID1 is 58% used with 717 GiB free. |
| 43–44 | Deferred | No v4 canary or maintenance offload was attempted. Their measurement and architecture gates remain unmet. |
| 46 | Identity guarded; handoff fail-closed fix in review | The deploy client rejects any `CAPROVER_APP` other than `timefusion` and proves the token belongs to that app before `HANDOFF`. PR #309 completes recovery, soak, receipt, and lease reconciliation before reporting an availability-SLO miss. A later rollout exposed a second flaw: three failed `FLUSH` attempts and a failed `HANDOFF` were warning-only, so replacement proceeded without proving a drained write fence. PR #311 removes pre-flush and fallback behavior and permits replacement only after one successful `HANDOFF`. |

## Pre-deploy production baseline

Captured at `2026-09-16T21:56:53Z` from `timefusion_stats`.

- Boot: `2026-09-16T18:02:15.313170Z`; uptime 3.91 hours.
- Running receipt: commit `a2e69c59`; image `ghcr.io/monoscope-tech/timefusion@sha256:500aae975fb4a0081e573ed696f7631d69bbceaa0af9ec09d4243b130855e94c`.
- Query latency: p50 132.139 ms, p95 463.986 ms, p99 1.002503 s, p99.9 3.394987 s across 223,043 queries.
- Late stream failures: 168.
- Memory: 19.91 GB charged of 118.11 GB, 16%; query pool 186.8 MB used of 22.53 GB; coordinator pool 1.56 GB used of 28.16 GB.
- Admission: 8,559 admitted; zero queued; zero queue timeouts.
- Tantivy prefilter: 5,148 attempts, 2,762 used, 2,383 skipped. Cap overflow caused 1,701 single-index and 293 combined skips.
- Retraction: 4,842,891 of 5,102,943 appended versions retracted, 94.90%.
- Maintenance: 712 pending, 78 retrying, 2 running; 15.81 GB estimated backlog; 665 dirty rollup partitions.
- Maintenance progress: 1,079 flushes, 1,880 light-packing waves, and 240 dedup waves committed.
- Rollups: 4 full hits, 337 hybrid hits, 15,283 misses. The largest recorded miss class was `unknown_filter` at 6,775.
- WAL: 24.00 GB, 25 files, recovery complete, zero replay rows, zero landed skips.

These counters are process-scoped. Later comparisons must use differences from the same boot or another process older than two hours at a matched workload hour.

## Runtime service and storage shape

Read-only inspection of the live Swarm service found:

- One amd64 replica pinned to `server1`, with a 32-core/120 GiB limit and a 12-core/32 GiB reservation.
- The host is a 48-thread AMD EPYC 8224P and exposes AVX-512, but the running image remains the tested x86-64-v3 image.
- The service uses a 110 GiB TimeFusion memory budget, eight query partitions, two maintenance rewrite workers, a 24 GiB ingest-buffer ceiling, and a 600 GiB Foyer disk cache.
- Durable data and the active WAL live below `/app/data/timefusion` on the 1.8 TiB RAID1 root filesystem. Spill and scratch subdirectories are nested mounts on a 3.5 TiB RAID0 XFS volume.
- The separate `/app/data/wal` mount is unused because the active WAL path is `<TIMEFUSION_DATA_DIR>/wal`. Correct or remove that misleading mount in a later, separately verified configuration change.

Before remediation, the RAID1 volume was 95% used with about 100 GB available. The old Foyer cache accounted for 300 allocated backing files and 644,151,390,208 bytes. Query/maintenance scratch had about 3.4 TB available.

## Foyer cache relocation

The cache is reconstructible from object storage, so it was moved without changing durable paths:

- Created `/mnt/ephemeral/timefusion-foyer-cache`, owned by the container UID/GID `65532:65532`.
- Persisted a nested CapRover bind mount from that directory to `/app/data/timefusion/cache`.
- Ran `FLUSH` and `HANDOFF`; both succeeded on the first attempt.
- Verified the replacement boot `1789599276197356`, zero-millisecond WAL recovery, the new Swarm mount, and `foyer.cache_dir=/app/data/timefusion/cache`.
- Ran the deployment soak: 54 probes, zero failures, and zero consecutive failures.
- Deleted the 300 hidden old Foyer backing files from RAID1 after the soak. No WAL or durable table files were removed.
- RAID1 use fell to 54%, with 778 GB available. The new sparse cache had allocated about 15.4 GB on RAID0 while warming; the scratch filesystem remained 3% used.

After the code deployment and further cache warming, the durable RAID was 50% used with 838 GB available. The ephemeral RAID was 3% used with 3.4 TB available, and Foyer reported 34,122,424,320 L2 bytes in use.

The next full replacement retained the nested bind mount. The active Swarm service and container both map `/mnt/ephemeral/timefusion-foyer-cache` to `/app/data/timefusion/cache`; the directory now allocates about 249 GiB. Foyer recovered 120 data blocks out of 180 clean blocks and 103,878 entries in 54 ms. At the latest snapshot, RAID0 was 10% used with 3.2 TiB free, while RAID1 was 58% used with 717 GiB free.

## Production deployment

The merged `c846742a` build is running as the immutable amd64 image `ghcr.io/monoscope-tech/timefusion@sha256:07610d156741471faa94eef4c8dc1b2d1acd81ad0cf90bcc07fcb7a4ac0a2235`.

- Replacement boot: `1789600407094582`.
- WAL recovery completed in 0 ms.
- The post-recovery soak passed 55 of 55 probes with no consecutive failures.
- The shared deployment receipt records this image and boot, and the production lease is clear.
- The live service retains the nested `/mnt/ephemeral/timefusion-foyer-cache` to `/app/data/timefusion/cache` bind mount.
- A production resource failure emitted `failure.class`, `query.fingerprint`, `query.template`, `query.tables`, `project.id`, `protocol`, `deadline_ms`, and `duration_us`. The query template contained placeholders instead of literals.

The first local deploy command inherited the web application's CapRover target and submitted the TimeFusion image to `monoscope`. Swarm paused that update before replacing any of its three healthy web tasks. The service was rolled back to `ghcr.io/monoscope-tech/monoscope:1c848e9225ed132d7eece08cb2ef499bdc97f47a`, its CapRover image record was corrected, and it returned to 3/3 replicas before the database app was deployed. This exposed a deployment-safety gap: local deployment credentials need an app-to-service identity check before `HANDOFF` or image submission.

The GitHub deploy failure had a separate credential cause: TimeFusion's app deploy token was disabled while the workflow still carried a stale token. The token was re-enabled, validated against the `timefusion` app, and installed with the explicit app name in GitHub Actions. Enabling it through this CapRover version's full app-definition endpoint reconciled the unchanged service. The replacement boot `1789601305924947` completed WAL recovery in 2 ms, retained the nested Foyer mount, and passed 55 of 55 soak probes. The deploy preflight now uses an empty CapRover upload request: status 1108 proves the token/app pair reached payload validation, which happens before CapRover schedules a build. Its diagnostics persist only the server host, app, verification time, and `token_bound=true`.

Two follow-up changes are also live:

- PR #308, commit `3bdd6e41`, deployed ordered merge-on-read admission. The replacement completed recovery and soak, and the live counter later reached 4,672 admissions for that exact class.
- PR #309, commit `be05291b`, changed rollout reconciliation so a missed availability SLO is reported only after the healthy replacement is soaked, receipted, and its lease is cleared. A follow-up deployment run recognized the already-running image and completed without restarting it.

The current production process at the September 17 follow-up has boot identifier `1789610570795968` and runs the amd64 manifest `sha256:ca762bbca0cb4a1218874e6e74a7e9a6d890cfa5d6b4246396925f8424eb6c73`. WAL recovery remains complete with a zero-millisecond recorded duration. Foyer still reports `/app/data/timefusion/cache`, its 600 GiB budget, and 132,724,158,464 bytes in L2 use on the ephemeral mount. The process reports 17.25 GB charged, 14% of its configured memory budget.

The PR #310 rollout exposed a fail-open handoff path. Three preliminary `FLUSH` calls exhausted roughly five minutes each or lost the connection. `HANDOFF` then exceeded its four-minute server limit, but the old script continued to CapRover replacement. The replacement replayed 57,455 WAL inserts carrying 12,773 MB in 134.907 seconds. The measured interval from the last ready old-process probe to the first ready new-process probe was 233.946 seconds. The recovered process passed all 45 soak probes, its receipt was written, and the lease was cleared, so the run failed only when it reported the availability-SLO miss. The same interval produced 368 `TF_WRITE_FAILED` events and 369 write-failure-to-DLQ events. PR #311 changes preparation to one `HANDOFF` with a 300-second client timeout and exits before image submission unless it succeeds.

The new wave instrumentation reconciled exactly after this boot: 29 staged IDs matched 29 landed IDs, with 3,890,950 rows and 287,046,441 bytes on each side. There were no unmatched staged IDs and no extra landed IDs.

The matching durable-ledger proof used Delta versions 606129 through 606240, timestamps `11:59:17.462Z` through `12:20:55.559Z`, and the newest 1,000 JSON log objects. Its 112 commits included 13 flush commits that wrote 75 Parquet files, 6,872,990 rows, and 376,261,039 bytes. Ninety-seven wave commits wrote 63 new Parquet files, 15,380,549 rows, and 1,855,924,149 bytes while removing 135 Parquet files. The ledger accounted for 41 same-path re-adds and 754,708 bytes of new deletion-vector sidecars separately. Two vacuum boundary commits wrote no data. This is a short post-deploy window, so it proves attribution and restart-safe accounting rather than a daily amplification rate.

## Object-store recovery

The named S3 GET and vacuum/list issues are transport failures rather than evidence of missing data. A daily September 11–17 search initially appeared to contain `NoSuchKey` matches, but those records were Monoscope access logs containing the diagnostic query text. Requiring both `NoSuchKey` and the generic S3 error marker returned zero events on every day.

The exact vacuum failure at `2026-09-12T00:16:19.641527Z` could not read one list response body. Other operations in the same scheduled sweep completed immediately afterward, including deletions of 133, 1,285, 1,134, and 247 objects. September 17 sweeps also completed repeatedly, with individual operations deleting as many as 3,610 objects.

The production logs show convergence rather than retained-log runaway. `otel_logs_and_spans` had a fresh checkpoint at version 606379 and 1,965 contiguous retained JSON versions from 604419 through 606383. `otel_metrics` had a fresh checkpoint at version 196552 and 1,019 contiguous retained JSON versions from 195547 through 196565. This audit supports the current retry and retention behavior; it does not justify shortening retention.

## Maintenance task-flow accounting

A paired 82-second capture compared live queue snapshots with two durable maintenance-journal snapshots after replaying each captured WAL tail. The live estimated backlog rose from 1,360,809,010,853 to 1,445,949,060,204 bytes, a gain of 85,140,049,351 bytes. The durable subset rose by 121,707,672,877 bytes: 521 active tasks carrying 123,058,426,529 bytes were added, four carrying 1,350,272,860 bytes retired, and one was repriced by -480,792 bytes. Work outside the durable journal fell by 36,567,623,526 bytes. The combined reconciliation residual was zero.

The durable growth was chiefly BaseRollup, led by metrics, dashboard, and sessions tiers. The state changes included 213 `superseded -> pending` and nine `complete -> pending` transitions. Source inspection confirms both can be valid: a superseded parent is fresh debt after its live descendants disappear, and a completed slice becomes pending after later invalidation. The next trace must connect each reactivation to its planner or invalidation cause before treating repeated creation as a bug.

A separate five-minute sample proved why event logs alone do not close the accounting. Its 200 starts and 196 finishes implied a 97.01 GB estimated decrease, while the live gauge rose 66.22 GB. Aggregate events reported 340 planned compaction-debt tasks and five coarsened slices without task identity. During the same interval, physical progress continued: five flush commits wrote 97,024 rows and 14,236,053 Parquet bytes, while seven wave commits wrote 2,396,715 rows and 133,113,961 bytes. PR #313 keeps estimated task flow, the durable subset, and physical Delta output separate.

## Query findings

The PostgreSQL parser already rewrites scalar membership into the indexed form. A local live `EXPLAIN` of the issue sample shape produced `array_has(hashes, 'abc')` in the logical and physical filters. Changing `= ANY(hashes)` to another spelling would not fix the timeout.

The current production plan for fingerprint `bfbe3f4a34ef5085edb38b355de6ffef` disproved the initial weighted-sort proposal. Its physical core is `DedupExec -> SortPreservingMergeExec -> OrderingProbeExec -> DeltaScanExec`, with eight scan groups and no `SortExec` or `AdmissionExec`. DataFusion's file repartitioner can split a compressed file above 10 MiB into eight ranges. The merge then opens all ranges and buffers one decoded batch from each. With wide telemetry rows and 2,048-row batches, this explains the observed approximately 930 MiB `SortPreservingMergeExec` reservation even when the outer query requests only 11 rows. `OrderingProbeExec` only observes order; it does not sort or fall back.

Admission coverage after the focused patch is:

| Physical shape | Admitted | Reason |
| --- | --- | --- |
| Unbounded `SortExec` | Yes | Spill and merge reservations compete for the fixed query pool. |
| Ordered `DedupExec` over multi-partition `SortPreservingMergeExec` | Yes | One decoded batch per merge input can approach a GiB before a small fetch returns. |
| Ordered `DedupExec` over a one-partition merge | No | DataFusion uses the pass-through path; there is no multi-way fan-in. |
| Bounded TopK | No | Memory is bounded by its fetch. |
| Ordinary hash aggregate or hash join | No | No current production evidence justifies gating every instance. |
| Rollup read or point lookup without either heavy shape | No | Preserve cheap-query concurrency. |

The next plan optimization to test is session-local: raise `datafusion.optimizer.repartition_file_min_size` for the exact listing query and verify that eight scan groups become one, results remain identical, and merge memory and latency fall. A global change needs a full single-file scan benchmark because it also removes intra-file parallelism from unrelated analytics.

The active timeout fingerprint `455e7ae0bbfa7d80ac7e1515f077270b` is Monoscope's endpoint auto-ack evidence query. It expands `hashes`, derives an hourly epoch expression, and groups both expressions across a seven-day window. Three consecutive executions reached the 90-second statement deadline. This is separate from the ordered-listing memory failure and needs a client-query result oracle before changing its semantics.

The result oracle is now established. One-day slices of the same candidate scan completed in approximately six seconds for the newest slice and one second for each older slice; all seven completed in about 12 seconds for 14 candidates. The combined result contained 31 `(hash, hour)` rows, proved zero candidates, and made no writes. Exact-hash `ORDER BY timestamp DESC LIMIT 20` was rejected as an alternative because a false candidate still read about 3.6 GB and took 35 seconds.

Monoscope PR #572 implements seven sequential, half-open daily statements. The newest statement retains the old query's open upper bound. Client aggregation sums duplicate `(hash, UTC hour)` buckets because a non-hour-aligned daily boundary can split one hour across two statements; the two-hour proof rule must still count that as one hour. Integration coverage proves both that evidence from two real hours combines across a daily boundary and that one split hour does not become two. At the pre-deploy follow-up, the old fingerprint still produced at least 50 failures in six hours, including repeated 30-second heavy-query queue timeouts. The deployment verification must show that this fingerprint stops recurring.

The production prefilter counters point to the remaining mechanism: high-hit predicates exceed the candidate cap and fall back to the raw scan. The event-sample query then evaluates the full matching window before its bounded TopK returns one row. A specialized newest-hit index path may help, but it needs mutable-version and deletion-vector correctness work. The new failure fields should first establish which fingerprints and windows justify that work.

## Post-retraction wave measurement

At `2026-09-17T05:55:55Z`, the current process was 3.89 hours old. It had retracted 4,196,878 of 4,424,463 appended merge-on-read versions, or 94.86%. The maintenance snapshot reported 1,841 light-optimization waves, 210 dedup waves, 294 pending tasks, three running tasks, and 13.36 GB of estimated backlog.

Lane-tagged production events from the boot at `02:02:50Z` through `06:00Z` show that packing work remains material:

| UTC hour | Light-optimization commits | Dedup commits | Light-optimization input | Staged bins | Input files |
| --- | ---: | ---: | ---: | ---: | ---: |
| 02:00 partial | 436 | 54 | 5.54 GB | 436 | 684 |
| 03:00 | 457 | 54 | 6.75 GB | 457 | 712 |
| 04:00 | 489 | 53 | 7.49 GB | 490 | 795 |
| 05:00 | 495 | 49 | 8.55 GB | 495 | 801 |

Across the interval, 1,878 light-optimization bins staged 28.33 GB from 2,992 input files. Mean fan-in was 1.59 files, maximum fan-in was four, and staging never waited for a light-rewrite permit. The hourly commit count did not fall as the process aged; it rose from 436 in the first partial hour to 495 in the last hour. This is not a matched before-and-after result because workload hour and row volume differ from the September 15 sample. It is enough to reject the idea that input retraction removed the planner problem and justified the simulation below.

The durable ledger sharpens that result. The retained spans log covered 2,408 commits from `02:51:59Z` through `08:52:59Z`. Flush wrote 18,474,142 rows in 1.793 GB of new Parquet. `wave_commit` wrote 297,236,004 physical rows in 39.263 GB, while 328 same-path DV re-adds wrote only 4.46 MB of new DV sidecars and were excluded from Parquet amplification. The retained metrics log covered 1,159 commits from `02:47:59Z` through `08:52:33Z`: flush wrote 16,651,122 rows / 744.9 MB, and `wave_commit` wrote 239,764,847 rows / 8.607 GB. These are retained-window ratios, not daily capacity estimates, but they prove that physical wave output still dominates flush output in both tables.

`wave_commit` contains Pack, Repair, and dedup units, so commit tags alone cannot safely choose a Pack policy. The local instrumentation now includes `pass`, input/output rows and bytes, output count, and `wave_id` on staging events. A confirmed `wave_bins_landed` event lists the surviving IDs after liveness and overlap checks, including self-landed and post-timeout-landed subsets. This avoids treating the observed 1.59-file mean as Pack fan-in when singleton Repair work is mixed into it, or crediting staged bins that were discarded before commit.

The first reconciliation will use one healthy four-to-six-hour window for spans and metrics separately. It will join staged and landed events by `wave_id`, split event analysis by boot, and compare the same half-open Delta timestamp/version window with the durable ledger. Physical Parquet and DV bytes come from the ledger. Unjoined landed IDs and ledger-only commits are coverage gaps, not zero work. Exported byte counters wait until this audit defines semantics that survive restarts and same-path DV commits.

The first counterfactual replay used only physical flush arrivals, excluded historical wave outputs, advanced at real five-minute ticks, applied the 15-minute seal lag, 55.2 MB budget-derived target, five-file minimum, one-million-row value floor, and 12-wave cap, and fed each simulated output back into the real selector. It is an isolated arrival-cohort experiment: both arms start empty instead of from a common active-file snapshot, and each tick grants all 12 rounds without charging staging time against the real 240-second deadline. The latter assumption favors debt drainage, so it cannot understate the candidate's live-file problem, but the wave counts are upper bounds rather than time-faithful production predictions. At the observed-size midpoint, the current ratio-off policy rewrote 4.020 GB across 66 waves and left 74 live files; ratio 4 rewrote 3.469 GB across 186 waves and left 260 live files. Maximum lineage depth fell from 26 to 11, but rewrite reduction was only 13.7%, waves rose 182%, and live files rose 251%. Output-size sensitivity from 0.9× to 1.1× produced only 5.7–16.8% rewrite reduction and 256–267 candidate live files. The isolated cohort rejects this candidate against its gates; a future candidate needs a common active-file seed and a measured duration model before stronger production predictions.

## Sessions decision

The checked-out client query is not equivalent to the proposed rollup state:

- It filters through `browserScope`, whose telemetry-language, resource-UA, name, and page-view branches are not tier dimensions.
- Its page value falls back from `url.path` to `url.full` and the span name. The proposed state stored only `url.path`.
- Its user agent falls back from the attribute value to the resource value. The proposed state stored only the attribute value.
- `first_value(... ORDER BY timestamp)` changes the earlier latest-page behavior and has no deterministic tie-break for equal timestamps.
- New measures must be absent from untagged legacy cells until those cells are rebuilt.

Implementation resumes only with an explicit page/UA contract, the literal client query in the routing test, and a raw-versus-routed result oracle.

## Local validation

Record final results here before push:

- `cargo fmt --all` and `cargo check --tests`: passed.
- `cargo test -q late_stream_failures_keep_scrubbed_query_context -- --nocapture`: passed (1 test).
- `make ci-signoff`: passed `fmt`, `clippy`, `test`, `pg-smoke`, and `e2e`; deployment helper tests and the production image smoke test also passed.
- Follow-up deployment guard: `python3 scripts/deploy/test_run.py` passed 10 tests, `python3 scripts/deploy/test_lease.py` passed 6 tests, and `make ci-signoff CHECKS="fmt"` passed while reusing the published image.
- The merged deploy guard then passed end to end in GitHub run `35163294171`: the app/token preflight succeeded, the existing signed image passed its smoke test, and the workflow recognized the same production boot and completed 46 readiness probes with zero transient failures instead of restarting it.
- Ordered-MOR admission patch: 9 focused unit tests and both pgwire admission tests passed. The complete E2E suite passed 65 of 65 tests and published its local attestation. A stale host-dependent queue assertion was replaced with a deterministic exhausted-semaphore test; concurrent pgwire queries still prove complete, exact results and one admission per query.
- PR #308 merged and deployed at commit `3bdd6e41`; live `scan.heavy_query_ordered_mor_admitted` later reached 4,672.
- PR #309 merged and deployed at commit `be05291b`; its follow-up deploy reconciled the running image without a restart.
- Monoscope endpoint auto-ack change: strict application build and the focused real-Postgres/real-TimeFusion suite passed, including both daily-boundary regressions. PR #573 repaired the clean-base lint/test drift; PR #572 then merged the query split. PR #574 repairs the generated cabal manifest and makes doctest CI self-contained so the merged query can deploy.
- Physical ledger utility: `python3 -m unittest bench/test_delta_work_ledger.py` passed 4 tests; `python3 -m py_compile bench/delta_work_ledger.py bench/test_delta_work_ledger.py` passed.
- Packing replay: the ignored production-trace arm ran the current policy and ratio 4 at 0.9x, 0.99x, 1.0x, and 1.1x output-size assumptions. Every ratio-4 run missed the rewrite-reduction/file-count gate.
- Final landed-work candidate: `cargo check --tests` passed. `make ci-signoff CHECKS="fmt clippy"` and `make ci-signoff CHECKS="test pg-smoke e2e"` passed; the Rust suite completed 2,008 tests plus 13 doctests, PostgreSQL smoke passed, and the production image passed its PGWire probe. `make ci-status` reports no outstanding GitHub checks.
- Monoscope handoff retry: the focused classifier/budget suite passed 11 examples. Local signoff passed frontend, build, 1,614 doctests, and 318 unit tests; GitHub passed hlint, integration, and E2E. PR #575 merged as `b1a86bf8`.
- Fail-closed deployment preparation: `python3 scripts/deploy/test_prepare.py` passed three paths covering success, server failure, and client timeout. Each path issues exactly one `HANDOFF`, issues no preliminary `FLUSH`, and writes drained state only on success. The complete local signoff passed two production-image tests, three prepare tests, six lease tests, and 12 run tests while reusing the signed production image.
- Documentation follow-up: `make ci-signoff` reused the current passing Rust attestations, reran two production-image tests, six lease tests, and 12 run tests, and reused the signed image.
- Maintenance task-flow tool: `python3 -m unittest bench/test_maintenance_task_flow.py` passed 7 tests; `python3 -m py_compile bench/maintenance_task_flow.py bench/test_maintenance_task_flow.py` and `git diff --check` passed. Its full `make ci-signoff` reused the valid Rust attestations, reran two production-image tests, six lease tests, and 12 run tests, and reused the signed image.
- Signed amd64 candidate for this source fingerprint: `ghcr.io/monoscope-tech/timefusion@sha256:ab992cb9e0c2e1baca9098369c992ced8afbea95ecede01644291f917176c547`.
- Signed multi-platform candidate: `ghcr.io/monoscope-tech/timefusion@sha256:d8646e00ebfb3a38dde9d877472e37c7e58ee314683687b0f4f96d67a69c148a`.
- Deployed amd64 manifest: `ghcr.io/monoscope-tech/timefusion@sha256:07610d156741471faa94eef4c8dc1b2d1acd81ad0cf90bcc07fcb7a4ac0a2235`.
- GitHub checks not covered locally: none reported by `make ci-status`.
