# September 16 implementation log

This log records execution against [the next-days work plan](2026-09-16-next-days-work-plan.md). It separates shipped behavior, local work, measurements, and rejected changes so a restart does not erase the decision state.

## Current state

| Plan items | State | Evidence and next action |
| --- | --- | --- |
| 01 | Investigated | Current `pgwire.stream_failed` events are dominated by the issue event-sample query and the endpoint auto-ack query. The former uses `? = ANY(hashes) ORDER BY timestamp DESC LIMIT 1`; the latter expands matching hashes into hourly groups. Recover the post-change fingerprint counts and physical plans before changing admission. |
| 02 | Deployed and verified | Late row-stream failures now carry a normalized fingerprint and template, table and project dimensions, protocol, effective deadline, duration, and failure class. The existing failure counter remains the aggregate signal. A controlled timeout/resource test covers the event, and production emitted the fields after deployment. |
| 03 | Audited; focused policy patch in progress | Admission previously wrapped unbounded `SortExec` only. The dominant log-listing plan instead carries an eight-way `SortPreservingMergeExec` below ordered `DedupExec`, with no `SortExec` or `AdmissionExec`. The focused patch makes that exact shape share the existing heavy-query gate; bounded TopK, one-way ordered scans, ordinary joins, and ordinary aggregates remain outside it. |
| 04 | Focused protection implemented locally | Multi-partition ordered merge-on-read fan-ins now take one heavy-query permit for the stream lifetime and expose `class=ordered_mor_merge` plus fan-in in `EXPLAIN`. A separate counter records their admissions. Production deployment awaits local CI and review. |
| 11–12 | Baseline captured | The pre-deploy process was 3.91 hours old and matched the deployment receipt for commit `a2e69c59`. Selected runtime evidence is below. A complete sanitized CapRover environment record remains open. |
| 25–29 | Blocked on product semantics | The proposed sessions additions were removed from the deploy candidate. They collided with a dimension name, lacked legacy-cell refusal, omitted the real `browserScope`, changed latest-page to first-page semantics, and dropped URL and resource-UA fallbacks. |
| 40 | Remediated and measuring | The 600 GiB Foyer cache was physically allocated on the 1.8 TiB durable RAID1 volume, which had reached 95% use. It now has a nested bind mount on the 3.5 TiB ephemeral RAID0 scratch volume. The old cache was removed only after recovery and a clean readiness soak. |
| 43–44 | Deferred | No v4 canary or maintenance offload was attempted. Their measurement and architecture gates remain unmet. |
| 46 | App identity guarded | The deploy client rejects any `CAPROVER_APP` other than `timefusion`, then proves the opaque app token belongs to that app before `HANDOFF`. The disabled TimeFusion token was enabled and both GitHub deployment secrets were repaired. Read-only verification of replica shape and mounts remains open. |

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

The production prefilter counters point to the remaining mechanism: high-hit predicates exceed the candidate cap and fall back to the raw scan. The event-sample query then evaluates the full matching window before its bounded TopK returns one row. A specialized newest-hit index path may help, but it needs mutable-version and deletion-vector correctness work. The new failure fields should first establish which fingerprints and windows justify that work.

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
- Ordered-MOR admission patch: 9 focused unit tests and both pgwire admission tests passed. The complete E2E suite passed 65 of 65 tests and published its local attestation. A stale host-dependent queue assertion was replaced with a deterministic exhausted-semaphore test; concurrent pgwire queries still prove complete, exact results and one admission per query. The remaining local signoff checks are being refreshed for the final source fingerprint.
- Signed multi-platform candidate: `ghcr.io/monoscope-tech/timefusion@sha256:d8646e00ebfb3a38dde9d877472e37c7e58ee314683687b0f4f96d67a69c148a`.
- Deployed amd64 manifest: `ghcr.io/monoscope-tech/timefusion@sha256:07610d156741471faa94eef4c8dc1b2d1acd81ad0cf90bcc07fcb7a4ac0a2235`.
- GitHub checks not covered locally: none reported by `make ci-status`.
