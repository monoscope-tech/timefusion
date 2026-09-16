# September 16 implementation log

This log records execution against [the next-days work plan](2026-09-16-next-days-work-plan.md). It separates shipped behavior, local work, measurements, and rejected changes so a restart does not erase the decision state.

## Current state

| Plan items | State | Evidence and next action |
| --- | --- | --- |
| 01 | Investigated | Current `pgwire.stream_failed` events are dominated by the issue event-sample query and the endpoint auto-ack query. The former uses `? = ANY(hashes) ORDER BY timestamp DESC LIMIT 1`; the latter expands matching hashes into hourly groups. Recover the post-change fingerprint counts and physical plans before changing admission. |
| 02 | Implemented locally | Late row-stream failures now carry a normalized fingerprint and template, table and project dimensions, protocol, effective deadline, duration, and failure class. The existing failure counter remains the aggregate signal. A controlled timeout/resource test covers the event. |
| 03 | Audited, no policy change | Admission wraps unbounded `SortExec` only. Bounded TopK, hash aggregate, and hash join plans bypass it. Production recorded 8,559 admissions, zero queued queries, and zero queue timeouts. This does not justify gating every join or aggregate. |
| 11–12 | Baseline captured | The pre-deploy process was 3.91 hours old and matched the deployment receipt for commit `a2e69c59`. Selected runtime evidence is below. A complete sanitized CapRover environment record remains open. |
| 25–29 | Blocked on product semantics | The proposed sessions additions were removed from the deploy candidate. They collided with a dimension name, lacked legacy-cell refusal, omitted the real `browserScope`, changed latest-page to first-page semantics, and dropped URL and resource-UA fallbacks. |
| 40 | Remediated and measuring | The 600 GiB Foyer cache was physically allocated on the 1.8 TiB durable RAID1 volume, which had reached 95% use. It now has a nested bind mount on the 3.5 TiB ephemeral RAID0 scratch volume. The old cache was removed only after recovery and a clean readiness soak. |
| 43–44 | Deferred | No v4 canary or maintenance offload was attempted. Their measurement and architecture gates remain unmet. |

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

## Query findings

The PostgreSQL parser already rewrites scalar membership into the indexed form. A local live `EXPLAIN` of the issue sample shape produced `array_has(hashes, 'abc')` in the logical and physical filters. Changing `= ANY(hashes)` to another spelling would not fix the timeout.

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
- Signed candidate: `ghcr.io/monoscope-tech/timefusion@sha256:d8646e00ebfb3a38dde9d877472e37c7e58ee314683687b0f4f96d67a69c148a`.
- GitHub checks not covered locally: none reported by `make ci-status`.
