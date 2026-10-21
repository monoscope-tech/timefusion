# Measurement methods and historical decisions

Reviewed: 2026-10-20. This reference summarizes decisions from August–October plans through source revision `ad1ee10b`.
It preserves corrections and experiment limits so future work does not repeat discarded premises.
The [current backlog](../plans/2026-10-19-next-work.md) owns next actions.
The [review inventory](../reviews/2026-10-19-timefusion-triage.md#plan-inventory) maps every original note to a topic summary.

## Evidence hierarchy

Distinguish implemented code, a passing local regression, exact-input release validation, deployed source, and production acceptance.
One level does not establish the next.
A source fingerprint must match checked inputs before reusing a result.
A deployed image digest must map to source before assigning it a local commit's behavior.
Missing output is not a pass.

For a bug fix, reproduce the failure and show the regression fails with the correction reverted.
The production PGWire path can differ from a direct local DataFusion context that repeats optimizer passes.
For a cost optimization, assert eliminated work as well as correct results.
A test that stays green with the fix reverted does not guard the claimed failure.

Separate simulated scheduling, local real-I/O behavior, and production economics.
A correctness benchmark run under compilation proves results, not uncontended latency.
An end-to-end count can hide wrong bucket placement; compare each expected bucket.
A process-wide memory peak includes setup and does not identify query heap usage.

## Comparable measurements

Record source/image, boot, uptime, project, query shape, range, cache state, and load window.
Use the same hours across days and a process aged at least two hours for maintenance comparisons.
Separate cold startup, warm repeats, and mature steady state.
A ladder of query windows warms later cells and cannot serve as an independent cold comparison.

Run production probes sequentially and bound raw scans.
Broad ad-hoc SELECTs can create the incident being investigated.
Do not restart production solely to obtain a measurement.
A deployment inside the observation window invalidates process-counter deltas.

Use paired ABBA then BAAB blocks for percentage claims.
Account for time correlation in confidence bounds.
A savings gate passes on its lower bound; a regression gate passes on its upper bound.
An inconclusive result needs more evidence, not a pass label.

## Read the counter's producer

Find every emitter before interpreting a counter.
One label once concealed four prefilter refusals; another hid three different rollup filter checks.
A zero can mean a path was never reached rather than a flag being off.
Zero resumed plus zero declined says less than either counter paired with its eligible population.

Keep these domains separate:

| Observation | What it does not establish |
| --- | --- |
| Plan-time selected files | Files decoded after runtime pruning or LIMIT |
| Pending estimated bytes | Compressed bytes on disk or physical rewrite work |
| Task completion | A physical commit, duplicate removal, or usable coverage |
| A superseded parent record | Live debt independent of its descendants |
| Zero age since commit | Useful work when the lane is empty |
| A generation tag | Valid source evidence or present output |
| Whole-process CPU/I/O | Cost of one lane without attributable phases |
| Lower median latency | A controlled improvement in p95/p99 or error rate |

When a query returns zero, inspect its assumptions about record shape.
A repair pass can have multiple outputs; filtering for one-in/one-out can incorrectly report no repairs.
A lifetime counter divided by a short process age can produce an enormous false rate.
An unprivileged failed `du` does not establish a directory's size.

## Decisions and corrections to retain

| Historical question | Settled lesson | Reopen only when |
| --- | --- | --- |
| August connection/query slowdown | Blocking fsync delayed runtime workers; residual age effects were episodic, not a simple monotone leak | New phase evidence identifies a current stall |
| Whole-day certification never grants | A churning day defeats unchanged-fingerprint accumulation; slice/file evidence needs its own producer and isolation proof | A current refusal is reproduced with exact scope |
| Read a different fingerprint after compaction | Physical change and logical change differ, but neither counts nor URI hashes alone prove equivalence | A stronger proof has parity and race coverage |
| Ledger held/proved mismatch | Publication ordering explained one-generation lag; the zero-disagreement gate was unreachable | A live consumer depends on a stronger invariant |
| Oldest task is 85 days old | Event-date age and eligibility horizon differ; old debt was not proof of current starvation | The unit is policy-eligible and its blockers are known |
| “28% full-file 1:1 rewrite lane” | Same-path DV re-adds were miscounted as Parquet rewrites | Physical new-output accounting shows real churn |
| Retraction eliminates the planner problem | It removed many buffered predecessors, but substantial physical wave work remained | Matched post-retraction work justifies another policy |
| Size ratio 4 reduces amplification | It missed the rewrite gate and multiplied waves/live files | A new candidate passes both write and read/file-count gates |
| Write-capture admission causes disk flood | Its removal hypothesis was rejected; write capture earned its cost | Current channel attribution changes the evidence |
| Foyer cannot reclaim blocks | September attribution was refuted; October found a specific zero-clean-block deadlock | Reproduce the specific recovery state rather than generalizing |
| A larger byte-admission gate protects reads | The tested gate never bound and did not improve the mixed workload | A current measured allocation is actually governed by it |
| Maintenance query-yield improves latency | Paired production comparison did not show benefit | New causal evidence demonstrates interference |
| CPU-token cap is the limiting resource | Tokens were free while state admission refused units | Current observations show a binding token ceiling |
| W46 key restriction improves dedup reads | Paired A/B missed the latency acceptance gate; the path was removed | New workload evidence justifies its complexity |
| Exact Parquet ranges reduce wall time | They moved fewer bytes but added GETs at comparable latency | The workload ceases to be round-trip-bound |
| More read-ahead is automatically better | The recorded A/B showed no gain | A measured I/O dependency chain changes |
| Session rollup has no consumer | That was true for an earlier version; v2 later served real queries | A current consumer inventory establishes disuse |
| All metrics need time-first order | The observed workload selects a metric then a window; metric-first won strongly | A new workload changes the access pattern |
| Publication batching fixes insert contention | Publications were below 1% of journal checkpoints in the measured window | Publication cost becomes material or another approved stage requires batching |

These are decisions about particular measurements, not universal claims about an engine or technique.
Keep the [read invariants](timefusion-reads.md), [maintenance contracts](timefusion-maintenance.md), and [recovery rules](timefusion-durability.md) alongside them.

## Work reduction before code speed

The September CPU campaign reported approximately 30% less CPU per row from four measured changes.
Its remaining buckets included system/library work rather than one obvious Rust loop.
The later fixed-server gate compared matched September 25 and September 30 hours.
It recorded 21,079 to 3,409 CPU-seconds per million ingested rows, with bounded or falling debt in the reported windows.
Neither figure is a current capacity guarantee or a transferable multiplication factor.

A faster unit still loses when reactivation, redundant preparation, or premature invalidation manufactures more units.
Measure commits, bytes, CPU, and useful coverage per original accepted ingest.
Retraction changes the flush-row denominator; report original ingest alongside flush rows.
Do not multiply gains from experiments with overlapping cost populations.

## Capacity and prior art

Early IO-free simulations appeared to support 10x and identified per-unit ceilings near much higher scales.
Later real-journal and arrival-model checks overturned several conclusions.
One synthetic arrival model generated about six times the real arrival rate.
Admission refusals, serialized oversized work, and unmodeled I/O also changed the apparent bottleneck.
A scheduler replay cannot prove sustainable ingest, disk endurance, query latency, or memory safety.

A capacity decision needs a current mixed read/write staging ladder and a stated resource model.
Report accepted throughput, backlog slope, freshness, latency distribution, failures, and limiting resource.
Treat x86-64-v4 and offload as conditional experiments with compatibility and rollback requirements.
Offloading maintenance adds durable ownership, OCC, networking, and failure recovery; a worker command does not solve them.
A large tenant can change both bytes per unit and the number of streams/partitions that mint work.

The prior-art research contributed these concepts:

- ClickHouse aggregate states avoid repeated raw aggregation, but exact updates still need duplicate/retraction handling.
- Druid separates ingestion rollup guarantees and resource allocation. Best-effort rollup cannot silently serve an exact query contract.
- Timescale-style materialized interiors plus raw tails suggest a useful hybrid shape, not a substitute for snapshot proofs.
- RocksDB/SILK illustrate bounded background work, flush protection, and predictable overload behavior.
- IOx/InfluxDB illustrate bounded compaction inputs and progressive time-ordered reads.
- Quickwit/Elasticsearch emphasize full-precision bounds and conservative handling of ties and unknown ranges.

These are historical design inputs. This consolidation did not recheck their current releases or claim their results transfer to TimeFusion.

## Experiment tools

| Tool | Retained purpose |
| --- | --- |
| [latest_bench.py](../../bench/latest_bench.py) | Actual client query shapes across projects/windows |
| [maintenance_task_flow.py](../../bench/maintenance_task_flow.py) | Paired event/journal transitions with explicit accounting gaps |
| [delta_work_ledger.py](../../bench/delta_work_ledger.py) | Physical Parquet/DV work separated from metadata re-adds |
| `timefusion sim` | Virtual-time scheduling under explicitly modeled arrivals, costs, permits, and policies |
| `timefusion run-unit` | A selected real maintenance unit, independently resource-bounded |
| `timefusion_stats` | Low-cost current-process diagnostics with restart-aware interpretation |

Keep query digests and exact-result oracles. Separate ambient counter differences from per-query attribution.
A lexical read-only guard is defense in depth, not a full SQL parser or permission boundary.

## Release evidence

Freeze related dependency pins and implementation changes before integrated release checks.
Run one Cargo process per cache and one local signoff at a time.
Use targeted `cargo nextest run` selections during iteration and `cargo lint` for linting.
Never replace required checks with a weaker command to obtain green output.
Before a master push, run `make ci-signoff` for the exact tree and retain commands, results, inputs, and logs.
Publish only passing attestations with the capabilities the environment actually provided.

A shard proves only its partition. Missing services and skipped tests remain explicit limits.
A flaky pass or nextest leak classification does not prove the underlying lifecycle problem is fixed.
A local Linux image probe and registry publication are separate actions.
Retain one coherent rollback requirement per release rather than expanding a checked batch with optional work.

The September macOS backtrace crash reproduced with an LLVM 15 linker override and passed with the platform linker.
The historical note records that diagnostic, not a permanent exemption from backtrace-enabled testing.
Future dependency/toolchain changes need their own evidence.

## Retrieving detailed historical evidence

The compact summaries replace the dated plans. No archive directory or per-note redirect files remain.
Original tables, raw test identifiers, designs, and chronology are available from the pre-consolidation source revision:

```sh
git show ad1ee10bfc7c76217924c9aa465a5c5d4f30392a:docs/plans/rollup-plan.md
git log -- docs/plans/
```

Substitute any original filename from the review inventory in the `git show` command.
The original revision contains the four September files retired earlier as well as the remaining dated notes.
New observations belong in the current review/backlog. Stable concepts belong in these four topic references.

## October 21 dependency applicability

The isolated candidate includes compatible advisory updates and the required cursor protocol correction.
Its final audit checks lockfile SHA-256 `2a930bdf13ffe14540eedfed11723150c52d4e008871ba76a87e86b62a3f3b92`.
The pinned cursor fork is `1388ea37ec8eeefd13b07771f8d0052f164ae7f1`.
The OSV scan returns 22 matched package versions and 34 records, with 28 groups after alias deduplication.
Seventeen matched versions appear in the Linux normal/build graph. Build dependencies and maintenance notices remain distinct from runtime triggers.
The registry package set matches the earlier candidate scan. A Git source change still requires fresh integrated validation.

Owner for the following dispositions: TimeFusion dependency maintainers. These are retained follow-ups, not a clean vulnerability report.

| Remaining package / advisory | Observed path or trigger | Disposition |
| --- | --- | --- |
| `h2 0.3.27`, RUSTSEC-2026-0258 | The legacy AWS HTTP client retains this normal dependency. The advisory concerns unbounded empty DATA frames | Open: migrate the legacy connector or backport its fix separately. Updating `h2 0.4` does not fix this branch |
| `rustls 0.21.12`, RUSTSEC-2026-0346 / 0349 | The AWS legacy connector retains this version. Session Debug disclosure requires the affected session formatting. The downgrade report concerns supported key-exchange state | Open: trace selected connector features and session-cache logging before an AWS/TLS migration. Do not infer post-quantum support from the broad affected-version range |
| `rustls-webpki 0.101.7`, RUSTSEC-2026-0098 / 0099 / 0104 / 0382 | Legacy Rustls retains certificate validation code. These cover name constraints, CRL parsing, and distrusted intermediates | Open: retain this branch in the TLS migration scope. The updated `0.103.16` branch does not remove it |
| `aws-smithy-json 0.61.9`, GHSA-8ffr-xgwf-xj56 | `aws-sdk-s3 1.119.0` retains the old parser. The advisory describes recursion in generated Smithy servers | Open: inspect client response parsing in the SDK migration. TimeFusion exposes PGWire/gRPC rather than a generated Smithy server. That difference alone is not proof of parser safety |
| `lru 0.12.5`, RUSTSEC-2026-0002 and its GHSA alias | Tantivy and AWS S3 retain this version. The defect concerns `LruCache::IterMut` | No affected iterator call identified in the inspected cache consumers. Recheck their complete call graph before closure or a dependency migration |
| `lru 0.12.5 / 0.16.4`, RUSTSEC-2026-0253 | The trigger requires a stored key's `Drop` to panic during `pop`, followed by continued cache use | Inspected keys use strings, paths, integers, and tuples of ordinary reference-counted strings. No panicking key destructor was identified. Retain the report for dependency maintenance |
| `rkyv 0.7.46`, RUSTSEC-2026-0235 | Walrus and rust_decimal retain this version. The defect requires conflicting metadata for archived shared pointers | No archived Rc/Arc appears in the inspected WAL metadata/index shapes. Keep the format unchanged in this batch. Any migration needs old-WAL compatibility and restart proof |
| `quick-xml 0.26.0 / 0.39.4`, RUSTSEC-2026-0194 / 0195 | Inferno uses XML output. Object-store code uses XML for storage responses. The defects concern duplicate attributes and namespace allocation | Open: inspect each parser entry point and input bound. Do not classify both paths as public XML ingestion |
| `bincode 1.3.3 / 2.0.1`, RUSTSEC-2025-0141 | Maintenance notice, including persisted-data consumers | Defer replacement to a format-compatible maintenance batch. The notice supplies no reason to rewrite live WAL bytes |
| `dotenv`, `instant`, `paste`, `proc-macro-error2`, `rustls-pemfile`, `unsafe-libyaml` | RUSTSEC-2021-0141, 2024-0384 / 0436 / 0449, 2025-0134, 2026-0173 are maintenance notices | Retain dependency replacement work. Build-only applicability is not a runtime exploit claim |
| `quinn-proto 0.11.15`, RUSTSEC-2026-0338 through 0342 | Absent from the selected Linux normal/build graph | Excluded from this runtime assessment. Recheck before enabling HTTP/3 or changing feature unification |
| `rsa 0.9.10`, RUSTSEC-2023-0071 | Absent from the selected Linux normal/build graph | Excluded from this runtime assessment. Retain development-feature applicability |
| `scc 2.4.0`, RUSTSEC-2026-0205; `tokio-tar 0.3.1`, RUSTSEC-2025-0111 and its GHSA alias | Absent from the selected Linux normal/build graph | Excluded from this runtime assessment. Recheck test/tooling exposure separately |
| `webpki-root-certs 1.0.7`, RUSTSEC-2026-0383 | Absent from the selected Linux normal/build graph | Excluded for this target. Recheck platform-specific certificate features before changing the deployment target |

The candidate fixes the compatible current branches. It does not resolve every retained advisory or maintenance notice.
Do not add audit suppressions or change persisted formats to obtain a green count.
Final release evidence must identify the checked lockfile, selected features, image, rollback image, and passing local signoff.
Raw scan and graph evidence: `/tmp/timefusion-advisory-refresh-2026-10-21/{final-candidate,frozen-pin-audit}/`.
