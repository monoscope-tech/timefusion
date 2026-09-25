# Rollups on a fixed server: incremental implementation plan

The [first-deployment checklist](2026-09-25-first-rollup-deployment.md) defines the initial release scope, validation gates, canary, and rollback requirements.

Date: 2026-09-24. Status: implementation in progress, not deployed. Code baseline: `08d34190`. This revision incorporates review of the existing coverage work and recent production incidents.

The objective is lower total CPU, memory, and I/O per accepted event, without a larger server. The primary target is less total server work at the same accepted traffic and query mix. More concurrency, deferred backlog, and shorter scan duration alone do not establish a saving. The first steps attribute current work, stop unused builds, and size scan batches by bytes.

Coverage reconciliation follows those low-cost experiments. Hour-level invalidation remains the initial implementation. Minute-level invalidation requires a measured benefit. Historical source files do not require a bulk format migration.

The target execution path is: classify the change, preserve unaffected coverage, read required input once, resolve versions once, and share winning rows. Compatible aggregates consume that stream. Batched publication persists their independent results and coverage. Certified clean input bypasses version resolution.

Dirty input retains complete winner and tombstone semantics. Every optimization must reduce measured total cost without weakening correctness, freshness, durability, or resource limits. All numerical gates are proposed acceptance thresholds, not measured savings. The experiment protocol below applies to every gate.

## Execution priorities

The stage names identify design sections, not a mandatory serial implementation order. Attribution precedes every behavior change.

| Order | Deliverable | Saving to measure | Cost | Evidence and decision |
| --- | --- | --- | --- | --- |
| First | One-hour work attribution and tier-usage inventory | Establish the attainable whole-server saving | Low | Current rollup CPU share is unknown |
| 1 | Pause unused session builds and review unserved HLL | All build work for a genuinely unused tier | Low–medium | Historical query was unsupported. Current client shape and other consumers need checks |
| 2 | Byte-aware rollup batches | Per-batch CPU overhead | Low | Existing `batch_rows_for` and September 15 batch-cost evidence |
| 3 | Stage 0 coverage reconciliation | Repeated remints and unnecessary rebuild scope | Medium | Production evidence. Existing-hybrid race regression is a rollout gate |
| 4 | Stage 1A certified-clean path | Repeated winner selection | Medium | Existing certificates. Qualifying traffic share remains unknown |
| Later | Stage 1B bounded single-pass execution | Repeated hash-shard input work | Medium–high | Rank from executed-unit shard exposure before experiments |
| Later | Stage 1D dependencies and stage 3 batching | Irrelevant rebuilds and commit overhead | Medium–high | Rank from mutation and publication attribution |
| Conditional | Stage 1C shared scans | Duplicate preparation across useful consumers | Medium–high | Defer dashboard/session sharing while sessions has no consumer |
| Conditional | Stages 2, 4, 5, and 6 | Remaining visibility, ingestion, fusion, or repair costs | High | Proceed only after measured residual cost justifies each change |

## First deliverable and rollout boundaries

The first deliverable is a measured baseline and two independent candidates: unused-tier suspension and byte-aware batches. Neither requires the coverage redesign. Local implementation and correctness tests can proceed during attribution. Production activation requires the baseline and the candidate's acceptance gates.

The session tier has no demonstrated routing benefit for the intended client query. That is not proof that all consumers are absent. The usage inventory must include direct table readers and less frequent queries before suspension. Pausing sessions also defers dashboard/session scan sharing. Removing an unserved HLL measure must preserve useful measures and stored-schema compatibility.

The rollout decisions are independent:

- Suspend a tier only after consumer checks, durable pause tests, and safe in-flight completion.
- Activate byte-aware batches only after CPU, memory, cancellation, and foreground-latency gates pass.
- Activate stage 0 only after the existing-hybrid race regression and empty-range recovery tests pass.
- Keep later optimizations conditional on their measured share of remaining server work.

The evidence appendix records local candidates and diagnostic samples separately from accepted results. No candidate or startup sample establishes production savings.

### Immediate work queue

Release validation uses frozen implementation batches. Passing checks remain valid only while their source, dependency, toolchain, and configuration inputs remain unchanged.
The first resource-safety release has three local commits, including the requested workflow rules. Its reader-only spill patch passed 47 tests and strict Clippy.
Those results precede an additional required quota-error fix. A real-file regression reproduced retained disk charges after rejected growth and file deletion.
The candidate now records the file charge before returning the quota error. All 114 selected execution and spill tests passed.
Strict Clippy and the full lint suite remain pending for this expanded patch.
The remaining dependency checks exposed a local license-tool mismatch, not a Rust defect. The CI-pinned tool passed without source changes.
The [deployment checklist](2026-09-25-first-rollup-deployment.md#batched-release-validation) records the remaining checks and the combined validation boundary.

1. Capture steady-state attribution and the executed shard-count inventory without additional production source scans.
2. Record each tier's current query consumers, routing eligibility, and maintenance cost.
3. Evaluate session suspension and byte-aware batches independently, using the release gates below.
4. Complete Stage 0's shared coverage proof and race tests before its production experiment.
5. Rank later work by measured avoidable CPU and I/O, implementation cost, and correctness risk.

The session pause removes future work, not stored history. Unserved HLL removal remains a separate schema change. Neither requires the full publication architecture.

### Durable pause policy candidate

The isolated worktree now contains `RollupBuildPolicy` with explicit `Paused` and `ResumeFrom` states. The candidate stores policy changes in the existing journal. Policy-bearing snapshots use version 2. Journals without policies retain version 1.

Admission checks cover enqueue, invalidation, direct claims, and restart retries. A paused parent also prevents derived builds. Resume requires a grain-aligned boundary and does not authorize earlier history. Existing in-flight work can finish. These policies do not certify output or change query routing.

The new tests cover WAL replay, snapshot recovery, dependency admission, and bounded resume. All 172 coordinator tests passed in 6.357 seconds after a 5m12s build. Formatting initially failed and was corrected with `cargo fmt`.

The persistence tests exposed a resume bug. Both open and fsync failures left the in-memory policy executable through `mark_running`. Both regression cases failed at that assertion. The candidate now blocks admission while the tier or parent has unpersisted policy changes. Successful checkpoint or compaction restores the claim index.

The regression table covers both failure types and both persistence recovery paths. All 176 coordinator tests passed in 5.616 seconds after a 4m33s build. This result includes the four persistence-recovery cases. Operator integration and consumer checks remain incomplete. No production policy changed.

The statistics path still counts paused tasks in eligibility gauges. Admission blocks these tasks, but the gauges can misrepresent runnable work. `claim_next` also reads frontier lag to allocate sealed-work turns. Paused frontier debt can therefore reduce capacity for useful sealed work.

The regression run confirmed the eligibility error: paused pending work reported one eligible rollup instead of zero. The in-flight case passed. The candidate now excludes paused, non-running tasks from eligibility, age, and frontier-lag calculations. Pending counts and backlog bytes remain unchanged. In-flight work remains active until it drains.

All 178 coordinator tests passed in 5.297 seconds after a 3m29s build. This includes both paused-task metric cases.

Backfill integration must precede activation. The database regression confirmed that enqueue-only policy checks were insufficient: backfill admitted two cells while every raw-derived tier was paused. The candidate now applies policy before tier inspection and raw-hole derivation. It also excludes paused, non-running tasks from the planner queue ceiling. Dedup required by active tiers or ingestion remains enabled.

The new `rollup_build_window` helper computes the permitted part of a planning interval. Existing task claims must fit that interval completely. A parent resume inside an hourly child bucket permits the next full hour, not the partial bucket. All 183 coordinator tests passed in 5.615 seconds after a 4m18s build. This includes five new interval and claim-boundary cases. The planner now uses this helper.

A database regression uses real source writes to exercise planner admission. It requires zero new raw work or tier inspection while all raw-derived tiers are paused. A bounded session resume must admit only the permitted suffix, without building other tiers or earlier dates. A forced damage repair outside the policy must retain its cursor. The failing run took 3.760 seconds after a 4m12s build.

The planner rechecks permitted intervals under the journal lock before it derives raw work. It also rechecks forced-repair policy before cursor advancement. A blocked repair prefix remains pending and does not repeatedly force allowed tiers. The database regression passed in 1.911 seconds after a 4m31s build.

All 201 selected coordinator, backfill, damage-repair, per-tier, and cross-source tests passed in 9.596 seconds. `cargo lint` then rejected one `let…else` that returned `None`. The candidate now uses `?`, without suppression. A fresh lint result remains pending.

The operator-control candidate uses the existing authenticated pgwire admin path and the live journal. It does not open a second journal writer. `ROLLUP POLICIES <source>` reports each declared tier, its parent, and its local override. The status distinguishes no override, a pending write, and a durable override. A parent policy still applies when a child has no local override.

The candidate accepts `ROLLUP PAUSE <source> <tier>` and `ROLLUP RESUME <source> <tier> FROM '<RFC3339>'`. Resume requires an explicit timestamp and the existing grain-alignment check. The parser rejects submicrosecond input instead of truncating it. Compilation found three namespace errors in the test-case attributes. Crate-qualified enum paths replaced the incorrect `super` paths. All 193 selected parser and coordinator tests then passed in 5.284 seconds after a 3m20s build.

A new pgwire lifecycle test uses a real authenticated listener and three database instances over one temporary journal. It checks policy inspection, invalid-command rejection, pause, bounded resume, and restart recovery. It also reads the durable journal before shutdown, so shutdown cannot hide an unpersisted acknowledgement. The lifecycle test passed in 0.520 seconds after a 3m18s build.

A later combined run reused the main worktree's test binary from the shared target directory. Its 175 passing tests do not establish candidate correctness. Lint also resolved the main worktree's library and reported missing candidate symbols in the benchmark. A forced candidate rebuild then passed all 212 selected tests in 6.535 seconds after a 2m32s build. This selection includes coordinator, backfill, damage-repair, per-tier, cross-source, parser, and pgwire lifecycle tests. Consumer checks and production acceptance gates remain incomplete.

Operator use remains gated by the consumer inventory and production acceptance checks. The controls use pgwire's simple-query protocol, like the existing administrative commands. They are not prepared statements. Policy inspection reports local overrides, not effective permission: a paused parent still blocks a child with no override.

After a failed policy command, inspect its status before retrying. A pending override blocks admission until persistence succeeds. Repeating the same command retries persistence. A durable override can survive an error from later snapshot compaction because its WAL record already exists. A command error therefore does not prove that the previous policy remains active.

An older binary cannot read policy records or version-2 snapshots. Binary rollback requires a policy-aware reader or a separately tested migration. Deleting policy metadata to permit rollback is not safe: it can resume unwanted historical builds. The current candidate does not implement that migration.

Fresh candidate lint passed in 2m03s after the forced rebuild. The command was `cargo lint`, with the shared target directory selected explicitly. The focused Rust review found no new suppression or weakened admission checks. These local results do not establish production savings or complete the coverage and visibility stages.

### Release decisions for the first changes

Each candidate has its own experiment and rollback decision. Do not combine tier suspension, batch sizing, and coverage changes in the first comparison.

| Candidate | Evidence required before activation | Success measurement | Stop condition |
| --- | --- | --- | --- |
| Pause sessions | Current query-shape check, consumer inventory, and durable pause/resume tests | Zero new session builds after draining, plus lower combined query and maintenance CPU | A required consumer loses its latency target or paused tasks return |
| Byte-aware batches | Narrow/wide input, cancellation, and memory checks | At least 10% lower build CPU under the shared protocol | Memory admission fails, flush stalls increase, or foreground p99 exceeds the regression gate |
| Coverage reconciliation | Shared range proof, empty-range recovery, and hybrid-race regressions | Stage 0 scan, remint, and eligible-hit targets | Any coverage consumer accepts stale or incomplete evidence |

Unserved HLL removal is a separate schema decision, not part of the session pause. Batch sizing changes execution, not materialization policy. Stage 0 changes coverage decisions, not ingestion durability.

## Before changes: attribution and experiment protocol

Read-only production snapshot at 18:23 UTC on 2026-09-25: the host had 48 logical CPUs and load averages of 68.88, 59.99, and 63.59.
Two one-second `vmstat` samples showed 10–12% idle CPU and 0% reported I/O wait.
Docker reported 3,375.96% Timefusion CPU, approximately 33.8 cores, against a configured 44-core quota.
Container memory was 51.91 GiB against a 120 GiB limit. The host had approximately 99.9 GiB available memory.
The container remained healthy on image `sha256:84504e8dc915bca8d8ff1e00102c1a64f246165b713d26033c55e7ad3989387b`, with one replica and stop-first updates.
Monoscope service discovery still returned HTTP 500. The snapshot used the authorized SSH path and changed no production state.
These short samples establish current CPU pressure, not maintenance attribution or a matched performance baseline.
Docker block-I/O totals are cumulative, so they cannot establish current throughput.
From 18:25:31 to 18:26:31 UTC, cgroup CPU usage increased by 1,707.576 CPU-seconds, approximately 28.46 cores on average.
The throttled-period and throttled-time counters did not increase. Memory-limit and OOM event counters also remained unchanged.
Thus, the configured CPU quota was not the immediate constraint during this interval. These counters do not attribute CPU to maintenance tasks.

First, capture a representative hour after process uptime exceeds one hour and coverage replay completes. This is an attribution baseline, not a performance verdict. Report CPU-seconds and whole-process CPU share for each rollup specification, deduplication, compaction, ingestion, queries, and metadata work. Record read/write bytes, commits, useful publications, retries, discarded output, and backlog growth alongside CPU. Leave unexplained CPU explicitly unattributed.

`timefusion.lane` identifies commit provenance, not CPU consumption. Shared async workers also prevent a simple thread-to-operation mapping. Use sampled stacks or task-aware attribution, then reconcile the result with process CPU. Separate raw-query savings from maintenance savings. If rollup maintenance consumes 10% of CPU, its direct elimination saves at most 10%. Broader targets require independently measured query or ingestion savings.

For every percentage gate, use at least two counterbalanced four-window blocks: ABBA, then BAAB. Each window lasts 60 minutes after warmup. A is the baseline and B is the candidate. Use identical replay input or matched tenant, query, mutation, cache, and backlog conditions.

These blocks counterbalance order effects but do not alternate every window. Use the same predeclared block order for comparable candidates. Separate the initial attribution hour from these acceptance windows.

Reset isolated replay state between windows. On production, record carryover and reject unmatched comparisons rather than erase real data. If safe rollback is unavailable, use isolated replay instead of alternating incompatible production binaries.

The minimum eight windows do not guarantee statistical power. Predeclare the primary metric, query cohorts, confidence method, and sample requirement. Use paired block comparisons and a 95% confidence interval that accounts for temporal correlation. Do not treat every query as independent.

A savings gate passes only when its lower confidence bound meets the threshold. A regression gate passes only when its upper bound stays within the limit. Sparse p99 cohorts need more observations, not a favorable whole-process percentile. If uncertainty crosses a gate, extend the experiment or report it as inconclusive.

CPU comparisons include work deferred beyond each window. A lower visible load with growing debt fails the experiment.

For a saturated server, CPU utilization can remain unchanged after a useful optimization. Compare CPU-seconds per accepted event at matched query volume. Also report completed useful work, oldest pending work, and backlog growth. Separate temporary backlog drainage from steady-state capacity.

Each report records the source commit, deployed image, configuration, uptime, workload counts, and backlog at both window boundaries. It separates total CPU-seconds from CPU per accepted event. It also reports query volume and mix. A lower event rate cannot count as an optimization.

Savings from consecutive stages use the remaining cost as their denominator. The final combined experiment determines whole-server savings. Neither summed stage percentages nor estimated scan bytes establish additional server capacity.

### First change: stop materialization without a consumer

The schema declares `sessions_1h_v1`, with a historical comment about an unsupported `max(concat(timestamp, ..., url))` aggregate. Current client SQL differs, as recorded in the implementation learning. Check the current query shape against the stored measures and routing matcher. Check actual routed queries and direct tier readers over a representative usage horizon. One quiet hour alone does not establish absence of consumers.

If there is no consumer, pause session build admission, census reminting, derived dependencies, and retries through an explicit durable policy. Let existing in-flight publication finish safely, or cancel it through the normal recovery path. Preserve stored data and query semantics. Paused coverage remains subject to invalidation.

The current journal has no tier-policy record. The implementation must cover `enqueue_inner`, `invalidate_slices`, `claim_next`, `claim_exact`, and direct `mark_running` calls, not only planner filtering. Invalidation inserts tasks directly, so an enqueue-only gate is insufficient. Snapshot compaction and WAL replay must preserve the policy. Tests must cover reminting, retries, manual claims, restart, and safe completion of work already running. A retry-reason string or source-cursor sentinel is not a policy record. Older binaries must not silently ignore a compacted pause policy and restart builds.

Restart and resume cannot reactivate stale output or recreate a paused backlog automatically. Resume only after an end-to-end test proves the intended query routes correctly and benefits from the tier. The exact client change needs a supported stored measure and correct latest-row semantics, not merely a different aggregate name.

Resume must carry an explicit, grain-aligned time boundary. Earlier holes remain discoverable but require separate backfill authorization. The policy must constrain recovered retries as well as newly planned work. Startup's `requeue_running` transition cannot bypass it. Policy changes do not certify existing output or reset source revisions.

`service_name_hll` is also explicitly unservable in `MEASURES_NOT_YET_SERVABLE`. Review its build cost now, not in stage 6. Stopping that measure needs schema/generation compatibility and raw fallback. It must not cause blanket historical rebuilding or disable useful dashboard measures.

The gate is zero new paused-tier builds after in-flight work drains, lower attributed work, and no consumer regression. Existing rebuild and hit/miss stats support this gate. Task/table identities distinguish specifications without new permanent counters. Measure total query and maintenance CPU together: increased raw fallback can offset avoided builds. Report session savings separately from HLL savings, with no assumed whole-server percentage.

### Early Stage 1 experiment: byte-aware batches

At baseline `08d34190`, `bounded_rollup_maintenance_context` hardcodes 256 rows. `batch_rows_for` already selects 256–8192 rows from decoded width and a target byte size. The default-off local candidate reuses that helper with the existing maintenance byte target. Supply consistent projected-byte and row estimates for the same input population. Unknown or zero-width estimates retain the conservative fallback. Increasing rows must not increase admitted task concurrency or its memory budget.

Compare the current setting with byte-aware batches, including narrow and wide schemas, sparse input, and oversized units. The [September 15 campaign](2026-09-15-cpu-per-byte-campaign.md) supports this experiment, but its combined gains do not predict rollup batch-size gains. The gate is at least 10% lower build CPU, bounded memory/spill, and no more than 5% foreground p99 regression under the shared protocol. Include cooperative-yield and cancellation tests.

This experiment needs no coverage rewrite, minute ledger, or new visibility protocol. Existing scan-duration and flush-stall stats support rollout. Process CPU and benchmark memory measurements decide the gate.

The local smoke command is `cargo bench --profile dev --bench rollup_work -- 8193 batches`. All eight ABBA/BAAB arms passed on the dirty `d9f00cce` worktree. Each returned 7,711 live rows from 8,193 inputs, with 13 scans, three output rows, three output files, and three commit actions. Candidate arms recorded three publications with batches larger than 256 rows. Baseline arms recorded none. Certified aggregation remained disabled in both arms.

Build CPU was 1.91–1.95 seconds for fixed batches and 1.87–1.95 seconds for byte-aware batches. These overlapping ranges do not establish the 10% saving gate. Estimated scan bytes were 404,763 and 404,676 respectively, not measured physical I/O. Both arms included a separate legacy certification sweep, which cost 17.72–18.35 CPU-seconds. That setup cost does not measure production certificate reuse.

This small, unoptimized fixture crosses batch boundaries but does not establish behavior under saturation. Peak memory, wide inputs, cancellation, foreground latency, and sustained backlog remain unmeasured. Independent prefixes also differ in file identities and write stamps. The candidate remains disabled by default.

## Evidence and existing behavior

Base rollups currently read raw files, resolve row versions, compute aggregates, and publish replacement output. The private maintenance session uses one DataFusion partition and batches of 256 rows. Flush deduplication covers buffered rows only and preserves tombstones. Cross-flush duplicates prevent unconditional aggregation of every flushed batch into additive states.

| Commit | Recorded failure | Consequence for this plan |
| --- | --- | --- |
| `4f84b9d7` | September 21: 928 base scans, 183.6 GB, and 608 worker-minutes in 25 minutes | Preserve the covered-by-wider check that now precedes expensive work |
| `dcef9932` | Coverage recovery created enough maintenance commits to starve flush commits | Preserve ingest-health admission and account for shared object-store capacity |
| `29d249d7` | One surviving slice incorrectly made an incomplete date appear covered | A nonempty slice map cannot establish full requested coverage |
| `95d3311e` | 12,255 no-op skips versus 420 rebuilds while hit rate remained near 1% | A skip must restore usable coverage, not merely prove one slice unchanged |
| `10f1bba0`, `4ca85774` | Stalled coalesced flushes and oversized commit batches | Aggregate publication must not add unbounded work to flush completion |

These are historical, pre-fix observations from commit messages, not a current production baseline. The September 24 investigation's persisted task-journal snapshot showed 26 demo metric tasks among 28 running base tasks. Neither snapshot distinguishes all missing historical coverage from repeated invalidation. Worker-minutes and scan-phase duration include waits. They must not become estimates of occupied CPU cores.

September 13 measurements recorded closed-hour log row reductions of 548:1 for Shipbubble and 162:1 for the Bitnob-associated project. Shared demo logs achieved 74:1. These ratios support compact states but do not establish per-flush savings or metric-state cost.

## Prior art: ideas to borrow and limits

| System | Borrowed idea | Limit for Timefusion |
| --- | --- | --- |
| [ClickHouse incremental views](https://clickhouse.com/docs/concepts/features/materialized-views/incremental-materialized-view) | Aggregate new blocks into mergeable states | Insert triggers do not repair arbitrary source replacements or cross-block duplicates |
| [ClickHouse projections](https://github.com/ClickHouse/clickhouse-docs/blob/main/docs/data-modeling/projections/2_materialized-views-versus-projections.md) | Explicit ownership between source parts and derived data | Restrictions around `FINAL` expose the difficulty of deduplication across parts |
| [Timescale continuous aggregates](https://github.com/timescale/timescaledb/blob/main/tsl/src/continuous_aggs/README.md) | Track changed ranges and delay materialization of the active tail | Repairs still read source data. Transaction-wide minimum/maximum ranges can over-refresh |
| [InfluxDB IOx](https://www.influxdata.com/blog/compactor-hidden-engine-database-performance/) | Combine related work in one scan and reconcile only overlaps | Separate compactor servers are outside our resource budget |
| [InfluxDB downsampling](https://docs.influxdata.com/influxdb3/core/plugins/library/official/downsampler/) | Bounded windows and explicit backfill batches | Scheduled downsampling does not automatically correct arbitrary mutations |
| [Druid rollup](https://druid.apache.org/docs/latest/ingestion/rollup/) | Aggregate during ingestion and merge states later | Best-effort means split aggregate groups, not permission to double-count duplicate events |
| [Thanos compaction](https://thanos.io/tip/components/compact.md/) | Stable blocks and progressively coarser states | Long default delays do not suit recent dashboards. Extra resolutions also consume storage |
| [Pinot upserts](https://docs.pinot.apache.org/build-with-pinot/ingestion/upsert-dedup/upsert.md) | Track logical row identity | Primary-key maps consume memory, and upsert tables cannot use star-tree preaggregation |
| [Materialize aggregates](https://materialize.com/docs/sql/explain-plan-operators/) | Maintain results through logical changes | Mutable min/max can retain substantial input state. A general differential engine is too large a first step |
| [DataFusion execution metrics](https://datafusion.apache.org/user-guide/metrics.html) | Inspect operator work, spills, and aggregation cost | Available metrics depend on our pinned version. Operator timers do not replace process CPU measurements |

IOx's non-overlap design supports a specific improvement: reuse our existing clean-window certificates before a rollup repeats deduplication. [Timescale refresh policies](https://github.com/timescale/docs/blob/latest/use-timescale/continuous-aggregates/refresh-policies.md) exclude the active bucket to reduce repeated refreshes. Timefusion already has quiet-period scheduling. Its next experiment must balance repeated builds against extra raw-tail query work.

## Map the design onto existing code

| Existing mechanism | Decision | Required change |
| --- | --- | --- |
| `rollup_slice_coverage` | Keep and extend | Preserve independently valid ranges and their generation, measure, and output evidence |
| `rollup_coverage` | Keep as a derived summary | Derive usable date coverage from the same validated ranges that routing accepts |
| `rollup_source_epochs` | Keep initially, then extend | Retain date epochs and attach sparse affected-range evidence before introducing finer revisions |
| `rollup_dirty` and its `u32` hour mask | Keep initially | Subtract dirty hours from candidate coverage without assuming the rest is valid |
| Bounded witness, `source_fp`, and `content_fp` | Keep correctness gates during transition | Recognize certified logical equivalence explicitly. Unexplained changes remain unservable |
| `rollup_ticket_current` | Keep, then strengthen | Share the range proof now. Later bind tickets to a captured source view |
| `rollup_journal.rs` and `TaskJournal` | Reuse | Retain current durability until source replay carries equivalent invalidation evidence |
| Existing coverage replay and staged publication | Reuse | Extend their format and recovery checks instead of creating a parallel ledger service |
| `dedup_window_certified` and clean-slice evidence | Reuse earlier | Check the exact build snapshot before bypassing version resolution |
| Hash-shard loop in `run_coordinator_rollup_selected` | Measure, then replace where costly | Avoid repeated source decoding merely to bound aggregate memory |
| Per-specification build tasks | Keep publication identities | Share compatible input scans and winner selection without coupling successful publications |

The main integration points are `src/database/maintain.rs`, `src/database/rollup.rs`, and `src/rollup.rs`. Source visibility also crosses `src/database/write.rs`, `src/write/mod.rs`, `src/write/wal.rs`, and `src/dml.rs`. Reuse `carry_dedup_witness`, called after landed deduplication in `dedup_partition_range_limited`, and existing bounded-witness routing checks. These already preserve some coverage across physical changes; stage 0 must not build a second carry mechanism.

## Dependencies, not a serial rewrite

- Attribution precedes behavior changes. Pausing unused work and the early Stage 1 batch experiment can ship before stage 0.
- Stage 0 requires the existing-hybrid race regression described in stage 2, plus empty-range recovery and agreement between coverage consumers.
- Stage 0 does not require stage 2's new visibility protocol unless the regression reveals a bug that needs it.
- Stage 1 measures remaining work. Stages 1A–1D remove repeated work before minute tracking or flush-path changes.
- Stages 1A and 1B can ship independently after stage 0, with existing snapshot and physical checks.
- Stage 1C requires two useful compatible consumers and measured duplicate preparation. Pausing unused sessions removes its current dashboard/session justification.
- Stage 1D first preserves physical gates. Logical reuse across changed fingerprints requires stage 2's evidence and visibility contract.
- Stage 2 reuses the stage 0 hybrid regression. The new visibility protocol is a separate feasibility decision.
- Stage 3 publication batching can proceed after stage 0, alongside stage 2, using existing physical proofs and journals.
- Stage 3's logical-evidence activation requires stage 2. WAL migration also requires compatible readers and recovery tests.
- Stage 4 activation requires both stage 2's visibility proof and stage 3's bounded publication queue.
- Stage 5 can ship with existing source validation and stage 3 batching; it does not require logical-only routing.
- Stage 6 minute revisions require stages 1 and 2. Existing hourly derivation does not wait for them.
- Stage 6 layout and broader materialization-policy experiments do not require minute revisions or logical-only routing.

If stage 2 fails its resource gate, keep physical freshness checks and defer logical-only routing and stage 4. Coverage repair, clean-window aggregation, bounded scans, shared input, and publication batching remain useful deliverables.

## Stage 0: reconcile coverage without changing ingestion

This stage ships without the later architecture, subject to attribution and the existing-hybrid regression gate. It keeps current hour masks and physical checks. It introduces no new WAL format and no flush-time aggregation.

One metadata-only calculation returns validated covered ranges and holes for a requested interval. Its consumers are `plan_rollup_backfill`/`readable_cells`, the no-op decision in `run_coordinator_rollup_selected`, `rollup_ticket_current`, and `rollup_rewrite_for`. The date summary uses the same proof. Consumers may request different intervals, but must not define different meanings of coverage. It checks generation, measures, live output, dirty ranges, and current source evidence. It preserves valid disjoint slices without declaring the entire date covered.

If existing evidence proves coverage, the no-op path restores the corresponding durable metadata without another raw aggregate scan. If the evidence proves only some ranges, the census schedules only the remaining holes. Existing persistence protects the metadata repair across restart.

`RollupCoverage` stores one generation, but independently built slices can have different generations. A reconciled proof must retain each range's actual generation and output evidence. It must not invent a whole-day generation or copy one slice's generation across other slices. The census and no-op path can accept a complete union without requiring a synthetic legacy day entry. Routing must preserve the association between each valid range and its permitted generations.

This association requires a SQL change, not only a richer metadata result. The existing router collects generations by project and date. Its SQL accepts each generation across every covered interval on that date. A local regression left obsolete output physically present beside its replacement. Both full-rollup and hybrid SQL returned 103 instead of 3. This demonstrates the missing selection boundary, not evidence that production currently contains this exact layout.

`generation_id` identifies the materialized schema and measure set. It deliberately excludes the source fingerprint, so successive source revisions can share the same generation. The range regression uses different generations. It does not establish exclusion of obsolete output from a same-generation repair. Empty coverage must authorize no output rows, rather than authorize a generation across its empty interval. Recovery must also prove that older output was retired or excluded. Generation identity must not replace publication identity or source-revision evidence.

The local candidate carries `GenerationRange` from day or slice selection into the SQL predicate. Each generation supplies only states in its proven source interval. Bucket-start timestamps require rounding the lower predicate to the stored grain. The independently computed query interiors still exclude incomplete buckets.

Adjacent ranges merge only within the same project, date, and generation, without bridging holes. This bounds redundant predicates without widening coverage. Long-window experiments must include planning CPU and generated predicate count. The candidate also checks empty replacements that have no physical rows. Range predicates cannot split an aggregate state. A correction inside an hourly bucket still requires replacement of that complete hour.

This change performs no source scan, file rewrite, or metadata commit. It is a prerequisite for preserved partial coverage and legacy packed-file reuse. The first candidate passed 141 rollup and hybrid-view tests in 162 seconds. The final candidate passed all 144 selected tests in 126 seconds, including coalescing and empty replacements. Formatting and whitespace checks passed. These are correctness results, not performance measurements. Shared proof reconciliation, empty publication recovery, and the scheduling change remain incomplete.

This deliberately replaces `95d3311e`'s rule, "no day coverage, no skip", with "no validated range proof, no skip". A full UTC day requires a union covering all 24 hours, with each interval valid or explicitly proven empty, and no dirty hour. For the current day, certify only the requested closed prefix; do not claim future hours. Disjoint valid ranges remain useful even when no full-day summary can be restored.

The primary scan saving is this: after a scoped invalidation, rebuild dirty hours and genuine holes, not the other proven hours of that date. Subtracting dirtiness alone is insufficient: surviving slices must still pass source-witness checks. If those checks cannot prove an unaffected slice, keep it unknown; stage 0 must not silently assume range-level logical revisions.

Empty output already has a representation: the build inserts slice coverage and checkpoints a `TaskJournal` publication with `rows = 0`. It can write no Parquet Add, so Add-tag recovery alone cannot recover that proof. Stage 0 must recover verified zero-row publications from the existing journal and reconcile them with later invalidations and replacements. If existing records lack required generation, measures, or source evidence, extend that existing record or leave the range unknown.

No output file, no task, and `output_files = 0` alone never prove emptiness. A proven empty replacement must also retire or exclude older output for its range. Legacy gaps without evidence need a bounded repair once, not repeated full-day scans or a fabricated empty certificate. This empty-proof work belongs to stage 0, not stage 3.

Baseline `Publication` records lack the materialized measure list and input content fingerprint. `TaskJournal::complete` also preserves an existing publication. A completed task alone does not establish that its retained publication matches the latest requested revision. Recovery must revalidate that evidence against subsequent invalidation and current source metadata.

The local empty-base candidate adds optional `PublicationEvidence` to the existing JSON journal and staged intent. It contains the input content fingerprint and materialized measures. Legacy records decode with no evidence. This does not change `WalEntry`, add a journal, or add a commit. Recovery compares current source files and deletion vectors, rejects overlapping output, and rechecks task completion under the invalidation lock. It records verified emptiness separately from an unknown output count. Empty ranges authorize no generation rows, including when rebuilds share a generation ID. Mixed empty/nonempty or mixed-generation slices cannot become one legacy day record.

The initial restart regression reproduced loss of both proof fields. Its first candidate restored them, but a negative test selected a slice outside the mutated hour. The corrected fixture selects the tombstone's own slice. Verification includes legacy decoding, empty query routing, changed source input, and overlapping old output. Results remain pending. Derived empty publications still require base-tier evidence and remain unsupported by this recovery candidate. Metadata validation cost, recovery races, no-op handling, and census reconciliation remain open gates.

The subsequent eight-test run passed six cases and failed two. The old-output fixture lacked a required `updated_at` value. The revised fixture supplies required identity and count fields from the generated schema. It also executes both original and rewritten SQL, rather than only constructing the rewrite. These fixture changes need a fresh run.

The existing restart/no-op regression also failed: reminting unchanged work advanced the tier version from 1 to 2. This is not an accepted tradeoff. The mixed empty/nonempty summary restriction and the day-presence skip gate require investigation together. Preserve the regression while implementing the shared range proof. Empty recovery alone does not complete Stage 0.

The corrected empty-recovery run passed all three selected cases in 9.34 seconds. It covers legacy JSON compatibility and both recovery cases: changed source and overlapping output. Both recovery cases execute the original query and the generated rollup query. This result does not cover concurrent recovery or derived empty publications.

The next no-op candidate removes day-record presence as a prerequisite. It requires the router's existing whole-partition row-witness check, current generation, unchanged input fingerprint, and matching live output files. The missing-day regression executes a query over the proven hour before reminting unchanged work. It also checks the full-day result. It requires the tier version to remain unchanged. This deliberately reverses the rebuild-to-restore-summary behavior, without inventing a mixed day generation.

The corrected missing-day, restart, and deletion-vector regressions passed together in 126.45 seconds. Census reconciliation, empty-output skips, bounded-witness reuse, and race gates remain incomplete. `readable_cells` still uses day-record presence, so fewer rebuilds alone cannot establish that repeated remints stopped. Stage 0 cannot ship with only this no-op change.

The first six-test run passed five cases, including restart and both batch modes. The missing-day case failed with `TinyInterior`. Its saved journal showed six complete 10-minute slices covering one hour, not incomplete bucket boundaries. The router requires coverage for at least 20% of the query window. One hour cannot pass that gate for a full-day query. The corrected regression requires routing over the proven hour and correct full-day results. It does not manufacture coverage for the other 23 hours. The subsequent three-test run includes this correction.

The deterministic completion regression failed after 174.42 seconds: invalidation after proof capture did not prevent no-op completion. The generic completion helper discarded the need for another check. The regression uses the real database, publication, journal, and invalidation path. It covers an unrelated-hour mutation and a mutation inside the proven slice.

The candidate captures the date epoch before source preflight. After the output check, it compares the epoch and full slice evidence under the existing invalidation lock. Only an unchanged, still-running task completes. A changed proof causes a bounded retry without resource-driven splitting. A task already returned to pending keeps its state and quiet-period deadline. Checkpoint I/O occurs after release of the invalidation lock. No new journal or commit path is required.

All five selected regression cases passed in 154.18 seconds. The completion case checks both invalidation orders, retained attempt counts, and the pending task's deadline. The other cases cover restart, deletion-vector changes, and empty recovery with changed source or overlapping output. This run predates the empty no-op assertions. Their separate baseline and candidate results follow.

This guard covers the no-op completion boundary, not atomic visibility across raw Delta, rollup Delta, and MemBuffer. Publication, recovery, and query-capture races remain separate gates. A passing completion test cannot establish snapshot-correct hybrid routing.

Empty no-op work has a separate durability requirement. `TaskJournal::enqueue` clears the previous publication. Completion without another scan must restore the verified empty publication through the existing checkpoint. Otherwise, a later restart loses the proof because no Parquet Add tags exist. The extended empty-recovery regression requires a skip, no completed rebuild, an unchanged tier version, and recoverable evidence after re-enqueueing. Both baseline cases failed at the missing-skip assertion in 7.34 seconds.

The empty no-op candidate accepts only explicit empty coverage with source fingerprint and measure evidence. It reuses the recovery check for overlapping target output. Missing timestamp bounds cannot prove that a file is disjoint. The guarded completion restores the empty publication in the existing task journal before its normal checkpoint. This adds neither Parquet output nor a separate commit stream. Source and output changes still require rebuilding.

All five selected cases passed in 286.31 seconds, including both empty no-op cases, restart, deletion-vector changes, and guarded completion. The empty cases prove a skip without a completed rebuild or tier-version change, followed by recovery of the restored journal evidence. This run predates the positive scan-counter assertions. It establishes correctness for these scenarios, not production savings or completion of the shared coverage design.

The Rust skill review identified two concerns in the earlier candidate: ambiguous output evidence and a 176-byte error from generation-range `coalesce`. Both approved refactors are now implemented. Coverage uses an explicit output-evidence enum, and generation ranges merge in place without lint suppression. The evidence appendix records their passing checks and the later lifecycle diagnostics. No commit or production activation occurred.

Inspection also found that `rollup_dirty` retains invalidation history rather than clearing every completed repair. It cannot directly serve as the proposed current dirty mask. Stage 0 must derive effective dirtiness per tier from pending work and validated current publications. The retained hour mask remains a recovery-discovery input, not independent proof that completed coverage is stale.

Deleting `rollup_coverage.remove()` alone is not this fix. The retained record still carries an old epoch and fingerprint, which routing and ticket checks reject. A presence-only census can then suppress necessary work while queries still miss. The implementation must reconcile proofs, not retain stale entries or stamp them with current epochs.

Required regressions include the fixtures from `29d249d7` and `95d3311e`, partial-day coverage, dirty hours, and restart recovery. An unchanged slice cannot establish unknown coverage for other hours. Metadata repair must not perform raw scans or create replacement aggregate Parquet. Include a sparse day, a zero-row repair after deletion, restart with no Add files, and a dirty hour between two valid ranges.

Existing stats are `rollup_hits_full_total`, `rollup_hits_hybrid_total`, `rollup_misses_total`, and `rollup_miss_not_built_total`. Work counters are `rollup_noop_rebuild_skipped_total`, `rollup_remint_skipped_total`, and `rollup_rebuilds_full_total`. Use `rollup_scan_estimated_bytes_total` divided by rebuild count, plus published task ranges, to measure scan reduction. Estimated bytes are not decoded bytes. Report rebuilt hours per scoped invalidation as the mechanism check. `cells_missing` and task identities come from census and publication logs; stage 0 needs no new stats key.

Instrumentation audit: the baseline coordinator declared scan-attempt, project, and estimated-byte counters without incrementing them. Both storage cases reproduced `[0, 0, 0]` after real builds. The extended regression requires positive counters after a build and no additional scan work during an empty no-op.

The instrumentation candidate counts each source-aggregate attempt immediately before collection, including failed attempts and repeated hash-shard passes. Project counts represent participations, not distinct tenants. Each attempt adds its full projected input-file estimate, without time proration. This estimate exposes repeated selected input, but does not measure decoded bytes or row-group pruning. The in-memory merge of shard states adds no source-scan count. Comparisons require a baseline binary with the same instrumentation, not the older always-zero counters.

Both counter regression cases passed in 11.88 seconds. Real builds increased all three counters, and empty no-ops left them unchanged. Failed-attempt and multi-shard accounting still need dedicated checks. The census regression also requires pending historical repairs to exclude the proven hour while retaining work for unknown hours.

The census needs range-aware decisions at both missing-cell detection and task creation. Changing only one leaves either false missing cells or broad repair tasks. The real-storage regression reproduced a full-day task overlapping the proven hour. It failed after 144.01 seconds.

The census candidate now uses current day fingerprints and epochs, plus the existing whole-partition or bounded slice witnesses. It merges adjacent proof intervals before clipping them to complete aggregate buckets. A partial day remains missing, but each tier queues only its uncovered intervals. Raw deduplication receives the union of gaps required by raw-derived tiers. Explicit damage repairs still bypass coverage reuse.

The source statistics and optional file-row witnesses come from one locked Delta snapshot. This adds metadata work, not a raw scan or a metadata commit. Its cost still needs measurement. Physical files can span several repair intervals, so narrower tasks alone do not establish lower decoded I/O. The old date-wide active-task veto remains conservative and can delay additional gaps until queued work completes.

`covering_slice_for` and `settle_covered_by_wider` can also expand a narrow repair to an older, wider publication range. The replacement set currently removes only files contained in the repair. Removing this guard alone can double-count the retained wider output. The legacy range adapter must preserve untouched states and replace only affected states before this expansion can disappear. Acceptance therefore needs an executed repair inside a packed historical publication, not only an assertion about queued task ranges. The wider-coverage completion path also needs the same invalidation-race guard as ordinary no-op completion.

The real-storage regression `a_scoped_repair_preserves_packed_output_without_widening_raw_work` now reproduces this cost failure. It first publishes three hours and checks that a live file spans all three. A new row changes only the middle hour. The one-hour repair reports `Complete` with no aggregate scan. The test failed at that assertion after 153.332 seconds. Its initial compilation failed on a test-only `Self` qualifier, corrected to `Database` before this run.

The remaining assertions require no wider pending raw aggregation and a physical aggregate total of four, with each neighboring row counted once. They did not execute after the baseline failure. That baseline changed no production replacement logic.

The opt-in candidate `timefusion_rollup_packed_repairs` now stages retained aggregate fragments beside the rebuilt interval. One existing Delta publication removes the original packed files and adds all replacement files. Its existing staged intent includes the complete replacement set. Raw aggregation retains the requested interval. Unaligned requests retain the conservative path because aggregate states cannot be split inside their grain.

The candidate requires complete output-row evidence before it copies a packed publication. It preserves the original source evidence and does not certify the retained fragments as newly rebuilt. A changed upper bound removes the old bounded row witness. The new slice's file-count evidence excludes retained files. Partial output loss therefore cannot become valid evidence merely through this copy.

The packed-range regression passed in 154.435 seconds after a 2m39s build. The middle hour performed an aggregate scan, no wider raw task remained pending, and the physical total was four. A subsequent change reuses `carried_coverage_tags` instead of copying unrelated file tags. Aggregate-copy time now counts as staging time. All seven selected packed-range, wider-file freshness, and invalidation-race tests passed in 185.032 seconds. Fresh `cargo lint` passed in 4m16s. These durations include local build contention and do not measure production performance.

The packed-range fixture now has empty and nonempty replacement cases. Each requires durable repaired-hour evidence after shutdown and a new database instance, with unchanged physical totals and no recovery rewrite. Retained fragment files must not turn an empty publication into a nonempty proof. Both cases and the default-off configuration assertion passed in 162.289 seconds after a 2m28s build. This proves recovery after completed publication, not recovery from an interrupted replacement.

Resume inspection found that an uncommitted intent records target paths without the target metadata captured during staging. A new regression changes the target generation tag under the same path before resume captures its snapshot. Its control leaves the replacement input unchanged. Both cases use real Parquet objects, Delta commits, and the durable manifest. The first compilation rejected a private helper call. The corrected fixture uses the existing manifest parser without changing production visibility. The control passed, but the changed-target case incorrectly resumed. The run finished in 1.985 seconds after a 2m30s build.

The recovery candidate adds optional before/after target fingerprints to the existing staged intent. The fingerprints cover the affected target partition, including paths, deletion vectors, file statistics, sizes, timestamps, partition values, and coverage tags. File and map order do not affect them. Recovery compares the pre-publication fingerprint before a commit and the post-publication fingerprint before landed-output bookkeeping. No new journal or commit is added.

Legacy intents still decode. An uncommitted legacy intent that removes files now declines resume without target proof. A landed legacy intent must match its recorded output metadata. The first fix build found that the deletion-vector storage type lacks `Hash`. The candidate uses its existing stable `AsRef<str>` representation. It excludes `data_change` because Delta snapshot reconstruction normalizes that flag. All 12 selected recovery tests passed in 5.113 seconds after a 2m29s build.

The compatibility baseline passed four tests and failed the old-decoder rejection assertion in 2.589 seconds after a 2m22s build. Legacy-replacement refusal and fingerprint sensitivity passed. The flat encoding let the pre-versioned decoder accept new evidence without enforcing its contract.

The candidate now writes the rollup body inside a typed `v2` wrapper. Older decoders reject this layout because required body fields are absent at the old location. The current decoder accepts both layouts. The wrapper uses borrowed data during serialization and adds no journal or commit. All 14 selected recovery tests passed in 6.512 seconds after a 2m28s build. Formatting and whitespace checks passed.

The compatibility test also requires rejection of unknown versions and incomplete `v2` bodies. The real manifest parser must retain a valid record after each rejected record. All 15 selected tests passed in 5.976 seconds after a 2m36s build. The command used `cargo nextest run --lib` with recovery, fingerprint, and manifest filters. Formatting and whitespace checks passed. Fresh `cargo lint` passed in 1m30s.

The Rust review found no new suppression, weakened API, or additional storage commit in the encoding change. The tests extend the existing scenario rather than duplicate its fixture. Older readers lose resume reuse for new intents and can rebuild that work. Interrupted packed-publication fixtures and the complete rollback lifecycle remain separate checks.

The packed-repair fixture now includes interruption before publication for empty and nonempty repairs. A held commit lock stops the real builder after its durable intent appears. The fixture drops that builder, reopens the database, and requests recovery. Recovery must reuse staged paths, commit exactly once, preserve both neighbors, and survive another restart. The fixture ages the record because both database instances share one process identity.

The first compile rejected debug formatting of `UnitRunReport`. The test now uses its existing display formatter and preserves build errors. No production type changed. The four-case run passed both completed-publication cases but failed both interrupted cases in 172.924 seconds.

Inspection found that the new fixture supplied a whole-table fingerprint to recovery. The builder instead fingerprints files selected for the requested interval. The revised fixture uses `run_unit_once` after restart, so production preflight supplies the evidence. It requires a complete task, zero new aggregate scans, one commit, and reuse of staged paths.

The worker-path run passed both completed-publication cases but returned `Retry` in both interrupted cases. It finished in 173.449 seconds. Dropping the builder invokes lease cleanup, which records `worker_error` and a retry deadline. Manual worker selection preserves that deadline. The fixture now checks the persisted reason and advances the test clock to the recorded deadline before recovery. The rerun is compiling. No production admission rule changed. This fixture does not simulate interruption during the Delta commit or abrupt process termination.

After that timing correction, both interrupted cases passed the resume, zero-scan, exact-path, and single-commit assertions. Both then failed the assertion that forbids unfinished work outside the repaired hour. The completed-publication cases passed. The run finished in 180.418 seconds. The assertion now reports task ranges, states, and retry reasons to distinguish runnable work from retained terminal entries. The narrow-repair requirement remains unchanged.

The diagnostic run showed `Pending` ten-minute tasks in the repaired hour and the following hour, not superseded entries. Both interrupted cases still failed. The two completed-publication cases passed. This five-test baseline also included the failing writer-cutoff test and finished in 429.382 seconds under local build contention.

The retained source evidence needs further work. Slice coverage records a whole-date row count and an optional prefix count, not interval-specific mutation revisions. `recover_date_coverage` queues sealed slices when those counts disagree. The current adapter preserves historical tags rather than invent current source evidence. A safe correction must preserve verifiable interval evidence through publication and recovery. Disabling the recovery check does not satisfy this requirement.

The existing invalidation snapshot is not sufficient proof for retained fragments. `RollupInvalidation` stores a date epoch and current dirty-hour mask, not the mutations since each publication. Its persistence can lag by one second, and unreadable state falls back to unknown. Promoting this scheduling snapshot to freshness authority requires the durable mutation contract first. Interval-specific file fingerprints can provide conservative proof, but a deletion-vector change on a multi-hour file can still invalidate every overlapping hour.

New intent evidence does not make older binaries enforce the new contract. Safe reader rollback, fingerprint stability tests, and interrupted packed-publication fixtures remain required before activation.

This candidate remains disabled by default. It streams each retained fragment through the maintenance context and keeps one fragment writer active at a time. Two retained fragments can read the compact aggregate input twice. That cost, writer memory, cancellation cleanup, empty output, restart recovery, and concurrent replacement still require acceptance checks. Unaffected ranges with insufficient source proof remain unknown, so this result does not establish the full historical scan-saving gate.

The retained-fragment writer currently flushes only at the end of each fragment. Streaming input therefore does not bound accumulated writer output. The pinned Delta dependency (`12847eb`) includes buffered output and the in-progress row group in `buffer_len()`. Existing rewrite code uses this value with `timefusion_writer_max_file_bytes` to cut files. The retained-fragment path still needs that behavior and a multiple-batch regression. A file cutoff alone does not bound an oversized input batch or all encoding memory.

The new writer regression builds 900 real aggregate rows across three hours. It stages the 600 rows in the outer hours with a one-byte file cutoff. Each fragment must span multiple files and retain its total row-count evidence. The test removes its uncommitted outputs through the existing cleanup path. The baseline passed its row-count assertions but failed the file-cut assertion in 4.001 seconds.

The candidate now checks `buffer_len()` after each batch and flushes at the existing writer limit. It records each flushed file immediately for caller cleanup. After the fragment ends, every cut file receives the complete fragment's row-count evidence. Publication still uses one transaction. The focused writer regression passed in 3.363 seconds after a 5m14s build. Formatting and whitespace checks passed. Fresh `cargo lint` passed in 1m52s. Oversized batches, encoding-memory overhead, and partial-flush errors remain separate resource checks.

Publication review found a separate recovery concern. Fresh publication checks task state under the invalidation lock, but the resumed-publication helper records completion without that guard. A new real-storage regression pauses landed-output recovery at the target commit lock, inserts another source row, then releases recovery. The baseline failed in 1.887 seconds after a 3m22s build: recovery reported success after source invalidation.

The candidate passes the epoch captured before source preflight through recovery. Before completion, it compares that epoch under the invalidation lock and checks task state. A changed epoch preserves an existing requeue or retries a still-running task without resource-driven splitting. A refusal retains the staged intent. Checkpoint I/O occurs after release of the invalidation lock. The existing `rollup_resume_declined` counter records refusal. Formatting and whitespace checks passed; the broader recovery selection is compiling. This guard does not establish the shared-snapshot contract or solve retained-interval freshness.

The first guard build found one unchanged call in the shared-checkpoint fixture. That fixture now supplies its initial epoch and marks tasks running before publication. It also requires each publication to report success. Its checkpoint-sharing and lock-release assertions remain unchanged. All 16 selected recovery and checkpoint tests passed in 6.137 seconds after a 3m21s build.

The race fixture now also inserts a source row after preflight but before the recovery call. Both cases require refusal to preserve the staged intent. This extension checks that recovery uses the caller's captured epoch rather than a later counter read. All 17 selected recovery and checkpoint tests passed in 6.755 seconds after a 2m49s build. No production behavior changed after the 16-test result.

Fresh `cargo lint` passed in 1m51s for the epoch guard. The same race fixture now covers staged output and already-landed output at both invalidation timings. This four-case extension exercises both recovery branches without another fixture or production hook. Formatting and whitespace checks passed. All 19 selected recovery and checkpoint tests passed in 8.747 seconds after a 3m31s build. The lint result precedes this test-only extension.

The adapter must retire or exclude obsolete rows atomically with replacement publication. Successive repairs can share a generation ID, so generation predicates alone cannot distinguish their rows. Retained states also need valid source evidence. A repair cannot assign current source evidence to untouched historical states merely because it copied them. Acceptance must include restart, empty replacement, concurrent publication, and bounded aggregate-file work, in addition to the one-hour raw-scan boundary.

Dependency review found another consumer of coarse evidence. `TaskJournal::dependencies_complete` accepts a day-level base-tier marker before its interval checks. The census still supplies that marker from physical partition presence. Derived-build preflight separately checks base coverage and refuses unreproduced input ranges. That safeguard does not establish correct admission or eliminate repeated refusals. Stage 0 needs a regression with partial base coverage and a derived repair across a hole. Readiness must refer to the required base specification and range, not any base tier on that date.

The candidate reuses source-witness predicates with routing and no-op checks. Full agreement on measures, live output, tickets, and concurrent publication remains incomplete. This is not a Stage 0 release result.

During compilation, another session advanced HEAD to `d9f00cce` and stashed the uncommitted Rust changes. Stash `9e35a4efc819b040658071a84661e4a30f34bc6d` preserves the census candidate and its tests. The current tracked worktree does not contain that candidate. Restoration requires coordination with the other session. Any result from the interrupted worktree must not count as verification of current HEAD.

Implementation now continues in isolated worktree `/tmp/timefusion-rollups.XCvHu4`, based on `d9f00cce`. Its initial tracked files exactly matched the preserved stash. The main checkout and original stash remain unchanged. The extended metadata test covers adjacent half-hour proofs and unknown or changed source-row counts. All seven targeted tests passed against the stable isolated source tree in 126.69 seconds. This replaces the earlier interrupted-worktree result as the focused correctness evidence.

The regression `derived_rollup_claim_waits_for_complete_base_hour` now uses the actual log-tier identities. It failed in 0.23 seconds: completed session work made the dashboard hourly task claimable while dashboard minute tasks remained pending. The completed-task check ignored the required physical table.

The candidate resolves the parent table from the existing `derive_from` schema declaration and requires matching completed tasks. Unknown source or parent definitions cannot supply this proof. The split-child regression now also uses registered tier identities. The change adds no durable field, journal, or WAL format. All 179 selected journal and simulator tests passed in 242.43 seconds. Day-level readiness shortcuts and publication freshness still require separate checks.

All four new regression cases failed in 14.50 seconds. Two cases showed that source invalidation retained the per-task `base_tier_present` proof. A third showed that invalidation retained the day-level readiness hint. The repair must preserve unaffected coverage without a full retained-task scan on every write.

The per-task candidate clears the proof through the existing exact-key invalidation lookup. A proof change also marks the task dirty for persistence. Both extended proof cases and the invalidation-idempotence case passed in 0.12 seconds. They covered physical-only changes, unaffected adjacent hours, and recovery after invalidation. Wider overlapping task proofs remain open and prevent Stage 0 activation.

The next candidate replaces the runtime day marker with merged ranges keyed by source, project, derived table, and date. The census supplies validated ranges from each derived specification's declared parent. It no longer copies whole-day proof into task flags. Source invalidation subtracts its interval through affected-cell lookups and preserves adjacent ranges. No new durable ledger or retained-task scan is required.

The census captures source epochs before metadata reads and compares them under the invalidation lock before publication. Changed dates supply no new readiness proof. New assertions cover clean adjacent hours, wider tasks across dirty holes, and unrelated derived tiers. The first build found an enqueue return-type mismatch. The candidate now uses the existing boolean-returning API with no per-task proof, but the verification run remains outstanding.

Census integration, concurrent refresh, legacy persisted flags, and completed-task freshness still require checks before activation. An epoch comparison protects only mutation paths that advance that epoch. `reconcile_maintenance_task_cursors` can retire readiness through `mint_maintenance_hours` without an epoch change.

The dependency regression now covers a completed wide parent followed by a narrow invalidation. One case also publishes cached census ranges after invalidation. Both failed because stale evidence overrode a pending base repair. The focused run finished with 160 passes and these two failures in 21.46 seconds.

The candidate checks overlapping active work for the declared parent before accepting any readiness shortcut. It reuses the existing scheduler index, which now retains running tasks until they finish. A running task still cannot receive another claim. The existing index-size gauge consequently includes running entries. The extended assertions cover index rebuilds and admission after repair completion. All 180 coordinator and simulator tests passed in 49.60 seconds against the isolated source tree.

This guard avoids a new scan of retained completed tasks before a readiness shortcut. Its active-work lookup cost still needs measurement. The legacy completed-task fallback still scans retained tasks when no shortcut applies.

The diagnostic review found that `claimability_census` still counts only per-task proof flags. It ignores the runtime coverage ranges that now establish readiness. The existing census test now includes both proof forms. The range case failed: it reported three unproven tasks instead of one. The flag case passed, and the run finished in 0.13 seconds. The candidate shares the cached-proof predicate with admission without searches through completed task history. Active repairs still veto admission before that predicate. The diagnostic describes cached evidence, not full eligibility. Both diagnostic cases and both active-repair cases passed in 0.07 seconds.

Review of `settle_covered_by_wider` found another proof disagreement. Its row-witness shortcut does not check the coverage generation, unlike routing and ordinary no-op completion. The existing real-storage regression now includes a stale-generation case with an unchanged row witness. That case requires a queued repair for the covering interval. It failed because the shortcut retired the repair. The current-generation case passed, and the run finished in 2.20 seconds. The candidate now uses the shared generation predicate before it accepts row evidence. Both cases passed in 2.02 seconds without additional Parquet objects.

The same shortcut also ignores `timefusion_rollup_bounded_witness`, unlike routing. The regression now includes bounded evidence with the setting enabled and disabled. A disabled setting must retain the covering repair when whole-partition evidence is absent. Three cases passed, but the disabled-setting case failed because the repair disappeared. The run finished in 2.42 seconds. The candidate now gates bounded evidence on the existing setting. All four cases passed in 3.58 seconds.

The current-generation case now also replaces live output with an obsolete generation while it retains current cached evidence. It uses the existing real-storage rewrite helper. The shortcut must retain a covering repair without new Parquet writes. This extension failed because the shortcut retired that repair. The other three cases passed, and the run finished in 2.36 seconds. The candidate now reuses `tier_still_holds_slice` for the covering range and rejects zero-file evidence. All four cases passed in 4.87 seconds. Metadata lookup cost still needs measurement.

The next regression holds the target-table write lock while the shortcut captures its proof and waits for the output check. It then invalidates the source before releasing the lock. The task must retry instead of completing with that captured proof. This test uses real locks and storage without sleeps or mocks. It failed in 91.42 seconds: the task became `Complete` instead of `Retry`.

The candidate passes the source epoch from the original metadata read into the wider-file shortcut. Both no-op paths now use the same completion guard under the invalidation lock. The guard checks the task state, source epoch, and coverage record for the actual proof interval. Empty publication evidence remains restricted to the task's own interval. Checkpoint I/O occurs after the invalidation lock is released. All eight focused cases passed in 79.94 seconds, including the race and ordinary and empty no-op behavior. This is not a Stage 0 release result.

The broader no-op run passed all 18 cases in 349.09 seconds. It covered restart, deletion-vector rebuilds, witness carry, hybrid routing, and masked-file retirement. These test durations include local contention and do not measure production performance. A fresh `cargo lint` run failed on the existing 176-byte error value in `merge_generation_ranges`. It reported no other errors. The proposed in-place merge still awaits approval, and no lint suppression was added.

Census review found that `readable_rollup_ranges` validates source evidence without a matching live-output check. The new real-storage regression first proves that a fresh base day needs no repair. It then removes the committed tier output but retains the cached source evidence. The next census must queue a repair. The regression failed at this final assertion in 4.13 seconds. The census queued no repair despite the missing output. No production fix for this failure is complete.

The fix must distinguish day summaries from publication evidence. Day summaries intentionally store `output_files: 0`, so a positive-count requirement would reject valid summaries. Recovery derives slice file counts from surviving files, which alone cannot prove that a publication lost no files. The durable coverage ledger stores file paths, but initial publication and recovery do not populate it identically. The fix must reuse a consistent output proof and preserve explicit empty coverage. A per-target metadata pass must replace repeated full-file scans for individual coverage intervals.

The same regression now covers obsolete output while the partition and file count remain unchanged. Both cases failed at the repair-queue assertion in 4.45 seconds. Therefore, partition presence and file counts alone cannot fix the census. The output proof must also match the generation. The existing publication journal records aggregate rows and source evidence, but not the complete output path set. Recovery and publication need one agreed proof before partial-file loss can be declared covered.

The census caller already withholds bounded file evidence when `timefusion_rollup_bounded_witness` is disabled. A helper-level check suggested a configuration mismatch, but caller inspection ruled out that production failure. No configuration change is required for this finding.

The census candidate now separates source freshness from output presence. One pass over each tier snapshot indexes generation-tagged file counts and occupied timestamp ranges. Nonempty slices require the expected count for their generation and interval. Files with deletion vectors do not prove unchanged output. Explicit empty slices require publication evidence and no overlapping output. Unknown partition metadata prevents empty-output proof. The candidate intersects these output intervals with source-fresh intervals and retains only complete aggregate buckets.

This candidate adds no durable metadata or commits. Its temporary indexes scale with live output metadata, not retained project-minutes. It reuses the existing range merge, intersection, generation check, and empty-publication constructor. The existing source-interval test remains a source-proof test under the renamed helper. The real empty-publication restart test now also checks census coverage. Focused verification is pending. The census metric affected is usable coverage and therefore `cells_missing`, not a new counter.

This is not the shared-proof release gate. Recovery still needs stronger evidence for partial-file loss, and concurrent snapshot transitions remain unproven. Metadata CPU and memory costs also remain unmeasured. Physical rewrites that change file counts need certified proof transfer to avoid unnecessary repairs.

The first candidate run passed four tests in 4.83 seconds. Both previously failing output cases now queue repairs, and their fresh-day baselines still need no repair. The expanded empty-coverage run reported two reference-type errors in the new test setup. Explicit guard dereferences correct those errors without changing the assertions. That expanded verification remains pending. The journal's existing `Publication.rows` is a candidate for additional recovery checks, but no split-file regression proves that use yet.

The corrected five-case run finished in 6.62 seconds. The source-interval case and both empty-publication cases passed. Both output cases passed their census assertions but failed new routing assertions. Routing still accepted output that the census marked for repair. Their fresh-output routing assertions passed, so unconditional refusal cannot satisfy the regression.

The next candidate shares `rollup_output_coverage` between census and route selection. Its temporary range maps distinguish generation, project, date, and empty versus populated output. Both routing paths intersect candidate coverage with the matching output intervals. A generation that stores one measure set cannot authorize another generation's range. No new durable record is required. Verification is pending. The added tier-metadata pass also needs a planning-cost gate before rollout. The ticket recheck still does not establish a common source/output snapshot.

All five focused cases passed with the shared output check in 13.44 seconds. Fresh output still routes, missing or obsolete output declines, and both explicit-empty recovery cases retain census coverage. This result does not prove the planning-cost or snapshot-race gates. The existing legacy-generation routing test is also running.

Ledger startup needs a separate guard. `seed_routing_from_ledger` currently restores `output_files: 0`, although each ledger entry can contain file paths. The output check can therefore refuse seeded coverage until tag replay completes. The existing ledger scenario now asserts that current seeded coverage routes before replay. Its first verification run is pending. A fix must retain rejection of legacy generations and cannot infer empty output from an absent file list.

That startup assertion failed in 6.41 seconds. The candidate now restores the file count from the existing ledger file list. The rerun is pending. This changes no durable format and does not prove that merged ledger intervals retain the original output geometry.

Ledger merge review found a separate witness error. `source_rows` describes the whole source partition, but `merge_coverage` adds values from adjacent entries. The case table now requires equal witnesses to remain unchanged. Different witnesses, fingerprints, or measure evidence must retain separate proofs. Unknown evidence must not erase its known neighbor. The six-case regression run is pending. Existing persisted merged entries also need a compatibility audit before startup correctness is established.

The legacy-generation scenario failed before its intended worker check: an active parent repair kept the derived task `Pending`, not `Retry`. Its fixture now settles already-covered base tasks through the real no-op path before changing generations. An additional assertion forbids another source scan during this setup. The worker's existing `Retry` assertion remains unchanged. Its rerun is pending.

The six merge cases finished in 0.20 seconds: one passed and five failed. Equal whole-partition witnesses became their sum, and differing evidence collapsed into one range. The candidate now merges only matching generation, source fingerprint, source witness, and measure evidence. It retains the shared witness unchanged. Verification includes the complete ledger test group and the startup and legacy-generation scenarios. Results are pending.

The single-publication ledger startup case passed in 4.18 seconds after file-count restoration. The ledger scenario now also publishes two adjacent 12-hour intervals from one two-row source batch. It requires one compact ledger entry, an unchanged two-row witness, both output files, and successful startup routing. Its reverse-coverage assertion now checks the union of tagged intervals instead of requiring one tag interval to cover the merged entry. This expanded case is awaiting verification.

The corrected legacy-generation fixture reached its worker assertions, then failed later in 7.05 seconds. An obsolete journal-only publication remained `Complete` after recovery. Recovery skipped nonempty journal publications before checking their generation. The candidate now checks generation first when journal measure evidence exists. Legacy records without measure evidence still rely on tagged output checks. This correction is awaiting verification.

The fourth case showed that the simulator completed no derived work. Its fixture assigned the base table identity to the derived stream. The candidate now uses the existing `key` helper to select the actual derived table. All 21 simulator cases passed in 159.38 seconds, including the positive derived-completion assertion. These scheduler checks do not establish production capacity.

The pending run completed successfully: seven targeted tests passed in 196.39 seconds. They covered the missing-day census, partial-day metadata, missing coverage, and interval subtraction. The real-storage regression rejected repairs across the proven hour while retaining repairs for unknown hours. Compilation overlapped worktree changes, including the inclusive-bound correction. A fresh run against a stable restored tree remains required. This result does not establish decoded-byte savings or production capacity.

Gap subtraction must also clip coverage to the requested interval. The boundary regression reproduced `[0, 150)` as a repair for request `[0, 100)` with coverage `[150, 200)`. The candidate clips and removes disjoint intervals in place before sorting and subtraction. It adds no metadata structure or allocation. The existing overlap cases and new boundary cases remain together in one test. That test passed in the seven-test run, subject to the stable-tree rerun requirement.

Use the initial attribution baseline and the repeated, counterbalanced windows from the shared experiment protocol. Record eligible hit rate, `cells_missing`, repeated same-revision remints/hour, scan bytes/rebuild, and rebuilt hours/invalidation. Skipped remints are not actual remints; derive repeated work from task identities, with skip counters as supporting evidence. Stage 0 acceptance targets are:

- At least 90% fewer repeated same-revision remints and false missing cells whose coverage is already provable.
- At least 50% lower estimated scan bytes per rebuild in the scoped-invalidation cohort; also report total bytes per invalidation.
- Eligible hit rate improves by at least 20 percentage points, or stays at least 90% if already above that level.
- No scheduled repair includes proven clean hours merely to recreate day metadata; genuine historical holes remain reported separately.
- Correctness tests pass, no additional flush stalls occur, and matched insert/query p99 regresses by no more than 5%.

If the cohort is too small or its baseline is zero, extend the observation; do not manufacture a percentage improvement. If physical file layout or witnesses prevent the scan target, report that limit before expanding the design. A high no-op count alone is not success. Capture all baseline and canary values in the experiment PR.

## Stage 1: measure total work, granularity, and publication economics

This stage uses the attribution baseline and shared experiment protocol. Later comparisons use the remaining workload after each accepted optimization. Counters and task records distinguish useful publications, repeated revisions, and genuine historical holes. Duplicate retries appear separately from unique winning rows.

For each representative build, capture the physical plan and execution metrics available in our pinned DataFusion version. Record source scan count, hash-shard count, deduplication operators, required columns, row groups, and aggregate-state memory. Object-cache hits do not prove that decoding or hashing disappeared. Whole-file estimates and field-count projection ratios are admission estimates, not measured physical cost. No experiment adds heavy paired scans to an already saturated production server.

After the early batch experiment, the granularity comparison uses one representative source snapshot and the same actual mutation set. It compares current hour masks with candidate minute ranges on a copied production day or isolated staging data. Expansion across Shipbubble, Bitnob-associated logs, demo metrics, and sparse projects follows only if the first comparison warrants it. Heavy paired scans must not run concurrently on production.

Measurements include selected files and row groups, physical bytes, decoded rows, CPU time, aggregation time, and metadata overhead. A minute predicate can decode the same row groups as an hour predicate. It can still reduce aggregate CPU or raw fallback, but those savings must exceed its tracking cost. If hour-level repair meets resource and latency goals, minute tracking remains deferred. The aggregate grain remains one minute regardless of repair granularity.

Use benchmark output for decoded bytes, selected buckets, tested granularity, and actual CPU; add no permanent stats for this experiment. Current `rollup_scan_estimated_bytes_total` is an estimate, not physical scan evidence. Minute tracking proceeds only if net repair CPU or decoded bytes fall at least 30% on work consuming at least 20% of that maintenance resource. Include tracking, scheduling, checkpoint, and recovery costs.

Do not combine a CPU share with an I/O reduction. Require the result across representative dense and sparse cohorts, without more than 5% insert/query p99 regression. Equal row-group reads with smaller predicates do not establish I/O savings.

Publication arithmetic uses measured committed flush batches, rather than configured timer frequency:

```text
naive_base_commits_per_day = sum(flush_batches_for_source * eligible_base_specs_for_source)
total_commits_per_day = base_publications + derived_publications + repair_publications
```

Logs have two raw-derived specifications: `dashboard_1m_v3` and `sessions_1h_v1`. The commit model counts only active specifications. A paused session tier contributes no new build commits after in-flight work drains. Metrics have one: `metrics_1m_v2`.

Both sources also have a derived hourly dashboard/metrics tier. Eligibility and grouping can reduce this estimate, so actual publication batches require measurement. Session/user enrichment can preserve dashboard aggregates while invalidating session aggregates. Dependency classification must therefore operate per specification.

The experiment records output bytes, actions per commit, object requests, commit latency, and source flush latency. Existing stats are `rollup_shared_commits_total`, `rollup_commit_actions_total`, `rollup_output_files_total`, and `flush_stalled_total`. Publication statistics must include failed attempts and retries where current success-only counters omit them.

### Stage 1A: aggregate certified clean windows directly

With the experiment disabled, the base path requests `SliceDedup` even when `dedup_window_certified` supplies clean-window evidence. `slice_input_sql` expresses winner selection through `ROW_NUMBER()` over identity and version columns. The physical plan determines its actual sort, memory, and CPU cost.

The fast path checks the existing certificate against the exact source snapshot and requested range. Its proof must include current files, deletion vectors, complete key ownership, and all relevant version-appended rows. Clean input skips winner selection, but still applies source deletion vectors and the required tombstone filter. The baseline helper removed its tombstone predicate with `dedup = None`, so that shortcut was insufficient. Uncertified input retains the current winner rule, including null and tied versions.

Individually clean files do not prove a clean union. Overlapping keys across files still require winner selection. Partial bypass requires proof that the clean and unresolved inputs cannot share a dedup key. Missing evidence selects the existing path, not another certificate service or a foreground scan for proof.

Implementation review found that baseline certificates retained file paths but omitted deletion-vector identities. The real-Delta regression reproduced reuse and republication after a same-path deletion-vector change. The local candidate now carries exact file visibility through publication, persistence, recovery, and certificate consumption. Its expanded 38-test run passed, including restart and unchanged-range reuse. Historical certificate renewal and metadata costs remain rollout gates.

The local aggregation candidate adds the default-off `timefusion_rollup_certified_clean` switch. It checks clean-window evidence under the same table guard that captures the source snapshot. It requires raw input with timestamp identity. Derived tiers and uncertified input retain winner selection. The provider still applies deletion vectors, and certified SQL still filters tombstones.

`SliceInput` distinguishes unversioned input, winner selection, and certified input. This prevents the caller from expressing certification by passing empty keys or removing the tombstone predicate. The existing publication event records `certified_clean` for eligibility analysis without a new permanent stats key. All 162 selected rollup, certification, configuration, and storage tests passed. The candidate has no measured CPU saving and remains disabled by default.

The experiment pairs both paths on identical snapshots, including stale certificates, deletes, late versions, ties, and restart recovery. Its gate is at least 20% lower CPU per qualifying build, with identical results and no resource-limit regression. Existing scan-duration and rebuild stats support rollout. Benchmark output supplies actual CPU and certificate eligibility rates. Rollout also requires lower combined server work and the shared correctness, freshness, and foreground latency gates.

The local `rollup_work` benchmark exercises the real coordinator in ABBA/BAAB order. The certification smoke command is `cargo bench --profile dev --bench rollup_work -- 128 certified`. The separate `batches` mode changes only batch sizing. It uses only localhost MinIO, deterministic logical records, and isolated test prefixes. Certification preparation and rollup building have separate process-CPU and elapsed-time measurements. Existing counters report scans, estimated input bytes, output rows, output files, and commit actions. The benchmark refuses a wrong aggregate or an unexpected execution path. Its output names the experiment, candidate arm, and debug-assertion status.

All eight smoke arms completed on the dirty `d9f00cce` worktree. Each arm produced 120 live rows from 128 inputs, 13 scans, three output rows, three output files, and three commit actions. Enabled arms recorded twelve certified publication events. Those events include empty builds and do not measure useful-byte eligibility. Estimated input bytes matched at 116,022 per arm.

Build CPU ranged from 1.62–1.82 seconds with winner selection and 1.37–1.43 seconds with certified input. These are unoptimized, tiny-fixture observations, not an accepted saving. The separate legacy certification sweep cost 17.22–17.84 CPU-seconds per arm, including its configured seven-day lookback. This does not price incremental certificate reuse in production. A build must reuse existing evidence, not start this sweep solely to qualify for the fast path.

The benchmark excludes the MinIO process, foreground traffic, sustained ingestion, and peak-memory measurement. Endpoint RSS was unavailable on this laptop and remained null. Independent prefixes contain equivalent logical records, but file identities and write stamps differ. Larger optimized runs, identical-snapshot comparisons, source-view races, and the shared acceptance protocol remain open. Synthetic MinIO prefixes remain for inspection. The first smoke attempt was stopped because its unrestricted tracing subscriber enabled unrelated log levels. The completed run restricted collection to maintenance INFO events.

Formatting and whitespace checks passed after the benchmark addition. The fresh `cargo lint` run failed on the existing 176-byte error value in `merge_generation_ranges`. The proposed in-place merge still awaits approval. No lint suppression, CI attestation, commit, or production activation occurred.

### Stage 1B: remove hash-shard scan amplification

The current runner executes one query per hash shard against the same selected source provider. The split logic can retain this path whenever further time splits fail to reduce the physical input footprint. This is an existing execution path, not only a hypothetical oversized minute. Computed key hashes generally cannot prune Parquet input through timestamp statistics. Repeated key decoding and hashing remain possible even with cached object bytes or late materialization of other columns.

First, inventory actual executed units from existing publication logs and recorded preflight estimates. The runner calculates `max(1, estimated_bytes.div_ceil(MAX_DECODED_BYTES))` after preflight and splitting decisions. The journal's `hash_shards` field is not necessarily that executed count. Superseded parents and metadata-only skips do not count as scans.

Report shard-count distributions weighted by input bytes and attributed CPU, not task count alone. Separate retries and missing observations. Historical records establish exposure, not actual decode amplification. If estimates are absent, use lightweight sampled execution records before larger experiments.

The inventory reports executed units in the 1, 2, 3–4, 5–8, and greater-than-8 shard groups. Each group includes observation count and estimated input bytes. CPU-weighted results remain unknown until attribution supplies CPU evidence. This inventory precedes controlled shard experiments and requires no new source scans.

The local inventory tool now emits these bands alongside its exact per-table shard counts. Recorded counts and counts inferred from the baseline formula remain separate. Each evidence class includes the multishard fraction of estimated input and the ratio of shard-weighted bytes to estimated input bytes. Zero-byte input produces null ratios, not a claim of zero amplification. These ratios describe successful publication observations only, not actual decoded bytes or attainable CPU savings.

All five inventory tests passed after two new cases failed on the missing summaries. The cases cover every band boundary, cross-tier totals, separate evidence classes, and empty input. The command was `PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts -p test_rollup_work_inventory.py`.

The September 25 hourly sample now establishes material estimated shard exposure. The evidence appendix records its scope and missing source identity. The next experiment compares 1/2/4/8 shards against one isolated snapshot, with physical scan metrics and exact winner-result checks. This does not authorize production activation or establish actual scan amplification. CPU attribution and representative workload matching remain required.

Only material exposure justifies the paired 1/2/4/8-shard experiment on an identical isolated snapshot. Only measured amplification justifies the replacement experiment. The candidate reads projected source input once and partitions bounded work through an existing spillable execution path. All versions of a complete dedup key enter the same partition.

Aggregation follows winner selection and tombstone filtering. The merge combines complete partial aggregate states, not finalized averages or percentiles.

The implementation must demonstrate its actual spill guarantees, scratch limits, cooperative yields, and crash cleanup. It must also bound intermediate aggregate states across shards. A bounded input pass cannot feed an unbounded state vector. An indivisible key or high-cardinality bucket must progress or report an explicit capacity limit without repeated immediate retries. The current bounded path remains available until the replacement passes correctness and resource tests.

The gate is at least 30% lower CPU or physical read bytes on affected units, including spill overhead. Peak memory and scratch usage must stay within admission limits, and unrelated project latency must not regress. Existing scan and invalidation-age stats support rollout. Benchmark reports record scan multiplicity and spill cost without new permanent keys.

#### Local single-snapshot scan experiment

The test `rollup_shard_scan_experiment` reads the same two Parquet files for every arm. They contain 4,096 identities, each with an older and a newer version. Newer versions change duration, and every seventh identity becomes a tombstone. Eight services exercise group boundaries.

The experiment reuses the production slice SQL, dashboard aggregation, state conversion, merge SQL, and bounded maintenance session. It mirrors the current hash-range predicate. The order is 1/2/4/8/8/4/2/1 shards. Each arm verifies count, duration sum, minimum, and maximum for every service against the fixture oracle.

| Shards | Parquet `bytes_scanned` per arm | Local elapsed range |
| --- | ---: | ---: |
| 1 | 143,171 | 0.164–0.193 s |
| 2 | 286,342 | 0.276–0.284 s |
| 4 | 572,684 | 0.456–0.465 s |
| 8 | 1,145,368 | 0.799–0.817 s |

The eight arms passed in one 3.83-second test run. The command was `cargo nextest run --lib -E 'test(rollup_shard_scan_experiment)' --no-capture`. An earlier fixture run failed because a temporary view remained registered. The corrected fixture releases each view after consumption. That failure was not evidence of a production bug.

These measurements establish repeated Parquet input requests on this fixture, including with warm caches. DataFusion increments `bytes_scanned` for requested byte ranges before the reader returns them. The metric does not measure disk-cache misses, decoded Arrow bytes, or CPU. Local elapsed time includes planning and a common state merge, including for the single-shard arm.

The fixture uses local Parquet, not Delta admission or publication. The initial run did not force spills or verify sketch equivalence. It also does not represent the session workload that dominated the hourly inventory. A bounded single-pass prototype is now justified for isolated evaluation. Production-sized state, scratch limits, cancellation, and representative CPU/I/O comparisons remain required before activation.

The follow-up spill experiment uses a real `FairSpillPool`, an 8 MiB scratch limit, and a test-only 64 KiB sort reservation. Both the 512 KiB and 1 MiB pools returned typed `ResourcesExhausted` errors. At 1 MiB, the sort requested 576.4 KiB with only 480 KiB available. The three-case run passed two cases and failed the expected-success spill case in 5.01 seconds. The ordinary-pool case passed, and the 512 KiB case correctly reported the capacity error.

The pinned DataFusion 54.1 sort cannot spill an empty input buffer to admit its first batch. Its reservation includes retained Arrow buffers and the logical batch size. Thus, a spillable operator alone does not prove bounded progress. Admission must account for minimum batch reservations and concurrent consumers, not only the total input estimate. A capacity error must preserve pending work without immediate retry loops. This fixture does not yet prove that scheduler behavior.

The next run retained both capacity-error cases and required successful spilling under a 2 MiB pool. It passed three cases and failed the spill-success case in 6.00 seconds. The sort requested 1,088.4 KiB with only 992 KiB available. Thus, doubling the pool did not resolve the failure. The spill-success assertion remains active and failing.

The experiment now records spill count and spill bytes through DataFusion's dedicated metric accessors. Name-based metric lookup excludes these built-in metrics. The ordinary-pool arms reported zero spills and retained the same 1/2/4/8-times source bytes. DataFusion records spill-file growth in `spilled_bytes`, not spill reads. Source-read savings must include spill writes and reads before they qualify as total I/O savings. Successful arms also require zero remaining memory and scratch reservations.

The diagnostic run identified the failure in the first two-shard query, after the one-pass arm completed its correctness and reservation checks. The dedup `SortExec` reported two spills, 224.4 KB of spill writes, and zero output rows. Its input filter selected hash buckets `[0, 32768)`. The filter produced seven batches with about 1,790 rows before the error. Thus, the 2 MiB failure does not establish that the one-pass arm failed. The diagnostic test still failed overall in 0.63 seconds.

The test now reports each completed arm immediately, before later arms can fail. Each bounded one-pass arm must demonstrate nonzero spills before execution continues. Error reports include the physical plan and its metrics while preserving the typed error chain.

The next four-case run passed three cases and failed the two-shard spill case in 5.68 seconds. All three bounded pools completed their first one-pass arm with correct aggregates and zero remaining memory and scratch reservations. This corrects the earlier interpretation of the capacity errors: none establishes a one-pass minimum above these pool limits.

| Pool limit | One-pass source bytes | Spill count | Spill writes | Elapsed time |
| --- | ---: | ---: | ---: | ---: |
| 512 KiB | 143,171 | 47 | 4,490,312 | 0.245 s |
| 1 MiB | 143,171 | 18 | 2,304,080 | 0.218 s |
| 2 MiB | 143,171 | 7 | 1,242,168 | 0.216 s |

These measurements establish bounded one-pass feasibility for this fixture, not a production admission limit. Even at 2 MiB, source reads plus spill writes exceed the ordinary-pool eight-shard source bytes. That comparison uses different memory limits and is not an acceptance result. It shows why the design must measure spill overhead rather than infer total I/O savings from source bytes. Smaller pools also increase spill writes sharply. CPU attribution, scratch reads, peak resident memory, cancellation, and high-cardinality state remain unmeasured.

`FairSpillPool` divides spillable capacity among registered consumers after unspillable reservations. Its error reports the consumer's allowance, not all unused pool memory. Any replacement must preserve the correctness oracle and successful-spill requirement without an unbounded pool.

The next diagnostic replayed the failing sort's input without the sort. Peak retained batch memory was 1,081,600 bytes. The existing `compact_batch` helper reduced this peak to 41,216 bytes, about 26 times smaller. Every compacted batch equaled its original batch. This establishes avoidable input-buffer retention on the fixture, independently of sort workspace. It does not yet prove that compaction makes the full query pass.

The pinned Arrow 58.3 coalescer doubles backing-buffer allocations up to 1 MiB. Its batch-finalization method does not reset that allocation-size state. This explains how small filtered batches can retain much larger backing allocations. The next candidate reuses `compact_batch` before the dedup sort reserves its input. It must preserve streaming, row order, partition properties, and cancellation without an unbounded intermediate collection. CPU and spill costs must include compaction.

The diagnostic remains test-only and replays synthetic files outside successful-arm timings. Its replay reuses physical nodes, so the final rendered input metrics include diagnostic work and are not acceptance measurements. The original typed resource error remains the test outcome. This run failed one test in 0.90 seconds, as expected before an implementation fix. An earlier diagnostic revision did not compile because it used unavailable DataFusion APIs. The corrected revision uses the public retained-memory function. No dependency source or production execution path changed.

#### Candidate: compact input before rollup sorts

The isolated implementation now appends a physical optimizer rule only to bounded rollup sessions. The rule inserts `CompactRollupSortInput` before sorts and avoids duplicate wrappers. The wrapper reuses `compact_batch`, forwards partition and ordering properties, and maps one batch at a time through a cooperative stream. It adds no queue, spawned task, durable record, or dependency change. Foreground sessions retain their existing rules.

The four-case run passed three cases in 12.62 seconds. Both the 1 MiB and 2 MiB cases now completed all eight arms with correct aggregates and released execution and scratch reservations. Those pools previously failed in the first two-shard sort. The ordinary-pool case also passed. All bounded cases now require success rather than accept the former typed capacity errors.

The 512 KiB case still failed, but after its one-pass and two-shard arms completed. The failure moved to aggregate spill reservation during the four-shard arm. It requested 39.7 KB with 36.7 KB available. That success assertion remains active and failing. The candidate fixes the observed sort-input retention failure, not every minimum-memory constraint.

With compaction, one-pass spill writes at 1 MiB decreased from 2,304,080 to 1,253,456 bytes on this fixture. At 2 MiB, they increased slightly from 1,242,168 to 1,267,224 bytes despite fewer spill events. Thus, spill counts alone do not establish savings. Source-read counts stayed unchanged. These local observations do not replace matched CPU or foreground-latency measurements.

Compaction temporarily holds the source batch and its replacement. The current tests establish reservation release, not a hard bound on peak resident memory. Wider production schemas, high-cardinality aggregate state, cancellation during a full spilling query, and broader rollup regressions remain verification work. The candidate is uncommitted and undeployed.

Three focused wrapper cases passed in 0.14 seconds with `cargo nextest run --lib -E 'test(compact_rollup_input)' --no-capture`. A real two-partition Arrow source supplies ordered rows and oversized string buffers. The cases preserve row order and values, forward the original plan properties, and prove rule idempotence. They also require source-array release after stream drops before polling, after one batch, and after full consumption. Emitted batches must retain less than one-quarter of the original batch memory. These checks cover wrapper cleanup, not cancellation of a complete spill pipeline. The fixture first required corrections for an absent import and an unavailable builder method.

The latest `cargo lint` still fails on `result_large_err` in `src/rollup.rs:692`. It reported no additional error for the wrapper. Rust review found no new unsafe code, suppression, or widened production API. The follow-up `cargo nextest run --lib -E 'test(rollup)'` completed in 280.45 seconds: 211 passed, one failed, and 13 were slow. The only failure was `rollup_shard_scan_experiment::half_mib_spill_pool`, with the same aggregate-spill reservation error. The run includes coverage, source-invalidation, restart, publication, and hybrid-routing regressions. It is a rollup selection, not the complete repository suite. No passing full-suite result or CI attestation is claimed.

The local diagnostic wraps the same bounded fair pool with DataFusion's existing `TrackConsumersPool`. This reports competing reservations without increasing the budget or accepting the resource error as success. Tracking adds diagnostic overhead, so new elapsed times are not directly comparable with earlier untracked samples. The aggregate needs temporary memory to sort emitted states before a spill, independently of the source-sort input reservation.

The tracked 512 KiB run failed in 1.32 seconds. `ExternalSorterMerge` held 475.3 KiB as non-spillable memory. The aggregate had released its accumulated state and retained only 216 bytes, but its 39.7 KiB spill-sort request exceeded the remaining 36.7 KiB. Thus, input compaction alone cannot solve this failure. Admission and execution must preserve downstream spill headroom while the upstream merge retains its inputs. This evidence does not justify more server memory or compaction before every operator.

The corrected diagnostic captures failure metrics before replay and builds a separate physical plan for replay. It requires failed queries to release execution and scratch reservations within five seconds. This fixes an observation limitation: reused scan nodes can be exhausted and produce zero replay rows. The cleanup condition passed. Fresh replay reported 41,216 bytes both before and after further compaction, consistent with already-compacted sort input. The original capacity error remained the test outcome in the 1.42-second run.

The pinned `StreamingMergeBuilder` supports multi-level merging but exposes no fan-in ceiling or downstream headroom parameter. Its internal merge selection grows reservations until the pool refuses them. The configured `sort_spill_reservation_bytes` is an initial reservation, not a maximum for the final merge. The implementation must not treat that setting as a whole-pipeline budget.

The 512 KiB regression reproduced in 1.813 seconds on the current candidate. The first one-pass and two-shard arms passed. The four-shard arm again failed with 475.3 KiB in the upstream merge and insufficient aggregate spill workspace.

The next candidate reserves downstream headroom from the same pool before sort execution. It releases that reservation after the first output batch, when the blocking sort has selected its final merge. The reservation uses the existing configured spill-reservation size. It does not increase the pool, inspect consumer-name strings, or change dependency code. The stream owns the reservation, including during cancellation and error paths.

The 512 KiB case now completes all eight shard arms with correct checked aggregates and zero remaining execution and scratch reservations. That case and the three existing input-wrapper cases passed in 5.493 seconds after a 3m54s build. One-pass spill writes increased from 2,808,232 to 4,268,184 bytes. This is a resource-progress result, not an I/O-saving result.

All five memory/input cases then passed in 47.272 seconds. The 1 MiB and 2 MiB one-pass spill writes remained 1,253,456 and 1,267,224 bytes. The larger input still requested 2,519,438 source bytes with one pass versus 20,155,504 with eight shards. These local observations do not establish production capacity or latency.

Five additional real-sort cases cover reservation release before polling, after first output, at EOF, for empty input, and after admission refusal. All five and the three existing wrapper cases passed in 0.085 seconds after a 3m11s build. The cases require zero retained reservations and preserve sorted values. Formatting and whitespace checks passed. Fresh `cargo lint` passed in 1m56s. Cancellation during an active spill remains required.

Two new cancellation cases use the production sort rule, a 512 KiB fair pool, and an 8 MiB scratch limit. One drops the stream after scratch usage becomes nonzero, before first output. The other drops it during the final merge, after first output. Both require active spill files, the correct headroom state, zero remaining reservations, and deletion of physical spill files. The cases wait on observed state rather than fixed sleeps.

The first build found a missing `anyhow::Context` import in the new fixture. After that correction, nine of ten wrapper cases passed in 0.228 seconds. Cancellation during spill preparation passed. The final-merge case reached first output with zero scratch usage, so it did not exercise the required cancellation point. Increasing the fixture from 64 to 256 batches exceeded the unchanged 8 MiB scratch limit before output. That run again passed nine cases and failed one, in 0.170 seconds.

The next fixture uses 128 batches with 32-byte strings instead of 128-byte strings. Its row and view buffers exceed the memory pool, with less payload for spill writes. The pool, scratch limit, and active-spill assertion remain unchanged. Nine cases passed and the final-merge case again failed at zero scratch usage, in 0.219 seconds. No production code changed after the passing memory/input matrix.

Dependency inspection changed the interpretation of those failures. In pinned DataFusion revision `155b68e`, `SpillReaderStream` opens the file but does not retain its `RefCountedTempFile` owner. That owner drops after the first blocking read, while the returned reader retains an open file handle. The drop decrements scratch accounting and can unlink the file before the reader finishes. Thus, zero reported scratch usage does not prove that a final merge no longer uses disk. The earlier fixture-size explanation was not established.

A focused regression writes two real batches and opens an unbuffered spill reader. After the first batch, it requires the full disk charge to remain until reader cancellation. It failed in 0.224 seconds: reported usage fell from 4,584 bytes to zero while the second batch remained unread. The existing final-merge cancellation assertion remains active. This dependency lifetime must be corrected before the scratch limit can establish a physical-disk bound for single-pass execution.

A local copy of the pinned dependency now carries `RefCountedTempFile` with the reader through its waiting and in-progress states. Completion, cancellation, or error drops both together. The change adds no allocation, extra file handle, or lint suppression. Its native regression covers both EOF and cancellation after partial consumption. All 45 dependency spill tests passed in 0.183 seconds after a 29.88-second build. Timefusion still uses its original dependency pin, so its disk-accounting regression remains red until integration. No fork commit or external push occurred.

The first dependency run stopped after 22 passes and one failure in the file-rotation fixture. Its threshold produced two batches per file, despite comments that described one. The fixture now uses a threshold below one batch and requires exactly three files. Its existing assertions still require partial disk release before completion and zero disk usage after cleanup. The reader must retain the charge until the next poll closes the consumed file.

Local dependency integration remains incomplete. Cargo accepted a `paths` override but warned that it changed the dependency graph. That configuration cannot support acceptance evidence. A git-source patch with the unchanged lockfile failed dependency resolution. Neither attempt changed the Timefusion dependency pin or established an integration result. A consistent local package graph, the Timefusion cancellation regressions, and dependency lint remain required.

A temporary `[patch.crates-io]` configuration now resolves all 33 DataFusion workspace crates to the local checkout without dependency-graph warnings. The generated lockfile differs only by removal of those 33 git-source lines. Package versions and dependency lists remain unchanged. The original lockfile is saved outside the repository for restoration after the experiment. The main workspace lockfile and both manifests retain their original dependency references.

The integration command is `cargo nextest run --config /tmp/timefusion-datafusion.u4aBXK/timefusion-local-patch.toml --locked --offline --lib --no-fail-fast -E 'test(compact_rollup_input_tests) | test(rollup_shard_scan_experiment)'`. The configuration must follow `nextest run` so its Cargo subprocesses receive the override. The earlier command placed it before `nextest` and failed during metadata resolution, before compilation. The corrected run passed all 16 selected tests in 35.636 seconds after an 8m16s build. Both former disk-accounting failures passed, with the existing memory and scratch limits unchanged. All five scan-experiment cases also passed, including the sketch assertions.

The test build reported vendor dead-code warnings, a linker unwind-table warning, and a dependency future-compatibility warning. These results do not establish a warning-free build or whole-suite signoff. The standard `cargo lint` passed in 3m20s with the same patch in the isolated Cargo configuration. Its first invocation missed the command-line override and failed before compilation. High-cardinality output, peak RSS, foreground contention, and production acceptance remain separate gates.

After integration, the original Cargo configuration and lockfile were restored without residual diffs. The lockfile SHA-256 is `653caed8eec4eea0f3ede7506f584678f59db3b0c9ba7f151e3b968d9ca5e601`, identical to the saved baseline. The tested dependency fix remains in the isolated DataFusion checkout, not the production pin. Timefusion lint does not cover the dependency's own test code.

Direct dependency lint failed on two existing Rust 1.98 findings in `datafusion-common`. A package-only pass also found five existing findings and two introduced by the first candidate. Retaining the file owner enlarged `SpillReaderStreamState` to 256 bytes and triggered `large_enum_variant`. The changed test message also triggered `uninlined_format_args`. The message now uses inline formatting. No lint suppression was added.

The revised candidate stores the reader and file owner in one boxed pair. It allocates once per open spill reader, then moves the same box between blocking tasks. This corrects the earlier no-allocation claim while avoiding a new allocation per batch. The pair keeps the reader and disk charge together through EOF, error, and cancellation. Package lint no longer reports either introduced finding, but five findings in unchanged code still prevent a passing result.

The native lifecycle fixture now requires zero disk usage and physical file deletion immediately at EOF, before stream drop. Its cancellation case still requires both after partial consumption. All 45 spill tests passed with the revised layout in 0.178 seconds after a 1m53s build. Earlier Timefusion integration and lint results apply to the unboxed candidate, so both require another run. The native run passes the debug-profile configuration through nextest explicitly, which caused a dependency rebuild. Formatting and whitespace checks passed.

The scan experiment now also checks merged sketches outside its timed section. It compares p50 and p95 with surviving source values at a declared 1% rank-error tolerance. It requires HLL cardinality one within each service and eight after merging all service groups. These checks use the existing SQL functions, not a duplicate sketch decoder. All five memory/input cases passed with the additional assertions. The combined six-test run finished in 35.453 seconds, with only the disk-accounting regression failing. These cases still contain eight aggregate groups, not a high-cardinality workload.

The next matrix adds two 512-service cases with 4,096 source identities: the ordinary pool and a 2 MiB pool. Each service retains multiple live values after tombstone filtering. Both cases use the existing forward/reverse shard order, production aggregation, and merged-state checks. Count, sum, extrema, and percentile requirements remain unchanged. The global HLL check permits 2% error for 512 services, while integer rounding retains the exact-count requirement for eight services.

These additions exercise more aggregate states, not peak process memory or arbitrary tenant cardinality. No pool or scratch limit increased. All 18 selected Timefusion cases passed against the boxed spill-reader revision in 49.811 seconds after an 8m35s build. Nextest reported one process-leak warning without a case name at the configured status level. The same selection now runs with per-test status visible. This result is not a clean signoff, and the temporary Cargo patch remains active until the checks finish.

The diagnostic rerun passed all 18 cases in 42.256 seconds without a leak warning. This does not identify the earlier warning's cause. The revised matrix now requires exact HLL cardinality for both eight and 512 services, consistent with the sparse representation.

The t-digest regression failed at the intended memory assertion in 0.167 seconds: reported usage was 1,672 bytes for at least 3,224 allocated bytes. It decodes 201 weighted centroids with one dominant weight, then checks the compressor's retained capacity. All seven scan cases passed with exact HLL assertions in the same 33.788-second run. This reproduces local under-accounting, not a measured production memory total.

The candidate stores a populated digest and its centroid capacity together in `AccountedDigest`. Construction captures the input vector's capacity. Compression records the replacement capacity from the pinned tdigests implementation. Merge preparation retains the existing slice concatenation behavior and captures its allocation before ownership passes to the digest. Small digests retain a small charge rather than a blanket 200-slot reservation. Serialization and percentile semantics remain unchanged.

The candidate adds one capacity field per populated digest, without a new allocation, lock, or commit. It depends on the pinned compressor's allocation rule, which requires review on dependency upgrades. All 62 selected function and scan tests passed in 43.331 seconds after a 3m12s build. This includes the original capacity regression and all seven scan cases with exact HLL counts. No leak warning occurred in this run.

The expanded capacity regression passed: a two-value digest retains a small charge, and a merge replaces the old allocation charge. Both accumulator-storage regressions failed at the intended assertion in the same 0.178-second run. HLL reported 48 bytes for a 56-byte accumulator. T-digest reported 32 bytes for a 40-byte accumulator. The eight-byte omission affected each accumulator object, separate from retained centroid capacity.

`SketchAccumulator::size()` now adds its non-sketch storage and padding to the sketch's existing allocation report. The private sketch contract explicitly includes the sketch value itself, so the calculation does not count it twice. This adds no allocation or runtime state. All 86 selected function, HLL, spill-wrapper, and scan tests passed in 52.087 seconds without a process-leak warning. Standard `cargo lint` remains active. The temporary dependency configuration remains active for these checks.

The [dependency review artifact](../../patches/datafusion/README.md) records the exact base, patch checksum, reproduction commands, and unresolved lint gate. Read-only forward and reverse application checks passed. This artifact preserves the candidate outside the temporary checkout but does not replace a reviewed dependency revision.

The requested [dependency upgrade](2026-09-25-dependency-upgrade.md) now targets compatible upstream bases with our fork behavior preserved. DataFusion 55.1 changes spill ownership and shared-buffer accounting, so old patches need semantic review before replay. Old-stack tests remain evidence for their original revisions only.

The kernel evaluator port now checks actual output fields before it reuses an identity transform. All 250 selected expression and schema tests passed. The Delta candidate reproduced incorrect physical positions after deletion-vector filtering in three regression cases. The port now advances positions across deleted rows, and all 49 scan-execution tests passed. These results preserve required fork behavior. They do not establish full-stack compatibility or resource savings. Exact bases, commands, and remaining checks are in the dependency upgrade document.

The next Delta port shares immutable deletion masks across executions and keeps only per-file cursors in each stream. It removes full-mask copies and repeated shifts of the remaining mask. All 50 scan tests passed, including reset and concurrent execution checks. Batch-sized mask allocation remains, and final-stack resource measurements are still required.

Timefusion `cargo lint` then passed in 5m04s against the boxed dependency and corrected sketch accounting. The original Cargo configuration and lockfile are restored with no residual diff. The lockfile checksum matches the saved baseline. The dependency's own lint gate remains unresolved. No commit, CI attestation, or production activation occurred.

#### CPU measurements at a fixed memory limit

The experiment now uses the existing process-CPU clock from `rollup_work`, shared through `support::test_helpers`. Each arm measures planning, execution, state conversion, and the final merge. Fixture creation, result checks, and failure diagnostics remain outside the measurement. The clock includes all process threads.

Three runs used the same 1 MiB limit and 1/2/4/8/8/4/2/1 order. Each completed all eight arms with correct count, sum, minimum, and maximum results. Successful arms released their execution and scratch reservations.

| Shards | CPU-seconds per arm, six observations | Source bytes per arm | Spill writes per arm |
| --- | ---: | ---: | ---: |
| 1 | 0.174–0.236 | 143,171 | 1,253,456 |
| 2 | 0.274–0.305 | 286,342 | 1,230,736 |
| 4 | 0.445–0.473 | 572,684 | 1,275,904 |
| 8 | 0.782–0.795 | 1,145,368 | 0 |

These observations show lower CPU for one pass, even with spill work. The mean CPU reduction versus eight shards was approximately 76% on this fixture. This is not a confidence bound or a production saving. Fixed planning cost can dominate this small fixture, which contains only eight aggregate groups.

The I/O tradeoff remains material. One pass requested 143,171 source bytes and wrote 1,253,456 spill bytes. Their sum exceeds the eight-shard source bytes before spill reads enter the comparison. Thus, lower CPU does not establish lower total I/O or higher server capacity.

The larger case contains 65,536 identities and two versions per identity. Three runs completed all eight arms under the ordinary maintenance pool. No arm spilled.

| Shards | CPU-seconds per arm, six observations | Source bytes per arm |
| --- | ---: | ---: |
| 1 | 1.658–1.858 | 2,519,438 |
| 2 | 2.297–2.383 | 5,038,876 |
| 4 | 3.074–3.263 | 10,077,752 |
| 8 | 4.917–5.013 | 20,155,504 |

Mean CPU fell from 4.964 seconds with eight shards to 1.732 seconds with one pass, approximately 65%. Source-byte requests fell by 87.5%. Both paths returned the same checked aggregates. This supports the single-pass candidate at a larger input size, but not production activation.

Both sizes still have eight aggregate groups. High-cardinality states, session rollups, foreground contention, peak resident memory, and spill-read cost remain unmeasured. These unoptimized local runs also do not satisfy the sustained acceptance protocol. Production shard policy remains unchanged.

The final five-case matrix passed four cases and failed the known 512 KiB case in 42.19 seconds. The three larger-input runs passed in 29.13, 28.79, and 29.20 seconds. The command for that case was `cargo nextest run --lib -E 'test(rollup_shard_scan_experiment::larger_input)' --no-capture`.

The initial four-case run passed three cases in 12.50 seconds. The 512 KiB case still failed during aggregation after its one-pass and two-shard arms completed. Two additional 1 MiB runs passed in 3.74 and 4.23 seconds. The success assertions, capacity error, and cleanup checks remain unchanged.

`cargo check --bench rollup_work` passed after the clock helper moved. The next lint run also found `items_after_test_module` in the sort-input tests. Moving that module after the production items removed this diagnostic without changing test bodies. The lint rerun still failed on `merge_generation_ranges`. All three relocated runtime tests passed in 0.08 seconds. Formatting and whitespace checks passed.

#### Worker capacity retries

Inspection of the no-spin path found a separate scheduler bug. `abandon_running` converts recognized allocation failures to `resource_exhausted`, then calls `retry_or_split`. The shared classifier did not recognize that canonical value. Repeated worker OOMs therefore bypassed both capacity splitting and escalated backoff after a refused split. The existing direct-admission and timeout tests did not exercise this path.

Two real-journal regression cases failed before the fix. A day-sized BaseRollup unit produced no smaller pending work. An indivisible unit with six attempts retained the transient 30-second delay instead of waiting at least 64 seconds. The failing run completed in 0.21 seconds. The fix adds exact recognition of the existing canonical value to the shared classifier, without a new reason or journal format. A direct classifier case also covers that value. The full maintenance-coordinator selection passed all 169 tests in 4.88 seconds. The two regression cases now also checkpoint and reload the real journal. They require recovered child intervals to cover the parent exactly once, or an indivisible unit to retain its backoff without multiplying work. The stronger restart checks passed in a second full coordinator run: 169 tests passed in 4.65 seconds. `cargo fmt --all --check` and the whitespace check also passed.

This scheduler fix does not make the 512 KiB aggregate query fit. It restores the existing repeated-failure policy while preserving work. Whole-pipeline admission, resource-limit reporting, and representative throughput measurements remain required.

The follow-up run passed the experiment and the existing Parquet metric-name test. The maintenance source-text guard failed on five foreground-query calls in earlier tests. Its text heuristic does not distinguish those tests from production maintenance. The guard remains unchanged pending a scope-aware correction. This run passed two of three tests, not the full verification gate.

Formatting and whitespace checks passed. Review of the sort-input candidate found no new suppression, unsafe code, or widened production API. The CPU experiment reuses the benchmark's existing clock syscall and safety checks through the shared test helpers. It adds no dependency or production policy change. The distillation review identified duplicated shard-predicate construction as a candidate for a shared helper, subject to approval. The latest `cargo lint` still fails on `result_large_err` at `src/rollup.rs:692`. No code was committed or activated.

### Stage 1C: share source preparation across compatible aggregates

Dashboard and session base rollups currently build independently from logs. Their overlapping repairs can repeat source reads and winner selection. If session builds pause for lack of consumers, defer this pairing. Do not retain unused builds to justify shared execution.

This stage requires at least two useful consumers and measured duplicate preparation after the earlier changes. The candidate shares one projected input stream and one winner stream across compatible specifications. It does not derive sessions from dashboard states because their dimensions and measures differ.

Compatibility requires the same project/source, pinned snapshot, winner semantics, and overlapping requested ranges. The scan projects the union of required inputs. Separate accumulators retain each specification's range, grain, generation, and measures. No consumer expands another consumer's logical coverage or makes an unrelated historical repair urgent. Different requested ranges can share decoded input only with exact per-consumer predicates. Duplicate versions must remain together before those predicates filter non-key values.

Each consumer reserves memory within one global byte budget. Backpressure and spill must remain bounded. If shared execution exceeds its budget, retain separate builds rather than an unbounded broadcast cache. Each publication retains independent source checks, task completion, and recovery identity.

A failed publication does not erase another consumer's committed work. Stage 3 can later batch compatible commits. A shared SQL expression is insufficient: the physical plan must contain one source scan and one required winner pass.

The gate is at least 20% lower combined CPU for the affected builds, including extra columns and state memory. Freshness and foreground p99 must meet the shared limits. Recent work must not wait for a compatible historical task. Existing rebuild and scan stats support rollout. Benchmark output compares combined work, not the fastest individual consumer.

### Stage 1D: prevent invalidation from irrelevant changes

Each specification records dependencies on dimensions, measures, filters, identity, timestamps, version ordering, and deletion semantics. Trusted partial updates can preserve aggregates whose logical inputs and row membership remain unchanged. For example, user enrichment can affect session aggregates without changing dashboard aggregates. An identical retry can preserve coverage only with proof that its winning logical contribution stays identical.

Changed-column names alone are not proof for a stale full-row replacement that becomes the new winner. Unknown writes, deletes, timestamp moves, identity changes, and uncertain replacements retain conservative invalidation. Timestamp moves affect both the old and new intervals. Specification changes invalidate their own dependencies. The classifier uses existing durable mutation evidence. It adds no per-row lookup or independent synchronous commit.

Initially, this stage suppresses only work for which existing physical evidence remains sufficient. Preserving logical coverage across a changed source fingerprint requires stage 2, including atomic visibility and publication checks. Skipping a rebuild cannot leave a router that rejects the same coverage indefinitely.

The experiment compares CPU from avoided builds with classifier, replay, and metadata cost. Its gate is at least 20% lower rebuild CPU in the irrelevant-update cohort and no more than 5% insert p99 regression. Existing rebuild, miss, and invalidation-age stats support rollout. Mutation fixtures must show zero missed relevant invalidations.

## Stage 2: prove source visibility before logical-only routing

A date epoch is useful mutation ordering, but it is not a snapshot across Delta, MemBuffer, and in-flight flushes. Minute exceptions alone do not solve this problem either. This stage extends the existing epoch mechanism and introduces a captured source view before weakening current freshness gates.

### First establish what today's hybrid path guarantees

Hybrid routing already ships. In `src/dml.rs`, physical planning finishes before `rollup_ticket_current` decides whether to accept the rewrite. Date tickets recheck epoch and fingerprint; slice tickets recheck retained slice identity and generation. This safeguard alone does not prove a shared visibility boundary across raw Delta, rollup Delta, and buffered rows. The local regressions described here reproduced a raw-read omission, not movement of an accepted snapshot. The broader snapshot guarantee remains unproven.

Before stage 0 rollout, add a deterministic regression that pauses routing, provider capture, ticket validation, and execution in turn. Interleave inserts into clean and raw ranges, late writes, deletes, and a flush handoff. Assert a result from an allowed single source view, without omissions or double counts; compare captured rows, not a later live raw query. If provider snapshots plus ticket validation already establish that view, document their lifetime and reuse them, reducing stage 2's scope. If the test fails, treat it as a present correctness bug, preserve the failing schedule, and fix or disable the affected hybrid route first. Do not defer a reproduced bug until the new architecture arrives.

The local `an_accepted_hybrid_plan_keeps_its_source_view` fixture covers writes after physical-plan acceptance and before execution. It exercises direct Delta writes and a real WAL/MemBuffer layer. A hit-counter assertion requires actual hybrid routing. The captured plan must return two original rows, while a new query sees all four rows.

The first run passed the direct-write case but failed the buffered case: the old and fresh queries both returned two rows. The captured plan retained its expected result. A diagnostic rerun also failed a plain ID query: only `late-covered` and `late-raw` survived. Both previously committed identities disappeared. This identifies a raw-read failure, not proven movement of an accepted snapshot.

`ProjectRoutingTable::scan` excludes Delta timestamps covered by buffered rows. A late row with a new key cannot prove ownership of other keys at that timestamp. The local candidate removes those exclusions for merge-on-read tables with declared deduplication keys. Their existing union deduplication resolves overlapping identities, and tombstones suppress deleted keys. The original project and query-time predicates remain unchanged. Other table types retain their existing behavior.

The candidate passed all three focused tests in 99 seconds: the new-key/tombstone regression and both hybrid-view cases. Fresh queries retained all four identities, and accepted hybrid plans retained their original two-row view. The focused regression also proved that a tombstone removes only its matching key. The diagnostic test failed before this change, exposing both missing Delta identities.

Five existing merge-on-read regressions also passed: version selection, tombstones, and enabled/disabled plan-shape checks. This correction can increase reads of overlapping Delta rows. Resource comparisons must use correct results as their baseline, not undercounting queries. Provider-capture interleaving, indexed histogram paths, deletes during capture, and flush handoff still require checks. Stage 0 activation remains gated on them.

The real indexed-file regression also reproduced the undercount through `capture_histogram`: one late buffered key reduced the expected two-key count to one. The candidate now applies the rule in `MemBuffer::get_bucket_ranges` and `snapshot_for_merge`, rather than only in ordinary scans. Version-append tables expose no timestamp-wide exclusion ranges. Schema validation already requires their keys, version column, and tombstone column. Both readers retain overlapping Delta keys for version resolution.

The combined rerun passed all six tests in 117 seconds. It covers both histogram execution modes, both hybrid cases, late keys and tombstones, and existing non-versioned exclusions. The histogram fixture requires an actual index contribution and checks that an older captured view remains unchanged.

The fixture extension uses the real Delta flush callback. After each late write, it captures a view, flushes the buffer, and compares captured and fresh counts. The buffer must be empty after the flush. Both histogram modes preserve the expected winner count, including deletion, while the original view remains unchanged. All six tests in the follow-up run passed in 16 seconds. These include all four histogram tests and two non-versioned buffer regressions. This covers completed handoffs, not a capture interleaved inside the handoff.

The next fixture extension pauses the real callback after the Delta commit but before buffer eviction. A bounded channel reports the commit, and a one-shot channel releases the callback after histogram capture. No mock, sleep, or production test hook controls the schedule. The capture must retain buffered rows while the committed Delta copy also exists. Both histogram modes must preserve the expected count after eviction, for a late insert and a tombstone. The earlier captured view must remain unchanged.

All four histogram tests passed in 7.51 seconds with this extension. The command was `cargo nextest run --lib -E 'test(database::histogram::tests)'`. This proves the specific committed-Delta/retained-memory overlap, not a shared source snapshot under arbitrary concurrent writes. Inserts during capture, multi-bucket capture, DML, and routing/provider interleavings remain open. The focused Rust review found no new suppression, unsafe code, or widened production API.

The existing `buffer_consistency_test` suite also contains two ignored mixed-store tests. Their comment assumes that buffered and committed data never overlap. Late writes contradict that assumption. Their current fixtures use recent timestamps, so enabling them alone does not reproduce the closed-bucket omission. The new regressions explicitly use historical timestamps and retain their recorded failing baseline.

### Proposed protocol, only where existing ownership is insufficient

`capture_histogram` already retains file metadata and Arrow batches across execution. `HistogramDmlState` rejects captures that overlap active SQL DML and invalidates captures when relevant DML begins. Its short mutex protects registration, not query execution. This is an existing implementation to evaluate before adding another visibility mechanism. It does not currently register ordinary inserts or flush handoffs, so it does not establish the proposed full source-view contract.

The proposed source token contains:

- The project/source identity and specification version.
- The date epoch plus sparse last-relevant-change epochs for repaired ranges.
- Pinned raw Delta snapshot versions and the corresponding logical file view.
- Immutable references to buffered and in-flight rows visible in that source view.
- Durable WAL positions represented by those rows, including shard positions and outstanding holes.

The epoch remains a mutation sequence. Snapshot references determine which rows execution reads. An aggregate publication persists covered ranges and source evidence, not process-local buffer pointers. Clean history shares compact baseline evidence with sparse exceptions. It needs no permanent counter for every retained minute.

The synchronization contract is:

1. Stage source data and complete required durability work before the visibility transition.
2. Under a short per-project/source visibility guard, update the epoch and affected-range evidence together with source visibility.
3. Under that same guard, let a query capture immutable raw, buffer, in-flight, and coverage references.
4. Release the guard before scans, Parquet decoding, or object-store I/O.
5. Execute raw and aggregate branches against captured references rather than resolving current tables again.

A flush handoff transfers ownership from buffered/in-flight rows to a pinned Delta snapshot without losing or duplicating its logical contribution. That handoff does not create a new logical mutation. Every direct DML and source write path must obey the same visibility contract. An external writer that bypasses this protocol requires conservative physical revalidation or cannot use logical-only routing.

A rollup build captures the same source view and relevant range epochs. It publishes artifacts and source evidence through the existing rollup transaction. Activation acquires the visibility guard and compares captured range epochs with current relevant changes. A concurrent relevant mutation leaves the affected range dirty, even if candidate files committed successfully. Unrelated range changes do not invalidate the candidate.

Queries already executing retain their captured source view. Later queries see the newer dirty state. If the source cannot provide immutable pinned references, this stage cannot claim snapshot-correct hybrid routing. A counter check before planning or an after-execution comparison is not a substitute. The feasibility experiment measures capture contention and retained buffer memory before production use.

At production batch rates and a 2x replay, require added insert p99 below 1 ms and throughput loss no greater than 5%. Measure per-source guard wait distributions in the experiment, not a cumulative timer that hides tail contention. Keep pinned-buffer retention within a fixed byte quota inside the existing memory budget, initially at most 5% of that budget. Admission must reserve that quota before execution.

Long queries cannot hold unlimited buffers or block flush eviction indefinitely. On quota exhaustion, decline the new route before execution and use the existing safe path, or apply bounded query admission. Do not drop pinned references or switch snapshots after emitting results. If these limits fail, retain existing physical gates and ship only the independent stages listed above.

Use existing hit/miss and flush-stall stats plus benchmark latency and memory traces; add no capture counters before this experiment needs them. Race tests cover concurrent inserts, late buffered writes, DML, flush handoff, publication, and interrupted recovery.

### Physical evidence remains a correctness backstop

Certified compaction or deduplication can carry logically equivalent coverage forward, as existing witness-carry paths already attempt. Known source mutations use their affected-range evidence. An unexplained file or deletion-vector change forces raw fallback or revalidation. Stage 2 adds the proposed counter `rollup_unexplained_source_change_total`.

It must not merely raise a metric while queries serve potentially stale aggregates. Fingerprints remain gates until all source visibility paths participate in the logical protocol. Later audits supplement that protocol, but a sampled audit alone cannot guarantee correctness.

## Historical ranges and migration

All intervals use UTC and half-open bounds `[start, end)`. The repair unit is initially an hour, with minute exceptions only after the granularity experiment succeeds. Storage partitions do not define refresh ranges.

| Operation | Raw input | Aggregate input |
| --- | --- | --- |
| Extend recent coverage from 14:00 to 15:00 | Missing eligible input in `[14:00, 15:00)` only | No previous hours |
| Complete an hourly tier | None if minute states are valid | That hour's minute states |
| Repair Tuesday 09:37 after Thursday coverage exists | Tuesday `[09:00, 10:00)` initially, or `[09:37, 09:38)` with minute tracking | Only affected parent states |
| Fill a historical hole | Missing or dirty intervals inside the explicit request | Existing valid states remain reusable |
| Restart or rewrite unchanged files | No raw scan solely for that operation | Durable metadata or equivalent active states |

A late correction never rewinds the recent cursor to Tuesday. Coverage intervals, dirty exceptions, recent progress, and finite historical requests remain distinct. The cursor is a scheduling aid, not proof that all earlier history is materialized. An optional daily tier can merge hourly states. It is not required or currently declared.

For requested ranges `R`, scheduling uses:

```text
valid   = materialized - dirty
missing = R - materialized
repair  = R intersect materialized intersect dirty
work    = (missing union repair) intersect eligible - leased
```

Task coalescing unions exact work ranges without filling clean gaps. An enclosing time predicate aids file pruning but cannot replace the exact work predicate. Shared row groups can cause physical read amplification. That does not justify republishing clean ranges.

Historical migration has three paths:

| Existing state | Action | Cost |
| --- | --- | --- |
| Valid compatible rollup with sufficient evidence | Adopt references and coverage metadata | Metadata reads and batched metadata writes |
| Rollup with uncertain evidence | Retain legacy checks or rebuild selected requested ranges | Metadata inspection, then optional projected raw scans |
| No rollup | Keep raw query fallback and backfill only selected ranges | No immediate conversion cost |

An activation boundary identifies source revisions from which new mutation tracking is complete. Late writes to older timestamps also enter that tracking. Legacy adoption reconciles relevant changes since its captured boundary. It cannot stamp old data with a current epoch without proof.

Existing 12-hour aggregate files can remain intact if their rows and evidence support finer logical selection. A replacement range requires the reader to exclude the old generation for exactly that range. Until that adapter exists, legacy readers and their conservative repair scope remain authoritative. An empty replacement must hide all old groups in the affected range.

No bulk raw-Parquet rewrite or compulsory historical backfill precedes new-data support. Backfill uses resumable bounded batches, skips valid coverage, and yields to recent work. Missing source history beyond retention is unavailable, not an endlessly retried repair. Inventory reusable, uncertain, and missing coverage before estimating migration cost. Task backlog bytes cannot substitute for this inventory.

## Stage 3: batch publication and reuse existing durability

This stage precedes flush-time aggregation. It extends existing staged publication, journal group commit, and recovery mechanisms rather than creating another metadata service. The six metadata rules remain:

| Rule | Implementation contract |
| --- | --- |
| Changes use the existing WAL/commit path | Each source batch carries affected ranges and stable mutation identity, without another per-row fsync |
| Coverage uses rollup publication | Artifact references, covered ranges, and source evidence share the existing rollup commit |
| Unchanged history uses intervals | Sparse exceptions represent relevant historical changes. Packed manifests retain detailed ownership |
| Bitmaps are bounded caches | Allocate only for active or dense dirty days. Cold history stays as intervals |
| Scheduling coalesces changes | One thousand changes to one bucket leave one newest pending revision, with no per-project timer |
| Checkpoints batch metadata | Recover a compact snapshot and a suffix of source/publication records rather than all retained history |

Two minute bitmaps consume 360 bytes per allocated day before other metadata. At 10,000 projects and 30 days, that is 108 MB per specification. A dense 64-bit minute revision array adds 3.46 GB. These are sizing examples, not a reason to allocate those structures. Hour masks and sparse ranges remain the default until minute tracking earns its cost.

Publication accumulates bounded bytes and actions, then commits several ready ranges per target table. Different Delta tables still require separate commits. Batching cannot make those transactions atomic together. Large metadata can use an immutable packed manifest referenced by the publication transaction. That artifact adds bytes and possibly an object request, which the benchmark must count.

The pending publication queue has global memory, staged-byte, and age limits. Flush acknowledgement does not wait for an additional rollup-table commit. If the queue is full, optional aggregate production defers and durable source evidence preserves missing coverage for later work. There is no unbounded queue of Arrow batches and no silent claim that deferred aggregates exist.

Current invalidation already calls `commit_journal()`, which group-commits task and rollup journals before write acknowledgement. Initially that cost remains. Eventual WAL integration removes redundant invalidation persistence, rather than adding another synchronous commit. Removal requires equivalent replay coverage for buffered writes, direct writes, DML, and existing recovery paths. Some source paths can retain journal persistence until they have equivalent durable evidence.

The resource gate forbids a separate coverage-journal commit and a synchronous per-flush rollup publication stream. It does not claim that new aggregate data needs zero writes or zero batched rollup commits. Total object requests, commits, and commit latency must improve or remain within the measured shared budget.

### WAL compatibility and rollout

`WalEntry` currently uses bincode, and `WAL_VERSION` is 1. Both the directory stamp check and the entry decoder reject unsupported versions. A constant bump alone prevents existing recovery and is not an acceptable migration.

If the evidence changes the WAL payload, the first release adds legacy and new-format decoders while it still writes version 1. The decoder maps legacy records to conservative invalidation where precise dependency evidence is unavailable. It must also support the directory stamp transition and replay of mixed retained record versions. The next release can enable new-format writes after compatibility and recovery fixtures pass.

Rollback targets must understand every retained record version. An older binary is not a valid rollback after new-format writes begin. No migration deletes accepted WAL data to accommodate a version bump.

Fixtures cover dirty restarts, mixed segments, old cursor snapshots, interrupted stamp transitions, and source holds that span the upgrade. The existing version rules require code changes before any new records are written.

### Publication, checkpoints, and recovery

Source and rollup transactions remain separate. Recorded source evidence establishes causality. If source commit succeeds before rollup publication, queries use raw fallback for uncovered ranges. If publication succeeds before cache activation, replay reconstructs coverage and reconciles subsequent relevant mutations. An empty generation explicitly supersedes all prior groups in its range.

The batching review found that staged-rollup recovery checked source row counts without checking the recorded content fingerprint. A real-Delta regression deleted one visible row through a deletion vector. File paths and physical row counts stayed unchanged, but recovery committed the stale aggregate. The new case failed while both existing rollup-resume cases passed.

The local candidate passes the existing preflight fingerprint into recovery and compares it with `PublicationEvidence.content_fp`. The comparison includes deletion-vector identity and adds no source scan or durable write. It also precedes reactivation of already-landed output. Legacy intents without fingerprint evidence decline conservatively. This is a recovery prerequisite, not the batched publication queue or a fix for mutations after source capture.

The broader resume run passed nine tests and failed four compaction fixtures at `admission_busy`, before their expected resume scenario. A serial rerun reproduced the same failures. Both runs passed the new rollup regression and the source-evidence classifier cases. The compaction failures remain unresolved, and no full-suite pass is claimed.

The focused command `cargo nextest run --lib -E 'test(a_killed_rollup) | test(a_rollup_resumes_only_when)'` passed all four cases in 2.26 seconds. Formatting and whitespace checks passed. The Rust review retained existing evidence types and added no suppression, unsafe code, or production API exposure. The earlier lint failure remains open.

Follow-up investigation resolved the four compaction failures. The diagnostic runner queued each unit with a 512 MiB estimate. Compaction requested eight CPU tokens before it inspected the selected files, even though the fixture contained only three tiny files. Serial execution did not change that request.

The local correction uses the existing selected-file byte calculation before admission. It preserves CPU pricing, memory limits, object-store tokens, and the rollup reservation. Empty selections finish without decode admission. The calculation adds no source scan, but metadata work now precedes admission. Existing worker and hygiene limits still apply. Preflight cost under backlog and genuinely oversized units remain resource gates.

The command `cargo nextest run --lib -E 'test(resum) | test(a_killed_rollup) | test(compaction)'` passed all 42 tests in 7.98 seconds. This includes the four previously failing fixtures and the stale-rollup recovery regression. These results do not establish production savings or completion of publication batching.

A second real-Delta regression found stale target acceptance during resume. An independent handle committed overlapping output while the database retained an older cached snapshot. Resume still reported success. The test permits a safe refusal or conflict and requires the final snapshot to contain only the external publication.

The local correction refreshes the target under its commit lock and declines changes since replacement validation. The resume transaction also disables automatic retries against newer snapshots. An external commit after the refresh must therefore fail the transaction instead of reusing an old replacement decision. Refusal retains staged objects and their intent.

All 43 selected recovery and compaction tests passed in 8.98 seconds, including this regression. The version check is deliberately conservative: unrelated target commits can also cause refusal. Finer-grained validation and its metadata cost remain batching work. Already-landed recovery races, the normal publication path, and source changes after capture still need separate checks. Formatting and whitespace checks passed, with no new suppression or production activation.

The extended case table reproduced stale acceptance in the already-landed branch too. An external writer replaced the cached output before recovery, but recovery still reported success without a new Delta transaction. That branch now refreshes under the commit lock and compares the target version before it restores bookkeeping. Refusal keeps the intent.

All 45 selected recovery and compaction tests passed in 9.68 seconds. A positive case also proves that unchanged landed output restores bookkeeping without advancing the Delta version. Both target branches now reject the tested pre-existing external replacement. External changes after the final refresh, source-view races, and normal publication still need separate checks. These tests do not establish the full atomic-visibility contract.

The normal-publication test required a later interleaving. An external commit before preflight left one correct output, so rejecting every successful return was an invalid assertion. The revised fixture holds the commit lock and waits for the real staged-intent record. It then commits competing output through an independent Delta handle before releasing the lock. This schedule reproduced two live output files where only one publication belonged.

The local correction compares the refreshed target partition with the partition used for replacement selection. An unchanged table version takes the fast path. Otherwise, the comparison includes paths, tags, partition values, statistics, deletion vectors, sizes, and modification times. Earlier changes to other project/date partitions do not invalidate this comparison. A changed partition follows the existing stale-publication cleanup and retry path.

The normal publication transaction also disables automatic rebasing after the final check. This still rejects an external commit in that final window, including an unrelated commit. The metadata and retry costs need measurement before batching. All 64 selected recovery, compaction, and rollup no-op tests passed in 284.02 seconds. Nextest reported 13 slow tests and one leaked-handle warning in the deletion-vector rebuild test. The warning remains under investigation and is not a measured memory leak.

The publication case table now includes a competing commit to another project's partition after staging. Both disjoint publications remain live, and the original project's aggregate stays at two rows. All eight focused publication and recovery cases passed in 3.40 seconds. This establishes partition isolation for the tested pre-refresh commit, not for a commit after the final refresh.

The deletion-vector rebuild test also passed alone in 78.05 seconds without the leaked-handle warning. That result makes the warning intermittent, not resolved. The focused Rust review added no mock, production test hook, lint suppression, or widened production API.

The publication case table also covers an external generation-tag change on the same file path. The fixture uses separate Remove and Add commits, then checks that the replacement is live before publication resumes. A same-version Remove and Add left no live replacement in the first fixture, so that failure did not establish a publication bug.

All five publication cases passed in 2.44 seconds. Removing only the tag comparison made the new case fail: the stale builder reported successful publication. This regression guard therefore checks metadata identity, not only path presence. After restoration, all 48 selected recovery and compaction tests passed in 11.94 seconds. Changes after the final refresh and atomic source visibility remain separate requirements.


Existing checkpoint machinery gains coverage intervals, sparse exceptions, pending-work discovery, and source/publication replay positions. One checkpoint batches many changes. Byte and record thresholds bound the suffix, with one shared time threshold for low traffic. Shard positions and unresolved holds prevent advancement past accepted input that a later completed write does not represent. Cold metadata can reside in indexed checkpoint pages under a bounded cache.

A durable completion marker identifies a usable checkpoint. Garbage collection retains required mutation evidence until that checkpoint represents its effects and existing source holds permit release. Interrupted checkpoints preserve the previous complete checkpoint and its replay suffix. Eviction of dirty cache entries also preserves their durable discovery path. Persistent checkpoint failure can grow the suffix, so recovery size and checkpoint age remain explicit operational limits.

Existing stats include `journal_commits`, `journal_commits_coalesced`, `rollup_journal_persists_total`, and `rollup_shared_commits_total`. Add only `rollup_publication_pending_bytes` and `rollup_recovery_suffix_bytes` when implementing the queue and recovery limits. The gate requires both to remain below their configured byte caps; reaching a cap must defer optional work without losing durable discovery. Measure metadata cache size and checkpoint timing in the experiment; do not add permanent keys without a gate that reads them. Commit-rate comparisons use matched traffic windows and include checkpoint and manifest writes.


### Local checkpoint-sharing candidate

Fresh publications, resumed publications, no-op completions, and invalidations now share task checkpoints through the existing `GroupCommit` primitive. Publication callers release journal locks before they wait. Invalidation callers still persist the dirty-range journal before acknowledgement. Publications do not serialize that separate map. This adds one in-memory barrier, not a new journal, worker, or replay format.

A controlled concurrency test queued three publication completions behind an active checkpoint. Two callers shared the next checkpoint leader. All intents remained until the durability barrier completed, and journal replay recovered all three publication records. The test failed before publication callers used the shared barrier. A real-Delta fixture also checks that fresh publication enters this barrier.

The failure test found that `TaskJournal::checkpoint()` discarded pending records before `fsync` succeeded. A real device accepted writes but rejected `fsync`. The next checkpoint reported success, but recovery lost a task and source cursor and restored a removed task. Pending records now remain until both the write and sync succeed. The regression covers task updates, source cursors, and removals.

A separate regression reproduced corruption after a partial write and retry without a restart. The journal now retains the failed append's starting offset in memory. Before retry, it truncates that append and preserves the acknowledged prefix. Complete writes with failed sync retain their framing and pending records. Healthy appends need no additional sync, and the file-length check also supplies the compaction decision. The durable format is unchanged.

The partial-write test uses a real file-size limit in an isolated child process, with no mock. Test-only `unsafe` calls set and restore the OS limit and signal handler. The test requires an actual unterminated append, then verifies both tasks and the source cursor after retry and recovery.

Restart tests also reproduced append corruption after an incomplete journal tail. Recovery ignored the tail, but the next checkpoint appended to it and broke the following restart. Recovery now validates every complete record before it truncates and syncs the unterminated suffix. Tests cover every byte boundary, including split UTF-8 and complete JSON without a newline, with and without a snapshot. Complete corrupt records remain untouched, and WAL read errors stop recovery instead of returning an empty journal. Healthy journals require no additional write or sync.

Tail repair requires exclusive journal ownership during recovery. The normal server entrypoint holds `WalDirLock` before database initialization and until shutdown. Direct library and bootstrap callers still need an ownership review, especially with different WAL directories that share one metadata directory. Concurrent journal writers remain outside these tests' coverage. Both repair paths report discarded byte counts without logging record contents.

All 185 selected coordinator, publication, journal, and group-commit tests passed in 8.00 seconds. New stats `task_journal_checkpoints` and `task_journal_checkpoints_coalesced` expose barrier attempts and coalesced callers. These are gauges, not physical `fsync` counts: an empty checkpoint can perform no I/O. Production CPU, journal bytes, and insert latency still determine acceptance under the shared experiment protocol.

The journal mutex still spans serialization and `fsync`. This can prevent other callers from applying mutations and reaching the shared barrier together. The controlled test proves sharing behavior, not its frequency under production contention. Removing that lock hold needs a separate durability design and failure tests.

Other direct checkpoint callers remain. Delta publications still commit individually, and the bounded publication queue is not implemented. Checkpoint sharing does not establish completion of Stage 3 or a whole-server saving. The latest `cargo lint` still fails on `result_large_err` at `src/rollup.rs:692`. No suppression was added.

## Stage 4: reuse buffered rows without endangering flush

The fast path computes aggregates from winning rows already available during flush preparation. It filters winning tombstones after version resolution. Raw storage keeps tombstones as required by its semantics. It publishes a first complete range only if no earlier contribution exists or exact ownership proves disjoint keys. Partial flushes, uncertain overlaps, and concurrent contributions require the normal repair path. Time alone cannot prove that no late rows will arrive.

This stage uses the bounded batched publication path from stage 3. It must not create a synchronous commit per flushed bucket per specification. Additional aggregation CPU and retained Arrow memory count against the same budget as flush and foreground queries. Snapshot ownership, queue-full behavior, and recovery tests precede production activation.

Stage 1C's shared preparation also applies here. Each additional specification must justify its own accumulator and publication cost. The fast path remains optional under pressure. Source durability must not depend on aggregate production.

Existing safety stats include `flush_stalled_total` and `rollup_rebuilds_full_total`. Benchmark output records aggregate CPU, reused rows, deferred candidates, source flush p95/p99, and MemBuffer pressure at matched input rates. No additional permanent stats are required for this first experiment. Any increase in stalled flushes or unbounded retained input blocks rollout.

## Stage 5: reuse deduplication work where it reduces total cost

The current DV-dedup pass reads identities and physical row positions, not all aggregate inputs. Fusion requires additional projected columns and aggregate state. Its benefit must exceed that additional decode and memory cost. It applies only to a complete logical ownership range, not an arbitrary subset of overlapping files.

Certified clean ranges already use stage 1A. This stage adds fusion only where unresolved input still requires deduplication. Compatible consumers reuse stage 1C's shared stream rather than create another raw scan.

Fusion is opportunistic. Rollup freshness must not depend exclusively on the DV lane, which can be starved. A bounded standalone repair remains available for overdue or queried ranges. Scheduling prevents duplicate simultaneous fused and standalone work for the same revision.

Repairs replace complete groups for their range. Counts, sums, min/max, and sketches use one publication contract. Min/max, HLL, and t-digest cannot generally retract an arbitrary old contribution. Floating sums and sketches require declared comparison tolerances.

Different digest merge orders need not produce identical percentiles. Stage 1D defines dependency-aware invalidation. Fusion cannot infer logical equivalence from physical deduplication alone.

Existing `rollup_oldest_invalidation_age_seconds` must not worsen because the DV lane lacks capacity. The benchmark records reused rows, fallback work, physical decoded bytes, and actual CPU for fused versus separate execution. Add no fusion-specific stats before the experiment establishes a benefit.

### Oversized ranges and single oversized minutes

Stage 1B owns the bounded-execution contract, including existing hash-shard amplification and oversized aggregate state. Fusion must reuse that contract rather than introduce another spill implementation. An oversized bucket cannot block other projects, lose its pending repair, or serve stale aggregates. Raw fallback remains subject to query resource limits.

## Stage 6: optional finer repairs and hierarchical state maintenance

Minute exceptions require stage 1's 30% saving on at least 20% of the relevant maintenance resource. The comparison uses the optimized hour path after stages 1A–1D, not an obsolete baseline with repeated scans. They extend existing range machinery and bounded caches rather than create a second durable ledger. Benchmark range counts and exception-memory cost against the hour baseline; reuse existing scan and rebuild stats for rollout.

### Physical layout and shared repair input

The source schemas already lead their sort order with timestamp. Another timestamp-sort proposal adds no new benefit. The experiment compares dirty intervals with actual file, row-group, and page spans. Candidate layouts retain packed files but improve time locality inside them during normal future writes or compaction.

The comparison counts footer size, compression, object requests, query CPU, repair CPU, and write amplification. No bulk historical rewrite precedes adoption. A targeted rewrite needs measured future savings greater than its one-time cost.

Repairs with the same physical footprint can share stage 1C's input mechanism, even with separate logical ranges. The reader can decode clean gaps without replacing their aggregate output or claiming new coverage. The logical repair set stays equal to explicit dirty ranges and holes. The experiment requires at least 30% less physical repair input for the affected cohort without increased total CPU or object-request pressure. Minute tracking remains deferred unless its additional savings pass the separate granularity gate.

Hourly dashboard and metric tiers merge active minute states. A changed minute affects only its containing hour. A stale hour can use valid minute states, then raw input only for remaining dirty ranges. The independent session tier retains its own dependencies and declared grain. Coarse query results do not require rebuilding historical source days.

Packed files hold many aggregate ranges. State compaction removes obsolete generations without raw scans or logical invalidation. The service-name HLL review belongs to the first deliverable, not this stage. Its removal or replacement requires a separate measured schema change.

### Materialize only work with demonstrated benefit

Each specification and costly measure gets a usage and cost review before broader deployment. The review includes maintenance CPU, state bytes, commit cost, eligible query frequency, and saved query work. Optional materialization is useful only where expected query savings exceed its maintenance and recovery cost over the chosen horizon. The experiment must include cold queries, bursts, and the cost of raw fallback after eviction or disabled materialization. Required product latency remains a constraint, even for an infrequent query.

Later candidates include expensive sketches and rarely queried per-project tiers. Session suspension and the unserved HLL review occur earlier. No change silently removes supported query semantics. Missing optional states use the existing raw path. New policy needs bounded discovery, activation hysteresis, and explicit coverage state, not per-project timers.

Measure or generation changes need compatibility handling. They must not trigger compulsory rebuilding of all retained history. Existing hit/miss, rebuild, output-file, and invalidation-age stats support this experiment.

### Freshness and execution tuning

The scheduler already coalesces invalidations through a quiet-period deadline and refuses unflushed base input. The experiment measures repeat builds per logical bucket before changing those mechanisms. A longer settling delay trades lower maintenance work for more raw-tail query work. The decision minimizes their combined CPU while preserving the product freshness limit and the first-dirty age bound. Recent work cannot starve under continuous writes. Historical work cannot consume its reserved capacity.

Batch sizing belongs to the early Stage 1 experiment. Increasing partitions or worker count does not establish reduced work on a saturated server.

## Shared resource and rollout gates

All maintenance shares one CPU, memory, object-request, and commit budget with foreground reservations. The initial experiment allows two heavy units, at most one per project/source, with one slot unavailable to historical backfill. These are starting values, not established optimal limits. Foreground queue delay, cgroup throttling, MemBuffer pressure, and stalled flushes constrain admission. Runtime timer lag alone cannot establish spare query capacity. Stages preserve the ingest-health protections from `dcef9932`.

Recent coverage, late repair, and historical backfill remain separate priorities under weighted fairness. No per-project timer or permanent per-minute task is required. A thousand writes to one bucket coalesce into one pending desired revision without resetting its first-dirty age. Historical requests consume spare capacity and cannot rewind recent progress.

Each experiment PR names its `timefusion_stats` counters, matching the requirement in `CLAUDE.md`. Only three permanent stats are proposed: unexplained source changes, publication pending bytes, and recovery suffix bytes. Add each with its stage, updating both hand-maintained stats lists and testing their exposure. Other measurements belong in benchmark reports or existing structured logs until an operational gate requires a permanent key. Counter deltas require stable process uptime and matched workload windows. Counts of successful rebuilds alone omit failed-attempt cost.

Fixed-resource replay compares 1x, 2x, and 4x traffic, separately for more rows and more projects/groups. These are experiments, not promised capacity multipliers. Required outcomes are stable backlog, lower total work per accepted row, correct query results, and protected source durability. Measurements include actual CPU, physical scan bytes, commits, object requests, metadata memory, recovery time, and foreground p50/p95/p99. Eligible aggregate queries can improve directly. Arbitrary log listings and unsupported filters benefit only from reduced contention.

### Capacity decision

Set whole-server savings targets after attribution, not from hypothetical workload shares. The earlier 60–70% objective is not an acceptance requirement. CPU, read bandwidth, memory, and commit capacity need separate budgets. The first exhausted resource sets the capacity limit. Do not add stage savings without a combined experiment. More projects and more rows require separate capacity results.

Growing backlog means utilization alone understates demand. Initial savings first reduce that deficit and fund backlog recovery. Admission limits that hide increasing debt fail the capacity gate. The final replay spans late writes, retries, and compaction cycles, then drains attributable work under the same budget. Success requires stable freshness, bounded backlog, preserved query SLOs, and reduced total work, including the drain period.

The target recurring cost follows new winning rows, genuinely corrected input revisited, and aggregate states merged. Frequent relevant historical changes still cost work. The design does not remove that fundamental limit. The priority table defines the first deliverables. The complete publication architecture is not a prerequisite for stopping unused work.

## Required correctness and migration checks

- Reader, census, and no-op decisions agree for full days, partial slices, dirty hours, and restarted coverage.
- Extending recent coverage by one hour does not scan unchanged previous days.
- A late correction preserves recent progress and valid unrelated coverage.
- Clean history survives certified compaction, while unexplained physical changes force fallback.
- Raw and aggregate branches use one captured source view across mutation and flush races.
- Mutation replay, split flushes, duplicate events, and publication retries do not double-count contributions.
- Deletes, timestamp moves, dimension changes, and empty generations replace all affected old groups.
- WAL upgrades recover legacy and new records without deleting accepted data.
- Publication queue saturation does not stall source acknowledgement or lose pending coverage work.
- Checkpoint interruption and dirty-cache eviction preserve recovery and pending-work discovery.
- Oversized buckets progress within bounded memory or report an explicit capacity limit without spinning.
- Historical adoption reuses only proven coverage and requires no bulk raw rewrite.
- Certified-clean aggregation matches winner resolution for deletes, ties, stale certificates, overlapping keys, and restart recovery.
- Single-pass partitioning preserves all versions of each key and bounds both spill and accumulated aggregate states.
- Shared scans produce independent correct results across grains, measures, partial ranges, and publication failures.
- Dependency classification never skips a relevant mutation, including stale replacements and timestamp moves.
- Physical scan grouping never expands logical repairs or revives superseded aggregate generations.
- Optional materialization and settling delays reduce combined query and maintenance work without violating product latency or freshness.
- Paused tiers remain paused across census, retries, and restart, without losing data or reactivating stale coverage.
- Byte-aware batches preserve memory admission, cancellation, and cooperative yields for both narrow and wide input.

Implementation remains gated on source snapshot feasibility, measured repair granularity, and publication economics. The existing correctness path remains available until each replacement passes its checks and resource experiment.

## Evidence appendix: implementation record

During histogram verification, HEAD advanced to `9327faa6` and another session changed maintenance admission. Those edits remain intact and are not attributed to this implementation. The design baseline remains `08d34190`. Final verification must cover the combined worktree after concurrent edits settle, not reuse earlier results for changed code.

During generation-range verification, the other session committed those existing admission edits as `2ae2b85c`. That commit changes the two scheduler files, not the generation-range candidate. The local candidate remains uncommitted. No deployment or whole-suite signoff is claimed by its 144-test result.

The full plan remains active. This implementation has not changed production behavior or deployed code. The first implementation adds the read-only inventory tool `scripts/rollup_work_inventory.py`, with tests in `scripts/test_rollup_work_inventory.py`.

The tool reads tracing JSON or ANSI-colored text from stdin. It groups successful publication observations by tier and shard count. New records use explicit `hash_shards`. Older records use the baseline formula. The report keeps recorded and estimated groups separate, even when their counts match. An invalid explicit count does not silently fall back to an estimate.

The tool preserves repeated observations, rejects missing estimates, and emits no project identifiers or payloads. It does not infer CPU share, unique tasks, query usage, or physical read amplification from byte estimates. Its three tests pass, including mixed-version records and malformed shard counts. The new evidence-label assertions failed before implementation and passed afterward.

### September 25 hour-spanning CPU profile sample

Authorized read-only sudo access resolved the profile-permission blocker. No profiler configuration, file permissions, or production service changed. The local evidence directory remains outside git.

The sample contains 57 consecutive SVGs, `cpu-000508.svg` through `cpu-000564.svg`. Their modification times span 06:35:00–07:35:53 UTC. Container `a45370c324c2` started September 24 at 21:28:57 UTC and still reported zero restarts at 07:37:40 UTC. The service digest remained `sha256:84504e8dc915bca8d8ff1e00102c1a64f246165b713d26033c55e7ad3989387b`. Its source revision remains unmapped.

The sample contains 2,562,127 recorded samples. The following categories count the union of matching sample intervals within each profile. Nested matching frames therefore count once per category.

| Inclusive category | Samples | Share |
| --- | ---: | ---: |
| Maintenance-named threads | 1,969,760 | 76.88% |
| Sort operators | 985,263 | 38.45% |
| Sort batch assembly | 826,785 | 32.27% |
| Aggregate operators | 762,559 | 29.76% |
| Visible rollup caller | 492,853 | 19.24% |
| Visible dedup caller | 305,149 | 11.91% |
| Parquet frames | 186,204 | 7.27% |
| Partition statistics | 45,019 | 1.76% |

These categories overlap and must not be added. Detached async execution can omit caller frames. Maintenance-named threads do not identify individual specifications. The percentages describe this sample, not CPU-seconds, achievable savings, or a steady-state baseline.

Sort batch assembly warrants direct measurement in the batch-size and single-pass experiments. Its inclusive share includes child work, not only assembly instructions. Partition statistics also require a separate metadata measurement. In profile 510, 3,247 of 3,277 partition-statistics samples sit under `add_actions_table`. This supports measuring repeated snapshot-to-Arrow materialization before introducing a cache. Any cache must preserve snapshot identity and deletion-vector evidence.

The local profiler code retains ten captures and specifies 60-second sampling windows. Production files disappeared between read-only inspections, consistent with bounded retention. The attribution workflow must export captures before pruning and record missing intervals. The export spans an hour, but capture modification times do not establish continuous sampling or exact sampling-window boundaries. Process-CPU reconciliation, per-specification attribution, ingestion and query counts, and acceptance windows remain outstanding. A point Docker sample near the window end reported 3272.49% CPU and 43.92 GiB memory against a 120 GiB limit. Neither value is an hourly rate.

The new `scripts/rollup_cpu_inventory.py` reports sample unions and artifact hashes without source rows or project identifiers. Its two tests cover nested matches, disjoint intervals, adjacency, overlap, and invalid root bounds. Both tests passed. The report preserves category overlap explicitly rather than presenting an unsupported additive CPU breakdown.

The overlapping log window was 06:35:00–07:36:00 UTC. The inventory consumed 150,380 lines and found 471 successful publications with complete estimate fields. It retained no raw logs. Estimated input totaled 192,923,245,541 bytes. The shard-weighted estimate totaled 512,486,032,952 bytes, or 2.656 times the input estimate. Multishard units accounted for 94.94% of estimated input. All shard counts remain inferred from the baseline formula, not recorded execution counts.

Sessions accounted for 236 publications and 158,733,169,837 estimated input bytes, or 82.28% of the total. Their shard-weighted estimate was 441,652,313,167 bytes, or 86.18% of the total. These counts strengthen the consumer-inventory priority without establishing that the session tier is unused. Failed and unfinished work remains outside this inventory. The first eleven profiles put partition statistics at 4.43% of samples. The hour-spanning sample lowers that figure to 1.76%, while sort batch assembly remains above 32%. This difference reinforces the need for representative windows.

### September 25 evening profile check

A read-only export captured six existing profiles, `cpu-001166.svg` through `cpu-001171.svg`, without changes to profiling or production configuration.
Their modification times span 18:21:54–18:27:23 UTC. The local evidence directory is `/tmp/timefusion-cpu-current.UnqhvA`, outside git.
The existing inventory script counted 277,520 samples: maintenance-named threads 91.98%, sort operators 46.14%, sort batch assembly 36.96%, and aggregates 20.81%.
Visible rollup callers accounted for 16.36%, dedup callers 7.36%, Parquet frames 8.48%, and partition statistics 1.91%.
These inclusive categories overlap. They do not identify per-specification CPU or establish achievable savings.
The short sample supports the priority of repeated sort and scan work. It does not replace the hour-spanning attribution or matched acceptance windows.

### Maintenance watcher guard

The existing source guard failed on five foreground test queries. It scanned test modules and used `expect(` as an exemption. The candidate scopes the same single-line detector to production code before the first test module. The `items_after_test_module` lint enforces the corresponding source layout. The candidate also removes the `expect(` exemption, so production calls cannot bypass the guard that way.

The original guard plus five scope cases passed in 0.402 seconds after the observed failure. The cases cover production calls, the former exemption, watched calls, test-only calls, and mixed production/test input. This remains a source-layout check, not a complete Rust parser or a runtime liveness proof. The guard-only edit followed the passing enum lint run. Final lint and CI signoff must cover the combined candidate before a commit or push.

### Approved generation-range merge

The in-place `Vec::dedup_by` merge replaces the large-error `coalesce` closure. It preserves identity boundaries and coverage holes without another allocation or lint suppression. The focused test covers nested and duplicate ranges and retained allocation. The test, `cargo lint`, formatting, and whitespace checks passed. These local results do not complete Stage 0 or authorize production activation.

### Approved output-evidence enum

The candidate replaces the coverage flag and file count with `Unknown`, `Empty`, and `Files(NonZeroU32)`. Zero files do not prove empty output. Missing or overflowing file counts produce `Unknown`, not a fabricated count. Empty publication recovery still requires source-content and measure evidence.

Date summaries can lack their own file count. The router still requires independent slice-output proof before using such a summary. Different nonzero file counts across slices do not prevent date recovery. Mixed empty and populated slices retain the existing conservative recovery rule. Journal, coverage-ledger, and WAL formats remain unchanged.

The selected run covered nine new state cases plus existing no-op, restart, hybrid, witness, and shard cases. It finished with 36 passes, one known shard failure, and two timeouts in 673.05 seconds. The shard failure remains the 512 KiB sort-merge/aggregate headroom limit. Both batch-size lifecycle cases reached the unchanged 600-second timeout.

Both cases also reached the unchanged 600-second timeout during the serial rerun. That run ended after 1200.21 seconds without a passing lifecycle case. A brief stack sample placed the adaptive case in physical-plan optimization during the fixture's dedup sweep. The locked DataFusion fork uses the same join-selection implementation as the registry version inspected. The sample does not establish an optimizer defect or explain the entire delay.

At 07:26 UTC, the laptop reported load averages of 80.89, 70.10, and 60.03. MinIO's health endpoint returned HTTP 200 in 2.5 milliseconds. These observations show local contention, not a completed regression diagnosis. Timing comparisons remain invalid under this load. Further heavy retries must wait for the current runs to finish and for a suitable local resource window.

Formatting and whitespace checks passed. `cargo lint` passed on the enum candidate in 8 minutes 31 seconds. The refactor is uncommitted and undeployed. Neither local success nor an unavailable result establishes the remaining Stage 0 gates.

A subsequent diagnostic added phase timestamps without changes to assertions, source rows, or the 600-second timeout. The first diagnostic enabled global logging and produced excessive output. We canceled that run after 76.21 seconds and restricted logging to the test module. That cancellation was not an assertion failure.

The adaptive case then passed in 197.78 seconds. The initial build completed at 99.13 seconds, and the unchanged-input assertions passed at 99.67 seconds. The late-write rebuild completed at 197.58 seconds. The final count assertion passed at 197.72 seconds. These phases establish progress through both rebuilds and the no-op path. They do not attribute time within each rebuild or explain the earlier timeouts.

The fixed-batch diagnostic also passed, in 208.61 seconds. Its initial build completed at 102.33 seconds, and the unchanged-input assertions passed at 102.95 seconds. The late-write rebuild completed at 208.41 seconds. The final count assertion passed at 208.57 seconds. Both lifecycle cases now have passing isolated results with unchanged assertions and timeouts.

Both storage volumes had free capacity: about 113 GiB on the host and 108 GiB in the local MinIO container. These checks exclude a full volume, not storage contention. Neither isolated timing nor the earlier loaded runs establish a performance comparison. The seven inventory-tool tests also passed. `cargo lint` passed on the test-guard and diagnostic changes in 4 minutes 14 seconds.

### September 25 hourly shard inventory

The read-only log window was September 25, 03:40–04:40 UTC, from container `a45370c324c2`. The inventory consumed only successful-publication records and retained no raw logs or project identifiers. All 538 observations contained the required estimates. Counts remain inferred from the baseline formula because the records contain no explicit shard count.

| Inferred shards | Publications | Estimated input bytes | Estimated input bytes × shards |
| --- | ---: | ---: | ---: |
| 1 | 316 | 21,589,788,937 | 21,589,788,937 |
| 2 | 71 | 55,998,383,362 | 111,996,766,724 |
| 3–4 | 149 | 214,378,108,351 | 725,994,651,326 |
| 5–8 | 2 | 5,548,555,120 | 33,291,330,720 |
| Total | 538 | 297,514,835,770 | 892,872,537,707 |

Multishard units represent 92.74% of estimated input. The weighted pass estimate is 3.001×. Session builds account for 291 observations and 89.52% of estimated input. They account for 91.16% of the shard-weighted estimate. These are exposure estimates, not CPU shares or promised savings. Failed and in-flight scans remain outside the sample.

At 04:41 UTC, the container still used digest `sha256:84504e8dc915bca8d8ff1e00102c1a64f246165b713d26033c55e7ad3989387b`. It started September 24 at 21:28:57 UTC and reported zero restarts. The image supplied no source-revision label. Its source mapping and the exact deployed shard formula therefore remain unverified.

One subsequent Docker sample reported 3724.65% CPU and 26.96 GiB container memory. Cumulative block I/O was 436 GB read and 30.9 TB written, not hourly rates. This point sample does not attribute CPU or establish steady-state capacity. Monoscope authentication succeeded, but facet discovery returned HTTP 500.

This evidence prioritizes an isolated shard experiment and the session-consumer inventory. It does not justify pausing a tier with unknown consumers or replacing the bounded path without resource measurements.

Local verification on the unchanged Rust candidate passed all 205 selected rollup tests in 264.97 seconds, with 13 slow cases. The command was `cargo nextest run --lib -E 'test(rollup)'`. This run reported no leaked-handle warning, but it does not resolve the earlier intermittent warning. All five inventory tests, whitespace checks, and plan-copy comparison also passed. The existing lint failure and production acceptance gates remain open.

### September 24 mature-process diagnostic

Follow-up at 22:12:58 UTC found a different container, `a45370c324c2`, with a 21:28:57 UTC start. Its service digest was `sha256:84504e8dc915bca8d8ff1e00102c1a64f246165b713d26033c55e7ad3989387b`. The source commit remains unmapped. Its 44-minute uptime fails the baseline gate. One Docker sample reported 3710.31% CPU and 34.06 GiB memory.

The latest 10,000 log lines contained seven successful publications with complete inventory fields. Their estimated input totaled 5,849,697,723 bytes. Two session publications had inferred shard counts of two and seven. Dashboard minute and metric minute publications each had two inferred shards. Three derived publications each had one inferred shard. These counts use the baseline formula, not recorded shard counts or measured reads. The bounded tail does not establish an observation duration, throughput, CPU share, or comparison with the earlier sample. Monoscope discovery still returned HTTP 500. All host operations were read-only, and raw logs were not retained.

At 21:10 UTC, container `8c185bcec85f` still ran from its 19:42:06 UTC start. Its service image digest was `sha256:02b1e440714c9488ecc94af417d3bbbddd0b7db93cf05089ee3b2254f3299a8f`. The source commit remains unmapped. One Docker sample reported 3827.14% CPU and 29.99 GiB memory. This satisfies only the uptime prerequisite, not replay completion or the attribution gate.

Monoscope discovery returned HTTP 500. Read-only host inspection confirmed that CPU profiling was enabled at startup. Docker's file API provided the existing `cpu-000080.svg` artifact (1,307,056 bytes, modification time 21:09:35 UTC). Its SHA-256 was `665dd1f9e0a46b664e12af28e5267ce9bce34ce5215a022834c44996e099bd6f`. The diagnostic parsed the profile without changing production or scanning source data.

The profile contained 45,001 samples. A `run_coordinator_rollup_selected` subtree contained 14,424 samples (32.05%). Maintenance-named threads contained 34,310 samples (76.24%). These inclusive counts overlap and must not be added. Detached DataFusion tasks prevent complete attribution from these frame names alone.

This single profile establishes visible rollup work, not its steady-state CPU share or an achievable saving. The raw artifact was not retained locally. The required hour, per-specification attribution, process-CPU reconciliation, workload matching, and repeated acceptance windows remain outstanding.

### September 24 initial evidence

Read-only inspection found a running image digest ending `587e699c6b063`, with container start time `2026-09-24T16:40:16Z`. This digest has not yet been mapped to the source baseline. The process was approximately ten minutes old, so it fails the one-hour uptime gate. A Docker CPU sample showed 3808.09% and 28.26 GiB container memory usage. This is a point observation, not lane attribution or a steady-state baseline.

A bounded container tail contained 9,992 lines and 138 successful publication observations. Every publication had the fields needed for the inventory. Their estimated input totaled 60,354,256,303 bytes. These are estimates from a bounded startup sample, not measured decoded bytes or a complete hour.

| Tier | Publications | Estimated input bytes | Inferred shard exposure |
| --- | --- | --- | --- |
| Dashboard 1m | 59 | 30,322,489,478 | 35 with one shard, 24 with two |
| Sessions 1h | 40 | 27,345,189,863 | 12 with one shard, 26 with two, 2 with six |
| Dashboard 1h | 33 | 233,239,989 | One shard |
| Metrics 1m | 5 | 2,334,267,634 | One shard |
| Metrics 1h | 1 | 119,069,339 | One shard |

Sessions account for 45.3% of estimated input in this sample, not 45.3% of CPU. This supports prioritizing the consumer inventory before shared-scan implementation. Multi-shard execution also has concrete exposure, but its physical amplification remains unmeasured.

Monoscope authentication succeeded, but service discovery returned HTTP 500. The bounded container-log query provided the inventory fallback. An earlier service-wide log request remained open and was deliberately canceled after the container query completed. No heavy source query, host modification, service restart, or pause policy change was performed.

Verification: `PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s scripts -p test_rollup_work_inventory.py` passed two tests. Cases cover formats, shard boundaries, repeated observations, and invalid evidence. Pending gates are stable one-hour attribution, per-tier consumer evidence, deployed-source identity, and all optimization experiments. Stages 0–6 remain incomplete. The next work is attribution and consumer inventory, followed by the gated early batch experiment.

### Early batch candidate and consumer-inventory learning

The local Rust candidate adds `timefusion_rollup_adaptive_batches`, default false. It passes byte-aware sizing into the private rollup session without changing partition count or admission limits. The default still uses 256 rows and skips the extra row-stat parsing. Successful publication logs now include actual `hash_shards`, `batch_rows`, and the optional input row estimate.

The candidate sums physical row counts from the same selected files as the unprorated projected-byte estimate. The existing `source_rows` is a raw-day witness, including for derived builds, so it is not a valid sizing denominator. Missing, invalid, or overflowing row counts select the 256-row fallback. The byte estimate remains approximate and needs memory validation before activation.

`cargo check --lib --locked` passed for the initial candidate. The first targeted test build exposed an unavailable `rstest` dependency. The fixture now uses this repository's existing async `test-case` pattern, without adding a dependency. The corrected run passed all nine targeted tests in 170 seconds. These cover sizing boundaries, default-off configuration, and both batch modes in the existing no-op/rebuild lifecycle test.

The lifecycle fixture now includes 8,193 additional source rows, crossing the fixed and maximum adaptive batch boundaries. It checks the final aggregate count after a no-op and a real rebuild. The expanded run passed all nine targeted tests in 183 seconds. Both batch modes returned the exact expected count of 8,195. The candidate also reuses `add_row_count` instead of adding a second statistics parser. Performance, cancellation, and wide-input resource gates remain open. Passing lifecycle tests does not establish CPU savings.

The local Monoscope source at `f6699ef3d` matches the application image tag seen during service inspection. Its current session SQL no longer matches the schema comment about `max(concat(...))`. `Models/Apis/LogQueries.hs` now uses an expression-based session key, ordered `ARRAY_AGG`, trace distinct counts, and `MAX(COALESCE(end_time, timestamp))`. These need a new routing fixture against the declared session measures. This source evidence does not establish query frequency or absence of other consumers. No session builds have been paused.

A later local inspection found Monoscope at `8f1aaa2fe5750bfcb863799c7aabee79ed1ee436`, with no local change to `src/Models/Apis/LogQueries.hs`. This commit has no verified deployment mapping. Its session key falls back from session ID to user ID and email, with empty strings excluded. The session tier stores neither fallback column as a dimension. Its stored user maxima cannot reconstruct groups for rows without a session ID. A client switch from ordered arrays to first-value aggregates alone therefore cannot make the current query eligible.

The routing fixture must cover the fallback key independently from the ordered context fields, trace sketch, and end-time expression. Existing session SQL tests establish aggregate execution, not routing into this tier. A full client-query fixture must also cover its nested aggregates and joins. These checks establish eligibility, not the absence of direct readers or infrequent consumers.

Six parameterized routing probes passed in 0.408 seconds after a 12-minute 39-second build under heavy laptop contention. The session-ID count selected `sessions_1h_v1`. Separate cases declined the fallback session key, end-time expression, ordered landing context, trace sketch, and user-name fallback. These component probes do not replace the full client-query fixture or consumer inventory. Formatting and whitespace checks passed. The preceding lint result predates these new probes.

The isolated worktree also contains `tests/fixtures/session_list_rollup.sql`, adapted from the same client revision with fixed synthetic parameters. It retains the nested summary, chart buckets, service join, and ordered context fields. No production query ran. Its new assertion requires successful planning and an aggregate node before it checks for no rollup routes.

The first seven-test run finished with six component passes and one full-fixture failure in 0.319 seconds. The bare planner rejected `'{}'::BIGINT[]` before routing. The test setup then prepended the existing `PgArrayLiteralRewriter` before the default analyzer rules, preserving the client SQL. Production already installs this rule before type coercion.

The rerun still failed on the same explicit cast in 0.322 seconds, while all six component cases passed. Thus the initial harness-only diagnosis was incomplete. The earlier rule handled bare literals inside `COALESCE`, but not explicit array casts. The full client fixture therefore exposed a compatibility gap before it established routing eligibility.

All three focused production-rule cases failed in 0.182 seconds. Empty and populated integer arrays produced cast errors. The empty text array silently returned `[{}]`, not `[]`. The candidate now parses explicit array casts with the existing literal parser and retains the declared target field, including nullability and metadata. Scalar casts and the parser's documented leniency remain unchanged. An initial compile error exposed the pinned API's `Cast.field` representation, which the corrected candidate now uses.

All 26 selected tests then passed in 0.404 seconds after a 2-minute 29-second build. The selection includes the array-rule group, six component routing probes, and the full client fixture. The three explicit-cast cases changed from failing to passing. The full session-list fixture plans successfully and receives no rollup route. This establishes local eligibility for the fixed query shape, not production frequency or absence of other consumers. Formatting and whitespace checks passed. The new compatibility fix still needs current lint and broader verification before release. No session policy changed, and no production activation occurred.

The broader optimizer and rollup-matcher selection passed all 300 tests in 1.979 seconds. That run used the lightweight fixture planner with the PostgreSQL array rule. The full-query test now uses the actual pgwire planner configuration and table providers through an isolated local database. This removes the hand-selected analyzer setup from that test. All 300 tests passed again in 1.860 seconds after a 2-minute 33-second build. `cargo lint` then passed on the unchanged candidate in 2 minutes 51 seconds. Neither planner test measures query execution latency. CI already includes the entire `tests` directory in the Rust-check inputs, including the new SQL fixture.

Static searches of Monoscope's `src`, `shared`, and `config` found no literal references to the session tier or `service_name_hll`. This check covers only those local directories at the inspected revision. Dynamic SQL, external clients, and production frequency remain outside its scope. It does not satisfy the suspension gate.

The broader run passed 16 of 17 cases in 11.00 seconds. Both ledger startup cases passed, including adjacent publications with one unchanged partition witness. All six merge cases passed. The ordered output index accepts contiguous publication intervals whose combined file count matches the ledger. It rejects overlapping intervals instead of counting the same range twice. This result establishes these correctness cases, not metadata cost under production load.

The legacy-generation case now passes the journal-only recovery assertion. It fails later because the derived task remains pending after a base rebuild. The next diagnostic records the remaining task states without bypassing dependency checks. Stage 0 remains incomplete.

The compatibility audit found that old ledger records cannot distinguish summed witnesses from valid partition witnesses. The candidate requires a versioned cache format that old and new readers reject when incompatible. Delta metadata can reconstruct the cache without a historical source rewrite. The existing on-disk lifecycle test now includes upgrade and rollback cases. The failing-test run precedes the format change. Temporary raw fallback during metadata recovery remains an upgrade cost.

Both compatibility cases failed before the format change: rollback accepted the corrected records, and upgrade accepted an unversioned summed witness. The ledger now stores each payload inside a versioned envelope. New readers reject unversioned records. Old readers reject the nested payload. Both paths use the existing unreadable-cache warning and Delta metadata recovery. No new journal or synchronous commit is added.

The post-fix run passed all 13 ledger and startup cases in 6.32 seconds. The command used `cargo nextest run --lib -E 'test(coverage_ledger_tests) | test(the_tag_replay_records_what_it_reads_into_the_coverage_ledger)' --no-fail-fast` with the shared local target directory. The cases include durable replacement, retention, persistence failure, witness merging, and routing before tag replay. Upgrade recovery duration and production metadata cost remain unmeasured.

The dependency diagnostic found a pending repair from 01:00 through 19:01 after the 24-hour base rebuild. Recovery created that task from untagged output. The active-parent guard correctly prevents derived admission until that repair receives current coverage proof. That run failed at its final dependency assertion. The follow-up uses the existing evidence-based worker reconciliation, as recorded next.

The inventory tool's three tests and `git diff --check` passed. Both plan copies match. `cargo lint` failed at `src/rollup.rs:692`: the generation-range coalescing closure returns a 176-byte error variant. The proposed in-place merge still awaits approval under the distillation review. No lint suppression was added. No whole-suite result, CI signoff, commit, or deployment is claimed.

The dependency follow-up passed all four targeted cases in 79.45 seconds. The lifecycle now runs the existing worker for the narrower recovery repair before requesting the derived build. A scan-counter assertion proves that this reconciliation reads no source data again. The derived result still equals four source contributions. The source-invalidation race guard and both dependency-proof cases also passed. This corrects the test's assumption about automatic task retirement, not production scheduling behavior.

The output-proof review found another untested shape: an untagged copy beside valid tagged output. The existing real-file helper creates this shape without a mock. The census/routing case table now includes it and requires a real repair to restore the exact source count. The new failing-test run is pending.

The untagged-copy case failed at the census repair assertion in 4.87 seconds. The other two output cases passed, including their new rebuild and exact-count assertions. The candidate now indexes unproven output intervals alongside occupied intervals. Missing identity tags or deletion vectors prevent those files from proving unchanged output. Overlapping unproven output prevents coverage acceptance. Unknown partition metadata prevents the output proof entirely.

The no-op and wider-slice checks now use the same output helper as routing and the census. This removes their independent tagged-file count check. Verification includes all three damage cases, legacy-generation recovery, ledger startup, and explicit-empty recovery. The added coverage-map traversal needs a cost measurement before activation. This change does not establish snapshot consistency or complete proof after partial-file loss.

All eight focused cases passed in 8.41 seconds with the shared output proof. The untagged-copy regression now queues repair, refuses routing, and restores the single source contribution after rebuilding. Missing output, obsolete output, both ledger startup cases, both explicit-empty cases, and the legacy-generation lifecycle also passed. The broader no-op regression group is running because its output check now uses the shared helper.

The implicit empty-prefix boundary remains unproven. The helper currently checks unproven output against the publication interval before it extends coverage toward midnight. A regression must include old untagged output inside that extension and a later valid slice. Full-day cases do not establish this boundary. Metadata cost, partial-file loss, and snapshot races remain separate gates.

The broader no-op run exposed a regression in the shared-output candidate. The deterministic invalidation case reached `Complete` instead of `Retry` after coverage disappeared during the output check. The fallback from failed proof to wider-slice repair lacked the completion guard. Its source epoch and task state must remain current under `rollup_journal_lock` before it completes the narrower task. Checkpoint I/O must occur after that lock is released. The regression assertion remains unchanged.

The partial-range case now retains old noon output, removes its source row, and builds valid evening coverage from a later source row. It requires a repair outside the evening interval and an exact hybrid count of one. Rejecting all coverage cannot satisfy this case. Its initial run is pending.

The initial partial-range run passed three cases and failed the new prefix case in 5.62 seconds. The candidate now extends coverage toward midnight only when that prefix contains no aggregate output. It retains the original valid slice when extension is unsafe. The test also requires repairs to exclude the valid evening interval and checks the hybrid result before rebuilding.

The broader no-op run finished with 13 passes and one invalidation-race failure in 361.99 seconds. The fallback candidate now checks task state and source epoch under the invalidation lock. It retries a changed proof and preserves a task already requeued by invalidation. It records a wider-slice escalation only when that transition occurs. Checkpoint I/O follows lock release. Focused race, output-damage, and wider-slice cases are running against these changes.

The corrected output-damage and invalidation-race run passed all five cases in 80.41 seconds. The prefix case preserves evening coverage, queues repair outside it, and returns the exact hybrid count before rebuilding. The original deterministic invalidation race now retries instead of completing. All four wider-slice cases also passed in 3.99 seconds, including stale-generation and bounded-witness decisions.

The race test now has a second case for an exact task requeued by invalidation. It requires the pending state, attempt count, and quiet-period deadline to survive settlement. This expanded test is compiling. The nine focused passes do not cover that new case or replace the broader no-op rerun. Lint approval, partial-file-loss recovery, metadata cost, and snapshot-visibility gates remain open.

The expanded invalidation test initially failed because its fixture had no normal task strictly inside the covering slice. The fixture now publishes a real one-hour slice before the race. Both invalidation cases passed in the next run. That seven-case run passed six cases and failed the new partial-output recovery case in 78.19 seconds.

The partial-output case splits a valid two-row publication into two real Parquet files. Recovery accepts that complete rewrite and returns both rows. The test then removes one file and clears the coverage caches. Recovery incorrectly accepts the survivor, and the census queues no repair. This is a confirmed correctness failure, not a performance result.

The candidate compares live output row counts with the matching journal publication. It uses the existing Delta metadata pass and adds no Parquet reads. Missing statistics, deletion vectors, or count overflow cannot prove the expected count. A contradiction removes matching cached coverage and queues an `IncompleteOutput` repair. Targeted output-damage and ledger-replay tests are running.

This check is incomplete when the journal no longer retains the publication. No-op completion can discard that record, and task retention is not artifact retention. Complete output proof must survive those paths without deriving its expected count from surviving files. Equal row counts also do not prove identical aggregate contents. Durable artifact evidence, certified rewrite transfer, and snapshot-safe publication remain open requirements.

The journal-count candidate passed all seven output-damage and ledger-replay cases in 6.86 seconds. The focused command selected `the_census_repairs_missing_output_despite_unchanged_source_evidence` and `the_tag_replay_records_what_it_reads_into_the_coverage_ledger`. A new case now runs the same real publication through a no-op rebuild before partial loss. It requires zero additional source scans and correct recovery afterward. That case is compiling. The earlier pass does not cover this lifetime gap.

The no-op lifetime case failed in 2.89 seconds: the census again accepted incomplete output after no-op completion discarded the journal publication. The next candidate writes `timefusion.output_rows` on every output Add in the existing publication commit. Recovery sums live file rows and compares them with the agreed tag value. A matching journal publication remains a fallback for legacy output. Without either expected count, recovery leaves the slice uncovered and queues an `UnprovenOutput` repair.

The candidate adds no journal, synchronous commit, or source scan. Its targeted verification is running. Existing rewrite code carries only six identity tags and drops the count and other proof tags. Preserving agreed proof tags across certified rewrites is the next required regression and implementation step. Row-count agreement alone still does not establish content identity or a consistent cross-table snapshot.

The output-count tag candidate passed all nine selected cases in 7.18 seconds. These cover six output-damage scenarios, both ledger-replay scenarios, and the legacy-generation lifecycle. The no-op case now recovers safely without another source scan before damage. The existing rewrite-tag test now includes publication proof and conflicting output counts. Its pre-fix run is compiling. The three inventory-tool tests also passed. No full-suite result or production saving is claimed.

The rewrite-tag regression passed the legacy case and failed the publication-proof case in 0.12 seconds. Only six tags survived instead of eleven. The candidate now requires agreement across all identity tags and preserves each additional proof tag only when every input agrees. Conflicting proof values are not carried.

The real split-file scenarios now use the production tag-transfer function instead of copying tags directly. Verification includes these scenarios, ledger replay, legacy generations, and the complete no-op regression group. That broader run is compiling. The change adds no source scan or publication commit. Immediate routing across changed file counts and metadata-validation cost remain separate gates.

The broader verification run started 26 tests after compilation completed. The real split-file scenario now also requires routing before metadata recovery and rejection immediately after partial loss. These new assertions compile in a separate focused run. The broader run covers the preceding test revision, not these added assertions.

The current output check still compares live file count with cached file count. Preserved proof tags alone do not remove this rewrite penalty. Any replacement rule must compare source evidence and the published output count, not only generation identity. Immediate rewrite reuse remains unverified.

The broader rewrite-proof run passed all 26 tests in 365.85 seconds, including the no-op and invalidation regressions. The separate pre-recovery run passed four cases and failed both split-file cases in 7.99 seconds. Both failures reported `not_built` after a valid physical rewrite.

The shared output candidate now compares source fingerprints and, when cached, the input content fingerprint. It also requires live row counts to match each recorded publication count. These checks permit a changed file count only when every contributing publication has complete count evidence. Legacy output retains the cached-file-count gate. The candidate reuses one metadata proof type in routing and recovery. Its focused verification is compiling. The 26-test pass predates this change and does not establish its correctness or resource cost.

The immediate-rewrite candidate passed all eleven focused tests in 9.80 seconds. A complete split-file rewrite routes before metadata recovery, and partial loss is rejected immediately. Both ledger-replay scenarios, both tag-transfer scenarios, and the legacy-generation lifecycle also passed.

The damage case table now includes changed source and input-content fingerprints with unchanged generation and file count. These cases require repair, raw fallback, and correct results after rebuilding. The expanded damage cases and the complete no-op group are compiling together. Metadata cost, snapshot races, and the remaining Stage 0 integration still need verification.

The expanded output-proof run started 28 tests after compilation completed. A separate recovery regression now extends the existing overtaken-slice scenario. One case retains the source rewrite and requires repair. The other appends beyond the covered prefix without rewriting the earlier source file. It requires recovery to preserve the bounded witness without new repair debt.

Code inspection found that `recover_date_coverage` still compares only whole-partition row counts. Routing and no-op decisions can use bounded witnesses. The new pre-fix recovery run is compiling. This is a suspected disagreement between coverage consumers, not yet a measured regression result.

A read-only production check at 2026-09-25 00:26:43 UTC found the same running container and service image as the earlier sample. The container started at 2026-09-24 21:28:57 UTC and reported zero restarts. It now meets the one-hour uptime prerequisite. Coverage-replay completion and matched workload conditions remain unverified.

One Docker sample reported 2490.96% CPU and 26.04 GiB of container memory against a 120 GiB limit. Reported block I/O was cumulative: 205 GB read and 13 TB written. These values are not an attributed CPU baseline or an I/O rate. A capped 10,000-line log sample contained ten successful derived publications with 84,421,855 estimated input bytes. The cap prevents any claim of complete hourly work or absence of base builds.

Monoscope authentication succeeded, but service discovery for the last hour returned HTTP 500 from `/api/v1/facets`. The SSH account could not read the mounted CPU-profile directory. No profiler was started, and no permissions or production state changed. Authorized read access or an exported profile is requested. Production attribution remains open while local implementation continues.

The expanded output-proof suite passed all 28 tests in 374.53 seconds. Nextest reported one process-leak warning for the restart scenario. That warning remains unresolved and is not a clean full-suite signoff.

The two-case recovery run passed the source-rewrite case and failed the late-append case in 154.34 seconds. The append changed pending base repairs from 12 to 19 during recovery. The source rewrite still required new proof, as intended.

The date-recovery candidate now uses `matches_slice` and `rows_below`, like the other bounded-witness consumers. Whole-partition agreement remains the fast path. Per-file metadata is read only when bounded evidence can resolve a disagreement. Both comparisons use one cloned Delta snapshot without holding the table lock through journal checkpoints. Metadata errors propagate through recovery. No raw scan or extra commit is added. The two recovery cases, output-damage cases, and ledger-replay cases are compiling together.

The bounded-recovery candidate passed eleven of twelve cases in 155.69 seconds. The late-append case passed. The previous negative case failed because it expected extra repairs after a dedup call. Inspection of the local fixture's Delta log found two append commits and no source rewrite. Distinct rows left the earlier bounded witness valid. Retaining that expectation would restore whole-day invalidation.

The negative case now writes inside the covered prefix. It requires a pending repair for that interval and refusal of stale rollup routing after recovery. The outside-prefix case requires no new repair debt and continued rollup routing. Both cases first require valid routing before mutation. Their corrected run is compiling.

The isolated restart check passed in 82.86 seconds without a leak warning. This does not identify the earlier cause. [Nextest's leak warning](https://nexte.st/docs/features/leaky-tests/) concerns output handles that remain open after process exit, not measured memory growth. No timeout or warning policy changed.

The corrected recovery cases are running. The output-damage case table now also checks the execution ticket. Each case first requires a fresh ticket to pass. After damage or supersession, the previously accepted ticket must fail its recheck. The pre-fix ticket run is compiling.

`rollup_ticket_current` currently checks cached identities but not the live output proof used by route selection. The new assertion targets that disagreement. It does not establish a pinned snapshot across raw Delta, rollup Delta, and MemBuffer. The ticket fix must preserve accepted interval boundaries and avoid a separate full metadata pass per interval.

The corrected recovery cases passed in 74.20 seconds. A write inside the covered prefix requires repair and refuses stale routing. An append outside that prefix preserves routing without new repair debt.

The ticket regression passed three cases and failed five in 6.74 seconds. Removed output, an obsolete generation, changed fingerprints, and an untagged copy still passed the execution recheck. Each case first required a fresh ticket to pass.

The ticket candidate retains the accepted output intervals, separated by project, date, generation, and empty or populated coverage. It clips these intervals to the actual rollup interior. The execution recheck now uses the shared live-output proof once per tier. It preserves the existing source checks and rejects metadata errors. No source-row scan, durable record, or maintenance commit is added.

The focused ticket run is compiling. This change adds metadata work during query planning. Its cost remains unmeasured, and the existing per-date source checks still repeat metadata work. The change does not pin raw Delta, rollup Delta, and MemBuffer to one snapshot. Those correctness and cost gates remain open.

The first ticket candidate passed all eight output-damage cases in 6.40 seconds. The same case table now also requires executable tickets after a complete physical rewrite and after repair. The evening-only hybrid case requires a ticket for its accepted interval, despite damaged output elsewhere on the date. These positive assertions prevent blanket rejection from satisfying the damage checks.

The expanded case table and both accepted-hybrid source-view cases passed all ten tests in 85.82 seconds. The review found no new suppression, unsafe block, or production panic in the ticket change. It retains existing source checks and uses the shared interval helpers. The earlier lint failure and broader snapshot and resource gates remain unresolved. No commit or production change occurred.

The same-pass certification regression now removes the real deletion vector after certification. It requires unchanged Parquet paths, two visible physical versions, and refusal of the old clean certificate. After correction of a path-ordering assertion, the regression failed at the intended check in 3.31 seconds. The old certificate still returned `Granted`. This establishes a local certificate-reuse defect, not evidence that this mutation occurred in production.

The same fixture has separate reuse and republication cases. Both failed at their intended assertions in 4.71 seconds. The publication case represents a mutation after the clean pass but before the final certificate record. It requires refusal even when Parquet paths remain unchanged. Stage 1A remains unavailable until the evidence survives producer, persistence, recovery, and consumer checks.

The publication candidate passes the checked `CountFiles` map to `record_certification`. This existing map includes paths and deletion-vector descriptors. The coordinator, legacy sweep, and batch probe capture it before their clean-pass checks. The masked coordinator pass carries its validated post-pass evidence into publication. The unmasked pass also rejects changed deletion vectors. No new durable record or commit is added.

The publication candidate passed fourteen of fifteen certification tests in 4.04 seconds. Republication now refuses changed visibility. The expected remaining failure was read-side reuse.

The next candidate replaces certificate and slice path lists with `CountFiles`. These existing records now retain exact deletion-vector descriptors, rather than a second parallel ledger. `physical_visibility_v2` rejects old path-only records and missing visibility fields. Historical Parquet data stays unchanged, but legacy certificates require fresh proof. The version also prevents an older reader from accepting the new records.

Whole-day, partial-window, per-file, and sweep checks now compare file visibility. Changed deletion vectors enter the same interval-overlap checks as newly added files. The candidate also checks visibility before excluding a date from certification work. Dirty-probe memoization uses the visibility fingerprint, so a same-path change cannot suppress a needed probe.

After the reference correction, all 38 selected certification tests passed in 9.41 seconds. These include both original regressions, format-version checks, existing restart cases, slice accumulation, and dirty-probe memoization. This establishes their local correctness results, not performance or the full source-view contract.

The expanded real-Delta fixture passed with all 38 selected tests in 8.94 seconds. It reopens persisted certification before the mask change and requires exact mask recovery plus a usable unchanged certificate. A distinct morning row retains usable coverage after the noon mask changes. The whole-day route and republication both refuse the changed visibility.

The historical-discovery run passed the recent-date case and failed three cases in 2.28 seconds. The 21-day and 120-day dates were absent. The ordering case also found no historical candidate. Each case requires discovery without a queued dirty bin, followed by certification through the existing worker.

The discovery candidate groups live partition metadata in one pass. It reads partition values and deletion-vector descriptors without Parquet statistics or source rows. Recent candidates precede historical candidates. Within each class, the existing busiest-project priority remains, with newer dates first. The 64-candidate limit, worker probe limits, concurrency, and shared deadline remain unchanged.

Visibility fingerprints reuse the existing calculation, including dirty-probe memoization. Missing partition identity and duplicate live paths produce diagnostics rather than usable evidence. All 21 selected discovery, deadline, memoization, and certification tests passed in 5.82 seconds. Metadata memory and runtime cost remain unmeasured. Recent work can still consume the available historical budget.

Legacy certificate rejection has a migration cost. The discovery candidate reaches retained sealed dates beyond the former fourteen-day limit. Certification still requires bounded key-only probes of source data. This is a certificate-refresh requirement, not a bulk raw-data format migration. Additional metadata work, metadata memory, and the wider snapshot contract remain unmeasured and unproven.

A visibility fix must preserve evidence from the checked source view through certificate publication. Reading current deletion vectors only after certification cannot close a mutation race. The producer, persisted certificate, restart reader, and each certificate consumer require the same evidence contract. The republication case exercises a changed snapshot but does not orchestrate concurrent producer execution.

Deadline review found that candidate discovery preceded the worker deadline check. All three real-Delta age cases failed the added expired-budget assertion in 1.34 seconds. The candidate now passes the maintenance deadline into discovery. It checks expiry before metadata access, during file iteration, between partitions, and before returning candidates. An interrupted metadata pass returns no candidates, because partial visibility cannot establish a complete partition.

The deadline candidate passed all 22 selected tests in 9.38 seconds. These include the three expired-budget regressions, discovery ordering, memoization, certification, and existing exhausted-worker-budget cases. Formatting and whitespace checks passed. These cooperative checks do not establish a hard memory cap or a strict wall-clock limit.

A partition comparison, sorting, and cleanup can exceed the remaining time. Table-lock acquisition also remains outside this synchronous helper. Peak metadata memory, cancellation during discovery, and lock-wait bounds remain open resource checks. The Rust review retained parameterized real-storage cases and added no lint suppression. Full lint, full-suite, performance, and CI gates remain incomplete.

The first Stage 1A SQL fixture used an undeclared column and failed during planning. After correction to `id`, the legacy-column case passed. The tombstone case failed at the intended assertion in 0.17 seconds: a bare select retained the deleted identity. The candidate adds tombstone filtering without `ROW_NUMBER()`. Its case table also covers null tombstones, absent tombstone columns, tenant boundaries, time boundaries, and shard predicates.

The real-storage scenario uses a clean probe to obtain evidence, then builds with the experiment disabled and enabled. It requires identical counts before and after a late version invalidates certification. All seven initial SQL, configuration-default, derived-input, and storage checks passed in 6.54 seconds. The subsequent extension covers deletion-vector execution and restart. Snapshot-interleaving, tied-version cases, and paired resource measurements remain separate Stage 1A gates.

Review strengthened the storage scenario with a later deletion of a live row. A temporary removal of the certificate gate produced the expected failure: certified mode returned 3 instead of 2. Ordinary mode passed, and the two-case run finished in 6.20 seconds. After restoration of the gate, all 162 selected tests passed in 7.87 seconds. This run includes the stronger storage guard, rollup SQL and routing tests, certification cases, and configuration defaults. Formatting and whitespace checks passed.

Certificate validation currently occurs during source preflight, before no-op and admission decisions. Its metadata work therefore affects attempts that never execute an aggregate. The resource comparison must include this cost, lock hold time, and certificate renewal. Aggregate-query CPU alone cannot establish a net saving.

The extended storage fixture masks a real source row through Delta deletion vectors before certification. It requires a live deletion-vector descriptor, persists the certificate, and reopens the database. The restored file visibility must match exactly, and the recovered certificate must remain usable without another probe. Both aggregation modes then exclude the masked row and winning tombstone. The existing resurrection and later-deletion assertions still apply.

All four selected tests passed in 10.22 seconds. These comprise both extended aggregation modes and both changed-deletion-vector certificate cases. The earlier 162-test run predates this fixture extension. This result establishes local deletion-vector execution and restart behavior, not the complete source-view or performance gates.

### Dependency compatibility update

The PostgreSQL upgrade candidate now preserves JSON output metadata and wire encoding, including binary JSONB and null values.
It also preserves the fork's canonical user-name normalization.
Both baseline regressions failed before these ports. All 120 selected library tests passed afterward.
The [dependency record](2026-09-25-dependency-upgrade.md) contains the command, timings, and remaining checks.
These component results do not establish full-stack compatibility or production resource savings.
The Delta sorted-compaction port compiled, and all 20 selected sort, deduplication, cap, timeout, and non-partitioned tests passed.
An additional real-table regression exposed incorrect suffix matching in selected-file rewrites.
The candidate now requires an exact stored path or table file URI. All 23 selected tests passed after this correction.
Incremental snapshot updates, additional file-selection and ordering checks, and full-stack resource checks remain open.
Four physical Parquet checks now pass for ascending and descending output on partitioned and unpartitioned tables.
They inspect values across two row groups and each footer declaration. All ten selected sort tests passed.
Footer declarations still come from caller-supplied writer properties. The sort API does not infer or validate them.
Nested leaf indices, null ordering, singleton admission, and declaration mismatch remain open.
The snapshot-layer port now preserves upstream cache identity checks and falls back on metadata changes or newer checkpoints.
All 113 selected snapshot tests passed, including append, removal, checkpoint-crossing, and metadata-change comparisons with full updates.
The commit option now reaches the post-commit hook and compaction commits. It remains disabled by default.
All 164 selected snapshot and transaction tests passed, including overwrite cases with the option enabled and disabled.
The stronger rerun also passed all 164 tests with serialized file-record comparisons, rather than path-only assertions.
This avoids omissions in the production equality implementation.
The real deletion-vector fixture now passes incremental append and compaction with the option enabled and disabled, including reload.
The sorted-output fixture also preserves its non-empty sort tag through incremental append and reload.
All 14 selected sort, tag, and deletion-vector tests passed. Non-empty row-tracking values still require a separate fixture.
Final Timefusion integration, concurrent-commit workload checks, and resource measurements remain open.
The broader Delta core, DataFusion integration, and Variant run passed all 1,439 executed tests. Nine tests remained skipped.
This result covers the local candidate, not all features or the final Timefusion dependency graph.
Deletion-vector write APIs for DELETE, UPDATE, and joined updates remain required ports.
The release gates remain open.

## Repository handoff

This plan is currently untracked. Before committing, review tenant names and production evidence against the repository's disclosure policy. Keep credentials, raw customer queries, payloads, and sensitive diagnostic captures out of git. Retain only approved summaries and reproducible references. Stage only the intended document. The unrelated `scripts/__pycache__/` directory is not part of this change.

Review the staged diff, including appendix identifiers and links, before committing. Keep restricted captures outside the repository. Record an approved evidence location when reviewers need access. This documentation revision does not authorize production changes or activation of local implementation candidates.

### Review disposition

The priority table and immediate queue put unused-tier review and byte-aware batches before coverage redesign. Attribution precedes activation. Stage 0 explicitly requires hybrid-race and empty-range recovery checks. Repeated counterbalanced windows replace single-window percentage decisions. Existing shard records establish exposure before controlled scan experiments. Proposed stats remain labeled, and later materialization work no longer defers the unserved-HLL review.

The historical session-schema comment is not sufficient evidence to disable current consumers. The implementation record identifies changed client SQL. Suspension therefore requires a current routing fixture and consumer inventory. No production pause, code activation, commit, or deployment forms part of this documentation update.
