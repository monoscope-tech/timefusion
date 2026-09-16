# TimeFusion: proposed work for the next days

Date: 2026-09-16. Status: **implementation in progress**. See [the implementation log](2026-09-16-implementation-log.md).

Source baseline at investigation start: `a2e69c59`.
During implementation review, local edits in `schemas/otel_logs_and_spans.yaml` and `src/rollup.rs` proposed landing-page and user-agent measures plus a sessions routing test.
They were removed from the deploy candidate after the audit found schema, legacy-cell, client-filter, and product-semantic blockers.
Items 25–29 must resolve those blockers as a paired client and engine design.

This plan combines the production investigation, all sections of the work-count plan, related research, and the open Monoscope issue inventory.
The objective is to choose work that improves customer queries and reduces repeated work on the existing single-node system.

**Recommendation:** start with query timeouts, a reliable maintenance baseline, and the real client queries that miss rollups.
Measure the effect of eager retraction before changing the wave planner.
Prepare the sessions query/spec change and its cost estimate together.
Keep CPU canaries and larger architecture changes behind measured need.

The list contains 45 items. It is a prioritized backlog, not a promise to finish 45 changes in five days.
Several items end with a measurement or a decision instead of a patch.
This document does not authorize a deployment, a historical backfill, or an infrastructure change.

## 1. Evidence and limits

### Production observations from September 16

The investigation sampled `timefusion_stats` around 19:00–19:04 UTC.
The process started at 18:02:15 UTC. Its age was approximately 58–62 minutes.
These observations describe that process. They do not establish an aged-process baseline or a causal improvement.

| Signal | Observation | Interpretation |
| --- | --- | --- |
| Eager retraction | 1,270,407 retracted / 1,350,793 appended versions, about 94% | The input-reduction mechanism is active. Physical write savings still need measurement. |
| Query latency | p50 44.7 ms, p95 451 ms, p99 980 ms, p99.9 2.52 s | Fleet counters do not describe each expensive query shape or include all failed-query costs. |
| Rollup routing | 2 full hits, 63 hybrid hits, 4,007 misses | About 1.6% of recorded outcomes were hits. This is not a workload-normalized regression measurement. |
| Dedup skip | 8 skips / 7,605 eligible scans | Today-heavy traffic can explain low skips. Sealed and current-day windows need separate measurements. |
| Stream failures | 43 → 45 during sampling | Failures continued despite an empty recent ERROR-level search. |
| Maintenance estimates | About 17 GB → 391 GB → 17 GB | These are estimated decoded task bytes, not physical database bytes. |
| Sealed debt estimates | 0 → 317 GB → 0 | The swing does not prove that the system physically compacted 317 GB. |
| Final queue | 607 pending, 83 retrying, 7 running | Zero sealed debt does not mean zero maintenance work. |
| Work progress | Flushes 272 → 291 and light-packing waves 445 → 485 across the sampled snapshots | Some lanes progressed. Dedup-wave commits stayed at 64. |
| Memory charge | Approximately 12% → 51% of the 110 GiB cgroup limit | The short interval needs attribution. It is not evidence of a leak by itself. |
| Initial journal wait | Mean commit wait 4.5 ms | The historical 362 ms journal wait is not the current observed baseline. |

The later implementation pass inspected the live service, host CPU, mounts, running digest, and the non-secret TimeFusion configuration. See [the implementation log](2026-09-16-implementation-log.md).
It also moved the reconstructible Foyer cache from the nearly full durable RAID1 volume to the roomy ephemeral RAID0 scratch volume after a drained handoff and readiness soak.

### Monoscope inventory and recent events

Project: `87576849-4941-49d3-a15d-680fef88a1a8`. Service: `timefusion`.
The inventory contains **356 unique open issues**, across four pages.
All titles were inspected through a first-pass classification. Selected issue bodies and recent events received deeper inspection.

| Title-based category | Issues |
| --- | ---: |
| SQL, schema, planning, and casts | 134 |
| Writes, updates, appends, and flushes | 79 |
| Object storage and Parquet | 46 |
| Maintenance | 21 |
| Memory, sorts, and spill | 18 |
| Explicit panic title | 1 |
| Other titles | 57 |

These categories use the first matching title keyword. They are neither validated root causes nor occurrence counts.
An issue's `updated_at` is not a reliable substitute for an event timestamp.
Several recently updated issues retain an August `first_seen`, `last_seen`, and occurrence count of one.

The six-hour ERROR search found three non-deployment failures around **14:21 UTC**.
Each involved an external sort and a **22 GiB query pool**.
The latest deployment produced write rejections around **18:02 UTC**.
The later 45-minute ERROR search returned no events, but WARN-level stream failures continued.

| Evidence reference | Finding | Follow-up |
| --- | --- | --- |
| Event `a0b8c294-d6c0-5012-b2e0-5883cc3ed137`, at `2026-09-16T19:01:22.584731Z` | `pgwire.stream_failed`, SQLSTATE `57014`, statement timeout | Items 01–04 |
| Event `2edd727b-627a-5651-9543-a5de2962fa98`, at `2026-09-16T14:21:53.442190Z` | External sorter requested 327.1 MB with 240.8 MB available | Items 03–04 |
| Issue `177dd125-4f63-4c73-987f-acfd33b8e7d7` | Aggregate allocation failed while several non-spillable hash joins held about 1.6 GB each | Items 03–04 |
| Issue `737aa6c4-0079-45b6-a73f-861a00e48a47` | Aggregate could not reserve memory during spill | Items 03–04 |
| Issue `b59da9bf-d3d7-4cfb-9c69-1b618113afad` | Query exceeded the configured 64 GB spill limit | Item 05 |
| Issue `c2789e65-e4bf-4d77-9ace-402be397979b` | Object-store GET failed while sending the request | Item 07 |
| Issue `601c3545-1ad7-4091-8564-ea4432cea53f` | Vacuum failed while reading an S3 list-response body | Item 08 |
| Issue `76eead0e-d6a5-48d2-9605-1d4911f7b0fe` | Append refused because the process was draining for deployment | Item 06 |
| Issue `68e7620e-56cd-4cb2-a6dd-c1a544b1fdf1` | Diagnostic SQL treated `"mor_%"` as a field | Item 10 |
| Issue `406d0573-f7e4-4b95-b7f8-cc38f989a47c` | Diagnostic query used unsupported `pg_size_pretty` | Item 10 |
| Issue `7f5d879c-cc70-49c5-bb9e-22d87c18228d` | Historical query task panic | Item 09 |

Event IDs can repeat across timestamps in search results. Use both the ID and timestamp for event retrieval.
No issue was acknowledged or archived during this investigation.

## 2. What the full work-count plan now means

Source: [Work count, not code speed](2026-09-15-work-count-not-code-speed.md), including every September 16 correction.

| Original section | Current interpretation | Work in this plan |
| --- | --- | --- |
| Initial 32x decomposition | Historical analysis counted same-path DV re-adds as full rewrites. Do not reuse that split. | 13 |
| L0: attribution | Maintenance lane tags exist. Full commit accounting and durable metric coverage still need an audit. | 13–14 |
| L1: geometric packing | Still a candidate. Lane evidence points to `wave_commit`, not a generic hot-tail selector change. | 15–18 |
| L2: 1:1 rewrite lane | Closed. The investigated lane was DV metadata, not whole-file physical churn. | Retain a counterexample in 13 |
| L3: metrics packing | Keep a separate metrics arm. The spans retraction benefit does not establish a metrics benefit. | 18 |
| L4: read work | Current code already accepts some `coalesce` forms. Diagnose actual client misses before extending the matcher. | 20–24 |
| L5: landed-skip | Source default is now `true`. An old request to enable it is stale without effective-config evidence. | 12 |
| L5: sessions | Query/spec mismatch remains in the checked-out client and schema. Cost and correctness require a paired design. | 25–29 |
| L5: maintenance offload | Conflicts with the standing single-process direction. No implementation commitment here. | 44 |
| L5: v4 canary | Dockerfile defaults to v3. Host support is documented, not freshly measured. Benefit remains unknown. | 43 |
| No-op UPDATE suppression | Implemented. Earlier production measurement found zero suppression because surviving updates changed values. | 15 and 30 |
| Eager retraction | Implemented by default and active in production. The observed retraction ratio is about 94–95%. | 15 |
| First wave-churn comparison | Explicitly invalidated by restart age and time-of-day confounds. | 11 and 15 |
| Revised 10x arithmetic | A scenario, not a capacity result. The estimated 50–70 cores still exceeds the 32-core cgroup. | 42 |

Three further qualifications matter:

- The proposed logarithmic rewrite count is a target model, not a proven lower bound for this workload.
- Reduced flush rows change the amplification denominator. Report absolute bytes and work per original ingest alongside work per flush row.
- Current-day packing can affect bounded rollup coverage even without a whole-day certification. A policy change still needs a proof-impact test.

## 3. Proposed first-five-days sequence

Dates are planning slots for September 17–21. They are not delivery commitments.
Owner labels name the affected area: **TF**, **Monoscope**, or **Ops**.
P0 means first investigation because current users can be affected. It does not declare a new outage.

| Slot | Main work | Deliverable before further behavior changes |
| --- | --- | --- |
| Day 1 | 01–03, 06, 09, 11–14 | Failure attribution, effective-build record, aged baseline, reliable work ledger |
| Day 2 | 04–05, 07–08, 20–23 | Reproduced expensive queries and a ranked query-shape miss report |
| Day 3 | 15–17, 24–28 | Matched retraction result, wave census, sessions design and cost proposal |
| Day 4 | 18–19, 29–34 | One evidence-backed implementation candidate and client contract coverage |
| Day 5 | 35–42, 45 | Staging results, revised capacity estimate, and next-week decisions |

Items 16–18 depend on the matched measurements. Item 29 depends on an accepted backfill budget.
Items 35–41 are investigations or conditional experiments, not a requirement to start several architecture projects.
Items 43–44 remain later decisions unless the preceding evidence changes their priority.

## 4. Ordered backlog

### A. Customer failures and operational evidence

#### 01. Attribute current statement timeouts — P0, TF + Monoscope

- **Evidence:** the inspected 19:01 WARN event reports SQLSTATE `57014`. The process recorded 45 stream failures.
- **Work:** connect each failure to a normalized query shape, endpoint, project, time range, deadline, and deployment.
- **Deliverable:** a ranked list of timeout shapes, with representative plans and bounded reproductions.
- **Done:** the dominant current failure classes have causes and owners. Distinguish server timeout, client cancellation, queue timeout, and resource failure.
- **Dependency:** none. Keep raw SQL and customer values out of metric labels.

#### 02. Make query failures visible regardless of log severity — P0, TF + Monoscope

- **Evidence:** ERROR-only searches missed current `pgwire.stream_failed` warnings. The OTel failure counter already exists.
- **Work:** inspect metric ingestion, dashboards, issue generation, and alert coverage for the existing counter.
- **Deliverable:** a proposed query-health view with failures, cancellations, queue waits, completions, and deployment markers.
- **Done:** a controlled statement timeout appears in query-health telemetry without a new duplicate counter.
- **Dependency:** 01. Alert publication is a later implementation step.

#### 03. Audit what heavy-query admission actually covers — P0, TF

- **Evidence:** `src/read/admission.rs` detects unbounded `SortExec`. Recent failures also identify large `HashJoinInput` and aggregate reservations.
- **Work:** inspect the failing physical plans for `AdmissionExec`. Measure peak memory, admitted concurrency, and queue waits.
- **Deliverable:** a coverage matrix for sorts, joins, aggregates, window queries, TopK, and rollup reads.
- **Done:** establish whether expensive plans bypass admission or exceed memory assumptions after admission.
- **Dependency:** 01. A join-only admission gap is a code-derived hypothesis, not a demonstrated production cause yet.

#### 04. Correct the dominant memory failure under mixed load — P1, TF

- **Evidence:** September 14–16 issues include non-spillable joins, sorter reservations, and aggregate spill failures.
- **Work:** reproduce the top failure with concurrent cheap queries. Select the smallest admission or execution change supported by 03.
- **Deliverable:** a focused patch, a failure reproduction, and a comparison against the current build.
- **Done:** the workload completes or receives bounded admission failure without OOM. Cheap-query latency and cancellation remain healthy.
- **Dependency:** 03. Increasing the global pool or partition count is not the default remedy.

#### 05. Diagnose the 64 GB spill-limit failure — P1, TF + Ops

- **Evidence:** issue `b59da9bf` explicitly reports the configured temporary-space limit, not filesystem exhaustion.
- **Work:** recover the query shape. Measure peak spill, concurrent spill users, cleanup, and the effective cap scope.
- **Deliverable:** a recommendation for less query work, different admission, or a justified limit change.
- **Done:** the shape has bounded disk use and leaves no spill files after cancellation.
- **Dependency:** 01 and 03. Keep query and maintenance disk budgets separate.

#### 06. Measure deployment handoff failures and retry behavior — P1, TF + Monoscope + Ops

- **Evidence:** the 18:02 deployment produced a burst of insert refusals. Similar issue records include merge-on-read append refusals.
- **Work:** measure the drain interval and replacement readiness. Trace retry outcomes through the client and proxy.
- **Deliverable:** handoff failure counts, recovery time, and evidence about eventual write outcomes.
- **Done:** accepted writes remain durable and transient refusals recover within the intended client budget.
- **Dependency:** none. The refusal message alone does not prove data loss or successful recovery.

#### 07. Separate transient object-store failures from missing data — P1, TF + Ops

- **Evidence:** issue `c2789e65` reports a request-send failure. Older titles mention missing objects and Parquet metadata failures.
- **Work:** classify transport, timeout, authorization, missing-object, and metadata-decode failures by recent recurrence.
- **Deliverable:** a recurrence table and one bounded reproduction per active class.
- **Done:** each active class has an appropriate retry or correctness response. Missing live objects receive immediate escalation.
- **Dependency:** 09. Historical issue titles alone do not justify a storage rewrite.

#### 08. Audit vacuum and log-cleanup recovery — P1, TF

- **Evidence:** issue `601c3545` reports a failed S3 list-response body. Issue `0bce73ff` concerns log cleanup.
- **Work:** inspect retry behavior, later success, Delta log growth, retention rules, and checkpoints.
- **Deliverable:** a recovery report with any persistent failure reproduced locally.
- **Done:** cleanup converges after transient failure and retains every object required by supported readers and recovery.
- **Dependency:** 07. Do not shorten retention to hide growth.

#### 09. Turn the 356-issue inventory into a recurrence ledger — P1, Monoscope + TF

- **Evidence:** many issue records are historical, duplicated by project, or recently updated without a recent occurrence timestamp.
- **Work:** group by validated cause. Attach last observed event, affected build, customer impact, and proposed disposition.
- **Deliverable:** current, historical, diagnostic, duplicate, and unresolved groups with representative issue IDs.
- **Done:** every group has an owner and a next action. Preserve unresolved panic and write-durability cases.
- **Dependency:** none. Issue archive/acknowledgment is separate from this document review.

#### 10. Remove diagnostic SQL noise from investigations — P2, TF + Ops

- **Evidence:** `"mor_%"`, `pg_size_pretty`, and table-function assumptions generated issues during diagnostics.
- **Work:** provide known-valid statistics queries and bounded event lookup examples in the operations guide.
- **Deliverable:** copyable commands that use the correct endpoint, literal quoting, and available schema.
- **Done:** those commands run without application errors and do not scan customer data unnecessarily.
- **Dependency:** 09. Unsupported PostgreSQL functions need product callers before becoming engine features.

### B. Establish the work baseline and reduce wave churn

#### 11. Capture an aged, comparable production baseline — P0, TF + Ops

- **Evidence:** the first wave comparison was invalid. This investigation also sampled a process younger than two hours.
- **Work:** record build, process age, workload mix, ingest, query outcomes, memory, CPU, disk rates, and maintenance progress.
- **Deliverable:** matched same-hour windows on processes older than two hours, plus deployment annotations.
- **Done:** rates use counter differences without crossing restarts. Comparisons separate current-day and sealed work.
- **Dependency:** none. Preserve a quiet measurement window unless incident response takes precedence.

#### 12. Record the running build and effective configuration — P1, TF + Ops

- **Evidence:** landed-skip defaults to true, eager retraction is default behavior, and heavy-query admission is unconditional in current source.
- **Work:** compare the running image digest and effective configuration with source defaults and deployment overrides.
- **Deliverable:** a sanitized configuration record, including landed-skip, pools, partitions, deadlines, and storage mounts.
- **Done:** each claimed deployed feature has runtime evidence. Zero landed-skips on a clean boot remains a valid dormant state.
- **Dependency:** 11. Use the existing dirty-boot tests before adding any new recovery test.

#### 13. Build a correct physical-write ledger — P1, TF

- **Evidence:** the original decomposition counted same-path deletion-vector re-adds as full physical rewrites.
- **Work:** join add/remove paths and lane metadata across a complete bounded Delta-log window.
- **Deliverable:** rows and physical bytes by lane, table, tenant, partition age, and operation.
- **Done:** distinguish new Parquet, same-path DV updates, rollup output, and flush output. Report missing attribution explicitly.
- **Dependency:** 11. Report both original-ingest and flushed-row denominators, including version appends and retractions.

#### 14. Audit restart-safe work metrics — P1, TF + Monoscope

- **Evidence:** lane tags exist. Counter histories and physical-byte semantics still need an end-to-end audit.
- **Work:** compare durable commits with exported metrics. Add only missing counters for committed output and queue transitions.
- **Deliverable:** a lane work dashboard definition and metric contract.
- **Done:** restarts do not look like work reduction. Metric totals reconcile with 13 within an explained tolerance.
- **Dependency:** 13. `progress_rows` is a liveness proxy, and worker duration includes waits. Neither is CPU usage.

#### 15. Measure the complete eager-retraction effect — P1, TF

- **Evidence:** retraction removes about 94–95% of appended versions. Earlier appends represented about 22% of reported ingest.
- **Work:** compare matched windows for retractions, original ingest, flush output, and lane-authored physical output.
- **Deliverable:** absolute savings and normalized savings with separate spans and metrics results.
- **Done:** distinguish the proven input effect from any downstream packing benefit. Explain retraction misses and late updates.
- **Dependency:** 11–14. Do not claim a 20% capacity gain from the retraction ratio alone.

#### 16. Census wave inputs and repeated file rewrites — P1, TF

- **Evidence:** the tagged sample attributed 174 of 217 commits and 19.3M rows to `wave_commit`.
- **Work:** reconstruct input/output chains, fan-in, size skew, file age, rows dropped, and repeated output reuse.
- **Deliverable:** ranked costly patterns per tenant-day, with separate dedup and hygiene reasons.
- **Done:** identify whether repeated large-file merges, required dedup, or fixed per-unit costs dominate after retraction.
- **Dependency:** 13 and 15. Keep this census read-only.

#### 17. Evaluate one wave-planner policy candidate — P1, TF

- **Evidence:** prior ratio/floor composition wedged a fixture. Current lane evidence does not justify changing `select_tail_bin` blindly.
- **Work:** replay the dominant pattern from 16. Compare current policy with one candidate in simulation and real-I/O staging.
- **Deliverable:** a decision with rewrite bytes, file count, freshness, latency, and progress results.
- **Done:** the candidate reduces physical work without starvation, proof loss, permit collapse, or growing maintenance debt.
- **Dependency:** 16. Treat the proposed ≤8 rewrite rounds as an experiment target, not a guarantee.

#### 18. Run a separate metrics packing arm — P1, TF

- **Evidence:** the historical metrics estimate was about 16x churn. Spans hash-update retraction does not establish equivalent savings here.
- **Work:** repeat the lane census and planner comparison for `otel_metrics`, including the self-monitoring project.
- **Deliverable:** a metrics-specific result with decoded width, rows, files, and physical bytes.
- **Done:** retain or reject the policy independently for metrics. Preserve progress for both tables under mixed load.
- **Dependency:** 16–17.

#### 19. Explain queue estimate oscillation and repeated task creation — P1, TF

- **Evidence:** decoded backlog estimates changed from 17 GB to 391 GB and back within minutes.
- **Work:** correlate task creation, supersession, pruning, reconciliation, and census passes with physical commits.
- **Deliverable:** a task-flow accounting report and a physical file census by tenant and date.
- **Done:** every large queue swing has an explanation. Separate real work completion from estimate replacement or task reclassification.
- **Dependency:** 11 and 13. Fix repeated task creation only after identifying a redundant transition.

### C. Make existing rollups answer real queries

#### 20. Refresh the latency and routing matrix — P1, TF + Monoscope

- **Evidence:** fleet rollup hit rates vary with workload and process age. The earlier 500 ms goal covers specific client shapes.
- **Work:** run actual top-tenant list, aggregate, RUM, service, container, and issue-chart queries across representative ranges.
- **Deliverable:** cold/warm, today/sealed, 1h/1d/7d/30d results with rows scanned, route, failures, and client RTT.
- **Done:** rank work by customer latency and resource cost. Record result equality against the raw path.
- **Dependency:** 11. Use a bounded ladder and stop at the agreed resource budget.

#### 21. Reproduce query-shape rollup misses precisely — P1, TF

- **Evidence:** current source includes `coalesced_column`, CASE normalization, and matching tests.
- **Work:** inspect actual status-code filters and groups after optimizer transformations. Compare parameterized and literal plans.
- **Deliverable:** the smallest failing real query, its miss reason, and its missing contract.
- **Done:** each high-volume unknown-filter or unsupported-shape class is either a correct refusal or a focused extension.
- **Dependency:** 20. Do not implement generic `coalesce` support that already exists.

#### 22. Audit adaptive dashboard bucket policy — P1, Monoscope + TF

- **Evidence:** hour-grain rollups cannot answer arbitrary one-minute buckets. The Infra tab adaptation is recorded as shipped.
- **Work:** inspect remaining widgets and cached plans for hardcoded bucket widths and unresolved parameters.
- **Deliverable:** a chart-by-chart bucket policy that preserves requested precision and uses eligible grains where appropriate.
- **Done:** wide-window charts route as intended. Narrow-window charts retain useful resolution and correct boundaries.
- **Dependency:** 20–21. Distinguish a product precision choice from a planner failure.

#### 23. Audit null guards, residual filters, and small interiors — P1, TF + Monoscope

- **Evidence:** current counters include multiple-null-guard, null-guard-mismatch, tiny-interior, and unaligned-bucket misses.
- **Work:** map each class to client query shapes and frequency. Separate regex/text-index queries from rollup candidates.
- **Deliverable:** ranked safe client simplifications or matcher extensions with raw-result comparisons.
- **Done:** eligible shapes route without changing null, filter, or boundary semantics. Correct misses remain correct.
- **Dependency:** 20–21.

#### 24. Audit sealed-window proof and coverage recovery — P1, TF

- **Evidence:** recent fixes preserve proofs, while live counters still include invalidated coverage and moved fingerprints.
- **Work:** compare controlled sealed-window and current-day queries. Measure recovered witnesses, rescue outcomes, and rebuild convergence.
- **Deliverable:** a regression assessment against the implemented #296–#300 mechanisms.
- **Done:** proven sealed windows route correctly across benign maintenance and restart. Genuine changes still invalidate coverage.
- **Dependency:** 20. Do not reopen the completed certification design solely because the fleet skip percentage is low.

### D. Pair the sessions query and its rollup

#### 25. Specify sessions semantics before changing storage — P1, TF + Monoscope

- **Evidence:** `otelSessionRows` still includes latest-page and user-agent expression aggregates absent from `sessions_1h_v1`.
- **Work:** specify page selection, timestamp ties, null handling, user-agent precedence, session filters, and late enrichment.
- **Deliverable:** a paired query/spec proposal with raw examples and expected answers.
- **Done:** behavior is explicit for sessions across services, hours, days, and requested range boundaries.
- **Dependency:** 20. Reuse current session/user columns before proposing new ingest columns.

#### 26. Choose a latest-page measure or bounded lookup — P1, TF + Monoscope

- **Evidence:** the current latest-page expression uses timestamp-plus-path MAX with a pageview filter.
- **Work:** compare a timestamp-aware argmax state with a lookup limited to the final page of sessions.
- **Deliverable:** correctness, storage, and latency comparison, including deterministic ties and nulls.
- **Done:** the selected approach preserves product semantics across tier merges and partial boundaries.
- **Dependency:** 25. Reuse existing first-value machinery where its semantics apply.

#### 27. Choose the user-agent representation — P1, TF + Monoscope

- **Evidence:** the query aggregates a fallback across attribute and resource user-agent values.
- **Work:** compare a declared measure, a normalized source value, and a bounded detail lookup.
- **Deliverable:** a paired client/spec choice with a cardinality estimate.
- **Done:** precedence and empty-string behavior match the raw query or an explicitly accepted product change.
- **Dependency:** 25. Extra state is a storage cost, not a free matcher extension.

#### 28. Price a bounded sessions backfill — P1, TF + Ops

- **Evidence:** the sessions tier is high-cardinality and the maintenance queue is not stable enough to assume free capacity.
- **Work:** estimate source bytes, output ratio, CPU, object operations, runtime, and interference for a small tenant/date sample.
- **Deliverable:** tenant/date scope, resource limits, pause conditions, resume procedure, and projected total cost.
- **Done:** the proposal identifies a safe first slice and a measurable stopping rule for review.
- **Dependency:** 19 and 25–27. This estimate does not start historical work.

#### 29. Implement and roll out the accepted sessions pair — P1, TF + Monoscope

- **Evidence:** a backfill alone cannot route the current query.
- **Work:** change the selected measures and client query together. Compare raw and rollup results before routing traffic.
- **Deliverable:** paired changes, a bounded backfill procedure, and a route/latency report.
- **Done:** selected sessions queries use the tier with correct page data and bounded maintenance debt.
- **Dependency:** 25–28 and accepted compute scope. Pause if failures, freshness, or queue age breach the agreed limits.

### E. Client correctness and compatibility

#### 30. Audit enrichment convergence and redundant client work — P1, TF + Monoscope

- **Evidence:** session/user columns are mutable and no-op version suppression exists. Earlier steady-state suppression was zero.
- **Work:** measure retry loops, successful changes, no-op statements, and circuit-breaker state for enrichment jobs.
- **Deliverable:** a list of active redundant work and obsolete workarounds, each tied to a reproduction.
- **Done:** enrichment converges without repeated full-row appends or permanent circuit-breaker suppression.
- **Dependency:** 12 and 15. Keep upstream guards that still reduce round trips.

#### 31. Extend the existing client contract suite — P1, TF + Monoscope

- **Evidence:** `tests/slt/monoscope_query_shapes.slt` already exists. The older roadmap proposes creating a suite from scratch.
- **Work:** add missing current failure shapes and critical routing assertions to the existing suite.
- **Deliverable:** fixtures sourced from real client queries, with explicit result and protocol expectations.
- **Done:** a relevant engine regression fails locally before release. Remove duplicate or obsolete fixtures.
- **Dependency:** 01 and 20. Include raw/rollup equality where routing changes.

#### 32. Rank active SQL and protocol gaps by real callers — P2, TF + Monoscope

- **Evidence:** 134 issue titles concern SQL/schema/planning. The September 2 roadmap lists many historical wire and dialect gaps.
- **Work:** reproduce current callers for parameter OIDs, arrays, casts, JSON access, aggregate syntax, and SQLSTATE mapping.
- **Deliverable:** implemented, still-broken, obsolete, and unsupported-by-design classifications.
- **Done:** select only current, consequential gaps for patches. Track client workarounds that each patch can remove.
- **Dependency:** 09 and 31. Do not treat the old feature list as the current engine inventory.

#### 33. Recheck exact counts and billing invariants — P1, TF + Monoscope

- **Evidence:** older plans describe count discrepancies and billing workarounds. This investigation did not establish a current wrong-answer defect.
- **Work:** compare acknowledged rows with raw, deduplicated, and routed counts across flush, update, DV, and restart boundaries.
- **Deliverable:** a current correctness verdict and retained regression cases.
- **Done:** exact count contracts hold. A reproduced mismatch becomes P0 and blocks dependent optimizations.
- **Dependency:** 31. Approximate sketches are not an oracle for exact counts or billing.

#### 34. Reassess long-range hash and issue-chart queries — P2, TF + Monoscope

- **Evidence:** the histogram research contains shipped work and several corrected diagnoses. Sparse completion counters alone do not prove inactivity.
- **Work:** compare indexed, raw-fallback, and cache paths on current builds under mutable tags and incomplete coverage.
- **Deliverable:** latency and correctness results across 1h, 1d, 7d, and 30d windows.
- **Done:** late tag updates, DVs, compaction, and partial boundaries return correct results without unrestricted scans.
- **Dependency:** 20 and 31. Price any new membership materialization separately.

### F. Conditional architecture and capacity work

#### 35. Reprice incremental rollup maintenance — P2, TF

- **Evidence:** earlier research measured a 191:1 raw-to-partial ratio on a closed-hour sample. Per-file ratios were not established by that sample.
- **Work:** inspect the current partial-rollup implementation and measure remaining raw reads, cardinality, and CPU by lane.
- **Deliverable:** a current cost model and a list of missing integration steps, if any.
- **Done:** either justify the next incremental-rollup step or defer it behind larger measured costs.
- **Dependency:** 13 and 20. Old `progress_rows` and worker-time estimates do not establish CPU demand.

#### 36. Prove partial-rollup correctness before serving it — P2, TF

- **Evidence:** mergeable states cannot generally subtract a superseded contribution from min/max or sketches.
- **Work:** design identity, replay, correction, watermark, and recovery behavior for duplicates and late updates.
- **Deliverable:** a shadow comparison against raw deduplication, using existing implementation where available.
- **Done:** no double counting across retry, crash, DV, compaction, or update. Inexact partials never silently serve exact queries.
- **Dependency:** 35. Favor a narrow experiment over a new rollup architecture without a cost case.

#### 37. Audit stalls and interruptibility by maintenance phase — P2, TF

- **Evidence:** historical small bins stalled for hours. Existing timeouts cannot preempt code that never yields.
- **Work:** inspect current phase timers and deadline coverage for reads, sorting, commits, and index publication.
- **Deliverable:** current slow-unit attribution and fault-injection results.
- **Done:** a stalled unit releases scarce capacity within its bound and preserves recoverable staged output.
- **Dependency:** 19. Split oversized units only when size, rather than a stalled phase, explains the delay.

#### 38. Evaluate client-latency feedback for maintenance — P2, TF

- **Evidence:** historical research proposes latency-aware scheduling. Current benefit remains unmeasured after the disk and CPU improvements.
- **Work:** correlate client tails with maintenance phases. Model a bounded controller with hysteresis and minimum maintenance progress.
- **Deliverable:** a staging comparison against existing flush-debt yielding.
- **Done:** lower client tail latency without an unbounded maintenance queue or permanent rollup starvation.
- **Dependency:** 11, 19, and 37. Do not add a feedback controller without demonstrated interference.

#### 39. Audit Tantivy coverage and index fanout — P2, TF

- **Evidence:** later profiles reduced Tantivy's CPU share substantially. Carry-forward already preserves indexes across compaction.
- **Work:** measure index count, fanout, uncovered inflow, reuse, and fallback cost on the current build.
- **Deliverable:** a ranked cost report for backfill, flush builds, histogram preparation, and reads.
- **Done:** pursue coarser granularity or bounded RAM builds only if current overhead justifies them.
- **Dependency:** 13 and 34. Do not reinstate the refuted claim that every compacted file requires full reindexing.

#### 40. Audit storage isolation, spill health, and endurance — P1, Ops

- **Evidence:** the live audit found the durable RAID1 volume at 95% use. The 600 GiB Foyer cache occupied about 644 GB there, while the 3.5 TiB ephemeral RAID0 scratch volume was 2% used. The active WAL is under the data mount; a separate `/app/data/wal` mount is empty.
- **Work:** the Foyer cache has been moved to a nested RAID0 bind mount and its old files reclaimed after a clean soak. Continue by measuring cache refill, I/O wait, swap activity, drive-write rates, and the remaining durable-volume growth. Resolve the unused WAL mount without moving durable WAL onto ephemeral storage.
- **Deliverable:** a current storage budget and endurance estimate under representative load.
- **Done:** WAL/journals remain on durable storage. Reconstructible cache and temporary work stay on ephemeral storage with adequate space and bounded cleanup. Configuration mounts match the paths the process actually uses.
- **Dependency:** 12. Do not rerun the historical RAID migration or disable useful write capture.

#### 41. Audit restart and journal overhead after existing fixes — P2, TF

- **Evidence:** group commit, delayed rollup-journal persistence, and retired-task pruning exist. Repeated restarts still disturb measurements and task discovery.
- **Work:** measure recovered coverage, census duplication, journal size, lock duty, and time to steady work after restart.
- **Deliverable:** a current restart-cost profile and a patch only for residual measured waste.
- **Done:** knowledge survives restart where intended, task history stays bounded, and durability barriers remain correct.
- **Dependency:** 12 and 19. Do not rebuild the existing pruning mechanism.

#### 42. Rerun the 1x/3x/10x capacity model and staging ladder — P1, TF + Ops

- **Evidence:** older simulation lacked real I/O and rewrite-permit constraints. The current work model changed after DV and retraction.
- **Work:** calibrate costs from 13–19 and run staged mixed read/write loads with explicit resource ceilings.
- **Deliverable:** sustainable throughput, backlog slope, rollup freshness, latency, failure rate, and bottleneck by load level.
- **Done:** every capacity claim states what the model includes and what real-I/O staging establishes.
- **Dependency:** 04, 15, 19, and 20. Do not extrapolate a microbenchmark into a 10x guarantee.

#### 43. Prepare an isolated x86-64-v4 experiment — P3, TF + Ops

- **Evidence:** the image defaults to v3. Documented host capability is not a benchmark or a scheduling guarantee.
- **Work:** obtain a compatible isolated x86 environment. Compare identical workloads and inspect every possible deployment host's CPU features.
- **Deliverable:** measured throughput/CPU/latency benefit, image provenance, placement constraints, and rollback procedure.
- **Done:** a canary decision rests on measured benefit and compatible placement.
- **Dependency:** 42 or a new profile showing SIMD cost. This task does not make the next production deployment a v4 canary.

#### 44. Revisit offload only after the single-node budget is explicit — P3, architecture

- **Evidence:** the older plan proposes maintenance offload but assumes shared coordination that does not currently follow from a standalone worker command.
- **Work:** price remote compute, network bytes, durable task ownership, OCC conflicts, failure recovery, and tenant fairness.
- **Deliverable:** a decision document comparing the measured single-node limit with offload costs.
- **Done:** an explicit architecture decision either retains the single-process constraint or accepts a scoped exception.
- **Dependency:** 42. The old approximate 20x threshold is not an established capacity boundary.

#### 45. Replace stale task status with an evidence-linked decision log — P1, TF

- **Evidence:** several plans retain their original recommendations before later sections reverse them.
- **Work:** link this review from the plan index. Record completed, rejected, conditional, and unmeasured items with dates and evidence.
- **Deliverable:** one current entry point and a short decision record after each experiment.
- **Done:** future work starts from the corrected state without repeating DV, cache, RAID, or certification investigations unnecessarily.
- **Dependency:** review of this document. Preserve historical measurements with their original time windows.

## 5. Research conclusions to retain

These are design inputs, not promises that another system's measured gains transfer to TimeFusion.
Primary-source pages were revisited on September 16 unless a limitation is stated.

| Research | Useful principle | TimeFusion constraint |
| --- | --- | --- |
| [ClickHouse part merges](https://clickhouse.com/docs/merges) | Merge lineage and part levels make repeated work observable. | Reconstruct lane-specific input/output chains before changing selection. |
| [RocksDB universal compaction](https://github.com/facebook/rocksdb/wiki/Universal-Compaction) | Size-based selection trades write amplification against other costs. | Existing floor/ratio composition previously wedged. Simulation and real reads decide the trade. |
| [ClickHouse aggregate states](https://clickhouse.com/docs/reference/data-types/aggregatefunction) | Store intermediate aggregate states and merge them later. | Mergeability alone does not solve replay, updates, or cross-file duplicate correction. |
| [Druid rollup](https://druid.apache.org/docs/latest/ingestion/rollup/) | Perfect and best-effort rollup have different guarantees. | Exact customer queries must not silently consume best-effort states. |
| [SILK, USENIX ATC 2019](https://www.usenix.org/conference/atc19/presentation/balmau) | Prioritize foreground work and permit maintenance preemption. | Bound starvation and debt. Cross-cgroup I/O controls cannot separate work inside one process. |

The earlier Timescale continuous-aggregate comparison remains a historical design reference in the repository research.
Its current documentation could not be retrieved through the browser during this review.
No recommendation here depends on a newly verified Timescale feature claim.

## 6. Existing work to preserve, not repeat

| Mechanism or conclusion | Current disposition |
| --- | --- |
| Same-path DV re-adds | Exclude them from full-file physical churn. Do not create an L2 rewrite project. |
| CPU-per-byte campaign | Preserve its measured gains. New micro-optimization needs a new profile. |
| Eager retraction | Default behavior, active in production. Measure downstream effect. |
| No-op UPDATE suppression | Implemented. Zero suppression can be the correct workload result. |
| Landed-skip | Default true in source. Inspect overrides instead of assuming an enablement task remains. |
| Heavy-sort admission | Implemented and default. Inspect coverage and sizing rather than adding another global gate. |
| `coalesce` rollup matching | Some forms already work. Use real failing expressions to define extensions. |
| Certification and witness fixes | Preserve the implemented design. Reproduce a current regression before reopening it. |
| Completed-task pruning | Implemented, with persistence tests. Measure residual journal cost. |
| Write-capture admission | The removal hypothesis was rejected and its gate deleted. |
| Storage split and boot fix | Recorded as executed. Inspect current health, not migration feasibility. |
| Warm cache and metadata | Existing mechanisms cover much of the old cold-start proposal. Reprofile remaining gaps. |
| Lying-footer rewrite campaign | Keep census and source checks. Do not manufacture a historical rewrite backlog. |

## 7. Review decisions

The proposed first tranche is **01–03, 06, 09, and 11–14**, followed by the highest-impact query fix that those measurements identify.
The wave-policy candidate waits for 15–16. Sessions implementation waits for the paired design and cost proposal.

Review these choices:

1. Prioritize current query failures and measurement before another packing-policy change.
2. Reserve an aged-process window for comparable measurements, subject to incident response.
3. Prepare the sessions pair now, but decide the historical backfill scope after the cost estimate.
4. Keep v4 and offload as conditional decisions rather than scheduled production changes.
5. Treat the remaining items as a ranked backlog, with daily selection from new evidence.

## 8. Validation and change discipline

For each implementation, record its target metric, raw-result oracle, and acceptable resource/latency bounds before the experiment.
Use simulation for policy progress and real-I/O staging for memory, spill, permits, and object-store behavior.
Require both for wave-planner changes.

Before pushing code, run the relevant checks from `ci/checks.tsv` through `make ci-signoff CHECKS="..."`.
Record commands, results, and outstanding GitHub checks in the PR description.
Publish attestations only for passing checks. Use standard GitHub-hosted runners.

The initial review changed documentation only. The linked implementation log now records the first telemetry patch, production measurements, and the Foyer cache relocation. No historical backfill or issue-status change has been started.

## 9. Repository sources

- [Work-count plan, including corrections](2026-09-15-work-count-not-code-speed.md)
- [CPU-per-byte campaign](2026-09-15-cpu-per-byte-campaign.md)
- [Five levers and execution log](2026-09-15-five-levers-to-500ms.md)
- [Merge-policy prior art and the composition wedge](2026-09-06-merge-policy-prior-art.md)
- [10x simulation and its limits](2026-09-06-ten-x-readiness.md)
- [Write-path capacity research](2026-09-12-surviving-10x-on-the-write-path.md)
- [Cache-admission investigation, disk split, and final corrections](2026-09-12-cache-admission-plan.md)
- [Group commit and production findings](2026-09-12-group-commit-in-production.md)
- [Rollup-maintenance research and corrected CPU premise](2026-09-13-rollup-maintenance-prior-art.md)
- [Append-and-merge design and measured cardinality](2026-09-13-append-merge-rollups-and-the-drain.md)
- [Witness and restart analysis](2026-09-13-stop-re-rolling-deduped-partitions.md)
- [Client compatibility roadmap](2026-09-02-monoscope-compatibility-roadmap.md)
- [Hash and issue-chart requirements](2026-09-08-hashes-and-long-range-issue-charts.md)
- [Indexed histogram execution history](2026-09-08-indexed-hash-histogram-plan.md)
- [Heavy-query admission](../../src/read/admission.rs)
- [Rollup matcher](../../src/rollup.rs)
- [Maintenance queue statistics and pruning](../../src/maintenance_coordinator.rs)
- [Configuration defaults](../../src/config.rs)
- [Sessions rollup schema](../../schemas/otel_logs_and_spans.yaml)
- [Existing client contract suite](../../tests/slt/monoscope_query_shapes.slt)

Client source inspected: `../monoscope/src/Pages/RealUserMonitoring.hs`, especially `otelSessionRows`.
At the initial inspection, the source contained both expression aggregates discussed in the sessions items.
Reinspect the client alongside the concurrent sessions edits before implementation.
