# TimeFusion alert, issue, and plan review

Reviewed: 2026-10-19. Owner: TimeFusion maintainers.
Repository input: `ad1ee10bfc7c76217924c9aa465a5c5d4f30392a`, initially clean.
Current work: [October 19 backlog](../plans/2026-10-19-next-work.md).

## Scope and evidence

The authenticated Monoscope project is `87576849-4941-49d3-a15d-680fef88a1a8`.
The issue inventory covers all 32 pages for service `timefusion`: 3,129 records, 3,129 unique IDs, final `has_more=false`.
The `srv-captain--timefusion` and `srv-timefusion-pgwire-proxy` issue lists each contain zero records.
All 16 saved monitors were inspected, including queries, thresholds, status, and evaluation timestamps.
Details were inspected for the 20 non-archived issues whose list `updated_at` is October 9 or later.
This is a complete issue inventory, not an individual root-cause investigation of all historical records.

A fixed telemetry window covers October 18 09:43:59Z through October 19 09:43:59Z.
The aggregate contains 820 error/fatal events. A separate aggregate counts 819 `No field named service` events.
The remaining error search returns one parser event and `has_more=false`.
The issue detail places the service-field failure's latest event at October 19 09:41:36Z.
Its list `updated_at` remains 00:00:14Z and its `occurrence_count` is 1.
Neither field reliably measures current recurrence.

The broader seven-day error search returns a sample of 100 events with `has_more=true`.
Its counts are not a seven-day total. It confirms JSON failures, S3 PUT failure, catalog probes, and deployment draining.
The incident list also caps its response at 100 despite a larger requested limit.
Incident episodes and the 38,209 project-wide log patterns were sampled, not exhaustively enumerated.
No status changes, public share links, notifications, or production mutations occurred.

The read-only host check reports the current task running for about three days.
Its immutable image is `sha256:0c92dc14b4379ff69df5eefe28333cd71f9b9878bb0370050f331bb0192900af`.
Its source fingerprint is `4e46b2c9b2c7279bee64e8f93708844ace5546e6bdc291d748db9ac2afb21dd3`.
This fingerprint was not mapped to a git revision during this review. Local HEAD is not proof of the deployed tree.
Root disk reports 54% usage, with 776 GiB available in the human-readable output.
This replaces the October 7 full-disk snapshot as current evidence. It does not prove the original retention cause resolved.

Raw command output resides in `/tmp/timefusion-review-2026-10-19/` for this session.
The tables and sanitized issue inventory preserve the findings in the repository.
Raw payloads, recipient addresses, and credentials are excluded.

## Monitors

All 16 report `normal`. None is deactivated or muted in the returned configuration.
Evaluation timestamps range from 09:27 to 09:41 UTC on October 19.
The values come from different monitor windows and cannot form one synchronized resource snapshot.

| Monitor | ID | Value | Alert threshold | Last evaluation (UTC) |
| --- | --- | ---: | ---: | --- |
| TF page: compaction has stopped committing | `eb9f4819-02ae-434b-9500-a3a03acb14d3` | 100 | 3600 | 09:41:07 |
| TF warn: MemBuffer pressure > 80% | `745d60c4-b89b-4c2b-9e3e-d22f9407bd64` | 9 | 80 | 09:39:05 |
| TF page: deleted-but-open file bytes > 10 GB (handle leak) | `45b11212-6396-4993-8ff7-e97b4e07972c` | 0 | 1e+10 | 09:39:05 |
| TF page: sealed compaction lane has stopped committing | `4f67bf38-7fa8-43c6-8b76-77868000db9a` | 78 | 7200 | 09:39:05 |
| TF page: flush sort fell back to writing an UNSORTED file | `fb7d109f-2197-490f-9613-112e06e29494` | 0 | 1 | 09:39:05 |
| TF warn: WAL disk usage > 60GB | `46b57972-3650-4a1b-9b7b-7de826a9cd2c` | 5.13802e+10 | 6.44245e+10 | 09:29:06 |
| TF page: MemBuffer oldest bucket age > 2x flush interval | `c7830a38-9bec-4192-85b0-314b2f645fd9` | 664 | 1200 | 09:39:05 |
| TF warn: stalled or failed detached flush commits | `134d6f69-d6e3-4cee-960a-b16bc70bd50a` | 0 | 0.5 | 09:39:05 |
| TF page: packer refused files the planner queued | `d8981aa4-c2a5-49f2-95de-996c7c53a25e` | 0 | 25 | 09:41:08 |
| TF warn: parquet files without a tantivy index | `5f0cf1a5-e50a-4727-bfe2-4831eb204bf3` | 3 | 50 | 09:32:07 |
| TF warn: maintenance work more than one day overdue | `34255787-b01b-4482-9544-8c9678e37417` | 67634 | 86400 | 09:38:03 |
| TF warn: queries exhausted their memory pool | `0d29895f-1fe2-4edc-b7e6-e48ee301dc4c` | 0 | 0.5 | 09:37:09 |
| TF warn: sealed compaction debt above 250 GB | `4dfbcc1d-3379-4c87-9d3c-dc2d5330df73` | 3.63488e+09 | 2.5e+11 | 09:27:05 |
| TF page: WAL corruption / replay failures | `d47e3766-b689-41c3-b65d-4d4f57b46e9c` | 0 | 1 | 09:37:09 |
| TF warn: flush failures | `70b9c7dd-add4-4a1d-9ff5-d07feb8cbe7f` | 0 | 1 | 09:41:09 |
| TF warn: compaction units claiming cells and doing nothing | `cf544717-cad2-4656-a350-f489364da99a` | 497 | 5000 | 09:27:05 |

The sealed-index monitor reports three uncovered files but deliberately excludes today.
The October 15 body-search record reports approximately 270 uncovered today files.
These measurements cover different dates and populations. Neither contradicts the other.
The no-work compaction monitor uses a reset-aware increase query and a threshold of 5,000.
Its value of 497 is not the old lifetime-counter alarm.
No saved monitor in this inventory covers all schema/planning query failures.
A normal resource dashboard therefore misses the dominant current error family.
The incident record also names a prepared filesystem-headroom monitor. It does not appear among these 16 saved monitors.
N7/N8 retain that coverage check rather than assuming activation occurred.

## Issues

| State | Count |
| --- | ---: |
| Open | 520 |
| Acknowledged | 176 |
| Archived | 2,433 |
| Total | 3,129 |

There are 3,043 critical and 86 info records. Severity is not a priority ranking.
The [outstanding inventory](2026-10-19-timefusion-outstanding-issues.tsv) contains all 696 non-archived IDs and titles.
It preserves the original issue state and list timestamps. It contains no event payloads.

The following grouping uses title keywords. It provides routing buckets, not confirmed root causes.
For example, a maintenance checkpoint failure belongs to maintenance even when disk exhaustion caused it.

| Title family | Open | Acknowledged | Archived | Total |
| --- | ---: | ---: | ---: | ---: |
| JSON/Variant/types | 16 | 0 | 0 | 16 |
| flush/commit/ingest | 178 | 104 | 8 | 290 |
| maintenance/optimization | 47 | 1 | 2372 | 2420 |
| memory/spill/admission | 18 | 63 | 0 | 81 |
| object-store/vacuum | 38 | 0 | 45 | 83 |
| other | 37 | 2 | 2 | 41 |
| schema/catalog/SQL planning | 185 | 6 | 6 | 197 |
| timeouts/connections | 1 | 0 | 0 | 1 |

### Current follow-ups from issue details

| Evidence | What it establishes | Disposition |
| --- | --- | --- |
| `308ca681`, latest 10-19 09:41Z | Missing `service` dominates the fixed 24-hour error window | N1: identify caller, correct contract, add failure visibility |
| `d71d1b87`, latest 10-18 01:43Z | Oversized JSON number fails conversion and the error repeats the input body | N3: explicit invalid-input behavior and bounded diagnostics |
| `d7b246dc`, 10-17 00:02Z | Delta PUT to version 936775 fails after 30 seconds | N2: establish commit outcome and topic recovery |
| `a76a6c2a`, latest 10-17 00:01Z | Generic `commit failed, aborting` | Correlate with N2. Message alone does not prove the same incident |
| `d12a2046`, `fcf01c14`, `6345e84a`, `76eead0e`, latest 10-15 13:50Z | Insert/UPDATE errors explicitly say deployment draining | N2: client retry and handoff acceptance, separate from storage failures |
| `580bf179`, `406d0573`, 10-17 | Missing `pg_user` and `pg_size_pretty` | N6: identify actual clients before adding compatibility |
| `2073adfd`, latest 10-13 23:18Z | Variant projection names disagree around `coalesce` | N6: exact PGWire query fixture |
| `ad8ba018`, latest 10-13 15:19Z | A metrics query names missing `attributes___db___namespace` | N6: query/schema ownership |
| `a71c104e`, 10-13 | Boolean-to-text comparison fails coercion | N6: recover emitted predicate before choosing owner |
| `100e80ce`, 10-11 | Ascending fetch rejects a DV selection | Existing replacement `5b2a3dec` follows the incident. Check recurrence before archive |
| `703a1f68`, 10-11 | Vacuum delete request fails at object storage | N2/N7: establish subsequent recovery |
| `25d519cc`, `0fd7f817`, `782d54c8`, `0444b0fa` | Duplicate expressions or invalid SQL/parser input | N6/N8: identify caller, separate diagnostics, preserve real regressions |
| `c2789e65`, latest 10-09 | Parquet GET reports a transport failure | N2: distinguish transient transport from missing data |

No issue was acknowledged or archived during this review.
A stale active incident is not proof of current failure. A short quiet window is not proof of remediation.

## Work already implemented

These items are complete at the stated evidence level. Historical timings retain their original workload and date.
This review ran no Rust tests and repeated no live performance matrix.

| Work | Evidence | What remains |
| --- | --- | --- |
| Resource-safe flush recovery and heavy-query admission | Mainline `75455c49`; write snapshots, publication recovery, weighted admission, and regression coverage exist | Production durable-outcome and resource acceptance, N2 |
| WAL deleted-handle and idle-writer fixes | `d5feb134`, `4fee4fd7`, `a17afcd1`, `9168baa3`; current deleted-open monitor reads zero | Current WAL trend and retention-pin attribution, N2/N7 |
| Metric-first layout and ordered dedup | Schema is `(metric_name, timestamp, series_id)`; October 5 record reports migration success | Full historical footer census and maintenance-budget acceptance remain distinguishable from the measured migration window |
| Web-vitals scan reduction | October 5 record: 44.7M to 368K decoded rows and 459 MB to 2.3 MB scanned | Reproduce only if the current query regresses |
| Persistent disk-cache recovery and block caching | `f91b6dfa`, `d35eebf5`; October record reports repeated query 20 s to 1.4 s | Cold in-process state, N5 |
| Rollup deadline and today-slice requeue | `b352c5df`, `6c091c89`, `63dd0510` | Current queue/proof acceptance, N7 |
| Routes and endpoint/hash rollup consumers | `f4686ed3`, schemas `routes_30m`, `hashes_30m`, `endpoints_1m`; October results report routed queries | Multi-project matrix, N6 |
| Sessions v2 | Schema `sessions_1h_v2`; September 30 record reports a routed sealed-day query | Current caller and exact-result coverage, N6 |
| Latest-N and mutable-hash narrowing | `622346ac`, `b35f7acd`; candidates include indexed/raw/MemBuffer timestamps | Current cold-run behavior, N5 |
| Bounded mutable-candidate planning | `651f7c4c` prices uncovered bytes; `ad1ee10b` remembers over-cap membership | The old “next: cap bytes” recommendation is complete in code |
| Body JSON text-search routing | `9707b2ae`, `f0537c13`; original predicate remains | Today's uncovered raw leg, N4 |
| Planning and routing cost reductions | `6c446306`, `ef36c8ed`, `04fb9f28`, `f66a86ba` | Refresh current workload measurements, N5/N6 |
| Deployment app/token preflight | `scripts/deploy/preflight.sh`, `run.py`, and regression fixtures; September implementation record confirms rollout | Wider app/service/image/mount assertions remain a separate ops review |
| Work-count attribution and eager retraction | Maintenance lane tags and default retraction exist; September 16 record reports 95.2% retraction | Matched physical-work economics, N7 |
| Fixed-server rollup cost gate | September 30 matched-hour record: 21,079 to 3,409 CPU-s/M rows | Historical result, not a current capacity guarantee |

The October 7 document's opening “not deployed” statement is stale as a current source-status claim.
Its implementation entered mainline in `75455c49`. The October 9 record identifies that image as its deployed baseline.
The running October 19 image still requires exact revision mapping. No current deployment acceptance is inferred from commit ancestry.

## What was merged

The [current backlog](../plans/2026-10-19-next-work.md) replaces independent scheduling in these documents:

| Source | Retained work | Closed or historical content |
| --- | --- | --- |
| September 16 next-days plan | N1–N8 and conditional experiments | Original 356-issue inventory and first-five-days order |
| September implementation log | Evidence for completed telemetry, storage split, deployment preflight, and sessions | No separate active queue |
| September work-count and five-levers plans | N6/N7 measurements and conditional wave policy | Same-path DV rewrite premise and completed feeder/certification work |
| `rollup-plan.md` | Conditional stages, query contracts, advisories, and ops follow-ups | September production snapshot and completed mechanisms |
| October 4 metrics layout | N7 maintenance acceptance and conditional attribute-index work | Layout implementation and recorded migration result |
| October 7 incident follow-up | N2, N3, N7, N8 | Full-disk snapshot, local sessions, and build handles |
| October 9 long-window reads | N4, N5, N6 | Completed October fixes and old open leads closed by later commits |

On October 20, the historical notes were synthesized into four topic references, then removed.
The summaries preserve useful concepts, correctness constraints, failed experiments, acceptance limits, and conditional design contracts.
They do not retain every historical observation or duplicate implementation diary.
The inventory maps each original note to its topic. Detailed originals remain available at revision `ad1ee10bfc7c76217924c9aa465a5c5d4f30392a`.
The plan directory now contains only its index and the current backlog.

### Decisions preserved from the first four retirements

The first four September retirements duplicated the current backlog and retained obsolete execution orders.
The later topic synthesis includes the remaining dated notes and the conditional design documents.
Their 46-item crosswalk remains below. Their original text is available at commit `ad1ee10bfc7c76217924c9aa465a5c5d4f30392a`.

```sh
git show ad1ee10bfc7c76217924c9aa465a5c5d4f30392a:docs/plans/2026-09-16-next-days-work-plan.md
```

The same command retrieves the other three filenames from the inventory.
The following decisions and acceptance caveats remain relevant:

- The September 16 arrival-cohort replay rejected size ratio 4. Rewrite bytes fell only 13.7%, while waves rose 66 to 186.
  Live files rose 74 to 260. The gates required at least 25% fewer rewrite bytes and at most 10% more files.
  This isolated replay lacked a common active-file seed and a measured duration model. It does not predict production capacity.
- Eager retraction reached 95.2%, but later physical-wave measurements still showed substantial rewrites.
  N7 retains matched-hour physical-work accounting. Retraction alone does not close the packing investigation.
- The old landed-skip enablement task is obsolete. The source default is true, and dirty-boot replay regression coverage exists.
  N2 retains exact-once logical results and replay acceptance. A clean deploy does not exercise the dirty-boot path.
- The stream-failure OTel counter already exists. N1/N8 address caller attribution and failure coverage rather than recreating that counter.
- The September session proposal failed its semantic review before later v2 work.
  N6 must preserve browser scope, latest-page semantics, equal-time tie handling, URL/UA fallbacks, and legacy-cell refusal.
  A routed query alone does not establish those semantics.
- September handoff measurements found 327 failed statements across eight projects over approximately 22 seconds.
  Client classification treated draining as non-transient. N2 retains retry and durable replay acceptance for refused writes.

### September backlog crosswalk

Every item in the former 46-item backlog has a destination. Mapping does not mark its acceptance complete.

| Former items | Current destination |
| --- | --- |
| 01–02 query failures and visibility | N1, N6, N8 |
| 03–05 admission, memory, spill | Implementation in `75455c49`; N2/N7 acceptance |
| 06–08 handoff, storage failures, vacuum | N2 and N7 |
| 09–10 recurrence and diagnostic noise | N8, with N1 caller attribution |
| 11–16 baseline, identity, work ledger, metrics, retraction, rewrite census | N7, with N2 deployed-image mapping |
| 17–18 wave and metrics packing candidates | Conditional wave policy after N7 |
| 19 queue oscillation | Existing deadline/requeue fixes; N7 residual acceptance |
| 20–24 routing matrix, shape matching, bucket policy, filters, proofs | N6, with N7 queue evidence |
| 25–29 sessions semantics, measures, user-agent representation, backfill, rollout | Existing sessions v2 implementation; N6 current contract acceptance |
| 30 enrichment convergence | Existing eager retraction and mutable/enrichment handling; N6 exactness and N7 economics |
| 31–34 contracts, protocol gaps, billing, hashes | N6 and N8, plus implemented hash narrowing |
| 35–36 incremental and partial-rollup designs | Conditional stages with their original correctness gates |
| 37 maintenance interruptibility | N2 fault cases and N7 phase/resource acceptance |
| 38 latency feedback controller | Conditional after N7 demonstrates interference. Earlier query-yield A/B showed no benefit |
| 39 index coverage and fanout | N4 and N5 |
| 40 storage isolation and endurance | N7 |
| 41 restart and journal overhead | N5/N7. Existing persistence and pruning remain |
| 42–44 capacity, v4, offload | Conditional on measured single-node limits |
| 45 evidence-linked decision log | This review and the current index |
| 46 deploy identity | App/token preflight implemented. Wider service/image/mount review remains conditional ops work |

### Decisions that remain closed

The old same-path DV re-add “28% rewrite lane” was a measurement error.
Do not create a new physical rewrite project from it.
The recorded query-yield, byte-admission, W46 restriction, write-capture removal, and range-alignment experiments did not justify activation.
Sessions v2 has consumers. Do not reuse the old “sessions has no consumer” premise.
Metric-first layout is implemented. Do not use the old proposed `(metric_name, series_id, timestamp)` order.

## Reproduction and validation

Use the original project explicitly if CLI configuration changes.
The CLI commands below are read-only.

```sh
monoscope -p 87576849-4941-49d3-a15d-680fef88a1a8 issues list --status all --service timefusion --per-page 100 --page 0
# Repeat pages 1 through 31 and require final has_more=false.
monoscope -p 87576849-4941-49d3-a15d-680fef88a1a8 monitors list
monoscope -p 87576849-4941-49d3-a15d-680fef88a1a8 metrics query 'resource.service.name == "timefusion" and severity.severity_text in ("error", "fatal") | summarize count() by bin(timestamp, 1h)' --from 2026-10-18T09:43:59Z --to 2026-10-19T09:43:59Z
monoscope -p 87576849-4941-49d3-a15d-680fef88a1a8 metrics query 'resource.service.name == "timefusion" and severity.severity_text in ("error", "fatal") and body contains "No field named service" | summarize count()' --from 2026-10-18T09:43:59Z --to 2026-10-19T09:43:59Z
```

The successful first aggregate used a relative 24-hour window whose returned bounds equal these fixed timestamps.
The fixed-bound missing-service aggregate returned 819.
An attempted aggregate grouped by `body` returned `Column not found`. It supplies no evidence.

Validation for this documentation change checks relative links, inventory IDs/counts, and whitespace.
The October 20 consolidation also checks topic coverage for every original note and replacement anchors.
Only documentation and reference comments changed. No runtime code, dependency, or build setting changed.
No Cargo checks or CI attestations were run.

## Plan inventory

The review inventoried 112 existing Markdown files: titles, status sections, checklists, and later corrections.
The mainline history and current code were checked for the completion claims above.
Historical measurements were not all reproduced. Reference status does not mean every proposal is implemented.
Any residual work must enter the current backlog with fresh evidence, an owner, and an acceptance condition.

| Original document at `ad1ee10b` | Concept summary |
| --- | --- |
| `2026-08-18-an-architecture-that-keeps-up.md` | [Decisions](../reference/timefusion-decisions.md#capacity-and-prior-art) |
| `2026-08-19-the-gap-is-decode-not-scheduling.md` | [Decisions](../reference/timefusion-decisions.md#decisions-and-corrections-to-retain) |
| `2026-08-21-post-hot-tier-speed.md` | [Decisions](../reference/timefusion-decisions.md#decisions-and-corrections-to-retain) |
| `2026-08-22-file-level-needle-pruning.md` | [Reads](../reference/timefusion-reads.md#query-correctness-comes-before-pruning) |
| `2026-08-22-make-14d-30d-complete.md` | [Reads](../reference/timefusion-reads.md#query-correctness-comes-before-pruning) |
| `2026-08-22-query-latency-matrix.md` | [Reads](../reference/timefusion-reads.md#query-correctness-comes-before-pruning) |
| `2026-08-22-rollup-correctness-and-routing.md` | [Maintenance](../reference/timefusion-maintenance.md#the-optimization-objective) |
| `2026-08-22-seven-window-six-project-matrix.md` | [Reads](../reference/timefusion-reads.md#query-correctness-comes-before-pruning) |
| `2026-08-22-untagged-tier-convergence.md` | [Maintenance](../reference/timefusion-maintenance.md#the-optimization-objective) |
| `2026-08-23-a-440gb-scan-no-longer-starves-the-box.md` | [Maintenance](../reference/timefusion-maintenance.md#the-optimization-objective) |
| `2026-08-23-log-list-30d-is-a-footer-repair-problem.md` | [Reads](../reference/timefusion-reads.md#query-correctness-comes-before-pruning) |
| `2026-08-23-the-row-witness-counts-the-wrong-rows.md` | [Maintenance](../reference/timefusion-maintenance.md#the-optimization-objective) |
| `2026-08-24-a-trivial-query-costs-seconds-after-hours-of-uptime.md` | [Decisions](../reference/timefusion-decisions.md#decisions-and-corrections-to-retain) |
| `2026-08-24-handoff-open-work.md` | [Decisions](../reference/timefusion-decisions.md#decisions-and-corrections-to-retain) |
| `2026-08-24-handoff-results.md` | [Decisions](../reference/timefusion-decisions.md#decisions-and-corrections-to-retain) |
| `2026-08-24-open-work-after-untagged-convergence.md` | [Maintenance](../reference/timefusion-maintenance.md#the-optimization-objective) |
| `2026-08-25-dedup-strategy-triage.md` | [Maintenance](../reference/timefusion-maintenance.md#the-optimization-objective) |
| `2026-08-25-ingest-profile-leftovers-closed.md` | [Durability](../reference/timefusion-durability.md#accepted-writes-and-ownership) |
| `2026-08-25-log-list-30d-is-a-pruning-problem.md` | [Reads](../reference/timefusion-reads.md#query-correctness-comes-before-pruning) |
| `2026-08-25-maintenance-throughput-and-rollup-reads.md` | [Maintenance](../reference/timefusion-maintenance.md#the-optimization-objective) |
| `2026-08-25-oldest-task-tail.md` | [Maintenance](../reference/timefusion-maintenance.md#the-optimization-objective) |
| `2026-08-25-partial-certification-design.md` | [Maintenance](../reference/timefusion-maintenance.md#the-optimization-objective) |
| `2026-08-25-preflight-floor-needs-a-byte-model-not-a-prod-journal.md` | [Maintenance](../reference/timefusion-maintenance.md#the-optimization-objective) |
| `2026-08-25-prod-followup-lag-and-ledger.md` | [Decisions](../reference/timefusion-decisions.md#decisions-and-corrections-to-retain) |
| `2026-08-25-prod-quiet-window-baseline.md` | [Decisions](../reference/timefusion-decisions.md#decisions-and-corrections-to-retain) |
| `2026-08-25-rollup-witness-design.md` | [Maintenance](../reference/timefusion-maintenance.md#the-optimization-objective) |
| `2026-08-25-the-900s-is-a-queue-not-a-rewrite.md` | [Maintenance](../reference/timefusion-maintenance.md#the-optimization-objective) |
| `2026-08-25-union-tags-stay-deferred.md` | [Maintenance](../reference/timefusion-maintenance.md#the-optimization-objective) |
| `2026-08-27-cheap-compaction-sort-dedup.md` | [Maintenance](../reference/timefusion-maintenance.md#the-optimization-objective) |
| `2026-08-28-branch-consolidation.md` | [Decisions](../reference/timefusion-decisions.md#decisions-and-corrections-to-retain) |
| `2026-08-28-damage-repair-converged-and-the-1h-tier.md` | [Maintenance](../reference/timefusion-maintenance.md#the-optimization-objective) |
| `2026-08-28-sortedness-known-at-write-time.md` | [Reads](../reference/timefusion-reads.md#query-correctness-comes-before-pruning) |
| `2026-08-31-how-other-systems-schedule-maintenance.md` | [Maintenance](../reference/timefusion-maintenance.md#the-optimization-objective) |
| `2026-08-31-maintenance-throughput-night.md` | [Maintenance](../reference/timefusion-maintenance.md#the-optimization-objective) |
| `2026-09-01-certification-cannot-converge.md` | [Maintenance](../reference/timefusion-maintenance.md#the-optimization-objective) |
| `2026-09-01-certification-coverage.md` | [Maintenance](../reference/timefusion-maintenance.md#the-optimization-objective) |
| `2026-09-01-morning-summary.md` | [Decisions](../reference/timefusion-decisions.md#decisions-and-corrections-to-retain) |
| `2026-09-01-per-class-worker-pools.md` | [Maintenance](../reference/timefusion-maintenance.md#the-optimization-objective) |
| `2026-09-01-slice-certification-plan.md` | [Maintenance](../reference/timefusion-maintenance.md#the-optimization-objective) |
| `2026-09-01-what-100x-actually-requires.md` | [Decisions](../reference/timefusion-decisions.md#decisions-and-corrections-to-retain) |
| `2026-09-02-dedup-units-sit-exactly-on-the-admission-boundary.md` | [Maintenance](../reference/timefusion-maintenance.md#the-optimization-objective) |
| `2026-09-02-monoscope-compatibility-roadmap.md` | [Reads](../reference/timefusion-reads.md#query-correctness-comes-before-pruning) |
| `2026-09-02-morning-brief.md` | [Decisions](../reference/timefusion-decisions.md#decisions-and-corrections-to-retain) |
| `2026-09-02-repair-budget-sized-by-the-target-file.md` | [Maintenance](../reference/timefusion-maintenance.md#the-optimization-objective) |
| `2026-09-02-scale-readiness-10x-100x.md` | [Decisions](../reference/timefusion-decisions.md#decisions-and-corrections-to-retain) |
| `2026-09-02-stop-manufacturing-duplicates.md` | [Durability](../reference/timefusion-durability.md#duplicate-producers-and-prevention) |
| `2026-09-03-correction-the-proof-is-stale-not-churned.md` | [Maintenance](../reference/timefusion-maintenance.md#the-optimization-objective) |
| `2026-09-03-maintenance-voids-the-proof-it-earns.md` | [Maintenance](../reference/timefusion-maintenance.md#the-optimization-objective) |
| `2026-09-03-morning-brief.md` | [Decisions](../reference/timefusion-decisions.md#decisions-and-corrections-to-retain) |
| `2026-09-03-the-admission-boundary-is-the-only-blocker.md` | [Maintenance](../reference/timefusion-maintenance.md#the-optimization-objective) |
| `2026-09-03-the-duplicate-measurement.md` | [Maintenance](../reference/timefusion-maintenance.md#the-optimization-objective) |
| `2026-09-03-the-frozen-mass-day-wide-dedup.md` | [Maintenance](../reference/timefusion-maintenance.md#the-optimization-objective) |
| `2026-09-03-the-hashes-pushdown-is-not-safe-and-already-exists.md` | [Reads](../reference/timefusion-reads.md#query-correctness-comes-before-pruning) |
| `2026-09-03-the-sorted-path-is-linear-the-unsorted-one-is-not.md` | [Maintenance](../reference/timefusion-maintenance.md#the-optimization-objective) |
| `2026-09-03-the-unpairable-band.md` | [Maintenance](../reference/timefusion-maintenance.md#the-optimization-objective) |
| `2026-09-03-user-queries-are-failing-on-sort-memory.md` | [Maintenance](../reference/timefusion-maintenance.md#the-optimization-objective) |
| `2026-09-03-why-hashes-queries-time-out.md` | [Reads](../reference/timefusion-reads.md#query-correctness-comes-before-pruning) |
| `2026-09-03-why-the-frozen-mass-is-a-read-path-bug.md` | [Maintenance](../reference/timefusion-maintenance.md#the-optimization-objective) |
| `2026-09-04-certification-proves-the-wrong-thing.md` | [Maintenance](../reference/timefusion-maintenance.md#the-optimization-objective) |
| `2026-09-04-lane-coverage-matrix.md` | [Maintenance](../reference/timefusion-maintenance.md#the-optimization-objective) |
| `2026-09-04-morning-brief.md` | [Decisions](../reference/timefusion-decisions.md#decisions-and-corrections-to-retain) |
| `2026-09-04-otel-metrics-never-got-the-collapse.md` | [Reads](../reference/timefusion-reads.md#query-correctness-comes-before-pruning) |
| `2026-09-04-the-row-cap-guards-the-wrong-cost.md` | [Reads](../reference/timefusion-reads.md#query-correctness-comes-before-pruning) |
| `2026-09-06-dv-dedup-phase1-design.md` | [Maintenance](../reference/timefusion-maintenance.md#the-optimization-objective) |
| `2026-09-06-dv-writer-phase0.md` | [Maintenance](../reference/timefusion-maintenance.md#the-optimization-objective) |
| `2026-09-06-merge-policy-prior-art.md` | [Decisions](../reference/timefusion-decisions.md#decisions-and-corrections-to-retain) |
| `2026-09-06-next-improvements-fable.md` | [Maintenance](../reference/timefusion-maintenance.md#the-optimization-objective) |
| `2026-09-06-ten-x-readiness.md` | [Decisions](../reference/timefusion-decisions.md#decisions-and-corrections-to-retain) |
| `2026-09-07-certify-on-completion-design.md` | [Maintenance](../reference/timefusion-maintenance.md#the-optimization-objective) |
| `2026-09-07-ingest-dedup-prevention-design.md` | [Durability](../reference/timefusion-durability.md#accepted-writes-and-ownership) |
| `2026-09-08-hashes-and-long-range-issue-charts.md` | [Reads](../reference/timefusion-reads.md#query-correctness-comes-before-pruning) |
| `2026-09-08-histogram-rust-review.md` | [Reads](../reference/timefusion-reads.md#query-correctness-comes-before-pruning) |
| `2026-09-08-indexed-hash-histogram-plan.md` | [Reads](../reference/timefusion-reads.md#query-correctness-comes-before-pruning) |
| `2026-09-09-automatic-hash-backfill-review.md` | [Reads](../reference/timefusion-reads.md#query-correctness-comes-before-pruning) |
| `2026-09-09-empty-hash-histogram-review.md` | [Reads](../reference/timefusion-reads.md#query-correctness-comes-before-pruning) |
| `2026-09-09-fresh-hash-index-review.md` | [Durability](../reference/timefusion-durability.md#accepted-writes-and-ownership) |
| `2026-09-09-full-index-rebuild-plan.md` | [Maintenance](../reference/timefusion-maintenance.md#the-optimization-objective) |
| `2026-09-09-hash-sql-benchmark-review.md` | [Reads](../reference/timefusion-reads.md#query-correctness-comes-before-pruning) |
| `2026-09-09-histogram-dml-ranges.md` | [Reads](../reference/timefusion-reads.md#query-correctness-comes-before-pruning) |
| `2026-09-09-local-production-image.md` | [Durability](../reference/timefusion-durability.md#accepted-writes-and-ownership) |
| `2026-09-09-streamed-histogram-cache-review.md` | [Reads](../reference/timefusion-reads.md#query-correctness-comes-before-pruning) |
| `2026-09-11-noop-skip-production-result.md` | [Maintenance](../reference/timefusion-maintenance.md#the-optimization-objective) |
| `2026-09-11-the-journal-lock-is-the-next-ceiling.md` | [Maintenance](../reference/timefusion-maintenance.md#the-optimization-objective) |
| `2026-09-11-the-rollup-lane-rebuilds-what-it-already-has.md` | [Maintenance](../reference/timefusion-maintenance.md#the-optimization-objective) |
| `2026-09-11-write-latency-self-inflicted-disk-flood.md` | [Durability](../reference/timefusion-durability.md#accepted-writes-and-ownership) |
| `2026-09-12-cache-admission-plan.md` | [Decisions](../reference/timefusion-decisions.md#decisions-and-corrections-to-retain) |
| `2026-09-12-group-commit-in-production.md` | [Durability](../reference/timefusion-durability.md#accepted-writes-and-ownership) |
| `2026-09-12-hygiene-starvation-and-the-10x-write-question.md` | [Decisions](../reference/timefusion-decisions.md#decisions-and-corrections-to-retain) |
| `2026-09-12-surviving-10x-on-the-write-path.md` | [Decisions](../reference/timefusion-decisions.md#decisions-and-corrections-to-retain) |
| `2026-09-13-append-merge-rollups-and-the-drain.md` | [Maintenance](../reference/timefusion-maintenance.md#the-optimization-objective) |
| `2026-09-13-rollup-maintenance-prior-art.md` | [Decisions](../reference/timefusion-decisions.md#decisions-and-corrections-to-retain) |
| `2026-09-13-stop-re-rolling-deduped-partitions.md` | [Maintenance](../reference/timefusion-maintenance.md#rollup-source-witnesses) |
| `2026-09-13-the-backlog-drain-arithmetic.md` | [Maintenance](../reference/timefusion-maintenance.md#the-optimization-objective) |
| `2026-09-13-where-the-28-cores-actually-go.md` | [Decisions](../reference/timefusion-decisions.md#decisions-and-corrections-to-retain) |
| `2026-09-13-why-every-query-misses-the-rollup.md` | [Maintenance](../reference/timefusion-maintenance.md#the-optimization-objective) |
| `2026-09-15-cpu-per-byte-campaign.md` | [Decisions](../reference/timefusion-decisions.md#decisions-and-corrections-to-retain) |
| `2026-09-15-five-levers-to-500ms.md` | [Maintenance](../reference/timefusion-maintenance.md#the-optimization-objective) |
| `2026-09-15-work-count-not-code-speed.md` | [Maintenance](../reference/timefusion-maintenance.md#the-optimization-objective) |
| `2026-09-16-implementation-log.md` | [Maintenance](../reference/timefusion-maintenance.md#the-optimization-objective) |
| `2026-09-16-next-days-work-plan.md` | [Maintenance](../reference/timefusion-maintenance.md#the-optimization-objective) |
| `2026-09-17-maintenance-task-flow.md` | [Maintenance](../reference/timefusion-maintenance.md#the-optimization-objective) |
| `2026-09-17-query-routing-matrix-harness.md` | [Reads](../reference/timefusion-reads.md#query-correctness-comes-before-pruning) |
| `2026-09-20-why-maintenance-left-cores-idle.md` | [Maintenance](../reference/timefusion-maintenance.md#the-optimization-objective) |
| `2026-09-24-rollups-on-a-fixed-server.md` | [Maintenance](../reference/timefusion-maintenance.md#conditional-fixed-server-stages) |
| `2026-09-25-dependency-upgrade.md` | [Durability](../reference/timefusion-durability.md#accepted-writes-and-ownership) |
| `2026-09-26-stage3-publication-design.md` | [Maintenance](../reference/timefusion-maintenance.md#conditional-publication-queue) |
| `2026-10-04-otel-metrics-metric-first-layout.md` | [Reads](../reference/timefusion-reads.md#metric-first-layout) |
| `2026-10-07-timefusion-incident-followup.md` | [Durability](../reference/timefusion-durability.md#accepted-writes-and-ownership) |
| `2026-10-09-long-window-reads.md` | [Reads](../reference/timefusion-reads.md#retained-query-targets-and-tools) |
| `README.md` | [Current index](../plans/README.md#historical-concepts) |
| `approaches-and-decisions.md` | [Decisions](../reference/timefusion-decisions.md#decisions-and-corrections-to-retain) |
| `rollup-plan.md` | [Maintenance](../reference/timefusion-maintenance.md#the-optimization-objective) |
