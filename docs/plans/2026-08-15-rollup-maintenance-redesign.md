# Rollup maintenance redesign: bounded cohort builds and durable invalidations

**Status:** proposed design. Measured on production 2026-08-15.

## Summary

Rollup maintenance currently pays substantial fixed costs for every project-day,
even when many projects share one physical Delta table. The redesign removes
avoidable per-tenant planning, session, scan, and commit amplification without
claiming that maintenance can be independent of data volume.

Sealed history is rebuilt in bounded multi-project scan cohorts and committed in
shared replacement waves. Steady-state maintenance is driven by a durable
dirty-hour journal. Reads, rollup schemas, generation filtering, and the public
SQL interface remain unchanged.

## Current behavior and cost

### Unit of work

A backfill unit is `(source, project_id, date)`, not
`(project_id, date, spec)`. Each unit rebuilds every declared rollup spec in
dependency order. Both current sources declare two specs: a one-minute base and
a derived one-hour tier.

Each spec is rebuilt as four disjoint six-hour replacements. A full unit
therefore performs eight target commits.

For the measured `otel_logs_and_spans` tick:

```text
pool=415 queued=415 attempted=21 built=14 uncertifiable=0 failed=7 elapsed_ms=61726
```

`61.7 s / 21 attempts = 2.9 s` is concurrency-adjusted throughput, not unit
latency. With three active slots, the implied slot occupancy is approximately
8.8 seconds. Measured throughput was 0.34 attempts/s and 0.23 successful
units/s. Across two sources, rebuilding today on a ten-minute cadence saturates
at roughly 68-102 projects, not 300.

Approximate full-pass commit counts are:

| Projects | Units per source | Commits for two sources |
|---:|---:|---:|
| 13 | 415 | 6,640 |
| 1,000 | 32,000 | 512,000 |

The calculation is `units/source × 8 commits/unit × 2 sources`.

### Production evidence

Every six-hour chunk calls `query_delta_only`, creates a maintenance context,
and repeatedly registers all four rollup tables. Production logs show these
registration bursts before nearly every chunk commit.

Completed chunks are interleaved with build failures. Failure isolation must
therefore remain per project even when scans and commits are shared.

A one-hour, project-scoped production log query exhausted the 16 GiB query pool
after `DedupExec` requested another 57.7 GiB. An unbounded all-tenant
`GROUP BY project_id` is consequently unsafe as the first implementation.

Projects with custom storage use separate Delta logs. They cannot share commits
with projects in unified tables, nor with one another when their physical table
locations differ.

### Scaling objective

Cost cannot be flat in tenant count when adding tenants also adds source bytes
and output groups. The objective is narrower: eliminate tenant-count fixed
costs so work scales primarily with source bytes, output groups, and changed
time ranges.

## Design

### 1. Bounded scan cohorts and shared replacement waves

Group candidates by `(source, date, physical table)`. Process base specs before
their derived specs.

Introduce three internal types:

- `RollupScanCohort`: projects scanned together for one source, date, physical
  table, spec, and six-hour range.
- `RollupReplaceUnit`: one project's staged replacement, including generation,
  source fingerprint, epoch, Removes, Adds, and coverage metadata.
- `RollupCommitWave`: compatible replacement units accumulated for one physical
  target table and committed atomically.

#### Cohort packing

Pack a cohort up to either limit:

- 64 projects;
- 512 MiB of estimated decoded input.

Use the existing estimate: `num_records × 4 KiB`, falling back to compressed
size multiplied by 12. Never deliberately schedule a cohort above the decoded
limit.

Run one six-hour aggregate for the cohort with `project_id` selected and grouped.
Create one reusable maintenance session per wave rather than recreating and
reregistering it for every chunk.

If a cohort fails because of memory exhaustion, timeout, or dedup resource
exhaustion, split it in half and retry both halves. Continue until the work
succeeds or reaches a failing singleton. Put only that singleton into the
existing per-project backoff; its peers continue.

#### Batch shaping and staging

Change `to_rollup_batches` to read `project_id` from aggregate output and use a
per-project generation map. Keep separate row IDs and coverage generations for
each project.

Stage the four disjoint six-hour outputs directly. Do not read or carry forward
the target after each chunk: their staged union is the complete replacement for
the project-date-spec partition.

After staging, recheck every project's source fingerprint and epoch. Delete and
exclude staged files for projects whose source moved or whose validation failed.

#### Shared commits

Accumulate successful replacements into a wave. Flush when any limit is met:

- 1,024 projects;
- 4,096 Delta actions;
- 30 seconds since the first staged result.

Reuse the coalesced-flush protocol in `src/buffered_write_layer.rs`:

1. Stage outside the commit lock.
2. Group by physical target table.
3. Union each project's Removes and Adds with explicit action attribution.
4. Acquire the table commit lock and retry OCC conflicts.
5. Probe commits whose result is ambiguous.
6. Clean files confirmed to be orphaned; retain files if landing is inconclusive.

Publish coverage only for projects present in a successful shared commit. Then
enqueue those projects for the derived spec. A derived tier must never start
from a base generation that has not committed.

Key waves by physical table. Custom-storage projects therefore remain isolated
from unified-table waves and from incompatible custom locations.

For one unified physical log, sealed history should require one or a few shared
commits per `(source, date, spec)`, rather than eight commits per project.

### 2. Durable invalidation and changed-hour refresh

Replace the in-memory-only `rollup_dirty` contract with a crash-safe maintenance
journal below `TIMEFUSION_DATA_DIR`. Persist:

```text
(project, source, date) -> { epoch, dirty-hour mask, unknown/full flag }
```

Use the same atomic snapshot pattern as existing dedup and certification state:
write a complete temporary snapshot, sync it as required by that protocol, then
atomically replace the previous snapshot.

Record invalidation before acknowledging an inbound write. A failed write may
over-invalidate, which is safe. For readable batches, preserve precise hour
masks. Time-bounded DML marks intersecting dates and hours. Timestamp-moving or
unscoped DML records unknown/full invalidation.

Ordering is part of the correctness contract:

1. Persist invalidation.
2. Commit source or target data as applicable.
3. After a successful full build, persist a zero mask.
4. After an incremental replacement, clear only hours included in the successful
   target commit.

A crash between target commit and journal clearing causes redundant work, never
stale coverage. Missing or corrupt journal state forces conservative full
rebuilds.

The target generation and source fingerprint remain the durable correctness
boundary. Keep fingerprint and epoch checks both after staging and during read
ticket validation. Invalidations optimize work discovery and rebuild scope; they
do not replace final correctness proofs.

Prioritize dirty hours for today and yesterday before historical backfill.
Expand dirty ranges to whole buckets for the spec being rebuilt. Rebuild a
derived one-hour bucket only after its corresponding base range commits.

### 3. Excluded: flush-time partial-state aggregation

Do not add ingestion-time rollup aggregation in this redesign:

- Source and rollup tables cannot commit atomically, so a crash can omit or
  duplicate partial state.
- WAL replay can deliver a source batch that already committed.
- Merge-on-read updates and deletes require logical dedup against existing Delta
  rows; aggregating only the flushed batch is incorrect.
- Min/max, t-digest, and HLL states are not generally invertible when an old row
  version is replaced.

Treat ingestion-time aggregation as a separate future design. It requires
durable source-commit identities, idempotent state application, and explicit
merge-on-read semantics.

## Rollout controls

Add internal configuration with safe defaults:

```text
TIMEFUSION_ROLLUP_MAINTENANCE_V2=false
TIMEFUSION_ROLLUP_MAINTENANCE_V2_SOURCES=
```

The source allowlist permits independent rollout. An empty allowlist enables no
source while V2 is disabled; define and test the enabled-with-empty behavior
explicitly before deployment to avoid an accidental global rollout.

## Observability

Add low-cardinality `timefusion_stats` and OpenTelemetry metrics for:

- pending dirty partitions and oldest invalidation age;
- cohort count, project count, estimated bytes, splits, and singleton failures;
- staged projects, shared commits, action count, OCC retries, and ambiguous
  landings;
- scan, staging, commit, and end-to-end duration;
- source rows and bytes, plus output rows and files;
- full versus incremental hours rebuilt.

Keep project and date values in structured logs only. Operators must be able to
diagnose backfill health from `timefusion_stats` without querying the overloaded
telemetry table.

## Verification

### Unit tests

- cohort packing at both caps;
- recursive binary split and singleton isolation;
- multi-project batch shaping and per-project generations;
- physical-table grouping;
- Remove/Add attribution to projects;
- journal ordering, merge, partial clear, missing state, and corrupt state.

### Integration tests

- one shared commit atomically replaces multiple project-date partitions;
- a failed or changed project is excluded while peers commit;
- derived work begins only after the matching base generation commits;
- custom-storage projects remain isolated;
- restart with dirty, cleared, missing, and corrupt journals;
- DML, late arrivals, WAL replay, OCC conflicts, and commits that landed but
  returned failure;
- raw-versus-rollup parity for count, sum, min/max, t-digest, filters, and hybrid
  raw tails.

### Benchmarks

Measure two independent axes:

1. Fixed total bytes split across 10, 100, and 1,000 projects, exposing tenant
   fixed costs.
2. Fixed bytes per project, confirming runtime grows with actual data volume.

### Acceptance criteria

- No cohort above the 512 MiB decoded estimate runs without first being split.
- Maintenance remains within its existing memory pool.
- A failed singleton never holds successful peers behind it.
- Sealed unified history uses at most `ceil(projects / 1024)` commits per
  date/spec after staging and action limits are satisfied.
- Dirty-hour work converges within the ten-minute cadence in the 1,000-project,
  fixed-total-data benchmark.
- Every routed result matches the raw plan.

## Deployment

Deploy with reads unchanged and V2 disabled. Enable V2 first for `otel_metrics`
and validate parity, memory bounds, commit counts, and convergence. Then enable
`otel_logs_and_spans`.

Rollback disables V2 only. The existing builder can safely replace the same
generated tables because generations, coverage, fingerprints, and read-ticket
validation remain unchanged.

## Assumptions

- Read rewriting, rollup schemas, generation filtering, and public query
  behavior remain unchanged.
- Local journal loss is recoverable through conservative full rebuilds; the
  journal is optimization state, not the correctness record.
- Unified logs receive the batching benefit while custom storage preserves its
  separate physical work.
- Initial backfill continues to scale with source data volume. This design
  removes avoidable session, planning, and commit amplification without claiming
  cost independent of bytes.
