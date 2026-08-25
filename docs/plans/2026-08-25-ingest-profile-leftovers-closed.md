# Legacy ingest-profile leftovers: both phases closed by inspection

**Owner:** none — closed. **Status:** CLOSED, no work scheduled.
**Last reviewed:** 2026-08-25. **Closes:** `tasks/15`.

## Verdict

Both surviving phases of the deleted 2026-07-30 ingest plan are **already fixed,
by one commit, and the fix is verifiable by reading the code — no profile
required.** `a3a4b25` ("perf(flush+scan): streaming k-way flush merge;
provider-cache version ring") replaced Phase 2's whole-bucket concat+sort with a
streaming k-way merge and replaced Phase 3's `resolve_table` snapshot churn with
a per-version provider ring. Its own commit message names Phase 3's number
verbatim — "prod: entries=1, 65.5% hit rate, 11.5% of live heap in snapshot
deserialization" — i.e. the work item and the commit that closed it are the same
measurement. The task file's stated blocker ("cannot be re-measured, CPU
profiling is off in prod since the 08-11 SIGSEGV") is real but **irrelevant**:
the question was "does the whole-bucket concat still exist", which is a
structural question about code, not a runtime question about heap. It does not.
**Close the task. Nothing to delete** — there is no dead code left behind, so the
"prepare the deletion" branch of this decision does not apply.

---

## Phase 2 — flush-path memory (claimed 44% of live heap)

**Answerable by inspection: YES.** The whole-bucket concat+sort is gone, and both
halves of it were replaced by self-documenting code.

Traced end to end:

- `prepare_flush` (`src/write/mod.rs:2603`) → `dedup_batches` (`:2618`).
  `src/write/mem_buffer.rs:637-658` concatenates **only the key columns**, never
  the payload — explicitly because of the 2 GB i32 offset-overflow incident.
  Survivors are filtered per batch, so the output stays a `Vec` of
  per-batch-bounded arrays.
- `flush_bucket` (`src/write/mod.rs:2640`) → `DeltaWriteCallback`
  (`src/server/mod.rs:138`) → `prepare_staged_write`
  (`src/database/write.rs:476`) → `sort_flush_group` (`write.rs:516`).
- `sort_flush_group` (`write.rs:123-165`): under `timefusion_sort_skip_bytes`,
  in-process `sort_batches_by_schema`; over it, a pooled/spilling DataFusion sort
  bounded by `flush_sort_pool_bytes`.
- `sort_batches_by_schema` (`src/database/mod.rs:6309`) returns `FlushBatches`
  (`mod.rs:6146`) — either `Ready(vec)` or **`Merge(SortMergeStream)`**. The
  streaming branch (`mod.rs:6395+`) sorts run by run, consuming the input `Vec`
  so the unsorted original drops as soon as its sorted copy exists, then k-way
  merges through an index min-heap and `interleave_record_batch`, freeing each
  drained run and its encoded keys (`mod.rs:6262-6268`).
- `stage_batches` (`write.rs:450-464`) consumes that stream and flushes at
  `max_file_bytes`. Nothing collects it.

Remaining `concat_batches` calls in the flush path, all deliberately bounded:

| Site | Bound |
|---|---|
| `mod.rs:6350` | only when `batches.len() == 1` or `total_rows <= MERGE_MIN_ROWS` (4096, `mod.rs:6109`) |
| `mem_buffer.rs:2698` | ingest-side coalesce of the *trailing run of small batches only*, ≤ `MAX_BATCH_BYTES_FOR_COALESCE` |
| `write.rs:452` | the schema-evolution `WriteBuilder` fallback only; the comment at `write.rs:444-451` states it keeps whole-bucket residency by necessity and is unreachable when a staged writer exists |

The commit message quantifies the change: "Peak allocation drops from 2-3x bucket
bytes held for the whole write (× `flush_parallelism`) to ~1x decaying inputs +
one ~8MiB chunk."

### The `declare_sorted` honesty constraint — still satisfied

The plan's one load-bearing constraint was that the footer may only claim the
order actually written. Sorting is per **flush group**, decided before the first
chunk, and the `sorted` bool is fed to `create_writer_properties` at
`write.rs:517` (ingest) and `mod.rs:2757` / `compact.rs:97, 563, 787, 962`
(rewrites). `UnsortedFallback::Allow | Forbid` (`mod.rs:6136-6143`) makes ingest
degrade-and-count (`record_flush_sort_unsorted_fallback`) while rewrites abort.
So a group that could not be sorted does not claim it was.

That matters beyond this task: an unsorted file that *claims* sortedness is
exactly what drives the `log_list` 30d OOM (`read/mod.rs:577-585` — one
unordered branch erases the ordering a whole leg declares). The mechanism that
prevents it is in place and counted.

### One residual, explicitly not this task

`src/write/mod.rs:2644` clones the batch `Vec` for the tantivy sidecar and
`index_flushed_files` (`mod.rs:2707`) hands it to a detached spawned task, so
bucket rows stay resident past the Delta commit until the tantivy build finishes.
It is a shallow `Arc` clone with no copy, but it does extend residency.

This is **a different item from the 44% concat claim** and sizing it genuinely
would need a heap profile. Recorded here so it is not lost; **not** scheduled, and
**not** a reason to keep task 15 open.

## Phase 3 — `resolve_table` snapshot churn (claimed 11.5% of heap)

**Answerable by inspection: YES for the item as written. It is done.**

- `type DeltaProviderCache = Arc<DashMap<(String, String), ProviderVersions>>`
  (`src/database/mod.rs:126`) — keyed by `(project, table)`, whose value is a
  **3-version ring** (`PROVIDER_VERSION_RETENTION`, `mod.rs:83`) with
  **exact-version lookup** (`mod.rs:94-98`, `:101-108`). That is functionally the
  `(project, table, version)` key the old plan asked for, implemented as
  key→ring so eviction and the TTL prune stay per-version
  (`mod.rs:5138-5168`, counted in providers, not keys).
- Lookup at `mod.rs:8419-8456`: optimistic read, write path only on miss,
  provider build outside any lock via `OnceCell`.
- Counters `PROVIDER_CACHE_HITS / MISSES / EVICTIONS` (`mod.rs:289-291`,
  incremented `:5168`, `:8456`, `:8458`) are **readable without a profiler** in
  `timefusion_stats`: `provider_cache_hits/misses/evictions/hit_pct`
  (`src/server/pg_compat.rs:1286-1289`) and `provider_cache_entries` (`:1380`).

The later finding the task file flags as contradicting the plan's premise stands
and reinforces the closure: provider build measured **0.1 ms**, and `scan()` over
the `FileSelection` was 87% of the routing tax. There is nothing left in the
provider cache to win.

**Not cached, and correctly so:** `refresh_table_snapshot` (`mod.rs:589`),
reached per query via `resolve_table` → `resolve_unified_table` →
`refresh_cached_table` (`mod.rs:5269-5276`) → `update_table` (`mod.rs:2774`),
gated by `should_refresh_table` (`mod.rs:570`). This is *transient*
deserialization (clone-update-swap, `mod.rs:625-643`), not cache residency, and
it already has three mitigations: the `read_commit_entry(v+1)` 404 staleness
probe (`:600-610`), incremental `advance_catchup` capped at
`REFRESH_APPEND_CATCHUP_MAX_GAP = 64` (`:582`, `:627-636`), and a
`TimedSection("delta_snapshot_refresh")` (`:595`).

Its readback is already owned by
`docs/plans/2026-08-24-a-trivial-query-costs-seconds-after-hours-of-uptime.md`
(`:235`, `:280-295`) — the `53 → 121 ms/call with process age alone` finding,
which is an age/growth mechanism, not the cold-miss churn Phase 3 described.
**Do not re-open it here**; it has a home.

## About the profiling blocker

`TIMEFUSION_CPU_PROFILE=false` since the 2026-08-11 SIGSEGV crashloop, and
jemalloc is `prof_active:false`, so there is no live profile. That is true and it
is why this task sat unresolved. It was the wrong gate: both phases asked
structural questions ("is the whole-bucket concat still there", "is the provider
cache keyed by version") that a profile answers indirectly and the source answers
directly. **A missing measurement is not a reason to keep a question open when
the question is about code shape.**

If anyone does want a runtime readback, both are available today without a
profiling build:

- Phase 3: `timefusion_stats` → `provider_cache_hit_pct`,
  `provider_cache_entries`, `section.delta_snapshot_refresh.avg_us`.
- Phase 2: flush duration and output file sizes, plus
  `flush_sort_unsorted_fallback`.

Neither needs the pprof CPU sampler, which is the thing that crashlooped prod.

## Nothing to delete

The task's other branch was "if this is dead weight, prepare the deletion". It is
not: there is no orphaned code, no unused flag, no shim. The concat path was
*replaced*, not disabled, and the provider cache was *rewritten*, not
supplemented. The only artefact that was dead was the task file's own
description, which is now this closure.

Repo-wide search confirms no leftover references: nothing outside `.git` /
`target` / `vendor` mentions `ingest-maintenance-plan`, `ingest profile` or
`ingest_profile`, and `docs/plans/` starts at 2026-08-18.

## Done

Closed 2026-08-25. Re-open only if a heap profile — taken on a deliberate,
reviewed profiling build — puts flush-path residency back above ~20% of live
heap, in which case the item is the **tantivy sidecar clone** named above, not
the concat.
