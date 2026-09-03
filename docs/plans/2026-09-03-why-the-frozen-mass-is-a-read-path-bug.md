# The frozen dedup mass and the query sort-OOMs are ONE defect

**2026-09-03.** Two threads investigated separately tonight turn out to be the
same problem, joined by a mechanism this repo already documented a month ago.
This is the synthesis, with the prior art that predicts it.

## The chain

```
monoscope enriches spans continuously (version_append)
  -> an UPDATE to an old row appends a NEW file carrying that OLD timestamp
  -> files stop being TIME-RANGE DISJOINT
  -> the Delta leg can declare no ordering
  -> DataFusion must insert a full SortExec (preserve_partitioning=true, 16 partitions)
  -> each partition holds an UNSPILLABLE ExternalSorterMerge reservation
  -> ~16 x 1.5 GB against a 16 GB pool -> the query dies
```

Every link is measured, not assumed:

- `version_append` breaking disjointness, and **"compaction is the only thing
  that restores it"** — traced 2026-08-02, kept as
  `tf_mor_breaks_time_disjoint_files_2026-08-02`. That note's `EXPLAIN` shows
  precisely `SortExec preserve_partitioning=[true]` under the Delta leg.
- The failing production query shape is that same operator: a 16-partition
  `ExternalSorter` + unspillable `ExternalSorterMerge`, which is **not** a shape
  any of the plans I EXPLAINed produce on a *healthy* partition — those plan
  `SortExec: TopK(fetch=100)` because the scan declares
  `output_ordering=[timestamp DESC]`.
- The partitions that cannot declare ordering are the ones compaction never
  reached — and 96% of the dedup queue's bytes had not moved in 18 days
  (`2026-09-03-the-frozen-mass-day-wide-dedup.md`).

**So the read-path failure is a symptom of the maintenance starvation, and the
horizon-turn fix (`dd4a557f`) is a read-path fix as much as a maintenance one.**

## Why the sort is a whale, specifically

A healthy partition's scan declares ordering, so the query plans a TopK bounded by
`LIMIT`. An un-deduped partition cannot, so the SAME query re-plans as a full
16-partition sort of the whole scanned range. **The query does not get slower
gradually — it changes plan shape.** That explains the burstiness measured
separately: 17 failures in one hour and zero in the next three is what you get
when a dashboard occasionally touches a stale day.

## Prior art: this is the known cost of merge-on-read

**InfluxDB IOx** states the design rule directly: the compactor's job is to
produce *"fewer, larger, and non-overlapped files"*, because *"overlapped files
may contain duplicates that need deduplication during query time, which reduces
query performance"* — and crucially, **non-overlapping files only need to be
UNIONED with the deduplication output, while overlapping files must go through
the Deduplicate & Merge operator**
([InfluxData](https://www.influxdata.com/blog/compactor-hidden-engine-database-performance/)).
The compactor is explicitly framed as *pre-query processing*: work moved off the
read path. When it stalls, the cost reappears in queries — which is exactly what
we measured.

**ClickHouse ReplacingMergeTree** avoids the problem by construction: parts are
sorted by the `ORDER BY` key, so a merge is a streaming k-way merge with no sort
at all. We adopted that shape on 2026-09-02 (dedup keys as a sort prefix +
`RunCollapse`), which is why our *rewrite* is cheap — but it only helps
partitions the rewrite actually reaches.

The LSM literature treats compaction granularity and data-movement policy as
first-class design dimensions rather than constants ([Sarkar et al., *Constructing
and Analyzing the LSM Compaction Design Space*, VLDB
2021](http://vldb.org/pvldb/vol14/p2216-sarkar.pdf)), and compaction memory as
something to budget explicitly ([Luo & Carey, *Adaptive Memory Management in
LSM-based Storage Systems*, VLDB 2021](https://vldb.org/pvldb/vol14/p241-luo.pdf)).
Both are the same message: **the scheduler's reachability is a correctness-ish
property of the read path, not a background nicety.**

## The lever this suggests, NOT YET IMPLEMENTED

IOx's distinction — union the non-overlapping, merge only the overlapping — is
one we do not make in the rewrite. `rows_filter` is
`{partition_filter} AND {DEDUP_FILE_COL} IN ({in_list}){shard_pred}`: scoped by
FILE SET, so a unit rewrites **every row of every selected file**, whether or not
that file overlaps any other.

We already own the cheap test. `probe_dup_bins` classifies every 10-minute bin of
a (project, date) with one probe and is ~200x cheaper than a rewrite
(`tf_dedup_is_a_proof_not_a_removal_2026-09-01`) — **but it is only wired into
`run_certification_pass`, not into the coordinator's dedup unit.** A day-wide unit
therefore rewrites 1,150 GiB even when only a few bins are dirty.

**Before building this, the caveat that could kill it:** `version_append` means
an enrichment UPDATE writes an old timestamp into a new file, so overlap is
created continuously and may be widespread rather than sparse. The measurement
that decides it is the ratio of dirty bins to total bins per day — which
`probe_dup_bins` can answer directly, and `cert_declined_dirty_bins = 12,716`
suggests is NOT small. **Measure before building.**

## Status

- `dd4a557f` (horizon turn) is live and 10x-validated; it makes the frozen band
  reachable. It does not make units cheaper.
- The bin-narrowed rewrite above is the next cost lever, and it is unbuilt and
  unmeasured.
- The query-side mitigation (bounding per-partition unspillable merge memory) is
  separate and also unbuilt — but if compaction keeps up, the whale plan should
  stop being generated at all.

## Do not confuse the retired dirty-bin QUEUE with the live probe

Chasing the lever above I found `dirty_bin_enqueued_total = 128` with
`dirty_bin_eligible_total = 0` and `dirty_bin_processed_total = 0`, and briefly
read it as another inert mechanism. **It is not — it is deliberate**, and the
code says so at `maintain.rs:~897`:

> the dedup cron skips every rollup-declared source as "owned by durable
> coordinator tasks", and both tables that produce dirty bins declare rollups. So
> the flush path has been filling a queue with no consumer … Retired HERE, not
> suppressed at `enqueue_dirty_bin`: the queue is the flush path's honest record
> of what changed, and six tests correctly assert it produces one.

`retire_undrainable_dirty_bins()` drops them on purpose. **`eligible = 0` is the
designed steady state** for `otel_logs_and_spans`, because the coordinator's
durable dedup tasks own that work instead.

**This does not weaken the IOx lever, but it does relocate it.** Two different
mechanisms share the word "bin":

| | what it is | status |
| --- | --- | --- |
| dirty-bin QUEUE | the flush path's durable record of changed bins | **retired by design** for rollup-declared tables |
| `probe_dup_bins` | a LIVE probe that classifies each 10-min bin, ~200x cheaper than a rewrite | alive, but wired only into `run_certification_pass` |

The lever is the **probe**, not the queue: give
`run_coordinator_dedup_once` the probe's answer so a day-wide unit rewrites only
the bins that actually carry duplicates. Nothing about the retired queue blocks
that — but equally, the retired queue is not evidence that bin-level narrowing is
already tried and rejected.

---

## RETRACTION — the IOx bin-narrowing lever is REFUTED, by a measurement taken two days ago

I proposed narrowing the dedup rewrite to dirty bins and said "measure before
building". **The measurement already existed**, in
`tf_dedup_removal_is_the_constraint_2026-09-01`, and it refutes the lever:

- Duplicates are **sparse** — 0.0004% of rows (~8k dropped per 2B scanned).
- Duplicates are **SPREAD** — **≈26-50 dirty bins of 144 per date**.
- And it was already tried: *"Slice-scoped dedup exists (`DedupRangeOptions.slice`)
  but does NOT help — `stage_dedup_chunk` re-reads every file a chunk touches and
  **files span bins**, so rewriting 18-35% of bins still touches most files.
  **The whole-date unit is approximately right-sized.**"*

**Narrowing ROWS does not narrow FILES**, and the file set is the cost. IOx's rule
holds for IOx because its compactor reasons over file overlap; our rewrite already
reads whole files, so bin-level selectivity buys nothing.

That note also records the near-miss in the other direction: a first sample of
1 decline / 1 dirty bin looked "concentrated" and would have justified building
bin-scoped scheduling for a workload with the opposite shape. **Sample size, not
reasoning, settled it — twice now.**

## Correction: how much of a read-path fix is `dd4a557f`, really?

Above I wrote that the horizon turn "is a read-path fix as much as a maintenance
one". **Qualify that.** The same note says dedup ranks by AGE and therefore works
the oldest backlog *while queries read the newest dates*, and its recommendation
was a boost scoped to the **last-14-day window** — which `window_turn` already
serves.

`dd4a557f` reserves a share for the **(14 d, 31 d]** band, i.e. work OUTSIDE the
query window. So:

- **What it does fix:** a band that was structurally unreachable, its
  contribution to certification, and the disjointness of days that 14d/30d
  dashboards read.
- **What it does NOT fix:** latency for the common recent-window query. That
  band was already covered by `window_turn`, and the burst failures measured
  today are not evidence about the 14-31 d band specifically.

The honest claim is narrower than the one I made: **the horizon turn unblocks
backlog and coverage; it is not the recent-query latency lever.**

## So what IS the remaining unit-cost lever?

Not bin narrowing. The same note points at the durable one, and it is an INGEST
fix, not a maintenance one:

> 0.0004% spread everywhere smells like client retries landing in separate files.
> Prevent them at ingest with a dedup-key check inside the MemBuffer's 10-minute
> bucket, so new dates are born certifiable and need neither rewrite nor probe.

That attacks the problem where it is cheapest — a hash check against rows already
in memory — instead of paying to remove duplicates after they have been written,
sorted, and spread across files. It is also the only option that scales to 100x,
because its cost is per-row-at-ingest rather than per-byte-rewritten-later.
**Unbuilt, and the correct next investigation.**
