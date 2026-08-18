# Plan: close the latency goal

Written 2026-08-18. Supersedes the ad-hoc ordering I had been working to. The
reordering is driven by one measurement taken tonight, not by a new idea.

## Where we actually are

```
tasks_pending              25,129   <- pinned at BACKFILL_PENDING_CEILING (25,000)
pending_base_rollup         9,742
pending_dedup               6,728
pending_derived_rollup      1,102
rollup_min_contiguous_days      0   <- THE goal metric. Unmoved.
rollup_hits_full_total          0
rollup_hits_hybrid_total        0
rollup_misses_total         2,948
  ...of which missing_project 2,870   <- 97%
```

Deployed SHA is `5c7259d`; #150-#153 are merged and building.

## The measurement that reorders everything

`rollup_miss_sampled` renders the refused plan. Every sample in the last two
hours is the SAME query:

```sql
select project_id::text, count(*)::int8
from otel_logs_and_spans
where timestamp >= $1 and timestamp < $2
group by project_id order by count(*) desc limit $3
```

It is refused by exactly one line:

```rust
let project_id = project_id.ok_or(MissReason::MissingProject)?;
```

Routing demands `project_id = <literal>`. This query does not filter on
project_id — it **groups by** it. So it falls back to a raw scan of every
project's rows for the window, and it runs at roughly **24 misses/minute**.

That is the largest single consumer of raw scan bandwidth on the box, it is
concurrent with maintenance, and it is a strong candidate for "with each spill
or any random thing queries get slow". I had this shape written down as "the
slowest query", filed under curiosities. It is 97% of all rollup misses.

## Order of work

### 1. Verify #153 in prod (deploy in flight)

`dirty_decay_ms:0 -> 10000`. RSS is the rollback signal — the premise for the
original setting was memory pressure, and if RSS climbs materially past the
~12.6 GB baseline the change comes back out. Then re-run
`perf record -F 99 -g` and confirm `clear_page_erms` /
`smp_call_function_many_cond` / `native_queued_spin_lock_slowpath` (~18% of CPU
combined) have fallen. If they have not, the attribution was wrong and this
gets reverted rather than kept on a hunch.

### 2. Route the cross-project shape (new, highest value per line)

Generalize routing from "one project_id literal" to "project_id is either
pinned by an equality filter OR carried as a group-by dimension".

The correctness argument is an intersection:

- resolve coverage per project, as today, for each of the 11 active projects
- the usable rollup range is the **intersection** of their covered ranges
- everything outside the intersection is read raw, by the existing hybrid union

So a project with no coverage for a window does not produce an undercount — it
drags that window into the raw leg, which is exactly today's behaviour. No new
way to return a wrong number, which is the only property that matters here.

Cost to watch: coverage resolution is per (project, source) and does one pass
over add actions. Doing it 11 times at plan time could make planning slower than
the scan it saves for narrow windows. Gate on window width and measure before
enabling for everything.

**Test first**: a routing unit test asserting the group-by shape routes, plus
one asserting that a project missing coverage for part of the window pushes that
part to the raw leg rather than dropping it.

#### What #155 actually buys, measured after the fact

The intersection is conservative by construction: one project short of coverage
for a range sends that range to the raw leg for EVERY project. So it matters
which projects land in the set, and that is decided by the window.

Measured 2026-08-18, prod:

- **10 projects** have source rows in the last hour, and every one of them has
  `dashboard_1m_v3` coverage for 08-15..08-17. The intersection is non-empty, so
  the ~24/min overview query — a 1-hour window — routes.
- The tier as a whole holds **13 projects**, and `4f020cf8` has exactly one day
  (08-16, 4 rows). Any window wide enough to include a project like that in the
  source loses the whole range to the raw leg.

So #155 helps the frequent narrow overview immediately, and helps 14d/30d
cross-project queries only as coverage becomes uniform. It is not a substitute
for finishing the backlog.

**The follow-up that removes the dependency**: group projects by their covered
set — there are only a few distinct ones — and emit one rollup leg per group
with `project_id IN (…)`, sending the rest to the raw leg. Exact, and it stops
one four-row project from voiding everyone else's coverage. Bounded by the
existing 32-branch check. Not attempted yet; it is a real change to the leg
structure and wants its own tests.

### 3. Dedup: stop decoding each partition K times

Root-caused and documented in
`2026-08-18-dedup-rescans-the-partition-k-times.md`. K is 84-256. This is the
throughput unlock for the 25,129-task backlog, and no amount of extra
concurrency substitutes for it.

Order within this item:

1. **Land the K logging** (`feat/log-dedup-shard-amplification`, pushed, no PR
   yet) so K is measured in prod before anything is tuned.
2. **Cheap wins**, both independent of the redesign:
   - replace SQL `md5()` bucketing with a non-cryptographic hash — 5.71% of all
     CPU, larger than the ZSTD decompression it exists to serve
   - re-derive `bytes_per_row = 4096` and `inflation = 12` from measurement.
     Every 2x of over-estimate is 2x the passes, and neither constant has ever
     been checked against a real decoded-vs-compressed ratio
3. **The redesign**: sort-based dedup (shape B) — one scan, `SortExec` (which
   already spills) on `(dedup keys, tiebreak)`, streaming take-winner-per-key.
   Removes the bucketing and the MD5 entirely and produces sorted output the
   writer wants anyway.

Non-negotiable, carried through any rewrite: both
`dedup_rewrite_counts_match` conservation checks, deletion-vector cardinality
subtraction, tombstone retention, and all-or-nothing per-shard staging. These
are what stopped the 2026-08-03 data loss. If the rewrite cannot keep them, it
does not ship.

### 4. Then, and only then, the goal metric

`rollup_min_contiguous_days` -> 30 for every project including shipbubble, and
a 30d dashboard query returning under 1s without a raw-scan fallback. Items 2
and 3 are what make this reachable; chasing it directly is what I have been
doing, and the queue has been pinned at its ceiling the whole time.

## What I am explicitly not doing

- Not raising `MAX_DECODED_BYTES` to divide K. That decode is outside the memory
  pool, so it trades CPU for untracked heap — the exact shape of the August OOM
  series.
- Not relaxing `dependencies_complete`. A rollup silently missing an hour is a
  wrong answer, which is worse than a slow one.
- Not rewriting the dedup path and the estimator in one change. The conservation
  checks are the only thing standing between a speedup and a data loss, and they
  need to fail loudly against one variable at a time.
