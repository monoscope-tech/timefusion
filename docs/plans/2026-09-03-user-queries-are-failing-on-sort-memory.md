# 0.083% of user queries fail on sort memory — and one query cannot fit the pool

**Measured 2026-09-03** on a single 62-minute prod process (`uptime_seconds` =
3698), build `d8253be`. This is the first *user-facing* failure rate this
investigation has produced; everything before it was maintenance throughput.

## The number

| | |
| --- | --- |
| `pgwire.queries_total` | 20,456 |
| `PgWire internal error` (all kinds) | **17** |
| … of which memory exhaustion in sort/merge | **17 — all of them** |
| failure rate | **0.083%** |

Every single pgwire error in the window was sort-memory exhaustion. There is no
other failing class. Against the standing goal — *"we absolutely can't be
breaking a sweat with our current workloads"* — this is the sharpest available
counter-example, and it is on the read path, which is what customers see.

## It is ONE query, not a crowd

The tempting read is "concurrency pressure — switch the Greedy pool to
FairSpill so one query stops starving its neighbours." **That is wrong, and the
consumer dump says so.** A representative failure:

```
Resources exhausted: Additional allocation failed for ExternalSorterMerge[7]
  ExternalSorter[5]#152485      (can spill: true)  consumed 3.8 GB, peak 4.4 GB,
  ExternalSorterMerge[8]#152492 (can spill: FALSE) consumed 2.2 GB, peak 2.2 GB,
  ExternalSorterMerge[10]#152496(can spill: FALSE) consumed 1508.9 MB,
  ExternalSorterMerge[15]#152506(can spill: FALSE) consumed 1508.9 MB,
  ExternalSorterMerge[11]#152498(can spill: FALSE) consumed 1508.9 MB.
Failed to allocate additional 188.6 MB for ExternalSorterMerge[7] with 754.4 MB
already allocated — 91.4 MB remain available for the total memory pool:
greedy(used: 15.9 GB, pool_size: 16 GB)
```

The reservation IDs are near-consecutive (#152485 … #152506) and the operator
indices are `[4] [5] [8] [9] [10] [11] [15]` — **partition indices of a single
16-partition sort**, not seven independent queries.

So the arithmetic is: `QUERY_PARTITIONS_MAX = 16` partitions, each holding an
**unspillable** merge reservation of ~0.75–2.2 GB, against a 16 GB query pool.
**16 × ~1.5 GB ≈ 24 GB > 16 GB.** The query cannot fit the pool it is given, at
any level of concurrency.

**Consequences, in order of importance:**

1. **FairSpill does not fix this.** It would stop the whale from taking its
   neighbours down — the 4-in-23-seconds bursts are exactly that collateral —
   but the whale itself still dies. Pool surgery treats the blast radius, not
   the failure.
2. **The bound that matters is per-partition merge memory**, and it is
   *unspillable*, so no amount of disk or spill tuning reaches it.
3. `sort_spill_reservation_bytes` for query sessions is **64 MB**
   (`database/mod.rs:5327`), which is **~20× below** the 754 MB–1.5 GB merges
   actually observed. The reservation exists precisely so the merge has room
   set aside; at 64 MB it under-reserves by a factor of 20, so sorts die
   *mid-merge* rather than spilling earlier with merge room banked.

## Two hypotheses killed on the way

Recording these so nobody re-runs them.

- **"Query spills land on a RAM-backed `/tmp`."** The maintenance runtime env
  deliberately pins spills to the data volume with the comment *"not a RAM-backed
  `/tmp`"*, and the query env (`build_query_runtime_env`) sets **only** the memory
  pool and metadata cache — a real asymmetry, so this looked live. It is not:
  `docker inspect` gives `Tmpfs=map[]` with a writable overlay rootfs, so query
  spills go to disk. **Dead.**
  *Residual wart, not the bug:* those spills land on the Docker overlay layer
  rather than the data volume, so they are invisible to volume accounting and
  still bound by DataFusion's 100 GB `max_temp_directory_size` default.
- **"The Greedy-pool default is stale."** Its documented justification is the
  2026-05-28 incident where FairSpill gave ~30 concurrent INSERTs ~76 MB slots.
  Whether that still applies is worth knowing — but it is now moot for *this*
  failure, per consequence 1 above.
- **"Files without honest footer ordering force a full sort under `DedupExec`,
  where the LIMIT cannot reach it."** This was my best structural theory — the
  dedup requires input ordered by `timestamp DESC`, so a scan that cannot
  declare that ordering must be sorted in full, below the fetch. **Refuted by
  counters before it was written up:** `mor_delta_leg_sorts_total` = 0 and
  `read_dedup.ordering_violations_{total,delta,mem}` = 0 on the live process.
  The delta leg is not paying a sort.

## Corrections to the first draft of this document

- **`query.text` IS logged** — the first draft said the whale was unattributable
  because `db.statement` was absent. Wrong field name: the pgwire span carries
  `query.text` with the statement parameterized. Attribution is available; I
  simply grepped for the wrong key.
- **The dominant shape near the failures carries a `LIMIT`.** It is monoscope's
  log-explorer list query:
  `select jsonb_build_array(...) from otel_logs_and_spans where project_id = ?
  and timestamp between ? and ? order by timestamp asc limit ?`
- **TopK DOES engage for that shape.** `EXPLAIN` on prod over 1-hour and 3-day
  windows both plan `SortExec: TopK(fetch=100)` above the `DedupExec`, with the
  scan declaring `output_ordering=[timestamp DESC]` so no sort is needed below.
  So "the LIMIT never became a TopK" is **not** the general story.

**Therefore the whale remains unattributed.** The failing sorts are 16-partition
`ExternalSorter` + unspillable `ExternalSorterMerge`, which is a
`preserve_partitioning=true` sort — a shape none of the plans above produce.
Correlating by `grep -B1` was too loose to trust (interleaved logs from ~20k
queries). **The sound method is to match the failing `ThreadId(N)` against that
thread's most recent `query.text`**, which is the next step. Note one candidate
seen in the loose correlation, worth checking first: monoscope's enrichment
`UPDATE ... FROM (SELECT unnest(...) ... ORDER BY ...)`, which sorts with **no
LIMIT at all**.

## What must be verified before any fix

The merge reservation is unspillable and scales with the number of spilled runs,
so the levers are partition count, batch size, and the up-front reservation —
all three interact, and two of them trade against query latency. Before touching
any of them:

1. **Get the query shape.** `db.statement` is not logged, so the whale is
   currently unattributable. A sort of 4+ GB with no effective bound suggests an
   `ORDER BY` that never became a TopK. If it carries a `LIMIT`, the real fix is
   why TopK did not engage — that is a bounded-memory fix worth far more than
   re-slicing the pool.
2. **Do not bundle this with the audit-fix verification window.** One change per
   deploy; the audit fix (`cf6e9099`) is mid-deploy and needs a clean window.
3. **Any change that makes queries spill *more* makes the 100 GB
   `max_temp_directory_size` default bind sooner** (prod has 504 GB free). That
   item stops being documentation-only and ships *with* such a change.

## Related

- `tf_oom_driver_is_bulk_insert_2026-08-15`, `tf_filter_stranded_above_dedup_2026-08-20`
  — prior memory-pool work on the read path.
- `docs/plans/2026-09-03-morning-brief.md` — the night's shipped changes.
- The `QUERY_PARTITIONS_MAX = 16` cap already exists *because* per-partition
  non-spillable reservations exhausted the pool on the 48-core box. **The cap was
  the right diagnosis and is still too high** for this query.

---

## Update — the failures are BURSTY, and have not recurred in 3h10m

Measured over a properly-established window (14:38:53 → 17:48:48, timestamps
sorted, multi-task):

| | |
| --- | --- |
| `PgWire internal error` | **9 — all "Prepared statement 'all' does not exist"** |
| sort-memory query failures | **0** |
| `Not enough memory to continue external sort` | 2, both `pass=Repair` maintenance |

**Nothing was changed in the query pool**, so this is not a fix — it is evidence
about the shape of the problem. The 17-in-62-minutes measured this morning was a
**burst**, not a steady rate, which matches the original observation that the
failures clustered (4 in 23 s, then 4 in 4 min). A whale query shape runs
occasionally and takes its neighbours down with it.

Two consequences:

1. **Do not quote 0.083% as a steady-state failure rate.** It was one process's
   hour. The honest statement is "a burst of 17 sort-memory failures in one hour,
   none in the following three".
2. **A burst is harder to attribute**, because the correlation has to be running
   when it happens. The thread-matched `query.text` correlation should be armed
   ahead of time rather than run retrospectively over interleaved logs.

Unrelated but worth an owner: 9 `Prepared statement 'all' does not exist` in the
same window is a client/protocol error from monoscope, not a TimeFusion memory
problem.
