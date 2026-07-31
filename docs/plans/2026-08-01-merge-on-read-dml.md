# Merge-on-read DML: updates become appends

Status: design (2026-08-01). Supersedes the in-place mutation model for tables
that opt in via schema. Motivated by the hot-tier work
(`2026-07-31-local-hot-tier.md`), which could not survive continuous enrichment
— but the same root cause is behind the MERGE OOMs, the OCC storms, the DV
machinery and the DML quarantine.

## The problem

TimeFusion is append-optimized end to end: WAL → MemBuffer → time buckets →
immutable Parquet → compaction. Enrichment asks it to mutate rows in place, so
every layer fights:

- `UPDATE` becomes a Delta MERGE that **rewrites whole files** (write
  amplification proportional to file size, not to changed rows).
- Those rewrites conflict with compaction and the dedup sweep (OCC storms,
  2026-07-22/23).
- Wide merges OOM the box (2026-07-17 date-prune fix, 2026-07-04
  `update_with_source`).
- Deletion vectors add a second correctness surface (2026-07-18/19/20).
- Every cache below the mutation goes stale — the local hot tier cannot hold a
  window that enrichment keeps rewriting.

Each was fixed individually. They are one problem: **in-place mutation on a
store whose value comes from immutability.**

Evidence for the shape of the workload (monoscope, `src/BackgroundJobs.hs`):
enrichment issues `UPDATE otel_logs_and_spans SET hashes = …` / session-user
COALESCE-fill, in bounded recent windows (`:2162`, `:2013`), repeatedly, until
effectively every row has been touched.

## The design

An update is an **append of a new version of the row**. Nothing is rewritten.
The read path resolves versions; compaction collapses them. This is the
merge-on-read model of Hudi MOR / Iceberg v2 / ClickHouse `ReplacingMergeTree`.

### 1. `updated_at` — a normal, schema-declared column

A visible `Timestamp(Microsecond, UTC)` column, nullable, declared per table:

```yaml
# schemas/otel_logs_and_spans.yaml
dedup_tiebreak: updated_at
fields:
  - name: updated_at
    data_type: 'Timestamp(Microsecond, Some("UTC"))'
```

Deliberately **not** internal/hidden. The merge is a generic mechanism keyed off
the schema, exactly like `dedup_keys` / `dedup_tiebreak` today: a table that
declares an `updated_at`-style tiebreak gets version merging, one that does not
behaves exactly as it does now. `otel_metrics` already declares
`dedup_tiebreak: ingested_at` and needs no change.

Why not `observed_timestamp`: it is OTel-assigned and never rewritten by an
`UPDATE`, so all versions of a row share it — a tie, not an ordering. (The
comment at `otel_logs_and_spans.yaml:12-17` describes the *re-emit* path, not
the `UPDATE` path, and is misleading as written; fix it with this change.)

Value assignment — TF owns it, never the client:

- stamped in `insert_coerce.rs` when the incoming batch lacks it (clients never
  send it; if one does, TF overwrites);
- `max(now_micros, last_issued + 1)` per table, so two versions never tie;
- seeded at boot from the max observed during WAL replay, so an NTP step
  backwards cannot make a new version lose to an old one;
- NULL sorts lowest, so pre-existing rows always lose to any new version — that
  is the whole migration story for existing data.

**Invariant: single writer per table.** Wall-clock-derived ordering is only
sound for one writing instance. Write it down now; a scale-out needs a real
sequencer.

### 2. Write path: `UPDATE` → version append

`UPDATE`/`DELETE` stop planning a Delta MERGE and instead:

1. resolve the target rows (the existing DML machinery already does this),
2. evaluate the `SET` expressions against them,
3. append the resulting rows through the normal WAL → MemBuffer → flush path,
   with a fresh `updated_at`.

`DELETE` appends a **tombstone** version (dedup key + `updated_at` + deleted
marker), collapsed at compaction. This retires the DV write path for deletes.

**Full-row versions, not partial.** The SQL requires reading the target row
regardless (`hashes = o.hashes || u.new_hashes` references the old value), so
once the row is in hand, emitting it whole is nearly free. Partial rows would
save storage but not the read, and would need a presence encoding to separate
"not set by this version" from "explicitly NULL" — complexity for the smaller
win. Cost accepted: ~2× storage on recent partitions until compaction collapses.

The append lands in the **same time partition as the base row** (it carries the
original `timestamp`), which is what keeps the merge local to a scan window.

### 3. Read path: keep-greatest

`read_dedup.rs` becomes keep-greatest-by-tiebreak **where it can be**, and stays
keep-first otherwise. This is strictly more correct than today, where keep-first
is *arrival* order and can already return a stale enriched value
nondeterministically (its own KNOWN GAP).

Streaming is preserved by the property the operator exploits: rows sharing a
dedup key share the same `timestamp`, so all versions of a key arrive in one
timestamp run, and the existing bounded-window logic (`detect_bound`) yields the
run boundary at which a key can be emitted. Verified: 1M lazily-yielded ordered
batches under `LIMIT 5` pull 6 batches (5 + one to close the final run).

**Load-bearing prerequisite, above keep-greatest itself** (found during phase 2):
the run property is only *visible* to the operator when its input still declares
an ordering. `DedupExec` requires `SinglePartition`, so `EnforceDistribution`
inserts a `CoalescePartitionsExec`, which declares none — and the mem ∪ hot ∪
delta `UnionExec` is itself unordered unless `OrderedUnionForTopK` fires, which
its own scope guards restrict to `ORDER BY … LIMIT`. So today keep-greatest
engages mainly on top-K plans, **not** on the plain scans and aggregations
phase 3 depends on.

Phase 3 therefore requires making the union order-preserving on the time column
whenever the table declares a tiebreak — sort the small MemBuffer branch as
`ordered_union_for_topk` already does, so EnforceSorting picks
`SortPreservingMergeExec` over a plain coalesce. That is a `database.rs` /
optimizer change, not a `read_dedup.rs` one, and it gates phase 3.

Where no ordering is available the operator falls back to keep-first (a
whole-scan per-key candidate buffer would be the 2026-07-21 wide-scan OOM
shape). On that path the **dedup sweep remains the authority** for which
physical version survives.

Emission is a subsequence of the input, so keep-greatest weakens no ordering
guarantee.

Also required, and not yet done: `ProjectRoutingTable::scan` must add
`schema.dedup_tiebreak` to the projection it augments for dedup keys, and call
`DedupExec::with_tiebreak(...)`. Until then keep-greatest is dormant.

Dedup mode: `Serial` only. The hash-partitioned mode is being deleted
(benchmark: 2–4× slower on wide rows, ~90× peak heap, because dedup cost is
per-row while the repartition copy is per-byte).

### 4. Compaction collapses versions

The existing dedup sweep already collapses `(dedup_keys, tiebreak)` duplicates —
it becomes the version collapser, and tombstones are dropped once no older
version can remain. Read amplification is therefore bounded by compaction lag,
not by update volume.

## What this deletes

- Delta MERGE for `UPDATE`/`DELETE`, and its OOM / OCC / quarantine machinery.
- DV writes on the delete path.
- Hot-tier invalidation **entirely** — files stop going stale, so
  `HotTier::invalidate*` and the adaptive demotion-suppression heuristic both
  become dead code.
- Cache-coherence concerns below the mutation, generally.

## Risks

| Risk | Mitigation |
| --- | --- |
| Wrong data (not slow data) if keep-greatest is wrong | Phased rollout, MERGE fallback per table, parity harness in `scripts/tf_vs_ts/` |
| Read amplification on the recent window — the window we are optimizing | Bound it: alert on versions-per-key; compaction cadence is the dial |
| Small files in older partitions from late appends | The standard MOR tax; the existing compaction cron absorbs it |
| Tiebreak ties / clock regression | HLC (`max(now, last+1)`) + replay-seeded boot; single-writer invariant |
| Storage 2× until compaction | Accepted; measure before optimizing to partial rows |

## Rollout

1. **Column + stamping.** Add `updated_at`, stamp on write, switch
   `dedup_tiebreak`. No behaviour change yet — but it immediately fixes the
   existing write-side/sweep ambiguity, where equal `observed_timestamp` makes
   "greatest wins" arbitrary.
2. **Keep-greatest read path.** Change `DedupExec`; prove streaming/LIMIT with
   tests. Still no write-path change.
3. **Version-append writes**, per table, behind a schema flag, MERGE as
   fallback. Validate with the parity harness.
4. **Retire** invalidation, suppression, and the MERGE path once proven.

Phases 1 and 2 are independently valuable and independently safe.

## Non-goals

Rollups/pre-aggregation. Multi-writer sequencing. Partial-row versions (revisit
after measuring phase 3 storage).
