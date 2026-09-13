# Stop re-rolling deduped partitions, and stop re-deriving state at boot

2026-09-13. Two changes with one root: **we discard knowledge we already hold,
then pay to rebuild it.** Dedup discards a rollup's proof it did not invalidate;
boot discards coverage it could have persisted.

This implements option **B** of `2026-08-25-rollup-witness-design.md`, whose
analysis stands and is not repeated here. What is new is the measured sizing,
the ordering constraint, and the startup half.

## Part 0 — the constraint that orders everything

The witness doc's recommendation is explicit: **re-enqueue witness-MOVED slices
before either B or C**, because it *"converts 'stale forever' into 'stale until
rebuilt', which is what makes every option below actually converge."*

That is not optional sequencing, and I hit it empirically today. **Skipping the
invalidation without a path back to `proven` is strictly worse than today**: we
would correctly skip the rebuild, and the read path — which re-verifies
independently — would refuse that cell forever. Right numbers, permanently slow
queries. Today the invalidation is what eventually re-proves the cell.

So: **(0) re-enqueue, then (1) carry-forward.** Never (1) alone.

## Part 1 — carry the witness across a dedup instead of invalidating

### Why it is sound, verified twice

The rollup builds from a deduplicated read:

```sql
SELECT … FROM (SELECT …, ROW_NUMBER() OVER (PARTITION BY dedup_keys
                                            ORDER BY tiebreak DESC NULLS LAST) AS __tf_rn
               FROM raw WHERE …) WHERE __tf_rn = 1 AND NOT tombstone
```

and physical dedup collapses the same `dedup_keys` keeping the greatest
`dedup_tiebreak`. **One schema declaration drives both**, so physical dedup
removes exactly the rows the build never counted. The rollup's numbers are
unchanged; only its *proof* breaks.

### What actually has to change

The arithmetic is free and already computed: `wave_dropped_rows(&result.landed)`
(`compact.rs:1029`) sums `DedupUnit::dropped()` over **landed bins only**, so a
bin that failed to commit contributes nothing — exactly the property a witness
update needs. Per landed dedup bin:

```
witness(project_id, bin.dedup.date) -= bin.dedup.dropped()
```

The work is durability — **four holders of one fact**:

| holder | cost |
|---|---|
| in-memory `rollup_slice_coverage` | trivial |
| the journal publication | trivial |
| the coverage ledger | trivial |
| tier files' `TAG_SOURCE_ROWS` | **a metadata-only Remove+Add against the tier** |

The tags are the recovery authority. **Skip them and the repair is undone by the
next restart** — and prod restarts on every deploy (six-plus today).

### Gate it on a declared tiebreak

Without `dedup_tiebreak`, both sides fall back to *keep-first*, and "first"
depends on scan order — physical and read dedup could pick different winners, so
`min`/`max`/`sum` over survivors could genuinely differ. `otel_logs_and_spans`
declares `updated_at`; the skip must be conditional on the declaration, not
blanket.

### Sizing — state it plainly, it is not the big lever

Measured on a fresh prod process today:

| class | count | share |
|---|---:|---:|
| `rollup_stale_grew` (ingest) | 229,361 | **92.6%** |
| `rollup_stale_shrank` (dedup) | 18,418 | **7.4%** |

This fixes the 7.4%. The witness doc says the same — *"grew: not addressed at
all. This is the 400, not the 4103."* It is worth doing because it is nearly
free and it repairs slices already stamped, **not** because it is the 10x lever.

**The 92.6% is `grew`** — the witness is the whole partition's `num_records`, so
any ingest anywhere in the day moves it, including hours no slice ever claimed.
That is option A (bounded witness) and it is the larger prize.

## Part 2 — stop re-deriving at boot

### Measured, so the framing is right

```
15:28:44  Starting TimeFusion
15:28:45  Listening on 5432              ← readiness is 1 s, NOT blocked
15:28:46  reconcile complete (34 tasks)
15:28:49  tantivy_coverage_census (uncovered=126)
15:29:46  rollup_backfill_census (base_tier_ready=344, then 802)
```

**The walks do not block availability** — that was worth checking before
"optimising" a startup path that is already fast. `table_preload` reads 13,141
files across 13 tables in ~1.2 s, parallel.

The real cost is what happens *after* readiness: the censuses enumerate the whole
retention window, and the reconcile walk re-reads commits from its cursor —
**on a box already at ~92% CPU, every deploy.** It competes with the maintenance
it just re-enqueued.

### The changes, cheapest first

1. **Do not re-census what the tags already prove.** Coverage is recovered from
   tier file tags; the census then re-derives the same facts. Recover once, then
   let invalidation drive changes.
2. **Bound the reconcile walk.** It reads every commit from cursor→version. After
   a long gap that is unbounded; the gap path already degrades to `ALL_HOURS`
   (`partition_hours.values_mut().for_each(|h| *h = ALL_HOURS)`), so beyond a
   threshold the walk is buying nothing it does not already assume.
3. **Stagger the censuses off the boot spike** rather than running them at
   readiness, so they do not collide with WAL replay, preload and the first
   maintenance ticks.

## Order of work

1. **(0)** re-enqueue witness-moved slices — zero correctness surface, only
   creates work; makes everything below converge.
2. **(1)** carry-forward, all four holders, gated on a declared tiebreak.
3. **(2)** boot: recover-once, bound the walk, stagger the censuses.
4. **Then** the `grew` 94% — but with option **C**, not A. See the correction below.

## Correction: option A is NOT the answer for `grew`

An earlier draft of this plan called A (bounded physical witness) "the actual big
lever" and framed its straddle rule as a policy call a human should make. **That
was wrong, and the original design doc says why in a sentence I had skimmed:**

> *"once hot-tail packing consolidates a sealed day into one file per date,
> **every** slice bound falls inside a file and the whole date is unverifiable."*

So A is self-defeating here: the straddle poisons on any file spanning a slice
bound, and packing a day into one file per date GUARANTEES every bound lands
inside a file. The better hot-packing and sealed consolidation work — which is
what we spent today making work — the more sealed days stop routing. The doc
ranks A fourth of four and notes "its straddle rule fights hot-tail packing."

**What `Unverifiable` costs a user, precisely:** nothing incorrect. `Valid`
routes to the tier; `Stale` and `Unverifiable` both keep the range OFF it, so the
query reads raw parquet and returns the same answer more slowly. The three-valued
verdict exists to distinguish which work fixes it (`Stale` → rebuild,
`Unverifiable` → the evidence must exist), not to gate correctness.

But "slower" lands in the wrong place: sealed days are exactly where a rollup
earns most, because a 30-day dashboard scanning raw is the expensive case. A
trades ingest-invalidated RECENT days for unroutable SEALED days.

**C (logical row count) is the target** — invariant under both packing and dedup,
moving only when rows genuinely change. Its blocker stands: it shares an index
family with the 2026-09-04 `COUNT(*)` 27%-low defect (`2f08c4a6`), which must be
settled before any witness is stamped from it.

## What is NOT in scope

- **Option C (logical witness).** The doc wants it shipped dark for a week first,
  and it shares an index family with the 2026-09-04 `COUNT(*)` 27%-low defect
  (`2f08c4a6`). Settle that before stamping witnesses from it.
- **Append-and-merge rollups** (#278). Different change, larger, and it does not
  subsume this one — dedup still moves the source partition under a built cell.
