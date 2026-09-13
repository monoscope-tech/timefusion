# Append-and-merge rollups, and why the drain is slower than its permits

2026-09-13, after #271. Prompted by a direct challenge: *"isn't our rollup
strategy inefficient? It's just a table where we keep adding new items and
rarely touch old stuff, so adding should be cheap, then it gets compacted like
any other table."*

**That is correct, it is what ClickHouse does, and our read path is already
built for it.**

## What we do today

The rollup publish is a **full partition replace**:

```rust
DeltaOperation::Write { mode: SaveMode::Overwrite, partition_by: ..., predicate: None }
```

Every invalidation re-aggregates a whole `(project, date)` cell from raw and
overwrites it. Prod runs **~460 BaseRollup units/hour**, the single largest
maintenance consumer.

## What the field does: append states, never rebuild

**ClickHouse `AggregatingMergeTree`** stores *partial aggregate states*. Rows are
INSERTed with `-State`; background part merges combine states sharing the ORDER
BY key; queries combine what remains with `-Merge`. There is no rebuild step at
all. `optimize_on_insert = 0` goes further — each source row becomes one row and
*all* aggregation is deferred to merge time.

**Druid** takes the same contract from the other end: roll up best-effort at
ingest, then let auto-compaction re-roll-up the interval to perfect it.

Both reduce rollup maintenance from `O(rows re-read)` to `O(new rows)` plus the
compaction that would happen anyway.

## Why TimeFusion is unusually ready for this

Three of the four pieces already exist and are in production:

1. **Every measure is a mergeable monoid.** `schema.rs:62` — "Only DECOMPOSABLE
   aggregates are expressible" — and `:56` — "every measure here is associative,
   so folding 1m states into 1h is exact". `dashboard_1m_v3` is entirely
   count/sum/min/max.
2. **We already merge partial states across grains.** The derived tier folds 1m
   into 1h. That IS `-Merge`, in production, today.
3. **THE READ PATH ALREADY MERGES DUPLICATE ROWS.** The rollup leg emits
   `measure.merge.sql(partial_states)` with `GROUP BY` on the dimensions
   (`rollup.rs:1440-1451`):

   ```sql
   SELECT dims…, merge(partial_states…) FROM <rollup> WHERE <ranges> GROUP BY dims…
   ```

   **So two partial rows for the same (bucket, dimensions) are already combined
   correctly by today's read path.** This was the biggest implementation risk in
   `2026-09-13-rollup-maintenance-prior-art.md`, and it is eliminated.

What is missing is only the write side: append instead of overwrite.

## The one hard problem: duplicates, and why append is worse than rebuild here

A rebuild self-heals — whatever the raw set is now, that is what the cell
becomes. **Append does not.** If a batch is aggregated into a partial twice, the
overcount is permanent under `merge = SUM`, and **min/max cannot be retracted at
all** — no monoid subtraction exists. This matters concretely: 58% of duplicate
groups in a sampled prod file were rows our own WAL replay re-inserted
(`2026-09-02-stop-manufacturing-duplicates.md`).

The design must therefore be Druid's, not a half-measure:

- **Appends are best-effort and idempotent.** Every appended partial carries an
  identity tag so a crash-replay append is recognised and skipped, reusing the
  `AlreadyLanded` machinery the rewrite paths already have.
- **Correction is re-aggregation, never decrement.** The dedup/compaction pass
  that already rewrites the raw files re-aggregates the cells it touched from
  its own deduplicated output, replacing accumulated partials with one exact
  row. That perfects the rollup as a side effect of work already scheduled, and
  collapses partial-row growth at the same time.
- **Certification is the "now exact" predicate.** `cert_slice_files_proved`
  already means a partition is provably dedup-clean.

## What it would buy, measured

- **BaseRollup's ~460 overwrite units/hour** collapse to appends proportional to
  new data.
- **A large share of the ~36x write amplification** goes with it.
- **Tantivy.** Attribution shipped in #273 and measured on a mature process:

  | cause | builds | rows indexed |
  |---|---:|---:|
  | **backfill** | 4 | **5,310,521 (83%)** |
  | wave | 10 | 825,980 (13%) |
  | flush | 52 | 288,970 (4.5%) |

  **Note the inversion: counts say flush, MASS says backfill.** Backfill
  re-indexes files that maintenance rewrites made uncovered — so partition
  churn, not ingest, drives ~10 of the box's cores. Removing overwrite churn
  attacks CPU and rollup cost with one change.

### The two gates, unchanged

1. **Measure cardinality first.** Appends are only cheap if partial rows per
   flush are far fewer than raw rows. Measure `(bucket x dimensions)` distinct
   count per file on a real partition. **Do not build before this number exists.**
2. **Shadow phase with the raw path as oracle.** Run both, assert equality on a
   sample, and only then trust the merge. The 09-07 ingest-dedup change passed
   every gate and did nothing in production because it lacked exactly this.

## Sequencing against the Logical witness — one roadmap, not two

These interact and must not be done blind to each other:

- **`Physical` -> `Logical` slice witness: do this FIRST.** Days of work on code
  that is already implemented and unit-tested, and it fixes the measured query
  pain (100% of dashboard queries currently miss the rollup, all on `moved`).
- **Append-and-merge: weeks.** It changes what "coverage" means — a watermark
  plus dirty-hours rather than a slice fingerprint — and that bookkeeping
  eventually subsumes the witness. Design it so the witness work is not thrown
  away.
- **Append does NOT by itself fix raw-source coverage voiding.** Dedup will still
  move the raw partition under a built cell.

## Separately: the drain is not permit-bound

Measured over 45 minutes: **42 bins, 10.1 permit-minutes of staging against 90
available — staging is 11% of permit time.** The rest is commit, tantivy
reindex, journal and claim overhead, which `staging_ms` does not count.

Bin sizes, same window: **median 1.4 MB, mean 14.2 MB, p90 60.8 MB, max 88.9 MB
against a 107 MB cap.** The median bin is 1.3% of the cap, so **fixed per-unit
cost dominates**, and `timefusion_pack_max_size_ratio` is 0, so the similar-size
rule is NOT what keeps bins small.

So the drain lever is neither K nor the cap: it is **cost per unit against unit
size**. Candidates, cheapest first, none yet implemented:

1. **Find out which lane admitted the over-cap bin.** One 111 MB bin (1.07x the
   sort budget) staged for 28.5 minutes while a 63 MB bin took 15 s — the cliff
   is at ~1.0x, and `wave_bin_staged` does not record which packer chose it.
   Label the lane before changing any budget.
2. **Batch cells.** A unit is one `(project, date)` cell; most cells are tiny.
   Several cells per unit would amortise the fixed cost that currently dominates.
3. **Prioritisation is visibly off**: `maintenance_hygiene_debt_unclaimed` fired
   54 times in 30 minutes with `refusal="outranked_by:…files=539"` — the most
   indebted cell keeps losing to another.

**And keep the drain in proportion.** The measured user-facing pain is rollup
routing misses, not fragmentation. The drain moves a gauge; the witness and the
append model move what customers feel.

## Postscript: the box is demand-saturated, which is the argument for all of this

The container CPU cap was raised 28 -> 32 while this was being written. Measured
on a mature (~30 min) process, both sides:

| | 28-core cap | 32-core cap |
|---|---:|---:|
| cores used | 25.0-27.7 | **28.4-31.4** |
| utilisation | ~92% | **~92%** |
| `nr_throttled` per 12 s | 111-116 | 49-97 |

**All four extra cores were absorbed and utilisation did not move.** Throttling
fell ~40% but did not stop. The change is worth roughly 14% real throughput and
should be kept — but **no core count available on this box makes CPU stop being
the constraint.**

That is the case for this proposal in one line: the system will consume whatever
CPU it is given, so the only durable lever is to stop generating the work.
BaseRollup's ~460 partition overwrites an hour, the backfill re-indexing they
cause (83% of tantivy's indexed rows), and a large share of the 36x write
amplification are **one root, not three**. At 10x traffic they scale together
into a wall that more cores cannot clear.

## GATE 1 CLEARED: the cardinality measurement

The design above was gated on "partial rows must be far fewer than raw rows —
do not build before this number is in hand." It is now in hand. One closed hour,
`dashboard_1m_v3`'s grain and dimensions (`minute x service x kind x
status_code`), measured directly against prod:

| project | raw rows | partial rows | ratio |
|---|---:|---:|---:|
| 87576849 (whale) | 96,535 | 596 | **162 : 1** |
| dcad860a | 423,721 | 304 | **1,394 : 1** |
| 28f62f01 | 141,487 | 258 | **548 : 1** |
| 00000000 (shared, worst) | 275,182 | 3,736 | **74 : 1** |
| **total** | **936,925** | **4,894** | **191 : 1** |

**The worst tenant is 74:1 and the aggregate is 191:1.** A 10-minute flush
bucket holding ~16 k raw rows produces on the order of **100 partial rows** —
appending that is trivial next to re-aggregating a whole day.

Two things worth noting from the spread:

- **The shared `00000000` partition is the outlier at 74:1**, which is expected:
  it is many small tenants multiplexed into one partition, so its distinct
  dimension set is the union of theirs. It is still an order of magnitude of
  compression, and it is the partition most in need of cheap rollups.
- **Higher-volume tenants compress BETTER, not worse** (dcad860a at 1,394:1).
  Cardinality is bounded by the dimension domain, not by row count — which is
  exactly the property that makes this design scale to 10x traffic, where a
  rebuild-based design scales linearly with rows.

So the remaining gate is the shadow phase, not the arithmetic.
