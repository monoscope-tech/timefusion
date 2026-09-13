# Why every dashboard query misses the rollup

2026-09-13, measured on prod at ~2 h uptime.

## The observation

`rollup_hits_hybrid_total` sat **frozen at 2,690** across repeated samples while
`rollup_misses_total` climbed ~120 per 50 s — **100% of routing attempts
missing** — and the miss increment was matched almost exactly by
`rollup_miss_stale_coverage_total`. Every dashboard query was falling back to a
raw scan.

This is not a data gap. The rollup rows are there and they are fresh:

```sql
SELECT count(*), min(timestamp), max(timestamp)
  FROM otel_logs_and_spans_rollup_dashboard_1m_v3
 WHERE project_id = '00000000-...' AND timestamp >= now() - interval '2 days';
-- 168834 | 2026-09-11T12:01:00Z | 2026-09-13T10:59:00Z
```

Current to the minute. Queries are being refused a rollup that exists.

## The cause, from the discriminator that was built for this

`stale_coverage_metric` splits the refusal four ways, and the split is total:

| sub-reason | count |
|---|---:|
| `rollup_stale_no_witness` | **0** |
| `rollup_stale_no_source_rows` | **0** |
| `rollup_stale_moved` | **2,618,297** |
| — `grew` | 2,499,123 (95.4%) |
| — `shrank` | 119,174 (4.6%) |

**Every refusal is `moved`.** The freshness proof requires
`coverage.source_rows` to equal the partition's live row count; when the
partition changes under a built slice, the slice is rejected.

`mod.rs:4650`'s own comment states the consequence: *"`moved` is the partition
genuinely changing under a verifiable slice, **which no amount of rebuilding
fixes on a churning day**."* Build throughput is the fix for `no_witness`, which
is zero here. Nothing in the rollup lane can fix `moved`.

## Why this couples the backlog to query latency

A partition's live row count moves for two reasons, and BOTH are visible:

- **`grew` (95.4%)** — ingest landing in the partition.
- **`shrank` (4.6%)** — rows being REMOVED, which on this system means **dedup**.

The second is the important one. **Maintenance invalidates the coverage that
queries depend on.** Dedup drops duplicate rows and consolidation rewrites file
sets, so every partition maintenance touches has its rollup coverage
invalidated, gets rebuilt, and is invalidated again by the next maintenance
pass. Working the backlog harder produces *more* coverage churn, not less.

So "the backlog is 1.2 TB **and as a result** queries are slow" is right, but
not by the mechanism it sounds like — it is not file fragmentation making scans
heavy, it is maintenance and ingest continuously invalidating the freshness
proof so the rollup is never trusted.

## It is NOT only the open day — a closed 8-day-old partition misses too

A sampled miss carries `lo/hi` = 06:00-12:00 of the current day, which invites
"this is just the open partition, and that is unavoidable". **Tested directly
against a CLOSED day** — 2026-09-05, eight days old, one pinned project, so no
cross-project intersection and no open-tail effect:

```
before  hits=2690  miss=13315
query   SELECT count(*) ... project_id=<pinned> AND ts IN [2026-09-05, 09-06)
after   hits=2690  miss=13329        -- +14 misses, ZERO hits
```

**A partition eight days old still fails the freshness proof.** Nothing is
ingesting into 09-05; the only thing still changing it is **maintenance** —
dedup dropping rows and consolidation rewriting file sets. That is the coupling
demonstrated rather than inferred: **the backlog work is what keeps the rollup
untrusted, and therefore what keeps queries slow.**

It also means "prove a closed day once and stop re-proving it" is not sufficient
on its own — a day is not settled while maintenance still has work queued
against it. The proof has to be stable across the rewrites maintenance performs,
which is what makes the dedup-certification predicate (`cert_slice_files_proved`)
the right anchor: it already means "this partition is provably clean", which is
a stronger and more durable statement than "its row count has not moved".

## The open day is still the common case, and TimescaleDB names that half

A sampled miss carries `lo = 1789272000000000`, `hi = 1789293600000000` —
**06:00 to 12:00 of the current day**. The dashboards ask about the open
partition, which is exactly the one that cannot stop growing.

This is the case TimescaleDB's **real-time aggregation** exists for: serve the
materialized aggregate for the settled part of the range and union raw rows for
the recent tail, rather than refusing because the bucket is not final.

**TimeFusion already has that shape** — `interiors(lo, hi, grain, horizon,
&covered)` and the comment that "the realtime fringe is ALWAYS on... With it off
the rollup had to answer the whole window or nothing". The fringe is not the
problem.

The problem is one level earlier: **the fringe splits `covered`, and `covered`
is empty**, because the freshness proof rejects the whole slice when the
partition's row count moved at all. So a slice holding perfectly good 06:00-11:00
minutes is discarded because rows landed at 11:55.

That is the asymmetry to exploit: for an append-mostly source, **`grew` tells you
the tail moved, not the interior.** A slice whose partition only GREW is still
authoritative for buckets that closed before the growth. The horizon machinery
to express that already exists; what is missing is letting `grew` narrow a
slice's usable range instead of voiding it.

The caveat that must be settled first, not assumed: late-arriving data lands in
EARLIER buckets, so "grew ⇒ only the tail changed" is not free. It needs either
a per-slice max-timestamp witness or the existing dirty-hour tracking to bound
where the new rows went.

## What to do about it — do NOT ship any of this unvalidated

Routing correctness has a documented history of under-count incidents
(hybrid under-count, the 08-22 slice-fingerprint build that "routed nothing,
failing safe as a permanent miss"). Options, cheapest first:

1. **Bound the proof to the part of the window that cannot move.** A closed past
   day should be provable once and stay proved; only the open tail should have to
   re-prove. The machinery for "this partition is settled" already exists in the
   certification lane.
2. **Make the proof tolerant of known-direction change.** `shrank` under a dedup
   that provably removed only duplicates does not change any SUM/COUNT the rollup
   serves *if* the rollup was built from the deduplicated set. Whether that holds
   needs checking, not assuming.
3. **Anything that widens what routes must be oracle-compared** against the raw
   path on real prod shapes before shipping. A rollup that answers faster and
   slightly wrong is far worse than a raw scan.

## A stale log message found on the way, worth fixing

`rollup_uncovered_project` warns: *"a project in the window contributed NO
covered range; with coverage intersected across the set **this refuses the whole
query**"*. The comment immediately above it says the opposite and is the current
behaviour: *"It no longer REFUSES the query, though: the project is read raw
across the whole window while the covered projects still route, so one lagging
tenant costs its own rows a raw scan instead of everyone's."*

The message describes behaviour that was deliberately removed. It fired 45 times
in 15 minutes, all for `00000000-0000-0000-0000-000000000000`, and it cost this
investigation a wrong hypothesis — the log said the query was being refused, so
the sentinel project looked like the culprit until the code was read.

## Prior art: TimescaleDB already solved this, and names the two pieces

TimescaleDB's continuous aggregates carry an **invalidation threshold**, also
called the **materialization watermark** — a time cutoff behind the hot head of
the table. Mutations landing *before* it are logged as invalidations, because
that region has already been summarized. Data *after* it is never summarized in
the first place.

**Real-time aggregation** then answers a query by combining two sources
transparently: pre-computed results from the materialization hypertable for the
historical range, and a live aggregation against the raw hypertable for anything
newer than the watermark, unioned automatically. (Since 2.13 `materialized_only`
defaults to true, so this is opt-in — the mode exists precisely because refusing
on an unsettled bucket is the wrong default for dashboards.)

Mapped onto TimeFusion:

| TimescaleDB | TimeFusion |
|---|---|
| invalidation log | `rollup_dirty` / `invalidate_rollup_hours` — **exists** |
| materialization watermark | `horizon` / buffered cutoff — **exists** |
| real-time union | `interiors(lo, hi, grain, horizon, &covered)` — **exists** |
| — | the freshness proof that voids a slice when `source_rows` moves — **ours alone** |

Every piece of the TimescaleDB design is already present. The divergence is the
last row: TimeFusion additionally requires an exact row-count match per slice,
and that check is what empties `covered` before the real-time union ever gets to
run. TimescaleDB does not verify that the source partition is byte-stable; it
relies on the watermark to say which region is summarizable and the invalidation
log to say which summarized regions are suspect.

So the change is not to build a new mechanism — it is to stop a
belt-and-braces proof from vetoing the mechanism already built. That reframing
is why this is worth doing carefully rather than quickly: the proof was added
for a reason (`rollup_correctness`, 2026-08-22), and removing its veto without
understanding which incident it was protecting against would re-open exactly the
under-count class it was built to close.

Sources: [About continuous aggregates](https://www.tigerdata.com/docs/use-timescale/latest/continuous-aggregates/about-continuous-aggregates),
[Continuous aggregate refresh, demystified](https://www.tigerdata.com/blog/continuous-aggregate-refresh-demystified),
[Understand continuous aggregates](https://www.tigerdata.com/docs/learn/continuous-aggregates)

## THE FIX IS ~80% BUILT AND DORMANT

`verify_slice_witness` (`rollup.rs:765`) already understands three witness kinds:

| witness | what it proves | status |
|---|---|---|
| `Physical(rows)` | total `num_records` across the partition is unchanged | **the only one production uses** |
| `PhysicalBelow { rows, bound }` | rows below a time bound unchanged — tolerant of growth above it | tests only |
| `Logical { rows, lo, hi }` | **deduplicated** row count over the exact slice range | tests only |

Both generalisations are implemented and unit-tested (`rollup.rs:4482-4509`), and
nothing in production stamps or verifies with them. `slice_coverage_agrees` is
hardcoded to `Physical` — its own doc says so — and passes `logical: None`, so
the logical path is fed nothing:

```rust
let source = LiveSource { files: ..., logical: None };
... verify_slice_witness(witness.map(SliceWitness::Physical), source) ...
```

**`Physical` is the wrong proof for this system, and the code says why.**
`SourceFile::rows` is "the add action's `num_records` — PHYSICAL, so it counts
tombstones and superseded merge-on-read versions", and `slice_coverage_agrees`
notes "*'Rows only accrue' is false here — dedup rewrites and vacuum shrink
`num_records` too*". So a dedup that removes only duplicates, or a consolidation
that rewrites files with identical logical content, changes the witness and
voids coverage **without changing a single answer the rollup would give**.

That is exactly what the 2,618,297 `moved` refusals are, and why an eight-day-old
partition fails.

`Logical` is immune to both: it counts the deduplicated rows over the slice's own
range, which is precisely the quantity the rollup aggregated. It is also
preferable to `PhysicalBelow`, whose straddle rule makes a packed day
`Unverifiable` — the reason its own comment calls it "a candidate and not a
recommendation".

### What remains to do

1. **Stamp** slices with a logical row count at build time instead of (or
   alongside) the physical tag. The logical-count index already exists; note the
   2026-09-02 constraint that **its key must equal the dedup key**, or the count
   it returns answers a different question.
2. **Feed** `LiveSource::logical` on the verify path — currently `None` at the
   only call site.
3. **Settle the 2026-09-04 count-pushdown undercount FIRST.** `COUNT(*)` was
   measured silently **27% low** and the pushdown was disabled in `2f08c4a6`.
   That is the same logical-count family a `Logical` witness would be stamped
   from. Establish whether the defect was in the INDEX or in the pushdown before
   stamping witnesses from it: if the index undercounts, `Logical` either never
   validates (safe but useless) or validates against a wrong count, which is the
   under-count class itself.
4. **Oracle-compare** before trusting it: run both witnesses over real prod
   slices and assert the `Logical` verdict never says Valid where the raw path
   would disagree. The 08-22 slice-fingerprint attempt "routed nothing, failing
   safe as a permanent miss"; the opposite failure — routing something it should
   not — is the under-count class, and is not safe.

This is the single highest-leverage item on the read path: the verifier is built
and tested, and the work is stamping and plumbing rather than design.
