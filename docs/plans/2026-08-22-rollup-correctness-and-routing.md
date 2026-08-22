# Rollup correctness, chart routing, and the dedup certification band

2026-08-22, follow-on from `2026-08-22-dashboard-query-profile.md`. That
document measured; this one diagnoses and plans. Investigation is complete
and recorded below; implementation follows the ordering in §5.

## 1. The P0, correctly diagnosed — it is NOT the interior/fringe split

The profile reported the routed throughput chart returning 4.43M rows where
raw returned 7.29M (P1, 3d), 18 of 73 hourly buckets absent. It attributed
this to the hybrid interior failing to hand `not_built` dates to the raw
fringe. **That attribution was wrong.** The split arithmetic is correct and
already well tested:

- `complement()` (`src/rollup.rs:554`) emits mid-window gaps, not just edges,
  and `the_rollup_intervals_and_their_complement_always_partition_the_window`
  proves ranges+gaps tile `[lo,hi)` exactly once for interior holes.
- `sql()` (`rollup.rs:848`) unions a raw leg over `complement(...)` whenever
  `fringes` is non-empty, so a mid-window gap does reach the raw table.

The real cause is that **the rollup's CONTENT is short while its coverage
claims the range.** Measured directly against the tier
(`otel_logs_and_spans_rollup_dashboard_1h_v2`) for P1:

| date | raw rows | rollup `sum(request_count)` | cells | verdict |
|---|---|---|---|---|
| 2026-08-17 | 2,915,666 | 2,915,666 | 231 | exact |
| 2026-08-18 | 2,697,137 | 2,697,137 | 236 | exact |
| 2026-08-19 | 2,467,916 | 2,467,916 | 258 | exact |
| 2026-08-20 | 2,527,257 | **1,979,375** | 239 | **short 547,882 (21.7%)** |
| 2026-08-21 | 2,319,379 | **90,109** | 59 | **short 2,229,270 (96%)** |
| 2026-08-22 | 141,434 | 22,682 | 70 | today, partial by design |

547,882 + 2,229,270 = 2,777,152, against the 2,855,431 shortfall the profile
measured over 08-19..08-22 (remainder is today's partial). **The arithmetic
closes.** A complete build is exact to the row — 08-17/18/19 match perfectly —
so the builder is right; what fails is that a date whose build is incomplete
or stale is still served as if complete.

### Why the existing guards do not catch it

Two coverage maps feed `rollup_rewrite_for` (`src/database/mod.rs:3451-3520`):

- `rollup_coverage`, keyed per DATE, is checked hard: `coverage.source_fp !=
  source_fp || coverage.source_epoch != source_epoch → StaleCoverage`, and
  again at execution in `rollup_ticket_current` which *re-derives* the
  fingerprint (`mod.rs:3615`). **This map is never inserted into anywhere in
  the tree** — only `get`, `remove` and `retain`. It is dead.
- `rollup_slice_coverage`, keyed per (project, source, target, start, end), is
  what recovery populates (`maintain.rs:2525-2551`) and is therefore the only
  live coverage. Its plan-time loop (`mod.rs:3495-3505`) checks **project,
  source, target and range overlap — and nothing else.** No fingerprint, no
  epoch. `rollup_ticket_current`'s slice arm (`mod.rs:3619`) only re-reads the
  same in-memory entry, so it detects a concurrent map change and nothing
  about the source.

So all live coverage is the unguarded kind. `invalidate_rollup_hours`
(`maintain.rs:2226`) does purge overlapping slices when TF observes a write,
which is why this is intermittent rather than constant — it fails whenever a
partition changes without that path running, or a build publishes short.

**The fingerprints are not interchangeable, which is why the date-path check
cannot simply be copied onto slices.** Build-time `source_fp` is an FNV hash
of the slice's selected source FILE LIST (`maintain.rs:1455-1458`); read-time
fingerprint is over the DATE partition's add-actions
(`partition_stats_bounded`, `mod.rs:1481`). Different quantities. Comparing
them would reject every slice and disable rollups outright.

### The fix: carry source-row evidence and check it at plan time

A complete rollup of a count-preserving measure satisfies one invariant that
needs no file-identity agreement at all:

> the source rows a slice was built from, summed over the slices covering a
> date, equals the source rows that date holds now — unless rows arrived
> since, which is exactly the staleness we must refuse.

**Both sides must be the SAME computation, or the check is worthless.** The
decoded input row count a build sees is not add-action `num_records`:
`num_records` counts tombstones and superseded merge-on-read versions, while
every read plan carries `deleted IS DISTINCT FROM true` and the builder
aggregates post-dedup rows. Those differ here — measured dup rate 0.2%, and
tombstones exist. So the recorded quantity is **the date partition's
`num_records` sum, computed exactly as `partition_stats_bounded` computes it**,
snapshotted at build time. It is a witness of "what the partition looked like",
not of "what the build ingested", and that is precisely what makes it
comparable.

1. `partition_stats_bounded` already reads `num_records` per add-action to
   build the fingerprint; `PartitionStats` (`mod.rs:1481`) just does not keep
   the sum. Adding `rows: i64` is free — same pass, same scan.
2. The slice does not yet carry it. Add `TAG_SOURCE_ROWS` alongside the five
   coverage tags at `maintain.rs:1606-1613`, and a `source_rows` field on
   `RollupCoverage`, populated in `recover_rollup_coverage` like `source_fp`.

**Semantics: all-must-agree, not sum-over-slices.** Slices of one day are
built at different times against a churning partition, so per-slice snapshots
of the *date* cannot be summed — summing them would be comparing a total
against one sample. Instead: for each date, every covered slice must carry the
same `source_rows`, and it must equal the date's current value. Any
disagreement — between slices, or against now — drops that date's slices to
the raw fringe.

*(The alternative form, recording each slice's own selected-file row sum, was
rejected: files whose timestamp range spans a slice boundary would be counted
by both slices unless selection provably partitions files, which is not
established.)*

**The check is two-sided — strict equality, not `>=`.** "Rows only accrue" is
false in this codebase: dedup rewrites and vacuum shrink `num_records` too.
Any disagreement in either direction means the partition moved under the
build, so any disagreement declines.

**Do not substitute a read-style fingerprint for the row count.** A fingerprint
folds file identity, so any bin-pack or compaction would void it and we would
recreate the Finding-5 churn dynamics — coverage racing maintenance — inside
the rollup tier. Row counts are invariant under bin-packing, which is exactly
the tolerance this needs.

Deliberate limitation, stated rather than hidden: a date whose slices cover
only PART of the day cannot be checked this way, because the uncovered hours
legitimately hold the difference. 08-21 (59 cells of ~236) is such a case —
its uncovered hours already go raw, but its covered hours could still be
stale and this check will not catch it. Closing that needs per-range source
counts, which `partition_stats_bounded` cannot give (it is per-date). **Not
in scope here; the full-day case is what the measured under-count was.**

A tag is a durable format addition, so: slices written before the tag exists
have no `source_rows`. Treat absent as "cannot verify" and **decline the
skip** — old slices fall to raw until rebuilt. That is a temporary throughput
cost with zero correctness risk, and it self-heals as the coordinator
republishes.

### Also, separately: delete `rollup_coverage`

It is dead — never inserted, so its careful fingerprint/epoch guards have
never run. Keeping it makes the read path look safer than it is; it is the
reason the profile's first diagnosis went to the wrong place. Removing it is
a pure subtraction and makes the live path's missing guard obvious. Do this
in the same change as the fix, not before, so one diff tells the whole story.

## 2. Chart routing — two syntactic blockers and one missing measure

Measured in the profile, A/B'd on prod (`ab.jsonl`):

| arm | 3d | 7d | routing |
|---|---|---|---|
| `lat` as monoscope emits it | 3,525 | 7,926 | miss: unsupported_shape |
| `lat`, `extract(epoch …)` lifted | 3,420 | 8,119 | miss: missing_measure |
| `lat`, lifted + under the `server` filter | **278** | **1,035** | HIT |
| `svc` as monoscope emits it | 3,237 | 6,826 | miss: unsupported_shape |
| `svc`, bare dimension column | **276** | **754** | HIT |

The group-expr matcher (`rollup.rs:1626-1648`) accepts a bare `Expr::Column`
naming a dimension, or a bare `time_bucket(w, timestamp)`; everything else
falls to `_ => Err(UnsupportedShape)`. monoscope emits neither form.

**2a. Unwrap `extract(epoch from time_bucket(w, ts))`.** Sound without
qualification: epoch-of-bucket is injective on the bucket, so the grouping is
identical and the conversion lifts above the aggregate unchanged.

**2b. Declare an unfiltered `duration_digest`** (`agg: tdigest, column:
duration`) on both grains in `schemas/otel_logs_and_spans.yaml`. The only
declared tdigest carries the `kind='server' OR name IN (…)` filter, while
`duration_sum/min/max/count` are all declared unfiltered — the digest is the
lone gap. Cost is one extra t-digest per cell; size it against the 1m grain's
cardinality before committing.

**2c. Unwrap `COALESCE(<dimension>, <literal>)` — gated.** COALESCE folds NULL
and the literal into one group, so grouping the rollup by the raw dimension
and coalescing above needs a re-aggregation or two groups leak where raw
produced one. The A/B arms returned 165 vs 219 groups and I did not establish
that the difference is only NULL-folding. **Requires a row-level equivalence
test before it ships**; it does not ride along with 2a.

## 3. Dedup and the sort — one lever, not two

`DedupExec::required_input_distribution` is `SinglePartition` and
`required_input_ordering` is the dedup key ordering (`read/mod.rs:481-486`).
The `SortPreservingMergeExec` that costs as much as the dedup exists only to
satisfy them, so removing the dedup for a partition removes both and lets the
scan stay parallel. That is why the pair is ~4 s of a ~7.9 s 7-day query.

The skip is already on by default and demonstrably pays (P1, full-day counts:
certified 08-12 **189 ms**/2.43M rows vs uncertified 08-13 789 ms/2.27M and
08-14 1,603 ms/2.58M — 4.2x and 8.5x). It is denied 100% of the time because
the 97 durable certifications sit on dates nobody queries: P1 has exactly
2026-08-08 and 2026-08-12.

Certification is keyed on the partition's whole file-set fingerprint and any
new file voids it, so recent partitions — rewritten continuously by ingest,
hot-tail compaction and the sealed/repair backlog — churn faster than sweeps
certify them.

**3a. Additive file-set certification.** Certify the file SET a sweep proved
clean; dedup runs over the uncertified remainder and the certified files
union above. SOUNDNESS GATE, and it is not a formality: the per-date skip was
sound because `date` derives from `timestamp` so no dedup key spans a date.
The file-level split has no such argument — merge-on-read versions of one row
can and do land in different files, so a key CAN span the split, and a naive
union would return both versions. Establishing a correct rule here (e.g. only
files whose dedup-key ranges provably do not overlap the uncertified set) is
the design work.

### The rule, established and tested — reference implementation below

> **A certified file may skip `DedupExec` iff no UNCERTIFIED file's timestamp
> span overlaps its own.**

*Proof.* The dedup key is `(timestamp, id)` (`schemas/otel_logs_and_spans.yaml`)
and merge-on-read re-appends preserve the original row's `timestamp` — the same
fact the per-date skip rests on, one level finer. So every version and tombstone
of a row carries that row's timestamp. A duplicate of a row in a certified file
therefore has a timestamp inside that file's span, and any file holding it has a
span containing that timestamp, i.e. overlapping. Excluding overlap with the
uncertified files excludes every duplicate that could be split across the two
legs. Duplicates *within* the certified set are excluded by construction — the
sweep proved that set clean together. ∎

**Per file, not per set.** Judging the certified set by its union span would let
one new file sitting among the certified ones refuse every certified file behind
it — which is exactly the churning-partition case this work exists to unblock.
The case table pins this: `old(10,20) mid(45,55) new(80,90)` against an
uncertified `(50,60)` keeps `old` and `new` and holds back only `mid`.

**Fails closed** on: a missing-statistics span on either side (an unknown span
overlaps everything — the direction `PartitionStats::overlaps` already takes);
an empty certified set (absence of evidence must never read as proved clean);
and inclusive-bound touching, because Delta min/max statistics are inclusive.

**Caller obligations the function cannot check**, and which the wiring must
honour: the scan must be Delta-only (a MemBuffer leg can hold an uncertified
newer version with no file span to compare), and `certified` must be the
sweep-proved set intersected with the LIVE file list, so a file compacted away
cannot vouch for its replacement.

### What remains, and why it is not in this deploy

Two pieces:

1. **Durable record of the clean FILE SET.** Certification stores a whole-
   partition hash (`partition_file_fp`) today; it must store the paths it proved.
   Sizing: ~200 files × 97 live certifications × ~100 B ≈ 2 MB against a 15 KB
   sidecar today, which is acceptable now and does not scale to 1,000 projects —
   so the alternative worth pricing first is a per-file Delta Add TAG, which
   makes "certified" readable straight off the live file list, needs no sidecar
   and is inherently additive. The blocker there is that Delta tags are set at
   commit, so tagging already-written files needs a metadata-only remove+add.
2. **Splitting the Delta leg by file set.** The plumbing already exists:
   `scan_delta_with_tantivy` takes file-path sets (`tantivy_covered_files`,
   `tantivy_exclude`), and the per-date skip already returns a second plan the
   caller unions ABOVE `DedupExec` (`mod.rs:8085-8105`). The file-level version
   is the same shape with a file set in place of a date set.

### The implementation, ready to drop in

Written and its case table run green before being lifted out here: an unused
`pub(crate)` function fails `cargo lint` (`-D dead-code`), and an `#[allow]` to
hold it in the tree is exactly the evasion this repo's review skills exist to
catch. It lands with the wiring, not before.

```rust
/// A file's row-timestamp span, as Delta add-action statistics report it.
///
/// `None` means the file carries no timestamp statistics. It is NOT "empty" and
/// must never be treated as disjoint from anything.
pub(crate) type FileSpan = Option<(i64, i64)>;

pub(crate) fn skippable_certified_files<'a>(
    certified: impl IntoIterator<Item = (&'a str, FileSpan)>, uncertified: &[FileSpan],
) -> HashSet<&'a str> {
    // One uncertified file without statistics has an unknown span, which
    // overlaps everything, so nothing in the scan can skip.
    if uncertified.iter().any(Option::is_none) {
        return HashSet::new();
    }
    certified
        .into_iter()
        .filter(|(_, span)| span.is_some_and(|(lo, hi)| uncertified.iter().flatten().all(|(flo, fhi)| *fhi < lo || *flo > hi)))
        .map(|(path, _)| path)
        .collect()
}
```

Its case table, all twelve green (`read::tests`):

| certified | uncertified | skippable |
|---|---|---|
| `a(10,20)` | `(30,40)` | `a` |
| `a(30,40)` | `(10,20)` | `a` |
| `a(10,20)` | — | `a` |
| `a(10,20)` | `(20,30)` | none — inclusive bounds touch |
| `a(10,20)` | `(5,10)` | none — touching low |
| `a(10,20)` | `(12,15)` | none — contained |
| `a(10,20)` | `(0,99)` | none — spanning |
| `a(10,20)` | `None` | none — unknown span overlaps everything |
| `a(None)` | `(30,40)` | none — unknown certified span |
| — | `(30,40)` | none — absence of evidence never grants |
| — | — | none |
| `old(10,20) mid(45,55) new(80,90)` | `(50,60)` | `old`, `new` — **the per-file win** |

Neither piece is shipped here. The failure mode of getting the wiring wrong is a
silent over-count on every dashboard tile, it cannot be validated on prod inside
one session, and the rule above is the part that had to be settled first. When
it is wired, ship it behind a default-off env switch and compare
`count(*)` with the switch on and off over a churning partition before making it
the default — the same discipline
`dedup_compaction_test::count_is_identical_with_and_without_the_dedup_skip`
already applies to the partition-level skip.

*Candidate rule to evaluate (not yet a verdict).* The dedup key is
`[timestamp, id]`, and per-file timestamp min/max already exists in add-action
stats. So "no key spans the split" may reduce to: **a certified file may skip
dedup iff no uncertified file's timestamp range overlaps it.** MoR re-appends
preserve the original row's timestamp (`write/mod.rs:1104` — the same fact the
per-date skip rests on), so any late version of a certified row necessarily
lands in an overlapping range and correctly pulls that certified file back
into dedup. This is computable at plan time from statistics already in hand,
with no IO — which would turn §3a from an open design task into a tractable
one. Evaluate it properly (including the no-stats and sentinel-range cases,
which must fail closed) before building on it.

**3b. Bias sweep ordering to the newest ~7 days — WITHDRAWN, already done.**
Reading `scheduling_class` (`maintenance_coordinator.rs:2160-2200`) settles
it: sealed work is *already* strictly newest-first, explicitly "for the same
reason the dedup drain and the rollup backfill are newest-first: recent days
are what dashboards read". Width outranks recency deliberately, because a
day-sized backfill unit is the only kind that advances the horizon and
newest-first alone spends every claim on yesterday's ten-minute leftovers.

So the observation that drove this item — 97 certifications sitting on
2026-08-04..08-21 rather than the last week — does **not** mean the scheduler
is looking in the wrong place. It means those are the only days that *can*
certify: recent partitions churn, and a write between clean slices voids the
accumulated coverage by design (`a_write_between_clean_slices_voids_accumulated_coverage`).
Adding a second recency bias would be a no-op at best, and at worst would
starve the contiguity goal that width-over-recency exists to protect.

**This makes §3a the only real lever, not merely the better one.** The
blocker is churn voiding accumulation, which is exactly what an additive
file-set certification escapes.

**3c. Today's partition keeps full dedup.** Correct end state, not a gap.

## 4. What this supersedes

`2026-08-22-dashboard-query-profile.md` Finding 2's mechanism paragraph
("the hybrid interior neither covers not-built dates nor hands them to the
raw fringe") is **wrong** and is corrected by §1 here. The measurement in that
finding — 4.43M vs 7.29M, 18 buckets absent — stands; only the attribution
changes. Finding 2's `not_built` probe result also stands, but it showed the
window declining ENTIRELY (correct behaviour), not the hybrid path; the
under-count came from slices whose content was stale, not from dates that
declined.

## 5. Ordering

1. **§1 — slice-coverage source-row guard + delete dead `rollup_coverage`.**
   Correctness. Ships alone. Failing test first: a slice whose source gained
   rows after the build must not be served.

   **The test must inject the staleness out-of-band.** Writing rows through the
   normal path calls `invalidate_rollup_hours`, which purges exactly the slices
   under test — the leak cannot be reproduced that way. Append to the source
   Delta table directly, or re-insert the slice-coverage entry after the write,
   to recreate the leaked state.

   **The `rollup_coverage` deletion cascades**: `ticket.dates`, the date arm of
   `rollup_ticket_current` (`mod.rs:3603-3617`) and `rollup_source_fingerprint`'s
   only caller all go with it. The diff grows; keep it in the one commit so a
   single change tells the whole story.

   **Deploy expectation, stated up front: the tier goes dark until rebuilds
   attach the tag.** Slices written before `TAG_SOURCE_ROWS` exists cannot be
   verified and therefore decline, so `thrpt` — the one currently-fast chart —
   regresses to raw (~6-10 s at 3d on today's measurements) until the
   coordinator republishes. That is the correct trade (a slow right answer over
   a fast wrong one), but check the republish cadence before pushing so the
   duration of "dark" is known rather than discovered.
2. **§2a + §2b — extract-epoch unwrap and the unfiltered digest.** Only after
   1, because both increase the share of traffic the tier answers.
3. **§2c — COALESCE unwrap**, after its equivalence test.
4. **§3b — WITHDRAWN**: already implemented (sealed work is newest-first).
5. **§3a — additive certification.** Design first, and only after the
   soundness rule is established.

## 6. Post-deploy state (2026-08-22 12:40 UTC, 7c73d19 live)

**The fix is verified on prod.** The routed shape and its unroutable control now
return byte-identical totals — 73 buckets, 7,293,464 rows each — where the same
pair returned 4.43M against 7.29M before. Both currently decline as
`not_built`: the tier is dark, exactly as §5 predicted, because no slice yet
carries `TAG_SOURCE_ROWS` and the added digest measure changed every
`generation_id`.

**BLOCKING FOLLOW-UP, and it is NOT this change: the maintenance coordinator is
stalled, and has been since ~09:30 UTC — about three hours before this deploy
landed at 12:26.**

Evidence, from the host:

| file | last written | meaning |
|---|---|---|
| `.timefusion_meta/maintenance_tasks.json` | **09:30** | the journal is checkpointed on every claim/complete — nothing has been claimed for 3h |
| `.timefusion_meta/rollup_invalidations.json` | 12:39 | the write path is alive and still invalidating |
| `.timefusion_meta/dedup_dirty_bins.json` | 12:36 | enqueueing works |

and from `timefusion_stats`: `tasks_running = 0` against `tasks_pending =
12,684`, with **every** work counter at zero — not just rollup, but dedup,
repair and packing too (`rollup_commit_actions_total`, `rollup_output_rows_total`,
`rollup_scan_duration_ms_total` all 0 after 25 minutes of uptime). Meanwhile
`dirty_bin_queue_depth = 16,791`, `pending_dedup = 5,638`, `pending_repair = 390`
and `oldest_task_age_seconds = 523,750` (6 days).

Work is being ENQUEUED and never CLAIMED. That is a coordinator-wide stall, it
touches none of the code this change modifies, and it is dated three hours
before the deploy.

**Why it matters here anyway:** the tier can only come back when the
coordinator rebuilds it. Until the stall is fixed, the dark period is
indefinite rather than temporary, so dashboards stay on the raw path
(~6-10 s at 3d) instead of recovering over the next few hours. The correctness
fix is still the right trade — a slow right answer beats a fast wrong one — but
the recovery half of the plan is blocked on this.

**Do not "fix" it by reverting this change.** Reverting restores the 39%
under-count and does not start the coordinator.
