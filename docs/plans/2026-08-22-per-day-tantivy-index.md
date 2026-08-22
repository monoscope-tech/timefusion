# One tantivy index per day — the shape where coverage converges

2026-08-22 morning. Follow-up to `2026-08-21-tantivy-keep-it-fix-it.md`, which
measured the divergence, and to `2026-08-22-file-level-needle-pruning.md`,
which shipped the per-file bloom sidecars this design leans on.

## The defect is the definition of "covered", not the throughput

Measured this morning (method and caveats in the sibling doc):

| quantity | value |
|---|---|
| new live parquet files/hr | ~529 |
| of which flush output, self-indexed at commit | ~370 |
| **uncovered accrual** — arriving with no index | **~160/hr** |
| **backfill drain** at `build_concurrency = 2` | **~100/hr** |
| uncovered growth | ~60/hr |
| standing backlog | ~5,700 files |

The instinct is to chase the 60/hr with throughput. That is treating a symptom,
and the decomposition says why. Flush indexes its own output at commit, so
**new rows are covered at birth**. Everything the backfill spends its life on —
that whole ~160/hr — is compaction rewriting rows that were already indexed.
Ingest adds rows; **compaction adds files without adding a single row**.
Coverage is computed as

```
uncovered = live parquet files − ∪ manifest_entry.covered_files
```

so every compaction rewrite un-covers rows that were already indexed, and the
backfill re-indexes bytes it has already read to produce an index that says
what the old one said. The work is manufactured by the bookkeeping. **No
throughput setting converges against a producer that is fed by the drain's own
neighbour.**

## What makes a per-day index different

Key the index by `(project_id, date)` instead of by parquet file, and make its
output what the read path actually consumes: **`id` terms**, not row addresses.
Then a compaction rewriting A+B+C into D changes nothing the index knows —
the same rows carry the same ids, and the index's answer ("these ids match
`message LIKE '%timeout%'`") stays true. Coverage becomes a property of a
**date**, not of a file list, and it is invalidated only by rows the index has
never seen, i.e. by genuinely new data.

That is the whole convergence argument. Uncovered accrual drops from ~160
files/hr to roughly one unit per active (project, date) per seal — because it
stops counting rewrites as new information, and rewrites are ~all of it.

### Division of labour with the blooms

Losing per-file granularity sounds like losing file pruning. It isn't, because
the file-level mechanism already shipped and already converged:

- **tantivy answers "which ids match this text predicate"** — the only
  mechanism that can serve `LIKE`, regex and substring at all.
- **bloom sidecars answer "which files could hold these ids"** — per-file,
  per-date, metadata-only, and demonstrably keeping up (its reconcile went
  cap-bound → steady state at ~40 files per 5-minute pass, and it is the
  counter this morning's accrual number came from).

Feeding tantivy's id set into the existing bloom prune is strictly better than
what per-file indexes do today: it prunes against the CURRENT file set, including
files written after any index was built. The 08-22 result — 284 files → 24 on a
needle lookup — was produced by exactly that path.

### Why the manifest already supports it

No new format is required:

- `ManifestEntry.covered_files` is already a `Vec<String>`; flush-built entries
  already cover several files.
- `ordinals_valid` is already computed as
  `e.ordinals_valid && e.covered_files.len() == 1` (`search.rs:218`), so a
  multi-file entry already turns row-ordinal selection off and degrades to
  id-plus-file-coverage on its own.
- Hits already carry the `_id` term (`search.rs:902`).

The read path therefore keeps working the day a multi-file entry appears; the
change is in what gets built and how coverage is judged.

## Design

**Blob and key.** Manifest key `day-{date}`, blob at
`{INDEX_PREFIX}/{table}/{VERSION}/project_id={pid}/date={date}/day.tar.zst`.
`file_uuid()` (the cache-dir namer) must accept that key alongside `bucket-`.

**Two tiers, mirroring sealed vs hot everywhere else in this codebase.**

- **Sealed day index** — built once per (project, date) once the date stops
  changing, over every file then live.
- **Hot tail** — the current date, and any date still being compacted, keeps
  today's cheap per-file indexes until the next seal folds them in. Bounded by
  construction: at most one or two dates per project have a tail.

**Coverage.** A date is covered when a sealed entry exists whose
`max_timestamp_micros` reaches the end of that date and whose `built_at` is
after the last row landed for it. A compaction rewrite does NOT un-cover it.
Late-arriving rows for a sealed date re-open it — that is the only invalidation
that should exist, and it is proportional to actual new information.

**GC.** Today an entry dies when its covered files die. A day entry must not:
partial file death is the normal case. It dies when its date leaves retention,
or when a reseal replaces it.

**Correctness direction.** The index is a candidate generator. Stale entries
produce false positives (an id whose row moved files), never false negatives —
the row is still there under the same id, and the bloom/scan stage filters. A
day entry that misses rows written after it was built is the only unsound case,
which is exactly what the hot tail and the reseal-on-late-arrival rule cover.

**Read fan-out, for free.** `indexes_per_query = 5`, `index_opens = 1478`,
`blob_fetch_us_avg = 3.5s`. A 30-day query over one project goes from hundreds
of tiny index opens to 30. That also makes "always local" affordable, which is
the second half of the ask: `warm_recent` currently warms every blob within
`prefetch_days`; at one blob per project-day, warming the entire 30-day window
for every project is a few hundred objects, and `reader_cache_entries = 2048`
comfortably holds all of it.

## Step 0 — carry the index forward across a compaction (cheapest converging change)

Before building anything per-day, there is a change that gets the convergence
property with no new index format at all, and it falls out of the same
observation: a compaction output holds exactly the rows of its inputs, under
the same ids.

**So don't re-index it — extend the existing entries to cover it.** When a
compaction rewrites A+B+C into D, add D to `covered_files` of the entries that
covered A, B and C. D is then covered, and nothing reads it back.

Note what this replaces, because it is not "add coverage that is missing":
**both rewrite paths already reindex their own output inline**, each by reading
the new file back from S3 and building a fresh index —
`compact.rs:206` (post-optimize reindex) and `maintain.rs:4953`
(`reindex_wave_outputs`), both at `build_concurrency`. Carry-forward turns that
read-back-and-rebuild into a manifest read-modify-write. It is the same
coverage for a tiny fraction of the IO, and it frees the build slots those two
paths currently share with the backfill.

That also sharpens the open question about the residual ~60/hr: if every
rewrite path covers its own output, the uncovered growth must come from those
inline reindexes **failing** or from a path with no reindex at all (footer
repair, vacuum, multi-file flush commits whose build errored).
`tantivy_wave_reindex_failed` and the post-optimize `errors=` counter are where
to look, and that is a cheap thing to check before building anything.

The read path needs no change, because every case is already handled:

- `ordinals_valid` is `e.ordinals_valid && covered_files.len() == 1`
  (`search.rs:218`), so a multi-covered entry stops claiming row ordinals by
  itself.
- Double coverage is already resolved conservatively:
  `zero_hit_files.retain(|f| !unprunable_files.contains(f))` (`search.rs:318`),
  so D is pruned only when EVERY entry covering it saw zero hits — which is
  exactly the right rule, since D's rows are the union of those entries' rows.

**The one guard that matters:** every input file must already be covered. If
any of A, B, C had no index, D holds rows no index has seen and marking it
covered would cause a false negative — the one failure mode that is not
tolerable. So: extend only when the whole input set is covered, otherwise leave
D uncovered and let the backfill build it as today.

Cost: a manifest read-modify-write per compaction commit, no S3 read-back, no
index build. Compare against ~160 files/hr of read-back-and-rebuild today.

The trade-off, and why per-day still follows: entries accumulate
`covered_files` and get coarser over time — pruning gets weaker with each
generation of compaction, and nothing shrinks the number of index blobs or the
read-side fan-out. Step 0 stops the divergence; the per-day design below is
what makes the steady state good.

## Sequencing

0. **Carry-forward on compaction** (above) — converges without a new format.
1. **Raise `build_concurrency`** (`TIMEFUSION_TANTIVY_BUILD_CONCURRENCY`) to
   buy headroom over the ~160/hr while the rest is built. Sized honestly: 4
   gives ~200/hr, a ~40/hr surplus and a **~140h** drain of the 5,700 backlog;
   clearing it inside a day needs ~8. Reversible by the same var, but 8
   concurrent parquet decodes is a real memory question on a box that OOMs —
   watch peak anon RSS and `oversized_skipped`. This is headroom, not the fix.
2. **Day-index builder + manifest key**, behind
   `timefusion_tantivy_day_index`, default off. Prove it on staging: a day
   index plus the bloom prune must return the same rows as per-file indexes for
   `LIKE`, regex and equality shapes.
3. **Date-level coverage**, so the census stops counting rewrites. This is the
   change that converges; steps 1-2 are what make it safe to make.
4. **Seal at compaction time** — a compaction that rewrites a date partition is
   already holding those rows in memory, so it is the cheapest possible moment
   to build that date's index, and it removes the S3 read-back entirely.

## The measurement that decides step 1 alone is not enough

The flush-vs-backfill split above was derived from `flush_completed_total`
assuming ~1 file per flush commit. That is the load-bearing assumption, and it
is worth one direct check before step 3: log the added-file count on the flush
commit path, or count `tantivy_backfill_pass built=` against the census over a
pass that actually completes. If flush commits routinely add several files, the
uncovered accrual is larger than ~160/hr and step 1 needs to be more aggressive
— but the shape of the fix does not change, because those files are still
covered at birth.

## Not addressed here

`search_us_avg` has regressed 0.34ms → 2.1ms and `reader_hit_pct` 96.9% →
85.2% since the 08-21 increment-2 measurement, with 334 blob fetches averaging
**3.5 seconds** each. Two hypotheses, both untested, and they are not
alternatives — they can both be true:

- **Working set > cache.** `reader_cache_entries = 2048` open readers against
  `indexes_searched_total = 15302` over 2895 queries. With one index per
  parquet file the live working set is far larger than the cache, so the LRU
  churns. This one *is* granularity: at one index per project-day, a month for
  every project fits in 2048 entries with room left, and `warm_recent` could
  warm the entire retention window instead of `prefetch_days = 3`.
- **IO contention.** The backfill pass never completes, so it is continuously
  reading parquet back and uploading blobs over the same OVH link a query's
  blob fetch uses. 3.5s for a download+unpack that used to be free is what a
  saturated link looks like. Disk is not the constraint — the host has 714G
  free against a 200G budget.

The first is fixed by this design. The second is fixed by not having a
permanently-running backfill, which is also what this design delivers.

## 2026-08-22 11:29 — the curve did NOT bend, and the census breakdown says why

Two clean points, one container (`bfa454a`, up 38 min, no restart between them),
using the per-age breakdown that landed with the sibling session's commit:

| time | total | today | week | older |
|---|---|---|---|---|
| 11:13:35 | 5768 | 586 | 1722 | 3460 |
| 11:29:54 | 5806 | **624** | 1723 | 3459 |

**+38 in 16 minutes (~140/hr), and every single one of them is `today`.**
`week` moved +1, `older` moved −1 — frozen to within a file.

Two conclusions, and the second one supersedes a lot of this document.

**1. The GC leak fix did not bend the curve.** Not disproven as a real defect —
it was, and the fix stands — but its collateral falls on partitions that are
being compacted, i.e. `week`/`older`, and those were already static. Note the
measurement is confounded: `bfa454a` (backfill manifest-write batching) landed
in the same restart, so this window measures both changes. It is thin evidence
(one 16-minute interval on a 38-minute-old container) and should be re-taken
after two quiet hours. But there is no sign of convergence in it.

**2. The backlog is not accruing — it is starving.** `week + older` = 5,182
files, flat. All churn is in `today`, which is exactly where flush and hot-tail
compaction are constantly writing and rewriting. So the divergence measured all
night was never a race between accrual and drain across the corpus; it was one
hot partition churning while 5,182 files sat untouched.

And that starvation is **structural, not a tuning problem**:

- `sort_backfill_uris_newest_first` (`mod.rs:2344`) sorts each project's queue
  reverse-lexically on `date=YYYY-MM-DD`, i.e. newest first.
- a pass is capped at `timefusion_tantivy_backfill_max_files_per_pass` = 150.
- `today` alone holds **624** uncovered files.

624 > 150, so **every pass is consumed by today's files before it can reach
yesterday, let alone `older`**. The older 5,182 can never be reached while
today's uncovered count exceeds one pass — and today's count is replenished
continuously by the churn. Raising the cap or the concurrency does not fix this;
it just chases churn faster. This is the same pathology as
`tf_horizon_frozen_by_newest_first_2026-08-17`, rediscovered in a new subsystem.

**What this changes.** Convergence needs the ordering fixed before any
throughput lever is worth pulling:

- **Reserve a share of each pass for the tail** (e.g. 2/3 newest, 1/3
  oldest-uncovered), so the backlog drains at a bounded rate no matter how
  hot today is. This is the smallest change that makes the backlog finite.
- **Stop indexing today's churn per-file at all.** Today's files are rewritten
  within hours; indexing each one is work with a half-life. This is precisely
  what the per-day design's *hot tail* is for — and it now looks like the main
  event rather than a detail: seal a date once it stops changing, and let the
  hot partition be served by the flush-built indexes it already has.

The step-0 carry-forward remains correct and cheap, but it addresses
`week`/`older` churn that this measurement says is already near zero. **Fix the
ordering first.**

## 2026-08-22 13:54 — tail reservation shipped; its verification is blocked

`timefusion_tantivy_backfill_tail_share_pct` (default 33) is live. First
evidence that it does what it was built to do — the tail moved for the first
time in any sample taken today:

| time | total | today | week | older |
|---|---|---|---|---|
| 12:59 | 5868 | 686 | 1723 | 3459 |
| 13:13 | 5920 | 741 | 1723 | **3456** |

`older` −3, having been frozen to within one file across every earlier sample.
But `today` climbed ~220/hr against ~140/hr before, which is the expected cost
of handing 50 of 150 slots to the tail. **Whether that trade is net-positive is
NOT established**, and could not be established today: prod restarted at 12:5x,
13:13, 13:40 and 13:53 — roughly every 13-30 minutes — while a backfill pass
takes over an hour. No pass completed inside any container's life, so there is
no clean window to measure. (This is the same deploy-cadence pathology recorded
for maintenance units: restarts every ~13 min against a 15 min unit deadline.)

Checked at 14:09 rather than assumed: every one of those restarts carried a
DISTINCT image SHA (`7c73d19`, `c59f9eb`, `761779d`, `8f29584`, `1b5d375`)
with an empty `Error` field, and there is no OOM or SIGKILL in the logs — the
only `oom` matches are inside the word `bloom`. So this is a deploy train, not
instability, and the fix is to stop deploying for two hours, not to touch the
service. Re-confirmed the same hour: the tail share stays at 33 because
lowering it would need another code-default flip and therefore another
restart — spending the very thing the measurement is short of.

**Left ON deliberately, on an argument rather than a measurement.** The backlog
was provably unreachable before — `week`/`older` frozen within one file across
three consecutive samples while `today` climbed — so any reservation converts an
infinite backlog into a finite one. The cost is bounded and reversible: it is
carved out of the cap, so pass cost is unchanged, and `=0` restores the previous
behaviour exactly. If a quiet window later shows `today` diverging faster than
the tail drains, lower it rather than reverting the concept.

**What the numbers actually argue for.** ~220/hr of accrual in `today` against a
150-file pass says per-file indexing of the hot partition can never keep up, no
matter how the pass is divided. That is the hot-tail half of this document, and
it is now the load-bearing change — not an optimisation.

### 14:13 — retract the "~220/hr" figure

A third point lands and the headline number from this section does not survive:

| time | total | today | week | older |
|---|---|---|---|---|
| 12:59 | 5868 | 686 | 1723 | 3459 |
| 13:13 | 5920 | 741 | 1723 | 3456 |
| 14:13 | 5954 | 775 | 1723 | **3456** |

`13:13 -> 14:13` is **+34 over a full hour**, against the "~220/hr" derived
above from the 14-minute 12:59->13:13 step. **A 14-minute interval is not a
rate** — the same error this document twice catches other people making (the
cap-bound `built=512`, the rate averaged across a restart), committed here by
me. Neither figure is trustworthy: the hour-long span crosses the 13:40 and
13:53 restarts, so it is contaminated in the other direction.

**The honest state: there is still no reliable accrual rate for `today`.** What
survives is the composition finding, which does not depend on a rate — `week`
has not moved off 1723 in any sample today, and `older` has moved exactly once
(-3, after the reservation shipped). The tail is still effectively frozen.

This weakens, but does not overturn, the argument for the hot tail: it rested on
"~220/hr against a 150-file pass", and that premise is now retracted. The
argument that stands on measurement alone is the narrower one — newest-first
ordering starves the tail, which three frozen samples establish without needing
any rate at all. Anyone reaching for the hot-tail change should re-measure
`today`'s accrual in a quiet window first, not cite the retracted number.

### 14:52 — `today` accrual is ~33/hr, and the reservation is not delivering

| time | total | today | week | older |
|---|---|---|---|---|
| 13:13 | 5920 | 741 | 1723 | 3456 |
| 14:13 | 5954 | 775 | 1723 | 3456 |
| 14:52 | 5975 | 796 | 1723 | 3456 |

Two spans that do not share an interval now agree: +34/hr and +32/hr for
`today`. Both still cross restarts, so treat it as an order of magnitude rather
than a figure — but **~33/hr, not ~220/hr**, and the retraction above was right
to be made.

That reframes the problem a third time. A 150-file pass is not remotely
outmatched by 33 files/hr of accrual; it should cover `today` and still have
~120 files of headroom for the tail every hour. Yet `week` has not moved off
1723 in nearly two hours and `older` has not moved off 3456 since 13:13.

**So the binding constraint is not accrual, not ordering, and not the cap — it
is that a pass never finishes.** The tail reservation only pays out when a pass
completes, and none has: restarts have come every 15-30 minutes all afternoon.
Last night's note records the same thing without restarts to blame — a 60-minute
pass with no completion line on a container that never restarted.

**Do not ship another throughput or ordering change against this.** The next
question is a different one: why does a 150-file pass take longer than an hour
when the work is ~150 small-file index builds, and can a pass checkpoint its
progress so a restart does not discard it? A pass that completes at ANY size
beats a larger one that never does — and nothing in the cap's history
(400 -> 150, sized for observability) considered that a pass must fit inside the
mean time between restarts.

### 15:17 — the pass does not START, and the reason is a once-an-hour cron

Chased the "a pass never finishes" claim to its mechanism, and it is worse and
simpler than that. Over **six hours** of prod logs:

- `tantivy_backfill_started`: **0**
- `tantivy_backfill_progress`: **0**
- `tantivy_backfill_pass`: **0**

`tantivy_backfill_started` is announced BEFORE the first build (added in
`c4843b5`, live), so zero of them means the pass is not starting at all — not
starting and dying.

The cause is `timefusion_tantivy_reconcile_schedule = "0 20 * * * *"`: the
tantivy reconcile fires **only at minute 20 of each hour**. Prod has restarted
every 15-30 minutes all afternoon, so most containers never live through a `:20`
boundary — the one booted at 14:33 has to survive until 15:20 before it even
begins — and a container that does catch a `:20` then needs to survive an hour
of pass to finish it.

**This is the whole drain, gated on one instant per hour.** Every other
finding today sits downstream of it: the tail reservation cannot pay out, the
cap is irrelevant, and `week`/`older` frozen at 1723/3456 is not a starvation
symptom at all — it is simply nothing having run.

**Corrects the section above.** "A pass takes longer than an hour" was inferred
from last night's single 60-minute uncompleted pass; today's evidence says the
usual case is that no pass begins. Both can be true, but the START gate is the
one that explains six hours of an entirely frozen tail.

The fix is not another ordering or throughput knob. It is either a schedule that
retries (e.g. `0 */15 * * * *`, so a container that misses one boundary catches
the next) or a run-on-boot-if-overdue, so the drain is not hostage to a single
minute of the hour. Deliberately NOT shipped here: it wants one quiet deploy and
a measurement, and prod has taken seven today.
