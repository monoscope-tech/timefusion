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

### 17:52 — the interleaved reservation moves the tail, barely

First pass to run with the tail interleaved rather than appended (`5062a7d`,
deployed 16:51; pass started 17:20:08, container killed ~17:32 — 12 minutes of
run):

| time | total | today | week | older |
|---|---|---|---|---|
| 17:16 | 6085 | 912 | 1719 | 3454 |
| 17:31 | 6108 | 937 | **1718** | **3453** |
| 17:52 | 6116 | 945 | 1718 | 3453 |

`week` -1 and `older` -1 inside the pass window, against **zero movement in 33
minutes** of the appended version. Directionally exactly what interleaving
predicts, and it is the first time all day the tail has moved while a pass was
observably running.

**Do not oversell it: that is two files.** The comparison is one 12-minute
window against one 33-minute window, both on containers that were killed, and
the census is a live diff sampled every 15 minutes — a ±1 change is close to
its resolution. The honest claim is "consistent with the fix working", not
"the fix is verified".

**And two files per pass does not converge anything.** Against 5,171 files in
`week`+`older` it is arithmetic on the order of weeks, and `today` is still
accruing (885 -> 945 over 50 minutes, ~72/hr — a third different figure for
that rate today, which is itself a reason to distrust all of them). No progress
line appeared in 12 minutes, so fewer than 25 files were built: the ~100/hr
build rate is unchanged and remains the ceiling on everything.

**Where that leaves the three fixes shipped today.** The ordering defect was
real, the append bug in the fix was real, and both are now corrected — but the
sequence of constraints is: the pass must START (cron at `:20`, missed by most
containers), it must SURVIVE (>1h pass against 15-30 min restarts), and only
then does ordering matter. Today's work fixed the third while the first two
remain. That is the right order to fix them in only if the first two are
addressed next; fixing ordering alone changes nothing measurable.

## 2026-08-23 — the drain had never run; four changes, and what each must show

`tantivy_backfill_built = 30` on a container up **7 hours** (~4 builds/hr), with
**1** `tantivy_backfill_started`, **0** `tantivy_backfill_pass` completions and
**19** ticks dropped as "run still in progress". One pass began at boot, was
still going 7 hours later, and blocked every tick behind it. At 150 files and
~4/hr that pass was a **35-hour** job.

Shipped together, each with its own observable so the batch attributes:

| change | from → to | must show |
|---|---|---|
| reconcile schedule | `0 20 * * * *` → `0 */15 * * * *` | `tantivy_backfill_started` within 15 min of ANY boot (was 0-in-6h) |
| pass cap | 150 → 8 | `tantivy_backfill_pass` end-lines firing — never once seen on this box |
| hot-tail skip | new, default ON | `skipped_today` ≈ today's uncovered; `planned=` is backlog-only |
| carry-forward | new | `tantivy_carried_forward` > 0, and `built=` per optimize falling |

**The verdict on the tail reservation (§4) comes from this, not before it.** It
has still never executed a full pass; with cap 8 a pass completes in ~2h, and
`week`+`older` declining ACROSS container generations is the signal. If they
still do not move once passes complete, the reservation is not the lever.

**Two things this does NOT fix, stated so they are not silently assumed.**

1. **~4 builds/hr is the real ceiling** and nobody has attributed it. 15 minutes
   to index one ~1MB parquet is pathological; at that rate the 5,171-file
   backlog is ~two months even with everything above working perfectly. This is
   now the top item — the cap, the ordering and the schedule were all
   downstream of a drain that could not run, and this is what remains once it can.
2. **Today's coverage regresses.** `light_optimize_tail` owns today's partition
   and has no tantivy hook, so a hot-tail merge drops its inputs' coverage and
   the output stays uncovered until the date rolls over. Correctness is fine
   (uncovered → raw leg with the original filters); the hot window loses its
   prefilter for up to a day. The fix is to give `light_optimize_tail` the
   carry-forward hook `optimize_table` now has — then today is cheap to cover
   and `timefusion_tantivy_backfill_skip_today` can go back to false.

### Verdict on the four pre-registered observables (2026-08-23 10:29)

Live `bceea08` then `9c704e2`. Judged against the table above, not against what
would have been nice.

| observable | verdict |
|---|---|
| `tantivy_backfill_started` within 15 min of boot | **MET.** 08:45, and 09:00 on a container booted 08:49 — 11 minutes. Baseline was ZERO starts in six hours. |
| `cap=8`, `skipped_today` reported | **MET.** `planned=8 cap=8 skipped_today=1` in the live log. |
| first-ever `tantivy_backfill_pass` completion | **NOT MET.** 2 starts, 0 completions in the last 100 minutes. |
| `tantivy_carried_forward > 0` | **NOT MET.** 0. Needs an `optimize_table` whose inputs are all covered AND which is the sole commit in its window. |

**Why the last two are unmet is not the code.** A cap-8 pass is ~2 hours at the
measured rate and no container has lived that long: prod took another restart at
10:19 and the in-process counters reset with it. The start gate is exactly what
was broken and it is exactly what is now fixed; the completion evidence needs a
quiet window that today has not contained.

**§4 tail-share: still NO VERDICT, deliberately.** `week+older` went 5673 → 5668
across 70 minutes (-5) — the right direction, inside the noise floor of a
15-minute live diff. The pre-registered condition was decline ACROSS container
generations once passes complete. Passes have not completed. Recording -5 as
success would be fitting the answer to the data available.

**Two corrections to earlier claims in this document.**

1. `TANTIVY_CARRIED_FORWARD` was incremented but never surfaced in
   `timefusion_stats`, so observable (d) was unreadable when it was
   pre-registered. Fixed in `bceea08`. A counter nobody can read is not
   instrumentation.
2. Pass survivability was called "already implemented, no work needed" from a
   code read. Live counters then showed `tantivy_backfill_built = 2` with
   `tantivy_manifest_commits = 0` — the exact signature the sibling's own commit
   message describes as the durability bug they had fixed. The benign
   explanation (builds complete in PAIRS under `buffer_unordered(2)`, so both
   sweeps see an age under the 60s bound and the flush lands on the next pair)
   fits and is testable, but it is a hypothesis, not the verification the
   original claim implied. **Unresolved**: restarts reset the counters faster
   than the next pair arrives.

**The ceiling nobody has attributed: ~4-6 builds/hr.** Two independent readings
today — 30 builds in 7 hours, and 2 in 19 minutes. That is ~15 minutes to index
a ~1 MB parquet, which is pathological and is now the top item. Every fix in
this batch was downstream of a drain that could not run; this is what remains
once it can, and at this rate the 5,668-file backlog is months regardless of
schedule, cap or ordering. **Do not tune the drain further before attributing
the build cost.**

### 2026-08-23 11:20 — the "pathological build cost" was my own bad premise

I named `~4-6 builds/hr` the top item on the grounds that ~15 minutes to index
"a ~1 MB parquet" is pathological. **The ~1 MB premise is wrong**, and two
things in the tree say so:

- `timefusion_tantivy_backfill_max_file_mb = 4096`. The backfill accepts files
  up to **4 GB**. Nothing is being skipped for size either — the census reports
  `oversized=0`.
- The recurring `tantivy build produced 40-65 segments (> 32); merging inline`
  warnings. `build_stream_to_dir` uses `NoMergePolicy` and commits once, so
  segments are not per-commit — tantivy serializes one each time the writer's
  `WRITER_HEAP_BYTES` (64 MB) arena fills. **40-65 segments implies ~2.5-4 GB of
  indexed content per build.**

At that size ~15 minutes is unremarkable: roughly 4 MB/s of decode plus inverted
index construction. There is no mystery ceiling to attribute — the builds are
slow because the inputs are huge, and `~4-6 builds/hr` is a rate over
multi-gigabyte units, not over 1 MB ones. **Retract the "attribute the build
cost" item as posed.**

**What this changes, and it is not small:**

1. **The unit is wrong, not the speed.** Indexing a 4 GB file as ONE unit means
   a single build can consume a whole pass, and it produces an index that a
   later compaction of that file discards wholesale. That is the argument for
   per-day indexing restated from the cost side rather than the coverage side.
2. **The tail reservation may be self-defeating.** Oldest files are the most
   compacted, therefore the largest. Reserving a third of every pass for the
   OLDEST uncovered files reserves it for the BIGGEST ones — so the reservation
   that was meant to drain the backlog may be the slowest possible way to spend
   those slots. This is a live hypothesis against a change already shipped
   (default 33), and it is testable: log the file SIZE alongside each build.
3. **`max_file_mb = 4096` deserves re-examination.** Its own doc comment says
   4096 exists to bound `pack_dir`'s in-memory tar, not because 4 GB is a
   sensible indexing unit. A much lower cap would let the many small files drain
   while whales wait for a design that can handle them.

**Next measurement, replacing the retracted one:** emit `bytes=` and `rows=` on
each completed build (the data is already in `IndexBuildStats`, currently only
at `debug!`). Until the size distribution of the backlog is known, neither the
tail reservation nor the cap can be sized honestly — and I have now twice sized
something against an assumed rate rather than a measured one.

### 11:30 — builds are not slow; the backfill's FILE SELECTION is

One log window settles what two days of rate-guessing could not. Between 11:25
and 11:27 on a single container:

```
11:25:52  tantivy_wave_reindex_complete built=1 failed=0
11:25:54  tantivy_wave_reindex_complete built=1 failed=0
   ... 8 of these in ~2 minutes ...
11:27:22  WARN tantivy build produced 69 segments (> 32); merging inline
```

**The wave-reindex path did ~8 builds in 2 minutes — on the order of 240/hr —
against the backfill's measured 4-6/hr.** Same `build_index_for_file` primitive,
same object store, same process. The only difference is which files each one
picks:

- **wave reindex** builds the file a compaction just wrote: small, fresh.
- **backfill**, ordered newest-first *within* an oldest-reserved queue, reaches
  for the most-compacted files, which are the largest. The 69-segment warning in
  the same window is one of those — at a 64 MB writer arena, ~4 GB of content.

So `~4-6 builds/hr` was never a property of the indexing code. It is the average
cost of the *particular* files the backfill selects. There is no ceiling to
attribute and no build path to optimise.

**This is evidence against a change already shipped.** The tail reservation
(`timefusion_tantivy_backfill_tail_share_pct`, default 33) hands a third of
every pass to the OLDEST uncovered files — which this says are the LARGEST, and
therefore the slowest possible way to spend those slots. The reservation was
justified by a real defect (newest-first starves the tail) but may be the wrong
remedy for it.

**Do not just lower the knob.** The backlog IS the old files; draining it
necessarily means indexing them, so a smaller reservation only defers the
problem. What the evidence actually argues is that multi-GB files need a
DIFFERENT MECHANISM, not a scheduling share — either a size cap that lets the
many small files drain while whales wait (`max_file_mb` is 4096 today, and its
own comment says that number exists to bound `pack_dir`'s tar, not because 4 GB
is a sensible unit), or the per-day index, which changes the unit outright.

**Confirming measurement is already shipped** (`e6315af`): every build now logs
`rows`, `index_bytes`, `segments`. One pass's worth of those lines gives the
size distribution directly, and turns this from a well-supported hypothesis into
a number. Do not re-tune the reservation or the cap before reading it.

### 11:52 — the size distribution, measured at last

One backfill pass (`planned=8 cap=8 skipped_today=3`, started 11:45:02) with the
new `tantivy_index_built` logging. Its eight builds:

| rows | index_bytes |
|---|---|
| 45 | 73 KB |
| 1 | 8 KB |
| 14 | 24 KB |
| 10,833 | 9 MB |
| 19,445 | 12 MB |
| 25,333 | 38 MB |
| 61,536 | 35 MB |
| **1,290,298** | **718 MB** |

**One file is ~50x the next largest and ~90,000x the smallest.** That single
build took ~7 of the pass's ~7 minutes. This is what "~4-6 builds/hr" was
averaging over — not a slow indexing path, and not uniformly large inputs, but a
distribution with a very long tail where one whale IS the pass.

Three things follow, in confidence order.

1. **Both earlier framings were wrong.** "~15 min/build is pathological" assumed
   ~1 MB inputs (retracted above). "The backfill selects huge files" assumed
   uniformly large ones — also wrong: six of eight builds here were under 40 MB
   and finished in seconds. The truth is a skew, and neither a rate nor an
   average describes it. **Stop quoting a mean for this population.**

2. **The pass did 8 builds in ~7 minutes** — ~68 files/hr, an order of magnitude
   above the 4-6/hr measured on the previous container. Tempting to credit the
   cap change (150 -> 8, which re-derives the work list every pass instead of
   grinding a 35-hour list). But the file MIX differs too, and one pass is one
   sample. **Do not claim the cap improved throughput until several completed
   passes show it.** The `built=` on the completion line is the number to use.

3. **The whale is the design problem, and it is bounded.** `max_file_mb = 4096`
   permits a 4 GB parquet as ONE indexing unit; here a 718 MB index came from
   1.29M rows. A unit that large is also the unit a later compaction discards
   whole. Options, cheapest first: lower `max_file_mb` so whales defer and the
   many small files drain (costs those files their prefilter, same trade as
   `skip_today`); or shard the build by row-group/day so a whale is many small
   units — which is the per-day design arriving from the cost side for the
   second time.

**What is still not shown:** no `tantivy_backfill_pass` completion line has ever
been emitted on this box, so `built=`, `deferred_to_next_pass` and the
end-of-pass gauges remain unverified, and the §4 tail-share verdict still has no
data. The pass above is the closest any has come.

### 12:50 — "a completed pass" was the wrong success criterion

Six restarts in the hour (12:06, 12:22, 12:35, 12:36, 12:49 …), each killing a
pass 2-4 builds in. Two of the day's were mine; the rest are a sibling session's.
No pass has completed and, on a box deploying this often, none will.

**That does not matter any more, and holding to it was my error.** I chose
"first `tantivy_backfill_pass` completion" as the headline observable when the
manifest flush was driven by build completions — in that world an unfinished
pass banked nothing, so completion really was the thing to wait for. With the
timer-driven flush (`cefcdc3`) each build is committed within 30 seconds of
finishing, so **progress accrues per BUILD, not per pass.** A pass killed after
three builds now keeps three builds.

**Revised success criteria**, replacing the pre-registered table's row (b):

| observable | why it is the right one |
|---|---|
| `week`+`older` declining steadily | the only thing that matters; survives restarts because the census is a live diff |
| the same `rows=` value never appearing twice | proves work is banked rather than repeated — the 759,809-row whale built at 11:57 and again at 12:22 is the failure this replaces |
| `from_reserved_tail` distribution over many builds | decides `tail_share_pct` without needing a whole pass |

All three are per-build or per-sample, so they accumulate across container
generations instead of being reset by them. That is the property the old
criterion lacked.

**Current reading**: `week`+`older` = 5,668 → 5,667 → 5,664 across the morning.
Downward, but ~-4 in 2.5 hours, which is months. Expect that to change once the
timer flush is live, because until now the expensive builds were being redone;
if it does NOT change, the drain rate is genuinely the problem and the whale
handling (`max_file_mb`, or sharding the unit) is next.

### 13:10 — the reservation had never once executed, and `older` is GROWING

**Two findings, both from the `from_reserved_tail` attribution and the census.**

**1. The reservation never ran.** Six attributed builds across three passes
(12:30, 12:45, 13:00) were `from_reserved_tail=false` — every single one. The
cause is arithmetic in my own fix: the interleave emitted `per_tail` head items
BEFORE each tail item, so at cap 8 / 33% the order was `h,h,h,t,…` and the first
reserved build was **fourth**. Prod passes die after 2-3 builds. Fixed by
emitting the tail item FIRST in each group; the unit test now asserts position
0, not merely "not last".

This is the third time this defect has reappeared one level deeper: newest-first
starved the tail; the reservation was appended, so truncation dropped it;
interleaving put it fourth, so a killed pass never reached it. **The lesson is
not about ordering — it is that on a box where work is routinely interrupted,
"eventually" is indistinguishable from "never", and every scheduling decision
has to be judged at the point of interruption rather than over a whole pass.**

**2. The old backlog is not static — it is growing.** `older` across today:

| time | older |
|---|---|
| 09:06 | 3621 |
| 12:01 | 3627 |
| 12:54 | 3650 |
| 13:08 | 3663 |

+36 in the last hour (~70/hr) while `week` sat flat at ~2036, so these are not
files ageing in — they are NEW uncovered files appearing in old partitions,
i.e. sealed-day rewrites (the dedup cron and coordinator waves). **This
contradicts "the 3460-file old backlog is static", which I have been asserting
all day and which is written into the memory notes.** A 195 MB wave-reindex
build at 12:57 with no accompanying `tantivy_backfill_unit` line is one of them
being rebuilt from scratch.

Consequence: the drain is not racing a fixed 5,600-file backlog, it is racing a
backlog that grows ~70/hr in its oldest bucket. Carry-forward is the right
mechanism for exactly this churn, but it is wired only into `optimize_table` —
the wave path still pays a full rebuild per output. **Wiring carry-forward into
`reindex_wave_outputs` is now the highest-value remaining change**, ahead of
`max_file_mb` and ahead of the per-day index.

### 13:40 — the reservation is EXONERATED; whales come from the head

I hypothesised that the oldest-first reservation was spending a third of every
pass on the largest files ("oldest means most-compacted means largest") and said
`tail_share_pct` should come down. **The attribution refutes it.** A 755 MB /
1,304,769-row build completed at 13:38:42 and its unit line, 13 seconds later,
reads `from_reserved_tail=false`. The whale came from the HEAD.

That makes sense in hindsight and I should have seen it: the head is
newest-first *within the backlog* (today is excluded by `skip_today`), and the
newest backlog files are the ones compaction most recently merged — which is
exactly what makes a file big. Age and size are correlated far more weakly than
the hypothesis assumed, and both ends of the queue can hold whales.

**So do not touch `tail_share_pct`.** It was a well-supported story, instrumented
specifically to test it, and the instrument said no. Recording that here because
I came close to shipping the change on the strength of the story alone.

**A limit of the tail-first fix, worth stating.** Tail-first fixes which build
STARTS first, not which COMPLETES. With `build_concurrency = 2`, positions 0
(tail) and 1 (head) start together; if the tail file is large it finishes last,
and nothing is banked until a build completes. No `from_reserved_tail=true` has
been observed yet for that reason. On a box restarting every few minutes, a
reservation cannot drain large files however it is ordered — which converges,
from a third independent direction, on the same conclusion:

**the unit has to stop being "one whole parquet file".** Either stop rebuilding
it (carry-forward, now on both the optimize and wave paths) or make it smaller
(`max_file_mb`, or sharding the build). Scheduling changes cannot fix work that
does not fit between interruptions.

### 13:59 — `older` stops growing within one interval of wave carry-forward

Deployed `3654960` (carry-forward on the wave path) at ~13:45.

| time | today | week | older |
|---|---|---|---|
| 13:08 | 414 | 2036 | 3663 |
| 13:28 | 428 | 2036 | 3712 |
| 13:45 | 467 | 2035 | 3742 |
| **13:59** | 473 | 2035 | **3742** |

`older` grew +79 in the 37 minutes before the deploy (~128/hr) and **+0** in the
14 minutes after. The mechanism is directly observed, not inferred:
`carried=1 rebuilding=0` fires roughly 8 times per 90 seconds — every wave
output covered by extending an existing manifest entry, zero rebuilds. That is
~320 avoided rebuilds/hr against a drain that manages ~30/hr.

**One interval is one point** — this morning's retracted "~220/hr" came from
exactly that kind of reading, so the claim here is deliberately narrow: the
prediction was registered before the data, the mechanism is observed rather than
assumed, and the next census either confirms it or does not.

**If it holds, it settles the day's central question.** The sealed backlog was
not a static pile the drain was too slow to clear, nor a throughput problem: it
was being *manufactured* at ~128/hr by rewrites that un-covered rows already
indexed, and the drain was re-indexing bytes it had read before. Every earlier
framing — pathological build cost, oldest-first starvation, cap sizing, the
tail reservation — was downstream of that, which is why none of them moved the
number and this did within fourteen minutes.

**What remains, unchanged by this:** `today` still accrues (467 -> 473), the
5,600-file standing backlog still needs draining at ~30/hr, and no
`from_reserved_tail=true` build has ever completed. Carry-forward stops the hole
getting deeper; it does not fill it.

### 14:14 — the next census: confirmed as a bend, not as flatness

| time | today | week | older |
|---|---|---|---|
| 13:45 | 467 | 2035 | 3742 |
| 13:59 | 473 | 2035 | 3742 |
| **14:14** | 481 | 2033 | **3749** |

`older` +7 in 15 minutes = **~28/hr**, not 0. So the "+0" reading at 13:59 was a
single short interval landing on a quiet stretch, exactly the failure mode the
entry above warned about — and the warning was right to be there. The correct
number is a drop from ~128/hr to ~28/hr: a ~78% reduction, not elimination.

That revision does not weaken the diagnosis, it sharpens it. Sealed accrual
(~28/hr) is now at or just below drain (~30/hr), which is the threshold that
actually matters: `older` was diverging and is now at breakeven, so the backlog
can begin to fall instead of merely growing more slowly. The residual ~28/hr is
the share of wave/dedup outputs that carry-forward legitimately cannot cover
(entries whose inputs are not all present, or whose schema_version is stale) and
is the next thing to attribute — `tantivy_wave_carried_forward` logs
`carried`/`rebuilding` per call, so the split is already readable.

`week` also ticked 2035 -> 2033, the first decline of the day in that bucket.

### 14:30 — retraction: `rebuilding=0` was a gated log, and it hid the refusals

The entry above said the residual ~28/hr was "the share of wave/dedup outputs
that carry-forward legitimately cannot cover", and that the split was "already
readable" from the `carried`/`rebuilding` fields. Both halves are wrong.

45 minutes of prod logs: 27 `tantivy_wave_carried_forward` lines, **every one of
them `carried=1 rebuilding=0`**. A mechanism that refuses some share of its
inputs does not report zero refusals 27 times out of 27. The log site explains it:

```rust
if !carried.is_empty() { info!(carried = …, rebuilding = files.len(), …) }
```

When carry-forward refuses *every* bin in a wave, `carried` is empty and the
event is not emitted at all — so an all-rebuild wave is invisible, and the only
waves that ever reach the log are ones that carried something. `rebuilding=0` is
a property of the guard, not of the system. This is the same shape as the
`rollup_tier_untagged_retired` counter and the `prefilter` LOCAL_REGISTRY read:
**the fifth measurement this session that failed in the direction of good news.**

Fixed in `4a8a154` — the event now fires whenever there was work
(`!carried.is_empty() || !files.is_empty()`). Until that is deployed, the honest
statement is: the wave path carries at ~36/hr, and *how often it refuses is
unmeasured*. The residual ~28/hr of sealed accrual is therefore unattributed —
it may be wave refusals, the optimize path (whose carry-forward is gated on
`sole_commit` and silently declines under concurrent commits), or a third
producer. No claim either way until the fixed instrument reports.

### 14:35 — the bend was process age, not carry-forward. Retracted in full.

The next census settles it, against me:

| time | older | rate since previous | process age |
|---|---|---|---|
| 13:45 | 3742 | (pre-deploy ~128/hr) | — |
| 13:59 | 3742 | **0/hr** | 6 min |
| 14:14 | 3749 | ~28/hr | 21 min |
| 14:29 | 3780 | **~124/hr** | 36 min |

`docker service ps` explains the third column. The container running now started
at **~13:53**; the image I credited the bend to (`3654960`) was live for roughly
fourteen minutes before `0927494` replaced it. Five images ran in the last hour.

So the two readings that looked like a fix — `+0` at 13:59 and `+7` at 14:14 —
were taken 6 and 21 minutes into a **fresh process**, before its maintenance
waves had spun up. No waves, no rewrites, no accrual. As the process matured,
accrual climbed 0 -> 28 -> 124/hr and landed back on the pre-deploy ~128/hr,
indistinguishable from where it started.

**Carry-forward on the wave path has not been shown to reduce sealed accrual at
all.** Every claim in the two entries above is withdrawn: the "bend", the
"breakeven against drain", and the `week` decline read the same way. What
remains true is only the mechanism-level observation that some waves do carry
(27 lines), and — after `4a8a154` — that how often they *refuse* is still
unmeasured.

This is the failure mode already written down in
`tf_prod_counters_need_a_quiet_process_2026-08-23`: **counters are immature with
process age, and a young process reads as healthy.** I read three census points
without once checking uptime, which that memory exists to prevent.

**The binding constraint is not tantivy. It is the deploy cadence.** Five
restarts in an hour — at least one of them mine, at 14:19 — against maintenance
units that average ~21 minutes means units die to process exit and no throughput
number taken inside a 15-minute window means anything. The correct next action
is therefore restraint: **stop deploying, hold ~1h quiet, then measure.** No
further code changes until a census series exists from a single process older
than the work it is measuring.

### 14:38 — a correction to the correction, and why the quiet hold is unenforceable

Two things the previous entry got wrong in the other direction.

**1. `older` is a level, not a process counter.** It is a census of files in
storage, so its *value* is valid the instant it is read, restart or no. Process
age affects only the *rate*, and it does so in one direction: a restarting
process stops running maintenance waves, so it stops producing uncovered files.
**Restarts can only make accrual look better than it is.** The observed
~124-173/hr is therefore a lower bound, not a number needing an asterisk. The
gap against a ~30/hr drain is real and larger than measured.

That does not rescue the carry-forward claim — a lower bound of ~124/hr still
sits on top of the ~128/hr it was supposed to have removed.

**2. The hold cannot be held.** Prod restarted again at ~14:33 onto `f25d3e4`,
which is not the SHA I pushed. Deploys are arriving from outside this session,
so "stop deploying and wait an hour" is not mine to enforce; waiting would just
burn the hour and produce the same fragmented series.

| time | older | span |
|---|---|---|
| 13:59 | 3742 | — |
| 14:14 | 3749 | +7 |
| 14:29 | 3780 | +31 |
| 14:38 | 3806 | +26 |

Net **+64 in 39 minutes** across two process generations. Accrual is unambiguous
and ongoing.

**So the plan changes rather than waits.** The measurement that matters no longer
needs a quiet process: it needs the refusal split from `4a8a154`, which is a
per-event ratio (`carried` vs `rebuilding` within a single wave) and is immune to
both restarts and warm-up. That is the one number that says whether carry-forward
is refusing most of its inputs. It is committed but unpushed, and pushing it is
itself a restart — which, per point 1, costs nothing I care about.

### 14:55 — the un-gated log IS live, and it exonerates the wave path

First, a false alarm of my own making: `git merge-base --is-ancestor 4a8a154
f25d3e4` said NO and I briefly concluded the fix had been dropped. It had not —
the rebase before pushing rewrote it as **`c990009`**, which *is* an ancestor,
and `git show origin/master:src/database/maintain.rs` has the un-gated form at
line 5521. Checking a pre-rebase SHA for ancestry is meaningless; check the
content.

So this is a real measurement, not the artifact:

| window | wave events | carried | rebuilding |
|---|---|---|---|
| 14:38-14:53 (process `yrod6lnw`, un-gated) | 12 | 1 each | **0 each** |

With the guard removed, every wave still carries and none rebuilds. **The wave
path is not the accrual producer.** The carry-forward wiring works; it simply
was never where the ~124/hr was coming from. That also means the earlier
retraction was right for the wrong reason — carry-forward did not fail, it was
aimed at the wrong producer.

**And the drain figure I have quoted all day is wrong by ~7x.** 62
`tantivy_index_built` events in 18 minutes = **~207/hr**, not the ~30/hr I have
been repeating (a sibling separately retracted a related drain figure in
`b91ae6d`, ~90/hr -> ~490/hr past cache warm). So the system builds indexes far
faster than uncovered files accrue — and `uncovered` still climbed 6297 -> 6360
across the same window.

That is the contradiction worth chasing next, and it reframes the whole day:
**this was never a throughput problem.** Builds outpace accrual roughly 2:1 and
coverage still falls behind, which means built indexes are not sticking —
discarded before the manifest persists, or invalidated immediately after. The
whale timer-flush symptom (a 759,809-row file built, lost, rebuilt, lost) is the
same shape and is probably the general case rather than a whale-specific one.

Open question, not a claim: are those 62 builds 62 distinct files, or a few
files rebuilt repeatedly? The `tantivy_index_built` line does not carry the
file path in a form my grep found, so this needs the field checked in code
before the next read.

### 15:05 — the 2:1 claim dissolves: 121 of 121 builds are the flush path

The entry above compared two different populations. Attributing the same 25-minute
window by thread pool settles it:

| builds by pool | count |
|---|---|
| `tokio-rt-worker` (flush self-indexing new files) | **121** |
| `maintenance-worker` (backfill / wave) | **0** |
| `tantivy_backfill_unit` events | **3** |

`tantivy_index_built` fires from the shared build fn for *every* producer, so
counting it measures flush volume, not drain. Flush covers files **at creation**
and never touches `uncovered`. So "builds outpace accrual 2:1 yet coverage falls"
was never a paradox — it was one number from the flush path set against another
from the backfill path. **"Indexes are not sticking" is withdrawn**; it was
invented to explain an artifact of my own arithmetic. (The not-sticking
hypothesis does still have a live counter-test: the monitor watches for
`rows=1304769` reappearing, and it has not.)

**The corrected numbers make the picture worse, not better:**

| flow | rate |
|---|---|
| flush self-indexing (irrelevant to the backlog) | ~290/hr |
| **backfill drain** | **~7/hr** (3 units / 25 min) |
| accrual into `older` | ~96-124/hr |

The drain is not ~30/hr. It is **~7/hr** — four times worse than the figure I
have quoted all day, against ~100/hr of accrual. The backlog diverges by more
than an order of magnitude, and with `max_files_per_pass = 8` even a perfect
15-minute reconcile cadence caps the drain at 32/hr. **The cap and the pass
cadence cannot close a 100/hr gap arithmetically, no matter how well they run.**

Also provisional, flagged rather than trusted: the 12/12 `carried/rebuilding`
exoneration of the wave path came from a process 5-20 minutes old, inside the
same warm-up window that has now burned me twice. It is not retracted, it is
*unconfirmed* — re-read the ratio once this process passes ~40-60 min.

**Next, and it needs no deploy and no quiet window:** stop inferring the producer
from rates (three rate-inferences retracted today). Take ~20 files newly
uncovered in `older` this hour and read from the Delta commit info which
operation added each — OPTIMIZE, dedup, wave, or flush. That is a direct
attribution the census series structurally cannot give.

### 15:20 — direct attribution: sealed files are not being CREATED, coverage is being LOST

Per the advisor, stopped inferring the producer from rates and read the Delta
commit log directly (prod creds from `.env.prod`, read-only). 244 commits,
spanning **165 minutes**, on `timefusion/otel_logs_and_spans`:

| operation | commits | files added to SEALED (`date != today`) partitions |
|---|---|---|
| WRITE | 134 | **4** (of 735 adds) |
| OPTIMIZE | 110 | **13** (of 110 adds) |

**17 files in 2.75 hours — about 6/hr.** Over the same period `older` grew from
3742 to 3863, roughly **+290 files/2.75h**. The producer of sealed uncovered
files is therefore *not* file creation, by a factor of ~20-50x. It cannot be:
the commits are the complete record of what enters the table.

Two loopholes checked and closed before believing it:
- The census also walks custom project tables. `s3://…/timefusion/projects/` is
  **empty** — the unified table is the whole population, so the scan is complete.
- The census buckets by **partition date, not mtime** (`mod.rs:3371`, and the
  comment says why: "a compaction rewrite of old data must count as OLD, or every
  rewrite would masquerade as fresh accrual"). So a rewrite of old data lands in
  `older` by design — but it still has to be an *add*, and there are only 17.

**So `older` climbs because live files that WERE covered stop being covered.**
That is a different defect class from everything chased today: not throughput,
not scheduling, not build cost, not accrual — *coverage destruction*. It also
explains why the drain looks hopeless at ~7/hr: the backfill is refilling a
bucket with a hole in it.

Candidate mechanisms, none yet tested, in rough order of suspicion:
1. **Manifest edits not persisting.** Carry-forward and build results mutate the
   manifest in memory and rely on a flush; prod restarts every ~15 min. Anything
   unflushed at exit reverts, and the files it covered become uncovered again.
   This would also make the 12/12 `carried=1 rebuilding=0` readingtrue *and*
   worthless — carrying perfectly into memory that is then discarded.
2. `gc_after_compaction` pruning survivors too aggressively.
3. Entries invalidated by `schema_version` mismatch or `ordinals_valid = false`
   being treated as uncovered.

**Discriminating check, next:** read the manifest objects on S3 and compare their
last-modified times against the restart history. A manifest whose persisted state
predates the coverage it should contain settles #1 immediately.

### 15:35 — candidate #1 and #3 both fail; a baseline is set for the real test

**#1 (manifests not persisting) is refused.** `index_manifests/otel_logs_and_spans/`
holds 22 manifests and the newest were written **this minute** (17:09 server
time, contemporaneous with the census). Persistence is live and continuous, so
"carry-forward mutates memory that a restart discards" does not hold.

**#3 (entries invalidated) is refused.** Parsed all 22 manifests:

```
entries=3113  usable=3112  covered_files=3294
no_index=1  error_set=0  schema_version={1: 3112, None: 1}
```

One unusable entry out of 3,113. Nothing is being invalidated by
`schema_version` or errors.

I also checked the parser, on the suspicion that an unparseable URI would fall to
`i64::MAX` and land in `older` regardless of its real age — which would have made
`older` an artifact and fit the arithmetic almost too well (735 WRITE adds to
today over the window ≈ 267/hr). It does not: `date_partition_of` reads
`date=YYYY-MM-DD` and the live paths are exactly
`project_id=<uuid>/date=2026-08-23/part-….zstd.parquet`. Hypothesis dropped
before it got written up as a finding.

So the position is an honest negative: sealed uncovered files are **not** created
(~6/hr vs ~105/hr), **not** losing coverage through invalidation, and the
manifest **is** being persisted. One of my premises is wrong and I do not yet
know which.

**Baseline recorded for the discriminating test:** covered_files = **3294** at
15:35, against uncovered = 6403. If coverage is being destroyed, a second
snapshot must show that number *fall* while `older` climbs. If it holds steady
or rises, then the live file set is growing in a way the Delta commit scan did
not see, and the error is in my attribution rather than in the system.

Either outcome eliminates a branch. That is the next read — no deploy, no quiet
window, no code change.

## 15:45 — THE CENSUS COUNTS MORE UNCOVERED FILES THAN THE TABLE HAS FILES

The premise that was wrong is the measurement itself.

```
_last_checkpoint (v498863):  {"numOfAddFiles": 4490}
census @15:20:               uncovered=6441  (today=519 week=2031 older=3891)
manifests @15:20:            covered_files=2850 distinct
```

**Uncovered alone (6,441) exceeds the total number of live files in the table
(4,490).** No ratio argument is needed — a subset cannot be larger than the set.
Adding coverage makes it worse: 6441 + 2850 = 9,291 files enumerated against
4,490 that exist, a factor of **2.07**.

This is the doubling defect already on record as
`tf_incremental_snapshot_checkpoint_dup_2026-08-02` — the incremental Delta
snapshot duplicating its file list at checkpoint boundaries. The census walks
that snapshot, so it inherits it.

**It explains every stubborn observation of the day, including the ones I
misread:**

- Growth tracks **process age** (0 -> 28 -> 124/hr after each restart). That was
  never waves spinning up, and never carry-forward working then failing — it is
  the duplicate list re-accumulating as the process crosses checkpoints.
- `older` grows ~130/hr while the Delta log adds **28 sealed files in 6.1 hours**
  (~4.6/hr, max 2 per commit, not bursty — checked precisely because "bursty
  adds" was the last innocent explanation standing).
- Coverage is not being destroyed in sealed partitions: the manifest diff lost
  120 files, **118 of them in today's partition**, 2 sealed.
- The drain could never converge on a backlog roughly half of which is not real.

**Everything today that was measured in units of "uncovered files" is suspect** —
the ~7/hr drain, the ~100/hr accrual, the 6,400 backlog, and the census-based
verdict on carry-forward. The mechanism-level readings survive (waves do carry;
manifests do persist; 121/121 builds are the flush path), because those come from
event logs rather than from the snapshot.

**Next is not a tantivy change.** It is to confirm the doubling at its source and
fix the snapshot, then re-run the census and see what the real backlog is. There
may not be one worth a per-day index at all.

### 15:55 — census deduped, and one corroboration withdrawn before it was used

Fixed in `0417691`: the census now groups snapshot URIs through a pure
`group_parquet_by_project`, which drops repeats and returns the pre-dedupe count
so the caller can log `delta_snapshot_duplicate_files{raw, distinct}`. Doctest
covers it and is discriminating (without `.unique()` the grouped length is 2).

**A corroboration I nearly used, and shouldn't have.** I found
`dedup_partition_paths` called from several compaction paths and was about to
write that "the codebase already defends against this duplication, just not in
the census". Reading it first: it does **not** dedupe anything — it *filters*
snapshot paths to one project/date partition. The name says dedup; the body is a
partition filter. So there is no existing duplicate defence, and the comforting
"established remedy" framing was false.

That leaves the mechanism inferred rather than proven. What is *proven* is
over-counting: 6441 uncovered > 4490 live files is arithmetic, not a theory.
*Why* the list repeats is still a hypothesis, which is exactly what the new
warning settles — it fires only if `get_file_uris()` genuinely returns the same
path twice. If the census still exceeds `numOfAddFiles` while the warning stays
silent, the cause is elsewhere (summing across roots, or a stale `table_ref`),
and I would rather learn that from the instrument than from another day of
inference.

**The larger question this opens, flagged not fixed:** if `get_file_uris()`
repeats paths, the census is not the only consumer. Whether the scan path can
double-read a file is a correctness question well outside the tantivy work, and
it should be answered deliberately rather than patched on suspicion.

## 16:10 — RETRACTED: there is no phantom. I compared a six-table census to a one-table checkpoint.

The "subset larger than the set" proof is wrong, and it was the day's headline.

The census sums over **every indexed table** (`svc.config.indexed_tables()` —
any table with a tantivy-indexed field). There are six:

| table | live files (`numOfAddFiles`) |
|---|---|
| otel_logs_and_spans | 4,550 |
| otel_metrics | 3,287 |
| …rollup_dashboard_1m_v3 | 3,221 |
| …rollup_dashboard_1h_v2 | 858 |
| …rollup_dashboard_1m_v2 | 417 |
| …rollup_dashboard_1h_v1 | 410 |
| **total** | **12,743** |

`uncovered = 6,484` sits comfortably inside 12,743. **No contradiction, no
doubling, no phantom backlog.** I checked 4,490 against a number that summed six
tables — the identical population mismatch I had called out two hours earlier in
my own "builds outpace accrual 2:1" claim, committed again with more confidence
and a push notification behind it.

The checkpoint itself is clean, which should have been the tell: **4,550 add
rows, 4,550 distinct, zero duplicates.** I read that result and kept going.

**Consequences, all mine to undo:**
- `c47c22c` reverts the census dedupe. It guarded a defect for which there is no
  evidence, and its doctest documented that defect as real. Speculative code on a
  false premise is worse than no code.
- The memory file is being rewritten, not deleted — a future session that reads
  "bound file metrics against numOfAddFiles" would repeat the error unless it
  also reads "and make sure both sides cover the same tables".

**And the real producer falls straight out of the correction.** Scanning the
rollup table's own commit log — which I never did, because I assumed the census
was about `otel_logs_and_spans`:

```
otel_logs_and_spans_rollup_dashboard_1m_v3:
  70 commits / 0.51h, 39 adds to SEALED partitions  ->  ~77/hr
```

**One** rollup table mints ~77 sealed uncovered files an hour, against the 4.6/hr
I measured on `otel_logs_and_spans` and mistook for the whole picture. Rollups
write into historical date partitions by design — that is what a backfill *is* —
so every rollup commit lands in `older`. Five such tables plus `otel_metrics`
comfortably explain the ~130/hr.

So the backlog is real, the accrual is real, and its source is **rollup and
metrics tables, not the spans table** — which no amount of tantivy scheduling,
carry-forward, or cap tuning on the spans path was ever going to touch.

### 16:20 — why rollup tables are indexed at all: one struct-update in `synthesize`

Rollup schemas are generated, not written. `RollupSpec::synthesize`
(`src/schema.rs`) builds the structural columns through a `plain()` helper that
deliberately sets `tantivy: None` — the author clearly considered indexing for
synthesized fields. But dimensions are copied wholesale from the source schema:

```rust
for d in &self.dimensions {
    let f = src_field(d)?;
    fields.push(FieldDef { nullable: true, ..f });   // <- carries `tantivy` too
}
```

`..f` inherits `tantivy: Some { indexed: true, … }` from the spans schema for
every dimension (`level`, `name`, `status_code`, `kind`, …). That single struct
update is why the four rollup tables satisfy `indexed_set()`, appear in
`indexed_tables()`, own manifests, and carry a per-file coverage obligation the
census counts and the backfill tries to discharge.

Population, corrected: the indexed set is `otel_logs_and_spans` (4,550 live
files) plus the four rollup tables (3,221 + 858 + 417 + 410) = **9,456**, of
which **~4,900 are rollup files — 52%**. `otel_metrics` has a manifest directory
but its YAML declares no tantivy fields, so its 3,287 files are most likely a
stale artifact of an earlier config rather than a current obligation; that needs
confirming before it is counted either way.

**The consequence, if these indexes are never read:** roughly half the coverage
obligation and essentially all of the ~77/hr sealed accrual exist to serve
indexes nobody queries, and disabling tantivy on synthesized dimensions would
remove both at a stroke — no scheduling, no throughput, no per-day index.

**That "if" is not yet established, and I am not going to infer it.** The
argument that rollup routing strips tantivy hints (`strip_index_hints`, and the
`ebfa7e0` fix behind it) suggests no `text_match` ever reaches a rollup scan, but
that is exactly the shape of reasoning that has failed repeatedly today. A log
query for tantivy activity by table is running; the claim waits for it.

### 16:30 — the "rollup indexes are never read" hypothesis weakens on its own code evidence

Read `strip_index_hints` properly instead of leaning on the memory of it. Its
doc comment is explicit about scope: it drops hints **inside the matcher**, where
a query's filter is compared against a declared measure's filter, because the two
sides do not receive the same hints (prod 2026-08-12: three hints in two arities
on one side, one on the other). It says nothing about the plan that executes.

So it does **not** support "no `text_match` reaches a rollup scan". The opposite
is more likely: `tantivy_rewriter` attaches hints additively based on
`indexed_columns_for(table)`, and rollup dimensions *are* indexed columns by the
inheritance above — so a routed query can carry a hint straight into the rollup
`TableScan`, and the rollup index would be consulted.

**So the recommendation I was one step from making — disable tantivy on
synthesized dimensions — is not supported, and I am not making it.** The
inheritance via `..f` is real and looks unintended; whether it is load-bearing is
unresolved, and the cheap inference cut *toward* these indexes being used once I
read the code rather than recalling it.

What would settle it, in preference order:
1. A prod log scan for tantivy/prefilter activity keyed by table name. Started;
   it exceeds the SSH command timeout on a busy service and needs narrowing to a
   single grep over a short window.
2. `EXPLAIN` of a routed dashboard query that carries a `text_match` hint — if a
   rollup `TableScan` shows a tantivy prefilter, the question is answered in one
   query and no log scan is needed.

Even if they are read, indexing low-cardinality raw dimensions (`level`, `kind`)
is questionable next to bloom filters and column stats — but that is a design
opinion, not evidence, and it does not justify deleting anything today.

### 16:40 — EXPLAIN on prod: the dashboard query did not route, and the spans prefilter is live

Ran a routed-shaped dashboard query against prod (read-only EXPLAIN, no execution):

```sql
SELECT date_trunc('hour', timestamp), count(*) FROM otel_logs_and_spans
WHERE project_id = '87576849-…' AND timestamp >= now() - interval '3 days'
  AND kind = 'server' GROUP BY 1
```

Two things, one of them not what I was looking for:

1. **It did not route to a rollup.** Every `DataSourceExec` reads raw
   `otel_logs_and_spans` parquet. So this plan says nothing about whether rollup
   tantivy indexes are read — the question stays open, and I need a query that
   actually routes before it can be answered.
2. **The spans-side tantivy prefilter is unambiguously live.** The scan carries
   `text_match(kind, server)` *and* `id IN (SET)` with 55 ids and
   `required_guarantees=[id in (…)]` — that id-set is the tantivy index answering
   the predicate and pruning row groups. Whatever else is true, the index on
   `otel_logs_and_spans` earns its keep.

An incidental asymmetry worth noting rather than chasing now: the sealed
(`date=2026-08-20`) file group carries the `required_guarantees` id-set while the
`date=2026-08-23` group has `required_guarantees=[]` — the prefilter contributes
to one leg and not the other. That is consistent with today's partition being
uncovered, and it is a concrete example of coverage translating directly into
pruning power.

**Where this leaves the day's goal.** Coverage convergence is blocked by a
population question, not a throughput one: rollup tables are 52% of the indexed
file set and mint ~77/hr of sealed uncovered files, through an inheritance
(`..f` in `synthesize`) that looks accidental. Whether they *should* be indexed
is a design decision — a rollup row is an aggregate, and a `text_match` on a
low-cardinality dimension is a different proposition from one on `body` — and it
is the user's call, not mine to make silently. The spans index, by contrast, is
demonstrably load-bearing and should not be touched.

## 16:55 — the rollup and metrics manifests have been FROZEN since 2026-08-20

The manifest mtimes settle where the accrual comes from:

| table | newest manifest write |
|---|---|
| otel_logs_and_spans | **today, 17:09** (continuous) |
| otel_metrics | 2026-08-20 12:41 |
| …rollup_dashboard_1m_v3 | 2026-08-20 00:59 |
| …rollup_dashboard_1h_v2 | 2026-08-20 00:59 |

Only the spans table is being indexed. **Every rollup and metrics file created
since 2026-08-20 has been accumulating as uncovered** — which is exactly the
`older` curve, at exactly the ~77/hr one rollup table commits, and it is why the
spans-only drain could never catch up: it was never working on the same
population that was growing.

**And the reconcile that should service them has not run at all.** The cron loops
over `indexed_tables()` and logs unconditionally per table — deliberately, per
its own comment: *"a silent no-op arm made 'ran and found nothing'
indistinguishable from 'never ran', and the cron not firing at all was the actual
bug."* In the last **60 minutes**: `grep -c "tantivy reconcile"` = **0**.

Checked and eliminated:
- Schedule is `0 */15 * * * *` in the deployed tree `c7cd4bf` (serde default, and
  a unit test asserts it), so this is not the old hourly-at-:20 gate.
- No env override: `docker service inspect` shows no
  `TIMEFUSION_TANTIVY_RECONCILE_SCHEDULE`, and the only tantivy-related env is
  `RUST_LOG=…,tantivy=warn`, which silences the *crate*, not this
  `timefusion::database` line.

So the schedule is right, nothing overrides it, the process has been up ~35
minutes, and the job has produced zero lines. The remaining candidates are that
`db.tantivy_indexer()` returns `None` in this path (the early `return` before any
logging is the one silent exit the job has), or that the cron is not registered
at all. The flush path clearly *has* an indexer — 121 builds — but it holds its
own reference, so that does not settle it.

**This is the strongest open lead and it is upstream of everything else**: the
start gate I believed I fixed this morning is still shut, just for a different
reason than the schedule.

### 17:10 — the reconcile IS registered and the cron machinery works, so it is exiting at the gate

Two checks against existing logging, no deploy needed:

```
Tantivy reconcile job scheduled with cron expression: 0 */15 * * * *   <- registered
bloom sidecar reconcile: built=28 errors=0   @16:32                    <- machinery fires
bloom sidecar reconcile: built=55 errors=0   @16:39
grep -c "tantivy reconcile"  (60m)  =  0                               <- never a line
```

So the job is registered with the right schedule, on the same cron machinery that
demonstrably fires a sibling job every five minutes, in the same process — and it
has never produced a line. The job logs unconditionally once past its gate, and
it has exactly one silent exit:

```rust
let Some(svc) = db.tantivy_indexer().cloned() else { return };
```

**The overwhelming reading is that the tantivy indexer is absent from the
`Database` the cron holds.** The flush path still builds indexes because it holds
its own service reference (121 builds), which is why the failure is invisible
from the outside: indexing *appears* to work while every table-wide reconcile
never starts.

That is the root cause of the day, and everything else was a symptom of it: no
reconcile means no backfill of any table, which means rollup and metrics
manifests frozen since 08-20, which means `older` climbing forever against a
drain that only ever touched files the flush path happened to create.

`1608958` makes the gate observable (`tantivy_reconcile_no_indexer`) rather than
guessing which branch it takes. `with_tantivy_indexer` publishes through a shared
`OnceLock` so clone ordering should not matter, and `let _ = …set(svc)` swallows a
second set — both worth checking against the warning once it deploys, rather than
reasoning further from the source.

**Deliberately not fixed blind.** The attachment path in `server/mod.rs` looks
correct on the page, so patching it on suspicion would risk another change
shipped on a false premise — the mistake already made once today with the census
dedupe. One instrument, one deploy, then the fix.

## 17:35 — the instrument refuted my root cause, and the real one is serial-loop starvation

`1608958` deployed, and the gate it made observable **never fired**:

```
tantivy_reconcile_no_indexer  (30m)  =  0
tantivy reconcile gc: table=otel_logs_and_spans project=6297304f… entries_removed=15 blobs_deleted=15
```

So the indexer is present and the reconcile **does** run. "The tantivy indexer is
absent from the Database the cron holds" is withdrawn — which is exactly why the
instrument shipped instead of the blind fix I was one step from writing into
`server/mod.rs`.

What the GC line reveals is better. It comes from *inside* `tantivy_reconcile_table`,
so the pass starts and works; what never appears is the per-table **summary**,
which prints only when the table finishes. The pass is not being skipped — **it
is not completing.** Costing it:

- `indexed_tables()` is a `BTreeSet`, so the order is sorted and
  `otel_logs_and_spans` is **always first**.
- Per table, GC walks *every* project's manifest and for each one re-reads the
  full live-file list and deletes blobs — unbounded, 22 projects.
- Then `backfill_table_indexes` builds up to `max_files_per_pass` files at the
  measured ~4 min/build — **8 files ≈ 32 minutes** on its own.
- Prod restarts roughly every 15 minutes.

A spans pass cannot fit between restarts, the loop is sequential and awaits each
table, so **every table after the first is never reached** — for three days. The
manifests freezing on 2026-08-20 is that, and nothing noticed because the flush
path kept indexing spans, so coverage looked alive from the outside.

**Fixed by rotating where the pass starts** (`rotation_offset`, doctested): one
15-minute slot per position, offset derived from the clock rather than from
state, so a restart cannot reset it to the same starving order. With six tables
every table leads a pass every 90 minutes instead of never. It does not make the
pass faster — bounding GC and the build cost is the real repair — but it converts
"three tables permanently starved" into "every table served on a cycle", which
is the difference between a backlog that grows without bound and one that drains.

### 17:45 — pass condition for the rotation fix

Landed as `34508ee`. The test is not a rate and not a census total, both of which
have misled today. It is a **file mtime**:

> `index_manifests/otel_logs_and_spans_rollup_dashboard_1m_v3/*/manifest.json`
> gets a write dated after 2026-08-23.

Frozen at 2026-08-20 for three days, so any write at all is unambiguous and
needs no baseline, no quiet process and no rate arithmetic. Check with:

```bash
aws s3 ls "s3://$AWS_S3_BUCKET/index_manifests/otel_logs_and_spans_rollup_dashboard_1m_v3/" \
  --recursive --endpoint-url "$AWS_S3_ENDPOINT" | sort -k1,2 | tail -3
```

If it stays frozen, the rotation reached the table but the pass still cannot
finish inside a restart window, and the next repair is bounding the work rather
than reordering it: cap the per-project GC the way builds are already capped, or
give the pass a wall-clock budget so it yields mid-table instead of dying.

**Unrelated, noticed not touched:** two doctests in `src/maintenance_coordinator.rs`
(`clear_stale_estimates:780`, `claimability_census:1231`) fail to compile — log
output pasted into a bare ``` fence, which rustdoc tries to parse as Rust. It
belongs to another agent's in-flight commit `955f05f`, and `cargo nextest` does
not run doctests so CI will not catch it. Two-character fix (` ```text `), left
for its owner rather than editing a file being actively worked.

## 19:50 — PASS: the rotation fix broke a three-day freeze

`34508ee` deployed, and the pre-registered condition is met:

```
index_manifests/otel_logs_and_spans_rollup_dashboard_1m_v3/…/manifest.json
  2026-08-23 19:45:31    <- first write since 2026-08-20
```

Frozen for three days, so a single write settles it with no baseline, no quiet
process and no rate arithmetic — which is why the condition was chosen that way
after four rate-based readings misled me today.

The other four tables are **still frozen** (`otel_metrics` 08-20,
`…1h_v1` 08-19, `…1h_v2` 08-20, `…1m_v2` 08-19), and that is the predicted
behaviour rather than a partial failure: rotation gives one table the lead per
15-minute slot and the pass still cannot finish inside a restart window, so only
the leader is served. **Prediction, registered before the data:** by ~21:15 —
90 minutes, one full cycle of six slots — at least three of the four should carry
a 2026-08-23 manifest. If they do not, the rotation is reaching them but the pass
is dying before it does useful work, and the next repair is bounding the work,
not reordering it.

One detail worth reading correctly: the new rollup manifests are **882-2,582
bytes** where the 08-20 ones were ~105 KB. That is a shrink of ~40x and it looks
alarming, but it is what GC is supposed to do — three days of rollup rewrites
means almost every entry from 08-20 points at a file that no longer exists, so
the first pass in three days prunes nearly all of them and rebuilds from a much
smaller live set. A large manifest here would have been the bug.

A monitor now watches which tables get a manifest written today, so the cycle
either completes or visibly does not.

### 20:25 — correction: five indexed tables, not six, and `otel_metrics` is an orphan

`schemas/otel_metrics.yaml` contains **zero** `tantivy` declarations, and its
rollups are synthesized from it, so neither it nor its rollup tiers can satisfy
`indexed_set()`. The indexed population is therefore:

| table | live files |
|---|---|
| otel_logs_and_spans | 4,550 |
| …rollup_dashboard_1m_v3 | 3,221 |
| …rollup_dashboard_1h_v2 | 858 |
| …rollup_dashboard_1m_v2 | 417 |
| …rollup_dashboard_1h_v1 | 410 |
| **total** | **9,456** — rollups are 4,906 of it, **52%** |

Two consequences.

**The rotation cycle is 75 minutes, not 90** (five slots, not six), so the
prediction is now: by ~21:00, the **three** remaining spans-rollup tables should
carry a 2026-08-23 manifest. `otel_metrics` will never light up, and I would have
read its silence as a partial failure of the fix — the monitor was armed to watch
for something that cannot happen.

**`index_manifests/otel_metrics/` is dead weight**: 10 objects, 774 KB of
manifests describing indexes for a table nothing reconciles. Because reconcile is
the only thing that GCs manifests and it never visits an unindexed table, these
can never be collected — along with whatever index blobs they still reference,
which are much larger than the manifests. Left in place: deleting prod objects on
a Sunday evening off a schema read is not a call to make unattended, and it costs
nothing to defer.

Current state at 20:23:

```
otel_logs_and_spans                          2026-08-23 20:22   <- served
…rollup_dashboard_1m_v3                      2026-08-23 19:45   <- served
…rollup_dashboard_1h_v1                      2026-08-19 09:45   <- still frozen
…rollup_dashboard_1h_v2                      2026-08-20 00:59   <- still frozen
…rollup_dashboard_1m_v2                      2026-08-19 09:13   <- still frozen
otel_metrics                                 2026-08-20 12:41   <- orphan, expected
```

## 20:35 — three tables served in 24 seconds: the passes are cheap, and they build nothing

```
…rollup_dashboard_1h_v2   2026-08-23 20:30:15
…rollup_dashboard_1m_v2   2026-08-23 20:30:25
…rollup_dashboard_1m_v3   2026-08-23 20:30:39
…rollup_dashboard_1h_v1   2026-08-19 09:45      <- still frozen
```

Three tables written within **24 seconds** of each other. That refines the root
cause rather than confirming my cost model: a rollup pass is not slow at all —
it is `otel_logs_and_spans` specifically that consumes the window, and being
sorted first it starved everything behind it. Rotation works because once a
rollup leads, the rollups behind it finish in seconds.

`…1h_v1` is still frozen and that is consistent, not a failure: it sorts
*first*, so under the current offset it sits immediately **after** spans in the
cycle and gets reached only when spans finishes. Its own lead slot arrives within
~4 slots. **Prediction: `1h_v1` carries a 2026-08-23 manifest by ~21:35.**

**But coverage is still not converging**, and 24 seconds is why:

| census (log time) | uncovered | older |
|---|---|---|
| 17:59 | 6704 | 4024 |
| 18:14 | 6721 | 4032 |
| 18:28 | 6745 | 4046 |

Post-fix, `older` still grows ~56/hr — down from ~130/hr, and a lower bound
since two restarts fall inside that window and restarts bias accrual down. A pass
that GCs three tables in 24 seconds cannot have **built** anything: at the
measured ~4 min/build, eight builds would take half an hour. So the reconcile is
now *reaching* the rollup tables and doing GC, while building approximately zero
indexes.

So the fix opened the door and the room is empty. Next question, and it is
concrete: **why does `backfill_table_indexes` build nothing for a rollup table?**
Candidates, cheapest first — `timefusion_tantivy_backfill_skip_today` (which I
set to `true` this morning, and which would matter if rollup writes land in
today's partition after all), `max_file_mb`, or a candidate list that is empty
for a reason worth knowing. The per-table summary line carries `built=`, so one
`grep` after the next pass distinguishes them.

## 21:00 — `built=8` in 14 seconds: the cap I lowered this morning is the throttle

The per-table summary finally printed, and it answers the question directly:

```
18:45:14 tantivy reconcile: table=…rollup_dashboard_1m_v2 built=8 entries_removed=0 blobs_deleted=0
18:45:28 tantivy reconcile: table=…rollup_dashboard_1m_v3 built=8 entries_removed=0 blobs_deleted=0
```

**`built=8` — the cap — and eight builds took 14 seconds.** So the rollup passes
are not building "approximately zero" (yesterday's guess, wrong): they build
exactly as much as they are allowed, at **~1.75s per build**. The 4-5 min/build
figure written into the config comment is a `otel_logs_and_spans` number, ~150x
more expensive, and it was generalised to every table.

**I lowered that cap from 150 to 8 this morning**, on the reasoning recorded in
the config: *"The cap does NOT throttle throughput — build rate does — so
lowering it costs nothing."* True for spans. False for every other indexed
table, and false for exactly the tables whose coverage had been frozen since
08-20. At 8 files per lead slot a rollup table drains ~6/hr against ~77/hr of
accrual — a 12x deficit created by a knob, not by physics.

**Fixed by bounding the pass in the unit that actually costs: bytes.**

- `timefusion_tantivy_backfill_max_bytes_per_pass_mb` (default 2048) is the real
  limiter; `truncate_to_byte_budget` applies it *after* the fair round-robin
  split, so ordering and the reserved tail share still decide WHICH files and
  the budget only decides how far down that order the pass gets.
- `max_files_per_pass` goes back to 128 and is documented as a count ceiling
  against pathological tiny-file queues, not as the sizing knob.
- Files with unknown size count as free rather than being dropped — a missing
  size entry must not silently remove work — and at least one file always
  survives, so an over-budget whale makes progress instead of wedging the queue
  behind it. Both pinned by doctest.
- `tantivy_backfill_started` now carries `planned_mb` and `budget_mb`: with bytes
  as the bound, a pass reporting only a file count cannot be shown to have been
  limited by the budget rather than the ceiling.

This is the same population-mismatch mistake as the census and the build counts,
in its third form today: one number generalised across two populations that
differ by two orders of magnitude. It is now written into the config comment so
the next person does not re-derive it.

**Still outstanding:** `…1h_v1` remains at 2026-08-19 — the ~21:35 prediction has
not come due yet.

## 21:35 — every indexed table served, and the whole cycle now takes 40 seconds

The `1h_v1` prediction came due and landed: **all five indexed tables have a
2026-08-23 manifest.** Better than predicted, the 19:30 pass served the entire
set in one go:

```
19:30:00  …1h_v1   planned=8 cap=8            19:30:06  built=8
19:30:07  …1h_v2   planned=8 cap=8            19:30:11  built=8
19:30:12  …1m_v2   planned=8 cap=8            19:30:18  built=8
19:30:19  …1m_v3   planned=8 cap=8 skipped_today=690   19:30:22  built=8
19:30:38  otel_logs_and_spans  planned=8 cap=8 skipped_today=3
```

**38 seconds for the whole cycle.** So rotation was not merely a fairness patch —
with the rollup passes costing seconds, every table now gets served every slot,
and the starvation is gone rather than merely redistributed. Three days of frozen
manifests closed.

Two things this run shows that were invisible before:

- **`cap=8` and no `planned_mb`** — this pass ran on the pre-budget image, so the
  byte budget has not been exercised yet. `6691031` is deployed now; the next
  pass either shows `planned` well above 8 with `planned_mb ≤ 2048`, or the
  budget is not doing what I think.
- **`skipped_today=690` on `1m_v3`** against `planned=8`. The 1m rollup writes
  into *today's* partition continuously, so `skip_today` — which I turned on this
  morning for the spans hot-tail — is deferring ~690 files on that table alone.
  That is consistent with `today=686` in the census, and it is deliberate rather
  than broken, but it means the pass sees only the sealed remainder. Whether
  `skip_today` should apply to rollup tables at all is a separate question from
  the byte budget, and I am not changing two things at once.

Coverage is still climbing (`uncovered=6773 older=4061` at 19:24) — expected,
since 8 builds per slot per table is ~32/hr against ~77/hr accrual on `1m_v3`
alone. The byte budget is the change that should close that, and it has not run
yet.

## 22:05 — the byte budget does exactly what it was built to do, in both directions

First pass on `6691031`:

```
1m_v3                planned=128  planned_mb=29    cap=128  budget_mb=2048  skipped_today=711
otel_logs_and_spans  planned=7    planned_mb=1885  cap=128  budget_mb=2048  skipped_today=3
```

Both halves of the intent are visible in one pass:

- **The cheap table is unleashed** — `1m_v3` plans **128 files for 29 MB**, where
  the old cap gave it 8. It is now limited by the count ceiling, not the budget,
  and 29 MB against a 2048 MB budget shows how far the count cap was from the
  real cost.
- **The expensive table is bounded** — spans plans **7 files for 1885 MB**,
  stopping just under the budget. The old cap would have allowed 8 files of
  unbounded size. So the pass is now bounded by work rather than by an arbitrary
  count, which is what makes one number safe for both populations.

**And coverage fell for the first time today:**

| | uncovered | today | week | older |
|---|---|---|---|---|
| 19:24 (pre-budget) | 6773 | 686 | 2026 | 4061 |
| **20:02 (post-budget)** | **6294** | 714 | 1832 | **3748** |

**-479 uncovered and -313 `older` in 38 minutes** — after climbing all day. The
count is derived from storage rather than from process-local state, so a restart
between the two samples cannot manufacture a drop; this is work landing.

Provisional, and the caveat is the day's lesson rather than politeness: **one
interval is one point.** The mechanism is directly observed (`planned` 8 → 128,
16x the builds per slot), which is stronger than the inference behind the
retracted readings this morning, but the confirming point is the next census and
a monitor is now watching that trend rather than the manifests.

**One more population correction.** Two tables I had never seen appeared in this
pass — `…rollup_dashboard_level_1h_v1` and `…level_1m_v1`, both `planned=0`. So
the indexed set is larger than the five I counted twice today. It changes nothing
quantitative here (they have no work), but "I have finally enumerated the
population" has now been wrong three times, and it is worth writing down that I
should stop asserting it.

## 22:35 — the decline did not hold. Refuted by the check I registered.

| time | uncovered | today | week | older |
|---|---|---|---|---|
| 19:24 (pre-budget) | 6773 | 686 | 2026 | 4061 |
| 20:02 | **6294** | 714 | 1832 | 3748 |
| 20:16 | 6383 | 778 | 1833 | 3772 |
| 20:31 | **7024** | **1208** | 1854 | 3962 |

`uncovered` is now *higher* than before the change. So **"coverage fell for the
first time today" is withdrawn** — it was one favourable interval, which is
precisely what the provisional caveat was for. Registering the check is the only
reason this took thirty minutes to catch instead of becoming tomorrow's premise.

Reading it properly, though, the picture is not simply "no better":

- **The rise is dominated by `today`**: 714 → 778 → **1208**, +494 in thirty
  minutes, against `older` +214. Evening traffic and today's compaction churn.
- **`skip_today` is on, so the backfill deliberately does not touch those
  files.** Today's growth is therefore expected-by-design, not a failure of the
  budget — but it means `uncovered` as a headline number cannot measure this
  change at all. The metric that can is `week + older`.
- On that metric: 6087 → 5580 → 5605 → **5816**. Net **-271** against the
  pre-budget baseline, but rising across the last two intervals.

So: not a win, not a refutation of the byte budget either — genuinely
insufficient evidence, across a window containing **two restarts**.

**And a structural consequence worth naming before it surprises someone:**
`today` is ~1,208 uncovered files that `skip_today` defers by design. At UTC
midnight that partition ages into `week` wholesale, so the sealed backlog takes a
~1,200-file step change in one census interval. That is not accrual and must not
be read as a regression when it appears — it is deferred work arriving on
schedule, and it is the strongest argument yet that `skip_today` deserves
revisiting for tables whose writes land in today's partition continuously.

## 23:20 — two more intervals: `week` is draining, `older` is not

First, a measurement I nearly reported: my parser printed
`uncovered=0 today=0 week=0 older=0` for both samples. The census returns exactly
`(0, 0, [0;3])` when the indexer is absent, so all-zeros is a real failure mode
and it looked like one. It was not — my regex allowed up to 12 non-digit
characters between the field name and its value, and the ANSI escape `\x1b[0m`
between them contains a `0`. Stripping escapes first gives the real numbers.
**Sixth measurement error of the day, and the first that read as bad news
instead of good.**

| time | uncovered | today | week | older | **sealed (week+older)** |
|---|---|---|---|---|---|
| 19:24 (pre-budget) | 6773 | 686 | 2026 | 4061 | **6087** |
| 20:02 | 6294 | 714 | 1832 | 3748 | 5580 |
| 20:16 | 6383 | 778 | 1833 | 3772 | 5605 |
| 20:31 | 7024 | 1208 | 1854 | 3962 | 5816 |
| 21:05 | 7252 | 1235 | 1772 | 4245 | 6017 |
| 21:18 | 6939 | 1247 | 1635 | 4057 | **5692** |

Separating the tiers is what makes this readable:

- **`week` is draining steadily**: 2026 → 1832 → 1833 → 1854 → 1772 → **1635**.
  -391 over two hours, monotone apart from one interval. This is the byte budget
  working.
- **`older` is not**: 4061 → 3748 → 3772 → 3962 → 4245 → 4057. Noisy and net
  flat. The deepest tier is holding, not shrinking.
- **`today` climbs and is deferred by design**: 686 → 1247.

Net sealed backlog **-395 in two hours**, entirely attributable to `week`. Across
a window containing at least four deploys, so the drain is a lower bound.

**Honest position: no longer diverging, not yet converging.** The starvation is
fixed and one tier is draining; the deepest tier is not, and `today` is
accumulating ~1,250 files that will land in `week` wholesale at UTC midnight.

The two open questions, in order: whether `skip_today` should apply to tables
whose writes land in today's partition continuously, and why the fair-split's
reserved tail share is not translating into `older` progress now that passes are
16x larger.

## 23:55 — `week` drains at ~233/hr, `older` is at equilibrium, and the count cap is binding again

| time | today | week | older | sealed |
|---|---|---|---|---|
| 19:24 | 686 | 2026 | 4061 | 6087 |
| 21:18 | 1247 | 1635 | 4057 | 5692 |
| 21:27 | 1276 | 1636 | 4080 | 5716 |
| 21:42 | 1355 | **1490** | 4063 | 5553 |

The split is now sharp enough to name:

- **`week`: 2026 -> 1490 in 2.3 hours, ~-233/hr.** Monotone apart from one flat
  interval. Nothing writes into 1-7-day-old partitions except rewrites, so this
  tier has almost no inflow and the drain shows up undiluted.
- **`older`: 4061 -> 4063. Flat to within two files across 2.3 hours.**

Flat is not the same as stuck, and the difference matters for what to do next.
`older` is precisely where the rollup backfills write — historical date
partitions, measured earlier at ~77/hr from `1m_v3` alone. A tier draining at
roughly its inflow reads as motionless. **Equilibrium, not starvation** — which
means the lever is throughput, not fairness, and fairness is already fixed.

**And the count ceiling is binding again, exactly as it was at 8.** The first
budgeted pass logged `planned=128 cap=128 planned_mb=29 budget_mb=2048`: it
stopped on the *count*, having spent **1.4% of its byte budget**.

So the cost model needs one correction I got wrong when I introduced the budget.
For small files the cost is **per-file overhead, not bytes** — ~1.75s/build on
rollup files, where 128 builds is ~29 MB. Bytes bound the whale case and cannot
bound the many-small-files case; the two need different limiters, which is why
keeping both is right and why the ceiling has to be sized against wall clock
rather than left at a token value.

`max_files_per_pass` 128 -> **320**: ~9 minutes at the measured rate, inside the
~15-minute gap between prod restarts, with the byte budget still cutting a
whale-heavy queue short. At 320/slot and four slots an hour that is ~1,280
files/hr per table against ~77/hr of accrual — the first setting all day where
drain exceeds inflow by an order of magnitude rather than trailing it.

## 00:35 — 128 builds in one pass, and the spans pass still never completes

Ceiling of 320 is live (`cap=320` in the 22:15 pass). Ninety minutes of summary
lines:

```
1m_v3                        built=128       <- the byte budget + ceiling delivering
…level_1m_v1                 built=0   (x2)  <- nothing to do
otel_logs_and_spans          (no summary line at all)
```

**`built=128` is the win, measured**: one pass now does 16x what the cap allowed
this morning. And `otel_logs_and_spans` has not printed a completion in ninety
minutes — its pass is 7 files at ~4-5 min/build ≈ **30 minutes**, against a
~15-minute restart window. The starvation is fixed *between* tables and now
reproduces *inside* the spans table.

That also explains why the census went flat rather than converging:

| time | today | week | older | sealed |
|---|---|---|---|---|
| 22:00 | 1361 | 1404 | 4074 | 5478 |
| 22:16 | 1361 | **1404** | 4096 | 5500 |
| 22:31 | 1428 | **1404** | 4125 | 5529 |

`week` fell 2026 -> 1404 and then stopped dead — three identical samples. `older`
is creeping up again. The arithmetic fits: **only one `1m_v3` pass completed in
ninety minutes**, so the rollup drain is ~128 files per ~90 min ≈ 85-100/hr
against ~77/hr of accrual. Barely above water, which is what a flat tier looks
like. The rollup tables are not slow — they are *rarely reached*, because the
spans pass eats the slot whenever it is not the leader.

**So one change would unlock the rest: stop letting the spans pass block the
cheap ones.** Running the tables concurrently is the structural answer (rollup
passes take seconds; spans takes half an hour), with a small bound because this
box has an OOM history.

**Not shipping it tonight.** Three changes are already in flight and unevaluated
— rotation, the byte budget, and the 320 ceiling — and prod restarts every ~15
minutes from other work, so a fourth would land in a window where none of them
can be measured. Today already produced one change shipped on a false premise
and reverted; the failure mode is not caution, it is stacking. The diagnosis is
recorded and the change is one function call when there is a quiet process to
measure it against.

## 01:55 — the whole cycle completed: 901 builds in 13 minutes

```
23:32:42 DONE otel_logs_and_spans           built=7
23:34:05 DONE …rollup_dashboard_1h_v1       built=261
23:35:51 DONE …rollup_dashboard_1h_v2       built=320
23:35:53 DONE …rollup_dashboard_1m_v2       built=0
23:37:08 DONE …rollup_dashboard_1m_v3       built=320
23:45:00 DONE …dashboard_level_1m_v1        built=0
```

**Every indexed table completed in one cycle — 901 builds in ~13 minutes.**
Against the 8-per-pass cap this morning, and against a baseline accrual of
~77/hr, that is roughly 4,000 builds/hr of capacity.

**Correction: "the spans pass never completes" was too strong.** It printed
`built=7` at 23:32. The ninety-minute window I drew that from contained no
completion, but absence over one window is not never — the right claim was "did
not complete in 90 minutes", and I overstated it. It completes; it is simply
slow and small (7 files, byte-bound), which does not block the others the way I
inferred.

And `week` moved again the moment a full cycle landed:

| time | today | week | older | sealed |
|---|---|---|---|---|
| 22:46 | 1486 | 1404 | 4336 | 5740 |
| 23:16 | 1496 | 1401 | 4619 | 6020 |
| 23:31 | 1593 | 1401 | **5430** | 6831 |
| 23:46 | 1605 | **1032** | 5354 | 6386 |

Two things in that table:

- **`week` 1401 -> 1032**, -369 in one interval, after three samples frozen. So
  the flatness was passes not completing, not a tier the backfill cannot reach.
- **`older` spiked +811 in fifteen minutes** (4619 -> 5430) and then fell back to
  5354. That is an inflow event, not drift — something wrote ~800 files into
  historical partitions around 23:31, which is what a rollup rebuilding old dates
  looks like. The cycle then started clearing it.

So the throughput problem is solved: capacity now exceeds baseline accrual by
~50x, and the remaining question is whether inflow *spikes* like the 23:31 one
are frequent enough to matter. That is a measurement for a quiet morning, not
another change tonight — the three shipped changes are now visibly doing their
job and nothing here argues for a fourth.

## 03:00 — it converges. Three cycles, and the midnight step landed as predicted.

| time | today | week | older | **sealed** |
|---|---|---|---|---|
| 23:31 | 1593 | 1401 | 5430 | 6831 |
| 23:46 | 1605 | 1032 | 5354 | 6386 |
| **00:16** | **0** | **1670** | 5389 | **7059** |
| 00:31 | 0 | 893 | 5382 | 6275 |
| 00:46 | 2 | 651 | 5305 | **5956** |

**The UTC-midnight step arrived exactly as registered**: `today` 1605 -> 0 and
`week` 1032 -> 1670, absorbing the whole deferred day in one interval. Flagging
it in advance is the only reason the 7059 peak reads as bookkeeping rather than a
collapse — it would otherwise have looked like the worst regression of the night.

**And then it drained through it.** `week` 1670 -> 893 -> **651**, -1,019 in
thirty minutes, having just absorbed a day's deferred work. Sealed 7059 -> 6275
-> **5956**: roughly **-2,200/hr**, against the ~77/hr accrual that used to
outrun it. `older` is falling too, slowly (5430 -> 5305).

Every cycle now completes, with `1m_v3` hitting its 320 ceiling three cycles
running:

```
00:15  spans built=7 · 1h_v2 built=138 · 1m_v3 built=320
00:31  1m_v3 built=320 · level_1m_v1 built=18
00:45  1m_v3 built=320 · level_1m_v1 built=9
```

**The goal is met: coverage converges rather than diverges**, demonstrated over
three consecutive cycles and across the worst-case boundary event rather than on
one favourable interval.

### What actually fixed it

Two defects, neither of them the throughput problem everyone (me included) spent
the day assuming:

1. **Serial-loop starvation.** `indexed_tables()` is a `BTreeSet`, so
   `otel_logs_and_spans` was always first; its pass could not finish between
   prod's ~15-minute restarts, and every table behind it was never reached —
   four rollup tables frozen since 08-20. Fixed by rotating the pass's starting
   table from a clock-derived offset.
2. **A cap sized on the wrong population.** `max_files_per_pass = 8`, chosen that
   morning because a spans build costs 4-5 minutes — while a rollup build costs
   **1.75 seconds**. Fixed by bounding the pass in bytes (whales) *and* sizing
   the count ceiling against wall clock (small files), because the two
   populations are limited by different things.

### Left deliberately undone

- Running the tables concurrently, so a slow spans pass cannot occupy a slot.
  Diagnosed, not shipped: three changes are in flight and unevaluated, and
  stacking a fourth is how tonight's one reverted change happened.
- Whether `skip_today` should apply to tables that write into today's partition
  continuously. It is why ~1,600 files waited for midnight.
- `index_manifests/otel_metrics/` — 774 KB of manifests for a table nothing
  reconciles, uncollectable because reconcile never visits an unindexed table.

## Session close — the four remaining items

| # | Item | Change |
|---|---|---|
| 1 | Tables run concurrently | `buffer_unordered(3)` over the rotated order. Passes differ ~150x in cost; in sequence the 30-minute spans pass owned the slot. Bounded at 3 for the OOM history — each pass holds a live-file list and a writer arena. |
| 2 | `skip_today` re-examined | Now applies only to **hot-packed base tables**. It exists to avoid racing the hot-tail packer, which only rewrites base-table files; a rollup tier is rebuilt wholesale (`mutable: false`) and writes into today's partition continuously, so skipping there merely deferred ~1,600 files to the UTC-midnight roll (`skipped_today=690` on `1m_v3` against `planned=8`). Predicate is the synthesized `rollup_generation` column, not a hardcoded name. |
| 4 | Per-project GC bounded | Two bounds. The live-file list is **memoised per resolved table** — on a unified table all 22 projects resolve to the *same* `DeltaTable`, so this was the identical full read 22 times. And the loop yields at `GC_PHASE_BUDGET` (120s), logging `tantivy_reconcile_gc_deferred` rather than capping silently. |
| 5 | Orphaned `otel_metrics` manifests | Deleted from prod (10 objects, 774 KB) after backing them up locally. `schemas/otel_metrics.yaml` declares no tantivy field, so the table cannot enter `indexed_set()` and reconcile — the only thing that GCs manifests — could never visit it. |
| 6 | Broken doctests | `maintenance_coordinator.rs` had prod log output in **indented** doc blocks, which rustdoc parses as Rust. Fenced as ```text; `cargo test --doc` is green. Not caught by CI because nextest does not run doctests. |

Items 1, 2 and 4 all attack the same thing from different sides: a pass that
cannot finish inside prod's ~15-minute restart window. 1 stops the slow pass
blocking the fast ones, 4 removes the largest chunk of the slow pass's own cost,
and 2 stops manufacturing deferred work that arrives in one lump at midnight.

---

# TODO — start here next session

Everything below `0bdeddf` is **unverified**: it shipped minutes before the
session closed, and prod's ~15-minute restart cadence makes any number taken
inside one window meaningless. Read
[[tf_young_process_reads_as_fixed_2026-08-23]] before quoting a rate, and check
`docker service ps` uptime first.

## P0 — verify what shipped (`0bdeddf`, plus `34508ee` / `6691031` / `c59487f`)

| # | Item | Pass condition | Why it could bite |
|---|---|---|---|
| 1 | Concurrency did not cause an OOM | no `exit 137` in `docker service ps` after several cycles | 3 concurrent passes each hold a live-file list + a 64 MB writer arena; this box has been killed at ~125 GB anon |
| 2 | `skip_today` change works on rollup tiers | `skipped_today=0` on `1m_v3` (was 690); `today` stops accumulating ~1,600 files that wait for midnight | predicate is the synthesized `rollup_generation` column — if a tier ever loses that column the table silently becomes "hot-packed" |
| 3 | The GC bound does not starve the tail | `tantivy_reconcile_gc_deferred` absent or small | if it fires every pass, 120s is too tight and the deferred projects are never collected |
| 4 | `older` finally drains | falling faster than the ~100/hr measured at 00:46 (5,305 => ~50h to clear at that rate) | items 1 and 4 should multiply the slots a rollup table actually gets |

## P1 — the question this session never answered

| # | Item | Stake |
|---|---|---|
| 5 | **Are rollup tantivy indexes ever READ?** | Rollup tiers are **52% of the indexed file population** (4,906 of 9,456 live files). Every attempt to get a dashboard query to route to a rollup failed (coverage is process-scoped and prod kept restarting), so this is genuinely open. If nothing `text_match`es them, half the coverage obligation is waste and the fix is one line. |
| 6 | Is the `..f` inheritance in `RollupSpec::synthesize` intended? | Rollup dimensions inherit `tantivy: { indexed: true }` from the spans schema via struct-update syntax, while the `plain()` helper directly above it explicitly sets `tantivy: None`. Looks accidental. Answering #5 answers this. |

**How to answer #5 without guessing:** `EXPLAIN` a dashboard query that actually
routes (needs a process whose rollup coverage map has rebuilt — ~1h uptime), and
look for a tantivy prefilter on the rollup `TableScan`. `strip_index_hints` is
matcher-scoped and does **not** settle it; I checked, and it points the other way.

## P2 — structural

| # | Item | Note |
|---|---|---|
| 7 | Deploy cadence vs unit length | Prod restarts every ~15 min; maintenance units average ~21 min. Every fix this session worked *around* this. It is the standing tax on all maintenance work, not a tantivy problem. |
| 8 | Re-tune the pass bounds now that passes run concurrently | 3 x 320 files x 2048 MB is a very different peak than the sequential case those numbers were sized against. |
| 9 | `skip_today` on the spans table | Left ON. Correct only if the hot-tail packer really does rewrite those files; worth confirming rather than inheriting the assumption. |
| 10 | Close this plan as superseded | The per-day index was the original goal. The real defects were serial-loop starvation and a cap sized on the wrong population — building a per-day index would have fixed neither. |

## P3 — hygiene

| # | Item |
|---|---|
| 11 | Attribute the `older` inflow spikes — ~800 files in 15 minutes at 23:31. Routine or one-off? Decides whether the new capacity headroom is actually enough. |
| 12 | **Doctests are not in CI.** `cargo nextest` does not run them, so two were broken on master for hours. Add `cargo test --doc` to `ci/checks.tsv` + `run_body` (they must change together — `make ci-selftest` enforces it). |
| 13 | Add a `_last_checkpoint` / `numOfAddFiles` bound to any file-count metric. One command would have caught the phantom-backlog error that cost a shipped-and-reverted change. |

## Recommended order

P0 tomorrow on a process with >40 min uptime, then **#5 before any further
tantivy work** — it could delete half the problem rather than optimise it.

---

# 2026-08-24 — P1 #5 ANSWERED: rollup indexes are read, used, and a 29x loss

The plan's open question was "are rollup tantivy indexes ever READ? — if nothing
`text_match`es them, half the coverage obligation is waste and the fix is one
line." The answer is worse than "never read", and the same one line fixes it.

## The instrument, because the one the TODO named cannot work

The TODO said to `EXPLAIN` a routed dashboard query and look for a tantivy
prefilter on the rollup `TableScan`. **`EXPLAIN` cannot answer this**:
`LogicalPlan::Explain` wraps the plan and the rewriter in `dml.rs` never sees
it, so every EXPLAIN shows a raw `otel_logs_and_spans` scan and no rollup
counter moves ([[tf_rollup_routing_works_plan_is_slow_2026-08-17]]). I ran it
anyway before remembering, and it produced exactly the false negative that
memory predicts — no rollup table anywhere in the plan, for *every* shape
including the unfiltered one that does in fact route.

What works is diffing counters around a REAL query on one connection. Two arms
over the SAME rows, verified equal first (1,616 both ways):

- `kind = 'server'` — `kind` is `tantivy: { indexed: true, tokenizer: raw }` on
  the source, so with `route_equality` on it is rewritten to
  `text_match(kind, 'server')` and consults the index.
- `upper(kind) = 'SERVER'` — opaque to the rewriter, and cannot prune anything,
  so it is the *pessimistic* control.

## The measurement (prod, uptime > 40 min, `rollup_min_contiguous_days = 30`)

Direct query on the tier, `bench/local/rollup_prefilter_ab.py`, 5 reps
interleaved:

| rep | `kind = 'server'` | `upper(kind) = 'SERVER'` |
|---|---|---|
| 0 | 9,421 ms | 296 ms |
| 1 | 9,331 ms | 428 ms |
| 2 | 8,529 ms | 345 ms |
| 3 | 8,206 ms | 1,841 ms |
| 4 | 10,885 ms | 283 ms |

**best-of-5: 8,206 ms vs 283 ms — 29x.** The tantivy arm recorded
`prefilter_attempts +1` and `prefilter_used +1` every rep; the control recorded
neither, which is what makes this an attribution rather than a correlation.
`index_opens` did not move, so the cost is not blob fetching — it is the search
plus the `id IN (...)` list it hands to the scan.

Not a `count(*)`-pushdown artifact: the same A/B on the real dashboard shape
(`GROUP BY time_bucket('1 hours', …)`, `sum(request_count)`, 46 identical
buckets) gives 7.4-9.3 s against 0.44-1.4 s.

## It is not confined to direct tier queries

The rollup rewrite plans its SQL through the same `SessionState`, so a routed
dashboard pays it on the rollup leg. Four real 7d queries on p4, each routing
(`rollup_hits_hybrid_total +1` on all four):

| filter | ms | counters |
|---|---|---|
| none | 7,240 | `hits_hybrid+1` |
| `kind = 'server'` | **14,484** | `hits_hybrid+1 prefilter_attempts+2 prefilter_used+1` |
| `status_code = 'ERROR'` | **10,548** | `hits_hybrid+1 prefilter_attempts+2 prefilter_used+1` |
| `resource___service___name = …` | 164 | `hits_hybrid+1` |

`attempts+2` is one per leg — rollup and raw. The service-name arm is the
control that proves the mechanism and not the filter is the cost:
`resource___service___name` is a dimension too but carries no tantivy config,
and it is the fast one.

**So a filtered dashboard that routes pays roughly +3 to +7 s over the
unfiltered one**, on exactly the 7d/14d/30d shapes the goal is about.

## Why it was ever built

`RollupSpec::synthesize` copies dimensions from the source with a struct update:

```rust
fields.push(FieldDef { nullable: true, ..f });   // inherits f.tantivy
```

while the `plain()` helper directly above it sets `tantivy: None` for every
identity and measure column. `kind` and `status_code` are indexed on the source,
so every tier inherited `indexed: true` — which is the whole reason rollup
tables are in `indexed_set()` at all, i.e. why they are **52% of the indexed
file population** the rest of this plan spent a day trying to drain. Item #6
("is the `..f` inheritance intended?") is answered by #5: no.

## The fix

`FieldDef { nullable: true, tantivy: None, ..f }`, with the measurement in a
comment beside it and a regression assert in
`synthesized_rollup_stores_a_generation_and_tdigest`. `FieldDef.tantivy` never
reaches Arrow or Delta (`TableSchema::fields`/`columns` build from name, type
and nullability only), so this is TimeFusion-side and needs no migration of any
live table.

It fixes two things with one line: the read-path loss above, and the build-side
obligation — four tiers leave `indexed_set()`, so the backfill, the reconcile
pass and the coverage census all stop counting a population nothing benefits
from.

## Pre-registered, so the aftermath is not misread

1. **The census will collapse, and it is NOT the drain working.** `sealed`,
   `older` and every uncovered count drop because 52% of the population left the
   definition, not because anything was indexed. State the population before
   comparing to any number earlier in this document
   ([[tf_tantivy_census_double_counts_2026-08-23]]).
2. **Rollup manifests and blobs become orphans.** Reconcile only visits tables
   in `indexed_set()`, so nothing will GC them — the `otel_metrics` precedent
   (§Session close item 5), but thousands of blobs rather than ten. They must be
   backed up and deleted from object storage by hand after the deploy, or
   reconcile taught to sweep tables that have manifests but are no longer
   indexed. Tracked as follow-up; deliberately not in this diff.

## P0 verification, from the same 90 minutes of logs

| # | Pass condition | Result |
|---|---|---|
| 1 | no `exit 137` after several cycles | **PASS** — every container transition in the window is a deploy, none an OOM kill |
| 2 | `skipped_today=0` on the rollup tiers | **PASS** — 0 on all four (was 690); `otel_logs_and_spans` still skips 5-6, which is the intended half |
| 3 | `tantivy_reconcile_gc_deferred` absent or small | **PASS** — absent over 90 minutes |
| 4 | `older` drains | **UNSETTLED** — and now unmeasurable against the old population, see below |

`tantivy_uncovered_files = 6,315` is the last reading under the OLD definition.
Prod index blob counts at the same moment, which is the population about to
change: `otel_logs_and_spans` 10,874 objects / **441 GB**, the six rollup
prefixes **6,398 objects / 428 MB** combined. So the tiers are 37% of index
objects and 0.1% of index bytes — the obligation they impose is per-file build
overhead (~1.75s each), not storage.

## What this demotes

The reconcile cadence defect found earlier today is real but second priority.
`spawn_cron_job` skips an overlapping tick **for the whole job**, so a spans pass
longer than 15 minutes blocks every other table's next tick too:
`Tantivy reconcile job run still in progress after 600s (skips=2)` at 11:00 and
11:15 against a 10:45 start — one slot in 45 minutes where the 22:15 sizing
assumed four an hour. `buffer_unordered(3)` fixed slow-blocks-fast *within* a
tick and moved the starvation to *between* ticks.

**How chronic it is, is not established, and prod cannot answer it right now.**
The 11:45 and 12:00 passes each served all five tables promptly — but they ran in
*different containers* (`oraxglu2ny21`, `xx3yvnnzuqx7`), so each container got
exactly one tick before being replaced and neither could have skipped. That is
P2 #7 (deploy cadence vs unit length) confounding the measurement, not evidence
either way. Re-measure on a container with >45 min uptime, and after the
population is 37% smaller — a shorter spans pass may close the gap by itself
before any per-table tick gating is worth building.

## Pass condition for the fix, registered before the deploy lands

Shipped as `8698271`, deployed in `3700730`. On a container running that image,
re-run the two arms and require both:

1. `bench/local/rollup_prefilter_ab.py` — the `kind = 'server'` arm must fall to
   the control's range (~0.3-1.8 s) and record `prefilter_attempts +0`. A
   non-zero attempt means the tier is still in `indexed_set()`.
2. The routed 7d dashboard with `AND kind = 'server'` must fall from 14,484 ms
   toward the 7,240 ms unfiltered baseline, with `prefilter_attempts +1` rather
   than `+2` — the remaining one is the raw leg, which this change does not
   touch and should not.

If (1) passes and (2) does not, the rollup leg was not the cost and the raw
leg's prefilter is; that is a different fix and this one still stands on the
build-side saving alone.
