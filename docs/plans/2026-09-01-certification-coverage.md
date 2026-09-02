# Certification coverage: the producer is pointed at the wrong data

2026-09-01, after deploy 19.

## What the split counters settled

Deploy 19 split the certified-skip refusal so the two causes could not hide in
one number. Prod, ~40 min after it went live:

| counter | value | reads as |
| --- | --- | --- |
| `cert_skip_blocked_no_stats` | 0 | statistics are present on every file |
| `cert_skip_blocked_overlap` | 128 | every certified file has an uncertified neighbour |
| `cert_skip_files` | 0 | nothing has ever skipped |
| `dedup_denied_never_certified` | 164 of 177 denials | **the partition had no certification at all** |
| `dedup_denied_by_leg` | 0 | no scan ever reached a whole-partition grant |

`no_stats = 0` kills the stats-poisoning hypothesis outright. `by_leg = 0` plus
`never_certified = 164/177` says the 128 overlap blocks all came from the
per-file path, on the few partitions holding any evidence at all.

**So neither contiguity nor the isolation rule is the first-order problem.
Coverage is.**

## Why coverage lands where queries do not

Certification has only ever been a side effect of dedup rewrites: ~21-minute
units, age-ordered, 2,573 pending. Evidence therefore accrues on old backlog
dates while queries read recent windows. Widening the containment test — the fix
I was about to build — is refuted by this data: a perfectly contained slice on a
date nothing queries is still worth nothing.

The code already says the other half of it, at `record_clean_slice`:

> whole-day coverage under an unchanged fingerprint never happens on a live
> table (`cert_granted_total` = 0 since 2026-08-20)

Today's date churns, so its fingerprint moves and it can never be granted whole.
A 14-day window is **13 sealed dates plus today** — and sealed dates take no
writes, so their fingerprint is stable and a grant sticks.

## The grant granularity that matters

Verified in `scan_delta_partitions`: certified files are returned as
`certified_plans` and unioned **above** `DedupExec`. A per-file skip therefore
routes those files *around* the operator — it does not remove it. The structural
term only disappears when the deduped leg is empty, i.e. when every file in the
scan is certified.

**Scattered per-file wins cannot move the 14-day latency; full-window coverage
can.** This is what discriminates between the candidate fixes.

## The lever: certification needs a proof, not a rewrite

`probe_dup_bins` already GROUP BYs the dedup keys over an entire
`(project, date)` and returns the bins whose group count exceeds one. **An empty
result is a whole-date cleanliness proof** — the same predicate a zero-drop
rewrite establishes, at key-only cost, writing nothing. A local bench put a
key-only probe at ~200x cheaper than a full read.

That proof is computed today and thrown away: `batch_probe_classify` keeps only
the per-bin classification.

### What dedup actually spends its time on

Prod, same window, from the `work.*` counters:

| operation | worker_secs | share | rows scanned | rows dropped |
| --- | --- | --- | --- | --- |
| Dedup | 7,843 | **96.5%** | 454,596,841 | 3,782 |
| SealedConsolidation | 49 | 0.6% | 555,789 | — |
| HotPacking | 25 | 0.3% | 440,120 | — |
| BaseRollup | 205 | 2.5% | 0 | — |

**A duplicate rate of 0.0008%.** Dedup scans 454 million rows to remove 3,782 —
it is overwhelmingly a cleanliness *proof*, not a removal. Paying full-rewrite
price for a proof a key-only aggregate can produce is the whole inefficiency,
and it is 96.5% of the maintenance fleet.

### Step 1 (built) — certify from the probe already running

Feed the empty-probe verdict through `record_certification` with
`(dropped = 0, complete = true)`, capturing the file list **before** the probe so
the fingerprint compare can still reject a grant if anything commits while it
runs. Reusing the existing recorder rather than a second rule is deliberate — the
soundness argument stays in one place.

The batch probe is already ordered recent-first, so its verdict lands where reads
are.

### Step 2 (next) — probe the sealed dates nothing enqueues

The batch probe only visits `(project, date)` groups with **queued dirty bins**.
Sealed dates that were fully processed have none, so they are never probed and
never certified — and they are exactly the 13/14 of a dashboard window. Extend
the candidate list with uncertified dates inside the query window, bounded per
tick and ordered newest-first.

### Not doing: flush-time certification

Sound only if a flushed file is internally duplicate-free, which is unestablished
— client retries inside the 10-minute bucket could put duplicate keys in one
file. Cross-file duplicates are already handled by the overlap rule. If pursued,
the flush has the batch in memory and a distinct-count over the dedup keys would
make the grant conditional rather than assumed. Left for after step 2, and it is
the churning-hot-date case, not the window case.

## Pre-deploy baseline (read immediately before shipping step 1)

| counter | value |
| --- | --- |
| `cert_granted_total` | **0** (and 0 since 2026-08-20) |
| `dedup_skipped` | 0 |
| `cert_slice_files_proved` | 2,066 |
| `pending_dedup` | 2,473 |
| `work.Dedup.progress_rows` | 1,971,371,644 |
| `work.Dedup.rows_dropped` | 8,081 |
| `work.Dedup.worker_secs` | 17,782 |

Duplicate rate over the longer window: **0.0004%** — nearly two billion rows
scanned to remove eight thousand.

`cert_granted_total` is the number the deploy has to move. It has never been
non-zero, which means `DedupExec` has never once been eliminated from a plan.

## Deploy 21 result: a null, and the null names the real hook

`cert_granted_total` stayed **0** after step 1 shipped. The refusal counters say
the grant was never *attempted*: `cert_refused_fp_moved`, `_empty` and
`_incomplete` are all 0, and the 2 `_dropped` cannot come from the new call site,
which passes `dropped = 0` as a literal.

My first explanation — "the probe ran and always found a duplicate somewhere in
144 bins" — was **wrong**, and the counters refuted it:

| counter | value |
| --- | --- |
| `dirty_bin_batch_probe_clean_total` | 0 |
| `dirty_bin_processed_total` | 0 |
| `dirty_bin_queue_depth` | 6 |
| `pending_dedup` | 2,427 |

**The batch probe never executed.** A probe group needs ≥2 queued bins in one
`(project, date)`; with six bins spread across the fleet, no group forms, and
`dedup_dirty_bins_for_table` then returned early on the empty queue.

So the dirty-bin queue was the wrong hook entirely. It is nearly empty while the
coordinator's `pending_dedup` sits at 2,427 — **they are different queues**, and I
had been reasoning about certification as though draining one implied the other.
Step 1 was necessary but could never fire alone.

Step 2 does not depend on the queue: it enumerates candidate dates from the Delta
snapshot. Its host runs unconditionally (`timefusion_dirty_bin_dedup_enabled`
defaults true), which is the check that stops it repeating the repair fix's silent
no-op.

## What to read next, and how to tell the difference

Deploy 22 (`e2400a21`) ships the snapshot-driven producer. The counters
discriminate three outcomes, and they mean different next moves:

1. **`cert_granted_total` > 0** — the mechanism works. Move to whether whole
   *windows* get covered, then re-run the latency matrix.
2. **`cert_granted_total` = 0 but `cert_refused_fp_moved` > 0** — the grant is now
   being ATTEMPTED and losing a race with concurrent commits. That is progress
   over deploy 21, and the fix is to certify dates that are genuinely quiescent
   (older than the maintenance write frontier), not to retry harder.
3. **Every `cert_*` counter still 0** — the producer still is not reaching prod's
   data. Stop adding producers. Instrument the candidate enumeration directly
   (how many dates it returns, and why the rest were filtered) rather than
   guessing a fourth time.

The discipline that has paid tonight: **a counter that stays zero is not a small
result, it is a claim that the code never ran** — and `dirty_bin_batch_probe_clean_total`
proved exactly that after deploy 21, refuting a much more plausible story about
duplicate-bearing bins.

## Success criterion

After the sweep covers one busy project's full 14-day window: re-run the latency
matrix on that project, confirm the plan no longer contains `DedupExec`, and
confirm the 42–47 s point moves. That measurement is what the whole chain exists
for — anything short of it is a counter moving, not a query getting faster.

## Deploy 25 result: the main table cannot be certified at ANY granularity yet

With `run_certification_pass` finally reaching `otel_logs_and_spans`:

| counter | value | meaning |
| --- | --- | --- |
| `cert_granted_total` | **0** | not one whole-date grant |
| `cert_probe_declined` | 68 | every probe found duplicates |
| `cert_skip_blocked_overlap` | 7,611 | per-file path blocked too |
| `cert_skip_files` | 0 | nothing skippable |

**This corrects the headline claim I made earlier tonight.** I argued dedup is "a
proof, not a removal" because it drops 0.0008% of rows. But a partition needs only
ONE duplicate to be uncertifiable, so a tiny global rate spread thinly makes EVERY
partition dirty. Duplicates here are **sparse but ubiquitous** — the worst case for
certification, and the reason the auxiliary tables (genuinely clean) granted 437
while the main table grants nothing.

Both granularities are therefore closed:

- **Whole-date** — 0/68. Needs a partition with zero duplicates; there are none.
- **Per-file** — 7,611 overlap blocks, 0 skippable. A certified file may only skip
  if no uncertified file overlaps it in time, and on a dup-bearing date its
  neighbours are always uncertified.

### What this means for the plan

The read-path lever is **gated on the write path**, not on certification
scheduling. `DedupExec` cannot leave a plan for `otel_logs_and_spans` until the
duplicates are physically removed — which is the 2,290-task `pending_dedup`
backlog. Certification is not an alternative to draining it; it is what *becomes
available* once drained, and it will then hold the gain cheaply.

So maintenance throughput is the binding constraint after all, which is where the
night started. The certification work is not wasted — it is live, correct, proven
on real queries (`dedup_skipped_per_date` 0 -> 6 on auxiliary/clean partitions),
and it is the mechanism that converts a drained backlog into query latency. But it
cannot lead.

**Next, in order:**
1. Drain `pending_dedup` for the busiest projects' recent dates specifically —
   dedup ordering currently ranks by age, not by which dates queries read.
2. Re-run the certification probe on those dates; they should then grant.
3. Only then does the latency matrix move.

## The pivot: removal is the binding constraint, and the mechanism hinges on ONE number

Deploy 25 closed certification as a leading strategy. The work now is draining
duplicates so dates *become* certifiable. Certification then converts a drained
backlog into query latency cheaply — it holds the gain, it cannot create it.

### Why the discriminator cannot be measured from outside

I tried to count physical duplicates with psql:

```sql
SELECT count(*) FROM (SELECT timestamp, id FROM otel_logs_and_spans
  WHERE project_id=... AND timestamp >= ... GROUP BY timestamp, id HAVING count(*) > 1)
```

It returned 1,730 dup groups (all inside ONE hour) on one date and **0** on three
others — while the in-process probe declines every date. The query is unreliable
because **the read path applies `DedupExec` (bounded mode) and collapses
duplicates before the aggregate sees them**. Only the maintenance probe observes
physical rows.

So the number is instrumented instead: `cert_declined_dirty_bins /
cert_probe_declined` = mean dirty bins per dirty date, out of 144.

### The decision it drives

- **Low ratio (1-3 of 144)** — duplicates are CONCENTRATED. A date is cleanable
  with surgical bin-scoped rewrites in minutes, and a busy project's 14-day window
  could drain in one night. This makes ordering almost irrelevant and makes the
  whole-date unit the bug.
- **High ratio (dozens)** — duplicates are SPREAD. The full-date unit is
  unavoidable at ~21 min each, and the lever is ordering: dedup currently ranks by
  AGE, so it works the oldest backlog while queries read the newest dates. A
  scoped boost (dedup tasks inside the top-K-by-file-count projects' last-14-day
  window jump the queue) beats a global re-rank.

Do not build either until the ratio is read.

### Deploy discipline from here

Units average ~21 min and die to process exit; ~8 restarts tonight each killed
in-flight dedup work. This instrument is the LAST deploy of the session. After it,
the most valuable thing for `pending_dedup` is to stop deploying and let the fleet
run quiet.

### For the morning, not for tonight

A 0.0004% duplicate rate that still touches every partition smells like **client
retries landing in separate files**. The durable 100x fix is preventing them at
ingest — a dedup-key check within the MemBuffer's 10-minute bucket — so new dates
are born certifiable and neither the rewrite nor the probe is needed for them.
That is a design discussion with the user, not a 1 AM build.

### The concentrated case needs NO new machinery

`stage_dedup_partition_range` already takes `DedupRangeOptions { slice:
Option<TimeSlice>, .. }` — bin/slice-scoped rewrites are supported today. So if
`cert_declined_dirty_bins / cert_probe_declined` comes back low, the fix is
scheduling (hand the unit a narrow slice around the dirty bins the probe already
identified), not a new code path. The probe returns exactly those bin ids and
currently discards them after the decline.

That makes the concentrated case a small, low-risk change — which is the main
reason the ratio is worth one deploy to measure.

## The ratio came back: duplicates are SPREAD (26.6 of 144 bins)

Deploy 26, after ~15 min on `otel_logs_and_spans`:

| counter | value |
| --- | --- |
| `cert_probe_declined` | 14 |
| `cert_declined_dirty_bins` | 373 |
| **mean dirty bins per dirty date** | **26.6 of 144 (18%)** |
| `cert_granted_total` | 4 (non-zero — some dates ARE clean) |

**The first sample read 1 decline / 1 dirty bin and looked decisively
"concentrated".** Acting on it would have built bin-scoped scheduling for a
workload that does not have that shape. Sample size, not reasoning, caught it —
the same lesson as every other refutation tonight, and worth more than the result.

### Which branch this selects

The **spread** branch. Slice-scoping 26 scattered bins is not a narrow slice, and
worse, it does not save proportional work: `stage_dedup_chunk` re-reads every file
the chunk touches (`partition_filter`), and files span bins — so rewriting 18% of
the bins can still touch most of the date's files. The whole-date unit is
approximately the right unit after all.

**So the lever is ORDERING, not unit size.** Dedup currently ranks by AGE
(`starved`, 3-31d), so it works the oldest backlog while queries read the newest
dates. The change is a scoped boost: dedup tasks whose (project, date) falls in
the top-K-by-file-count projects' last-14-day window jump the queue. Prefer that
to a global re-rank — `claim_next` has a history of incidents
(`hole_rank` stall, `starved` demoting the biggest cells, SUPERSEDED vetoes).

### Why this is NOT being shipped tonight

Dedup rewrites DELETE ROWS. It is the one area where a wrong change costs data
rather than latency, and CLAUDE.md mandates `sim` -> `run-unit` -> staging for
scheduler changes precisely here. Five hypotheses were refuted tonight; shipping
an ordering change at 02:00 on one counter reading, unsupervised, trades a bounded
upside against an irreversible downside.

Equally: ~8 restarts tonight each killed in-flight ~21-minute dedup units. With
the diagnosis complete, **the highest-value action for `pending_dedup` between now
and morning is to stop deploying.**

### Morning checklist

1. `timefusion sim <journal>` with the scoped boost; confirm the newest-date
   dedup units actually get claimed sooner without starving the tail.
2. `timefusion run-unit --op Dedup` on a busy project's recent date to price one
   unit, so drain time is predicted rather than discovered.
3. Then ship the boost alone, and verify: a deduped date should certify within one
   cron cycle (the rewrite moves the fp, which invalidates the decline memo and
   triggers an automatic re-probe).
4. Separately, the durable 100x fix: prevent duplicates at ingest via a dedup-key
   check inside the MemBuffer's 10-minute bucket, so new dates are born
   certifiable and need neither rewrite nor probe. Design discussion first.

## The ordering defect, measured — and the obvious fix REFUTED locally

`months_old_history_outranks_the_dates_dashboards_read` (in
`maintenance_coordinator.rs`) pins it as a passing test:

- `STARVATION_MICROS` is 3 days, so of a 14-day dashboard window only days 1-3
  escape the starved lane.
- Starved work drains OLDEST-first, so day 90 outranks day 4 and day 10.
- Capacity goes to data nobody queries before reaching the window everybody does.
  Prod 2026-09-01: `pending_dedup` ~2,250 with a 1.7M-row/day project at 0 of 8
  sampled dates certified.

### Refuted: raising the threshold

Raising `STARVATION_MICROS` 3d -> 15d, so the measured 7d/14d windows order
newest-first, **is wrong and makes it worse**. `starved` is `u8::MAX` when NOT
starved and smaller-is-better as it ages, so **any starved task outranks any
non-starved task**. Raising the threshold EVICTS the window from the privileged
lane rather than protecting it.

The local suite caught it in minutes: **9 failures**, including my own new test
plus `damage_outranks_work_inside_the_starvation_window`,
`sealed_work_ages_out_of_starvation_without_becoming_oldest_first` and
`the_starvation_window_demotes_the_biggest_debt_when_it_is_young` — eight
invariants that constant is load-bearing for. Reverted, and recorded at the
constant so it is not retried.

### The actual lever: bound the starved lane's SHARE, not its threshold

`claim_next` already does exactly this for sealed work — a `claim_tick` counter
reserves one claim in two, halved to one in four while the frontier is behind. The
same shape applies: reserve a fraction of claims for tasks whose slice falls
INSIDE the query window, chosen without reference to `starved`, so history cannot
monopolise the queue while remaining able to drain.

**Do not implement this blind.** That reservation share is the single most
dangerous dial in the file: raising the sealed share to 3-in-4 OOM-killed prod at
**124.9 GB anon RSS** (2026-08-17), because sealed partitions are far larger than
frontier slices and the same permit count then admits far more bytes. A third lane
changes the same fan-in envelope.

Ladder, per CLAUDE.md: `timefusion sim <journal>` first — it is IO-free and
replays a real prod queue on virtual time, so it answers "does the window drain
without starving the tail" in seconds. Then `run-unit`, then staging, then prod.

## The A/B on the REAL prod journal: the queue is CAPACITY-bound, not order-bound

77,034 tasks fetched read-only from the running container
(`docker cp c9e33d1e7ccb:/app/data/timefusion/.timefusion_meta/maintenance_tasks.json`),
replayed through `timefusion sim --hours 24`. One variable, isolated by checking
out `maintenance_coordinator.rs` at the pre-change commit.

| metric | baseline | +window reservation | delta |
| --- | --- | --- | --- |
| `pending_end` | 10,610 | 10,576 | -34 (0.3%) |
| Dedup completions | 8,183 | 8,264 | **+81 (1.0%)** |
| executions | 27,175 | 27,232 | +57 |
| `frontier_lag_secs_max` | 86,978 | 86,518 | -460 (0.5%) |
| `min_contiguous_days_end` | 0 | 0 | none |
| `hours_to_contiguous_14` | None | None | none |

**The reservation is real, safe and worth ~1%. It is not the lever.** The 14-day
window never becomes contiguous under either policy.

### What the numbers actually say

`pending_start` 22,162 and 24 simulated hours produce 27,175 executions, and the
backlog only halves. `frontier_lag_secs_max` is 86,978 s — a full DAY behind, in
both arms. **Reordering cannot fix a throughput deficit.** No claim policy makes
27k executions cover 22k+ tasks plus everything minted while they run.

So the goal — "keep up with the data streams" — is a CAPACITY question:
throughput per unit, or fewer units, not better ordering. The ordering defect is
real (`months_old_history_outranks_the_dates_dashboards_read`) and worth ~1%; it
should not be mistaken for the answer.

### Consequences for the 10x/100x target, in priority order

1. **Fewer units.** Prevent duplicates at ingest — a dedup-key check inside the
   MemBuffer's 10-minute bucket — so dates are born certifiable and need neither
   a rewrite nor a probe. This deletes work rather than scheduling it, and it is
   the only item on this list that scales to 100x. Design discussion first.
2. **Cheaper units.** A unit averages ~21 min and the fleet is ~96% Dedup. The
   2026-09-01 batch-size fix (bytes, not rows) took one file 39.4s -> 20.3s;
   that class of work compounds where ordering does not.
3. **Ordering.** ~1%. Ship it when something else is already deploying; it does
   not justify a restart on its own (~8 restarts tonight each killed in-flight
   21-minute units).

### Method notes, both errors caught here

- The first "baseline" was invalid: the change had already been COMMITTED, so
  `git stash` stashed nothing and both arms ran the same code. A/B by
  `git checkout <pre-commit> -- <file>`, and verify with a marker
  (`grep -c window_turn` must be 0 in the baseline build).
- `synth:whale` cannot validate scheduling: it is a fixed 813-task backlog that
  always drains, so no reservation ever binds. Only the real journal reproduces
  contention.

## THE CAPACITY LEVER: dedup is 8.7x faster on sorted input

`cargo bench --bench dedup_benchmarks`, 1M rows, zero duplicates (prod's case —
the measured rate is 0.0004%), `greatest` mode (prod's mode, from the EXPLAIN:
`DedupExec: keys=[timestamp, id], mode=bounded[timestamp]/greatest`):

| input | time | throughput |
| --- | --- | --- |
| shuffled | 396.81 ms | 2.52 Melem/s |
| **sorted** | **45.64 ms** | **21.91 Melem/s** |

**8.7x.** On the operation that is ~96% of maintenance worker time, against ~1%
from the claim-ordering work. This is the "cheaper units" item the prod-journal
A/B identified as the actual lever, quantified.

Next question, and the one that decides whether this is reachable: does the
maintenance rewrite path (`stage_dedup_partition_range`) feed `DedupExec` sorted
input, or does it shuffle? The read path's `bounded[timestamp]` mode suggests it
already exploits timestamp ordering; the rewrite path is where the 96% is spent
and is what to check first.

Related and already known: sorted footers were being lost
([[tf_sort_skip_kills_footer_ordering_2026-08-01]], `SORT_SKIP_BYTES` vs the
compaction target). If files land unsorted, every later dedup pays the 8.7x.
