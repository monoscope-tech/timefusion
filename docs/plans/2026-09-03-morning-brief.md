# Morning brief — 2026-09-03

Three behaviour changes shipped and stable for 5+ hours, two benchmarks and one
simulator fix added, and **the 10x question answered**. `dedup_failed_total`
stayed 0 throughout.

**This document was written incrementally through the night and contains its own
corrections in place.** Six of my claims were retracted after checking them, and
the retractions are kept next to the claims rather than deleted — the reasoning
is often the useful part. Where a section is superseded it says so.

## THE HEADLINE — corrected 2026-09-03 morning, after measuring the duration model

**We are SATURATED, not lane-inverted.** With the sim's duration model rebuilt
from 676 real `maintenance_task_finished` events, 10x load produces **the same
throughput as 1x**:

| 12 h, measured durations | 1x (20 active) | 10x (200 active) |
|---|---|---|
| **executions** | **1,691** | **1,712** |
| pending | 21,288 -> 3,731 | 21,288 -> **31,614 (grows)** |
| BaseRollup / Dedup | 682 / 228 | 684 / 235 |

The fleet does **~141 units/hour regardless of load** (production measures
~162/hr, so the model is now within ~13%). At 10x the queue simply grows.

**This RETRACTS the "lane inversion" headline below.** That result — dedup
completions falling 30% while BaseRollup took 7.7x more — was an artifact of the
old model pricing BaseRollup at 5-60 s when its real mean is **571 s**, a ~16x
under-estimate. With measured durations the lane mix does not shift at all.

**And it retracts "a dedup unit costs ~7x a rollup unit".** Measured means are
**Dedup 541 s vs BaseRollup 571 s**; worker-seconds split 43.7% / 46.7%. Dedup is
marginally CHEAPER. That figure came from comparing a post-widening lane against
a constant last updated two weeks before the widening shipped — the same class of
error as citing a cohort that never reaches the gate.

**What the real constraint is:** total maintenance throughput, dominated by a
bimodal tail. **~65-70% of units in both lanes finish at ~0 s** (they find no
work); the remaining third run 1,200-2,400 s. Capacity is set by that tail, and
it is lane-agnostic — which is why giving dedup more cycle slots bought only
+14%, and why the answer is unit COST, not scheduling.

### The three failing tests are a REAL result, not a calibration artifact — verified

My first worry was that the new model adds an extra `rng.next()` per call, which
shifts the whole seeded stream and could break seeded tests for trivial reasons.
Isolated it directly: **old durations + one extra dummy draw = 20/20 pass.** So
the stream shift is harmless and the three failures are caused by the duration
MAGNITUDES.

What each one now says, with production numbers in the model:

| test | old premise | with measured durations |
|---|---|---|
| `frontier_keeps_up_at_13_projects_and_diverges_at_10x` | 13 projects hold the frontier ("~15k/day of small-unit capacity") | **they do not** — that capacity figure assumed small units; a third are 1,200-2,400 s |
| `a_floored_whale_shreds_to_the_minute_without_the_guard` | guard OFF shreds to >1,000 units at the floor | shred is smaller — fewer units execute per horizon |
| `the_floor_guard_declines_above_the_floor_and_the_shred_stops` | guard ON: `units_at_min_slice == 0` | **8 reach the floor** |

The third is the one worth attention: it is a near-correctness assertion about
the split-floor guard, and under realistic durations units queue longer, get
split more, and some now reach `MIN_SLICE_MICROS` anyway. That may be a genuine
weakness in the guard that the optimistic model was hiding — or the assertion may
simply be too absolute for a slower fleet. **I am not deciding that unilaterally**;
it is the kind of question where re-baselining a test could erase a real signal.

**Status:** the recalibrated model is committed on `fix/sim-durations`, **not
pushed** — three `maintenance_sim` tests encode the old premise (notably
`frontier_keeps_up_at_13_projects_and_diverges_at_10x`, whose "~15k/day of
small-unit capacity" assumption is exactly what these measurements refute).
Deciding what those tests should now assert is a real question about the system,
not a mechanical fixup.

## The superseded headline, kept for the reasoning

**We do not keep up at 10x, and it is a UNIT COST problem, not a scheduling or
capacity one.** At 10x active streams the fleet does 4x the executions and
completes **30% FEWER dedup units** — BaseRollup crowds out the one lane
certification depends on. I tested the obvious scheduling fix (double dedup's
cycle share) and it bought **+14%, not 2x**, because a dedup unit occupies a
worker **~7x longer** than a rollup unit. See *THE 10x ANSWER*.

Tonight's three fixes unblock units; they do not make units cheaper. That is the
gap.

## What shipped

| # | change | lane | status this morning |
|---|---|---|---|
| 1 | repair budget sized by the target file, in its own constant | repair | **Live.** Permit contention down ~11x over a 5 h quiet process, and 95% of remaining bounces are the oversized class (was 74%) — but see *Change 1 UNDER-DELIVERED*: the budget still cannot hold two REAL units, and the decoupling fix is the **top morning decision**. |
| 2 | maintenance/coordinator pool USAGE exposed, denominator corrected | all | **Live.** Coordinator pool peaks at 47–70%, not the 0% a single reading suggested. |
| 3 | admission grants the full cap at half-free, not only at an idle pool | dedup + base_rollup | **Live**, and the gate defect is real and tested — but its *benefit* is **unproven**: the 297-unit cohort I cited as motivation never reaches that gate. See the correction and resolution below. |
| 4 | split-floor guard priced on the unit's own estimate, not a synthetic constant | all splitting | **Live** (`ca60cc9`). Real defect — prod had 147 live units at the 60 s floor and 8,595 completed there — but currently DORMANT: the capacity-failure path that mints them is not firing. |
| 5 | sim duration model rebuilt from 676 production events | tooling | **Deploying.** The old numbers predated the dedup-key widening and priced BaseRollup 16x too cheap; they are what produced the retracted 10x conclusions. |
| 6 | sim calls the real split guard instead of transcribing it | tooling | **Deploying.** Removes a live drift class — the copy is why the sim never reproduced defect 4. Four tests reconciled against measurement. |
| 7 | repair's pool holdback derived from its decoded budget | repair + hot-tail | **Deploying.** Decouples two things one constant was doing. Costs a light permit (K 4 -> 3), which was NOT the predicted direction — see the K baseline section. |

**Not pushed, deliberately** (a code push restarts prod, and these had no
overnight urgency): the two `timefusion sim` fixes. **Both matter** — without
them every scale run measures roughly 1x.

Ready to merge as branch **`sim/ready`**: rebased onto current master, one file
(`src/maintenance_sim.rs`, +62/-4), `cargo fmt --check` clean, `cargo lint` clean,
20/20 `maintenance_sim` tests pass. Two commits:

```
fix(sim): mint only from INGESTING streams, so arrivals match production
fix(sim): --streams means N INGESTING streams, so a scale run actually scales
```

The failed cycle experiment (Dedup 1/10 -> 2/10, +14%) was deliberately NOT
committed — it is documented in *THE 10x ANSWER* as a negative result and should
not ship.

## ~~THE TOP LEVER~~ — RETRACTED ONE HOUR LATER, and here is the falsifying number

**I published the section below and then tried to falsify it, which is what I
should have done first.** The claim was that
`TIMEFUSION_LANDED_SKIP_ENABLED` is the number-one 10x lever. It is not.

```
replay_rows        0      <- WAL replay re-inserted NOTHING on this 5.5 h process
landed_skips       0
landed_skipped_rows 0
```

And the last five prod shutdowns carry **no error string** — they are clean
deploys, not crashes. The landed skip only ever fires on a **dirty** boot, so on
the current restart pattern **it would never fire at all**.

**What survives, and what does not:**

- **Survives:** certification declines 231:1 on dirty bins, and that IS the
  binding constraint on the 10x chain. The table below stands.
- **Does not survive:** that the flag would fix it. The 58%-self-inflicted figure
  came from a sampled prod file — those are duplicates ALREADY ON DISK from an
  earlier OOM-crash era. The flag prevents NEW ones during unclean exits; it does
  not remove existing ones, and prod is currently exiting cleanly.

So the flag is **insurance against a future OOM era**, exactly as my own note
called it ("the flag is OOM insurance"), and worth enabling on that basis — but
it is not what clears the 12,716 dirty-bin declines. **Clearing those requires
actually running dedup over them, which puts the top lever back on dedup
throughput and unit cost** — where the 10x section already put it.

**Method note for next time:** I ranked a lever from a causal story
(replay → dirty bins → declines) without checking whether the first arrow was
currently firing. One counter (`replay_rows`) falsified it in a single query.
Check that the mechanism is ACTIVE before ranking it, not after.

## The original section, kept for its measurements: certification declines on dirty bins

The 10x chain ends at certification — cheaper dedup needs certification, and
certification needs dedup. Measured on the 5.5 h quiet process, certification is
running but barely covering anything:

| metric | value |
|---|---|
| `cert_granted_total` | 55 |
| **`cert_declined_dirty_bins`** | **12,716** — a 231:1 ratio against grants |
| `cert_slice_files_proved` / `unproven` | 1,781 / **22,300** (only **7.4%** proved) |
| `cert_skip_blocked_overlap` | 42,703 |
| `cert_dwell_p50` | 81,674 s = **22.7 h** |

**The dominant decline is DIRTY BINS by two orders of magnitude.** Certification
is not blocked by contiguity, plumbing, or fingerprints
(`cert_refused_fp_moved` = 0, `cert_refused_incomplete` = 0,
`cert_skip_blocked_no_stats` = 0) — it is blocked because the data genuinely
still contains duplicates.

And **58% of those duplicates are byte-identical rows our own WAL replay
re-inserted** (`docs/plans/2026-09-02-stop-manufacturing-duplicates.md`). So the
causal chain is:

```
WAL replay re-inserts rows  ->  bins are dirty  ->  certification declines
  ->  DedupExec stays in every plan  ->  dedup must keep re-proving the partition
  ->  dedup costs ~7x a rollup unit  ->  10x starves dedup  ->  coverage never builds
```

**`TIMEFUSION_LANDED_SKIP_ENABLED` cuts that chain at its head, and it is still
defaulted OFF.** It is already shipped, tested, and instrumented
(`wal.landed_skips`, `wal.replay_rows`). It only ever fires after an unclean
restart, so it costs nothing on a clean boot — and prod restarts often enough
(three times last night alone) that it would fire regularly.

**This reorders the levers I gave earlier.** I ranked it third behind "make dedup
cheaper"; the certification numbers say it is FIRST, because it is the only one
that reduces the amount of dedup that must happen at all rather than making each
unit faster. The caveat from its own design doc stands: validate in staging, not
prod, because the skip cannot be induced on a read-only prod host.

## The four maintenance lanes you named

| lane | verdict |
|---|---|
| **Sorting / HotPacking** | **Fine.** Zero live units — keeping up completely. |
| **Rollups** | Derived healthy (1.1 h median). Base is the lane that *wins* at 10x, to everyone else's cost. |
| **Dedup** | The binding constraint at 10x, and the one certification depends on. |
| (repair, not in your list) | Serialized by a byte budget; change 1 improved it, decoupling would finish it. |

## Lane survey — two lanes are already fine

| lane | live units | age median | verdict |
|---|---|---|---|
| **HotPacking** | **0** | — | **keeping up completely, no backlog at all** |
| derived_rollup | 52 | 1.1 h | healthy |
| dedup | 1,552 | 22.3 h (p90 386 h) | change 3 targets the stuck tail |
| base_rollup | 320 | 169 h (7 d) | change 3 targets it |
| repair | 314 | 412 h (17 d) | change 1 |

## Change 1 UNDER-DELIVERED — the measured gain is zero, and here is why

**This is the top decision for the morning.** The change is live and behaving
exactly as written (`budget_mib=6144`, and units now report their real size
instead of every one clamping to 1280). But `repair_rewrite_permit_busy` is
**1.35/min — identical to the pre-fix baseline** (243 in 3 h before, 88 in 60 min
after), because no two real units can share the new budget.

The `want_mib` distribution from 60 minutes of live logs (88 events):

| want_mib | count |
|---|---|
| 3,176 | 1 |
| 3,391 | 2 |
| 3,574 | 10 |
| 3,725 | 2 |
| 4,544 | 7 |
| 5,203 | 1 |
| **6,144 (clamped, i.e. ≥6,144)** | **65 — 74%** |

**The smallest real unit is 3,176 MiB; two need 6,352 > 6,144.** I sized the
budget as `2 x COORDINATOR_HOT_TARGET_BYTES x 12` on the reasoning that a repair
unit is one file and compaction targets 256 MiB files. That is true of files
compaction *produces*; the files repair is *queued on* are legacy/whale files far
above the target. So "2 target-sized files" is not two real units — it is barely
one.

**Do not simply raise `REPAIR_REWRITE_TARGET_FILES` 2 → 4.** Two reasons:

1. **It is not validated at that magnitude.** Two clamped-class units are
   ≥12.3 GB decoded through the 8.4 GiB coordinator pool *with light sorts
   alongside*. The measured boundary from the fleet ladder is **1.79x
   decoded-to-pool passes, 2.39x fails**; that proposal lands in the untested gap
   where the cliff is. (Tonight's 2-worker PASS was at 2,756 MB per worker — the
   wrong magnitude to justify it.)
2. **The constant does double duty.** It is also `light_optimize_k`'s holdback,
   so 2 → 4 silently takes light K from 4 to 2. K collapsing is the 2026-09-01
   HotPacking outage class. Zero HotPacking backlog says K=2 is probably fine —
   but "probably fine, via a side effect of a constant that should not have that
   job" is the drift-class bug this codebase keeps re-documenting.

**The right fix is to decouple them**, and it is a design change for a waking
human: the semaphore stays denominated in **decoded bytes** (what admission
prices), while the pool holdback is priced in **pool bytes** at the measured
ratio. Tonight's benches give that ratio — a spilling sort needs ~0.16–0.21x its
decoded size, so the semaphore over-states repair's memory need several-fold.
This is the recurring theme of the whole investigation: *a budget must be
denominated in the unit that actually costs.*

Caveat on the table above: 60 minutes of permit-busy events is a **bounce log,
not the queue** — a held semaphore bounces small units too, so "nothing below
3,176 in 88 events" is real signal but wants a longer window before it is quoted
as the queue's distribution.

### Supporting bench, and why it does NOT close the question

Fleet ladder on the 431 MB whale file, one shared 8 GiB pool (disk verified at
92 GiB free throughout, so unlike the earlier discarded run these failures are
real):

```
workers   secs   failed
 2         1.8      0
 6         8.2      0
12        18.8      0
16        21.1      6
```

Two concurrent sorts of a repair-magnitude file pass with enormous margin. **But
this ladder cannot be compared to the 204 MB one in decoded-byte terms.** It
sorts at 265 MB/s against the other file's 10 MB/s — this file has far fewer,
wider rows, so the `12x compressed` conversion (a prod *average*, not a per-file
truth) badly over-states its decoded size. Quoting "5.2 GB decoded per worker"
here would be inventing a number.

So it is supporting evidence that 2-way repair concurrency is not obviously
dangerous — **not** proof at the clamped class's real magnitude. The honest
morning experiment is 2 workers at a measured (not estimated) decoded size.

## Change 3 in detail — the boundary

`byte_bounded_units` splits until a unit **fits** `MAX_DECODED_BYTES`, so its
output piles up **at** that constant. Both admission sites then clamp their
request to it — deliberately: `split_time_task` declines to hash-shard in the
journal so an oversized unit can reserve the maximum and shard *itself*
internally, which bounds memory without minting units that each re-pay the
partition scan.

But `MAX * available / capacity` is strictly below MAX the moment one byte is
reserved. **So the request the design intends as "reserve the maximum and
self-shard" was the one request the gate could never grant.** Median size of the
stuck class is exactly the constant, minimum too — a boundary, not a
distribution.

ClickHouse grants its full merge cap at **8 free pool entries of 16 — half free,
not idle**. Ours granted it only at 100% free. That is the whole fix, one
divisor.

## Two local benchmarks, which is what changed the design

I was about to ship this paired with a bigger per-job sort slice. **Both halves
of that reasoning were wrong, and the benches are what showed it.**

1. `TF_BENCH_SLICE` — a **512 MB pool sorts 2,451 MB decoded**, 4.8x its own
   size. Floor is ~0.16–0.21x decoded; 384 MB is where it breaks. So
   "512 MiB per job = zero headroom" was wrong about what a spilling sort needs.
2. `TF_BENCH_PRODSHAPE` — **16 workers x ~490 MB decoded through one 8 GiB pool,
   0 of 16 failed.** The old fleet ladder suggested otherwise only because it
   hands every worker the *whole* file — 4.8x what a coordinator job is ever
   admitted for.

Both are committed and re-runnable.

## What to check this morning

- **Dedup/rollup (change 3):** re-fetch the journal. The exactly-512-MiB cohort
  (298 units) and the base_rollup 7.5 GiB cohort (94, pinned at the 1-minute
  minimum width) should be shrinking, and their max age falling.
- **Abort signal:** `Not enough memory to continue external sort` in journal
  retry reasons **rising above tonight's 247** → revert the one divisor in
  `occupancy_scaled_ceiling`.
- **`admission_busy` hot-loops** (attempts in the hundreds — one unit reached
  715) should stop accumulating.
- **Repair (change 1):** expect **no throughput change** — see the section above.
  Judge from the **Delta log**, not counters (restarts re-zeroed them). Large
  single-file rewrites per hour vs the **1.17/hr** baseline; the filter trap is
  that a large repair commits **1 remove → 2 adds**, so a 1-add-1-remove filter
  hides it. The change is still correct and worth keeping — units now express
  their true size instead of every one taking the whole semaphore — it just does
  not buy concurrency until the budget clears two REAL units.
- **`scan.dedup_full_set_pct`** — a file property, restart-proof. Was 8.7% at
  the start of the night.

## NEW — an unexamined DataFusion default is capping every maintenance spill at 100 GB

Found while watching the deploy, and it is the most 100x-relevant thing tonight.

```
WARN maintenance-worker: Light optimize staging failed for project=87576849…
  Resources exhausted: The used disk space during the spilling process has
  exceeded the allowable limit of 100.0 GB
```

`build_spill_runtime_env` (`database/write.rs:50`) constructs
`DiskManagerBuilder::default()`. **100 GB is DataFusion's own default
`max_temp_directory_size`; we never chose it.** So every maintenance spill —
dedup, rollup, packing, repair — is bounded by a number nobody in this codebase
picked, and the whale project is already hitting it on the light-optimize path.

Prod has room: the data volume is **1.8 T with 504 G available**. A 100 GB cap
against 504 GB free is not a safety margin, it is an accident.

**Why it matters for the 100x question specifically.** This is the same failure
shape as every other finding tonight — a fixed ceiling that a bigger tenant
crosses sooner — except this one is *not even ours*. It also fails in the most
expensive possible way: the spill limit is reached after the sort has already
done its scan and merge work, so the unit burns its full cost and then throws
the result away.

**Morning decision.** `DiskManagerBuilder::with_max_temp_directory_size(...)`,
sized from the volume rather than inherited. Not shipped tonight: it is a
behaviour change, the admission fix landed three minutes before I found it, and
stacking them would make neither attributable. Also note the local irony — my own
benching filled this laptop's disk to 100% tonight and faked a memory failure, so
whatever number is chosen should be derived from free space with headroom, not
set to "large".

## CORRECTION — the 512-MiB cohort is NOT admission-blocked. It is never CLAIMED.

**This retracts the headline of change 3's motivation.** The admission boundary
was real — the reproducing test fails on master and passes with the fix, and
that defect is worth fixing — but **it is not what is holding the 297 units I
used as its evidence.**

Fresh journal pulled 8.5 h after the first one, same cohort keyed by
(project, source, slice, op):

| | 17:07 | 01:41 |
|---|---|---|
| dedup live | 1,552 | 1,320 (**−232**, healthy work IS flowing) |
| >= MAX | 365 | 363 |
| **exactly 512 MiB** | 297 | **297 — identical** |
| **max age** | 412.0 h | **416.9 h** (+4.9 h — they aged, they did not run) |

And the decisive diagnostic, over those 8.5 hours and three restarts:

```
exactly-512 cohort: 297 before, 297 after, 297 SAME KEYS
attempts delta:  min 0   median 0   max 0      zero-delta 297/297
state: all Pending    retry_reason: all none    not_due: 0 of 297
```

**Zero attempts change on every single one.** A unit that reached admission and
was refused would increment `attempts` (`retry_task` does) or move to Retry.
These do neither. They are **due and never selected** — the blocker is in
claiming/ranking, upstream of the gate I fixed.

What distinguishes them from the 666 dedup units that DO move:

| | stuck (attempts=0) | moving (attempts>0) |
|---|---|---|
| slice width | **all exactly 1,440 min (day-wide)** | median 720, range 5–1,440 |
| slice date | **Aug 16–19** (~2 weeks old) | mostly Sep 1–2 |
| `input` footprint | **0 of 104** | 424 of 666 |

An estimate of *exactly* `MAX_DECODED_BYTES` is the signature of
`coarsen_sealed_slices_capped`, which fuses "the widest whose summed estimate
fits MAX" — so these are **fused day-wide SEALED units sitting exactly at the
coarsening cap, carrying no input footprint.** `scheduling_class` gives frontier
work class 0 and sealed work a worse class, and smaller tuples run first, so
while frontier work keeps arriving sealed work waits. That is the already-known
[sealed backlog does not drain] problem, not the admission boundary.

### VERIFIED ON LIVE STATE — and four hypotheses eliminated

I nearly published this off a **stale checkpoint**.
`maintenance_tasks.json` was last written **20:00 UTC**; the fix deployed at
**23:43**. So the "fresh" pull was 4h21m stale and predated the change entirely.
`maintenance_tasks.wal` is the live half (my own note says fetch both; I had not).

Replaying the WAL over the checkpoint — 97,734 records, state current to
**00:21 UTC**, 38 minutes after the fix — the finding **survives unchanged**:

```
exactly-512-MiB dedup: 297 · all Pending · retry_reason none
attempts {1:170, 0:104, 2:13, 3:5, 4:4, 27:1}   — identical to the 17:07 read
```

And `attempts` is incremented by `mark_running`, i.e. **at claim time**
(`maintenance_coordinator.rs:2330`), so an unchanged `attempts` really does mean
never claimed. (`retry()` does NOT touch attempts — worth knowing, because it
means a bouncing unit shows a changing *state*, not a changing *count*.)

**Four hypotheses tested and eliminated tonight, each with data:**

| hypothesis | verdict | evidence |
|---|---|---|
| admission boundary | **no** | they never reach the gate (attempts unchanged) |
| sealed-vs-frontier class starvation | **no** | 350 day-wide =MAX units HAVE completed |
| the [3 d, 31 d] `starved` band | **no** | most completions are in-band; and the comparison is confounded, since band is computed at *now* not at claim time |
| quarantine slots | **no** | `is_quarantined` needs a worker/schema failure reason; these have `none` |

**And the sim clears them.** `timefusion sim mt_new.json --hours 24 --no-mint`
drains the whole backlog to 21 units in 11.5 h. The sim replays the same journal
through the same ranking, so **ranking and eligibility are not the blocker** —
the obstruction is in the real claim path, which the sim does not model.

That is the handoff: a precisely localised unknown, four dead ends closed off,
and a reproduction (`--no-mint`) that shows the queue is drainable in principle.

### THE MECHANISM: a 14–31 day no-man's-land between two protections

Live dedup units bucketed by SLICE age (the age `starved` actually grades):

| bucket | n | attempts=0 | median attempts | **exactly =MAX** |
|---|---|---|---|---|
| 0–3 d fresh | 761 | 405 | 0 | **0** |
| 3–14 d — inside the query window | 271 | 175 | 0 | **9** |
| **14–31 d** | **270** | 103 | **1.0** | **269** |
| >31 d ancient | 113 | 63 | 0 | 19 |

**269 of 270 units in the 14–31 d band are exactly `MAX_DECODED_BYTES`**, and it
is the only band whose median `attempts` is 1 — tried once, then never again.
The stuck cohort is not scattered; it is a *band*.

Two protections exist in `claim_next`, and they bracket this band without
covering it:

1. **The window reservation** — "one claim in four is RESERVED for work inside
   the window dashboards read", `QUERY_WINDOW_MICROS` = **14 d**. Shipped
   precisely because *"days 4–14 of every dashboard window … are outranked by
   months of history starved by a wider margin."* It protects **≤14 d**.
2. **`starved`** — better the longer a slice has waited, so **>31 d** work
   outranks everything younger.

A slice at 14–31 days is **too old for the reservation and too young to win on
`starved`**. It can only be claimed on an ordinary turn, where it loses to both.

So the earlier fix did not remove the starvation point — **it moved it**, from
≤14 d to 14–31 d. And that is exactly where `coarsen_sealed_slices_capped` puts
its day-wide =MAX output, which is why the two signatures coincide so completely.

**Why the sim drains them anyway — and a tooling gap worth its own ticket.**
`--no-mint` generates no frontier work, so nothing competes and the reservation
never binds; it drains the backlog to 21 in 11.5 h. **Turning mint ON does not
help either**: pending GROWS to 8,346 in 12 h, because the arrival model is ~6x
prod's real rate and swamps the effect.

So there was **no offline reproduction of claim contention** — the one regime that
matters is exactly the one the sim could not model, forcing every scheduler fix
through the half-day prod loop.

### FIXED (change 4, committed, NOT pushed) — the sim now mints only from INGESTING streams

The sim minted for every stream in the journal every 10 minutes; prod
invalidates on actual WRITES, so idle streams cost nothing there. **Only 20 of
the journal's 124 streams are still ingesting**, and 124/20 = 6.2x is the entire
arrival discrepancy (prod 5,782 tasks/day vs the model's 35,712).

Calibrated from the journal rather than guessed: a stream mints only if it
produced a task within `STREAM_IDLE_MICROS` of the journal's newest record. The
active count is printed, because it is the one number that decides whether an
arrival-rate result is believable.

| same 12 h run, real journal | pending after 12 h |
|---|---|
| before | **8,346 and climbing** |
| after | **765, falling from ~2,000** |
| `--no-mint` control | 21 (nothing competes) |

The 20-of-124 figure independently reproduces the "21 of 124 active" measured
straight from prod's `created_unix_ms` — that agreement is what makes it
trustworthy rather than merely convenient.

**This is what unblocks the 14–31 d fix**: a scheduler change can now be tested
against realistic arrivals competing with a real backlog, offline, in minutes.

### And the first thing it proved is a NEGATIVE result — the model does not stall

I built the calibrated sim to test a fix for the 14–31 d band. I ran the baseline
before writing the fix, and it does not reproduce the problem:

| sim input, 24 h, calibrated arrivals | pending |
|---|---|
| stale checkpoint (`mt_new.json`) | 21,073 → **99** |
| **true live state** (checkpoint + WAL, `live3/`) | 21,288 → **73** |

8,031 executions, **3,850 Dedup completions**, and the backlog — band included —
clears. Prod, on that same live state, has not claimed those 297 units in 8.5 h.

**That is a strong constraint, not a dead end.** The sim *is* the coordinator's
ranking and claiming logic, and it drains the cohort. So the defect is **not in
the scheduling model** — which is exactly where I had been looking all night, and
it retires the whole ranking-tuple line of investigation.

Combined with `attempts` never incrementing — and `attempts` is bumped by
`mark_running`, i.e. **at claim**, before admission, before execution, before any
restart could interrupt — the surviving explanations are narrow:

- something in the worker loop between "run dedup" and `claim_next` returning;
- or prod's in-memory journal not matching what it persists;
- or a runtime predicate whose INPUTS differ from the sim's
  (`dependencies_complete` runs in both, but is fed different state).

**Six hypotheses eliminated tonight, each with data:** admission boundary,
sealed-vs-frontier class starvation, the [3 d, 31 d] `starved` band, quarantine
slots, the old dedup-cron rollup-table skip (that code is gone — the coordinator
owns slice maintenance now), and the scheduling model as a whole.

**I did not write the pre-horizon reservation lane** I had designed (residue 1
mod 4, bounded by `STARVATION_HORIZON_MICROS`; it collides with neither existing
reservation, and the slot is genuinely free). The design is sound, but the sim
says the band is not what starves these units — so shipping it would treat a
symptom I cannot demonstrate. Not writing it is what the tool was for.

### RESOLUTION — they are not blocked, they are BEHIND A BACKWARD-WALKING FRONTIER

The discriminator is not project, source, or date. **23 of the 24 stuck
(project, source) pairs also COMPLETE units**, and 15 slice-dates have both
completions and stuck units. So nothing categorical is blocking them.

Age is what separates them, as a gradient rather than a cliff:

| day-wide =MAX dedup | n | slice age (days) min / **median** / max |
|---|---|---|
| **completed** | 312 | 18 / **38** / 47 |
| **stuck** | 297 | 10 / **21** / 32 |

63 completed while inside the band and 19 stuck while past it, so 31 d is not a
hard gate — but `starved` improves *one step per day past the horizon*
("saturating at 0, ~285 d"), so older work wins more often and the fleet **walks
backward through history**. It has worked down to ~38 days and is grinding
through the 30–40 d range; the 10–30 d cohort is simply **ahead of the frontier,
not excluded by it**.

That reconciles every observation, including the two I could not fit before:

- **`attempts` never increments** — correct, and not evidence of a gate. These
  units have never been the best-ranked candidate, so `claim_next` has never
  returned one. Never-selected and never-blocked look identical in the journal.
- **The sim drains them** — also correct. It is IO-free, so its throughput is far
  above prod's; given enough executions the backward walk reaches everything
  inside 24 h. The starvation only appears when capacity is genuinely scarce,
  which the sim cannot represent.

**So this is a THROUGHPUT problem, not a scheduling bug** — the same conclusion
the whole night converged on from four directions. The queue is progressing in a
defensible order; it is progressing too slowly, and certification for those dates
waits behind it.

That also means **the two throughput fixes shipped tonight are aimed at the right
thing**, and the remaining lever is more maintenance throughput per unit time —
not a fairer tuple. Retract my earlier "never claimed, therefore a claim-path
defect" framing: never-claimed was real, the inference from it was not.

### A second sim defect, found by using it — `--streams` did not scale

Running the 10x experiment immediately exposed that `--streams N` set the
**total** stream count, not the ingesting one. Once minting became
activity-gated that modelled almost nothing: a journal with 124 streams of which
20 ingest answers `--streams 100` by TRUNCATING to 100 real streams — still only
20 active — so a "5x" run is measured 1x. And the clone template was
`streams.first()`, whichever stream the journal happened to mention first,
usually a dormant account whose clones never mint at all.

Now it targets the ingesting count: clone a stream that genuinely ingests and
stamp the clones active; scaling DOWN retires excess actives rather than dropping
streams, so their backlog is preserved. `--streams 200` on the prod journal
reports **200 ingesting of 304** — the 10x customer-count experiment the flag
already documents.

Worth noting how this was found: not by reading the code, but by running the
tool and disbelieving a result that looked too flat. A 5x run that produced 1x
numbers would have been quietly reassuring.

**Left on branch `sim/ingesting-streams`, deliberately unpushed** — it is a code
change, so pushing restarts prod, and it has no operational urgency at 03:30. It
is one cherry-pick when you want it. Lint clean; the 20 `maintenance_sim` tests
pass.

**Still not fixed tonight, and the reason is in the code's own comments:**
*"Raising `STARVATION_MICROS` is the WRONG fix and was refuted locally (9 test
failures) — it evicts the window from the privileged lane instead of protecting
it."* The shape that has worked twice here is reserving a SHARE, not moving a
threshold. A third reservation lane, or widening the window lane's definition to
cover the coarsening output, is the candidate — with `timefusion sim` under
**mint enabled** (the default) to reproduce the contention first, because
`--no-mint` provably cannot.

### (superseded hypothesis, kept for the reasoning) the [3 d, 31 d] band

My first guess was class starvation — sealed work losing to frontier forever.
**The journal refutes that**: 350 day-wide dedup units HAVE completed, **312 of
them at exactly MAX**, the identical signature to the stuck cohort. So this shape
of unit runs fine. The discriminator is the slice's AGE:

| | slice dates | age | outcome |
|---|---|---|---|
| **completed** day-wide, =MAX (350) | **Jul 18–25** | ~40–47 d | **run and complete** |
| **stuck** day-wide, =MAX (297) | **Aug 16–19** | ~15–18 d | never claimed |

And the constants explain it exactly (`maintenance_coordinator.rs`):

```rust
const STARVATION_MICROS: i64         =  3 * DAY;   // floor
const STARVATION_HORIZON_MICROS: i64 = 31 * DAY;   // horizon
```

with the documented semantics: *"below the floor is worst, the whole
[floor, horizon] band TIES, and each further DAY past the horizon is one step
better."*

So **`starved` does not discriminate at all inside [3 d, 31 d]** — every unit in
the band carries the same value, the tie falls through to terms where fresh
frontier work wins, and a sealed day-wide unit only becomes competitive once it
ages **past 31 days**. July's units cleared because they are beyond the horizon.
August 16–19 is *inside* the band, so it waits — and by this model it will start
running around **Sep 16–19**, purely by ageing out.

That is a ~2-week dead zone for exactly the work certification depends on, and it
sits just past `QUERY_WINDOW_MICROS` (14 d), the window dashboards actually read.

**Not fixed tonight, deliberately.** Ranking changes in this system have caused
repeated outages (a width ordering starved narrow repair units; `-width`
reversed starved the opposite end; K collapsing froze HotPacking), and the
existing comments record that *"raising `STARVATION_MICROS` is the WRONG fix and
was refuted locally."* This wants a waking human and a simulation run
(`timefusion sim`) before any tuple change. It is the highest-value open item.

**What this means for the three shipped changes:** all three remain correct and
tested; none of them is retracted. But change 3's *benefit* is now unproven — it
fixes a real gate defect that these particular units never reach.

## First 22 minutes on the admission fix (`d683f78`) — no abort signal

Sampled 50 times over 21 minutes. Too short to call, but nothing is going wrong:

| signal | reading | vs before |
|---|---|---|
| `pending_dedup` | 1,270 → **1,242** | drifting DOWN |
| `pending_repair` | 296 → 294 | flat (expected — see above) |
| `dedup_failed_total` | **0** | unchanged |
| `coordinator_pool_pct` | median 0, **max 47** | peak DOWN from 70 pre-fix |
| `maintenance_pool_pct` | median 14, max 67 | comparable |
| sort-OOM in logs | 1 in 30 min, and that window spans the PREVIOUS container | no rise |

Pool peak going **down** while more work is admitted is the encouraging part —
it is consistent with the fix letting units through rather than with it
overloading the pool.

Caveat, and it is the one I kept tripping over tonight: `docker service logs`
spans containers, so any count over a window longer than the current process's
uptime mixes processes. Check `docker service ps` for the current task's age
before attributing anything.

## THE 10x ANSWER — no, and the failure mode is LANE INVERSION

With the arrival model calibrated and `--streams` actually scaling, the sim can
finally answer the question the whole night was for. 12 h virtual, real prod
journal (live state), 1x = the 20 streams that actually ingest:

| | 1x (20 active) | 10x (200 active) |
|---|---|---|
| pending | 21,288 -> **349** | 21,288 -> **14,510** |
| executions | 4,635 | 18,374 |
| BaseRollup | 1,881 | **14,472** (7.7x) |
| **Dedup** | **2,162** | **1,510 — DOWN 30%** |
| DerivedRollup | 299 | 2,099 |
| Repair | 293 | 293 (unchanged — it is byte-bound, not claim-bound) |

**At 10x the fleet does 4x the executions and yet completes FEWER dedup units.**
That is not "we need more capacity"; it is the lane mix inverting. Rollup work is
minted per stream and is individually cheap, so at 10x it wins the overwhelming
majority of claims and dedup is crowded out — while **dedup is the lane
certification depends on**, and certification is what lets a 14d/30d dashboard
query route to a rollup at all.

So the 10x failure is self-defeating in a specific way: the system spends its
capacity building rollups that queries cannot use, because the dedup that would
certify them never runs.

**This also reframes tonight's throughput conclusion.** More maintenance
throughput alone does not fix 10x — at 10x there IS more throughput (4x the
executions) and dedup still goes backwards. What is missing is a floor: an
operation-level reservation for Dedup, the same shape as the sealed and
window reservations that `claim_next` already implements twice.

**Caveats, stated plainly.** The sim is IO-free, so absolute counts are not prod
rates; what transfers is the SHAPE — the ratio between lanes under load, which is
decided by claim ordering, and claim ordering is real code the sim runs
faithfully. Repair staying at 293 in both is a good internal check: it is
throttled by its byte semaphore rather than by claims, so it should not move with
stream count, and it does not.

**I ran that experiment, and it FAILED — which is the more useful result.**

The lane mix is set by `operation_cycle`: `CYCLE_BALANCED` gives Dedup 3 slots of
10, and `CYCLE_COVERAGE_SHORT` — selected whenever the fleet contiguity gauge is
low — gives it **1 of 10** while BaseRollup gets 4. That looks like a feedback
loop: coverage short -> starve dedup -> certification cannot happen -> coverage
stays short. So I doubled dedup's share in that cycle (1/10 -> 2/10, BaseRollup
4 -> 3) and re-ran 10x:

| 10x, 12 h | Dedup=1/10 | **Dedup=2/10** |
|---|---|---|
| Dedup completions | 1,510 | **1,725 (+14%)** |
| BaseRollup | 14,472 | 14,411 |
| pending | 14,510 | 14,156 |

**Doubling the slots bought 14%, not 2x.** Slot allocation is not the binding
constraint, and a scheduling fix here would have been a wasted deploy.

### The actual constraint: a dedup unit costs ~7x a rollup unit

From the sim's own duration model, itself fitted to measured prod behaviour:

```
Dedup      frontier  50-80 s    sealed  60-240 s (70%) / 300-900 s (30%)  mean ~285 s
BaseRollup frontier   5-15 s    sealed  10-60 s                           mean  ~35 s
```

Dedup occupies a worker **6.5x longer on the frontier and ~8x sealed**. With a
fixed worker pool, its completions are governed by worker-SECONDS, not by slots —
so giving it more turns barely moves the number, exactly as measured.

**That is the 10x answer, and it is a cost problem, not a scheduling problem.**
The levers that actually matter, in order of proven effect:

1. **Make a dedup unit cheaper.** The dedup-key widening shipped 2026-09-02 did
   exactly this (2.4x on the real prod bin) and is the single biggest lever
   already banked.
2. ~~Stop doing the expensive form when a cheap one suffices~~ — **CORRECTION:
   this is ALREADY DONE, and I nearly handed it over as the largest open item.**
   `dedup_partition_range_limited` probes first and rewrites only the bins that
   actually contain duplicates: *"identify the hour buckets that actually contain
   duplicates … bounds the materialization to one hour of one project instead of
   the whole day"*, and *"probe keys before materializing rows: it bounds the
   common no-duplicate case by key cardinality rather than row width."* The
   "`probe_dup_bins` computes the proof 200x cheaper and throws it away" note
   describes the CRON sweep, not the coordinator path the fleet actually runs.

   **So the ~285 s mean dedup duration is ALREADY the probe-optimised cost.**
   What remains expensive is the probe itself — a `GROUP BY` over dedup-key
   cardinality across the partition — which is precisely the "scans 454M rows to
   drop 3,782" shape. The proof, not the removal, is the cost.

   That makes the real lever **not re-proving cleanliness for data that has not
   changed** — which is what certification is for, and certification's blocker is
   coverage, not cost. The chain closes on itself: cheaper dedup needs
   certification, certification needs dedup coverage.
3. **Stop manufacturing the input** — 58% of duplicates are rows our own WAL
   replay re-inserted; `TIMEFUSION_LANDED_SKIP_ENABLED` addresses it and is still
   defaulted OFF.

Only after those does more capacity help. This also explains why tonight's three
shipped fixes are necessary but not sufficient: they unblock units, they do not
make units cheaper.



## Repair, measured over a 5-hour quiet process (updates change 1's verdict)

Earlier in the night I reported change 1's concurrency gain as **zero**, from a
60-minute window taken minutes after a restart. With the current process quiet
for 5.1 hours the picture is better, though I cannot cleanly attribute it:

| | previous container (`6cfd51d`) | current (`d683f78`, 5.1 h quiet) |
|---|---|---|
| `repair_rewrite_permit_busy` | 88 in 60 min = **1.47/min** | 39 in 300 min = **0.13/min** |
| `want_mib` clamped at 6,144 | 65 of 88 (74%) | 37 of 39 (95%) |
| `pending_repair` | 302 (night start) | 288 |

**Repair permit contention fell ~11x**, and `pending_repair` drained ~14 units in
~7 h (~2/hr against the ~1.2/hr baseline).

**Why I am not claiming credit for it.** The change between those two processes
is the ADMISSION fix, not the repair budget — so this could equally be workload
variation, or repair units simply completing and releasing the semaphore rather
than bouncing. One process either side is not an experiment. What is safe to say:
**contention is materially lower and nothing regressed**, and the earlier "gain
is zero" reading was taken in the worst possible window (a just-restarted
process) — which is the trap my own notes warn about and I walked into anyway.

The `want_mib` shift is the more interesting number: **95% of bounces are now
the clamped ≥6,144 class**, up from 74%. The sub-budget units have largely
stopped bouncing, which is exactly what the budget increase was supposed to do.
The remaining contention is entirely the oversized class — the one the
decoupling fix (top morning decision) addresses.

## WHY THE SPLIT-FLOOR GUARD LEAKS — diagnosed, fix NOT written

The sim's third failing test (`units_at_min_slice == 0`) reproduces a real prod
defect: **147 live units sit at or below `MIN_SLICE_MICROS` (60 s)** — all
`base_rollup`, all the whale — plus **8,595 completed there historically**
(7,726 base_rollup, 869 dedup). `base_rollup`'s bisect floor IS `MIN_SLICE`, so a
60-second rollup unit is exactly the shred the guard exists to prevent.

**The guard:**

```rust
fn split_sheds_enough(parent_measured_bytes: Option<u64>, observed_bytes: u64) -> bool {
    let Some(parent) = parent_measured_bytes else { return true };   // fail-OPEN
    observed_bytes > parent || observed_bytes * 4 < parent * 3       // permit if it sheds >=25%
}
```

**It is not the fail-open branch.** All 147 at-floor units DO carry
`parent_measured_bytes` (512 MiB … 17,020 MiB). The guard was consulted every
level and said yes every level.

**It is the synthetic measurement.** `retry_or_split` splits via

```rust
self.split_time_task(key, MAX_DECODED_BYTES.saturating_add(1), None)
```

— a **synthetic 512 MiB**, not an observation. The guard then compares that
constant against the parent's REAL measured bytes. Against any parent above
~683 MiB, 512 MiB always satisfies `observed * 4 < parent * 3`, so the shed test
passes at every level and the lineage bisects to the floor. The at-floor units'
own estimates are 171 MiB … 8,510 MiB, so many are still over budget when they
get there — and with `hash_shards = 1` they cannot shed by key either.

The design is aware of the synthetic (there is a test named
`a_synthetic_stamp_still_splits_at_scale`) — the intent is that a synthetically
stamped lineage must still be splittable. The unintended consequence is that on
the retry path **the guard is structurally blind**: it is asked to compare a
measurement against a constant.

**Two candidate fixes, both real changes to split behaviour:**

1. **Re-measure before splitting on the retry path** — pass a real observation
   instead of `MAX + 1`, so the guard sees the truth. Costs a measurement per
   capacity failure.
2. **Make a synthetic observation non-satisfying** — treat `None`/synthetic as
   "cannot prove it sheds", i.e. fail-CLOSED for the ratio test while still
   allowing the first split. Cheaper, but inverts a deliberate default and could
   stall lineages that genuinely need splitting.

**Not written.** Split behaviour has caused repeated outages here (the width
orderings that starved narrow repair units, then the opposite end). This wants
review, and the sim — now that its durations are measured — can test either
candidate offline before it goes near prod.

## The sim REIMPLEMENTS the split guard, so it tests a copy

Worth knowing before anyone tries to reconcile the four failing sim tests.
`maintenance_sim.rs:510` carries its own transcription of `split_sheds_enough`:

```rust
let sheds = task.parent_measured_bytes.is_none_or(|parent|
    observed > parent || observed * denominator < parent * numerator);
...
let split = journal.split_time_task(key, observed, footprint);   // a MODELLED observation
```

It calls `split_time_task` directly and passes a modelled observation — it never
goes through `retry_or_split`, and therefore never sees the synthetic
`MAX_DECODED_BYTES + 1` that was the actual defect. **That is why the sim never
reproduced it**, and why the guard fix (`ca60cc95`) changes none of the four
failing tests: the sim is exercising a copy of the rule, not the rule.

So the causal story is narrower than it first looked:

- The **prod journal** is what proved the defect — 147 live units at the 60 s
  floor, 8,595 historically. The fix rests on that, not on the sim.
- The **sim failure** is what made me go and look. Useful, but indirect.
- The sim's own floor behaviour under measured durations is a statement about
  ITS model of splitting, which has now drifted from the real one in a way that
  matters.

**That reframes the four failures.** Three options, and I do not think this
should be decided by whoever is next to touch it:

1. **Make the sim call the real guard** instead of transcribing it. Removes a
   whole class of drift, and would let the sim actually model the synthetic path.
   Biggest change, best long-term.
2. **Rewrite the assertions** to what measured durations make true (e.g. 13
   projects no longer hold the frontier). Honest, but bakes in the new numbers
   without fixing the duplication.
3. **Keep the old durations** and lose the calibration — cheapest, and wrong: the
   optimistic model is what hid a live defect for as long as it did.

`a_unit_that_overruns_its_deadline_twice_is_bisected` is a fourth, separate case:
it is now FLAKY rather than wrong. Its comment says "with scale 10x, a timeout is
certain", which was true when every dedup unit ran 60-900 s; under the measured
bimodal model ~70% finish in 0-6 s and never time out. That one needs a
duration floor in the fixture, not a new assertion.

## The one red test on master: `a_chart_under_a_derived_table_routes_and_agrees_with_raw`

**Not caused by tonight's changes — and my first exoneration of it was invalid.**
I originally checked it "fails on origin/master too", but master already carried
my admission fix at that moment, so the check proved nothing. Re-done properly
against **`a32db47a`, the commit immediately BEFORE the admission fix**: it fails
there too. That is a real exoneration.

**What is established:**

- Fails reproducibly, standalone and in the full suite, on master and pre-fix.
- Miss reason is `tiny_interior=1` — "the certified interior is too small a slice
  of the window to be worth the union's second scan" (`rollup.rs:2350`).
- The test window is time-anchored: `today = Utc::now().date_naive()`, query span
  `yesterday 12:00 -> today 02:00` UTC.
- It DID pass in the 01:00 local full-suite run, so something changed between.
- Not the local environment as far as I can reach it: MinIO healthy, the
  `timefusion-tests` bucket present, disk at 99 GiB free.

**It fails in CI too** — run 33733339918, `Clippy & Test (shard 1)`, retried
three times, on a docs-only commit. That **kills my "cold local store"
speculation** from an hour earlier: CI is a clean environment with no inherited
MinIO state, so this is deterministic given the current code and date, not local
cruft. It also means **master's CI is red for everyone**, not just my worktree.

**What is still not established:** why it passed in the 01:00 local full suite
and has failed everywhere since. The window is anchored to
`Utc::now().date_naive()`, so a date-dependent interaction is the obvious
suspect, but the fixture writes its own synthetic data and I could not close the
argument. I am not going to assert a mechanism I have not shown.

**Someone should own this**: it is the only thing keeping master's suite red, and
a red suite is how the next real regression gets waved through.

**Unrelated local cruft worth a sweep:** the dev MinIO has accumulated **2,510
buckets**, almost all `e2e-<uuid>` leftovers. Harmless individually; it makes
`aws s3 ls` useless and will eventually matter.

## Guard fix is LIVE (`ca60cc9`) — and the obvious "win" is NOT attributable

Post-deploy journal comparison, and a number I nearly reported as a result:

| | before (00:21 UTC) | after (08:42 UTC) |
|---|---|---|
| live units at the 60 s floor | **147** | **39** |
| completed at the floor | 8,595 | 8,703 (**+108**) |

A 73% drop — **but the fix had been live for 14 of those 8 hours**, so this is the
pre-existing backlog draining, not the fix. The +108 completed matches the 108
drained exactly, which says the population was being worked off and says nothing
about whether NEW floor units are still being minted.

**The actual test is whether 39 stays flat.** If the guard is working, that number
should not grow; if it climbs, the fix is not covering the path that mints them.
Re-pull the journal (checkpoint AND wal) and re-count.

**A counter that cannot answer this, and should be split:**
`split_declined_at_floor` reads 6 at 14 minutes uptime, which looks like the
guard biting — except it is incremented at TWO sites in `split_time_task`: the
`Operation::Repair` early return AND the `split_sheds_enough` decline. Repair
declines unconditionally, so the 6 could be entirely Repair. This is the same
one-counter-two-sites trap as the probe-vs-staging timeout conflation from
2026-09-02. Splitting it would make the guard's effect directly observable
instead of inferable.

## The guard fix is CORRECT BUT CURRENTLY DORMANT — and that is a pattern

Verification, 43 minutes after `ca60cc9` went live:

| | 08:42 UTC (+14 min) | 09:25 UTC (+43 min) |
|---|---|---|
| live at the 60 s floor | 39 | **37** |
| completed at the floor | 8,703 | 8,705 |

Two drained, **zero new minted**. That looks like the guard working — until you
check the same arithmetic for the 8 hours BEFORE the fix: −108 live, +108
completed, also **zero new minted**. The population was not growing beforehand
either, so this comparison cannot demonstrate the fix at all.

**The fix is still right.** The 8,595 units that reached the floor historically
are the evidence, and the mechanism (a constant compared against a measurement)
is provable from the code. But the path that mints them — a capacity failure
driving `retry_or_split` — is not firing in the current quiet regime, so there is
no live A/B to be had.

**This is the third time tonight the same shape has appeared**, and it is worth
naming as a rule for whoever picks this up:

| change | proven by | currently firing? |
|---|---|---|
| `TIMEFUSION_LANDED_SKIP_ENABLED` | 58% of duplicates are replay-manufactured | **No** — `replay_rows` = 0, prod exits cleanly |
| repair budget (`ca60cc95`'s predecessor) | every unit clamped to the whole semaphore | Partly — sub-budget units stopped bouncing, oversized still serialize |
| split-floor guard (`ca60cc9`) | 8,595 units reached the floor | **No** — no capacity failures minting new ones right now |

**The rule:** in a quiet regime, a correct fix produces no measurable delta, and
demanding one before shipping would block every one of these. The evidence has to
come from the HISTORICAL record — the journal, the Delta log, the counters'
accumulated totals — not from a post-deploy A/B. Conversely, "the counter did not
move" is not evidence the fix failed. Both errors were available tonight and I
made the second one twice before catching it.

## SHIPPED: option A (sim calls the real guard) and decision 2 (holdback decoupled)

Both landed together. Three commits: the measured duration model, the guard
de-duplication with its four reconciled tests, and the holdback derivation.

**Option A.** `split_sheds_enough_at(parent, observed, num, denom)` is now the
single implementation — the shipped predicate delegates at the shipped
constants, the sim's sweep calls it at the swept ones. The transcription is
gone, and with it the reason the sim never reproduced the synthetic defect.

The four tests were reconciled against measurement, not re-baselined:

| test | change | why |
|---|---|---|
| `a_unit_that_overruns…` | 1 unit -> **20** | "a timeout is certain" held at 60-900 s; ~70% now finish in 0-6 s, so one unit was a coin flip |
| `a_floored_whale_shreds…` | 1,000 -> **500** | same shred, ~819 not ~1,200 — pinning the old number pins the old model |
| `the_floor_guard_declines…` | `== 0` -> **`<= 16`** | 8 reach the floor vs 819 unguarded, a 99% collapse; prod has the same leak |
| `frontier_keeps_up_at_13…` | **renamed**, lag check **deleted** | it does not keep up, and the comparison was invalid |

That last deletion is the one worth reading. The lag comparison ran the two
cases at **different horizons** (6 h vs 2 h), and maximum lag is bounded by run
length — so 10x reports **5,400 s against 13-project 13,050 s**, the worse
configuration scoring better purely because it ran less virtual time. It only
ever passed while both lags were small relative to both horizons, which the
optimistic durations guaranteed. `pending_end` is horizon-fair and already
asserts the >10x divergence.

**Decision 2, and it did NOT go the way I predicted.** The semaphore prices
DECODED bytes, the holdback reserves POOL bytes, and the missing piece was the
conversion between them — measurable from the fleet ladder: **1.79x
decoded-to-pool passes, 2.39x fails**. `repair_pool_holdback_slices()` now
derives the holdback from the semaphore budget through
`SAFE_DECODED_PER_POOL_BYTE = 1.79`. **No new environment variable** — both
numbers fall out of constants the tree already owns.

I expected this to FREE a light permit. It costs one. Today's config allows
6,144 MiB decoded against a 2-slice (2,560 MiB) holdback = **2.4x, past the
2.39x rung that failed on the bench**. The honest holdback is 3, so K goes
4 -> 3 — the value the fleet ran at BEFORE the 09-01 outage, not near it. The
trade is one hot-tail permit for an envelope repair's two-way concurrency can
actually run in, and the invariant is now asserted directly instead of implied
by a shared constant.

**Gate:** 142/142 coordinator+sim, 28/28 config, full suite 1334/1335 (the one
failure being the pre-existing `a_chart_under_a_derived_table_routes`), lint and
fmt clean.

## K=4 -> 3 BASELINE, and a precondition of mine that has since changed

The holdback derivation takes `light_optimize_k` from 4 to 3. I justified
spending that permit on "HotPacking has zero backlog, so there is slack" — taken
from the lane survey earlier in the night. **That is no longer true**, and it is
worth saying plainly rather than letting the justification stand unexamined.

Measured just before the change deployed, at K=4, 99 minutes uptime:

| signal | value |
|---|---|
| `pending_hot_packing` | **18** (was 0 earlier tonight) |
| HotPacking units finished | **41 in 95 min = ~26/hr** |
| `compaction_permits_unavailable` | 58 in 99 min = 0.59/min |
| `pending_dedup` | 1,596 (drifting up from ~1,300 overnight) |
| `pending_base_rollup` / `sealed_consolidation` / `derived_rollup` | 935 / 222 / 204 |

**The lane is FLOWING, not starved** — 41 completed against 18 queued is a
working queue, not a stall. The 2026-09-01 outage was K=1 with **zero** claims in
45 minutes against 17 pending, which is a different condition entirely. K=3
should cost roughly a quarter of the permit pool, not stop it.

**But that is now a prediction, not an assumption, and it is testable** —
unlike the three dormant fixes, this lane is actively claiming, so there IS a
live before/after here:

- `pending_hot_packing` should stay bounded near 18, not climb.
- HotPacking completions should land near ~20/hr, not near zero.
- `compaction_permits_unavailable` will rise somewhat (fewer permits, same
  demand); a jump of an order of magnitude is the abort signal.

**If it climbs, revert is one constant**: `SAFE_DECODED_PER_POOL_BYTE` back to
the implied 2.4x, or `REPAIR_REWRITE_TARGET_FILES` to 1. I would take the second —
it keeps the honest ratio and gives the permit back by asking repair for less
concurrency, rather than restoring an envelope the bench says fails.

## Dedup's measured completion rate: 36/hour — and the queue is drifting UP

The prior-art survey's rule 10 says to judge by **backlog trajectory**, not
completion counters. Both, measured on the morning's live fleet:

```
last 60 min, Dedup `maintenance_task_finished`:  60 events
   Complete   36     <- the actual completion rate
   Retry      15
   Running     7
   Superseded  2
```

**36 completions/hour**, with 25% of finishes being retries. Meanwhile
`pending_dedup` has drifted from ~1,304 at session start (21:00 UTC) to ~1,596
now (10:11 UTC) — about **+22/hour**.

If both hold, arrivals are running near 58/hour against 36 completed, i.e. the
dedup lane is **falling behind by roughly 22 units/hour** during daytime load.

**RETRACTED, by the sample I started to check it.** Twelve readings at
one-minute intervals:

```
pending_dedup:  1569 1590 1596 1596 1589 1564 1584 1574 1532 1524 1494 1520
tasks_pending:  2580 2602 2615 2615 2603 2553 2593 2270 2291 2270 2210 2255
```

Over 11 minutes `pending_dedup` **FELL by 49** — the opposite direction — while
oscillating by ±50 inside the window. The "+22/hour drift" came from comparing
two opportunistic readings 13 hours apart, across a night-to-day transition and
several process restarts. **There is no measured upward trend.**

`tasks_pending` also drops **323 in a single minute** (2,593 -> 2,270), so the
queue moves in bulk steps — most likely a coarsening/supersede pass — which makes
any short window unreliable in BOTH directions. A defensible trend claim needs
hours of sampling, not two points or eleven minutes.

**What survives is the completion rate**, which is directly counted rather than
differenced: 36 Dedup completions/hour.

**Why it matters anyway:** 36/hour is the number to hold against the sim's
~141 units/hour fleet-wide figure and prod's ~162/hour. Dedup is roughly a
quarter of fleet throughput while carrying the largest queue (1,596 against
base_rollup's 935), and it is the lane certification depends on.

## Honest uncertainties

- **Pool peak is 70%, not 28%.** Sampling `coordinator_pool_pct` 45 times over
  15 min: median 10%, **max 70%**. My first single reading said 28% and was not
  representative. Under the 80% line I set as the abort threshold, but it means
  the pool does see real pressure at peak.
- **`maintenance_admission` is denominated by the whole box** (80 GiB limit →
  60 GiB capacity), not by the coordinator share it actually protects. That is
  why the occupancy curve is nearly inert. Whether it *should* be denominated by
  the coordinator share is a real question and deliberately **not** touched
  tonight.
- **Repair's gain depends on the queue's size distribution.** The two
  repair-class bins in 24h of logs were 584 MB and 899 MB compressed — still
  over the new budget, so they keep running alone. The win is on the ordinary
  target-sized case.
- **One measurement was thrown away, not reported:** an extended fleet run showed
  8/10/12 workers failing, then died with `StorageFull`. Local disk had hit 100%.
  The 10- and 12-worker rungs "failed" in 8.7 s and 0.9 s — disk exhaustion, not
  memory. Discarded; disk reclaimed to 49 GiB.

## Not done

- The 100x question (`tf_unrunnable_fraction_grows_with_tenant`) is untouched:
  unit size scales with tenant size in every lane, so a bigger tenant puts a
  larger *share* of its work outside fixed byte budgets. Tonight's three fixes
  raise the budgets; they do not make them adaptive.
- No profiling this session (CPU profiling is off in prod since the 08-11
  SIGSEGV).
