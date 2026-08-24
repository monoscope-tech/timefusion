# Handoff results: what the open items actually measured

2026-08-24, ~10:45-12:50 UTC. Answers to
`2026-08-24-handoff-open-work.md`, item by item. Every number is stamped with
the image SHA and container age it was taken on, because the box was redeployed
six times during this window (`5635686` -> `0bdeddf` -> `8c37d37` -> `5e7934b` ->
`1e42237`, plus one more) and every `timefusion_stats` counter is
process-scoped.

Tools written for this, all under `bench/local/` (gitignored):

- `journal_snapshot.py` — checkpoint + **WAL replay**. The plan's §1 snippet reads
  `maintenance_tasks.json` alone, and that file was **2h51m stale** the whole
  session (mtime 08:48, unchanged at 11:38). The `.wal` beside it was current to
  the second. Any sealed count taken from the checkpoint is a reading of 08:48.
- `drain_sampler.sh` — the above plus the counters, every 15 min, with the image
  SHA, so a restart annotates the series instead of truncating it.
- `routing_probe.py` — reads the routing counters immediately before and after
  each query on the same connection, so a cell's latency and its hit/miss delta
  are one event.
- `small_file_census.py` — object storage, not counters, per (project, date).

---

## 1. Does the sealed backlog drain? **No. It is flat.**

Eight samples over 115 minutes, spanning six deploys:

| UTC | image | age | sealed units | sealed cells | complete_base_rollup |
|---|---|---|---|---|---|
| 10:50 | `0bdeddf` | 6 min | 931 | 435 | 14,468 |
| 11:05 | `0bdeddf` | 21 min | 928 | 435 | 14,475 |
| 11:22 | `0bdeddf` | 37 min | 930 | 435 | 14,488 |
| 11:38 | `8c37d37` | 4 min | 933 | 435 | 14,498 |
| 11:53 | `5e7934b` | 4 min | 928 | 435 | 14,541 |
| 12:13 | `5e7934b` | 25 min | 924 | 435 | 14,563 |
| 12:28 | `1e42237` | 1 min | 924 | 435 | 14,572 |
| 12:44 | `1e42237` | 16 min | **980** | 435 | 14,589 |

- **Sealed cells: 435 in every one of eight samples**, across six deploys, 115
  minutes, and one 38-minute quiet stretch. Not one cell retired, ever. This is
  the result; everything else is commentary on it.
- **Sealed units do not trend, they oscillate: 931, 928, 930, 933, 928, 924, 924,
  980.** The last step is +56 in 16 minutes — and **inflation moved 2.12x -> 2.25x
  in the same step, with cells fixed at 435**. The queue grew 6% without a single
  new cell entering it, which is only possible by splitting units that were
  already there. That is the shred signature, live, and it means the unit count
  is not a backlog measure at all.

A deploy pause was attempted mid-session and **held for 38 minutes** before the
sixth deploy (`1e42237`, 12:27). It changed nothing measurable: the two samples
inside it moved sealed by −4 and cells by 0.

`complete_base_rollup` rose by 121 over the 115 minutes (~63/hr) while the sealed
count did not move, so essentially all completion is the live frontier — which
ingest replenishes. `pending_base_rollup` (journal) went 1,619 -> 1,816 across the
same window: the queue GREW.

Two corrections to the plan's premises:

- It says the remaining sealed units are "**every one day-wide**". Measured
  **inflation 2.12x to 2.25x** over 435 cells, in every sample. The coarsening win
  did not hold at 1.0.
- The plan's "best rate observed, ~41/hr of sealed work" is not reproducible here.
  Over 115 minutes the sealed CELL rate is exactly **0/hr**.

**So the organisational ask in the plan — "a few hours without a deploy" — is not
the blocker.** The 37-minute quiet stretch on `0bdeddf` and the 38-minute pause
before `1e42237` were each long enough to see a drain if one existed, and there
was none.

One incidental correction to trap 2 — `rollup_min_contiguous_days` read **30 on a
4-minute-old container** at 11:53, and 0 on an equally young one at 11:38. The
"resets to 0 and needs ~25 min" rule is not reliable in either direction now, so
read the gauge rather than reasoning from uptime.

Where it goes, from a claim census of `maintenance_task_started`. **Read the
window sizes, not the `--since` flags** — `docker service logs --since 6h` returned
only what the CURRENT container had retained, which was 5 minutes of it. Asking
for six hours and getting five minutes is a trap worth naming, because 124 starts
looks like a long-window sample and is not:

```
window B — 5 min, 1 container (11:38-11:43, four minutes after a deploy)
  BaseRollup          43   2026-07-19: 9   2026-08-01: 7   2026-08-24: 27
  Dedup               34   2026-07-24..27: 23              2026-08-24: 11
  DerivedRollup       12                                   2026-08-24: 12
  HotPacking          12                                   2026-08-24: 12
  Repair              13   2026-07-26: 1                   2026-08-23: 12
  SealedConsolidation 10   2026-08-13: 3  08-14: 1  08-16: 6

window A — 68 min, 2 containers (10:11-11:18, mature process)
  SealedConsolidation 27   2026-08-13: 1                   2026-08-23: 26
```

Sealed rollup work IS running — but on two days (07-19, 08-01) out of 435 cells.
The claim order concentrates, it does not sweep.

The A/B contrast is itself a finding: **a fresh container consolidates old sealed
days; a mature one spends 26 of 27 claims on the newest sealed day.** That is the
signature of state that accumulates within a process and is cleared by restart —
`attempts`, and therefore quarantine.

**Repair is in an immortal-unit loop.** Its 13 claims in window B carry
`attempts = 9, 10, 10, 12, 40, 41, 41, 42, 42, 43, 43, 44` — twelve of thirteen
on 2026-08-23 — and in window A Repair timed out **14 times in 30 claims (47%)**.
`QUARANTINE_ATTEMPTS` is 2, so all of it is running through the
quarantined-occupancy permit. This is the largest single identified sink and it is
not any of the plan's seven items.

## 2. Does routing offset the load? **It routes `throughput` and never routes `p95_latency`.**

Thirty wide-window cells, coverage held at 30 for all of them (checked per row,
per trap 2). `bench/local/routing_probe.log`:

| shape | 7d | 14d | 30d |
|---|---|---|---|
| `throughput` | p4 **hit**, p5 **hit**, p6 **hit** | p4 **hit**, p6 **hit** | p4 **hit** |
| `p95_latency` | 0 hits / 5 misses | 0 hits / 5 misses | 0 hits / 5 misses |

- **Six hybrid hits, all `throughput`.** The routed cells complete: p4 30d
  throughput 33.4 s, p6 14d 48.1 s — slow, but answered, where the raw path
  fails outright.
- **p1 and p2 never routed on `throughput`, at any window.**

### The miss reasons, and a correction to the plan

A first reading said "every miss reason is 0 except `not_built`". **That reading
was wrong, and wrong by the plan's own trap 1** — it was taken after a restart, so
it described three fresh misses on a new process, not the fifteen the probe had
just produced on the old one. Re-run as a same-connection, same-container
before/after over all fifteen `p95_latency` cells, coverage 30 on every row
(`bench/local/miss_reasons.log`):

| reason | count | where |
|---|---|---|
| `not_built` | 13 | p2 7d/14d/30d, p6 14d (11 on one query) |
| `stale_coverage` | 6 | p1 7d/14d/30d, p5 14d/30d, p6 30d |
| `unsupported` | 5 | p4 7d/14d/30d, p5 7d, p6 7d |
| `filter_not_eligible` | 2 | p1 30d, p2 30d |
| `tiny_interior` | 1 | p6 30d |

**This contradicts the plan's §2 premise directly.** It states "all routing RULES
are fixed: `filter_not_eligible` 0 (null guard), `stale_coverage` 0
(witness/bound fix); remaining misses are `not_built`". Measured, `not_built` is
under half of them, `stale_coverage` is the sole reason p1 never routes, and
`unsupported` — which the plan does not mention at all — is the sole reason p4 and
p5 miss at their narrower windows.

`p95_latency` is also not uniformly unroutable: p4 7d, p4 14d and p6 14d each
recorded hybrid hits ALONGSIDE a miss, so parts of those plans route and one leg
does not. The shape is partially reachable, not blocked.

So §2's "done when" is half met. Wide cells that route do complete, and the hit
counter moves for exactly those. But the dashboard's latency panel is reachable
only in pieces, and the reason it is not reachable **differs per project** — three
distinct defects wearing one symptom, which is why "route more shapes" as a single
work item would have been mis-scoped.

## 3. Why did query latency regress? **Cold cache, not contention — settled by a third arm.**

The plan's free control fired at 11:33 UTC (`idle_probe.sh`, `tasks_running = 0`
before and after both probes):

| p1 `87576849` | 2026-08-22 baseline | busy arm | idle arm |
|---|---|---|---|
| throughput 1h | 199 ms | 2,430 / 7,320 ms | **8,650 ms** |
| throughput 3d | 3,297 ms | 8,217 ms | **18,150 ms** |

**Removing the maintenance load made p1 SLOWER, not faster.** Combined with the
already-recorded fact that p4 got 6x FASTER in the same sweep that made p1 8.7x
slower, "maintenance contention" is refuted as the cause. The summary should not
lean on it.

**But the idle arm is confounded, and the confound is the surviving hypothesis.**
`tasks_running = 0` only happens inside `wait_for_preload`, i.e. in the first 300 s
of a container — the drain sampler caught the same moment and recorded image
`8c37d37`, **age 4 minutes**. So the idle arm is also the coldest possible foyer
cache. It cannot separate "no contention" from "no cache", and cold cache is
exactly the cache-invalidation hypothesis the plan proposes.

What it does establish: contention is not sufficient, and every post-baseline p1
reading — busy or idle, 2.4 s to 8.7 s — is 12x to 43x the baseline. The one
thing common to all of them is that p1's files are being rewritten underneath it.

**That prediction is now untestable as written.** The plan says to re-run p1 "after
its out-of-policy cells reach zero". Per §5 below, p1's largest cell is never
claimed, so it will not reach zero on its own.

### The warm arm settles half of it

Third arm, added 12:13 UTC on a MATURE container (`5e7934b`, 24 min old,
`tasks_running = 16` — busy, but with a warm cache). Three consecutive reps of the
same query on one connection:

| p1 `87576849` | baseline | rep 1 | rep 2 | rep 3 |
|---|---|---|---|---|
| throughput 3d | 3,297 ms | 8,706 | 3,746 | **3,293** |
| throughput 1h | 199 ms | 1,783 | 3,034 | 2,592 |

**The 3d regression is cold cache, and nothing else.** Rep 3 lands at 3,293 ms
against a 3,297 ms baseline — a 4 ms difference after a reported 2.5x regression.
Repetition alone recovers it completely, on a box with all 16 maintenance workers
running. That rules out contention AND rules out any read-path change shipped this
session as the cause of the wide-window numbers.

**The 1h shape is a separate, unexplained problem.** It does not recover with
warmth — it gets worse across reps (1,783 -> 3,034 -> 2,592) and stays ~9x the
199 ms baseline. A narrow window is the cheapest possible query, so whatever this
is, it is not scan volume and not cache. This is the residual worth chasing, and it
is a much smaller question than "why did everything regress".

## 4. p1 30-day `log_list` OOM — reproduces, untouched

```
Resources exhausted: unordered merge-on-read dedup exceeded its 2048 MiB per-query budget
```

Confirmed in the completed matrix (`matrix3.csv`): p1 30d `log_list` OOMs, p6 30d
OOMs on three shapes, p4 answers the same 30d shape in 4,504 ms. Duplicate
density, not window width, as the plan says.

**One correction.** The plan says mode B "does not currently reproduce in prod"
because the scan ceiling refuses the cell first. That is not true of what ran
here: `2f7754a` (`TIMEFUSION_WIDE_SCAN_REJECT_MB`) exists only on branch
`tier1-14d-30d-complete`, is not on master, and is an ancestor of **none** of the
four images deployed during this session (`5635686`, `0bdeddf`, `8c37d37`,
`5e7934b`) — `grep WIDE_SCAN_REJECT_MB src/` returns nothing on master. No cell in
the sweep returned `REFUSED`. The OOM is live and unmasked.

Not fixed, per the plan's own instruction. The three obstacles it lists are
unchanged.

## 5. Compaction: **the fixes work; the cell that matters was never CLAIMED, and now we know why**

The fixes are confirmed live (`680acac`, `8844064`, `e16f157` are all ancestors of
the deployed image) and confirmed working:

- **The silent branch is quiet.** Four `compaction_unit_selected_nothing` events in
  40 minutes, against "every 30-60 s" before. All four are correct refusals — three
  HotPacking cells where only 1-2 of 8-16 under-target files were unsorted
  candidates, and one SealedConsolidation cell holding exactly one file for that
  project/date. No fourth silent-refusal variant.
- **Object storage agrees.** Small files fleet-wide: 1,772 (pre-fix) -> 1,273
  (v500674) -> **1,170** (v500735, 11:15) -> **1,148** (v500838, 12:30).

A third census at 12:47 (v500856) read **1,148 again — zero files retired in 17
minutes** — and `out_of_policy_cells` was 48 in all three.

So object storage tells the same story as the journal, with an independent
instrument: **−22 files in 75 minutes, then 0 in 17, and the cell count never
moves.** Files trickle out of the small cells; the large ones do not lose a single
file. `87576849 / 2026-08-19` has read exactly 238 at four census points spanning
a day.

**The residual is not a refusal, it is a claim.** `87576849 / 2026-08-19` holds
**238 small files in 1.9 GB — the single largest file-debt cell in the fleet** —
and has been at exactly 238 across every measurement since yesterday. Searching
the retained prod logs for its slice (`slice_start=1787097600000000`) returns
**nothing at all**: not a start, not a timeout, not a funnel event, for any
operation. That is **73 minutes of retained logs across 3 containers**, in which
37 SealedConsolidation claims landed on five other dates — not the "6 hours" the
`--since` flag asked for, per the window note in §1.

This matters because the funnel log cannot see it. `680acac` instrumented "a unit
was claimed and selected nothing". There is no instrument for "a cell was planned
and never claimed", and that is the branch this cell is in — the planner reports
`planned=341` every 60 s, so it is being planned.

**Mechanism 2 is PROVEN and FIXED ON MASTER** (`3465ecc`), by a unit test with
no prod dependency. `plan_compaction_debt` chooses the
operation from the calendar (`date == today` mints HotPacking, older mints
SealedConsolidation) while `is_live_frontier` stays true for a full 24 h after a
slice ENDS. So yesterday's consolidation unit holds **class 0** — strict priority
over every genuinely sealed cell — until it is 48 h old. The test asserts
`scheduling_class(yesterday).0 == 1` and fails on the current code with
`left: 0, right: 1`.

That is exactly what window A measured: 26 of 27 claims on the newest sealed day.
Class being strict priority means `scheduling_class`'s own promise — "a cell worth
200 files outranks one worth 3" — could never apply across a day boundary.

The fix (`is_frontier_task`, applied to the five sites that decide or report claim
order) is green on 906 lib tests and `cargo lint`.

**Landed as the default, by explicit decision, without a staging run.** The
tradeoff accepted: it ships on the next push by any session, and this box OOMs
under maintenance fan-in, so the first quiet window after it deploys is worth
watching. What to watch: `SealedConsolidation` claims should stop concentrating on
the newest sealed day, `out_of_policy_cells` should fall below 48 for the first
time, and the new `maintenance_hygiene_debt_unclaimed` line should stop reporting
`outranked_by` for the 238-file cell.

Both candidate mechanisms were run down. Neither is still open:

1. **Quarantine — REAL, INTENTIONAL, and not a defect.** Traced through
   `enqueue`: a unit in `Retry` carrying `WORKER_FAILURE_REASON` makes the 60 s
   re-plan **return early** rather than reset it, and the code says why — that
   reset "erased both every 60s, which is how day-wide units re-claimed every 5-8
   minutes against a >=900s floor". So quarantine survives the planner tick BY
   DESIGN, and undoing it would restore a known duty-cycle bug. `attempts` is
   never reset on any path.

   This also **explains the A/B log contrast above**, which was left unexplained
   when it was recorded. Hygiene units are not persisted (§7), so a restart wipes
   quarantine along with the queue: a FRESH container has no quarantined hygiene
   units and consolidates old sealed days (08-13/14/16), while a MATURE one has
   accumulated them and concentrates on whatever is left. That is precisely the
   68-minute vs 5-minute window difference, from the other side.

   **So there is nothing to fix here without evidence** — and the evidence is now
   collected automatically: `most_indebted_unclaimed` returns `quarantined:` as a
   first-class reason. If the 238-file cell is quarantined, the log now says so in
   as many words, and the question becomes whether the permit is sized right — not
   whether quarantine should exist.

   The numbers behind it: `QUARANTINE_ATTEMPTS = 2`, and hygiene units time out at
   900 s (SealedConsolidation: 8 timeouts in 27 claims, 30%). So two timeouts make
   a unit claimable only through the narrow quarantined-occupancy permit, and a
   238-file, 17 GB-decoded cell is the most likely thing in the fleet to have timed
   out twice. That inverts benefit ordering by attempt count — the largest debt
   becomes the least reachable work — which is a real effect, just not a bug to
   remove. Whether it is what bites here is now measured, not argued.

2. **Frontier misclassification — PROVEN and FIXED, see above** (`3465ecc`).

**Built, `f7e2717`.** `most_indebted_unclaimed` selects the eligible hygiene unit
with the most files — for hygiene that IS the debt, and the planner already
counted it — and reports either the per-task reason (`not_due`, `quarantined`,
`dependencies`) or, when eligibility is fine and the refusal is in the ordering,
`outranked_by:<project>:<date>`. `plan_compaction_debt` logs it for both hygiene
operations beside the `planned=N` that could not distinguish "nothing needs doing"
from "queued and never claimed".

Neither existing instrument could reach this: `claimability_census` and
`first_refused_sealed` both sample the first 64 tasks in journal order, so a
specific cell may never be looked at, and `first_refused_sealed` answers a bare
`CLAIMABLE` without naming the winner. `rank` was lifted out of `claim_next` into
a method so a read-only caller can ask the question; ordering is unchanged and the
19 scheduling tests that would have caught a change pass untouched.

**The instrument alone does not fix the starvation** — it makes the next diagnosis
one log line instead of a guess, which is the lesson the lever-2 page already ends
on. Both candidate mechanisms were then run down: one was a real defect and is
fixed (`3465ecc`), the other is deliberate and correctly left alone. See above.

## 6. The known-red test is **green**

`config::tests::tantivy_defaults_are_the_deserialized_ones_not_the_derived_ones`
passes on master with `src/config.rs` unmodified in the working tree. The
concurrent tantivy work fixed it. Nothing to carry forward.

## 7. The two smaller things

- **`SealedConsolidation` does not survive a restart** — confirmed directly, not
  inferred. A full journal replay (checkpoint + WAL, 41,580 tasks) contains
  `base_rollup`, `dedup`, `derived_rollup` and `repair` and **zero** hygiene
  operations of either kind, while both were running at that moment. The trade
  the plan describes is real and visible.
- **Interactive `OPTIMIZE`** — not retried; nothing observed contradicts it.

---

## Code: the coordinator WIP in the tree

Two half-finished changes were sitting uncommitted when this started — tests
written, implementation missing. Both are now complete and green
(`cargo nextest run --lib`, 22 targeted tests pass):

- `fold_fleet_gauge` — a tier younger than the coverage horizon abstains from the
  cross-tier `.min()` instead of pinning it. Prod 2026-08-24: a two-hour-old
  `dashboard_level_1m_v1` at 2 days held the fleet gauge at 2 while four healthy
  tiers sat at 30, running the cluster in coverage-short mode for a tier nobody
  queries.
- `parent_measured_bytes` — a child that did not get meaningfully cheaper than its
  parent stops bisecting, because that IS the row-group floor observed.

One trap worth recording, and it is now a test
(`a_synthetic_stamp_does_not_freeze_a_lineage`): `retry_or_split` forces a
bisection with a synthetic `MAX_DECODED_BYTES + 1`, which is a "does not fit"
signal, not a measurement. Stamping it and then comparing the next real
measurement against it would decline every split in that lineage forever — the
immortal-unit shape this file already carries three incidents of. The guard is
therefore two-sided: a child measuring MORE than its parent is evidence the
parent's number was never a measurement, so it still splits.

A concurrent session was implementing the same guard in the same working tree at
the same time — `split_declined_at_floor` appeared inside `split_time_task` while
this was being written. The two implementations were reconciled into one: the
counter and comment from that session, the two-sided guard and the synthetic-seed
test from this one. Both halves have since been committed and deployed by that
session — `8c37d37` (`fold_fleet_gauge`) and `69e6503` ("stop bisecting a slice
once it stops getting cheaper") — and the 22 targeted tests pass on the tree as it
stands at `0d8b0ed`.

Nothing was pushed by hand and nothing was deployed from this session. What was
committed locally is listed in the handoff below.

---

# Pick up here tomorrow

## What is on master and NOT yet deployed

Both land on the next push by anyone. Neither has had a staging run.

| commit | what | risk |
|---|---|---|
| `3465ecc` | `is_frontier_task` — a `SealedConsolidation` unit is never the live frontier | **Scheduling change.** Routes more large sealed partitions into the heavy path, and this box OOM-killed at 124.9 GB on a maintenance fan-in before. This is the one to watch. |
| `f7e2717` | `most_indebted_unclaimed` + the `maintenance_hygiene_debt_unclaimed` log line | Read-only instrument, logs once per 60 s planner tick. |

Shipped earlier by the concurrent session, already live: `8c37d37`
(`fold_fleet_gauge`), `69e6503` (stop bisecting at the floor).

## The first thing to do after it deploys

Wait for a container older than ~40 min (the work period), then check three
signals. They are independent — if the first moves and the others do not, the
starvation had a second cause.

1. **Claims stop concentrating on the newest sealed day.** Was 26 of 27 on a
   mature container.
   ```bash
   ssh ubuntu@captain.s.past3.tech 'docker service logs srv-captain--timefusion --since 60m 2>&1' \
     | sed -e 's/\x1b\[[0-9;]*m//g' | grep -a maintenance_task_started | grep -a SealedConsolidation
   ```
   Parse `slice_start` to a date before quoting anything — and **read the window
   you actually got**, not the one you asked for (see the `--since` trap below).
2. **`out_of_policy_cells` drops below 48** — it was 48 at all three censuses.
   ```bash
   set -a; source .env.prod; set +a
   export AWS_REQUEST_CHECKSUM_CALCULATION=when_required AWS_RESPONSE_CHECKSUM_VALIDATION=when_required
   python3 bench/local/small_file_census.py
   ```
3. **`87576849 / 2026-08-19` finally loses files.** It read exactly 238 at four
   censuses spanning a day. It is the top line of the census output.

If it is still 238, the new log line now answers why directly:

```bash
ssh ubuntu@captain.s.past3.tech \
  'docker service logs srv-captain--timefusion --since 30m 2>&1 | grep -a maintenance_hygiene_debt_unclaimed | tail'
```

`quarantined:` means the permit is the constraint (see §5 — quarantine is
deliberate, so the question is SIZING, not removal). `outranked_by:` means
something still outranks it and the line names what. `not_due:` means backoff.
No line at all means the biggest debt is winning its claims and the problem is
downstream of selection.

## Open threads, most tractable first

1. **The p1 1h shape at ~9x baseline.** The only unexplained latency finding left.
   3d is fully explained by cold cache (rep 3 lands 4 ms off baseline on a busy
   box); 1h does NOT recover with warmth — it got worse across reps
   (1,783 -> 3,034 -> 2,592 ms) against a 199 ms baseline. A narrow window is the
   cheapest possible query, so this is neither scan volume nor caching. Smallest
   open question, and it needs no prod coordination — reproduce with
   `bench/local/` against p1 and read `EXPLAIN ANALYZE`.
2. **Quarantine permit sizing.** Only worth touching if signal 3 above says
   `quarantined:`. Do not "fix" quarantine itself — `enqueue`'s early return for
   `Retry` + `WORKER_FAILURE_REASON` is load-bearing and its removal restores a
   known duty-cycle bug (day-wide units re-claiming every 5-8 min against a
   >=900 s floor).
3. **§4, the p1 30d `log_list` OOM.** Untouched on purpose — the original plan
   says "do not rush this", and its three obstacles are unchanged. Note the
   correction in §4 above: the scan ceiling is NOT deployed, so the OOM is live
   and unmasked, not hidden as the plan assumed.
4. **Reconcile with `9602e49`.** That commit concludes "the drain is BLOCKED by
   deploy churn, not waiting on wall-clock". The eight samples in §1 say the drain
   is flat *during* quiet stretches too — 435 cells unchanged through a 37-minute
   and a 38-minute pause. Both cannot be right, and the difference decides whether
   batching deploys is worth doing.

## The tools, and why each exists

All in `bench/local/` (gitignored). Create `bench/local/tfurl` first:
`grep -m1 '^TIMEFUSION_PG_URL=' ../monoscope/.env | cut -d= -f2- > bench/local/tfurl`

| tool | why it exists |
|---|---|
| `journal_snapshot.py` | The checkpoint is NOT live state — it was **2h51m stale** all session. This replays the `.wal` beside it. Any count taken from the JSON alone is a reading of hours ago. |
| `drain_sampler.sh` | Samples the journal + counters every 15 min **with the image SHA**, so a deploy annotates the series instead of silently truncating it. |
| `small_file_census.py` | Object storage, per (project, date). Counters cannot answer this — they are process-scoped and the box restarts constantly. |
| `routing_probe.py` | Reads routing counters immediately before and after each query **on the same connection**, so latency and hit/miss are one event. |
| `miss_reasons` sweep | Same, for the miss-reason breakdown specifically. |

## Traps this session hit, beyond the five the original plan lists

Each cost real time here, and each produced a reading that looked correct.

1. **`docker service logs --since 6h` returned FIVE MINUTES.** The flag asks; the
   container's retention decides, and a replaced container takes its logs with it.
   124 task-starts looked like a long-window sample and was not. **Parse min/max
   timestamps out of any log fetch before quoting a duration.**
2. **Counter attribution needs the same CONNECTION, not just the same box.** The
   first miss-reason reading said "every reason is 0 except `not_built`" — taken
   after a restart, so it described 3 fresh misses on a new process rather than
   the 15 the probe had just produced. This is the plan's own trap 1, recommitted.
3. **The idle arm is confounded with a cold cache.** `tasks_running = 0` only
   happens inside `wait_for_preload`, i.e. in a container's first 300 s — so
   "no contention" and "no cache" arrive together. It took a third arm
   (busy + WARM) to separate them, and that arm is what settled §3.
4. **Count CELLS, not units.** The sealed unit count oscillated 924-980 and once
   grew 6% in 16 minutes with **zero** new cells — units split. The unit count is
   not a backlog measure.
5. **`rollup_min_contiguous_days` read 30 on a 4-minute-old container** and 0 on
   an equally young one. The "resets to 0, needs ~25 min" rule is not reliable in
   either direction — read the gauge, do not infer it from uptime.
6. **A test can encode the bug it was written beside.** The
   `most_indebted_unclaimed` test's winner won *because* of the class-0 defect, so
   fixing the defect broke the test. That failure was the fix working.

## Working-tree hazard

This checkout is shared with concurrent sessions. During this session it entered a
conflicted rebase mid-edit, and `src/maintenance_coordinator.rs` changed under an
in-flight edit twice. If that happens again: stop editing, put a ref on your
commits (`git update-ref refs/heads/<name> <sha>`) so they cannot be lost, and
work in `git worktree add` with `CARGO_TARGET_DIR` pointed at the existing
`target/` — a second build tree is ~75 G and the disk has ~195 G.
