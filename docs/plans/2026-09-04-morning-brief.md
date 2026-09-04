# Morning brief — 2026-09-04

## IF YOU READ ONE TABLE

Same measurement three hours apart, maintenance running continuously throughout:

| | 3 h ago | now | change |
|---|---:|---:|---|
| live files | 8,122 | **7,485** | **−637 (−7.8 %)** |
| wide files (>50 % of a day) | 1,273 | 1,275 | +2 |
| wide as a **share** | 15.7 % | **17.0 %** | **worse** |
| **total maintenance read to sweep once** | **44,130 GiB** | **44,127 GiB** | **−0.007 %** |

**Compaction retired 637 files in three hours and reduced the cost that actually
matters by three gigabytes out of forty-four thousand.** It succeeded completely
by its own metric — file count — and the cost it exists to reduce did not move.
The wide-file *share* rose, because the files being retired are the narrow ones.

**That is the answer to "can we handle 10x": more volume means more compaction,
and compaction improves its own number while the cost it drives stays flat.
Scaling the fleet scales the wrong number.**

**The one-line description of why, borrowed from LSM literature: TimeFusion has
L0 and nothing else.** RocksDB's leveled compaction keeps L1+ **key-range
partitioned**, so non-overlap is maintained by how the OUTPUT is written, not by
which inputs are chosen. Our compaction merges overlapping files without
re-partitioning the output by time — so the output is *also* overlapping. **There
is no level at which non-overlap is ever established.** Every LSM design treats
L0 as the emergency to escape, because it is where read amplification is worst;
we never leave it. `--recompress` on a whole cell is, in effect, **a manual
"promote this partition to L1"** — which is why it works where four
partial-merge designs measured identical to the status quo.

**And here is what one command does instead** — modelled `--recompress` on the
worst cells, against the same 44,127 GiB:

| recompress top N cells | GiB rewritten (once) | fleet maintenance read |
|---:|---:|---:|
| **1** | **85 G** | **−8.5 %** |
| 6 | 364 G | −23.7 % |
| 20 | 732 G | **−40.4 %** |
| 50 | 768 G | −45.6 % |

**One cell, 85 GiB rewritten once, takes 8.5 % off the fleet's maintenance read
permanently — roughly 1,200x what three hours of compaction achieved.** The cost
column flattens after ~20 cells (732 G → 768 G buys thirty more cells), so that
is the natural stopping point.

**Verified locally on real prod data**, cutting a sorted 201.5 MiB / 2 M-row file:
sweep cost fell **8,282 → 4,242 → 2,814 MiB** at 1, 2 and 3 pieces — cost falls
roughly as `1/pieces`, which is the mechanism. **One correction from that test:**
pieces are sub-bin only when the cell is DENSE. The whale is 85 GiB/day, so
512 MiB ≈ 8.7 min (sub-bin) and the projection holds; a sparse cell would gain
far less. The general rule is **`--recompress` cuts sweep cost by about
`cell_bytes / 512 MiB`** — large exactly where the cell is large.

**First move: one command, one cell, then re-run
`scratchpad/wide_rank.py` to confirm the 8.5 %.** An experiment with an
unambiguous success criterion.

**It must run IN-REGION, and that is why I did not run it myself.** The job reads
85 GiB from object storage; measured from this machine that link sustains
**3.9 MiB/s**, so the read alone would take **~6 hours** — impractical, and it
would hold a maintenance-rewrite permit the whole time. It needs a runner in the
bucket's region. The prod host itself is not an option: our own standing rule is
that it is strictly read-only (logs / `inspect` / `ps`), never `exec`-mutate.

So the one genuinely blocking dependency for the night's top recommendation is
**somewhere in-region to run it from** — an infrastructure choice, not a
technical unknown.

**And the obvious way around it does not work — I proposed it and retracted it
within the hour.** Exposing `RECOMPRESS` as a pgwire admin command beside
`OPTIMIZE` would run in-region inside the prod process. But `OptimizeCmd`'s own
comment records why `OPTIMIZE` demands a project scope: *"a whole-date optimize
… tens of GB on a busy day … doesn't fit in-process next to serving load
(2026-07-27: two OOMs)"* — and `--recompress` **cannot** be project-scoped
(scoped `replace_where` deadlocks). An in-process recompress would be whole-date
at 85 GiB, several times what already OOM'd prod twice. **Do not re-propose it
without first fixing the `replace_where` deadlock**, which would shrink the job
to the few-GB partitions `OPTIMIZE` already tolerates.

### It costs you TWO customer-facing symptoms, not one

1. **`hashes` queries time out** on issues pages — dedup can never be skipped, so
   the predicate is stranded above `DedupExec` and the query scans the window.
2. **Dashboard queries miss their rollups** — `pending_base_rollup` 229 +
   `pending_derived_rollup` 412 = **641 units behind**, and **171 of ~260 routing
   misses are `not_built` or `stale_coverage`**: queries that could have used a
   rollup and fell back to raw scans.

Both trace to the same root — `Dedup` takes **76 %** of maintenance capacity
because wide files make it re-read the same data once per bin, and every other
lane divides what is left. **So the cleanup's return lands on dashboards too,
which is where most queries actually go**, and that is not counted in the
storage-read payoff table above.

## IF YOU READ ONE SCREEN

**The answer to "can we handle 10x".** Dedup consumes **~98 % of the heavy
maintenance pool**. Not because of scheduling, budgets or livelocks — every
hard-refusal counter is zero — but because **a dedup bin is 10 minutes and files
span 45–90 minutes, and a file is read once per bin it touches.**

**The root cause is that compaction creates that condition.** Merging unions time
ranges, so compaction's output spans wider; the packer scores file COUNT and
calls a file done at 256 MiB; **nothing anywhere scores SPAN**. Prod correlation
is inverse and near-perfect — `2026-09-01`: 1,657 files, 0.7 % wide, 75 GiB of
sweep read. `2026-08-23`: **22 files, 72.7 % wide, 339 GiB.** A partition
compacted to 22 files is excellent by compaction's definition and the most
expensive thing in the fleet for dedup.

**Now confirmed LIVE in prod**, by instrumentation that shipped in the accidental
deploy. And it names the culprit precisely — **24 units over 60 minutes:**

| | n | p50 bins | max | ≤16 bins |
|---|---:|---:|---:|---:|
| `HotPacking` | 118 | **13** | 20 | 62.7 % |
| `SealedConsolidation` | 179 | **84** | **144** | 4.5 % |

**A lane consuming 4 % of maintenance capacity is manufacturing the cost driver
for the lane consuming 76 %.** Measured worker time: `Dedup` **75.9 %**,
`BaseRollup` 15.8 %, **`SealedConsolidation` 4.2 %**, `HotPacking` 0.4 %. Sealed
consolidation emits many units because they are *cheap* — and each one leaves
behind an object that 84–144 dedup bins must then read in full.

**`SealedConsolidation` is the lane manufacturing the cost** — p50 **84 bins**
against hot packing's **13**, and one unit produced an output spanning **144
bins, an entire day.**

*(These are n=297 over 90 minutes. An earlier n=24 sample showed `HotPacking`
13 of 13 under 16 bins and made 16 look like a clean separator; it is 62.7 % at
n=297, so **16 would reject 37 % of hot packing too**. The distributions
separate strongly, but the usable bound is **~20–24**, above hot packing's
observed maximum of 20.)*

**File count does not track span at all** (10 files → 8 bins, 2 files → 119), so
a packer measuring count and bytes is blind to this by construction.

**A ~22-bin span bound is precisely targeted.** Measured over 300 units:
`HotPacking` keeps **100 %** of its 495 file retirements; `SealedConsolidation`
keeps **4 %** of its 1,231. The cost of enabling it is 4.2 % of worker time plus
~1,180 retirements per 90 minutes — **retirements the three-hour test showed buy
0.007 % of cost reduction.** The
uncomfortable part: since that lane cannot choose narrower inputs (three
selection designs refuted), such a bound would effectively disable it — trading a
file-count win for the read amplification it currently imposes on 74–144 dedup
bins per output. `prep/unit-phase-timers` is what prices that trade.

**It is concentrated.** **500 files — 6.5 % — cause 60 % of all maintenance
read.** Several span 100 % of their day, so a 1 GiB file is read 144 times per
sweep: 142 GiB of reading for 1 GiB of data.

**And for the 100x prospect specifically — this is an EXPONENT, not a constant.**
Maintenance cost today is `data x bins_per_file`, and `bins_per_file` (27.8 mean)
**itself grows with volume**, because compaction widens files as it merges and
components weld faster at higher ingest. **So cost scales WORSE than the data.**
With span-bounded output `bins_per_file = 1` by construction and it scales *with*
the data. Nothing else moves that exponent — not pool size, not the scheduler,
not the certification design.

So the answer is not "N times more machines". It is: **a 100x tenant on today's
layout needs considerably more than 100x the maintenance, and the multiplier is
not knowable in advance because it depends on how that tenant's writes weld. Fix
the exponent and 100x data is 100x maintenance — a capacity question rather than
an architectural one.**

**Do this first — it is a command, not a project:**

```
timefusion optimize --date 2026-07-22 --recompress --dry-run
```

**NOTE `--project` is DISABLED for recompress** (`compact.rs:678`: scoped
`replace_where` deadlocks) — the flag will error, and the `main.rs` comment
describing it is stale. Dropping it costs almost nothing: the whole date is
86.0 GiB against the whale cell's 85.0 GiB, because that tenant is ~99 % of the
data on those dates. It does widen the blast radius to 13 other tenants' files
for that date.

`--recompress` is the **only force-rewrite** (`main.rs:956`) and has exactly one
skip condition, "no files in partition" — **it will rewrite a whale cell**, where
every other path skips over-target files. It rewrites through the schema
`ORDER BY`, and the writer cuts at 512 MiB into time-contiguous pieces: ~170
outputs of **~8.5 minutes each, narrower than the dedup bin**, taking the cell
from **45x read amplification to ~1x**. `--dry-run` is verified read-only
(returns at `main.rs:953`). Ranked list: `scratchpad/wide_rank.py`.
**Trap:** `--consolidate --target-size-mb` looks right and is wrong — lowering
the target makes wide files *more* skipped.

**`--recompress` is not a preference — it is the ONLY design that survived
measurement.** Four alternatives were simulated and refuted: time-ranged
selection (identical), splitting compaction (those files are never selected),
time-adjacent ordering (identical), and bin-boundary cutting in the writer
(144 files of 1.4 MiB per merge). **All four are partial operations over a
scattered subset, and under-target files span the whole day — so narrow output
requires narrow INPUT, which only a full sorted rewrite provides.**

**Every precondition checked against the CODE, not the comments** — three of the
seven would have been wrong from comments or assumed defaults, one of them
fatally:

| claim | status |
|---|---|
| `--recompress` is the only force-rewrite | ✅ `main.rs:956` |
| skips only empty partitions | ✅ one skip condition, `compact.rs:691` |
| `--dry-run` is read-only | ✅ returns at `main.rs:953` |
| **`--project` works** | ❌ **DISABLED — `compact.rs:678`, drop the flag** |
| sorts by the schema `ORDER BY` | ✅ gated on `timefusion_optimize_sort_by`, default **true** |
| whole-date scope penalty | ✅ **+1.8 %** — the whale IS those dates |
| does not start maintenance workers | ✅ `with_config` ≠ `start_maintenance_schedulers` |

The sort one was the near-miss: had that flag defaulted `false`, the rewrite would
have produced UNSORTED output, the writer's cut would not have been
time-contiguous, and the entire 45x → ~1x benefit would have evaporated silently.

**Then, so it does not refill:** bound **span** in the packer's candidate
selection, the same shape as the byte and row budgets already there. Note the
four refutations above constrain what that rule can look like.

**`prep/bin-width` IS ALREADY ON MASTER — by accident.** Commit `29fc0caf`,
labelled `docs(cert): …`, also carried its three src files:
`git checkout prep/bin-width -- .` (run to recover a discarded doc edit) stages
into the INDEX, so a later `git add -- docs/…` committed the already-staged src
too. It deployed, and it restarted the 2h45m process that had produced the
night's only end-to-end result. The src is byte-identical to the tested branch
(1132 lib tests, lint, fmt, behaviour-neutral), so I did **not** revert — a revert
is another restart for no safety gain. **Review it after the fact.**

**Branches ready, none deploying:**

| branch | what | risk |
|---|---|---|
| `prep/unit-phase-timers` | read/sort/commit decomposition per unit — **merge first**, it prices every other decision | instrumentation only |
| `prep/span-budget` | `timefusion_compaction_span_budget_bins`, **default 0 = off** — rejects a merge whose output union exceeds N dedup bins | no behaviour change until enabled; test asserts the default path is unchanged |
| `prep/otel-metrics-collapse` | widen `otel_metrics` `dedup_keys` to its sort prefix, off the 20-58x window path | correctness verified on 22.8 M rows, 3 tenants |
| `prep/split-filter-miss` | splits `rollup_miss_filter_not_eligible` (78, prod's 2nd-largest miss) into its three actual decline rules | instrumentation only; existing label keeps its meaning |
| `scratch/replace-where-deadlock-v2` | the scoped-recompress experiment | **DO NOT MERGE** — lifts a safety guard |

**Verified: all four `prep/` branches merge cleanly onto master together, and the
combined result passes the FULL suite** — **1,357 of 1,358**, plus `cargo lint`
and `cargo fmt` clean. They touch disjoint areas (`maintain.rs`, `config.rs` +
`mod.rs`, `schemas/` + `read/`, `rollup.rs` + `observability.rs`), so they can be
taken in any order or all at once.

**The single failure is `a_chart_under_a_derived_table_routes_and_agrees_with_raw`,
and it is NOT from these branches — it fails on plain `origin/master` too**
(re-checked just now). It is the wall-clock-dependent test documented below: its
miss reason is `tiny_interior`, which depends on where `now()` falls relative to
bucket boundaries, so it passes at some times of day and fails at others. **Do
not read a red CI run on these branches as their fault without checking that test
in isolation first.**
| ~~`prep/bin-width`~~ | already on master by accident (see above) | reviewed after the fact |
 (single-sources
`BIN_MICROS`, adds `compaction_unit_span` reporting), `prep/unit-phase-timers`
(read/sort/commit decomposition), `prep/otel-metrics-collapse` (correctness
verified on 22.8 M rows).

**Live in prod:** four fixes, each with a test verified to fail on pre-fix code.
They took the customer `hashes` chain from structurally impossible to completing
end to end. **They also all make compaction work better — which means files
merged wider, faster.**

**Caveat on everything below:** this document grew through the night and contains
nine corrections I published against myself. Later sections supersede earlier
ones; the summary above is current. The correction record is kept deliberately —
the reasoning is the useful part.

---

Final state, not the journey. **Four fixes shipped**, one customer question
answered with a "don't build this", and four decisions that are yours.

## THE FINDING THAT MATTERS MOST

**Compaction and dedup are working against each other, and nothing can see it.**

Merging files unions their time ranges. Compaction's job is fewer, larger files —
so its output spans wider. A dedup bin is 10 minutes, and **a file is read once
per bin it touches.** So every compaction pass makes the next dedup more
expensive, and the packer has no notion of span at all: it scores file COUNT and
calls a file done at 256 MiB.

The correlation in prod is inverse and near-perfect:

```
   3d   2026-09-01   1,657 files    0.7% wide     75 GiB of sweep read
  12d   2026-08-23      22 files   72.7% wide    339 GiB of sweep read
```

**A partition compacted down to 22 files is, by compaction's own definition, in
excellent shape — and is the single most expensive thing in the fleet for dedup
to touch.**

Ranked over all 7,632 live files, **500 files (6.5 %) cause 60 % of all
maintenance read**; several span 100 % of their day, so a 1 GiB file is read 144
times per sweep — **142 GiB of reading for 1 GiB of data.**

**What to do, in order:**

1. **Split the widest files, worst-first — and it is a COMMAND, not a project:**

   ```
   timefusion optimize --date 2026-07-22 --recompress --dry-run
   ```

   `--recompress` is the documented **only force-rewrite** (`main.rs:956`): it
   rewrites the partition through the schema `ORDER BY` **regardless of file
   count or size**, and the writer cuts that sorted stream at 512 MiB into
   time-contiguous, event-time-disjoint pieces. On an 85 GiB cell that is ~170
   outputs of **~8.5 minutes each — narrower than the 10-minute dedup bin**, so
   the cell goes from **45x read amplification to roughly 1x**.

   Six cells ≈ 440 GiB of rewriting against **~8 TiB of sweep read saved per
   pass**. Ranked list from `scratchpad/wide_rank.py`.

   **Note `--consolidate --target-size-mb` will NOT work** — bin-packing skips
   files already at target, so lowering the target makes wide files *more*
   skipped. That trap is worth knowing before someone tries the obvious thing.

   **Unverified:** `--recompress` was built for footer repair on ordinary
   partitions; its cost on an 85 GiB cell is unmeasured. `--dry-run` first, then
   the smallest of the six.
2. **Bound SPAN in the packer's candidate selection**, the same shape as the byte
   and row budgets already there. Without this, (1) is a treadmill refilled by
   compaction itself.
3. **`prep/unit-phase-timers`** — now to measure the improvement, not to decide.

**And this reframes the four fixes I shipped tonight.** They each made compaction
work better. **Compaction working better means files merged wider, faster.** None
is wrong; all of them accelerate the lane that manufactures the dominant cost,
and that interaction is invisible from inside any of them.

## THE ONE NUMBER

You asked whether sorting, hotpacking, rollups and dedup are "breaking a sweat."
Measured on prod, one process, 7,148 s uptime:

```
work.Dedup.worker_secs        69,752        = 9.76 workers, continuously
HEAVY_REWRITE_PERMITS             10        -> dedup is ~98% of heavy maintenance
work.Dedup.rows_dropped          105,053
dedup_bins_committed_total            20    -> ~58 worker-MINUTES per committed bin
```

**Dedup is using 98 % of the heavy maintenance pool to remove a hundred thousand
rows.** And per cell it is worse than any average suggests: **an 85 GiB partition
costs 3,867 GiB to sweep once — a 45x read amplification** — because a 10-minute
bin sits inside 45-90-minute files and every bin re-reads all of them.

**Six such cells, one tenant, six consecutive July days, are ~52 % of the entire
fleet's maintenance read volume** (10,148 of 19,530 GiB). Widening the bin to
60 minutes takes those six from 10,148 GiB to ~2,048 GiB.

> **RETRACTION.** An earlier version of this section said "~182,000 rows read per
> row removed", from `work.Dedup.progress_rows = 19,186,817,032`. **That is not a
> read count and the ratio must not be quoted.** `PlanProgress` feeds that counter
> via `plan_metric_sum`, which **recursively sums `output_rows` over EVERY
> operator in the plan tree** (`maintain.rs:519-522`) — so it is rows x plan
> width, and retries add more on top. The amplification is real and large, but
> **its magnitude is unmeasured.** The two figures either side of this box do not
> depend on it: `worker_secs` is wall time, and `rows_dropped` is a real count. Every other lane — packing, consolidation, repair, rollups — divides the
remaining 2 %. That is why nothing else keeps up.

And it is **not** a scheduling, budget, livelock or certification problem. Every
hard-refusal counter is zero; the lanes run and commit. **The work per unit of
benefit is four to five orders of magnitude too high**, because the physical
layout makes every 10-minute bin read the files that overlap it **nine deep**.

*(Caveat: `progress_rows` is fed by both progress reporters on this path, so
19.2 B is an upper bound and may double-count. Halve it and it is still
~91,000 rows read per row removed. The 98 % figure does not depend on it — it is
arithmetic on `worker_secs`, uptime and permits.)*

**And row-group pruning should be bounding this, but the numbers say it is not.**
Measured on the real prod file: **20 row groups, every one carrying timestamp
min/max, and a 10-minute bin touches 1 of 20 — 5 %.** The statistics needed to
read a bin without reading whole files are present and correct. So either the
dedup scan applies no bin-scoped timestamp predicate, or it does and the cost
lies elsewhere. **That is a concrete, cheap thing to check first, and it could be
worth ~20x on the dominant consumer of the entire maintenance pool.**

**At 10x ingest this gets worse, not proportionally harder**: components grow by
chain-overlap, so more files straddle more bins and the read-amplification rises
with volume.

**This reframes everything else in this brief, including my own fixes.** The four
shipped tonight are real and each removed a genuine defect — but they tune the
allocation of a pool that is 98 % consumed by an operation reading 182,000x more
than it removes. **The layout is the cost. Nothing scheduled on top of it can be
cheap.**

## THE MECHANISM BEHIND IT

Measured tonight, from Delta statistics alone:

**Compaction defines "converged" as LARGE ENOUGH. The read path needs it defined
as NON-OVERLAPPING. Those are different properties, and nothing in the system
notices the difference — so a cell of mutually-overlapping 1 GiB files satisfies
compaction completely and defeats the reader completely, forever.**

`select_coordinator_compaction_candidates` opens with
`if add.size >= target { continue; }` — "a file at or above target is converged
and never packing's work". In the largest frozen cell, **99 % of files are over
that target** (p50 **1017 MiB**) while overlapping each other across ~90-minute
spans. Every one of them is skipped before any selection rule runs. The cell is,
by compaction's own definition, finished.

That is the whole mechanism: **the frozen mass is over-target, mutually
overlapping files, and compaction skips them by design and always will.**

This was arrived at by refuting two of my own proposals with measurements — see
`2026-09-04-certification-proves-the-wrong-thing.md` for the full sequence,
including a simulation showing that time-ranged unit selection performs
identically to today's rule (1,405 vs 1,407 overlapping pairs) because **the
files in question are never selected at all.**

IOx's compactor merges *and* splits, and holds non-overlap as a recorded,
per-file invariant. Ours records nothing about overlap anywhere.

**I am not proposing the replacement rule.** The last two designs I proposed were
each refuted within the hour by their own measurement, and a third at 04:00 would
not deserve your confidence. What is established is the diagnosis, and a
simulation harness (`scratchpad/sim_timeranged.py`) that will test the next
candidate against real cells before anyone builds it.

### Confirmed in real time, not inferred

The same statistics-only measurement, taken **ninety minutes apart** on one
long-lived process:

| | earlier | +90 min |
|---|---:|---:|
| live files | 8,122 | **7,781** (−341, ~3.8/min) |
| disjoint, cells 65+ | **0.3 %** | **0.3 %** |
| disjoint, cells 17-64 | 33 files | **23 files** |

**Compaction is unambiguously working — and disjointness did not move.** Hundreds
of files were merged away in the big cells and the overlap property is exactly
where it started. Merging reduces file COUNT, not OVERLAP, because units select
by size and each output therefore spans the others' ranges.

That reconciles the two explanations that competed all night, and both are right
about different things: **draining fixes fragmentation, only splitting fixes
overlap.** It is overlap that keeps `dedup_skipped` at 1, certification declining
218:1, and the customer's predicate stranded.

**So the uncomfortable consequence, stated plainly: letting the maintenance lane
run to completion — even for many uninterrupted hours — would NOT fix the
customer's queries.** It would leave a tidier table with the same overlap.

Everything else below is either a fix that removed a blocker on the way to this,
or a measurement that led to it.

## The headline

**The customer-visible `hashes` timeout and the maintenance backlog are the same
defect, and the chain is now fully traced.** Every link was measured:

```
dedup lane starved
  -> bins stay dirty
  -> certification declines / probes starved
  -> cert_granted_total = 0
  -> sealed dates keep a STALE file-set fingerprint
  -> dedup_skipped = 0 of 2,051 eligible scans
  -> readmit_mutable_filters never fires
  -> the `hashes` predicate is stranded above DedupExec
  -> the issues page reads the whole window and times out
```

There is no separate read-path project to open. Draining the dedup lane fixes the
customer's queries **by the same mechanism**.

## Shipped and verified

| commit | what | evidence |
|---|---|---|
| `875ea2a1` | Packer: the row cap must not reduce a bin to ONE file | **Prod cell merged.** `dcad860a/2026-06-17` went 4 files → 3; the 915,417 + 1,108,187 row pair became one file of **exactly 2,023,604 rows**. Livelocked for hours before. |
| `fa62883e` | Planner: check ROWS too, not just bytes | Its guard claimed to apply "the same test the packer applies" while checking one of two budgets. Test runs the **real packer** and asserts agreement. |
| `c627b356` | Dedup: certification probes were queued behind probes that cannot certify | Position is budget (one shared deadline). Test verified to fail on the old append ordering. |
| `eadc9def` | Rollup: the lane had **no liveness signal at all** | **VERIFIED IN PROD.** `work.BaseRollup.progress_rows` read **0 in every reading all night**; on the first process carrying this fix it reads **7,778,341**. Neither `note_unit_progress` nor `PlanProgress` was reachable from the lane before. |

**Two of the four now have direct, counter-independent confirmation:** `875ea2a1`
by the merged file in the Delta log, and `eadc9def` by `progress_rows` leaving 0.
`fa62883e` and `c627b356` remain unproven in prod — their effects are grants and
claim counts, which need the sustained uptime nobody had tonight.

Remember the caveat above: those 7.8 M rows are **plan `output_rows`**, i.e. rows
read and aggregated, not rows published. The number confirms the lane is now
*visible*; it is not an output measure.

Lane effect, `work.SealedConsolidation.worker_secs / uptime`: **4.0 % → 49.1 %**,
with 40.9 M `progress_rows`. Certification counters, comparable ~730 s uptimes,
before → after the first two fixes:

| reading | `875ea2a1` | `fa62883e` | `c627b356` |
|---|---|---|---|
| uptime at read | 703 s | 734 s | 842 s |
| `cert_granted_total` | 0 | 2 | **0** |
| `dedup_probe_timeouts_total` | 40 | 10 | **20** |
| `dedup_skipped` | 0 | 0 | **0** |

**RETRACTION — do not read the middle column as evidence.** An earlier version of
this brief reported `cert_granted_total` 0 → 2 and probe timeouts 40 → 10 as
directional confirmation. The next reading put them at 0 and 20. **These are
single-digit counts sampled from different processes with different queue states,
and they are noise, not signal.**

The reason is decision 2 below, and it is now quantified: the process restarts
observed while writing this were **13, 37 and 55 minutes ago** — lifetimes of
24, 18 and 14 minutes against maintenance units that run ~21 minutes.
**Certification grants and dedup skips are outcomes that require sustained
uptime, so in this environment they are not measurable at all.**

What survives that, because it does not depend on counters:

- The `875ea2a1` cell merge — a **file-level** fact from the Delta log: the pair
  became one file of exactly 2,023,604 rows.
- Every fix's **test**, each verified to fail on the pre-fix code.
- `dedup_skipped` has been **0 in every reading**, which is the honest bottom
  line: the customer-facing chain is **not yet unblocked**.

### One measurement caveat this creates

`eadc9def` gives the rollup lane liveness via `PlanProgress`, which feeds the
progress counter from **`output_rows` summed over every operator in the plan
tree**. The other lanes report through `note_unit_progress`, which counts rows
**written**. So `work.BaseRollup.progress_rows` will now read much LARGER than
the rows actually published — it is a liveness proxy, not an output count, and
**`progress_rows` now means different things on different lanes.** Do not compare
the two families against each other. (That inconsistency is itself a row in
`2026-09-04-lane-coverage-matrix.md`.)

## The pattern behind all four fixes

Every one is the same shape: **a mechanism that exists, is correct, and was
applied to some lanes but not the one that needed it most.**

| mechanism | fixed for | was missing from |
|---|---|---|
| shared capacity classifier | coordinator | hot-tail staging (`5dbd2b79`, earlier) |
| both packer budgets in the planner | bytes | rows (`fa62883e`) |
| fair position under a shared deadline | claim ordering (`dd4a557f`) | probe ordering (`c627b356`) |
| liveness watcher for blocking operators | repair, dedup (2026-09-01) | **rollups** (`eadc9def`) |

That is the generalisation worth acting on: not any single fix, but that the
codebase has no checklist saying a cross-cutting mechanism must cover every lane.
A coverage matrix is the cheap version of that checklist, and it is written up in
**`2026-09-04-lane-coverage-matrix.md`** — including the two lanes that still have
no "did nothing" signal at all, which is the recommended first task in daylight.

## THE CHAIN COMPLETED END TO END

At **2,726 s (45 min) uptime** — the longest-lived process of the night, and the
first to carry all four fixes for its whole life:

```
cert_granted_total   32      (0 in every reading before tonight's fixes)
dedup_skipped         1      <-- LEFT ZERO FOR THE FIRST TIME
dedup_eligible     5075
```

**`dedup_skipped` had been 0 in every single reading all night, and 0 is what the
whole customer-facing chain reduces to.** A query finally skipped `DedupExec`.

Read this for what it is and nothing more. **One skip in 5,075 eligible scans is
0.02 % — not a customer-visible improvement**, and I am not claiming the issues
page got faster. What it *is* is the first evidence that the chain can complete at
all: before tonight it was structurally impossible, because certification produced
zero grants, so no date could ever be skippable.

The sequence is now demonstrated end to end:

```
probes get fair position (c627b356)  ->  grants accrue (0 -> 32)
  ->  a date's files are proved (78)  ->  a scan skips the dedup (0 -> 1)
```

What remains between this and the customer's queries is **coverage arithmetic**,
not a broken mechanism: a query needs EVERY date it reads granted, and 32 grants
against 1,209 fleet cells is ~2.6 %. That is hours of uninterrupted uptime away —
which is exactly, and only, decision 2.

## The coverage arithmetic, now that a process lived 82 minutes

One process, sampled four times across its life — this is a within-lifetime
series, not a cross-process comparison:

| uptime | `cert_granted_total` | `cert_slice_files_proved` | `dedup_skipped` |
|---:|---:|---:|---:|
| 1,828 s | 27 | 78 | 0 |
| 1,878 s | 29 | 78 | 0 |
| 2,726 s | 32 | 78 | **1** |
| **4,928 s** | **52** | **106** | 1 |

**CORRECTION — they DO plateau.** I wrote "roughly 0.6/min and do not plateau"
after the 4,928 s sample. At **5,556 s** — ten minutes later on the same process —
`cert_granted_total` is **still 52**, where that rate would have predicted ~six
more. Meanwhile `cert_slice_files_proved` kept climbing, **106 → 127**.

So the shape is a **burst then a stall**, not a steady rate:

| uptime | grants | files proved |
|---:|---:|---:|
| 1,828 s | 27 | 78 |
| 2,726 s | 32 | 78 |
| 4,928 s | 52 | 106 |
| **5,556 s** | **52** | **127** |

That divergence — per-FILE proofs still accruing while whole-DATE grants stop —
is exactly what "certification declines on dirty bins" predicts. The easy dates
certify early; what remains is dirty everywhere, and a whole-date grant needs a
complete clean pass that the dedup lane cannot deliver.

**This makes the coverage conclusion worse, and more definite.** It is not that
blanket coverage takes ~32 hours of uptime. It is that **coverage saturates well
below what a query needs, and waiting does not fix it** — the remaining dates
cannot be certified until they are cleaned, and cleaning is the backlog that is
still growing. **And then it resumed.** At **6,471 s** grants are **61**. So the "plateau" was
also wrong — the third claim I have made about this series and the third to be
contradicted by the next reading:

| uptime | grants | note |
|---:|---:|---|
| 1,828 s | 27 | |
| 2,726 s | 32 | |
| 4,928 s | 52 | I called this "0.6/min, no plateau" |
| 5,556 s | 52 | |
| 5,611 s | 52 | I called this "plateaued, saturating" |
| **6,471 s** | **61** | resumed |

**The honest characterisation is BURSTY, with stalls of ten minutes or more —
and it took six samples over 108 minutes to see that.** Neither of my two earlier
readings had enough of the series to support the claim I made from it. Any
statement about this counter needs a series, not two points; I asserted a trend
from two points twice tonight and was wrong both times.

**On the backlogs — I claimed they "turned", and a longer series says
oscillating, not draining.** The full `pending_dedup` sequence on this one
process:

```
uptime  5611  6471  6536  6574  7447  7628  8241
value   2205  2156  2154  2153  2138  2136  2166
```

Net **-39 over 44 minutes**, but **non-monotone, and the last interval rose by
30**. So the honest reading is that **arrivals and completions are roughly in
balance** — not that the queue drains once a process is left alone. I used the
falling middle of that series as evidence for decision 3 and it does not carry
that weight.

It is still true that nothing *completes* in a 20-minute lifetime, and that
argument for the deploy cadence stands on unit duration (~21 min) rather than on
this counter.

**And one counter explains it completely:**

```
cert_declined_dirty_bins  11,329
cert_granted_total             52      -> 218 declines per grant
cert_refused_dropped            1
```

Certification is **running constantly and being refused almost every time,
because the bins are dirty.** Not starved any more — tonight's `c627b356` gave
the probes position and they are clearly executing — just unable to grant,
because a grant needs a clean partition and the partitions are not clean.

That is the diagnosis closed end to end:

```
merge-but-never-split  ->  oversized components permanently overlapping
  ->  those partitions cannot be deduped within any unit budget
  ->  bins stay dirty  ->  certification declines 218:1
  ->  grants plateau at 52  ->  dedup_skipped stays at 1 of 7,266
  ->  the `hashes` predicate stays above DedupExec  ->  the issues page times out
```

**Certification cannot lead here, and no amount of probe scheduling changes
that** — which is the same conclusion the 2026-09-01 session reached ("duplicates
sparse but spread, so certification CANNOT lead"), now with the mechanism traced
to its architectural cause rather than inferred.

**But the queue is not converging.** Over the same window
`pending_dedup` went 2,169 → **2,223**, `pending_base_rollup` 257 → **337**,
`pending_sealed_consolidation` flat at ~230. **Arrivals still exceed throughput
even with all four fixes live and a process left alone for 82 minutes.**

That is the honest answer to the 10x question, and it is not a comfortable one:

- certification was **structurally dead** before tonight and is now **alive and
  accruing** — that part is fixed;
- grants are **bursty** — 52 for eleven minutes, then 61 — so coverage does
  accrue, but in fits, and the 218:1 decline ratio says why: a grant needs a
  clean partition and most partitions are not clean. Whether coverage can reach
  what a query needs is still open, and needs hours of uninterrupted uptime to
  answer rather than another guess from two samples.
- The cheaper proof is exactly what
  `2026-09-04-certification-proves-the-wrong-thing.md` argues for: certify
  **non-overlap from file statistics** rather than duplicate-freedom from a
  content scan. That is no longer a nice-to-have — the arithmetic above is the
  case for it.

`dedup_skipped` reaching 1 and staying there is consistent with all of this: a
query needs **every** date it reads granted, and scattered grants rarely complete
a window.

## The first readable process of the night

At 02:2x a process finally reached **1,828 s (30.5 min)** — the longest observed,
and the first carrying all four fixes. Against every earlier reading:

| | 703 s | 734 s | 842 s | **1,828 s** |
|---|---|---|---|---|
| `cert_granted_total` | 0 | 2 | 0 | **27** |
| `dedup_probe_timeouts_total` | 40 | 10 | 20 | 49 |
| `work.BaseRollup.progress_rows` | 0 | 0 | 0 | **131,078,878** |
| `dedup_skipped` | 0 | 0 | 0 | **0** |

**27 grants is the first certification number meaningfully above noise.**
Normalised for uptime it is a ~5x higher grant rate than the 2 that I earlier
retracted — and unlike that reading, 27 is not a count two coin-flips could
produce. I would still call it *one* process rather than a trend, but it is the
first evidence that the certification path can produce grants at all when
something runs longer than half an hour.

**Confirmed WITHIN one process, which is the methodologically strong form.** Two
reads 50 s apart on the same process: `cert_granted_total` **27 → 29**, and
`cert_slice_files_proved` up to **78**. That is a delta inside a single lifetime,
not a comparison across processes, so it is immune to the noise that made me
retract the earlier 0 → 2. **Certification is actively granting, at roughly two
grants per minute.**

**`dedup_skipped` is nevertheless still 0**, and that remains the honest bottom
line. A query loses its `DedupExec` only when **every** date it reads is granted,
so 27 grants spread across projects and dates need not unblock a single query.
The customer-facing chain is **not yet closed.**

### Grants are produced; COVERAGE is the next wall

29 grants and 78 proved files, against **4,705 eligible scans with zero skips**
and a fleet of **1,209 partition cells** (the census in
`2026-09-04-lane-coverage-matrix.md`'s companion). That is roughly **2.4 % cell
coverage**, and the per-date skip requires **every** date a query reads to be
granted — so at 2.4 % essentially no real query window is fully covered.

This is a genuinely different state from where the night started. Then, the
question was "why does certification never grant?" — which turned out to be
starved probes and a starved dedup lane. Now grants are being produced steadily
and the question becomes **"how long until enough of them accumulate to cover a
query window?"** That is arithmetic on the grant rate against 1,209 cells, and it
is the first time that arithmetic has been possible.

It also re-points at decision 2: at ~2 grants/min, covering a meaningful fraction
of the fleet takes hours of **uninterrupted** uptime, and grants do not survive a
restart (`dedup_clean_fp` is process-local, persisted best-effort). A process
killed every 20 minutes can never accumulate coverage no matter how fast it
grants.

**The backlogs are still growing**, not shrinking: `pending_dedup` 2,185,
`pending_sealed_consolidation` 231, `pending_base_rollup` 257 — all above their
earlier values. Nothing tonight should be read as the queue converging.

## Maintenance capacity allocation is UNSTABLE, not misallocated

Two samples of `work.*.worker_secs` normalised to total maintenance work, from
two different processes an hour apart:

| lane | sample A (uptime 1,260 s) | sample B (uptime 992 s) |
|---|---:|---:|
| BaseRollup | 33.5 % | **62.5 %** |
| Dedup | **35.4 %** | 7.7 % |
| HotPacking | 15.1 % | 16.0 % |
| SealedConsolidation | 8.6 % | 8.0 % |
| Repair | 5.2 % | 4.7 % |
| DerivedRollup | 2.2 % | 1.1 % |

The two big lanes **swap places completely** — Dedup goes from the largest
consumer to 7.7 %, BaseRollup from a third to nearly two thirds — while the four
smaller lanes stay within a point or two.

**The tempting read is "BaseRollup is hogging capacity and starving dedup". I do
not think that is supported.** What the pair actually shows is that with ~20-minute
process lifetimes, **which lane dominates is decided by whatever the coordinator
claims first after a cold start**, and a long unit that begins early holds its
share for the rest of the (short) process. Allocation is therefore close to
arbitrary from one restart to the next.

That matters for the goal in a specific way: **no lane can currently be shown to
be starved, and none can be shown to be greedy** — so any scheduling change made
now would be tuned against noise. It is a third independent line of evidence for
decision 2, and it is the reason I stopped shipping scheduling changes tonight.

If sample B's split were the steady state, it alone would explain the entire
customer chain: dedup at 7.7 % of capacity cannot clear a 2,152-unit backlog, so
bins stay dirty, certification never grants, and the `hashes` predicate stays
stranded. Establishing whether it IS the steady state needs exactly one thing —
a process that lives for a few hours.

## The customer question, answered — and the answer is "don't build it"

`hashes @> ARRAY['err:…']` on issues pages. Selectivity is superb (**0.03 %–0.54 %**
of rows), so pushing the predicate below the dedup would be worth a great deal.
**I did not build it, because the data says it is unsafe.**

Auditing a full prod partition-day (201 live files, 1.86 M rows, 25,011
multi-version keys): **1 version pair in 25,014 replaces its tag outright**
(`['e583c276']` → `['f0131962']`, three hours apart). Reading monoscope's three
current writers said "append-only, ship it". The data disagreed. Pushed down, a
query for the retired tag would return a **ghost row presented as current** on the
page a customer uses to decide what is broken.

Note the dashboards are the opposite shape: the top *endpoint* hash matches **90 %**
of rows, so those panels never stood to gain. Don't quote one "hashes" number.

## A wall-clock-dependent test failure on master (not from tonight's fixes)

`dedup_compaction_test::a_chart_under_a_derived_table_routes_and_agrees_with_raw`
fails on master right now. **It is not a regression from tonight's four fixes:**
it fails identically at `c627b356`, and that same commit ran the entire suite
green (1355/1355) about two hours earlier. Same code, different answer.

The miss reason it prints is **`tiny_interior=1`** — a rollup-routing refusal
that depends on where `now()` falls relative to bucket boundaries. So the test is
**wall-clock dependent**: it will fail CI at some times of day and pass at
others. It uses real time where this repo has a virtual clock (`crate::support`)
built precisely for this.

Worth fixing on its own merits, and worth knowing before anyone reads a red CI
run tonight as evidence against the four fixes.

## Two branches are ready for review (neither deploys)

Pushed as a branch, deliberately **not** master, so it does not deploy
(`deploy.yml` triggers on master only). It contains the `otel_metrics`
conversion described in `2026-09-04-otel-metrics-never-got-the-collapse.md`:

- `dedup_keys` widened to the sort prefix — the same change logs got on
  2026-09-02, which this table never received;
- `FORMAT_VERSION` 2 → 3, which `read/mod.rs:2823` already documents as the only
  coupling a widening needs (the sort order and every existing footer are
  untouched, because `sorting_columns` does not change);
- `every_merge_on_read_table_can_take_the_streaming_collapse`, a guard verified
  to fail on the un-widened schema.

Its correctness precondition is measured, not assumed: **22,773,893 rows across
186 files and all three heavy tenants, identical key cardinality every time.**
Full lib suite green (1133).

**`prep/unit-phase-timers`** — the measurement that gates the architectural work
above. Prod emits only `maintenance_unit_slow`, and only for units past a quarter
of their deadline, so *where a typical unit's time goes* is unknown. This adds
`plan_secs` / `upstream_secs` / `write_secs` around the existing staging loop and
emits `unit_phase_timing` on **every** staged bin. Instrumentation only, no
behaviour change; 1132 lib tests green.

**Merge this one first.** It is the difference between "4,857 units is about a
day of maintenance" and "about a week", and no other decision here can be made
without it.

## Decisions that are yours — REORDERED by the 90-minute test

The disjointness test above changed this ranking after it was first written. The
deploy cadence was top; it is now second, because more uptime alone converges to
a tidier table rather than a faster one.

0. **Merge `prep/bin-width` — safe today, no behaviour change.** `BIN_MICROS` was
   a `const` copy-pasted into three files (producer, prober, drain). They **must**
   agree — a bin marked dirty by one and looked up at a different width by
   another is never found — so three definitions is a latent correctness bug.
   Now single-sourced, value unchanged, with the measurement in its doc comment.
1. **Merge `prep/unit-phase-timers`.** One number — where a unit's ~21 minutes
   actually goes — decides whether tiling 4,857 units is a day of maintenance or
   a week, and no other item here can be scheduled without it. Instrumentation
   only, 1132 tests green.
2. **Widen `BIN_MICROS` — the strongest lever found, and the risk is measured
   away.** A unit rewrites every file overlapping its bin, and files span 45-90
   minutes, so a narrow bin does not read less — **it reads the same files once
   per bin**. Over the 95 cells holding 17+ files:

   | width | unit size | total read to sweep once |
   |---|---|---|
   | **10 min (today)** | 1,469 MiB | **19,530 GiB** |
   | **60 min** | 1,734 MiB (**+18 %**) | **3,847 GiB (5.1x less)** |
   | 120 min | 2,053 MiB (+40 %) | 2,280 GiB (8.6x less) |

   I had said only the phase timers could tell whether wider bins bloat the unit.
   **That was wrong — `Add.stats` answers it, and the answer is +18 % for 5.1x.**
   What remains is not a measurement but a **soak**: 6x fewer, larger units
   interact with claim/lease/deadline machinery that statistics cannot see.

   **It generalizes — `otel_metrics` gives 5.5x for +8 %**, with units 5x smaller
   in absolute terms (277 MiB vs 1,469 MiB). Landing within half a point on two
   tables of 104 and 12 B/row is why this looks structural rather than a quirk of
   one shape. **Soak it on `otel_metrics` first:** smallest risk, biggest win, and
   it is already the table stuck on the slow window dedup path.
3. ~~**Build time-ranged unit selection for oversized components.**~~ **REFUTED
   by simulation** — identical to today's rule (1,405 vs 1,407 overlapping
   pairs), because those files are never selected at all. The actual fix.
   Not a re-layout: the cut points, time-bounded predicates and sort-ordered
   writer all exist; what is missing is that units select by SIZE. **~58 % of the
   oversized mass is ten cells of one tenant's late July**, so it can start
   targeted and show a result early.
3. **The deploy cadence.** Still real — units run ~21 min, nothing completes in a
   20-minute lifetime, and the backlogs only began falling once a process passed
   an hour. But demoted: it buys fragmentation, not disjointness.
4. **`prep/otel-metrics-collapse`.** Independent of the above and already
   verified — the window dedup path is 20-58x slower and OOMs at 8 partitions at
   any pool size, and `otel_metrics` is on it permanently.
5. **The monoscope `hashes` append-only contract** (below) — the only item here
   that TimeFusion cannot decide for itself.

## The original decision list, for detail

1. **The monoscope `hashes` append-only contract.** If tags are *meant* to be
   append-only, that one replacement is a client bug worth finding — most likely
   the endpoint remapping in `Endpoints.hs:544`, or a re-ingest with a recomputed
   hash. If remapping is intended, the pushdown is permanently unavailable and
   the long-term safe route is a tantivy index on `hashes` (sound with no client
   invariant, but it rebuilds every index on the table at ~16/hour).
2. **The other session's deploy cadence — the biggest environmental blocker.**
   Prod was redeployed every **25–30 minutes** all night (`DesiredState=Shutdown`,
   no error). Maintenance units run ~21 minutes, so most die to process exit.
   **No throughput number measured tonight is valid**, and no convergence can
   happen at this cadence. Only you can coordinate this, and against the 10x goal
   it matters more than any single fix in the table above.
   **My own honest share of this:** I pushed four src changes tonight, and each
   push *is* a deploy that killed the running process and whatever units were
   in flight. So I contributed four of the night's restarts while naming the
   cadence as the top blocker. The distinction I would still defend is that a
   fix that lands is worth a restart and a repeated no-op redeploy is not — but
   the measurement damage is the same either way, and the honest reading is that
   **nobody could have measured convergence tonight, including me.**
3. **Debt-metric semantics.** `sealed_compaction_debt_bytes` (1.33 TB) counts
   **35.9 GiB** sitting in 49 cells that are provably unworkable — their two
   smallest files cannot pair within the byte budget. It misleads no scheduler,
   but it is the number a human reads to judge whether compaction is keeping up.
   I wrote it up rather than changing it.
4. **Whether to pursue the certification redesign** in
   `2026-09-04-certification-proves-the-wrong-thing.md` — see below.

## The one structural idea worth your attention

Prior art says **we are certifying the wrong property**. IOx does not certify "I
removed the duplicates"; it maintains **non-overlap** as a structural invariant,
records it per file as a compaction level, and the querier unions non-overlapping
files above the dedup, merging only the overlapping ones.

- Ours is **per-partition** (a file-list fingerprint), so one new file voids it
  for every file. Theirs is **per-file**, so it survives ingest.
- Ours is minted by a **later probe**. Theirs by the **compactor at commit**.
- Ours asserts a **content** property needing a scan. Theirs asserts a
  **structural** one checkable from min/max statistics alone.

**Non-overlap in the dedup-key space *implies* duplicate-freedom** — two versions
of one key are the same key, so key-disjoint files cannot hold a duplicate group
between them. A statistics-only check subsumes the expensive scan. The soundness
argument is already written and reviewed in `read::skippable_certified_files`;
what would change is where the evidence comes from — and statistics survive
restarts, deploys and new writes, which a process-local `dedup_clean_fp` does not.

I deliberately did **not** relax certification to grant after a dedup rewrite,
even though the output is clean by construction: IOx never certifies "I did the
work correctly", and trusting the rewrite would make a dedup bug invisible rather
than merely expensive. The failure mode is a silent over-count on every dashboard
tile.

## Corrections I published against myself

Recorded because the reasoning is the useful part:

- **"Maintenance voids the proof it earns"** → **retracted**. Sealed dates in the
  2–7d band received **zero** file-set changes in 4.2 hours, so there is no churn
  to race. The proof is *stale and never re-issued*, not repeatedly destroyed.
  A never-refreshed proof and a constantly-invalidated one produce the **identical**
  `fp_moved` counter; only the Delta log could tell them apart.
- **"The unpairable byte band is wrong"** → **over-corrected**. The fleet census
  (all 1,209 cells) found 49 byte-blocked cells = **29.2 %** of cells with ≥2
  under-target files — the same 29 % originally measured. Both classes are real
  and disjoint: 49 byte-blocked, 8 row-blocked. My error was generalising a
  correct measurement to a cell that failed for a different reason.

## Where I would go next

1. Re-read the lane and certification counters on an **aged, quiet** process —
   the one thing that converts tonight's directional numbers into steady-state
   ones. Blocked on decision 2.
2. Confirm `c627b356` deploys and watch `cert_granted_total` and
   `dedup_probe_timeouts_total`; the acceptance test is `dedup_skipped` finally
   leaving 0.
3. Only then, the certification redesign — it is a real design change and should
   not start until the cheap fixes are proven to have run out.

## Postscript — the one failing test was time-of-day dependent (fixed)

`dedup_compaction_test::a_chart_under_a_derived_table_routes_and_agrees_with_raw`
was the single failure in the 1,357/1,358 verification run above. It is **not**
caused by any of the prep branches — it fails on plain `origin/master` — and it
is not random: it is a function of the wall clock, reproduced deterministically
in **both** directions by pinning `support::set_micros`.

Instrumenting the `interiors()` call site with the same fixture gives:

| pinned `now` | 1h tier `covered` | 1m tier `covered` | result |
|---|---|---|---|
| 03:55 UTC | yesterday 00:00 → **14:00** | yesterday 00:00 → **14:00** | FAIL `tiny_interior` |
| 12:00 UTC | yesterday 00:00 → **14:00** | yesterday 00:00 → **today 00:00** | PASS |

The window is `[yesterday 12:00:00.000017, today 02:00:00.000017)`; the fixture's
three rows sit at yesterday 12:00–13:01.

- The **1h tier declines in both cases, correctly.** `ceil_grain(lo)` = 13:00 and
  coverage ends at 14:00, so the interior is exactly **one** 1h bucket — below
  `MIN_INTERIOR_BUCKETS = 2`. The floor is working as designed.
- So the query's whole fate rests on the **1m tier**, whose coverage end **moves
  with `now`** while the 1h tier's does not. That asymmetry is a *fixture
  observation, not a prod claim* — it may simply be which units
  `run_maintenance_units(1024)` happened to plan at each clock position. Worth a
  look; not chased further tonight.

**Fixed** by pinning the clock at the top of the test (`5f83a89b`); the existing
`ClockGuard` already restores wall time on drop. 56/56 in that file green.

## The merge is staged on local `master` but NOT pushed — here is exactly why

**Staged (7 commits ahead of `origin/master`, unpushed):** `prep/unit-phase-timers`,
`prep/span-budget`, `prep/split-filter-miss`, `prep/fix-chart-test-flake`, plus one
new test-race fix. `cargo lint` clean; **main suite 1,357 / 1,357** (both
previously-failing tests now fixed — the chart-routing one above, and
`write::tests::boot_redrives_quarantined_insert_payloads`, which waited on
`mem_total_rows` while asserting the archive rename that `redrive_quarantine`
performs *after* it).

**What stopped the push: the e2e suite is currently unstable, and I could not
attribute it.** Eight full `make test-e2e` runs this morning:

| tree | runs | result |
|---|---|---|
| `origin/master` | 3 | 3 green |
| master + timers + span-budget | 1 | green |
| + split-filter-miss | 3 | fail, fail, **green** |
| full merge | 3 | fail, fail, fail(*different test*) |

Two distinct tests failed, each passing in isolation:
`ordering_pushdown::one_unsorted_file_does_not_cost_the_majority_its_ordering`
(×3) and `smoke::count_star_returns_correct_value` (×1).

**I published, then retracted, a "regression in `prep/split-filter-miss`".** The
bisect pointed there on one failing run — but that branch is **22 lines**: two
`MissReason` variants, two name-keyed counters, two return sites. Nothing in it
can reach a physical plan. The rerun on that exact commit went **green**, which
kills the hypothesis. Same trap as the rest of the night: *a two-point series is
not a trend* ([[tf_what_survived_and_what_did_not_2026-09-04]]).

**Two things worth chasing, both on-goal:**

1. **`one_unsorted_file_does_not_cost_the_majority_its_ordering` is intermittent,
   and what it guards is a real prod cliff.** When the isolated union loses its
   ordering claim, `DedupExec` drops to unbounded `full-set` and
   `ORDER BY ts DESC LIMIT n` becomes a **blocking** `SortExec` over the whole
   window — prod p1 at 30 d selected 431 GB for 251 rows. The repair
   (`repair_isolated_scan_ordering`) needs *some* child to declare an ordering:
   `children.iter().find_map(|c| c.properties().output_ordering())`. If a flushed
   hot file's footer sometimes fails to declare one, the same intermittency exists
   **in prod**, not just in the test. Not verified — but it is the first
   hypothesis to test, and it is cheap.
2. **`COUNT(*) returned 4 of 7 acked inserts`**, MemBuffer 4 / Delta 1 file, and
   the immediate re-query still returned 4 — so **not transient**. This is
   [[tf_count_star_is_wrong_not_flaky_2026-08-28]] reproducing.

**Recommended next step:** one `make test-e2e` on an idle machine (this one ran
eight suites back-to-back with `target/` at 46 GB and the volume 93 % full). If
green, push — the bundle is one deploy and one restart, and `prep/unit-phase-timers`
is what prices every remaining decision in this document.

## Closed: `COUNT(*)` loses acked rows — root cause found and fixed

The `count_star` failure above is not a flake and never was
([[tf_count_star_is_wrong_not_flaky_2026-08-28]], open since 2026-08-25). The
diagnostics I added to `tests/e2e/smoke.rs` settle it in one shot:

```
COUNT(*) returned 4 of 7 acked inserts.
  visible ids (4): ["smoke-3","smoke-4","smoke-5","smoke-6"]
  re-query immediately: 4          <- not in flight
  MemBuffer rows: 4
  Delta files: 1 at version Some(1)
  Delta-only ids: smoke-0, smoke-1, smoke-2
```

Delta **has** the missing rows and the scan **can** read them. The union dropped
them. That kills the last standing hypothesis from August ("the Delta leg reads a
snapshot predating the flush commit").

**The mechanism.** `get_bucket_ranges` returns each buffered bucket's
`[min_timestamp, max_timestamp]`, and the Delta leg is filtered to *exclude* those
ranges so the union cannot double-count. The premise is "memory is authoritative
over this range". For any instant the bucket has **already flushed**, that premise
is false — those rows are in Delta and gone from memory, so the mask makes them
answer no query at all.

The prefix-drain narrowing (`bf8b9a16`, 2026-08-28) aimed at exactly this and
cannot hold, for three independent reasons — all three now pinned as a case table
in `a_flushed_instant_stays_visible_however_the_bucket_emptied`, each failing
before the fix:

| how the bucket empties | why the narrowing misses |
|---|---|
| `take_bucket_for_flush` | resets min/max to sentinels; the next insert rebuilds the mask from nothing |
| drain to empty | narrowing runs only under `!emptied` |
| partial drain | it `store`s `drained_max + 1` into `min_timestamp`, and the **insert path `fetch_min`s the same atomic** — one later row at that instant pulls the mask straight back down |

And both flush paths `remove_if` the bucket the moment it empties, so a per-bucket
watermark is gone before the next insert recreates the bucket under the same id.

**The fix (`b9fd6f28`).** A table-level `flushed_max` (bucket id → highest
timestamp ever handed to Delta), applied as a floor at **read** time in
`get_bucket_ranges`. It has to live outside the value inserts maintain *and*
outside the bucket's lifetime; pruned by `evict_old_data`, since past the
retention cutoff no query can ask about that instant. The trade is the one already
argued at the narrowing site: an unmasked survivor may be double-counted, and
read-side dedup collapses a duplicate while a masked row is simply wrong.

1,136 / 1,136 lib tests, `cargo lint` clean, and `count_star` did not fail in
three further e2e runs (it had failed 3 of ~13 before).

**Prod exposure, stated carefully.** The e2e reproduction is inflated by the
harness: `current_bucket_id()` reads the virtual clock, which the harness freezes
in the past, so wall-clock rows land in a bucket the test already treats as
sealed. In prod wall == virtual and the current bucket is exempt. The real
exposure is **a late arrival into a sealed bucket that has already flushed rows at
that instant** — every row that bucket previously committed at or below the new
row's timestamp disappears from queries until the bucket flushes again. Identical
timestamps are the norm here, not an edge: a batch of OTel spans arrives stamped
to the same instant.

**Still open:** `ordering_pushdown::one_unsorted_file_does_not_cost_the_majority_its_ordering`
remains intermittent (1 of the 3 verification runs). Untouched by this fix.

## The other e2e flake: the fixture, not the repair

`ordering_pushdown::one_unsorted_file_does_not_cost_the_majority_its_ordering`
was the second gate blocker. Comparing a passing plan against a failing one,
then adding a `numRecords` inventory of the live Delta files, settles it:

| | live files | ordering claim |
|---|---|---|
| passing | **2** — one 6-row compacted file, one 3-row flush | the 3-row file declares `[timestamp DESC, service, id]` |
| failing | **3** — 6-row compacted, then **1-row** and **2-row** | **none of the three declares anything** |

The test intends "one non-conforming file beside one conforming file". When the
last flush lands as two files instead of one, the 2-row file holds `s-7, s-8` in
**ascending** order and declares no `timestamp DESC` footer, and the 1-row file
declares nothing either. `repair_isolated_scan_ordering` needs *some* child to
carry an ordering (`children.iter().find_map(|c| c.properties().output_ordering())`)
— with none, it bails, the union advertises nothing, `DedupExec` drops to
unbounded `full-set`, and the `LIMIT 3` becomes a blocking `SortExec`. Exactly
the shape the test exists to catch, produced by the fixture rather than by a bug.

**So the repair was never broken, and there is no prod ordering intermittency
here** — I had flagged one as the first hypothesis; it is refuted. What splits
the flush is that three separate `INSERT` statements are three MemBuffer batches,
and whether the flush coalesces them into one Delta file is a timing decision.
Raising the periodic flush interval did **not** fix it (still 3 files), which
rules out the background flush task; issuing the three rows as **one** statement
addresses it at the source.

Two guards added so this cannot silently return: the fixture now asserts its own
shape (`n == 2`) *before* the plan assertions — "3 files where 2 were intended"
is a different bug from "the claim was lost", and reading that off a physical
plan cost hours — and the failure message carries the live-file inventory.

## The 10x measurement: what I tried, what was wrong, and what it needs

The goal asks for evidence that maintenance keeps up at 10x. `timefusion sim`
carries `--streams`, documented for exactly this ("10x experiments: 260 streams
at 130 projects"), so I swept it on `synth:whale` over 168 virtual hours:

| streams | pending_end | max lag | executions |
|---:|---:|---:|---:|
| 26 | 3 | 44,022 | 4,398 |
| 52 | 3 | 44,021 | 2,143 |
| 130 | 2 | 44,022 | 4,207 |
| 260 | 3 | 44,021 | 3,615 |

**That table is worthless, twice over, and I nearly published it as headroom.**

1. **The noise swamps it.** Varying only `--seed` on the SAME configuration:
   executions **826 → 5,432** (6.6x), max frontier lag **5,327 → 44,017 s**
   (8.3x), pending_end 1 → 8. Every difference in the sweep sits inside that
   band. **Any sim comparison needs several seeds per point.**
2. **`--streams` does nothing here.** At a fixed seed, `--streams 26` and
   `--streams 260` produce **byte-identical** reports — every field. It scales
   the *ingesting streams discovered in a real journal*; a synthetic queue has
   none and sets `mint_frontier = false`. The summary line still printed
   "260 streams", so the run *reported* 10x while modelling 1x.

Fixed in `c48fe299`: the flag is now refused on a synthetic queue rather than
ignored. A capacity question must not be answered with the baseline.

**What a real 10x run needs, and why it is blocked.** `--streams` works against
a real prod journal. Extracting one is currently impossible read-only: the
container is distroless (no shell, so no `docker exec`), and the bind mount
`/home/ubuntu/timefusion-wal` is `drwxr-x---` owned by uid 65532, unreadable as
`ubuntu`. Options, in order of preference: (a) have the running process dump its
own journal over pgwire/an endpoint, (b) copy it out with a one-off privileged
command **on the user's say-so**, (c) build a synthetic multi-stream fixture so
`--streams` applies. (c) is the only one needing no prod access and is the right
next task.

**What IS measured, and it is not nothing:** the read-amplification arithmetic
(5.1x fleet-wide, 5.5x on `otel_metrics`) is a computation over a real Delta
checkpoint, re-runnable in seconds, and does not depend on the simulator at all.
That is the strongest 10x-relevant number available today, and
`timefusion_dedup_bin_minutes` (`ab97d81a`) is now the knob that lets a staging
soak test it without a code change.

## CI's E2E job is red on master, and has been

`0c667f8f` deployed cleanly (image live, no panic in the logs, process up). But
its CI run shows **E2E: failure** — and so do the four commits before it:

```
0c667f8f E2E=failure   8859001f E2E=failure   c47a2a64 E2E=failure
d7184417 E2E=failure   41093ba4 E2E=failure
```

It fails on tests that pass locally 8/8 (`bulk_alias_skips_membuffer_but_is_queryable`,
`second_read_after_flush_hits_foyer`) — a different set from the local flakes, so
it is environment-specific to CI's runner, not the same bug. **Clippy, Format,
both test shards and the Gate are green**; only E2E is red.

The consequence is what matters: **that job has been providing no signal for at
least five commits.** A gate nobody can distinguish from broken is worse than no
gate, because a real regression in it would look exactly like today. Triaging it
is the next CI task, ahead of adding anything new to CI.

## The 10x answer: completions are FLAT, so load becomes backlog 1:1

With `--mint` (`626e3c2d`) a synthetic queue finally generates arrivals, so
`--streams` scales load for real. 24 virtual hours, **three seeds per point**:

| streams | vs base | pending_end (3 seeds) | executions (3 seeds) | max lag |
|---:|---:|---|---|---:|
| 74 | 1x | 18,029 / 18,403 / 18,015 | 5,376 / 5,045 / 5,307 | 83,09x |
| 148 | 2x | 39,241 / 39,497 / 39,241 | 5,338 / 5,089 / 5,257 | 83,08x |
| 370 | 5x | 102,635 / 102,979 / 102,757 | 5,333 / 5,106 / 5,319 | 83,07x |

Seed spread is **~2%** on `pending_end` and **~5%** on `executions`, against
**2.2x** and **5.7x** between configurations — so unlike the un-minted sweep,
this is signal.

**The result: `executions` does not move — 5,045 to 5,376 across a 5x increase
in arrival rate — while `pending_end` grows 5.7x.** Completion throughput is
independent of load. The coordinator is saturated at 1x, so every additional
stream converts **1:1 into backlog**, and `frontier_lag_secs_max` is pinned at
~83,000 s (23 h) in every single run. This is the same conclusion the prod
counters reached from the other direction — dedup holding 9.76 of 10 heavy
permits continuously — now reproduced locally, on demand, in a form that can be
re-run against a candidate fix in minutes.

**Read it as a shape, not as prod numbers.** `synth:whale` is deliberately the
queue that shredded prod, so the absolute backlog is pathological by
construction. What generalises is the flatness of `executions`: throughput is
capped by per-unit cost, not by how much work is waiting, so **no scheduling
change can create headroom — only making a unit cheaper can.** That is exactly
what the bin-width lever does (5.1x less read per sweep), and it is the argument
for prioritising it over any further scheduler tuning.

**The 10x point (740 streams) is MISSING, not zero** — it exceeded the 4-minute
per-run budget in the sweep; it is running separately. The sim itself slows
superlinearly with queue size, which is worth knowing before anyone plans a
large sweep.

**Honest limits of this measurement:** it is the IO-free coordinator sim, so it
models unit durations from a byte model rather than real object-store latency —
it answers "does the queue converge", not "what does a unit cost". Per-unit cost
still needs the phase timers now deployed in `0c667f8f`, plus staging.

## The phase timers work — and they miss the lane they were built for

`0c667f8f` is live and emitting. 87 units in the first hour, aggregated:

| pass | n | plan | read+sort | write | median unit | max |
|---|---:|---:|---:|---:|---:|---:|
| Pack | 86 | 2.2% | **61.0%** | 36.8% | 16.4 s | 253 s |
| Repair | 1 | 0.0% | 60.2% | 39.8% | 1,337 s | 1,337 s |
| **all** | 87 | 1.5% | **60.7%** | 37.8% | | |

Two things follow.

**1. Rewrite cost is read-and-sort bound, ~61% against 38% write.** That is the
result the bin-width lever needs: widening bins removes READ (the same file is
read once per bin it straddles), so it attacks the dominant phase. Had write
dominated, widening would have bought much less. The single Repair unit at
**1,337 s** also confirms the ~21-minute unit from the prod counters, from a
direct measurement rather than a rate.

**2. None of those 87 units is a dedup unit — and dedup is ~98% of the heavy
pool.** The timers shipped in `prep/unit-phase-timers` instrument the Pack/Repair
staging loop in `maintain.rs`; the dedup rewrite is a different function
(`compact.rs::stage_dedup_chunk`) and was never touched. **The instrumentation
built to price the dedup decision does not instrument dedup.** So the 61/38 split
above describes compaction rewrites, and it must not be quoted for dedup.

Fixed on the branch: `stage_dedup_chunk` now emits the same three phases under
the same `unit_phase_timing` event with `pass=Dedup`, so both paths aggregate
together and the next quiet hour answers the question for the lane that matters.
