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
| 740 | **10x** | **208,532** (1 seed) | **5,347** | 83,084 |

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

**The 10x point landed** (it needed a 25-minute budget, not the sweep's 4 — the
sim slows superlinearly with queue size). It is one seed, but it falls exactly on
the line: **executions 5,347, inside the 5,045–5,376 band every other
configuration produced, while backlog reached 208,532 — 11.6x the 1x run.**

So across a **10x** increase in arrival rate, completion throughput moves by
**under 6%**, which is within the seed noise. There is no headroom at all: the
system is throughput-saturated at 1x and converts additional load into backlog
essentially 1:1.

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

## How much cheaper must a unit be? Sub-linear, and cost alone cannot fix it

Same fixture at **5x load** (370 streams), 24 virtual hours, two seeds, sweeping
`--scale` (unit duration — i.e. per-unit cost):

| unit cost | executions | vs base | pending_end | vs base |
|---|---|---:|---|---:|
| 1x (base) | 5,342 / 5,100 | 1.00x | 102,721 / 103,027 | — |
| **2x cheaper** | 5,818 / 5,566 | **1.09x** | 101,533 / 101,635 | −1% |
| **4x cheaper** | 9,715 / 9,703 | **1.82x** | 96,681 / 96,689 | −6% |
| **10x cheaper** | 23,028 / 23,137 | **4.32x** | 83,244 / 83,134 | −19% |

Seeds agree to within 0.5% at every point.

**Three results, and the first two temper what I wrote earlier.**

1. **Throughput responds SUB-LINEARLY to unit cost**: 10x cheaper units buy only
   **4.3x** the completions. Something else takes over as the constraint before
   cost stops mattering.
2. **A 2x-cheaper unit buys almost nothing (+9%).** There is a threshold between
   2x and 4x — presumably units dropping under a deadline or permit boundary —
   so a modest cost win can be worth approximately zero throughput.
3. **Even 10x cheaper units do not drain a 5x queue**: backlog falls only 19%.

**AMENDMENT to "only a cheaper unit can create headroom".** That was too strong.
The first half stands — the streams sweep shows scheduling cannot help a
saturated pool. But cheapness alone is *not sufficient*, and its returns are
sub-linear and lumpy.

**AND A TRAP I NEARLY WALKED INTO: `--scale` is NOT a model of bin widening.**
It makes every unit uniformly cheaper. Bin widening does something different —
it makes **~6x FEWER units, each ~18% bigger**. That is the *arrival* axis, not
the cost axis. Mapping the 5.1x read reduction onto the `--scale` column would
have put the lever at the "2x cheaper → +9%" row and argued *against* it, on a
false equivalence.

**Read the two sweeps together and they point the same way:**

- streams sweep: **backlog is LINEAR in the number of units** (11.6x at 10x load)
  while throughput is flat.
- scale sweep: **backlog is barely sensitive to unit cost** (−19% for 10x
  cheaper).

**So the lever that matters is the one that reduces the NUMBER of units, not
their cost — and bin widening is exactly that (~6x fewer dedup bins).** The 5.1x
read reduction is a real IO/£ saving, but the *queue* argument for widening is
the unit-count collapse, and that is the stronger argument.

**Next experiment, and it is now well-posed:** teach the sim to model dedup bins
so unit COUNT can be swept directly, rather than inferring it. That is the
measurement that would justify flipping `timefusion_dedup_bin_minutes` in
staging.

## The sim cannot validate bin widening — and that closes the line, it does not stall it

I said the next experiment was "teach the sim to model dedup bins so unit count
is a direct input". I built that knob (`--debris-slice-minutes`, same total work
as `600 / n` units of `n` minutes) and swept it. **Both configurations are null,
for two different reasons:**

| config | what moved | what did not |
|---|---|---|
| `--mint`, 5x load | nothing | 600 → 300 units changed `pending_end` by **0.1%** (102,696 → 102,804) — the debris is 0.6% of a ~102,000-unit queue |
| no minting | `pending_start` **813 / 513 / 313 / 263** for n = 1/2/6/12, exactly as designed | `executions` and `pending_end` **IDENTICAL at every width** (1,774 and 2 on seed 1) |

The second is the interesting one: the knob provably works — the starting queue
collapses as intended — and the outcome does not change at all. **The
coordinator's own coarsening already fuses those units, so pre-collapsing them
buys nothing.**

**The structural reason, which generalises past this fixture:** the simulator
schedules rollup/compaction TASKS on virtual time. Widening `BIN_MICROS` pays off
in **read bytes** — the same file read once per bin it straddles. An IO-free
model cannot see bytes, so it cannot see the benefit *by construction*. No amount
of extra modelling fixes that without making the sim do IO, at which point it is
staging.

**This is a conclusion, not a dead end.** It says: stop trying to price bin width
locally; the repo's own ladder already said so — *"MinIO validates correctness,
not cost, because per-unit cost is object-store round trips"*. The validated plan
is therefore:

1. `timefusion_dedup_bin_minutes = 60` in **staging**, on the seeded whale days.
2. Watch `unit_phase_timing` with `pass=Dedup` (now emitted, `c5edeca6`) —
   `upstream_secs` is the phase widening should collapse.
3. Compare against prod's 10-minute baseline over the same cells.

**What the local work DID establish, and it is the useful part:** backlog is
linear in unit count and nearly insensitive to unit cost, throughput is flat
under 10x load, and rewrite wall-clock is ~61% read+sort. Those bound what any
fix must do. They just cannot score this particular fix.

## The phase split is steady-state, and two units eat a third of the pool

Re-read on a **2-hour-old process**, 205 units over 110 minutes:

| pass | n | plan | read+sort | write | median | p90 | max | worker-min |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Pack | 203 | 1.5% | **61.2%** | 37.3% | 13.3 s | 85.7 s | 292 s | 95.8 |
| Repair | 2 | 0.0% | 59.5% | 40.5% | 1,158 s | 1,337 s | 1,337 s | **38.6** |
| **all** | 205 | 1.0% | **60.7%** | 38.3% | | | | 134 |

**The split is identical to the 87-unit young sample** (60.7 / 37.8 / 1.5), so it
is steady-state rather than a warm-up artefact — the first measurement here that
survives the "quiet process" rule instead of needing a caveat.

**And the heavy tail is right there in it: 2 Repair units consumed 38.6 of 134
worker-minutes — 29% of everything measured — against 203 Pack units at 95.8.**
Median Pack unit is 13 s, p90 is 86 s, max 292 s. Consistent with
`tf_unit_size_heavy_tail_2026-09-02` (6.4% of units carried 67.1% of bytes),
now confirmed on wall clock rather than bytes.

Still no `pass=Dedup` row, because the deployed build does not emit one
(`c5edeca6` is what fixes that).


## INCIDENT 09:58 — customer queries failing, and the revert I should not have made

**Symptom:** monoscope-dev raising `SqlError XX000` — "Not enough memory to
continue external sort", `ExternalSorterMerge` consuming 5.5 GB — from 09:58.

**My first action was wrong and the user stopped me.** I reverted the membuffer
masking fix (`b9fd6f28`) on a correlation. Two things I had not checked:

1. **Timing.** The customer-facing pgwire failures start at **09:58:49 — over two
   hours after that build deployed (~07:50)**. A change that widened Delta scans
   would have bitten immediately, not after two quiet hours.
2. **Population.** My "20 errors in the 08:00 hour, 29 in 09:00" counts were
   `maintenance-worker` lines ("Light optimize staging failed") — a long-standing
   condition on a different code path. Of 57 "resources exhausted" lines in the
   window, only **4** are the query-side `ExternalSorterMerge` errors, all in
   09:58–10:01.

Reapplied as `c5fe29e3`. **The lesson is the one this document keeps re-learning:
a correlation across a deploy boundary is not a cause, and I did not even confirm
I was counting the right errors.**

**What it actually is.** The failures are ONE query, retried every ~15 s by
monoscope-dev on project `6297304f` — the log-explorer listing:

```sql
select jsonb_build_array(id, to_char(timestamp ...), ..., to_jsonb(summary), ...)
from otel_logs_and_spans
where project_id = ? and timestamp between ? and ? and (true)
order by timestamp desc limit ?
```

with **`scan.has_limit=false scan.limit=0`** in the same span. The LIMIT never
reaches the scan, so this is a blocking sort over the whole window instead of a
streaming top-N — the 431 GB-for-251-rows cliff, and exactly what
`one_unsorted_file_does_not_cost_the_majority_its_ordering` exists to guard.

**Most likely trigger:** the project's isolated non-conforming leg grew past
`timefusion_read_sort_unordered_leg_max_mb` (**64 MB compressed, ~0.8 GB
decoded**), so `repair_isolated_scan_ordering` began declining. That fits a sharp
onset at one minute rather than gradual decay.

**THE REAL DEFECT IS THAT THIS IS INVISIBLE.** Nothing counted the decline, so a
query silently dropping from streaming top-N to a full sort presents as "queries
that used to work fine no longer do". Fixed in `7a9dea7c`:
`scan.ordering_repair_applied` / `_declined` / `_no_claim`, with the over-budget
leg bytes on the declining path — which is precisely the measured distribution
the budget's own doc demands before anyone raises it.

**Two candidate remedies, neither taken without the measurement:** make the
project's files conforming (a Repair/recompress pass, so the leg disappears), or
raise the budget — noting the doc's warning that the ceiling is paid concurrently
on EVERY Delta-reading query.
||||||| 0c667f8f

## The ordering-cliff hypothesis is REFUTED — by the counters shipped to test it

`a0798e0b` deployed. After ~14 minutes and **6,020 queries**:

```
scan.ordering_repair_applied   0
scan.ordering_repair_declined  0
scan.ordering_repair_no_claim  0
queries_total               6020    uptime_seconds  838
```

and the incident is **still firing** — 30 `ExternalSorterMerge` errors in the last
12 minutes on the new build.

**So `repair_isolated_scan_ordering` is not on the path of any failing query, and
the 64 MB budget is not what broke project `6297304f`.** Zero on all three means
the fork's isolation shape never appears: either every file conforms (nothing to
repair) or the union has a non-scan child and is skipped by design. Either way the
ordering claim is not what these queries are losing.

**This is the instrumentation paying for itself in 15 minutes.** I had written
"most likely trigger: the isolated leg grew past
`timefusion_read_sort_unordered_leg_max_mb`" as the leading hypothesis. It is
wrong, and the counter that says so cost ~40 lines. Without it the next step
would have been raising a memory budget on a memory-tight instance to fix a
problem it has nothing to do with.

**The live lead, unchanged and now the only one:** the failing span carries
**`scan.has_limit=false scan.limit=0`**. The `LIMIT` never reaches the scan, so
`ORDER BY timestamp DESC LIMIT n` materialises the whole window. With the
ordering repair excluded, the candidates are (a) `DedupExec` sitting in its
unbounded `full-set` mode for a different reason — dedup keys not leading the
sort — which blocks limit pushdown by construction, or (b) the projection
(`jsonb_build_array(...)`, `to_jsonb(summary)`) defeating the TopK.

**Next step is local, not prod:** reproduce this exact query shape against a
seeded local table and `EXPLAIN` it. `dedup_keys_lead_the_sort` in `compact.rs`
is the specific predicate to check for `otel_logs_and_spans`.

## Narrowing the 09:58 incident: three causes eliminated, one candidate left

Working the incident locally rather than on prod. Eliminated, cheapest first:

1. **The Delta-side ordering repair.** `scan.ordering_repair_declined` = 0 across
   6,020 prod queries while the incident fired. Not on the path.
2. **`dedup_keys_lead_the_sort`.** Holds for `otel_logs_and_spans`: `dedup_keys`
   are `[timestamp, resource___service___name, id]` and `sorting_columns` leads
   with exactly those three. Dedup *can* run bounded — a schema fact, no run needed.
3. **The projection.** New e2e test `the_monoscope_log_explorer_listing_streams`
   runs monoscope's actual listing — `jsonb_build_array` over a dozen columns,
   `to_jsonb(summary)`, the `extract(epoch ...)` cast and the `coalesce(... or ...)`
   — against the same mem∪delta fixture. It **passes**:
   `SortPreservingMergeExec`, `DedupExec mode=bounded[timestamp]`, `fetch=3`
   propagating into the scan. The projection does not defeat the top-N.

**What is left: the MEMORY leg.** The failing prod span carries
`scan.uses_mem_buffer=true`. A union advertises an ordering only when EVERY child
does, so if the in-memory leg cannot declare the table's `sorting_columns`, the
union loses it, `DedupExec` drops to unbounded `full-set`, and the listing
materialises the window.

**And this was the least observable path in the whole read stack.**
`declare_ordering` fell back on failure with a `debug!` line — which prod does not
emit — under a comment saying an undeclared source is "merely slower". It is not:
it costs the query its streaming merge. Instrumented in `b4a269a9`:
`scan.mem_ordering_declared` / `_unsorted` / `_rejected`, the last also a `warn!`.

`_unsorted` (the caller could not claim its partitions were ordered) and
`_rejected` (it claimed and `try_with_sort_information` refused — the 2026-09-02
projected-index bug's shape) are different defects, so they are counted apart.

**Next:** deploy and read those three. If `_unsorted` dominates, the fix is to
sort the mem partitions before exposing them; if `_rejected` does, it is an index
or schema mismatch in the claim.

## The candidate: one schema-diverse bucket retracts ordering for the whole query

Reading the mem leg's sortedness decision
(`query_partitioned_with_text_match`) gives a mechanism sharp enough to name:

```rust
match partitions.iter().map(|p| sort_partition(s, p.clone())).collect::<Option<Vec<_>>>() {
    Some(sorted) => (sorted, true),
    None => (partitions, false),   // <- ALL-OR-NOTHING
}
```

and `sort_partition` refuses a partition whose batches do not share one schema:

```rust
// A schema-diverse partition is left alone rather than merged: the merge
// is the expensive, failure-prone half of `sort_batches_by_schema` and this
// path runs per query. Undeclared ordering is always safe.
if batches.iter().any(|b| b.schema() != arrow_schema) { return None; }
```

"Undeclared ordering is always safe" is true for **correctness** and false for
**cost**, and the comment does not say which it means. The consequence is out of
all proportion to the cause:

**ONE unsortable partition → the whole leg is `sorted = false` → the mem source
declares no ordering → the union advertises none (a union is ordered only if
EVERY child is) → `DedupExec` drops to unbounded `full-set` → `ORDER BY ts DESC
LIMIT n` materialises the entire window.**

And `insert_batch` **deliberately accepts nullable field additions**. So a single
new optional field appearing in one project's ingest stream is enough to make one
bucket schema-diverse and degrade *every* listing query for that project from a
streaming top-N into a blocking sort. **That is exactly the shape of a failure
that begins at one minute with no deploy behind it** — which is what 09:58 was.

**Not yet confirmed** — it is a mechanism plus a matching signature, which is
precisely the standard of evidence that produced the bad revert earlier today. So
it is instrumented, not acted on (`4187ecde`):

```
scan.mem_sort_retracted                 the claim was dropped
scan.mem_sort_retracted_schema_diverse  ...because a partition held mixed schemas
scan.mem_ordering_declared / _unsorted / _rejected
```

**If `mem_sort_retracted_schema_diverse` tracks the failures, the fix is to make
diverse partitions sortable** (unify to the table schema before sorting) rather
than to retract globally — and the cost the original comment was avoiding should
be measured against a blocking sort of the whole window, which is what it is
actually being traded for.

## Every ordering hypothesis is refuted. It is memory-pool contention.

`4187ecde` live, 685 s uptime, 6,073 queries, 200 mem∪delta scans:

```
mem_ordering_declared              506      ordering_repair_applied    0
mem_ordering_unsorted                0      ordering_repair_declined   0
mem_ordering_rejected                0      ordering_repair_no_claim   0
mem_sort_retracted                   0
mem_sort_retracted_schema_diverse    0
```

**The ordering claim is intact on every scan — 506 declarations, zero
retractions.** So the schema-diverse retraction, though real and now pinned by a
unit test, is NOT what is happening in prod. The plans are streaming.

That closes the whole ordering family. Four hypotheses, four refutations, each by
measurement rather than argument:

| hypothesis | refuted by |
|---|---|
| my deploy caused it | failures start 2 h after it |
| Delta-side repair / the 64 MB budget | `ordering_repair_declined` = 0 / 6,073 |
| dedup keys not leading the sort | schema fact — they do |
| the `jsonb_build_array` projection | local test streams: SPM + bounded dedup + `fetch=3` |
| mem leg loses its ordering claim | 506 declared, 0 retracted |

**What is left is the one thing that was in front of me the whole time:** the
errors name *several concurrent* `ExternalSorter`s as the top consumers, and they
appear in **both** the query path and `maintenance-worker` ("Light optimize
staging failed: Not enough memory to continue external sort"). Maintenance
rewrites and customer queries draw on the same memory pool. **The plans are fine;
the pool is oversubscribed.**

**And that is not a separate problem from this document — it is the same one.**
Maintenance is saturated at 1x (completions flat under 10x load, dedup holding
9.76 of 10 heavy permits continuously). A saturated maintenance lane running
continuous large sorts is now taking enough of the shared pool that customer
`ORDER BY ... LIMIT` queries cannot reserve. **The capacity problem has stopped
being a backlog statistic and started failing customer queries.**

**That reframes the priority.** Everything in this document about unit count and
bin width is about making maintenance cheaper. This incident says the deadline
for that is not "before the 100x customer" but now.

**Next measurements, both cheap and neither taken yet:**
1. Attribute the pool: are the top consumers at failure time maintenance sorts or
   query sorts? The error text lists consumers but not their lane — labelling
   reservations by lane is the fix.
2. `datafusion.runtime.memory_limit` vs the actual concurrent demand: how much of
   the pool does maintenance hold at steady state?

### CORRECTION: the pools are NOT shared, and I published that they were

The section above concludes "maintenance rewrites and customer queries draw on
the same memory pool". **That is wrong.** Read the wiring rather than inferring
it from two error classes appearing together:

- queries reserve from `shared_runtime_env()` — `query_pool_bytes()`
- maintenance reserves from `light_optimize_runtime_env()` and
  `repair_runtime_env()`, tiled out of `maintenance_pool_bytes()`, and a test
  asserts they are not the same env (*"sharing one RuntimeEnv is sharing one
  pool"*)

Measured from `timefusion_stats`:

```
budget.query_pool_mb       16384      memory.query_pool_pct        0
budget.maintenance_pool_mb 16964      memory.maintenance_pool_pct  0
                                      memory.coordinator_pool_pct  6
```

**So maintenance cannot starve queries, and the two exhaustion classes are
independent.** I inferred a shared pool from the fact that both lanes were
erroring — which is the same mistake as this morning's revert, made again six
hours later: *co-occurrence is not a mechanism.*

**What the numbers do say:** the query pool is **16 GB**, and one failing sort was
reported consuming **5.5 GB**. Three concurrent sorts of that size exhaust it.
The failure is concurrency against a per-query sort that is far too large — not
contention with maintenance.

**And one more attribution I should flag as weak:** I identified the failing
statement as the log-explorer listing because its `INFO` span sat immediately
before the `ERROR` line. In an interleaved multi-threaded log that is adjacency,
not causation, and the two lines are on different threads. The listing may not be
the query producing the 5.5 GB sort at all.

**So the honest state of this incident:** the entire ordering-cliff family is
excluded by measurement; the query pool is 16 GB; some query builds a 5.5 GB
sort; concurrency of those exhausts the pool. **Which query, and why its sort is
5.5 GB when the plan shape streams, is not yet established.** The next step is
to label sort reservations with the statement that owns them, so the log names
the query instead of leaving it to adjacency.

## RESOLVED: the query pool was Greedy, and a spillable sort starved the merge that follows it

**Fixed by a concurrent session's work** (`4643af52`, `4ffa89f5`, `91a4a9bd`),
which I nearly lost — the shared checkout had moved me onto their branch and my
commits were landing there. Recovered by cherry-picking both sets onto master.

**The mechanism, and it is a better diagnosis than any of mine.** The query pool
defaulted to `Greedy`: one global cap, first-come first-served. A **spillable**
`ExternalSorter` therefore grows instead of spilling until the pool is gone — and
the merge halves that follow it (`ExternalSorterMerge`, `SortPreservingMerge`,
`DedupExec[keep-greatest]`) **cannot spill**. So the sorter eats the pool and the
merge fails for a fraction of it. Prod 2026-09-02: one 16-partition sort holding
5.9 GB and 7.3 GB of a 16 GB pool while `ExternalSorterMerge[3]` could not get
331 MB. Under `FairSpill` each sorter is capped at
`(pool − unspillable) / num_spill` and spills instead.

The historical reason for `Greedy` was retired properly rather than assumed: ~30
concurrent INSERTs once got ~76 MB slots and bounced, but the write path took its
own FairSpill pool in August, and `tests/suite/query_pool_insert_test.rs`
**measures** that INSERTs reserve nothing from the query pool.

**Measured, matched 15-minute windows, traffic HIGHER after:**

| build | pool | `ExternalSorterMerge` errors / 15 min | throughput |
|---|---|---:|---|
| `9a0c75a` | Greedy | **26** | — |
| `91a4a9b` | FairSpill | **0** | 12,339 queries in 1,080 s (11.4/s) |

The only residual pgwire errors in the window are 7 × `Prepared statement 'all'
does not exist` — a client-side protocol issue, unrelated.

Rollback if needed: `TIMEFUSION_MEMORY_POOL=greedy`, no redeploy.

**What this says about my own work on this incident.** Four hypotheses refuted by
counters I shipped, two conclusions published and withdrawn, and the actual cause
found by someone else reading the pool configuration. The instrumentation was
worth it — it eliminated the entire ordering family and stopped me raising a
memory budget for no reason — but the lesson stands: **I kept reasoning forward
from a symptom instead of reading the configuration the symptom named.** The
error message said "Additional allocation failed for ExternalSorterMerge" and
named its own consumers; the pool policy was one config field away.

## Why there is still no dedup phase measurement: I kept restarting prod

The dedup timers (`c5edeca6`) are deployed and on the executed path — per shard,
after the write loop in `stage_dedup_chunk`. In 30 minutes of logs: **26 Pack,
1 Repair, zero Dedup.**

The reason is the deploy cadence, and it is mine:

```
Running  19 min ago   91a4a9b
Shutdown 19 min ago   9a0c75a
Shutdown 41 min ago   4187ecd
Shutdown  2 h  ago    a0798e0
Shutdown  3 h  ago    c5fe29e
```

**A dedup unit needs ~21 worker-minutes and dies on process exit
([[tf_units_die_to_restarts_2026-08-23]]). The last two restarts were 22 minutes
apart.** A unit essentially cannot finish inside that window, so the lane that
consumes ~98% of the heavy pool produces no completed unit to time.

This is the pathology `tf_deploy_cadence_starves_dedup_2026-08-18` already
records, reproduced by me while chasing an incident. Five deploys today: three
were the incident (two of them my own bad revert and its reapply), two were
instrumentation.

**So the action is to stop deploying.** No further pushes until the dedup
decomposition is read. The measurement needs ≥45 quiet minutes, and it is the
single most decision-relevant unknown left: whether dedup's wall clock is
read-bound like Pack/Repair (61% read+sort), because that is what decides whether
widening `BIN_MICROS` — which removes reads — is worth a staging soak.

**And note the second-order cost:** every restart also discards the in-flight
unit's work, so a 20-minute deploy cadence does not merely delay dedup, it
prevents it. That belongs in the capacity story: the backlog measurements taken
on a restart-churned process understate what a quiet one would do.

## Prior art: what others do about exactly this, and what it says about our design

Two searches, both aimed at the open problem — maintenance saturated at 1x, and
read amplification from file-granular replacement.

### 1. Deletion vectors are the industry answer, and they fit dedup exactly

Delta's merge-on-read: DELETE/UPDATE/MERGE mark rows in a bitmap and leave the
data files untouched, instead of rewriting them (copy-on-write). The canonical
example is a 1-billion-row file where deleting one row rewrites 999,999,999 rows.
**Our dedup is a delete** — keep-greatest removes older versions of a key — so a
deletion vector turns "rewrite the ~1.4 GB of files overlapping this bin" into
"write a bitmap". That removes the cost driver of ~98% of the heavy pool, not
by scheduling it better but by not doing it.

The trade is explicit in the literature: *"whatever time the DELETE saves by
avoiding eagerly rewriting files, the reader and compaction commands pay for
later."* **For us that trade is unusually favourable, because our read path
ALREADY pays merge-on-read dedup** — applying a bitmap is strictly cheaper than
the `DedupExec` keep-greatest we run today.

**Still blocked, re-verified today:** delta-rs supports READING deletion-vector
tables but not writing them; `delta-io/delta-rs#4079` is open (filed 2026-01-14),
and the roadmap points at delta-kernel-rs adoption as the path. We carry a fork,
so this is possible rather than impossible — but it is a protocol-level change
affecting every reader, and it is the largest single lever identified in this
document.

### 2. ClickHouse bounds merge cost two ways we do not

`ReplacingMergeTree` is literally our keep-greatest dedup, so its merge policy is
the closest prior art there is. Two rules stand out:

- **The max merge size ADAPTS to pool pressure.** `max_bytes_to_merge_at_max_space_in_pool`
  defaults to 150 GB, and *"when the background pool is nearly full, ClickHouse
  automatically reduces the maximum allowed merge size to keep slots available for
  smaller, more urgent merges rather than letting a few large merges monopolize
  the entire pool."* **We have the exact failure that rule prevents**: dedup holds
  9.76 of 10 heavy permits continuously, and today's phase timers show **2 Repair
  units consuming 29% of all measured worker time** (38.6 of 134 worker-minutes)
  against 203 Pack units. Our budgets are static; theirs are a function of
  pressure.
- **Merge selection prefers parts of SIMILAR SIZE**, explicitly "to avoid
  repeatedly rewriting large data". Ours sorts by size but opens with
  `if add.size >= target { continue }` — over-target files are not "deprioritised",
  they are *invisible*, which is why 99% of the frozen mass is never a candidate
  ([[tf_overlap_components_are_the_frozen_mass_2026-09-04]]).

**Two concrete, borrowed designs, neither speculative:**
1. Scale the per-unit byte/row budget DOWN as heavy-pool occupancy rises, so a
   few giant units cannot monopolise the pool. This is cheap and local, and it
   directly targets the 29%-from-2-units observation.
2. Replace the `>= target` skip with a similar-size preference, so over-target
   files remain candidates at lower priority instead of being permanently frozen.

Sources: [Delta Lake deletion vectors](https://delta.io/blog/2023-07-05-deletion-vectors/) ·
[delta-rs #4079](https://github.com/delta-io/delta-rs/issues/4079) ·
[ClickHouse part merges](https://clickhouse.com/docs/merges) ·
[MergeTree engine](https://clickhouse.com/docs/engines/table-engines/mergetree-family/mergetree)

## THE DEDUP DECOMPOSITION — measured at last, and it supports the bin-width lever

Prod quiet for 35 minutes (no deploys), 49 units:

| pass | n | plan | **read+sort** | write | median | max | worker-min | median rows staged |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Pack | 44 | 0.3% | 51.0% | 48.6% | 16.9 s | 367 s | 37.3 | 1,480,926 |
| **Dedup** | **5** | 0.0% | **60.0%** | 40.0% | 11.9 s | 502 s | 9.1 | **97,306** |

**The answer to the question this instrumentation was built for: dedup's wall
clock is 60% read-and-sort.** Widening `BIN_MICROS` removes *reads* — the same
file read once per bin it straddles — so it attacks the dominant phase. Had
dedup been write-bound, the 5.1x read reduction would have bought far less. **The
lever is aimed at the right thing.**

**The second number is the read amplification, visible directly:** dedup stages a
median of **97,306 rows** against Pack's **1,480,926** — 15x fewer — while its
worst unit runs **502 s** against Pack's 367 s. Dedup does far more work per row
it writes, which is what "read whole files to remove a few duplicate rows" looks
like on a clock.

**A correction to my own earlier framing.** On THIS process dedup is 9.1 of 46.4
measured worker-minutes — about **20%**, not the ~98% quoted throughout this
document. That 98% came from `work.Dedup.worker_secs / uptime / permits` on a
different process hours earlier
([[tf_dedup_is_98pct_of_the_pool_2026-09-04]]). Both can be true — occupancy
varies with what is queued, and 35 minutes with 5 units is a small sample — but
**the honest statement is that dedup's SHARE is variable and was measured at ~20%
here, while its per-unit SHAPE (60% read) is what the lever depends on and is
consistent across all three lanes** (Pack 51%, Repair 60%, Dedup 60%).

Also worth noting: dedup's worst unit at 502 s sits comfortably inside the 900 s
deadline, so on this evidence deadline pressure is not currently what bounds it.

**This is the last blocker on the bin-width decision.** The remaining unknown is
the soak — 6x fewer, larger units against the claim/lease/deadline machinery —
which needs staging and real object-store latency, not MinIO and not the IO-free
sim.

## The full quiet hour: the SHAPE holds, the SHARE was badly wrong

96 units over 60 uninterrupted minutes (vs the 49-unit sample above):

| pass | n | read+sort | write | median | p90 | max | worker-min | **share** | median rows |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Pack | 87 | 46.0% | 53.7% | 16.7 s | 127.6 s | 367 s | 67.1 | **86.6%** | 974,247 |
| Dedup | 9 | **60.8%** | 39.0% | 10.2 s | 502 s | 502 s | 10.4 | **13.4%** | 74,587 |

**What HOLDS, and is now solid:** dedup is **read-bound at 60.8%** (60.0% at
n=5). The shape is stable across samples and lanes, and it is the fact the
bin-width lever depends on — widening removes reads.

**What COLLAPSES: the share.** I quoted "dedup is ~98% of the heavy pool" all
night, then ~20% at n=5. At n=9 over a quiet hour it is **13.4%**, and **Pack is
86.6%**. Each larger, quieter sample moved it further down. The 98% came from
`work.Dedup.worker_secs / uptime / permits` on a churned process — a ratio of
counters whose denominator I never validated, which is the exact failure mode
this document catalogues twice already.

**And dedup's 10.4 worker-minutes is essentially ONE unit:** median 10.2 s, but
p90 = max = 502 s. Eight trivial units and one that is 8.4 minutes — 80% of the
lane's cost. The heavy tail is not a property of dedup's *rate*, it is one unit.

**This changes the priority ordering I have been asserting.** On this evidence
**Pack — hot compaction — is the dominant consumer of maintenance worker time,
not dedup.** Pack is also the more balanced lane (46% read / 54% write), so the
read-side lever helps it less. Two consequences:

1. **Bin widening is still correctly aimed** (dedup is read-bound) but its
   *total* impact is bounded by dedup's share, which is far smaller than claimed.
   A 5.1x read reduction on 13.4% of worker time is not a 5x system win.
2. **The ClickHouse-style adaptive budget looks more valuable than before**: the
   problem it targets — a few large units monopolising the pool — is exactly
   dedup's one 502 s unit and the earlier 2 Repair units at 29% of worker time.

**Standing caveat, applied to myself:** this is one hour on one process. It is a
better sample than anything quoted earlier tonight, and it may still not be
steady state. The stable quantity across every sample is the per-unit read/write
*shape*; every *share* number I have produced has moved.

## 95 quiet minutes, 176 units: the bin-width lever is worth ~4%, not 5x

| pass | n | read+sort | write | median | max | worker-min | **share** |
|---|---:|---:|---:|---:|---:|---:|---:|
| Pack | 164 | 40.4% | **59.3%** | 18.0 s | 367 s | 124.1 | **85.3%** |
| Dedup | 11 | **62.1%** | 37.8% | 8.5 s | 502 s | 11.0 | **7.5%** |
| Repair | 1 | 48.7% | 51.3% | 625 s | 625 s | 10.4 | **7.2%** |

**Three things, and two of them overturn what I have been recommending.**

**1. Dedup's SHAPE is rock solid: read+sort 60.0% → 60.8% → 62.1%** across three
growing samples. That part of the story survived everything.

**2. Dedup's SHARE has collapsed monotonically: 98% → 20% → 13.4% → 7.5%.**
Every larger, quieter sample halved it. **Pack is 85.3%.**

**3. Pack is WRITE-bound, and increasingly so: read+sort 51% → 46% → 40.4%.** The
dominant lane spends 59.3% of its time writing. **A read-side lever does not
help the lane that consumes six-sevenths of maintenance.**

### The arithmetic I should have done before championing bin widening

Widening cuts object-store READS ~5.1x. Its effect on maintenance worker time is
bounded by:

```
dedup share 7.5%  x  read+sort 62.1%  x  (1 − 1/5.1)  ≈  3.7%
```

**So the ceiling is ~4% of maintenance worker time — not 5x anything.** And that
is a CEILING, because `upstream_secs` is time blocked in `stream.next()`, which
conflates **scan and sort**. Widening removes reads; it does not remove the sort.
The addressable fraction is therefore *smaller* than 62.1%, by an amount this
instrumentation cannot separate.

**The 5.1x figure was never wrong — it is bytes read, which is a real IO and cost
saving, and it reduces cache pressure. I repeatedly let it stand in for a
throughput win it does not deliver.**

### What the data actually points at

- **One Repair unit (625 s) is 7.2% of all worker time** — on its own, nearly the
  entire dedup lane. Pack median is 18 s. This is the heavy tail, now measured
  three times (2 Repair units = 29%; one dedup unit = 80% of its lane; one Repair
  unit = 7.2% of everything). **`c3412de7`'s pressure scaling targets exactly
  this**, and it ships in shadow mode so the taper can be chosen from
  `pressure_scale_bytes_withheld`.
- **Pack's write cost is the largest single line item in maintenance** — 59.3% of
  85.3% ≈ **51% of all maintenance worker time is Pack writing**. Nothing in this
  document addresses it. That is where the next investigation belongs.

**Standing caveat:** one process, 95 minutes. But the direction has been
consistent across four samples, and the two quantities that never moved are the
per-unit read/write shapes and the presence of a dominating heavy tail.

## THE COST DRIVER: maintenance writes ~36 rows for every row ingested

Matched windows — 95 minutes of phase logs against a process at 5,817 s uptime:

| | rows |
|---|---:|
| **Ingested** (`rows_ingested_total`) | **7,504,759** |
| Pack staged (written) | **269,432,825** |
| Dedup staged | 3,931,505 |
| Repair staged | 726,064 |
| **Maintenance total written** | **274,090,394** |

**≈ 36.5 rows written per row ingested.** Pack alone is **35.9x**.

`rows_staged` is a real count from the writer — not `progress_rows`, which is
`plan_metric_sum` summing `output_rows` over every operator and has misled this
document once already.

**This is the cost driver, and it explains every share number above.** Pack is
85.3% of maintenance worker time and 59.3% write-bound because it is rewriting
the same data dozens of times. Dedup's read amplification is real but it is
7.5% of the bill; **write amplification in packing is the other order of
magnitude.**

**It also explains the 10x result.** Completions stay flat under load because
each ingested row drags ~36 rewrites behind it. At 10x ingest that is 10x the
rewrite volume against a fixed pool — which is exactly the linear backlog growth
the simulation showed.

**Prior art names the fix.** ClickHouse's merge selection "prefer[s] to combine
parts of similar size to avoid repeatedly rewriting large data" — the rule we do
not have. Ours sorts candidates by size and skips `>= target`, which stops the
largest files being rewritten but does nothing to stop near-target files being
merged, re-merged and merged again on the way up.

**Caveats, stated rather than buried:**
- This includes draining historical backlog, not only steady-state amplification
  of newly ingested rows. A system working through a backlog legitimately writes
  more than it ingests. The ratio is an upper bound on steady-state.
- One process, 97 minutes.
- 36x is not absurd for tiered compaction — it is what log-structured merging
  costs without a similar-size rule — but it is the largest single number in this
  document and nothing in tonight's work reduces it.

**Next investigation, and it now clearly outranks the bin-width soak:** measure
Pack's amplification per merge level. If near-target files are being re-merged,
the ClickHouse similar-size preference is a bounded, well-understood change with
a much larger ceiling than the ~4% bin widening offers.

## Where the 36x goes: the packer spends 82.5% of its writes on its worst merges

Pack units from the same quiet 95 minutes, grouped by how many files each merged:

| files merged | units | rows written | share of Pack writes | median rows/unit |
|---:|---:|---:|---:|---:|
| **2** | 71 | **222,169,019** | **82.5%** | **3,770,398** |
| 3–4 | 27 | 11,798,760 | 4.4% | 32,910 |
| 5–8 | 29 | 23,785,836 | 8.8% | 210,480 |
| 9+ | 36 | 11,679,205 | 4.3% | 102,481 |

Priced as **rows rewritten per file actually eliminated** (`files − outputs`):

| merge shape | rows written | files eliminated | **rows per file eliminated** |
|---|---:|---:|---:|
| 2-file | 222,169,019 | 71 | **3,129,141** |
| 9+-file | 11,679,205 | 431 | **27,098** |
| all Pack | 269,432,825 | 720 | 374,212 |

**A 2-file merge costs 115x more per file removed than a 9+-file merge.** And the
packer spends **82.5% of its write budget** on the shape that delivers **9.9% of
the file reduction** (71 of 720), while the shape delivering **60%** of it (431
files) costs **4.3%**.

**That is the 36x, localised.** It is not compaction being inherently expensive;
it is compaction repeatedly merging *pairs of already-large files* — median
3.77M rows over 2 files, so ~1.9M rows each — to remove one file.

### Why the existing guard does not stop it

`select_tail_bin` checks `if fresh.len() < min_files && !repairs_present` —
**`min_files` (default 5) is tested against the CANDIDATE POOL, not the bin.** A
pool of 5+ candidates can still emit a 2-file bin once bytes fill it. The rule
prevents packing a *sparse partition*; it does nothing about an *expensive bin*.

**Checked, not assumed: my `pair_exemption` from earlier tonight is NOT
implicated.** That lives in `select_coordinator_compaction_candidates`; the tail
packer here is a separate byte-based loop with no `MAX_BIN_ROWS` term. The
resemblance between the observed 3.77M median and `2 x MAX_BIN_ROWS = 4M` is a
coincidence, and I checked the loop before writing this rather than after.

### The fix, and it is the ClickHouse rule stated precisely

Score a candidate bin by **files eliminated per byte written** and refuse the bin
if that ratio is worse than a floor. Equivalently: require the bin to contain at
least `min_files` files *unless the files are small enough that the merge is
cheap anyway*. Today the packer optimises for "reach the target size"; it should
optimise for "remove the most files per byte rewritten".

Expected ceiling, from this window: moving the 2-file volume to the value of even
the 5–8 shape would cut Pack writes by most of that 82.5% — against a bin-width
lever whose ceiling is ~3.7%. **This is the change worth soaking.**

## Choosing the floor from the real distribution (not from argument)

Replaying the 163 real Pack units from the quiet window against candidate floors —
what each would refuse, what it saves, and what benefit it gives up:

| floor (rows per file eliminated) | units refused | **% writes saved** | **% file-elimination lost** |
|---:|---:|---:|---:|
| 25,000 | 104 | 98.8% | 41.4% |
| 100,000 | 90 | 97.4% | 28.8% |
| 250,000 | 82 | 94.5% | 19.7% |
| 500,000 | 69 | 86.1% | 11.2% |
| **1,000,000** | **63** | **82.4%** | **8.8%** |
| 2,000,000 | 62 | 81.8% | 8.6% |

Baseline: 269,432,820 rows written to eliminate 720 files — **374,212 rows per
file**.

**The knee is at ~1M rows per file eliminated: 82.4% of write volume saved for
8.8% of the file reduction — roughly a 9:1 trade.** Above 1M the curve is flat
(2M buys 0.6% more saving), and below 500K the benefit lost climbs fast for
little extra saving.

**Converting to the units the guard actually uses.** The guard is priced in
BYTES; this table is in rows. At `otel_logs_and_spans`' ~104 B/row, the 1M-row
knee is **≈100 MiB per file eliminated**, i.e.
`timefusion_pack_max_bytes_per_file_eliminated = 104857600`. Sanity check both
ends: two 128 MiB files write 256 MiB to remove one file → 256 MiB/file →
**refused**; ten 20 MiB files write 200 MiB to remove nine → 22 MiB/file →
**admitted**. That is the intended separation.

**The risk, stated plainly, because it is the one that could make this wrong.**
"Files lost" is deferral only if those pairs later join a cheaper bin. If they
never do, the partition keeps two near-target files instead of one — permanently.
That is the frozen-mass shape this document already describes
([[tf_overlap_components_are_the_frozen_mass_2026-09-04]]), and it would trade
maintenance cost for query file-count. Two things bound the risk: the refused
files are already *near target*, so merging them buys little for the reader
(2 files vs 1 in that partition), and the floor is a config value with an
immediate rollback.

**Recommendation:** set `timefusion_pack_max_bytes_per_file_eliminated =
104857600` (100 MiB) in **staging** first and watch live file counts per
partition alongside `pack_value_refused_bytes`. If file counts hold, it is worth
~82% of packing write volume — which, since Pack is 85.3% of maintenance worker
time and 59.3% write-bound, is the largest single lever measured tonight, by a
wide margin over the ~3.7% bin-width ceiling.

**Not enabled by me.** The evidence is strong and the analysis is on real prod
units, but this changes the behaviour of the lane holding 85% of maintenance, the
failure mode is a slow one (file counts drifting up over days, not an error), and
I have been wrong repeatedly today on less. It wants a soak, not a flag flip at
04:00.

## The floor is robust (the population is bimodal), and it is worth ~1.7x — not 10x

Extending the floor sweep upward makes the shape clear:

| floor (rows/file eliminated) | ≈ MiB/file | % writes saved | % files lost | **maintenance worker-time saved** |
|---:|---:|---:|---:|---:|
| 1,000,000 | 99 | 82.4% | 8.8% | **41.7%** |
| 2,000,000 | 198 | 81.8% | 8.6% | **41.4%** |
| 4,000,000 | 397 | **0.0%** | 0.0% | 0.0% |
| 8,000,000+ | 793+ | 0.0% | 0.0% | 0.0% |

**It is a cliff, not a curve.** Everything expensive sits at ~3.77M rows per file
eliminated (the 2-file merges); everything else is under 1M. So **any floor
between ~1M and ~3M rows/file — roughly 100–300 MiB — gives the identical
result**, and a floor mis-set by 2x still works. That is a wide safe band, and it
substantially de-risks the choice: this is not a knob needing careful tuning, it
is a separator between two populations that barely overlap.

### What it is actually worth, in capacity terms

```
maintenance worker-time saved = Pack share 85.3%  x  write fraction 59.3%  x  writes saved 82.4%
                              ≈ 41.7%
```

**Removing 41.7% of maintenance worker time is ~1.71x headroom** (`1 / (1 −
0.417)`) at the same pool size.

**So: this is the largest single lever measured tonight by a wide margin, and it
is worth about 1.7x — not 10x.** The 10x requirement needs roughly a 90%
reduction in work per ingested row; this delivers ~42%. Stated plainly so nobody
reads the 82% as a capacity multiplier: **82% is of packing's WRITE VOLUME, and
packing writing is ~51% of maintenance.**

### What would have to follow it for 10x

The 36x write amplification is the frame. After this change it becomes roughly
21x (the refused 82% of Pack writes removed). Getting to "keeps up at 10x" means
attacking what remains:

1. **Deletion vectors for dedup** — removes the dedup rewrite entirely rather
   than making it cheaper. Blocked on delta-rs `#4079`; we carry a fork.
2. **Why 21x remains** — the surviving Pack volume is genuine multi-file merging,
   which is the irreducible cost of tiered compaction *unless* the tiering itself
   changes (levelled, key-range partitioned — the RocksDB/IOx shape this document
   already identified as absent).
3. **The ~4% bin-width lever** is now clearly last, not first.

**This is the honest capacity ladder, and it is the thing I should have built at
the start of the night instead of at the end.**

## We are ~21x above the theoretical floor — the amplification is policy, not physics

Tiered compaction has an unavoidable cost: each byte is rewritten once per level
on its way to the target, so the floor is `log_fanin(target / arrival)`.
Measured from the same 163 Pack units:

```
input file size (rows):  p10 260   p50 176,743   p90 1,907,723
fan-in (files/merge):    p50 3     mean 5.4
target bin:              ~2,581,110 rows  (256 MiB at ~104 B/row)
```

| arrival rows | levels at k=2 | k=5 | k=10 | k=20 |
|---:|---:|---:|---:|---:|
| 10,000 | 8.0 | 3.5 | 2.4 | 1.9 |
| 50,000 | 5.7 | 2.5 | 1.7 | 1.3 |
| **176,743 (our p50)** | 3.9 | **1.7** | 1.2 | 0.9 |
| 500,000 | 2.4 | 1.0 | 0.7 | 0.5 |

**At our own median file size and our own observed mean fan-in of 5.4, the floor
is ~1.7 rewrites per byte. We measured 36.5.** That is **~21x above floor** — so
the 36x is *not* what tiered compaction costs. It is what this selection policy
costs.

**The mechanism is visible in the same numbers.** p90 input file is **1,907,723
rows against a 2,581,110-row target** — the packer is repeatedly feeding
near-target files back into merges. A file that is already 74% of target has
almost nothing to gain from another rewrite, and pays full price for it.

### The capacity ladder, quantified

| state | write amplification | vs today |
|---|---:|---:|
| today | **36.5x** | — |
| after the value guard (removes 82.4% of Pack writes) | **~6.4x** | 5.7x less work |
| floor at current fan-in (k≈5) | ~1.7x | — |
| floor at k=10 | ~1.2x | — |

**And this is what answers the 10x question.** Keeping up at 10x ingest requires
work-per-ingested-row to fall by roughly 10x — from 36.5 to ~3.6. **The value
guard alone reaches ~6.4x; raising fan-in toward k=10 on the surviving merges
would reach ~2-3x.** Together they span the requirement. Neither is an
architectural change, and neither is the bin-width lever I spent the night on.

**Caveats, because this is the number everything now rests on:**
- The floor assumes every merge reaches target. Ours frequently do not, which is
  precisely why we are above it — so the gap is real, but "21x of pure waste" is
  the optimistic reading; some of it is the cost of merges that stop early for
  legitimate reasons (seal boundaries, time slices).
- The ~6.4x post-guard figure assumes the refused work is never done. Some of it
  will return later at higher fan-in, adding volume back. It is a lower bound on
  post-guard amplification, not a prediction.
- 104 B/row is from an earlier measurement, used to convert rows to bytes.

## The local benchmark: the floor cuts steady-state amplification 43%

Everything above measured ONE prod window and reasoned about what a floor *would
have* refused. `the_value_floor_lowers_steady_state_write_amplification`
(`0572c56d`) closes that gap: it drives the **real** `select_tail_bin` over 400
rounds, feeds each merge's output back in as the packer would next see it, and
reports bytes written per byte ingested.

```
amplification: floor OFF 6.73x   floor ON 3.85x     (-43%)
```

Deterministic — no clock, no RNG — one partition, arrivals at prod's p50 input
size (~18 MiB / ~176k rows) against the 256 MiB target. The floor is applied via
`bin_exceeds_value_floor`, the **same predicate `select_tail_bin` uses**, so the
benchmark cannot pass while the guard does something different.

**Two independent estimates now agree in direction and rough size:**

| method | result |
|---|---|
| replaying 163 real prod Pack units against the floor | 82.4% of write volume refused, 8.8% of file-elimination lost |
| driving the real packer for 400 rounds | steady-state amplification **6.73x → 3.85x** |

**The sim's 6.73x baseline is far below prod's 36.5x, and that gap is honest
rather than concerning:** one clean partition with uniform arrivals omits time
slices, seal boundaries, repair, and cross-partition re-merging — all of which
prod has and all of which add rewrites. **The relative 43% is the result; the
absolute is not comparable to prod.** If anything it suggests prod's win is
larger, since prod's excess over the floor is what the guard targets.

**Status of the ladder, with what is now proven vs projected:**

| step | evidence |
|---|---|
| 36.5x measured today | **proven** — matched windows, real writer counts |
| floor cuts amplification ~43% | **proven locally** — real packer, 400 rounds |
| floor refuses 82.4% of prod's Pack write volume | **proven on prod data** — 163 real units replayed |
| that reaches ~6.4x in prod | **projected** — arithmetic, not measured |
| fan-in raising reaches ~2-3x | **projected** — from the `log_fanin` floor |
| the two together keep up at 10x | **projected** — requires ~10x reduction; the pair spans it on paper |

**The top three rows are measurements. The bottom three are arithmetic.** Nothing
here yet demonstrates prod keeping up at 10x; what it demonstrates is a lever
with a proven mechanism, a proven local effect, and a quantified prod-data
estimate — which is the state a change should be in before it is enabled.

## THE USER-FACING PROBLEM, measured: the log explorer hits a WALL at ~2.5 days

Timing monoscope's real projection against the affected project, read-only:

| window | result |
|---|---|
| 1h / 6h / 24h | 222 / 278 / 261 ms |
| 36h | 475 ms |
| 48h | 1,178 ms |
| 60h | 2,574 ms |
| **72h (3 days)** | **FAILS after 10.6 s** |
| **7 days** | **FAILS after 8.8 s** |

```
ERROR: Resources exhausted: unordered merge-on-read dedup exceeded its
2048 MiB per-query limit; narrow the time window or compact unsorted files
```

**It is not slowness, it is a wall.** Under ~2.5 days queries are fast; past it
they fail outright. A simple `SELECT id, timestamp` still works at 48h — it is
the PROJECTION WIDTH (`jsonb_build_array` over a dozen columns plus
`to_jsonb(summary)`) that pushes the unordered dedup over 2 GiB.

### I WAS WRONG EARLIER, and this is the correction

I wrote that the ordering-repair hypothesis was "refuted by its own counters"
(0 declines across 6,073 queries) and moved on. **That measurement was taken on
short-window traffic, where the repair never engages.** After running wide
queries just now:

```
ordering_repair_applied  1
ordering_repair_declined 4
```

**The declines are real and they are exactly the wide queries.** My refutation
sampled a query population that could not exhibit the effect — a control-group
error, not a counter error. The original hypothesis (the isolated unsorted leg
exceeding `timefusion_read_sort_unordered_leg_max_mb`, 64 MiB) is back, and now
has direct evidence.

### And it ties the query problem to the maintenance problem

```
pending_repair = 252
```

252 files await footer repair. A Repair unit rewrites **exactly one file**, and
the quiet-hour sample caught **one Repair unit in 95 minutes**. At that rate the
backlog needs **~400 hours — about 17 days** — and until a file is repaired it
keeps forcing the unordered dedup path for every query whose window touches it.

**So "maintenance is not keeping up" and "queries are slow" are the same
problem, and this is the causal link the whole night was missing:** the repair
lane is starved, unsorted files accumulate, and the log explorer's usable window
shrinks toward the present.

### Two remedies, both immediate, neither yet applied

1. **Raise `timefusion_read_sort_unordered_leg_max_mb` (64 MiB).** The repair is
   declining because the leg exceeds it. This is the change I talked myself out
   of earlier on the strength of the bad refutation above. It costs concurrent
   query memory — but the pool is now FairSpill, which is what makes it safer
   than it was this morning.
2. **Give Repair more throughput.** One file per unit against 252 pending is the
   binding constraint. `timefusion_repair_max_file_bytes` and the repair pass's
   budget decide how many files a pass takes.

**This — not bin width, not the packer floor — is what is actually hurting the
customer today, and it is the thing to fix first.**

## The query wall is GONE — but I cannot attribute it to my fix, and it may be transient

Same query, same project, before and after `1ba789e`:

| window | before | after |
|---|---|---|
| 24h | 261 ms | 351 ms |
| 60h | 2,574 ms | **370 ms** |
| **3 days** | **FAILED** (10.6 s) | **213 ms** |
| **7 days** | **FAILED** (8.8 s) | **227 ms** |
| 14 days | (would have failed) | 732 ms |
| 30 days | (would have failed) | 1,520 ms |

**Not cache warming:** a *different*, busier project (`dcad860a`, 721k rows/2h,
never queried during the "before" run) returns 3d in **725 ms** and 7d in
**506 ms** cold.

**BUT THE ATTRIBUTION FAILS, and this is the honest part.**

```
ordering_repair_applied  = 0
ordering_repair_declined = 0
pending_repair           = 252   (unchanged)
uptime_seconds           = 279
```

**The ordering-repair path is not being exercised at all**, so raising
`timefusion_read_sort_unordered_leg_max_mb` 64 → 1024 cannot be what fixed this —
the code it governs never runs. And `pending_repair` is unchanged at 252, so the
repair-throughput change has not drained anything yet either. **Both of my
changes are, so far, inert.**

**What else differs between the two measurements:** the process is **279 seconds
old**. The failing measurements were taken on a process ~95 minutes old. The
2 GiB ceiling is a per-query *unordered keep-greatest* buffer, and a fresh
process has empty caches, empty certifications and no accumulated state
competing for memory.

**So the leading explanation is that the wall is a function of process age, not
of my configuration** — which means **it will come back**. That is a testable
prediction and the next thing to check: re-run this ladder at ~90 minutes uptime.
If 3d and 7d fail again, the fix is illusory and the real defect is whatever
state accumulates with age.

**I am recording this rather than reporting a win** because the "before" numbers
and the "after" numbers differ in at least three ways (config, process age,
elapsed maintenance) and I changed all of them at once. That is the same
attribution error as this morning's revert, and the only reason I caught it here
is that I shipped a counter that could contradict me.

## The age curve, and why the prime suspect is MY OWN mask fix

Query latency against process age, same query, same project:

| uptime | 3 days | 7 days | MemBuffer rows |
|---|---|---|---|
| 5 min | 213 ms | 227 ms | — |
| 20 min | **475 ms** | **958 ms** | 143,000 |
| ~95 min (the original failure) | **FAILED** | **FAILED** | 449,580 |

**Latency rises with process age, and MemBuffer size rises with it too.** That is
the shape the process-age hypothesis predicted, now with two independent
quantities moving together.

### The mechanism points at `b9fd6f28` — the mem-mask fix I shipped this morning

The mask excludes, from the Delta leg, the time ranges the MemBuffer is
authoritative for. My fix floors that exclusion at the bucket's flushed
watermark, so **an instant the bucket has already flushed is no longer masked** —
which is correct, and is what stopped `COUNT(*)` losing acked rows.

But the consequence scales with age:

- **fresh process** — nothing flushed yet, `flushed_max_ts` at its sentinel, every
  bucket's mask intact, Delta leg tightly excluded, few rows into dedup;
- **aged process** — many buckets have flushed, so their masks are floored away,
  the Delta leg is admitted over those instants, and **every one of those rows
  now flows through `DedupExec`** toward its 2 GiB unordered ceiling.

**So the rows my fix correctly un-hid are the same rows now inflating the dedup
buffer.** The fix is not wrong — those rows are real, committed, and were
invisible before — but I priced it as "a duplicate read-side dedup collapses" and
never measured what it does to dedup's working set. I flagged it as "plausibly
making queries slower" hours ago and did not follow up.

**This is a hypothesis with a mechanism and a matching age curve, not a
conclusion.** What would confirm it: a counter for exclusion ranges dropped by
the floor, rising with uptime alongside dedup buffer size. That counter does not
exist — the same gap that made every other cliff tonight invisible.

**If it is confirmed, the fix is not to revert** (that restores silent row loss)
but to stop the un-masked rows reaching the unordered dedup path — i.e. the
ordering repair, which is exactly what `pending_repair = 252` is blocking.
**Everything converges on the repair backlog.**

## Status at hand-off — what is deployed, what is observed, what is not

**Live on prod (`1f9e2402`):**

| change | deployed | observed doing anything? |
|---|---|---|
| query pool `FairSpill` | yes | **YES** — 26 errors/15 min → 0 |
| `COUNT(*)` mask fix | yes | **YES** — 4-of-7 rows → all 7 |
| repair 1 → 4 files/pass | yes | **NO** — `pending_repair` still 252 |
| unordered leg 64 → 1024 MiB | yes | **NO** — `ordering_repair_declined` = 0, path not exercised |
| packer value floor 100 MiB | yes | **NO** — `pack_value_refused` = 0 |
| pressure scaling | yes | shadow only, by design |
| dedup bin width knob | yes | inert by design (default 10) |

**Three of the five behavioural changes have not been observed doing anything in
prod.** They are deployed, gated and counted; the counters read zero. That is not
the same as "working" and must not be reported as such.

### Two hypotheses I raised and then had to drop

1. **"The packer floor reliably SIGSEGVs."** Wrong. 3 runs on a laptop at 98%
   disk under parallel builds; it does not reproduce (3 batch-queue runs, 3 full
   suites, lldb, and a full gate all clean). I disabled the largest measured
   lever on that, and the user challenged it.
2. **"The query wall is a function of process age."** Not supported. The curve is
   non-monotonic — 8.5 min gave 813/588 ms while 20 min gave 234/277 ms. Variance
   with concurrent load exceeds any age trend.

**The pattern across the whole session is one thing: I formed conclusions from
small samples faster than the data justified, and the counters I shipped are what
kept catching it.** Every claim that survived came from a computation over
matched windows; every claim that fell came from a correlation.

### What is genuinely established

- **Write amplification is 36.5x** (7.5M rows ingested, 274.1M written, matched
  windows) against a `log_fanin` floor of **~1.7x**.
- **82.5% of packing write volume is 2-file merges** delivering **9.9%** of file
  elimination — 115x worse per file removed.
- **The system does not keep up at 10x**: completions flat within 6%, backlog
  11.6x.
- **Dedup is 13.4% of worker time, not 98%**; Pack is 86.6% and write-bound.
- **The log explorer walls past ~2.5 days** on an aged process, with 252 files
  pending repair.

### What to watch next

`pack_value_refused_bytes` (does the floor ever engage?), `pending_repair` (does
4-per-pass drain 252?), and the window ladder on an aged process (does the wall
return?). All three are counters that already exist; none needs new code.

## The age hypothesis is REFUTED, and no wall has returned

Five samples across a 40-minute process life, same query, same project:

| uptime | 3d | 7d |
|---:|---:|---:|
| 507 s | 813 ms | 588 ms |
| 1,109 s | 846 ms | 307 ms |
| 1,209 s | 234 ms | 277 ms |
| 1,711 s | 1,766 ms | 2,657 ms |
| 2,316 s | 1,251 ms | 535 ms |

**7d spans 277–2,657 ms — a 9.6x spread — and the correlation with uptime is
0.28.** There is no age trend; the variation is concurrent load. **My
process-age hypothesis is refuted by its own measurement**, which is the third
hypothesis tonight to die that way (the first two: "my deploy caused the
incident", "the packer floor reliably segfaults").

**And the wall has NOT returned.** Every sample completed; the failure mode that
produced `2048 MiB per-query limit` at 72h and 7d has not recurred in 40 minutes
of a live process under real traffic. Wide windows that were impossible this
morning are now consistently sub-3-second.

**But I still cannot say which change did it.** `ordering_repair_declined = 0`,
`pack_value_refused = 0`, `pending_repair = 252` unchanged. All three levers read
zero. The honest position is: **the wall is gone, and the cause is unestablished
— possibly the FairSpill pool change, possibly a workload difference, possibly
something not in this session's changes at all.**

**Standing question for whoever picks this up:** re-run the ladder when the
process has been up for hours under load. If the wall never returns, the fix is
real and mis-attributed; if it returns at high load rather than high age, the
target is concurrency, not age or any of tonight's knobs.

## CORRECTION: there are TWO repair lanes, and my fix is on the wrong one

`pending_repair` is set from the maintenance JOURNAL —
`per_operation[Operation::Repair]` in `maintenance_coordinator.rs:3341` — so the
**252** is 252 coordinator Repair *tasks*, executed by
`run_coordinator_compaction_once(Operation::Repair)`.

`timefusion_footer_repair_files_per_pass`, which I raised 1 → 4 and shipped as
"the fix for the query wall", changes `select_tail_bin`'s `TailPass::Repair` —
the footer-repair CRON. **A different code path.**

| lane | executed by | backlog | touched by my change? |
|---|---|---|---|
| Coordinator `Operation::Repair` | `run_coordinator_compaction_once` | **252 pending** | **NO** |
| Tail-pass `TailPass::Repair` | footer-repair cron / `select_tail_bin` | unmeasured | yes (1 → 4) |

**So the change cannot drain the 252, and `pending_repair` holding at 252 is not
"the pass has not fired yet" — it is the wrong lane.** I asserted the opposite
twice tonight.

**What this means for the diagnosis.** The chain I proposed — unsorted files →
unordered dedup → 2 GiB ceiling → wide queries fail — may still be right, but the
throughput lever for it is the COORDINATOR repair lane, whose rate I never
measured. The tail-pass rate (1-2 units/95 min, from the phase timers) describes
the lane I did measure, which is not the one holding the backlog.

**Next, and it is now well-posed:** measure the coordinator Repair lane's
completion rate (`work.Repair.*` counters and journal task transitions), then
decide whether 252 is draining at all. If it is not, that is the thing blocking
wide queries, and neither knob shipped tonight addresses it.

**The tail-pass change is not wasted** — 4 files per pass is still strictly more
footer repair per pass than 1, and the pass's own budget bounds it. But it should
not be described as draining the 252.

## THE REPAIR LANE IS THE BOTTLENECK, and it is measured

Sampled 541 wall-seconds apart on one process (no restart between):

```
work.Repair.worker_secs   1,526 -> 6,577   (+5,051 in 541 s = 9.3 WORKERS CONTINUOUSLY)
pending_repair              252 ->   252   (UNCHANGED)
```

**Repair consumes ~9.3 of the 10 heavy permits and the queue does not move.**
That is the entire heavy pool, spent on the lane whose backlog gates wide
queries. Note this is a different lane from the one I chased all night — dedup
was 13.4% of worker time; Repair is currently ~93%.

**The mechanism, and the two counters agree exactly:**

```
compaction_permits_unavailable      34
retry.Repair.compaction_incomplete  34
```

`run_coordinator_compaction_once` returns `BinOutcome::Retry` when it cannot get
a compaction rewrite permit; `maintain.rs:3511` then re-queues the task with
`compaction_incomplete` and a 30-second delay. **Claim a task, fail to get a
permit, retry, repeat.**

**The budget is derived, and small:**

```
repair_rewrite_budget_bytes = REPAIR_REWRITE_TARGET_FILES (2)
                            x COORDINATOR_HOT_TARGET_BYTES (256 MiB)
                            x DECODED_BYTES_PER_COMPRESSED (12)
                            = 6 GiB decoded
```

The config's own comment already describes this failure verbatim from an earlier
occurrence: *"~2 units/hour against `pending_repair = 358` — 173
`repair_rewrite_permit_busy` events in 40 minutes, a queue flat by arithmetic."*
**It is flat by arithmetic again.**

### The obvious lever, and why I am NOT pulling it tonight

`REPAIR_REWRITE_TARGET_FILES = 2` sets the budget. Raising it to 4 doubles
repair's concurrency envelope. But it is deliberately *derived* rather than
hand-set, and `repair_pool_holdback_slices()` is computed from it — the comment
records a bench where 2.4x decoded-to-pool FAILED at the 2.39x rung. **Raising
it without redoing that sizing risks the 2026-09-01 pool exhaustion it exists to
prevent**, and I have mis-attributed three times tonight on less.

**This is the next change, and it is well-posed:** re-derive the holdback for
`REPAIR_REWRITE_TARGET_FILES = 4`, confirm the decoded-to-pool ratio stays under
the failing rung, then raise it. That drains 252, which fixes the unsorted files,
which is what forces wide queries onto the unordered dedup path.

**Correcting myself once more:** I said earlier tonight that dedup was the lane
to fix, then that Pack was. On this process it is **Repair** — at ~93% of the
heavy pool, achieving nothing. The share numbers move with what is queued; the
only stable way to read them is `work.<Op>.worker_secs` deltas over a known wall
interval on one process, which is what this section does.

## Sizing the repair lever: it CANNOT be raised without starving packing

I said the next step was to re-derive `repair_pool_holdback_slices()` for
`REPAIR_REWRITE_TARGET_FILES = 4`. Doing it:

```
per-sort slice     = COORDINATOR_PER_SORT_BUDGET_BYTES = 1,280 MiB
holdback slices    = ceil( files x 256 MiB x 12 / 1.79 / 1,280 MiB )
coordinator_share  = min(coordinator_jobs x 512 MiB, maintenance_pool x 3/5)
                   = min(16 x 512 MiB, 9.9 GiB) = 8 GiB = 6 slices
light_optimize_k   = 6 slices − holdback   (floored at 1)
```

| `REPAIR_REWRITE_TARGET_FILES` | decoded budget | pool needed | holdback | **`light_optimize_k`** |
|---:|---:|---:|---:|---:|
| **2 (today)** | 6,144 MiB | 3,432 MiB | 3 | **3** |
| 3 | 9,216 MiB | 5,149 MiB | 5 | **1** |
| 4 | 12,288 MiB | 6,865 MiB | 6 | **1** |

**Any increase above 2 drives `light_optimize_k` to 1 — the exact 2026-09-01
HotPacking outage the code comments record** (*"K went 3 -> 1 and packing stopped
being claimed at all... zero HotPacking units in 45 minutes with 17 pending"*).

**So the obvious lever is unsafe, and not marginally: 3 is as bad as 4.** Repair
already holds 3 of the coordinator's 6 slices; doubling it takes all six.

### The real constraint is the COORDINATOR SHARE, not repair's budget

Repair and hot-tail packing draw from one 6-slice coordinator budget, and it is
capped by `coordinator_jobs x MAX_DECODED_BYTES = 16 x 512 MiB = 8 GiB` — well
under the `maintenance_pool x 3/5` = 9.9 GiB ceiling. **The cap that binds is the
job count (16, deliberately, for the job:permit ratio), not the pool.**

Three ways out, none of them "raise the repair budget":

1. **Shrink `COORDINATOR_PER_SORT_BUDGET_BYTES`** (1,280 MiB). At 1,024 MiB the
   same 8 GiB yields 8 slices, so `files = 4` leaves `K = 2` instead of 1. Cheapest
   arithmetic change; needs the per-sort budget to actually be sufficient at 1 GiB.
2. **Raise the coordinator share** past 8 GiB by lifting the job cap — but that
   cap exists because a 6:1 job:permit ratio collapsed completions from ~0.6/s to
   0.035/s. It would need re-benching.
3. **Give repair its own pool** rather than a holdback against packing's. The two
   lanes are only coupled because they share the coordinator budget.

**This is why the 252 backlog is not a tuning problem.** Repair is spending 9.3
of 10 heavy permits and completing nothing, and the fix is blocked behind a
budget split that cannot accommodate both lanes at once. That is a design
constraint, and it is the thing standing between here and wide queries working
reliably — not bin width, not the packer floor, not dedup.

## The tuning space is CLOSED: repair needs a cheaper unit, not a bigger budget

I tried option 1 — shrink `COORDINATOR_PER_SORT_BUDGET_BYTES` 1,280 → 896 MiB so
the 8 GiB share yields 9 slices instead of 6, and raise
`REPAIR_REWRITE_TARGET_FILES` 2 → 3. On paper that gave `K = 3` (today's packing
concurrency) with a 50% larger repair budget: a strict improvement.

**A test refused it, correctly:**

```
the_packing_permit_follows_the_coordinator_pool_not_the_light_share
  light_optimize_k + repair_pool_holdback_slices  ==  6
  left: 9   right: 6
  "the fleet must run at the measured optimum, not one rung either side —
   the light/repair SPLIT may move, the total may not"
```

**Slice size is not a free parameter.** It sets how many concurrent rewrites fit,
and the bench measured **6 as the optimum, with 8 as a cliff**. My change would
have run 9 — past the cliff. Shrinking the slice does not create capacity, it
just re-labels it.

**So the constraint is not the coordinator share after all** (my previous
section said it was — wrong). It is the **measured 6-concurrent-rewrite
optimum**, and within it:

| | holdback | K (packing) | total |
|---|---:|---:|---:|
| today (`files = 2`) | 3 | 3 | 6 |
| `files = 3` | 5 | 1 → **starves packing** | 6 |
| `files = 4` | 6 | 0 → floored to 1, total 7 | — |

**Repair can only get more budget by taking it from packing, and packing at
K = 1 is the 2026-09-01 outage.** There is no setting that gives repair more
without breaking something the bench already measured.

### What that means, and it is the session's most useful conclusion

**Repair throughput cannot be fixed by tuning. The unit has to get cheaper.**
A repair unit is a whole-file rewrite (measured 43 minutes contention-free on a
1 GiB file), and there are 252 of them queued behind a 6-slot envelope shared
with packing. The options are structural:

1. **Don't rewrite the whole file.** Repair exists to give a file a sorted footer;
   if sortedness could be recorded without a full rewrite — or if the writer
   never emitted unsorted files in the first place (`repair_sorted_at_write_total`
   = 490 suggests that path already exists and is partially working) — the queue
   drains without competing for the envelope at all.
2. **Deletion vectors** would remove the dedup rewrites from the same envelope,
   freeing slots for repair. Blocked on delta-rs `#4079`.
3. **A bigger box** raises the pool but NOT the concurrency optimum, which is a
   bench-measured property of the sort machinery. It would make each of the 6
   slots larger, not create a seventh.

**This is where the 10x question actually lands.** Not on bin width, not on the
packer floor, not on dedup — on the fact that a fixed 6-slot rewrite envelope is
shared by three lanes, one of which (repair) has a 252-unit backlog of
40-minute units. At 10x ingest that envelope does not grow.

## GOOD NEWS: the bleed is already stopped — 252 is a FINITE legacy backlog

Measured over 300 seconds of live traffic:

| counter | change |
|---|---|
| `repair_sorted_at_write_total` | 68 → 101 (**+33 files, ~400/hour**) |
| `rows_flushed_total` | +332,539 rows |
| `pending_repair` | **252 → 252 (flat)** |

**The writer is marking new files sorted at write, and no new repair tasks are
arriving.** `timefusion_repair_mark_sorted_at_write` defaults true and is working.

**That changes the shape of the problem entirely.** The 252 is not a symptom of
an ongoing failure that will grow with traffic — it is a fixed legacy population,
written before that path existed. Every conclusion above about the repair lane
being "the thing standing between here and wide queries" holds, but the remedy is
a **one-time cleanup**, not an architectural change to the maintenance envelope.

**And it means the 10x story is better than the preceding sections imply.** The
6-slot rewrite envelope is shared by three lanes, but repair's demand on it is
**bounded and shrinking to zero**, not proportional to ingest. At 10x traffic the
repair lane does not scale up — only packing and dedup do.

**The practical remedy, and it does not need the envelope:**
`timefusion optimize --date <D> --recompress` is the only force-rewrite path and
runs outside the coordinator's slot budget. It needs an in-region runner (from
here, one whale cell is 85 GiB at 3.9 MiB/s ≈ 6 hours), which is the same
blocker recorded earlier in this document for the bin-width recompress.

**Revised priority, final:**
1. **Run `--recompress` from an in-region runner** over the dates holding the 252.
   Finite, one-time, unblocks wide queries, needs no code.
2. **Watch `pack_value_refused_bytes`** — the packer floor is live and its
   ~41% worker-time projection is the largest *recurring* saving identified.
3. **Everything else** (bin width ~3.7%, pressure scaling, deletion vectors)
   ranks below those two.

## Final verification: wide queries are fast and STABLE

The 7-day log-explorer listing, six consecutive runs on the current process:

```
384 ms   292 ms   445 ms   495 ms   317 ms   362 ms      (p50 ~370, max 495)
```

**This morning the same query FAILED after 8.8 s** with
`unordered merge-on-read dedup exceeded its 2048 MiB per-query limit`.

The single 8,694 ms reading I took just before this was an outlier — a cold
window after a restart, or a concurrent maintenance burst. Six runs put the
steady state at **~370 ms**.

**And it is not the unordered path.** Every ordering counter is clean on this
process:

```
mem_ordering_declared 3065   mem_ordering_rejected 0   mem_ordering_unsorted 0
mem_sort_retracted 0         ordering_repair_* all 0
```

So the 252 legacy files are NOT currently forcing queries onto the unordered
dedup path, and the recompress cleanup — while still correct to do — is **not
urgent**. That is a third revision of this document's priority list, and it is
what the measurement says.

### Where the query problem actually stands

| | this morning | now |
|---|---|---|
| 7-day listing | **FAILS** after 8.8 s | **~370 ms**, 6/6 runs |
| 3-day listing | **FAILS** after 10.6 s | ~210-475 ms |
| 30-day listing | (would fail) | 1.5-2.5 s |
| sort-exhaustion errors | 26 / 15 min | **0** |

**The query wall is gone and wide windows are fast.** What I still cannot say is
which change did it — the ordering knobs read zero usage, so `FairSpill` remains
the most plausible cause and it was not my change. Attribution failed; the
outcome is real and verified over repeated runs.

## The packer floor, finally correct: −69% amplification with a fan-in escape

Two bugs in my own guard, both found by its own counter reading zero in prod:

1. **It could never fire.** I derived 100 MiB from a ROWS measurement
   (3,129,141 rows per file eliminated) converted at 104 B/row **decoded** — but
   `select_tail_bin` compares `add.size`, **compressed**, 12x smaller. Prod's
   median 2-file merge is 3.77M rows ≈ **31 MiB compressed**, far under a 100 MiB
   floor. Now priced in ROWS, which `TailAdd::rows`' own doc says is what a
   rewrite costs.
2. **A biting floor WEDGES packing.** At 18 MiB arrivals (~2.18M rows/file) even
   a 10-file merge exceeds 1M rows per file eliminated, so the measured knee
   refused **every** bin: amplification fell to **0.00x** — nothing merged. The
   harness also reported 0.04x at another setting, below the 1.0x floor of
   writing each byte once, which is what flagged it as untrustworthy.

**The fix is a fan-in escape: never refuse a bin of `min_files` or more.** High
fan-in is the shape we are steering toward; refusing it leaves the partition
unpacked forever. The guard can now only remove LOW-fan-in bins — exactly the
measured problem (2-file merges: 82.5% of packing write volume for 9.9% of the
file reduction) and never the remedy.

**Result on the real `select_tail_bin`, 400 rounds:**

```
amplification  6.73x -> 2.06x   (-69%)
live files       31  ->   32    (no regression)
```

**2.06x sits just above the `log_fanin` floor of ~1.7x** for this file size, and
the numbers are physically sensible again.

**Shipped enabled** at 1,000,000 rows per file eliminated (`395980d3`), gate
green at 1387/1387 + 62/62 after confirming the one failure was load flake by
rate (3 clean lib runs, then a clean full gate). Rollback: set to 0.

**This is the first maintenance change tonight with a mechanism, a safety
argument, and a measured effect that survived its own scrutiny.** The counter
`pack_value_refused` / `pack_value_refused_rows` is what to watch in prod — and
it is the same counter that caught both bugs above, which is the argument for
shipping guards in shadow first.

## VERIFIED IN PROD: the packer floor cuts Pack write volume 66%

Deployed on both lanes (`779ead68`). After 8.5 minutes:

```
pack_value_refused        5
pack_value_refused_rows   10,982,857     (11M rows of rewriting avoided)
```

**The guard fires.** And the outcome, measured from `unit_phase_timing`:

| | before the guard | after |
|---|---:|---:|
| Pack write rate | **170,168,000 rows/hour** | **57,037,747 rows/hour** |
| median fan-in | 2-3 | **4** |

**−66% of packing write volume**, against the local benchmark's **−69%**
prediction and the prod-replay estimate of **82%**. Three independent methods
agreeing within a reasonable band, the last of them on live traffic.

**What had to be fixed to get here, all found by the counter reading zero:**
1. Priced in BYTES from a ROWS measurement — 12x off via the compression ratio,
   so the floor could never fire.
2. A biting floor WEDGED packing (amplification 0.00x) until the fan-in escape.
3. Placed on `select_tail_bin` while the work came through
   `select_coordinator_compaction_candidates` — both feed `stage_hot_bin` and
   both emit `pass=Pack`, so the phase timings could not tell them apart.

**Caveats, because this is a 9-minute sample:** the 170M/hour baseline came from
a different process in a more backlogged state, so some of the difference may be
workload rather than the guard. The counter proves the mechanism fires and the
magnitude is consistent across three methods — but the honest claim is "-66% on
this sample", not "-66% steady state".

**Against the 36.5x write amplification measured earlier, a 66% cut in the
dominant lane's write volume is the first change tonight that moves the capacity
number rather than describing it.** Pack was 85.3% of maintenance worker time and
59.3% write-bound.

## Write amplification now reads 4.7x — with the caveat that kills a clean claim

Measured since boot on the both-lanes build, matched by construction (both
quantities count from process start):

```
Pack   6 units   2,857,093 rows
Dedup  3 units     558,566 rows
                 ----------
maintenance written  3,415,659      ingested  723,479
WRITE AMPLIFICATION  4.7x           (this morning: 36.5x)
```

**Do NOT read that as "the guard cut amplification 7.8x."** The 36.5x came from a
**95-minute window on a mature, backlogged process**; this is a **12-minute
window on a 705-second-old one**, with only 9 maintenance units in it. A young
process has less accumulated debt to grind through, so lower amplification is
expected regardless of the guard. Comparing them is exactly the
different-process-state error this document has already recorded three times.

**What IS defensible, because the windows were comparable:**

| claim | evidence |
|---|---|
| the guard fires | `pack_value_refused = 5`, `pack_value_refused_rows = 10,982,857` in 8.5 min |
| Pack write rate fell | **170M -> 57M rows/hour (−66%)**, phase-timer measurement either side |
| median fan-in rose | 2-3 -> **4** |
| the benchmark agrees | −69% on the real packer over 400 rounds |

**And the 4.7x is still worth recording as a data point**: it is far closer to
the `log_fanin` floor of ~1.7x than to 36.5x, on a process where the guard is
active. Whether steady state lands at 4.7x or drifts back up as debt accumulates
is the thing to measure tomorrow on an aged, quiet process — the same conditions
that produced the 36.5x.

**Measurement note for whoever continues:** both sessions were deploying roughly
every 18 minutes tonight, which resets every counter and destroys any window
longer than that. The 36.5x baseline exists only because prod happened to run 95
minutes undisturbed. Getting a comparable post-guard number requires the same
quiet, which means agreeing not to deploy for an hour.

## What the verified 66% actually buys against 10x: about 6%

The packer floor is real, deployed and measured: **−66% of Pack write volume**.
Converting that to capacity, using only measured quantities:

```
maintenance worker-time saved = Pack share 85.3% x write fraction 59.3% x cut 66%
                              = 33.4%
implied unit-cost multiplier  = 0.67x
```

And throughput's response to unit cost was measured directly (5x load, 400-round
sweep):

| unit cost | executions | vs baseline |
|---|---:|---:|
| 1.0x | 5,342 | 1.00x |
| 0.5x | 5,818 | 1.09x |
| 0.25x | 9,709 | 1.82x |
| 0.1x | 23,082 | 4.32x |

At **0.67x**, interpolation gives **~5,660 executions — a 1.06x throughput gain.**

**10x load needs ~10x throughput. The largest lever found tonight, verified
working in production, delivers ~1.06x.**

### Why, and this is the structural answer

**Throughput is sub-linear and threshold-shaped in unit cost.** Halving cost buys
9%; only at 4x cheaper does it start to pay (1.82x), and even 10x cheaper gives
4.32x. So no amount of making units cheaper reaches 10x on its own — the
constraint is the fixed 6-slot rewrite envelope, not the size of what goes in it.

**That reframes the whole night correctly:**
- The guard is a **cost/IO win** — a third less maintenance work, real money and
  real headroom — and it should stay.
- It is **not a capacity fix**, and neither is bin width (~3.7%), pressure
  scaling, or anything else in the tuning space.
- **10x needs the envelope to grow or the work to disappear**, i.e. deletion
  vectors (removes dedup rewrites entirely, blocked on delta-rs #4079) or
  levelled/key-range compaction (removes the re-merging that makes packing
  dominant), or more concurrent rewrite slots than the bench's measured optimum
  of 6 allows.

**The honest bottom line for the 100x customer:** current architecture does not
get there by tuning. The measurements now say exactly which of the three
structural options to price, and the counters to prove any of them exist and are
deployed.

## CORRECTION: the 6-slot envelope is pool-derived, not a law — and the pool is job-capped below its memory cap

I wrote that "a bigger box makes each slot larger, not create a seventh." **That
is wrong.** The bench the 6 comes from (`benches/rewrite_throughput.rs`,
`TF_BENCH_FLEET=1`, N concurrent rewrites of a real 204 MB prod file):

```
4 workers  29.16 MB/s  0 failed
5 workers  29.31 MB/s  0 failed
6 workers  33.32 MB/s  0 failed   <- best
8 workers  15.07 MB/s  4 FAILED   <- cliff
```

**The 8-worker cliff is 8 x ~1.33 GiB against a shared 8 GiB pool — it is an OOM,
not a scheduling property.** So the envelope is `pool / per-sort footprint`, and
more pool really does buy more slots.

**And the coordinator's pool is capped below its memory ceiling by a JOB COUNT:**

```
coordinator_share = min(coordinator_jobs x MAX_DECODED_BYTES, maintenance_pool x 3/5)
                  = min(16 x 512 MiB,  16.6 GiB x 3/5)
                  = min(8 GiB,         9.9 GiB)        -> 8 GiB, job-bound
```

**There is 1.9 GiB of maintenance pool the coordinator is not allowed to use**,
because `coordinator_jobs` is clamped at 16. At the 1.25 GiB slice that is
**one and a half extra slots** — a 17-25% increase in concurrent rewrites, which
on the bench's own curve is the direction that was still improving at 6.

**Why the clamp exists, and what it would need:** 16 jobs against
`HEAVY_REWRITE_PERMITS = 10` was chosen because "at a 6:1 job:permit ratio,
completions collapsed from ~0.6/s to 0.035/s". Raising jobs alone worsens that
ratio. **Raising jobs AND permits together is the untested combination**, and the
bench harness to test it already exists.

**So option 3 ("more slots") is NOT blocked by a measured law, as I claimed.** It
is blocked by an untested interaction between two clamps, with a bench available
to test it. That is a materially more hopeful position than "tuning is
exhausted", and it is the first thing I would run on a machine that is not also
serving prod.

## 37-minute window: amplification 1.2x, fan-in 8 — and the caveat that matters

Same measurement, longer window, guarded build live:

| | pre-guard (95 min) | now (37 min) |
|---|---:|---:|
| Pack write rate | 170,168,000 rows/hr | **4,885,364 rows/hr** |
| **write amplification** | **36.5x** | **1.2x** |
| median fan-in | 2-3 | **8** |
| Pack units | 164 | 10 |

**The fan-in shift is the guard's mechanical signature.** It refuses low-fan-in
bins, so the survivors are high-fan-in — 2-3 to 8 is exactly what the mechanism
produces, and it is causal rather than merely correlated. 1.2x is at the floor
of what is physically possible (1.0x = writing each byte once).

**THE CAVEAT, and it is not small: Pack ran 10 units in 37 minutes, against 164
in 95.** That is ~7x fewer units per minute. Two readings fit:

1. **Efficiency** — the guard replaced many cheap-benefit merges with a few
   high-benefit ones, which is exactly its design (fan-in 8 supports this).
2. **Idleness** — this process simply has less to do than the backlogged one that
   produced the 36.5x, and the guard is incidental.

**Both are consistent with the data I have, and I cannot separate them without a
matched-state comparison.** The honest claim is the one that does not depend on
which is true: **the guard fires (verified counters), it shifts fan-in from 2-3
to 8 (its designed mechanism), and Pack's write volume per unit of file
reduction is far lower.** Whether steady-state amplification settles at 1.2x or
drifts back toward 36.5x as backlog accumulates needs an aged, quiet process —
the same conditions that produced the baseline.

**What to check tomorrow, in one query:**

```sql
SELECT key, value FROM timefusion_stats
WHERE key IN ('pack_value_refused','pack_value_refused_rows','uptime_seconds');
```

plus `median fan-in` from `unit_phase_timing`. If fan-in stays high and
`pack_value_refused_rows` keeps climbing on a process that has been up for
hours, the win is real and steady.

## The 8-worker "cliff" is an OOM, confirmed by re-running the bench

Re-ran `benches/rewrite_throughput.rs` (`TF_BENCH_FLEET=1`) with a file small
enough that memory never binds (35 MB prod sample), at two pool sizes:

| workers | pool 512 MB | pool 1024 MB | failed |
|---:|---:|---:|---:|
| 1 | 51.86 MB/s | 53.11 | 0 |
| 6 | 132.58 | 142.94 | 0 |
| 8 | 138.03 | 144.09 | 0 |
| 10 | 137.26 | **148.01** | 0 |
| 12 | 133.90 | 139.45 | 0 |
| 16 | 140.09 | 143.27 | **0** |

**Zero failures at every concurrency, up to 16 workers.** Throughput rises to a
plateau around 6-10 and stays flat — it does **not** collapse.

Compare the original bench, which produced the "6 optimum / 8 cliff" constant
with a **204 MB** file and an 8 GiB pool:

```
6 workers  33.32 MB/s  0 failed
8 workers  15.07 MB/s  4 FAILED
```

**The difference is entirely file size against pool.** 8 x ~1.33 GiB exceeded
8 GiB and four workers died; 16 x a small file fits anywhere. **So the cliff is
an out-of-memory boundary, not a scheduling property of the sort machinery, and
the "measured optimum of 6" is `pool / per-unit footprint` — not a law.**

### Why that matters more than it looks

**The packer floor reduces per-unit footprint** (it refuses the large low-fan-in
bins; median fan-in went 2-3 -> 8, and those bins are smaller per file
eliminated). **A smaller per-unit footprint means MORE units fit the same pool** —
so the two levers compose: cheaper units do not just cost less, they raise the
concurrency ceiling that the whole capacity argument rests on.

**This is the first evidence tonight that the 6-slot envelope can move**, and it
did not need a bigger box — only smaller units, which is already deployed.

**What it does NOT establish:** the optimal N at *prod's* file sizes and pool.
The small-file run shows the cliff is memory-shaped; it cannot say where prod's
new optimum sits now that units are smaller. That needs the 204 MB file
(`scratchpad/whale/recent204.parquet`, already on disk) against an 8 GiB pool,
on a machine not also serving prod — this laptop was at 97% disk and 360 MB free
memory, which is why the small file was used.

### Prod-scale confirmation is blocked: the sample files were deleted

Attempting the same sweep with the 204 MB prod file (to find where the OOM
threshold sits at real file size, and whether it doubles with the pool) failed:

```
panicked at benches/rewrite_throughput.rs:127: stat: No such file or directory
```

`scratchpad/whale/` is now empty — `recent204.parquet` (195 MB),
`wide431.parquet` (411 MB) and `big1148.parquet` (1.1 GB) were removed by disk
pressure while this laptop sat at 97-98% full. Only the 35 MB `prod_sample.parquet`
survives.

**So the structural claim stands on the small-file run** (cliff is memory-shaped:
0 failures to 16 workers when memory suffices) **and the arithmetic of the
original bench** (8 x 1.33 GiB > 8 GiB), **but the prod-scale optimum is
unmeasured.** Re-running it needs `recent204.parquet` back — roughly 50 minutes
to re-fetch at the ~3.9 MiB/s this machine gets from the bucket, or seconds from
an in-region runner.

**The one-command version, for whoever has a box:**

```bash
TF_BENCH_FLEET=1 TF_BENCH_PARQUET=<204MB prod file> TF_BENCH_POOL_MB=8192 \
  cargo bench --bench rewrite_throughput
# then repeat at TF_BENCH_POOL_MB=12288. If the failure threshold moves from
# 8 workers to ~12, the envelope is pool-bound and `coordinator_jobs` (clamped
# at 16, holding 1.9 GiB of maintenance pool unreachable) is the lever.
```

## Prod-scale bench: the cliff does NOT reproduce, and concurrency scales to 16

Re-fetched a 202 MB prod file (`project_id=dcad860a…/date=2026-06-17`) and ran
the same fleet sweep:

| workers | pool 2048 MB | pool 4096 MB | failed |
|---:|---:|---:|---:|
| 1 | 165.23 MB/s | 185.04 | 0 |
| 6 | 831.35 | 817.30 | 0 |
| 8 | 742.43 | 890.68 | 0 |
| 12 | 873.65 | 893.71 | 0 |
| 16 | **900.04** | 888.28 | **0** |

**Zero failures at every concurrency up to 16, on a 2 GiB pool** — against the
original bench's **4 failures at 8 workers on an 8 GiB pool**. Aggregate
throughput *rises* from 831 to 900 MB/s between 6 and 16 workers; per-worker
throughput falls (165 -> 56 MB/s), which is ordinary contention, not collapse.

**So the "6 optimum / 8 cliff" does not reproduce at prod file size.** Combined
with the earlier small-file run (also 0 failures to 16), the constant behind
`COORDINATOR_PER_SORT_BUDGET_BYTES` and the whole 6-slot envelope is **not a
portable law** — it described one file, one pool and one machine.

**What this does and does not license:**
- It DOES mean the envelope should be re-derived rather than trusted. The
  `light_optimize_k + holdback == 6` invariant is asserted in a test as "the
  bench's measured optimum"; that bench does not reproduce here.
- It does NOT mean prod can run 16 concurrent rewrites. This laptop's NVMe spill
  and CPU differ from the prod box, the file I fetched may differ in row-group
  layout from the original, and prod's pool is shared with three other lanes.
  **The number must be re-measured on prod hardware before anything is changed.**

**Concretely, the follow-up is one command on a prod-like box**, with the file
already identified:

```bash
TF_BENCH_FLEET=1 TF_BENCH_POOL_MB=8192 \
  TF_BENCH_PARQUET=<202MB file> cargo bench --bench rewrite_throughput
```

If it shows what this run shows, `coordinator_jobs` (clamped at 16, holding
1.9 GiB of maintenance pool unreachable) and the 6-slot split are both leaving
throughput on the table — and that is the remaining structural path to 10x,
alongside the packer floor already deployed.

## The guard holds as the process ages — it does not drift back

I flagged that 1.2x might drift toward 36.5x as backlog accumulated. Measured
across three windows on the SAME process:

| window | amplification | median Pack fan-in | Pack rate |
|---|---:|---:|---:|
| 12 min | 4.7x | 4 | — |
| 37 min | 1.2x | 8 | 4,885,364 rows/hr |
| **51 min** | **1.1x** | **9** | **3,633,568 rows/hr** |

**Amplification FALLS and fan-in RISES as the window lengthens** — the opposite
of the drift I was worried about. 1.1x is essentially the floor (1.0x = writing
each byte once), and `pack_value_refused` climbed 3 -> 4 with
`pack_value_refused_rows` 6.75M -> 8.91M over the same period, so the guard is
actively working the whole time rather than having fired once early.

**The remaining honest caveat is unchanged and cannot be closed from here:** this
process carries less backlog than the one that produced 36.5x, so part of the
difference is state, not the guard. What the three windows DO establish is that
**within one process the effect is stable and improving, not transient** — which
was the specific worry.

**Reading for tomorrow:** if a process that has been up for hours still shows
median Pack fan-in near 9 and `pack_value_refused_rows` climbing, the guard is
doing in steady state exactly what it did here.

## Safety check: the guard is not starving packing

The one way this change could be quietly harmful is by refusing so much that
files accumulate. Checked:

```
pending_hot_packing         16 -> 16    (flat across every reading tonight:
                                         uptime 510s, 765s, 3148s, and a restart)
pending_sealed_consolidation 93 -> 94
```

**Neither queue trends up.** Packing is keeping pace with a much smaller write
rate, which is the intended outcome — fewer, higher-fan-in merges doing the same
file reduction.

### Reconciling the two regimes, honestly

Pack now writes **3.6M rows/hr** against **170M rows/hr** before, at comparable
ingest (4.0 vs 4.7 M rows/hr). That is a **47x** difference, far more than the
66% I first measured on a 9-minute window — and more than the guard alone can
plausibly explain.

**The reconciliation is that the two processes were in different REGIMES:**
- the 36.5x baseline ran 95 minutes on a process **grinding a backlog** —
  164 Pack units, much of it re-merging accumulated debt;
- this process is in **steady state** — 11 Pack units, queues flat, fan-in 9.

**So "36.5x -> 1.1x" is not a like-for-like measurement of the guard, and should
never be quoted as one.** What is like-for-like and does hold:
- the guard fires continuously (`pack_value_refused_rows` 6.75M -> 8.91M);
- median fan-in moved 2-3 -> 9, which is its designed mechanism and nothing
  else in the system does that;
- queues are flat, so the reduction is not deferred work piling up;
- the local benchmark, which IS like-for-like, showed **6.73x -> 2.06x (-69%)**.

**The defensible summary: the guard works as designed and the benchmark's -69% is
the number to quote. The prod 36.5x -> 1.1x spans a regime change and overstates
it.**

## I priced the guard on the WRONG AXIS: it is worth ~3.3x, not 1.06x

Earlier I converted the guard's benefit through **unit cost** — units get cheaper,
and the measured throughput-vs-cost curve is sub-linear, so I concluded ~1.06x
against 10x. **That was the wrong axis.**

The sim's own streams sweep measured backlog as **linear in unit COUNT**:

| arrivals | pending_end |
|---:|---:|
| 74 (1x) | 18,029 (1.0x) |
| 148 (2x) | 39,241 (2.2x) |
| 370 (5x) | 102,635 (5.7x) |
| 740 (10x) | 208,532 (11.6x) |

**And the guard's primary effect is on count, not cost.** Fan-in moved 2-3 -> 9,
so files eliminated per unit went **1.5 -> 8.0**:

```
units needed for the SAME file reduction   5.3x FEWER
Pack share of maintenance worker time      85.3%
maintenance UNIT COUNT removed             ~69%
headroom on the axis backlog is linear in  ~3.3x
```

**So the guard is worth roughly 3.3x, not 1.06x** — I under-valued my own change
by 3x by measuring it against the wrong variable.

**Caveats, and they are real:**
- fan-in 9 comes from a steady-state process; the 2.5 baseline from a
  backlog-grinding one. Same regime confound as the amplification numbers, so
  treat 5.3x as an upper estimate of the count reduction.
- "same file reduction" assumes the total need is unchanged; if higher fan-in
  leaves some files unmerged the comparison flatters the guard. `pending_hot_packing`
  flat at 16 argues against that, but does not disprove it.
- the sim's linearity was measured on minted rollup arrivals, not Pack units.

### What this does to the 10x picture

| lever | status | multiplier |
|---|---|---:|
| packer floor (unit COUNT) | **deployed, verified firing** | **~3.3x** |
| rewrite envelope re-derivation | bench says the 6-slot cliff does not reproduce; 16 workers ran clean | up to ~2x |
| **combined** | | **~6.6x** |

**That is within sight of 10x for the first time tonight**, from two changes
neither of which is an architectural rewrite — one already shipped, one a
re-measurement on prod hardware. It does not demonstrate 10x, and the caveats
above are not small. But the gap is now a factor of ~1.5, not a factor of 10.

## Simulated directly: the guard's 5.3x is real, the envelope lever is worth ZERO

The sim takes `--workers`, so both levers can be tested at 10x load rather than
argued about. 24 virtual hours, seed 1, `--mint --streams N`:

| scenario | pending_end | executions |
|---|---:|---:|
| 10x load, guard OFF (740 streams) | **211,356** | 794 |
| 10x load, guard ON (140 streams ≡ 5.3x fewer units) | **39,756** | 794 |
| 10x + guard + **2x workers** | 39,378 | **1,473** |

**Two results, one confirming and one refuting my own claims:**

1. **The guard's count-axis benefit is CONFIRMED: 211,356 -> 39,756, a 5.3x
   backlog reduction at 10x load.** That is the first independent check of the
   ~3.3x/5.3x estimate, and it lands at the top of the range.

2. **Doubling workers is worth NOTHING: 39,756 -> 39,378 (0.9%), while
   executions doubled 794 -> 1,473.** The queue is arrival-dominated in this
   regime — more workers complete more units without touching the backlog.
   **So "re-derive the envelope for up to 2x" is refuted**, and my "~6.6x
   combined, gap of 1.5x" from the previous section is wrong. Combined is ~5.3x,
   because the second lever contributes nothing.

**Where that leaves 10x, measured rather than estimated:** at 10x load with the
guard, backlog ends at **39,756 against the 1x baseline of 18,029** — still
**2.2x above** the level that constitutes keeping up. **The guard closes most of
the gap (11.6x -> 2.2x) and does not close it.**

**The remaining 2.2x has to come from somewhere other than worker count**, and
the sim says explicitly where not to look. The candidates left are the ones that
remove work rather than schedule it faster: deletion vectors for dedup, and
whatever further raises fan-in beyond 9 (the guard's floor is already at the
measured knee, so this means changing how files are written, not how they are
selected).

## A MODELLED PATH TO 10x: the deployed guard + doubling the bin target

The residual after the guard is 1.9x, and it has to come from unit COUNT (the sim
proved worker count contributes nothing). Fan-in is bounded by
`bin target / input file size`, so doubling `COORDINATOR_HOT_TARGET_BYTES`
(256 MiB) roughly doubles fan-in, 9 -> ~18.

Simulated at 10x load, 24 virtual hours:

| scenario | pending_end |
|---|---:|
| **1x baseline — the keeping-up level** | **18,335** |
| 10x + guard (fan-in 9) | 37,267 |
| **10x + guard + 512 MiB target (fan-in ~18)** | **18,285** ✓ |
| 10x + guard + 768 MiB target (fan-in ~27) | 18,001 |

**At 10x load, the deployed guard plus a doubled bin target reaches the same
backlog as 1x today.** That is the first modelled demonstration of keeping up at
10x in this document, and it needs two constants, one of which is already shipped.

### The assumptions, stated — this is a model, not a measurement

1. **Fan-in scales linearly with the bin target.** Plausible (a bin fills to the
   target, so twice the target holds twice the files) but not measured. The
   packer's other budgets — `MAX_BIN_ROWS`, the row cap — may bind first.
2. **Unit-count reduction is modelled as a stream-count reduction** (67 streams
   ≈ 140 / 2.1). The sim has no bin-target input, so this is the closest
   available proxy.
3. **The sim is IO-free.** Larger files have real costs it cannot see.

### The tension this creates, and it is the one to check first

**Bigger files make dedup worse.** A dedup unit reads every file overlapping its
bin, in full, so a file spanning more bins is read more times — the finding that
opened this whole investigation. Doubling the pack target widens files and
therefore widens that span.

**But the ratio now favours it:** Pack is **85.3%** of maintenance worker time,
dedup **7.5-13%**. Trading a worse 10% lane for a much better 85% lane is
probably right — and **`timefusion_dedup_bin_minutes` (shipped, default 10) is
the exact offset**: widening dedup bins to match the wider files restores the
files-per-bin ratio. The two knobs are complements, which is not how I framed
them earlier — I ranked bin width last on its own throughput merits, and it may
instead be the enabler for the pack-target change.

**Recommended next step:** measure fan-in and dedup read volume at
`COORDINATOR_HOT_TARGET_BYTES = 512 MiB` in staging, with
`timefusion_dedup_bin_minutes` at 10 and 20, before touching prod. Assumption 1
is the one that decides whether any of this holds.

## Assumption 1 CONFIRMED: fan-in scales linearly with the bin target

The 10x path rested on one unverified step — that doubling
`COORDINATOR_HOT_TARGET_BYTES` doubles fan-in. `MAX_BIN_ROWS` or the row cap
could have bound first. Driven through the **real `select_tail_bin`** with 40
contiguous 16 MiB candidates (`fan_in_scales_with_the_bin_target`):

```
fan-in:  256 MiB -> 16    512 MiB -> 32    768 MiB -> 40 (fixture cap)
```

**Exactly linear.** And it calibrates against prod: observed fan-in 9 at a
256 MiB target implies ~28 MiB average inputs, so 512 MiB should give ~18 —
which is precisely the number the 10x simulation needed.

### Every link in the 10x chain now has evidence

| link | evidence | type |
|---|---|---|
| the guard raises fan-in 2-3 -> 9 | prod `unit_phase_timing` | **measured** |
| that cuts unit count ~5.3x | sim: 211,356 -> 39,756 at 10x | **simulated** |
| backlog is linear in unit count | sim streams sweep, 4 points | **measured (sim)** |
| fan-in scales with bin target | real packer: 16 -> 32 -> 40 | **measured** |
| guard + 512 MiB reaches the 1x backlog at 10x | sim: 18,285 vs 18,335 | **simulated** |

**What is still NOT demonstrated, and it is the honest boundary:** every step is
evidenced, but two are simulated on an IO-free model, and the model cannot see
the cost of wider files on the dedup lane — a dedup unit reads every file
overlapping its bin in full, so doubling file width widens that span. `timefusion_dedup_bin_minutes`
is the offset and is already shipped, but the combination has never been run
against real object storage.

**So the state is: a complete, evidenced chain to 10x, with one unmodelled
interaction (wider files vs dedup reads) that staging must price.** That is a
materially different position from "the architecture cannot get there by
tuning", which is where this document stood four hours ago — and the difference
came from measuring the packer instead of trusting the arithmetic around it.

## The last unmodelled interaction, PRICED from 5,701 real prod files

The 10x path's only remaining unknown: doubling the pack target widens files, and
a dedup unit reads every file overlapping its bin **in full**, so wider files
span more bins. Computed from the live Delta checkpoint (`521610`), 5,701 files
with timestamp stats — no data read, no staging:

```
file event-span seconds:  p50 = 1,616    p90 = 85,798
dedup read = sum over files of  size x (span / BIN + 1)
```

| change | dedup read volume |
|---|---:|
| baseline | 1.00x |
| files **2x wider** | **1.99x** |
| files 3x wider | 2.98x |
| **files 2x wider + dedup bins 2x wider** | **1.00x** |
| files 3x wider + dedup bins 3x wider | **1.00x** |

**The regression is real — doubling file width almost exactly doubles dedup read
— and widening the dedup bin to match cancels it exactly.** That is
`timefusion_dedup_bin_minutes`, shipped tonight and defaulted to 10, which I
ranked LAST on its own throughput merits. **It is not a throughput lever; it is
the enabler that makes the pack-target change free.**

### The complete chain, every link evidenced

| link | evidence | type |
|---|---|---|
| guard raises fan-in 2-3 -> 9 | prod `unit_phase_timing` | measured |
| guard cuts unit count ~5.3x | sim 211,356 -> 39,756 at 10x | simulated |
| backlog linear in unit count | sim streams sweep | simulated |
| fan-in scales with bin target | **real packer 16 -> 32 -> 40** | measured |
| guard + 512 MiB reaches 1x backlog at 10x | sim 18,285 vs 18,335 | simulated |
| wider files cost 2x dedup read | **5,701 real files** | measured |
| **widening dedup bins cancels it exactly** | **5,701 real files** | **measured** |

**So the 10x proposal is two constants, both already implemented as config:**

```
COORDINATOR_HOT_TARGET_BYTES      256 MiB -> 512 MiB
timefusion_dedup_bin_minutes           10 -> 20
```

on top of the packer floor already live in prod.

**What remains genuinely unproven:** the two simulated links, and whether dedup's
WORKER TIME follows its read volume (sort cost is not purely proportional to
bytes read). Those want a staging run. But the interaction I flagged as the
blocker is priced, and it is neutral — which is a stronger position than "staging
must find out".

## The second constant is NOT safe alone: dedup already overruns its deadline

Before recommending `timefusion_dedup_bin_minutes` 10 -> 20, I checked what
bounds a dedup unit's wall clock:

```
timefusion_dedup_schedule  "0 */5 * * * *"   -> 300 s period
tick_budget                 300 x 0.8        -> 240 s
drain_deadline              240 x 0.6        -> 144 s      (mod.rs:5094)
bin_deadline = stage_deadline.min(remaining)               (maintain.rs:5952)
```

**The drain gets 144 seconds. Tonight's phase timers measured a dedup unit at
502 seconds — already 3.5x over.** Widening bins doubles the work per bin, so it
would push further past a deadline that is already being exceeded.

**So the two-constant proposal needs a third change, or it is unsafe:**

| constant | change | why |
|---|---|---|
| `COORDINATOR_HOT_TARGET_BYTES` | 256 -> 512 MiB | doubles fan-in (measured on the real packer) |
| `timefusion_dedup_bin_minutes` | 10 -> 20 | cancels the dedup read cost exactly (measured on 5,701 files) |
| **`timefusion_dedup_schedule`** | **5 min -> 10+ min** | **a 144 s drain deadline cannot hold a bin that already takes 502 s** |

**This is why the change wants staging rather than a 4 a.m. prod flip**, and it is
a better reason than "unvalidated": there is a specific, measured conflict
between the proposed bin width and the deadline the drain runs under. The
sequencing matters too — the schedule change must land before or with the bin
widening, never after.

**And note the shape of this finding: it came from reading what bounds the unit,
not from running anything.** The same 20 minutes spent earlier would have saved
the detour where I ranked bin width last on throughput grounds; its real
constraint was never throughput, it was the deadline.

### Correcting the third constant: it is 35 minutes, not "10+", and it has a cost

I wrote `timefusion_dedup_schedule` "5 min -> 10+ min". Worked out properly
(`drain = period x 0.8 x 0.6`):

| schedule | drain deadline | holds today's 502 s unit | holds 2x-widened (~1004 s) |
|---|---:|---|---|
| 5 min (today) | 144 s | no | no |
| 10 min | 288 s | no | no |
| 20 min | 576 s | **yes** | no |
| **35 min** | **1008 s** | **yes** | **yes** |

**The bin widening requires a 7x reduction in dedup cadence**, not the ~2x I
implied. And that is not free: dedup running every 35 minutes instead of every 5
means duplicates persist up to 7x longer before removal, which the READ path pays
for in merge-on-read work on every query touching those windows.

**So the 10x proposal's true shape is:**

```
COORDINATOR_HOT_TARGET_BYTES   256 MiB -> 512 MiB     (pack: 2x fan-in)
timefusion_dedup_bin_minutes        10 -> 20          (dedup read: neutral)
timefusion_dedup_schedule        5 min -> 35 min      (deadline: required)
                                                       ^ costs read-path latency
```

**That third line is a genuine trade, not a technicality**, and it is the thing
to weigh before adopting the path: pack throughput at 10x, bought partly with
dedup latency. Whether that is the right trade depends on how much merge-on-read
the query path can absorb — which is measurable (`DedupExec` mode and row counts
per query) and has NOT been measured.

**Alternative worth pricing first:** widen the pack target WITHOUT widening dedup
bins, accepting 1.99x dedup read but keeping the 5-minute cadence. Dedup is
7.5-13% of worker time, so 2x on that lane is 8-13% overall — possibly cheaper
than a 7x cadence loss. **That variant needs no schedule change at all**, and the
data to price it is already in this document.

## The pack-target change would hit INTERACTIVE queries — so raise the SEALED target instead

Before recommending Option A, I measured what doubling file width does to query
pruning, from the same checkpoint (5,701 files, Delta min/max per file):

| window | selected now | if files 2x wider | ratio |
|---|---:|---:|---:|
| **1 hour** | 0.2 GB | 0.6 GB | **2.59x** |
| 24 hours | 4.2 GB | 4.5 GB | 1.05x |
| 7 days | 25.2 GB | 26.0 GB | 1.03x |

**Doubling file width costs 2.6x on one-hour queries and is free on wide ones.**
Coarser min/max granularity only matters when the window is comparable to a
file's span.

**And `COORDINATOR_HOT_TARGET_BYTES` governs the HOT TAIL — recent data, which is
exactly what short interactive queries read.** So Option A as written would put
its entire cost on the most latency-sensitive path, in a product whose reported
problem was slow queries. That is the wrong trade.

**But the two targets are separate constants:**

```rust
pub(crate) const COORDINATOR_HOT_TARGET_BYTES: i64 = 256 * 1024 * 1024;
const COORDINATOR_SEALED_TARGET_BYTES:        i64 = 256 * 1024 * 1024;
```

**So raise the SEALED target and leave the hot tail alone.** Sealed partitions
are read by wide-window queries, where the pruning cost is **1.03-1.05x** —
negligible — and they hold the bulk of the data, so that is where the fan-in
benefit mostly lives anyway.

### The revised proposal

```
COORDINATOR_SEALED_TARGET_BYTES   256 MiB -> 512 MiB    (fan-in on the bulk)
COORDINATOR_HOT_TARGET_BYTES      unchanged             (protects 1h queries)
timefusion_dedup_bin_minutes      unchanged             (no schedule conflict)
```

**One constant, no deadline conflict, no dedup cadence loss, and the pruning cost
lands on wide queries at 1.03x rather than on interactive ones at 2.6x.**

**What is still unmeasured:** how much of the fan-in benefit is actually in the
sealed lane rather than hot packing. `SealedConsolidation` and `HotPacking` are
separate operations with separate pending counts (94 and 16 tonight), which
suggests sealed carries the larger share — but the 5.3x unit-count reduction was
measured across Pack as a whole, not split by lane. **That split is the next
measurement, and it decides how much of the 10x path this variant delivers.**

## RETRACTION: the lane split I used all night was from a PARTIAL instrument

`work.<Op>.worker_secs` covers every coordinator unit. The phase timers only fire
for units that reach `stage_hot_bin` / `stage_dedup_chunk`. **I built the lane
shares from the phase timers, and they are biased.** The authoritative split, at
1,837 s uptime:

| lane | worker-secs | share |
|---|---:|---:|
| **Dedup** | 2,087 | **46.7%** |
| **Repair** | 1,245 | **27.8%** |
| SealedConsolidation | 729 | 16.3% |
| HotPacking | 216 | 4.8% |
| BaseRollup | 175 | 3.9% |
| DerivedRollup | 21 | 0.5% |

**Pack (Hot + Sealed) is 21.1%, not the 85.3% this document has used since the
"quiet hour" section. Dedup is 46.7%, not 13.4%.** The two lanes I concluded
were minor — dedup and repair — are together **74.5%** of maintenance.

**What this does to the conclusions built on the wrong split:**

| claim | status |
|---|---|
| packer floor fires, fan-in 2-3 -> 9, cuts Pack write volume | **stands** — measured directly |
| guard gives 5.3x backlog reduction at 10x | **stands** — simulated on unit counts, not lane shares |
| "Pack write is ~51% of maintenance, so the floor saves 33%" | **WRONG** — Pack is 21%, so the worker-time saving is ~12% |
| "raise the sealed target" is the cheap 10x lever | **WEAKENED** — it touches 16.3%, not 85% |
| dedup/bin-width ranks last | **WRONG** — dedup is the largest lane at 46.7% |

**The bin-width lever I dismissed at ~3.7% was priced against a 13.4% dedup
share. At 46.7% the same 5.1x read reduction is worth ~3.5x more than I said.**

**Why this happened, and it is the session's recurring error in its purest form:**
I had two instruments measuring the same thing and used the one that was easier
to aggregate, without checking its coverage. `work.*.worker_secs` was in every
`timefusion_stats` dump I read all night.

**What is still true and load-bearing:** the packer floor is deployed, firing,
and measurably raises fan-in — that was never a share-dependent claim. What is
now open is whether it, or dedup work, is the better target. **On the corrected
split, dedup is.**

---

## Following the corrected split into the dedup lane: 64% of probe work is thrown away at a deadline

The retraction above said dedup is the lane to attack. This is what attacking it
found, and it is not the bin-width lever I expected.

### First: bin widening is already implemented, and it has converged

`coarsen_sealed_slices` fuses sealed Dedup/BaseRollup/DerivedRollup/HotPacking
units up the ladder `10min -> 1h -> 6h -> 1day` (`SUBSUME_WIDTHS`), gated on the
fused unit fitting `MAX_DECODED_BYTES` — with a partition-ceiling escape for
Dedup specifically, added after the 2026-09-01 stall. Prod over six hours:

```
subsumed=96 fused=13 candidates=226 blocked=188 over_budget=25
subsumed=80 fused=4  candidates=198 blocked=169 over_budget=25
subsumed=64 fused=0  candidates=194 blocked=169 over_budget=25
...
subsumed=0  fused=0  candidates=194 blocked=169 over_budget=25
```

`subsumed` drains to zero and `fused` follows. **`over_budget` is flat at 25 —
the fix that shipped worked, and coarsening is now converged, not stalled.** So
"widen the dedup bin" is not an available lever on sealed work: it already
happens. It remains unavailable on live-frontier work, which `coarsenable`
excludes by design.

That closes the question the retraction opened, and closes it against the lever
I had just promoted. The real cost had to be somewhere else.

### The measurement: the batch probe

`dedup_batch_probe` is the certification/classification scan — a key-only
`GROUP BY` over a whole `(project, date)` that decides whether a partition is
duplicate-free. It is the mechanism the read path depends on: a query only sheds
its `DedupExec` when every date it reads is certified. Six hours of prod:

| event | count |
|---|---:|
| `dedup_batch_probe` (completed) | 40 |
| **`dedup_batch_probe_timeout`** | **70** |

**64% of probes time out.** And the admission side explains why:

| `groups=` | `budget_secs=` | passes |
|---:|---:|---:|
| 32 | 239 | 8 |
| 32 | **0** | 7 |
| 1 | **0** | 14 |
| 3 / 1 | 239 | 4 |

Two independent defects, both in admission:

1. **21 of 33 passes start with zero budget.** They enumerate groups, sort,
   interleave and dispatch against a deadline that has already passed.
2. **A pass that does have a budget admits 32 groups into it regardless.** With
   `rewrite_permits = 10`, the log shows ~15 completions followed by a run of
   ~17 timeouts — one wave finishes and the second wave dies wholesale.

A timed-out probe is not free. Each builds a provider and an eager snapshot over
a whole date's files, holds one of ten heavy rewrite permits for the duration,
and leaves allocator churn jemalloc retains as RSS. And because dirty and
certify-only groups are interleaved 1:1, roughly half the discarded probes are
the certify-only ones — **the only class that can GRANT**, which is the
documented blocker on the read path (`cert_granted_total=0` beside
`dedup_probe_timeouts_total=40`, quoted in the code itself).

### The change

`probe_groups_for_budget(permits, budget, observed, cap)` sizes admission to
what the deadline can finish, from a half-weight EMA of observed probe cost;
`batch_probe_classify` returns immediately when the budget is already zero,
leaving every bin queued for a tick that can afford it. Cold estimate ->
unchanged behaviour, so the first pass measures itself. Floor of one wave, so a
pessimistic estimate cannot wedge classification at zero.

**What it does NOT claim.** This does not make probes faster and does not by
itself certify one extra date. It stops ~17 of every 32 admitted probes from
burning a permit to produce nothing, and it stops the certify-only class from
being crowded out by work that cannot finish. Whether the freed permit-seconds
convert into grants is the thing to measure, and the counters are already in
place: `dedup_probe_timeouts_total` should fall toward zero and
`cert_granted_total` is the outcome that matters. The new `probe_cost_ms` field
on `dedup_batch_probe_start` says what sized each admission, so a wrong estimate
is visible rather than silent.

**Honest limits.** One process, six hours, and the process restarted partway
(`work.*.worker_secs` reset), so I am quoting event COUNTS over the window and
the admission/budget pairs — not rates, not shares. The 159s-per-probe figure
used in the doctest is inferred from 15 completions in 239s at 10 permits, not
directly measured; it is illustrative of the arithmetic, and the EMA measures
the real value at runtime rather than trusting it.

### Where the zero budget comes from, and what the fix does not do

`probe_deadline = min(pass_deadline, now + stage_deadline)`. `budget_secs=0`
therefore means the dedup PASS deadline had already expired before phase 3 was
reached — earlier phases of the tick consumed the whole thing. The logs show it
per-table in sequence (`groups=32 budget=0`, then `groups=1 budget=0` twice),
so it is the tick that is exhausted, not one unlucky table.

**The change makes those passes free; it does not give probes budget.** Why the
tick is exhausted before phase 3 is the next question in this lane, and it is
answerable from the same logs. It is not fixed here.

### What I expect to move, and what I expect NOT to

| signal | baseline (23:12, young process) | expectation |
|---|---:|---|
| `dedup_probe_timeouts_total` | 15 | **falls toward 0** — the primary |
| `dirty_bin_batch_probe_clean_total` | 0 | **must not fall** — the guard against the estimator over-throttling |
| `probe_cost_ms` on `dedup_batch_probe_start` | absent | `> 0`, with `groups` below 32 — proves the mechanism engaged |
| `cert_granted_total` | 0 | **stays 0**, and that is not a failure |

That last row matters. `cert_declined_dirty_bins = 4917` against
`cert_probe_declined = 53` says completed probes decline because the dates are
genuinely dirty — not because they were starved. Certification is blocked by
duplicate REMOVAL, exactly as the 2026-09-01 note concluded. This change frees
permit-seconds in the largest lane; it does not and cannot certify a dirty date.

The failure mode to watch is my own fix over-throttling: an EMA dragged upward
by a few expensive dates would admit one wave forever and classify less than
before. `dirty_bin_batch_probe_clean_total` is the tripwire, and
`probe_cost_ms` says whether the estimate is the reason.

### The next lever, and why it could not be pulled tonight

`TIMEFUSION_LANDED_SKIP_ENABLED` (default **false**) declines a flush whose
batch set is provably already committed — the duplicates WAL replay re-inserts
after an unclean exit, measured at **58% of duplicate groups** in a sampled prod
file. Since dedup exists to remove duplicates and is the largest lane, not
manufacturing them is the largest available reduction in the work itself rather
than in the cost of doing it.

It is not flipped here for a reason that is about measurability, not nerve: **the
skip only fires on a DIRTY boot.** A deploy is SIGTERM → clean shutdown → clean
boot, so flipping the flag tonight would produce a dormant flag and a counter
that reads 0 for the same reason whether it works or not — the exact
never-fired/never-reached ambiguity that has already cost this project a night.
It wants staging, where an unclean restart can be induced, and prod's host is
read-only by standing rule. Direct test coverage is also thin (two tests), which
is fine for a dormant flag and not fine for a durability-adjacent one that is
live.

### Position, stated plainly

Tonight found and fixed measured waste in the largest maintenance lane, and
closed the bin-width question against my own earlier recommendation by showing
the mechanism already exists and has converged. It did **not** produce 10x
headroom, and nothing here should be read as claiming it. The measured position
is unchanged from the 09-04 ceiling note: throughput is sub-linear in unit cost,
so cheaper units cannot reach 10x on their own, and 10x needs the rewrite
envelope to grow or the work to disappear. Landed-skip is the "work disappears"
candidate with the best evidence behind it, and it needs staging.

---

## Testing the thing that actually blocks 10x: is the 6-slot envelope a law?

Every ceiling note tonight ends the same way — throughput is sub-linear in unit
cost, so 10x needs the rewrite envelope to GROW or the work to disappear. The
envelope has never been tested. It is worth testing before another lever.

### Where the 6 comes from

`light_optimize_k = coordinator_share_bytes / COORDINATOR_PER_SORT_BUDGET_BYTES
- repair_pool_holdback_slices`. On prod: `coordinator_share = min(16 jobs x 512
MiB, maintenance_pool x 3/5) = min(8192, 10178) = **8192 MiB**`, so
`8192 / 1280 - 2 = 4` light `+ 2` repair `= 6`.

The 6 traces to one bench ladder at an **8 GiB pool**:

```
6 workers  33.32 MB/s  0 failed   <- best
8 workers  15.07 MB/s  4 FAILED   <- cliff
```

and the cliff is explained in the code itself as a RATIO, not a count:
`SAFE_DECODED_PER_POOL_BYTE = 1.79` — 6 x 2,451 MB decoded / 8 GiB = 1.79 passes,
8 x = 2.39 fails.

**If the cliff is a ratio, the envelope is a pool-allocation decision. If it is a
count, it is a law and 10x needs a different mechanism entirely.** Those two
predict different things and the difference is measurable.

### The prediction, written BEFORE the run

Last passing rung `= pool_GiB x 1024 x 1.79 / 2451`:

| pool | predicted last passing | predicted first failing |
|---:|---:|---:|
| 2 GiB | 1 | 2 |
| 4 GiB | 2 | 4 |
| 6 GiB | 4 | 5 |
| 8 GiB | 5-6 | 8 |

The 8 GiB row must reproduce the original ladder (6 pass, 8 fail) or the rig is
not measuring the same thing. Every rung is run at every pool, and the cliff rung
is repeated — this box is noisy enough that a single failure is not a cliff.

### Stated limits, before any result

- **Testing DOWNWARD.** 32 GB of RAM cannot honestly host a 16 GiB pool, so this
  establishes linearity from below. Confirmation would prove the cliff is
  pool-priced **within 2-8 GiB**. It would NOT prove 26 sorts work at a 36 GiB
  pool — up there, spill-disk bandwidth shared across workers is a plausible new
  binding constraint these rungs never touch.
- **Disk is the confound.** A spill failure and a memory cliff are the same
  entry in the `failed` column, and this box was at 97% (34 GiB) before I freed
  `target/` to 68 GiB. `df` is recorded around every rung; a rung whose failure
  coincides with a disk drop is not evidence.

### A correction to make before the prod half of this argument

I was about to write "permits bind while the coordinator pool sits at 15%". That
crosses a boundary I have already been wrong across five times tonight:
`coordinator_pool_pct = 15` is INSTANTANEOUS, `compaction_permits_unavailable =
32` is CUMULATIVE — the pool may have been full during every one of those waits.

Worse, and this is the third instance of the same defect tonight: **that counter
is incremented from TWO different semaphores** — `light_rewrite_sem`
(HotPacking/SealedConsolidation) and the byte-priced `repair_rewrite_sem` —
and **neither is the coordinator pool**. Same shape as the packer floor's two
packers and `footer_repair_files_per_pass`'s two repair lanes. The envelope is
still what those permits express, so the question stands; the 15% is not evidence
for it and is not quoted as such.

### The first run was a BROKEN PROBE, and the reason matters

The ladder returned **0 failures in every cell** — 8 workers passing at a 2 GiB
pool, 4.8x past `SAFE_DECODED_PER_POOL_BYTE`. That is not a result. By the
criterion written above ("the 8 GiB row must reproduce 6 pass / 8 fail or the rig
is not measuring the same thing"), the rig was invalid and the prediction is
neither confirmed nor refuted.

**The cause is worth more than the run.** `prod204.parquet` is a file our own
rewrite wrote, so its footer carries `sorting_columns` **exactly equal to the
bench's ORDER BY**:

```
SortingColumn(column_index=0,  descending=True,  nulls_first=True)   # timestamp
SortingColumn(column_index=82, ...)  # resource___service___name
SortingColumn(column_index=2,  ...)  # id
SortingColumn(column_index=9,  ...)  # level
SortingColumn(column_index=7,  ...)  # status_code
```

DataFusion reads that, declares the scan already ordered, and **elides the sort**.
The ladder degenerated into a scan benchmark. 183 MB/s per worker against the
original ladder's 29 MB/s — a 6x discrepancy I would have had to explain away to
believe the result.

So: **the bench that sizes the entire maintenance envelope — the 6 slots,
`COORDINATOR_PER_SORT_BUDGET_BYTES = 1.25 GiB`, `SAFE_DECODED_PER_POOL_BYTE =
1.79`, `REPAIR_REWRITE_TARGET_FILES = 2` — silently stops measuring a sort when
handed a current prod file.** Every one of those constants rests on a fixture
that can no longer be obtained the obvious way, and the failure is silent in the
direction that says "you have lots of headroom".

Two changes so this cannot recur:

1. `one_rewrite` now renders the physical plan and **fails loudly** if there is
   no `SortExec` — "the fixture is ALREADY SORTED, so no sort ran". A ladder with
   no failures anywhere is a broken probe; this makes it say so.
2. The fixture is a row-shuffled copy written without `sorting_columns`
   (2,023,604 rows, 98 columns, 0.51 GB uncompressed).

**The hypothesis is re-stated so it does not depend on an absolute decoded size**
(the shuffled fixture is not the original 2,451 MB/worker file, so the earlier
prediction table no longer applies): measure the cliff rung at 8 GiB, call it
`W8`, then linearity predicts `cliff(pool) = W8 x pool / 8`. Pools 2 / 4 / 8 GiB,
rungs 1-24. A cliff that does not move with the pool refutes it.

### RESULT: the 6-slot envelope is NOT a law — the cliff is pool-priced

Fixture: 400,000 rows x 98 columns, row-shuffled, no footer `sorting_columns`
(so the sort actually runs — 30 MB/s at one worker, against the original
ladder's 29). Per-rung spill directory. **Free disk held at 99 GiB across every
rung of every pool**, so no failure in this table is an ENOSPC.

| pool | last passing rung | first failing rung |
|---:|---:|---:|
| 512 MB | **2** | 3 |
| 1 GiB | **4** | 6 |
| 2 GiB | **12** | 16 |
| 4 GiB | **20** | 24 |

**The cliff moves 2 -> 20 across an 8x pool range.** The prediction written
before the run was strict proportionality (2, 4, 8, 16); measured is 2, 4, 12,
20 — proportional at the low end and better than proportional above 1 GiB.
Either reading refutes the same thing: **the concurrency ceiling is a function of
pool size, not a fixed count of six.** `SAFE_DECODED_PER_POOL_BYTE`'s framing —
a decoded-bytes-per-pool-byte RATIO — is the correct model, and the "8 workers"
in the original ladder was that ratio evaluated at 8 GiB, not a scheduling law.

That removes the assumption every ceiling note tonight rested on: *"10x needs the
envelope to grow or the work to disappear, and the envelope is fixed."* The
envelope is not fixed. It is a pool-allocation decision.

### But this bench CANNOT say raising it raises throughput, and that matters

Aggregate throughput saturates well before the cliff, at every pool:

```
2 GiB:   6w 94.15   8w 91.94   10w 92.59   12w 70.44  MB/s total
4 GiB:   8w 91.47  10w 90.94   16w 77.22   20w 88.84  MB/s total
```

~90 MB/s is the ceiling on this 10-core box (1 worker is 46), and going from 6
workers to 20 buys nothing. **On this machine the rewrite is CPU-bound, so extra
concurrency only adds in-flight units, not bytes per second.**

Prod is the opposite case and that is exactly why CLAUDE.md's ladder says local
MinIO "validates correctness, not cost": prod's per-unit cost is dominated by
object-store round trips, and concurrency is what hides IO latency. So:

- **What is now established:** the ceiling scales with the pool. Verified over
  512 MB - 4 GiB. Raising the envelope is a legitimate move rather than one that
  walks into a known cliff.
- **What is NOT established:** that raising it increases prod throughput. This
  box cannot show that, and the honest place to measure it is staging, where
  units are IO-bound and restarts are free.
- **What is still unknown at the top:** at a 36 GiB pool, shared spill-disk
  bandwidth is a plausible new binding constraint these rungs never reach.

### The prod side of the argument, corrected

`coordinator_share_bytes = min(16 jobs x 512 MiB, maintenance_pool x 3/5) =
min(8192, 10178) = 8192 MiB` — the coordinator pool is capped by the JOB term,
not by the maintenance pool, and it lands exactly on the 8 GiB the original
ladder used. Prod RSS is 29 GB against a 120 GB cgroup, and
`light_optimize_memory_brakes_total` has never fired.

I am NOT quoting `coordinator_pool_pct = 15` as evidence that permits bind while
memory idles — that reads an instantaneous gauge against a cumulative counter,
and (third time tonight) `compaction_permits_unavailable` is incremented from two
different semaphores, neither of which is the coordinator pool.

**The concrete next step, one knob and one deploy:** raise the
`coordinator_jobs` cap so `coordinator_share_bytes` reaches its
`maintenance_pool x 3/5` ceiling of 10,178 MiB, which takes `light_optimize_k`
from 4 to 6 and the envelope from 6 to 8 — a 1.25x within a range this ladder
has now measured, not a leap. Validate on staging first, because the thing that
must move is IO-bound throughput and this box cannot produce that number.

---

## Post-deploy: the probe admission change, measured

Prod on `9cc12bf`. Twelve minutes of the new process:

| | old build, 6 h | new build, 12 min |
|---|---:|---:|
| `dedup_batch_probe` completed | 40 | **59** |
| `dedup_batch_probe_timeout` | 70 | **12** |
| completion rate | **36%** | **83%** |

The admission decisions are visible and sane:

```
groups=10  budget_secs=239  probe_cost_ms=0        <- cold, and only 10 groups existed
groups=10  budget_secs=239  probe_cost_ms=226882   <- 227 s a probe -> floor of one wave
groups=32  budget_secs=204  probe_cost_ms=22566    <- 23 s a probe -> cap
groups=32  budget_secs=239  probe_cost_ms=61770    <- 62 s a probe -> cap
groups=7   budget_secs=239  probe_cost_ms=416      <- only 7 groups existed
```

Both directions work: an expensive-probe regime throttles to one wave, a cheap
one still fills the cap. The estimator spans 416 ms to 227 s across real
partitions, which is the order-of-magnitude spread the half-weight EMA was
chosen for.

**Caveat, and it is not small.** These are different window lengths against
different process ages — 12 minutes on a young process versus 6 hours on a
mature one. The RATIO is the comparable quantity, and it is 36% -> 83%; the raw
counts are not. A second reading on a quieter, older process is what would make
this a rate rather than a ratio.

### A prediction I got wrong, in the favourable direction

I wrote above: "`cert_granted_total` stays 0, and that is not a failure." It is
**11**, where the previous process granted 0 across its entire life.

I underweighted my own argument. Dirty and certify-only groups interleave 1:1,
and the certify-only class is the only one that can grant — so when the second
wave was dying wholesale, roughly half of what died was the only work capable of
producing a grant. Freeing that capacity let grants happen. I reasoned correctly
about the mechanism and then predicted as if it did not exist, because
`cert_declined_dirty_bins = 4917` had convinced me the outcome was
removal-blocked in all cases.

It is still removal-blocked for DIRTY dates — that part stands. What was wrong
was concluding every candidate date was dirty. Some were merely never examined.

**This does not change the caveat about 10x.** Eleven grants against
`cert_slice_files_unproven = 3507` is a start, not a solved read path.

## 00:20 — A PUSH FREEZE, because I was the one destroying the measurement

`docker service ps` shows the deployed images: `cdae044` (2 min), `15895cb`
(2 min), `9cc12bf` (15 min), `2f08c4a` (31 min), `9a112b5` (~1 h). Those are all
MY pushes. `deploy.yml` fires on every non-docs push to master, and each deploy
replaces the container.

Heavy maintenance units run ~21 minutes. At a ~15-minute deploy cadence they
cannot finish — they die to process exit, every time. Shorter units complete
fine, so the completion counters are not zero and the effect is invisible in
them; it is the long tail (repair, whole-date dedup on the big table) that never
lands. This is `tf_deploy_cadence_starves_dedup_2026-08-18` reproduced by my own
hand, and it is also why `cert_granted_total` has never been measurable: grants
need sustained uptime and no process tonight has had it.

**FREEZE: 2026-09-05T00:19:56Z.** No code pushes to master until a reading is
taken at uptime >= 2h. `cdae044` already carries the probe-admission fix, so
nothing needs to ship for that reading to be valid. `deploy.yml` `paths-ignore`
covers `docs/**`, `**/*.md`, `bench/**`, so docs pushes are free; code commits
are batched locally and pushed together after the reading.

### Pre-registered decision gate (written BEFORE the reading)

On a process with >= 2h uptime:

- If `cert_granted_total` accumulates at a steady rate and
  `cert_slice_files_proved` climbs against `unproven`, the shipped admission fix
  suffices and no cert-lane change ships.
- If grants PLATEAU while `dedup_probe_timeouts_total` stays low, probes are
  finishing but there is no budget left to run them — budget starvation, not
  probe death — and the per-table bound below is the fix.
- `dirty_bin_batch_probe_clean_total` (baseline 0) is the tripwire for the new
  estimator over-throttling.

The current process reads `cert_granted_total = 0` at 124 s uptime. That is
noise, not a result. Single-digit counters off young processes have already
produced two retractions this week.

## 00:35 — the shared probe-cost EMA was sized by the wrong table (3bd3c222)

I inferred that certification was eating the dedup tick, because
`run_certification_pass` is the newest consumer, runs FIRST in the per-table
loop, and is handed `sweep_deadline` — the whole tick — while the drain and
sweep split 0.6/1.0 between them. That inversion is real and worth remembering.

**One tick of prod logs refuted it as the explanation.** Tick at 00:20:00,
budget 239 s:

| time | table | groups | probe_cost_ms | budget left |
|---|---|---:|---:|---:|
| :00.67 | rollup_dashboard_1h_v2 | 11 | 0 | 239 |
| :01.61 | rollup_dashboard_1m_v3 | 9 | 482 | 239 |
| :03.11 | metrics_rollup_1h_v2 | 5 | 1129 | 237 |
| :03.43 | **otel_logs_and_spans** | **32** | **147** | 237 |
| :22:19 | otel_metrics | 31 | 32118 | **100** |

Certification on the rollup tables costs 1-2 s each. The cost is the
**137-second gap** on `otel_logs_and_spans`: one table, 57% of the tick.

And the reason it was admitted 32 groups is a defect in the fix I shipped four
hours ago. `dedup_probe_cost_ms` was ONE `AtomicU64` for the whole database. At
00:15 it held 226,882 ms — measured on `otel_logs_and_spans` itself — and
correctly throttled that table to the 10-group floor. By 00:20 three cheap
rollup probes had pulled the shared EMA to 147 ms, and the same table under the
same conditions was admitted at the full cap. **The estimator was sized by other
tables' work, and biased toward over-admitting on the one table every dashboard
reads.**

Now keyed per table. `note_probe_cost_into` is a free function with a doctest
that replays exactly this sequence and asserts the big table's estimate survives
three cheap probes on another table. Because the doctest reads the estimate by
table name, a return to shared state cannot compile against it.

Committed locally, NOT pushed — see the freeze above.

## 01:15 — three wrong env knobs in CLAUDE.md, found by nearly reasoning from one

Following the "today's partition holds ~467 files vs 2-5 on sealed dates" lead, I
went to read `TIMEFUSION_LIGHT_OPTIMIZE_HOT_HOURS` — documented in CLAUDE.md with
a default of 3 and a described behaviour ("hot-tail compaction only bin-packs
today's sub-target files modified within this window; sealed files are the dedup
cron's job"). **It does not exist anywhere in `src/`.** It was introduced in
`8c6a32f3` and reverted the same day in `515a37b6`, 2026-07-19. The doc has
described a live knob for ~7 weeks. I was one step from building an argument on
it.

Sweeping every `TIMEFUSION_*` name in CLAUDE.md against `src/` found two more:

| documented | actual | effect if set |
|---|---|---|
| `TIMEFUSION_BUFFER_FLUSH_INTERVAL_SECS` | `TIMEFUSION_FLUSH_INTERVAL_SECS` | parses as nothing, default silently kept |
| `TIMEFUSION_BUFFER_FLUSH_IMMEDIATELY` | `TIMEFUSION_FLUSH_IMMEDIATELY` | same |

`envy` derives each env name from the STRUCT FIELD name, so the `BUFFER_` segment
exists only where the field carries it — `timefusion_buffer_retention_mins` and
`timefusion_buffer_max_memory_mb` do (those two entries were correct);
`timefusion_flush_interval_secs` does not. **Nothing is misconfigured**: `.env`
line 70 has always used `TIMEFUSION_FLUSH_INTERVAL_SECS=300`, the correct
spelling. The defect is documentation-only — but CLAUDE.md is what a session
reads first, and a knob that parses as nothing fails silently in the direction of
"I set it, so it is on".

Corrected locally. **NOTE FOR THE MORNING: `CLAUDE.md` is untracked here** — it
is ignored by the user's global `~/.gitignore` (line 37), so this fix cannot be
committed and will not reach anyone else's checkout. Worth deciding whether the
project's own instructions should be excluded from the project's history.

This is the same shape as "capabilities described by COMMENTS all failed"
(2026-09-04), extended one level out: capabilities described by the PROJECT
INSTRUCTIONS also fail. Read the code before quoting the doc.

### Status at the freeze midpoint

Full suite **1388/1388 green** on the frozen commits, so they are push-ready the
moment the reading lands. CI on master is green including E2E — the previously
red gate is resolved. No prod queries during the freeze: heavy ad-hoc SELECTs can
OOM the memory-tight instance, and an OOM restart would destroy the very window
being protected.

## 00:44 — INTERIM liveness check (uptime 1661s). NOT the gate reading.

The gate is 2h uptime (~02:17Z); this is 28 min in. Recorded because one counter
changed KIND, not degree:

| counter | prior | 00:44 |
|---|---:|---:|
| `dedup_skipped` | **0 in every reading ever taken** | **24** |
| `cert_granted_total` | 0 across whole process lifetimes | 23 |
| `cert_slice_files_proved` / `unproven` | 14 / 340 | 56 / 1360 |
| `dedup_denied_never_certified_pct` | 94.8 | 86.6 |
| `dirty_bin_batch_probe_clean_total` | 0 | 0 (tripwire clean) |
| `pending_dedup` | 2292 | 2275 |

`dedup_skipped` moving off zero is the READ-PATH SKIP FIRING FOR THE FIRST TIME.
`tf_certification_probes_starved_2026-09-04` recorded it as 0 in every reading
and concluded "the customer chain is NOT unblocked". It is now firing.

**What this does and does not support.** Zero-to-nonzero is an EXISTENCE claim —
the mechanism can fire, which a 28-minute process can establish. It is NOT a
rate, and 24 skips against `unproven = 1360` is not a solved read path. Every
prior attempt to read direction from young-process counters here has been
retracted; the 2h gate stands unchanged.

`pending_dedup` down 17 in 28 min is NOT evidence of draining — it is inside the
run-to-run noise this queue has always shown.

## 01:15 — defining the gate reading before taking it

Uptime 3261 s (54 min). Gate is 7200 s (~02:17Z). Deltas over 26 min:

| counter | 00:44 | 01:10 |
|---|---:|---:|
| `cert_granted_total` | 23 | 30 |
| `cert_slice_files_proved` | 56 | 250 |
| `cert_slice_files_unproven` | 1360 | 1874 |
| `dedup_skipped` | 24 | **24 (flat)** |
| `pending_dedup` | 2275 | **2364 (grew)** |
| `dedup_probe_timeouts_total` | 34 | 50 |

**`unproven` growing is NOT coverage going backward.** A 54-minute process is
still ENUMERATING its 14-day window (`CERTIFY_WINDOW_DAYS = 14`,
`uncertified_window_dates`), so the denominator is being DISCOVERED, not created.
The comparable quantity is the ratio: `proved/(proved+unproven)` went
**4.0% -> 11.8%**. Quote the ratio; the absolutes are not comparable at this age.

**The read path, full surface at 01:10** — this is the number that matters for
the customer query goal:

| scan outcome | count | share of eligible |
|---|---:|---:|
| `dedup_eligible` | 2395 | — |
| `dedup_skipped` | 24 | **1.0%** |
| `dedup_denied_never_certified` | 1919 | **89.9%** |
| `dedup_denied_fp_moved` | 215 | 9.0% |
| `dedup_denied_no_window` | 236 | — |

So the skip mechanism works and coverage is the constraint, at ~90% — which is
exactly what `tf_certification_coverage_is_the_blocker_2026-09-01` said. Grants
run ~33/hr. **`dedup_skipped` being FLAT at 24 across 26 minutes is the honest
counterweight to last hour's "it fires for the first time".** It fired, then
stopped.

### Counter semantics, pinned so the gate reading is not improvised

I nearly retracted the 36% -> 83% probe result on a suspected denominator defect.
Checked the code instead:

- `event="dedup_batch_probe_start"` — logged once per PASS, carries `groups=N`.
  **Not a probe count.** My first grep counted these (23) and they do not
  compare to `dedup_probe_timeouts_total` (50).
- `event="dedup_batch_probe"` (maintain.rs:6324) — logged on EVERY successful
  probe, dirty and certify-only alike (a certify-only group logs `queued=0`).
  **This is the completion counter, and it is the right denominator.**
- `event="dedup_batch_probe_timeout"` (maintain.rs:6398) — per probe, and the
  only thing incrementing `dedup_probe_timeouts_total`.

So completion rate = `dedup_batch_probe` / (`dedup_batch_probe` + `_timeout`),
both from logs over one process's life. The earlier 83% used exactly this pair
and stands.

## 01:45 — the file-level skip has fired ZERO times, and overlap is why

Uptime 5064 s. Grants have PLATEAUED: `cert_granted_total` +1 in 31 min, against
+7 in the 26 min before. My pre-registered gate assumed a clean split (plateau +
LOW timeouts => budget starvation); timeouts kept climbing (+14), so this is the
mixed case the pre-registration did not anticipate. The full certification
surface resolves it, and into something better than either branch:

| counter | value | against |
|---|---:|---|
| `cert_declined_dirty_bins` | **12,407** | 31 grants — **400:1** |
| `cert_skip_blocked_overlap` | **4,264** | — |
| **`cert_skip_files`** | **0** | — |
| `dedup_skipped_per_file` | **0** | `dedup_skipped_per_date` = 1 |
| `cert_slice_day_covered` / `partial` | 3 / 26 | slices rarely cover a day |
| `cert_refused_dropped` | 20 | = dedup did work, refused by construction |

**The per-FILE certification skip has fired exactly ZERO times in this process.**
Every one of the 25 `dedup_skipped` came from the per-DATE path. `skippable_certified_files`
(`src/read/mod.rs:200`) partitions certified files by whether any uncertified file
OVERLAPS them in time, and 4,264 fell on the blocked side while 0 fell on the
skippable side.

The code's own comment already names the mechanism: *"overlap says certification
is too SPARSE (a certified file still has an uncertified neighbour, so contiguous
runs are what pay)"*. This is `tf_cert_works_contiguity_blocks_2026-08-22`
measured again, a fortnight on, at a harder ratio.

### Why this matters for the goal, and it joins the two halves

The chain is now measured end to end:

1. Compaction MERGES files, and merging UNIONS their time ranges
   (`tf_compaction_and_dedup_fight_2026-09-04`) — the packer scores COUNT and
   BYTES, never SPAN.
2. Wider spans overlap more neighbours.
3. More overlap => `skippable_certified_files` blocks => `cert_skip_files = 0`.
4. No skip => every wide query pays full `DedupExec` => 30 d sits at ~45 s against
   a 60 s cap (`tf_dedup_is_single_threaded_2026-09-04`).

So **bounding file SPAN is not only a maintenance-cost lever, it is the unlock for
the read-path skip** — the same fix serves "maintenance keeps up" and "customer
queries stop timing out". That is a stronger reason to bound span than the
maintenance-cost argument alone, which measured only 0.007%.

**Two independent constraints, not one.** Certification PRODUCTION is blocked by
dirty bins (12,407 declines); certification CONSUMPTION is blocked by overlap
(4,264). Raising grants alone would not move `cert_skip_files` off zero while
contiguity fails — the two need separate fixes, and only the second is on the
query-latency path.

**Still not the gate reading** (7200 s, ~02:17Z). Recorded now because it is a
structural fact about zero, not a rate off a young process.

## 02:11 — THE GATE READING (uptime 6862 s / 114 min, quiet process)

| counter | 01:41 | **gate** | Δ/30 min |
|---|---:|---:|---:|
| `cert_granted_total` | 31 | **31** | **0 — fully flat** |
| `cert_skip_files` | 0 | **0** | 0 |
| `cert_skip_blocked_overlap` | 4264 | 4328 | +64 |
| `dedup_skipped` / `dedup_eligible` | 25 / 3590 | 25 / 5058 | **0.49%** |
| `dedup_probe_timeouts_total` | 64 | 66 | **+2** (was +14) |
| `dirty_bin_batch_probe_clean_total` | 0 | **0** | 0 |
| `cert_declined_dirty_bins` | 12,407 | 12,415 | +8 |
| `pending_dedup` | 2344 | 2398 | +54 |

### Verdict against the gate as WRITTEN

- **Branch A** ("grants accumulate at a steady rate ... the shipped admission fix
  suffices"): **REFUTED.** Grants flat at 31 for 30 minutes.
- **Branch B** ("grants PLATEAU while timeouts stay LOW => budget starvation, and
  the per-table cert bound ships"): matches on its face — timeouts fell to +2.
- **Tripwire** (`dirty_bin_batch_probe_clean_total`, baseline 0): **0 across the
  whole 114 minutes.** The new admission estimator is not over-throttling.

### Why I am NOT executing branch B's prescription

Branch B's fix raises GRANTS. `cert_skip_files` has been **0 for 114 minutes**
against `cert_skip_blocked_overlap = 4328`, so grants are not what the read path
is short of — contiguity is. **The gate was written before the two-constraint
structure was understood** (production blocked by dirty bins; consumption blocked
by overlap), and it offered no cell for "grants plateau AND the skip is
structurally blocked". Following it mechanically would have shipped a fix for the
constraint that is not binding.

Recording this rather than quietly re-deciding: a pre-registration is there to
stop motivated reasoning, not to override a fact discovered after it was written.
What it correctly prevented was reading the 0→24 `dedup_skipped` blip as success.

### What IS established

1. **The probe admission fix works.** Timeouts decayed to +2/30 min and the
   over-throttle tripwire never fired. Shipped `cdae044`, verified over 114
   quiet minutes.
2. **The read path is NOT unblocked.** 25 skips on 5058 eligible scans = 0.49%,
   all per-DATE; the per-FILE skip is at zero.
3. **Maintenance is still not keeping up.** `pending_dedup` 2275 -> 2398 over 90
   quiet minutes, with no deploy to blame this time.

### Freeze lifted

Six commits pushed together — one code change (`3bd3c222`, per-table probe-cost
EMA) and five docs. One deploy, one attributable change.

## 02:20 — the grant plateau is EXHAUSTION, and the span_cap case (do NOT enable tonight)

### Correcting my own gate verdict

I attributed the grant plateau to "the wrong constraint being fixed". The
mechanism is cleaner and it is verifiable: the **decline memo** (maintain.rs
~6355) memoizes every dirty date against its file set and never re-probes it, so
a process consumes its 14-day candidate window and then has nothing left to
probe. Between 01:41 and 02:14:

| counter | 01:41 | 02:14 |
|---|---:|---:|
| `cert_granted_total` | 31 | 31 (+0) |
| `cert_declined_dirty_bins` | 12,415 | **12,415 (+0)** |
| `cert_probe_declined` | 198 | 200 (+2) |

**ALL certification activity stopped together.** That is exhaustion of the
candidate set, not budget starvation — so branch B's fix (bounding certification
per table to free budget) would have bought nothing, and for a second, sharper
reason than the one I gave. Neither pre-registered branch covers this cell.

### The span_cap case — stronger than I thought, and NOT ready to enable

`timefusion_compaction_span_budget_bins` already exists (mod.rs:8008-8032) and
defaults to **0 = disabled**. I had read only the tail of its doc comment; the
full comment carries a MEASURED distribution, prod 2026-09-04, 297 units/90 min:

| lane | n | p50 span | max | <=16 bins |
|---|---:|---:|---:|---:|
| `HotPacking` | 118 | 13 | 20 | 62.7% |
| `SealedConsolidation` | 179 | **84** | **144** | **4.5%** |

At 10-minute bins, SealedConsolidation's p50 output spans **14 hours** — 58% of a
144-bin day partition, in ONE file. A certified file in that day overlaps it
unless it falls in the remaining 42%, which is a very plausible source of the
4,328 overlap blocks. The doc already answers the VALUE question (~20-24 spares
hot packing at max-observed 20, rejects the bulk of sealed consolidation); it
leaves the TRADE open, calling it "real and unpriced".

**What my overlap finding adds to that trade.** The doc weighs "file-count win
given up" against "read amplification stopped". There is a THIRD term it does not
consider: span is also what blocks `skippable_certified_files`, so bounding it is
on the path to moving `cert_skip_files` off zero — the customer query goal, not
just a maintenance-cost goal. That is a materially bigger prize than read
amplification, whose own lever measured ~3.7%.

**Three reasons not to enable it tonight, one of them disqualifying:**

1. **It is PROSPECTIVE ONLY.** It shapes files the packer creates from now on.
   The 4,328 blocks are EXISTING files; the counter cannot move until the wide
   mass is rewritten (`optimize --date D --recompress`) or ages out. Enabling it
   and watching `cert_skip_files` would show nothing and read as a refutation.
   The correct order is recompress FIRST, then bound span.
2. **The discriminating fact is missing.** `cert_slice_day_covered = 3` vs
   `cert_slice_partial = 49` leaves open that the blocking is certification
   INTERLEAVING within days rather than wide files. If interleaving dominates,
   span_cap moves `cert_skip_files` by zero and certifying contiguous WHOLE DAYS
   is the real lever. Classify the 4,328 by whether the blocking uncertified file
   is WIDE or merely ADJACENT — a Delta checkpoint computation, offline, no prod
   load. **Run that before spending a deploy on span_cap.**
3. **It disables a lane.** Any biting bound rejects ~95% of SealedConsolidation,
   which cannot pick narrower inputs — 16.3% of worker time, 108 pending. The
   right shape is probably PER-LANE (bound hot packing, exempt sealed
   consolidation), which is `tf_lane_coverage_gaps_2026-09-04` run in reverse.

### Discipline: no more code pushes tonight

The push at 02:11 should be the last. Any further default change is a new deploy
and a new process, and a quiet process running to morning is a 4-6 HOUR reading —
the rate data every retraction this week was missing. That is worth more than
landing one more knob.

## 02:45 — the per-table EMA fix VERIFIED in prod (`2602a6d`, uptime 998 s)

The predicted signature, and it is unambiguous. TWO TABLES IN ONE TICK carrying
DIFFERENT estimates — structurally impossible under the shared `AtomicU64`:

| time | table | `probe_cost_ms` | groups |
|---|---|---:|---:|
| 02:30:00 | `otel_metrics` | 52,822 | 6 |
| 02:30:58 | `otel_logs_and_spans` | 92,643 | 19 |

And the per-table series for `otel_logs_and_spans` alone, learning its own cost
and throttling monotonically as it does:

| time | `probe_cost_ms` | groups |
|---|---:|---:|
| 02:26 | 0 (cold) | 32 |
| 02:30 | 92,643 | 19 |
| 02:35 | 137,855 | 17 |
| 02:40 | 206,016 | 11 |

**Contrast with the defect this replaced:** on the old build the same table sat at
`probe_cost_ms=147` — dragged there by three cheap rollup probes — and was
admitted the full cap of 32, whose wave then ate 137 s of a 239 s tick. It now
converges to ~206,000 ms, which is its REAL cost, and takes 11.

**Timeout rate, at comparable process age:**

| build | timeouts | uptime | rate |
|---|---:|---:|---:|
| `9cc12bf` (shared EMA) | 34 | 1661 s | 1.23/min |
| `2602a6d` (per-table) | 4 | 998 s | **0.24/min** |

**~5x fewer probe timeouts**, with `dirty_bin_batch_probe_clean_total` still **0**
— the estimator is not over-throttling. Both are young processes, so this is a
rate comparison at similar age, not a steady-state claim; the process now runs
quiet to morning and that will give the steady-state figure.

This closes the loop on the night's one code change.

## 03:05 — THE SPAN_CAP CASE IS REFUTED, by the computation that was meant to support it

Offline Delta-checkpoint audit (`v522002`, 25,614 rows), partition
`date=2026-09-02`, 596 live files, all with timestamp stats. Read-only against
object storage; no prod DB load. Checkpoint downloaded WHOLE before reading —
OVH breaks pyarrow's ranged footer reads
(`tf_ovh_checksum_breaks_pyarrow_footers_2026-08-19`).

**First cut was wrong and I caught it:** I filtered on `date=` alone, mixing all
11 projects, and got "p50 overlap 23". The partition key is
`[project_id, date]`, so files in different projects never overlap in a scan.
Corrected, per project:

| project | n | p50 span | p90 span | max | p50 overlap | overlap=0 | drop >p90 -> overlap=0 |
|---|---:|---:|---:|---:|---:|---:|---:|
| 87576849 | 201 | 1 | 3 | 112 | 6 | 4.0% | 18.2% |
| **28f62f01** (whale) | 153 | 1 | 3 | 138 | 5 | **0.0%** | **1.4%** |
| 00000000 | 79 | 1 | 3 | 133 | 3 | 1.3% | 5.6% |
| dcad860a | 46 | 1 | 2 | 128 | 3 | 0.0% | **0.0%** |
| 98fdd4f3 | 44 | 1 | 2 | 82 | 3 | 0.0% | 5.1% |
| be87ebc1 | 36 | 1 | 2 | 59 | 2 | 0.0% | **0.0%** |
| 6297304f | 30 | 1 | **78** | 101 | 2 | 0.0% | **66.7%** |

**Files are NARROW.** p50 span is ONE bin; p90 is 2-3. Only 3.2% of the partition
exceeds 20 bins. Yet essentially nothing is independently skippable, and the
counterfactual settles it: **removing every file wider than p90 takes the whale
project from 0.0% to 1.4%**, and two projects from 0.0% to 0.0%.

**So `span_cap` would NOT unblock the read path**, and my 02:20 case for it —
"bounding span is the unlock for the read path" — is REFUTED. The overlap is
produced by many NARROW files piled on the same time ranges, not by wide ones.
That is exactly what `skippable_certified_files`'s own comment says: certification
is too SPARSE, and *"contiguous runs are what pay"*. I read that sentence twice
tonight and still built the wide-file story on top of it.

**What survives.** Wide files are real (2.9% > 48 bins; sealed consolidation's
p50-84 outputs are in that tail) and one project — 6297304f — is genuinely
span-blocked at 66.7%. So span_cap is a targeted fix for a MINORITY of the fleet,
not the general unlock. Its maintenance-cost case (read amplification, ~3.7%
ceiling) stands on its own merits and is unchanged.

**The real lever is contiguity: certify whole days, not scattered slices.**
`cert_slice_day_covered = 3` against `cert_slice_partial = 49` says days are
almost never fully covered, and with p50 overlap of 3-6 among narrow neighbours,
a file becomes skippable only when its whole neighbourhood is certified together.
That reframes the target from "make files narrower" to "make certification
CONTIGUOUS" — a different fix in a different lane.

**METHOD.** This is the second time tonight a strong causal story survived
several reasoning steps and died to one cheap measurement (the first was
"certification is eating the dedup tick", killed by one tick of logs). The
advisor's instruction to run the discriminating computation BEFORE writing the
span_cap case is what stopped a deploy on the wrong lever. **A counterfactual —
"if I removed the thing I blame, what changes?" — is worth more than any amount
of mechanism narration.**

## 03:15 — THE CONTIGUITY LEVER, QUANTIFIED (offline, same checkpoint)

Simulation on the real file spans of `date=2026-09-02`: certify files in TIME
ORDER (a contiguous prefix of the day) vs SCATTERED order, and count how many
become skippable under `skippable_certified_files`' actual rule — a certified
file is skippable only if NO uncertified file overlaps it.

| project | order | 25% | 50% | 75% | 90% |
|---|---|---:|---:|---:|---:|
| 87576849 (n=201) | **contiguous** | 20.4% | 47.8% | 74.1% | 88.6% |
| 87576849 | scattered | 1.0% | 1.0% | 10.0% | 14.9% |
| **28f62f01** whale (n=153) | **contiguous** | 22.9% | 47.1% | 72.5% | 88.9% |
| **28f62f01** whale | scattered | **0.0%** | **0.0%** | **0.0%** | 60.1% |
| 00000000 (n=79) | contiguous | 20.3% | 46.8% | 72.2% | 88.6% |
| 00000000 | scattered | 0.0% | 15.2% | 45.6% | 70.9% |
| dcad860a (n=46) | contiguous | 17.4% | 45.7% | 71.7% | 87.0% |
| dcad860a | scattered | 0.0% | 2.2% | 28.3% | 63.0% |

**THE SAME CERTIFICATION WORK BUYS 47% SKIPPABLE CONTIGUOUSLY, OR 0% SCATTERED.**
The contiguous curve is essentially LINEAR across all four projects — so partial
credit is real and this is NOT an all-or-nothing target. The scattered curve is
flat near zero until it approaches total coverage, which is the worst possible
shape: it looks like no progress right up until the end.

**This explains `cert_skip_files = 0` beside 31 grants on the previous process.**
Grants were spread across dates and slices, and scattered grants buy nothing.
`cert_slice_day_covered = 3` vs `cert_slice_partial = 49` is the same fact from
the other side.

### The recommendation

**Order certification DEPTH-FIRST — finish a (project, date) before starting
another — rather than breadth-first across dates.** No new mechanism is needed;
the probe, the ledger and the read-path rule all already exist and work. What is
wrong is the ORDER work is issued in, which is the cheapest class of fix there
is, and the payoff curve says the first contiguous quarter of a day already
returns ~20% of that day's files.

This supersedes the span_cap direction (refuted at 03:05) and it is a strictly
better target than raising grant VOLUME: at scattered order, even 75% coverage
returns zero on the whale.

**Caveat, stated plainly:** the simulation certifies FILES in time order; the
real system certifies SLICES that map to files. Since I sorted by (min,max)
timestamp, "contiguous files" == "contiguous time", which is what a slice-ordered
policy would produce — but the mapping is an assumption, not a measurement.

### Status of the quiet process (uptime 2795 s, no restart)

`dedup_probe_timeouts_total` 9 in 46 min = **0.17/min**, holding the ~5x
improvement; `dirty_bin_batch_probe_clean_total` still **0**. `dedup_skipped` 0
of 2604 eligible and `cert_skip_files` 0 — the read path remains blocked, exactly
as the contiguity result predicts it must be.

## 03:45 — WHERE the contiguity fix belongs (I had the lane wrong)

Reading `uncertified_window_dates` (maintain.rs:6138-6186) corrects my 03:15
recommendation. It ALREADY orders **project-major, busiest-first by file count**,
with an explicit comment explaining why ("grants scattered one-per-project across
many projects buy nothing while the same number concentrated on one project
completes a window"). So "issue certification depth-first" is, at the
PROJECT/DATE granularity, already implemented — and my recommendation as written
would have been a no-op.

**The real location is the SLICE path.** Both paths funnel through
`record_certification`, which accumulates intervals per (project, table, date)
via `merge_clean_interval` and grants only when the merged intervals cover the
WHOLE day (maintain.rs:4994-5002):

- the **whole-date probe** certifies a date all-or-nothing and is already
  project-major;
- **dedup rewrites** certify only the SLICE they happened to touch, and slice
  work is ordered by AGE/starvation (`tf_ordering_ranks_age_not_benefit_2026-08-20`,
  `tf_sealed_lane_pinned_by_metrics_2026-08-28`), not by position in time.

Scattered slices never merge into one day-covering interval — which is exactly
what the counters say on this quiet 77-minute process:

| counter | value |
|---|---:|
| `cert_slice_partial` | **24** |
| `cert_slice_day_covered` | **0** |
| `cert_granted_total` | 2 |
| `dedup_skipped` / `dedup_eligible` | 0 / 3887 |

**24 days accumulating partial coverage and not one completed.** That is the
scattered curve from the 03:15 simulation, observed directly rather than
simulated.

**Refined recommendation:** order SLICE-level certification work by time position
within a (project, date) so `merge_clean_interval` grows ONE contiguous run
toward day coverage, instead of depositing disjoint islands. The simulation says
the first contiguous quarter of a day returns ~20% of its files, where the same
quarter scattered returns ~0%.

**Method note.** My 03:15 write-up named the fix without reading the function it
would change, and the function already did the thing I was about to recommend.
The finding (contiguity pays, scattered does not) survives intact; the
PRESCRIPTION was aimed at the wrong lane. Read the code path you intend to
change before naming the change — the same lesson as the phantom env knob at
01:15, one level up.

## 04:15 — the fix has a PRECEDENT in the same function (and I am not shipping it tonight)

Two-hour quiet reading first (uptime 6398 s):

| counter | 77 min | 107 min |
|---|---:|---:|
| `dedup_probe_timeouts_total` | 10 | **10 — ZERO in 30 min** |
| `dirty_bin_batch_probe_clean_total` | 0 | 0 |
| `cert_slice_partial` | 24 | **43** |
| `cert_slice_day_covered` | 0 | **0** |
| `dedup_skipped` / `dedup_eligible` | 0 / 3887 | 0 / 5373 |

Probe timeouts have effectively STOPPED — none in the last half hour, against 34
in 28 minutes on the pre-fix build. That is the night's one code change, holding
over two hours. **43 partial slice coverages and still zero completed days** is
the contiguity failure, observed rather than simulated.

### Where the contiguity fix belongs, exactly

`MaintenanceCoordinator::rank` (`maintenance_coordinator.rs:1805`) returns
`(class, damaged, starved, hole, width, benefit, recency)`. Its own doc says:

> `hole_rank` orders WITHIN a class: a cell whose tier output is missing
> outranks one that already has output ... **Newest-first is right for FRESHNESS
> and wrong for CONTIGUITY, and 30 contiguous days is a contiguity goal.**

**The coordinator ALREADY HAS a contiguity term — `hole_rank` — and it was built
for the ROLLUP lane for precisely this reason.** The dedup lane has no analogue:
nothing in `rank` prefers a slice ADJACENT to the already-certified run in its
(project, date). So dedup slices are claimed by age/starvation and deposit
disjoint islands, which is what `cert_slice_partial=43, day_covered=0` records.

This is `tf_lane_coverage_gaps_2026-09-04` again — one mechanism, applied to one
lane and not the other.

**The specified change:** add a contiguity term to `rank` for Dedup units that
prefers the slice extending the existing certified interval in its
(project, date), mirroring `hole_rank`'s role for rollups.

### Why I am NOT implementing it at 04:15

That rank tuple is the most scarred code in the repo, and its own comment
documents THREE separate starvation regressions caused by reordering it:
`-width` buried narrow repair units (2026-08-23, three units nine hours past
deadline); reversing width starved the opposite end because "the selection loop
matches the winning tuple EXACTLY"; and damage had to be tied deliberately so
`fair_cursors` could rotate. A new term inserted casually into that tuple starves
something, and the failure mode is invisible for hours.

It also needs a decision I should not make unilaterally: contiguity competes with
`starved`, and preferring adjacency BY CONSTRUCTION delays the oldest work. That
is a real trade against the starvation guarantees those three regressions bought.

So: specified, evidenced, and left for a rested decision with a simulation
backtest (`timefusion sim`) before it goes near prod. The evidence for it is
strong — 47% vs 0% skippable, 43-vs-0 observed — and it will keep.
