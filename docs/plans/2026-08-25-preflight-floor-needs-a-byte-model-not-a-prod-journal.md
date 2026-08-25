# The preflight floor needs a byte model in `sim`, not a prod journal

**Owner:** unassigned. **Status:** decision recorded, implementation not started.
**Last reviewed:** 2026-08-25. **Closes:** `tasks/09`'s framing.

## Verdict

The premise that journal access is what blocks this validation is **wrong**, and
wrong in the useful direction. `timefusion sim` and `timefusion run-unit` both
exist (`src/main.rs:77`, `src/main.rs:102`) — the worry that they were still
"Phase 0" is stale. But `sim` **does not model the byte-driven preflight at
all**: its own module doc says so (`src/maintenance_sim.rs:6`, "Byte-driven
splits, memory admission, and intra-call operation order are outside the model"),
and the code bears it out — `split_time_task` is called only from the two real-IO
preflights in `src/database/maintain.rs:1278` and `:1749`, never from the sim
loop. The sim's `report.splits` counts a completely different thing: units that
timed out, were abandoned, and bisected on a **synthetic** `MAX_DECODED_BYTES +
1`. So `split_sheds_enough`, `parent_measured_bytes` and
`split_declined_at_floor` — the entire fix `69e6503` shipped — are never
exercised by `sim`, and **feeding it the real prod journal would not change
that**. The work owed is a ~60-line byte model inside `sim` plus a synthetic
journal fixture; the prod journal is a nice-to-have that calibrates one constant,
not the gate. Recommendation: build the byte model and the synthetic fixture,
keep the journal ask open at low priority, and do not spend any more effort on
host access for this item.

---

## 1. The blocker, stated precisely, and what would unblock it

For the record, because it is still owed and someone will ask again.

**What is wanted:** `/data/.timefusion_meta/maintenance_tasks.json` and the
`maintenance_tasks.wal` beside it, from the running
`srv-captain--timefusion` container on `captain.s.past3.tech`.

**Why it cannot be had today:**

- The host paths (`/home/ubuntu/timefusion-wal`, `/home/ubuntu/timefusion-data`,
  per `docker inspect`) are not readable as `ubuntu`; they need `sudo`, which
  that host does not grant.
- The image is distroless, so `docker exec sh` is not available.
- `docker cp` would work and is the documented fetch — `src/main.rs:222` names it
  in the `sim` CLI's own doc comment — but it **mutates nothing and is still
  outside the standing read-only remit** for that host, which is `logs` /
  `inspect` / `ps`. The remit is a rule about what an unattended agent may do,
  not a statement about what the command does.

**What sanction unblocks it, exactly one of:**

1. The operator runs `docker cp <container>:/data/.timefusion_meta/` themselves
   and drops the two files somewhere fetchable; or
2. The operator explicitly widens the remit to permit `docker cp` **out of** the
   container (read direction only), in writing, in `CLAUDE.md`; or
3. Someone with `sudo` on the host copies the two files out.

Any of the three takes minutes. **None of them is on the critical path for §2
below**, which is the whole point of this document.

**Do not** improvise around it. Every alternative that comes to mind (an
in-process HTTP endpoint that dumps the journal, a `timefusion_stats` row
carrying serialized tasks, a debug pgwire function) is a new production surface
that exports scheduler internals, added to answer a question a local fixture
answers for free.

## 2. Why the prod journal would not have answered §2 anyway

This is the finding.

The pathology being validated is a **multi-level cycle through the journal**:
preflight measures a slice for real → it is over `MAX_DECODED_BYTES` → bisect →
each child is claimed, measured for real, still over budget because the cost
floor is `files × row_group_bytes` and not a function of time width → bisect
again, down to `MIN_SLICE_MICROS`. Prod held 3,455 units for one
(project, tier, day).

Every arrow in that cycle that says "measured for real" is missing from `sim`:

| Step | Real code | In `sim`? |
|---|---|---|
| Claim a unit | `TaskJournal::claim_next` | **yes** — real code, `maintenance_sim.rs:444` |
| Measure the claimed slice's bytes | scan estimate in `maintain.rs:1223-1278` | **no** — no byte model exists |
| `journal.split_time_task(&key, estimated_bytes, footprint)` | `maintain.rs:1278`, `:1749` | **no call site in the sim** |
| `split_sheds_enough(parent_measured_bytes, observed)` | `maintenance_coordinator.rs:2380` | unreachable from the sim |
| `byte_bounded_units` prorating children by time share | `maintenance_coordinator.rs:487` | unreachable from the sim |
| Timeout → `abandon_running` → forced bisect | `maintenance_sim.rs:419` | **yes**, and this is what `report.splits` counts |

The sim models *duration* (`duration_range_secs`, `maintenance_sim.rs:109`) and
therefore models timeouts. It does not model *bytes*. A real prod journal
supplies `estimated_decoded_bytes` and `input` footprints on the tasks it
contains, but nothing in the sim ever reads them to make a split decision — so
replaying it would produce a run in which the floor guard never fires, in either
direction, and the `split_declined_at_floor` counter the task file says to watch
would read 0 by construction.

**Corollary:** the reason the fix is only unit-tested is not journal access. It
is that the tool everyone pointed at cannot see the mechanism.

## 3. What to build instead

Two pieces. Both live in `src/maintenance_sim.rs`; neither touches production
code paths.

### 3a. A byte model with a floor

One function, mirroring the physics the fix is about:

```
bytes(slice_width, partition) =
      files_overlapping(slice) * ROW_GROUP_BYTES      // the floor: at least one
                                                      // row group per file
    + day_bytes * (slice_width / DAY_MICROS)          // the part that does
                                                      // shrink with width
```

`files_overlapping` should itself decay with width but bottom out — a whale day's
files are spread across the day, so a 5-minute slice still overlaps a large
fraction of them. The measured anchor from the incident is **302 MB for a
5-minute slice**; pick `ROW_GROUP_BYTES` and the file-count decay so the model
reproduces that point, and record in a comment that the number is an anchor, not
a derivation.

Then call it where prod calls its real counterpart: at claim time, before the
unit is dispatched to a worker, mirroring `maintain.rs:1273-1278`:

```
let observed = model.bytes(&task.key);
journal.record_input(&task.key, footprint);
if journal.split_time_task(&task.key, observed, Some(footprint)) { continue; }
```

That single insertion makes `split_sheds_enough`, `parent_measured_bytes` and
`byte_bounded_units` real code under test, on virtual time, at journal scale.

**The model must be a `SimConfig` field, not a constant**, with at least two
shapes: `floorless` (bytes strictly proportional to width — the model
`byte_bounded_units` itself assumes) and `floored` (the formula above). The
floorless shape is the control: it must **not** shred, which proves the shred is
caused by the floor and not by the scheduler.

### 3b. A synthetic journal fixture

`sim` can already be fed a synthetic journal today, and its own tests show how:
`journal_with_streams` (`maintenance_sim.rs:582`) builds `MaintenanceTask`s and
`upsert`s them into a `TaskJournal::load(tempdir)`. Extend that, do **not**
hand-write `maintenance_tasks.json` — the on-disk shape is an internal serde
detail that will drift, and a fixture that pins it will rot silently.

The fixture needs a `--synth` input mode on the CLI (`timefusion sim
synth:whale`), or a `#[test]` that constructs it directly. It must produce, at
minimum:

- **Skewed cell sizes.** One whale (project, tier, day) whose modelled day bytes
  are ~100x `MAX_DECODED_BYTES`, a mid tenant at ~5x, and a long tail of ~30
  cells that fit in one unit. A uniform queue cannot reproduce the observed
  behaviour, in which one cell holds 3,455 units and the rest hold one each.
- **Many cells.** ≥ 200 (project, source, day) cells so `claim_next`'s ordering,
  the debt-occupancy cap and coarsening all operate at a scale where they
  interact. A 3-cell journal answers no scheduling question.
- **Pre-existing shred debris.** ~600 one-minute units on one partition with
  `input: None` and an inflated `estimated_decoded_bytes` — the exact shape
  `a_footprintless_shred_fuses_once_the_partition_ceiling_is_known` already
  builds — so the run exercises fusion and coarsening against the floor guard at
  the same time, which is the interaction no unit test covers.
- **A `parent_measured_bytes: None` start.** Every synthetic unit begins with no
  parent evidence, so the first split of each lineage is unconditional and the
  guard only engages from the second level down — the real sequence.

### 3c. Success criteria

The run is `timefusion sim synth:whale --hours 6 --workers 16`, both byte-model
shapes, both guard states.

1. **The bug reproduces.** `floored` model with `split_sheds_enough` forced to
   always-true yields **> 1,000 units** for the whale cell and units at
   `MIN_SLICE_MICROS`. If it does not, the model is wrong and nothing below
   means anything — this is the gate, not a formality.
2. **The control is clean.** `floorless` model, guard on or off, yields a whale
   cell in **tens of units**, no unit at `MIN_SLICE_MICROS`. This is what
   separates "the floor causes the shred" from "the scheduler causes the shred".
3. **The fix works.** `floored` model with the guard as shipped yields the whale
   cell in **tens of units**, at least one unit with `hash_shards > 1` at a width
   **strictly above** `MIN_SLICE_MICROS`, and no unit exceeding
   `MAX_DECODED_BYTES` at run time.
4. **The fix does not stall.** `split_declined_at_floor` rises while
   `pending` for that cell **stops growing and then falls**. Both rising is the
   documented worse-than-the-bug outcome: units declined, then failing to run.
5. **The threshold is calibrated, not argued.** Sweep
   `SPLIT_MUST_SHED_NUMERATOR/DENOMINATOR` over 1/2, 2/3, 3/4, 4/5 against ≥ 3
   floor shapes (whale, mid, tail). Publish unit count and completed work for
   each. If 3/4 is not the best or is not on a flat part of the curve, say so —
   that is the one thing the constant was chosen by argument for, and it is the
   only thing a real journal would calibrate better.
6. **The lineage guard still holds.** A synthetic-stamp lineage
   (`MAX_DECODED_BYTES + 1`, the `retry_or_split` shape) must still split, in the
   sim, at scale — `a_synthetic_stamp_does_not_freeze_a_lineage` proves the
   predicate; this proves the loop.

## 4. What a real prod journal would still add, once available

Only calibration, and it is worth having at low priority: the real distribution
of `files × row_group_bytes` per (tenant, day), which is what sets the floor. The
synthetic model's floor is an anchor point plus a shape assumption; a journal
replay would tell you whether the shape assumption is right for tenants other
than the one the 302 MB came from. That is criterion 5 refined, not criteria 1-4
replaced.

Nothing else. The queue *shape* — how many cells, how skewed, how much debris —
is reproducible synthetically and is the part that matters for 1-4.

## 5. What NOT to do, carried forward

Unchanged from `tasks/09`, repeated because this document now supersedes it:

- **Do not** change the bisection arithmetic in `byte_bounded_units`, and **do
  not** lower `MIN_SLICE_MICROS`. Both make more units, which is the symptom.
- **Do not** write a debris-cleanup migration. `clear_stale_estimates()` exists,
  and the partition ceiling rescues footprint-less debris by fusing it. Deleting
  those units discards real queued rollup work whose only defect is a mispriced
  estimate.
- **Do not** add a production endpoint to export the journal (see §1).

## 6. Results, 2026-08-25 — built, run, and the fix does NOT hold

Implemented in `src/maintenance_sim.rs` only; `maintenance_coordinator.rs` is
untouched. `DayShape`/`ByteModel` are the floored cost model (anchored: a
9.2 GB / 1,000-file day models **302 MB at five minutes**, pinned by a doctest),
`preflight()` runs at claim time where `database/maintain.rs:1273-1278` runs it,
and `SplitGuard` selects the shipped predicate, the pre-fix behaviour (clearing
`parent_measured_bytes`, so the coordinator's `None` arm does the work) or a
swept threshold. `synthetic_whale_queue` builds 214 cells through the
`TaskJournal` API, including the 600-unit footprint-less debris shape.

**Criterion by criterion**, `synth:whale`, 6 virtual hours, 16 workers:

| # | Claim | Result |
|---|---|---|
| 1 | Bug reproduces | **PASS** — floored + guard off: 1,697 units for the whale cell, 1,440 of them at `MIN_SLICE_MICROS` |
| 2 | Floorless control is clean | **PASS** — 129 units, **zero** at the floor, guard on or off. The shred is caused by the floor, not the scheduler |
| 3 | Fix works | **FAIL** — floored + shipped guard is **byte-for-byte identical to guard off**: 1,697 units, 1,440 at the floor, and **no unit hash-sharded above `MIN_SLICE_MICROS`**. Only the memory bound holds (max run 505 MB ≤ 512 MiB), via runner-internal sharding at the floor |
| 4 | Fix does not stall | **N/A** — `split_declined_at_floor` stays **0**, so nothing is declined and nothing can stall. Not the good half of the criterion |
| 5 | Threshold calibrated | **Swept, and 3/4 is on the wrong side of the only cliff** — see below |
| 6 | Lineage guard holds | **PASS** — the `MAX_DECODED_BYTES + 1` synthetic-stamp lineage still splits at scale (49 units, not frozen) |

### Why 3 fails: the guard is a between-call test on a within-call recursion

The lineage trace (`whale_lineage_trace`):

```
level 0: width= 86400s observed= 82867MB parent_stamp=    none  split=true
level 1: width=   300s observed=  1751MB parent_stamp= 82867MB  split=true
level 2: width=    60s observed=  1517MB parent_stamp=  1751MB  split=false (never asked)
```

`byte_bounded_units` descends *many* levels inside **one** call — the day unit
becomes 256 five-minute children in a single split — and stamps every
descendant with the same measurement. So the whale's ladder is only **two**
journal levels: the day (no stamp, splits unconditionally) and the five-minute
child (1,751 MB against a stamp of 82,867 MB — it sheds 98%, so the guard
correctly allows it). The third level is already **at** `MIN_SLICE_MICROS`,
where `database/maintain.rs:1276` never calls `split_time_task` at all.

The 60-second unit measures 1,517 MB against its parent's 1,751 MB — a 13%
shed, which the guard *would* decline. It is never asked. `69e6503` can only
fire on a lineage that takes **three or more measured levels** to reach the
floor; a model that is wrong by 50x reaches the floor in two.

### Criterion 5: the sweep

`SPLIT_MUST_SHED_NUMERATOR/DENOMINATOR` against three floor shapes (whale day
bytes as a multiple of `MAX_DECODED_BYTES`). Larger threshold = weaker guard;
`Off` is the pre-fix baseline.

| floor | 1/2 | 2/3 | **3/4 (shipped)** | 4/5 | off |
|---|---|---|---|---|---|
| 100x | 1697 / 1440@min / 0 declined | 1697 / 1440 / 0 | **1697 / 1440 / 0** | 1697 / 1440 / 0 | 1697 / 1440 / 0 |
| 20x | 97 / 0 / 64 | 97 / 0 / 64 | **225 / 0 / 0** | 225 / 0 / 0 | 225 / 0 / 0 |
| 5x | 25 / 0 / 0 | 25 / 0 / 0 | **25 / 0 / 0** | 25 / 0 / 0 | 25 / 0 / 0 |

Read: `units / units at MIN_SLICE / splits declined`. Every run drained to
`pending_end = 0`, so no threshold stalls the queue.

Three findings, none of them arguable from a spreadsheet:

1. **At 100x nothing helps.** The guard is never consulted, at any threshold.
2. **At 20x the guard works and 3/4 is on the wrong side of the cliff.** 1/2 and
   2/3 decline 64 splits and land the cell in **97** units with 64 executions
   hash-sharded *above* the floor — exactly the behaviour §3c.3 wanted. 3/4 and
   4/5 decline nothing and produce **225**. The transition sits between 2/3 and
   3/4, so the shipped constant is not on a flat part of the curve; it is the
   first value that does nothing. (Completed work: 324 against 388 — fewer,
   larger units, not less progress.)
3. **At 5x the threshold is irrelevant** — the model is close enough that no
   second level is ever needed.

### What this implies (not implemented — out of scope for this item)

The fix as shipped is not wrong, it is *unreachable* in the shape that caused
the incident. Two candidate directions, both needing their own decision:

- Move the floor test **inside** `byte_bounded_units` — bisect one level per
  measurement rather than a whole subtree per measurement — so the guard sees
  every level. This changes the bisection arithmetic, which §5 forbids without
  a decision.
- Or tighten `SPLIT_MUST_SHED_*` to 2/3, which is free at 5x, strictly better at
  20x, and still nothing at 100x.

Neither should ship on this evidence alone: the floor's real per-tenant shape is
the one thing §4 says a prod journal would calibrate, and it decides which of
the three rows above production actually lives in.

## 7. Decision, 2026-08-25: bisect ONE level per measurement

**Taken:** `byte_bounded_units` no longer recurses. One call halves the slice
once — two children, priced by time share, each stamped with what the parent
MEASURED — and the next level is minted only after its own preflight has
measured it for real. The guard is therefore consulted at every level of the
ladder, which is the whole of the defect: it was a between-call test on a
within-call recursion.

This is §6's first candidate direction, and it is the change §5 forbids
"without a decision". This is the decision. `SPLIT_MUST_SHED_NUMERATOR/
DENOMINATOR` stay at 3/4 — one decided change, not two — and `MIN_SLICE_MICROS`
is untouched. The hash-shard branch (a slice already at the floor) is unchanged,
so `split_time_task` still declines to mint hash-shard units and lets the runner
shard internally.

**Rejected: tighten `SPLIT_MUST_SHED_*` to 2/3.** §6's own sweep refutes it. The
100x row reads identically at 1/2, 2/3, 3/4, 4/5 and guard-off — no constant can
repair a predicate that is never consulted, so the required outcome
(`split_declined_at_floor > 0` on the whale) is unreachable by any threshold.
2/3 helps only the 20x row, which is a shape the incident was not.

**Rejected: leave it and rely on the runner's internal hash sharding.** Memory
is bounded either way; the queue is not. 1,440 journal units at the floor is the
cost the incident was about.

**Failure mode this risks — the decline side.** A declined leaf runs with
`observed / MAX_DECODED_BYTES` internal hash shards, and every shard re-reads
the row-group floor: work amplification, bounded in memory, not starvation. And
a lineage now needs ~8 sequential claim→measure→split rounds where it needed 1,
so a whale converges to runnable width more slowly. The *volume* of preflights
is roughly unchanged (~255 against ~257 — every minted unit was preflighted
anyway); it is depth that grows. The opposite failure mode — over-splitting into
a shred — strictly decreases, because no level is ever minted unmeasured.

### 7a. Results after the fix, same fixture, same six criteria

_(Filled in below from the post-fix run; the fixture is byte-for-byte the one
§6 used.)_

## Done when

§3c criteria 1-4 pass in CI as a `#[test]` over the synthetic fixture (not a
manual CLI run), and 5's sweep is published in this document. 6 pins the
regression.
