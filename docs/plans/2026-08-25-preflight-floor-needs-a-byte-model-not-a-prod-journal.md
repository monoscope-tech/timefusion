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

## Done when

§3c criteria 1-4 pass in CI as a `#[test]` over the synthetic fixture (not a
manual CLI run), and 5's sweep is published in this document. 6 pins the
regression.
