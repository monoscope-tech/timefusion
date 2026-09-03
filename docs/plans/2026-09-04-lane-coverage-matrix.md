# Lane coverage matrix — which cross-cutting mechanisms reach which lanes

Every fix shipped on 2026-09-04 was the same shape: **a mechanism that exists, is
correct, and was applied to some lanes but not the one that needed it most.** Four
times in one night is not four coincidences, it is a missing checklist. This is
the checklist, filled from source.

Cells are what I verified tonight; anything I did not open is marked `?` rather
than guessed.

## The matrix

| mechanism | HotPacking | SealedConsolidation | Repair | Dedup | BaseRollup | DerivedRollup |
|---|---|---|---|---|---|---|
| **liveness watcher** (blocking operators) | ✅ `maintain.rs:7157` | ✅ same path | ✅ same path | ✅ `compact.rs:1711` + `collect_watched` ×4 | ⚠️ **only since `eadc9def`** | ⚠️ **only since `eadc9def`** |
| **progress reporting** | ✅ `note_unit_progress` (rows **written**) | ✅ same | ✅ same | ✅ same | ⚠️ `PlanProgress` (plan **output_rows**) | ⚠️ same |
| **capacity classification** (`is_capacity_failure`) | ✅ since `5dbd2b79` (`maintain.rs:7232`) | ✅ coordinator | ✅ coordinator | ✅ coordinator | ✅ coordinator | ✅ coordinator |
| **retry / split ladder** | ✅ `retry_or_split`, journal-level | ✅ | ✅ | ✅ | ✅ | ✅ |
| **planner/packer budget agreement** | ✅ since `fa62883e` | ✅ since `fa62883e` | n/a | n/a | ? | ? |
| **"did nothing" funnel event** | ✅ `compaction_unit_selected_nothing` | ✅ same | ? | ? | ❌ **none** | ❌ **none** |

## What the gaps mean

**1. Progress metrics do not mean the same thing on every lane.** This is the
live one, created tonight. `note_unit_progress` counts rows a unit **wrote**;
`PlanProgress` derives from **`output_rows` summed over the whole plan tree**, so
it counts rows *read and aggregated*, which for a `GROUP BY` is vastly more than
rows published. After `eadc9def`, `work.BaseRollup.progress_rows` is a **liveness
proxy, not an output count**, and comparing it against
`work.SealedConsolidation.progress_rows` is meaningless. Either name them apart or
document it at the metric; today a reader has no way to know.

**2. The rollup lanes have no "did nothing" signal.** The compaction lanes emit
`compaction_unit_selected_nothing` with a funnel of every filter stage — which is
the only reason tonight's livelock was findable at all, and even that funnel was
missing the one field that mattered until `c627b356`'s predecessor added it.
The rollup lanes emit no equivalent. **A rollup unit that claims, runs and
publishes nothing is currently invisible**, which is exactly the state the
compaction lane was in before the funnel existed.

**3. `is_capacity_failure` and `retry_or_split` are genuinely uniform**, because
they live in the coordinator and every operation routes through the journal. That
is the shape the other mechanisms should copy: put the mechanism where the lanes
converge, not in each lane's own path. The two defects tonight
(`5dbd2b79`'s second classifier, `fa62883e`'s half-checked budget) were both cases
of a lane re-implementing a shared rule locally.

## The rule this suggests

> A cross-cutting mechanism belongs at the point where lanes converge — the
> coordinator, the journal, a shared helper — and when it cannot, its coverage
> must be asserted by a **source-level test**, because the failure mode is the
> ABSENCE of a call and no behavioural test can see that.

Three such guards now exist and all three were written after the corresponding
outage or livelock:

- `no_second_capacity_classifier_exists` (`maintenance_coordinator.rs:7574`)
- `maintenance_aggregates_are_collected_watched` (`database/maintain.rs`)
- `the_planner_admits_exactly_what_the_packer_can_bin` (`database/mod.rs`)

## What I did NOT do, and why

I did not ship fixes for the two ❌ cells. The bar I held to from ~01:45 was:
*same already-proven pattern, prod counters confirm it is binding now, mechanical
diff, failing test first.* A rollup "did nothing" funnel fails the second test —
I have no prod evidence that rollup units are claiming and publishing nothing,
precisely because the signal does not exist. Building the instrument to look is
correct; doing it as a fifth src change at 2am, on top of two fixes that have not
yet been observed in prod, is not.

**That is the recommended next step, in daylight:** add the funnel event to the
rollup lanes, deploy, and read it — the same sequence that made tonight's
compaction livelock findable and fixable in one cycle.
