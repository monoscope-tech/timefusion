# 29% of compaction cells are permanently unselectable — the unpairable band

**2026-09-03.** A lane holding **1.33 TB** of debt (`sealed_compaction_debt_bytes`)
with 220 pending cells was doing ~7 seconds of work per claim. This is why.

## The measurement

Of 63 `SealedConsolidation` starts in 30 minutes, **31 ended in
`compaction_unit_selected_nothing` — 49%.** The funnel log names where the
candidates go, and it is not the filters:

```
snapshot_files=6373  after_date_filter=77  after_project_filter=19
after_range_filter=19  unsorted_candidates=0  under_target=19  selected=0
```

**Nineteen files pass every filter, are under target, and none is selected.**

## The cause: a threshold mismatch

`select_coordinator_compaction_candidates` bin-packs smallest-first under a
budget. With `unsorted_candidates=0` every file is already a sorted run, so the
budget IS `target` (256 MiB). **Two files can therefore only pair if each is under
target/2.** But `converged()` marks a file done only at **7/8 of target
(224 MiB)**.

**Files between 128 MiB and 224 MiB are neither converged nor pairable.** They are
counted as debt, claimed, and select nothing — forever.

Measured over the live Delta checkpoint, cells with >= 2 under-target files:

| | |
| --- | --- |
| cells with >=2 under-target files | 168 |
| **cannot pair even the two SMALLEST** | **49 = 29.2%** |
| can pair | 119 |

```
edb04135 2026-07-04  files=4  two smallest = 179 MB + 181 MB > 256 MB
87576849 2026-06-20  files=3  two smallest = 111 MB + 162 MB > 256 MB
98fdd4f3 2026-07-02  files=4  two smallest = 165 MB + 168 MB > 256 MB
```

29.2% of cells stuck against 49% of claims empty is consistent: a stuck cell never
clears, so it is re-claimed indefinitely and is over-represented among claims.

**This is a regression of a shape already fixed once.** The selector's own comment
records prod 2026-08-23 with `after_range_filter=47 ... selected=0`, fixed by
sorting SMALLEST FIRST so one large file could not eat the budget. Smallest-first
is necessary but not sufficient: when the two smallest ALREADY exceed the budget,
ordering cannot help.

## Two candidate fixes — neither shipped tonight

1. **Raise the convergence threshold to `target/2`.** A file above half the target
   cannot be paired with another above half the target, so it is converged in
   practice. Stops counting phantom debt and stops the wasted claims. **Does not
   reduce file count** for those cells — it admits they are done.
2. **Let a bin exceed the target when it would otherwise select nothing.**
   179 + 181 = 360 MB is over the 256 MB target but retires a file, and the target
   is a target rather than a cap. Actually reduces file count and therefore
   overlap — which is the thing the read path needs — at the cost of files above
   target.

(2) is the one that helps the read-path overlap problem; (1) only stops the waste.
They are not exclusive: (1) is the honest accounting, (2) is the throughput fix.

**Not shipped:** this would be the third selection/packing change of one session,
in the area with this repo's worst incident history, and `d1ff6a32` has not yet
been observed in prod. The measurement, the funnel, and both candidates are
recorded so the next session can pick with evidence rather than rediscover it.

**Acceptance test when it is done:** `compaction_unit_selected_nothing` rate falls
from ~49% of SealedConsolidation claims, and `work.SealedConsolidation.worker_secs`
rises from its current ~0.25% of one worker.

---

## CORRECTION: fix (2) is NOT a one-line relaxation — `MAX_BIN_ROWS` blocks it too

I wrote that letting a bin exceed the target would retire a file "at the cost of
files above target". Reading the loop, **two guards bind, not one**:

```rust
if !selected.is_empty() && (bytes + add.size > limit || next_rows > MAX_BIN_ROWS) {
    break;
}
```

`MAX_BIN_ROWS = 2_000_000`, and its comment says it was chosen to sit **just above
the 1.73 M rows a 256 MB logs bin holds**, so a bin fits the 900 s deadline at
prod's staging rate. Two ~180 MB files are ~360 MB ~= **2.4 M rows — over the
cap.** Relaxing only the byte budget therefore changes nothing for exactly the
cells this document is about; the rows guard rejects the same pair.

Making (2) work means relaxing **both**, which puts a 2.4 M-row bin against a
deadline that `MAX_BIN_ROWS` exists to protect. That is a real trade, not a
tidy-up, and it wants a soak rather than a late-night edit.

## Which leaves (1) as the right fix, and it is not a cop-out

Two files of ~180 MB are **not fragmentation** — they are near-target files. The
overlap that costs the read path 20-43x comes from MANY SMALL files in one time
bucket, not from a cell holding three or four near-target files. So a cell whose
files cannot pair within either budget **is converged in every sense that
matters**, and counting it as debt is what wastes ~49% of the lane's claims.

**The honest fix is accounting, not packing:** stop treating a file that cannot
pair as outstanding debt. The subtlety to respect when implementing it — and the
reason this is still not a one-liner — is that "cannot pair" is a property of the
SET, not of one file: a 200 MB file pairs fine with a 40 MB one. A per-file
`>= target/2` rule is a good approximation (any two such files always exceed the
target) but it is an approximation, and it belongs in the planner's debt
accounting rather than in the selector, which already returns empty correctly.

**Net for the morning:** the measurement stands (29.2% of cells, 49% of claims),
the diagnosis stands, and the fix is smaller in scope than first written — but it
lands in `plan_compaction_debt`, not in `select_coordinator_compaction_candidates`.
