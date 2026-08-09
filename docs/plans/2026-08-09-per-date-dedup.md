# Per-date dedup — bound dedup memory to one date, not the query window

Status: **designed, not built.** Written 2026-08-09 after the footer-repair
incident, whose user-visible symptom this change would have made impossible.

## Why

`DedupExec` runs over the whole scan. Two consequences, both measured:

1. **Memory scales with the WINDOW.** A 30-day scan buffers 30 days of dedup
   state. In `full-set` mode it cannot terminate early, so it runs until the
   2 GiB cap and errors — which is exactly how shipbubble and talstack lost
   14/30-day queries for days.
2. **Every scan pays for `id`.** `ProjectRoutingTable::scan` augments the
   projection with the dedup keys whenever the query did not ask for them.
   Measured on a representative live file (370 MB, 7.8M rows, 96 columns):
   `id` alone is **151 MB — 43% of the file**, and the four dedup columns are
   45%. So a chart doing `time_bucket + count(*)` projects `timestamp` and then
   reads `id` regardless.

## The key fact that makes it sound

Dedup keys are `[timestamp, id]`, and the `date` partition column is DERIVED
from `timestamp`. Therefore **two rows with the same dedup key always share a
date partition** — duplicates can never cross one.

Dedup is therefore decomposable per date: running a separate `DedupExec` per
date and unioning the results is EQUIVALENT to one global dedup, not an
approximation.

This holds under merge-on-read too: a MOR UPDATE appends the row's ORIGINAL
timestamp (that is what makes keep-greatest work), so every version of a key
lands in the same date.

## Design

In `ProjectRoutingTable::scan` / `wrap_result`, split the scan by date instead
of building one union:

- Group the window's dates into (a) dates whose files all declare an ordering
  and (b) the rest. Two groups is enough — full per-date is the general form
  but costs a Delta scan per date.
- Wrap each group in its own `DedupExec`, then union.
- The mem/hot legs cover recent dates only; assign them to the group holding
  those dates.

Wins:
- A poisoned date forces `full-set` for ITS OWN rows only. One bad file stops
  walling a whole window — the entire failure class disappears.
- Dedup memory scales with the largest DATE, not the window.

## The larger prize: drop the `id` projection

Once dedup is per-date, a date the sweep certified duplicate-free needs no
`DedupExec` at all, so its scan need not project `id`. That is ~45% fewer bytes
read for every chart and every `count(*)` over sealed dates.

`dedup_skip_allowed` already exists for this but is blocked by three things:

1. A blanket `version_append` bail (`dedup_skip_allowed`, database.rs ~13040).

   **Both halves of the safety argument are now VERIFIED BY INSPECTION**
   (2026-08-09), which is what this section previously flagged as unknown:

   - **Tombstones do not depend on dedup.** `filter_tombstones` is applied to
     the post-dedup plan unconditionally — the `match &tombstone` sits *outside*
     and *after* the `match dedup_on`, so it runs whether or not `DedupExec`
     did. Skipping dedup therefore cannot resurrect a deleted row.
   - **The sweep really does collapse MOR versions.** The sweep calls
     `mem_buffer::dedup_batches(.., schema.dedup_tiebreak, None)`
     (database.rs:7505), and that function is keep-greatest by the tiebreak —
     its own comment: *"the greatest value wins, ties → last"*.

   So on a partition the sweep certified duplicate-free with a still-matching
   file fingerprint, there is exactly ONE row per key and it is the winning
   version; a deleted row survives only as its winning tombstone, which the
   filter removes. **The bail is over-conservative and the lever is real.**

   **Still required before shipping: an EXECUTION test, not inspection.**
   Inspection establishes plausibility; it cannot rule out an ordering or
   edge-case interaction. Prove it with
   `buffer_consistency_test::test_{update,delete}::immediate` plus a new case
   that certifies a partition, skips dedup, and asserts the UPDATED value (not
   the superseded one) is returned and the DELETED row is absent.
2. The skip is decided AFTER the projection is built and requires
   `output_projection.is_none()` — but augmenting the projection is what sets
   it. The decision must be hoisted ABOVE projection computation.
3. A wide window always overlaps the mem buffer, and the union path never
   grants the skip. The per-date split is what fixes this: certified sealed
   dates that mem/hot do not cover get a no-dedup scan; the rest keep dedup.

## How to validate

- Equivalence: for a window spanning several dates with known duplicates,
  per-date dedup must return byte-identical results to global dedup.
- MOR: an UPDATE and a DELETE must still resolve correctly across the split,
  including when the updated row's date differs from the update's arrival date.
- Memory: dedup peak should track the largest date, not the window width.
- Regression: a poisoned date must no longer force `full-set` for the window.

## Do not

- Do not assume a poisoned date is rare. It is the normal state of the backlog.
- Do not skip step 1 above. Serving a superseded row is silent data corruption,
  which is strictly worse than the slow query this change is optimising.
