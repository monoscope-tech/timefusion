# TimeFusion plans

Last reviewed: 2026-10-20. Owner: TimeFusion maintainers.

## Current work

[Next work](2026-10-19-next-work.md) is the single backlog, with priorities, owners, dependencies, and acceptance checks.
The [October 19 review](../reviews/2026-10-19-timefusion-triage.md) records the alert/issue snapshot and completed work.
Its [outstanding issue inventory](../reviews/2026-10-19-timefusion-outstanding-issues.tsv) contains the 696 non-archived records at that review.
The telemetry snapshot remains dated October 19. This consolidation did not refresh it.

## Historical concepts

The dated plans are synthesized into four references. They retain useful mechanisms, correctness constraints, experiments, and conditional designs.

| Reference | Retained material |
| --- | --- |
| [Reads and routing](../reference/timefusion-reads.md) | Ordered dedup, metric-first layout, indexes, histograms, mutable visibility, query contracts, and targets |
| [Maintenance and rollups](../reference/timefusion-maintenance.md) | Admission, packing, DV/certification proofs, witnesses, policies, and conditional publication/stage designs |
| [Durability and recovery](../reference/timefusion-durability.md) | WAL ownership, snapshots, ambiguous publication, retries, storage, spill, and deployment lessons |
| [Measurement and decisions](../reference/timefusion-decisions.md) | Evidence limits, rejected experiments, capacity claims, prior art, tools, and release discipline |

The [source inventory](../reviews/2026-10-19-timefusion-triage.md#plan-inventory) maps every original note to its summary.
Original tables and chronology remain retrievable from git. No per-note archive or redirect files remain.

## Keeping this small

Update the current backlog for new work. Update a topic reference for stable concepts and settled decisions.
Keep observation dates and acceptance limits with historical measurements.
Separate implementation, deployment, and production acceptance.
Add another plan file only for a distinct active project that cannot fit the current backlog.
