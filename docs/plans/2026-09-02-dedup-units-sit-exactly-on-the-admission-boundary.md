# Dedup's stuck class sits EXACTLY on the admission boundary

Analysis 2026-09-02, from the real prod journal (`maintenance_tasks.json`,
79,559 tasks). No code shipped from this yet — the paired fix is gated on the
pool instrument deployed the same night. Read
`2026-09-02-repair-budget-sized-by-the-target-file.md` first: this is the same
defect class in the largest lane.

## The measurement

Live dedup units (state not complete/superseded): **1,552**.

```
priced (estimated_decoded_bytes > 0)   624
  of those, >= MAX_DECODED_BYTES       365   = 58.5%
UNPRICED (estimate == 0)               928
```

The 58.5% reproduces the "58.7% never admissible" figure, and **its denominator
is the PRICED units, not all dedup units.** Stated against all live dedup work
it is 23.5%. Both numbers are true of different populations; quoting the first
while meaning the second overstates the class by 2.5x.

The stuck class, characterised:

| property | value |
|---|---|
| count | 365 |
| **size, median** | **512.0 MiB — exactly `MAX_DECODED_BYTES`** |
| size, minimum | 512 MiB |
| **exactly 512 MiB** | **298 of 364 due units** |
| size p90 / max | 1.51 GiB / 1,150 GiB |
| over 2 GiB | 24 |
| age, median | **351 h = 14.6 days** |
| age, max | 412 h = 17.2 days |
| due now (not `not_due`) | 364 of 365 |
| state | 358 Pending, 7 Retry |
| attempts == 0 | 165 |

A median of *exactly* the constant, with the minimum also exactly the constant,
is not a distribution — it is a boundary.

## Why it is a boundary

`byte_bounded_units` splits until each unit is `<= MAX_DECODED_BYTES`, so its
output piles up AT the constant. Admission then does:

```rust
// database/maintain.rs — the dedup site
let request = Resources { decoded_bytes: estimated_bytes.clamp(1, MAX_DECODED_BYTES), .. };
```

```rust
// maintenance_coordinator.rs
if request.decoded_bytes > occupancy_scaled_ceiling(available, capacity) { return None; }
fn occupancy_scaled_ceiling(available, capacity) -> u64 {
    (MAX_DECODED_BYTES * available / capacity).clamp(MAX_DECODED_BYTES / 16, MAX_DECODED_BYTES)
}
```

`MAX * available / capacity` is **strictly less than MAX whenever `available <
capacity`** — i.e. the moment any single byte is reserved by anything. So a
request of exactly MAX is admitted only into a **perfectly empty pool**, and the
splitter's own target is exactly MAX.

**The producer targets a constant the gate cannot grant.** That is the same
sentence as the repair defect, in the lane that carries 96% of maintenance.

The code's own comment already states the general form, two lines above the
request:

> "Admission scales its ceiling by how full the pool is, so a request that always
> asks for `MAX_DECODED_BYTES` is refused whenever the pool is busy — which is
> always."

The fix applied there — ask for the unit's OWN size — repairs the class *below*
MAX. It cannot repair the class *at or above* MAX, because the clamp pins those
back to exactly MAX.

## Two corrections to what I previously recorded

1. **These units are not accruing `resource_admission`.** Observed reasons on
   the stuck class: `none` 358, `admission_busy` 4, `resource_admission` 3. The
   "328/328 all `resource_admission`" note is about **repair**, not dedup;
   applying it to dedup was a conflation. Refusals here are deliberately
   classified `admission_busy` (transient) precisely so `retry_or_split` does
   NOT split them — splitting on refusal caused 230,015 retries in 33 minutes on
   2026-09-01.
2. **`retry_reason = none` does not mean "never refused".** 165 units read
   `attempts = 0` at 14.6 days old. Both fields reset as a unit cycles back to
   Pending, so they describe the CURRENT cycle, not the unit's history — a
   standing trap when reading this journal.

## The curve is nearly inert, which changes the fix

`maintenance_admission` is constructed with `cfg.derived.memory_limit_bytes`
(the whole box, ~120 GB), and capacity is 3/4 of that ≈ **90 GB**. Sixteen
coordinator jobs at 512 MiB each reserve 8 GiB — about **9%**. So `available /
capacity` sits around 0.91–1.00 and the ceiling lives in roughly
[466 MiB, 512 MiB): the 32 MiB floor is unreachable and the occupancy curve
barely moves.

So this is **not** "reshape the curve ClickHouse-style" — that would be tuning a
curve that never travels. The defect is entirely at the **boundary**: MAX is
attainable only at exact idleness.

The prior-art shape still applies, just differently. ClickHouse grants its FULL
cap at **8 free pool entries of 16 — half free, not idle**
(`number_of_free_entries_in_pool_to_lower_max_size_of_merge = 8`). Ours grants
the full cap only at 100% free. Reaching MAX at some healthy free fraction is
the one-line form of that rule.

## Why this must NOT ship alone

Widening admission moves the failure rather than removing it. The second jaw is
already visible in this journal: the unpriced class's retry reasons include 247
`dedup` entries carrying

> `Not enough memory to continue external sort ... Additional allocation failed
> for ExternalSorter[0]`

and the arithmetic says why:

```
coordinator_share_bytes = min(jobs x MAX_DECODED_BYTES, maintenance_pool * 3/5)
                        = 16 x 512 MiB = 8 GiB
per job                 = 8 GiB / 16   = 512 MiB
```

**Each job's sort slice is exactly the bytes it is admitted to decode** — zero
headroom for merge buffers or spill batches. Admit more 512 MiB units and they
graduate from starving to failing, which reads as a regression in every counter.

So Decision 2 is a PAIR — the admission boundary and the per-sort slice — and
shipping half of it is worse than shipping neither.

## What settles the design

The `maintenance_pool_pct` / `coordinator_pool_pct` instrument deployed tonight
(`6cfd51d`). It answers the one question the journal cannot: where the ceiling
and the sort slice actually sit under load, rather than where the arithmetic
says they should. Sample over a window — a handful of reads finds the modal
state and misses the peak, and the peak is what sizes the fix.

Do not pre-commit to a formula before that data exists.
