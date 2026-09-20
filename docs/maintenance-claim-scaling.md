# Claiming maintenance work at 10-50x

Why the maintenance claim path became the bottleneck, why the obvious fix does
not work, and what carries us to 50x on the same hardware.

Companion to `maintenance-dispatch-and-yielding.md`, which this supersedes on the
question of contention.

---

## 1. What was measured

Prod, two samples 4,399 s apart (`timefusion_stats`, component `block`):

| | value | note |
|---|---|---|
| claims | **219/sec** | steady across both samples |
| `coordinator_claim.avg_us` | **4,686** | 4.7 ms, unchanged as the backlog drained 16x |
| `coordinator_claim.max_ms` | 1,730 -> **3,289** | one claim stalled the fleet 3.3 s |
| `journal_lock_wait.total_ms` | ~0.8 worker-equivalents | time spent only WAITING |
| `journal_hold` duty cycle | **41%** | the mutex is busy nearly half the time |

The cost did not move when `pending_base_rollup` fell 4,432 -> 269. That is the
whole clue: the claim scan is not proportional to the work queued.

## 2. The cause

The journal is one `Vec<MaintenanceTask>` holding every task in every state:

```
tasks_complete   93,326      <- 99% of the journal
tasks_pending       857
tasks_retry         156
tasks_running        10
```

`claim_next` filters the whole vector — three times per claim (two `best_class`
passes plus the selection pass) — to choose among ~1,000 candidates:

```
94,349 tasks x 3 passes x ~15 ns  ~=  4.2 ms      (measured 4.7 ms)
```

Completed tasks are retained deliberately: `dependencies_complete` proves a
derived unit's base tier from them. They simply have no business in the CLAIM
scan.

## 3. Why a dispatcher does not fix it

The companion doc proposed one claimer feeding workers, on ClickHouse's
`MergeTreeBackgroundExecutor` shape. Two numbers kill it as specified:

**A single claimer saturates.** 219 claims/sec x 4.7 ms = **1.03 cores** of
selection work. One task on one thread supplies at most 1.0. The dispatcher
becomes the bottleneck and 66 workers queue behind it — strictly worse than the
contention it replaces.

**Rank-outside-lock is unaffordable here.** The escape was RocksDB's SuperVersion
trick: clone a snapshot, rank outside the lock, re-acquire to commit. But
`Snapshot.tasks` is a plain `Vec`:

```
94,349 x ~330 B  ~=  31 MB per clone
31 MB x 219/sec  ~=  6.8 GB/s of memcpy
```

RocksDB gets away with it because SuperVersion is a few pointers and refcounts,
not a deep copy of every task. Ours would copy the entire journal per claim.

**What the prior art actually shares** is neither of those: ClickHouse's executor
queue holds PENDING tasks, and RocksDB maintains compaction scores incrementally
on version change rather than scanning every SST at pick time. Both keep the
SELECTION SET SMALL. That is the part we were missing, and it is what §4 and §5
restore. Note also that RocksDB does NOT centralise picking — background threads
pick under `db_mutex_`, structurally like ours.

## 4. Landed: the claimable index — O(all) to O(claimable)

A `claimable: BTreeSet<TaskKey>` holding only Pending/Retry keys. `claim_next`
walks it instead of the journal.

- **Permissive by construction.** Every mutation already passed through
  `dirty_tasks`; both now route through one `mark_dirty`, so a task cannot become
  claimable without being recorded. Stale entries are filtered on the way past.
- **Asymmetric on purpose.** A stale entry costs one lookup; a MISSING entry
  strands a unit forever. Tests assert the direction that matters.
- **Re-derived on the existing minute-scale prune sweep**, which bounds the
  permissive set's drift. That sweep is also the safety net: a key missed by some
  path is claimable again within 60 s rather than never.
- `claimable_tasks` is published; read it against `tasks_pending` — they should
  track.

Effect today: 94,349 -> ~1,023, about **92x**, which removes the contention at
its source and makes the dispatcher unnecessary.

**Ordering hazard this exposed, worth remembering:** `mark_dirty` reads the
task's state, so it MUST run after the mutation. For in-place updates the borrow
checker enforces that (the `&mut task` borrow cannot be held across the call).
For INSERT paths it does not — `upsert` and `enqueue_inner` both called it before
`insert_task`, so newly enqueued units never entered the index and were silently
unclaimable. The tests caught it; the compiler could not.

## 5. Next: sub-linear selection, for 10-50x

The index makes the scan proportional to CLAIMABLE work rather than to history.
But claimable work grows with ingest. At 50x:

```
~50,000 pending x 3 passes x 15 ns  ~=  2.2 ms     — back where we started
```

So the index buys headroom, not a scaling story. What scales is making selection
sub-linear:

**5.1 Ordered index per operation.** A `BTreeSet` keyed by `(rank, key)` makes
claiming `O(log N)`. Keying per operation also shards the structure six ways, so
lanes stop contending with each other for free.

**5.2 The obstacle is invalidation, and it is bigger than "rank ages".**

An early read of this said only `starved` varies with time and flips once. That
is WRONG, and the correction is the main reason 5.x is not a quick follow-on.
`rank` has FOUR moving inputs:

| input | changes when | shape |
|---|---|---|
| `scheduling_class` / `is_frontier_task` | a task crosses the frontier boundary | time |
| `starved` | every DAY past the horizon — it is GRADED, not a flip | time, recurring |
| `hole_rank` (`untagged_cells`, `tier_holes`) | those sets mutate | **bulk** |
| `adjacent` (`dedup_complete_edges`) | edges mutate | **bulk** |

```rust
let starved = if waited < STARVATION_MICROS { u8::MAX }
    else { (u8::MAX - 1).saturating_sub(waited.saturating_sub(STARVATION_HORIZON_MICROS).max(0) / DAY_MICROS) };
```

So an ordered index is a MATERIALIZED VIEW with two time-driven and two bulk
invalidation sources — not a membership set with a richer key.

**5.3 Why that raises the stakes rather than just the effort.** The membership
index degrades safely: a missed entry means a unit is claimed LATE, and the
minute-scale rebuild heals it. An ordered index degrades SILENTLY: a stale key
means claiming in the wrong priority order, which violates exactly the starvation
and deadline guarantees `rank` exists to enforce, with nothing to notice it.

**5.4 Sketch, to be designed properly before any code.** One ordered set per
`(operation, class)` plus a time-ordered promotion queue keyed on the next
instant a task's `starved` step changes (day granularity, so the queue is small).
Bulk sources invalidate by re-inserting the affected cells, which means
`untagged_cells` / `tier_holes` / `dedup_complete_edges` need to report WHICH
keys they touched — today they do not. That reverse mapping is the real work, and
it is a prerequisite, not a detail.

A cheaper intermediate worth measuring first: the claim makes THREE passes over
the candidate set (two `best_class` calls plus selection). Folding the two
`best_class` calls into one pass is semantics-preserving and buys ~3x on whatever
the candidate count is, with none of the invalidation exposure above.

**5.5 What to keep from the turn machinery.** `claim_next`'s sealed/window/
horizon turns are reservations, not orderings — they choose WHICH ordered set to
pop from, so they survive the change untouched. That matters: the rank encodes
starvation horizons and deadlines that were hard-won, and ClickHouse's move to
round-robin (PR #46247) is explicitly NOT what we want to copy, because it would
discard them.

**Do not start 5.x before the index has landed and been measured.** The ordered
index is the same maintenance problem with a richer key, and every ordering
hazard in §4 applies to it with more edges.

## 6. Verification

| signal | before | after the index | after 5.x |
|---|---|---|---|
| `coordinator_claim.avg_us` | 4,686 | expect < 100 | flat as N grows |
| `coordinator_claim.max_ms` | 3,289 | expect < 50 | flat |
| `journal_hold` duty cycle | 41% | expect < 2% | flat |
| `claimable_tasks` | — | should track `tasks_pending` | same |
| `maintenance_task_started` rate | 586/min | must not regress | scales with slots |

If `claimable_tasks` drifts toward `tasks_complete`, the permissive set is not
being re-derived and the scan is quietly paying for dead tasks again.
