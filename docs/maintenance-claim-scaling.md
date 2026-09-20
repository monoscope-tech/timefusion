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

## 5. What the deploy actually showed, and what it changes

Measured over 459 s of steady state after the index and the fold landed:

| | before | after | change |
|---|---|---|---|
| `coordinator_claim` avg | 4.66 ms | **2.94 ms** | 1.6x |
| `journal_hold` duty cycle | 41% | **16.2%** | 2.5x |
| `journal_lock_wait` | 0.80 workers | **0.58 workers** | 1.4x |
| **worst single claim** | 3,289 ms | **394 ms** | **8.3x** |
| candidates walked | 94,372 | 1,324 | 71x |

**The 71x fewer candidates bought 1.6x, and that is the finding.** §2's
arithmetic mis-attributed the cost: the 94k walk only ran the CHEAP filter
(`state` check plus an operation compare, ~3 ns). `rank` and
`dependencies_complete` only ever ran on the ~1,000 candidates that PASSED it.
So the scan was ~18% of the claim, not 90%.

The fold is the proof. It moved rank evaluations from 3 per candidate to 2 and
produced 4.66 -> 2.94 ms — almost exactly 3:2. **`rank` is the claim.** It is
microseconds per call because it does two `hole_rank` set probes,
`scheduling_class`, and a `dedup_complete_edges` lookup.

The tail is the part that already paid off: a worst claim of 394 ms instead of
3.3 s. That is the number that parks runtime threads.

So the lever is EVALUATIONS PER CANDIDATE, not candidates scanned.

## 5a. Rank once per claim (landed with this)

The combined pass ranked every candidate and the selection pass ranked them all
again. Selection now reuses the first pass's `(rank, task)` pairs: 2 -> 1.

No invalidation exposure whatsoever — the reuse lives for a single claim.

## 5b. Rank once per CHANGE — the endgame

| | rank computations per claim |
|---|---|
| originally | ~3 x 1,300 |
| after the fold | ~2 x 1,300 |
| after 5a | ~1 x 1,300 |
| **after 5b** | **~1** |

An ordered set keyed by `(cached_rank, key)`, popped instead of scanned.

**Prior art removed the hard part.** An earlier draft here worried that a stale
key means claiming in the WRONG ORDER, silently, and scoped a reverse mapping so
`untagged_cells` / `tier_holes` / `dedup_complete_edges` could report which keys
they touched, plus a promotion queue for the day-graded `starved` term.

Both are unnecessary. The standard alternative to `decrease-key` is lazy
validation: reinsert rather than update in place, and VERIFY ON POP —
"store the current best priority outside the heap and ignore stale heap entries
when popped". Applied here:

    pop the minimum (cached_rank, key)
    recompute that ONE task's rank
      equal    -> claim it
      differs  -> reinsert at the fresh rank, pop again

A claim is therefore NEVER made on a stale rank, because the popped entry is
verified before it is used. A stale entry costs one rank computation and a
reinsert — not a mis-ordered claim. The silent failure mode that made this
risky does not exist in this form.

RocksDB's shape is the same idea from the other side: `ComputeCompactionScore`
runs when `VersionStorageInfo` is UPDATED, not when picking, and the scores hang
off an immutable Version so picking only reads. We cannot copy the immutable
version (ours is a 31 MB `Vec`), but "compute on change, read at pick" is the
principle, and verify-on-pop is how a mutable structure gets there safely.

**What is still needed:** ranks must be written into the set when a task is
mutated (`mark_dirty` already routes every mutation), and a generation counter
on the bulk sources is worth adding — NOT for correctness, which verify-on-pop
already gives, but to skip pointless verifies after a bulk change.

**Ordering the set is then free.** Once entries carry a rank, `BTreeSet` gives
`O(log N)` selection, and keying per operation shards it six ways so lanes stop
contending. But the win is the rank count, not the ordering: ~1,300 -> ~1.

## 5d. Concrete plan, and where Rust does the work for us

The invariant that matters is *a claim is never made on a stale rank*. In most
languages that is a code-review rule. Here it can be a COMPILE ERROR.

**D1. Make the rank a type, not a tuple.**

```rust
// Was: type Rank = (u8, u8, u8, u8, u8, i64, i64, i64);
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
struct Rank { class: u8, hole_present: u8, starved: u8, hole: u8,
              not_adjacent: u8, width: i64, benefit: i64, order: i64 }
```

Derived `Ord` keeps the exact field order, so ordering semantics are unchanged —
but the fields get names, and `BTreeSet<(Rank, _)>` sorts correctly for free.
Today an accidental field reorder silently changes scheduling priority; after
this it is a named struct literal.

**D2. Make "verified" unforgeable — the core Rust leverage.**

```rust
/// A rank recomputed against the CURRENT world and found equal to the cached one.
///
/// Only `pop_verified` mints this, and `claim` demands it, so "claimed on a stale
/// rank" is not a bug you can write — it does not typecheck.
#[must_use]
pub(crate) struct Verified(Rank);
```

`claim_next` takes `Verified`, never a bare `Rank`. The silent-wrong-order
failure mode stops being a thing to test for and becomes a thing to compile.

**D3. Cheap keys — stop hashing strings in the hot path.**

`TaskKey` holds three `String`s, so every `self.task(key)` in the candidate walk
pays a string hash. A generational slot makes it a `Copy` integer:

```rust
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Slot { index: u32, generation: u32 }   // generation survives `retain_tasks`
```

The ordered set becomes `BTreeSet<(Rank, Slot)>`: comparisons are integer
compares, lookups are a bounds-checked index, and a slot freed by pruning fails
its generation check instead of aliasing a new task. This is the arena/slotmap
pattern, and it is what makes the pop path allocation-free.

**D4. Interior mutability for the cache, so the walk stays `&self`.**

`Cell<Option<Rank>>` on the task: `Cell` is `Copy`-only and has no runtime cost,
so caching during an immutable walk needs no `RefCell` borrow checks and no
`&mut` plumbing. Exclude it from `PartialEq`/`Serialize` by hand — a cache must
not make two equal tasks compare unequal, and must never reach the journal file.

**D5. No allocation per claim.**

5a introduces a per-claim `Vec`. Hoist it to a scratch buffer owned by the
journal and `clear()` it each time: at 167 claims/sec that is ~16 MB/s of
allocation avoided, and the buffer reaches steady-state capacity immediately.

**D6. Prove the invariant with `proptest` (already a dependency).**

The property is exact and worth stating as one:

> For any sequence of enqueues, mutations, time advances and bulk-source
> changes, `claim_next` returns the same task as a brute-force scan that ranks
> every candidate from scratch.

That is the whole correctness argument for 5b in one test, and proptest will find
the interleaving a hand-written case would not. Pair it with a shrink-friendly
model: a `Vec<MaintenanceTask>` and the naive selector.

**D7. Sequence.**

1. D1 (Rank struct) — mechanical, no behaviour change, lands alone.
2. D6's property test against the CURRENT selector — establishes the oracle
   while the implementation is still the simple one.
3. D4 + cached rank, still linear scan. Measure: expect ~2.94 ms -> ~50 us.
4. D2 + D3 + ordered set with verify-on-pop. Measure: expect ~1 rank per claim.
5. D5 once the shape is settled.

Steps 1-3 are where the measured win is. Step 4 is what makes it hold at 50x.
The property test from step 2 guards every step after it.

## 5c. What to keep from the turn machinery

`claim_next`'s sealed/window/horizon turns are reservations, not orderings — they
choose WHICH set to pop from, so they survive untouched. The rank encodes
starvation horizons and deadlines that were hard-won, and ClickHouse's move to
round-robin (PR #46247) is explicitly not what to copy here.

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
