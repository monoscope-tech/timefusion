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

## 5d. Landed: D1, D4, D5 and the property that guards them

The invariant that matters is *a claim is never made on a stale rank*. In most
languages that is a code-review rule. Here most of it is a type or a property.

**D1. `Rank` is a struct, not a tuple.** Derived `Ord` compares in declaration
order, so the field order IS the scheduling policy — now it is eight named
fields instead of eight positions, and `.4` at a call site is `.not_adjacent`.

**D4. The rank is memoised, and the cache key is EXACT.**

The insight that makes this sound rather than heuristic: `rank` depends on time
only through STEP functions of `waited = now - slice.end` — the frontier interval
`[start, end + 24 h]`, the starvation floor at `end + 3 d` (which also flips the
recency sign), and the graded term's day steps past `end + 31 d`. So
`scheduling_class_until` returns the class AND the first `now` at which it could
change, computed next to the comparisons it mirrors. No expiry guess, no drift.

A cached entry is trusted on four witnesses:

| witness | moved by |
|---|---|
| `valid_until` | time |
| `rank_generation` | `set_tier_holes`, `set_untagged_cells`, `restore_untagged_cells`, `clear_untagged_cell` |
| `dedup_generation` | every dedup completion — consulted ONLY by Dedup units, or ~1,300 entries would flush a few times a second |
| `RankInputs` | the task's own `scheduling_width()` and `input.files` |

`RankInputs` is the interesting one. The obvious design clears the cache from
every mutation site, but `snapshot.tasks[index]` is written in EIGHT places that
bypass `task_mut`, and a missed hook there mis-orders claims silently — the same
shape as the `mark_dirty`-before-`insert_task` bug in §4, which the compiler
could not catch. Comparing two scalar fields costs far less than `rank` and
cannot be forgotten. The cache carries no invalidation protocol at all.

`Cell<Option<CachedRank>>`, not `RefCell`: `CachedRank` is `Copy`, so the walk
memoises through `&MaintenanceTask` with no borrow flag and no `&mut` plumbing.
`Clone` yields an EMPTY slot, which makes `insert_task` and every task that
escapes via `claim_next` safe for free; `PartialEq` is unconditional, because a
cache must not make two equal tasks compare unequal. `#[serde(skip)]` keeps it
out of the journal file.

**Per-operation claim index.** `claimable` is now keyed by `Operation`, so a
claim walks its own lane's keys instead of all six lanes' and discarding
five-sixths after the `TaskKey` hash.

**D5. The claim allocates nothing.** The ranked-candidate buffer lives on the
journal and is reused; it holds INDICES, not references, so it can be owned by
the same `self` the walk borrows. At 167 claims/sec that is ~16 MB/s of pure
allocation churn removed.

**D6. One property is the whole correctness argument.**

> After any interleaving of enqueues, completions, claims, time advances, input
> rewrites and bulk-set replacements, every memoised rank equals a freshly
> computed one.

`rank_uncached` is both the only place rank is computed and the oracle it is
checked against. The time strategy straddles every boundary deliberately, and
the input-rewrite step writes through `snapshot.tasks[index]` on purpose — the
path a hook-based design would have missed.

Two more pin what the property alone cannot:

- **The memo must actually HIT.** A cache that never hits satisfies
  transparency vacuously. So: 50 ranks over an unchanged world compute ZERO, and
  crossing `valid_until` or moving a bulk source computes exactly one.
- **`valid_until` must be honest.** The class is probed at arbitrary points
  inside `[now, valid_until)` and must be unchanged. Recomputing one claim early
  costs a rank; recomputing one late mis-orders a claim.

**Measure it with `maintenance_rank_computations_total`** against
`coordinator_claim.count`. That ratio is the scheduler's efficiency in one
number: ~1,300 before, and it should now track newly enqueued or newly aged
units rather than candidates walked. A ratio climbing back toward the candidate
count means something invalidates every claim — suspect the generations first.

Expect ~0.2-0.5 ms per claim, NOT the 50 us an earlier draft implied: the walk
still pays one `TaskKey` hash (three strings) per candidate. That residual is
what D3 below exists to remove, so do not read 0.3 ms as the memo failing.

## 5e. Still to do — D2 and D3, the ordered set

Held back deliberately: verify-on-pop has two design gaps §5b glosses over, and
neither blocks the win above.

**The reservation turns break naive pop-min.** On a sealed turn you pop past
every class-0 entry to reach sealed work. Worse, on a WINDOW turn the rank order
is inverted with respect to what the turn wants — starved units sort FIRST
(small `starved`) and window units LAST (`starved = u8::MAX`) — so with no
frontier work pending, pop-min walks the entire starved backlog, and that
degenerate case is exactly the deep backlog the turn exists to serve. It needs
per-band sets with lazy migration on pop, or a hybrid: ordered set for the normal
and sealed turns, memoised linear scan for window and horizon.

**A min-rank task failing `dependencies_complete`** must go to a side buffer for
the duration of the claim, or verify-on-pop loops on it forever.

When they are built, D2 and D3 are what make them safe and cheap:

- **D2, `#[must_use] struct Verified(Rank)`** — minted only by `pop_verified`,
  demanded by `claim`. "Claimed on a stale rank" stops being a thing to test for
  and becomes a thing that does not typecheck.
- **D3, a generational `Slot { index: u32, generation: u32 }`** replacing
  `TaskKey` in the set. Integer compares instead of three string hashes, and a
  slot freed by pruning fails its generation check rather than aliasing a new
  task.

## 5c. What to keep from the turn machinery

`claim_next`'s sealed/window/horizon turns are reservations, not orderings — they
choose WHICH set to pop from, so they survive untouched. The rank encodes
starvation horizons and deadlines that were hard-won, and ClickHouse's move to
round-robin (PR #46247) is explicitly not what to copy here.

## 6. Measured, 2026-09-21

Prod, a 677 s steady-state interval at 202 claims/sec (136,995 claims). Read as
INTERVAL deltas — an instantaneous read at 177 s uptime said 1.99 ms and was
cold-start contamination, not signal.

| | original | after the index | after the memo |
|---|---|---|---|
| ranks computed **per claim** | ~1,300 | ~1,300 | **0.023** |
| `coordinator_claim` avg | 4,686 us | 2,940 us | **339 us** |
| `coordinator_claim` max | 3,289 ms | 394 ms | **293 ms** |
| `journal_hold` duty cycle | 41% | 16.2% | **3.0%** |
| `journal_lock_wait` | 0.80 workers | 0.58 workers | **0.050 workers** |

**0.023 ranks per claim.** Not "once per claim" — once per FORTY claims, because
a claim usually finds every candidate's rank still valid on all four witnesses.
Against ~1,300 that is the whole point of the exercise, and it lands 8.7x on the
claim and 16x on time spent waiting for the journal mutex.

**339 us lands exactly in the 200-500 us band predicted, not the 50 us an
earlier draft implied.** That was the honest prediction and it held: with the
ranking gone, what remains is one `TaskKey` hash (three strings) per candidate,
which is what D3's generational `Slot` exists to remove. Do not read 339 us as
the memo underperforming — read it as the profile having moved, for the second
time, exactly where the arithmetic said it would.

The tail is now 293 ms against an original 3,289 ms. That is the number that
parks runtime threads, and it is no longer in a range where it can.

Backlogs at the same moment, for context on what this is buying: base_rollup 470,
dedup 272, sealed_consolidation 228, derived_rollup 108, hot_packing 17, repair 0
— against a wedge that ran base_rollup 2,198 -> 5,026 while committing nothing.

## 6a. What to watch

| signal | healthy | what a regression means |
|---|---|---|
| `maintenance_rank_computations_total` / `coordinator_claim.count` | ~0.02 | climbing toward the candidate count: something invalidates the memo every claim — suspect the two generations first |
| `coordinator_claim.avg_us` | ~340, flat as N grows | growth with the journal means the per-operation index is not being re-derived |
| `journal_hold` duty cycle | ~3% | — |
| `claimable_tasks` | tracks `tasks_pending` | drift toward `tasks_complete` means the permissive set is paying for dead tasks again |
| `maintenance_task_started` rate | scales with slots | must not regress |

