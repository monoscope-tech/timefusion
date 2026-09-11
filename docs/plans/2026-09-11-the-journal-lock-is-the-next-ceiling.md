# The maintenance journal lock is the next 10x ceiling

2026-09-11. Found while verifying the rollup no-op skip, which makes units
cheaper and therefore makes this the binding constraint sooner.

## The measurement

`timefusion_stats`, component `block`, two independent processes:

| | 11h process | 34 min post-deploy |
|---|---:|---:|
| `journal_hold.count` | 1,347,689 | 69,082 |
| `journal_hold.avg_us` | 5,859 | 5,452 |
| `journal_hold.total_ms` | 7,896,251 | 376,687 |
| `journal_hold.max_ms` | 3,682 | 1,855 |
| `journal_lock_wait.total_ms` | 4,697,938 | 131,744 |
| `journal_lock_wait.max_ms` | 3,271 | 2,631 |

**The lock is HELD for ~19% of wall-clock time**, on both processes:
7,896s of 39,600s, and 377s of 2,040s. Workers additionally spent 4,698s
*waiting* for it over the 11h process.

## Why it is a ceiling and not just a cost

`Database::journal()` takes `self.maintenance_tasks`, a single
`std::sync::Mutex<TaskJournal>` (`database/mod.rs:3308`). Every claim, enqueue,
invalidate, publish, complete, retry and cursor update in the whole maintenance
system serializes through it — `checkpoint()` alone has 46 call sites.

And `checkpoint()`, called on each of those, does this **while holding the
mutex**:

1. serializes the dirty records,
2. `wal.write_all()` then **`wal.sync_all()` — a real fsync**,
3. rewrites the ENTIRE snapshot via `compact()` whenever the WAL passes
   `JOURNAL_COMPACT_BYTES`,
4. `publish_statistics()`.

The existing comment is explicit that this was already understood as a latency
source — "the `fsync` here is the blocking syscall that
`block.journal_hold.max_ms = 2,380` on prod was measuring (2026-08-24)" — and
the fix applied then was `without_blocking_the_worker`, which moves the syscall
off the tokio worker. It deliberately did **not** change the locking: "the mutex
is still held across it, so durability ordering is unchanged."

That solved the symptom it was aimed at (a held tokio worker stalling unrelated
tasks, which is what `SELECT 1` costing seconds looked like). It does not touch
the throughput ceiling, because the lock duty cycle is unchanged.

**The arithmetic.** 69,082 acquisitions in 34 minutes is 2,032/min at 5.45 ms
each = 11.1 s of lock per minute = 18.5% duty cycle. Duty cycle scales linearly
with task rate and the lock is global, so:

| load | journal lock duty cycle |
|---|---:|
| 1x (today) | ~19% |
| 5x | ~93% |
| 10x | **185% — impossible** |

Maintenance throughput cannot reach 10x with this design regardless of cores,
pool sizes or per-unit cost. It is the first hard wall on the stated 10-100x
goal that is a *serialization* limit rather than a capacity one, which is why it
deserves attention before another round of per-unit tuning.

Note the no-op skip (shipped today, `6b533469`) *raises* the pressure here: it
removes ~50 seconds of work from a unit while leaving its journal traffic — a
claim, a complete, and their checkpoints — intact. Cheaper units mean more units
per second mean more lock acquisitions per second. Good change, but it spends
its winnings partly into this.

## The prior art, and why it fits

Group commit. Postgres (`commit_delay`/`commit_siblings`), RocksDB's WAL group
leader, and ClickHouse's batched system-log flushes all solve exactly this:
many small durable appends contending on one sync.

The shape here:

- Hold the mutex only long enough to mutate the in-memory journal and push the
  dirty records onto a shared buffer. Release.
- One writer task drains the buffer, does a single `write_all` + `sync_all` for
  the whole batch, and wakes the waiters whose records are now durable.
- A caller that needs durability awaits its batch; a caller that does not
  (most of the 46 sites are bookkeeping, not acked writes) does not wait at all.
- `compact()` moves out of the hot path entirely — it is a periodic maintenance
  action on the journal, not something a task completion should ever pay for.

Amortization is the whole point: 2,032 fsyncs/min becomes ~60 (one per 1 s
batch, say), and the duty cycle stops tracking the task rate.

**Before building it**, get the number this document does not have: the fsync's
share of the 5.45 ms. The average spans cheap acquisitions (`checkpoint()`
early-returns when nothing is dirty) and expensive ones, so the group-commit
win is bounded by a figure nobody has measured yet. Split `journal_hold` into
dirty and clean acquisitions, or time `sync_all` separately — one cheap
instrumentation pass answers it, and it decides whether this is a 5x lever or a
1.2x one.

## Risk

Durability ordering is the thing to be careful with. The current design is
trivially correct: the mutex covers the mutation and its fsync together, so no
observer can see a journal state that is not durable. A group-commit version
must keep "a task is Complete in memory" from outliving "its record is durable"
in any way a crash could expose. The safe formulation is that the in-memory
mutation is published only after its batch syncs, which is what the waiters
above are for.

This is not a change to make unattended.
