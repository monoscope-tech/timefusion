# Maintenance dispatch: one claimer, and units that yield

Implementation plan for two related changes to the maintenance coordinator:

- **Part A** — replace 66 workers contending on one journal mutex with a single
  dispatcher feeding them over a channel.
- **Part B** — make long units yield cooperatively, so a stalled or
  compute-heavy unit stops monopolising its runtime thread.

They share a motivation (a unit must not be able to hold the fleet up) but are
independently landable, and Part B is the cheaper and safer of the two.

---

## 1. Evidence

Measured on prod, uptime 13,337 s, 44 runtime threads / 66 slots:

| `timefusion_stats` (component=`block`) | value | reading |
|---|---|---|
| `coordinator_claim.count` | 2,896,518 | **217 claims/sec** |
| `coordinator_claim.avg_us` | 4,693 | 4.7 ms per claim |
| `coordinator_claim.max_ms` | **1,730** | one claim stalled everyone 1.7 s |
| `coordinator_claim.total_ms` | 13,594,443 | **≈1.02 worker-equivalents** |
| `journal_lock_wait.total_ms` | 10,576,024 | **≈0.79 workers** merely WAITING |
| `journal_lock_wait.max_ms` | 1,844 | worst wait |
| `journal_hold.total_ms` | 5,440,948 | lock held **41% of wall-clock** |

So ~1.8 worker-equivalents are spent on claim plus lock-wait, and the lock is
occupied 41% of the time.

**Why this is worse than a throughput tax.** `self.journal()` takes a
`std::sync::Mutex`, which blocks the OS THREAD, not just the task. Tokio's own
scheduler post describes the failure mode exactly: workers pile up behind one
critical section until "all runtime workers are asleep behind one critical
section; there is no free worker left to steal the otherwise runnable tasks."
With 66 slots on 44 threads and a measured 1.73 s hold, that is reachable.

**Why it appeared now.** It is a consequence of raising slots 10 → 66
(`8aaa224f`). Claim cost scales with the BACKLOG (`claim_next` makes up to three
passes over an operation's task list) while claim RATE scales with the SLOTS.
Both went up. At 10 slots this was ~0.1% and genuinely ignorable.

---

## 2. Prior art

| system | pattern | what we take |
|---|---|---|
| ClickHouse `MergeTreeBackgroundExecutor` | a scheduler selects, worker threads only execute | the ARCHITECTURE of Part A |
| ClickHouse (PR #46247) | round-robin instead of strict priority, because big merges starved | **declined** — our rank encodes starvation horizons, deadlines and turn reservations |
| ClickHouse | tasks split into ordered **steps**; long tasks yield between them | the IDEA behind Part B |
| RocksDB SuperVersion | cheap immutable snapshot under the mutex, expensive work outside | the MECHANISM inside Part A's dispatcher |
| Go / Tokio schedulers | per-worker local queues, global queue only on overflow; steal-half | why one shared queue is the thing to remove |

**Declined: snapshot-outside-lock applied directly to all 66 workers.** Each
would rank the same snapshot independently, pick the same top-ranked task, and
collide on commit — trading lock hold for retry churn. One selector has no
collisions, so the dispatcher subsumes it.

---

## 3. Part A — single dispatcher, workers over a channel

### 3.1 Current shape

`src/database/rollup.rs` spawns `coordinator_job_slots()` identical workers:

```
for worker in 0..coordinator_job_workers {
    loop { run_maintenance_coordinator_once() }        // claims + runs
}
```

`run_maintenance_coordinator_once` walks a CYCLE of operations and, per
operation, calls `run_coordinator_{dedup,rollup,compaction}_once`, each of which
reaches `claim_coordinator_task` → `self.journal()` → `claim_next`.

So every worker is both a SELECTOR and an EXECUTOR, and selection is the
contended part.

### 3.2 Target shape

```
dispatcher task (1)                        worker tasks (N = coordinator_job_slots)
------------------                         ----------------------------------------
loop {                                     loop {
  req = requests.recv().await     <-- a      (reply_tx, reply_rx) = oneshot()
  snapshot = journal.snapshot()   <-- µs     requests.send(reply_tx).await
  best = rank(snapshot)           <-- no     unit = reply_rx.await
  unit = claim(best)              <-- µs     run(unit)      // never touches the
  req.reply.send(unit)                     }                //   journal at all
}
```

- The dispatcher is the ONLY holder of the journal for claiming.
- **A unit is claimed only in response to a request from an idle worker**, so
  nothing is ever Running-stamped while waiting to start (see §3.4).
- Backpressure is inherent: no request, no claim.
- Ranking happens with NO lock held (RocksDB SuperVersion); the lock is taken
  twice, each time for microseconds — once to clone the snapshot, once to stamp.

### 3.3 Ordering constraint that must be preserved

`maintain.rs` takes the light rewrite permit **before** the claim, with this
comment:

> Take the rewrite permit BEFORE the claim, never inside `stage_hot_bin`:
> blocking on it after `claim_next` has stamped the unit Running spends the
> unit's whole deadline in a queue.

The dispatcher must therefore acquire the permit before claiming, and hand the
`OwnedSemaphorePermit` to the worker along with the task. This is natural — the
permit is already `Owned` and `Send`.

### 3.4 Shutdown is what decides the handoff model

The database restarts a couple of times a day, so shutdown is a HOT path, not an
edge case. Three facts from the code make it decisive:

1. `TaskJournal::mark_running` does `attempts += 1` — claiming costs an attempt.
2. `TaskLease::drop` on a still-`Running` unit calls `abandon_running`, which
   records `WORKER_FAILURE_REASON` ("worker_error"), applies exponential backoff,
   and at `attempts >= 2` calls `split_task(.., SplitTrigger::RepeatedFailure)`.
3. `QUARANTINE_ATTEMPTS = 2`.

**Therefore a PUSH design is unsafe here.** If the dispatcher claims ahead into a
channel, every unit sitting in that channel at shutdown is stamped Running,
costs an attempt, and is abandoned as a worker failure despite never having run a
row. At two restarts a day, a unit that is unlucky twice gets SPLIT or
QUARANTINED for someone else's deploy.

**So the dispatcher PULLS, it does not push.** Workers ask for work; the
dispatcher claims exactly one unit in response and hands it back:

```rust
// Worker: ask, then run. No unit is ever claimed without a worker already
// waiting to execute it, so a restart cannot abandon work that never started.
let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
requests_tx.send(WorkRequest { reply: reply_tx }).await?;
let Some(unit) = reply_rx.await? else { /* idle backoff */ };
run(unit).await;
```

The single-claimer property (and therefore the whole lock win) is preserved: only
the dispatcher touches the journal. What is given up is pipelining — a worker
waits one channel round-trip between units. At 217 claims/sec that is noise
against a 4.7 ms claim.

This also removes §3.4's earlier concern about capacity burning deadlines: there
is no queue depth to tune, because there is no queue of claimed work.

### 3.5 Fix the unstarted-abandon penalty regardless

Even today, without any dispatcher, a deploy that kills a unit one second after
it claimed charges it a full attempt and a `worker_error`. With twice-daily
restarts that is real churn, and it is wrong: nothing failed.

Add an explicit "never started" release:

```rust
/// Give a claimed unit back WITHOUT charging it an attempt.
///
/// `mark_running` increments `attempts`, and `abandon_running` treats a Running
/// unit as a worker failure — backoff, then `RepeatedFailure` splitting at two.
/// Neither is true of a unit the process simply shut down on before it did any
/// work, and at a couple of restarts a day that misclassification compounds.
pub fn release_unstarted(&mut self, key: &TaskKey) -> bool { .. }   // state -> Pending, attempts -= 1
```

`TaskLease` gains a `started: AtomicBool`, set at the unit's first real work
(the existing `note_unit_progress` site is the natural hook, or explicitly at the
first phase past `admission`). `Drop` routes on it:

- `started == false` → `release_unstarted` — requeue, no attempt, no backoff.
- `started == true` → existing `abandon_running`.

This is independently valuable and should land BEFORE the dispatcher, because it
also reduces churn from the restarts we already do.

### 3.6 Shutdown ordering for the dispatcher

`shutdown_by` already: flushes the rollup journal, cancels
`maintenance_shutdown`, closes `maintenance_tasks_tracker`, waits with a
deadline. The dispatcher slots in as:

1. `maintenance_shutdown.cancel()` — the dispatcher's `select!` returns on the
   cancellation branch and it stops claiming FIRST. This is the ordering that
   matters: no new unit may be claimed once teardown starts.
2. Drop the request receiver. Workers blocked in `reply_rx.await` get
   `Err(RecvError)` and exit their loop — no unit was claimed for them.
3. Executors already running finish or hit the deadline, exactly as today.

Because nothing is claimed without a waiting worker, the set of units needing
requeue at shutdown is the same as today's: only genuinely in-flight work.

### 3.7 Rollout: default ON

No flag. The dispatcher ships enabled, the per-worker claim path is deleted in
the same commit, and if it does not hold up in production the change is
REVERTED rather than toggled. Two live claim paths is the configuration this
codebase has repeatedly been bitten by, and a dark-launched path is one nobody
reads.

## 4. Part B — cooperative yielding inside units

### 4.1 Problem

A maintenance unit is one long `async` body, but its expensive sections are
**synchronous CPU inside `poll()`** (parquet decode, sort, merge). Tokio's
cooperative budget only applies to tokio's own primitives, so a DataFusion loop
can hold a runtime thread for a long time without an await point. Consequences:

- Other units on that thread do not run, even when they are ready.
- A unit parked on S3 cannot be resumed promptly if all threads are busy in
  compute.
- `runtime_lag_ms` rises, which throttles the admission ceiling — maintenance
  throttles itself.
- **Shutdown is slow and unbounded.** `shutdown_by` notes that "a unit mid-sort
  observes cancellation only at its next checkpoint, and one was measured running
  5,051 s against a 900 s deadline", and `PERMIT_PHASES` records the same trap:
  "`tokio::time::timeout` cannot preempt a future that never reaches an await
  point". Await points are precisely what Part B adds, so it is what makes the
  EXISTING cancellation and timeout machinery able to fire at all. At a couple of
  restarts a day this is the change with the most direct operational payoff.

### 4.2 Mechanism: `tokio::task::consume_budget()`

Stable since tokio 1.24 (we run 1.48). It consumes one unit of the task's
cooperative budget and yields back to the scheduler when the budget is
exhausted. It is the intended API for long CPU-bound work inside async, and it is
cheaper than `yield_now()`, which yields UNCONDITIONALLY — a per-batch
`yield_now()` would add a scheduling round-trip to every batch.

### 4.3 Where to put it

The natural checkpoints already exist: every place we report progress is a place
we have just finished a chunk of work.

| site | current | add |
|---|---|---|
| `maintain.rs:6384` | `note_unit_progress(batch.num_rows())` | `consume_budget().await` |
| `compact.rs:1484` | `note_unit_progress(batch.num_rows())` | `consume_budget().await` |

Implement it once, not at each call site, by making the progress helper async
where it is already in an async context:

```rust
/// Report rows AND give the runtime a chance to switch units.
///
/// `consume_budget` yields only when this task's cooperative budget is spent, so
/// a short unit pays nothing while a long decode loop reliably lets its
/// neighbours run. `yield_now` would round-trip the scheduler on EVERY batch.
pub(crate) async fn note_unit_progress_yielding(rows: usize) {
    note_unit_progress(rows);
    tokio::task::consume_budget().await;
}
```

Keep the existing sync `note_unit_progress` for the non-async callers
(`PlanProgress::sample` runs on a timer task and must not yield there).

### 4.4 The ClickHouse "steps" idea, scoped

ClickHouse splits a task into ordered steps so a long merge yields between them
and short merges interleave. Our equivalent, without restructuring units:

- `consume_budget()` at batch boundaries (above) — the fine-grained version.
- A yield between the PHASES already named in `PERMIT_PHASES`
  (`claim`, `admission`, `resolve_table`, `compaction_files`, `processed_bytes`,
  `repair_sorted_probe`, `resume_scan`, `staging`). Those are genuine step
  boundaries and already instrumented.

This does NOT address `permit_held_seconds = 6300` (a unit holding a light permit
1.75 h). That needs the permit released across phases it does not need, which is
a separate change and is deliberately out of scope here.

---

## 5. Tests

**Part A**
1. `dispatcher_claims_are_the_only_journal_claimants` — run the coordinator with
   N workers, assert `coordinator_claim.count` grows by exactly the number of
   dispatched units (one claimer), not N×.
2. `a_dropped_dispatch_queue_requeues_every_lease` — fill the channel, drop the
   receiver, assert the journal shows every unit queued again, none Running.
3. `dispatch_does_not_rank_further_ahead_than_capacity` — assert Running-stamped
   units never exceed `capacity + slots`, the deadline-burn guard from §3.4.
4. `a_slow_worker_does_not_stall_the_dispatcher` — one worker parked, assert
   other workers keep receiving.
5. Re-run the existing admission/coordinator suites unchanged (323 tests).

**Part B**
6. `a_long_unit_yields_to_its_neighbours` — two units on a single-threaded
   runtime, one CPU-heavy with progress checkpoints; assert the second makes
   progress before the first completes. This FAILS today and is the regression
   guard.
7. Assert `consume_budget` is not called from `PlanProgress::sample` (timer
   context) — a compile-level separation, enforced by keeping that helper sync.

---

## 6. Verification in production

Land Part B first (low risk), then Part A.

| signal | now | expected after A | expected after B |
|---|---|---|---|
| `coordinator_claim.total_ms` | 13,594,443 | **↓ ~N×** (one claimer) | — |
| `coordinator_claim.max_ms` | 1,730 | ↓ to ms | — |
| `journal_lock_wait.total_ms` | 10,576,024 | **↓ toward zero** | — |
| `journal_hold` duty cycle | 41% | < 5% | — |
| `maintenance_task_started` rate | 586/min | ≥ same (must not regress) | ≥ same |
| `runtime.scheduling_lag_ms` avg | ~1–8 ms | — | ↓ or flat |
| query p50 under load | 0.49 s | flat | **↓ or flat** |

**Stop condition for Part A:** if `maintenance_task_started` rate drops, the
dispatcher round-trip has become the bottleneck. Do NOT respond by claiming
ahead — that reintroduces the shutdown penalty in §3.4. Respond by letting the
dispatcher answer several pending requests from ONE snapshot, which keeps
"claimed only for a waiting worker" intact.

---

## 7. Risks

| risk | mitigation |
|---|---|
| Dispatcher becomes a single point of throughput | Batch per scan; measure `maintenance_task_started` rate as the gate |
| Units burn deadlines queued while Running-stamped | Capacity ≤ slots/4; test 3 |
| Lease leak on shutdown | Test 2; dropping the receiver must requeue |
| Two claim paths coexisting | Flag deleted in the immediate follow-up, not "later" |
| `consume_budget` in the wrong context | Keep the sync helper for timer tasks; test 7 |
| Yielding too often costs throughput | `consume_budget` is budget-aware, unlike `yield_now`; measure task rate |

---

## 8. Order of work

1. **`release_unstarted`** (§3.5). Smallest, fixes a misclassification we pay on
   every deploy TODAY, and is a prerequisite for reasoning about A's shutdown.
2. **B** — `consume_budget` at the two progress checkpoints + test 6. One commit,
   low risk, and the change with the most direct shutdown payoff.
3. Observe `scheduling_lag_ms`, task rate, and shutdown duration for one deploy.
4. **A**, default ON, per-worker claim path deleted in the SAME commit. Tests 1–5.
5. Observe §6. If it does not hold up, revert — do not add a toggle.
