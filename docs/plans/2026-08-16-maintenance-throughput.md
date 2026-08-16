# Maintenance throughput, rollup liveness, and index repair — plan

Written 2026-08-16, from production measurements taken the same evening against
`b6c4a3e`. Every number below is observed, not estimated. Sources are
`timefusion_stats` over pgwire, timed pgwire queries, and `docker`/`ssh` on the
CapRover host.

Goal this plan serves: maintenance (dedup, sort, rollups, packing) must keep up
with current traffic **and** with 10x the current project count on the same
single node; rollups must be continuously up to date and actually used, so that
popular 14d and 30d queries return in ~1s for **every** project including
shipbubble; and the Tantivy reindex must be driven to a conclusion without
competing with production writers.

---

## 1. Measured starting state

### 1.1 Query latency — the user-visible failure

Timed from this machine over pgwire, `count(*)` on `otel_logs_and_spans` for
shipbubble (`28f62f01-46a1-400e-8195-da7bc3505b5b`):

| window | result |
|---|---|
| 1d | **41.0 s** (2,884,451 rows) |
| 7d | **timeout, >60 s** |
| 14d | **timeout, >60 s** |
| 30d | **timeout, >60 s** |

Target is 1 s. 14d and 30d do not complete at all. Server-side scan latency
distribution agrees: `scan.lat_p99_us_approx = 8.4 s`,
`scan.lat_p999_us_approx = 33.5 s`.

### 1.2 Maintenance is serialized to exactly one job

`src/database.rs:3045`:

```rust
AdmissionController::new(1, memory_limit_bytes, 1, 1)
```

and `src/database.rs:3652`:

```rust
const COORDINATOR_JOB_WORKERS: usize = 1;
// "Job concurrency is deliberately one; see the admission controller initialization."
```

CPU, object-read and object-write tokens are each **1**, so at most one unit is
ever admitted. Confirmed live: `maintenance.cpu_tokens_used = 1`,
`object_read_tokens_used = 1`, `object_write_tokens_used = 1`,
`tasks_running = 1`, `decoded_bytes_used = 512 MiB` (exactly one
`MAX_DECODED_BYTES` unit).

Meanwhile the decode budget is `cgroup * 3/4` = **~64 GB**, of which 512 MiB is
used — **99.2% idle** — and process memory is at `memory.charged_pct = 8`
(6.9 GB of the 85.9 GB limit).

This serialization was a deliberate 2026-08-16 safety response to maintenance
starving foreground health checks (the runtime-isolation comment above it is
the real fix for that; the token cap of 1 is belt-and-braces that went too far).

### 1.3 The backlog that serialization produces

| metric | value |
|---|---|
| `maintenance.backlog_bytes` | **20.26 TB** |
| `maintenance.sealed_compaction_debt_bytes` | **8.05 TB** |
| `maintenance.tasks_pending` | **18,397** |
| `maintenance.tasks_complete` | 2,742 |
| `maintenance.tasks_running` | **1** |
| `maintenance.oldest_task_age_seconds` | 39,142 (**10.9 h**) |
| `maintenance.dirty_bin_queue_depth` | 1,525 |
| `maintenance.dirty_bin_processed_total` | **0** |
| `maintenance.dedup_bins_committed_total` | **0** |
| `maintenance.dedup_waves_committed_total` | **0** |
| `maintenance.processed_bytes_per_second` | **0** |
| `maintenance.retry_reason` | `compaction_debt_remaining` |

Light hot-tail compaction is the only tier making progress
(`light_optimize_bins_committed_total = 63`).

### 1.4 Rollups are built zero times and used zero times

| metric | value |
|---|---|
| `rollup_output_rows_total` | **0** |
| `rollup_rebuilds_full_total` / `_incremental_total` | 0 / 0 |
| `rollup_hits_full_total` / `rollup_hits_hybrid_total` | **0 / 0** |
| `rollup_dirty_partitions` | 41 |
| `rollup_oldest_invalidation_age_seconds` | 112,640 (**31.3 h**) |
| `rollup_singleton_failures_total` | 0 |

Note `rollup_singleton_failures_total = 0`: the oversized-cohort failure mode
that motivated the original redesign is genuinely gone. Rollups now produce
nothing for the simpler reason that the coordinator never reaches them.

### 1.5 The single root cause linking all of it

`record_certification` (`src/database.rs:10755`) is the only grantor of
certification, and it runs off the **dedup** path. Because dedup commits
nothing, certification is never granted:

- `scan.cert_granted_total = 0`
- `scan.dedup_denied_never_certified_pct = **100.0**` (697 of 697)
- `scan.dedup_eligible = 710`, `scan.dedup_skipped = 3`

Two independent consequences, both fatal to the 1s goal:

1. **Read-side dedup is denied on every scan**, so `DedupExec` stays in every
   plan and each query pays full-set ordered dedup.
2. **Rollup routing requires a contiguous certified prefix** from the start of
   the window (comment at `src/database.rs:10792` states this explicitly, and
   names it as exactly why "7d and 30d windows never routed while 24h did").
   One uncertified day disqualifies the whole window.

So the chain is:

```
COORDINATOR_JOB_WORKERS = 1  (+ admission cpu/read/write = 1)
  -> dedup bins committed = 0
    -> record_certification never fires -> cert_granted_total = 0
      -> (a) read dedup denied 100%  -> DedupExec in every plan
      -> (b) no certified prefix     -> rollup routing hits = 0
        -> 14d/30d queries time out
```

Fixing throughput is therefore not one of three parallel workstreams. It is the
**precondition** for the other two. This is the single most important
conclusion in this document, and it changes the order of work.

### 1.6 Tantivy index repair

Three reconcile containers were OOM-killed today
(`timefusion-tantivy-reconcile-{streaming,4cpu,2cpu}`, all **exit 137**). They
ran as separate Docker containers on the same host as production, so they
competed with the server for the same cgroup-adjacent memory with no shared
admission control. `tantivy.recovery_pending_files = 0` and
`budget.tantivy_peak_mb = 1536`.

They were not stopped deliberately; they died. Any resumption that reuses the
"separate container" shape will reproduce this.

---

## 2. Design principles for the fix

1. **Bound work per unit, then run many units.** The 512 MiB `MAX_DECODED_BYTES`
   ceiling is correct and stays. Serialization on top of it is what is wrong.
2. **Admission must be driven by measured headroom, not a hard-coded 1.** The
   reason for the cap (protecting foreground liveness) is legitimate; the
   mechanism should be a real memory/CPU reading with a reserve, not a constant.
3. **Never let maintenance share Tokio workers with pgwire.** The existing
   dedicated `maintenance-runtime` already does this and must be preserved — it
   is what actually fixed the health-check starvation, and it is why raising job
   concurrency is now safe.
4. **Certification is the product.** Throughput work is judged by
   `cert_granted_total` rising and `never_certified_pct` falling, not by bytes
   processed.
5. **Newest-first everywhere.** Dashboards read recent data; an oldest-first
   drain spends a whole process lifetime on data nobody queries. The code
   already argues this for rollup backfill and the dedup drain; apply it to the
   sealed-compaction debt drain too.
6. **In-process, admission-controlled index repair.** No more sibling
   containers outside the memory governor.

---

## 3. Workstreams

### WS-1 — Parallel maintenance admission (unblocks everything)

**Change A — make job concurrency a function of cores, not a constant.**

`src/database.rs:3652`

```rust
const COORDINATOR_JOB_WORKERS: usize = 1;
```
becomes a derived value, `(cores / 8).clamp(2, 6)` by default, overridable by
`TIMEFUSION_COORDINATOR_JOB_WORKERS`. The coordinator runtime's worker threads
must scale with it (currently `(cores / 8).clamp(2, 4)`), so that admitted jobs
are actually polled rather than queued behind each other on two threads.

**Change B — admission tokens sized from real capacity.**

`src/database.rs:3045`

```rust
AdmissionController::new(1, memory_limit_bytes, 1, 1)
```
becomes cpu = job workers, object_reads/object_writes sized independently
(object I/O is latency-bound, not memory-bound, so it can exceed CPU — start at
`2 * jobs`). Decode budget stays `cgroup * 3/4` but is now genuinely reachable
because more than one unit can hold a reservation.

**Change C — a foreground-pressure brake that replaces the constant.**

Before admitting, check live `memory.charged_pct` and mem-buffer pressure. Above
a high-water mark (start at 70% of the cgroup, matching the existing maintenance
brake), admit nothing new and let in-flight units drain. This gives back the
safety the hard-coded `1` was providing, but only when pressure is real.
Instrument as `maintenance_admission_braked_total`.

**Verification.** `tasks_running` > 1 in prod; `decoded_bytes_used` well above
512 MiB; `processed_bytes_per_second` > 0; `dedup_bins_committed_total` rising;
pgwire p99 not regressing; no health-check failures; no exit-137.

### WS-2 — Drain the 20.3 TB backlog to a steady state

With WS-1 landed, the queue drains, but 20.3 TB will not clear in one pass and
must not be attempted in one pass.

- **Prioritize by query value, newest-first.** Recent dates first; within a
  date, projects by read frequency. shipbubble is explicitly in scope.
- **Separate the sealed-compaction debt (8.05 TB) from the live frontier.** The
  live frontier must never queue behind historical debt; that is what
  `retry_reason = compaction_debt_remaining` is reporting today. Give debt its
  own admission slice (a minority share) so it makes progress without starving
  the frontier.
- **Certification-first ordering.** Order work so that whole partitions become
  certifiable as early as possible: a partially deduped date grants no
  certification and therefore buys zero query latency. This is a scheduling
  change with an outsized payoff.

**Verification.** `backlog_bytes` monotonically decreasing across a sustained
window; `oldest_task_age_seconds` falling; `cert_granted_total` > 0 and rising;
`never_certified_pct` falling from 100.

### WS-3 — Rollup liveness and routing

Only meaningful once WS-2 produces certified prefixes.

- Confirm rollup build resumes (`rollup_output_rows_total` > 0,
  `rollup_rebuilds_incremental_total` > 0) and that
  `rollup_oldest_invalidation_age_seconds` (31.3 h) collapses toward the 15-min
  watermark.
- Then confirm routing: `rollup_hits_full_total` / `rollup_hits_hybrid_total`
  > 0 for 14d/30d dashboard shapes.
- Re-check the `rollup_miss_*` family for the dominant residual reason. Today
  the only non-zero misses are `rollup_miss_filter_not_eligible_total = 2` and
  `rollup_miss_missing_project_total = 3`, which are too small to act on until
  real traffic routes.

**Verification.** shipbubble 14d and 30d complete in ~1s, served by rollups; the
same for the other projects; `rollup_hits_*` > 0.

### WS-4 — Tantivy index repair, in-process and bounded

- Move reconcile/backfill **into the coordinator** as a task `Operation`, so it
  is subject to the same 512 MiB unit ceiling, admission tokens, and pressure
  brake as every other tier. No sibling containers.
- Keep it in the lowest priority band, below flush durability, dedup/rollup, and
  hot packing, so it can never compete with production writers — the explicit
  requirement in the goal.
- Bound peak by the existing `budget.tantivy_peak_mb = 1536` reservation and
  stream rather than collect.
- Drive to conclusion: track index coverage as a first-class stat and run until
  coverage is complete, then keep it incremental.

**Verification.** No exit-137; index coverage stat reaches complete; hybrid
indexed/raw search (PR #96) reports index hits; foreground write latency flat
across the repair window.

### WS-5 — 10x acceptance

- Replay production-shaped traffic at 10x project count and 10x ingest on this
  hardware.
- Acceptance: no unit exceeds 512 MiB reserved decode; `tasks_pending` bounded
  (not monotonically growing); eligible-rollup coverage trails the 15-min
  watermark by <= 5 min at p95; 14d/30d p95 <= 1s for all projects including
  shipbubble; zero OOM kills; zero exit-134.

---

## 4. Sequencing

WS-1 is a small, high-leverage change and lands first, alone, so its effect on
foreground latency is unambiguous. WS-2 is mostly scheduling policy and lands
second. WS-3 is expected to be largely emergent once WS-2 runs — it is verified
before it is coded, and only the residual misses are fixed. WS-4 is independent
of WS-1..3 and can land in parallel, but is deployed separately so an OOM can be
attributed. WS-5 is the gate.

Each step deploys on its own and is monitored before the next starts. Prod is a
single node with a live customer workload; batching these would make any
regression unattributable.

## 5. Risks

- **Raising job concurrency re-introduces foreground starvation.** Mitigated by
  the dedicated maintenance runtime (already in place, and the actual fix), plus
  the WS-1C pressure brake. Watch pgwire p99 and health checks; the kill switch
  is `TIMEFUSION_COORDINATOR_JOB_WORKERS=1`, which restores today's behavior
  exactly.
- **More concurrency means more OCC conflict on Delta commits.**
  `dml.occ_conflicts_total` is 0 today and must be watched;
  `rollup_occ_retries_total` likewise.
- **Memory.** Concurrency multiplies peak transient heap by the number of
  in-flight units (512 MiB each). At 6 jobs that is ~3 GB of tracked decode
  against 79 GB of headroom — safe, but the brake is what keeps it safe under a
  simultaneous ingest burst.
- **The 20.3 TB drain is long.** It must be explicitly treated as a background
  convergence with progress reporting, not a task expected to finish in a
  session.

## 6. Kill switches

| change | switch | restores |
|---|---|---|
| WS-1 job concurrency | `TIMEFUSION_COORDINATOR_JOB_WORKERS=1` | today's serialized behavior |
| WS-1 admission sizing | same variable drives token counts | today's `(1,1,1)` |
| WS-4 in-process repair | disable the repair `Operation` | no index repair, no sibling containers |
