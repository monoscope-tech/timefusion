# Long-term architecture: making the incident classes structurally impossible

Date: 2026-07-27. Input: two months of prod incidents (memory index), plus a
code survey of TimeFusion's write/memory/maintenance paths and monoscope's
ingest/ack contract. Goal: not "fix the bug", but change the architecture so
each *class* of incident can no longer occur.

## 1. The five recurring failure classes

Every incident since June falls into one of these:

| Class | Instances | Structural cause |
|---|---|---|
| **A. Silent ack-without-durability** | 2026-07-27 zero-row ack (adc59bf66), 2026-07-09 DLQ not-due commit-over, 2026-07-06 ce-type poisoning, gRPC direct-write drops (open), PubSub cache-miss/quota drops (open) | The "offset committed ⇒ row durable" invariant is enforced by *convention* in many call sites, not by construction in one place |
| **B. OOM death spiral** | 2026-07-04 (89GB), 2026-06-22, 2026-07-19 (concurrency=2), 2026-07-22 (85GiB cap), 2026-07-26/27 (4 kills/9h) | Multiple non-communicating memory systems + large *untracked* allocations (DML coalescer, scan decode, maintenance collect, flush encode) sharing one cgroup; no single meter, no universal admission control |
| **C. Wedge → backlog → spiral** | Flush-hang watchdog (07-01), DML off-lock rewrite (07-02), MERGE multi-source re-queue loop (07-26/27, open), light-optimize OCC starvation (07-20) | Global `flush_lock`; unified-table commit key collapses all tenants; emergency-flush path needs the same wedged lock; poison work is retried in place instead of quarantined |
| **D. Maintenance vs ingest contention** | Maintenance-pool sort starvation (07-21), OPTIMIZE partition-scope conflict (06-24), dedup OCC storms (06-2x, 07-14), file-count blowups → slow commits → slow flush | Compaction/dedup/DML/ingest/query are one process fighting over one commit log, one tokio runtime, one commit-lock per physical table |
| **E. Blind spots** | Visibility gap (06-17), parking invisibility, "lag-0 lies", this week's 9-min gap needed manual archaeology | Watchdogs check liveness/hourly parity, not per-minute continuity or consumer *progress*; durability tests used graceful `Drop`, never kill -9 (fixed by tests/kill_recovery.rs) |

The causal chain observed twice this month (survey confirmed mechanically):
DML/flush wedge → `flush_lock` held / holds orphaned → WAL reclaim floor pinned
→ MemBuffer hits 120% hard limit → inserts rejected (pre-WAL, so *not durable*)
→ WAL grows toward 192GB gate → untracked heap pushes cgroup → OOM →
restart → replay backlog → repeat.

## 2. Target architecture

### 2.0 Hard constraint: Kafka is optional

TimeFusion must remain a standalone single-node database — a simple deployment
has no Kafka, no external coordinator. So **TF's own WAL is the durability
substrate**, and every mechanism below must hold with Kafka absent. Kafka is a
*monoscope-side* ingest buffer only: it strengthens the pipeline's replay story
where present, and must never be a TF correctness dependency. Concretely this
kills the "Kafka as the WAL" idea from the earlier draft (was phase 6 — dropped)
and makes §2.4 (unwedge flush) and §2.3 (memory governor) the real backbone,
since on a single node there is no upstream buffer to absorb a wedge.

### 2.1 One durability contract (kills class A)

Principle: **an ack is emitted only by the code path that observed a durable
write or a durable hand-off — and there is exactly one such code path.** In TF
that means the WAL append; in monoscope, where Kafka *is* deployed, it means
the offset commit.

- **gRPC OTLP handler publishes to Kafka and acks after the produce flush.**
  Today it returns success immediately and does the TF write in a detached
  thread that drops on error (`OtlpServer.hs mkOtlpRpcHandler` /
  `grpcRunBackground`). This is the largest remaining silent-loss hole. After
  this change there is one write path (the Kafka consumer), so the adc59bf66
  discipline is enforced once.
- **PubSub/SDK path gets the same treatment**: `ProcessMessage.hs:169-181`
  still acks every decodable message while dropping rows on project-cache miss
  and quota; ids are random per attempt so replays duplicate. Route it through
  the same converter + deterministic-id + routeBatchOutcome machinery.
- **Idempotency everywhere, so at-least-once is always safe**: add
  `ON CONFLICT DO NOTHING` to the PG logs/spans unnest insert
  (`Telemetry.hs:1426`) — today only metrics have it, which is why `WriteTarget`
  narrowing is a *correctness* requirement instead of an optimization, and why
  re-drives are scary. Once both legs are idempotent, the answer to every
  "did we lose X?" is "rewind and replay, unconditionally".
- **Separate "drop" from "failure"**: quota drops and unresolvable-project
  drops must be *counted, logged, acked* — never DLQ'd (the adc59bf66 fix
  currently routes quota traffic into the parking ladder) and never silently
  acked. A single `IngestOutcome = Written | Dropped(reason) | Failed(leg)`
  type returned by the one write path makes the ambiguity unrepresentable.

### 2.2 Split TimeFusion into roles (kills classes B and D)

The process already has the seams; make them deployment topology:

Note: this is the *scaled* topology. The single-node deployment collapses all
three boxes into one process (today's shape) and must stay fully supported —
the split is opt-in via config, not a new requirement.

```
      ingest (pgwire/gRPC; Kafka only if deployed)
               │
   ┌───────────▼──────────┐      ┌─────────────────────┐
   │  INGEST node          │      │  MAINTENANCE worker  │
   │  pgwire/gRPC ingest   │      │  dedup, optimize,    │
   │  WAL + MemBuffer      │      │  vacuum, recompress, │
   │  flush → Delta        │ OCC  │  checkpoint, DML     │
   │  hot-window queries   │◄────►│  merges (coalescer)  │
   └───────────┬──────────┘  S3   └──────────▲──────────┘
               │ Delta log (conditional-put OCC)          
   ┌───────────▼──────────┐                               
   │  QUERY replica(s)     │  query_delta_only + Foyer;   
   │  no WAL, no MemBuffer │  hot window served by ingest 
   └──────────────────────┘  node or accepted-staleness   
```

- **Maintenance worker first — cheapest, biggest win.** The off-box
  `timefusion optimize` CLI already proves the model (S3 conditional-put OCC
  coordination). Extend the CLI/worker to dedup, vacuum, recompress,
  checkpoint, reconcile; set the cron schedules to `""` on the serving node
  (already supported — every job is cron-gated and disables cleanly). Result:
  maintenance memory can never OOM the serving node again (classes B/D
  incidents 07-04, 07-21, 07-22 all become impossible on the serving node),
  and the maintenance box can be big/preemptible/cheap.
- **Move DML merges to the worker.** The coalescer is already a standalone
  task with a clean queue/drain boundary. Persist the queue (it's already
  WAL-journaled as Update/Delete entries) and drain it from the worker.
  Ingest keeps only the in-memory leg (bounded, admission-controlled — §2.3).
  A poison MERGE then wedges a worker, not ingest.
- **Query replicas** are close: `query_delta_only`/`bypass_buffer` exists.
  A replica needs no WAL/MemBuffer; hot-window reads either route to the
  ingest node or accept ~15min staleness (fine for most dashboards). This
  removes the other repeated OOM source — wide dashboard scans — from the
  ingest node (07-21 GatedScanExec incident class).
- **WAL stays single-writer per ingest node** (flock is a hard constraint);
  scaling ingest later means sharding tenants across ingest nodes, each with
  its own WAL dir — not sharing one.
- **Longer-term option (bigger surgery): make Kafka the WAL.** If the ingest
  node replays from Kafka on boot (offsets as cursors) the local WAL, its GC
  floor, the 192GB gate, and the replay-bounds class disappear entirely, and
  ingest nodes become stateless. Cost: TF ack latency coupled to Kafka, and a
  bookmark store for flushed offsets. Worth a design spike after the roles
  split; not a prerequisite.

### 2.3 One memory meter + universal admission (kills class B)

Regardless of the split, the serving process needs:

- **A single process-level budget** that MemBuffer, the DataFusion pools,
  Foyer, and a new "untracked" reservation ledger all draw from. Today the
  only reconciliation is a boot-time warning that *omits the maintenance
  pool* (`autotune.rs:183-187`). Target invariant: sum of budgets +
  measured RSS slack < cgroup limit, enforced at boot (refuse to start on
  oversubscription) and gauged at runtime.
- **Admission control on every write-shaped path.** Ingest INSERT has CAS
  admission; DML mem-legs (`bwl.rs:2632-2690`) and coalescer `enqueue`
  (unbounded `HashMap` of full RecordBatches + pinned `SessionState` per
  group) have none. Give the coalescer a byte cap that *rejects/spills*
  (error to client beats OOM for everyone), and route DML legs through
  `try_reserve_memory`.
- **Bound the flush-encode and scan-decode transients** with byte-based
  reservations from the same meter instead of ad-hoc concurrency guesses
  (12× inflation heuristics, per-crate semaphores).
- **pgwire soft-gate parity with gRPC**: gRPC soft-rejects at 85% pressure;
  pgwire INSERT doesn't. Backpressure must reach the client *before* the
  hard limit, because hard-limit rejection happens pre-WAL (the acknowledged
  non-durable seam, `bwl.rs:740-751`).

### 2.4 Unwedge the flush path (kills class C)

- **Shard `flush_lock` per (project, table)** — it is global today
  (`bwl.rs:350`); one slow tenant's commit stalls relief for all tenants,
  and the *emergency* WAL flush takes the same lock, so the remedy is as
  wedgeable as the disease.
- **Un-collapse the unified-table commit key** or introduce per-tenant
  staging + a small commit-batcher, so cross-tenant commits stop serializing
  on one Delta log mutex and one OCC domain.
- **Poison quarantine instead of retry-in-place**: the MERGE multi-source
  wedge persists because (a) fold-fingerprint changes reset the attempts
  counter, and (b) failed groups re-queue in front of new work in a *serial*
  drain. Rule: any group failing twice with a deterministic error is
  serialized to a quarantine dir (same pattern as WAL quarantine) and
  drained out-of-band; the drain loop never blocks on poison. Also fix the
  actual bug class: round-splitting keys on source byte-equality while the
  merge matches on join-equality after coercion — split on the coerced key.
- **Watchdog abort must not double-commit**: the 600s flush watchdog drops a
  commit future that may still land (duplicate rows). With idempotent writes
  (§2.1) this becomes harmless by construction.

### 2.5 Detection that matches the failure modes (kills class E)

- **Per-minute ingest continuity** (global + top-tenant), alerting on a
  5-min window >50% below trailing baseline — this week's 9-minute gap is
  exactly the resolution the hourly checks miss.
- **Consumer progress, not liveness**: alarm on committed-offset staleness
  and lag growth per partition; group-membership checks proved insufficient
  (30-min poll interval hides wedged workers).
- **Watch the new bounded queues**: coalescer bytes, quarantine dir size,
  WAL *backlog* (unreplayed entries — currently unmeasurable; add a cursor-lag
  gauge), orphaned flush holds.
- **Keep kill -9 in CI**: `tests/kill_recovery.rs` (SIGKILL the real binary,
  6 scenarios) is the only test class that would have caught graceful-Drop
  masking; run it in CI permanently and extend when new durability seams
  appear.

## 3. Phased roadmap

| Phase | Work | Effort | Risk retired |
|---|---|---|---|
| **1 (this week)** | Quarantine + coerced-key split for poison MERGE groups; coalescer byte cap; pgwire soft-gate; DML legs through reservation | S | stops the live spiral (C, part of B) |
| **2** | gRPC→Kafka; PubSub path discipline + deterministic ids; PG ON CONFLICT; drop-vs-fail outcome type; quota out of DLQ ladder | M | class A closed by construction |
| **3** | Maintenance worker (extend off-box CLI to all jobs; empty cron schedules on serving node); DML drain moves to worker | M | classes B & D off the serving node |
| **4** | Single memory meter + boot-time oversubscription refusal; shard flush_lock; per-tenant commit staging | M–L | remaining B & C |
| **5** | Query replicas (`query_delta_only`); per-minute continuity + lag watchdogs; queue gauges | M | E, plus dashboard-scan OOMs |

(A "Kafka as the WAL" phase appeared in the first draft and is **dropped**:
Kafka is optional, so TF cannot depend on it for durability.)

Ordering rationale: 1 stops the bleeding; 2 makes every future recovery a
safe mechanical re-drive (which converts *all* remaining loss classes into
"gap until re-driven"); 3–4 remove the co-tenancy that turns local failures
into node death; 5 is compounding hardening.

Because Kafka is optional (§2.0), phases **1 and 4 carry the load** for
single-node deployments — there is no upstream buffer to hide a wedge behind.
Detailed designs for those two live in
`2026-07-27-flush-unwedge-design.md` and `2026-07-27-memory-governor-design.md`.
