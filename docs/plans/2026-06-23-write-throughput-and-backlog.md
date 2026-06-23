# TimeFusion write throughput + backlog drain (2026-06-23)

Goal: **write to TF faster** to drain a large backlog and keep up with live ingest.
Supersedes the dedup/OCC draft (that path is now healthy — see "Already healthy").
The throughput problems here are **mostly independent of the OOM** — they were present
before it. OOM is the stability floor (§D), not the headline.

## The backlog (Kafka, `apitoolkit_eu`)

- **DLQ `apitoolkit_eu_dlq`: TOTAL-LAG ≈ 7.6M** — `otlp_deadletter` p0–3 each ~1.9M. These
  are events whose **TF write leg failed** and are being re-driven through the *same* slow
  path. This is the bulk of "so much waiting to be written".
- **Live `apitoolkit_eu`: lag is skewed, not global.** `otlp_logs` p7 = **142K**,
  `otlp_spans` p0 = 6.6K — both owned by **one consumer** (`mono-ingest-688084`). Every
  other partition is at ~10–40. So the live side is a single slow consumer/partition, not
  TF saturation (§E).

## How writes actually flow (verified in code)

monoscope dual-writes each batch to **PG and TF concurrently**
(`Telemetry.hs:1088-1091`, `Ki.scoped` + two `forkWithCtx`). **Both legs use the same
Hasql/PostgreSQL path** — the TF leg is `labeled @"timefusion" @Hasql $
bulkInsertOtelLogsAndSpans records` (line 1090). **TF is written over pgwire, not gRPC.**

`bulkInsertOtelLogsAndSpans` (Telemetry.hs:1264) chunks input at
**`bisectCap = 2^9 = 512` rows** and emits a prepared `INSERT INTO otel_logs_and_spans
(...~128 cols...) VALUES (...)×512` via bind params. **512 is dictated solely by
PostgreSQL's libpq 65 535-bind-param ceiling** (`bisectCap * length otelColumns <= 65535`,
line 1261) — a *PG client* limit that TF inherits for no reason of its own.

Per write to TF, therefore: a 512-row text INSERT → pgwire parse → analyzer rules
(VariantInsertRewriter + schema patch) → a ~128-column `ProjectionExec` (confirmed via
EXPLAIN) → one WAL append → one MemBuffer insert. **Tiny batches, text protocol,
per-statement planning** — high fixed cost amortized over only 512 rows.

Inside TF, **all Delta commits serialize on one global `delta_commit_lock`**
(`database.rs:923`; held by flush appends, dedup Overwrite, staged-commit, schema
evolution). The lock itself is brief (commit-log append; parquet upload is outside it),
so the ceiling is **commits/sec × rows/commit**.

TF already has a **gRPC Arrow-IPC ingest endpoint** (`grpc_handlers.rs`, STREAM_CONCURRENCY
16) and an internal **`skip_queue=true` Delta-direct path** (`database.rs:2786`, bypasses
WAL+MemBuffer, writes parquet straight to Delta) — **neither is used by monoscope today**,
and skip_queue isn't exposed to any client.

## Levers, by impact × feasibility

### A. Bigger TF write batches — decouple TF from the 512-row libpq cap (highest ROI)

512 rows/INSERT is the core throughput tax and it's a PG artifact. For the **TF leg only**,
raise rows-per-INSERT by 10–50× so fewer round-trips, WAL appends, plans, and commits cover
the same data.

- **A1 (smaller lift):** give the TF leg an **inlined-literal** encoder (values escaped into
  SQL text, no bind params) so it escapes the 65 535-param ceiling; chunk at e.g.
  5–20K rows. Keep the bind-param path for PG. Trade-off: bigger SQL text to parse, and the
  bisect poison-isolation retry needs a text-path variant. Measure TF parse time on a
  10K-row VALUES before committing to a size.
- **A2 (bigger lift, best ceiling):** write the TF leg via **gRPC Arrow IPC** (binary
  columnar, no param cap, no SQL parse/plan per insert). Blocker: **no Haskell Arrow-IPC
  encoder** (monoscope has gRPC *server* libs + ProtoLens, not Arrow). Would need an Arrow
  encoder or an FFI shim. Park behind A1 unless A1's parse cost dominates.

Verify: rows/commit up, pgwire `queries_total` per ingested row down, WAL append count down.

### B. Dedicated bulk path for the backlog — bypass WAL+MemBuffer (drains the 7.6M)

DLQ events are **old event-time**. Routed through MemBuffer they scatter across hundreds of
historical 5-min buckets → memory blow-up → OOM, and leave "lingering buckets" that never
drain. Backlog should **not** touch the buffer.

- **B1:** expose the existing **`skip_queue=true` Delta-direct path** as an ingest mode (a
  pgwire session flag, or a gRPC field) that writes large parquet files straight to Delta
  date partitions — **no WAL, no MemBuffer, no memory pressure, no OOM contribution**. Point
  the **DLQ drainer** (`apitoolkit_eu_dlq`) and any historical backfill at it.
- **B2:** dedup trade-off — the direct path skips ingest-time dedup. Either accept transient
  duplicates and let periodic `optimize` + the (now-fast) dedup reconcile, or run a bounded
  dedup pass after the bulk load. Document which.

Verify: DLQ lag falls steadily; MemBuffer pressure does **not** rise during backlog drain
(proof the backlog isn't going through the buffer).

### C. Commit efficiency — more useful work per commit

With A landing bigger batches, make the flush side coalesce them well:

- **C1 bounded-chunk flush** (from the 2026-06-22 plan): cap a `CoalescedGroup` by rows/bytes
  so one backfill can't build a multi-GB combined bucket (memory transient → OOM) — but keep
  chunks large enough that commits stay efficient. It's a *both-ends* knob: floor for
  throughput, ceiling for memory.
- **C2:** confirm flush appends stay `conflicts_checked=0` (they do today) so commit time
  stays in the ms-on-lock regime, not the dedup-Overwrite regime.

### D. Stability floor — stop the OOM so throughput is sustainable (not the whole story)

Throughput is moot if TF restarts mid-drain (each restart → WAL replay → re-flush dupes →
DLQ flood). Root cause: **autotune sizes pools off the 66 GB cgroup limit** (query 0.30 +
buffer 0.25 + foyer 0.15 + meta 0.02 ≈ **48 GB budget**) while the **host backs only ~42 GB**
(shared with timescaledb) → kernel `global_oom` at ~60 GB anon-rss (kernel log 18:33:57).
`MALLOC_ARENA_MAX=2` already set, so it's budgeted memory, not fragmentation.

**What fills the Greedy query pool: dashboard SELECTs over the 52.9k-file table** (§G).
Steady-state logs (19:36:55–57) show **6+ concurrent `scan_type=full` SELECTs at 3–6 s
each** (time_bucket/quantile dashboard queries, `pfilt=52895` — pruning works but still walks
all 52.9k file stats). Concurrent multi-second scans on the Greedy pool are the most likely
source of the otherwise-unexplained ~50 GB anon-rss. So **§G (cut file count) is also an OOM
fix**, not just a latency fix.

- **D1 (ops, no code):** container mem limit 66→**32 GB**; explicit
  `BUFFER_MAX_MEMORY_MB=8000`, `FOYER_DISK_GB=60`, `MEMORY_LIMIT_GB=8` (Greedy pool rejects a
  heavy SELECT instead of OOM-ing the host), drop `MEMORY_FRACTION`, `WAL_MAX_FILE_COUNT=50`.
  **The 2026-06-22 P0 was never applied** (prod env confirms). One restart.
- **D2 (code):** autotune clamps the **sum** of pool budgets to ≤0.8×cgroup, not each
  fraction independently; expose RSS + per-pool bytes in `timefusion_stats`.

### G. File-count explosion — 52.9k files, optimize is a no-op (the steady-state limiter)

The unified `otel_logs_and_spans` table holds **~52,924 files** (doubled from ~26k, largely
from restart-replay dupes). This is now the dominant steady-state cost: **every dashboard
SELECT and every dedup Overwrite walks all 52.9k file stats** (3–6 s per dashboard query;
dedup is ~0.9 s because it emits few files but still pays the metadata walk).

**Optimize is not compacting it.** Scheduled `light optimize` logs
`partitions_optimized=0, total_considered_files=0` every run — it only considers *today's
recent small* files, so the 52.9k accumulated/historical files are never bin-packed. (The
only `optimize completed` lines are for the `variant_bench` test table.)

- **G1 (immediate):** run a **full compaction** over `otel_logs_and_spans` (all dates, not
  just today) to pull file count back toward ~26k or lower. Cuts dashboard-query scan time
  and per-query memory (→ relieves the OOM, §D) in one shot.
- **G2 (durable):** widen the scheduled optimize scope beyond "today's recent" — bin-pack
  any date partition whose file count / small-file ratio exceeds a threshold, so the count
  can't ratchet up again. Coordinate with `delta_commit_lock` so it doesn't starve flush.
- **G3:** much of the doubling traces to restart-replay duplicates; fixing §D (OOM) +
  §F (WAL) removes the source that keeps re-inflating the count.

### E. Live partition skew — one slow consumer (monoscope-side)

`otlp_logs` p7 (142K) and `otlp_spans` p0 are both on `mono-ingest-688084` while peers are
near-zero. Investigate: a hot tenant hashing onto p7, that consumer instance being
resource-starved, or it hitting TF write failures and bisecting (512→256→…→1 = throughput
collapse on that partition). Fixing A (bigger batches, fewer failures) likely helps; also
check partition key distribution and that consumer's error logs.

### F. WAL append serialization (pre-OOM failure→DLQ source)

`another batch write already in progress` (walrus per-topic append collision) surfaces as an
**insert error → DLQ**, and stalls the post-commit cursor advance (→ WAL bloat). A per-topic
WAL append queue (2026-06-22 plan P1#1) serializes appends instead of erroring. Removes a
standing DLQ contributor independent of OOM.

## Already healthy (don't spend effort)

- Dedup planning-scan pruning + OCC conflict storm — fixed (PR #77/#78), prod-confirmed
  (`conflicts_checked=0`, scans prune, 9–130 ms). MemBuffer sawtooths (not wedged).
- Insert *plan* is trivial; the cost is batch size + commit serialization, not planning.

## Suggested order

1. **G1** (full compaction, 52.9k→~26k) + **D1** (OOM config) — cheapest, highest-leverage:
   together they cut dashboard-query memory/latency and stop the OOM restart→DLQ-flood loop.
   Mostly ops. Do first.
2. **A1** (bigger TF batches) — biggest *write*-throughput ROI, contained to monoscope's TF
   encoder.
3. **B1** (skip_queue bulk path for DLQ drainer) — the lever that actually clears the 7.6M
   without OOM risk. Needs a small TF API surface + monoscope drainer wiring.
4. **F** (WAL append queue) + **C1** (bounded-chunk flush) — remove standing DLQ sources /
   memory transients.
5. **G2** (widen optimize scope) — so file count can't re-inflate after G1.
6. **E** (partition skew) — investigate alongside; may resolve via A1.
7. **D2 / A2 hardening** — once the backlog is shrinking.

## Verify throughput (not just absence of errors)

```sql
-- rows committed per unit time + pressure during a drain
SELECT component,key,value FROM timefusion_stats
WHERE key IN ('queries_total','pressure_pct','estimated_mb_approx','oldest_bucket_age_secs');
```
Watch DLQ `TOTAL-LAG` trend (drain rate), live p7 lag (skew), and that pressure stays flat
while the backlog drains (proof B1 bypasses the buffer). Success = DLQ lag falling at a rate
that clears in hours, no `exit 137`, live lag back to ~tens across all partitions.
