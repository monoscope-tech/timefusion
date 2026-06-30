# TimeFusion ↔ TimescaleDB correctness parity — test + implementation plan

Goal: make `otel_logs_and_spans` on TimeFusion (TF) byte-for-byte equivalent to
TimescaleDB (TS, source-of-truth) for every settled window, so monoscope can cut
over reads to TF without changing customer query results.

Audit that motivated this: `scripts/tf_vs_ts/FINDINGS.md` (2026-06-29). Three
defects: (1) episodic row loss, (2) duplication, (3) enrichment gap.

## 0. Correctness oracle & cutover gates

The parity harness (`scripts/tf_vs_ts/`) is the acceptance oracle. A window is
"settled" once it is older than `max(flush_interval, retention) + extraction lag`
(conservatively ≥ 2 h old). Cutover is allowed only when ALL gates hold across a
rolling 7-day scan of the top ~20 projects:

- **G1 (no loss):** `TF distinct == TS distinct` for every settled window.
- **G2 (no dup):**  `TF total == TF distinct` for every window.
- **G3 (parity):**  `row_diff.js` reports 0 field mismatches, including the
  enrichment columns (`hashes`, `attributes.url.path`, `attributes.http.route`).

Commands:
```
scripts/tf_vs_ts/counts_matrix.sh <pid> 10 <ISO_start>...   # G1 + G2
node scripts/tf_vs_ts/row_diff.js  <pid> <start> <end>      # G3
```

Each defect below ships with a failing test FIRST (mandatory workflow,
CLAUDE.md §Bug-fix workflow), then the fix, then the gate it unblocks.

---

## Holistic stack model: where correctness can break (WAL → pgwire → Delta)

Code-grounded audit of the TF pipeline. The two prod defects map to specific
seams; most stages are already safe.

| # | Stage | Verdict | Evidence |
|---|-------|---------|----------|
| 1 | pgwire INSERT response | LEAK→DUP | `fast_insert_batch` returns accurate count (`database.rs:4610`), but the `INSERT 0 N` **tag is emitted in the datafusion-postgres fork** — suspected source of monoscope reading a success as `TuplesOk`/0-row → false failure → DLQ replay → duplication. |
| 2 | Memory admission / backpressure | **PRIMARY LOSS** | `insert()` reserves memory **before** `wal.append_batch` (`buffered_write_layer.rs:728` then `:741`). When `reserve_with_backpressure` times out it returns `Err` at `:588` *before any WAL write* — the rows are **never logged and are dropped** (the "data remains in WAL" log at `:584` is wrong). Recovery relies entirely on the upstream DLQ, which doesn't drain. |
| 3 | WAL durability | DURABILITY-GAP (minor) | fsync deferred up to `FSYNC_SCHEDULE_MS=200` (`wal.rs:213`); a crash within 200 ms loses acked-but-unsynced writes. Checkpoint/`advance_by_counts` happens only **after** Delta commit (`buffered_write_layer.rs:1283`), so flush failures don't lose data. |
| 4 | MemBuffer | SAFE | within-flush dedup on `(id,timestamp)` (`mem_buffer.rs:432`); eviction is now strictly drain-after-flush (`buffered_write_layer.rs:1406` — "this used to silently drain… that lost data"). |
| 5 | Flush → Delta write | SAFE(loss)/DUP(cross-flush) | `SaveMode::Append`, no PK (`database.rs:3017`). Buckets retained in MemBuffer on any commit failure. Append + no PK ⇒ a same-id row from a *separate* flush appends a physical duplicate. |
| 6 | Cross-flush dedup sweep | **PERMANENT DUP** | `dedup_today_partitions` (`database.rs:4098`) collapses by `dedup_keys=[id,timestamp]` via `replace_where`, but is scoped to `today = Utc::now().date_naive()` only. A duplicate that lands in a **prior-day** partition (late DLQ replay, backfill) is **never** swept. |
| 7 | Read union (MemBuffer ∪ Delta) | DUP surfaces / footgun | Sealed-bucket exclusion is fixed via `force_flushed` tracking (`mem_buffer.rs:1133`), so no double-count/mask. BUT the **read-side `row_number()` dedup that the schema comments promise does not exist** — physical Delta dupes surface directly in `COUNT(*)`. Also `WHERE project_id=…` is optional: absent ⇒ silent fallback to `default_project` (`database.rs:5205`), i.e. wrong/partial results, not an error. |

**Net:** loss is seam #2 (drop-before-WAL under pressure); permanent duplication
is seams #6+#7 (today-only sweep + no read-side dedup); seam #1 manufactures the
duplicate source. Enrichment (Defect 3) is purely monoscope-side. The plan below
is revised to target these exact seams.

---

## Defect 1 — Episodic row loss (G1)

**Root cause.** Base dual-write (`Telemetry.hs:1136-1173`, `WriteBoth`) is
transactional: a TF failure/under-persist DLQs the batch as `tf-failed` for
`WriteTfOnly` replay. But (a) TF rejects writes under memory backpressure, and
(b) the DLQ does not drain (commit-starvation, memory `dlq_commit_starvation`),
so those batches are never replayed → permanent TF gap. Loss magnitude tracks
load (0% quiet → 10-50% busy → 99.8% in a burst).

### Tests first
- **monoscope (closest level):** consumer integration test reproducing
  commit-starvation — a batch that no-acks (`[]`) must not pin the commit base;
  after a reseek-to-committed the DLQ drains to 0 lag. Name:
  `dlq_reseek_drains_after_noack`.
- **TF e2e (`tests/e2e/`):** `pressure_writes_never_drop` — drive inserts past
  `BUFFER_MAX_MEMORY_MB` under virtual clock; assert every submitted id is
  present in Delta after `force_flush()` (0 rejected inserts). Builds on the
  existing `buffered_write_layer_pressure_flushes_current_bucket` guard.

### Fix (in priority order — the TF-side fix is primary, not the DLQ)
1. **WAL-before-admit (TF, the real fix):** reorder `insert()` so
   `wal.append_batch` happens **before** the memory reservation gate
   (`buffered_write_layer.rs:728→741`). Then a batch that can't be admitted to
   MemBuffer is still durably logged and replayed by TF itself — the WAL, not the
   upstream DLQ, becomes the durability boundary. Rejection must mean "applied
   backpressure / will replay from WAL," never "dropped." (Watch WAL disk growth
   under sustained overload — pair with #2; the WAL is bounded by `wal_max_file_count`,
   so also raise flush throughput so the log drains.) This makes TF self-healing
   regardless of monoscope DLQ behavior.
2. **Raise flush throughput so rejection effectively never fires:** the rejection
   path triggers only when Delta flush can't free RAM within
   `write_backpressure_timeout`. Coalesced-flush + the OPTIMIZE/dedup conflict
   fixes (`optimize_partition_scope_bug`, `tierc_dedup_planning_scan`, PR #77)
   are the levers; verify drain keeps up at burst rate.
3. **monoscope DLQ self-heal** (defense-in-depth): reseek-to-committed on no-ack
   chunk + drop static `group.instance.id`. Per `dlq_commit_starvation` +
   `scripts/local/dlq-evict/`. Backfills already-DLQ'd `tf-failed` rows. Note:
   replaying old batches will create old-partition duplicates unless Defect 2 #1
   lands first — sequence accordingly.
4. **Capacity sanity:** host now 188 GB, OOM-loop era over
   (`oldest_bucket_age_dwell_fix`); keep headroom so OOM restarts (which lose the
   ≤200 ms unsynced WAL tail) don't reopen the loss window.

### Verify → unblocks G1
Re-run `counts_matrix.sh` over the historically-divergent windows (06-24, 06-29)
once DLQ has drained; expect MATCH (no `TF_MISSING`).

---

## Defect 2 — Duplication (G2)

**Root cause (revised after stack audit).** TF dedup is structurally unable to
guarantee `COUNT == COUNT(DISTINCT id)`:
- Flush dedup (`mem_buffer.rs:432`) collapses dupes only **within one flush's
  buckets** — not across flushes or against committed Delta rows.
- The cross-flush sweep `dedup_today_partitions` (`database.rs:4098`) is scoped to
  **`today` only**, so a duplicate landing in a prior-day partition is never
  collapsed.
- The **read-side `row_number()` dedup the schema comments promise was never
  implemented** (`schema_loader.rs:28`, `mem_buffer.rs:431` are comments only).
- Delta write is `SaveMode::Append` with no PK (`database.rs:3017`).

So any same-id redelivery/replay that crosses a flush or a UTC-day boundary
produces a permanent physical duplicate that surfaces in `COUNT(*)`. The trigger
is monoscope re-sends (the `TuplesOk` 0-row-success false-failure,
`Telemetry.hs:1049-1059`), but the *defect* is TF's missing end-to-end dedup.
Observed 06-25: TF total = 2× distinct, still present 4 days later.

### Tests first
- **TF e2e `dup_across_flush_is_deduped_on_read`:** insert id X in flush A,
  insert id X again in a *later* flush B into the same date, query `count(*)
  where id=X` → must be 1 (this fails today — proves the missing read-side dedup).
- **TF e2e `dup_in_prior_day_partition_gets_swept`:** write a dup into a
  yesterday-dated partition, run the sweep → dup collapses (fails today — sweep is
  today-only).
- **monoscope unit `tuplesok_insert_is_not_underpersist`:** a TF insert returning
  `TuplesOk` for N rows classifies as success, not `SilentUnderPersistError`.

### Fix
1. **Read-side dedup (TF, the correctness guarantee):** implement the long-promised
   `row_number()`-style rewrite — keep the last row per `(id, timestamp)` at query
   time — so `COUNT(*)` is correct regardless of physical duplicates. This is the
   only fix that makes results correct *immediately* and independent of sweep
   timing. (Scope it to the dedup_keys; cost is a window/aggregate over the scan.)
2. **Sweep recent partitions, not just `today` (TF):** ✅ DONE — `dedup_today_partitions`
   now sweeps `[today − timefusion_dedup_lookback_days … today]` (default 1),
   discovering project_ids per swept date; global version-skip still bounds cost.
   Catches the day-boundary late-replay case. Guard:
   `dedup_compaction_test::dedup_sweep_collapses_prior_day_partition`. (A bounded
   lookback still can't catch arbitrarily-late replays — that's what #1 covers.)
3. **monoscope: fix `TuplesOk` classification** — removes the dominant duplicate
   *source* so #1/#2 rarely have to fire.

### Verify → unblocks G2
`counts_matrix.sh` shows no `TF_DUP(+N)` on any window incl. 06-25; the two new
TF e2e tests pass.

---

## Defect 3 — Enrichment gap (G3): `hashes`, `http.route`, parameterized `url.path`

**Root cause.** Base row is written un-enriched to both. Enrichment runs in
post-write background UPDATEs:
- UPDATE-1 (parameterized `url.path` + `http.route` + `hashes` merge) is
  **PG-only by design** — DataFusion can't run `jsonb_build_object`/`now()`
  (`BackgroundJobs.hs:1719-1748`).
- UPDATE-2 (`pat:` hash tag) is dual but best-effort/circuit-broken on TF
  (`BackgroundJobs.hs:1565-1584`).

So TF permanently lacks these fields. Per-row values otherwise match exactly.

### Decision: re-emit the enriched row (Option A2), do NOT port jsonb UPDATEs
The extraction worker already holds the fully-enriched `OtelLogsAndSpans` struct
in memory after computing `norm_path`/`http.route`/`hashes`. Rather than translate
UPDATE-1's `jsonb_build_object` SQL into DataFusion-compatible DML (brittle,
needs `now()`/jsonb funcs TF lacks), have the worker **re-write the enriched row
to TF with the same `id`** (a `WriteBoth`/`WriteTfOnly` of the enriched struct).
Defect-2's dedup-on-id then collapses base+enriched to the latest version.
This reuses the existing write path and needs no new TF SQL surface.

(Alternative A1 — implement UPDATE-1 as TF DML via `dml.rs` + Variant ops — kept
as fallback if re-emit volume is too high; rejected as primary for complexity.)

### Tests first
- **monoscope integration:** `extraction_reemits_enriched_row_to_tf` — ingest a
  span with a raw `/v1/customers/cus_X` path, run the extraction worker, then read
  the row from TF and assert `attributes.url.path == /v1/customers/{param}`,
  `attributes.http.route` present, and `hashes` contains the content + `pat:` tag.
- **TF e2e:** `reinsert_same_id_replaces_attributes` — re-insert an id with new
  `attributes`/`hashes`; after flush the queried row reflects the newer values
  (dedup keeps latest, not first).

### Fix
1. monoscope extraction worker re-emits the enriched struct to TF after UPDATE-1
   (same `id`), instead of `pgOnlyExec`. Make UPDATE-2's TF arm durable (not
   silently dropped) or fold the `pat:` tag into the re-emit.
2. TF dedup keeps the **latest** version per id on flush (ties broken by write
   order / a monotonic version), so the enriched re-emit wins over the base row.

### Verify → unblocks G3
`row_diff.js` over a settled window reports 0 mismatches on `hashes`,
`attributes` (incl. `url.path`/`http.route`).

---

## Continuous parity monitoring (gate + ongoing guard)

Stand up a scheduled job (cron / monoscope routine) that every hour runs
`counts_matrix.sh` + `row_diff.js` over the last settled hour for the top-N
projects and emits metrics: `parity.loss_pct`, `parity.dup_pct`,
`parity.field_mismatch`. Alert on any nonzero. This is both the cutover gate
evidence (7 consecutive green days) and the post-cutover regression guard.

## Phased cutover sequence

1. Land Defect-1/2 TF backstops (dedup-on-id, no-drop pressure) — TF self-heals
   even before monoscope changes.
2. Land monoscope DLQ drain + `TuplesOk` fix → backfill historical gaps.
3. Land enrichment re-emit (Defect 3).
4. Turn on continuous parity monitoring; require 7 days all-green (G1–G3).
5. **Shadow phase:** monoscope reads from BOTH, diffs results, logs mismatches
   (read-side equivalent of the harness) — catches query-path divergence the
   row-level harness can't (planner/UDF/type quirks).
6. Canary: route a small % of read traffic to TF; watch parity + latency.
7. Full cutover; keep dual-write + monitoring for one retention cycle as rollback.

## Rollback

Dual-write stays on through cutover; if parity monitoring trips, flip reads back
to TS (no data migration needed since TS retains the source-of-truth).

---

## Implementation log + chosen-approach designs

### Done
- **Defect 2 #2 — dedup sweep lookback window.** `dedup_today_partitions` now
  sweeps `[today − timefusion_dedup_lookback_days … today]` (default 1),
  discovering project_ids per date; global version-skip preserved.
  `src/database.rs:dedup_today_partitions`, `src/config.rs:d_dedup_lookback_days`.
  Test: `tests/dedup_compaction_test.rs::dedup_sweep_collapses_prior_day_partition`.
- **Defect 2 #1 — read-side dedup (the G2 correctness guarantee).** New physical
  operator `DedupExec` (`src/read_dedup.rs`) wired into `ProjectRoutingTable::scan`
  at the single `wrap_result` choke point (`src/database.rs`), so it sits over the
  routed + pruned MemBuffer ∪ Delta plan — *after* `project_id` routing and time/
  partition pruning, avoiding the reverted-blocker's pushdown breakage. Now
  `COUNT(*) == COUNT(DISTINCT dedup_keys)` at query time regardless of physical
  dupes (cross-flush appends, prior-day replays the sweep hasn't reached). Design
  choices that differ from the superseded note below:
  - **Streaming keep-first, not buffer-then-`dedup_batches`.** Holds only a
    `HashSet` of encoded key-rows and filters each batch in place — never buffers
    the fat payload, never concats it (dodges Arrow's 2 GB offset limit), and
    preserves streaming + downstream early-LIMIT on the hot table. Keep-first means
    the MemBuffer copy (unioned first) wins; same-key dupes are byte-identical today
    so no tiebreaker is needed yet (add a monotonic write column with Defect 3's
    enriched re-emit — see below).
  - **Projection handled inside `DedupExec`, no trailing `ProjectionExec`.** scan()
    augments the pushed projection with any dedup-key columns the query projected
    away; `DedupExec` restores the requested columns via `RecordBatch::project`
    (which preserves row count for the empty `COUNT(*)` projection). Single
    operator, no extra plan node.
  - **`required_input_distribution = SinglePartition`** → DataFusion inserts a
    streaming `CoalescePartitionsExec` so a key can't straddle partitions.
  - No flag (default behavior, per directive). Tests:
    `tests/dedup_compaction_test.rs::dup_across_flush_is_deduped_on_read` (COUNT(*)
    + keys-projected-away `SELECT name`). The two sweep/compaction tests now assert
    *physical* row counts via `test_helpers::delta_physical_row_count` (Delta log
    `num_records`) since the routed COUNT is now deduped.
  - **Perf caveat (validate during rollout, per plan):** `DedupExec` wraps *every*
    scan of a `dedup_keys` table, including the hot mem-only `now()-5m` dashboard
    path. It streams (no blocking barrier) and the key set is tiny, but the forced
    coalesce removes scan-level parallelism for the dedup stage. Watch scan p99 on
    `otel_logs_and_spans` after deploy; if it regresses, scope dedup to the
    union/Delta-present paths only (skip the pure mem-only branch).
  - **LIMIT-undercount fix (found in `/code-review`):** a pushed `LIMIT N` must
    not be forwarded into the underlying scans — DedupExec drops rows *after* the
    scan, so a truncated-to-N scan could yield < N distinct rows the outer
    GlobalLimitExec can't recover. `scan()` now suppresses the per-scan `limit`
    when dedup is active (outer limit still caps). Guard:
    `tests/dedup_compaction_test.rs::limit_query_not_truncated_below_read_dedup`
    (verified failing pre-fix). Code-review also confirmed: all 4 scan() return
    paths funnel through `wrap_result`; timestamp type matches across the union;
    Variant root-wrap/defer-projection are logical-plan rules insulated from the
    physical DedupExec; record_scan fires once. DedupExec deliberately declares
    no output ordering (the inserted CoalescePartitionsExec wouldn't preserve it
    anyway) — `ORDER BY` adds a SortExec, part of the documented perf caveat.
  - Full suite green (lib unit, buffer_consistency, integration, sqllogictest,
    test_dml_operations, jsonb_oid, statistics, postgres_json). Pre-existing
    unrelated flake: `database::tests::test_batch_queue_under_load` times out at
    30 s on a clean baseline too (loads prod `.env` + scans WAL dir; no SELECT).

### ⚠️ BLOCKER found — logical `Distinct::On` breaks tenant routing (attempted + reverted)
Tried the analyzer-rule approach (wrap each dedup-keys `TableScan` with
`Distinct::On`). It correctly deduped (new test passed), but **broke 5 existing
tests including cross-tenant routing**: `SELECT … WHERE project_id='pb'` returned
pa's rows. Cause: wrapping the `TableScan` interposes a `Distinct` node between
the `project_id` filter and `ProjectRoutingTable::scan`, and DataFusion's
`push_down_filter` does **not** push the predicate through `Distinct::On` — so the
filter never reaches `scan()`, routing falls back to `default_project`, and
time-range/partition pruning is lost too. Reverted in full (rule, registration,
`Database::read_dedup`, test). **Correct approach: a physical dedup operator
INSIDE `ProjectRoutingTable::scan`, applied to the union AFTER routing/pruning** —
with projection augmentation (add `id,timestamp` to the pushed projection when
absent, dedup, then a trailing `ProjectionExec` to restore the requested schema).
Bigger + careful; the sweep (shipped) holds the line meanwhile.

### Design — Defect 2 #1: read-side dedup (SUPERSEDED — see blocker above)
- **Why not a custom physical operator at `ProjectRoutingTable::scan`:** scan
  pushes the requested projection DOWN into the Delta/MemBuffer scans, so at the
  single `wrap_result` point (`database.rs:5310`) the dedup keys (`id,timestamp`)
  may already be projected away (`SELECT name …`, `SELECT count(*)`). Deduping
  there needs manual projection-augmentation + a trailing `ProjectionExec` —
  error-prone on the hottest table.
- **Chosen:** an `AnalyzerRule` that wraps each `TableScan` of a `dedup_keys`
  table with `Distinct::On { on=[id,timestamp], select=<scan cols>, sort=[id,
  timestamp, …tiebreak] }`. DataFusion's planner then guarantees key-column
  availability and correct projection, and produces an efficient grouped plan.
  `transform_up` replaces the leaf once per analyze pass (no re-descent → no
  infinite wrap); register alongside the existing analyzer rules
  (`database.rs:1734`). Keep-first prefers MemBuffer (unioned first) = freshest.
- **No flag (per directive):** default behavior. The background sweep keeps
  physical storage mostly unique, so the dedup usually has nothing to drop;
  validate the grouped-plan overhead on the hot table during rollout.
- **Tiebreaker:** none needed today (same-key dupes are identical exact
  redeliveries); add a monotonic write column once Defect 3 re-emits enriched
  rows (no such column exists today, confirmed).
- **Test:** `dup_across_flush_is_deduped_on_read` — cross-flush dup, query via the
  normal scan (not `query_delta_only`), assert COUNT = 1; plus a `SELECT name`
  case proving dedup still works when keys are projected away.

### ⚠️ BLOCKER found — Defect 1 decouple needs a WAL cursor redesign first
The naive "append-before-admit" reorder is **unsafe** with today's cursor.
`WalManager::advance_by_counts` (`wal.rs:706`) is **count-based FIFO**: on flush it
pops exactly `count` entries per shard from the front (`read_next(advance=true)`),
no per-entry offsets. The current invariant is therefore *"only append what you
will admit to MemBuffer"* — hence `insert()` reserves before appending. If a batch
is WAL-appended but skipped from MemBuffer (rejected admission), it sits at the
FIFO front; the next admitted batch's flush `advance_by_counts` pops and discards
it — **silent loss, worse than today**. A loss-safe decouple requires changing the
WAL to track separate *admitted* vs *flushed* offsets (position-based), so the
background admitter and the flush checkpoint advance independent cursors. That is a
durability-core change and must be designed + e2e-soaked on its own; it is NOT a
surgical reorder. **Sequencing adjusted: do read-side dedup (safe, read-path-only)
first; tackle the WAL cursor redesign as a dedicated change.**

### Design — Defect 1: decouple WAL from MemBuffer admission (chosen: full)
WAL becomes the durability + ack boundary; a background admitter drains
WAL→MemBuffer under the memory budget, applying backpressure there (never on the
insert thread, never dropping). Phased so each step is independently testable:
1. **WAL-first ordering:** in `insert()` move `wal.append_batch` ahead of the
   memory gate; ack the caller once WAL has the batch. (Removes the drop-before-WAL
   loss seam immediately.) Test: `pressure_writes_never_dropped_from_wal`.
2. **Background admitter:** task draining WAL tail → MemBuffer, `try_reserve` with
   wait-for-flush (no reject). Insert no longer calls `mem_buffer.insert` directly.
   Reconcile with `advance_by_counts`/checkpoint so admitted-vs-committed cursors
   stay correct on replay. Test: `admitter_drains_backlog_after_flush_frees_ram`.
3. **Read-after-write:** ensure steady-state admit lag is sub-second; queries that
   need the WAL tail (rows not yet admitted) — decide whether to surface them or
   accept bounded lag. Test: `recent_write_visible_within_admit_window`.
Risk: touches replay/checkpoint/eviction bookkeeping — land behind
`timefusion_wal_admit_decouple` (default OFF) and validate on the e2e harness +
a soak before prod enable.

**Code-grounded status (2026-06-29, re-audited before starting):** the loss seam
is narrower than the table row #2 implies but real. `insert()` no longer drops on
*any* memory pressure — `reserve_with_backpressure` (`buffered_write_layer.rs:549`)
now flushes-to-make-room (single-flight `relieve_memory_pressure` + retry) for up
to `write_backpressure_timeout`, and only returns `Err` (→ reject before
`wal.append_batch`) when that budget is **exhausted** (`:581-588`). So residual
loss fires only under sustained overload where Delta flush can't free RAM in time;
the `"data remains in WAL"` log at `:584` is still wrong (append hasn't run). The
fix is unchanged (WAL-before-admit) and the blocker stands: `advance_by_counts`
(`wal.rs:706`) is confirmed count-based FIFO (`read_next(advance=true)` pops N from
the front). `current_position`/`WalPosition` exist but feed Delta-commit watermarks
only — the *checkpoint advance* is count-based, so the decouple must move advance
to position-based. **Not attempted this session: durability-core, soak-gated per
the directive above; would itself risk the data loss it targets if rushed
autonomously.** Read-side dedup (safe, read-path-only) shipped first as sequenced.
Done this session toward Defect 1: corrected the misleading `:584` reject log
(now states the batch is NOT durable — WAL append happens only after admission).

**Feasibility hand-off for the dedicated effort (walrus cursor verified):** the
vendored walrus exposes the position primitives a decouple needs —
`current_position` / `persisted_read_position` / `set_persisted_read_position`
(`vendor/walrus-rust/src/wal/runtime/position.rs`) and `read_next(col, checkpoint)`
(`walrus_read.rs:23`, `checkpoint` only controls *persistence* of the cursor). BUT
walrus keeps a **single in-memory read cursor per topic** (`ColReaderInfo.cur_block_*`
advances on every `read_next`), shared with the flush-advance path. So a background
admitter cannot get an *independent* read cursor without modifying walrus. Two viable
shapes, both still soak-gated:
  1. **In-order admitter with WAIT (no walrus change, preferred):** insert appends to
     WAL + acks; a single background admitter drains WAL→MemBuffer *strictly in order*,
     `try_reserve` with **wait-for-flush instead of reject** (never skips an entry). Because
     admission stays in WAL order and is never skipped, the count-based-FIFO advance
     invariant (`force_flush`'s `has_buckets_before` guard, `buffered_write_layer.rs:648`)
     stays valid unchanged. The loss seam closes because rejection becomes backpressure-wait,
     never a drop. Cost: the admitter and the flush-advance must coordinate the one shared
     walrus cursor (admitter reads ahead into MemBuffer; flush advances behind it) — needs a
     read-vs-commit position split, which is the part to design + soak.
  2. **Position-based low-water-mark advance (walrus-aware):** advance the WAL to the
     min flushed position across all buckets in a shard (not per-bucket counts), via
     `set_persisted_read_position`. Heavier; only needed if out-of-order admission is ever
     wanted.
Flag-OFF must be byte-for-byte the current path. Tests (e2e harness, testcontainers
MinIO): `pressure_writes_never_dropped_from_wal`, `admitter_drains_backlog_after_flush_frees_ram`,
`recent_write_visible_within_admit_window`.

### Monoscope status (re-audited against the repo 2026-06-30 — list was stale)
- **Defect 2 #3 (`TuplesOk` classification): ✅ DONE & committed.**
  `retryHasqlWrite` (`src/Models/Telemetry/Telemetry.hs:1052-1062`) treats a
  `TuplesOk` wire-mismatch as a 0-row success (no retry → no duplicate), and the
  `classify`/`unaccountedRows` cross-check (`:1164-1196`) converts a report-success-
  but-persist-nothing into a `Left (SilentUnderPersistError)` so the batch DLQs
  instead of a silent ack. Tested:
  `test/integration/Opentelemetry/TimefusionWriteFailureSpec.hs` — "TuplesOk
  wire-mismatch is swallowed into a 0-row success" + "TF reports success but
  persists nothing → Left (That tfErr), never a silent ack".
- **Defect 1 #3 (DLQ self-heal reseek): ✅ DONE & committed.**
  `src/Pkg/Queue.hs:526-601` records the lowest un-acked offset per partition
  (`reseekVar`) and the poll thread reseeks (`base ← reseek offset`, ahead cleared)
  on any no-ack chunk, with revoked-partition cleanup — the commit-starvation
  self-heal from `dlq_commit_starvation`. (Optional follow-up: dropping the static
  `group.instance.id` at `:486` is not done, but the reseek removes the wedge.)
- **Defect 3 (enrichment re-emit): ❌ NOT done.** `src/BackgroundJobs.hs:1756`
  still runs `pgOnlyExec update1Sql` — the parameterized-`url.path`/`http.route`/
  `hashes` UPDATE remains PG-only; the enriched row is never written to TF.

### Genuinely remaining
1. **Defect 1 #1/#2 — WAL decouple (TF).** Durability-core; soak-gated; design +
   feasibility hand-off above. Own branch/PR + e2e soak before merge-enable.
   - **Phase-1 SHIPPED this session (behind `timefusion_wal_admit_decouple`, default
     OFF):** closes the drop-before-durability loss seam without the walrus cursor
     rework. When the backpressure budget is exhausted, `insert()` now ADMITS the
     batch over the memory hard limit instead of returning `Err` (the reject that
     dropped the batch before any WAL append). Because the batch is still admitted
     to MemBuffer and recorded, the count-based FIFO advance stays correct — no
     skipped/un-admitted entry, so the blocker above does not apply. The WAL append
     is the durability boundary; over-budget growth is bounded by the relief flush +
     WAL replay on restart. `src/config.rs` (`timefusion_wal_admit_decouple` +
     `wal_admit_decouple()`), `src/buffered_write_layer.rs` (`force_reserve` + the
     flag branch in `insert()`). Tests:
     `buffered_write_layer::tests::wal_admit_decouple_{off_rejects_when_backpressure_exhausted,
     on_never_drops_over_budget}` (OFF rejects = the seam; ON never drops + admits
     past the hard limit). **Caveat / soak gate:** ON trades a reject for unbounded
     growth if Delta flush can't keep up — watch RSS + flush throughput before prod
     enable. Phase-2 (the non-blocking background admitter so the insert thread
     never blocks, per the design above) is still the follow-up; Phase-1 already
     removes the data-loss seam, leaving only the thread-blocking cost (which the
     existing single-flight relief already bounds).
2. **Defect 3 — enrichment re-emit.** Coordinated TF + monoscope change.
   - **TF tiebreaker — ✅ SHIPPED this session (no schema migration needed).** Instead
     of a new version column, dedup now breaks ties by the existing nullable
     `observed_timestamp`: `dedup_batches` gained a `tiebreak: Option<&str>` param and
     keeps the row with the greatest tiebreak value per key (ties → last, so identical
     retries are unchanged; nulls sort lowest so a non-null enriched row beats a null
     base). Wired into BOTH paths that collapse dupes — flush dedup
     (`buffered_write_layer.rs:flush_bucket`) and the cross-flush sweep
     (`database.rs:dedup_partition`) — and declared in the schema
     (`schemas/otel_logs_and_spans.yaml: dedup_tiebreak: observed_timestamp`,
     `TableSchema::dedup_tiebreak` + validation). So once monoscope re-emits the
     enriched row with a fresh `observed_timestamp`, flush/sweep keep it. Test:
     `mem_buffer::tests::dedup_batches_tiebreak_keeps_greatest`. NOTE: the read-side
     `DedupExec` stays streaming keep-first (unchanged) → the enriched row wins
     deterministically *after the sweep* runs; before that there's a transient window
     where the read MAY show the base. That's fine for the cutover gate, which only
     asserts on **settled windows** (>2 h old, post-sweep). A deterministic-read
     keep-latest operator is an optional later refinement, not required for G3 on
     settled windows.
   - **monoscope re-emit — ❌ remaining (design now validated against the code; not
     auto-implemented — see why below).** `BackgroundJobs.hs:~1753-1756` still runs
     `pgOnlyExec update1Sql`.

     **Validated design (2026-06-30 code audit):**
     - The worker does NOT hold a ready enriched struct (the plan's original premise
       was off): UPDATE-1 merges *deltas* (`normalizedPaths`, `perRowHashesJson`,
       `perRowErrorsJson`, all index-aligned with `spans`) against the DB row in SQL.
       But the enriched struct is reconstructable IN-MEMORY: `hashes = base.hashes ∪
       new_hashes` (DISTINCT), `errors = new ?? base.errors`, and the parameterized
       path goes into the `attributes :: Maybe (AesonText (Map Text Value))` map.
     - **Flat columns are safe via the insert mapping.** `attributes___url___path` /
       `attributes___http___route` are READ by queries (`LogQueries.hs:570/612`,
       `Endpoints.hs:200`, parser `Expr.hs:754`), but they are DERIVED from the nested
       attributes map at insert time by the column→snippet table in
       `Telemetry.hs:~1439` (`("attributes___url___path", aT "url.path")`), and the same
       `bulkInsertOtelLogsAndSpans` parameterized insert runs for PG and TF. So setting
       the enriched struct's `attributes` keys `"url.path"` and `"http.route"` to
       `norm_path` makes TF's flat columns match PG's — no struct field for the flat
       column is needed.
     - **Write path:** `bulkInsertOtelLogsAndSpansTF WriteTfOnly enrichedSpans`
       (`Telemetry.hs:1124`) — TF-only, and it does NOT re-enter extraction (no
       re-emit loop). Set `observed_timestamp = Just now` so the TF tiebreaker (shipped
       above) keeps the enriched row. Mirror UPDATE-1's gates (`enableHashUpdates`,
       `hashUpdateMaxAgeSecs`) so we only re-emit what UPDATE-1 would have touched.
     - **Test:** an integration spec (model on `TimefusionWriteFailureSpec.hs`):
       ingest a span with raw `/v1/customers/cus_X`, run the worker, read from TF, assert
       `attributes___url___path == /v1/customers/{param}`, `http.route` present, `hashes`
       contains the merged tags. Needs the live TF+PG test harness.

     **Why not auto-implemented overnight:** it changes the prod extraction worker
     (runs on every batch), is enrichment-correctness-sensitive (a wrong merge writes
     bad data to TF — the inverse of the parity goal), carries the write-volume impact
     Option A2 itself flagged, and "compiles clean" ≠ "correct" — it must be validated
     against the monoscope integration suite (a live TF+PG harness) before deploy. cabal/
     ghc build here, so implementation is unblocked; it just needs runtime validation +
     a volume sign-off that an unsupervised pass can't provide. The TF tiebreaker that
     makes it WIN is already shipped, so this is the only remaining piece for G3.
