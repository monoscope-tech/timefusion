# Flush/commit unwedge — implementation design

Date: 2026-07-27. Companion to `2026-07-27-long-term-architecture.md` (class C,
and the commit-rate half of class D). **Status: designed and adversarially
reviewed against HEAD 607b691 + pinned fork rev 4537de8. All three parts survive
with corrections. Not yet implemented.** Constraint: TF is a standalone
single-node DB; no Kafka, no external coordinator. Its own WAL is the durability
substrate, so a wedge here cannot be absorbed by an upstream buffer.

## §0 — Two LIVE loss bugs found during review (fix these first)

Neither is part of the redesign; both are shipping bugs found while verifying it.

### 0.1 The DML coalescer silently drops data — CONFIRMED FIRING IN PROD

`dml_coalescer.rs:644-651`: after 3 failed drains a group is **dropped** with only
an `error!` and a metric. Prod, **2026-07-27T04:42:24Z**:

```
DROPPING 00000000-…-000000000000/otel_logs_and_spans group after 3 failed drains
(7457 stmts, 1252311 rows): Failed to execute Delta MERGE UPDATE (deletion
vectors): … Resources exhausted: Failed to allocate additional…
```

**1.25M rows of enrichment updates permanently lost from Delta.** Two corrections
to the earlier narrative in this file's first draft:

- It does **not** "retry forever". The attempts counter cannot be reset: fold
  inherits `max` (:471), requeue takes `max` (:678), and a folded group can never
  absorb fresh enqueues (fingerprints differ, :459 vs :530; :376 rejects
  already-folded groups). The bug is a *silent drop*, not a loop. So the
  "un-resettable counter" work in Part 3 below fixes a non-existent problem —
  **the quarantine-instead-of-drop part is the entire value.**
- The trigger here was **memory exhaustion**, not the coerced-key MERGE bug. Two
  distinct failure modes funnel into the same drop, which is why the memory work
  and this work are coupled.

**There is no self-heal.** The mem leg runs first (`dml.rs:625-697`) and the Delta
leg is watermark-clamped (`dml.rs:738-760`) to target only rows already flushed to
Delta *and gone from the buffer*. A dropped Delta leg therefore leaves stale
pre-DML values in Delta with **no newer copy anywhere**; read-side dedup is
first-seen-wins with no value recency (`read_dedup.rs:261-299`). Quarantine is
acceptable *only* if the sidecar is durable and actually re-driven — otherwise it
is today's silent loss with a nicer name.

### 0.2 Cross-tenant WAL cursor contamination — verified in code, latent

The watermark in `commitInfo.info` is keyed by **shard index only**
(`{"0":{block_id,offset},…}`, `database.rs:667-699`) with **no topic identifier**.
`derive_wal_cursor_for_table` (`database.rs:4078-4098`) reads `table.history()` of
the **shared** unified log — all default projects live in
`.../timefusion/default/{table}/` — takes the MAX across *all tenants'* commits,
then `merge_persisted_positions` (`wal.rs:998-1017`) advances this project's
cursor whenever `cand > local`. WalPositions are per-`topic:shard` and are **not
comparable across topics**, so project B's boot cursor can be advanced by project
A's positions, skipping B's unreplayed entries — **acked-write loss on any
Delta-scan boot**. `tests/kill_recovery.rs` is single-tenant and cannot catch it.

**This must be fixed before Part 2**, which would otherwise bake the topic-less
format in permanently. It also supersedes the standing note that "the
Delta-derived cursor only advances to max(local, delta), so a shallow scan causes
duplicates, never loss" — true *within* a topic, false *across* topics on a
shared log.

## Why this is the highest-value work

The causal chain in every recent OOM: a single hung S3 commit holds the
**global** `flush_lock` (`buffered_write_layer.rs:350`) for up to 600s
(watchdog default), during which *no tenant* can flush, MemBuffer keeps
filling, WAL reclaim floor stays pinned, and the process walks into the cgroup
limit. Both the emergency WAL flush and pressure relief need that same lock, so
the remedy is as wedgeable as the disease.

Two facts found while designing, both load-bearing:

1. **`with_flush_paused` (:2347) has zero callers.** Its doc claims OPTIMIZE
   uses it; nothing does. So the only consumer of *global* flush exclusivity is
   dead code — the global lock is protecting an invariant nobody needs.
2. **Every mutating helper is already per-(project, table) keyed** —
   `register/release/orphan_inflight_holds` (:2098-2177),
   `compute_wal_watermark` (:2004), `advance_wal_watermark` (:2053). Per-key
   exclusion covers the whole domain the global lock was covering.

## Part 1 — Shard `flush_lock` per (project_id, table)

Mirror the existing `commit_locks`/`dml_locks` idiom in `database.rs`:

```rust
flush_locks:      DashMap<(String, String), Arc<Mutex<()>>>
flush_gate:       RwLock<()>                       // flushers read(); true-global takes write()
flush_started_at: DashMap<(String, String), Instant>
```

- **Lock order, one rule:** `relief_lock`(try) → `flush_gate.read()` → *exactly
  one* per-key mutex → S3. No path holds two per-key mutexes, so no cycle
  exists. Clone the `Arc` out before locking — never hold a DashMap guard
  across an `await`, or the shard itself wedges.
- **Periodic flush uses `try_lock` and skips** a busy/stalled topic instead of
  queueing behind it. This is the actual de-wedging: a hung tenant no longer
  accumulates a queue. Shutdown uses blocking `lock()` within its existing
  deadline (thread an `Acquire::{Wait, Skip}` through `flush_buckets_where`).
- **`write_post_flush_snapshot` (:2237) is global** (one
  `cursor_snapshot.json` for all topics) and was implicitly serialized by
  `flush_lock`. It moves to the end of the call with its own `snapshot_lock` +
  `try_lock`-and-skip — it is a forward-only coalescing write, so skipping a
  concurrent duplicate is free.
- **Preserve the no-await window** between `register_inflight_holds` and the
  holds upgrade (:1712-1716). That, not the global lock, is what keeps
  `compute_wal_watermark` from seeing a hold-less window.
- **Relief no-progress gate (:1541-1556) must learn a third state:** "committed
  0 because every topic was busy" is *progress*, not the Delta-is-broken bail.
- **Map hygiene:** after releasing, `remove_if(&key, |_, m| Arc::strong_count(m) == 1)`
  (re-checks under the shard lock, so a racing acquirer keeps its entry).
- **New observability:** `stalled_flush_topics` and `oldest_flush_age_secs`
  from `flush_started_at`. Today an operator has *no* way to see which tenant
  is wedging flush — that gap cost hours during the incidents.

**REVIEW VERDICT: SOUND-WITH-CAVEATS.** Claims (a)-(d) all confirmed —
`with_flush_paused` really has no callers (and `database.rs:4206-4208` says
OPTIMIZE must *never* pause flushes, relying on OCC + bounded retry instead), the
per-topic registration-before-snapshot with a no-await window is the real
mechanism, all hold bookkeeping is per-key or unique-token keyed, and WAL GC
already reads the floor concurrently with flushes. Strong supporting evidence the
design didn't cite: **the code already runs up to `flush_parallelism` groups
concurrently via `buffer_unordered` inside one lock hold**, so per-group
concurrency safety is proven in production today. That also right-sizes the win:
sharding buys concurrent *callers* (relief vs periodic vs DML force-flush) and
unblocks one-stalled-tenant-freezes-all — **not** new cross-table parallelism.

Two corrections:

1. **The eviction data-loss fear is a phantom** — good news. `run_eviction_task`
   never evicts unflushed data: `evict_drained_metadata` is warn-only,
   `reap_expired_empty_buckets` only reaps zero-row DML shells, and
   `MemBuffer::evict_old_data` has **no production caller** (tests only). So
   try_lock-SKIP cannot cause eviction-before-flush loss.
2. **Starvation is still real, just differently.** Today one lock hold snapshots
   *all* topics per cycle — implicit fairness that try_lock-SKIP removes with no
   replacement. A topic pinned by a wedged same-key flush would be skipped every
   cycle while its WAL holds pin GC, and the consequence is unbounded MemBuffer
   growth → backpressure/WAL breaker. **Required addition: per-key skip-count/age
   escalation** (after N skips or T seconds, acquire blocking).
3. **The relief progress gate fix in the original draft was wrong about the
   mechanism.** The gate compares the *global* `flush_completed_total`, so under
   sharding an unrelated tenant's flush makes relief believe its own no-op round
   made progress (spinning to `MAX_RELIEF_ROUNDS`) and vice versa. The fix is not
   a busy-vs-broken distinction on a shared counter — relief must gate on **its
   own `FlushStats` return value**, which is currently discarded at :770-786.

**Kill switch:** `TIMEFUSION_FLUSH_LOCK_SHARDED` (default true). When false the
key function returns a constant `("","")` — every path collapses onto one
mutex, byte-for-byte today's behavior, with no branch to get wrong.

**Failing tests first:** `flush_of_one_topic_does_not_block_another` (stall A's
callback via a `Notify` in `delta_write_callback`, assert B completes <1s —
fails today), `relief_flushes_healthy_topics_while_one_stalls`,
`flush_all_now_drains_healthy_topics_under_deadline_with_one_stalled`,
`periodic_flush_skips_busy_topic_instead_of_queueing` (and never
double-snapshots a bucket), plus the existing watermark proptests re-run under
concurrent per-key flushes, plus a `kill_recovery.rs` case: SIGKILL while A's
commit stalls and B's just landed — B's cursor advanced, A's did not, zero
acked rows lost.

## Part 2 — Un-collapse the unified-table commit key

`table_lock_key` (`database.rs:3461`) returns `("", table)` for every
non-custom-storage project, so all tenants' flush commits plus dedup plus
optimize serialize on one mutex. The lock *cannot* simply be dropped: the
comment at :1351-1359 records that delta-kernel's OCC checker errors
(`arrow_cast should have been simplified`) on the bare-string `replace_where`
predicate, so in-process serialization is what keeps the broken checker out of
the path.

**⚠ REVIEW: the premise above is STALE — re-ground the design before building.**
Dedup no longer uses `replace_where`; it commits explicit Remove+Add with
`predicate: None` (`database.rs:5085-5100`, :5425-5429), so the kernel
`arrow_cast` abort no longer applies to it. The only surviving bare-string
`replace_where` is `recompress_partition` (:4799-4804), which **doesn't take
`commit_lock` at all** — nor do the OPTIMIZE paths (:4236, :4606, :5931). So the
"all in-process commits to a log must be serialized" invariant this design is
built to preserve **is already violated by existing code**, and blind
append-vs-append would rebase fine without any lock (an append carries no read
predicate, so `ConcurrentAppend` never fires). Design against the *current*
conflict semantics — per-file `AddContainer` checks and the fork's
snapshot-isolation downgrade — not the stale comment at :1351-1359.

**Cheap alternative to try FIRST** (visible in the code, likely most of the win
for a fraction of the risk): hoist the `pre_uris` walk out of the lock and
deduplicate the refresh-probe/`get_latest_version` double round-trip. The
`commit_timing` logs already exist to size the gain before building a committer
task.

**Rejected options.** Narrowing the lock to `(project_id)` is unsound *for the
predicate-carrying commits that remain*. Partition-scoped OCC is the right
long-term answer but is a delta-kernel fork change, weeks of work, and every
mistake is a lost-commit bug.

**Recommendation: per-log commit batching behind one committer task.** Many
tenants' `Add` actions land in one Delta commit:

- Parquet staging stays outside the batcher (as today). The staged path sends a
  `CommitReq{project, table, adds, watermark, dirty_bins, reply}` and awaits a
  oneshot instead of taking `commit_lock`.
- The committer drains up to `commit_batch_window_ms` (25) /
  `commit_batch_max` (32) / a max-actions cap, then does **one**
  `refresh_table_snapshot` + **one** `CommitBuilder…with_actions(union)`.
- **`replace_where` / schema-evolution / maintenance commits run SOLO** through
  the same channel (batch flushed first). All in-process commits to a log still
  pass through one serial committer, so the rebase still sees no newer version
  and the broken checker is still never invoked — this is the property that
  makes it safe.
- **Delete the in-lock `pre_uris = get_file_uris()` walk (:3783).** Each
  request's added URIs come from its own staged `Add` paths joined to
  `table_url()`, reusing the existing normalization helper (:365-374) verbatim
  for the trailing-slash/query-string mismatch. `record_committed_write` takes
  `added` as a parameter instead of recomputing it.
- **Watermark metadata becomes multi-topic** (`{topic → ShardHolds}`).
  **⚠ REVIEW: this was understated, and "retain the legacy parse forever" would
  preserve a live bug.** The current format has *no topic identifier at all*
  (`database.rs:667-699`), and `DeltaWatermark` cannot express a second topic —
  so this is a format redesign, not an extension. More importantly it is the fix
  for §0.2: adding the topic id and filtering by it in
  `derive_wal_cursor_for_table` is what stops cross-tenant cursor advance. Do
  **not** keep an unfiltered legacy path; treat topic-less commits as
  "contributes nothing" rather than "applies to everyone".
- `dml_lock` keeps `table_lock_key` unchanged; only the *commit* is batched.

**Expected gain.** Serial section today ≈ `refresh_table_snapshot` (15-40ms
GET probe) + conditional PUT (50-150ms) + `get_file_uris` walk (5-50ms at
5k-50k files) ≈ **100-250ms per commit ⇒ a ~4-10 commits/s ceiling for the
entire unified log**, shared by every tenant plus maintenance. After batching
with the walk removed: ~65-190ms *per batch* ⇒ **~30-50× effective tenant
commit rate**, and 32× fewer Delta versions (compounding: cheaper checkpoints,
faster boot snapshot replay, far less OCC pressure against OPTIMIZE/dedup).
Cap on **actions**, not just request count; returns diminish past 32-64.

**Partial-batch failure** must never over-advance a cursor for a member whose
flush failed. On non-OCC error, bisect the batch once (two halves, ≤2 levels)
then fail individually. Durability is unaffected either way — a failed flush
commit restores the bucket and the WAL still holds the rows.

**⚠ REVIEW added three requirements the design missed:**

1. **Partial failure happens BEFORE the commit, not just at it.** Per-tenant
   `cast_record_batch`/writer failures (:3727-3739) and the schema-evolution
   fallback (:3699-3711) fail one tenant pre-batch; the batcher must atomically
   exclude that tenant's Adds **and** its watermark. The solo-request framing
   doesn't cover this.
2. **Per-tenant bookkeeping must fan out.** `record_committed_write` stamps
   `last_written_versions[(project,table)]` (:3969) which gates read-your-writes
   (:2714, :2742), plus dirty-bins (:4004-4006) and reconcile offsets (:4017) —
   all for *one* project. A batched commit that doesn't fan these out per tenant
   silently breaks read-after-write and dedup scheduling.
3. **The commit watchdog × batching.** The dropped-but-may-still-land commit
   future (:1929-1946) currently strands one tenant's attribution; under batching
   it strands N. Needs a `probe_commit_landed`-style reconciliation per batch.

Good news from review: **multi-partition batched commits are safe at the Delta
level.** The fork's conflict checker evaluates predicates per-file via
`AddContainer` (`conflict_checker.rs:526-543`); nothing assumes a commit's Adds
share a partition, and a concurrent replace_where/OPTIMIZE coarsens conflicts in
the *abort* (safe) direction. Commits are all-or-nothing single conditional PUTs,
so log-level partial failure cannot over-advance a cursor. **Deleting the
`pre_uris` walk is also confirmed sound and strictly better** — `added` feeds only
tantivy attribution and cache warming, dirty-bins come from batch timestamps, and
today's diff even leaks concurrent external actors' files into `added`. One
gotcha: staged `add.path` is bucket-relative and must go through
`log_store.to_uri` to match consumers.

**Kill switch:** `TIMEFUSION_COMMIT_BATCH_MAX` (default 32; **`0` = every
request runs solo**, behaviorally identical to today) and
`_WINDOW_MS` (25). With `0`, batches are never multi-topic, so the new
watermark map form is never emitted — one switch de-risks both changes.

**Failing tests first:** `commit_batcher_merges_concurrent_tenant_commits` (8
tenants → Delta version advances by exactly 1, each gets only its own URIs —
fails today, +8), `watermark_metadata_multi_topic_roundtrip` **plus a legacy
single-topic commit in the same log**, `replace_where_commit_is_never_batched_with_appends`,
`batch_failure_bisects_and_reports_per_request`, and a `kill_recovery.rs`
SIGKILL right after a batched multi-tenant commit lands (every member's rows
present exactly once, no cursor over-advanced).

## Part 3 — Fix and quarantine poison DML groups

**Root cause.** `split_rounds` (`dml_coalescer.rs:257`) builds its
`RowConverter` from the **source batch's own types**, while
`build_join_predicate` (`dml.rs:1110`) emits `col(target.k) = col(source.k)`,
whose equality is evaluated after DataFusion comparison coercion against the
**target** type. So rows that are byte-distinct but join-equal escape the
splitter, and the fork's merge fails "matched a target row against multiple
source rows" deterministically, forever. `build_folded` (:433) makes it worse:
it appends `project_id` as hardcoded `Utf8` non-nullable and joins it against a
**partition** column.

**Fix — one source of truth.** New `join_key_compare_types(table, join_keys,
source_schema)` in `dml.rs`: resolve the target field type from
`schema_loader::get_schema` (partition columns included — this is what fixes
folded `project_id`), then compute the comparison type via DataFusion's own
`type_coercion::binary::comparison_coercion`. `split_rounds` casts each key
column to that type *before* building the key `RowConverter`; the full-row
dedup converter stays on raw types (it only drops byte-identical rows, which
remains sound). `build_join_predicate` calls the same helper so splitter and
merge **agree by construction** — drift becomes a test failure, not a prod
loop. Coercion returning `None` falls back to the source type with a `warn!`
(no coercion ⇒ no join equality either).

**REVIEW VERDICT: root cause CONFIRMED, and pre-cast is safe for these keys.** The
fork's merge runs the full DataFusion Analyzer including `TypeCoercion`
(`merge_dv.rs:229-238`), so the join compares coerced values, and the failure site
is exactly `split_row_index` rejecting a target row matched by multiple source rows
(`merge_dv.rs:353-379`). `strip_source_conjuncts` applies only to the file-pruning
predicate, not the join, so it is not a confounder. **Decisive corroboration: the
MemBuffer leg already casts source keys to target types before its RowConverter**
(`mem_buffer.rs:1958-1975`) — the fix simply makes the Delta leg match the leg that
already works. The `build_folded` Utf8 hardcode is confirmed against a `Utf8View`
target (`schema_loader.rs:194-195`), so *every* folded merge has a mismatched key
pair by construction.

Coercion is injective for the current key types (strings → Utf8View; µs-UTC
timestamps, where tz is metadata only). **Guard for the future:** float ±0.0
total-order encoding, decimal rescale, and especially **timeunit coarsening**
(`(Microsecond, Second) → Second`, `binary.rs:2048-2068`) would lose distinctions
and *over*-merge — silent wrong results. Reject/warn on those coercion classes
rather than casting blindly if key types ever widen.

**~~Un-resettable attempts~~ — DROPPED, this fixed a non-existent bug.** Review
established the counter cannot be reset: fold inherits `max` (:471), requeue takes
`max` (:678), and a folded group can never absorb fresh enqueues (fingerprints
differ, :459 vs :530; :376 rejects already-folded groups). "3 attempts" *does*
fire — and then **drops the data** (§0.1). Skip the DashMap refactor; it is
churn. Keep only:

- **Only non-OCC errors should count toward terminality** (reuse
  `is_occ_conflict_err`). ⚠ Caveat from review: that classifier is
  substring-based and matches broadly (e.g. "Transaction failed"), so some
  *permanent* errors will be misclassified as OCC and retry forever without ever
  reaching quarantine. Tighten the classifier or add an absolute attempt ceiling
  as a backstop.
- Clear terminal state on success and after a TTL so a fixed schema recovers.

**Quarantine at 3 deterministic failures.** Hoist `write_owner_only` /
`quarantine_entry` (`bwl.rs:44-102`) into a shared `quarantine_write` and reuse
the WAL quarantine pattern: Arrow **IPC file** payload (self-describing schema)
+ `.meta` sidecar (project(s), table, join keys, assignments, predicate,
attempts, last error, shape fingerprint, rows), mode 0600, under
`<wal_dir>/quarantine/dml/`. The group is already drained out of the map, so it
cannot re-enter the queue — and the code must **not** requeue after
quarantining. Encode/write on `spawn_blocking`, joined at the end of `drain`,
because `drain` holds `drain_lock` and a slow disk would stall every future
drain. Re-drive is an explicit admin function
(`timefusion.redrive_dml_quarantine()`), **never** boot auto-replay — that
would re-poison the queue on every restart.

**Bound the queue in bytes.** `queued_bytes` tracked on enqueue/drain;
over `TIMEFUSION_DML_COALESCE_MAX_BYTES` (default 256MB) `enqueue` returns
false and the caller runs the Delta leg **synchronously** — the pre-coalescer
behavior, a slow path not a loss path (mem leg + WAL append already happened).
Expose `dml_coalesce_queued_bytes` / `_groups` in stats; today this memory is
invisible to `reserved_bytes`, the DataFusion pool, and every dashboard, which
is precisely why it only ever showed up as an OOM.

Follow-up (not this change): each group pins an `Arc<dyn Session>`, keeping a
whole `SessionState` + plan cache alive. Store a session factory instead.

**Kill switches:** `TIMEFUSION_DML_SPLIT_COERCED_KEYS`,
`TIMEFUSION_DML_COALESCE_QUARANTINE`, `TIMEFUSION_DML_COALESCE_MAX_BYTES`.

**Failing tests first:** `split_rounds_separates_utf8view_source_from_utf8_target`
(2 byte-distinct join-equal rows ⇒ 2 rounds; returns 1 today — *this is the
regression test for the live prod loop*), dictionary and Int32/Int64 variants,
`folded_project_id_column_matches_target_partition_type`,
`poison_attempts_not_reset_by_concurrent_enqueue` (loops forever today),
`poison_attempts_survive_fold_membership_change`,
`occ_failures_do_not_count_toward_quarantine`,
`deterministic_failure_quarantines_and_does_not_requeue` (IPC + meta on disk,
0600, metric bumped, queue empty), `enqueue_rejects_over_byte_budget_and_falls_back_to_sync`,
an end-to-end same-key multi-tag enrichment in `test_dml_operations.rs` with a
Utf8View source against a Utf8 target (the 2026-07-19 prod shape, asserting all
tags land), and a `kill_recovery.rs` case that a quarantine file survives
SIGKILL and re-drives exactly once.

## Sequencing (revised after review)

**Ship the two live-loss fixes first — they are small and prod is losing data now.**

1. **§0.1 — DML quarantine instead of drop.** 1.25M rows were lost today. Make the
   terminal branch write a durable, replayable Arrow IPC + `.meta` sidecar and add
   a re-drive entrypoint. Alert on the metric. Independent of everything else.
2. **§0.2 — topic-scoped watermark.** Add the topic identifier and filter on it in
   `derive_wal_cursor_for_table`; treat topic-less commits as contributing
   nothing. Must precede Part 2. Needs a *multi-tenant* kill-recovery test — the
   current suite is single-tenant and structurally cannot catch this.
3. **Part 3's coerced-key fix** (~30 lines, independently testable) — removes one
   of the two failure modes that reach the drop path.
4. **Part 1** — sharded flush locks, *with* the skip-age escalation and the
   own-`FlushStats` relief gate. Self-contained; kill switch is a one-line key
   function.
5. **Part 2 cheap half** — hoist `pre_uris` out of the lock, dedupe the
   refresh/`get_latest_version` round-trip. Measure with the existing
   `commit_timing` logs before deciding whether the committer task is worth it.
6. **Part 2 full committer** — only if the measurement justifies it, and only with
   the per-tenant bookkeeping fan-out, pre-commit exclusion, and watchdog
   reconciliation from the review.

Note the memory-governor work (companion doc) is *coupled* to step 1: the drop
that fired in prod was triggered by memory exhaustion, not by the key bug, so
quarantine bounds the damage but only the memory work removes the trigger.
