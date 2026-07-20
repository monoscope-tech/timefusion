# Plan: cut the enrichment MERGE storm (~125/min → ~12/min) via coalescer dup-key collapse

**Status:** ready for handoff. Diagnosed 2026-07-19; not yet implemented (prod dual-write
path — needs staging validation, not a blind deploy).

**Owner:** TBD. Touches `src/dml_coalescer.rs` (primary) and optionally
`monoscope/src/BackgroundJobs.hs`.

---

## 1. Problem

Prod TF takes **~125 `perform_merge_update`/min** (1256 in a 10-min sample). Each is a
Delta MERGE = plan + metadata scan + (rewrite or DV) + a Delta-log commit with an OCC
version check. Downstream damage:

- **OCC starvation.** 125 merges/min bumps the Delta table version every ~0.5s, so
  compaction and sealed-bin dedup can never land a clean commit — 29 optimize
  commit-conflicts in a 15-min sample. Result: the **dirty-bin dedup backlog grows
  unbounded** (1862 → 2647 → 3118 over the session) and tiny files never compact, which
  is what keeps recent-window scans expensive.
- **CPU/mem churn** — each merge materializes source Arrow and scans; this is a large part
  of the sustained 1000%+ CPU load and the memory pressure that turned two well-intentioned
  background changes (compaction, full-file warm) into OOMs on this memory-tight box.

The merge count is **not** raw write volume — it's fan-out inside the coalescer.

## 2. Exact mechanism (verified in code)

`TIMEFUSION_DML_COALESCE_SECS=60`. The `DmlCoalescer` (`src/dml_coalescer.rs`):

1. **`enqueue`** (line ~374) groups deferred `UPDATE … FROM` statements by
   `GroupKey{project_id, table_name, shape_fingerprint}` — same join keys + assignments +
   residual predicate + schema share one `PendingGroup`, accumulating `batches`. So it
   **does collapse by shape, not just serialize.**
2. **`drain`** (line ~418, every 60s or at `MAX_QUEUED_SOURCE_ROWS=1_000_000`) concatenates
   each group's batches and calls **`split_rounds`** (line ~254).
3. **`split_rounds`** drops exact-duplicate rows, then puts *rows sharing a join key into
   successive rounds* (round N = each key's Nth distinct payload). Delta forbids a MERGE
   source that matches a target row twice, hence the split. **The drain runs one MERGE per
   round.**

So **merges per project per drain = the max number of distinct payloads any single
`(span_id, trace_id)` key accumulated in the 60s window.**

Where do same-key/different-payload rows come from? **Cross-emit accumulation.** monoscope's
drain-age-flush timer (`BackgroundJobs.hs`, `runDrainAgeFlushTimer`, every 10s) emits an
enrichment UPDATE per project per flush. A span that stays hot across ~6 flushes in the 60s
window is emitted ~6 times with different tags/hashes → 6 rows, same key → **6 rounds → 6
merges**. Across ~12 active projects: ~12 × ~10 ≈ **120 merges/drain, ~once/min ≈ the
observed 125/min.**

Two monoscope emitters both feed this:
- **UPDATE-1** (`BackgroundJobs.hs:2044`) — already sends **one array row per span**
  (`new_hashes` = `ARRAY(SELECT jsonb_array_elements_text(hashes_json))`, applied as
  `hashes = ARRAY(SELECT DISTINCT h FROM unnest(o.hashes || u.new_hashes))`). Collapsed
  *within* an emit, but still fragments *cross-emit*.
- **UPDATE-2** (`BackgroundJobs.hs:2353`) — sends **one scalar row per (span, tag)**
  (`hashes = COALESCE(o.hashes,'{}') || ARRAY[u.tag]`). One tag/span/emit today, so it too
  fragments *cross-emit*, not within.

**Correction to the first-pass framing:** "have monoscope aggregate tags per span into an
array" does **not** fix UPDATE-2 — it already has one tag/span/emit; the dup keys are
strictly cross-emit. The robust fix is TF-side.

## 3. Fix — Option B (recommended): collapse dup keys in the coalescer

In `drain`, before `split_rounds`, **group the concatenated source by join key and reduce
each assignment**, producing **one row per key → one MERGE** instead of N rounds. The
reduction per assignment column:

- **Additive-array-append** (`col = <expr(col)> || ARRAY[src_scalar]`, or `… || src_array`,
  or the `ARRAY(SELECT DISTINCT h FROM unnest(col || src_array))` shape): concatenate all
  the per-key `src` values into one array and apply once. Equivalent because array append is
  associative and the enrichment is set-semantic (`NOT (… @> …)` predicate + `SELECT
  DISTINCT` make re-application idempotent).
- **Last-write-wins** (`col = COALESCE(src, col)`, `col = src`, `processed_at = now()`):
  keep the **last** row's value per key (arrival order). Equivalent because N sequential
  rounds already overwrite the key's value each round; collapsing keeps the final one.

This is exactly what running the N rounds produces, in one MERGE. Both enrichment shapes
(UPDATE-1 and UPDATE-2) are covered.

### Implementation sketch

1. **Classify each assignment** in `PendingGroup.assignments` once per drain:
   `Additive(src_col)` vs `LastWriteWins`. Detect additive by matching the `Expr` shape
   `BinaryExpr(_, Operator::StringConcat/ArrayConcat, …)` whose RHS references a source
   column, plus the `ARRAY(SELECT DISTINCT … unnest(col || src))` variant. Anything not
   recognized → treat the whole group as **non-collapsible** and fall back to today's
   `split_rounds` (safe default — no behavior change for shapes we don't understand).
2. **Collapse** the concatenated source `RecordBatch` by join key:
   - group row indices by key (reuse the `RowConverter` key machinery already in
     `split_rounds`);
   - for `Additive(src)` columns, build a `List` column = per-key concatenation of the
     src values (in arrival order);
   - for `LastWriteWins` columns, take the last row's value per key;
   - the join-key columns take any representative (all equal within a key).
3. **Rewrite the merge** for this collapsed source: additive assignments become
   `col = <expr(col)> || u.<agg_list>` (append the aggregated list instead of a scalar);
   LWW assignments unchanged. Run **one** MERGE.
4. **Guardrails:** gate behind `TIMEFUSION_DML_COALESCE_COLLAPSE` (default **off**) for
   canary. Keep `split_rounds` as the fallback for unrecognized shapes and as the kill
   switch. Preserve the `ORDER BY span_id, trace_id` lock-ordering property (collapse keeps
   one row per key, so ordering is trivially consistent).

### Edge cases / correctness matrix (must be tested)

- **Array ordering:** if `hashes` order is semantically meaningful anywhere, verify concat
  order == round order (arrival). (It isn't today — read side treats it as a set — but
  assert it.)
- **`NOT (… @> ARRAY[tag])` residual predicate:** collapsing changes the source from N rows
  to 1; the predicate must still make an already-tagged target a no-op. With the aggregated
  array, `NOT (o.hashes @> u.tags)` — confirm partial overlap (some tags present, some not)
  still appends the missing ones. UPDATE-1's `SELECT DISTINCT` already handles dedup; UPDATE-2's
  scalar `@>` needs an array-aware rewrite (`… @> u.tags` with the append de-duping).
- **Deletion vectors:** the merge-on-read DV path (`use_deletion_vectors=true`, default)
  must produce identical row states after collapse. Run the DV e2e suite.
- **Flush-watermark clamp** (`clamp_decomposed`): unchanged — operates on the widened
  predicate, not per-row.
- **Mixed additive + LWW in one statement (UPDATE-1):** the reduction is per-column; test a
  statement that both appends `hashes` and LWW-sets `errors`/`norm_path`/`processed_at`.
- **Non-collapsible fallback:** a synthetic assignment we don't classify must fall back to
  `split_rounds` and behave exactly as today.

## 4. Fix — Option A (complementary, monoscope-side): reduce cross-emit re-emission

Independently useful, and reduces the *input* to the coalescer:

- In the drain buffer, **accumulate all tags/hashes for a span across the flush window and
  emit each span at most once per ~coalesce-window**, as an array row (UPDATE-1 already has
  the array shape; make UPDATE-2 match it: `u.tags text[]`,
  `hashes = COALESCE(o.hashes,'{}') || u.tags`). This shrinks per-key dup rows and total
  enqueued rows.
- This does **not** fully eliminate cross-emit dups (a span hot for >window still re-emits),
  so it's a mitigation, not a replacement for Option B. Do Option B first; Option A is a
  smaller follow-up if merge volume is still high.

## 5. Expected benefit (quantified)

Primary, high-confidence:
- **~10× fewer Delta MERGEs:** ~125/min → ~12/min (1 per project per drain). Directly:
  ~10× fewer Delta-log commits, OCC version checks, and post-commit hooks; ~10× less
  merge-commit S3 write and checkpoint churn.

Cascade (high-likelihood, magnitude workload-dependent):
- **OCC starvation broken:** version bumps every ~5s instead of ~0.5s give compaction and
  sealed-bin dedup real commit windows. Expect the **29 optimize-conflicts/15min to drop
  sharply**, the **dirty-bin backlog (3118, growing) to start draining**, and tiny files to
  compact — which is the actual lever on recent-window scan latency.
- **CPU/mem relief:** merges are a large share of the 1000%+ load and the background memory
  pressure. Fewer, larger merges = less Arrow materialization churn = more headroom (the
  headroom whose absence OOM'd the compaction and warm-full experiments).

Measurement / acceptance:
- **Batching ratio:** `timefusion.dml.coalesce_enqueued / timefusion.dml.coalesce_merges`.
  Today merges ≈ enqueued-split-into-rounds (low ratio). Target: merges ≈ #groups
  (≈ #active projects), i.e. ratio jumps ~10×. These counters already exist
  (`src/metrics.rs:89-90`).
- Log rate of `perform_merge_update` (should fall ~10×), `optimize` OCC conflicts (should
  fall), `dirty_bin_queue_depth` in `timefusion_stats` (should stop growing, then drain).
- No change in enriched-row correctness: a parity check that `hashes`/`errors`/`norm_path`
  end states match the pre-change engine (reuse `scripts/tf_vs_ts/`).

Honest uncertainty: the 10× merge reduction is near-certain; the *downstream* latency win
depends on how much of the load is merges vs SELECTs vs ingest, and on whether compaction
(currently reverted/backlogged) is re-enabled to consume the freed OCC windows. Pair this
with draining the dirty-bin backlog off-box (see the 2026-07-14 off-box compaction recipe).

## 6. Test plan (before any deploy)

Unit (`src/dml_coalescer.rs` `mod tests`):
- `split_rounds` unchanged path still splits (regression guard).
- `collapse` produces one row per key with concatenated arrays (additive) and last value
  (LWW); assert equivalence to applying the N rounds sequentially on a fixture.
- Unrecognized assignment → falls back to `split_rounds`.

E2E (`tests/e2e/`, real MinIO + bootstrap):
- Enqueue the same `(span,trace)` key across several deferred statements with different
  tags; force a drain; assert **1 merge** (via `coalesce_merges` delta) and the final
  `hashes` = union of all tags. Run with `use_deletion_vectors` true **and** false.
- The additive+LWW mixed statement (UPDATE-1 shape): assert `hashes` union AND `errors`/
  `processed_at` = last writer.
- Idempotence: re-running the same enrichment touches 0 rows (`NOT @>` holds post-collapse).

## 7. Rollout

1. Land Option B behind `TIMEFUSION_DML_COALESCE_COLLAPSE=false`.
2. Validate in staging with the merge-heavy replay (or a synthetic enrichment load) — watch
   the batching ratio, correctness parity, and DV e2e.
3. Canary: enable on one instance; watch `perform_merge_update` rate, OCC conflicts,
   `dirty_bin_queue_depth`, CPU/mem. Roll back = flip the flag (no redeploy of behavior).
4. Only then default on. Coordinate with the team's active `dml_merge_key_prune` work — both
   touch the merge source; sequence them so the parity harness isolates each.

## 8. Non-goals / do-not

- Do **not** widen `TIMEFUSION_DML_COALESCE_SECS` to reduce drains — a longer window
  accumulates *more* cross-emit dup keys → *more* rounds (worse) without the collapse.
- Do **not** ship this to prod without the DV e2e + parity check green in staging. The
  2026-07-19 compaction and warm-full incidents both looked healthy in aggregate metrics and
  still OOM'd/regressed; this path mutates enrichment correctness, so validate behavior, not
  just "it deployed."
