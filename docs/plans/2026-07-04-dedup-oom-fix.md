# Plan: stop the recurring TF OOM crash-loop (dedup decoded-footprint + maintenance concurrency + watermark deploy)

## Root cause recap (evidenced 2026-07-04)

- Kernel: `cgroup OOM Killed process (timefusion) anon-rss:89189484kB` invoked by a `tokio-rt-worker`; at the same time `mem_buffer=1.4GB`, `process_rss=34GB` climbing. The 88 GB is Arrow working memory **outside** the tracked buffer.
- 90 s before each kill: 17 `dedup` + 1 `OPTIMIZE`, no query storm.
- `dedup rewrite … before=1000317 after=962350` — dedup `SELECT * … collect()`s a whole ~1M-row 10-min chunk (`database.rs:4275`), guarded only by `timefusion_dedup_max_rewrite_bytes = 2 GiB` measured as **compressed on-disk `sum(add.size)`** (`database.rs:4255`). Wide Variant/JSON rows decode to Arrow at 5–20× → an at-budget chunk materializes tens of GB.
- `build_optimize_session_state` already uses the pooled `shared_runtime_env`, yet it still OOMed → **the DataFusion `GreedyMemoryPool` does not account for `SELECT *`/collect Arrow buffers** (it tracks operator working memory only). The prior "route maintenance through the pool" fix was necessary but insufficient. This is why it recurs.
- Self-sustaining loop: OOM → restart → 42 s WAL re-replay + re-flush of not-yet-checkpointed buckets → manufactures duplicates → dedup gets million-row chunks → OOM.

## Fix 1 — dedup budget measures DECODED in-memory bytes, not compressed on-disk bytes

**File:** `src/database.rs` (`dedup_rewrite_chunk`, ~4251–4275), `src/config.rs`.

- Add config `timefusion_dedup_max_decoded_bytes` (new; default e.g. `4 * GIB`) alongside the existing `timefusion_dedup_max_rewrite_bytes` (keep the compressed guard too — cheap early-out).
- Compute an **estimated decoded footprint** for the target file set before `collect()`:
  - Preferred: sum `num_records` across `targets` (from `Add` stats via `f.stats`/`get_stats().num_records`) × a per-row-bytes estimate derived from the schema (or a conservative constant for Variant tables, e.g. 4–8 KB/row), doubled to cover the `dedup_batches` RowConverter's parallel keyed copy.
  - Fallback when stats absent: `sum(add.size) × inflation_factor` (new config `timefusion_dedup_decode_inflation`, default ~12) — never smaller than the compressed guard.
- If estimated decoded bytes > `timefusion_dedup_max_decoded_bytes`: **skip loudly** (reuse the existing `record_dedup_chunk_skipped()` + `error!` skip path). Correctness is preserved — duplicates persist and read-side `DedupExec` collapses them at query time (the documented design). Verify: this replaces an OOM with a logged skip.
- Verify wording of the misleading comment at 4269–4272 ("bounded by … the shared memory pool (a miss errors here instead of OOM)") — correct it to state the pool does NOT bound the `SELECT *` collect, and the decoded-byte pre-check is the real guard.

## Fix 2 — global maintenance-rewrite semaphore (backstop for estimate error)

**File:** `src/database.rs` (`Database` struct + `dedup_rewrite_chunk`, `optimize_table_light`, recompress).

- Add `maintenance_rewrite_sem: Arc<tokio::sync::Semaphore>` to `Database`, permits = new config `timefusion_maintenance_rewrite_concurrency` (default **1**). Rationale: only one heavy maintenance materialization in flight at a time, so even a mis-estimated chunk can use a large slice of headroom without a second op stacking on top.
- Acquire a permit around the materialize+write body of `dedup_rewrite_chunk`, `optimize_table_light_inner`, and `recompress_partition`. Scope the guard to the heavy section only (not the whole sweep), so cheap no-op ticks don't hold it.
- **Merges (dual-write UPDATE path): NOT gated by this semaphore.** Decision + rationale: merges are on the ingest hot path; serializing them behind maintenance would re-introduce the ingest stalls we're trying to remove, and merges were not the observed OOM trigger (they cap source rows at `MAX_UPDATE_SOURCE_ROWS = 1M` and the target read lives inside delta-rs `MergeBuilder`). Left as an explicit follow-up if merge footprint later shows up in an OOM. This is a deliberate divergence from the "across … merges" wording, called out for review.

## Fix 3 — make the watermark rework deployable (cut duplicate generation)

**Scope boundary:** actual prod rollout (CapRover/CI) is the user's gated action — NOT done autonomously here.

- The uncommitted watermark work (WAL position-watermark, snapshot/prefix-drain flush, DML pinning, reap, corruption-continue) from earlier this session stops re-committing already-flushed rows on restart → fewer duplicates → smaller dedup chunks. It's the loop's fuel line.
- Action: commit it to a branch with the full test suite green (222 lib + 12 regression + e2e 16/16 already verified), push, and hand the user a deploy-ready SHA. Do not deploy.

## Tests (bug-fix workflow: failing test first)

- `dedup_skips_chunk_over_decoded_budget`: build a chunk whose `num_records × per-row` exceeds `max_decoded_bytes` but whose compressed `add.size` is under `max_rewrite_bytes`; assert the rewrite **skips** (returns 0, increments dedup_chunk_skipped) rather than materializing. This fails today (compressed budget lets it through → collect).
- `maintenance_rewrite_semaphore_serializes`: assert two concurrent `dedup_rewrite_chunk`/optimize calls do not hold the heavy section simultaneously (permit count 1).
- Keep existing dedup correctness tests green (dedup still drops dupes when under budget).

## Verification

- `cargo test` (unit + regression), `cargo clippy --lib` clean, e2e green.
- Post-deploy prod: OOM kills stop (no exit 137), `dedup_chunk_skipped` may rise (expected — skips instead of OOMs), Kafka `apitoolkit_eu` lag drains to ~0 and stays, `retryHasqlWrite … starting up` log volume drops to near-zero.
