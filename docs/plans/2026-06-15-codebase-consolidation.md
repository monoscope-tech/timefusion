# Codebase Consolidation Plan — LOC Reduction

_Generated 2026-06-15 from a multi-agent audit of `src/` and `tests/` (~26.6k src LOC across ~45 files), grounded in the CLAUDE.md conciseness/zero-boilerplate philosophy._

## Method

- 14 per-module finders + 6 cross-cutting duplication sweeps → **99 raw findings**.
- Top 14 by claimed impact were **adversarially verified against the actual code**; only viable, net-LOC-positive ones survived.
- Result: **~230 LOC** of realistic, **low-risk** reduction across 6 independent workstreams. Larger speculative refactors were rejected (see end) — they net zero/negative LOC or risk correctness. **Do not re-attempt the rejected items.**

Durability paths (WAL append, flush, Delta commit) are untouched except pure error-message wrapping that preserves the variant and message text.

## Workstreams (ordered by LOC-saved-per-risk)

### 1. Shared error-wrapping helpers — ~55 LOC, low risk
~21 identical `.map_err(|e| DataFusionError::ArrowError(Box::new(e), None))` sites, plus repetitive `DataFusionError::Execution(format!(...))` chains (dml.rs ×10) and `WalError`-wrap chains (buffered_write_layer.rs ×4).

1. Add an `errors` helper (or top of database.rs): `pub fn arrow_err(e: ArrowError) -> DataFusionError { DataFusionError::ArrowError(Box::new(e), None) }`. → verify: `cargo build`.
2. Replace all ~21 ArrowError closures with `.map_err(arrow_err)` — database.rs:498,523,575,607,608,645; mem_buffer.rs:456,460,1540,1660,1665,1668,1705,1714,1718,1734,1756; plan_cache.rs:220,250; stats_table.rs:192; dml.rs:393; functions.rs:282. → verify: `cargo build` clean, `cargo test` green.
3. Add `exec_err(ctx: &'static str) -> impl Fn(E) -> DataFusionError`; apply to dml.rs Execution sites, keeping distinct context strings. → verify: `cargo test --test test_dml_operations --test pgwire_dml_tag_test` (identical error strings).
4. Add `wal_err(op: &'static str) -> impl Fn(WalError) -> DataFusionError`; apply to buffered_write_layer.rs:1637,1654,1671,1679. → verify: WAL append error path still surfaces the op name.

**Files:** database.rs, mem_buffer.rs, plan_cache.rs, stats_table.rs, dml.rs, functions.rs, buffered_write_layer.rs.
**Dependency:** steps 1 & 3/4 should live in one helper module to avoid duplicate definitions.

### 2. Collapse UDF registration + thin wrappers (functions.rs) — ~45 LOC, low risk
`register_custom_functions` has 14+ `ctx.register_udf(ScalarUDF::from(T::default()))` lines plus 7 single-line `create_*_udf()` forwarding wrappers.

1. Add `macro_rules! reg { ($ctx:expr, $($T:ty),+ $(,)?) => { $( $ctx.register_udf(ScalarUDF::from(<$T>::default())); )+ } }`; replace contiguous default-ctor registrations (390,403,404,422-434). Keep `create_udf`-based and test-clock UDFs as-is. → verify: `cargo test` + `\df` over psql shows the same UDF set.
2. Inline the 7 thin `create_*_udf()` wrappers (e.g. `create_to_char_udf` at 544) into call sites; delete the wrappers. Keep factory fns with real logic (set_clock, advance_clock, time_bucket, percentile). → verify: `cargo test --test test_custom_functions --test test_postgres_json_functions`.
3. _Optional:_ extend `scalar_udf_boilerplate!` with a `=> ReturnType` arm for static-return UDFs only. **Do NOT touch `jsonb_wrapper!`** (rejected). → verify: `cargo test`.

**Files:** functions.rs.

### 3. Cache stats convenience methods (object_store_cache.rs) — ~22 LOC, low risk
Repeated `update_stats(|s| { ... })` blocks (794-798, 972-976, 1052-1056, 1397-1401) and single-line hit/put/ttl closures (721,766,775,866,893,948,1133).

1. Add `async fn record_hit/record_miss_with_fetch/record_ttl/record_put` (+ metadata variants) delegating to existing `update_stats`/`update_metadata_stats`. → verify: `cargo build`.
2. Replace the 11 call sites. → verify: `cargo test`; cache hit/miss integration test reports identical counters.

**Files:** object_store_cache.rs.

### 4. Shared test-harness helpers — ~60 LOC, low risk
`src/test_utils.rs::test_helpers` (TestConfigBuilder, json_to_batch, test_span_ts) and `tests/e2e/harness.rs` already exist as homes.

1. Move `insert_at`/`insert_for` (byte-identical across eviction.rs:34, flush_lifecycle.rs:61, restart_recovery.rs:145/157) to e2e/harness.rs; import everywhere. → verify: `cargo test --test e2e` (Docker) or `cargo build --tests`.
2. Add `array_get_str(arr, idx) -> String` to test_helpers; replace `get_str` dupes in buffer_consistency_test, delta_rs_api_test, test_postgres_json_functions, test_dml_operations, test_custom_functions. → verify: `cargo test --test test_custom_functions --test test_dml_operations`.
3. Migrate `create_test_config` copies to TestConfigBuilder. **Conflict:** test_dml_operations uses a distinct `/tmp` prefix — only migrate if the builder accepts a path prefix, else migrate just the 2 identical copies (integration_test, pgwire_dml_tag_test). → verify: unchanged temp-dir isolation.
4. Add `short_test_id()` / `short_uuid(len)` for the scattered `Uuid::new_v4().to_string()[..n]` idiom. → verify: `cargo build --tests`.

**Files:** test_utils.rs, e2e/{harness,eviction,flush_lifecycle,restart_recovery}.rs, buffer_consistency_test, delta_rs_api_test, test_postgres_json_functions, test_dml_operations, test_custom_functions, integration_test, pgwire_dml_tag_test.

### 5. WAL serialization shim cleanup — ~14 LOC, low risk
3 near-identical bincode deserialize fns (wal.rs:992-1005) + 2 pass-through public wrappers (1010-1019).

1. Add `fn decode_payload<T: Decode>(data: &[u8]) -> Result<T, WalError>`; replace the 3 fns; update callers to `decode_payload::<DeletePayload>(...)` etc. → verify: WAL replay round-trips Delete/Update payloads.
2. Make `serialize_record_batch`/`deserialize_record_batch` `pub(crate)`, delete the `_public` wrappers, update 2 callers. → verify: WAL `UPDATE…FROM` source-batch round-trip.

**Files:** wal.rs, buffered_write_layer.rs.

### 6. Optimizer + tantivy shared predicates & constants — ~34 LOC, low risk
1. Add `extract_utf8_string(&ScalarValue) -> Option<String>` and `is_string_datatype(&DataType) -> bool` to optimizers/mod.rs; replace 4 + 3 inline sites. → verify: optimizer unit tests.
2. Extract shared `string_extractor` for tantivy builder.rs/udf.rs (strict Result + lenient Option variants). → verify: `cargo test --test tantivy_index_test`.
3. Replace raw `* 1024 * 1024 * 1024` with `GIB` at config.rs:754 (+ 4 test assertions). → verify: `cargo test config`.

**Files:** optimizers/{mod,variant_insert_rewriter,tantivy_rewriter,pg_array_literal_rewriter}.rs, tantivy_index/{builder,udf}.rs, config.rs.

## Rejected (do not re-attempt)

- **Unify `resolve_unified_table`/`resolve_custom_table`** — incompatible key types (String vs tuple), sentinel version-key contract, divergent error messages. Net ≤0 LOC + indirection.
- **Generic `get_or_compute<K,V,F,G>`** across caches — ~150 LOC of trait/lifetime machinery to save ~30; net **−110 LOC** and hides per-cache differences.
- **Merge `perform_delta_update`/`perform_delta_delete`** — UpdateBuilder vs DeleteBuilder share no trait, different metrics fields/spans, UPDATE has an assignment loop. Net 0 to −5, correctness-risky.
- **Rework `const_default` macro** (52 expansions) — invocations carry load-bearing incident-history comments that must stay adjacent. ~12 LOC best case, medium risk, lost doc locality.
- **Collapse `jsonb_wrapper!` / dual JSONB wrapper layer** — name parameterization breaks derives; breakeven-to-negative, loses `aliases()` clarity.
- **Unify pgwire TestServer / connection-retry / server-spawn** (3 findings) — retry strategies, return types, port/init paths diverge materially; forced extraction loses test isolation, near-zero net.
- **Timestamp array unit-dispatch macro** (Micros/Nanos) — Arrow arrays aren't polymorphic; per-arm output types + per-row math differ. ~10 LOC, hides domain logic.
- **Multi-variant string-array downcast helper** (9 sites) — surrounding code (mask build vs iterator vs lifetime-capturing closure vs AsArray) diverges; net-negative.
- **Generic retry-with-backoff helper** (database.rs update_table + write_append) — different lock-release-before-sleep semantics; only pursue behind a failing test guarding the lock-release invariant.

## Suggested execution order

Workstreams are independent. Do **1 → 2** first (mechanical, compiler-verified, zero behavior change), then **4** (test helpers, no prod risk), then **3, 5, 6**. Run `cargo test` after each workstream; each step above carries its own verify check.
