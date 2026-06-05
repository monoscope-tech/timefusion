# TimeFusion: Code Conciseness & Consolidation Research

*Scope: `src/` (~18.3k LOC across 38 files). Vendored crates under `vendor/` excluded.
Findings are grounded in file:line evidence; pattern counts verified by grep.*

## TL;DR

The codebase is generally well-structured and already uses good tools (`strum` in 7
files, `derive_more` in 3, an `envy`-driven config, a `const_default!` macro, a
`BinaryAccessor` accessor enum). The conciseness wins are concentrated in **a few
cross-cutting patterns repeated across many files** plus **boilerplate clusters in the
five large files**. Realistic, low-risk reduction: **~1,000–1,300 LOC** with materially
better readability.

The three highest-leverage moves, in order:

1. **A shared Arrow array-access / downcast layer** — `downcast_ref::<…>()` appears
   **65 times**; string/timestamp multi-type downcast-and-loop blocks repeat across
   `database.rs`, `functions.rs`, `mem_buffer.rs`, and `tantivy_index/{builder,udf,mem_index}.rs`.
2. **Error-conversion extension traits / wider `thiserror`** — `map_err(|e| DataFusionError::…)`
   (28×), `Execution(format!(…))` (27×), and `map_err(|e| anyhow!(…))` (18×) are mechanical noise.
3. **A scheduled-job + "apply to all tables" helper** in `database.rs` — ~200 LOC of
   closure/`Box::pin` scaffolding and duplicated unified+custom table loops.

---

## Part 1 — Cross-cutting wins (highest leverage)

These touch many files, so one helper pays off repeatedly.

### 1.1 Shared Arrow array-access module  ⭐ biggest single win
**Evidence:** `downcast_ref::<…>` = 65 sites; `StringViewArray` downcast logic duplicated in
`database.rs`, `functions.rs`, `mem_buffer.rs`, `tantivy_index/builder.rs`, `tantivy_index/udf.rs`,
`tantivy_index/mem_index.rs`, `wal.rs`. Examples:
- `functions.rs:44-52` (`extract_scalar_string`), `:979-997`, `:1631-1656` — StringView→String→Utf8 fallback
- `functions.rs:923-949`, `:586-609`, `:693-739`, `:1132-1166` — `TimestampMicrosecond`/`Nanosecond` downcast-and-build
- `builder.rs:154-166` and `udf.rs:88-102` — two near-identical `string_extractor` fns
- `database.rs:279-289` — 4-arm timestamp-precision downcast

**The codebase already has the right pattern half-built:** `functions.rs:1501` defines
`enum BinaryAccessor<'a>` with `try_new()` + unified `value()`. Promote this idea into a
shared module:

```rust
// src/arrow_access.rs  (new)
pub enum StrAccessor<'a> { View(&'a StringViewArray), Utf8(&'a StringArray), Large(&'a LargeStringArray) }
impl<'a> StrAccessor<'a> {
    pub fn try_new(arr: &'a ArrayRef) -> DFResult<Self> { /* one downcast cascade */ }
    pub fn value(&self, i: usize) -> Option<&str> { /* unified */ }
}
pub enum TsAccessor<'a> { Micros(&'a TimestampMicrosecondArray), Nanos(&'a TimestampNanosecondArray), /* … */ }
impl<'a> TsAccessor<'a> {
    pub fn try_new(arr: &'a ArrayRef) -> DFResult<Self> { … }
    pub fn iter_micros(&self) -> impl Iterator<Item = Option<i64>> + '_ { … }
}
```

**Impact:** collapses ~15 string blocks and ~6 timestamp blocks; est. **180–250 LOC** removed
and the remaining call sites become 1–2 lines. Highest ROI of any item here.

### 1.2 Error-conversion extension traits (and wider `thiserror`)
**Evidence:** `map_err(|e| DataFusionError::…)` 28×, `Execution(format!(…))` 27×,
`map_err(|e| anyhow!(…))` 18×. `thiserror` is currently used in **only** `wal.rs`.
Hot spots: `database.rs:223/248/300/332-333/370` (Arrow→DF), `:1386/1396/1417/1427` (exec context),
`buffered_write_layer.rs:924/941`, and the whole `tantivy_index/` tree.

```rust
// src/error_ext.rs (new) — two tiny extension traits
pub trait ArrowErrExt<T> { fn df(self) -> DFResult<T>; }
impl<T> ArrowErrExt<T> for Result<T, ArrowError> {
    fn df(self) -> DFResult<T> { self.map_err(|e| DataFusionError::ArrowError(Box::new(e), None)) }
}
pub trait ExecCtx<T> { fn exec_ctx(self, msg: &str) -> DFResult<T>; }
impl<T, E: Display> ExecCtx<T> for Result<T, E> {
    fn exec_ctx(self, m: &str) -> DFResult<T> { self.map_err(|e| DataFusionError::Execution(format!("{m}: {e}"))) }
}
```
Plus a one-line lint-style cleanup in `tantivy_index/`: replace `.map_err(|e| anyhow!("…: {e}"))`
with `anyhow::Context::context("…")` (already the style in `store.rs`; standardize everywhere —
`reader.rs:23/26/27`, `builder.rs:72-73`).

For module-level error enums (`buffered_write_layer`, `wal` already), lean on `thiserror`’s
`#[from]` so `?` converts automatically instead of manual `map_err`.
**Impact:** ~60–90 LOC, plus uniform error messages. Low risk.

### 1.3 Centralized key/path builders
**Evidence:** `format!("{}:{}", project_id, table_name)` in `wal.rs:266`, `statistics.rs:42/149`;
the tuple key `(project_id.to_string(), table_name.to_string())` appears **13×** in `database.rs`
(54, 315, 377, 1315, 1377, 1408, 1495, 1537, …); object-store path `format!`s in
`tantivy_index/manifest.rs:59`, `store.rs:28`.

```rust
fn table_key(p: &str, t: &str) -> (String, String) { (p.into(), t.into()) }
fn cache_key(p: &str, t: &str) -> String { format!("{p}:{t}") }
```
**Impact:** ~25 LOC + removes a class of "forgot to match the key format" bugs.

---

## Part 2 — Per-file boilerplate clusters

### 2.1 `database.rs` (4050 LOC — the elephant)
| # | Pattern | Sites | Fix | ~LOC |
|---|---------|-------|-----|------|
| A | Scheduled-job scaffolding: `Job::new_async(… { let db=db.clone(); move|_,_| { let db=db.clone(); Box::pin(async move {…}) }})` then `scheduler.add(…)` | 6 (826, 864, 909, 944, 975, 995) | `add_scheduled_job(&self, sched, schedule, closure)` helper | ~120 |
| B | "Iterate unified tables, then custom tables, apply op + log ok/err" | 5 (833, 871, 953, 1004, 3833) | `for_each_table(\|name, tbl\| …)` helper | ~70–90 |
| C | Cache double-check resolve (read-lock check → write-lock create) for unified vs custom | 2 (1370-1390, 1438-1532) | generic `resolve_table_generic<K>(cache, key, create_fn)` | ~70 |
| D | Variant column rebuild (iterate fields, mutate cols, rebuild schema) | 2 (197-249, 303-371) | `BatchMutator` builder | ~80 |
| E | Partition-filter-by-date construction | 3 (2005, 2192, 2249) | `partition_filters_for_date_range()` | ~10 |
| F | `DisplayAs::fmt_as` identical match arms | `2849-2860` | collapse to single `write!` | ~5 |

Structural: the `impl Database` block spans **lines 430–2442 (~2000 LOC)**. Split by concern into
`database/{resolve, write, maintenance, session, stats}.rs` impl blocks (no LOC change, large
navigability gain). This is the single biggest readability lever in the repo.

### 2.2 `functions.rs` (1727 LOC)
- **UDF boilerplate:** 8 `ScalarUDFImpl` impls repeat `as_any/name/signature` (the
  `scalar_udf_boilerplate!` macro at `:60-72` only covers 3 of 5 methods). A `define_scalar_udf!`
  macro that also generates the struct + `Default` (signature init) collapses ~**100 LOC**.
  UDFs at 546, 652, 783, 854, 905, 1307, 1405.
- Arg extraction + string/timestamp downcast → folded into the shared accessor module (§1.1).
- **Split** into `functions/{udfs,aggregates,json,variant,registry}.rs` — five clear seams.

### 2.3 `mem_buffer.rs` (2027 LOC) + write path
- `bucket.batches.lock().iter().cloned().collect()` snapshot repeated 6× (822, 848, 981, …) →
  `TimeBucket::snapshot_batches()`.
- `DFSchema::try_from(schema) + ExecutionProps::new()` repeated (1078, 1156, 1264) → one helper.
- `wal.rs`: `append` / `append_delete` / `append_update` share identical topic→shard→key→serialize
  scaffolding (312-321, 343-357, 361-379) → one private `append_entry(op, data)`. ~25 LOC.
- `TimeBucket::new` should take `shards_per_topic` and pre-size `wal_shard_state` vectors,
  removing the runtime `if counts.len() <= shard` resize checks (1375-1381).

### 2.4 `object_store_cache.rs` (1409 LOC)
- `ObjectStore` impl (902-986) hand-delegates ~10 methods to `self.inner`. Add the **`delegate`
  crate** (~`delegate = "0.13"`) and wrap the pure pass-throughs in one `delegate! { to self.inner {…} }`
  block; keep only the methods with cache-invalidation side effects (`copy_opts`, etc.) hand-written.
  ~25–30 LOC and far clearer intent.

### 2.5 `config.rs` (686 LOC) — *correction vs. first impression*
Config is **not** hand-rolled: it already uses `envy::from_env()` per struct (`:8-20`) and a
`const_default!` macro (`:48`) for ~50 default fns. The residual repetition is the pairing of each
`const_default!(d_x …)` with a `#[serde(default = "d_x")]` attribute. A single
`config_field!(name: T = default)` macro that emits *both* the default fn and is referenced by the
field would remove the duplication — modest (~30–40 LOC), medium effort, optional.

### 2.6 `autotune.rs` (182 LOC)
The `if env_unset("X") { compute; if derived != cfg.field { cfg.field = derived; applied.push(("X", fmt)) } }`
block repeats **7×** (71, 80, 89, 98, 108, 115, 125). Extract:
```rust
fn apply_override<T: PartialEq>(env: &str, field: &mut T, derived: T,
    applied: &mut Vec<(&str,String)>, fmt: impl Fn(&T)->String) { … }
```
~30 LOC and turns each block into one call.

### 2.7 Smaller, high-clarity wins
- `clock.rs:36-61` — extract `get_or_wall()` for the `FROZEN_NOW == WALL_SENTINEL ? now : v` check
  used 4×.
- `telemetry.rs:67-79` — build the fmt layer once, conditionally `.json()`, instead of the
  duplicated if/else branches.
- `stats_table.rs:46-99` — a `stat_row!` macro (or `add_stat(…)` helper) for the repeated
  `(component, key.into(), value.to_string())` rows.
- `grpc_handlers.rs` — `WriteAck::ok(seq, pressure)` / `WriteAck::error(seq, pressure, msg)`
  constructors (or `derive_more::Constructor`) instead of inline struct literals (156-174).
- `pgwire_handlers.rs:203-242` — `classify_query` as a `strum`-derived enum instead of
  `starts_with`/`contains` chains.
- `tantivy_index`: one `create_index_with_tokenizers(schema)` wrapper (used by builder/store/mem_index)
  so `register_tokenizers` is never forgotten; a `manifest::mutate_with_report` to fold the
  load-modify-save in `service.rs:156-186` into the existing `manifest::mutate` style.
- `plan_cache.rs` — newtype `PlanCacheKey(String)` for type safety on the canonical-SQL key.

---

## Part 3 — Library / derive / macro strategy

**Already in use — extend, don’t add:**
- `strum` (7 files) — apply to `classify_query` and any remaining string↔enum maps.
- `derive_more` (3 files) — use `Constructor`, `From`, `Display` on the many small structs/newtypes
  (`WriteAck`, cache keys, config sub-structs).
- `thiserror` (only `wal.rs`!) — **biggest untapped derive**: define per-module error enums with
  `#[from]` so `?` replaces the 28+ manual `map_err(|e| DataFusionError::…)` conversions.

**Worth adding:**
- **`delegate`** — kills the `ObjectStore` pass-through boilerplate (§2.4). Small, well-maintained.

**Project-local macros to introduce:**
- `define_scalar_udf!` (UDF struct + boilerplate, §2.2).
- `config_field!` (default fn + serde default, §2.5).
- `stat_row!` (§2.7).
- Extension traits `ArrowErrExt` / `ExecCtx` (§1.2) — not a macro, but the same de-boilerplating effect.

**Patterns that need a helper, not a crate:** array accessors (§1.1), key builders (§1.3), the
scheduled-job and for-each-table helpers (§2.1) — these are domain-specific and best kept in-repo.

---

## Part 4 — Suggested order of work (risk-adjusted)

1. **§1.2 error traits + `thiserror`/`context` cleanup** — mechanical, compiler-checked, touches
   everything, immediate readability. *(~1 day, ~80 LOC, very low risk)*
2. **§1.1 shared array-access module** — biggest LOC win; migrate file-by-file behind the new module.
   *(~2 days, ~200 LOC, low risk — covered by existing tests)*
3. **§2.1 database.rs job + table-loop helpers** and **§2.6 autotune** — contained, high clarity.
4. **§2.2/§2.3 UDF macro + write-path snapshot/append helpers.**
5. **Structural splits** (`database.rs`, `functions.rs` into submodules) — do last, purely
   mechanical `mod` moves, large navigability payoff.
6. **Optional/low priority:** `delegate` crate, `config_field!` macro, the §2.7 micro-wins.

**Estimated total reduction:** ~1,000–1,300 LOC (≈6–7% of `src/`), with the bigger benefit being
fewer divergent copies of the same logic (downcast cascades, error strings, key formats) that today
must be kept in sync by hand.
