# TimeFusion patches on vendored datafusion-postgres

This directory is a vendored copy of [`datafusion-postgres`](https://github.com/datafusion-contrib/datafusion-postgres) at the version pinned in the top-level `Cargo.toml`. The next `cargo vendor` / manual resync will silently overwrite the local edits below. If you upgrade the dep, re-apply each section by hand or regenerate the patch with `git diff master -- vendor/datafusion-postgres/`.

## CI sentinel

Drop this guard into the `Format` (or any always-running) job so a silent
resync fails the build instead of regressing the pgwire optimisation:

```bash
grep -q 'was_pre_optimized' vendor/datafusion-postgres/src/hooks/mod.rs \
    && grep -q 'was_pre_optimized' vendor/datafusion-postgres/src/handlers.rs \
    || { echo "vendored datafusion-postgres lost TimeFusion patches — see vendor/datafusion-postgres/PATCHES.md"; exit 1; }
```

## 1. `src/hooks/mod.rs` — `QueryHook::was_pre_optimized`

Adds a method to the `QueryHook` trait:

```rust
fn was_pre_optimized(&self, _canonical_sql: &str) -> bool {
    false
}
```

Lets a hook signal that any `LogicalPlan` it returned from `handle_extended_parse_query` has already been through `state.optimize()`. Default is conservative (`false` → caller will optimize as if freshly parsed). TimeFusion's `PlanCacheHook` overrides this to look up the canonical SQL in its post-optimize cache — when present the same plan is going through optimize a second time per query, which is the dominant cost under load.

## 2. `src/handlers.rs` — skip per-query optimize on cache hit

The extended-query path in `DfSessionService::do_query`:

```rust
let canonical_sql = &portal.statement.statement.0;
let pre_optimized = self.query_hooks.iter().any(|h| h.was_pre_optimized(canonical_sql));
let optimised = if pre_optimized {
    plan
} else {
    self.session_context.state().optimize(&plan)
        .map_err(|e| PgWireError::ApiError(Box::new(e)))?
};
```

Replaces the unconditional `state.optimize(&plan)` call. Bypass paths (DDL, literal-only SQL, statements where no hook claimed the plan) still optimize. Measured impact at 300w + 75r @ 1500 r/s: server-side pgwire p95 dropped from 131 ms → 8 ms (~16×).

**Scope** — only the extended query handler is patched. `SimpleQueryHandler` is not, because `PlanCacheHook::handle_simple_query` returns `None` (simple-query plans are not cached), so `was_pre_optimized` would always be `false` and the optimize call is correct. Ad-hoc `psql` sessions running simple queries still pay the per-query optimize cost; this is intentional — the optimisation targets parameterised production traffic, not interactive debugging.
