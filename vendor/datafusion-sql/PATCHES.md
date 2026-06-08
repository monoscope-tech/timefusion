# TimeFusion patches to `datafusion-sql`

Vendored from `datafusion-sql = "53.1.0"` (crates.io). Diff against upstream
should be limited to the patches listed here.

## `UPDATE ... FROM` planner guard removed

**File:** `src/statement.rs`, in the `Statement::Update { .. }` arm of
`sql_statement_to_plan` (around the construction of `update_from`).

**Upstream:** had a defensive early-return:

```rust
// UPDATE ... FROM is currently not working
// TODO fix https://github.com/apache/datafusion/issues/19950
if update_from.is_some() {
    return not_impl_err!("UPDATE ... FROM is not supported");
}
```

**Patched:** guard deleted. `update_to_plan` (~line 2151) already accepts
`from: Option<TableWithJoins>` and threads it through `plan_from_tables`,
so the surrounding code lowers `UPDATE … FROM src WHERE …` to a normal
`Dml { input: Projection → Filter → Join }` plan. TimeFusion's
`DmlQueryPlanner` (`src/dml.rs`) recognizes the `Join`, materializes the
source side, and dispatches to either the `MergeBuilder` (committed Delta
data) or a MemBuffer hash-join (uncommitted buffered rows).

**Tracking:** when `apache/datafusion#19950` lands upstream and we bump
`datafusion` past that fix, this vendor directory can be removed and the
`[patch.crates-io]` override deleted from the workspace `Cargo.toml`.
