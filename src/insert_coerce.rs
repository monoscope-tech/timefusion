//! Write-path coercion: multi-row INSERT placeholder types (below), and the
//! TF-owned version stamp applied to inbound batches (bottom of the file).
//!
//! Problem. DataFusion parses `INSERT INTO t (cols) VALUES ($1..$N), ($N+1..$2N), ...`
//! into:
//!
//! ```text
//! Dml(Insert)
//!   Projection: column1 AS target_col1, column2 AS target_col2, ...
//!     Values: ($1, ..), ($N+1, ..), ...
//! ```
//!
//! The Projection coerces each `columnX` to the target column type, but the
//! coercion lives on the *column reference* (e.g. `column1 AS target_col`),
//! not on the placeholders inside Values. So
//! `LogicalPlan::get_parameter_types()` reports each `$N` as `None`, and
//! `datafusion-postgres`'s `extract_placeholder_cast_types()` finds no
//! casts either. pgwire then *infers* types positionally from the first
//! row and applies them across all rows — so `$8` (a uuid in row 2 of a
//! 7-col INSERT) gets typed as the row-1 column-1 type (timestamptz) and
//! parsing the uuid string as a datetime errors out.
//!
//! Fix. After the plan is built and before pgwire reads placeholder types,
//! walk the tree, find Values nodes, and wrap each untyped placeholder in
//! `CAST($N AS <values_column_type>)`. The Values column types ARE correct
//! (they've been unified through the Projection), so this makes the
//! placeholders' types match what pgwire needs to ship back to the client.
//! Invoked from the `plan_cache` miss path so every parsed plan goes
//! through it once before being cached.

use std::sync::{
    Arc, OnceLock,
    atomic::{AtomicI64, Ordering},
};

use dashmap::DashMap;
use datafusion::{
    arrow::{
        array::{Array, ArrayRef, TimestampMicrosecondArray},
        datatypes::{DataType, Field, Schema, TimeUnit},
        record_batch::RecordBatch,
    },
    common::tree_node::{Transformed, TreeNode},
    logical_expr::{Cast, Expr, LogicalPlan, Values},
};
use tracing::warn;

pub fn rewrite_plan(plan: LogicalPlan) -> LogicalPlan {
    let result = plan
        .clone()
        .transform_up(|node| {
            let LogicalPlan::Values(values) = node else {
                return Ok(Transformed::no(node));
            };
            let schema = values.schema.clone();
            let column_types: Vec<_> = schema.fields().iter().map(|f| f.data_type().clone()).collect();
            let new_rows: Vec<Vec<Expr>> = values
                .values
                .iter()
                .map(|row| {
                    row.iter()
                        .enumerate()
                        .map(|(col_idx, expr)| {
                            let Some(target_ty) = column_types.get(col_idx).cloned() else {
                                return expr.clone();
                            };
                            let Expr::Placeholder(_) = expr else {
                                return expr.clone();
                            };
                            // Always wrap in Cast. Even if the Placeholder's inferred
                            // `field` already has a matching type, that information
                            // is only set reliably for row-1 placeholders in a
                            // multi-row VALUES; row-2+ get `field: None` and so
                            // `get_parameter_types()` reports them as unknown. Adding
                            // the explicit Cast forces extract_placeholder_cast_types
                            // to pick up every placeholder.
                            Expr::Cast(Cast::new(Box::new(expr.clone()), target_ty))
                        })
                        .collect()
                })
                .collect();
            Ok(Transformed::yes(LogicalPlan::Values(Values { schema, values: new_rows })))
        })
        .map(|t| t.data);
    match result {
        Ok(p) => p,
        Err(e) => {
            // Falling back to the un-coerced plan can leave pgwire serving the wrong
            // placeholder types for multi-row INSERTs — surface at warn! so it's
            // visible in ops dashboards.
            warn!(target: "insert_coerce", "plan rewrite skipped (multi-row INSERT type inference may suffer): {e}");
            plan
        }
    }
}

// ---------------------------------------------------------------------------
// TF-owned version stamp for the schema's `dedup_tiebreak` column.
// ---------------------------------------------------------------------------

/// Per-table hybrid logical clock: the last value issued for each table.
///
/// **INVARIANT: single writer per table.** The ordering these stamps establish
/// is derived from ONE process's wall clock plus its own last-issued value. Two
/// TimeFusion instances writing the same table would issue interleaved and
/// therefore un-orderable stamps, and "greatest wins" would silently pick the
/// wrong version. Scaling writers out needs a real sequencer (or a per-writer
/// epoch in the high bits) — it is not a config change.
static LAST_ISSUED: OnceLock<DashMap<String, AtomicI64>> = OnceLock::new();

fn with_cell<R>(table: &str, f: impl FnOnce(&AtomicI64) -> R) -> R {
    let map = LAST_ISSUED.get_or_init(DashMap::default);
    // Early-return on the read path so we never hold a shard's read guard while
    // taking its write guard (same-shard deadlock).
    if let Some(cell) = map.get(table) {
        return f(cell.value());
    }
    f(map.entry(table.to_string()).or_insert_with(|| AtomicI64::new(i64::MIN)).value())
}

/// Issue the next stamp for `table`: `max(now, last_issued + 1)`. Strictly
/// increasing by construction, so two versions of a row written inside the same
/// microsecond — or after the wall clock steps backwards — can never tie.
pub fn next_stamp(table: &str) -> i64 {
    with_cell(table, |cell| {
        let now = crate::clock::now_micros();
        let mut prev = cell.load(Ordering::Relaxed);
        loop {
            let next = now.max(prev.saturating_add(1));
            match cell.compare_exchange_weak(prev, next, Ordering::AcqRel, Ordering::Relaxed) {
                Ok(_) => return next,
                Err(cur) => prev = cur,
            }
        }
    })
}

/// Fold an already-issued value into the table's clock. Called for every stamp
/// seen during WAL replay so the first stamp issued after a boot exceeds
/// everything durable — without this, a boot behind an NTP step (or with a
/// stamp issued from `last + 1` past wall-clock) would re-issue values that
/// already exist and a new version could lose to an old one.
pub fn observe_stamp(table: &str, value: i64) {
    with_cell(table, |cell| cell.fetch_max(value, Ordering::AcqRel));
}

/// The declared version-stamp column for `table`: the schema's `dedup_tiebreak`
/// when it is a microsecond timestamp. Tables with no tiebreak, or one that
/// isn't TF-ownable (a client-supplied ordering column of some other type), get
/// `None` and are left completely alone.
fn stamp_column(table: &str) -> Option<(&'static str, DataType, bool)> {
    let schema = crate::schema_loader::get_schema(table)?;
    let name = schema.dedup_tiebreak.as_deref()?;
    let (dt, nullable) = schema.field_def(name)?;
    matches!(dt, DataType::Timestamp(TimeUnit::Microsecond, _)).then_some((name, dt, nullable))
}

/// Stamp every batch's version column with a fresh monotonic value.
///
/// One stamp per batch, not per row: a batch is one write, and rows inside it
/// are distinct rows rather than versions of each other. Successive writes get
/// strictly increasing stamps, which is what versioning needs.
///
/// Any value the client sent is **overwritten** — the stamp is TF's, and a
/// client-supplied one would break monotonicity (and let a client pin a stale
/// row as the winner forever). A missing column is appended.
///
/// This sits on `BufferedWriteLayer::insert`, the single funnel every live write
/// passes through (pgwire INSERT → `write_all` → `insert_records_batch`, gRPC
/// ingest, DML-driven appends). WAL replay deliberately bypasses it: those rows
/// were stamped on their original append and must keep that value; replay feeds
/// `observe_stamp` instead.
pub fn stamp_version(table: &str, batches: Vec<RecordBatch>) -> Vec<RecordBatch> {
    let Some((name, data_type, nullable)) = stamp_column(table) else {
        return batches;
    };
    batches
        .into_iter()
        .map(|batch| {
            let tz = match &data_type {
                DataType::Timestamp(_, tz) => tz.clone(),
                _ => None,
            };
            let mut arr = TimestampMicrosecondArray::from(vec![next_stamp(table); batch.num_rows()]);
            if let Some(tz) = tz {
                arr = arr.with_timezone(tz);
            }
            let arr = Arc::new(arr) as ArrayRef;
            let field = Arc::new(Field::new(name, data_type.clone(), nullable));
            let (old_schema, mut columns) = (batch.schema(), batch.columns().to_vec());
            let mut fields: Vec<_> = old_schema.fields().iter().cloned().collect();
            match old_schema.index_of(name) {
                Ok(i) => (fields[i], columns[i]) = (field, arr),
                Err(_) => {
                    fields.push(field);
                    columns.push(arr);
                }
            }
            let schema = Arc::new(Schema::new_with_metadata(fields, old_schema.metadata().clone()));
            // Rebuild is infallible by construction (same row count, matching
            // types); fall back to the un-stamped batch rather than fail a write.
            RecordBatch::try_new(schema, columns).unwrap_or_else(|e| {
                warn!(target: "insert_coerce", "version stamp skipped for table {table}: {e}");
                batch
            })
        })
        .collect()
}

/// Forget a table's issued-stamp state, so a test can simulate a fresh boot.
#[cfg(test)]
pub fn reset_stamp_state(table: &str) {
    LAST_ISSUED.get_or_init(DashMap::default).remove(table);
}

/// Fold a replayed batch's stamps into the table's clock (see `observe_stamp`).
pub fn observe_batch(table: &str, batch: &RecordBatch) {
    let Some((name, _, _)) = stamp_column(table) else {
        return;
    };
    let Some(col) = batch.column_by_name(name) else {
        return;
    };
    if let Some(arr) = col.as_any().downcast_ref::<TimestampMicrosecondArray>()
        && let Some(max) = datafusion::arrow::compute::max(arr)
    {
        observe_stamp(table, max);
    }
}

#[cfg(test)]
mod stamp_tests {
    use datafusion::arrow::array::{Int64Array, StringArray};
    use serial_test::serial;

    use super::*;

    fn unique_table() -> String {
        format!("t{}", &uuid::Uuid::new_v4().to_string()[..8])
    }

    /// One row batch with an `id` column, plus optionally a client-supplied
    /// `updated_at` we expect TF to overwrite.
    fn batch_with(client_stamp: Option<i64>) -> RecordBatch {
        let mut fields: Vec<Arc<Field>> = vec![Arc::new(Field::new("id", DataType::Int64, false))];
        let mut cols: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![1i64]))];
        if let Some(v) = client_stamp {
            fields.push(Arc::new(Field::new("updated_at", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), true)));
            cols.push(Arc::new(TimestampMicrosecondArray::from(vec![v]).with_timezone("UTC")));
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).unwrap()
    }

    fn stamp_of(batch: &RecordBatch) -> Option<i64> {
        let col = batch.column_by_name("updated_at")?;
        let arr = col.as_any().downcast_ref::<TimestampMicrosecondArray>()?;
        (!arr.is_null(0)).then(|| arr.value(0))
    }

    /// Two writes inside the SAME microsecond must not tie — "greatest version
    /// wins" is meaningless if two versions share a stamp. The clock is frozen
    /// (not slept on) so the microsecond really is identical.
    #[test]
    #[serial]
    fn stamps_strictly_increase_within_one_microsecond() {
        let t = unique_table();
        crate::clock::set_micros(4_000_000_000_000_000);
        let (a, b, c) = (next_stamp(&t), next_stamp(&t), next_stamp(&t));
        crate::clock::unfreeze();
        assert!(a < b && b < c, "stamps must be strictly increasing, got {a} {b} {c}");
    }

    /// An NTP step backwards must not let a NEW version be issued a stamp that
    /// an OLD version already holds.
    #[test]
    #[serial]
    fn stamps_survive_a_clock_going_backwards() {
        let t = unique_table();
        crate::clock::set_micros(4_000_000_000_000_000);
        let before = next_stamp(&t);
        crate::clock::set_micros(3_000_000_000_000_000); // clock steps back an epoch
        let after = next_stamp(&t);
        crate::clock::unfreeze();
        assert!(after > before, "stamp regressed across a backwards clock step: {before} -> {after}");
    }

    /// Boot seeding: whatever WAL replay observed bounds every later stamp,
    /// even when it is far ahead of the wall clock.
    #[test]
    #[serial]
    fn observed_replay_stamps_bound_the_next_issue() {
        let t = unique_table();
        crate::clock::set_micros(4_000_000_000_000_000);
        let replayed = 9_000_000_000_000_000_i64; // well past "now"
        observe_stamp(&t, replayed);
        let next = next_stamp(&t);
        crate::clock::unfreeze();
        assert!(next > replayed, "post-boot stamp {next} must exceed the replayed max {replayed}");
    }

    #[test]
    fn stamp_fills_a_missing_column_and_overwrites_a_client_supplied_one() {
        let out = stamp_version("otel_logs_and_spans", vec![batch_with(None), batch_with(Some(1_234))]);
        let filled = stamp_of(&out[0]).expect("missing column is appended and populated");
        let overwritten = stamp_of(&out[1]).expect("client value is replaced, not left");
        assert_ne!(overwritten, 1_234, "a client-supplied stamp must be overwritten by TF's");
        assert!(overwritten > filled, "successive batches get increasing stamps");
    }

    /// A table declaring no `dedup_tiebreak` is left byte-for-byte alone.
    #[test]
    fn table_without_a_tiebreak_is_untouched() {
        assert!(stamp_column("variant_bench").is_none(), "variant_bench declares no dedup_tiebreak");
        let before = batch_with(None);
        let out = stamp_version("variant_bench", vec![before.clone()]);
        assert_eq!(out[0].schema(), before.schema());
        assert_eq!(out.len(), 1);
        // Unknown tables (per-test WAL/MemBuffer tables) are likewise untouched.
        assert!(stamp_column(&unique_table()).is_none());
    }

    /// `observe_batch` reads the schema's tiebreak column, whatever it is named
    /// — nothing hard-codes `updated_at`.
    #[test]
    fn observe_batch_is_schema_driven() {
        let t = unique_table();
        let schema = Arc::new(Schema::new(vec![Field::new("ingested_at", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), true)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(TimestampMicrosecondArray::from(vec![7_777_i64]).with_timezone("UTC")) as ArrayRef]).unwrap();
        // otel_metrics declares `ingested_at`; the same code path picks it up.
        assert_eq!(stamp_column("otel_metrics").map(|(n, _, _)| n), Some("ingested_at"));
        observe_batch("otel_metrics", &batch);
        assert!(next_stamp("otel_metrics") > 7_777);
        // A table with no schema observes nothing (and must not panic).
        observe_batch(
            &t,
            &RecordBatch::try_new(Arc::new(Schema::new(vec![Field::new("x", DataType::Utf8, true)])), vec![Arc::new(StringArray::from(vec!["a"])) as ArrayRef])
                .unwrap(),
        );
    }
}
