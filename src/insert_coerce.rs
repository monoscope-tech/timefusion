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
        datatypes::{DataType, Field, FieldRef, Schema, TimeUnit},
        record_batch::RecordBatch,
    },
    common::tree_node::{Transformed, TreeNode},
    logical_expr::{Cast, Expr, LogicalPlan, Values},
};
use tracing::warn;

pub fn rewrite_plan(plan: LogicalPlan) -> LogicalPlan {
    plan.clone()
        .transform_up(|node| {
            let LogicalPlan::Values(values) = node else {
                return Ok(Transformed::no(node));
            };
            let Values { schema, values } = values;
            let values = values
                .into_iter()
                .map(|row| {
                    row.into_iter()
                        .zip(schema.fields())
                        // Always wrap in Cast. Even if the Placeholder's inferred `field`
                        // already has a matching type, that information is only set
                        // reliably for row-1 placeholders in a multi-row VALUES; row-2+
                        // get `field: None` and so `get_parameter_types()` reports them as
                        // unknown. The explicit Cast forces extract_placeholder_cast_types
                        // to pick up every placeholder.
                        .map(|(expr, f)| match expr {
                            Expr::Placeholder(_) => Expr::Cast(Cast::new(Box::new(expr), f.data_type().clone())),
                            _ => expr,
                        })
                        .collect()
                })
                .collect();
            Ok(Transformed::yes(LogicalPlan::Values(Values { schema, values })))
        })
        .map(|t| t.data)
        .unwrap_or_else(|e| {
            // Falling back to the un-coerced plan can leave pgwire serving the wrong
            // placeholder types for multi-row INSERTs — surface at warn! so it's
            // visible in ops dashboards.
            warn!(target: "insert_coerce", "plan rewrite skipped (multi-row INSERT type inference may suffer): {e}");
            plan
        })
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
    f(map.entry(table.to_string()).or_insert(AtomicI64::new(i64::MIN)).value())
}

/// Issue the next stamp for `table`: `max(now, last_issued + 1)`. Strictly
/// increasing by construction, so two versions of a row written inside the same
/// microsecond — or after the wall clock steps backwards — can never tie.
pub fn next_stamp(table: &str) -> i64 {
    with_cell(table, |cell| {
        let now = crate::clock::now_micros();
        let next = |prev: i64| now.max(prev.saturating_add(1));
        // Ok/Err both carry the CAS'd `prev`, so re-deriving `next(prev)` is exactly what was stored.
        next(cell.fetch_update(Ordering::AcqRel, Ordering::Relaxed, |prev| Some(next(prev))).unwrap_or_else(|prev| prev))
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

/// The declared version-stamp column for `table`: the schema's `dedup_tiebreak`,
/// when it is a microsecond timestamp AND the table declares `version_append`.
///
/// `version_append` is what makes the tiebreak **TF-owned** — it is the flag that
/// says UPDATE/DELETE append a new version carrying a fresh tiebreak, which is
/// only sound if TF, not the client, issues that value. A microsecond-timestamp
/// tiebreak on its own is NOT enough to claim ownership — which is exactly why
/// flipping `version_append` on `otel_logs_and_spans` / `otel_metrics`
/// (2026-08-02) had to MOVE their tiebreak first: they broke ties on the
/// client-supplied `observed_timestamp` / `ingested_at`, both
/// `Timestamp(Microsecond, _)`, and stamping those would have silently
/// destroyed ingested, user-queried data on every write. Both now point at the
/// TF-owned `updated_at`. A table that declares a client-owned tiebreak must
/// leave `version_append` off (`mor_dormant` is the fixture for that shape).
fn stamp_column(table: &str) -> Option<(FieldRef, Option<Arc<str>>)> {
    let schema = crate::schema_loader::get_schema(table).filter(|s| s.version_append)?;
    let name = schema.dedup_tiebreak.as_deref()?;
    let (dt, nullable) = schema.field_def(name)?;
    let DataType::Timestamp(TimeUnit::Microsecond, tz) = &dt else { return None };
    Some((Arc::new(Field::new(name, dt.clone(), nullable)), tz.clone()))
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
    let Some((field, tz)) = stamp_column(table) else {
        return batches;
    };
    batches
        .into_iter()
        .map(|batch| {
            let arr = Arc::new(TimestampMicrosecondArray::from(vec![next_stamp(table); batch.num_rows()]).with_timezone_opt(tz.clone())) as ArrayRef;
            let old_schema = batch.schema();
            // Positional replace-or-append; a fold over the field list would clone every
            // column to express the same thing.
            let (mut fields, mut columns) = (old_schema.fields().to_vec(), batch.columns().to_vec());
            match old_schema.index_of(field.name()) {
                Ok(i) => (fields[i], columns[i]) = (field.clone(), arr),
                Err(_) => {
                    fields.push(field.clone());
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
    if let Some((field, _)) = stamp_column(table)
        && let Some(max) =
            batch.column_by_name(field.name()).and_then(|c| c.as_any().downcast_ref::<TimestampMicrosecondArray>()).and_then(datafusion::arrow::compute::max)
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
        let id = (Arc::new(Field::new("id", DataType::Int64, false)), Arc::new(Int64Array::from(vec![1i64])) as ArrayRef);
        let stamp = client_stamp.map(|v| {
            (
                Arc::new(Field::new("updated_at", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), true)),
                Arc::new(TimestampMicrosecondArray::from(vec![v]).with_timezone("UTC")) as ArrayRef,
            )
        });
        let (fields, cols): (Vec<Arc<Field>>, Vec<ArrayRef>) = std::iter::once(id).chain(stamp).unzip();
        RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).unwrap()
    }

    fn stamp_of(batch: &RecordBatch) -> Option<i64> {
        let col = batch.column_by_name("updated_at")?;
        let arr = col.as_any().downcast_ref::<TimestampMicrosecondArray>()?;
        arr.is_valid(0).then(|| arr.value(0))
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
        let out = stamp_version("mor_versioned", vec![batch_with(None), batch_with(Some(1_234))]);
        let filled = stamp_of(&out[0]).expect("missing column is appended and populated");
        let overwritten = stamp_of(&out[1]).expect("client value is replaced, not left");
        assert_ne!(overwritten, 1_234, "a client-supplied stamp must be overwritten by TF's");
        assert!(overwritten > filled, "successive batches get increasing stamps");
    }

    /// Only a `version_append` table has a TF-owned tiebreak; every other table
    /// is left byte-for-byte alone. Covers both reasons for that: no tiebreak at
    /// all (`variant_bench`), and a declared tiebreak with the write path OFF
    /// (`mor_dormant`) — the shape `otel_logs_and_spans` and `otel_metrics` had
    /// until they flipped merge-on-read on 2026-08-02, when their tiebreak also
    /// moved off the client-supplied `observed_timestamp` / `ingested_at` and
    /// onto the TF-owned `updated_at` precisely BECAUSE stamping overwrites it.
    #[test]
    fn only_version_append_tables_are_stamped() {
        for t in ["variant_bench", "mor_dormant"] {
            assert!(stamp_column(t).is_none(), "{t} is not a version_append table — TF must not own its tiebreak");
            let before = batch_with(None);
            let out = stamp_version(t, vec![before.clone()]);
            assert_eq!(out[0].schema(), before.schema(), "{t} batch schema must be untouched");
            assert_eq!(out.len(), 1);
        }
        // Unknown tables (per-test WAL/MemBuffer tables) are likewise untouched.
        assert!(stamp_column(&unique_table()).is_none());
    }

    /// `observe_batch` reads the schema's declared tiebreak column, whatever it
    /// is named — nothing hard-codes `updated_at`.
    #[test]
    fn observe_batch_is_schema_driven() {
        let t = unique_table();
        let declared = crate::schema_loader::get_schema("mor_versioned").expect("fixture registered").dedup_tiebreak.clone().expect("declares a tiebreak");
        assert_eq!(stamp_column("mor_versioned").map(|(f, _)| f.name().clone()), Some(declared.clone()), "the stamp column comes from the YAML, not a literal");
        let schema = Arc::new(Schema::new(vec![Field::new(&declared, DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), true)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(TimestampMicrosecondArray::from(vec![7_777_i64]).with_timezone("UTC")) as ArrayRef]).unwrap();
        observe_batch("mor_versioned", &batch);
        assert!(next_stamp("mor_versioned") > 7_777);
        // A table with no schema observes nothing (and must not panic).
        observe_batch(
            &t,
            &RecordBatch::try_new(Arc::new(Schema::new(vec![Field::new("x", DataType::Utf8, true)])), vec![Arc::new(StringArray::from(vec!["a"])) as ArrayRef])
                .unwrap(),
        );
    }
}
