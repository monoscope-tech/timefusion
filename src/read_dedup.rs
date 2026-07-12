//! Read-side dedup operator (parity plan Defect 2 #1).
//!
//! TF's write path can leave physical duplicates of a `(id, timestamp)` row in
//! Delta (cross-flush `SaveMode::Append`, late prior-day DLQ replays the
//! background sweep hasn't reached yet). `DedupExec` guarantees `COUNT(*)` is
//! correct *at query time* by collapsing those duplicates over the routed +
//! pruned MemBuffer ∪ Delta union, independent of sweep timing.
//!
//! Why a physical operator and not an `AnalyzerRule` wrapping the `TableScan`:
//! a `Distinct::On` node between the `project_id` filter and
//! `ProjectRoutingTable::scan` blocks `push_down_filter`, so routing falls back
//! to `default_project` and time/partition pruning is lost (see the plan's
//! reverted-blocker note). Deduping here, after routing, avoids all of that.
//!
//! Implementation: a **streaming keep-first** dedup. It requires a single input
//! partition (DataFusion inserts a `CoalescePartitionsExec`) so a key can't be
//! split across partitions, then keeps a `HashSet` of seen key-rows and filters
//! each batch in place. Only the (tiny) key columns are held — never the fat
//! body/attributes payload — so it streams, supports downstream early-LIMIT,
//! and never trips Arrow's 2 GB string-offset limit. The set is keyed on the
//! encoded key bytes (`ahash`, borrowed-probe, allocate-on-miss); when the
//! input is already sorted by a dedup-key column it runs in bounded-window mode
//! (see `Bound`) which caps the set at one timestamp run instead of O(distinct
//! over the whole scan). Keep-first means the
//! MemBuffer copy (unioned first) wins. KNOWN GAP: this does NOT apply the
//! `dedup_tiebreak` (observed_timestamp) that the write-side `dedup_batches`
//! and the sweep now honor, so a Defect-3 enriched re-emit that hasn't been
//! collapsed yet may read back as the stale base row (COUNT is still correct).
//! A streaming keep-first operator can't choose the greatest-tiebreak survivor
//! without buffering the whole stream, which would break early-LIMIT; the
//! sweep is the authority for which physical row survives.
//!
//! Dedup keys must be present in the input; the caller augments the pushed
//! projection so they are, then `output_projection` restores the requested
//! columns (via `RecordBatch::project`, which preserves row count for the empty
//! `COUNT(*)` projection).

use std::{collections::HashSet, sync::Arc};

use datafusion::{
    arrow::{
        array::{ArrayRef, BooleanArray, RecordBatch},
        compute::filter_record_batch,
        datatypes::SchemaRef,
        row::{RowConverter, SortField},
    },
    error::{DataFusionError, Result as DFResult},
    execution::TaskContext,
    physical_plan::{DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, PlanProperties, SendableRecordBatchStream, stream::RecordBatchStreamAdapter},
};
use futures::StreamExt;

use crate::errors::arrow_err;

/// Seen-key set for the streaming dedup. Arrow's `Row`/`OwnedRow` hash and
/// compare purely on their encoded `data` bytes (the `RowConfig` is not part of
/// identity), so we key directly on the raw bytes: `Box<[u8]>` drops the
/// per-entry `RowConfig`/`Arc` word `.owned()` would carry, and `Borrow<[u8]>`
/// lets us probe with a borrowed `&[u8]` and allocate only on a miss (never once
/// per duplicate row). `ahash` replaces std's SipHash.
type SeenSet = HashSet<Box<[u8]>, ahash::RandomState>;

/// Bounded-window dedup state (parity plan Point 3, Tier 2). When the input is
/// already sorted by a dedup-key column (`timestamp` leads the table sort
/// order), duplicates of a key are confined to a single bound-value run: two
/// rows with the same dedup key share the same `timestamp` (equal key ⇒ equal
/// bound), so they arrive contiguously. Clearing `seen` each time the bound
/// *advances* past the current run caps it at O(distinct keys within one
/// timestamp value) instead of O(distinct over the whole scan) — the fix for
/// the multi-GB seen-set risk on wide historical scans. Opportunistic only: we
/// never *require* the ordering (that could make EnforceSorting insert a
/// blocking SortExec over unsorted MemBuffer partitions and break streaming);
/// when the input isn't sorted, `detect_bound` returns `None` and dedup falls
/// back to the full-set path — always sound.
struct Bound {
    /// Bound column index within the input schema.
    idx: usize,
    /// True when the sort is descending (bound decreases down the stream).
    desc: bool,
    /// The current run's bound value; `None` until the first row.
    last: Option<i64>,
}

/// The i64-backed values of a bound column (timestamps / Int64), for cheap
/// run-boundary comparison. `None` for any other type → bounded mode disabled.
fn bound_slice(col: &ArrayRef) -> Option<&[i64]> {
    use datafusion::arrow::{
        array::AsArray,
        datatypes::{DataType, Int64Type, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType},
    };
    Some(match col.data_type() {
        DataType::Int64 => col.as_primitive::<Int64Type>().values(),
        DataType::Timestamp(TimeUnit::Second, _) => col.as_primitive::<TimestampSecondType>().values(),
        DataType::Timestamp(TimeUnit::Millisecond, _) => col.as_primitive::<TimestampMillisecondType>().values(),
        DataType::Timestamp(TimeUnit::Microsecond, _) => col.as_primitive::<TimestampMicrosecondType>().values(),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => col.as_primitive::<TimestampNanosecondType>().values(),
        _ => return None,
    })
}

/// Enable bounded mode iff the input's leading sort column is a dedup key of an
/// i64-backed type. That guarantees equal dedup keys share the bound value, so
/// clearing on a bound advance can never evict a key that could still recur.
fn detect_bound(input: &Arc<dyn ExecutionPlan>, keys: &[String], in_schema: &SchemaRef) -> Option<Bound> {
    use datafusion::{arrow::datatypes::DataType, physical_expr::expressions::Column};
    let se = input.properties().output_ordering()?.iter().next()?;
    let any: &dyn std::any::Any = se.expr.as_ref();
    let col = any.downcast_ref::<Column>()?;
    if !keys.iter().any(|k| k == col.name()) {
        return None;
    }
    matches!(in_schema.field(col.index()).data_type(), DataType::Int64 | DataType::Timestamp(..)).then(|| Bound {
        idx: col.index(),
        desc: se.options.descending,
        last: None,
    })
}

/// The input's output ordering, remapped through `output_projection` onto the
/// dedup output schema. Keeps the longest prefix of plain-column sort exprs
/// whose columns survive the projection; `None` when nothing survives or the
/// input declares no ordering.
fn remap_ordering(
    input: &Arc<dyn ExecutionPlan>, output_projection: &Option<Vec<usize>>, schema: &SchemaRef,
) -> Option<datafusion::physical_expr::LexOrdering> {
    use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr, expressions::Column};
    let in_ordering = input.properties().output_ordering()?;
    let mut out: Vec<PhysicalSortExpr> = Vec::new();
    for se in in_ordering.iter() {
        // Explicit Any upcast — the PhysicalExpr trait's `as_any` collides
        // with downcast-rs's blanket method in this crate's scope.
        let any: &dyn std::any::Any = se.expr.as_ref();
        let Some(col) = any.downcast_ref::<Column>() else { break };
        let new_idx = match output_projection {
            None => Some(col.index()),
            Some(idxs) => idxs.iter().position(|&i| i == col.index()),
        };
        let Some(new_idx) = new_idx else { break };
        out.push(PhysicalSortExpr::new(Arc::new(Column::new(schema.field(new_idx).name(), new_idx)), se.options));
    }
    LexOrdering::new(out)
}

#[derive(Debug)]
pub struct DedupExec {
    input: Arc<dyn ExecutionPlan>,
    keys: Vec<String>,
    /// Indices of the key columns within `input.schema()`.
    key_idxs: Vec<usize>,
    /// Indices into `input.schema()` to emit after dedup, restoring the
    /// originally-requested projection. `None` = emit the input schema as-is.
    output_projection: Option<Vec<usize>>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl DedupExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, keys: Vec<String>, output_projection: Option<Vec<usize>>) -> DFResult<Self> {
        let in_schema = input.schema();
        let key_idxs = keys
            .iter()
            .map(|k| in_schema.index_of(k).map_err(|_| DataFusionError::Internal(format!("DedupExec key `{k}` not in input schema"))))
            .collect::<DFResult<Vec<_>>>()?;
        let schema = match &output_projection {
            Some(idxs) => Arc::new(in_schema.project(idxs)?),
            None => in_schema.clone(),
        };
        // Keep-first dedup preserves the input's row order, so the input's
        // output ordering remains valid on the output (remapped through the
        // projection). Without this the sorted Delta scan's declared order
        // (fork sort-order pushdown) dies here and `ORDER BY timestamp
        // LIMIT n` re-sorts the whole window instead of early-terminating.
        let eq = match remap_ordering(&input, &output_projection, &schema) {
            Some(ordering) => datafusion::physical_expr::EquivalenceProperties::new_with_orderings(schema.clone(), [ordering]),
            None => datafusion::physical_expr::EquivalenceProperties::new(schema.clone()),
        };
        let properties = Arc::new(PlanProperties::new(
            eq,
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            input.properties().emission_type,
            input.properties().boundedness,
        ));
        Ok(Self { input, keys, key_idxs, output_projection, schema, properties })
    }
}

impl DisplayAs for DedupExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "DedupExec: keys=[{}]", self.keys.join(", "))
    }
}

#[async_trait::async_trait]
impl ExecutionPlan for DedupExec {
    fn name(&self) -> &'static str {
        "DedupExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        // Global dedup needs every row in one partition.
        vec![Distribution::SinglePartition]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        // Streaming keep-first: surviving rows appear in input order. Lets
        // EnforceSorting swap the single-partition coalesce below for a
        // SortPreservingMergeExec when a downstream ordering requires it.
        vec![true]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DedupExec::new(children[0].clone(), self.keys.clone(), self.output_projection.clone())?))
    }

    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!("DedupExec only produces partition 0, got {partition}")));
        }
        let in_schema = self.input.schema();
        let sort_fields: Vec<SortField> = self.key_idxs.iter().map(|&i| SortField::new(in_schema.field(i).data_type().clone())).collect();
        let converter = RowConverter::new(sort_fields).map_err(arrow_err)?;

        let input = self.input.execute(0, context)?;
        let out_schema = self.schema.clone();

        // Streaming keep-first dedup. State threads the loop-invariant config
        // (key_idxs, output_projection, converter) alongside the mutable cursor
        // (`input`) and the `seen` set, so nothing is re-cloned per poll. `seen`
        // holds only the encoded key rows — never the payload — but it does grow
        // for the whole stream lifetime, O(distinct keys) over the scan.
        let bound = detect_bound(&self.input, &self.keys, &in_schema);
        let state = (input, converter, SeenSet::default(), self.key_idxs.clone(), self.output_projection.clone(), bound);
        let stream = futures::stream::unfold(state, |(mut input, converter, mut seen, key_idxs, output_projection, mut bound)| async move {
            loop {
                match input.next().await {
                    None => return None,
                    Some(Err(e)) => return Some((Err(e), (input, converter, seen, key_idxs, output_projection, bound))),
                    Some(Ok(batch)) => match dedup_batch(&batch, &key_idxs, &converter, &mut seen, output_projection.as_deref(), bound.as_mut()) {
                        Ok(Some(b)) => return Some((Ok(b), (input, converter, seen, key_idxs, output_projection, bound))),
                        Ok(None) => continue, // entire batch was duplicates → pull the next one
                        Err(e) => return Some((Err(e), (input, converter, seen, key_idxs, output_projection, bound))),
                    },
                }
            }
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(out_schema, stream)))
    }
}

/// Drop rows whose key tuple was already emitted, then restore the requested
/// projection. Returns `None` when nothing survives (caller pulls the next batch).
fn dedup_batch(
    batch: &RecordBatch, key_idxs: &[usize], converter: &RowConverter, seen: &mut SeenSet, output_projection: Option<&[usize]>, bound: Option<&mut Bound>,
) -> DFResult<Option<RecordBatch>> {
    let key_arrays: Vec<ArrayRef> = key_idxs.iter().map(|&i| batch.column(i).clone()).collect();
    let rows = converter.convert_columns(&key_arrays).map_err(arrow_err)?;
    // Bounded-window state (Tier 2): the bound column's per-row values, sort
    // direction, and the run cursor. `bound_slice` returning None (unsupported
    // type) silently disables eviction for this batch — still correct.
    let (bvals, desc, mut last) = match &bound {
        Some(b) => (bound_slice(batch.column(b.idx)), b.desc, b.last),
        None => (None, false, None),
    };
    // Borrowed probe: hash the encoded bytes in place; on a miss (first sighting)
    // allocate one `Box<[u8]>`. Duplicates never allocate — the mask is the
    // negation folded into the `&&` so a hit short-circuits before `insert`.
    // When bounded and the bound advances (strictly past the current run), the
    // seen-set is cleared first: no earlier key can recur in a sorted stream.
    let mask: BooleanArray = (0..batch.num_rows())
        .map(|i| {
            if let Some(vals) = bvals {
                let t = vals[i];
                if last.is_some_and(|l| if desc { t < l } else { t > l }) {
                    seen.clear();
                }
                if last.is_none_or(|l| if desc { t < l } else { t > l }) {
                    last = Some(t);
                }
            }
            let bytes = rows.row(i).data();
            !seen.contains(bytes) && {
                seen.insert(bytes.into());
                true
            }
        })
        .collect();
    if let Some(b) = bound {
        b.last = last;
    }
    let kept = filter_record_batch(batch, &mask).map_err(arrow_err)?;
    let out = match output_projection {
        Some(idxs) => kept.project(idxs).map_err(arrow_err)?,
        None => kept,
    };
    Ok((out.num_rows() > 0).then_some(out))
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::{
        array::{Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
    };

    use super::*;

    fn batch(ids: &[&str], vals: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false), Field::new("v", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(ids.to_vec())), Arc::new(Int64Array::from(vals.to_vec()))]).unwrap()
    }

    fn conv() -> RowConverter {
        RowConverter::new(vec![SortField::new(DataType::Utf8)]).unwrap()
    }

    /// COUNT-correctness + keep-first across batches: the seen-set threads
    /// state, duplicates (including cross-batch) collapse, and the *first*
    /// occurrence's row survives (its `v`). Regression guard for the
    /// bytes-keyed borrowed-probe rewrite (parity plan Point 3, Tier 1).
    #[test]
    fn dedup_batch_keeps_first_and_counts_distinct() {
        let converter = conv();
        let mut seen = SeenSet::default();
        let b1 = batch(&["a", "b", "a", "c"], &[1, 2, 3, 4]);
        let out1 = dedup_batch(&b1, &[0], &converter, &mut seen, None, None).unwrap().unwrap();
        let ids = out1.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let vs = out1.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(ids.iter().flatten().collect::<Vec<_>>(), vec!["a", "b", "c"]);
        assert_eq!(vs.values(), &[1, 2, 4], "first occurrence survives (a→1, not a→3)");

        // Second batch: every key already seen → whole batch drops (None).
        let b2 = batch(&["b", "a"], &[9, 9]);
        assert!(dedup_batch(&b2, &[0], &converter, &mut seen, None, None).unwrap().is_none());

        // A fresh key in an otherwise-dup batch survives alone.
        let b3 = batch(&["a", "d"], &[9, 5]);
        let out3 = dedup_batch(&b3, &[0], &converter, &mut seen, None, None).unwrap().unwrap();
        assert_eq!(out3.num_rows(), 1);
        assert_eq!(out3.column(0).as_any().downcast_ref::<StringArray>().unwrap().value(0), "d");
    }

    /// Tier-2 bounded-window: keying on `(id, ts)` with `ts` as the bound, a
    /// later batch whose `ts` has advanced clears the seen-set — so an `id`
    /// re-seen at a *new* ts is a distinct dedup key and correctly survives
    /// (it is NOT the same row), while an exact `(id, ts)` dup within one run
    /// still collapses. Also asserts the set actually shrinks (eviction fired).
    #[test]
    fn dedup_batch_bounded_window_evicts_on_advance() {
        // Two-column key (id, ts); bound = ts (col 1), ascending.
        let converter = RowConverter::new(vec![SortField::new(DataType::Utf8), SortField::new(DataType::Int64)]).unwrap();
        let mut seen = SeenSet::default();
        let mut bound = Bound { idx: 1, desc: false, last: None };

        // Run ts=10: a,b,a → within-run dup of `a` collapses.
        let r1 = batch(&["a", "b", "a"], &[10, 10, 10]);
        let o1 = dedup_batch(&r1, &[0, 1], &converter, &mut seen, None, Some(&mut bound)).unwrap().unwrap();
        assert_eq!(o1.num_rows(), 2, "(a,10),(b,10) survive; second (a,10) dropped");
        assert_eq!(seen.len(), 2);

        // ts advances to 11: seen cleared; `a` re-seen at a NEW ts is a distinct
        // key and survives. Set shrank to the new run only.
        let r2 = batch(&["a", "a"], &[11, 11]);
        let o2 = dedup_batch(&r2, &[0, 1], &converter, &mut seen, None, Some(&mut bound)).unwrap().unwrap();
        assert_eq!(o2.num_rows(), 1, "(a,11) survives once; second (a,11) is a same-run dup");
        assert_eq!(seen.len(), 1, "seen-set bounded to the current run, not O(all distinct)");
        assert_eq!(bound.last, Some(11));
    }
}
