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
//! and never trips Arrow's 2 GB string-offset limit. Keep-first means the
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
        row::{OwnedRow, RowConverter, SortField},
    },
    error::{DataFusionError, Result as DFResult},
    execution::TaskContext,
    physical_plan::{DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, PlanProperties, SendableRecordBatchStream, stream::RecordBatchStreamAdapter},
};
use futures::StreamExt;

use crate::errors::arrow_err;

#[derive(Debug)]
pub struct DedupExec {
    input:             Arc<dyn ExecutionPlan>,
    keys:              Vec<String>,
    /// Indices of the key columns within `input.schema()`.
    key_idxs:          Vec<usize>,
    /// Indices into `input.schema()` to emit after dedup, restoring the
    /// originally-requested projection. `None` = emit the input schema as-is.
    output_projection: Option<Vec<usize>>,
    schema:            SchemaRef,
    properties:        Arc<PlanProperties>,
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
        let properties = Arc::new(PlanProperties::new(
            datafusion::physical_expr::EquivalenceProperties::new(schema.clone()),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            input.properties().emission_type,
            input.properties().boundedness,
        ));
        Ok(Self {
            input,
            keys,
            key_idxs,
            output_projection,
            schema,
            properties,
        })
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

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DedupExec::new(
            children[0].clone(),
            self.keys.clone(),
            self.output_projection.clone(),
        )?))
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
        let state = (
            input,
            converter,
            HashSet::<OwnedRow>::new(),
            self.key_idxs.clone(),
            self.output_projection.clone(),
        );
        let stream = futures::stream::unfold(state, |(mut input, converter, mut seen, key_idxs, output_projection)| async move {
            loop {
                match input.next().await {
                    None => return None,
                    Some(Err(e)) => return Some((Err(e), (input, converter, seen, key_idxs, output_projection))),
                    Some(Ok(batch)) => match dedup_batch(&batch, &key_idxs, &converter, &mut seen, output_projection.as_deref()) {
                        Ok(Some(b)) => return Some((Ok(b), (input, converter, seen, key_idxs, output_projection))),
                        Ok(None) => continue, // entire batch was duplicates → pull the next one
                        Err(e) => return Some((Err(e), (input, converter, seen, key_idxs, output_projection))),
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
    batch: &RecordBatch, key_idxs: &[usize], converter: &RowConverter, seen: &mut HashSet<OwnedRow>, output_projection: Option<&[usize]>,
) -> DFResult<Option<RecordBatch>> {
    let key_arrays: Vec<ArrayRef> = key_idxs.iter().map(|&i| batch.column(i).clone()).collect();
    let rows = converter.convert_columns(&key_arrays).map_err(arrow_err)?;
    let mask: BooleanArray = (0..batch.num_rows()).map(|i| seen.insert(rows.row(i).owned())).collect();
    let kept = filter_record_batch(batch, &mask).map_err(arrow_err)?;
    let out = match output_projection {
        Some(idxs) => kept.project(idxs).map_err(arrow_err)?,
        None => kept,
    };
    Ok((out.num_rows() > 0).then_some(out))
}
