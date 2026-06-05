//! Sorted-stream distinct: emits one row per `key_columns` tuple from a
//! pre-sorted input stream. Used to collapse cross-file Delta duplicates
//! at query time when the partition's per-file min/max ranges overlap.
//!
//! Correctness invariant: the input MUST be sorted by `key_columns` —
//! enforced via `required_input_ordering` and `required_input_distribution`.

use std::{any::Any, sync::Arc};

use arrow::{
    array::{ArrayRef, BooleanArray},
    compute::filter_record_batch,
    row::{OwnedRow, RowConverter, SortField},
};
use datafusion::{
    common::DataFusionError,
    error::Result as DFResult,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_expr::{EquivalenceProperties, LexRequirement, OrderingRequirements, PhysicalSortRequirement, expressions::Column},
    physical_plan::{
        DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, PlanProperties, stream::RecordBatchStreamAdapter,
    },
};
use futures::StreamExt;

#[derive(Debug)]
pub struct DeduplicateExec {
    input:       Arc<dyn ExecutionPlan>,
    key_columns: Arc<Vec<usize>>,
    properties:  Arc<PlanProperties>,
}

impl DeduplicateExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, key_columns: Vec<usize>) -> Self {
        let in_props = input.properties();
        // Distinct preserves the input ordering and partitioning; only the
        // row set shrinks. Equivalence properties forward from the input.
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(input.schema()),
            in_props.partitioning.clone(),
            in_props.emission_type,
            in_props.boundedness,
        ));
        Self { input, key_columns: Arc::new(key_columns), properties }
    }
}

impl DisplayAs for DeduplicateExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let schema = self.input.schema();
        let names: Vec<String> = self.key_columns.iter().map(|&i| schema.field(i).name().clone()).collect();
        write!(f, "DeduplicateExec: keys=[{}]", names.join(","))
    }
}

#[async_trait::async_trait]
impl ExecutionPlan for DeduplicateExec {
    fn name(&self) -> &'static str {
        "DeduplicateExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(children[0].clone(), (*self.key_columns).clone())))
    }

    /// Streaming distinct only works on a single partition — without it, two
    /// partitions could each carry a copy of the duplicate row and our
    /// per-partition stream wouldn't see across them.
    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    /// Demand that the input is sorted ascending on every key column. The
    /// caller wraps with `SortPreservingMergeExec` (or `SortExec`) to satisfy.
    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        let schema = self.input.schema();
        let req = LexRequirement::new(self.key_columns.iter().map(|&i| {
            PhysicalSortRequirement::new(
                Arc::new(Column::new(schema.field(i).name(), i)) as Arc<dyn datafusion::physical_expr::PhysicalExpr>,
                Some(datafusion::arrow::compute::SortOptions { descending: false, nulls_first: true }),
            )
        }));
        vec![req.map(OrderingRequirements::new)]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> DFResult<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let schema = self.input.schema();
        let key_columns = self.key_columns.clone();
        let sort_fields: Vec<SortField> =
            key_columns.iter().map(|&i| SortField::new(schema.field(i).data_type().clone())).collect();
        let converter = RowConverter::new(sort_fields).map_err(arrow_to_df)?;
        let last: Option<OwnedRow> = None;

        // (input_stream, converter, last_emitted_key) carried across batches.
        let stream = futures::stream::try_unfold((input, converter, last, key_columns), move |state| async move {
            let (mut input, converter, mut last, key_columns) = state;
            loop {
                let Some(batch_res) = input.next().await else {
                    return Ok::<_, DataFusionError>(None);
                };
                let batch = batch_res?;
                if batch.num_rows() == 0 {
                    continue;
                }
                let arrs: Vec<ArrayRef> = key_columns.iter().map(|&i| batch.column(i).clone()).collect();
                let rows = converter.convert_columns(&arrs).map_err(arrow_to_df)?;
                let mut mask = Vec::with_capacity(batch.num_rows());
                for i in 0..rows.num_rows() {
                    let cur = rows.row(i);
                    // `OwnedRow.row()` returns a view comparable with the current `Row`.
                    let keep = match &last {
                        Some(prev) => prev.row() != cur,
                        None => true,
                    };
                    if keep {
                        last = Some(cur.owned());
                    }
                    mask.push(keep);
                }
                let mask_arr = BooleanArray::from(mask);
                let filtered = filter_record_batch(&batch, &mask_arr).map_err(arrow_to_df)?;
                if filtered.num_rows() > 0 {
                    return Ok(Some((filtered, (input, converter, last, key_columns))));
                }
                // All rows in this batch were duplicates of prior emissions — pull next batch.
            }
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

fn arrow_to_df(e: arrow::error::ArrowError) -> DataFusionError {
    DataFusionError::ArrowError(Box::new(e), None)
}

/// True iff any partition in `table` has two Add files with overlapping
/// ranges on EVERY `dedup_keys` column. Cross-file duplicates can only
/// exist when every key dimension overlaps — a single disjoint dimension
/// proves a (file, file) pair is clean.
///
/// Returns `true` conservatively when stats are missing for any required
/// column — we'd rather pay the sort than miss a duplicate.
pub fn table_has_any_overlap(table: &deltalake::DeltaTable, dedup_keys: &[String]) -> anyhow::Result<bool> {
    use std::collections::HashMap;

    use datafusion::common::ScalarValue;

    if dedup_keys.is_empty() {
        return Ok(false);
    }
    let snapshot = table.snapshot()?;
    let actions = snapshot.add_actions_table(true)?;
    let n = actions.num_rows();
    if n <= 1 {
        return Ok(false);
    }

    // Per-key min/max columns must exist for overlap-stats gating; if missing,
    // conservatively assume overlap so correctness is preserved.
    let mut key_cols: Vec<(&arrow::array::ArrayRef, &arrow::array::ArrayRef)> = Vec::with_capacity(dedup_keys.len());
    for key in dedup_keys {
        let (Some(mn), Some(mx)) =
            (actions.column_by_name(&format!("min.{}", key)), actions.column_by_name(&format!("max.{}", key)))
        else {
            return Ok(true);
        };
        key_cols.push((mn, mx));
    }

    // Partition-column arrays for grouping. Absence (custom-project tables
    // without a project_id partition) → single virtual group keyed by date.
    let part_proj = actions.column_by_name("partition.project_id");
    let part_date = actions.column_by_name("partition.date");
    let group_key = |row: usize| -> String {
        let p = part_proj.map(|a| arrow::util::display::array_value_to_string(a, row).unwrap_or_default()).unwrap_or_default();
        let d = part_date.map(|a| arrow::util::display::array_value_to_string(a, row).unwrap_or_default()).unwrap_or_default();
        format!("{}|{}", p, d)
    };

    // Bucket file rows by partition group, then run pairwise overlap check
    // per group. False positives across groups are impossible (different
    // partitions can never share a (project_id, date) tuple).
    let mut groups: HashMap<String, Vec<Vec<(ScalarValue, ScalarValue)>>> = HashMap::new();
    for row in 0..n {
        let mut per_key = Vec::with_capacity(dedup_keys.len());
        for (mn, mx) in &key_cols {
            if mn.is_null(row) || mx.is_null(row) {
                return Ok(true); // Missing stats → assume overlap.
            }
            per_key.push((ScalarValue::try_from_array(mn, row)?, ScalarValue::try_from_array(mx, row)?));
        }
        groups.entry(group_key(row)).or_default().push(per_key);
    }

    for ranges in groups.values() {
        if ranges.len() <= 1 {
            continue;
        }
        for i in 0..ranges.len() {
            for j in (i + 1)..ranges.len() {
                let all_overlap = ranges[i].iter().zip(&ranges[j]).all(|((a_lo, a_hi), (b_lo, b_hi))| a_lo <= b_hi && b_lo <= a_hi);
                if all_overlap {
                    return Ok(true);
                }
            }
        }
    }
    Ok(false)
}
