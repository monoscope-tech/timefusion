//! Keep `DedupExec` fed by an order-PRESERVING merge, not an order-erasing coalesce.
//!
//! `DedupExec` requires `Distribution::SinglePartition` and declares the ordering
//! keep-greatest depends on, so the planner normally satisfies both at once with a
//! `SortPreservingMergeExec` over the mem ∪ hot ∪ delta union.
//!
//! It stops doing that as soon as a pushed predicate pins the sort column to a constant.
//! `timestamp = '...'` (a log-item detail lookup by primary key) makes DataFusion infer
//! `timestamp` is constant on that leg, `ordering_satisfy` then reports the requirement
//! *trivially* satisfied — a constant column is ordered under any direction — EnforceSorting
//! drops the merge as redundant, and EnforceDistribution supplies the single partition with a
//! `CoalescePartitionsExec` instead.
//!
//! That reasoning is sound for a logical ordering and wrong for this operator. `DedupExec`
//! does not consume ordering as a property; it consumes the *physical run structure* the merge
//! produces — every version of a key arriving contiguously. A coalesce interleaves partitions
//! arbitrarily, so `detect_bound` finds no ordering, the operator falls back to `full-set`
//! mode and buffers the whole scan.
//!
//! Measured on prod (project 28f62f01, 2026-08-15): the equality lookup planned
//! `CoalescePartitionsExec` → `mode=full-set` and died on
//! `unordered merge-on-read dedup exceeded its 2048 MiB per-query limit`, 500ing every log-item
//! detail panel opened from a `target_event` link. The identical query written as a
//! one-microsecond half-open range — same selectivity, but not constant-inferable — planned
//! `SortPreservingMergeExec` → `mode=bounded[timestamp]` and answered in 113ms.
//!
//! So the rule runs after EnforceSorting/EnforceDistribution have had their say and puts the
//! merge back wherever a `DedupExec` that needs ordering is fed by a coalesce. It is not a
//! pessimisation: the branches underneath are already ordered (that is why the coalesce was
//! legal), so this is a k-way merge of sorted streams, not a sort.
use std::sync::Arc;

use datafusion::{
    common::Result,
    common::tree_node::{Transformed, TransformedResult, TreeNode},
    config::ConfigOptions,
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{ExecutionPlan, coalesce_partitions::CoalescePartitionsExec, sorts::sort_preserving_merge::SortPreservingMergeExec},
};

use super::downcast;
use crate::read_dedup::DedupExec;

#[derive(Debug)]
pub struct DedupNeedsOrderedInput;

impl PhysicalOptimizerRule for DedupNeedsOrderedInput {
    fn name(&self) -> &str {
        "DedupNeedsOrderedInput"
    }

    fn schema_check(&self) -> bool {
        true
    }

    fn optimize(&self, plan: Arc<dyn ExecutionPlan>, _config: &ConfigOptions) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|node| {
            let Some(dedup) = downcast::<DedupExec>(node.as_ref()) else {
                return Ok(Transformed::no(node));
            };
            // No declared ordering means keep-greatest is dormant (the table has no
            // `version_append`); the operator is ordering-agnostic and a coalesce is fine.
            let Some(req) = dedup.required_ordering().cloned() else {
                return Ok(Transformed::no(node));
            };
            let child = Arc::clone(node.children()[0]);
            let Some(coalesce) = downcast::<CoalescePartitionsExec>(child.as_ref()) else {
                return Ok(Transformed::no(node));
            };
            let merged = Arc::new(SortPreservingMergeExec::new(req, Arc::clone(coalesce.children()[0]))) as Arc<dyn ExecutionPlan>;
            Ok(Transformed::yes(node.with_new_children(vec![merged])?))
        })
        .data()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::compute::SortOptions;
    use datafusion::{
        arrow::{
            array::{Int64Array, RecordBatch},
            datatypes::{DataType, Field, Schema},
        },
        datasource::{memory::MemorySourceConfig, source::DataSourceExec},
        physical_expr::{LexOrdering, PhysicalSortExpr, expressions::Column},
    };

    fn ts_ordering() -> LexOrdering {
        LexOrdering::new(vec![PhysicalSortExpr::new(Arc::new(Column::new("ts", 0)), SortOptions { descending: true, nulls_first: false })]).unwrap()
    }

    /// Two ordered partitions, so a coalesce over them is legal but interleaving.
    fn ordered_source() -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new("ts", DataType::Int64, false), Field::new("id", DataType::Int64, false)]));
        let batch = |a: i64| RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![a])), Arc::new(Int64Array::from(vec![a]))]).unwrap();
        let cfg = MemorySourceConfig::try_new(&[vec![batch(2)], vec![batch(1)]], schema, None).unwrap().try_with_sort_information(vec![ts_ordering()]).unwrap();
        Arc::new(DataSourceExec::new(Arc::new(cfg)))
    }

    fn dedup_over(child: Arc<dyn ExecutionPlan>, requiring: Option<LexOrdering>) -> Arc<dyn ExecutionPlan> {
        Arc::new(DedupExec::with_tiebreak(child, vec!["ts".into(), "id".into()], None, None).unwrap().requiring(requiring))
    }

    fn child_name(plan: &Arc<dyn ExecutionPlan>) -> String {
        format!("{:?}", plan.children()[0]).split_whitespace().next().unwrap_or_default().to_string()
    }

    #[test]
    fn restores_the_merge_a_constant_sort_column_let_the_planner_discharge() {
        // The shape a `timestamp = '...'` point lookup reaches execute with: DedupExec needs
        // run structure, but its input is a coalesce that interleaves partitions, so
        // `detect_bound` finds no ordering and the operator buffers the whole scan.
        let coalesced = Arc::new(CoalescePartitionsExec::new(ordered_source())) as Arc<dyn ExecutionPlan>;
        let plan = dedup_over(coalesced, Some(ts_ordering()));
        assert!(child_name(&plan).contains("CoalescePartitionsExec"), "precondition");

        let fixed = DedupNeedsOrderedInput.optimize(plan, &ConfigOptions::default()).unwrap();

        assert!(child_name(&fixed).contains("SortPreservingMergeExec"), "coalesce must become an order-preserving merge, got: {}", child_name(&fixed));
    }

    #[test]
    fn leaves_an_ordering_agnostic_dedup_alone() {
        // No declared ordering = keep-greatest dormant (no `version_append`). Forcing a merge
        // there would charge every scan of such a table for a property nothing consumes.
        let coalesced = Arc::new(CoalescePartitionsExec::new(ordered_source())) as Arc<dyn ExecutionPlan>;
        let plan = dedup_over(coalesced, None);

        let out = DedupNeedsOrderedInput.optimize(plan, &ConfigOptions::default()).unwrap();

        assert!(child_name(&out).contains("CoalescePartitionsExec"), "must stay a coalesce, got: {}", child_name(&out));
    }

    #[test]
    fn leaves_an_already_merged_dedup_alone() {
        let merged = Arc::new(SortPreservingMergeExec::new(ts_ordering(), ordered_source())) as Arc<dyn ExecutionPlan>;
        let plan = dedup_over(merged, Some(ts_ordering()));

        let out = DedupNeedsOrderedInput.optimize(plan, &ConfigOptions::default()).unwrap();

        assert!(child_name(&out).contains("SortPreservingMergeExec"));
    }
}
