//! Parallel merge-on-read dedup for narrow global COUNT plans.
//!
//! The normal MOR path orders every row by timestamp, merges to one stream,
//! and resolves versions there. That preserves streaming order for arbitrary
//! queries, but a global COUNT consumes no row ordering. For that one narrow
//! shape, hash every complete dedup key to one worker, resolve greatest-version
//! winners independently, and let `EnforceDistribution` coalesce the survivors
//! for the aggregate. The result is identical because all versions of a key
//! have exactly one partition owner.

use std::sync::Arc;

use datafusion::{
    common::{
        Result,
        tree_node::{Transformed, TreeNode},
    },
    config::ConfigOptions,
    physical_expr::{Partitioning, expressions::Column},
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{
        ExecutionPlan, aggregates::AggregateExec, filter::FilterExec, projection::ProjectionExec, repartition::RepartitionExec,
        sorts::sort_preserving_merge::SortPreservingMergeExec,
    },
};

use super::downcast;

#[derive(Debug, Default)]
pub struct ParallelCountDedup;

fn rewrite_count_input(plan: &Arc<dyn ExecutionPlan>, partitions: usize) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    if let Some(dedup) = downcast::<crate::read_dedup::DedupExec>(plan.as_ref()) {
        // This rule is deliberately limited to the ordered global implementation
        // built by ProjectRoutingTable. Removing any other child could erase a
        // semantically required operator.
        let Some(merge) = downcast::<SortPreservingMergeExec>(dedup.input().as_ref()) else {
            return Ok(None);
        };
        let Some(raw) = merge.children().first().copied().cloned() else {
            return Ok(None);
        };
        // Hash repartition copies its projected input. Keep the exception narrow:
        // COUNT(*) over MOR currently carries timestamp/id/tiebreak/tombstone;
        // a wider schema is the previously measured 2–4x/90x-heap bad shape.
        if raw.schema().fields().len() > 6 {
            return Ok(None);
        }
        let hash_expr = dedup
            .keys()
            .iter()
            .map(|key| {
                let index = raw.schema().index_of(key)?;
                Ok(Arc::new(Column::new(key, index)) as Arc<dyn datafusion::physical_expr::PhysicalExpr>)
            })
            .collect::<Result<Vec<_>>>()?;
        let repartitioned = Arc::new(RepartitionExec::try_new(raw, Partitioning::Hash(hash_expr, partitions.max(2)))?) as Arc<dyn ExecutionPlan>;
        return Ok(Some(Arc::new(dedup.partitioned_over(repartitioned)?) as Arc<dyn ExecutionPlan>));
    }

    // COUNT's residual timestamp/tombstone Filter and projection are row-local;
    // rebuild only through those known-transparent nodes to reach DedupExec.
    if (downcast::<FilterExec>(plan.as_ref()).is_some() || downcast::<ProjectionExec>(plan.as_ref()).is_some())
        && plan.children().len() == 1
        && let Some(child) = rewrite_count_input(plan.children()[0], partitions)?
    {
        return Ok(Some(Arc::clone(plan).with_new_children(vec![child])?));
    }
    Ok(None)
}

impl PhysicalOptimizerRule for ParallelCountDedup {
    fn name(&self) -> &str {
        "parallel_count_dedup"
    }

    fn schema_check(&self) -> bool {
        true
    }

    fn optimize(&self, plan: Arc<dyn ExecutionPlan>, config: &ConfigOptions) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(plan
            .transform_down(|node| {
                let Some(aggregate) = downcast::<AggregateExec>(node.as_ref()) else {
                    return Ok(Transformed::no(node));
                };
                let is_plain_count = aggregate.group_expr().is_true_no_grouping()
                    && aggregate.aggr_expr().len() == 1
                    && aggregate.aggr_expr()[0].fun().name() == "count"
                    && !aggregate.aggr_expr()[0].is_distinct()
                    && aggregate.filter_expr().iter().all(Option::is_none);
                if !is_plain_count {
                    return Ok(Transformed::no(node));
                }
                let Some(input) = node.children().first().copied() else {
                    return Ok(Transformed::no(node));
                };
                match rewrite_count_input(input, config.execution.target_partitions)? {
                    Some(new_input) => Ok(Transformed::yes(node.with_new_children(vec![new_input])?)),
                    None => Ok(Transformed::no(node)),
                }
            })?
            .data)
    }
}

#[cfg(test)]
mod tests {
    use datafusion::{
        arrow::{
            array::{BooleanArray, Int64Array, RecordBatch, StringArray},
            compute::SortOptions,
            datatypes::{DataType, Field, Schema},
        },
        datasource::{memory::MemorySourceConfig, source::DataSourceExec},
        physical_expr::{LexOrdering, PhysicalSortExpr},
    };

    use super::*;

    #[tokio::test]
    async fn narrow_ordered_dedup_becomes_hash_partitioned_without_the_global_merge() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("id", DataType::Utf8, false),
            Field::new("updated_at", DataType::Int64, true),
            Field::new("deleted", DataType::Boolean, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![2, 1])),
                Arc::new(StringArray::from(vec!["a", "b"])),
                Arc::new(Int64Array::from(vec![Some(2), Some(1)])),
                Arc::new(BooleanArray::from(vec![false, false])),
            ],
        )
        .unwrap();
        let ordering =
            LexOrdering::new(vec![PhysicalSortExpr::new(Arc::new(Column::new("timestamp", 0)), SortOptions { descending: true, nulls_first: true })]).unwrap();
        let source =
            MemorySourceConfig::try_new(&[vec![batch.clone()], vec![batch]], schema, None).unwrap().try_with_sort_information(vec![ordering.clone()]).unwrap();
        let source = Arc::new(DataSourceExec::new(Arc::new(source))) as Arc<dyn ExecutionPlan>;
        let merge = Arc::new(SortPreservingMergeExec::new(ordering.clone(), source)) as Arc<dyn ExecutionPlan>;
        let dedup = Arc::new(
            crate::read_dedup::DedupExec::with_tiebreak(merge, vec!["timestamp".into(), "id".into()], Some("updated_at".into()), Some(vec![0, 3]))
                .unwrap()
                .requiring(Some(ordering)),
        ) as Arc<dyn ExecutionPlan>;

        let rewritten = rewrite_count_input(&dedup, 4).unwrap().expect("narrow count dedup should rewrite");
        let partitioned = downcast::<crate::read_dedup::DedupExec>(rewritten.as_ref()).unwrap();
        assert_eq!(partitioned.properties().output_partitioning().partition_count(), 4);
        assert!(downcast::<RepartitionExec>(partitioned.input().as_ref()).is_some(), "the global merge must be replaced by a hash repartition");
        let shown = format!("{}", datafusion::physical_plan::displayable(partitioned).one_line());
        assert!(shown.contains("scope=hash-partition"), "rewritten plan must expose its correctness scope: {shown}");

        let output = datafusion::physical_plan::collect(rewritten, Arc::new(datafusion::execution::TaskContext::default())).await.unwrap();
        assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 2, "copies routed from different source partitions must still collapse");
    }
}
