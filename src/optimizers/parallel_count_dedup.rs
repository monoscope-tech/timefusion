//! Parallel merge-on-read dedup for narrow global COUNT plans.
//!
//! A global COUNT consumes no row ordering. Hash every complete dedup key to
//! one worker, resolve greatest-version winners independently, and let
//! `EnforceDistribution` coalesce the survivors for the aggregate.

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
        // The older experiment required an SPM here, which made it a no-op on
        // the production regression: the deliberately-unsortable Delta leg is
        // exactly what suppresses that merge. Accept the raw unordered input
        // too; hash ownership supplies the correctness property we need.
        let raw = if let Some(merge) = downcast::<SortPreservingMergeExec>(dedup.input().as_ref()) {
            let Some(raw) = merge.children().first().copied().cloned() else {
                return Ok(None);
            };
            raw
        } else {
            dedup.input().clone()
        };
        // Repartition copies projected arrays. Keep this exception narrow.
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

    // COUNT's residual timestamp/tombstone Filter and projection are row-local.
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
            datatypes::{DataType, Field, Schema},
        },
        datasource::{memory::MemorySourceConfig, source::DataSourceExec},
    };

    use super::*;

    #[tokio::test]
    async fn unordered_dedup_becomes_hash_partitioned_and_preserves_count() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("id", DataType::Utf8, false),
            Field::new("updated_at", DataType::Int64, true),
            Field::new("deleted", DataType::Boolean, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![2, 1, 2])),
                Arc::new(StringArray::from(vec!["a", "b", "a"])),
                Arc::new(Int64Array::from(vec![Some(1), Some(1), Some(2)])),
                Arc::new(BooleanArray::from(vec![false, false, false])),
            ],
        )
        .unwrap();
        let source = MemorySourceConfig::try_new(&[vec![batch]], schema, None).unwrap();
        let source = Arc::new(DataSourceExec::new(Arc::new(source))) as Arc<dyn ExecutionPlan>;
        let dedup = Arc::new(
            crate::read_dedup::DedupExec::with_tiebreak(source, vec!["timestamp".into(), "id".into()], Some("updated_at".into()), Some(vec![0, 3])).unwrap(),
        ) as Arc<dyn ExecutionPlan>;

        let rewritten = rewrite_count_input(&dedup, 4).unwrap().expect("unordered count dedup should rewrite");
        let partitioned = downcast::<crate::read_dedup::DedupExec>(rewritten.as_ref()).unwrap();
        assert_eq!(partitioned.properties().output_partitioning().partition_count(), 4);
        assert!(downcast::<RepartitionExec>(partitioned.input().as_ref()).is_some());
        let shown = format!("{}", datafusion::physical_plan::displayable(partitioned).one_line());
        assert!(shown.contains("scope=hash-partition"), "{shown}");

        let output = datafusion::physical_plan::collect(rewritten, Arc::new(datafusion::execution::TaskContext::default())).await.unwrap();
        assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    }
}
