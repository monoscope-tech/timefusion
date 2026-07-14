//! Order the unordered branches of a routed MemBuffer∪Delta union so an
//! `ORDER BY <sort-keys> LIMIT n` becomes a streaming, early-terminating TopK
//! instead of a full blocking sort over the whole window.
//!
//! Background. `ProjectRoutingTable::scan` returns `Union([mem, delta])`. After
//! the parquet sort-order pushdown (delta-rs fork) the Delta branch advertises
//! the table's footer ordering (`[timestamp DESC, …]`); the MemBuffer branch
//! (`MemorySourceConfig`) advertises none. A union is order-preserving only when
//! *every* child shares the ordering, so the union is unordered and DataFusion
//! inserts a blocking `SortExec` that reads the entire mem∪delta window before
//! `LIMIT` — the Delta scan can never stop early.
//!
//! This rule runs *before* `EnforceDistribution`/`EnforceSorting`. When it finds
//! a `SortExec`/`SortPreservingMergeExec` **with a fetch** (the `ORDER BY … LIMIT`
//! shape) whose input contains such a union — one child already satisfying the
//! requested ordering, another not — it wraps the unsatisfying child(ren) in a
//! `SortExec(req).with_fetch(n)`. Now the union is order-preserving, so the
//! built-in rules replace the coalesce with a `SortPreservingMergeExec` and drop
//! the top blocking sort, keeping only the fetch: the merge pulls the front of
//! the newest (mem) rows and the newest Delta files and stops.
//!
//! Scope guards (why it never regresses other plans):
//! - Only fires under a *fetching* sort — plain scans / non-LIMIT sorts are
//!   untouched, so no MemBuffer sort is bolted onto counts/aggregations.
//! - Only fires on a *mixed* union (≥1 child already ordered, ≥1 not). When the
//!   Delta pushdown is off (the mixed-footer window during the DESC rollout)
//!   neither child is ordered → no-op → current blocking-sort behavior.
//! - The requested ordering is re-resolved by column *name* against the union
//!   schema, so an intervening projection (e.g. `DedupExec`'s column restore)
//!   can't misalign the sort-key indices.

use std::sync::Arc;

use datafusion::{
    arrow::datatypes::Schema,
    common::{
        Result,
        tree_node::{Transformed, TreeNode},
    },
    config::ConfigOptions,
    physical_expr::{LexOrdering, PhysicalSortExpr, expressions::Column},
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{
        ExecutionPlan,
        sorts::{sort::SortExec, sort_preserving_merge::SortPreservingMergeExec},
        union::UnionExec,
    },
};

#[derive(Debug, Default)]
pub struct OrderedUnionForTopK;

/// Re-express `req` (sort keys, possibly indexed against a downstream projected
/// schema) as plain columns resolved by name against `schema`. Returns `None`
/// if any key is not a plain column or is absent from `schema` — in which case
/// the rule bails (correctness over cleverness).
fn resolve_ordering(req: &LexOrdering, schema: &Schema) -> Option<LexOrdering> {
    let mut out = Vec::with_capacity(req.len());
    for se in req.iter() {
        // Explicit Any upcast — the PhysicalExpr trait's `as_any` collides with
        // downcast-rs's blanket method in this crate's scope (see read_dedup.rs).
        let any: &dyn std::any::Any = se.expr.as_ref();
        let col = any.downcast_ref::<Column>()?;
        let idx = schema.index_of(col.name()).ok()?;
        out.push(PhysicalSortExpr::new(Arc::new(Column::new(col.name(), idx)), se.options));
    }
    LexOrdering::new(out)
}

/// Walk down from a fetching sort through single-child order-preserving
/// operators to the first `UnionExec`; if the union is mixed (some children
/// satisfy `req`, some don't), sort the unsatisfying children by `req` (with
/// `fetch`) so the union becomes order-preserving. Returns the rewritten
/// subtree, or `None` when nothing applied.
fn order_union(plan: &Arc<dyn ExecutionPlan>, req: &LexOrdering, fetch: Option<usize>) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    // Explicit Any upcast — `ExecutionPlan: Any` but exposes no `as_any` trait
    // method in this build (see read_dedup.rs for the same pattern).
    let plan_any: &dyn std::any::Any = plan.as_ref();
    if let Some(union) = plan_any.downcast_ref::<UnionExec>() {
        let Some(req_here) = resolve_ordering(req, &union.schema()) else {
            return Ok(None);
        };
        let children = union.children();
        let sat: Vec<bool> = children
            .iter()
            .map(|c| c.properties().equivalence_properties().ordering_satisfy(req_here.iter().cloned()))
            .collect::<Result<Vec<_>>>()?;
        // Mixed union only: an ordered child to merge toward AND an unordered
        // child to fix. All-ordered needs nothing; none-ordered means the Delta
        // pushdown is off (mixed footers) — leave the blocking sort in place.
        if !sat.iter().any(|&s| s) || sat.iter().all(|&s| s) {
            return Ok(None);
        }
        let new_children: Vec<Arc<dyn ExecutionPlan>> = children
            .into_iter()
            .zip(sat)
            .map(|(c, s)| if s { c.clone() } else { Arc::new(SortExec::new(req_here.clone(), c.clone()).with_fetch(fetch)) as Arc<dyn ExecutionPlan> })
            .collect();
        return Ok(Some(UnionExec::try_new(new_children)?));
    }
    // Descend only through single-child, order-preserving operators so the
    // ordering we create actually propagates up to the sort.
    let children = plan.children();
    if children.len() == 1 && plan.maintains_input_order().first() == Some(&true) {
        if let Some(new_child) = order_union(children[0], req, fetch)? {
            return Ok(Some(Arc::clone(plan).with_new_children(vec![new_child])?));
        }
    }
    Ok(None)
}

impl PhysicalOptimizerRule for OrderedUnionForTopK {
    fn name(&self) -> &str {
        "ordered_union_for_topk"
    }

    fn schema_check(&self) -> bool {
        true
    }

    fn optimize(&self, plan: Arc<dyn ExecutionPlan>, _config: &ConfigOptions) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(plan
            .transform_down(|node| {
                // Anchor: a fetching global sort (the `ORDER BY … LIMIT n` shape).
                let node_any: &dyn std::any::Any = node.as_ref();
                let (req, fetch) = if let Some(s) = node_any.downcast_ref::<SortExec>() {
                    (s.expr().clone(), s.fetch())
                } else if let Some(m) = node_any.downcast_ref::<SortPreservingMergeExec>() {
                    (m.expr().clone(), m.fetch())
                } else {
                    return Ok(Transformed::no(node));
                };
                if fetch.is_none() {
                    return Ok(Transformed::no(node));
                }
                let children = node.children();
                let Some(input) = children.first().copied() else {
                    return Ok(Transformed::no(node));
                };
                match order_union(input, &req, fetch)? {
                    Some(new_input) => {
                        let new_node = Arc::clone(&node).with_new_children(vec![new_input])?;
                        Ok(Transformed::yes(new_node))
                    }
                    None => Ok(Transformed::no(node)),
                }
            })?
            .data)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::{
        arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
        common::config::ConfigOptions,
        physical_expr::{LexOrdering, Partitioning, PhysicalSortExpr, expressions::Column},
        physical_plan::{
            ExecutionPlan, PlanProperties,
            execution_plan::{Boundedness, EmissionType},
            sorts::sort::SortExec,
            union::UnionExec,
        },
    };
    use datafusion::arrow::compute::SortOptions;

    use super::*;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
            Field::new("id", DataType::Utf8, false),
        ]))
    }

    fn ts_desc() -> LexOrdering {
        LexOrdering::new(vec![PhysicalSortExpr::new(Arc::new(Column::new("timestamp", 0)), SortOptions { descending: true, nulls_first: true })])
            .unwrap()
    }

    /// A leaf exec that lets the test declare whatever output ordering it wants,
    /// standing in for either the ordered Delta scan or the unordered MemBuffer.
    #[derive(Debug)]
    struct MockLeaf {
        props: Arc<PlanProperties>,
    }

    impl MockLeaf {
        fn new(schema: SchemaRef, ordering: Option<LexOrdering>) -> Arc<dyn ExecutionPlan> {
            use datafusion::physical_expr::EquivalenceProperties;
            let eq = match ordering {
                Some(o) => EquivalenceProperties::new_with_orderings(schema, [o]),
                None => EquivalenceProperties::new(schema),
            };
            let props = Arc::new(PlanProperties::new(eq, Partitioning::UnknownPartitioning(1), EmissionType::Incremental, Boundedness::Bounded));
            Arc::new(MockLeaf { props })
        }
    }

    impl datafusion::physical_plan::DisplayAs for MockLeaf {
        fn fmt_as(&self, _t: datafusion::physical_plan::DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "MockLeaf")
        }
    }

    impl ExecutionPlan for MockLeaf {
        fn name(&self) -> &'static str {
            "MockLeaf"
        }
        fn properties(&self) -> &Arc<PlanProperties> {
            &self.props
        }
        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }
        fn with_new_children(self: Arc<Self>, _c: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
            Ok(self)
        }
        fn execute(
            &self, _p: usize, _c: Arc<datafusion::execution::TaskContext>,
        ) -> Result<datafusion::physical_plan::SendableRecordBatchStream> {
            unimplemented!()
        }
    }

    fn count_sort_over_mem(plan: &Arc<dyn ExecutionPlan>) -> usize {
        // Number of SortExec nodes anywhere in the tree.
        let any: &dyn std::any::Any = plan.as_ref();
        let mut n = usize::from(any.downcast_ref::<SortExec>().is_some());
        for c in plan.children() {
            n += count_sort_over_mem(c);
        }
        n
    }

    // Bug: `ORDER BY timestamp DESC LIMIT n` over a mem∪delta union re-sorts the
    // whole window because the MemBuffer branch advertises no ordering, so the
    // union is unordered and the delta scan can't early-terminate. The rule must
    // sort the mem branch to match delta's advertised ordering.
    #[test]
    fn wraps_unordered_mem_branch_of_fetching_sort() {
        let s = schema();
        let ord = ts_desc();
        let mem = MockLeaf::new(s.clone(), None); // MemBuffer: no ordering
        let delta = MockLeaf::new(s.clone(), Some(ord.clone())); // Delta: [ts DESC]
        let union: Arc<dyn ExecutionPlan> = UnionExec::try_new(vec![mem, delta]).unwrap();
        let top: Arc<dyn ExecutionPlan> = Arc::new(SortExec::new(ord.clone(), union).with_fetch(Some(50)));

        let out = OrderedUnionForTopK.optimize(top, &ConfigOptions::new()).unwrap();

        // The union is now order-preserving: it advertises [ts DESC].
        let union = out.children()[0];
        assert!(
            union.properties().equivalence_properties().ordering_satisfy(ord.iter().cloned()).unwrap(),
            "union must advertise the sort ordering after the rule runs"
        );
        // Top sort + one injected mem sort = 2 SortExecs.
        assert_eq!(count_sort_over_mem(&out), 2, "exactly one SortExec injected over the mem branch");
    }

    // Guard: no fetch (plain ORDER BY, no LIMIT) → rule must not touch the plan,
    // so counts/aggregations and unbounded sorts don't grow a MemBuffer sort.
    #[test]
    fn ignores_sort_without_fetch() {
        let s = schema();
        let ord = ts_desc();
        let mem = MockLeaf::new(s.clone(), None);
        let delta = MockLeaf::new(s.clone(), Some(ord.clone()));
        let union: Arc<dyn ExecutionPlan> = UnionExec::try_new(vec![mem, delta]).unwrap();
        let top: Arc<dyn ExecutionPlan> = Arc::new(SortExec::new(ord.clone(), union));

        let out = OrderedUnionForTopK.optimize(top.clone(), &ConfigOptions::new()).unwrap();
        assert_eq!(count_sort_over_mem(&out), 1, "no injection when there is no fetch");
    }

    // Guard: when no child is ordered (Delta pushdown off during mixed-footer
    // rollout) the rule is a no-op — the built-in blocking sort stays.
    #[test]
    fn ignores_union_with_no_ordered_child() {
        let s = schema();
        let ord = ts_desc();
        let mem = MockLeaf::new(s.clone(), None);
        let delta = MockLeaf::new(s.clone(), None);
        let union: Arc<dyn ExecutionPlan> = UnionExec::try_new(vec![mem, delta]).unwrap();
        let top: Arc<dyn ExecutionPlan> = Arc::new(SortExec::new(ord.clone(), union).with_fetch(Some(50)));

        let out = OrderedUnionForTopK.optimize(top, &ConfigOptions::new()).unwrap();
        assert_eq!(count_sort_over_mem(&out), 1, "no injection when neither branch is ordered");
    }
}
