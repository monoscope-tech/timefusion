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

use super::downcast;

#[derive(Debug, Default)]
pub struct OrderedUnionForTopK;

/// Re-express `req` (sort keys, possibly indexed against a downstream projected
/// schema) as plain columns resolved by name against `schema`. Returns `None`
/// if any key is not a plain column or is absent from `schema` — in which case
/// the rule bails (correctness over cleverness).
fn resolve_ordering(req: &LexOrdering, schema: &Schema) -> Option<LexOrdering> {
    req.iter()
        .map(|se| {
            let col = downcast::<Column>(se.expr.as_ref())?;
            let idx = schema.index_of(col.name()).ok()?;
            Some(PhysicalSortExpr::new(Arc::new(Column::new(col.name(), idx)), se.options))
        })
        .collect::<Option<Vec<_>>>()
        .and_then(LexOrdering::new)
}

/// The shared mechanism: `children` with every child that does not already
/// satisfy `req` wrapped in `SortExec(req)` (carrying `fetch`), so a union over
/// them advertises `req`.
///
/// Callers: this rule (top-K, `fetch` known) and `ProjectRoutingTable::scan`,
/// which uses it with no fetch so the mem ∪ hot ∪ delta union advertises the
/// table's lead sort key and `EnforceDistribution` satisfies `DedupExec`'s
/// `SinglePartition` with a `SortPreservingMergeExec` rather than a
/// `CoalescePartitionsExec` (which declares no ordering, leaving keep-greatest
/// dormant — see `docs/plans/2026-08-01-merge-on-read-dml.md` §3).
///
/// `Ok(None)` means "leave the plan alone":
/// - every child already satisfies `req` (nothing to inject), or
/// - a child that doesn't satisfy it is marked unsortable — `sortable[i] ==
///   false` says "this leg is a whole-window parquet scan; a blocking sort on
///   it costs far more than the ordering buys" (indices past `sortable`'s end
///   are sortable), or
/// - `require_ordered_child` and no child is ordered. The top-K rule sets this:
///   with the Delta footer pushdown off, neither branch is ordered and sorting
///   the mem branch alone buys nothing. `scan` does not — a MemBuffer-only scan
///   has no ordered leg by construction, yet is exactly where a fresh version
///   append lives, so it must still be ordered.
pub fn ordered_children(
    children: &[Arc<dyn ExecutionPlan>], req: &LexOrdering, fetch: Option<usize>, sortable: &[bool], require_ordered_child: bool,
) -> Result<Option<Vec<Arc<dyn ExecutionPlan>>>> {
    let sat: Vec<bool> = children.iter().map(|c| c.properties().equivalence_properties().ordering_satisfy(req.iter().cloned())).collect::<Result<Vec<_>>>()?;
    if sat.iter().all(|&s| s) || (require_ordered_child && !sat.iter().any(|&s| s)) {
        return Ok(None);
    }
    // `preserve_partitioning`: sort each input partition independently (in
    // parallel) and keep them, rather than requiring — and getting — a
    // `CoalescePartitionsExec` that serialises the leg. Per-partition ordering
    // is what the union advertises and what the merge above consumes, so this
    // is the cheaper shape for both callers.
    let sort = |c: &Arc<dyn ExecutionPlan>| Arc::new(SortExec::new(req.clone(), Arc::clone(c)).with_preserve_partitioning(true).with_fetch(fetch)) as _;
    let mut out = Vec::with_capacity(children.len());
    let mut recovered = 0;
    for (i, (child, s)) in children.iter().zip(sat).enumerate() {
        out.push(match (s, sortable.get(i).copied().unwrap_or(true)) {
            (true, _) => Arc::clone(child),
            (false, true) => sort(child),
            // An unsortable leg (a whole-window parquet scan) must not grow a
            // blocking sort — but it may still be a UNION whose branches are
            // only partly unordered, which is the shape the delta-rs fork
            // produces when a handful of files carry no `sorting_columns`
            // footer. Sorting just those branches costs a fraction of the leg
            // and restores the ordering the whole scan lost.
            (false, false) => match recover_nested_ordering(child, req, fetch)? {
                Some(fixed) => {
                    recovered += 1;
                    fixed
                }
                // One unrecoverable leg voids the rewrite, so any recovery
                // counted above is discarded with it — hence counting only
                // once the whole rewrite is adopted.
                None => return Ok(None),
            },
        });
    }
    NESTED_ORDERING_RECOVERED.fetch_add(recovered, std::sync::atomic::Ordering::Relaxed);
    Ok(Some(out))
}

/// The unordered branch may be at most this share of the leg. "A few stragglers
/// poisoning the majority" is the case worth rescuing; a leg that is mostly
/// unordered is a whole-window sort by another name, and that is what exhausted
/// the query pool on 2026-08-02.
///
/// A ratio, not an absolute size, because the only size signal available here
/// is `total_byte_size` off the Delta add actions — whole-FILE bytes, while the
/// scan may project 4 of 89 columns. Any absolute threshold would therefore be
/// wrong by an unknown factor and, worse, wrong in the direction that silently
/// disables the rescue on exactly the big-file tables that need it. The ratio
/// is invariant to that error since both sides are measured the same way.
/// `MAX_NESTED_SORT_BYTES` remains as a backstop for a scan so large that even
/// a small share of it is unreasonable; a sort under it still spills through
/// the query pool rather than growing unbounded, so it bounds latency, not
/// safety.
const MAX_NESTED_SORT_SHARE: f64 = 0.25;
const MAX_NESTED_SORT_BYTES: usize = 4 * 1024 * 1024 * 1024;

/// Rewrites of this shape actually applied — a steady climb means the footer
/// repair still has stragglers; a steady zero means every scan declares.
pub static NESTED_ORDERING_RECOVERED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// Legs left unordered because the unordered branch was too big to sort.
pub static NESTED_ORDERING_TOO_BIG: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn branch_bytes(plan: &Arc<dyn ExecutionPlan>) -> usize {
    plan.partition_statistics(None).ok().and_then(|s| s.total_byte_size.get_value().copied()).unwrap_or(usize::MAX)
}

/// Make `plan` satisfy `req` by sorting only the unordered branches of a union
/// nested inside it. Descends through single-child operators and *verifies* the
/// rewrite (`ordering_satisfy` on the rebuilt node) rather than assuming any
/// operator propagates ordering — `DeltaScanExec` re-derives its ordering from
/// its input yet leaves `maintains_input_order` at the default `false`, so a
/// declaration-based descent would give up on exactly the plan this exists for.
fn recover_nested_ordering(plan: &Arc<dyn ExecutionPlan>, req: &LexOrdering, fetch: Option<usize>) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    let Some(here) = resolve_ordering(req, &plan.schema()) else { return Ok(None) };
    let rewritten = match downcast::<UnionExec>(plan.as_ref()) {
        Some(union) => {
            let children: Vec<Arc<dyn ExecutionPlan>> = union.children().into_iter().cloned().collect();
            let sat: Vec<bool> =
                children.iter().map(|c| c.properties().equivalence_properties().ordering_satisfy(here.iter().cloned())).collect::<Result<Vec<_>>>()?;
            // Mixed only: with no ordered branch there is nothing to merge
            // toward and this would sort the entire leg.
            if sat.iter().all(|&s| s) || !sat.iter().any(|&s| s) {
                return Ok(None);
            }
            // `branch_bytes` yields `usize::MAX` for a branch with no size
            // stat, which fails both guards below — unknown size declines.
            let sized: Vec<(usize, bool)> = children.iter().zip(&sat).map(|(c, s)| (branch_bytes(c), *s)).collect();
            let unordered_bytes: usize = sized.iter().filter(|(_, s)| !s).map(|(b, _)| *b).sum();
            let total_bytes: usize = sized.iter().map(|(b, _)| *b).sum();
            let oversized = unordered_bytes > MAX_NESTED_SORT_BYTES || total_bytes == 0 || unordered_bytes as f64 > total_bytes as f64 * MAX_NESTED_SORT_SHARE;
            if oversized {
                NESTED_ORDERING_TOO_BIG.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                tracing::debug!(unordered_bytes, total_bytes, "scan ordering not recovered: non-declaring branch too big to sort");
                return Ok(None);
            }
            tracing::debug!(unordered_bytes, total_bytes, branches = children.len(), "sorting the non-declaring branch to recover scan ordering");
            let ordered: Vec<Arc<dyn ExecutionPlan>> = children
                .iter()
                .zip(sat)
                .map(|(c, s)| match s {
                    true => Arc::clone(c),
                    false => Arc::new(SortExec::new(here.clone(), Arc::clone(c)).with_preserve_partitioning(true).with_fetch(fetch)) as _,
                })
                .collect();
            UnionExec::try_new(ordered)? as Arc<dyn ExecutionPlan>
        }
        None => {
            let children = plan.children();
            if children.len() != 1 {
                return Ok(None);
            }
            let Some(child) = recover_nested_ordering(children[0], req, fetch)? else { return Ok(None) };
            Arc::clone(plan).with_new_children(vec![child])?
        }
    };
    // Self-validating: adopt only if the rebuilt node really does satisfy `req`.
    Ok(rewritten.properties().equivalence_properties().ordering_satisfy(here.iter().cloned())?.then_some(rewritten))
}

/// Walk down from a fetching sort through single-child order-preserving
/// operators to the first `UnionExec`; if the union is mixed (some children
/// satisfy `req`, some don't), sort the unsatisfying children by `req` (with
/// `fetch`) so the union becomes order-preserving. Returns the rewritten
/// subtree, or `None` when nothing applied.
fn order_union(plan: &Arc<dyn ExecutionPlan>, req: &LexOrdering, fetch: Option<usize>) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    if let Some(union) = downcast::<UnionExec>(plan.as_ref()) {
        let Some(req_here) = resolve_ordering(req, &union.schema()) else {
            return Ok(None);
        };
        // Mixed union only (`require_ordered_child`): an ordered child to merge
        // toward AND an unordered child to fix. All-ordered needs nothing;
        // none-ordered means the Delta pushdown is off (mixed footers) — leave
        // the blocking sort in place.
        let children: Vec<Arc<dyn ExecutionPlan>> = union.children().into_iter().cloned().collect();
        return ordered_children(&children, &req_here, fetch, &[], true)?.map(UnionExec::try_new).transpose();
    }
    // Descend only through single-child, order-preserving operators so the
    // ordering we create actually propagates up to the sort — but NEVER through
    // a DedupExec: its survivors are decided across ALL input rows, and a
    // `with_fetch` cut on a leg below it truncates that input. Under
    // version_append the leg's top-n fills with row *versions*, so dedup can
    // emit fewer than n distinct rows while more exist, and an equal-timestamp
    // cut can keep a stale version whose newer sibling was truncated away.
    let children = plan.children();
    if children.len() == 1
        && downcast::<crate::read_dedup::DedupExec>(plan.as_ref()).is_none()
        && plan.maintains_input_order().first() == Some(&true)
        && let Some(new_child) = order_union(children[0], req, fetch)?
    {
        return Ok(Some(Arc::clone(plan).with_new_children(vec![new_child])?));
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
                let Some((req, fetch)) = downcast::<SortExec>(node.as_ref())
                    .map(|s| (s.expr().clone(), s.fetch()))
                    .or_else(|| downcast::<SortPreservingMergeExec>(node.as_ref()).map(|m| (m.expr().clone(), m.fetch())))
                    .filter(|(_, fetch)| fetch.is_some())
                else {
                    return Ok(Transformed::no(node));
                };
                let children = node.children();
                let rewritten = children.first().copied().map(|input| order_union(input, &req, fetch)).transpose()?.flatten();
                Ok(match rewritten {
                    Some(new_input) => Transformed::yes(node.with_new_children(vec![new_input])?),
                    None => Transformed::no(node),
                })
            })?
            .data)
    }
}

#[cfg(test)]
mod tests {
    use datafusion::{
        arrow::{
            compute::SortOptions,
            datatypes::{DataType, Field, SchemaRef, TimeUnit},
        },
        physical_expr::{EquivalenceProperties, Partitioning},
        physical_plan::{
            PlanProperties,
            execution_plan::{Boundedness, EmissionType},
        },
    };

    use super::*;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
            Field::new("id", DataType::Utf8, false),
        ]))
    }

    fn ts_desc() -> LexOrdering {
        LexOrdering::new(vec![PhysicalSortExpr::new(Arc::new(Column::new("timestamp", 0)), SortOptions { descending: true, nulls_first: true })]).unwrap()
    }

    /// A leaf exec that lets the test declare whatever output ordering it wants,
    /// standing in for either the ordered Delta scan or the unordered MemBuffer.
    #[derive(Debug)]
    struct MockLeaf {
        props: Arc<PlanProperties>,
        bytes: usize,
    }

    impl MockLeaf {
        fn leaf(schema: SchemaRef, ordering: Option<LexOrdering>) -> Arc<dyn ExecutionPlan> {
            Self::sized(schema, ordering, 1024)
        }

        fn sized(schema: SchemaRef, ordering: Option<LexOrdering>, bytes: usize) -> Arc<dyn ExecutionPlan> {
            // `Option<LexOrdering>` is an iterator of 0..1 orderings — no branch needed.
            let eq = EquivalenceProperties::new_with_orderings(schema, ordering);
            let props = Arc::new(PlanProperties::new(eq, Partitioning::UnknownPartitioning(1), EmissionType::Incremental, Boundedness::Bounded));
            Arc::new(MockLeaf { props, bytes })
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
        fn execute(&self, _p: usize, _c: Arc<datafusion::execution::TaskContext>) -> Result<datafusion::physical_plan::SendableRecordBatchStream> {
            unimplemented!()
        }
        fn partition_statistics(&self, _p: Option<usize>) -> Result<Arc<datafusion::common::Statistics>> {
            let mut s = datafusion::common::Statistics::new_unknown(&self.schema());
            s.total_byte_size = datafusion::common::stats::Precision::Exact(self.bytes);
            Ok(Arc::new(s))
        }
    }

    /// Number of `SortExec` nodes anywhere in the tree.
    fn count_sorts(plan: &Arc<dyn ExecutionPlan>) -> usize {
        usize::from(downcast::<SortExec>(plan.as_ref()).is_some()) + plan.children().into_iter().map(count_sorts).sum::<usize>()
    }

    /// `SortExec(ts DESC, fetch)` over `mem (unordered) ∪ delta`, where delta
    /// advertises `[ts DESC]` only when `delta_ordered` — the two dimensions the
    /// guards below vary. Returns the plan and the requested ordering.
    fn fetching_sort_over_union(delta_ordered: bool, fetch: Option<usize>) -> (Arc<dyn ExecutionPlan>, LexOrdering) {
        let (s, ord) = (schema(), ts_desc());
        let union = UnionExec::try_new(vec![MockLeaf::leaf(s.clone(), None), MockLeaf::leaf(s, delta_ordered.then(|| ord.clone()))]).unwrap();
        (Arc::new(SortExec::new(ord.clone(), union).with_fetch(fetch)), ord)
    }

    // Bug: `ORDER BY timestamp DESC LIMIT n` over a mem∪delta union re-sorts the
    // whole window because the MemBuffer branch advertises no ordering, so the
    // union is unordered and the delta scan can't early-terminate. The rule must
    // sort the mem branch to match delta's advertised ordering.
    #[test]
    fn wraps_unordered_mem_branch_of_fetching_sort() {
        let (top, ord) = fetching_sort_over_union(true, Some(50));

        let out = OrderedUnionForTopK.optimize(top, &ConfigOptions::new()).unwrap();

        // The union is now order-preserving: it advertises [ts DESC].
        assert!(
            out.children()[0].properties().equivalence_properties().ordering_satisfy(ord.iter().cloned()).unwrap(),
            "union must advertise the sort ordering after the rule runs"
        );
        // Top sort + one injected mem sort = 2 SortExecs.
        assert_eq!(count_sorts(&out), 2, "exactly one SortExec injected over the mem branch");
    }

    // Guard: no fetch (plain ORDER BY, no LIMIT) → rule must not touch the plan,
    // so counts/aggregations and unbounded sorts don't grow a MemBuffer sort.
    #[test]
    fn ignores_sort_without_fetch() {
        let (top, _) = fetching_sort_over_union(true, None);
        let out = OrderedUnionForTopK.optimize(top, &ConfigOptions::new()).unwrap();
        assert_eq!(count_sorts(&out), 1, "no injection when there is no fetch");
    }

    // `ordered_children` is also called directly by `ProjectRoutingTable::scan`
    // (merge-on-read), with `sortable` marking the cheap in-memory legs and
    // `require_ordered_child = false`. Both guards must hold: an unsortable
    // unordered leg (a whole-window Delta scan) aborts the whole rewrite, and a
    // MemBuffer-only scan with no ordered leg at all still gets sorted.
    #[test]
    fn ordered_children_honours_sortable_and_lone_leg() {
        let (s, ord) = (schema(), ts_desc());
        let mem = MockLeaf::leaf(s.clone(), None);
        let delta_ordered = MockLeaf::leaf(s.clone(), Some(ord.clone()));
        let delta_unordered = MockLeaf::leaf(s.clone(), None);

        // mem (sortable) ∪ delta (ordered) → mem gets sorted.
        let out = ordered_children(&[mem.clone(), delta_ordered], &ord, None, &[true, false], false).unwrap().expect("mixed union is rewritten");
        assert_eq!(count_sorts(&out[0]), 1, "the mem leg is sorted");
        assert_eq!(count_sorts(&out[1]), 0, "the already-ordered Delta leg is untouched");

        // mem (sortable) ∪ delta (UNORDERED LEAF, unsortable) → bail entirely: a
        // blocking sort over a whole-window parquet scan is the 2026-07-21 OOM.
        assert!(ordered_children(&[mem.clone(), delta_unordered], &ord, None, &[true, false], false).unwrap().is_none());

        // A lone unordered mem leg: nothing to merge toward, but it is exactly
        // where a fresh version append lives, so it is still sorted.
        assert!(ordered_children(std::slice::from_ref(&mem), &ord, None, &[true], false).unwrap().is_some());
        // ...unless the caller is the top-K rule, which requires an ordered peer.
        assert!(ordered_children(std::slice::from_ref(&mem), &ord, None, &[true], true).unwrap().is_none());
    }

    /// The production Delta leg: a `DeltaScanExec`-style wrapper over the union
    /// the delta-rs fork builds when some files carry no `sorting_columns`
    /// footer — an ordered branch plus an isolated non-declaring one.
    fn delta_leg(s: &SchemaRef, ord: &LexOrdering, unordered_bytes: usize) -> Arc<dyn ExecutionPlan> {
        let inner =
            UnionExec::try_new(vec![MockLeaf::sized(s.clone(), Some(ord.clone()), 8 << 30), MockLeaf::sized(s.clone(), None, unordered_bytes)]).unwrap();
        Arc::new(datafusion::physical_plan::filter::FilterExec::try_new(datafusion::physical_expr::expressions::lit(true), inner).unwrap())
    }

    // Regression (prod 2026-08-07): a 30-day scan of `otel_logs_and_spans` had
    // 25 of 30 files declaring `[timestamp DESC]` and 5 declaring nothing. The
    // Delta leg is unsortable, so `ordered_children` bailed, the mem∪delta union
    // advertised no ordering, and `DedupExec` fell back to `full-set` mode —
    // which buffered the whole 62M-row scan and failed the query at its 2 GiB
    // ceiling. Sorting only the isolated non-declaring BRANCH restores the leg's
    // ordering for a fraction of the cost.
    #[test]
    fn recovers_ordering_by_sorting_only_the_non_declaring_branch() {
        let (s, ord) = (schema(), ts_desc());
        let mem = MockLeaf::leaf(s.clone(), None);
        let delta = delta_leg(&s, &ord, 16 << 20);

        let out = ordered_children(&[mem, delta], &ord, None, &[true, false], false).unwrap().expect("nested mixed union is recoverable");

        assert!(
            out[1].properties().equivalence_properties().ordering_satisfy(ord.iter().cloned()).unwrap(),
            "the Delta leg must advertise the ordering after the rewrite"
        );
        // One sort over the mem leg, one over the non-declaring branch only —
        // NOT one over the whole Delta leg.
        assert_eq!(count_sorts(&out[0]), 1, "mem leg sorted");
        assert_eq!(count_sorts(&out[1]), 1, "exactly one sort, over the non-declaring branch");
    }

    // Guard: the rescue is bounded. A non-declaring branch bigger than
    // `MAX_NESTED_SORT_BYTES` is a whole-window sort by another name, so the
    // leg stays unordered and the caller bails as it did before.
    #[test]
    fn refuses_to_sort_an_oversized_non_declaring_branch() {
        let (s, ord) = (schema(), ts_desc());
        let mem = MockLeaf::leaf(s.clone(), None);
        // Ordered branch is 8 GiB in `delta_leg`, so 4 GiB unordered is both
        // over the absolute cap and over the 25% share.
        let delta = delta_leg(&s, &ord, MAX_NESTED_SORT_BYTES + 1);
        assert!(ordered_children(&[mem.clone(), delta], &ord, None, &[true, false], false).unwrap().is_none());

        // ...and a branch well under the absolute cap is still refused when it
        // is most of the leg: that is a whole-window sort, not a rescue.
        let majority = delta_leg(&s, &ord, 6 << 30);
        assert!(ordered_children(&[mem, majority], &ord, None, &[true, false], false).unwrap().is_none());
    }

    // Guard: when no child is ordered (Delta pushdown off during mixed-footer
    // rollout) the rule is a no-op — the built-in blocking sort stays.
    #[test]
    fn ignores_union_with_no_ordered_child() {
        let (top, _) = fetching_sort_over_union(false, Some(50));
        let out = OrderedUnionForTopK.optimize(top, &ConfigOptions::new()).unwrap();
        assert_eq!(count_sorts(&out), 1, "no injection when neither branch is ordered");
    }
}
