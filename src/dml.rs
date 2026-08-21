use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
    time::Instant,
};

use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::RecordBatch,
        compute::concat_batches,
        datatypes::{DataType, Field, Schema, SchemaRef},
    },
    catalog::Session,
    common::{
        Column, JoinType, Result,
        tree_node::{Transformed, TreeNode},
    },
    error::DataFusionError,
    execution::{
        SendableRecordBatchStream, SessionStateBuilder, TaskContext,
        context::{QueryPlanner, SessionState},
    },
    logical_expr::{Expr, Join, LogicalPlan, WriteOp, utils::split_conjunction},
    physical_plan::{DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, PlanProperties, stream::RecordBatchStreamAdapter},
    physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner},
};
use futures::{StreamExt, TryStreamExt};
use tracing::{Instrument, debug, error, field::Empty, info, instrument, warn};

use crate::{
    database::Database,
    observability::{arrow_err, exec_err},
    read::optimizers::extract_project_id_from_expr,
    write::BufferedWriteLayer,
};

/// Reject larger `UPDATE ... FROM` sources before materializing them.
const MAX_UPDATE_SOURCE_ROWS: usize = 1_000_000;
const SLOW_DML_PHASE_US: u64 = 1_000_000;
/// Maximum source rows in one merge-on-read scan. Each chunk becomes bounded
/// IN-lists on the complete join key, so a large enrichment UPDATE never falls
/// back to decoding and deduplicating its whole target time window.
// Keep this well below the depth that DataFusion's expression optimizer can
// safely visit on a Tokio worker stack. Production aborted at 4,096 rows
// while optimizing the two complete-key IN lists (SIGABRT: stack overflow).
// 256 bounds both decoded work and optimizer recursion without relying on a
// larger process/thread stack.
const MOR_KEY_PUSHDOWN_ROWS: usize = 256;

fn log_slow_phase(phase: &'static str, table_name: &str, project_id: &str, started: Instant, rows: Option<u64>) {
    let duration_us = started.elapsed().as_micros() as u64;
    if duration_us >= SLOW_DML_PHASE_US {
        info!(event = "dml.slow_phase", phase, table.name = table_name, project_id, duration_us, rows, "slow DML phase");
    }
}

/// Build a clean SessionState with config + runtime from the given session but with
/// delta-rs's DeltaPlanner instead of our custom DmlQueryPlanner.
pub(crate) fn delta_session_from(session: &SessionState) -> Arc<dyn Session> {
    // delta-rs's DELETE/UPDATE re-reads existing parquet files and rewrites
    // them. Without `schema_force_view_types=false`, the reader returns
    // Struct{BinaryView,BinaryView} for our Variant columns while
    // delta_kernel's `unshredded_variant()` schema declares Binary —
    // mismatch rejects the operation with "Expected ... Binary, got ...
    // BinaryView" even on an empty table.
    //
    // Start from `DeltaSessionConfig::default()` so we inherit delta-rs's
    // other required defaults (hash_join_inlist_pushdown=0, etc.) and only
    // override the view-types flag.
    let cfg: datafusion::prelude::SessionConfig = deltalake::delta_datafusion::DeltaSessionConfig::default().into();
    let cfg = cfg.set_bool("datafusion.execution.parquet.schema_force_view_types", false);
    // Same nullability-widened file set as `Database::create_session_context`
    // (2026-07-31, 7d68f01): a DML plan reading those files must not trip the
    // physical-vs-logical aggregate schema check either.
    let cfg = cfg.set_bool("datafusion.execution.skip_physical_aggregate_schema_check", true);
    // A MERGE-UPDATE re-reads and rewrites WHOLE wide otel rows, so it is the
    // most decode-expensive read in the system — and it was the only one still
    // on DataFusion's 8192-row default. The query and maintenance sessions have
    // run these same rows at 2048 since the 2026-08-07 heap work; this session
    // was missed, and a dump taken mid-burst on 2026-08-13 put 38.3 GiB (57% of
    // live heap) back in exactly the stack that work had cut —
    // `extend_from_dictionary` under `ByteArrayDecoder::read`.
    let mut cfg = cfg;
    let _ = cfg.options_mut().set("datafusion.execution.batch_size", crate::database::WIDE_ROW_DECODE_BATCH_SIZE);
    Arc::new(
        SessionStateBuilder::new()
            .with_config(cfg)
            .with_runtime_env(session.runtime_env().clone())
            .with_default_features()
            .with_query_planner(deltalake::delta_datafusion::planner::DeltaPlanner::new())
            .build(),
    )
}

/// Materialized RHS of an `UPDATE ... FROM` statement together with the
/// equi-join key spec that pairs target rows with source rows.
///
/// `batch` is the fully-materialized source side (capped at
/// [`MAX_UPDATE_SOURCE_ROWS`]). Assignment exprs reference its columns via
/// the `source` qualifier (e.g. `col("source.value")`); downstream code
/// expects those refs to resolve against `schema`.
#[derive(Clone)]
pub struct UpdateSource {
    pub batch: RecordBatch,
    pub schema: SchemaRef,
    /// `(target_col, source_col)` pairs. Names refer to bare column names;
    /// table qualifiers are stripped during extraction.
    pub join_keys: Vec<(String, String)>,
}

/// Output of [`extract_dml_info`]: parsed DML shape, with an unmaterialized
/// source plan when the input contained a `Join` (i.e. `UPDATE ... FROM`).
/// Materialization runs asynchronously in [`DmlQueryPlanner::create_physical_plan`].
pub struct DmlInfo {
    pub table_name: String,
    pub project_id: String,
    pub predicate: Option<Expr>,
    pub assignments: Option<Vec<(String, Expr)>>,
    /// Source plan + join keys when the input contained a `Join`. Materialized
    /// into [`UpdateSource`] before the physical [`DmlExec`] is constructed.
    pub source_plan: Option<UpdateSourcePlan>,
}

#[derive(Clone)]
pub struct UpdateSourcePlan {
    pub plan: LogicalPlan,
    pub join_keys: Vec<(String, String)>,
}

#[derive(derive_more::Debug)]
pub struct DmlQueryPlanner {
    #[debug(skip)]
    planner: DefaultPhysicalPlanner,
    #[debug(skip)]
    database: Arc<Database>,
}

impl DmlQueryPlanner {
    pub fn new(database: Arc<Database>) -> Self {
        Self { planner: DefaultPhysicalPlanner::with_extension_planners(vec![]), database }
    }
}

/// Give `plan`'s columns the qualifiers `target` carries, field for field.
///
/// The rollup SQL produces the right NAMES — its aliases are the aggregate's own
/// field names — but SELECT aliases are unqualified, while an aggregate's
/// group-by column keeps its source qualifier. A `Column` reference in an
/// untouched node above resolves on `(qualifier, name)`, so without this the
/// substitution would not resolve.
pub(crate) fn requalified(plan: LogicalPlan, target: &datafusion::common::DFSchemaRef) -> Result<LogicalPlan> {
    let expr = target
        .iter()
        .map(|(qualifier, field)| {
            let column = Expr::Column(Column::new_unqualified(field.name()));
            match qualifier {
                Some(qualifier) => column.alias_qualified(Some(qualifier.clone()), field.name()),
                None => column,
            }
        })
        .collect();
    Ok(LogicalPlan::Projection(datafusion::logical_expr::logical_plan::Projection::try_new(expr, Arc::new(plan))?))
}

/// Swap `replacement` in for the `matched` node, leaving every other node as the
/// optimizer produced it.
///
/// This is the whole reassembly. It replaces peeling the plan apart and
/// rebuilding it, which could only ever accept a fixed grammar of parent nodes
/// and kept declining production shapes that had one layer more.
pub(crate) fn substitute(plan: &LogicalPlan, matched: &LogicalPlan, replacement: LogicalPlan) -> Result<LogicalPlan> {
    use datafusion::common::tree_node::TreeNodeRecursion;
    // EVERY occurrence, not just the first: an inlined CTE referenced twice
    // plans to two identical aggregates, and replacing one would leave the other
    // scanning raw — the same answer at half the saving.
    let mut replaced = 0usize;
    let rewritten = plan
        .clone()
        .transform_down(|node| {
            if &node != matched {
                return Ok(Transformed::no(node));
            }
            replaced += 1;
            // `Jump` skips the subtree just inserted; siblings are still visited.
            Ok(Transformed::new(replacement.clone(), true, TreeNodeRecursion::Jump))
        })?
        .data;
    // A target that is not in the plan would leave it untouched — a correct RAW
    // answer reported as a rollup hit, which is the one failure the counters
    // cannot show. Fail instead; the caller records it as a miss.
    if replaced == 0 {
        return Err(DataFusionError::Internal("rollup substitution target is not in the plan".into()));
    }
    // Parent nodes cache their `DFSchema`. Rebuilding bottom-up both refreshes
    // those caches and re-resolves every expression against its new input, so a
    // replacement whose fields do not line up fails HERE — as a recorded miss
    // and a raw fallback — rather than reaching the physical planner.
    rewritten.transform_up(|node| node.recompute_schema().map(Transformed::yes)).map(|rewritten| rewritten.data)
}

#[async_trait]
impl QueryPlanner for DmlQueryPlanner {
    #[instrument(
        name = "dml.create_physical_plan",
        skip_all,
        fields(
            operation = Empty,
            table.name = Empty,
            project_id = Empty,
        )
    )]
    async fn create_physical_plan(&self, logical_plan: &LogicalPlan, session_state: &SessionState) -> Result<Arc<dyn ExecutionPlan>> {
        // COUNT(*) stats pushdown — answers gate-eligible count tiles from
        // Delta add-action stats with zero parquet IO; declines to `None`
        // for anything it can't prove exact.
        if let Some(exec) = crate::read::try_count_pushdown(logical_plan, &self.database).await? {
            return Ok(exec);
        }
        match self.database.rollup_sql(logical_plan, session_state).await {
            Ok(Some(crate::database::RollupRewrite { sql, grain, mode, matched, ticket })) => {
                let rewritten = async {
                    let plan = session_state.create_logical_plan(&sql).await?;
                    session_state.optimize(&substitute(logical_plan, &matched, requalified(plan, matched.schema())?)?)
                }
                .await;
                match rewritten {
                    // Names, order and types must match; the rollup SQL's aliases
                    // are unqualified where the original aggregate keeps the
                    // source qualifier, and a derived `==` on DFSchema compares
                    // qualifiers — which rejected every grouped query.
                    Ok(rewritten) => match rewritten.schema().has_equivalent_names_and_types(logical_plan.schema()) {
                        Ok(()) => match self.planner.create_physical_plan(&rewritten, session_state).await {
                            Ok(exec) if self.database.rollup_ticket_current(&ticket).await => {
                                crate::observability::record_rollup_hit(mode, &grain);
                                return Ok(exec);
                            }
                            Ok(_) => crate::observability::record_rollup_miss(crate::rollup::MissReason::StaleCoverage),
                            Err(error) => {
                                warn!(%error, event = "rollup_rewrite_failed", stage = "physical", "rollup rewrite could not be planned; using raw plan");
                                crate::observability::record_rollup_miss(crate::rollup::MissReason::UnsupportedShape);
                            }
                        },
                        // The mismatch names the offending field and both types.
                        // Discarding it leaves `rewrite_schema_mismatch` with no
                        // way to tell WHICH column drifted.
                        Err(error) => {
                            warn!(%error, event = "rollup_rewrite_failed", stage = "schema", "rollup rewrite does not match the query schema; using raw plan");
                            crate::observability::record_rollup_miss(crate::rollup::MissReason::RewriteSchemaMismatch);
                        }
                    },
                    Err(error) => {
                        warn!(%error, event = "rollup_rewrite_failed", stage = "sql", "rollup rewrite SQL could not be planned; using raw plan");
                        crate::observability::record_rollup_miss(crate::rollup::MissReason::UnsupportedShape);
                    }
                }
            }
            Ok(None) => {}
            Err(reason) => {
                crate::observability::record_rollup_miss(reason);
                // A miss counter alone cannot be acted on. Prod 2026-08-17 sat at
                // ~2.7 MissingProject misses/second with rollup_hits at 0, and
                // there was no way to tell from outside whether the refused plans
                // were monoscope's parameterized dashboards or ad-hoc literal
                // queries — the two need opposite fixes. Sampled so a
                // multiple-per-second rate cannot flood the log, and the plan is
                // only rendered when a sample is actually taken.
                if crate::observability::sample_rollup_miss() {
                    warn!(
                        reason = reason.label(),
                        plan = %fmt_capped(&logical_plan.display_indent().to_string(), 1200),
                        event = "rollup_miss_sampled",
                        "rollup routing refused; falling back to the raw plan"
                    );
                }
            }
        }
        match logical_plan {
            LogicalPlan::Dml(dml) if matches!(dml.op, WriteOp::Update | WriteOp::Delete) => {
                let span = tracing::Span::current();
                let operation = if matches!(dml.op, WriteOp::Update) { "UPDATE" } else { "DELETE" };
                span.record("operation", operation);

                let input_exec = self.planner.create_physical_plan(&dml.input, session_state).await?;
                let is_update = matches!(dml.op, WriteOp::Update);
                let info = extract_dml_info(&dml.input, &dml.table_name.to_string(), is_update)?;

                span.record("table.name", info.table_name.as_str());
                span.record("project_id", info.project_id.as_str());

                // For `UPDATE ... FROM`, materialize the source RHS once at plan
                // construction. Both backends (MemBuffer hash-join + Delta MergeBuilder)
                // consume the materialized batch; replaying the source SQL at execution
                // time would be non-deterministic if the source references mutable state.
                let source = if let Some(sp) = info.source_plan { Some(materialize_source(&self.planner, session_state, sp).await?) } else { None };

                let session = delta_session_from(session_state);
                let exec = if is_update {
                    DmlExec::update(info.table_name, info.project_id, input_exec, self.database.clone(), session)
                        .predicate(info.predicate)
                        .assignments(info.assignments.unwrap_or_default())
                        .source(source)
                } else {
                    DmlExec::delete(info.table_name, info.project_id, input_exec, self.database.clone(), session).predicate(info.predicate)
                };
                // Resolve the layer at PLAN time, not planner-construction time:
                // sessions (and this planner) are created during boot before the
                // buffered layer is attached to the Database.
                Ok(Arc::new(exec.buffered_layer(self.database.buffered_layer().cloned())))
            }
            _ => self.planner.create_physical_plan(logical_plan, session_state).await,
        }
    }
}

/// Extract DML information from logical plan.
///
/// Walks the projection/filter/scan chain of `dml.input`. When a `Join` is
/// encountered (i.e. the user wrote `UPDATE t SET … FROM src WHERE t.k = src.k`),
/// it identifies which side scans the target table, extracts equi-join keys, and
/// stashes the *other* side's `LogicalPlan` for later async materialization. The
/// walk then continues down the target side as a plain `UPDATE`.
fn extract_dml_info(input: &LogicalPlan, table_name: &str, extract_assignments: bool) -> Result<DmlInfo> {
    // Imperative descent: each node kind updates a different slot of the state and
    // the walk is not a fixed-length iteration, so a fold would just thread the same
    // four fields by hand. Iterative (not recursive) — plan trees can be deep.
    let mut current_plan = input;
    let mut predicate: Option<Expr> = None;
    let mut assignments = None;
    let mut project_id = String::new();
    let mut source_plan: Option<UpdateSourcePlan> = None;

    loop {
        match current_plan {
            LogicalPlan::Projection(proj) if extract_assignments => {
                match &mut assignments {
                    // First Projection encountered: real UPDATE assignments.
                    None => assignments = Some(extract_assignments_from_projection(proj)),
                    // Nested Projection (DataFusion CSE introduces one that defines
                    // `__common_expr_*`). Inline its aliases into our assignments so
                    // references to those synthetic columns resolve when we evaluate
                    // physical exprs against the bare table schema below.
                    Some(existing) => inline_projection_aliases(proj, existing)?,
                }
                current_plan = proj.input.as_ref();
            }
            LogicalPlan::Filter(filter) => {
                // AND-merge, never overwrite: the walk may already hold the
                // cross-side conjunct pulled from `join.filter` above (e.g. the
                // enrichment guard `NOT (o.hashes @> ARRAY[u.tag])`), and the
                // optimizer pushes the target-side filter BELOW the join —
                // overwriting here silently dropped the guard, so every
                // enrichment pass re-appended full-row versions for every
                // matched span, tagged or not (unbounded write amplification;
                // prod 2026-08-03).
                project_id = extract_project_id_from_expr(&filter.predicate).unwrap_or(project_id);
                let p = filter.predicate.clone();
                predicate = Some(match predicate.take() {
                    Some(existing) => existing.and(p),
                    None => p,
                });
                current_plan = filter.input.as_ref();
            }
            LogicalPlan::Join(join) if extract_assignments => {
                // `UPDATE ... FROM` lowers to a `Join` whose left or right side
                // scans the target table. Detect which side is target; the other
                // is the source to materialize.
                if source_plan.is_some() {
                    return Err(DataFusionError::NotImplemented("UPDATE with multiple FROM sources (chained joins) is not supported".to_string()));
                }
                if join.join_type != JoinType::Inner {
                    return Err(DataFusionError::NotImplemented(format!("UPDATE ... FROM with {:?} join is not supported (only INNER)", join.join_type)));
                }
                let (target_side, source_side, keys) = identify_target_side(join, table_name)?;
                // DataFusion stores cross-side conditions (e.g. user wrote
                // `NOT (o.hashes @> ARRAY[u.tag])`) in `join.filter` rather
                // than the surrounding `Filter`. Pull it into the predicate
                // path so the Delta MergeBuilder AND-s it into the join key
                // expression, and the MemBuffer hash-join evaluates it
                // against the widened batch.
                if let Some(jf) = &join.filter {
                    predicate = Some(predicate.take().map_or_else(|| jf.clone(), |existing| existing.and(jf.clone())));
                }
                source_plan = Some(UpdateSourcePlan { plan: source_side.clone(), join_keys: keys });
                current_plan = target_side;
            }
            LogicalPlan::SubqueryAlias(alias) => {
                // Aliases on the source subquery (e.g. `FROM (...) AS u`) wrap
                // the inner plan; descend through them transparently.
                current_plan = alias.input.as_ref();
            }
            LogicalPlan::TableScan(scan) => {
                project_id = scan.filters.iter().find_map(extract_project_id_from_expr).unwrap_or(project_id);

                predicate = predicate.or_else(|| scan.filters.iter().cloned().reduce(Expr::and));
                break;
            }
            other => {
                // Unknown node — Window/Subquery/Union/etc. Fall through the first
                // input; warn so a missing predicate/project_id below is traceable
                // to a plan shape this extractor doesn't understand.
                warn!(target: "dml", node = ?std::mem::discriminant(other), "extract_dml_info: unhandled LogicalPlan node, descending first child — predicate/project_id extraction may be incomplete");
                match other.inputs().first() {
                    Some(input) => current_plan = input,
                    None => break,
                }
            }
        }
    }

    if project_id.is_empty() {
        return Err(DataFusionError::Plan(format!("{} requires a project_id filter in WHERE clause", if extract_assignments { "UPDATE" } else { "DELETE" })));
    }
    // Columns are IMMUTABLE by default, and the read path pushes filters on them
    // below the merge-on-read dedup on that basis — sound only while every
    // version of a row agrees on their value. An UPDATE assigning an undeclared
    // column would break that silently and at read time, surfacing a stale
    // version that matches a predicate the winning version does not. Refuse at
    // plan time so the declaration is enforced rather than trusted.
    //
    // The tiebreak and tombstone are exempt: `stamp_version` rewrites the
    // tiebreak on every append and a delete appends a tombstone row, so both are
    // mutable by construction and already excluded from the pushdown.
    if let Some(assigned) = assignments.as_ref()
        && let Some(schema) = crate::schema::get_schema(table_name).filter(|schema| schema.version_append)
    {
        let allowed = |column: &String| {
            schema.fields.iter().any(|field| &field.name == column && field.mutable)
                || schema.dedup_tiebreak.as_ref() == Some(column)
                || schema.tombstone_column.as_ref() == Some(column)
        };
        if let Some((blocked, _)) = assigned.iter().find(|(column, _)| !allowed(column)) {
            return Err(DataFusionError::Plan(format!(
                "UPDATE cannot assign `{blocked}` on `{table_name}`: columns are immutable unless declared `mutable: true`, and read filters on \
                 immutable columns are pushed below the merge-on-read dedup on that basis"
            )));
        }
    }

    Ok(DmlInfo { table_name: table_name.to_string(), project_id, predicate, assignments, source_plan })
}

/// Walk a [`LogicalPlan`] tree until we hit a `TableScan`. Returns the matched
/// scan's qualified name or `None` if no scan is reachable.
fn find_table_scan_name(plan: &LogicalPlan) -> Option<String> {
    // Iterative BFS rather than recursion: plan trees can be arbitrarily deep and Rust has no TCO.
    let mut q = VecDeque::from([plan]);
    while let Some(p) = q.pop_front() {
        if let LogicalPlan::TableScan(scan) = p {
            return Some(scan.table_name.to_string());
        }
        q.extend(p.inputs());
    }
    None
}

/// Given a `Join` and the target table name, decide which child is the target
/// (the side that scans the target table) and extract equi-join key pairs in
/// `(target_col_name, source_col_name)` order.
#[allow(clippy::type_complexity)] // Tuple shape is the natural result of "(target, source, key_pairs)" and a named type would be one-shot.
fn identify_target_side<'a>(join: &'a Join, target_table_name: &str) -> Result<(&'a LogicalPlan, &'a LogicalPlan, Vec<(String, String)>)> {
    let left_scan = find_table_scan_name(&join.left);
    let right_scan = find_table_scan_name(&join.right);

    let target_is_left = match (left_scan.as_deref(), right_scan.as_deref()) {
        (Some(l), _) if l.ends_with(target_table_name) => true,
        (_, Some(r)) if r.ends_with(target_table_name) => false,
        _ => {
            return Err(DataFusionError::Plan(format!(
                "UPDATE target table `{}` not found on either side of FROM-join (left={:?}, right={:?})",
                target_table_name, left_scan, right_scan
            )));
        }
    };

    let (target_side, source_side) = if target_is_left { (join.left.as_ref(), join.right.as_ref()) } else { (join.right.as_ref(), join.left.as_ref()) };

    let bare = |e: &Expr| {
        expr_to_bare_col(e).ok_or_else(|| DataFusionError::NotImplemented(format!("UPDATE ... FROM join key must be a plain column reference, got: {e}")))
    };
    // `join.on` is Vec<(left_expr, right_expr)>. Flip if target was on the right.
    let join_keys = join
        .on
        .iter()
        .map(|(l, r)| {
            let (tgt_expr, src_expr) = if target_is_left { (l, r) } else { (r, l) };
            Ok((bare(tgt_expr)?, bare(src_expr)?))
        })
        .collect::<Result<Vec<_>>>()?;

    Ok((target_side, source_side, join_keys))
}

/// Pull a bare column name (drop any table qualifier) from an `Expr::Column`.
/// Unwraps `Alias`, `Cast`, and `TryCast` — DataFusion's logical planner often
/// inserts an implicit cast on join keys when the two sides have slightly
/// different types (e.g. `Utf8` vs `Utf8View`), which is irrelevant for the
/// purposes of identifying which target column the join key resolves to.
/// Returns `None` for any other expression shape, which propagates as a clean
/// "not supported" error to the caller.
fn expr_to_bare_col(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Column(c) => Some(c.name.clone()),
        Expr::Alias(a) => expr_to_bare_col(&a.expr),
        Expr::Cast(cast) => expr_to_bare_col(&cast.expr),
        Expr::TryCast(cast) => expr_to_bare_col(&cast.expr),
        _ => None,
    }
}

/// Materialize an [`UpdateSourcePlan`] into a single [`RecordBatch`] by running
/// the source plan as a regular DataFusion query and concatenating the streamed
/// batches. Errors if the source exceeds [`MAX_UPDATE_SOURCE_ROWS`].
async fn materialize_source(planner: &DefaultPhysicalPlanner, session_state: &SessionState, sp: UpdateSourcePlan) -> Result<UpdateSource> {
    let started = Instant::now();
    let phys = planner.create_physical_plan(&sp.plan, session_state).await?;
    let planning_us = started.elapsed().as_micros() as u64;
    let schema = phys.schema();
    let task_ctx = Arc::new(TaskContext::from(session_state));

    // The source plan may be multi-partition; stream each in turn (lazily, one at
    // a time) and cap as we go so an oversized source never fully materializes.
    let (total_rows, batches) = futures::stream::iter((0..phys.properties().partitioning.partition_count()).map(|p| phys.execute(p, task_ctx.clone())))
        .try_flatten()
        .try_fold((0usize, Vec::new()), |(rows, mut acc), batch| async move {
            let rows = rows + batch.num_rows();
            if rows > MAX_UPDATE_SOURCE_ROWS {
                return Err(DataFusionError::Execution(format!(
                    "UPDATE ... FROM source exceeded the {MAX_UPDATE_SOURCE_ROWS} row cap; refine the source query or page the update"
                )));
            }
            acc.push(batch);
            Ok((rows, acc))
        })
        .await?;

    let combined = concat_batches(&schema, &batches).map_err(arrow_err)?;
    let duration_us = started.elapsed().as_micros() as u64;
    if duration_us >= SLOW_DML_PHASE_US {
        info!(
            event = "dml.slow_phase",
            phase = "update_source_materialization",
            planning_us,
            execution_us = duration_us.saturating_sub(planning_us),
            duration_us,
            source_rows = total_rows,
            "slow DML phase"
        );
    }

    Ok(UpdateSource { batch: combined, schema, join_keys: sp.join_keys })
}

/// Extract UPDATE assignments from a projection: aliased exprs that actually
/// change the column (a bare `col AS col` passthrough is not an assignment).
fn extract_assignments_from_projection(proj: &datafusion::logical_expr::Projection) -> Vec<(String, Expr)> {
    proj.expr
        .iter()
        .zip(proj.schema.fields())
        .filter_map(|(expr, field)| {
            let field_name = field.name();
            match expr {
                Expr::Alias(alias) if alias.name == *field_name => {
                    (!matches!(&*alias.expr, Expr::Column(col) if col.name == *field_name)).then(|| (field_name.clone(), (*alias.expr).clone()))
                }
                Expr::Column(_) => None,
                _ => Some((field_name.clone(), expr.clone())),
            }
        })
        .collect()
}

/// Inline aliases from a nested (CSE) Projection into the existing UPDATE assignment
/// exprs. Without this, refs like `__common_expr_1` survive into mem_buffer's physical
/// expr evaluation against the bare table schema and fail with "Column not found".
fn inline_projection_aliases(proj: &datafusion::logical_expr::Projection, assignments: &mut [(String, Expr)]) -> Result<()> {
    let subs: HashMap<&str, &Expr> = proj
        .expr
        .iter()
        .zip(proj.schema.fields())
        .filter_map(|(expr, field)| match expr {
            Expr::Alias(a) if a.name != *field.name() || a.name.starts_with("__common_expr_") => Some((a.name.as_str(), a.expr.as_ref())),
            _ => None,
        })
        .collect();
    if subs.is_empty() {
        return Ok(());
    }
    assignments.iter_mut().try_for_each(|(_, value_expr)| {
        *value_expr = value_expr
            .clone()
            .transform(|e| match &e {
                Expr::Column(col) => Ok(match subs.get(col.name.as_str()) {
                    Some(replacement) => Transformed::yes((*replacement).clone()),
                    None => Transformed::no(e),
                }),
                _ => Ok(Transformed::no(e)),
            })
            .map(|t| t.data)
            .map_err(exec_err("Failed to inline CSE alias"))?;
        Ok(())
    })
}

/// Unified DML execution plan
#[derive(Clone, derive_more::Debug)]
pub struct DmlExec {
    op_type: DmlOperation,
    table_name: String,
    project_id: String,
    predicate: Option<Expr>,
    assignments: Vec<(String, Expr)>,
    /// Materialized source for `UPDATE ... FROM`. When `Some`, dispatch
    /// routes to [`perform_update_with_source`] / [`perform_delta_merge_update`].
    #[debug(skip)]
    source: Option<UpdateSource>,
    #[debug(skip)]
    input: Arc<dyn ExecutionPlan>,
    #[debug(skip)]
    database: Arc<Database>,
    #[debug(skip)]
    buffered_layer: Option<Arc<BufferedWriteLayer>>,
    #[debug(skip)]
    session: Arc<dyn Session>,
    #[debug(skip)]
    properties: Arc<PlanProperties>,
}

#[derive(Debug, Clone, PartialEq, strum::Display, strum::AsRefStr)]
enum DmlOperation {
    #[strum(to_string = "UPDATE")]
    Update,
    #[strum(to_string = "DELETE")]
    Delete,
}

impl DmlExec {
    fn new(
        op_type: DmlOperation, table_name: String, project_id: String, input: Arc<dyn ExecutionPlan>, database: Arc<Database>, session: Arc<dyn Session>,
    ) -> Self {
        let properties = Arc::new(PlanProperties::new(
            datafusion::physical_expr::EquivalenceProperties::new(input.schema()),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            input.properties().emission_type,
            input.properties().boundedness,
        ));
        Self { op_type, table_name, project_id, predicate: None, assignments: vec![], source: None, input, database, buffered_layer: None, session, properties }
    }

    pub fn update(table_name: String, project_id: String, input: Arc<dyn ExecutionPlan>, database: Arc<Database>, session: Arc<dyn Session>) -> Self {
        Self::new(DmlOperation::Update, table_name, project_id, input, database, session)
    }

    pub fn delete(table_name: String, project_id: String, input: Arc<dyn ExecutionPlan>, database: Arc<Database>, session: Arc<dyn Session>) -> Self {
        Self::new(DmlOperation::Delete, table_name, project_id, input, database, session)
    }

    pub fn predicate(mut self, predicate: Option<Expr>) -> Self {
        self.predicate = predicate;
        self
    }
    pub fn assignments(mut self, assignments: Vec<(String, Expr)>) -> Self {
        self.assignments = assignments;
        self
    }
    pub fn source(mut self, source: Option<UpdateSource>) -> Self {
        self.source = source;
        self
    }
    pub fn buffered_layer(mut self, layer: Option<Arc<BufferedWriteLayer>>) -> Self {
        self.buffered_layer = layer;
        self
    }
}

impl DisplayAs for DmlExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "{}: table={}, project_id={}", self.name(), self.table_name, self.project_id)?;
                if self.op_type == DmlOperation::Update && !self.assignments.is_empty() {
                    write!(f, ", assignments=[{}]", self.assignments.iter().map(|(col, expr)| format!("{} = {}", col, expr)).collect::<Vec<_>>().join(", "))?;
                }
                if let Some(ref pred) = self.predicate {
                    write!(f, ", predicate={}", pred)?;
                }
                Ok(())
            }
            _ => write!(f, "{}", self.name()),
        }
    }
}

#[async_trait]
impl ExecutionPlan for DmlExec {
    fn name(&self) -> &'static str {
        match self.op_type {
            DmlOperation::Update => "DeltaUpdateExec",
            DmlOperation::Delete => "DeltaDeleteExec",
        }
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self { input: children[0].clone(), ..(*self).clone() }))
    }

    #[instrument(name = "dml.execute", skip_all, fields(operation = self.op_type.as_ref(), table.name = %self.table_name, project_id = %self.project_id, has_predicate = self.predicate.is_some(), rows.affected = Empty))]
    fn execute(&self, _partition: usize, _context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        let span = tracing::Span::current();
        // DataFusion's standard DML output schema: a single UInt64 "count"
        // column. The pgwire layer's dml_completion reads exactly this shape
        // to build the CommandComplete tag — any other name/type silently
        // reports "UPDATE 0" to clients regardless of rows affected.
        let schema = Arc::new(Schema::new(vec![Field::new("count", DataType::UInt64, false)]));
        let schema_clone = schema.clone();

        // One clone of the (Arc-backed) plan instead of nine field clones; the
        // future must own everything it touches.
        let this = self.clone();

        let future = async move {
            let DmlExec { op_type, table_name, project_id, predicate, assignments, source, database, buffered_layer, session, .. } = this;
            // A merge-on-read table re-appends every affected row through
            // `insert_records_batch`, which already invalidates each date those
            // rows land in — and the append carries each row's ORIGINAL
            // timestamp, so those are exactly the dates that changed, whatever
            // the predicate looked like. Invalidating here as well can only be
            // broader, never more precise.
            //
            // The exception is a statement that ASSIGNS `timestamp`: the row
            // moves to a new date, the append invalidates only the new one, and
            // the old partition is left holding coverage for a version that is
            // now superseded. `invalidate_rollup_dml` falls back to the
            // source-wide wipe for exactly that case.
            if !is_version_append(&table_name) || assignments.iter().any(|(column, _)| column == "timestamp") {
                database.invalidate_rollup_dml(&project_id, &table_name, predicate.as_ref(), &assignments)?;
            }
            let result = match op_type {
                DmlOperation::Update => {
                    perform_update_with_buffer(&database, buffered_layer.as_ref(), &table_name, &project_id, predicate, assignments, source, session, &span)
                        .await
                }
                DmlOperation::Delete => {
                    perform_delete_with_buffer(&database, buffered_layer.as_ref(), &table_name, &project_id, predicate, session, &span).await
                }
            };

            result
                .inspect(|rows| {
                    span.record("rows.affected", rows);
                })
                .and_then(|rows| RecordBatch::try_new(schema_clone, vec![Arc::new(datafusion::arrow::array::UInt64Array::from(vec![rows]))]).map_err(arrow_err))
                .inspect_err(|e| error!("{} failed: {}", op_type.as_ref(), e))
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, futures::stream::once(future))))
    }
}

struct DmlContext<'a> {
    database: &'a Database,
    buffered_layer: Option<&'a Arc<BufferedWriteLayer>>,
    table_name: &'a str,
    project_id: &'a str,
    predicate: Option<Expr>,
}

impl DmlContext<'_> {
    /// `delta_op` is a closure (not a bare Future) so its body — which may
    /// acquire a write lock and call `update_state` — is only constructed
    /// when there is committed data to operate on. It receives the
    /// watermark-clamped predicate (see [`delta_leg_predicate`]), computed
    /// here so no DML path can run a Delta leg with an unclamped window or
    /// a pre-`await_inflight_flushes` watermark.
    async fn execute<F, G, Fut>(self, mem_op: F, delta_op: G) -> Result<u64>
    where
        F: FnOnce(&BufferedWriteLayer, Option<&Expr>) -> Result<u64>,
        G: FnOnce(Option<Expr>) -> Fut,
        Fut: std::future::Future<Output = Result<u64>>,
    {
        let has_uncommitted = self.buffered_layer.is_some_and(|l| l.has_table(self.project_id, self.table_name));

        let mem_rows = match self.buffered_layer.filter(|_| has_uncommitted) {
            Some(layer) => {
                let started = Instant::now();
                let rows = mem_op(layer, self.predicate.as_ref())?;
                log_slow_phase("mem_buffer_mutation", self.table_name, self.project_id, started, Some(rows));
                rows
            }
            None => 0,
        };
        debug!(
            "DML mem leg for {}/{}: layer_present={} table_in_buffer={} mem_rows={}",
            self.project_id,
            self.table_name,
            self.buffered_layer.is_some(),
            has_uncommitted,
            mem_rows
        );

        // Order the Delta leg AFTER any airborne flush commit of this table:
        // a commit snapshotted before the mem leg above lands PRE-DML row
        // values, and only a Delta merge/delete that runs after it can
        // correct them (critical for DELETE — the removed rows have nothing
        // left in memory to supersede the stale copies). Also makes the
        // has_committed check below see a table whose first-ever commit was
        // airborne when this statement arrived.
        if let Some(layer) = self.buffered_layer {
            let started = Instant::now();
            layer.await_inflight_flushes(self.project_id, self.table_name).await;
            log_slow_phase("await_inflight_flush", self.table_name, self.project_id, started, None);
        }

        // Check if there's committed data: either in custom project tables or unified tables.
        // The unified-tables lookup intentionally uses table_name only (no project_id):
        // unified tables are shared across all default projects, so a hit here means "some
        // project has committed data in this table", not "this project has". The delta_op's
        // predicate already includes `project_id = $self.project_id`, so we never delete or
        // update another project's rows — at worst we issue a Delta scan that matches nothing.
        let has_committed = {
            let custom_tables = self.database.custom_project_tables().read().await;
            let unified_tables = self.database.unified_tables().read().await;
            custom_tables.contains_key(&(self.project_id.to_string(), self.table_name.to_string())) || unified_tables.contains_key(self.table_name)
        };

        let delta_rows =
            match has_committed.then(|| delta_leg_predicate(self.buffered_layer, self.table_name, self.project_id, self.predicate.as_ref())).flatten() {
                Some(delta_pred) => delta_op(delta_pred).await?,
                None => 0,
            };

        Ok(mem_rows + delta_rows)
    }
}

/// Debug-format `value` into at most ~`limit` bytes, discarding output past
/// the cap instead of materializing it first — a 40k-row IN-list predicate
/// debug-prints to ~50MB, and building that string on the DV-merge failure
/// path (i.e. during overload) broke OTLP export for the whole process
/// (2026-07-26 incident).
fn fmt_capped(value: &dyn std::fmt::Debug, limit: usize) -> String {
    struct Trunc {
        buf: String,
        limit: usize,
        truncated: bool,
    }
    impl std::fmt::Write for Trunc {
        fn write_str(&mut self, s: &str) -> std::fmt::Result {
            let room = self.limit.saturating_sub(self.buf.len()).min(s.len());
            let n = (0..=room).rev().find(|&n| s.is_char_boundary(n)).unwrap_or(0);
            self.buf.push_str(&s[..n]);
            self.truncated |= n < s.len();
            Ok(())
        }
    }
    let mut w = Trunc { buf: String::with_capacity(limit.min(4096)), limit, truncated: false };
    let _ = std::fmt::write(&mut w, format_args!("{value:?}"));
    if w.truncated {
        w.buf.push_str("… [truncated]");
    }
    w.buf
}

/// Watermark-clamp a Delta leg's predicate (see
/// `dml::clamp_to_watermark`): rows above the flush watermark are
/// buffer-only, so the mem leg already updated them and the flush persists
/// their post-DML values. `None` means the whole window is unflushed and the
/// Delta leg can be skipped outright.
///
/// Called only from `DmlContext::execute`, after its `await_inflight_flushes`:
/// a flush snapshotted before the mem leg commits PRE-DML values that only
/// the Delta leg can correct, and that flush raises the watermark before
/// committing — so only a post-await watermark is guaranteed to sit
/// at-or-above every row whose Delta copy might be stale. Clamping with an
/// earlier watermark would cut exactly those rows out of the merge and lose
/// the update.
fn delta_leg_predicate(buffered_layer: Option<&Arc<BufferedWriteLayer>>, table_name: &str, project_id: &str, predicate: Option<&Expr>) -> Option<Option<Expr>> {
    let time_col = crate::dml::table_time_column(table_name);
    let base: Option<Expr> = match buffered_layer {
        None => predicate.cloned(),
        Some(layer) => {
            let watermark = layer.delta_flushed_watermark(project_id, table_name);
            match crate::dml::clamp_to_watermark(predicate, time_col, watermark) {
                crate::dml::WatermarkClamp::Keep(p) => p,
                crate::dml::WatermarkClamp::SkipDelta => {
                    crate::observability::record_dml_delta_leg_skipped();
                    debug!("DML delta leg skipped for {project_id}/{table_name}: time window entirely above flush watermark");
                    return None;
                }
            }
        }
    };
    // Derive `date`-partition bounds from the (watermark-clamped) time-column
    // predicate so the Delta leg prunes files instead of scanning every
    // partition — only for tables actually partitioned by `date`.
    let partitions_by_date = crate::schema::get_schema(table_name).is_some_and(|s| s.partitions.iter().any(|p| p == "date"));
    Some(match base {
        Some(p) if partitions_by_date => {
            let augmented = crate::read::optimizers::time_range_partition_pruner::with_date_partition_filters(p, time_col);
            // Diagnostic for the 2026-07-20 residual full-scans. One compact line
            // per merge: `days` is the span the derived `date` bounds cover.
            // Correlate with ScanMetadataCompleted.predicate_filtered:
            //   days=0 (empty)  → shape gap, timestamp→date derivation missed it;
            //   days large      → legit wide time-window, nothing to prune.
            let bounds = crate::read::optimizers::time_range_partition_pruner::extract_date_bounds(&augmented);
            let day_span = match (bounds.iter().map(|(_, d)| *d).min(), bounds.iter().map(|(_, d)| *d).max()) {
                (Some(lo), Some(hi)) => hi - lo + 1,
                _ => 0,
            };
            info!(project_id, table_name, date_bounds = bounds.len(), days = day_span, "DML delta-leg date-partition bounds");
            Some(augmented)
        }
        other => other,
    })
}

/// Aliases the merge-on-read plan gives its two sides. The source alias matches
/// `perform_delta_merge_update`'s so [`requalify_for_merge`] serves both paths;
/// the target keeps the TABLE NAME because the statement's predicate and
/// assignments were planned against it and may still carry that qualifier.
const MOR_SOURCE: &str = "source";

/// Merge-on-read DML (`docs/plans/2026-08-01-merge-on-read-dml.md`). On a
/// `version_append` table an UPDATE/DELETE rewrites NOTHING: it resolves its
/// target rows through the normal routed read path — mem ∪ hot ∪ delta, already
/// version-collapsed by `DedupExec` — evaluates the `SET` expressions against
/// them, and appends the results as new row versions.
/// [`BufferedWriteLayer::insert`] stamps a fresh monotonic `dedup_tiebreak`
/// (`insert_coerce::stamp_version`) on the way through, so the appended version
/// outranks every older copy at read time. No Delta MERGE, no deletion vector,
/// no OCC retry, and — because nothing existing changes — no hot-tier
/// invalidation.
///
/// DELETE appends the same FULL row with the schema's `tombstone_column` set,
/// not a key-only stub: the row has just been read anyway, a stub could not
/// satisfy the table's NOT NULL columns, and the read side keys off the marker
/// alone.
///
/// STREAMED, not collected. Rows are appended in scan-sized chunks, so an
/// UPDATE over a wide window costs one batch of memory rather than the whole
/// match set — the 2026-07-04 `update_with_source` OOM shape. The trade is
/// statement atomicity: a failure partway leaves the versions already appended
/// in place. That is sound under merge-on-read (each is a COMPLETE row version,
/// never a half-written row) and the client sees the error and retries, which
/// re-appends them idempotently.
#[allow(clippy::too_many_arguments)]
async fn perform_version_append(
    database: &Arc<Database>, layer: Option<&Arc<BufferedWriteLayer>>, table_name: &str, project_id: &str, predicate: Option<Expr>,
    assignments: &[(String, Expr)], source: Option<&UpdateSource>, tombstone: bool, session: &Arc<dyn Session>,
) -> Result<u64> {
    use datafusion::{
        datasource::{MemTable, provider_as_source},
        logical_expr::{LogicalPlanBuilder, col, lit},
    };

    let schema =
        crate::schema::get_schema(table_name).ok_or_else(|| DataFusionError::Execution(format!("merge-on-read: no registered schema for {table_name}")))?;
    let tombstone_col = schema.tombstone_column.clone();
    if tombstone && tombstone_col.is_none() {
        // `version_append` is documented to require all three columns; a DELETE
        // with nowhere to write the marker would silently delete nothing.
        return Err(DataFusionError::Execution(format!("merge-on-read: {table_name} sets version_append but declares no tombstone_column")));
    }
    let table_schema = schema.schema_ref();

    // The routing provider IS the logical table: it unions MemBuffer, the hot
    // tier and Delta, prunes by the predicate's project/time bounds, and runs
    // DedupExec — so the rows we read are already the current versions.
    let provider =
        Arc::new(crate::database::ProjectRoutingTable::new(project_id.to_string(), database.clone(), table_schema.clone(), None, table_name.to_string()));
    // `project_id` is STRIPPED from the DML predicate (it is routing
    // information, consumed by `extract_dml_info`), so it must be put back as a
    // row filter here. Routing alone is not tenant isolation: every default
    // project shares ONE unified Delta table, so without this conjunct an
    // UPDATE scoped to one tenant rewrites the matching rows of every tenant in
    // that table. It also prunes, which is why the in-place Delta leg
    // re-augments its own predicate the same way.
    let tenant = Expr::Column(Column::from_name("project_id")).eq(lit(project_id));
    let source_cols: std::collections::HashSet<String> = source.map(|s| s.schema.fields().iter().map(|f| f.name().clone()).collect()).unwrap_or_default();
    // The predicate splits at the join: a conjunct referencing any source
    // column (the enrichment guard `NOT (hashes @> ARRAY[u.tag])`) can only be
    // evaluated on the joined row, while target-only conjuncts (tenant, time
    // bounds) belong on the scan where they prune. Dropping the source-side
    // conjuncts instead of deferring them un-guards the UPDATE — every pass
    // then re-appends versions for every matched row (prod 2026-08-03).
    let (pre_join, post_join): (Vec<Expr>, Vec<Expr>) = predicate
        .as_ref()
        .map(|p| split_conjunction(p).into_iter().cloned().partition(|c| !c.column_refs().iter().any(|col| source_cols.contains(&col.name))))
        .unwrap_or_default();
    let filter = pre_join.into_iter().fold(tenant, Expr::and);
    let mut builder = LogicalPlanBuilder::scan(table_name, provider_as_source(provider), None)?.filter(filter)?;

    if let Some(src) = source {
        // The join keys never prune the scan on their own — the equi-join
        // matches AFTER every row in the window is decoded. Pushing the
        // source's key values down as IN-lists engages the parquet bloom
        // filters on exactly these columns and shrinks the scan from "whole
        // window, all columns" to the matched pages. Sound: join-key target
        // columns are identity columns, never version-mutable. Skipped above
        // a cap so a giant source can't build a pathological expression.
        if src.batch.num_rows() <= MOR_KEY_PUSHDOWN_ROWS {
            for (t, s) in &src.join_keys {
                let idx = src.schema.index_of(s)?;
                let arr = src.batch.column(idx);
                let mut vals = (0..arr.len()).map(|i| datafusion::common::ScalarValue::try_from_array(arr, i)).collect::<Result<Vec<_>>>()?;
                vals.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                vals.dedup();
                builder = builder.filter(Expr::Column(Column::from_name(t)).in_list(vals.into_iter().map(lit).collect(), false))?;
            }
        }
        let mem = MemTable::try_new(src.schema.clone(), vec![vec![src.batch.clone()]])?;
        let src_plan = LogicalPlanBuilder::scan(MOR_SOURCE, provider_as_source(Arc::new(mem)), None)?.build()?;
        let on: Vec<Expr> = src
            .join_keys
            .iter()
            .map(|(t, s)| Expr::Column(Column::new(Some(table_name.to_string()), t)).eq(Expr::Column(Column::new(Some(MOR_SOURCE.to_string()), s))))
            .collect();
        builder = builder.join_on(src_plan, JoinType::Inner, on)?;
        if let Some(guard) = post_join.into_iter().reduce(Expr::and) {
            builder = builder.filter(requalify_for_merge(guard, &source_cols, MOR_SOURCE, table_name)?)?;
        }
    }

    // Full-row versions: every column is carried forward, with the assignments
    // (and the tombstone marker) substituted in place. The version stamp is
    // deliberately NOT set here — `insert` owns it.
    let exprs = table_schema
        .fields()
        .iter()
        .map(|f| {
            let name = f.name();
            if tombstone && tombstone_col.as_deref() == Some(name.as_str()) {
                return Ok(lit(true).alias(name));
            }
            match assignments.iter().find(|(c, _)| c == name) {
                Some((_, e)) => Ok(requalify_for_merge(e.clone(), &source_cols, MOR_SOURCE, table_name)?.alias(name)),
                None => Ok(col(Column::new(Some(table_name.to_string()), name)).alias(name)),
            }
        })
        .collect::<Result<Vec<_>>>()?;
    let plan = builder.project(exprs)?.build()?;

    // `session` is already `delta_session_from(...)`: default analyzer rules
    // only, so Variant columns round-trip as raw Structs instead of being
    // wrapped in `variant_to_json` for the wire and re-parsed on the way back.
    let physical = session.create_physical_plan(&plan).await?;
    let mut stream = datafusion::physical_plan::execute_stream(physical, session.task_ctx())?;
    let mut rows = 0u64;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        if batch.num_rows() == 0 {
            continue;
        }
        rows += batch.num_rows() as u64;
        // Marked before the write, and through the SAME funnel every other
        // write uses — `insert_records_batch` is what stamps the version
        // (`insert_coerce::stamp_version`) and routes to the buffered layer
        // when there is one. Appending via `BufferedWriteLayer::insert`
        // directly skips the stamp, and every version of a row then ties on the
        // tiebreak, which keep-greatest resolves ARBITRARILY: five successive
        // updates read back as the first one.
        let batches = vec![batch];
        if let Some(l) = layer {
            l.mark_version_buckets(project_id, table_name, &batches);
        }
        database
            .insert_records_batch_bounded(project_id, table_name, batches, false, None, false)
            .await
            .map_err(|e| DataFusionError::Execution(format!("merge-on-read append failed for {project_id}/{table_name}: {e}")))?;
    }
    debug!(project_id, table_name, rows, tombstone, "merge-on-read version append");
    Ok(rows)
}

/// Merge-on-read is a per-table property of the SCHEMA alone. It must not also
/// depend on a buffered layer being attached: a table that appends versions on
/// one deployment and mutates in place on another would resolve versions
/// differently for the same data.
fn is_version_append(table_name: &str) -> bool {
    crate::schema::get_schema(table_name).is_some_and(|s| s.version_append)
}

#[allow(clippy::too_many_arguments)]
async fn perform_update_with_buffer(
    database: &Arc<Database>, buffered_layer: Option<&Arc<BufferedWriteLayer>>, table_name: &str, project_id: &str, predicate: Option<Expr>,
    assignments: Vec<(String, Expr)>, source: Option<UpdateSource>, session: Arc<dyn Session>, span: &tracing::Span,
) -> Result<u64> {
    // Merge-on-read tables take neither leg: one append supersedes the row
    // wherever it lives, so there is no mem/Delta split to coordinate.
    //
    // Same-key source rows still need SUCCESSIVE rounds, for a reason specific
    // to versioning: `stamp_version` issues ONE stamp per batch, so two source
    // rows for the same key applied in a single append become two versions
    // sharing a tiebreak — and keep-greatest resolves a tie ARBITRARILY, so the
    // last write silently loses (the enrichment worker's same-key multi-tag
    // pattern, prod 2026-07-19). One round per key occurrence gives each
    // version its own strictly greater stamp, which is what makes
    // last-write-wins hold.
    if is_version_append(table_name) {
        let append_span = tracing::trace_span!(parent: span, "mor.update");
        let rounds = match source {
            Some(src) => crate::dml::split_source_rounds(src)?.into_iter().map(Some).collect(),
            None => vec![None],
        };
        let mut total = 0u64;
        for round in rounds {
            // `split_source_rounds` preserves successive applications for
            // duplicate keys. Keys are disjoint within each round, so bounded
            // sequential chunks preserve semantics and keep pushdown enabled.
            let chunks = round.map_or_else(|| vec![None], |source| bounded_mor_source_chunks(source).into_iter().map(Some).collect());
            for chunk in chunks {
                total +=
                    perform_version_append(database, buffered_layer, table_name, project_id, predicate.clone(), &assignments, chunk.as_ref(), false, &session)
                        .instrument(append_span.clone())
                        .await?;
            }
        }
        return Ok(total);
    }

    // `UPDATE ... FROM` path: MemBuffer takes the join via update_with_source,
    // Delta path uses MergeBuilder via perform_delta_merge_update — either
    // synchronously or deferred through the coalescer when enabled.
    if let Some(src) = source {
        let coalescer = database.dml_coalescer().cloned();
        // `async move` must not take the Vec itself — the mem closure borrows it.
        let assignments = &assignments;
        // Same-key source rows must be applied in successive rounds: one MERGE
        // can't match a target row against two source rows, and the MemBuffer
        // hash-join would keep only one — silently dropping the rest.
        let mut total = 0u64;
        for round in crate::dml::split_source_rounds(src)? {
            let src_for_mem = round.clone();
            let src_for_delta = round;
            let (coalescer, session) = (coalescer.clone(), session.clone());
            let update_span = tracing::trace_span!(parent: span, "delta.merge_update");
            total += DmlContext { database, buffered_layer, table_name, project_id, predicate: predicate.clone() }
                .execute(
                    |layer, pred| layer.update_with_source(project_id, table_name, pred, assignments, &src_for_mem),
                    |delta_pred| async move {
                        if let Some(coalescer) = coalescer {
                            coalescer.enqueue(project_id, table_name, delta_pred.as_ref(), assignments, &src_for_delta, session);
                            return Ok(0);
                        }
                        perform_delta_merge_update(database, table_name, project_id, delta_pred, assignments.clone(), src_for_delta, session)
                            .instrument(update_span)
                            .await
                    },
                )
                .await?;
        }
        return Ok(total);
    }

    let update_span = tracing::trace_span!(parent: span, "delta.update");
    // The delta closure body is only constructed (and assignments only
    // cloned) when there is committed data. Mem path borrows `assignments`.
    DmlContext { database, buffered_layer, table_name, project_id, predicate }
        .execute(
            |layer, pred| layer.update(project_id, table_name, pred, &assignments),
            |delta_pred| perform_delta_update(database, table_name, project_id, delta_pred, assignments.clone(), session).instrument(update_span),
        )
        .await
}

fn bounded_mor_source_chunks(source: UpdateSource) -> Vec<UpdateSource> {
    let rows = source.batch.num_rows();
    if rows <= MOR_KEY_PUSHDOWN_ROWS {
        return vec![source];
    }
    info!(
        event = "dml.mor_source_chunked",
        source_rows = rows,
        chunk_rows = MOR_KEY_PUSHDOWN_ROWS,
        chunks = rows.div_ceil(MOR_KEY_PUSHDOWN_ROWS),
        "bounded merge-on-read source to preserve complete-key pushdown"
    );
    (0..rows)
        .step_by(MOR_KEY_PUSHDOWN_ROWS)
        .map(|offset| UpdateSource {
            batch: source.batch.slice(offset, (rows - offset).min(MOR_KEY_PUSHDOWN_ROWS)),
            schema: source.schema.clone(),
            join_keys: source.join_keys.clone(),
        })
        .collect()
}

async fn perform_delete_with_buffer(
    database: &Arc<Database>, buffered_layer: Option<&Arc<BufferedWriteLayer>>, table_name: &str, project_id: &str, predicate: Option<Expr>,
    session: Arc<dyn Session>, span: &tracing::Span,
) -> Result<u64> {
    // Merge-on-read: a DELETE appends a tombstone version instead of planning a
    // Delta delete + deletion vector (see `perform_version_append`).
    if is_version_append(table_name) {
        let append_span = tracing::trace_span!(parent: span, "mor.delete");
        return perform_version_append(database, buffered_layer, table_name, project_id, predicate, &[], None, true, &session).instrument(append_span).await;
    }

    let delete_span = tracing::trace_span!(parent: span, "delta.delete");
    // The clamp applies to deletes too: rows above the flush watermark were
    // removed from the buffer by the mem leg and will never flush, so Delta
    // has nothing to do.
    DmlContext { database, buffered_layer, table_name, project_id, predicate }
        .execute(
            |layer, pred| layer.delete(project_id, table_name, pred),
            |delta_pred| perform_delta_delete(database, table_name, project_id, delta_pred, session).instrument(delete_span),
        )
        .await
}

/// Perform Delta UPDATE operation
#[instrument(
    name = "delta.perform_update",
    skip_all,
    fields(
        table.name = %table_name,
        project_id = %project_id,
        has_predicate = predicate.is_some(),
        assignments_count = assignments.len(),
        rows.updated = Empty,
    )
)]
pub async fn perform_delta_update(
    database: &Database, table_name: &str, project_id: &str, predicate: Option<Expr>, assignments: Vec<(String, Expr)>, session: Arc<dyn Session>,
) -> Result<u64> {
    info!("Performing Delta UPDATE on table {} for project {}", table_name, project_id);

    let span = tracing::Span::current();
    // Clone captures per attempt: the operation may rerun after an OCC conflict.
    // zstd tier for the rewrite: without it the UpdateBuilder writes SNAPPY.
    let writer_properties = database.dml_writer_properties(table_name, false);
    let use_dv = database.config().maintenance.timefusion_use_deletion_vectors;
    perform_delta_operation(database, table_name, project_id, |delta_table| {
        let (predicate, assignments, session) = (predicate.clone(), assignments.clone(), session.clone());
        let writer_properties = writer_properties.clone();
        async move {
            let builder = delta_table.update().with_session_state(session).with_writer_properties(writer_properties).with_deletion_vectors(use_dv);
            let builder = match predicate {
                Some(pred) => builder.with_predicate(convert_expr_to_delta(&pred)?),
                None => builder,
            };
            let builder = assignments
                .into_iter()
                .try_fold(builder, |b, (column, value_expr)| -> Result<_> { Ok(b.with_update(column, convert_expr_to_delta(&value_expr)?)) })?;

            builder.await.map(|(table, metrics)| (table, metrics.num_updated_rows as u64)).map_err(exec_err("Failed to execute Delta UPDATE"))
        }
    })
    .await
    .inspect(|rows| {
        span.record("rows.updated", rows);
    })
}

/// Perform Delta DELETE operation
#[instrument(
    name = "delta.perform_delete",
    skip_all,
    fields(
        table.name = %table_name,
        project_id = %project_id,
        has_predicate = predicate.is_some(),
        rows.deleted = Empty,
    )
)]
pub async fn perform_delta_delete(database: &Database, table_name: &str, project_id: &str, predicate: Option<Expr>, session: Arc<dyn Session>) -> Result<u64> {
    info!("Performing Delta DELETE on table {} for project {}", table_name, project_id);

    let span = tracing::Span::current();
    // zstd tier for the rewrite: without it the DeleteBuilder writes SNAPPY.
    let writer_properties = database.dml_writer_properties(table_name, false);
    let use_dv = database.config().maintenance.timefusion_use_deletion_vectors;
    perform_delta_operation(database, table_name, project_id, |delta_table| {
        let (predicate, session) = (predicate.clone(), session.clone());
        let writer_properties = writer_properties.clone();
        async move {
            let builder = delta_table.delete().with_session_state(session).with_writer_properties(writer_properties).with_deletion_vectors(use_dv);
            let builder = match predicate {
                Some(pred) => builder.with_predicate(convert_expr_to_delta(&pred)?),
                None => builder,
            };

            builder.await.map(|(table, metrics)| (table, metrics.num_deleted_rows.unwrap_or(0) as u64)).map_err(exec_err("Failed to execute Delta DELETE"))
        }
    })
    .await
    .inspect(|rows| {
        span.record("rows.deleted", rows);
    })
}

/// Max attempts for a DML Delta operation that loses an OCC race (e.g. a flush
/// commit landing mid-merge). Backoff mirrors the flush/optimize paths.
const DML_MAX_ATTEMPTS: usize = 4;

/// Common Delta operation logic. Runs the operation on a snapshot clone with
/// NO table lock held — the exclusive lock used to be held across
/// update_state → merge → swap, convoying every reader and insert commit
/// behind each multi-second UPDATE. Like the flush path, we commit
/// optimistically and take the write lock only for a version-guarded swap;
/// OCC conflicts (concurrent flush commit) are retried on a fresh snapshot.
async fn perform_delta_operation<F, Fut>(database: &Database, table_name: &str, project_id: &str, operation: F) -> Result<u64>
where
    F: Fn(deltalake::DeltaTable) -> Fut,
    Fut: std::future::Future<Output = Result<(deltalake::DeltaTable, u64)>>,
{
    // Use resolve_table which routes to unified or custom table based on storage config
    let table_lock = database
        .resolve_table(project_id, table_name)
        .await
        .map_err(|e| DataFusionError::Execution(format!("Table not found: {} for project {}: {}", table_name, project_id, e)))?;

    let dml_lock = database.dml_lock(project_id, table_name).await;
    let _dml_guard = dml_lock.lock().await;

    let mut attempt = 0;
    loop {
        // Refresh via clone-update-swap (write lock held for the swap only).
        crate::database::refresh_table_snapshot(&table_lock, database.incremental_snapshot()).await.map_err(exec_err("Failed to refresh table state"))?;
        let snapshot = { table_lock.read().await.clone() };
        let pre_version = snapshot.version();
        match operation(snapshot).await {
            Ok((new_table, rows_affected)) => {
                if attempt > 0 {
                    crate::observability::record_dml_retry_success();
                }
                // A merge matching zero rows commits nothing — same table back,
                // version unchanged: skip persist + swap entirely.
                if new_table.version() > pre_version {
                    // Persist so boot replays only post-commit log, same as the
                    // insert/maintenance paths.
                    database.persist_snapshot(&new_table);
                    let mut guard = table_lock.write().await;
                    if new_table.version() > guard.version() {
                        *guard = new_table;
                    }
                }
                return Ok(rows_affected);
            }
            Err(e) if attempt + 1 < DML_MAX_ATTEMPTS && crate::database::is_occ_conflict_err(&e.to_string()) => {
                attempt += 1;
                crate::observability::record_dml_conflict();
                warn!("DML delta op conflict on {}/{}, retrying ({}/{}): {}", project_id, table_name, attempt, DML_MAX_ATTEMPTS, e);
                tokio::time::sleep(crate::database::occ_backoff(attempt)).await;
            }
            Err(e) => {
                if attempt + 1 == DML_MAX_ATTEMPTS && crate::database::is_occ_conflict_err(&e.to_string()) {
                    crate::observability::record_dml_retry_exhausted();
                }
                return Err(e);
            }
        }
    }
}

/// Convert DataFusion Expr to Delta-compatible format.
/// Recursively walks the expression tree and strips table qualifiers from Column references
/// (e.g., `table.column` becomes just `column`). All other expression types (literals,
/// binary ops, functions, etc.) pass through unchanged, preserving types like Utf8View.
fn convert_expr_to_delta(expr: &Expr) -> Result<Expr> {
    expr.clone()
        .transform(|e| match &e {
            Expr::Column(col) => Ok(Transformed::yes(Expr::Column(Column::from_name(&col.name)))),
            _ => Ok(Transformed::no(e)),
        })
        .map(|t| t.data)
        .map_err(exec_err("Failed to convert expression"))
}

/// Rewrite column references in `expr` so they address `MergeBuilder`'s
/// `source` / `target` aliases instead of whatever aliases the user wrote
/// in the SQL (e.g. `UPDATE otel_logs_and_spans o ... FROM (...) AS u`).
///
/// Rule:
/// - Cols already qualified `source.x` or `target.x` pass through unchanged.
/// - Cols with any other qualifier (or no qualifier) whose name appears in
///   the source schema are rewritten to `source.x`.
/// - All other cols become bare `x`, leaving `MergeBuilder` to resolve them
///   against the target (target columns are unambiguous since source columns
///   were already routed above).
fn requalify_for_merge(expr: Expr, source_cols: &HashSet<String>, source_alias: &str, target_alias: &str) -> Result<Expr> {
    expr.transform(|e| match &e {
        Expr::Column(c) => Ok(match c.relation.as_ref() {
            Some(r) if r.table() == source_alias || r.table() == target_alias => Transformed::no(e),
            _ if source_cols.contains(&c.name) => Transformed::yes(Expr::Column(Column::new(Some(source_alias.to_string()), c.name.clone()))),
            _ => Transformed::yes(Expr::Column(Column::from_name(c.name.clone()))),
        }),
        _ => Ok(Transformed::no(e)),
    })
    .map(|t| t.data)
    .map_err(exec_err("Failed to requalify for merge"))
}

/// Keep only the conjuncts of `predicate` that reference none of `strip_cols`,
/// for use as the DV merge's file-pruning `target_predicate`. `strip_cols` holds
/// the source columns AND the equi-key TARGET columns, because neither class can
/// prune target files and both break the fork's file-skipping scan:
///   - equi-key equalities (`o.context___span_id = u.span_id`) and `NOT (... @> u.tag)`
///     reference SOURCE columns the file scan has no schema for;
///   - the optimizer inserts `IsNotNull(o.context___span_id)` null-rejection on
///     the join keys — TARGET-only, so it survived a source-only strip, but the
///     high-cardinality key isn't in the stats schema, so the file-skipping scan
///     fails to resolve it ("No field named otel_logs_and_spans.context___span_id",
///     dropping ~1000 rows/drop after 3 retries — prod 2026-07-20).
///
/// The join_predicate still enforces the equi-keys and their non-null-ness, so
/// dropping these from the file-pruning predicate is sound (pruning is only an
/// optimization). Columns matched by NAME, per [`requalify_for_merge`]'s convention.
fn strip_source_conjuncts(predicate: &Expr, strip_cols: &HashSet<String>) -> Option<Expr> {
    split_conjunction(predicate).into_iter().filter(|c| !c.column_refs().iter().any(|col| strip_cols.contains(&col.name))).cloned().reduce(Expr::and)
}

/// Columns to strip from a DV merge's file-pruning predicate: the source columns
/// plus the equi-key TARGET columns, EXCEPT partition columns — those resolve from
/// partition values (no stats-schema gap), and stripping them would drop the
/// coalescer fold's `project_id IN (...)` filter, un-pruning the merge scan back to
/// every tenant's files in the window.
fn dv_strip_cols(source_cols: &HashSet<String>, join_keys: &[(String, String)], partition_cols: &[String]) -> HashSet<String> {
    source_cols.iter().cloned().chain(join_keys.iter().map(|(t, _)| t.clone())).filter(|c| !partition_cols.contains(c)).collect()
}

/// Build the join predicate that drives the merge: a conjunction of
/// `target.k_i = source.k_i` clauses for each equi-key pair, AND-ed with the
/// optional user predicate (which gets routed through [`requalify_for_merge`]
/// so the user's source/target aliases resolve under `MergeBuilder`'s).
fn build_join_predicate(
    target_alias: &str, source_alias: &str, join_keys: &[(String, String)], extra: Option<&Expr>, source_cols: &HashSet<String>,
) -> Result<Expr> {
    use datafusion::prelude::col;
    let keys = join_keys
        .iter()
        .map(|(t, s)| col(format!("{target_alias}.{t}")).eq(col(format!("{source_alias}.{s}"))))
        .reduce(Expr::and)
        .ok_or_else(|| DataFusionError::Plan("UPDATE ... FROM requires at least one join key".to_string()))?;
    Ok(match extra {
        Some(p) => keys.and(requalify_for_merge(p.clone(), source_cols, source_alias, target_alias)?),
        None => keys,
    })
}

/// Perform Delta UPDATE ... FROM via [`deltalake::operations::merge::MergeBuilder`]
/// with only a `WHEN MATCHED THEN UPDATE` clause. The materialized
/// `UpdateSource.batch` becomes the merge source DataFrame; `join_keys` lower
/// to a conjunctive equi-join predicate; the user's WHERE predicate is AND-ed
/// in after re-qualification under the `target` alias.
#[instrument(
    name = "delta.perform_merge_update",
    skip_all,
    fields(
        table.name = %table_name,
        project_id = %project_id,
        has_predicate = predicate.is_some(),
        assignments_count = assignments.len(),
        source_rows = source.batch.num_rows(),
        rows.updated = Empty,
    )
)]
pub async fn perform_delta_merge_update(
    database: &Database, table_name: &str, project_id: &str, predicate: Option<Expr>, assignments: Vec<(String, Expr)>, source: UpdateSource,
    session: Arc<dyn Session>,
) -> Result<u64> {
    info!("Performing Delta MERGE-UPDATE on table {} for project {} ({} source rows)", table_name, project_id, source.batch.num_rows());

    // Gate concurrent merges: each scans the time-windowed target to hash-join
    // keys; ungated bursts of per-project drains stampede a CPU-throttled box and
    // starve read queries (prod 2026-07-19). Held across the OCC retry loop.
    let _merge_permit = database.dml_merge_sem().acquire().await.map_err(exec_err("dml merge semaphore closed"))?;

    let span = tracing::Span::current();
    let source_cols: HashSet<String> = source.schema.fields().iter().map(|f| f.name().clone()).collect();

    // Re-qualify assignments before moving into the closure so the user's
    // source/target aliases address `MergeBuilder`'s `source` / `target`
    // (the predicate is re-qualified inside `build_join_predicate`).
    let assignments = assignments
        .into_iter()
        .map(|(col_name, expr)| Ok((col_name, requalify_for_merge(expr, &source_cols, "source", "target")?)))
        .collect::<Result<Vec<_>>>()?;

    // Our zstd tier for the rewrite: without it the MergeBuilder writes SNAPPY.
    let writer_properties = database.dml_writer_properties(table_name, true);
    let use_dv = database.config().maintenance.timefusion_use_deletion_vectors;

    perform_delta_operation(database, table_name, project_id, |delta_table| {
        // RecordBatch clones are Arc-backed (cheap); needed since the
        // operation may rerun after an OCC conflict.
        let (source_batch, source_schema, join_keys, source_cols) =
            (source.batch.clone(), source.schema.clone(), source.join_keys.clone(), source_cols.clone());
        let (predicate, assignments, session) = (predicate.clone(), assignments.clone(), session.clone());
        // Cloned per attempt (the closure reruns on OCC conflict).
        let writer_properties = writer_properties.clone();
        async move {
            let join_pred = build_join_predicate("target", "source", &join_keys, predicate.as_ref(), &source_cols)?;

            // Merge-on-read: append only the updated matched rows and mask the
            // originals with a DV instead of rewriting whole matched files (the
            // enrichment-MERGE OOM hotspot). Assignments/join_pred already address
            // the target/source aliases the DV op scans under.
            if use_dv {
                use deltalake::operations::merge_dv::{MergeDvUpdate, merge_update_with_deletion_vectors};
                // The equi-key target cols carry the optimizer's IsNotNull(join-key)
                // conjuncts the fork's stats scan can't resolve — prod drop bug.
                let partition_cols = crate::schema::get_schema(table_name).map(|s| s.partitions.clone()).unwrap_or_default();
                let strip_cols = dv_strip_cols(&source_cols, &join_keys, &partition_cols);
                let target_predicate = predicate.as_ref().and_then(|p| strip_source_conjuncts(p, &strip_cols));
                let equi_keys = if database.config().maintenance.timefusion_dml_merge_key_prune { join_keys } else { vec![] };
                return merge_update_with_deletion_vectors(
                    &delta_table,
                    session.as_ref(),
                    MergeDvUpdate {
                        source_batches: vec![source_batch],
                        source_schema,
                        target_predicate: target_predicate.clone(),
                        join_predicate: join_pred.clone(),
                        equi_keys: equi_keys.clone(),
                        updates: assignments,
                        target_alias: "target".to_string(),
                        source_alias: "source".to_string(),
                        writer_properties: Some(writer_properties),
                        // Sort the appended rows by the table's sort keys so the
                        // file's footer can declare them. Unsorted, ONE such
                        // file disables the reader's all-or-nothing footer
                        // ordering for the whole partition — measured on prod
                        // 2026-08-01, a 1-row DML file cost a tenant its top-N
                        // pushdown while its other 24 files were all sorted.
                        // Enrichment writes these continuously, so compaction
                        // cannot sweep them faster than they arrive.
                        append_sort_by: crate::schema::get_schema(table_name)
                            .map(|s| s.sorting_columns.iter().map(|c| (c.name.clone(), c.descending, c.nulls_first)).collect())
                            .unwrap_or_default(),
                        // Sound here and only here: every perform_delta_merge_update
                        // caller ran the mem leg first (DmlContext::execute), so
                        // concurrently flushed rows already carry post-DML values.
                        tolerate_concurrent_appends: database.config().maintenance.timefusion_dml_merge_append_rebase,
                    },
                )
                .await
                .map_err(|e| {
                    // Diagnostic for the prod "No field named …context___span_id" DV
                    // merge drops (2026-07-20): local repro couldn't trigger it, so
                    // capture the exact predicate shape from prod on failure.
                    // Truncated: a large source (40k-row IN lists) debug-printed
                    // here produced 50MB span events that broke OTLP export for
                    // the whole process during the 2026-07-26 incident.
                    warn!(
                        error = %e,
                        join_predicate = %fmt_capped(&join_pred, 4096),
                        target_predicate = %fmt_capped(&target_predicate, 4096),
                        equi_keys = ?equi_keys,
                        "DV MERGE UPDATE failed — predicate diagnostic"
                    );
                    exec_err("Failed to execute Delta MERGE UPDATE (deletion vectors)")(e)
                });
            }

            // Wrap the materialized source RecordBatch as a DataFrame. The
            // throwaway SessionContext only provides the DataFrame builder; merge
            // execution uses the session passed via `with_session_state`.
            let source_df =
                datafusion::prelude::SessionContext::new().read_batch(source_batch).map_err(exec_err("Failed to wrap UPDATE FROM source as DataFrame"))?;

            let (new_table, metrics) = delta_table
                .merge(source_df, join_pred)
                .with_source_alias("source")
                .with_target_alias("target")
                .with_session_state(session)
                .with_writer_properties(writer_properties)
                .with_safe_cast(true)
                .when_matched_update(|u| assignments.iter().fold(u, |u, (col_name, value_expr)| u.update(col_name.clone(), value_expr.clone())))
                .map_err(exec_err("when_matched_update failed"))?
                .await
                .map_err(exec_err("Failed to execute Delta MERGE UPDATE"))?;
            Ok((new_table, metrics.num_target_rows_updated as u64))
        }
    })
    .await
    .inspect(|rows| {
        span.record("rows.updated", rows);
    })
    // Diagnostic for the still-unreproduced "No field named ...context___span_id"
    // schema failures (prod 2026-07-19): dump the exact predicates + keys so the
    // next occurrence pins the plan shape that leaks a column into the scan.
    .inspect_err(|e| warn!(target: "dml", "Delta MERGE-UPDATE failed for {project_id}/{table_name} keys={:?}: {e}", source.join_keys))
}

#[cfg(test)]
mod session_tests {
    use std::sync::Arc;

    use datafusion::arrow::{array::StringArray, record_batch::RecordBatch};

    /// The DML session must decode the wide otel schema at the same batch size
    /// as every other session that reads it.
    ///
    /// A MERGE-UPDATE re-reads and rewrites WHOLE wide rows, so it is the most
    /// decode-expensive read in the system, and decode buffers cost
    /// `batch_size × row width` with none of it pool-accounted. The query and
    /// maintenance sessions were cut to 2048 by the 2026-08-07 heap work; this
    /// one kept DataFusion's 8192 default, and a dump taken mid-burst on
    /// 2026-08-13 put 38.3 GiB — 57% of live heap — back in the very stack that
    /// work had cut. A default that differs by session is invisible until it is
    /// measured in a heap profile, so pin it.
    #[test]
    fn the_dml_session_decodes_wide_rows_at_the_shared_batch_size() {
        let base = datafusion::execution::session_state::SessionStateBuilder::new().with_default_features().build();
        let want: usize = crate::database::WIDE_ROW_DECODE_BATCH_SIZE.parse().expect("the shared constant is a number");
        // The inherited default is the bug: assert the session actually moves
        // off it, so this cannot pass by coincidence if DataFusion's default
        // ever happens to equal ours.
        assert!(base.config().options().execution.batch_size > want, "DataFusion's default is the wider batch this fix exists to override");
        let session = super::delta_session_from(&base);
        assert_eq!(session.config().options().execution.batch_size, want, "a DML rewrite must not decode at a wider batch than a query does");
    }

    #[test]
    fn large_mor_sources_stay_inside_the_key_pushdown_bound() {
        let rows = super::MOR_KEY_PUSHDOWN_ROWS * 2 + 17;
        let batch = RecordBatch::try_from_iter(vec![("span_id", Arc::new(StringArray::from_iter_values((0..rows).map(|i| format!("span-{i}")))) as _)])
            .expect("source batch");
        let schema = batch.schema();
        let chunks = super::bounded_mor_source_chunks(super::UpdateSource { batch, schema, join_keys: vec![("context___span_id".into(), "span_id".into())] });

        assert_eq!(chunks.iter().map(|chunk| chunk.batch.num_rows()).sum::<usize>(), rows);
        assert_eq!(chunks.len(), 3);
        assert!(chunks.iter().all(|chunk| chunk.batch.num_rows() <= super::MOR_KEY_PUSHDOWN_ROWS));
        assert_eq!(chunks[2].batch.num_rows(), 17);
    }
}

#[cfg(test)]
mod strip_tests {
    use std::collections::HashSet;

    use datafusion::prelude::{col, lit};

    use super::strip_source_conjuncts;

    #[test]
    fn strips_source_referencing_conjuncts_keeps_target_only() {
        // The prod hash-enrichment shape: project_id + timestamp bounds (target-only)
        // AND-ed with the equi-key equality and the NOT(@> u.tag) cross-filter, both
        // of which reference source columns.
        let source_cols: HashSet<String> = ["span_id", "trace_id", "tag"].iter().map(|s| s.to_string()).collect();
        let pred = col("project_id")
            .eq(lit("p1"))
            .and(col("timestamp").gt(lit(1000i64)))
            .and(col("context___span_id").eq(col("span_id"))) // references source `span_id`
            .and(col("hashes").is_not_null().or(col("tag").is_not_null())); // references source `tag`

        let stripped = strip_source_conjuncts(&pred, &source_cols).expect("target-only conjuncts remain");
        let s = format!("{stripped}");
        assert!(s.contains("project_id"), "kept project_id: {s}");
        assert!(s.contains("timestamp"), "kept timestamp: {s}");
        assert!(!s.contains("span_id"), "dropped the source equi-key: {s}");
        assert!(!s.contains("tag"), "dropped the source cross-filter: {s}");
    }

    #[test]
    fn strips_isnotnull_on_equi_key_target_columns() {
        // Prod 2026-07-20 drop bug: the optimizer inserts IsNotNull(o.context___span_id)
        // null-rejection on the join keys. It's TARGET-only (survives a source-only
        // strip) but the high-cardinality key isn't in the stats schema, so the fork's
        // file-skipping scan fails to resolve it. strip_cols includes the equi-key
        // TARGET columns so these conjuncts are dropped from the file-pruning predicate.
        let strip_cols: HashSet<String> = ["span_id", "trace_id", "tag", "context___span_id", "context___trace_id"].iter().map(|s| s.to_string()).collect();
        let pred = col("date")
            .gt_eq(lit("2026-07-20"))
            .and(col("timestamp").gt(lit(1000i64)))
            .and(col("context___span_id").is_not_null()) // optimizer null-rejection — must strip
            .and(col("context___trace_id").is_not_null());

        let stripped = strip_source_conjuncts(&pred, &strip_cols).expect("date + timestamp remain");
        let s = format!("{stripped}");
        assert!(s.contains("date") && s.contains("timestamp"), "kept prunable partition/stat conjuncts: {s}");
        assert!(!s.contains("context___span_id"), "dropped IsNotNull(context___span_id): {s}");
        assert!(!s.contains("context___trace_id"), "dropped IsNotNull(context___trace_id): {s}");
    }

    /// The coalescer fold's `project_id IN (...)` conjunct must SURVIVE the
    /// equi-key strip: `project_id` is a partition column (resolvable from
    /// partition values), and stripping it un-prunes the folded merge back to
    /// every tenant's files in the window.
    #[test]
    fn partition_column_equi_key_conjuncts_survive_the_strip() {
        let source_cols: HashSet<String> = ["span_id", "trace_id", "tag", "project_id"].iter().map(|s| s.to_string()).collect();
        let join_keys: Vec<(String, String)> = [("context___span_id", "span_id"), ("context___trace_id", "trace_id"), ("project_id", "project_id")]
            .iter()
            .map(|(t, s)| (t.to_string(), s.to_string()))
            .collect();
        let partition_cols = ["project_id".to_string(), "date".to_string()];
        let strip_cols = super::dv_strip_cols(&source_cols, &join_keys, &partition_cols);

        let pred =
            col("date").gt_eq(lit("2026-07-26")).and(col("project_id").in_list(vec![lit("p1"), lit("p2")], false)).and(col("context___span_id").is_not_null());
        let stripped = strip_source_conjuncts(&pred, &strip_cols).expect("date + IN-list remain");
        let s = format!("{stripped}");
        assert!(s.contains("project_id") && s.contains("IN"), "partition IN-list must survive: {s}");
        assert!(!s.contains("context___span_id"), "IsNotNull(equi data key) still stripped: {s}");
    }

    #[test]
    fn all_source_conjuncts_strips_to_none() {
        let source_cols: HashSet<String> = ["span_id"].iter().map(|s| s.to_string()).collect();
        let pred = col("context___span_id").eq(col("span_id"));
        assert!(strip_source_conjuncts(&pred, &source_cols).is_none());
    }
}

// ===== dml_coalescer =====
// Deferred, batched Delta legs for `UPDATE ... FROM` (DML coalescing), plus
// the flush-watermark predicate clamp shared with the synchronous DML path.
//
// Why: one Delta MERGE commit per statement (monoscope's hash tagging runs
// ~1.4k/hr) starves OPTIMIZE via OCC conflicts, accumulates small files, and
// pays a full copy-on-write parquet rewrite per handful of rows. The mem-leg
// (synchronous MemBuffer mutation, WAL-backed) already gives read-your-writes
// through the scan overlay, so the Delta leg is pure durability convergence —
// it can be deferred and batched.
//
// Grouping: statements coalesce when (project, table, join keys, assignments,
// non-time residual predicate, source schema) all match; per-statement
// timestamp-range conjuncts are widened to the union window. Same-key source
// rows with different payloads (e.g. two tags for one span) cannot share one
// MERGE (Delta forbids duplicate source matches), so the drained batch splits
// into ordered rounds — round N holds each key's Nth occurrence.
//
// Contract (see `d_dml_coalesce_secs`): deferred statements must be
// idempotent under re-application. A row flushed between the mem leg and the
// drain sees the assignment applied twice, and a failed drain retries whole
// groups (including rounds that already committed).
//
// Durability: the mem leg WAL-appends `UpdateWithSource` before enqueue, so
// buffer-resident rows survive a crash with their post-DML values. What a
// crash CAN lose is the deferred Delta leg for rows that were already in
// Delta when the statement ran — bounded by the drain interval.
//
// A group that exhausts `MAX_DRAIN_ATTEMPTS` is **parked**, not dropped: its
// rows go to `<wal_dir>/quarantine/dml` as Arrow IPC + a `.meta` sidecar
// (`timefusion.dml.coalesce_quarantined`). Dropping was unrecoverable — the
// Delta leg targets rows already flushed out of the buffer, so there is no
// newer copy to converge from and read-side dedup (first-seen-wins) cannot
// repair it. `timefusion.dml.coalesce_dropped` now means the *quarantine
// write itself* failed, i.e. genuine loss.

use std::{
    collections::{BTreeMap, hash_map::Entry},
    hash::{Hash, Hasher},
    sync::atomic::{AtomicUsize, Ordering},
};

use datafusion::{
    arrow::{
        array::UInt32Array,
        compute::take,
        row::{RowConverter, SortField},
    },
    common::ScalarValue,
    logical_expr::{BinaryExpr, Operator},
    prelude::lit,
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::read::optimizers::{is_col_through_cast, scalar_micros, swap_comparison, unwrap_literal};

/// Queue-size pressure threshold: total buffered source rows above which a
/// drain is triggered immediately instead of waiting for the timer. Matches
/// `MAX_UPDATE_SOURCE_ROWS` — a drained group must stay mergeable.
const MAX_QUEUED_SOURCE_ROWS: usize = 1_000_000;

/// Drain attempts per group before it is quarantined (each drain already
/// carries perform_delta_merge_update's 4-attempt OCC retry underneath).
const MAX_DRAIN_ATTEMPTS: u32 = 3;

/// Max source rows fed to a single Delta MERGE. `MAX_QUEUED_SOURCE_ROWS` only
/// *notifies* a drain, so a group can grow past it unbounded — on 2026-07-27
/// one reached 1_252_311 rows (7457 statements) and every MERGE attempt died
/// with "Resources exhausted", costing the whole group. Rounds are therefore
/// chunked: many bounded merges instead of one unbounded one. Each chunk is an
/// independent commit, which the idempotence contract already permits.
const MAX_MERGE_ROWS: usize = 100_000;

/// Zero-copy slices of `batch` of at most `max` rows, covering every row once.
fn chunk_rows(batch: &RecordBatch, max: usize) -> impl Iterator<Item = RecordBatch> + '_ {
    (0..batch.num_rows()).step_by(max).map(move |off| batch.slice(off, max.min(batch.num_rows() - off)))
}

/// Persist a terminally-failed group's source rows as an Arrow IPC file plus a
/// `.meta` sidecar, so the Delta leg can be re-driven instead of lost.
///
/// Before this existed the terminal branch logged and dropped: 2026-07-27
/// 04:42Z lost 1_252_311 enrichment rows that way. The loss is permanent
/// without a sidecar — the mem leg already applied, and the Delta leg is
/// watermark-clamped to rows that have *already* flushed and left the buffer,
/// so Delta keeps stale pre-DML values with no newer copy anywhere (read-side
/// dedup is first-seen-wins and cannot repair it).
///
/// Returns false when nothing could be persisted; the caller must then keep
/// the loud error path, because the rows are genuinely gone.
fn quarantine_group(dir: &std::path::Path, key: &GroupKey, group: &PendingGroup, batches: &[RecordBatch], reason: &str) -> bool {
    if let Err(e) = std::fs::create_dir_all(dir) {
        error!("dml quarantine: cannot create {dir:?}: {e}");
        return false;
    }
    // Schema drift is itself a quarantine reason (concat failure), so keep
    // only what this IPC file can actually hold and say so if any are left.
    let writable: Vec<&RecordBatch> = batches.iter().filter(|b| b.schema() == group.schema).collect();
    let skipped = batches.len() - writable.len();
    if writable.is_empty() {
        error!(
            "dml quarantine: no batch matches the group schema for {}/{} — {} rows LOST: {reason}",
            key.project_id,
            key.table_name,
            batches.iter().map(RecordBatch::num_rows).sum::<usize>()
        );
        return false;
    }
    let rows: usize = writable.iter().map(|b| b.num_rows()).sum();
    // Fingerprint disambiguates two groups for the same project/table parked in
    // the same microsecond; `create_new` then turns any residual collision into
    // an error rather than a silent truncation of already-parked user data.
    let stem = format!("{}_{:016x}_{}__{}", crate::support::now_micros(), key.fingerprint, key.project_id, key.table_name).replace(['/', '\\', ':', '\0'], "_");
    let Some((path, file)) = (0..16).find_map(|n| {
        let p = dir.join(if n == 0 { format!("{stem}.arrow") } else { format!("{stem}-{n}.arrow") });
        crate::write::create_owner_only(&p, true).ok().map(|f| (p, f))
    }) else {
        error!("dml quarantine: cannot create a unique payload file under {dir:?} for {}/{}", key.project_id, key.table_name);
        return false;
    };

    // Arrow IPC (not raw bytes): self-describing schema, so a re-drive needs
    // only the sidecar for the merge shape. Streamed straight to the file —
    // buffering a multi-GB group in a Vec first risks OOM at the exact moment
    // memory exhaustion is what brought us here.
    if let Err(e) = datafusion::arrow::ipc::writer::FileWriter::try_new(std::io::BufWriter::new(file), &group.schema)
        .and_then(|mut w| writable.iter().try_for_each(|b| w.write(b)).and_then(|()| w.finish()))
    {
        error!("dml quarantine: IPC write failed for {path:?}: {e}");
        return false;
    }

    let meta = format!(
        "project_id={}\ntable_name={}\nfolded_projects={}\njoin_keys={}\nassignments={}\npredicate={}\ntime_col={}\nattempts={}\nrows={}\nstatements={}\nreason={reason}\n",
        key.project_id,
        key.table_name,
        group.folded_projects.as_ref().map_or(String::new(), |p| p.join(",")),
        group.join_keys.iter().map(|(t, s)| format!("{t}={s}")).collect::<Vec<_>>().join(","),
        group.assignments.iter().map(|(c, e)| format!("{c}:={e}")).collect::<Vec<_>>().join(","),
        group.predicate.reconstruct(group.time_col).map_or(String::new(), |e| e.to_string()),
        group.time_col,
        group.attempts,
        rows,
        group.batches.len(),
    );
    if let Err(e) = crate::write::write_owner_only(&path.with_extension("meta"), meta.as_bytes()) {
        error!("dml quarantine: meta write failed for {path:?}: {e}");
    }
    error!("dml quarantine: parked {}/{} ({rows} rows) at {path:?}: {reason}", key.project_id, key.table_name);
    crate::observability::record_dml_coalesce_quarantined();
    if skipped > 0 {
        // Partially parked is partially LOST — the skipped batches have no
        // other copy. Page on it, or the recoverable-looking quarantine metric
        // would mask real loss.
        let lost: usize = batches.iter().filter(|b| b.schema() != group.schema).map(RecordBatch::num_rows).sum();
        crate::observability::record_dml_coalesce_dropped();
        error!("dml quarantine: {skipped} schema-mismatched batch(es) for {}/{} could NOT be parked — {lost} rows LOST", key.project_id, key.table_name);
    }
    true
}

/// The table's time column ("timestamp" unless the schema overrides it) —
/// the column whose range conjuncts are widened and watermark-clamped.
pub(crate) fn table_time_column(table_name: &str) -> &'static str {
    crate::schema::get_schema(table_name).map_or("timestamp", |s| s.time_column_name())
}

/// One extracted `time_col CMP literal` conjunct.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct TimeBound {
    pub value: ScalarValue,
    pub inclusive: bool,
}

/// A predicate split into at most one lower / one upper bound on the time
/// column plus everything else verbatim. Extracting only the first bound per
/// direction is always sound: the reconstruction conjoins residual + bounds,
/// so the predicate is preserved exactly — extra time conjuncts just live in
/// `residual` and block cross-statement grouping via the fingerprint.
#[derive(Debug, Clone, Default)]
pub(crate) struct DecomposedPredicate {
    pub residual: Vec<Expr>,
    pub lower: Option<TimeBound>,
    pub upper: Option<TimeBound>,
}

impl DecomposedPredicate {
    pub fn decompose(predicate: Option<&Expr>, time_col: &str) -> Self {
        let Some(pred) = predicate else { return Self::default() };
        split_conjunction(pred).into_iter().fold(Self::default(), |mut d, conjunct| {
            match classify_time_conjunct(conjunct, time_col) {
                Some((bound, true)) if d.lower.is_none() => d.lower = Some(bound),
                Some((bound, false)) if d.upper.is_none() => d.upper = Some(bound),
                _ => d.residual.push(conjunct.clone()),
            }
            d
        })
    }

    /// Conjoin residual + bounds back into a predicate (inverse of decompose).
    pub fn reconstruct(&self, time_col: &str) -> Option<Expr> {
        let time_expr = |b: &TimeBound, lower: bool| {
            let op = match (lower, b.inclusive) {
                (true, true) => Operator::GtEq,
                (true, false) => Operator::Gt,
                (false, true) => Operator::LtEq,
                (false, false) => Operator::Lt,
            };
            Expr::BinaryExpr(BinaryExpr { left: Box::new(datafusion::prelude::col(time_col)), op, right: Box::new(lit(b.value.clone())) })
        };
        self.residual
            .iter()
            .cloned()
            .chain(self.lower.as_ref().map(|b| time_expr(b, true)))
            .chain(self.upper.as_ref().map(|b| time_expr(b, false)))
            .reduce(|a, b| a.and(b))
    }

    /// Widen this predicate's time window to also cover `other`'s (union):
    /// lower takes the smaller bound, upper the larger; a missing bound on
    /// either side is unbounded and dominates.
    fn widen(&mut self, other: &Self) {
        self.widen_bounds(&other.lower, &other.upper);
    }

    /// `widen` against bare bounds — a statement's own window, without
    /// fabricating a whole predicate to carry it.
    fn widen_bounds(&mut self, lower: &Option<TimeBound>, upper: &Option<TimeBound>) {
        self.lower = match (self.lower.take(), lower) {
            (Some(a), Some(b)) => Some(widen_bound(a, b, true)),
            _ => None,
        };
        self.upper = match (self.upper.take(), upper) {
            (Some(a), Some(b)) => Some(widen_bound(a, b, false)),
            _ => None,
        };
    }
}

/// Union-widen two bounds on the same side: for lowers keep the smaller
/// value, for uppers the larger; on equal values inclusive wins. Values that
/// don't compare (mixed types) widen to the safest available — the caller's
/// fingerprint makes this near-impossible, but never tighten on uncertainty.
fn widen_bound(a: TimeBound, b: &TimeBound, lower: bool) -> TimeBound {
    match a.value.partial_cmp(&b.value) {
        Some(std::cmp::Ordering::Equal) => TimeBound { inclusive: a.inclusive || b.inclusive, value: a.value },
        Some(ord) if (ord == std::cmp::Ordering::Less) == lower => a,
        Some(_) => b.clone(),
        None => TimeBound { inclusive: true, ..a },
    }
}

/// Drain-time merge window. The Delta MERGE hash-join builds against every
/// target row in the predicate's time window regardless of source chunking,
/// so join memory ∝ window width — 2026-07-30 union-window drains (~1h)
/// reserved 2.5+ GB per merge and exhausted the 10 GB pool, and k chunks paid
/// k full scans of the whole window. Bucketing statements by their own bounds
/// keeps each merge's target side ∝ bucket, not burst.
const DML_MERGE_BUCKET_MICROS: i64 = 5 * 60 * 1_000_000;

/// A statement's own time bounds, captured at enqueue (the group predicate is
/// widened to the union, so these are the only record of the narrow window).
type StmtBounds = (Option<TimeBound>, Option<TimeBound>);
type BoundBatch = (RecordBatch, StmtBounds);

/// Timestamp scalar → microseconds (inverse of [`watermark_scalar`]).
/// The `DML_MERGE_BUCKET_MICROS` bucket span a statement's window covers, or
/// None when a side is unbounded or non-timestamp (those statements share one
/// catch-all unit). Spanning statements keep their full window — the span key
/// just puts same-span statements together; it never narrows anything.
fn bounds_span(b: &StmtBounds) -> Option<(i64, i64)> {
    let lo = scalar_micros(&b.0.as_ref()?.value)?;
    let up = b.1.as_ref()?;
    // An exclusive upper on a bucket edge contains no row at the edge itself.
    let hi = scalar_micros(&up.value)? - i64::from(!up.inclusive);
    Some((lo.div_euclid(DML_MERGE_BUCKET_MICROS), hi.max(lo).div_euclid(DML_MERGE_BUCKET_MICROS)))
}

/// Split a drained group into per-time-bucket merge units: each unit keeps the
/// group's shape (residual, keys, assignments, attempts) but narrows the time
/// window to the union of only its own statements' bounds. Single-bucket
/// groups pass through untouched — exactly today's one-merge-unit behavior.
fn bucket_group(mut group: PendingGroup) -> Vec<PendingGroup> {
    let buckets: BTreeMap<Option<(i64, i64)>, Vec<BoundBatch>> = std::mem::take(&mut group.batches).into_iter().fold(BTreeMap::new(), |mut m, bb| {
        m.entry(bounds_span(&bb.1)).or_default().push(bb);
        m
    });
    if buckets.len() <= 1 {
        group.batches = buckets.into_values().next().unwrap_or_default();
        return vec![group];
    }
    buckets
        .into_values()
        .map(|batches| {
            let predicate = batches[1..].iter().fold(
                DecomposedPredicate { residual: group.predicate.residual.clone(), lower: batches[0].1.0.clone(), upper: batches[0].1.1.clone() },
                |mut p, (_, (lo, up))| {
                    p.widen_bounds(lo, up);
                    p
                },
            );
            PendingGroup { predicate, batches, ..group.clone() }
        })
        .collect()
}

/// Classify a conjunct as `(bound, is_lower)` when it is
/// `time_col CMP literal` (either operand order); anything else is residual.
fn classify_time_conjunct(e: &Expr, time_col: &str) -> Option<(TimeBound, bool)> {
    let Expr::BinaryExpr(BinaryExpr { left, op, right }) = e else { return None };
    let (value, op) = match (left.as_ref(), right.as_ref()) {
        (Expr::Column(c), Expr::Literal(v, _)) if c.name == time_col => (v.clone(), *op),
        (Expr::Literal(v, _), Expr::Column(c)) if c.name == time_col => (v.clone(), op.swap()?),
        _ => return None,
    };
    match op {
        Operator::Gt => Some((TimeBound { value, inclusive: false }, true)),
        Operator::GtEq => Some((TimeBound { value, inclusive: true }, true)),
        Operator::Lt => Some((TimeBound { value, inclusive: false }, false)),
        Operator::LtEq => Some((TimeBound { value, inclusive: true }, false)),
        _ => None,
    }
}

/// The half-open `[lo, hi)` microsecond window a statement's predicate can
/// touch on `time_col`, with `i64::MIN`/`i64::MAX` for an unbounded side.
/// `None` = no bound at all could be derived, and the caller must treat the
/// statement as touching the whole table.
///
/// Only AND-position `time_col CMP literal` conjuncts contribute (through
/// `Cast`/`TryCast` on either side — extended-protocol param binding wraps the
/// literal, which is what silently disabled the merge date-prune for ~6% of
/// prod enrichment merges). Anything else — OR, NOT, a non-literal bound, a
/// unit that doesn't convert — is simply not a bound, so the window can only
/// ever be a SUPERSET of the touched rows.
///
/// Both ends round OUTWARD — `lo = floor(v)`, `hi = floor(v) + 1` — so a
/// sub-µs literal can never cut off a µs-granular row on the boundary, at the
/// cost of at most 1µs of extra width. That also makes the
/// inclusive/exclusive distinction irrelevant, so there is one rule per side
/// instead of four.
pub(crate) fn dml_time_window(predicate: Option<&Expr>, time_col: &str) -> Option<(i64, i64)> {
    let (lo, hi) = split_conjunction(predicate?)
        .into_iter()
        .filter_map(|conjunct| {
            let Expr::BinaryExpr(BinaryExpr { left, op, right }) = conjunct else { return None };
            let (bound, op) = if is_col_through_cast(left, time_col) {
                (right.as_ref(), *op)
            } else if is_col_through_cast(right, time_col) {
                (left.as_ref(), swap_comparison(*op))
            } else {
                return None;
            };
            Some((unwrap_literal(bound).and_then(crate::read::optimizers::scalar_micros)?, op))
        })
        .fold((i64::MIN, i64::MAX), |(lo, hi), (v, op)| match op {
            Operator::Gt | Operator::GtEq => (lo.max(v), hi),
            Operator::Lt | Operator::LtEq => (lo, hi.min(v.saturating_add(1))),
            Operator::Eq => (lo.max(v), hi.min(v.saturating_add(1))),
            _ => (lo, hi),
        });
    ((lo, hi) != (i64::MIN, i64::MAX)).then_some((lo, hi))
}

/// Express `watermark_micros` in the same scalar type as `template` so bounds
/// stay comparable. None (no clamping) when the template isn't a timestamp or
/// the unit conversion overflows.
fn watermark_scalar(template: &ScalarValue, micros: i64) -> Option<ScalarValue> {
    Some(match template {
        ScalarValue::TimestampSecond(_, tz) => ScalarValue::TimestampSecond(Some(micros.div_euclid(1_000_000)), tz.clone()),
        ScalarValue::TimestampMillisecond(_, tz) => ScalarValue::TimestampMillisecond(Some(micros.div_euclid(1_000)), tz.clone()),
        ScalarValue::TimestampMicrosecond(_, tz) => ScalarValue::TimestampMicrosecond(Some(micros), tz.clone()),
        ScalarValue::TimestampNanosecond(_, tz) => ScalarValue::TimestampNanosecond(Some(micros.checked_mul(1_000)?), tz.clone()),
        _ => return None,
    })
}

/// Watermark clamp outcome for a DML Delta leg.
pub(crate) enum WatermarkClamp {
    /// Run the Delta leg with this (possibly tightened) predicate.
    Keep(Option<Expr>),
    /// The whole time window lies above the flush watermark: every matchable
    /// row is buffer-only, the flush persists its post-DML value, and the
    /// Delta leg would scan + commit for nothing. Skip it.
    SkipDelta,
}

/// Clamp `predicate`'s time window to rows that can exist in Delta: rows with
/// `time_col > watermark` were never handed to a Delta commit (the watermark
/// is raised before every commit and persisted with it), so the upper bound
/// tightens to the watermark — and when even the lower bound is above it, the
/// Delta leg skips entirely. Predicates without a literal time bound pass
/// through untouched (no type template to clamp against).
pub(crate) fn clamp_to_watermark(predicate: Option<&Expr>, time_col: &str, watermark_micros: i64) -> WatermarkClamp {
    let mut d = DecomposedPredicate::decompose(predicate, time_col);
    match clamp_decomposed(&mut d, watermark_micros) {
        ClampAction::Skip => WatermarkClamp::SkipDelta,
        ClampAction::Unchanged => WatermarkClamp::Keep(predicate.cloned()),
        ClampAction::Clamped => WatermarkClamp::Keep(d.reconstruct(time_col)),
    }
}

enum ClampAction {
    Unchanged,
    Clamped,
    Skip,
}

/// Shared clamp core over a decomposed predicate (used by both the
/// synchronous path above and the coalescer drain, which clamps the widened
/// window at drain time — the watermark only rises, so later is tighter).
fn clamp_decomposed(d: &mut DecomposedPredicate, watermark_micros: i64) -> ClampAction {
    let Some(wm) = d.lower.as_ref().or(d.upper.as_ref()).and_then(|b| watermark_scalar(&b.value, watermark_micros)) else {
        return ClampAction::Unchanged;
    };
    let above_watermark = |b: &TimeBound| match b.value.partial_cmp(&wm) {
        Some(std::cmp::Ordering::Greater) => true,
        Some(std::cmp::Ordering::Equal) => !b.inclusive,
        _ => false,
    };
    if d.lower.as_ref().is_some_and(above_watermark) {
        return ClampAction::Skip;
    }
    let tighter = match &d.upper {
        Some(up) => matches!(up.value.partial_cmp(&wm), Some(std::cmp::Ordering::Greater)),
        None => d.lower.is_some(), // only add a bound when a time window exists at all
    };
    if tighter {
        d.upper = Some(TimeBound { value: wm, inclusive: true });
        ClampAction::Clamped
    } else {
        ClampAction::Unchanged
    }
}

/// Split an [`UpdateSource`] into merge rounds so no single leg sees duplicate
/// join keys: the Delta MERGE rejects a source that matches a target row twice,
/// and the MemBuffer hash-join would silently keep only one match — dropping the
/// rest (prod 2026-07-19: same-key multi-tag hash enrichment lost tags / errored).
/// Returns one round for the common no-duplication case.
pub(crate) fn split_source_rounds(source: UpdateSource) -> Result<Vec<UpdateSource>> {
    let key_indices: Vec<usize> = source.join_keys.iter().map(|(_, s)| source.schema.index_of(s)).collect::<std::result::Result<_, _>>()?;
    Ok(split_rounds(&source.batch, &key_indices)?
        .into_iter()
        .map(|batch| UpdateSource { batch, schema: source.schema.clone(), join_keys: source.join_keys.clone() })
        .collect())
}

/// Split `batch` into merge rounds: exact-duplicate rows are dropped, and
/// rows sharing a join key land in successive rounds (round N = each key's
/// Nth distinct payload, in arrival order) so no single MERGE sees duplicate
/// source keys — Delta rejects a source that matches a target row twice.
fn split_rounds(batch: &RecordBatch, key_indices: &[usize]) -> Result<Vec<RecordBatch>> {
    let fields = |cols: &[datafusion::arrow::array::ArrayRef]| cols.iter().map(|c| SortField::new(c.data_type().clone())).collect::<Vec<_>>();
    let key_cols: Vec<_> = key_indices.iter().map(|&i| batch.column(i).clone()).collect();
    let key_rows = RowConverter::new(fields(&key_cols))?.convert_columns(&key_cols)?;
    let full_rows = RowConverter::new(fields(batch.columns()))?.convert_columns(batch.columns())?;

    // Imperative by necessity: the round assignment carries borrowed dedup
    // state across rows. Row views are bound first so the byte-slice keys can
    // borrow from them across the loop — no per-row heap copies.
    let full_row_views: Vec<_> = (0..batch.num_rows()).map(|i| full_rows.row(i)).collect();
    let key_row_views: Vec<_> = (0..batch.num_rows()).map(|i| key_rows.row(i)).collect();
    let mut seen_full: HashSet<&[u8]> = HashSet::new();
    let mut rounds: Vec<(HashSet<&[u8]>, Vec<u32>)> = Vec::new();
    for i in 0..batch.num_rows() {
        if !seen_full.insert(full_row_views[i].as_ref()) {
            continue; // exact duplicate statement row — one application suffices
        }
        let key: &[u8] = key_row_views[i].as_ref();
        match rounds.iter_mut().find(|(keys, _)| !keys.contains(key)) {
            Some((keys, idxs)) => {
                keys.insert(key);
                idxs.push(i as u32);
            }
            None => rounds.push(([key].into_iter().collect(), vec![i as u32])),
        }
    }
    rounds
        .into_iter()
        .map(|(_, idxs)| {
            let idx = UInt32Array::from(idxs);
            let cols = batch.columns().iter().map(|c| take(c, &idx, None)).collect::<std::result::Result<Vec<_>, _>>()?;
            RecordBatch::try_new(batch.schema(), cols).map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
        })
        .collect()
}

/// Delta's DV MERGE rejects a source that matches one target row twice.
/// `split_rounds` should prevent it, yet prod groups still hit it (2026-07-28..30,
/// root cause open) — so on that error, bisect: halve the source and merge each
/// half. Single rows cannot multi-match, so recursion always terminates, the
/// data lands, and the log narrows down the offending key pair.
pub(crate) fn merge_bisect<'a>(
    db: &'a crate::database::Database, table_name: &'a str, project_id: &'a str, predicate: Option<Expr>, assignments: Vec<(String, Expr)>,
    source: UpdateSource, session: Arc<dyn Session>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<u64>> + Send + 'a>> {
    Box::pin(async move {
        let rows = source.batch.num_rows();
        match crate::dml::perform_delta_merge_update(db, table_name, project_id, predicate.clone(), assignments.clone(), source.clone(), session.clone()).await
        {
            Err(e) if e.to_string().contains("multiple source rows") && rows > 1 => {
                let (a, b) = (source.batch.slice(0, rows / 2), source.batch.slice(rows / 2, rows - rows / 2));
                warn!("dml merge: multi-source match on {rows} rows for {project_id}/{table_name}; bisecting ({} + {} rows)", a.num_rows(), b.num_rows());
                // Sequential await per half: the recursion is fallible and ordered, which an iterator chain can't express here.
                let mut n = 0;
                for half in [a, b] {
                    let src = UpdateSource { batch: half, schema: source.schema.clone(), join_keys: source.join_keys.clone() };
                    n += merge_bisect(db, table_name, project_id, predicate.clone(), assignments.clone(), src, session.clone()).await?;
                }
                Ok(n)
            }
            r => r,
        }
    })
}

/// Re-drive parked `quarantine/dml/*` groups (hash-enrichment shape only; see
/// [`parse_quarantine_meta`]). Rebuilds the merge from the sidecar, replays it
/// through round-splitting + [`merge_bisect`], and moves recovered pairs into
/// `<dir>/redriven/`. Returns `(recovered, skipped)`.
pub async fn redrive_dml_quarantine(db: &Arc<crate::database::Database>, dir: &std::path::Path, dry_run: bool) -> (usize, usize) {
    use datafusion::logical_expr::{col, in_list};
    let Ok(rd) = std::fs::read_dir(dir) else {
        info!("dml redrive: no quarantine dir at {dir:?}");
        return (0, 0);
    };
    let ctx = db.clone().create_session_context();
    // Variant columns: the pgwire-facing session reads them as Utf8View, which
    // the DV-merge write leg cannot cast back to Struct{metadata,value}. Use
    // the same variant-safe session the interactive DML path hands to merges.
    let session: Arc<dyn Session> = crate::dml::delta_session_from(&ctx.state());
    let redriven = dir.join("redriven");
    let (mut ok, mut skipped) = (0usize, 0usize);
    for entry in rd.flatten() {
        let path = entry.path();
        if path.extension().is_none_or(|e| e != "arrow") {
            continue;
        }
        let meta_path = path.with_extension("meta");
        let Some(meta) = std::fs::read_to_string(&meta_path).ok().as_deref().and_then(parse_quarantine_meta) else {
            warn!("dml redrive: {path:?} meta missing or not the reconstructible enrichment shape; leaving parked");
            skipped += 1;
            continue;
        };
        let batches: Result<Vec<RecordBatch>> = std::fs::File::open(&path)
            .map_err(|e| DataFusionError::External(Box::new(e)))
            .and_then(|f| datafusion::arrow::ipc::reader::FileReader::try_new(f, None).map_err(|e| DataFusionError::ArrowError(Box::new(e), None)))
            .and_then(|r| r.collect::<std::result::Result<Vec<_>, _>>().map_err(|e| DataFusionError::ArrowError(Box::new(e), None)));
        let merged = match batches.and_then(|b| {
            let schema = b.first().map(RecordBatch::schema).ok_or_else(|| DataFusionError::Execution("empty IPC file".into()))?;
            concat_batches(&schema, &b).map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
        }) {
            Ok(m) => m,
            Err(e) => {
                warn!("dml redrive: cannot read {path:?}: {e}; leaving parked");
                skipped += 1;
                continue;
            }
        };
        // Predicate: same shape the group was parked with. Bare column names —
        // requalification in the merge routes them (project_id binds to the
        // source side, which the join equates with the target's anyway).
        //
        // WINDOW SLICING: the merge hash-join builds against every target row
        // in the predicate window — independent of source chunking — and a
        // burst group's full window exhausted a 24 GB pool (2026-07-30,
        // HashJoinInput 3.7 GB × several partitions). Each target row lies in
        // exactly one time slice, so merging the SAME source against each
        // slice is semantically identical with join memory ÷ slices.
        let tz: Arc<str> = Arc::from("UTC");
        let (ts_upper, upper_incl) = meta.ts_upper;
        let slices = (meta.rows / 150_000).clamp(1, 32) as i64;
        let span = (ts_upper - meta.ts_lower).max(slices);
        let base_conj = |lo: i64, hi: i64, hi_incl: bool| {
            let ts_lit = |v: i64| lit(ScalarValue::TimestampMicrosecond(Some(v), Some(tz.clone())));
            [
                col("context___span_id").is_not_null(),
                col("context___trace_id").is_not_null(),
                col("date").gt_eq(lit(ScalarValue::Date32(Some(meta.date_bounds.0)))),
                col("date").lt_eq(lit(ScalarValue::Date32(Some(meta.date_bounds.1)))),
                col("timestamp").gt_eq(ts_lit(lo)),
                if hi_incl { col("timestamp").lt_eq(ts_lit(hi)) } else { col("timestamp").lt(ts_lit(hi)) },
            ]
            .into_iter()
            .chain((meta.projects.len() > 1).then(|| in_list(col("project_id"), meta.projects.iter().map(lit).collect(), false)))
            .reduce(Expr::and)
        };
        let slice_bounds: Vec<(i64, i64, bool)> = (0..slices)
            .map(|i| {
                let lo = meta.ts_lower + span * i / slices;
                let hi = if i + 1 == slices { ts_upper } else { meta.ts_lower + span * (i + 1) / slices };
                // interior slice upper bounds are exclusive; the last keeps the meta's inclusivity
                (lo, hi, if i + 1 == slices { upper_incl } else { false })
            })
            .collect();
        // Rebuilt equivalents of the two parked shapes (no empty-list literal
        // needed): scalar `tag` → append-or-singleton; list `new_hashes` →
        // take-or-concat.
        let assignment = if meta.list_source {
            datafusion::logical_expr::when(col("hashes").is_null(), col("new_hashes"))
                .otherwise(datafusion::functions_nested::expr_fn::array_concat(vec![col("hashes"), col("new_hashes")]))
                .expect("static CASE expr")
        } else {
            datafusion::logical_expr::when(col("hashes").is_not_null(), datafusion::functions_nested::expr_fn::array_append(col("hashes"), col("tag")))
                .otherwise(datafusion::functions_nested::expr_fn::make_array(vec![col("tag")]))
                .expect("static CASE expr")
        };
        info!(
            "dml redrive: {} rows for {} ({} projects), window {:?}: replaying{}",
            merged.num_rows(),
            meta.table_name,
            meta.projects.len(),
            meta.date_bounds,
            if dry_run { " [DRY RUN]" } else { "" }
        );
        if dry_run {
            ok += 1;
            continue;
        }
        let key_indices: Result<Vec<usize>> = meta.join_keys.iter().map(|(_, s)| Ok(merged.schema().index_of(s)?)).collect();
        let rounds = match key_indices.and_then(|idx| split_rounds(&merged, &idx)) {
            Ok(r) => r,
            Err(e) => {
                warn!("dml redrive: round split failed for {path:?}: {e}; leaving parked");
                skipped += 1;
                continue;
            }
        };
        // Sequential awaits with `?`: the first failed slice leaves the group parked.
        let outcome = async {
            for (si, &(lo, hi, hi_incl)) in slice_bounds.iter().enumerate() {
                let predicate = base_conj(lo, hi, hi_incl);
                if slice_bounds.len() > 1 {
                    info!("dml redrive: {path:?} slice {}/{} [{lo}..{hi}]", si + 1, slice_bounds.len());
                }
                for round in rounds.iter().flat_map(|r| chunk_rows(r, MAX_MERGE_ROWS)) {
                    let source = UpdateSource { batch: round, schema: merged.schema(), join_keys: meta.join_keys.clone() };
                    let assignments = vec![("hashes".into(), assignment.clone())];
                    merge_bisect(db, &meta.table_name, &meta.project_id, predicate.clone(), assignments, source, session.clone()).await.inspect_err(|e| {
                        error!(
                            "dml redrive: merge failed for {path:?} (slice {}/{}): {e}; leaving parked (applied slices re-append tags only on retried rows)",
                            si + 1,
                            slice_bounds.len()
                        );
                    })?;
                }
            }
            Ok::<(), DataFusionError>(())
        }
        .await;
        if outcome.is_err() {
            skipped += 1;
            continue;
        }
        ok += 1;
        if let Err(e) = std::fs::create_dir_all(&redriven)
            .and_then(|()| std::fs::rename(&path, redriven.join(entry.file_name())))
            .and_then(|()| std::fs::rename(&meta_path, redriven.join(meta_path.file_name().unwrap_or_default())))
        {
            warn!("dml redrive: recovered but could not move {path:?} to redriven/: {e}");
        }
    }
    info!("dml redrive: {ok} group(s) recovered, {skipped} left parked");
    (ok, skipped)
}

/// Parsed `quarantine/dml/*.meta` sidecar for the known hash-enrichment shape.
/// Only what the re-drive needs is machine-recovered; groups whose meta doesn't
/// match this shape stay parked (assignments/predicate are stored as Expr
/// Display strings, which are not generally re-parseable).
#[derive(Debug, PartialEq)]
pub(crate) struct QuarantineMeta {
    pub table_name: String,
    pub project_id: String,
    pub projects: Vec<String>,
    pub join_keys: Vec<(String, String)>,
    pub date_bounds: (i32, i32),
    /// (micros, inclusive_upper) — lower is always inclusive.
    pub ts_lower: i64,
    pub ts_upper: (i64, bool),
    pub rows: usize,
    /// Source carries a `new_hashes` list column (second enrichment shape)
    /// rather than a scalar `tag`.
    pub list_source: bool,
}

pub(crate) fn parse_quarantine_meta(meta: &str) -> Option<QuarantineMeta> {
    let field = |k: &str| meta.lines().find_map(|l| l.strip_prefix(k).and_then(|l| l.strip_prefix('=')).map(str::to_owned));
    let (table_name, project_id, predicate, assignments) = (field("table_name")?, field("project_id")?, field("predicate")?, field("assignments")?);
    // Shape guard: only the two hash-enrichment folds are reconstructible.
    let list_source = assignments.starts_with("hashes:=CASE WHEN o.hashes IS NULL THEN u.new_hashes");
    if !list_source && !assignments.starts_with("hashes:=array_concat(") {
        return None;
    }
    let join_keys: Vec<(String, String)> = field("join_keys")?
        .split(',')
        .filter(|s| !s.is_empty())
        .map(|p| p.split_once('=').map(|(a, b)| (a.to_string(), b.to_string())))
        .collect::<Option<_>>()?;
    let projects: Vec<String> =
        field("folded_projects").filter(|fp| !fp.is_empty()).map_or_else(|| vec![project_id.clone()], |fp| fp.split(',').map(str::to_string).collect());
    // Date32("YYYY-MM-DD") bounds → day numbers since epoch.
    let dates: Vec<i32> = predicate
        .match_indices("Date32(\"")
        .filter_map(|(i, m)| {
            let s = &predicate[i + m.len()..];
            s.split('"').next()?.parse::<chrono::NaiveDate>().ok().map(|d| (d - chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days() as i32)
        })
        .collect();
    let (lo, hi) = (dates.iter().min()?, dates.iter().max()?);
    // `timestamp >= TimestampMicrosecond(N` / `timestamp <[=] TimestampMicrosecond(N`.
    let ts_after = |pat: &str| {
        predicate.find(pat).and_then(|i| {
            let s = &predicate[i + pat.len()..];
            let s = s.split("TimestampMicrosecond(").nth(1)?;
            s.split(|c: char| !c.is_ascii_digit() && c != '-').next()?.parse::<i64>().ok()
        })
    };
    let ts_lower = ts_after("timestamp >= ")?;
    let (ts_upper, upper_incl) = match (ts_after("timestamp <= "), ts_after("timestamp < ")) {
        (Some(t), _) => (t, true),
        (None, Some(t)) => (t, false),
        (None, None) => return None,
    };
    Some(QuarantineMeta {
        table_name,
        project_id,
        projects,
        join_keys,
        date_bounds: (*lo, *hi),
        ts_lower,
        ts_upper: (ts_upper, upper_incl),
        rows: field("rows").and_then(|r| r.parse().ok()).unwrap_or(0),
        list_source,
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct GroupKey {
    project_id: String,
    table_name: String,
    fingerprint: u64,
}

#[derive(Clone)]
struct PendingGroup {
    join_keys: Vec<(String, String)>,
    assignments: Vec<(String, Expr)>,
    predicate: DecomposedPredicate,
    time_col: &'static str,
    schema: SchemaRef,
    /// Statement batches, each with its own time bounds so the drain can
    /// bucket by window ([`bucket_group`]).
    batches: Vec<BoundBatch>,
    /// Freshest enqueuing statement's session — keeps the drain's function
    /// registry identical to what the synchronous merge would have used.
    session: Arc<dyn Session>,
    attempts: u32,
    /// `Some(projects)` marks a drain-time cross-project fold (see
    /// `fold_groups`): batches carry an appended `project_id` column, the
    /// residual carries a `project_id IN (...)` filter, and the watermark
    /// clamp must use the MAX watermark across these projects (a row is
    /// provably buffer-only only when it is unflushed for EVERY member).
    /// `None` for ordinary single-project statement groups.
    folded_projects: Option<Vec<String>>,
}

/// Hash of everything that must match exactly for two statements to share
/// one MERGE: join keys, assignment exprs, residual predicate conjuncts
/// (order-insensitive), and the source schema.
fn shape_fingerprint(join_keys: &[(String, String)], assignments: &[(String, Expr)], residual: &[Expr], schema: &SchemaRef) -> u64 {
    let mut h = std::hash::DefaultHasher::new();
    join_keys.hash(&mut h);
    for (c, e) in assignments {
        (c, e.to_string()).hash(&mut h);
    }
    let mut res: Vec<String> = residual.iter().map(ToString::to_string).collect();
    res.sort_unstable();
    res.hash(&mut h);
    for f in schema.fields() {
        (f.name(), format!("{:?}", f.data_type())).hash(&mut h);
    }
    h.finish()
}

/// `project_id = '<id>'` equality conjunct, either operand order — via the
/// canonical matcher (`extract_project_id_from_expr`) so the routing, DML
/// extraction, and folding shapes can't drift apart. Any other shape stays in
/// the residual and blocks folding for that group.
fn is_project_eq(e: &Expr, project_id: &str) -> bool {
    crate::read::optimizers::extract_project_id_from_expr(e).as_deref() == Some(project_id)
}

/// Fold same-shape single-project groups on unified tables into one group per
/// (table, shape): `project_id` moves from a per-group residual equality into
/// a source column + join key + `IN (...)` partition filter, so one drain
/// issues ONE merge (one kernel metadata scan, one OCC commit) instead of one
/// per project. Data-scan bytes are unchanged — `project_id` is a partition
/// column, so the folded IN-list prunes to exactly the union of the files the
/// per-project merges would have read.
///
/// Groups are eligible only when they carry exactly one `project_id = <own>`
/// residual conjunct (anything else risks changing which rows the predicate
/// matches), their source schema has no `project_id` column yet, and the
/// project stores the table in the unified Delta table (custom-storage
/// projects resolve to physically separate tables — never fold those).
/// Ineligible groups and singleton buckets pass through untouched; any arrow
/// failure while folding a bucket falls back to its unfolded members.
fn fold_groups(groups: Vec<(GroupKey, PendingGroup)>, custom_storage: &HashSet<(String, String)>) -> Vec<(GroupKey, PendingGroup)> {
    // A fold candidate: its enqueue key/group plus the residual with the
    // own-project equality stripped.
    type Member = (GroupKey, PendingGroup, Vec<Expr>);
    let is_eligible = |(key, group, stripped): &Member| {
        group.folded_projects.is_none()
            && !stripped.iter().any(|e| e.column_refs().iter().any(|c| c.name == "project_id"))
            && group.schema.field_with_name("project_id").is_err()
            && !group.join_keys.iter().any(|(t, s)| t == "project_id" || s == "project_id")
            && !custom_storage.contains(&(key.project_id.clone(), key.table_name.clone()))
    };
    // The optimizer usually pushes `project_id = '<id>'` into the TableScan
    // (partition column), so most predicates carry no project conjunct at all —
    // scope rides in `key.project_id`. Strip an explicit own-project equality
    // when present; any OTHER reference to project_id (IN, !=, expressions) is a
    // shape we can't restate as the folded IN-list, so it stays unfolded.
    let (candidates, ineligible): (Vec<Member>, Vec<Member>) = groups
        .into_iter()
        .map(|(key, group)| {
            let stripped: Vec<Expr> = group.predicate.residual.iter().filter(|e| !is_project_eq(e, &key.project_id)).cloned().collect();
            (key, group, stripped)
        })
        .partition(is_eligible);
    let buckets: HashMap<(String, u64), Vec<Member>> = candidates.into_iter().fold(HashMap::new(), |mut m, (key, group, stripped)| {
        let fp = shape_fingerprint(&group.join_keys, &group.assignments, &stripped, &group.schema);
        m.entry((key.table_name.clone(), fp)).or_default().push((key, group, stripped));
        m
    });
    let unfolded = |members: Vec<Member>| members.into_iter().map(|(k, g, _)| (k, g)).collect::<Vec<_>>();
    unfolded(ineligible)
        .into_iter()
        .chain(buckets.into_iter().flat_map(|((table_name, shape_fp), mut members)| {
            if members.len() == 1 {
                return unfolded(members);
            }
            // Deterministic member order → stable IN-list, fingerprint, and rep key.
            members.sort_by(|a, b| a.0.project_id.cmp(&b.0.project_id));
            let total_rows: usize = members.iter().flat_map(|(_, g, _)| &g.batches).map(|(b, _)| b.num_rows()).sum();
            if total_rows > MAX_QUEUED_SOURCE_ROWS {
                return unfolded(members);
            }
            match build_folded(&table_name, shape_fp, &members) {
                Ok(folded) => {
                    debug!("dml coalesce: folded {} projects into one {} merge group", members.len(), table_name);
                    vec![folded]
                }
                Err(e) => {
                    warn!("dml coalesce: folding {} groups for {} failed ({e}), draining per-project", members.len(), table_name);
                    unfolded(members)
                }
            }
        }))
        .collect()
}

/// Assemble the folded group: append a constant `project_id` column to every
/// member batch, widen the union time window, and swap the per-project
/// equality residual for one `project_id IN (...)` filter. The folded
/// GroupKey's fingerprint hashes the shape AND the member set, so a failed
/// fold re-queued via `requeue` can only ever merge with a fold of the exact
/// same members — a different member set gets a different key (mixing them
/// would pin an older IN-list to newer members' rows and silently drop their
/// delta legs).
fn build_folded(table_name: &str, shape_fp: u64, members: &[(GroupKey, PendingGroup, Vec<Expr>)]) -> Result<(GroupKey, PendingGroup)> {
    use datafusion::arrow::{array::StringArray, datatypes::Field};
    let (rep_key, base, stripped) = &members[0];
    let projects: Vec<String> = members.iter().map(|(k, _, _)| k.project_id.clone()).collect();

    let schema: SchemaRef = Arc::new(datafusion::arrow::datatypes::Schema::new(
        base.schema
            .fields()
            .iter()
            .cloned()
            .chain(std::iter::once(Arc::new(Field::new("project_id", datafusion::arrow::datatypes::DataType::Utf8, false))))
            .collect::<Vec<_>>(),
    ));
    let out_schema = &schema; // &SchemaRef is Copy — capturable by both closures without moving `schema`
    let batches: Vec<BoundBatch> = members
        .iter()
        .flat_map(move |(key, group, _)| {
            group.batches.iter().map(move |(batch, bounds)| {
                let project_col = Arc::new(StringArray::from_iter_values(std::iter::repeat_n(key.project_id.as_str(), batch.num_rows())));
                let cols = batch.columns().iter().cloned().chain(std::iter::once(project_col as _)).collect();
                Ok((RecordBatch::try_new(out_schema.clone(), cols).map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?, bounds.clone()))
            })
        })
        .collect::<Result<_>>()?;

    let predicate = members[1..].iter().fold(
        DecomposedPredicate {
            residual: stripped
                .iter()
                .cloned()
                .chain(std::iter::once(datafusion::prelude::col("project_id").in_list(projects.iter().map(|p| lit(p.as_str())).collect(), false)))
                .collect(),
            lower: base.predicate.lower.clone(),
            upper: base.predicate.upper.clone(),
        },
        |mut p, (_, group, _)| {
            p.widen(&group.predicate);
            p
        },
    );

    let mut h = std::hash::DefaultHasher::new();
    shape_fp.hash(&mut h);
    projects.hash(&mut h);
    let key = GroupKey { project_id: rep_key.project_id.clone(), table_name: table_name.to_string(), fingerprint: h.finish() };
    let group = PendingGroup {
        join_keys: base.join_keys.iter().cloned().chain(std::iter::once(("project_id".to_string(), "project_id".to_string()))).collect(),
        assignments: base.assignments.clone(),
        predicate,
        time_col: base.time_col,
        schema,
        batches,
        // Any member's session works: same shape ⇒ same function registry.
        session: base.session.clone(),
        attempts: members.iter().map(|(_, g, _)| g.attempts).max().unwrap_or(0),
        folded_projects: Some(projects),
    };
    Ok((key, group))
}

/// Accumulates deferred `UPDATE ... FROM` Delta legs and drains them as
/// batched merges. One instance per `Database`, created when
/// `TIMEFUSION_DML_COALESCE_SECS > 0`.
/// Manual format: `PendingGroup` holds an `Arc<dyn Session>`, which has no `Debug`.
#[derive(derive_more::Debug)]
#[debug("DmlCoalescer {{ interval_secs: {interval_secs}, queued_rows: {}, .. }}", queued_rows.load(Ordering::Relaxed))]
pub struct DmlCoalescer {
    interval_secs: u64,
    /// See `fold_groups` — cross-project folding of same-shape groups.
    fold: bool,
    groups: std::sync::Mutex<HashMap<GroupKey, PendingGroup>>,
    queued_rows: AtomicUsize,
    drain_notify: Notify,
    /// Serializes drains (timer vs shutdown vs test-triggered).
    drain_lock: tokio::sync::Mutex<()>,
    /// Where terminally-failed groups are parked instead of dropped.
    quarantine_dir: std::path::PathBuf,
}

impl DmlCoalescer {
    pub fn new(interval_secs: u64, fold: bool) -> Self {
        Self {
            interval_secs: interval_secs.max(1),
            fold,
            groups: std::sync::Mutex::new(HashMap::new()),
            queued_rows: AtomicUsize::new(0),
            drain_notify: Notify::new(),
            drain_lock: tokio::sync::Mutex::new(()),
            // try_config: unit tests construct a coalescer without init_config.
            quarantine_dir: crate::config::try_config()
                .map_or_else(|| std::path::PathBuf::from("./data/wal"), |c| c.core.wal_dir())
                .join(crate::write::wal::QUARANTINE_DIR_NAME)
                .join("dml"),
        }
    }

    /// Hold the drain serialization lock so a test can make any concurrent
    /// `drain()` block on lock acquisition (exercises shutdown's deadline).
    #[cfg(test)]
    pub(crate) async fn lock_drain_for_test(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.drain_lock.lock().await
    }

    /// Defer a statement's Delta merge. The caller has already applied the
    /// mem leg (and its WAL append) and verified committed data exists.
    pub fn enqueue(
        &self, project_id: &str, table_name: &str, predicate: Option<&Expr>, assignments: &[(String, Expr)], source: &UpdateSource, session: Arc<dyn Session>,
    ) {
        let time_col = table_time_column(table_name);
        let decomposed = DecomposedPredicate::decompose(predicate, time_col);
        let key = GroupKey {
            project_id: project_id.to_string(),
            table_name: table_name.to_string(),
            fingerprint: shape_fingerprint(&source.join_keys, assignments, &decomposed.residual, &source.schema),
        };
        let rows = source.batch.num_rows();
        let bounds = (decomposed.lower.clone(), decomposed.upper.clone());
        {
            let mut groups = self.groups.lock().expect("dml coalescer mutex poisoned");
            match groups.entry(key) {
                Entry::Occupied(mut g) => {
                    let g = g.get_mut();
                    g.predicate.widen(&decomposed);
                    g.batches.push((source.batch.clone(), bounds));
                    g.session = session;
                }
                Entry::Vacant(v) => {
                    v.insert(PendingGroup {
                        join_keys: source.join_keys.clone(),
                        assignments: assignments.to_vec(),
                        predicate: decomposed,
                        time_col,
                        schema: source.schema.clone(),
                        batches: vec![(source.batch.clone(), bounds)],
                        session,
                        attempts: 0,
                        folded_projects: None,
                    });
                }
            }
        }
        crate::observability::record_dml_coalesce_enqueued();
        if self.queued_rows.fetch_add(rows, Ordering::Relaxed) + rows > MAX_QUEUED_SOURCE_ROWS {
            self.drain_notify.notify_one();
        }
    }

    /// Drain every pending group: clamp the widened window to the flush
    /// watermark, split into duplicate-key-free rounds, and run one merge per
    /// round. Failed groups are re-queued (merging with anything enqueued
    /// meanwhile) up to `MAX_DRAIN_ATTEMPTS`, then quarantined.
    pub async fn drain(&self, db: &crate::database::Database) {
        let _serial = self.drain_lock.lock().await;
        let groups: Vec<(GroupKey, PendingGroup)> = {
            let mut g = self.groups.lock().expect("dml coalescer mutex poisoned");
            self.queued_rows.store(0, Ordering::Relaxed);
            g.drain().collect()
        };
        let groups = if self.fold && groups.len() > 1 { fold_groups(groups, &db.custom_storage_keys().await) } else { groups };
        // Bucket after folding so each merge unit's target window stays ∝
        // DML_MERGE_BUCKET_MICROS, not the group's union window (see const).
        for (key, mut group) in groups.into_iter().flat_map(|(key, g)| bucket_group(g).into_iter().map(move |b| (key.clone(), b))) {
            if let Some(layer) = db.buffered_layer() {
                // Folded groups clamp against the MAX member watermark: a row
                // is excludable only when it is unflushed for every member.
                let wm = match &group.folded_projects {
                    Some(ps) => ps.iter().map(|p| layer.delta_flushed_watermark(p, &key.table_name)).max().unwrap_or(i64::MIN),
                    None => layer.delta_flushed_watermark(&key.project_id, &key.table_name),
                };
                if matches!(clamp_decomposed(&mut group.predicate, wm), ClampAction::Skip) {
                    crate::observability::record_dml_delta_leg_skipped();
                    debug!("dml coalesce: skipping {}/{} group — window entirely above flush watermark", key.project_id, key.table_name);
                    continue;
                }
            }
            // A failure in any prep step (schema drift within a
            // fingerprint-matched group, missing join key, row conversion) is
            // a bug, not an operational state — but the rows are still
            // unapplied in Delta, so park them rather than drop them.
            let park_group = |stage: &str, e: &dyn std::fmt::Display, batches: &[RecordBatch]| {
                if !quarantine_group(&self.quarantine_dir, &key, &group, batches, &format!("{stage} failed: {e}")) {
                    crate::observability::record_dml_coalesce_dropped();
                    error!("dml coalesce: {stage} failed for {}/{} — rows LOST: {e}", key.project_id, key.table_name);
                }
            };
            let statements = group.batches.len();
            let merged = match concat_batches(&group.schema, group.batches.iter().map(|(b, _)| b)) {
                Ok(b) => b,
                Err(e) => {
                    park_group("concat", &e, &group.batches.iter().map(|(b, _)| b.clone()).collect::<Vec<_>>());
                    continue;
                }
            };
            if merged.num_rows() == 0 {
                continue;
            }
            let key_indices: Result<Vec<usize>> = group.join_keys.iter().map(|(_, s)| Ok(merged.schema().index_of(s)?)).collect();
            let rounds = match key_indices.and_then(|idx| split_rounds(&merged, &idx)) {
                Ok(r) => r,
                Err(e) => {
                    park_group("round split", &e, std::slice::from_ref(&merged));
                    continue;
                }
            };
            let predicate = group.predicate.reconstruct(group.time_col);
            // Chunk each round to bound per-MERGE memory (see MAX_MERGE_ROWS).
            // Sequential awaits with `?`: the first failure abandons the group.
            let outcome = async {
                for round in rounds.iter().flat_map(|r| chunk_rows(r, MAX_MERGE_ROWS)) {
                    let source = UpdateSource { batch: round, schema: group.schema.clone(), join_keys: group.join_keys.clone() };
                    let rows =
                        merge_bisect(db, &key.table_name, &key.project_id, predicate.clone(), group.assignments.clone(), source, group.session.clone()).await?;
                    crate::observability::record_dml_coalesce_merge();
                    debug!("dml coalesce: merged {statements} stmts for {}/{} — {rows} rows updated", key.project_id, key.table_name);
                }
                Ok::<(), DataFusionError>(())
            }
            .await;
            if let Err(e) = outcome {
                group.attempts += 1;
                if group.attempts >= MAX_DRAIN_ATTEMPTS {
                    // Park, don't drop: the mem leg already applied and the
                    // Delta leg targets rows no longer in the buffer, so a
                    // dropped group is permanent divergence with no self-heal.
                    let reason = format!("{} failed drains: {e}", group.attempts);
                    if !quarantine_group(&self.quarantine_dir, &key, &group, std::slice::from_ref(&merged), &reason) {
                        crate::observability::record_dml_coalesce_dropped();
                        error!(
                            "dml coalesce: LOST {}/{} group after {} failed drains ({statements} stmts, {} rows) — quarantine write failed: {e}",
                            key.project_id,
                            key.table_name,
                            group.attempts,
                            merged.num_rows()
                        );
                    }
                } else {
                    warn!(
                        "dml coalesce: drain failed for {}/{} (attempt {}/{MAX_DRAIN_ATTEMPTS}), re-queueing: {e}",
                        key.project_id, key.table_name, group.attempts
                    );
                    self.requeue(key, group);
                }
            }
        }
    }

    /// Put a failed group back, merging with statements enqueued during the
    /// drain. The failed batches are older, so they go in front to preserve
    /// per-key round order; the group's newer session wins.
    fn requeue(&self, key: GroupKey, group: PendingGroup) {
        let rows: usize = group.batches.iter().map(|(b, _)| b.num_rows()).sum();
        let mut groups = self.groups.lock().expect("dml coalescer mutex poisoned");
        match groups.entry(key) {
            Entry::Occupied(mut g) => {
                let newer = g.get_mut();
                newer.batches = group.batches.into_iter().chain(std::mem::take(&mut newer.batches)).collect();
                newer.predicate.widen(&group.predicate);
                newer.attempts = newer.attempts.max(group.attempts);
            }
            Entry::Vacant(v) => {
                v.insert(group);
            }
        }
        self.queued_rows.fetch_add(rows, Ordering::Relaxed);
    }

    /// Background drain loop: timer, queue-pressure notify, or shutdown (which
    /// runs one final drain so the stop-grace window flushes pending merges).
    pub async fn run(self: Arc<Self>, db: crate::database::Database, shutdown: CancellationToken) {
        info!("DML coalescer started (interval {}s)", self.interval_secs);
        loop {
            tokio::select! {
                _ = tokio::time::sleep(std::time::Duration::from_secs(self.interval_secs)) => {}
                _ = self.drain_notify.notified() => {}
                _ = shutdown.cancelled() => {
                    self.drain(&db).await;
                    info!("DML coalescer stopped");
                    return;
                }
            }
            self.drain(&db).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::{
        arrow::{
            array::{Int64Array, StringArray},
            datatypes::{DataType, Field, Schema},
        },
        prelude::col,
    };

    use super::*;

    fn ts(micros: i64) -> ScalarValue {
        ScalarValue::TimestampMicrosecond(Some(micros), Some("UTC".into()))
    }

    fn window(lo: i64, hi: i64) -> Expr {
        col("timestamp").gt_eq(lit(ts(lo))).and(col("timestamp").lt(lit(ts(hi))))
    }

    /// The window a DML invalidation of the hot tier is scoped to. It must be a
    /// SUPERSET of the touched rows in every shape it accepts, and `None`
    /// (→ full-table invalidation) in every shape it doesn't.
    #[test]
    fn dml_time_window_derives_supersets_or_nothing() {
        let w = |e: Option<&Expr>| dml_time_window(e, "timestamp");
        // The monoscope enrichment shape: `>= lo AND < hi`. Both ends round
        // outward by design, so the window is [lo, hi+1) — a superset.
        assert_eq!(w(Some(&window(100, 200))), Some((100, 201)));
        // Inclusive/exclusive on either end, and reversed operands.
        assert_eq!(w(Some(&col("timestamp").gt(lit(ts(100))).and(col("timestamp").lt_eq(lit(ts(200)))))), Some((100, 201)));
        assert_eq!(w(Some(&lit(ts(100)).lt_eq(col("timestamp")))), Some((100, i64::MAX)));
        // One-sided bounds keep the side they have.
        assert_eq!(w(Some(&col("timestamp").gt_eq(lit(ts(100))))), Some((100, i64::MAX)));
        assert_eq!(w(Some(&col("timestamp").lt(lit(ts(200))))), Some((i64::MIN, 201)));
        assert_eq!(w(Some(&col("timestamp").eq(lit(ts(100))))), Some((100, 101)));
        // A cast-wrapped literal (extended-protocol param binding — the 2026-07-20
        // shape that silently disabled the merge date-prune) still derives.
        let cast = Expr::Cast(datafusion::logical_expr::expr::Cast::new(
            Box::new(lit(ts(100))),
            DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Nanosecond, Some("UTC".into())),
        ));
        assert_eq!(w(Some(&col("timestamp").gt_eq(cast))), Some((100, i64::MAX)));
        // Sub-µs literals floor outward, never inward.
        let ns = |n: i64| lit(ScalarValue::TimestampNanosecond(Some(n), Some("UTC".into())));
        assert_eq!(w(Some(&col("timestamp").gt(ns(1_500)).and(col("timestamp").lt(ns(9_500))))), Some((1, 10)));
        // Residual conjuncts are irrelevant; the time bound still applies.
        assert_eq!(w(Some(&col("project_id").eq(lit("p")).and(window(100, 200)))), Some((100, 201)));

        // No derivable bound → caller must invalidate the whole table.
        assert_eq!(w(None), None);
        assert_eq!(w(Some(&col("project_id").eq(lit("p")))), None);
        // OR is not an AND-conjunct, NOT is not a comparison, a non-literal
        // bound is not a bound, and a different column is not the time column.
        assert_eq!(w(Some(&window(100, 200).or(col("id").eq(lit("x"))))), None);
        assert_eq!(w(Some(&!col("timestamp").gt_eq(lit(ts(100))))), None);
        assert_eq!(w(Some(&col("timestamp").gt_eq(col("other_ts")))), None);
        assert_eq!(w(Some(&col("observed_timestamp").gt_eq(lit(ts(100))))), None);
    }

    /// Regression guard for the 2026-07-27 04:42Z loss: the terminal drain
    /// branch dropped 1_252_311 enrichment rows with only an `error!`. The
    /// rows must instead land on disk, self-describing and re-drivable.
    // #[serial]: asserts exact deltas on the process-global DML_STATS counters.
    #[test]
    #[serial_test::serial]
    fn terminal_failure_quarantines_rows_recoverably() {
        let dir = tempfile::tempdir().unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false), Field::new("n", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(vec!["a", "b"])), Arc::new(Int64Array::from(vec![1, 2]))]).unwrap();
        let key = GroupKey { project_id: "proj/1".into(), table_name: "otel_logs_and_spans".into(), fingerprint: 7 };
        let group = PendingGroup {
            join_keys: vec![("id".into(), "id".into())],
            assignments: vec![("n".into(), lit(9i64))],
            predicate: DecomposedPredicate::decompose(Some(&window(100, 200)), "timestamp"),
            time_col: "timestamp",
            schema: schema.clone(),
            batches: vec![(batch.clone(), (None, None))],
            session: Arc::new(datafusion::prelude::SessionContext::new().state()),
            attempts: MAX_DRAIN_ATTEMPTS,
            folded_projects: Some(vec!["proj/1".into(), "proj2".into()]),
        };

        let before = crate::observability::dml_stats().coalesce_quarantined.load(Ordering::Relaxed);
        assert!(quarantine_group(dir.path(), &key, &group, std::slice::from_ref(&batch), "resources exhausted"));
        assert_eq!(crate::observability::dml_stats().coalesce_quarantined.load(Ordering::Relaxed), before + 1);

        let files: Vec<_> = std::fs::read_dir(dir.path()).unwrap().map(|e| e.unwrap().path()).collect();
        let arrow = files.iter().find(|p| p.extension().is_some_and(|e| e == "arrow")).expect("no .arrow payload");
        let meta = std::fs::read_to_string(files.iter().find(|p| p.extension().is_some_and(|e| e == "meta")).expect("no .meta")).unwrap();

        // Payload round-trips through Arrow IPC with rows intact — this is
        // what makes a re-drive possible at all.
        let f = std::fs::File::open(arrow).unwrap();
        let reader = datafusion::arrow::ipc::reader::FileReader::try_new(f, None).unwrap();
        let read: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
        assert_eq!(read.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        assert_eq!(read[0].schema().fields().len(), 2);

        // Sidecar carries the merge shape a re-drive needs.
        for want in ["project_id=proj/1", "rows=2", "join_keys=id=id", "reason=resources exhausted", "folded_projects=proj/1,proj2"] {
            assert!(meta.contains(want), "meta missing {want:?}:\n{meta}");
        }
        assert!(meta.contains("assignments=n:="), "meta missing assignments:\n{meta}");

        // Filename must not smuggle the '/' from project_id into a subdir.
        assert!(arrow.parent().unwrap() == dir.path(), "payload escaped the quarantine dir");
    }

    /// Two parks of the same project/table must never overwrite each other,
    /// even under a frozen clock (e2e virtual time) — an overwrite would be
    /// exactly the silent loss this whole mechanism exists to prevent. And a
    /// batch that cannot be parked must page via `coalesce_dropped`, not hide
    /// behind the recoverable-looking `coalesce_quarantined`.
    #[test]
    #[serial_test::serial]
    fn parks_are_collision_proof_and_partial_loss_pages() {
        let dir = tempfile::tempdir().unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
        let key = GroupKey { project_id: "p".into(), table_name: "t".into(), fingerprint: 1 };
        let mut group = PendingGroup {
            join_keys: vec![("n".into(), "n".into())],
            assignments: vec![("n".into(), lit(1i64))],
            predicate: DecomposedPredicate::decompose(None, "timestamp"),
            time_col: "timestamp",
            schema: schema.clone(),
            batches: vec![(batch.clone(), (None, None))],
            session: Arc::new(datafusion::prelude::SessionContext::new().state()),
            attempts: MAX_DRAIN_ATTEMPTS,
            folded_projects: None,
        };

        for _ in 0..3 {
            assert!(quarantine_group(dir.path(), &key, &group, std::slice::from_ref(&batch), "x"));
        }
        let payloads = std::fs::read_dir(dir.path()).unwrap().filter(|e| e.as_ref().unwrap().path().extension().is_some_and(|x| x == "arrow")).count();
        assert_eq!(payloads, 3, "parks overwrote each other instead of getting distinct names");

        // A batch whose schema differs from the group's cannot go in the IPC
        // file; that is real loss and must bump the paging metric.
        let other =
            RecordBatch::try_new(Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)])), vec![Arc::new(StringArray::from(vec!["z"]))]).unwrap();
        group.batches = vec![(batch.clone(), (None, None)), (other.clone(), (None, None))];
        let dropped_before = crate::observability::dml_stats().coalesce_quarantined.load(Ordering::Relaxed);
        assert!(quarantine_group(dir.path(), &key, &group, &[batch.clone(), other], "mixed"));
        assert_eq!(crate::observability::dml_stats().coalesce_quarantined.load(Ordering::Relaxed), dropped_before + 1);
        let meta = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().is_some_and(|x| x == "meta"))
            .map(|p| std::fs::read_to_string(p).unwrap())
            .find(|m| m.contains("reason=mixed"))
            .expect("no meta for the mixed-schema park");
        // Only the matching batch's row is accounted as parked.
        assert!(meta.contains("rows=1"), "meta over-claims parked rows:\n{meta}");
    }

    /// A round larger than `MAX_MERGE_ROWS` must be fed to Delta as several
    /// bounded slices covering every row exactly once — one unbounded MERGE is
    /// what exhausted memory on 2026-07-27.
    #[test]
    fn oversized_round_chunks_to_bounded_merges() {
        let rows = MAX_MERGE_ROWS * 2 + 7;
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
        let round = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from_iter_values(0..rows as i64))]).unwrap();

        let chunks: Vec<RecordBatch> = chunk_rows(&round, MAX_MERGE_ROWS).collect();

        assert_eq!(chunks.len(), 3);
        assert!(chunks.iter().all(|c| c.num_rows() <= MAX_MERGE_ROWS));
        assert_eq!(chunks.iter().map(RecordBatch::num_rows).sum::<usize>(), rows, "chunking must not drop or duplicate rows");
    }

    #[test]
    fn decompose_extracts_bounds_and_residual() {
        let pred = col("project_id").eq(lit("p1")).and(window(100, 200));
        let d = DecomposedPredicate::decompose(Some(&pred), "timestamp");
        assert_eq!(d.lower, Some(TimeBound { value: ts(100), inclusive: true }));
        assert_eq!(d.upper, Some(TimeBound { value: ts(200), inclusive: false }));
        assert_eq!(d.residual.len(), 1);
        // Round-trip preserves the conjunction (order may differ).
        let rebuilt = d.reconstruct("timestamp").unwrap();
        let parts: Vec<String> = split_conjunction(&rebuilt).iter().map(ToString::to_string).collect();
        assert_eq!(parts.len(), 3);
        assert!(parts.iter().any(|p| p.contains("project_id")));
    }

    #[test]
    fn widen_takes_union_window() {
        let mut a = DecomposedPredicate::decompose(Some(&window(100, 200)), "timestamp");
        let b = DecomposedPredicate::decompose(Some(&window(50, 150)), "timestamp");
        a.widen(&b);
        assert_eq!(a.lower.unwrap().value, ts(50));
        assert_eq!(a.upper.unwrap().value, ts(200));

        // A statement without an upper bound widens the union to unbounded.
        let mut a = DecomposedPredicate::decompose(Some(&window(100, 200)), "timestamp");
        let unbounded = DecomposedPredicate::decompose(Some(&col("timestamp").gt_eq(lit(ts(10)))), "timestamp");
        a.widen(&unbounded);
        assert_eq!(a.lower.unwrap().value, ts(10));
        assert!(a.upper.is_none());
    }

    #[test]
    fn clamp_skips_fully_unflushed_window() {
        let pred = window(1_000, 2_000);
        assert!(matches!(clamp_to_watermark(Some(&pred), "timestamp", 500), WatermarkClamp::SkipDelta));
        // Exclusive lower exactly at the watermark also skips.
        let pred = col("timestamp").gt(lit(ts(500)));
        assert!(matches!(clamp_to_watermark(Some(&pred), "timestamp", 500), WatermarkClamp::SkipDelta));
        // Inclusive lower at the watermark must keep (row at wm may be flushed).
        let pred = col("timestamp").gt_eq(lit(ts(500)));
        assert!(matches!(clamp_to_watermark(Some(&pred), "timestamp", 500), WatermarkClamp::Keep(_)));
    }

    #[test]
    fn clamp_tightens_upper_bound_to_watermark() {
        let pred = window(100, 2_000);
        match clamp_to_watermark(Some(&pred), "timestamp", 500) {
            WatermarkClamp::Keep(Some(p)) => {
                let d = DecomposedPredicate::decompose(Some(&p), "timestamp");
                assert_eq!(d.upper, Some(TimeBound { value: ts(500), inclusive: true }));
                assert_eq!(d.lower.unwrap().value, ts(100));
            }
            _ => panic!("expected clamped predicate"),
        }
        // Window already below the watermark: untouched.
        let pred = window(100, 300);
        match clamp_to_watermark(Some(&pred), "timestamp", 500) {
            WatermarkClamp::Keep(Some(p)) => assert_eq!(p, pred),
            _ => panic!("expected unchanged predicate"),
        }
        // No time bounds at all: nothing to clamp against.
        let pred = col("project_id").eq(lit("p1"));
        match clamp_to_watermark(Some(&pred), "timestamp", 500) {
            WatermarkClamp::Keep(Some(p)) => assert_eq!(p, pred),
            _ => panic!("expected unchanged predicate"),
        }
    }

    #[test]
    fn parse_quarantine_meta_recovers_enrichment_shape() {
        // Verbatim shape of the 2026-07-28..30 parked groups.
        let meta = "project_id=00000000-0000-0000-0000-000000000000\n\
            table_name=otel_logs_and_spans\n\
            folded_projects=00000000-0000-0000-0000-000000000000,28f62f01-46a1-400e-8195-da7bc3505b5b\n\
            join_keys=context___span_id=span_id,context___trace_id=trace_id,project_id=project_id\n\
            assignments=hashes:=array_concat(CASE WHEN o.hashes IS NOT NULL THEN o.hashes ELSE List([]) END, make_array(u.tag))\n\
            predicate=otel_logs_and_spans.context___span_id IS NOT NULL AND date >= Date32(\"2026-07-30\") AND date <= Date32(\"2026-07-30\") AND project_id IN ([]) AND timestamp >= TimestampMicrosecond(1785408669559194, Some(\"UTC\")) AND timestamp < TimestampMicrosecond(1785410181576000, Some(\"UTC\"))\n\
            time_col=timestamp\nattempts=3\nrows=766\nstatements=20\nreason=3 failed drains: x\n";
        let m = parse_quarantine_meta(meta).expect("parses");
        assert_eq!(m.projects.len(), 2);
        assert_eq!(m.join_keys.len(), 3);
        assert_eq!(m.date_bounds, (20664, 20664)); // 2026-07-30
        assert_eq!(m.ts_lower, 1785408669559194);
        assert_eq!(m.ts_upper, (1785410181576000, false));
        assert_eq!(m.rows, 766);
        // The list-source shape parses too; unknown shapes stay parked.
        let list = meta.replace(
            "hashes:=array_concat(CASE WHEN o.hashes IS NOT NULL THEN o.hashes ELSE List([]) END, make_array(u.tag))",
            "hashes:=CASE WHEN o.hashes IS NULL THEN u.new_hashes ELSE array_concat(o.hashes, u.new_hashes) END",
        );
        assert!(parse_quarantine_meta(&list).is_some_and(|m| m.list_source));
        assert!(parse_quarantine_meta(&meta.replace("hashes:=array_concat(", "other:=fn(")).is_none());
    }

    #[test]
    fn split_rounds_separates_duplicate_keys_and_drops_exact_dups() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("span_id", DataType::Utf8, false),
            Field::new("tag", DataType::Utf8, false),
            Field::new("n", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "a", "a", "b"])),
                Arc::new(StringArray::from(vec!["t1", "t1", "t2", "t1", "t1"])),
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 2])),
            ],
        )
        .unwrap();
        // Key = span_id. Rows: a/t1/1, b/t1/2, a/t2/3, a/t1/4, b/t1/2(exact dup).
        let rounds = split_rounds(&batch, &[0]).unwrap();
        assert_eq!(rounds.len(), 3, "key 'a' has 3 distinct payloads");
        assert_eq!(rounds[0].num_rows(), 2); // a/t1/1, b/t1/2
        assert_eq!(rounds[1].num_rows(), 1); // a/t2/3
        assert_eq!(rounds[2].num_rows(), 1); // a/t1/4
        let total: usize = rounds.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total, 4, "exact duplicate dropped");
    }

    #[test]
    fn is_project_eq_matches_only_exact_literal_equality() {
        assert!(is_project_eq(&col("project_id").eq(lit("p1")), "p1"));
        assert!(is_project_eq(&lit("p1").eq(col("project_id")), "p1")); // swapped operands
        assert!(!is_project_eq(&col("project_id").eq(lit("p2")), "p1")); // other project
        assert!(!is_project_eq(&col("project_id").not_eq(lit("p1")), "p1")); // wrong op
        assert!(!is_project_eq(&col("other_col").eq(lit("p1")), "p1")); // wrong column
    }

    /// Pin the folded group's whole shape: appended project column + join key,
    /// IN-list residual, union window, and a member-set-sensitive fingerprint
    /// (a re-queued fold must never merge with a fold of different members —
    /// its IN-list would drop the extra members' delta legs).
    #[test]
    fn build_folded_appends_project_column_and_unions_windows() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new("span_id", DataType::Utf8, false), Field::new("tag", DataType::Utf8, false)]));
        let batch = |ids: &[&str]| {
            RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(ids.to_vec())), Arc::new(StringArray::from(vec!["t"; ids.len()]))]).unwrap()
        };
        let member = |project: &str, lo: i64, hi: i64, ids: &[&str]| {
            let pred = col("project_id").eq(lit(project)).and(window(lo, hi));
            let d = DecomposedPredicate::decompose(Some(&pred), "timestamp");
            let stripped: Vec<Expr> = d.residual.iter().filter(|e| !is_project_eq(e, project)).cloned().collect();
            assert!(stripped.is_empty());
            let bounds = (d.lower.clone(), d.upper.clone());
            let group = PendingGroup {
                join_keys: vec![("context___span_id".into(), "span_id".into())],
                assignments: vec![("hashes".into(), col("source.tag"))],
                predicate: d,
                time_col: "timestamp",
                schema: schema.clone(),
                batches: vec![(batch(ids), bounds)],
                session: Arc::new(datafusion::execution::SessionStateBuilder::new().build()),
                attempts: 0,
                folded_projects: None,
            };
            (GroupKey { project_id: project.into(), table_name: "otel_logs_and_spans".into(), fingerprint: 1 }, group, stripped)
        };
        let members = vec![member("p1", 100, 200, &["a", "b"]), member("p2", 50, 150, &["c"])];
        let (key, folded) = build_folded("otel_logs_and_spans", 42, &members).unwrap();

        // Schema + batches: project_id appended as a constant column per member.
        assert_eq!(folded.schema.fields().last().unwrap().name(), "project_id");
        assert_eq!(folded.batches.len(), 2);
        let projects_of = |b: &RecordBatch| b.column(2).as_any().downcast_ref::<StringArray>().unwrap().value(0).to_string();
        assert_eq!(projects_of(&folded.batches[0].0), "p1");
        assert_eq!(projects_of(&folded.batches[1].0), "p2");
        // Join keys extended; window is the union; residual is the IN-list.
        assert_eq!(folded.join_keys.last().unwrap(), &("project_id".to_string(), "project_id".to_string()));
        assert_eq!(folded.predicate.lower.as_ref().unwrap().value, ts(50));
        assert_eq!(folded.predicate.upper.as_ref().unwrap().value, ts(200));
        assert_eq!(folded.predicate.residual.len(), 1);
        assert!(folded.predicate.residual[0].to_string().contains("IN"));
        assert_eq!(folded.folded_projects.as_deref(), Some(&["p1".to_string(), "p2".to_string()][..]));

        // Member-set-sensitive key: same shape, different members → different fingerprint.
        let (key2, _) = build_folded("otel_logs_and_spans", 42, &members[..1]).unwrap();
        assert_ne!(key.fingerprint, key2.fingerprint);

        // Folded batches concat + round-split cleanly under the widened keys.
        let merged = concat_batches(&folded.schema, folded.batches.iter().map(|(b, _)| b)).unwrap();
        assert_eq!(merged.num_rows(), 3);
        let src = UpdateSource { batch: merged, schema: folded.schema.clone(), join_keys: folded.join_keys.clone() };
        assert_eq!(split_source_rounds(src).unwrap().len(), 1, "distinct (span, project) keys must stay in one round");
    }

    /// Build a group whose statements each carry their own window, the way
    /// `enqueue` records them (group predicate widened, per-batch bounds kept).
    fn group_with_windows(windows: &[(i64, Option<i64>)]) -> PendingGroup {
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(vec!["a"]))]).unwrap();
        let decomposed: Vec<DecomposedPredicate> = windows
            .iter()
            .map(|&(lo, hi)| {
                let pred = match hi {
                    Some(hi) => col("service").eq(lit("s1")).and(window(lo, hi)),
                    None => col("service").eq(lit("s1")).and(col("timestamp").gt_eq(lit(ts(lo)))),
                };
                DecomposedPredicate::decompose(Some(&pred), "timestamp")
            })
            .collect();
        let predicate = decomposed[1..].iter().fold(decomposed[0].clone(), |mut p, d| {
            p.widen(d);
            p
        });
        PendingGroup {
            join_keys: vec![("id".into(), "id".into())],
            assignments: vec![("n".into(), lit(9i64))],
            predicate,
            time_col: "timestamp",
            schema,
            batches: decomposed.into_iter().map(|d| (batch.clone(), (d.lower, d.upper))).collect(),
            session: Arc::new(datafusion::prelude::SessionContext::new().state()),
            attempts: 2,
            folded_projects: None,
        }
    }

    const B: i64 = DML_MERGE_BUCKET_MICROS;

    /// Statements in distinct 5-min buckets become separate merge units whose
    /// time bounds cover only their own statements — not the group union.
    #[test]
    fn bucket_group_splits_distinct_buckets_with_narrowed_bounds() {
        let units = bucket_group(group_with_windows(&[(0, Some(60)), (10, Some(90)), (2 * B, Some(2 * B + 60))]));
        assert_eq!(units.len(), 2);
        assert_eq!(units[0].batches.len(), 2);
        assert_eq!(units[0].predicate.lower.as_ref().unwrap().value, ts(0));
        assert_eq!(units[0].predicate.upper.as_ref().unwrap().value, ts(90));
        assert_eq!(units[1].batches.len(), 1);
        assert_eq!(units[1].predicate.lower.as_ref().unwrap().value, ts(2 * B));
        assert_eq!(units[1].predicate.upper.as_ref().unwrap().value, ts(2 * B + 60));
        // Everything but the time window is carried through unchanged.
        for u in &units {
            assert_eq!(u.predicate.residual.len(), 1);
            assert!(u.predicate.residual[0].to_string().contains("service"));
            assert_eq!(u.attempts, 2);
            assert_eq!(u.join_keys, vec![("id".to_string(), "id".to_string())]);
        }
    }

    /// A statement spanning a bucket boundary keeps its FULL window (own unit),
    /// never narrowed to either bucket.
    #[test]
    fn bucket_group_never_narrows_spanning_statement() {
        let units = bucket_group(group_with_windows(&[(B - 10, Some(B + 10)), (0, Some(50))]));
        assert_eq!(units.len(), 2);
        let spanning = units.iter().find(|u| u.batches.len() == 1 && u.predicate.upper.as_ref().unwrap().value == ts(B + 10)).expect("spanning unit");
        assert_eq!(spanning.predicate.lower.as_ref().unwrap().value, ts(B - 10));
    }

    /// A group whose statements all share one bucket drains exactly as today:
    /// one merge unit with the union window.
    #[test]
    fn bucket_group_single_bucket_is_one_unit() {
        let units = bucket_group(group_with_windows(&[(0, Some(50)), (60, Some(100))]));
        assert_eq!(units.len(), 1);
        assert_eq!(units[0].batches.len(), 2);
        assert_eq!(units[0].predicate.lower.as_ref().unwrap().value, ts(0));
        assert_eq!(units[0].predicate.upper.as_ref().unwrap().value, ts(100));
    }

    /// Unbounded statements can't be bucketed — they share a catch-all unit
    /// whose window stays unbounded; bounded statements still bucket narrowly.
    #[test]
    fn bucket_group_unbounded_statements_share_catchall() {
        let units = bucket_group(group_with_windows(&[(0, Some(50)), (10, None)]));
        assert_eq!(units.len(), 2);
        let unbounded = units.iter().find(|u| u.predicate.upper.is_none()).expect("catch-all unit");
        assert_eq!(unbounded.predicate.lower.as_ref().unwrap().value, ts(10));
        let bounded = units.iter().find(|u| u.predicate.upper.is_some()).unwrap();
        assert_eq!(bounded.predicate.upper.as_ref().unwrap().value, ts(50));
        // No statement is dropped or duplicated by bucketing.
        assert_eq!(units.iter().map(|u| u.batches.len()).sum::<usize>(), 2);
    }

    /// An exclusive upper exactly on a bucket edge still belongs to the bucket
    /// below the edge (the window contains no row at the boundary).
    #[test]
    fn bucket_group_exclusive_upper_on_edge_stays_below() {
        let units = bucket_group(group_with_windows(&[(0, Some(B)), (10, Some(20))]));
        assert_eq!(units.len(), 1);
    }

    #[test]
    fn bucket_group_empty_group_passes_through() {
        let mut g = group_with_windows(&[(0, Some(50))]);
        g.batches.clear();
        assert_eq!(bucket_group(g).into_iter().map(|u| u.batches.len()).sum::<usize>(), 0);
    }

    #[test]
    fn fingerprint_groups_same_shape_only() {
        let jk = vec![("context___span_id".to_string(), "span_id".to_string())];
        let assign = vec![("hashes".to_string(), col("source.tag"))];
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new("span_id", DataType::Utf8, false)]));
        let d1 = DecomposedPredicate::decompose(Some(&col("project_id").eq(lit("p1")).and(window(1, 2))), "timestamp");
        let d2 = DecomposedPredicate::decompose(Some(&window(5, 9).and(col("project_id").eq(lit("p1")))), "timestamp");
        // Same residual (order-insensitive), different windows → same group.
        assert_eq!(shape_fingerprint(&jk, &assign, &d1.residual, &schema), shape_fingerprint(&jk, &assign, &d2.residual, &schema));
        // Different residual constant → different group.
        let d3 = DecomposedPredicate::decompose(Some(&col("project_id").eq(lit("p2")).and(window(1, 2))), "timestamp");
        assert_ne!(shape_fingerprint(&jk, &assign, &d1.residual, &schema), shape_fingerprint(&jk, &assign, &d3.residual, &schema));
    }
}
