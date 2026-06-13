use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::RecordBatch,
        compute::concat_batches,
        datatypes::{DataType, Field, Schema, SchemaRef},
    },
    catalog::Session,
    common::{Column, JoinType, Result},
    error::DataFusionError,
    execution::{
        SendableRecordBatchStream, SessionStateBuilder, TaskContext,
        context::{QueryPlanner, SessionState},
    },
    logical_expr::{BinaryExpr, Expr, Join, LogicalPlan, Operator, WriteOp},
    physical_plan::{DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, PlanProperties, stream::RecordBatchStreamAdapter},
    physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner},
};
use futures::StreamExt;
use tracing::{Instrument, error, field::Empty, info, instrument, warn};

use crate::{buffered_write_layer::BufferedWriteLayer, database::Database};

/// Hard cap on the number of source rows we'll materialize for an `UPDATE ... FROM`.
/// Beyond this we error rather than blowing memory; the caller must page or pre-aggregate.
const MAX_UPDATE_SOURCE_ROWS: usize = 1_000_000;

/// Build a clean SessionState with config + runtime from the given session but with
/// delta-rs's DeltaPlanner instead of our custom DmlQueryPlanner.
fn delta_session_from(session: &SessionState) -> Arc<dyn Session> {
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
    pub batch:     RecordBatch,
    pub schema:    SchemaRef,
    /// `(target_col, source_col)` pairs. Names refer to bare column names;
    /// table qualifiers are stripped during extraction.
    pub join_keys: Vec<(String, String)>,
}

/// Output of [`extract_dml_info`]: parsed DML shape, with an unmaterialized
/// source plan when the input contained a `Join` (i.e. `UPDATE ... FROM`).
/// Materialization runs asynchronously in [`DmlQueryPlanner::create_physical_plan`].
pub struct DmlInfo {
    pub table_name:  String,
    pub project_id:  String,
    pub predicate:   Option<Expr>,
    pub assignments: Option<Vec<(String, Expr)>>,
    /// Source plan + join keys when the input contained a `Join`. Materialized
    /// into [`UpdateSource`] before the physical [`DmlExec`] is constructed.
    pub source_plan: Option<UpdateSourcePlan>,
}

#[derive(Clone)]
pub struct UpdateSourcePlan {
    pub plan:      LogicalPlan,
    pub join_keys: Vec<(String, String)>,
}

/// Custom query planner that intercepts DML operations
#[derive(derive_more::Debug)]
pub struct DmlQueryPlanner {
    #[debug(skip)]
    planner:        DefaultPhysicalPlanner,
    #[debug(skip)]
    database:       Arc<Database>,
    #[debug(skip)]
    buffered_layer: Option<Arc<BufferedWriteLayer>>,
}

impl DmlQueryPlanner {
    pub fn new(database: Arc<Database>) -> Self {
        Self {
            planner: DefaultPhysicalPlanner::with_extension_planners(vec![]),
            database,
            buffered_layer: None,
        }
    }

    pub fn with_buffered_layer(mut self, layer: Arc<BufferedWriteLayer>) -> Self {
        self.buffered_layer = Some(layer);
        self
    }
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
                let source = if let Some(sp) = info.source_plan {
                    Some(materialize_source(&self.planner, session_state, sp).await?)
                } else {
                    None
                };

                let session = delta_session_from(session_state);
                let exec = if is_update {
                    DmlExec::update(info.table_name, info.project_id, input_exec, self.database.clone(), session)
                        .predicate(info.predicate)
                        .assignments(info.assignments.unwrap_or_default())
                        .source(source)
                } else {
                    DmlExec::delete(info.table_name, info.project_id, input_exec, self.database.clone(), session).predicate(info.predicate)
                };
                Ok(Arc::new(exec.buffered_layer(self.buffered_layer.clone())))
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
    let mut current_plan = input;
    let mut predicate = None;
    let mut assignments = None;
    let mut project_id = String::new();
    let mut source_plan: Option<UpdateSourcePlan> = None;

    loop {
        match current_plan {
            LogicalPlan::Projection(proj) if extract_assignments => {
                match &mut assignments {
                    // First Projection encountered: real UPDATE assignments.
                    None => assignments = Some(extract_assignments_from_projection(proj)?),
                    // Nested Projection (DataFusion CSE introduces one that defines
                    // `__common_expr_*`). Inline its aliases into our assignments so
                    // references to those synthetic columns resolve when we evaluate
                    // physical exprs against the bare table schema below.
                    Some(existing) => inline_projection_aliases(proj, existing)?,
                }
                current_plan = proj.input.as_ref();
            }
            LogicalPlan::Filter(filter) => {
                predicate = Some(filter.predicate.clone());
                project_id = extract_project_id(&filter.predicate).unwrap_or(project_id);
                current_plan = filter.input.as_ref();
            }
            LogicalPlan::Join(join) if extract_assignments => {
                // `UPDATE ... FROM` lowers to a `Join` whose left or right side
                // scans the target table. Detect which side is target; the other
                // is the source to materialize.
                if source_plan.is_some() {
                    return Err(DataFusionError::NotImplemented(
                        "UPDATE with multiple FROM sources (chained joins) is not supported".to_string(),
                    ));
                }
                if join.join_type != JoinType::Inner {
                    return Err(DataFusionError::NotImplemented(format!(
                        "UPDATE ... FROM with {:?} join is not supported (only INNER)",
                        join.join_type
                    )));
                }
                let (target_side, source_side, keys) = identify_target_side(join, table_name)?;
                // DataFusion stores cross-side conditions (e.g. user wrote
                // `NOT (o.hashes @> ARRAY[u.tag])`) in `join.filter` rather
                // than the surrounding `Filter`. Pull it into the predicate
                // path so the Delta MergeBuilder AND-s it into the join key
                // expression, and the MemBuffer hash-join evaluates it
                // against the widened batch.
                if let Some(jf) = &join.filter {
                    predicate = Some(match predicate.take() {
                        None => jf.clone(),
                        Some(existing) => Expr::BinaryExpr(BinaryExpr {
                            left:  Box::new(existing),
                            op:    Operator::And,
                            right: Box::new(jf.clone()),
                        }),
                    });
                }
                source_plan = Some(UpdateSourcePlan {
                    plan:      source_side.clone(),
                    join_keys: keys,
                });
                current_plan = target_side;
            }
            LogicalPlan::SubqueryAlias(alias) => {
                // Aliases on the source subquery (e.g. `FROM (...) AS u`) wrap
                // the inner plan; descend through them transparently.
                current_plan = alias.input.as_ref();
            }
            LogicalPlan::TableScan(scan) => {
                project_id = scan.filters.iter().find_map(extract_project_id).unwrap_or(project_id);

                predicate = predicate.or_else(|| {
                    (!scan.filters.is_empty())
                        .then(|| {
                            scan.filters.iter().cloned().reduce(|acc, filter| {
                                Expr::BinaryExpr(BinaryExpr {
                                    left:  Box::new(acc),
                                    op:    Operator::And,
                                    right: Box::new(filter),
                                })
                            })
                        })
                        .flatten()
                });
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
        return Err(DataFusionError::Plan(format!(
            "{} requires a project_id filter in WHERE clause",
            if extract_assignments { "UPDATE" } else { "DELETE" }
        )));
    }

    Ok(DmlInfo {
        table_name: table_name.to_string(),
        project_id,
        predicate,
        assignments,
        source_plan,
    })
}

/// Walk a [`LogicalPlan`] tree until we hit a `TableScan`. Returns the matched
/// scan's qualified name or `None` if no scan is reachable.
fn find_table_scan_name(plan: &LogicalPlan) -> Option<String> {
    use std::collections::VecDeque;
    let mut q: VecDeque<&LogicalPlan> = VecDeque::new();
    q.push_back(plan);
    while let Some(p) = q.pop_front() {
        if let LogicalPlan::TableScan(scan) = p {
            return Some(scan.table_name.to_string());
        }
        for child in p.inputs() {
            q.push_back(child);
        }
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

    let (target_side, source_side) = if target_is_left {
        (join.left.as_ref(), join.right.as_ref())
    } else {
        (join.right.as_ref(), join.left.as_ref())
    };

    // `join.on` is Vec<(left_expr, right_expr)>. Flip if target was on the right.
    let join_keys: Result<Vec<(String, String)>> = join
        .on
        .iter()
        .map(|(l, r)| {
            let (tgt_expr, src_expr) = if target_is_left { (l, r) } else { (r, l) };
            let tgt_col = expr_to_bare_col(tgt_expr)
                .ok_or_else(|| DataFusionError::NotImplemented(format!("UPDATE ... FROM join key must be a plain column reference, got: {tgt_expr}")))?;
            let src_col = expr_to_bare_col(src_expr)
                .ok_or_else(|| DataFusionError::NotImplemented(format!("UPDATE ... FROM join key must be a plain column reference, got: {src_expr}")))?;
            Ok((tgt_col, src_col))
        })
        .collect();

    Ok((target_side, source_side, join_keys?))
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
    let phys = planner.create_physical_plan(&sp.plan, session_state).await?;
    let schema = phys.schema();
    let task_ctx = Arc::new(TaskContext::from(session_state));

    // The source plan may be multi-partition; collect each into a Vec then concat.
    let partition_count = phys.properties().partitioning.partition_count();
    let mut batches: Vec<RecordBatch> = Vec::new();
    let mut total_rows = 0usize;
    for p in 0..partition_count {
        let mut stream = phys.execute(p, task_ctx.clone())?;
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            total_rows += batch.num_rows();
            if total_rows > MAX_UPDATE_SOURCE_ROWS {
                return Err(DataFusionError::Execution(format!(
                    "UPDATE ... FROM source exceeded the {} row cap; refine the source query or page the update",
                    MAX_UPDATE_SOURCE_ROWS
                )));
            }
            batches.push(batch);
        }
    }

    let combined = concat_batches(&schema, &batches).map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

    Ok(UpdateSource {
        batch: combined,
        schema,
        join_keys: sp.join_keys,
    })
}

/// Extract assignments from projection
fn extract_assignments_from_projection(proj: &datafusion::logical_expr::Projection) -> Result<Vec<(String, Expr)>> {
    Ok(proj
        .expr
        .iter()
        .zip(proj.schema.fields())
        .filter_map(|(expr, field)| {
            let field_name = field.name();
            match expr {
                Expr::Column(col) if col.name == *field_name => None,
                Expr::Alias(alias) if alias.name == *field_name => {
                    (!matches!(&*alias.expr, Expr::Column(col) if col.name == *field_name)).then(|| (field_name.clone(), (*alias.expr).clone()))
                }
                Expr::Column(_) => None,
                _ => Some((field_name.clone(), expr.clone())),
            }
        })
        .collect())
}

use crate::optimizers::extract_project_id_from_expr as extract_project_id;

/// Inline aliases from a nested (CSE) Projection into the existing UPDATE assignment
/// exprs. Without this, refs like `__common_expr_1` survive into mem_buffer's physical
/// expr evaluation against the bare table schema and fail with "Column not found".
fn inline_projection_aliases(proj: &datafusion::logical_expr::Projection, assignments: &mut [(String, Expr)]) -> Result<()> {
    use std::collections::HashMap;

    use datafusion::common::tree_node::{Transformed, TreeNode};

    let mut subs: HashMap<String, Expr> = HashMap::new();
    for (expr, field) in proj.expr.iter().zip(proj.schema.fields()) {
        match expr {
            Expr::Alias(alias) if alias.name != *field.name() || alias.name.starts_with("__common_expr_") => {
                subs.insert(alias.name.clone(), (*alias.expr).clone());
            }
            Expr::Alias(alias) => {
                // Pass-through alias matching the field name and not a CSE synthetic — skip.
                let _ = alias;
            }
            _ => {}
        }
    }
    if subs.is_empty() {
        return Ok(());
    }
    for (_, value_expr) in assignments.iter_mut() {
        let new_expr = value_expr
            .clone()
            .transform(|e| match &e {
                Expr::Column(col) => match subs.get(&col.name) {
                    Some(replacement) => Ok(Transformed::yes(replacement.clone())),
                    None => Ok(Transformed::no(e)),
                },
                _ => Ok(Transformed::no(e)),
            })
            .map(|t| t.data)
            .map_err(|e| DataFusionError::Execution(format!("Failed to inline CSE alias: {}", e)))?;
        *value_expr = new_expr;
    }
    Ok(())
}

/// Unified DML execution plan
#[derive(Clone, derive_more::Debug)]
pub struct DmlExec {
    op_type:        DmlOperation,
    table_name:     String,
    project_id:     String,
    predicate:      Option<Expr>,
    assignments:    Vec<(String, Expr)>,
    /// Materialized source for `UPDATE ... FROM`. When `Some`, dispatch
    /// routes to [`perform_update_with_source`] / [`perform_delta_merge_update`].
    #[debug(skip)]
    source:         Option<UpdateSource>,
    #[debug(skip)]
    input:          Arc<dyn ExecutionPlan>,
    #[debug(skip)]
    database:       Arc<Database>,
    #[debug(skip)]
    buffered_layer: Option<Arc<BufferedWriteLayer>>,
    #[debug(skip)]
    session:        Arc<dyn Session>,
    #[debug(skip)]
    properties:     Arc<PlanProperties>,
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
        Self {
            op_type,
            table_name,
            project_id,
            predicate: None,
            assignments: vec![],
            source: None,
            input,
            database,
            buffered_layer: None,
            session,
            properties,
        }
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
                    write!(
                        f,
                        ", assignments=[{}]",
                        self.assignments.iter().map(|(col, expr)| format!("{} = {}", col, expr)).collect::<Vec<_>>().join(", ")
                    )?;
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

    fn as_any(&self) -> &dyn Any {
        self
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
        Ok(Arc::new(Self {
            input: children[0].clone(),
            ..(*self).clone()
        }))
    }

    #[instrument(name = "dml.execute", skip_all, fields(operation = self.op_type.as_ref(), table.name = %self.table_name, project_id = %self.project_id, has_predicate = self.predicate.is_some(), rows.affected = Empty))]
    fn execute(&self, _partition: usize, _context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        let span = tracing::Span::current();
        let field_name = if self.op_type == DmlOperation::Update { "rows_updated" } else { "rows_deleted" };

        let schema = Arc::new(Schema::new(vec![Field::new(field_name, DataType::Int64, false)]));
        let schema_clone = schema.clone();

        let op_type = self.op_type.clone();
        let table_name = self.table_name.clone();
        let project_id = self.project_id.clone();
        let assignments = self.assignments.clone();
        let predicate = self.predicate.clone();
        let source = self.source.clone();
        let database = self.database.clone();
        let buffered_layer = self.buffered_layer.clone();
        let session = self.session.clone();

        let future = async move {
            let result = match op_type {
                DmlOperation::Update => {
                    perform_update_with_buffer(
                        &database,
                        buffered_layer.as_ref(),
                        &table_name,
                        &project_id,
                        predicate,
                        assignments,
                        source,
                        session,
                        &span,
                    )
                    .await
                }
                DmlOperation::Delete => {
                    perform_delete_with_buffer(&database, buffered_layer.as_ref(), &table_name, &project_id, predicate, session, &span).await
                }
            };

            if let Ok(rows) = &result {
                span.record("rows.affected", rows);
            }

            result
                .and_then(|rows| {
                    RecordBatch::try_new(schema_clone, vec![Arc::new(datafusion::arrow::array::Int64Array::from(vec![rows as i64]))])
                        .map_err(|e| DataFusionError::External(Box::new(e)))
                })
                .map_err(|e| {
                    error!("{} failed: {}", op_type.as_ref(), e);
                    e
                })
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, futures::stream::once(future))))
    }
}

struct DmlContext<'a> {
    database:       &'a Database,
    buffered_layer: Option<&'a Arc<BufferedWriteLayer>>,
    table_name:     &'a str,
    project_id:     &'a str,
    predicate:      Option<Expr>,
}

impl<'a> DmlContext<'a> {
    /// `delta_op` is a closure (not a bare Future) so its body — which may
    /// acquire a write lock and call `update_state` — is only constructed
    /// when there is committed data to operate on.
    async fn execute<F, G, Fut>(self, mem_op: F, delta_op: G) -> Result<u64>
    where
        F: FnOnce(&BufferedWriteLayer, Option<&Expr>) -> Result<u64>,
        G: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<u64>>,
    {
        let mut total_rows = 0u64;
        let has_uncommitted = self.buffered_layer.is_some_and(|l| l.has_table(self.project_id, self.table_name));

        if let Some(layer) = self.buffered_layer.filter(|_| has_uncommitted) {
            total_rows += mem_op(layer, self.predicate.as_ref())?;
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

        if has_committed {
            total_rows += delta_op().await?;
        }

        Ok(total_rows)
    }
}

#[allow(clippy::too_many_arguments)]
async fn perform_update_with_buffer(
    database: &Database, buffered_layer: Option<&Arc<BufferedWriteLayer>>, table_name: &str, project_id: &str, predicate: Option<Expr>,
    assignments: Vec<(String, Expr)>, source: Option<UpdateSource>, session: Arc<dyn Session>, span: &tracing::Span,
) -> Result<u64> {
    // `UPDATE ... FROM` path: MemBuffer takes the join via update_with_source,
    // Delta path uses MergeBuilder via perform_delta_merge_update.
    if let Some(src) = source {
        let update_span = tracing::trace_span!(parent: span, "delta.merge_update");
        let src_for_mem = src.clone();
        let src_for_delta = src;
        return DmlContext {
            database,
            buffered_layer,
            table_name,
            project_id,
            predicate: predicate.clone(),
        }
        .execute(
            |layer, pred| layer.update_with_source(project_id, table_name, pred, &assignments, &src_for_mem),
            || perform_delta_merge_update(database, table_name, project_id, predicate, assignments.clone(), src_for_delta, session).instrument(update_span),
        )
        .await;
    }

    let update_span = tracing::trace_span!(parent: span, "delta.update");
    // The delta closure body is only constructed (and assignments only
    // cloned) when there is committed data. Mem path borrows `assignments`.
    DmlContext {
        database,
        buffered_layer,
        table_name,
        project_id,
        predicate: predicate.clone(),
    }
    .execute(
        |layer, pred| layer.update(project_id, table_name, pred, &assignments),
        || perform_delta_update(database, table_name, project_id, predicate, assignments.clone(), session).instrument(update_span),
    )
    .await
}

async fn perform_delete_with_buffer(
    database: &Database, buffered_layer: Option<&Arc<BufferedWriteLayer>>, table_name: &str, project_id: &str, predicate: Option<Expr>,
    session: Arc<dyn Session>, span: &tracing::Span,
) -> Result<u64> {
    let delete_span = tracing::trace_span!(parent: span, "delta.delete");
    DmlContext {
        database,
        buffered_layer,
        table_name,
        project_id,
        predicate: predicate.clone(),
    }
    .execute(
        |layer, pred| layer.delete(project_id, table_name, pred),
        || perform_delta_delete(database, table_name, project_id, predicate, session).instrument(delete_span),
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
    let result = perform_delta_operation(database, table_name, project_id, |delta_table| async move {
        let mut builder = delta_table.update().with_session_state(session);

        if let Some(pred) = predicate {
            builder = builder.with_predicate(convert_expr_to_delta(&pred)?);
        }

        for (column, value_expr) in assignments {
            builder = builder.with_update(column, convert_expr_to_delta(&value_expr)?);
        }

        builder
            .await
            .map(|(table, metrics)| (table, metrics.num_updated_rows as u64))
            .map_err(|e| DataFusionError::Execution(format!("Failed to execute Delta UPDATE: {}", e)))
    })
    .await;

    if let Ok(rows) = &result {
        span.record("rows.updated", rows);
    }

    result
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
    let result = perform_delta_operation(database, table_name, project_id, |delta_table| async move {
        let mut builder = delta_table.delete().with_session_state(session);

        if let Some(pred) = predicate {
            builder = builder.with_predicate(convert_expr_to_delta(&pred)?);
        }

        builder
            .await
            .map(|(table, metrics)| (table, metrics.num_deleted_rows.unwrap_or(0) as u64))
            .map_err(|e| DataFusionError::Execution(format!("Failed to execute Delta DELETE: {}", e)))
    })
    .await;

    if let Ok(rows) = &result {
        span.record("rows.deleted", rows);
    }

    result
}

/// Common Delta operation logic
async fn perform_delta_operation<F, Fut>(database: &Database, table_name: &str, project_id: &str, operation: F) -> Result<u64>
where
    F: FnOnce(deltalake::DeltaTable) -> Fut,
    Fut: std::future::Future<Output = Result<(deltalake::DeltaTable, u64)>>,
{
    // Use resolve_table which routes to unified or custom table based on storage config
    let table_lock = database
        .resolve_table(project_id, table_name)
        .await
        .map_err(|e| DataFusionError::Execution(format!("Table not found: {} for project {}: {}", table_name, project_id, e)))?;

    // Hold the write lock continuously across update_state → operation → snapshot
    // swap. Releasing between operation and the second write opened a TOCTOU window
    // where a concurrent DELETE/UPDATE could commit a new version that we'd then
    // overwrite with the stale snapshot from the closure's clone.
    let mut guard = table_lock.write().await;
    guard.update_state().await.map_err(|e| DataFusionError::Execution(format!("Failed to refresh table state: {}", e)))?;
    let (new_table, rows_affected) = operation(guard.clone()).await?;
    *guard = new_table;
    // UPDATE/DELETE advance the version too — persist so boot replays only
    // post-commit log, same as the insert/maintenance paths.
    database.persist_snapshot(&guard);
    Ok(rows_affected)
}

/// Convert DataFusion Expr to Delta-compatible format.
/// Recursively walks the expression tree and strips table qualifiers from Column references
/// (e.g., `table.column` becomes just `column`). All other expression types (literals,
/// binary ops, functions, etc.) pass through unchanged, preserving types like Utf8View.
fn convert_expr_to_delta(expr: &Expr) -> Result<Expr> {
    use datafusion::common::tree_node::TreeNode;
    expr.clone()
        .transform(|e| match &e {
            Expr::Column(col) => Ok(datafusion::common::tree_node::Transformed::yes(Expr::Column(Column::from_name(&col.name)))),
            _ => Ok(datafusion::common::tree_node::Transformed::no(e)),
        })
        .map(|t| t.data)
        .map_err(|e| DataFusionError::Execution(format!("Failed to convert expression: {}", e)))
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
fn requalify_for_merge(expr: Expr, source_cols: &std::collections::HashSet<String>, source_alias: &str, target_alias: &str) -> Result<Expr> {
    use datafusion::common::tree_node::{Transformed, TreeNode};
    expr.transform(|e| match &e {
        Expr::Column(c) => match c.relation.as_ref() {
            Some(r) if r.table() == source_alias => Ok(Transformed::no(e)),
            Some(r) if r.table() == target_alias => Ok(Transformed::no(e)),
            _ => {
                if source_cols.contains(&c.name) {
                    Ok(Transformed::yes(Expr::Column(Column::new(Some(source_alias.to_string()), c.name.clone()))))
                } else {
                    Ok(Transformed::yes(Expr::Column(Column::from_name(c.name.clone()))))
                }
            }
        },
        _ => Ok(Transformed::no(e)),
    })
    .map(|t| t.data)
    .map_err(|e| DataFusionError::Execution(format!("Failed to requalify for merge: {}", e)))
}

/// Build the join predicate that drives the merge: a conjunction of
/// `target.k_i = source.k_i` clauses for each equi-key pair, AND-ed with the
/// optional user predicate (which gets routed through [`requalify_for_merge`]
/// so the user's source/target aliases resolve under `MergeBuilder`'s).
fn build_join_predicate(
    target_alias: &str, source_alias: &str, join_keys: &[(String, String)], extra: Option<&Expr>, source_cols: &std::collections::HashSet<String>,
) -> Result<Expr> {
    use datafusion::prelude::col;
    let mut key_iter = join_keys.iter().map(|(t, s)| {
        Expr::BinaryExpr(BinaryExpr {
            left:  Box::new(col(format!("{target_alias}.{t}"))),
            op:    Operator::Eq,
            right: Box::new(col(format!("{source_alias}.{s}"))),
        })
    });
    let mut acc = key_iter.next().ok_or_else(|| DataFusionError::Plan("UPDATE ... FROM requires at least one join key".to_string()))?;
    for next in key_iter {
        acc = Expr::BinaryExpr(BinaryExpr {
            left:  Box::new(acc),
            op:    Operator::And,
            right: Box::new(next),
        });
    }
    if let Some(p) = extra {
        let p = requalify_for_merge(p.clone(), source_cols, source_alias, target_alias)?;
        acc = Expr::BinaryExpr(BinaryExpr {
            left:  Box::new(acc),
            op:    Operator::And,
            right: Box::new(p),
        });
    }
    Ok(acc)
}

/// Re-qualify assignment value exprs via [`requalify_for_merge`] so the
/// user's source/target aliases address `MergeBuilder`'s `source` / `target`.
fn requalify_assignments_for_merge(
    assignments: Vec<(String, Expr)>, source_schema: &SchemaRef, source_alias: &str, target_alias: &str,
) -> Result<Vec<(String, Expr)>> {
    let source_cols: std::collections::HashSet<String> = source_schema.fields().iter().map(|f| f.name().clone()).collect();
    assignments
        .into_iter()
        .map(|(col_name, expr)| {
            let new_expr = requalify_for_merge(expr, &source_cols, source_alias, target_alias)?;
            Ok((col_name, new_expr))
        })
        .collect()
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
    info!(
        "Performing Delta MERGE-UPDATE on table {} for project {} ({} source rows)",
        table_name,
        project_id,
        source.batch.num_rows()
    );

    let span = tracing::Span::current();
    let source_schema = source.schema.clone();
    let source_batch = source.batch.clone();
    let join_keys = source.join_keys.clone();
    let source_cols: std::collections::HashSet<String> = source_schema.fields().iter().map(|f| f.name().clone()).collect();

    // Re-qualify assignments AND predicate before moving into the closure so
    // the user's source/target aliases address `MergeBuilder`'s `source` /
    // `target` aliases.
    let assignments = requalify_assignments_for_merge(assignments, &source_schema, "source", "target")?;

    let result = perform_delta_operation(database, table_name, project_id, |delta_table| async move {
        // Wrap the materialized source RecordBatch as a DataFrame. The
        // throwaway SessionContext only provides the DataFrame builder; merge
        // execution uses the session passed via `with_session_state`.
        let ctx = datafusion::prelude::SessionContext::new();
        let source_df = ctx
            .read_batch(source_batch)
            .map_err(|e| DataFusionError::Execution(format!("Failed to wrap UPDATE FROM source as DataFrame: {}", e)))?;

        let join_pred = build_join_predicate("target", "source", &join_keys, predicate.as_ref(), &source_cols)?;

        let merge = delta_table
            .merge(source_df, join_pred)
            .with_source_alias("source")
            .with_target_alias("target")
            .with_session_state(session)
            .with_safe_cast(true);

        let merge = merge
            .when_matched_update(|mut u| {
                for (col_name, value_expr) in &assignments {
                    u = u.update(col_name.clone(), value_expr.clone());
                }
                u
            })
            .map_err(|e| DataFusionError::Execution(format!("when_matched_update failed: {}", e)))?;

        let (new_table, metrics) = merge.await.map_err(|e| DataFusionError::Execution(format!("Failed to execute Delta MERGE UPDATE: {}", e)))?;
        Ok((new_table, metrics.num_target_rows_updated as u64))
    })
    .await;

    if let Ok(rows) = &result {
        span.record("rows.updated", rows);
    }
    let _ = source_schema; // kept for diagnostics; silence unused if compiler complains
    result
}
