//! Turning a [`LogicalPlan`] into a tree of [`Operator`]s.
//!
//! This is the join between the two halves of the owned engine. Everything up
//! to here produces or consumes plans; everything below here moves batches.
//! Without it the operators are a pile of correct components that cannot run a
//! query, which is exactly what they were until this module existed.
//!
//! # What a table is, here
//!
//! The builder does not know about object storage, Vortex or Parquet. It asks a
//! [`TableResolver`] for a [`BatchSource`] and gets batches back. That keeps
//! `basin-exec` free of a storage dependency, lets an entire plan be executed
//! against in-memory tables in a unit test, and means the real storage layer
//! plugs in by implementing one trait rather than by editing this file.
//!
//! # What is not built yet
//!
//! [`LogicalPlan`] has more variants than there are operators. Anything without
//! one returns [`BuildError::Unsupported`] naming the construct. That is a
//! deliberate, visible gap rather than a silent fallback — a planner that
//! quietly does something else when it meets an unimplemented node produces
//! wrong answers instead of errors.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

use basin_pgtype::PgType;
use basin_plan::{ColId, CteId, Expr, LogicalPlan, OnConflict, SortKey as PlanSortKey, TableId};

use crate::aggregate::{AggFunc, AggregateSpec, HashAggregate};
use crate::cte::{CteBuffer, ProjectSet};
use crate::dml::{ConflictAction, Delete, Insert, MemoryRowSink, RowSink, Update};
use crate::join::HashJoin;
use crate::lateral::{InnerFactory, LateralJoin};
use crate::limit::Limit;
use crate::operator::{ExecError, Operator};
use crate::project::{Filter, Project};
use crate::recursive::{RecursiveCte, RecursiveTermFactory};
use crate::scan::{BatchSource, Scan};
use crate::setop::{Distinct, Empty, SetOp, Values};
use crate::sort::{Sort, SortKey, TopK};
use crate::window::{OrderKey, WindowAgg, WindowFunc, WindowSpec};

/// The current outer row a correlated rebuild is bound to — `Some` only
/// while [`build_inner`] is constructing a `LateralJoin`'s inner side (once
/// per outer row) or a `WITH RECURSIVE` recursive term (once per
/// iteration); `None` everywhere else, including the entire ordinary build
/// path. See [`bind_outer`].
type Outer<'a> = Option<(&'a RecordBatch, usize)>;

/// Every [`CteBuffer`] registered so far in this build, shared (not copied)
/// into the `'static` closures a `LateralJoin`/`RecursiveCte` factory
/// captures — see [`SnapshotResolver`]'s doc comment for why those closures
/// cannot simply borrow anything from the top-level call instead.
type CteRegistry = Rc<RefCell<HashMap<CteId, CteBuffer>>>;

/// Why a plan could not be turned into operators.
#[derive(Debug, Clone, PartialEq)]
pub enum BuildError {
    /// A plan shape with no operator behind it yet.
    Unsupported(String),
    /// A table the resolver does not know.
    UnknownTable(TableId),
    /// A `CteRef` naming a `CteId` no enclosing `Cte` node registered. This
    /// is a planner bug, not a user error — every `CteRef` a correct
    /// planner emits is inside the scope of the `Cte` that defines it — and
    /// it is reported rather than built as an empty relation precisely
    /// because an empty relation would look like a valid, if surprising,
    /// answer instead of the broken plan it actually is.
    UnknownCte(CteId),
    /// A sort or group key that is not a plain column reference. Physical
    /// operators key on column positions, so anything else has to be
    /// materialised by a `Project` below them first — a job for the optimizer,
    /// not for this builder to improvise.
    NonColumnKey(&'static str),
    /// Building the operator itself failed.
    Exec(ExecError),
}

impl std::fmt::Display for BuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BuildError::Unsupported(what) => write!(f, "not yet executable: {what}"),
            BuildError::UnknownTable(t) => write!(f, "unknown table {t:?}"),
            BuildError::UnknownCte(id) => write!(
                f,
                "CTE reference to {id:?}, which nothing registered — a planner bug"
            ),
            BuildError::NonColumnKey(w) => {
                write!(f, "{w} key must be a column reference at this stage")
            }
            BuildError::Exec(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for BuildError {}

impl From<ExecError> for BuildError {
    fn from(e: ExecError) -> Self {
        BuildError::Exec(e)
    }
}

/// Supplies the batches behind a table.
///
/// One method, so the storage layer implements exactly this and nothing else.
pub trait TableResolver {
    /// Open a source for `table`, giving it the chance to apply `projection`
    /// and `filters` itself.
    ///
    /// Both are **requests, not guarantees**. A resolver that ignores them is
    /// still correct — [`ScanPushdown::accepted`] reports what it actually did,
    /// and the builder applies whatever was declined, Arrow-side.
    ///
    /// Passing them matters: reading only the projected columns and pruning
    /// files by predicate is Basin's whole scan advantage over Postgres's heap
    /// tuples. A resolver that opens the full table and lets the engine filter
    /// afterwards throws that away, and throws it away *silently* — the answers
    /// stay right and only the I/O gets worse, which surfaces months later as a
    /// benchmark regression nobody can attribute.
    fn open(
        &self,
        table: TableId,
        projection: &[usize],
        filters: &[Expr],
    ) -> Option<(Box<dyn BatchSource>, ScanPushdown)>;
}

/// What a [`TableResolver`] actually applied, of what it was offered.
///
/// Defaults to accepting nothing, so a resolver that does not opt in stays
/// correct by construction.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ScanPushdown {
    /// The source already applied the projection, and its batches carry only
    /// the requested columns, in the requested order. The scan must NOT project
    /// again — the indices it holds address the *table*, not the narrowed
    /// batch, so re-applying them would read the wrong columns.
    pub projection_applied: bool,
    /// The source already applied every filter it was given. The scan may skip
    /// its own filtering. Partial application is deliberately not
    /// representable: a source that can only honour some predicates should
    /// report `false` and let the scan re-apply all of them, which is cheap and
    /// always correct, rather than track which survived.
    pub filters_applied: bool,
}

/// A resolver backed by in-memory sources, for tests and for executing against
/// literal relations.
#[derive(Default)]
pub struct MemTableResolver {
    tables: HashMap<u32, Vec<(arrow_schema::SchemaRef, Vec<arrow_array::RecordBatch>)>>,
}

impl MemTableResolver {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(
        &mut self,
        table: TableId,
        schema: arrow_schema::SchemaRef,
        batches: Vec<arrow_array::RecordBatch>,
    ) {
        self.tables.insert(table.0, vec![(schema, batches)]);
    }
}

impl TableResolver for MemTableResolver {
    /// Accepts nothing. In-memory tables have no I/O to save, so pushing down
    /// would add code without buying anything; the scan filters and projects.
    fn open(
        &self,
        table: TableId,
        _projection: &[usize],
        _filters: &[Expr],
    ) -> Option<(Box<dyn BatchSource>, ScanPushdown)> {
        let (schema, batches) = self.tables.get(&table.0)?.first()?;
        Some((
            Box::new(crate::scan::VecBatchSource::new(
                schema.clone(),
                batches.clone(),
            )),
            ScanPushdown::default(),
        ))
    }
}

/// Where `INSERT`/`UPDATE`/`DELETE` write. The write-side mirror of
/// [`TableResolver`]: `build.rs` asks for a [`RowSink`] and stays free of a
/// storage dependency the same way the read side does via `BatchSource`.
///
/// Unlike a `Scan`'s projection, a write has no partial-column shape to
/// negotiate — `RowSink::insert`'s contract already fixes the batch as the
/// *full* write schema (see `dml.rs`'s module docs) — so this trait hands
/// back that schema and the row-identity columns within it (what
/// `UPDATE`/`DELETE` match on, and `INSERT ... ON CONFLICT`'s default
/// target when the statement does not name one) alongside the sink itself,
/// rather than asking for them piecemeal the way `TableResolver::open`
/// negotiates a projection.
pub trait DmlResolver {
    /// Open a sink for `table`, together with its full write schema (every
    /// column, in table-column order) and the positions within it that
    /// uniquely identify a row.
    fn open(&self, table: TableId) -> Option<(Box<dyn RowSink>, SchemaRef, Vec<usize>)>;
}

/// A [`RowSink`] that writes through a shared, `Rc`-backed
/// [`MemoryRowSink`], so a [`MemDmlResolver`]'s caller can keep its own
/// handle to inspect what was written after a build+execute — `RowSink`
/// itself is consumed by value into the operator tree, so nothing else can
/// reach it there. Not `Rc<RefCell<MemoryRowSink>>` implementing `RowSink`
/// directly, because `RowSink`'s methods take `&mut self` and `Rc` alone
/// does not give interior mutability.
struct SharedMemoryRowSink(Rc<RefCell<MemoryRowSink>>);

impl RowSink for SharedMemoryRowSink {
    fn insert(&mut self, batch: &RecordBatch) -> Result<u64, ExecError> {
        self.0.borrow_mut().insert(batch)
    }

    fn delete(&mut self, keys: &RecordBatch) -> Result<u64, ExecError> {
        self.0.borrow_mut().delete(keys)
    }
}

/// One registered table's write-side state, as [`MemDmlResolver`] tracks it.
type MemDmlTable = (Rc<RefCell<MemoryRowSink>>, SchemaRef, Vec<usize>);

/// A [`DmlResolver`] backed by in-memory [`MemoryRowSink`]s, for tests —
/// the write-side counterpart of [`MemTableResolver`].
#[derive(Default)]
pub struct MemDmlResolver {
    tables: HashMap<u32, MemDmlTable>,
}

impl MemDmlResolver {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `table` as writable, with `key_cols` (positions within
    /// `schema`) as its uniqueness key. Returns a handle the caller can use
    /// to inspect the sink's contents after running a built plan — see
    /// [`SharedMemoryRowSink`]'s doc comment for why a plain `RowSink`
    /// cannot serve that purpose once it has been handed to `Insert`.
    pub fn insert_table(
        &mut self,
        table: TableId,
        schema: SchemaRef,
        key_cols: Vec<usize>,
    ) -> Rc<RefCell<MemoryRowSink>> {
        let sink = Rc::new(RefCell::new(MemoryRowSink::new(
            schema.clone(),
            key_cols.clone(),
        )));
        self.tables
            .insert(table.0, (Rc::clone(&sink), schema, key_cols));
        sink
    }
}

impl DmlResolver for MemDmlResolver {
    fn open(&self, table: TableId) -> Option<(Box<dyn RowSink>, SchemaRef, Vec<usize>)> {
        let (sink, schema, key_cols) = self.tables.get(&table.0)?;
        Some((
            Box::new(SharedMemoryRowSink(Rc::clone(sink))),
            schema.clone(),
            key_cols.clone(),
        ))
    }
}

/// Default memory budget for a buffering operator, in bytes.
///
/// Deliberately a constant here rather than a policy: the real budget is a
/// per-query slice of the bounded pool that already exists in the engine, and
/// wiring that through is a separate increment. Picking a number and hiding it
/// would be worse than picking one and saying so.
pub const DEFAULT_OPERATOR_BUDGET: usize = 256 * 1024 * 1024;

/// Build an operator tree for `plan`. No `INSERT`/`UPDATE`/`DELETE` in
/// `plan` can build through this entry point — see [`build_with_dml`] — so
/// this signature never changes shape no matter what else this module
/// learns to build; existing callers are never asked for more than a
/// [`TableResolver`].
pub fn build(
    plan: &LogicalPlan,
    tables: &dyn TableResolver,
) -> Result<Box<dyn Operator>, BuildError> {
    build_with_budget(plan, tables, DEFAULT_OPERATOR_BUDGET)
}

/// Build an operator tree with an explicit memory budget for buffering
/// operators. Like [`build`], data-modifying statements are refused — see
/// [`build_with_dml`].
pub fn build_with_budget(
    plan: &LogicalPlan,
    tables: &dyn TableResolver,
    budget: usize,
) -> Result<Box<dyn Operator>, BuildError> {
    let ctes: CteRegistry = Rc::new(RefCell::new(HashMap::new()));
    build_inner(plan, tables, None, budget, &ctes, None)
}

/// Build an operator tree that may contain `INSERT`/`UPDATE`/`DELETE`,
/// resolving their write side through `dml`. A separate entry point rather
/// than a new parameter on [`build`]/[`build_with_budget`]: those two stay
/// exactly as they were before DML existed, so nothing that already calls
/// them needs to acquire a write resolver it does not use.
pub fn build_with_dml(
    plan: &LogicalPlan,
    tables: &dyn TableResolver,
    dml: &dyn DmlResolver,
    budget: usize,
) -> Result<Box<dyn Operator>, BuildError> {
    let ctes: CteRegistry = Rc::new(RefCell::new(HashMap::new()));
    build_inner(plan, tables, Some(dml), budget, &ctes, None)
}

/// The actual recursive builder. `ctes` is threaded (not created fresh per
/// call) so a `CteRef` anywhere under the plan — including inside a
/// `LateralJoin`'s inner side or a `WITH RECURSIVE` recursive term's
/// rebuilt subplan — can see every `Cte` registered by an ancestor. `outer`
/// is `None` for the entire ordinary build; see [`Outer`].
fn build_inner(
    plan: &LogicalPlan,
    tables: &dyn TableResolver,
    dml: Option<&dyn DmlResolver>,
    budget: usize,
    ctes: &CteRegistry,
    outer: Outer<'_>,
) -> Result<Box<dyn Operator>, BuildError> {
    match plan {
        LogicalPlan::Scan {
            table,
            projection,
            filters,
            ..
        } => {
            let cols: Vec<usize> = projection.iter().map(|c| c.0 as usize).collect();
            let filters: Vec<Expr> = filters
                .iter()
                .map(|f| bind_outer(f, outer, tables, dml, budget, ctes))
                .collect::<Result<_, _>>()?;
            let (source, pushed) = tables
                .open(*table, &cols, &filters)
                .ok_or(BuildError::UnknownTable(*table))?;

            // Whatever the source declined, the scan still does. When the
            // source applied the projection its batches are already narrowed,
            // so the scan's indices — which address the full table — become
            // identity over the narrowed batch rather than being re-applied
            // against it, which would read the wrong columns.
            let scan_cols: Vec<usize> = if pushed.projection_applied {
                (0..cols.len()).collect()
            } else {
                cols
            };
            let scan_filters = if pushed.filters_applied {
                Vec::new()
            } else {
                filters
            };
            Ok(Box::new(Scan::new(source, scan_cols, scan_filters)?))
        }

        LogicalPlan::Filter { input, predicate } => {
            let child = build_inner(input, tables, dml, budget, ctes, outer)?;
            Ok(Box::new(Filter::new(
                child,
                bind_outer(predicate, outer, tables, dml, budget, ctes)?,
            )))
        }

        LogicalPlan::Project { input, exprs } => {
            let child = build_inner(input, tables, dml, budget, ctes, outer)?;
            let exprs: Vec<(Expr, String)> = exprs
                .iter()
                .map(|(e, n)| Ok((bind_outer(e, outer, tables, dml, budget, ctes)?, n.clone())))
                .collect::<Result<_, BuildError>>()?;
            Ok(Box::new(Project::new(child, exprs)?))
        }

        // `Limit` with a fetch and a sorted input is the top-K shape. Basin's
        // published ORDER BY … LIMIT numbers depend on early termination, so
        // recognising it here rather than sorting everything and truncating is
        // load-bearing, not a micro-optimisation.
        LogicalPlan::Limit {
            input,
            skip,
            fetch,
            with_ties,
        } => {
            if *with_ties {
                return Err(BuildError::Unsupported("FETCH … WITH TIES".into()));
            }
            let fetch = fetch
                .as_ref()
                .map(|e| bind_outer(e, outer, tables, dml, budget, ctes))
                .transpose()?;
            let skip_n = match skip
                .as_ref()
                .map(|e| bind_outer(e, outer, tables, dml, budget, ctes))
                .transpose()?
            {
                Some(e) => Some(
                    const_usize(&e)
                        .ok_or_else(|| BuildError::Unsupported("non-constant OFFSET".into()))?,
                ),
                None => None,
            };
            let fetch_n = match &fetch {
                Some(e) => Some(
                    const_usize(e)
                        .ok_or_else(|| BuildError::Unsupported("non-constant LIMIT".into()))?,
                ),
                None => None,
            };
            if fetch_n.is_none() && skip_n.is_none() {
                return build_inner(input, tables, dml, budget, ctes, outer);
            }

            // `ORDER BY … LIMIT` with no offset fuses into a bounded heap, which
            // is what makes the published numbers for that shape depend on early
            // termination rather than a full sort. Every other combination —
            // including an offset, which the heap cannot express — becomes a
            // streaming Limit over whatever the input already is.
            match (input.as_ref(), skip_n, fetch_n) {
                (LogicalPlan::Sort { input: si, keys }, None, Some(k)) => {
                    let child = build_inner(si, tables, dml, budget, ctes, outer)?;
                    let keys = bind_sort_keys(keys, outer, tables, dml, budget, ctes)?;
                    Ok(Box::new(TopK::new(child, sort_keys(&keys)?, k)))
                }
                _ => {
                    let child = build_inner(input, tables, dml, budget, ctes, outer)?;
                    Ok(Box::new(Limit::new(child, skip_n.unwrap_or(0), fetch_n)))
                }
            }
        }

        LogicalPlan::Sort { input, keys } => {
            let child = build_inner(input, tables, dml, budget, ctes, outer)?;
            let keys = bind_sort_keys(keys, outer, tables, dml, budget, ctes)?;
            Ok(Box::new(Sort::new(child, sort_keys(&keys)?, budget)))
        }

        LogicalPlan::Aggregate {
            input,
            group,
            aggs,
            grouping_sets,
        } => {
            if grouping_sets.is_some() {
                return Err(BuildError::Unsupported(
                    "GROUPING SETS / ROLLUP / CUBE".into(),
                ));
            }
            let child = build_inner(input, tables, dml, budget, ctes, outer)?;
            let group: Vec<Expr> = group
                .iter()
                .map(|e| bind_outer(e, outer, tables, dml, budget, ctes))
                .collect::<Result<_, _>>()?;
            let group_cols = group
                .iter()
                .map(|e| column_index(e).ok_or(BuildError::NonColumnKey("GROUP BY")))
                .collect::<Result<Vec<_>, _>>()?;
            let aggs: Vec<Expr> = aggs
                .iter()
                .map(|e| bind_outer(e, outer, tables, dml, budget, ctes))
                .collect::<Result<_, _>>()?;
            let specs = aggs
                .iter()
                .enumerate()
                .map(|(i, a)| agg_spec(a, &format!("agg{i}")))
                .collect::<Result<Vec<_>, BuildError>>()?;
            Ok(Box::new(HashAggregate::new(
                child, group_cols, specs, budget,
            )?))
        }

        LogicalPlan::Join {
            left,
            right,
            kind,
            on,
            filter,
        } => {
            // `Left`/`Right`/`Full` need the residual to survive a failed
            // pair as an unmatched, NULL-extended row rather than drop it —
            // `HashJoin` has no path for that (see its module docs' own
            // "Residual filter" section), so this is refused here, by name,
            // rather than built into something that runs and returns wrong
            // rows. `Inner`/`Cross`/`LeftSemi`/`LeftAnti` are the kinds
            // `basin-plan`'s decorrelation of `EXISTS`/`IN` actually
            // produces a residual for (`opt/decorrelate.rs`'s transforms
            // 1-3); an outer join carrying one would have to come from a
            // hand-written `ON`-clause residual on an explicit outer join,
            // which is real SQL but not the case this task exists for.
            if filter.is_some()
                && matches!(
                    kind,
                    basin_plan::JoinKind::Left
                        | basin_plan::JoinKind::Right
                        | basin_plan::JoinKind::Full
                )
            {
                return Err(BuildError::Unsupported(format!(
                    "{kind:?} join with a non-equi residual condition — a pair that fails the \
                     residual must survive as an unmatched, NULL-extended row, and there is no \
                     physical operator that implements that today"
                )));
            }
            let l = build_inner(left, tables, dml, budget, ctes, outer)?;
            let r = build_inner(right, tables, dml, budget, ctes, outer)?;
            let mut lk = Vec::with_capacity(on.len());
            let mut rk = Vec::with_capacity(on.len());
            for (a, b) in on {
                let a = bind_outer(a, outer, tables, dml, budget, ctes)?;
                let b = bind_outer(b, outer, tables, dml, budget, ctes)?;
                lk.push(column_index(&a).ok_or(BuildError::NonColumnKey("join"))?);
                rk.push(column_index(&b).ok_or(BuildError::NonColumnKey("join"))?);
            }
            // `filter` mixes relation-0 (left, sometimes flat over
            // left++right — see `join.rs`'s `flatten_filter`) and
            // relation-1 (right, decorrelation's own convention) column
            // refs; `bind_outer` only ever substitutes `relation ==
            // OUTER_REF` (1) columns when this join sits inside a LATERAL's
            // per-row closure (`outer` is `Some`). A decorrelated filter's
            // own relation-1 tags are a different thing entirely (the
            // join's own right side, not the enclosing LATERAL row) that
            // happens to reuse the same tag value — see `on`'s existing
            // `bind_outer(b, ...)` call just above, which has carried this
            // same ambiguity for `on`'s right-hand pairs since before this
            // change. Not resolved here — this join arm is not the place to
            // fix a relation-tag collision that predates it — but real: a
            // decorrelated Semi/Anti nested inside a LATERAL would mis-bind.
            let filter = filter
                .as_ref()
                .map(|f| bind_outer(f, outer, tables, dml, budget, ctes))
                .transpose()?;
            Ok(Box::new(HashJoin::with_filter(
                l, r, *kind, lk, rk, filter, budget,
            )?))
        }

        // `LATERAL` — the inner side is rebuilt fresh per outer row via a
        // factory, because its predicates may reference the outer row's own
        // columns. See `lateral.rs`'s module docs for why a factory rather
        // than a fixed operator, and [`bind_outer`]/[`SnapshotResolver`] for
        // how this builder resolves the correlation and satisfies the
        // factory's `'static` bound.
        LogicalPlan::LateralJoin {
            outer: outer_plan,
            inner,
            kind,
        } => {
            if !matches!(
                kind,
                basin_plan::JoinKind::Inner
                    | basin_plan::JoinKind::Cross
                    | basin_plan::JoinKind::Left
            ) {
                return Err(BuildError::Unsupported(format!(
                    "LATERAL join of kind {kind:?}"
                )));
            }
            if inner.is_mutating() {
                return Err(BuildError::Unsupported(
                    "data-modifying statement inside a LATERAL subquery".into(),
                ));
            }
            let outer_op = build_inner(outer_plan, tables, dml, budget, ctes, outer)?;

            let inner_plan = inner.as_ref().clone();
            let mut snapshot = SnapshotResolver::default();
            snapshot_scans(&inner_plan, tables, &mut snapshot)?;
            let snapshot = Rc::new(snapshot);

            // The inner side's schema is needed up front (the operator's own
            // output schema depends on it) but no real outer row exists yet.
            // A single all-NULL probe row gives every `OUTER_REF` column a
            // literal of the right TYPE (derived from the outer schema's
            // Arrow type, independent of the value) without needing one —
            // see `outer_literal`'s NULL handling.
            let outer_schema = outer_op.schema();
            let probe_cols: Vec<arrow_array::ArrayRef> = outer_schema
                .fields()
                .iter()
                .map(|f| arrow_array::new_null_array(f.data_type(), 1))
                .collect();
            let probe_batch = RecordBatch::try_new(Arc::clone(&outer_schema), probe_cols)
                .map_err(|e| BuildError::Exec(ExecError::Internal(e.to_string())))?;
            let inner_schema = build_inner(
                &inner_plan,
                snapshot.as_ref(),
                None,
                budget,
                ctes,
                Some((&probe_batch, 0)),
            )?
            .schema();

            let snapshot_for_factory = Rc::clone(&snapshot);
            let ctes_for_factory = Rc::clone(ctes);
            let inner_plan_for_factory = inner_plan.clone();
            let make_inner: InnerFactory = Box::new(move |row_batch: &RecordBatch, idx: usize| {
                build_inner(
                    &inner_plan_for_factory,
                    snapshot_for_factory.as_ref(),
                    None,
                    budget,
                    &ctes_for_factory,
                    Some((row_batch, idx)),
                )
                .map_err(build_error_to_exec)
            });
            Ok(Box::new(LateralJoin::new(
                outer_op,
                inner_schema,
                make_inner,
                *kind,
            )))
        }

        LogicalPlan::Values { rows, schema } => {
            let names: Vec<String> = schema.iter().map(|(n, _)| n.clone()).collect();
            let rows: Vec<Vec<Expr>> = rows
                .iter()
                .map(|r| {
                    r.iter()
                        .map(|e| bind_outer(e, outer, tables, dml, budget, ctes))
                        .collect::<Result<_, _>>()
                })
                .collect::<Result<_, BuildError>>()?;
            Ok(Box::new(Values::new(rows, names)?))
        }

        LogicalPlan::Empty {
            produce_one_row,
            schema,
        } => {
            let fields = schema
                .iter()
                .map(|(n, t)| {
                    basin_pgtype::physical(*t)
                        .map(|dt| arrow_schema::Field::new(n, dt, true))
                        .map_err(|e| BuildError::Unsupported(e.to_string()))
                })
                .collect::<Result<Vec<_>, _>>()?;
            let arrow_schema = std::sync::Arc::new(arrow_schema::Schema::new(fields));
            Ok(Box::new(Empty::new(arrow_schema, *produce_one_row)))
        }

        LogicalPlan::Distinct { input, on } => {
            let child = build_inner(input, tables, dml, budget, ctes, outer)?;
            match on {
                None => Ok(Box::new(Distinct::new(child, budget))),
                Some(exprs) => {
                    let exprs: Vec<Expr> = exprs
                        .iter()
                        .map(|e| bind_outer(e, outer, tables, dml, budget, ctes))
                        .collect::<Result<_, _>>()?;
                    let cols = exprs
                        .iter()
                        .map(|e| column_index(e).ok_or(BuildError::NonColumnKey("DISTINCT ON")))
                        .collect::<Result<Vec<_>, _>>()?;
                    Ok(Box::new(Distinct::new_on(child, cols, budget)))
                }
            }
        }

        LogicalPlan::SetOp {
            left,
            right,
            op,
            all,
        } => {
            let l = build_inner(left, tables, dml, budget, ctes, outer)?;
            let r = build_inner(right, tables, dml, budget, ctes, outer)?;
            Ok(Box::new(SetOp::new(l, r, *op, *all, budget)?))
        }

        // Every window expression in one node shares a PARTITION BY / ORDER BY,
        // because the planner groups them that way — one operator per distinct
        // window, not per expression. The operator requires its input already
        // sorted by those keys and never re-sorts, so an unsorted input is a
        // planner bug it will not paper over.
        LogicalPlan::Window { input, windows } => {
            let child = build_inner(input, tables, dml, budget, ctes, outer)?;
            let windows: Vec<Expr> = windows
                .iter()
                .map(|e| bind_outer(e, outer, tables, dml, budget, ctes))
                .collect::<Result<_, _>>()?;
            let (partition_by, order_by) = window_keys(&windows)?;
            let specs = windows
                .iter()
                .enumerate()
                .map(|(i, w)| window_spec(w, &format!("window{i}")))
                .collect::<Result<Vec<_>, BuildError>>()?;
            Ok(Box::new(WindowAgg::new(
                child,
                partition_by,
                order_by,
                specs,
                budget,
            )?))
        }

        // Set-returning functions in the target list — the shape
        // `jsonb_udf.rs:16` records as impossible inside DataFusion, which is
        // why `generate_series` and `unnest` in a SELECT list do not work on
        // the current engine at all.
        //
        // Note the LCM trap: multiple SRFs in one list do NOT produce the least
        // common multiple of their lengths. Since Postgres 10 they run in
        // lockstep to the LONGEST, padding the shorter with NULL. The operator
        // implements the modern rule; this comment exists because the older one
        // is what most references and most recollections still describe.
        LogicalPlan::ProjectSet { input, srfs } => {
            let child = build_inner(input, tables, dml, budget, ctes, outer)?;
            let named: Vec<(Expr, String)> = srfs
                .iter()
                .enumerate()
                .map(|(i, e)| {
                    Ok((
                        bind_outer(e, outer, tables, dml, budget, ctes)?,
                        format!("srf{i}"),
                    ))
                })
                .collect::<Result<_, BuildError>>()?;
            Ok(Box::new(ProjectSet::new(child, named)?))
        }

        // A `WITH` body is built once into a `CteBuffer` and registered by
        // `CteId`; every `CteRef` to that id — however many there are —
        // takes its own `CteReader` off the same buffer (see `cte.rs`'s
        // module docs on why materialize-once/replay-many is the right
        // shape, not just a convenient one). `recursive` selects whether the
        // body is built as an ordinary subplan or as a `RecursiveCte`
        // fixpoint loop (see `build_recursive_cte`); either way, the RESULT
        // is wrapped in the same `CteBuffer`, so a recursive CTE referenced
        // twice also replays in full both times, not just a non-recursive
        // one.
        LogicalPlan::Cte {
            name,
            recursive,
            body,
            input,
        } => {
            let body_op: Box<dyn Operator> = if *recursive {
                build_recursive_cte(*name, body, tables, budget, ctes, outer)?
            } else {
                build_inner(body, tables, dml, budget, ctes, outer)?
            };
            let buffer = CteBuffer::new(body_op, budget);
            ctes.borrow_mut().insert(*name, buffer);
            build_inner(input, tables, dml, budget, ctes, outer)
        }

        // A planner bug, not a user error, if `name` was never registered —
        // see `BuildError::UnknownCte`'s doc comment.
        LogicalPlan::CteRef { name, .. } => {
            let reader = {
                let registered = ctes.borrow();
                let buffer = registered.get(name).ok_or(BuildError::UnknownCte(*name))?;
                buffer.reader()
            };
            Ok(Box::new(reader))
        }

        LogicalPlan::Insert {
            table,
            input,
            columns: _,
            on_conflict,
            returning,
        } => {
            let dml_resolver = dml.ok_or_else(|| {
                BuildError::Unsupported("INSERT (no write resolver configured)".into())
            })?;
            let (sink, write_schema, key_cols) = dml_resolver
                .open(*table)
                .ok_or(BuildError::UnknownTable(*table))?;
            let input_op = build_inner(input, tables, dml, budget, ctes, outer)?;
            if input_op.schema() != write_schema {
                return Err(BuildError::Unsupported(format!(
                    "INSERT input schema does not match {table:?}'s write schema — expected \
                     {write_schema:?}, got {:?} (defaults/column order are assumed already \
                     resolved upstream, per dml.rs's module docs)",
                    input_op.schema()
                )));
            }
            let action = bind_on_conflict(on_conflict, &write_schema, &key_cols)?;
            let want_returning = returning.is_some();
            let dml_op: Box<dyn Operator> =
                Box::new(Insert::new(input_op, sink, action, want_returning));
            wrap_returning(dml_op, returning, outer, tables, dml, budget, ctes)
        }

        // `Update`/`Delete` carry no explicit input plan (unlike `Insert`) —
        // this builder synthesises `Scan(table) [+ Filter(predicate)]`
        // itself. `UPDATE … FROM` / `DELETE … USING` are refused rather than
        // improvised as a cross join with no declared join condition to
        // narrow it; see the module docs' "What is not built yet" posture.
        LogicalPlan::Update {
            table,
            set,
            from,
            predicate,
            returning,
            snapshot,
        } => {
            if from.is_some() {
                return Err(BuildError::Unsupported("UPDATE … FROM".into()));
            }
            let dml_resolver = dml.ok_or_else(|| {
                BuildError::Unsupported("UPDATE (no write resolver configured)".into())
            })?;
            let (sink, write_schema, key_cols) = dml_resolver
                .open(*table)
                .ok_or(BuildError::UnknownTable(*table))?;
            let n = write_schema.fields().len();
            let scan = LogicalPlan::Scan {
                table: *table,
                projection: (0..n as u16).map(ColId).collect(),
                filters: Vec::new(),
                snapshot: *snapshot,
            };
            let scanned = build_inner(&scan, tables, dml, budget, ctes, outer)?;
            let matched: Box<dyn Operator> = match predicate {
                Some(p) => Box::new(Filter::new(
                    scanned,
                    bind_outer(p, outer, tables, dml, budget, ctes)?,
                )),
                None => scanned,
            };
            let mut set_map: HashMap<usize, Expr> = HashMap::new();
            for (c, e) in set {
                set_map.insert(
                    c.0 as usize,
                    bind_outer(e, outer, tables, dml, budget, ctes)?,
                );
            }
            let exprs: Vec<(Expr, String)> = (0..n)
                .map(|i| {
                    let name = write_schema.field(i).name().clone();
                    let e = set_map.remove(&i).unwrap_or_else(|| {
                        Expr::Column(basin_plan::ColumnRef {
                            relation: 0,
                            index: i as u16,
                            name: name.clone(),
                        })
                    });
                    (e, name)
                })
                .collect();
            let new_rows = Project::new(matched, exprs)?;
            let want_returning = returning.is_some();
            let dml_op: Box<dyn Operator> = Box::new(Update::new(
                Box::new(new_rows),
                sink,
                key_cols,
                want_returning,
            ));
            wrap_returning(dml_op, returning, outer, tables, dml, budget, ctes)
        }

        LogicalPlan::Delete {
            table,
            using,
            predicate,
            returning,
            snapshot,
        } => {
            if using.is_some() {
                return Err(BuildError::Unsupported("DELETE … USING".into()));
            }
            let dml_resolver = dml.ok_or_else(|| {
                BuildError::Unsupported("DELETE (no write resolver configured)".into())
            })?;
            let (sink, write_schema, key_cols) = dml_resolver
                .open(*table)
                .ok_or(BuildError::UnknownTable(*table))?;
            let n = write_schema.fields().len();
            let scan = LogicalPlan::Scan {
                table: *table,
                projection: (0..n as u16).map(ColId).collect(),
                filters: Vec::new(),
                snapshot: *snapshot,
            };
            let scanned = build_inner(&scan, tables, dml, budget, ctes, outer)?;
            let matched: Box<dyn Operator> = match predicate {
                Some(p) => Box::new(Filter::new(
                    scanned,
                    bind_outer(p, outer, tables, dml, budget, ctes)?,
                )),
                None => scanned,
            };
            let want_returning = returning.is_some();
            let dml_op: Box<dyn Operator> =
                Box::new(Delete::new(matched, sink, key_cols, want_returning));
            wrap_returning(dml_op, returning, outer, tables, dml, budget, ctes)
        }
    }
}

/// Translate `Insert`'s `on_conflict` into the physical [`ConflictAction`].
/// `ON CONFLICT ... WHERE` and a `SET` list narrower than the full row are
/// refused rather than improvised — see `dml.rs`'s module docs on why
/// `ConflictAction::DoUpdate` can only mean "replace the row wholesale".
fn bind_on_conflict(
    on_conflict: &Option<OnConflict>,
    write_schema: &SchemaRef,
    resolver_key_cols: &[usize],
) -> Result<Option<ConflictAction>, BuildError> {
    match on_conflict {
        None => Ok(None),
        Some(OnConflict::DoNothing { .. }) => Ok(Some(ConflictAction::DoNothing)),
        Some(OnConflict::DoUpdate {
            target,
            set,
            predicate,
        }) => {
            if predicate.is_some() {
                return Err(BuildError::Unsupported(
                    "ON CONFLICT ... DO UPDATE ... WHERE".into(),
                ));
            }
            if set.len() != write_schema.fields().len() {
                return Err(BuildError::Unsupported(
                    "ON CONFLICT DO UPDATE with a SET list narrower than the full row".into(),
                ));
            }
            let key_cols = if target.is_empty() {
                resolver_key_cols.to_vec()
            } else {
                target.iter().map(|c| c.0 as usize).collect()
            };
            Ok(Some(ConflictAction::DoUpdate { key_cols }))
        }
    }
}

/// Wrap a DML operator's output with the `RETURNING` projection, if any —
/// `Insert`/`Update`/`Delete` themselves only gate WHETHER any rows come
/// out (`want_returning`), always at full row width; picking out (and
/// computing) the actual `RETURNING` list is this builder's job, the same
/// way `dml.rs`'s module docs describe "a `Project` to pick out the
/// `RETURNING` list" sitting above the DML node.
fn wrap_returning(
    dml_op: Box<dyn Operator>,
    returning: &Option<Vec<(Expr, String)>>,
    outer: Outer<'_>,
    tables: &dyn TableResolver,
    dml: Option<&dyn DmlResolver>,
    budget: usize,
    ctes: &CteRegistry,
) -> Result<Box<dyn Operator>, BuildError> {
    match returning {
        None => Ok(dml_op),
        Some(ret) => {
            let exprs: Vec<(Expr, String)> = ret
                .iter()
                .map(|(e, n)| Ok((bind_outer(e, outer, tables, dml, budget, ctes)?, n.clone())))
                .collect::<Result<_, BuildError>>()?;
            Ok(Box::new(Project::new(dml_op, exprs)?))
        }
    }
}

/// Upper bound on `WITH RECURSIVE` iterations — see `recursive.rs`'s module
/// docs item 4 on why this crate needs one at all (no independent timer to
/// let `statement_timeout` interrupt a non-terminating recursive term).
/// Generous rather than tight: a genuinely converging query at this scale
/// is already well past anything reasonable to run synchronously.
const DEFAULT_RECURSION_LIMIT: usize = 10_000;

/// Build a `RecursiveCte` for `body`, which must be `anchor UNION [ALL]
/// recursive_term` — the only shape SQL's `WITH RECURSIVE` has. `dml` is
/// deliberately not threaded into the recursive term's rebuilds: it runs
/// inside a `'static` factory closure (see [`SnapshotResolver`]'s doc
/// comment for why), and a data-modifying statement re-run once per
/// iteration is refused up front instead, same as `LateralJoin`'s inner
/// side.
fn build_recursive_cte(
    name: CteId,
    body: &LogicalPlan,
    tables: &dyn TableResolver,
    budget: usize,
    ctes: &CteRegistry,
    outer: Outer<'_>,
) -> Result<Box<dyn Operator>, BuildError> {
    let LogicalPlan::SetOp {
        left,
        right,
        op: basin_plan::SetOpKind::Union,
        all,
    } = body
    else {
        return Err(BuildError::Unsupported(
            "WITH RECURSIVE body must be UNION [ALL] of an anchor and a recursive term".into(),
        ));
    };
    if right.is_mutating() {
        return Err(BuildError::Unsupported(
            "data-modifying statement inside a WITH RECURSIVE recursive term".into(),
        ));
    }

    let anchor_op = build_inner(left, tables, None, budget, ctes, outer)?;
    let anchor_schema = anchor_op.schema();

    let recursive_plan = right.as_ref().clone();
    let mut snapshot = SnapshotResolver::default();
    snapshot_scans(&recursive_plan, tables, &mut snapshot)?;
    let snapshot = Rc::new(snapshot);
    let ctes_captured = Rc::clone(ctes);
    let schema_for_feed = Arc::clone(&anchor_schema);
    let outer_owned: Option<(RecordBatch, usize)> = outer.map(|(b, i)| (b.clone(), i));

    let recursive_term: RecursiveTermFactory = Box::new(move |working_table: Vec<RecordBatch>| {
        // Bind `name` — this CTE's own name, as it appears inside its
        // recursive term — to a plain one-shot replay of ONLY the working
        // table just finished, never the shared, materialize-once buffer
        // the enclosing `Cte` node is about to register `name` under (that
        // buffer does not even exist while this factory runs, and would be
        // the wrong semantics regardless — recursive.rs's module docs item
        // 2). Re-inserting on every call is deliberate: each iteration's
        // working table replaces the previous one under the same key.
        let feed: Box<dyn Operator> =
            Box::new(VecFeed::new(Arc::clone(&schema_for_feed), working_table));
        ctes_captured
            .borrow_mut()
            .insert(name, CteBuffer::new(feed, budget));
        let bound_outer = outer_owned.as_ref().map(|(b, i)| (b, *i));
        build_inner(
            &recursive_plan,
            snapshot.as_ref(),
            None,
            budget,
            &ctes_captured,
            bound_outer,
        )
        .map_err(build_error_to_exec)
    });

    Ok(Box::new(RecursiveCte::new(
        anchor_op,
        recursive_term,
        *all,
        budget,
        DEFAULT_RECURSION_LIMIT,
    )))
}

/// A `'static`-safe, one-shot replay of an already-materialized set of
/// batches — the recursive term's view of "the previous iteration's working
/// table" (recursive.rs's module docs item 2), and the anchor half of the
/// `Feed` shape every operator file's own test module defines locally.
struct VecFeed {
    schema: SchemaRef,
    batches: std::collections::VecDeque<RecordBatch>,
}

impl VecFeed {
    fn new(schema: SchemaRef, batches: Vec<RecordBatch>) -> Self {
        Self {
            schema,
            batches: batches.into(),
        }
    }
}

impl Operator for VecFeed {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ExecError> {
        Ok(self.batches.pop_front())
    }
}

/// A fully self-contained (owned, no borrowed lifetime) [`TableResolver`]
/// covering exactly the `(table, projection)` pairs a subplan's `Scan`
/// nodes asked for, built once by [`snapshot_scans`] before a
/// `LateralJoin`'s inner side or a `WITH RECURSIVE` recursive term is
/// handed to its factory closure.
///
/// # Why this exists
///
/// `lateral.rs`'s `InnerFactory` and `recursive.rs`'s `RecursiveTermFactory`
/// are both `Box<dyn FnMut(...) -> ...>` with no lifetime parameter, which
/// Rust defaults to `'static` for a boxed trait object with none written.
/// The factory this builder installs has to call [`build_inner`] again on
/// every invocation — once per outer row, or once per iteration — to
/// realise the documented "re-lower and re-bind, then build a fresh
/// physical operator" design (`lateral.rs`'s module docs), which means it
/// needs a `TableResolver` to hand to that call. The one the top-level
/// caller passed to [`build_with_budget`]/[`build_with_dml`] is only
/// `&dyn TableResolver` for the duration of that one call — it cannot
/// satisfy a `'static` closure no matter how long the referent actually
/// lives, because the borrow checker has no way to know that from the type
/// alone. Rather than widen every public entry point's signature to demand
/// an owned, `'static`-safe handle that ordinary (non-correlated,
/// non-recursive) queries never need, this builder eagerly drains exactly
/// the base tables the correlated/recursive subplan touches — ONCE, while
/// the real resolver is still in scope — into a private, fully owned
/// snapshot, and gives the closure that instead.
///
/// This trades pushdown for the correlated/recursive fallback path only:
/// every row/iteration rebuilds against the SAME in-memory snapshot rather
/// than re-querying storage. The always-correct general LATERAL/`WITH
/// RECURSIVE` path this exists to provide (see `lateral.rs`'s module docs
/// on the textual rewrite it backstops) was never meant to match the fast
/// path's I/O profile; only correctness is this path's contract.
#[derive(Default)]
struct SnapshotResolver {
    tables: HashMap<(u32, Vec<usize>), (SchemaRef, Vec<RecordBatch>)>,
}

impl TableResolver for SnapshotResolver {
    fn open(
        &self,
        table: TableId,
        projection: &[usize],
        _filters: &[Expr],
    ) -> Option<(Box<dyn BatchSource>, ScanPushdown)> {
        let (schema, batches) = self.tables.get(&(table.0, projection.to_vec()))?;
        Some((
            Box::new(crate::scan::VecBatchSource::new(
                schema.clone(),
                batches.clone(),
            )),
            // The columns stored are EXACTLY the ones requested, in the
            // requested order — `projection_applied: true` is what tells
            // the `Scan` arm not to re-index them as if they addressed the
            // full, un-projected table.
            ScanPushdown {
                projection_applied: true,
                filters_applied: false,
            },
        ))
    }
}

/// Populate `into` with every `Scan` node's `(table, projection)` reachable
/// from `plan`, draining each through the real `tables` resolver exactly
/// once per distinct pair. See [`SnapshotResolver`]'s doc comment.
fn snapshot_scans(
    plan: &LogicalPlan,
    tables: &dyn TableResolver,
    into: &mut SnapshotResolver,
) -> Result<(), BuildError> {
    if let LogicalPlan::Scan {
        table, projection, ..
    } = plan
    {
        let cols: Vec<usize> = projection.iter().map(|c| c.0 as usize).collect();
        let key = (table.0, cols.clone());
        if let std::collections::hash_map::Entry::Vacant(e) = into.tables.entry(key) {
            let (mut source, _pushed) = tables
                .open(*table, &cols, &[])
                .ok_or(BuildError::UnknownTable(*table))?;
            let schema = source.schema();
            let mut batches = Vec::new();
            while let Some(b) = source.next_batch()? {
                batches.push(b);
            }
            e.insert((schema, batches));
        }
    }
    let mut result = Ok(());
    plan.for_each_input(&mut |child| {
        if result.is_ok() {
            result = snapshot_scans(child, tables, into);
        }
    });
    result
}

fn build_error_to_exec(e: BuildError) -> ExecError {
    match e {
        BuildError::Exec(inner) => inner,
        other => ExecError::Internal(other.to_string()),
    }
}

/// Following the correlation convention `basin_plan::opt::decorrelate`
/// already establishes for correlated subqueries (`ColumnRef::relation ==
/// 1` marks a reference reaching the enclosing query's row — see that
/// module's docs), this builder adopts the same rule for
/// `LogicalPlan::LateralJoin`'s inner side: `Expr` has no dedicated "outer
/// reference" variant of its own, and a LATERAL subplan's correlation is
/// structurally the same "0 is my own scope, 1 is the enclosing one"
/// relationship a join's `on`/`filter` already uses.
const OUTER_REF: u16 = 1;

/// Bind `expr` to a specific outer row for a `LateralJoin`'s per-row
/// rebuild or a `WITH RECURSIVE` recursive term's per-iteration rebuild
/// (module docs: "constant-folding the correlated column references into
/// literals, then building a fresh physical operator from that") — `outer`
/// is `None` everywhere except while rebuilding one of those two shapes —
/// AND fold every uncorrelated scalar subquery `expr` contains into its
/// (once-evaluated) result, regardless of `outer`; see
/// [`materialize_scalar_subquery`]. No longer a no-op clone when `outer` is
/// `None`: that used to mean "nothing to do", but a plain, non-LATERAL
/// `SELECT ... WHERE x = (SELECT ...)` reaches this exact path with `outer:
/// None` and still has a subquery to fold.
fn bind_outer(
    expr: &Expr,
    outer: Outer<'_>,
    tables: &dyn TableResolver,
    dml: Option<&dyn DmlResolver>,
    budget: usize,
    ctes: &CteRegistry,
) -> Result<Expr, BuildError> {
    bind_outer_rec(expr, outer, tables, dml, budget, ctes)
}

fn bind_sort_keys(
    keys: &[PlanSortKey],
    outer: Outer<'_>,
    tables: &dyn TableResolver,
    dml: Option<&dyn DmlResolver>,
    budget: usize,
    ctes: &CteRegistry,
) -> Result<Vec<PlanSortKey>, BuildError> {
    keys.iter()
        .map(|k| {
            Ok(PlanSortKey {
                expr: bind_outer(&k.expr, outer, tables, dml, budget, ctes)?,
                descending: k.descending,
                nulls_first: k.nulls_first,
            })
        })
        .collect()
}

/// The recursive worker behind [`bind_outer`]. Walks every `Expr` variant —
/// mirroring `basin_plan::Expr::for_each_child`'s own exhaustive match —
/// doing two unrelated rewrites in the same pass because both need the same
/// exhaustive traversal and neither cares about the other:
///
/// 1. Replacing `Column(relation == OUTER_REF)` with the corresponding
///    literal from `outer`'s row, when `outer` is `Some`.
/// 2. Replacing an uncorrelated scalar subquery (`Subquery { kind: Scalar,
///    operand: None, .. }`) with its once-evaluated result — see
///    [`materialize_scalar_subquery`]. This runs regardless of `outer`.
///
/// `Subquery`'s own `subplan` is otherwise left untouched by (1) — its
/// `operand`, which belongs to THIS query level, is not — the same "a
/// subquery is a separate query level" rule `Expr::for_each_child` already
/// states for exactly this reason; (2) is the one deliberate exception,
/// because materializing IS building and running that separate query
/// level, once, right here. Aggregate and window `ORDER BY` lists are left
/// unbound by (1): a correlated ordering inside an aggregate/window is a
/// corner this builder does not reach today — narrower than the general
/// case, not silently wrong, since any `Column(OUTER_REF)` actually hiding
/// there simply survives into `eval`, which has no relation-0 column to
/// resolve it against and errors instead of guessing.
fn bind_outer_rec(
    expr: &Expr,
    outer: Outer<'_>,
    tables: &dyn TableResolver,
    dml: Option<&dyn DmlResolver>,
    budget: usize,
    ctes: &CteRegistry,
) -> Result<Expr, BuildError> {
    let b = |e: &Expr| -> Result<Box<Expr>, BuildError> {
        Ok(Box::new(bind_outer_rec(
            e, outer, tables, dml, budget, ctes,
        )?))
    };
    let v = |es: &[Expr]| -> Result<Vec<Expr>, BuildError> {
        es.iter()
            .map(|e| bind_outer_rec(e, outer, tables, dml, budget, ctes))
            .collect()
    };
    let ob = |o: &Option<Box<Expr>>| -> Result<Option<Box<Expr>>, BuildError> {
        o.as_deref().map(b).transpose()
    };
    Ok(match expr {
        Expr::Column(c) if c.relation == OUTER_REF => match outer {
            Some((batch, row)) => outer_literal(batch.column(c.index as usize).as_ref(), row)?,
            None => expr.clone(),
        },
        Expr::Column(_) | Expr::Literal(..) | Expr::Parameter { .. } => expr.clone(),
        Expr::Unary { op, arg } => Expr::Unary {
            op: *op,
            arg: b(arg)?,
        },
        Expr::Binary { op, lhs, rhs } => Expr::Binary {
            op: *op,
            lhs: b(lhs)?,
            rhs: b(rhs)?,
        },
        Expr::Cast { arg, to, kind } => Expr::Cast {
            arg: b(arg)?,
            to: *to,
            kind: *kind,
        },
        Expr::Case {
            operand,
            whens,
            else_,
        } => Expr::Case {
            operand: ob(operand)?,
            whens: whens
                .iter()
                .map(|(w, t)| {
                    Ok((
                        bind_outer_rec(w, outer, tables, dml, budget, ctes)?,
                        bind_outer_rec(t, outer, tables, dml, budget, ctes)?,
                    ))
                })
                .collect::<Result<_, BuildError>>()?,
            else_: ob(else_)?,
        },
        Expr::Coalesce(xs) => Expr::Coalesce(v(xs)?),
        Expr::IsNull { arg, negated } => Expr::IsNull {
            arg: b(arg)?,
            negated: *negated,
        },
        Expr::BoolTest { arg, test } => Expr::BoolTest {
            arg: b(arg)?,
            test: *test,
        },
        Expr::DistinctFrom { lhs, rhs, negated } => Expr::DistinctFrom {
            lhs: b(lhs)?,
            rhs: b(rhs)?,
            negated: *negated,
        },
        Expr::InList { arg, list, negated } => Expr::InList {
            arg: b(arg)?,
            list: v(list)?,
            negated: *negated,
        },
        Expr::Between {
            arg,
            low,
            high,
            symmetric,
            negated,
        } => Expr::Between {
            arg: b(arg)?,
            low: b(low)?,
            high: b(high)?,
            symmetric: *symmetric,
            negated: *negated,
        },
        Expr::Like {
            arg,
            pattern,
            escape,
            case_insensitive,
            negated,
        } => Expr::Like {
            arg: b(arg)?,
            pattern: b(pattern)?,
            escape: ob(escape)?,
            case_insensitive: *case_insensitive,
            negated: *negated,
        },
        Expr::ScalarFn { func, args } => Expr::ScalarFn {
            func: *func,
            args: v(args)?,
        },
        Expr::Aggregate {
            func,
            args,
            distinct,
            filter,
            order_by,
        } => Expr::Aggregate {
            func: *func,
            args: v(args)?,
            distinct: *distinct,
            filter: ob(filter)?,
            order_by: order_by.clone(),
        },
        Expr::Window {
            func,
            args,
            partition_by,
            order_by,
            frame,
        } => Expr::Window {
            func: *func,
            args: v(args)?,
            partition_by: v(partition_by)?,
            order_by: order_by.clone(),
            frame: frame.clone(),
        },
        Expr::SetReturning { func, args } => Expr::SetReturning {
            func: *func,
            args: v(args)?,
        },
        Expr::Subquery {
            kind,
            subplan,
            operand,
        } => {
            let operand = ob(operand)?;
            if *kind == basin_plan::SubqueryKind::Scalar && operand.is_none() {
                materialize_scalar_subquery(subplan, tables, dml, budget, ctes)?
            } else {
                Expr::Subquery {
                    kind: *kind,
                    subplan: subplan.clone(),
                    operand,
                }
            }
        }
        Expr::ArrayLit(xs) => Expr::ArrayLit(v(xs)?),
        Expr::RowLit(xs) => Expr::RowLit(v(xs)?),
        Expr::Subscript { arg, indices } => Expr::Subscript {
            arg: b(arg)?,
            indices: indices
                .iter()
                .map(|s| {
                    Ok(match s {
                        basin_plan::Subscript::Index(e) => basin_plan::Subscript::Index(
                            bind_outer_rec(e, outer, tables, dml, budget, ctes)?,
                        ),
                        basin_plan::Subscript::Slice { lower, upper } => {
                            basin_plan::Subscript::Slice {
                                lower: lower
                                    .as_ref()
                                    .map(|e| bind_outer_rec(e, outer, tables, dml, budget, ctes))
                                    .transpose()?,
                                upper: upper
                                    .as_ref()
                                    .map(|e| bind_outer_rec(e, outer, tables, dml, budget, ctes))
                                    .transpose()?,
                            }
                        }
                    })
                })
                .collect::<Result<_, BuildError>>()?,
        },
        Expr::FieldSelect { arg, field } => Expr::FieldSelect {
            arg: b(arg)?,
            field: *field,
        },
    })
}

/// Build and run `subplan` exactly once, folding its single-row,
/// single-column result into a `Literal` — Postgres's InitPlan: a subquery
/// with no correlated reference to the enclosing query is evaluated once
/// per statement, not once per row (the only version worth having, since
/// nothing about its result can vary row to row). Reached only for
/// `Expr::Subquery { kind: Scalar, operand: None, .. }` (see
/// [`bind_outer_rec`]) — `basin_plan::opt::decorrelate` guarantees `subplan`
/// carries no `Column(relation == OUTER_REF)` by the time a plan reaches
/// this builder still shaped as `Expr::Subquery`: a correlated scalar
/// subquery it CAN decorrelate becomes a join instead, and anything it
/// declines to decorrelate has no correlation predicate to lean on in the
/// first place (see that module's docs). So this always builds with
/// `outer: None`, and any subquery nested inside `subplan` gets the exact
/// same one-shot treatment when `build_inner` reaches it in turn.
///
/// Two Postgres rules for a scalar subquery, both enforced here: zero rows
/// is `NULL` — not an error, and not an empty result for the query this
/// subquery is embedded in, since this only ever produces a `Literal` and
/// never short-circuits anything above it — and more than one row is
/// SQLSTATE 21000 `cardinality_violation` ([`ExecError::CardinalityViolation`]),
/// not a silently-picked first row.
fn materialize_scalar_subquery(
    subplan: &LogicalPlan,
    tables: &dyn TableResolver,
    dml: Option<&dyn DmlResolver>,
    budget: usize,
    ctes: &CteRegistry,
) -> Result<Expr, BuildError> {
    let mut op = build_inner(subplan, tables, dml, budget, ctes, None)?;
    let schema = op.schema();
    if schema.fields().len() != 1 {
        return Err(BuildError::Exec(ExecError::Internal(format!(
            "scalar subquery must return exactly one column, got {} — a planner bug",
            schema.fields().len()
        ))));
    }
    let ty = pg_type_for_arrow(schema.field(0).data_type())?;

    let mut result: Option<Expr> = None;
    while let Some(batch) = op.next_batch().map_err(BuildError::Exec)? {
        let col = batch.column(0).as_ref();
        for row in 0..batch.num_rows() {
            if result.is_some() {
                return Err(BuildError::Exec(ExecError::CardinalityViolation(
                    "more than one row returned by a subquery used as an expression".into(),
                )));
            }
            result = Some(outer_literal(col, row)?);
        }
    }
    Ok(result.unwrap_or(Expr::Literal(basin_plan::Datum::Null, ty)))
}

/// Read one value out of an Arrow array as an [`Expr::Literal`]. Shared by
/// [`bind_outer_rec`]'s `LateralJoin`/`WITH RECURSIVE` outer-row binding and
/// [`materialize_scalar_subquery`]'s InitPlan result — both need "one Arrow
/// value, at a known row, folded into a literal". Only the handful of
/// primitive types either caller is expected to see — matching
/// [`pg_type_for_arrow`]'s coverage — are supported; anything else is
/// refused rather than guessed at.
fn outer_literal(col: &dyn arrow_array::Array, row: usize) -> Result<Expr, BuildError> {
    use arrow_array::{
        BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, StringArray,
    };
    use basin_plan::Datum;

    let ty = pg_type_for_arrow(col.data_type())?;
    if col.is_null(row) {
        return Ok(Expr::Literal(Datum::Null, ty));
    }
    let datum = match col.data_type() {
        arrow_schema::DataType::Boolean => Datum::Bool(
            col.as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(row),
        ),
        arrow_schema::DataType::Int16 => Datum::Int16(
            col.as_any()
                .downcast_ref::<Int16Array>()
                .unwrap()
                .value(row),
        ),
        arrow_schema::DataType::Int32 => Datum::Int32(
            col.as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(row),
        ),
        arrow_schema::DataType::Int64 => Datum::Int64(
            col.as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(row),
        ),
        arrow_schema::DataType::Float32 => Datum::Float32(
            col.as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(row),
        ),
        arrow_schema::DataType::Float64 => Datum::Float64(
            col.as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(row),
        ),
        arrow_schema::DataType::Utf8 => Datum::Utf8(
            col.as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(row)
                .to_string(),
        ),
        other => {
            return Err(BuildError::Unsupported(format!(
                "a correlated reference or scalar subquery result of Arrow type {other:?}"
            )))
        }
    };
    Ok(Expr::Literal(datum, ty))
}

/// The [`PgType`] whose [`basin_pgtype::physical`] round-trips to `dt` —
/// the reverse of the direction `eval.rs`'s `eval_literal` normally needs,
/// required here because [`outer_literal`]'s source column only carries an
/// Arrow type, and the literal replacing it must carry a `PgType` that maps
/// back to that exact Arrow type or `eval` will build the wrong kind of
/// array for it.
fn pg_type_for_arrow(dt: &arrow_schema::DataType) -> Result<PgType, BuildError> {
    use arrow_schema::DataType;
    Ok(match dt {
        DataType::Boolean => PgType::BOOL,
        DataType::Int16 => PgType::INT2,
        DataType::Int32 => PgType::INT4,
        DataType::Int64 => PgType::INT8,
        DataType::Float32 => PgType::FLOAT4,
        DataType::Float64 => PgType::FLOAT8,
        DataType::Utf8 => PgType::TEXT,
        other => {
            return Err(BuildError::Unsupported(format!(
                "a correlated reference or scalar subquery result of Arrow type {other:?}"
            )))
        }
    })
}

/// The column position an expression refers to, if it is a plain column.
fn column_index(e: &Expr) -> Option<usize> {
    match e {
        Expr::Column(c) => Some(c.index as usize),
        _ => None,
    }
}

/// A constant non-negative integer, for `LIMIT`.
fn const_usize(e: &Expr) -> Option<usize> {
    match e {
        Expr::Literal(basin_plan::Datum::Int64(v), _) if *v >= 0 => Some(*v as usize),
        Expr::Literal(basin_plan::Datum::Int32(v), _) if *v >= 0 => Some(*v as usize),
        _ => None,
    }
}

fn sort_keys(keys: &[PlanSortKey]) -> Result<Vec<SortKey>, BuildError> {
    keys.iter()
        .map(|k| {
            Ok(SortKey {
                column: column_index(&k.expr).ok_or(BuildError::NonColumnKey("ORDER BY"))?,
                descending: k.descending,
                nulls_first: k.nulls_first,
            })
        })
        .collect()
}

/// Map a Postgres aggregate function OID to the physical accumulator.
///
/// These are real `pg_proc` OIDs, read from a live PostgreSQL 18 rather than
/// invented — `count(*)` is 2803, and each of sum/min/max/avg has a distinct
/// OID per input type because Postgres resolves aggregates by signature. The
/// physical layer only cares which accumulator to run, so many OIDs collapse to
/// one variant here; the type-specific behaviour lives in the accumulator.
fn agg_func_of(oid: u32) -> Option<AggFunc> {
    Some(match oid {
        2803 => AggFunc::CountStar,
        2107 | 2108 | 2111 | 2114 => AggFunc::Sum,
        2115 | 2116 | 2120 | 2130 => AggFunc::Max,
        2131 | 2132 | 2136 | 2146 => AggFunc::Min,
        2100 | 2101 | 2103 | 2105 => AggFunc::Avg,
        // `array_agg(anynonarray)` and `array_agg(anyarray)` — read
        // individually from a live PostgreSQL 18.2 `pg_proc`, not
        // transcribed as a block (see `window_func_of`'s doc for why that
        // burned this file before: 3108/3110/3111/3112 were assigned to
        // the wrong window functions because lag/lead's overloads
        // interleave in the real table).
        2335 | 4053 => AggFunc::ArrayAgg,
        // `string_agg(text, text)` is 3538, `string_agg(bytea, bytea)` is
        // 3545 — not adjacent to array_agg's OIDs or to each other. The
        // `delim_col` placeholder here is always overwritten in `agg_spec`
        // once the call's second argument (the delimiter, resolved to a
        // column per row) is known; `agg_func_of` only sees the OID, not
        // the argument list.
        3538 | 3545 => AggFunc::StringAgg { delim_col: 0 },
        _ => return None,
    })
}

/// Translate a logical aggregate into the physical operator's spec.
///
/// The physical aggregate keys on column positions rather than expressions —
/// an argument more complex than a column reference has to be materialised by a
/// `Project` beneath the aggregate. That is the optimizer's job, so an
/// unexpected shape is reported rather than improvised around.
fn agg_spec(e: &Expr, alias: &str) -> Result<AggregateSpec, BuildError> {
    match e {
        Expr::Aggregate {
            func,
            args,
            distinct,
            filter,
            order_by,
        } => {
            if !order_by.is_empty() {
                return Err(BuildError::Unsupported(
                    "ORDER BY inside an aggregate".into(),
                ));
            }
            let mut f = agg_func_of(func.0.get()).ok_or_else(|| {
                BuildError::Unsupported(format!("aggregate function with OID {}", func.0.get()))
            })?;
            let input_col = match args.first() {
                None => None,
                Some(a) => {
                    // `count(x)` counts non-null values; `count(*)` counts
                    // rows. They share no OID, but a Count with an argument
                    // must not be run as CountStar or nulls would be counted.
                    if f == AggFunc::CountStar {
                        f = AggFunc::Count;
                    }
                    Some(column_index(a).ok_or(BuildError::NonColumnKey("aggregate"))?)
                }
            };
            // `string_agg(value, delimiter)`'s delimiter is read per row
            // from a resolved column (`AggFunc::StringAgg::delim_col`'s own
            // doc), not carried as a constant — a literal delimiter still
            // arrives here as a column reference because the optimizer
            // materialises any non-column aggregate argument beneath the
            // aggregate first (see `agg_spec`'s own doc). `agg_func_of`
            // cannot fill this in itself since it only sees the OID, not
            // the argument list.
            if let AggFunc::StringAgg { .. } = f {
                let delim_expr = args.get(1).ok_or_else(|| {
                    BuildError::Unsupported("string_agg without a delimiter argument".into())
                })?;
                let delim_col = column_index(delim_expr)
                    .ok_or(BuildError::NonColumnKey("string_agg delimiter"))?;
                f = AggFunc::StringAgg { delim_col };
            }
            let filter_col = match filter {
                None => None,
                Some(x) => {
                    Some(column_index(x).ok_or(BuildError::NonColumnKey("aggregate FILTER"))?)
                }
            };
            Ok(AggregateSpec {
                func: f,
                input_col,
                distinct: *distinct,
                filter_col,
                alias: alias.to_string(),
            })
        }
        _ => Err(BuildError::Unsupported(
            "non-aggregate expression in an aggregate list".into(),
        )),
    }
}

/// Postgres OIDs for the window functions, from `pg_proc` on a live server.
///
/// Ranking and offset functions are true window functions with their own
/// OIDs; sum/count/min/max/avg are ordinary aggregates used in a window
/// context and reuse the aggregate OIDs, which is why this falls through to
/// the aggregate mapping rather than duplicating it.
fn window_func_of(oid: u32) -> Option<WindowFunc> {
    Some(match oid {
        3100 => WindowFunc::RowNumber,
        3101 => WindowFunc::Rank,
        3102 => WindowFunc::DenseRank,
        // The oids below are the real ones, dumped from PostgreSQL 18.2:
        //
        //   3106 lag(anyelement)                    3109 lead(anyelement)
        //   3107 lag(anyelement, integer)           3110 lead(anyelement, integer)
        //   3108 lag(anycompatible, int, anycompat) 3111 lead(anycompatible, ...)
        //   3112 first_value   3113 last_value      3114 nth_value
        //
        // This table previously read 3108 as lead, 3110 as first_value, 3111 as
        // last_value and 3112 as nth_value — every one of them off by the size
        // of lag's and lead's overload blocks. `basin-pgtype`'s func.rs had the
        // right oids all along, so the two tables disagreed, and the visible
        // symptom was only that `first_value(x)` fell back: it resolved to 3112,
        // arrived here as NthValue, and failed the arity check.
        //
        // The invisible symptom was the dangerous one. `lead(x, n)` resolves to
        // 3110, which this table called FirstValue — an arity the builder
        // accepts. That is a wrong answer, not a fallback.
        3106 | 3107 => WindowFunc::Lag,
        3109 | 3110 => WindowFunc::Lead,
        3112 => WindowFunc::FirstValue,
        3113 => WindowFunc::LastValue,
        3114 => WindowFunc::NthValue,
        // 3108 and 3111 — lag/lead's THREE-argument form, which supplies a
        // default for rows with no peer at the offset — are deliberately absent.
        // `window_spec` reads args[0] and args[1] and has nowhere to put a
        // default, so mapping them here would silently drop it and return NULL
        // where Postgres returns the default. Falling back is the correct
        // answer until the operator can carry one.
        2803 => WindowFunc::CountStar,
        2107 | 2108 | 2111 | 2114 => WindowFunc::Sum,
        2115 | 2116 | 2120 | 2130 => WindowFunc::Max,
        2131 | 2132 | 2136 | 2146 => WindowFunc::Min,
        // 2102 is avg(smallint), which was missing — the others are
        // avg(bigint)/avg(integer)/avg(numeric)/avg(float8).
        2100 | 2101 | 2102 | 2103 | 2105 => WindowFunc::Avg,
        _ => return None,
    })
}

/// The PARTITION BY / ORDER BY shared by every window expression in one node.
///
/// They must agree: the operator sorts nothing and computes one partitioning,
/// so two windows with different keys in one node would silently produce wrong
/// answers for one of them. Disagreement is reported rather than resolved.
fn window_keys(windows: &[Expr]) -> Result<(Vec<usize>, Vec<OrderKey>), BuildError> {
    let mut part: Option<Vec<usize>> = None;
    let mut ord: Option<Vec<OrderKey>> = None;
    for w in windows {
        let Expr::Window {
            partition_by,
            order_by,
            ..
        } = w
        else {
            return Err(BuildError::Unsupported(
                "non-window expression in a window list".into(),
            ));
        };
        let p = partition_by
            .iter()
            .map(|e| column_index(e).ok_or(BuildError::NonColumnKey("PARTITION BY")))
            .collect::<Result<Vec<_>, _>>()?;
        let o = order_by
            .iter()
            .map(|k| {
                Ok(OrderKey {
                    column: column_index(&k.expr)
                        .ok_or(BuildError::NonColumnKey("window ORDER BY"))?,
                    descending: k.descending,
                    nulls_first: k.nulls_first,
                })
            })
            .collect::<Result<Vec<_>, BuildError>>()?;
        if let Some(prev) = &part {
            if *prev != p {
                return Err(BuildError::Unsupported(
                    "window expressions with differing PARTITION BY in one node".into(),
                ));
            }
        }
        if let Some(prev) = &ord {
            let same = prev.len() == o.len()
                && prev
                    .iter()
                    .zip(&o)
                    .all(|(a, b)| a.column == b.column && a.descending == b.descending);
            if !same {
                return Err(BuildError::Unsupported(
                    "window expressions with differing ORDER BY in one node".into(),
                ));
            }
        }
        part = Some(p);
        ord = Some(o);
    }
    Ok((part.unwrap_or_default(), ord.unwrap_or_default()))
}

fn window_spec(e: &Expr, alias: &str) -> Result<WindowSpec, BuildError> {
    let Expr::Window {
        func, args, frame, ..
    } = e
    else {
        return Err(BuildError::Unsupported(
            "non-window expression in a window list".into(),
        ));
    };
    let mut f = window_func_of(func.0.get()).ok_or_else(|| {
        BuildError::Unsupported(format!("window function with OID {}", func.0.get()))
    })?;
    // count(x) counts non-null values; count(*) counts rows. Same OID split as
    // the aggregate path, and the same consequence if conflated.
    let arg_col = match args.first() {
        None => None,
        Some(a) => {
            if f == WindowFunc::CountStar {
                f = WindowFunc::Count;
            }
            Some(column_index(a).ok_or(BuildError::NonColumnKey("window argument"))?)
        }
    };
    let offset_col = match args.get(1) {
        None => None,
        Some(a) => Some(column_index(a).ok_or(BuildError::NonColumnKey("window offset"))?),
    };
    let default_col = match args.get(2) {
        None => None,
        Some(a) => Some(column_index(a).ok_or(BuildError::NonColumnKey("window default"))?),
    };
    Ok(WindowSpec {
        func: f,
        arg_col,
        offset_col,
        default_col,
        nth_col: if f == WindowFunc::NthValue {
            offset_col
        } else {
            None
        },
        frame: frame_of(frame)?,
        alias: alias.to_string(),
    })
}

/// Carry the plan's frame across. `None` means the SQL had no explicit frame,
/// which the operator resolves to Postgres's default — NOT "the whole
/// partition". With an ORDER BY the default is RANGE UNBOUNDED PRECEDING TO
/// CURRENT ROW, which is why `last_value(x) OVER (ORDER BY y)` returns the
/// current row's value rather than the partition's last.
fn frame_of(f: &basin_plan::WindowFrame) -> Result<Option<crate::window::WindowFrame>, BuildError> {
    use crate::window::{FrameBound as XB, FrameUnits as XU, WindowFrame as XF};
    use basin_plan::{FrameBound as PB, FrameUnits as PU};

    let units = match f.units {
        PU::Rows => XU::Rows,
        PU::Range => XU::Range,
        PU::Groups => XU::Groups,
    };

    // A frame offset is a plan-time constant here, matching how sort keys are
    // resolved rather than evaluated. ROWS and GROUPS count rows and peer
    // groups; RANGE's offset is in the ORDER BY column's own unit, so an
    // INTERVAL bound against a timestamp is not representable as an f64 and is
    // refused rather than silently coerced into a wrong window.
    let bound = |b: &PB| -> Result<XB, BuildError> {
        Ok(match b {
            PB::UnboundedPreceding => XB::UnboundedPreceding,
            PB::CurrentRow => XB::CurrentRow,
            PB::UnboundedFollowing => XB::UnboundedFollowing,
            PB::Preceding(e) => XB::Preceding(offset_of(e, units)?),
            PB::Following(e) => XB::Following(offset_of(e, units)?),
        })
    };

    Ok(Some(XF {
        units,
        start: bound(&f.start)?,
        end: bound(&f.end)?,
    }))
}

fn offset_of(
    e: &Expr,
    units: crate::window::FrameUnits,
) -> Result<crate::window::FrameOffset, BuildError> {
    use crate::window::{FrameOffset, FrameUnits};
    let n = match e {
        Expr::Literal(basin_plan::Datum::Int64(v), _) if *v >= 0 => *v as f64,
        Expr::Literal(basin_plan::Datum::Int32(v), _) if *v >= 0 => *v as f64,
        Expr::Literal(basin_plan::Datum::Float64(v), _) if *v >= 0.0 => *v,
        _ => {
            return Err(BuildError::Unsupported(
                "window frame offset must be a non-negative constant (INTERVAL bounds not yet \
                 representable)"
                    .into(),
            ))
        }
    };
    Ok(match units {
        FrameUnits::Rows | FrameUnits::Groups => FrameOffset::Count(n as u64),
        FrameUnits::Range => FrameOffset::Range(n),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, Int32Array, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use basin_pgtype::PgType;
    use basin_plan::{ColId, ColumnRef, Datum, SnapshotId};
    use std::sync::Arc;

    /// Pins every window oid against the real `pg_proc` block, because this
    /// table was wrong by exactly the width of lag's and lead's overload runs
    /// and nothing caught it. Two agents investigated "why does `first_value`
    /// fall back" without finding it, because the fallback was a symptom of a
    /// mapping error three oids away.
    ///
    /// Dumped from PostgreSQL 18.2:
    ///   3100 row_number  3101 rank       3102 dense_rank
    ///   3106 lag(any)    3107 lag(any,int)   3108 lag(any,int,any)
    ///   3109 lead(any)   3110 lead(any,int)  3111 lead(any,int,any)
    ///   3112 first_value 3113 last_value 3114 nth_value
    #[test]
    fn window_oids_match_the_real_pg_proc_block() {
        assert_eq!(window_func_of(3100), Some(WindowFunc::RowNumber));
        assert_eq!(window_func_of(3101), Some(WindowFunc::Rank));
        assert_eq!(window_func_of(3102), Some(WindowFunc::DenseRank));
        assert_eq!(window_func_of(3106), Some(WindowFunc::Lag));
        assert_eq!(window_func_of(3107), Some(WindowFunc::Lag));
        assert_eq!(window_func_of(3109), Some(WindowFunc::Lead));
        assert_eq!(window_func_of(3110), Some(WindowFunc::Lead));
        assert_eq!(window_func_of(3112), Some(WindowFunc::FirstValue));
        assert_eq!(window_func_of(3113), Some(WindowFunc::LastValue));
        assert_eq!(window_func_of(3114), Some(WindowFunc::NthValue));
    }

    /// `lead(x, n)` is oid 3110. This table used to call that `FirstValue` —
    /// an arity the builder ACCEPTS, so the query ran and returned the wrong
    /// column. Every other error in the block produced a fallback; this one
    /// produced an answer. Pinned separately so its significance is not lost
    /// among the rest.
    #[test]
    fn lead_with_an_offset_is_lead_and_not_first_value() {
        assert_eq!(window_func_of(3110), Some(WindowFunc::Lead));
        assert_ne!(window_func_of(3110), Some(WindowFunc::FirstValue));
    }

    /// lag/lead's three-argument form carries a DEFAULT for rows with no peer
    /// at the offset. `window_spec` reads only args[0] and args[1], so mapping
    /// these would silently drop the default and return NULL where Postgres
    /// returns it. Absent on purpose; falling back is the right answer.
    #[test]
    fn the_three_argument_lag_and_lead_forms_are_refused_not_silently_truncated() {
        assert_eq!(window_func_of(3108), None);
        assert_eq!(window_func_of(3111), None);
    }

    fn table() -> (Arc<Schema>, RecordBatch) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("v", DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
                Arc::new(Int32Array::from(vec![10, 20, 30, 40])),
            ],
        )
        .unwrap();
        (schema, batch)
    }

    fn resolver() -> MemTableResolver {
        let (schema, batch) = table();
        let mut r = MemTableResolver::new();
        r.insert(TableId(1), schema, vec![batch]);
        r
    }

    fn col(i: u16, name: &str) -> Expr {
        Expr::Column(ColumnRef {
            relation: 0,
            index: i,
            name: name.into(),
        })
    }

    fn scan_plan(projection: Vec<ColId>, filters: Vec<Expr>) -> LogicalPlan {
        LogicalPlan::Scan {
            table: TableId(1),
            projection,
            filters,
            snapshot: SnapshotId(0),
        }
    }

    fn drain(mut op: Box<dyn Operator>) -> Vec<RecordBatch> {
        let mut out = Vec::new();
        while let Some(b) = op.next_batch().unwrap() {
            out.push(b);
        }
        out
    }

    /// The whole point of this module: a plan becomes operators and produces
    /// rows. Before this existed, every operator was individually correct and
    /// no query could run.
    #[test]
    fn a_scan_plan_executes_end_to_end() {
        let batches =
            drain(build(&scan_plan(vec![ColId(0), ColId(1)], vec![]), &resolver()).unwrap());
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 4);
        assert_eq!(batches[0].num_columns(), 2);
    }

    /// Filter over scan — the predicate reaches the evaluator through the
    /// operator tree, which is the seam this module exists to close.
    #[test]
    fn filter_over_scan_executes_and_reduces_rows() {
        let plan = LogicalPlan::Filter {
            input: Box::new(scan_plan(vec![ColId(0), ColId(1)], vec![])),
            // id > 2 — OID 521 is int4 '>', verified against pg_operator.
            predicate: Expr::Binary {
                op: basin_plan::OpId(basin_pgtype::Oid(521)),
                lhs: Box::new(col(0, "id")),
                rhs: Box::new(Expr::Literal(Datum::Int32(2), PgType::INT4)),
            },
        };
        let rows: usize = drain(build(&plan, &resolver()).unwrap())
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(rows, 2, "ids 3 and 4 survive `id > 2`");
    }

    /// `ORDER BY … LIMIT` must become TopK, not a full Sort followed by a
    /// truncation. Basin's published numbers for this shape depend on early
    /// termination, so the recognition is load-bearing.
    #[test]
    fn order_by_limit_becomes_top_k() {
        let plan = LogicalPlan::Limit {
            input: Box::new(LogicalPlan::Sort {
                input: Box::new(scan_plan(vec![ColId(0), ColId(1)], vec![])),
                keys: vec![PlanSortKey {
                    expr: col(0, "id"),
                    descending: true,
                    nulls_first: false,
                }],
            }),
            skip: None,
            fetch: Some(Expr::Literal(Datum::Int64(2), PgType::INT8)),
            with_ties: false,
        };
        let rows: usize = drain(build(&plan, &resolver()).unwrap())
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(rows, 2);
    }

    // ── array_agg / string_agg OID WIRING ───────────────────────────────
    //
    // `aggregate.rs` (commit 2d4d481f) implements both accumulators and is
    // unit-tested against them directly via `AggregateSpec` literals — see
    // `array_agg_spec`/`string_agg_spec` in that file's own test module.
    // What was missing was `agg_func_of` ever mapping `array_agg`'s and
    // `string_agg`'s real `pg_proc` OIDs to those variants, so every call
    // hit the `ok_or_else` in `agg_spec` and fell back. These tests pin the
    // OIDs (dumped from a live PostgreSQL 18.2 — see the query in
    // `agg_func_of`'s own comment) and exercise the full `LogicalPlan ->
    // build() -> RecordBatch` path, not just the accumulator in isolation.

    /// A table with a `Utf8` pair, for `string_agg` — `table()`'s two
    /// `Int32` columns can't stand in for a value/delimiter pair.
    fn table_text() -> (Arc<Schema>, RecordBatch) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("grp", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("delim", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 1, 2])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
                // Every row's delimiter is the same literal here, but it is
                // still a resolved COLUMN, not a constant folded into the
                // spec — `string_agg`'s delimiter is read per row (see
                // `AggFunc::StringAgg::delim_col`'s doc), which is exactly
                // what distinguishes it from every other aggregate's single
                // `input_col`.
                Arc::new(StringArray::from(vec![",", ",", "|"])),
            ],
        )
        .unwrap();
        (schema, batch)
    }

    fn resolver_text() -> MemTableResolver {
        let (schema, batch) = table_text();
        let mut r = MemTableResolver::new();
        r.insert(TableId(1), schema, vec![batch]);
        r
    }

    fn agg_expr(oid: u32, args: Vec<Expr>) -> Expr {
        Expr::Aggregate {
            func: basin_plan::FuncId(basin_pgtype::Oid(oid)),
            args,
            distinct: false,
            filter: None,
            order_by: vec![],
        }
    }

    /// `array_agg(anynonarray)` is oid 2335 on a live PostgreSQL 18.2 —
    /// `array_agg(id)` over an `int4` column resolves to this overload, not
    /// 4053 (`array_agg(anyarray)`, i.e. `array_agg` of an *array-typed*
    /// column, which `id` is not). Ungrouped, so the whole table collapses
    /// to one output row holding every `id` in scan order.
    #[test]
    fn array_agg_oid_2335_reaches_the_array_agg_accumulator() {
        let plan = LogicalPlan::Aggregate {
            input: Box::new(scan_plan(vec![ColId(0), ColId(1)], vec![])),
            group: vec![],
            aggs: vec![agg_expr(2335, vec![col(0, "id")])],
            grouping_sets: None,
        };
        let batches = drain(build(&plan, &resolver()).unwrap());
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1, "ungrouped: one output row");
        let list = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::ListArray>()
            .unwrap();
        assert!(!list.is_null(0));
        let elems = list
            .value(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .clone();
        assert_eq!(
            (0..elems.len()).map(|i| elems.value(i)).collect::<Vec<_>>(),
            vec![1, 2, 3, 4],
            "array_agg(id) with no ORDER BY still preserves scan order"
        );
    }

    /// `string_agg(text, text)` is oid 3538. Grouped by `grp`, with a
    /// per-row delimiter column (see `table_text`'s doc) — this is the shape
    /// that specifically exercises `agg_spec` filling in `delim_col` from
    /// `args[1]` rather than `agg_func_of`'s placeholder `0`.
    #[test]
    fn string_agg_oid_3538_reaches_the_string_agg_accumulator_with_its_delimiter_column() {
        let plan = LogicalPlan::Aggregate {
            input: Box::new(LogicalPlan::Scan {
                table: TableId(1),
                projection: vec![ColId(0), ColId(1), ColId(2)],
                filters: vec![],
                snapshot: SnapshotId(0),
            }),
            group: vec![col(0, "grp")],
            aggs: vec![agg_expr(3538, vec![col(1, "name"), col(2, "delim")])],
            grouping_sets: None,
        };
        let batches = drain(build(&plan, &resolver_text()).unwrap());
        let mut rows: Vec<(i32, Option<String>)> = Vec::new();
        for b in &batches {
            let grp = b.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
            let sa = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
            for i in 0..b.num_rows() {
                rows.push((
                    grp.value(i),
                    (!sa.is_null(i)).then(|| sa.value(i).to_string()),
                ));
            }
        }
        rows.sort_unstable();
        assert_eq!(
            rows,
            vec![(1, Some("a,b".into())), (2, Some("c".into()))],
            "group 1 joins on its own rows' delimiter ','; group 2's row uses '|'"
        );
    }

    /// `array_agg(x ORDER BY y)` is real syntax, but `AggregateSpec` has no
    /// field to carry the ordering — the refusal a few lines above (`if
    /// !order_by.is_empty() { ... }`) is unconditional, for every aggregate,
    /// and must survive wiring `array_agg`'s OID in. Silently dropping the
    /// `ORDER BY` would return an array in scan order while claiming to
    /// honour the query's requested order — wrong data, not a missing
    /// feature.
    #[test]
    fn array_agg_order_by_still_refuses_rather_than_silently_reordering() {
        let plan = LogicalPlan::Aggregate {
            input: Box::new(scan_plan(vec![ColId(0), ColId(1)], vec![])),
            group: vec![],
            aggs: vec![Expr::Aggregate {
                func: basin_plan::FuncId(basin_pgtype::Oid(2335)),
                args: vec![col(0, "id")],
                distinct: false,
                filter: None,
                order_by: vec![PlanSortKey {
                    expr: col(1, "v"),
                    descending: false,
                    nulls_first: false,
                }],
            }],
            grouping_sets: None,
        };
        let err = match build(&plan, &resolver()) {
            Err(e) => e,
            Ok(_) => panic!("array_agg(x ORDER BY y) has nowhere to carry the ordering"),
        };
        assert_eq!(
            err,
            BuildError::Unsupported("ORDER BY inside an aggregate".into())
        );
    }

    // ── VALUES IN FROM ──────────────────────────────────────────────────
    //
    // `basin-plan` commit 8a87750d lowers `SELECT * FROM (VALUES ...) AS
    // v(i, s)` — confirmed by that crate's own
    // `a_values_list_in_from_may_be_column_aliased` test — to a `Project`
    // (renaming to the alias list) over a `LogicalPlan::Values` (still
    // carrying its own default `column1`/`column2` names internally; the
    // alias list only renames the outer `Scope`'s copy of the schema, per
    // `build_range_subselect`'s doc, not the `Values` node's own). Both
    // `LogicalPlan::Project` and `LogicalPlan::Values` already had
    // operators and builder arms *before* that lowering commit (`Values`
    // since c14120ea) — so despite that commit's own message saying "VALUES
    // in FROM still falls back", nothing here was actually missing; this
    // test exists to nail that down rather than take it on faith.
    #[test]
    fn values_in_from_with_a_column_alias_list_builds_end_to_end() {
        let plan = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Values {
                rows: vec![
                    vec![
                        Expr::Literal(Datum::Int32(1), PgType::INT4),
                        Expr::Literal(Datum::Utf8("a".into()), PgType::TEXT),
                    ],
                    vec![
                        Expr::Literal(Datum::Int32(2), PgType::INT4),
                        Expr::Literal(Datum::Utf8("b".into()), PgType::TEXT),
                    ],
                ],
                schema: vec![
                    ("column1".into(), PgType::INT4),
                    ("column2".into(), PgType::TEXT),
                ],
            }),
            exprs: vec![(col(0, "i"), "i".into()), (col(1, "s"), "s".into())],
        };
        let batches = drain(build(&plan, &resolver()).unwrap());
        assert_eq!(
            batches[0]
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect::<Vec<_>>(),
            vec!["i", "s"],
            "the alias list renames the output columns, not just the Scope"
        );
        let ids: Vec<i32> = batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .iter()
                    .map(|v| v.unwrap())
                    .collect::<Vec<_>>()
            })
            .collect();
        let ss: Vec<String> = batches
            .iter()
            .flat_map(|b| {
                b.column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .map(|v| v.unwrap().to_string())
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(ids, vec![1, 2]);
        assert_eq!(ss, vec!["a".to_string(), "b".to_string()]);
    }

    /// An unimplemented plan shape must say so by name rather than silently
    /// doing something else. A builder that guesses produces wrong answers
    /// instead of errors.
    #[test]
    fn an_unbuildable_plan_names_the_construct() {
        // This test exists to go stale. DISTINCT was the example, then window
        // functions, then CTEs — all three build now (and so do LATERAL,
        // WITH RECURSIVE, and INSERT/UPDATE/DELETE). GROUPING SETS / ROLLUP /
        // CUBE is the current frontier — when this fails again, that is the
        // good outcome and the next unimplemented shape takes its place.
        let plan = LogicalPlan::Aggregate {
            input: Box::new(scan_plan(vec![ColId(0), ColId(1)], vec![])),
            group: vec![col(0, "id")],
            aggs: vec![],
            grouping_sets: Some(basin_plan::GroupingSets(vec![vec![0]])),
        };
        let err = match build(&plan, &resolver()) {
            Err(e) => e,
            Ok(_) => panic!("GROUPING SETS has no operator yet and must not build"),
        };
        assert_eq!(
            err,
            BuildError::Unsupported("GROUPING SETS / ROLLUP / CUBE".into())
        );
    }

    #[test]
    fn an_unknown_table_is_reported_not_papered_over() {
        let plan = LogicalPlan::Scan {
            table: TableId(999),
            projection: vec![ColId(0)],
            filters: vec![],
            snapshot: SnapshotId(0),
        };
        let err = match build(&plan, &resolver()) {
            Err(e) => e,
            Ok(_) => panic!("an unknown table must not build"),
        };
        assert_eq!(err, BuildError::UnknownTable(TableId(999)));
    }

    /// `WITH TIES` currently degrades to `ONLY` in the DataFusion path
    /// (select_advanced.rs:455), silently returning fewer rows than Postgres.
    /// The owned builder refuses instead — an error beats a wrong row count.
    #[test]
    fn with_ties_is_refused_rather_than_silently_degraded() {
        let plan = LogicalPlan::Limit {
            input: Box::new(scan_plan(vec![ColId(0)], vec![])),
            skip: None,
            fetch: Some(Expr::Literal(Datum::Int64(1), PgType::INT8)),
            with_ties: true,
        };
        let err = match build(&plan, &resolver()) {
            Err(e) => e,
            Ok(_) => panic!("WITH TIES must be refused, not silently degraded"),
        };
        assert!(matches!(err, BuildError::Unsupported(ref s) if s.contains("WITH TIES")));
    }
    /// A set-returning function reaches the operator through the builder.
    /// `generate_series` in a target list is one of the two shapes that
    /// motivated replacing DataFusion at all, so this is the first time it is
    /// reachable from a plan rather than only from a hand-built operator.
    #[test]
    fn a_set_returning_function_builds_and_expands() {
        let plan = LogicalPlan::ProjectSet {
            input: Box::new(LogicalPlan::Empty {
                produce_one_row: true,
                schema: vec![],
            }),
            // generate_series(1, 3) — OID 1067 is the two-argument int form,
            // from pg_proc on a live server.
            srfs: vec![Expr::SetReturning {
                func: basin_plan::FuncId(basin_pgtype::Oid(1067)),
                args: vec![
                    Expr::Literal(Datum::Int32(1), PgType::INT4),
                    Expr::Literal(Datum::Int32(3), PgType::INT4),
                ],
            }],
        };
        let rows: usize = drain(build(&plan, &resolver()).unwrap())
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(
            rows, 3,
            "generate_series(1,3) expands one input row to three"
        );
    }

    // ── JOIN ─────────────────────────────────────────────────────────────
    //
    // Reached from a real `SELECT ... JOIN ... ON ...` these build/exec-error
    // out (see `fallback_histogram`'s per-query attribution and
    // `docs/migration/df-removal` session notes for `165cd438`/`3cb16d97`,
    // the panic fix and the projection-pruning fix that preceded this). This
    // module had ZERO coverage of `LogicalPlan::Join` -> `HashJoin` before
    // these tests — only `LateralJoin` (a different node) was exercised.
    //
    // Root-caused here (in `basin-exec`, not `basin-plan`): with the
    // asymmetric-width fixtures below, driving a real
    // `lower_select`+`optimize_default` plan through `build()` succeeds
    // end-to-end (self-join, 3-row result) — `column_index()` correctly
    // reads a join key's `ColumnRef::index` as-is because
    // `lower/select.rs::split_equijoin_conjuncts` already rebases the
    // right-hand operand of an `on` pair to be relative to the RIGHT input's
    // own schema (see that function's doc comment) before `Join::on` ever
    // reaches this builder — there is no flat-vs-per-side confusion left in
    // `build.rs` for `on` to trip over. The four SQL shapes in the histogram
    // still fall back, but for reasons outside this file:
    //
    // - Both joins: `basin-plan/src/opt/projection.rs`'s `Join` arm of
    //   `prune()` (`collect_both_conventions`) applies the FLAT-position
    //   interpretation to `on`'s right-hand element too. That element is
    //   already right-schema-relative (rebased, per the paragraph above),
    //   so treating it as an unrebased flat position across the
    //   concatenated scope misattributes it to `left_required` whenever its
    //   (right-relative) index is `< left_width` — which prunes the join
    //   key clean out of the right scan's projection. Confirmed directly:
    //   building the SAME plan with `optimize_default` skipped succeeds;
    //   running it through `optimize_default` first leaves the right scan
    //   with `projection: []` and `build()` reports exactly
    //   `join key index 0 is out of range for the right side's 0-column
    //   schema`. Fix belongs in that `Join` arm: `r`'s columns should be
    //   attributed to `right_required` directly (offset by 0, not
    //   `left_width`), not run through the same flat/relation-1 dispatch as
    //   `l`.
    // - `WITH x AS (SELECT id FROM t) SELECT id FROM x`: builds and executes
    //   fine here (see the CTE section below, and `CteRegistry` in this
    //   file) — the plan itself never gets a chance to run. The bridge in
    //   `basin-engine/src/owned_engine.rs`'s `collect_tables_stmt` walks
    //   only `stmt.from_clause` (plus `larg`/`rarg` for a set op) and never
    //   `stmt.with_clause`, so a CTE's real table references (here, `t`
    //   inside the CTE body) are never prefetched into the resolver, and
    //   the CTE's own name (`x`) is looked up as if it were a catalog table
    //   and fails — `Fallback::Ineligible("table not found in the
    //   catalog")`, before `lower_select` is ever called.
    // - `SELECT generate_series(1,3)`: `ProjectSet` builds and expands fine
    //   here (see `a_set_returning_function_builds_and_expands` above) —
    //   again, the plan never gets that far. `basin-plan/src/lower/select.rs`'s
    //   `lower_target_list` hard-refuses with `LowerError::Unsupported`
    //   ("set-returning functions in the SELECT list are not yet lowered")
    //   the moment it sees `expr.contains_srf()`, rather than building the
    //   `LogicalPlan::ProjectSet` this file already knows how to run.
    //
    // None of the three bullets above are in `crates/basin-exec/**`.

    /// A left side and a right side with DIFFERENT widths and the join key
    /// at a DIFFERENT position on each side (left index 2, right index 0) —
    /// deliberately not the "both sides are column 0 of equal-width
    /// schemas" shape that a flat-vs-per-side index mixup could pass by
    /// coincidence. If `build()` ever started treating a join key as a flat
    /// position across the concatenated left+right schema (rather than the
    /// per-side position `HashJoin::new` documents), the right key here
    /// would land out of bounds (right schema is 1 column wide) and this
    /// would fail loudly rather than quietly return the wrong rows.
    fn asymmetric_join_resolver() -> MemTableResolver {
        let left_schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int32, true),
            Field::new("y", DataType::Int32, true),
            Field::new("id", DataType::Int32, true),
        ]));
        let left_batch = RecordBatch::try_new(
            left_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![100, 100, 100])),
                Arc::new(Int32Array::from(vec![200, 201, 202])),
                Arc::new(Int32Array::from(vec![1, 2, 5])),
            ],
        )
        .unwrap();

        let right_schema = Arc::new(Schema::new(vec![Field::new("rid", DataType::Int32, true)]));
        let right_batch = RecordBatch::try_new(
            right_schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let mut r = MemTableResolver::new();
        r.insert(TableId(1), left_schema, vec![left_batch]);
        r.insert(TableId(2), right_schema, vec![right_batch]);
        r
    }

    fn asymmetric_join_plan(kind: basin_plan::JoinKind) -> LogicalPlan {
        LogicalPlan::Join {
            left: Box::new(LogicalPlan::Scan {
                table: TableId(1),
                projection: vec![ColId(0), ColId(1), ColId(2)],
                filters: vec![],
                snapshot: SnapshotId(0),
            }),
            right: Box::new(LogicalPlan::Scan {
                table: TableId(2),
                projection: vec![ColId(0)],
                filters: vec![],
                snapshot: SnapshotId(0),
            }),
            kind,
            on: vec![(col(2, "id"), col(0, "rid"))],
            filter: None,
        }
    }

    /// Inner join, asymmetric widths: only `id`s 1 and 2 are on both sides
    /// (5 has no match on the right, 3 has no match on the left), so an
    /// inner join keeps exactly those two rows — and, crucially, keeps them
    /// with the RIGHT `id`/`rid` values lined up with the correct row, which
    /// a scrambled key index would not reliably do.
    #[test]
    fn an_inner_join_with_asymmetric_widths_matches_the_right_rows() {
        let plan = asymmetric_join_plan(basin_plan::JoinKind::Inner);
        let batches = drain(build(&plan, &asymmetric_join_resolver()).unwrap());
        let mut pairs: Vec<(i32, i32)> = Vec::new();
        for b in &batches {
            let id = b.column(2).as_any().downcast_ref::<Int32Array>().unwrap();
            let rid = b.column(3).as_any().downcast_ref::<Int32Array>().unwrap();
            for i in 0..b.num_rows() {
                pairs.push((id.value(i), rid.value(i)));
            }
        }
        pairs.sort_unstable();
        assert_eq!(
            pairs,
            vec![(1, 1), (2, 2)],
            "only id=1 and id=2 match on both sides, each id lined up with the SAME rid"
        );
    }

    /// Left join, asymmetric widths: every left row survives (1, 2, 5), with
    /// `rid` NULL for the unmatched `id=5` — proving the join key comparison
    /// used the right side's OWN column 0, not some flat position that would
    /// either miss real matches or fabricate ones.
    #[test]
    fn a_left_join_with_asymmetric_widths_keeps_every_left_row() {
        let plan = asymmetric_join_plan(basin_plan::JoinKind::Left);
        let batches = drain(build(&plan, &asymmetric_join_resolver()).unwrap());
        let mut pairs: Vec<(i32, Option<i32>)> = Vec::new();
        for b in &batches {
            let id = b.column(2).as_any().downcast_ref::<Int32Array>().unwrap();
            let rid = b.column(3).as_any().downcast_ref::<Int32Array>().unwrap();
            for i in 0..b.num_rows() {
                pairs.push((
                    id.value(i),
                    if rid.is_null(i) {
                        None
                    } else {
                        Some(rid.value(i))
                    },
                ));
            }
        }
        pairs.sort_unstable();
        assert_eq!(
            pairs,
            vec![(1, Some(1)), (2, Some(2)), (5, None)],
            "every left row survives; only id=5 (no right match) gets a NULL rid"
        );
    }

    // ── SCALAR SUBQUERY (InitPlan) ──────────────────────────────────────
    //
    // `SELECT id FROM t WHERE id = (SELECT max(id) FROM t)` fell back with
    // `eval.rs`'s `Expr::Subquery { .. } => Err(Internal("subqueries must be
    // decorrelated into a join (or a scalar materialized elsewhere) before
    // scalar eval sees them"))` — the "materialized elsewhere" the comment
    // promised did not exist. `basin_plan::opt::decorrelate` correctly
    // leaves this one alone (uncorrelated: no correlation predicate to join
    // on, better run once than turned into a join), so it reached `build()`
    // still shaped as `Expr::Subquery`. `materialize_scalar_subquery`
    // (called from `bind_outer_rec`, reached from every `bind_outer` call
    // site including `Filter`'s predicate) now builds and runs it exactly
    // once — an InitPlan — and folds the result into a `Literal` before
    // `eval` ever sees it.

    /// `max(id)` — an aggregate with no GROUP BY over the whole table,
    /// exactly the shape `SELECT max(id) FROM t` lowers to.
    fn max_id_subplan() -> LogicalPlan {
        LogicalPlan::Aggregate {
            input: Box::new(scan_plan(vec![ColId(0), ColId(1)], vec![])),
            group: vec![],
            // `max(int4)` is pg_proc oid 2116 — see `agg_func_of`.
            aggs: vec![Expr::Aggregate {
                func: basin_plan::FuncId(basin_pgtype::Oid(2116)),
                args: vec![col(0, "id")],
                distinct: false,
                filter: None,
                order_by: vec![],
            }],
            grouping_sets: None,
        }
    }

    /// The happy path: exactly one row (id=4) equals the table's max id.
    #[test]
    fn a_scalar_subquery_materializes_once_and_filters_correctly() {
        let plan = LogicalPlan::Filter {
            input: Box::new(scan_plan(vec![ColId(0), ColId(1)], vec![])),
            // id = (SELECT max(id) FROM t) — OID 96 is int4 '='.
            predicate: Expr::Binary {
                op: basin_plan::OpId(basin_pgtype::Oid(96)),
                lhs: Box::new(col(0, "id")),
                rhs: Box::new(Expr::Subquery {
                    kind: basin_plan::SubqueryKind::Scalar,
                    subplan: Box::new(max_id_subplan()),
                    operand: None,
                }),
            },
        };
        let batches = drain(build(&plan, &resolver()).unwrap());
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 1, "only id=4 equals max(id)=4");
        let ids: Vec<i32> = batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(ids, vec![4]);
    }

    /// Zero rows from the subquery is Postgres's NULL rule, not an error and
    /// not an empty result for the whole query — `id = NULL` is never true
    /// under three-valued logic, so every row is filtered out, but the
    /// STATEMENT still succeeds.
    #[test]
    fn a_scalar_subquery_with_zero_rows_is_null_not_an_error() {
        let empty_subplan = LogicalPlan::Aggregate {
            // `WHERE false` empties the input before the aggregate ever
            // sees a row — `max` over zero rows is the classic "NULL, not
            // zero" aggregate case.
            input: Box::new(LogicalPlan::Filter {
                input: Box::new(scan_plan(vec![ColId(0), ColId(1)], vec![])),
                predicate: Expr::Literal(Datum::Bool(false), PgType::BOOL),
            }),
            group: vec![],
            aggs: vec![Expr::Aggregate {
                func: basin_plan::FuncId(basin_pgtype::Oid(2116)),
                args: vec![col(0, "id")],
                distinct: false,
                filter: None,
                order_by: vec![],
            }],
            grouping_sets: None,
        };
        let plan = LogicalPlan::Filter {
            input: Box::new(scan_plan(vec![ColId(0), ColId(1)], vec![])),
            predicate: Expr::Binary {
                op: basin_plan::OpId(basin_pgtype::Oid(96)),
                lhs: Box::new(col(0, "id")),
                rhs: Box::new(Expr::Subquery {
                    kind: basin_plan::SubqueryKind::Scalar,
                    subplan: Box::new(empty_subplan),
                    operand: None,
                }),
            },
        };
        let batches = drain(build(&plan, &resolver()).unwrap());
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            rows, 0,
            "id = NULL is never true, so every row is filtered — but this is \
             success (Ok with 0 rows), not an error"
        );
    }

    /// More than one row from a scalar subquery is SQLSTATE 21000
    /// `cardinality_violation` in Postgres — a real runtime error, not a
    /// silently-picked first row.
    #[test]
    fn a_scalar_subquery_with_more_than_one_row_is_a_cardinality_violation() {
        let multi_row_subplan = scan_plan(vec![ColId(0)], vec![]); // 4 rows, not 1
        let plan = LogicalPlan::Filter {
            input: Box::new(scan_plan(vec![ColId(0), ColId(1)], vec![])),
            predicate: Expr::Binary {
                op: basin_plan::OpId(basin_pgtype::Oid(96)),
                lhs: Box::new(col(0, "id")),
                rhs: Box::new(Expr::Subquery {
                    kind: basin_plan::SubqueryKind::Scalar,
                    subplan: Box::new(multi_row_subplan),
                    operand: None,
                }),
            },
        };
        let err = match build(&plan, &resolver()) {
            Err(e) => e,
            Ok(_) => panic!("a multi-row scalar subquery must be refused, not built"),
        };
        assert!(
            matches!(err, BuildError::Exec(ExecError::CardinalityViolation(_))),
            "got {err:?}, expected a CardinalityViolation"
        );
    }

    // ── CTE ──────────────────────────────────────────────────────────────

    /// A `WITH x AS (...) SELECT * FROM x` plan reaches the
    /// `CteBuffer`/`CteReader` operators through the builder — the whole
    /// point of wiring `LogicalPlan::Cte`/`CteRef` at all.
    #[test]
    fn a_non_recursive_cte_executes_end_to_end() {
        let plan = LogicalPlan::Cte {
            name: basin_plan::CteId(0),
            recursive: false,
            body: Box::new(scan_plan(vec![ColId(0), ColId(1)], vec![])),
            input: Box::new(LogicalPlan::CteRef {
                name: basin_plan::CteId(0),
                schema: vec![
                    ("id".into(), basin_pgtype::PgType::INT4),
                    ("v".into(), basin_pgtype::PgType::INT4),
                ],
            }),
        };
        let rows: usize = drain(build(&plan, &resolver()).unwrap())
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(rows, 4);
    }

    /// A CTE referenced TWICE must return the FULL result both times — a
    /// replay that drained the body on the first reference would silently
    /// leave the second with zero rows. `cte.rs`'s own
    /// `cte_referenced_twice_returns_full_results_both_times` proves this
    /// one layer down (`CteBuffer`/`CteReader` directly); this proves the
    /// BUILDER actually wires two `CteRef`s to two independent
    /// `CteReader`s off one `CteBuffer`, rather than, say, re-running the
    /// body twice (which would also pass a row-count check but violate the
    /// "materialize once" contract `cte.rs`'s module docs describe).
    #[test]
    fn a_cte_referenced_twice_via_the_builder_returns_full_results_both_times() {
        let cte_ref = || LogicalPlan::CteRef {
            name: basin_plan::CteId(0),
            schema: vec![
                ("id".into(), basin_pgtype::PgType::INT4),
                ("v".into(), basin_pgtype::PgType::INT4),
            ],
        };
        let plan = LogicalPlan::Cte {
            name: basin_plan::CteId(0),
            recursive: false,
            body: Box::new(scan_plan(vec![ColId(0), ColId(1)], vec![])),
            input: Box::new(LogicalPlan::SetOp {
                left: Box::new(cte_ref()),
                right: Box::new(cte_ref()),
                op: basin_plan::SetOpKind::Union,
                all: true,
            }),
        };
        let rows: usize = drain(build(&plan, &resolver()).unwrap())
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(
            rows, 8,
            "4 rows from each of the two references to the same CTE, not 4+0"
        );
    }

    /// A `CteRef` to a `CteId` nothing registered is a planner bug, not a
    /// user error, and must be REPORTED — not silently built as an empty
    /// relation, which would look like a valid (if surprising) answer
    /// instead of the broken plan it actually is.
    #[test]
    fn an_unregistered_cte_ref_is_reported_not_emptied() {
        let plan = LogicalPlan::CteRef {
            name: basin_plan::CteId(7),
            schema: vec![("x".into(), basin_pgtype::PgType::INT8)],
        };
        let err = match build(&plan, &resolver()) {
            Err(e) => e,
            Ok(_) => panic!("an unregistered CteRef must not silently build as empty"),
        };
        assert_eq!(err, BuildError::UnknownCte(basin_plan::CteId(7)));
    }

    // ── WITH RECURSIVE ──────────────────────────────────────────────────

    /// `WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM t WHERE
    /// n < 5) SELECT n FROM t` — the classic bounded counter (verified live
    /// against Postgres in `recursive.rs`'s module docs), reaching
    /// `RecursiveCte` through the builder for the first time. Previously
    /// this operator was only exercised directly, with no planner path to
    /// it at all.
    #[test]
    fn with_recursive_bounded_counter_executes_end_to_end() {
        let cte_ref = LogicalPlan::CteRef {
            name: basin_plan::CteId(0),
            schema: vec![("n".into(), PgType::INT4)],
        };
        let anchor = LogicalPlan::Values {
            rows: vec![vec![Expr::Literal(Datum::Int32(1), PgType::INT4)]],
            schema: vec![("n".into(), PgType::INT4)],
        };
        let recursive_term = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Filter {
                input: Box::new(cte_ref.clone()),
                // n < 5 — OID 97 is int4 '<', verified against pg_operator.
                predicate: Expr::Binary {
                    op: basin_plan::OpId(basin_pgtype::Oid(97)),
                    lhs: Box::new(col(0, "n")),
                    rhs: Box::new(Expr::Literal(Datum::Int32(5), PgType::INT4)),
                },
            }),
            exprs: vec![(
                // n + 1 — OID 551 is int4 '+'.
                Expr::Binary {
                    op: basin_plan::OpId(basin_pgtype::Oid(551)),
                    lhs: Box::new(col(0, "n")),
                    rhs: Box::new(Expr::Literal(Datum::Int32(1), PgType::INT4)),
                },
                "n".into(),
            )],
        };
        let plan = LogicalPlan::Cte {
            name: basin_plan::CteId(0),
            recursive: true,
            body: Box::new(LogicalPlan::SetOp {
                left: Box::new(anchor),
                right: Box::new(recursive_term),
                op: basin_plan::SetOpKind::Union,
                all: true,
            }),
            input: Box::new(cte_ref),
        };
        let batches = drain(build(&plan, &resolver()).unwrap());
        let mut values: Vec<i32> = batches.iter().flat_map(|b| col_i32(b, 0)).collect();
        values.sort();
        assert_eq!(values, vec![1, 2, 3, 4, 5]);
    }

    fn col_i32(batch: &RecordBatch, i: usize) -> Vec<i32> {
        batch
            .column(i)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .flatten()
            .collect()
    }

    // ── LATERAL ──────────────────────────────────────────────────────────

    fn two_table_resolver() -> MemTableResolver {
        let mut r = MemTableResolver::new();
        let outer_schema = Arc::new(Schema::new(vec![Field::new("o", DataType::Int32, true)]));
        let outer_batch = RecordBatch::try_new(
            outer_schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        r.insert(TableId(1), outer_schema, vec![outer_batch]);

        let inner_schema = Arc::new(Schema::new(vec![
            Field::new("fk", DataType::Int32, true),
            Field::new("v", DataType::Int32, true),
        ]));
        let inner_batch = RecordBatch::try_new(
            inner_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 1, 3])),
                Arc::new(Int32Array::from(vec![100, 101, 300])),
            ],
        )
        .unwrap();
        r.insert(TableId(2), inner_schema, vec![inner_batch]);
        r
    }

    /// `SELECT * FROM o CROSS JOIN LATERAL (SELECT * FROM t2 WHERE t2.fk =
    /// o.o)` — the correlated fallback path `LateralJoin` exists for,
    /// reached through the builder for the first time. `o`=2 has no
    /// matching `t2` rows and must be dropped entirely (Inner LATERAL
    /// semantics — see `lateral.rs`'s own
    /// `inner_lateral_drops_outer_row_with_zero_inner_rows`); `o`=1
    /// multiplies into two rows, `o`=3 into one.
    #[test]
    fn a_lateral_join_executes_end_to_end_with_correlation() {
        let outer_plan = LogicalPlan::Scan {
            table: TableId(1),
            projection: vec![ColId(0)],
            filters: vec![],
            snapshot: SnapshotId(0),
        };
        let inner_plan = LogicalPlan::Filter {
            input: Box::new(LogicalPlan::Scan {
                table: TableId(2),
                projection: vec![ColId(0), ColId(1)],
                filters: vec![],
                snapshot: SnapshotId(0),
            }),
            // t2.fk = o.o — OID 96 is int4 '='. `relation: 1` marks the
            // outer reference — see `OUTER_REF`.
            predicate: Expr::Binary {
                op: basin_plan::OpId(basin_pgtype::Oid(96)),
                lhs: Box::new(col(0, "fk")),
                rhs: Box::new(Expr::Column(ColumnRef {
                    relation: 1,
                    index: 0,
                    name: "o".into(),
                })),
            },
        };
        let plan = LogicalPlan::LateralJoin {
            outer: Box::new(outer_plan),
            inner: Box::new(inner_plan),
            kind: basin_plan::JoinKind::Inner,
        };
        let batches = drain(build(&plan, &two_table_resolver()).unwrap());
        let mut pairs: Vec<(i32, i32)> = batches
            .iter()
            .flat_map(|b| {
                let o = b
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .clone();
                let fk = b
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .clone();
                (0..b.num_rows()).map(move |i| (o.value(i), fk.value(i)))
            })
            .collect();
        pairs.sort();
        assert_eq!(
            pairs,
            vec![(1, 1), (1, 1), (3, 3)],
            "o=2 has no matching t2 rows and is dropped under Inner LATERAL"
        );
    }

    // ── DML ──────────────────────────────────────────────────────────────

    fn dml_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("val", DataType::Utf8, true),
        ]))
    }

    /// `INSERT INTO t (id, val) VALUES (...) RETURNING id, val` reaches the
    /// `Insert` operator through the builder for the first time —
    /// previously it was only exercised directly in `dml.rs`, with no
    /// planner path to it.
    #[test]
    fn an_insert_executes_end_to_end_with_returning() {
        let schema = dml_schema();
        let mut dml = MemDmlResolver::new();
        let sink = dml.insert_table(TableId(1), schema.clone(), vec![0]);

        let plan = LogicalPlan::Insert {
            table: TableId(1),
            input: Box::new(LogicalPlan::Values {
                rows: vec![
                    vec![
                        Expr::Literal(Datum::Int64(1), PgType::INT8),
                        Expr::Literal(Datum::Utf8("a".into()), PgType::TEXT),
                    ],
                    vec![
                        Expr::Literal(Datum::Int64(2), PgType::INT8),
                        Expr::Literal(Datum::Utf8("b".into()), PgType::TEXT),
                    ],
                ],
                schema: vec![("id".into(), PgType::INT8), ("val".into(), PgType::TEXT)],
            }),
            columns: vec![ColId(0), ColId(1)],
            on_conflict: None,
            returning: Some(vec![
                (col(0, "id"), "id".into()),
                (col(1, "val"), "val".into()),
            ]),
        };
        let batches =
            drain(build_with_dml(&plan, &resolver(), &dml, DEFAULT_OPERATOR_BUDGET).unwrap());
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 2, "RETURNING yields both inserted rows");
        assert_eq!(sink.borrow().len(), 2);
    }

    /// `INSERT` reached through the STABLE, 2-argument `build()` (no write
    /// resolver available) must be refused cleanly, not panic or silently
    /// drop the write — `build()`'s signature never changed to require a
    /// `DmlResolver` every caller would otherwise have to acquire.
    #[test]
    fn insert_via_build_without_a_write_resolver_is_refused() {
        let plan = LogicalPlan::Insert {
            table: TableId(1),
            input: Box::new(LogicalPlan::Values {
                rows: vec![],
                schema: vec![("id".into(), PgType::INT8)],
            }),
            columns: vec![ColId(0)],
            on_conflict: None,
            returning: None,
        };
        let err = match build(&plan, &resolver()) {
            Err(e) => e,
            Ok(_) => panic!("INSERT must not build without a write resolver"),
        };
        assert!(matches!(err, BuildError::Unsupported(ref s) if s.contains("INSERT")));
    }

    /// `UPDATE t SET val = 'new' WHERE id = 1 RETURNING id, val` — `Update`
    /// carries no explicit input plan (unlike `Insert`), so this proves the
    /// builder's own `Scan(table) + Filter(predicate) + Project(new
    /// values)` synthesis, not just the `Update` operator underneath it.
    #[test]
    fn an_update_executes_end_to_end_with_returning() {
        let schema = dml_schema();
        let existing = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["old1", "old2"])),
            ],
        )
        .unwrap();

        // The scan side (what UPDATE reads to find rows to change) and the
        // write side (what it writes through) are two independent mocks
        // here, the same way `TableResolver`/`DmlResolver` are two
        // independent traits — pre-populate both consistently, as a real
        // Storage-backed pair already would agree by construction.
        let mut tables = MemTableResolver::new();
        tables.insert(TableId(1), schema.clone(), vec![existing.clone()]);
        let mut dml = MemDmlResolver::new();
        let sink = dml.insert_table(TableId(1), schema.clone(), vec![0]);
        sink.borrow_mut().insert(&existing).unwrap();

        let plan = LogicalPlan::Update {
            table: TableId(1),
            set: vec![(
                ColId(1),
                Expr::Literal(Datum::Utf8("new".into()), PgType::TEXT),
            )],
            from: None,
            // id = 1 — OID 410 is int8 '='.
            predicate: Some(Expr::Binary {
                op: basin_plan::OpId(basin_pgtype::Oid(410)),
                lhs: Box::new(col(0, "id")),
                rhs: Box::new(Expr::Literal(Datum::Int64(1), PgType::INT8)),
            }),
            returning: Some(vec![
                (col(0, "id"), "id".into()),
                (col(1, "val"), "val".into()),
            ]),
            snapshot: SnapshotId(0),
        };
        let batches = drain(build_with_dml(&plan, &tables, &dml, DEFAULT_OPERATOR_BUDGET).unwrap());
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 1, "only id=1 matched the predicate");
        let vals: Vec<String> = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .map(|v| v.unwrap().to_string())
            .collect();
        assert_eq!(vals, vec!["new"]);
        assert_eq!(
            sink.borrow().len(),
            2,
            "id=2 untouched, id=1 rewritten in place"
        );
    }

    /// `DELETE FROM t WHERE id = 2 RETURNING id` — same "no explicit
    /// input" synthesis as `UPDATE`, minus the new-value `Project`.
    #[test]
    fn a_delete_executes_end_to_end_with_returning() {
        let schema = dml_schema();
        let existing = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap();
        let mut tables = MemTableResolver::new();
        tables.insert(TableId(1), schema.clone(), vec![existing.clone()]);
        let mut dml = MemDmlResolver::new();
        let sink = dml.insert_table(TableId(1), schema.clone(), vec![0]);
        sink.borrow_mut().insert(&existing).unwrap();

        let plan = LogicalPlan::Delete {
            table: TableId(1),
            using: None,
            // id = 2 — OID 410 is int8 '='.
            predicate: Some(Expr::Binary {
                op: basin_plan::OpId(basin_pgtype::Oid(410)),
                lhs: Box::new(col(0, "id")),
                rhs: Box::new(Expr::Literal(Datum::Int64(2), PgType::INT8)),
            }),
            returning: Some(vec![(col(0, "id"), "id".into())]),
            snapshot: SnapshotId(0),
        };
        let batches = drain(build_with_dml(&plan, &tables, &dml, DEFAULT_OPERATOR_BUDGET).unwrap());
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 1);
        assert_eq!(sink.borrow().len(), 1, "id=2 removed, id=1 remains");
    }
}
