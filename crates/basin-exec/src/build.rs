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

use std::collections::HashMap;

use basin_plan::{Expr, LogicalPlan, SortKey as PlanSortKey, TableId};

use crate::aggregate::{AggFunc, AggregateSpec, HashAggregate};
use crate::join::HashJoin;
use crate::operator::{ExecError, Operator};
use crate::project::{Filter, Project};
use crate::scan::{BatchSource, Scan};
use crate::setop::{Distinct, Empty, SetOp, Values};
use crate::sort::{Sort, SortKey, TopK};
use crate::window::{OrderKey, WindowAgg, WindowFunc, WindowSpec};

/// Why a plan could not be turned into operators.
#[derive(Debug, Clone, PartialEq)]
pub enum BuildError {
    /// A plan shape with no operator behind it yet.
    Unsupported(String),
    /// A table the resolver does not know.
    UnknownTable(TableId),
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

/// Default memory budget for a buffering operator, in bytes.
///
/// Deliberately a constant here rather than a policy: the real budget is a
/// per-query slice of the bounded pool that already exists in the engine, and
/// wiring that through is a separate increment. Picking a number and hiding it
/// would be worse than picking one and saying so.
pub const DEFAULT_OPERATOR_BUDGET: usize = 256 * 1024 * 1024;

/// Build an operator tree for `plan`.
pub fn build(
    plan: &LogicalPlan,
    tables: &dyn TableResolver,
) -> Result<Box<dyn Operator>, BuildError> {
    build_with_budget(plan, tables, DEFAULT_OPERATOR_BUDGET)
}

/// Build an operator tree with an explicit memory budget for buffering
/// operators.
pub fn build_with_budget(
    plan: &LogicalPlan,
    tables: &dyn TableResolver,
    budget: usize,
) -> Result<Box<dyn Operator>, BuildError> {
    match plan {
        LogicalPlan::Scan {
            table,
            projection,
            filters,
            ..
        } => {
            let cols: Vec<usize> = projection.iter().map(|c| c.0 as usize).collect();
            let (source, pushed) = tables
                .open(*table, &cols, filters)
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
                filters.clone()
            };
            Ok(Box::new(Scan::new(source, scan_cols, scan_filters)?))
        }

        LogicalPlan::Filter { input, predicate } => {
            let child = build_with_budget(input, tables, budget)?;
            Ok(Box::new(Filter::new(child, predicate.clone())))
        }

        LogicalPlan::Project { input, exprs } => {
            let child = build_with_budget(input, tables, budget)?;
            Ok(Box::new(Project::new(child, exprs.clone())?))
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
            if skip.is_some() {
                return Err(BuildError::Unsupported("OFFSET".into()));
            }
            let k = match fetch {
                Some(e) => const_usize(e)
                    .ok_or_else(|| BuildError::Unsupported("non-constant LIMIT".into()))?,
                None => return build_with_budget(input, tables, budget),
            };
            if let LogicalPlan::Sort { input: si, keys } = input.as_ref() {
                let child = build_with_budget(si, tables, budget)?;
                Ok(Box::new(TopK::new(child, sort_keys(keys)?, k)))
            } else {
                Err(BuildError::Unsupported("LIMIT without ORDER BY".into()))
            }
        }

        LogicalPlan::Sort { input, keys } => {
            let child = build_with_budget(input, tables, budget)?;
            Ok(Box::new(Sort::new(child, sort_keys(keys)?, budget)))
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
            let child = build_with_budget(input, tables, budget)?;
            let group_cols = group
                .iter()
                .map(|e| column_index(e).ok_or(BuildError::NonColumnKey("GROUP BY")))
                .collect::<Result<Vec<_>, _>>()?;
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
            if filter.is_some() {
                return Err(BuildError::Unsupported("non-equi join condition".into()));
            }
            let l = build_with_budget(left, tables, budget)?;
            let r = build_with_budget(right, tables, budget)?;
            let mut lk = Vec::with_capacity(on.len());
            let mut rk = Vec::with_capacity(on.len());
            for (a, b) in on {
                lk.push(column_index(a).ok_or(BuildError::NonColumnKey("join"))?);
                rk.push(column_index(b).ok_or(BuildError::NonColumnKey("join"))?);
            }
            Ok(Box::new(HashJoin::new(l, r, *kind, lk, rk, budget)))
        }

        LogicalPlan::Values { rows, schema } => {
            let names: Vec<String> = schema.iter().map(|(n, _)| n.clone()).collect();
            Ok(Box::new(Values::new(rows.clone(), names)?))
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
            let child = build_with_budget(input, tables, budget)?;
            match on {
                None => Ok(Box::new(Distinct::new(child, budget))),
                Some(exprs) => {
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
            let l = build_with_budget(left, tables, budget)?;
            let r = build_with_budget(right, tables, budget)?;
            Ok(Box::new(SetOp::new(l, r, *op, *all, budget)?))
        }

        // Every window expression in one node shares a PARTITION BY / ORDER BY,
        // because the planner groups them that way — one operator per distinct
        // window, not per expression. The operator requires its input already
        // sorted by those keys and never re-sorts, so an unsorted input is a
        // planner bug it will not paper over.
        LogicalPlan::Window { input, windows } => {
            let child = build_with_budget(input, tables, budget)?;
            let (partition_by, order_by) = window_keys(windows)?;
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

        other => Err(BuildError::Unsupported(plan_kind(other).to_string())),
    }
}

/// A short, stable name for a plan variant, for error messages.
fn plan_kind(plan: &LogicalPlan) -> &'static str {
    match plan {
        LogicalPlan::Scan { .. } => "Scan",
        LogicalPlan::Values { .. } => "VALUES",
        LogicalPlan::Empty { .. } => "empty relation",
        LogicalPlan::Project { .. } => "Project",
        LogicalPlan::Filter { .. } => "Filter",
        LogicalPlan::Aggregate { .. } => "Aggregate",
        LogicalPlan::Sort { .. } => "Sort",
        LogicalPlan::Limit { .. } => "Limit",
        LogicalPlan::Join { .. } => "Join",
        LogicalPlan::LateralJoin { .. } => "LATERAL join",
        LogicalPlan::SetOp { .. } => "UNION / INTERSECT / EXCEPT",
        LogicalPlan::Distinct { .. } => "DISTINCT",
        LogicalPlan::Window { .. } => "window function",
        LogicalPlan::ProjectSet { .. } => "set-returning function",
        LogicalPlan::Cte { .. } => "CTE",
        LogicalPlan::CteRef { .. } => "CTE reference",
        LogicalPlan::Insert { .. } => "INSERT",
        LogicalPlan::Update { .. } => "UPDATE",
        LogicalPlan::Delete { .. } => "DELETE",
    }
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
        3106 => WindowFunc::Lag,
        3108 => WindowFunc::Lead,
        3110 => WindowFunc::FirstValue,
        3111 => WindowFunc::LastValue,
        3112 => WindowFunc::NthValue,
        2803 => WindowFunc::CountStar,
        2107 | 2108 | 2111 | 2114 => WindowFunc::Sum,
        2115 | 2116 | 2120 | 2130 => WindowFunc::Max,
        2131 | 2132 | 2136 | 2146 => WindowFunc::Min,
        2100 | 2101 | 2103 | 2105 => WindowFunc::Avg,
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
    use arrow_array::{Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use basin_pgtype::PgType;
    use basin_plan::{ColId, ColumnRef, Datum, SnapshotId};
    use std::sync::Arc;

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

    /// An unimplemented plan shape must say so by name rather than silently
    /// doing something else. A builder that guesses produces wrong answers
    /// instead of errors.
    #[test]
    fn an_unbuildable_plan_names_the_construct() {
        // This test exists to go stale. DISTINCT was the example, then window
        // functions; both build now. CTEs are the current frontier — when this
        // fails again, that is the good outcome and the next unimplemented
        // shape takes its place.
        let plan = LogicalPlan::CteRef {
            name: basin_plan::CteId(0),
            schema: vec![("x".into(), basin_pgtype::PgType::INT8)],
        };
        let err = match build(&plan, &resolver()) {
            Err(e) => e,
            Ok(_) => panic!("CTEs have no operator yet and must not build"),
        };
        assert_eq!(err, BuildError::Unsupported("CTE reference".into()));
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
}
