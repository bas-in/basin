//! `HashJoin` — the equijoin (and `Cross`) join family.
//!
//! # Shape
//!
//! Build side is always RIGHT, fully materialized into one hash table before
//! any output row is produced. Probe side is always LEFT, pulled from the
//! child operator one batch at a time — so a join over a huge left input and
//! a small right input streams instead of buffering everything, and
//! cancellation is still checked between probe batches (see
//! [`crate::operator::Operator`]'s docs on why that matters).
//!
//! This is the opposite of the intuitive "build the smaller side" heuristic a
//! cost-based optimizer would pick, but nothing here does cost-based side
//! selection — the physical planner decides which input is `left` and which
//! is `right` before this operator exists, and `HashJoin` always builds on
//! `right`. That is a deliberate simplification, not an accident: see
//! `docs/migration/df-removal/03-physical-operators.md`.
//!
//! # Single partition only
//!
//! Basin pins `target_partitions = 1`, and raising it for a bulk scan
//! simultaneously disables repartitioning of joins (`executor.rs:10984`), so
//! there is never a partitioned or symmetric hash join to build here — one
//! hash table, one probe stream, full stop.
//!
//! # NULL never equals NULL
//!
//! A join key with a NULL in *any* of its columns cannot match anything,
//! including another row whose key is also all-NULL in the same columns.
//! [`arrow::row::RowConverter`]'s byte encoding does not help here: two NULL
//! keys encode to the *same* sentinel bytes regardless of the non-null value
//! they'd otherwise carry, so inserting every row's encoded key into the
//! hash table unconditionally would make every NULL-keyed row match every
//! other NULL-keyed row — `null_keys_never_match_each_other` below is the
//! test for exactly that. The fix applied throughout this module: a row
//! whose key has a NULL component is never inserted into the build-side hash
//! table, and never looked up on the probe side. Such a row can still be
//! *itself* an "unmatched" row for `Left`/`Right`/`Full`/`LeftAnti` — it
//! simply always takes that branch.
//!
//! # Memory
//!
//! [`HashJoin::memory_used`] reports the concatenated build-side (right)
//! table's array memory honestly, and `next_batch` refuses to buffer past
//! `memory_budget` while materializing it — see
//! `sort_reports_out_of_memory_past_its_budget` in `sort.rs` for the sibling
//! operator this mirrors. As with [`crate::sort::Sort`], there is no disk
//! spill; exceeding budget fails clean with [`ExecError::OutOfMemory`].
//!
//! # No equijoin key at all
//!
//! `on` can be *empty* while `filter` is not: `t JOIN u ON t.id < u.n`,
//! `ON a.id <> b.id`, `ON t.id IS NOT DISTINCT FROM u.tid`, and a
//! decorrelated `EXISTS` whose only correlation is an inequality all reach
//! this operator with zero key pairs. There is nothing to hash, but the join
//! is still ordinary SQL and Postgres executes all of them, so the equality
//! test is treated as vacuously true: every build-side row is a candidate
//! for every probe row (`all_right_rows`), and the residual alone decides.
//! That makes the keyless case a nested-loop join — O(left x right)
//! candidate pairs — sharing this operator's build/probe skeleton rather
//! than a separate one. `Cross` keeps its own dedicated path because it has
//! no residual to evaluate per pair.
//!
//! The unified `filter` handling below still applies unchanged: `Inner`
//! post-filters the cartesian batch, `LeftSemi`/`LeftAnti` run the residual
//! as the match test per candidate. `Left`/`Right`/`Full` with a residual
//! are refused by the constructor either way, so a keyless outer join is
//! only ever reached without one (where it is a plain cross product with the
//! preserved side's empty-build-table case still NULL-extending correctly).
//!
//! # Residual filter
//!
//! `on` is only ever equality — a hash table cannot test anything else — so
//! any other conjunct in the original condition (`a.x = b.y AND b.z > 5`)
//! rides along separately as `filter`, evaluated over the *concatenated*
//! left++right row (`eval::eval` addresses it as one flat batch: the
//! constructor rewrites any `relation == 1` column in `filter` to a flat
//! index — `left_schema.len() + index` — before storing it, so this module
//! never has to special-case a per-side tag at evaluation time; see
//! `flatten_filter`'s doc comment for why `relation == 1` shows up in
//! `filter` at all).
//!
//! Three-valued like every other SQL predicate — a NULL residual excludes
//! the pair, exactly like `WHERE` (`project.rs`'s `Filter` module docs cover
//! the same rule; this does not reinvent it, `eval_residual_mask` below just
//! applies it by hand instead of going through `filter_record_batch`, since
//! semi/anti needs to inspect the mask rather than use it to filter rows).
//!
//! **Getting the kind wrong here does not error — it returns the wrong
//! rows**, silently:
//!
//! - `Inner`/`Cross`: the residual is a plain post-join predicate — evaluate
//!   it over the already-built output batch and keep only the TRUE rows,
//!   same as [`crate::project::Filter`] would if it sat above the join.
//! - `LeftSemi`/`LeftAnti`: the residual is part of the *match test*, not an
//!   output filter. A left row is semi-matched only if some right row both
//!   shares its key AND passes the residual; anti is the negation of that
//!   combined test, not "no key match" alone. Filtering semi/anti's output
//!   *after* the fact cannot express this at all — semi/anti's output never
//!   carries the right-side columns the residual needs, and anti's rows are
//!   exactly the ones that would fail such a filter, which would delete the
//!   rows anti exists to keep.
//! - `Left`/`Right`/`Full`: the residual is part of the join condition
//!   itself. A pair that fails it does not vanish — the row survives as
//!   unmatched, NULL-extended on the other side, same as a pair that never
//!   shared a key at all. Filtering the output afterwards would instead
//!   *delete* that row, which is not what an outer join with an `ON`-clause
//!   residual means. **Not implemented** — see `build.rs`'s join arm, which
//!   refuses these by name rather than let this module guess.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::compute::{concat_batches, filter_record_batch};
use arrow::row::{OwnedRow, RowConverter, SortField};
use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch, UInt32Array};
use arrow_schema::{ArrowError, Field, Schema, SchemaRef};
use arrow_select::take::take;
use basin_plan::{ColumnRef, Expr, JoinKind};

use crate::eval;
use crate::operator::{default_session, ExecError, Operator, SessionRef};

/// Rewrite `filter` so every column it reads is a flat index into the
/// concatenated left++right row, ready for direct use with `eval::eval`.
///
/// `Join::filter` reaches this module in one of two shapes depending on
/// which pass produced it (see `basin-plan`'s `opt/decorrelate.rs` and
/// `opt/projection.rs`'s "BOTH conventions are live in this codebase and
/// both must work" comment on its own `Join` handling — this function is
/// `basin-exec`'s side of the same fact):
///
/// - An ordinary multi-table `WHERE` clause (`lower/select.rs`) lowers the
///   whole `FROM` scope as one flat relation before a `Join` node ever
///   splits it, and the leftover (non-equijoin) conjuncts are never
///   rebased — so a right-side column already carries `relation: 0` with
///   `index >= left_len`, i.e. already the flat index this module needs.
/// - A decorrelated `EXISTS`/`IN` (`opt/decorrelate.rs`'s
///   `split_join_conjuncts`) tags each side the way `Join::on` always has —
///   `relation: 0` for the join's own left, 0-based; `relation: 1` for its
///   right, *also* 0-based, per `ColumnRef::relation`'s doc comment ("0/1
///   for the sides of a join"). That right-side index needs `left_len`
///   added before it means anything against a concatenated batch.
///
/// One rewrite handles both without telling them apart: a `relation == 0`
/// column's index is left untouched (correct for a shape-A left OR
/// right-side reference, and for a shape-B left reference — all three are
/// already the correct flat/left-relative index), and a `relation == 1`
/// column gets `left_len` added (shape B's right-side reference; shape A
/// never produces `relation == 1` in `filter` at all, so this arm is
/// unreachable for it). See this module's own tests
/// (`flatten_filter_leaves_a_flat_relation_zero_index_alone` and
/// `flatten_filter_offsets_a_relation_one_index_by_left_len`) for the two
/// cases nailed down independently of the rest of this file.
fn flatten_filter(filter: &Expr, left_len: usize) -> Expr {
    walk(filter, left_len)
}

/// The actual rewrite. `Expr` has no generic rewrite helper — only the
/// read-only, non-rebuilding `for_each_child` — so this mirrors the
/// exhaustive-match shape `build.rs`'s `bind_outer_rec` already uses for the
/// same reason (a `match` that must name every variant to compile is the
/// safety net here: an `Expr` shape added later and left off this list is a
/// compile error, not a column that silently stops getting flattened).
/// Unlike `bind_outer_rec`, this never touches a nested `Subquery`'s own
/// `subplan` — same rule for the same reason: a subquery's body is a
/// separate scope with its own relation-0/1 meaning, not part of this
/// join's concatenated row.
fn walk(e: &Expr, left_len: usize) -> Expr {
    let b = |e: &Expr| Box::new(walk(e, left_len));
    let v = |es: &[Expr]| es.iter().map(|e| walk(e, left_len)).collect::<Vec<_>>();
    let ob = |o: &Option<Box<Expr>>| o.as_deref().map(b);
    match e {
        Expr::Column(c) if c.relation == 1 => Expr::Column(ColumnRef {
            relation: 0,
            index: c.index + left_len as u16,
            name: c.name.clone(),
        }),
        Expr::Column(_) | Expr::Literal(..) | Expr::Parameter { .. } => e.clone(),
        Expr::Unary { op, arg } => Expr::Unary {
            op: *op,
            arg: b(arg),
        },
        Expr::Binary { op, lhs, rhs } => Expr::Binary {
            op: *op,
            lhs: b(lhs),
            rhs: b(rhs),
        },
        Expr::Cast { arg, to, kind } => Expr::Cast {
            arg: b(arg),
            to: *to,
            kind: *kind,
        },
        Expr::Case {
            operand,
            whens,
            else_,
        } => Expr::Case {
            operand: ob(operand),
            whens: whens
                .iter()
                .map(|(w, t)| (walk(w, left_len), walk(t, left_len)))
                .collect(),
            else_: ob(else_),
        },
        Expr::Coalesce(xs) => Expr::Coalesce(v(xs)),
        Expr::IsNull { arg, negated } => Expr::IsNull {
            arg: b(arg),
            negated: *negated,
        },
        Expr::BoolTest { arg, test } => Expr::BoolTest {
            arg: b(arg),
            test: *test,
        },
        Expr::DistinctFrom { lhs, rhs, negated } => Expr::DistinctFrom {
            lhs: b(lhs),
            rhs: b(rhs),
            negated: *negated,
        },
        Expr::InList { arg, list, negated } => Expr::InList {
            arg: b(arg),
            list: v(list),
            negated: *negated,
        },
        Expr::Between {
            arg,
            low,
            high,
            symmetric,
            negated,
        } => Expr::Between {
            arg: b(arg),
            low: b(low),
            high: b(high),
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
            arg: b(arg),
            pattern: b(pattern),
            escape: ob(escape),
            case_insensitive: *case_insensitive,
            negated: *negated,
        },
        Expr::ScalarFn { func, args } => Expr::ScalarFn {
            func: *func,
            args: v(args),
        },
        Expr::Aggregate {
            func,
            args,
            distinct,
            filter,
            order_by,
        } => Expr::Aggregate {
            func: *func,
            args: v(args),
            distinct: *distinct,
            filter: ob(filter),
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
            args: v(args),
            partition_by: v(partition_by),
            order_by: order_by.clone(),
            frame: frame.clone(),
        },
        Expr::SetReturning { func, args } => Expr::SetReturning {
            func: *func,
            args: v(args),
        },
        // Left entirely untouched: `subplan` is a separate query level with
        // its own relation-0/1 meaning (same rule `bind_outer_rec` follows),
        // and `operand` — the one part that does belong to this level — is
        // recursed into like everything else.
        Expr::Subquery {
            kind,
            subplan,
            operand,
        } => Expr::Subquery {
            kind: *kind,
            subplan: subplan.clone(),
            operand: ob(operand),
        },
        Expr::ArrayLit(xs) => Expr::ArrayLit(v(xs)),
        Expr::RowLit(xs) => Expr::RowLit(v(xs)),
        Expr::Subscript { arg, indices } => Expr::Subscript {
            arg: b(arg),
            indices: indices
                .iter()
                .map(|s| match s {
                    basin_plan::Subscript::Index(e) => {
                        basin_plan::Subscript::Index(walk(e, left_len))
                    }
                    basin_plan::Subscript::Slice { lower, upper } => basin_plan::Subscript::Slice {
                        lower: lower.as_ref().map(|e| walk(e, left_len)),
                        upper: upper.as_ref().map(|e| walk(e, left_len)),
                    },
                })
                .collect(),
        },
        Expr::FieldSelect { arg, field } => Expr::FieldSelect {
            arg: b(arg),
            field: *field,
        },
    }
}

fn arrow_err(e: ArrowError) -> ExecError {
    ExecError::Internal(e.to_string())
}

/// The output schema for a join of `left` and `right` under `kind`.
///
/// Mirrors `basin_plan::schema::join_schema`: `LeftSemi`/`LeftAnti` report
/// the left schema alone (a semi/anti join filters rows, it never widens
/// them), everything else concatenates left ++ right. Columns from a side
/// that can appear as all-NULL in the output (the non-preserved side of
/// `Left`/`Right`, both sides of `Full`) are forced nullable here, since this
/// is the physical Arrow schema batches actually get built against — unlike
/// `basin_plan::Schema`, which carries no nullability at all (see that
/// module's `KNOWN LIMITATION` note).
fn join_output_schema(left: &SchemaRef, right: &SchemaRef, kind: JoinKind) -> SchemaRef {
    fn nullable_fields(schema: &SchemaRef) -> Vec<Field> {
        schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone().with_nullable(true))
            .collect()
    }

    match kind {
        JoinKind::LeftSemi | JoinKind::LeftAnti => Arc::clone(left),
        JoinKind::Inner | JoinKind::Cross => {
            let mut fields: Vec<Field> = left.fields().iter().map(|f| f.as_ref().clone()).collect();
            fields.extend(right.fields().iter().map(|f| f.as_ref().clone()));
            Arc::new(Schema::new(fields))
        }
        JoinKind::Left => {
            let mut fields: Vec<Field> = left.fields().iter().map(|f| f.as_ref().clone()).collect();
            fields.extend(nullable_fields(right));
            Arc::new(Schema::new(fields))
        }
        JoinKind::Right => {
            let mut fields = nullable_fields(left);
            fields.extend(right.fields().iter().map(|f| f.as_ref().clone()));
            Arc::new(Schema::new(fields))
        }
        JoinKind::Full => {
            let mut fields = nullable_fields(left);
            fields.extend(nullable_fields(right));
            Arc::new(Schema::new(fields))
        }
    }
}

/// Per-row: does any key column hold a NULL at this row? Computed once per
/// batch rather than re-checked per lookup.
fn null_key_mask(key_cols: &[ArrayRef], num_rows: usize) -> Vec<bool> {
    let mut mask = vec![false; num_rows];
    for col in key_cols {
        for (i, m) in mask.iter_mut().enumerate() {
            if col.is_null(i) {
                *m = true;
            }
        }
    }
    mask
}

fn project_columns(batch: &RecordBatch, indices: &[usize]) -> Vec<ArrayRef> {
    indices
        .iter()
        .map(|&i| Arc::clone(batch.column(i)))
        .collect()
}

fn take_by_indices(
    source: &RecordBatch,
    indices: &UInt32Array,
) -> Result<Vec<ArrayRef>, ExecError> {
    source
        .columns()
        .iter()
        .map(|c| take(c.as_ref(), indices, None).map_err(arrow_err))
        .collect()
}

/// A fully-NULL run of `len` rows, shaped like `schema`. Used for the
/// unmatched-right tail of a `Right`/`Full` join, where there is no live
/// left batch left to `take` NULLs out of — the left child is already
/// exhausted by the time this runs.
fn null_columns(schema: &SchemaRef, len: usize) -> Vec<ArrayRef> {
    schema
        .fields()
        .iter()
        .map(|f| arrow_array::new_null_array(f.data_type(), len))
        .collect()
}

enum Phase {
    /// Materializing the right (build) side. Loops internally over the
    /// child, same documented gap as `Sort`/`TopK`: cannot honour
    /// `statement_timeout` mid-build.
    Building,
    /// Pulling one left (probe) batch at a time.
    Probing,
    /// `Right`/`Full` only: after the left side is exhausted, one final
    /// batch of right rows that were never matched, right side real values
    /// paired with all-NULL left columns.
    UnmatchedRight,
    Done,
}

/// Hash join. See module docs for the build/probe shape, the NULL-key rule,
/// and the memory-accounting contract.
pub struct HashJoin {
    left: Box<dyn Operator>,
    right: Box<dyn Operator>,
    kind: JoinKind,
    left_keys: Vec<usize>,
    right_keys: Vec<usize>,
    budget: usize,
    schema: SchemaRef,
    left_schema: SchemaRef,
    right_schema: SchemaRef,
    row_converter: RowConverter,
    /// The residual (non-equijoin) condition, already flattened by
    /// [`flatten_filter`] so every column is a direct index into the
    /// concatenated left++right row — see the module docs' "Residual
    /// filter" section for what this means per join kind. `None` for the
    /// overwhelming majority of joins, which have no residual at all.
    filter: Option<Expr>,
    /// left_schema ++ right_schema, unmodified nullability. Only built (and
    /// only used) when `filter.is_some()` and `kind` is `LeftSemi`/
    /// `LeftAnti` — those need a real left++right batch to evaluate the
    /// residual against even though the join's own *output* schema
    /// (`schema` above) is left-only. `Inner`/`Cross` reuse `schema`
    /// directly instead, since for them `schema` already *is* left++right.
    filter_schema: SchemaRef,
    /// The statement's evaluation context, used only for `filter`. See
    /// [`SessionRef`] and [`Self::in_session`].
    session: SessionRef,

    phase: Phase,
    right_bytes: usize,
    right_batches: Vec<RecordBatch>,
    /// Set once building completes; `None` only during `Phase::Building`.
    right_table: Option<RecordBatch>,
    /// Encoded key -> row indices into `right_table`. Rows whose key has a
    /// NULL component are never inserted (see module docs).
    hash_table: HashMap<OwnedRow, Vec<u32>>,
    /// One entry per row of `right_table`; set as matches are found while
    /// probing. Only consulted for `Right`/`Full`, but harmless (and cheap)
    /// to maintain for every kind.
    right_matched: Vec<bool>,
    /// `0..right_table.num_rows()`, materialized once, and used as *the*
    /// candidate list for every probe row when the join has no equijoin key
    /// at all — see [`Self::probe_matches`]. Empty (and never read) for a
    /// keyed join or for `Cross`.
    all_right_rows: Vec<u32>,
}

impl HashJoin {
    /// `left_keys`/`right_keys` are column positions into each side's own
    /// schema, one per equijoin conjunct, in matching order — `left_keys[i]`
    /// is compared against `right_keys[i]`. Empty for `Cross`, which ignores
    /// them entirely, and empty for a join whose `ON` has no equality
    /// conjunct at all (see the module docs' "No equijoin key at all"),
    /// which pairs every row with every row and lets the residual decide.
    ///
    /// # Panics
    ///
    /// If the right-side key columns' Arrow types cannot form a row
    /// encoding (`arrow::row::RowConverter::new` rejects them). Equijoin
    /// planning is expected to have already cast both sides' keys to a
    /// common type; reaching this is a planner bug, the same class of
    /// invariant `ExecError::TypeMismatch`'s doc comment describes for
    /// unrelated operators — but since it can only be evaluated once the
    /// concrete Arrow types are known, it is checked at construction rather
    /// than assumed silently.
    pub fn new(
        left: Box<dyn Operator>,
        right: Box<dyn Operator>,
        kind: JoinKind,
        left_keys: Vec<usize>,
        right_keys: Vec<usize>,
        memory_budget: usize,
    ) -> Result<Self, ExecError> {
        Self::with_filter(
            left,
            right,
            kind,
            left_keys,
            right_keys,
            None,
            memory_budget,
        )
    }

    /// Same as [`Self::new`], plus a residual `filter` — see the module's
    /// "Residual filter" docs for the concatenated-row semantics and how
    /// they differ by `kind`.
    ///
    /// # Errors
    ///
    /// Returns [`ExecError::Internal`] if `filter.is_some()` and `kind` is
    /// `Left`/`Right`/`Full` — an outer join's residual has to survive a
    /// failed pair as an unmatched (NULL-extended) row rather than drop it,
    /// which this operator does not implement (see the module docs). This
    /// is a defence-in-depth check, not the primary guard: `build.rs`'s
    /// join arm is expected to refuse these by name, with a message a user
    /// can act on, before ever constructing a `HashJoin`. Reaching this
    /// error means that guard was skipped — a planner bug, not user error.
    pub fn with_filter(
        left: Box<dyn Operator>,
        right: Box<dyn Operator>,
        kind: JoinKind,
        left_keys: Vec<usize>,
        right_keys: Vec<usize>,
        filter: Option<Expr>,
        memory_budget: usize,
    ) -> Result<Self, ExecError> {
        let left_schema = left.schema();
        let right_schema = right.schema();
        let schema = join_output_schema(&left_schema, &right_schema, kind);

        // `left_keys[i]` is compared against `right_keys[i]`, so a length
        // mismatch means one side's key list lost an entry the other still
        // has. That is not caught by the bounds check below — both lists can
        // be individually in range — and the probe path would then encode a
        // different number of key columns on each side, which `RowConverter`
        // answers with encodings that never compare equal rather than with an
        // error: silently zero matched rows, not a failure.
        if left_keys.len() != right_keys.len() {
            return Err(ExecError::Internal(format!(
                "join has {} left key column(s) but {} right — equijoin keys are pairs and \
                 must come in matching counts; a planner bug",
                left_keys.len(),
                right_keys.len()
            )));
        }

        // Bounds-check before indexing. `right_schema.field(i)` panics on an
        // out-of-range index, and a panic here aborts the session rather than
        // returning an error — the bridge's fallback-on-error safety net never
        // sees an unwind. This fired on `SELECT a.id FROM t a JOIN t b ON
        // a.id = b.id`, where projection pruning had removed every column from
        // the build side while the join key still referred to one of them.
        for (side, keys, schema) in [
            ("left", &left_keys, &left_schema),
            ("right", &right_keys, &right_schema),
        ] {
            if let Some(&bad) = keys.iter().find(|&&i| i >= schema.fields().len()) {
                return Err(ExecError::Internal(format!(
                    "join key index {bad} is out of range for the {side} side's {}-column \
                     schema — a planner bug: the key survived into the plan while the column \
                     it names did not",
                    schema.fields().len()
                )));
            }
        }

        if filter.is_some() && matches!(kind, JoinKind::Left | JoinKind::Right | JoinKind::Full) {
            return Err(ExecError::Internal(format!(
                "HashJoin: a residual filter on a {kind:?} join is not implemented — a failed \
                 pair must survive as an unmatched, NULL-extended row rather than be dropped, \
                 and this operator has no path for that. build.rs's join arm is expected to \
                 refuse this before construction; reaching here means it did not."
            )));
        }

        let key_fields: Vec<SortField> = right_keys
            .iter()
            .map(|&i| SortField::new(right_schema.field(i).data_type().clone()))
            .collect();
        let row_converter =
            RowConverter::new(key_fields).expect("join key columns support row encoding");

        let filter = filter.map(|f| flatten_filter(&f, left_schema.fields().len()));
        let filter_schema = {
            let mut fields: Vec<Field> = left_schema
                .fields()
                .iter()
                .map(|f| f.as_ref().clone())
                .collect();
            fields.extend(right_schema.fields().iter().map(|f| f.as_ref().clone()));
            Arc::new(Schema::new(fields))
        };

        Ok(Self {
            left,
            right,
            kind,
            left_keys,
            right_keys,
            budget: memory_budget,
            schema,
            left_schema,
            right_schema,
            row_converter,
            filter,
            filter_schema,
            session: default_session(),
            phase: Phase::Building,
            right_bytes: 0,
            right_batches: Vec::new(),
            right_table: None,
            hash_table: HashMap::new(),
            right_matched: Vec::new(),
            all_right_rows: Vec::new(),
        })
    }

    /// Evaluate this join's residual filter in `session`. See [`SessionRef`].
    ///
    /// Nothing else in a hash join is session-dependent: the equijoin keys are
    /// column indices resolved at plan time, and row equality is byte equality
    /// over the encoded key, which no `TimeZone` changes.
    pub fn in_session(mut self, session: SessionRef) -> Self {
        self.session = session;
        self
    }

    /// Drain the right child fully, concatenate into one table, and build
    /// the probe hash map. Runs once, in one call to `next_batch` (the same
    /// documented cancellation gap as `Sort`'s materialization).
    fn build(&mut self) -> Result<(), ExecError> {
        while let Some(batch) = self.right.next_batch()? {
            let batch_bytes = batch.get_array_memory_size();
            let requested = self.right_bytes + batch_bytes;
            if requested > self.budget {
                return Err(ExecError::OutOfMemory {
                    requested,
                    budget: self.budget,
                });
            }
            self.right_bytes = requested;
            self.right_batches.push(batch);
        }

        let table = if self.right_batches.is_empty() {
            RecordBatch::new_empty(Arc::clone(&self.right_schema))
        } else {
            let combined =
                concat_batches(&self.right_schema, self.right_batches.iter()).map_err(arrow_err)?;
            self.right_batches.clear();
            combined
        };
        self.right_bytes = table.get_array_memory_size();
        self.right_matched = vec![false; table.num_rows()];

        // Cross join never probes the hash table, so building it would be
        // pure waste (and its RowConverter was built from an empty key
        // list, which cannot encode anything meaningful anyway).
        if self.kind != JoinKind::Cross && !self.right_keys.is_empty() {
            let key_cols = project_columns(&table, &self.right_keys);
            let mask = null_key_mask(&key_cols, table.num_rows());
            let rows = self
                .row_converter
                .convert_columns(&key_cols)
                .map_err(arrow_err)?;
            for (i, &is_null) in mask.iter().enumerate() {
                if is_null {
                    continue; // NULL key: can never match, never inserted.
                }
                self.hash_table
                    .entry(rows.row(i).owned())
                    .or_default()
                    .push(i as u32);
            }
        }

        // No equijoin key at all — an `ON` whose every conjunct is a
        // non-equality (`t.id < u.n`, `a.id <> b.id`, `IS NOT DISTINCT
        // FROM`), or a decorrelated `EXISTS` whose only correlation is one.
        // The equality test is then vacuously true for every pair, so every
        // build-side row is a candidate for every probe row and the residual
        // alone decides. A hash table cannot express "matches everything",
        // so the candidate list is materialized once here — this is a
        // nested-loop join wearing the hash join's clothes, and it is
        // deliberately not a hash lookup.
        if self.kind != JoinKind::Cross && self.right_keys.is_empty() {
            self.all_right_rows = (0..table.num_rows() as u32).collect();
        }

        self.right_table = Some(table);
        self.phase = Phase::Probing;
        Ok(())
    }

    fn right_table(&self) -> &RecordBatch {
        self.right_table
            .as_ref()
            .expect("right_table is set before Phase::Probing begins")
    }

    /// Look up one probe row's matches. Empty means either a NULL key
    /// (never matches anything) or a real key with no build-side rows.
    ///
    /// `key` is `None` for a join with no equijoin key at all, where the
    /// equality test is vacuously true and *every* build-side row is a
    /// candidate — see `build`'s `all_right_rows` comment. The NULL-key rule
    /// does not apply in that case: there is no key to be NULL in, so
    /// `rows_null` (which `null_key_mask` reports as `false` for an empty
    /// key list anyway) is not consulted.
    fn probe_matches(&self, rows_null: bool, key: Option<&OwnedRow>) -> &[u32] {
        let Some(key) = key else {
            return &self.all_right_rows;
        };
        if rows_null {
            return &[];
        }
        self.hash_table.get(key).map(Vec::as_slice).unwrap_or(&[])
    }

    /// Evaluate `filter` (already flattened by [`flatten_filter`] — every
    /// column is a direct index into `batch`) and return the resulting
    /// mask. Downcast failure is a planner bug, not user error: `filter` is
    /// only ever produced from a boolean-typed predicate.
    fn eval_filter(&self, filter: &Expr, batch: &RecordBatch) -> Result<BooleanArray, ExecError> {
        let array = eval::eval_with(filter, batch, &self.session)?;
        let bools = array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                ExecError::TypeMismatch(format!(
                    "join residual filter must evaluate to boolean, got {:?} — a planner bug, not \
                 user error",
                    array.data_type()
                ))
            })?;
        Ok(bools.clone())
    }

    /// `Inner`/`Cross` only: apply `self.filter` as a plain post-join
    /// predicate over `batch` (already shaped left++right, since that is
    /// `self.schema` for both these kinds — see `join_output_schema`). A
    /// no-op when there is no filter, which is every `Left`/`Right`/`Full`
    /// call site that (harmlessly) routes through this same helper, since
    /// the constructor refuses to pair a filter with those kinds at all.
    fn apply_residual_filter(&self, batch: RecordBatch) -> Result<RecordBatch, ExecError> {
        let Some(filter) = &self.filter else {
            return Ok(batch);
        };
        let mask = self.eval_filter(filter, &batch)?;
        // `filter_record_batch` treats a null mask entry as false — the
        // same three-valued "NULL excludes the row" rule `WHERE` uses (see
        // `project.rs`'s `Filter` module docs; this mirrors it rather than
        // reimplementing it).
        filter_record_batch(&batch, &mask).map_err(arrow_err)
    }

    /// `LeftSemi`/`LeftAnti` only, when a residual filter is present:
    /// whether `left_row` has at least one right-side row, among
    /// `right_rows` (already known to share the join key), that ALSO
    /// passes the residual. One small left++right candidate batch per left
    /// row rather than one batch-wide evaluation, because the residual can
    /// only be evaluated once a *specific pair* is known — Inner's
    /// "evaluate once over the whole matched batch" approach does not apply
    /// here, since semi/anti's own output never carries the right-side
    /// columns the residual reads.
    fn residual_passes_any(
        &self,
        filter: &Expr,
        left_batch: &RecordBatch,
        left_row: u32,
        right_rows: &[u32],
    ) -> Result<bool, ExecError> {
        let left_idx = UInt32Array::from(vec![left_row; right_rows.len()]);
        let right_idx = UInt32Array::from(right_rows.to_vec());
        let mut cols = take_by_indices(left_batch, &left_idx)?;
        cols.extend(take_by_indices(self.right_table(), &right_idx)?);
        let candidate =
            RecordBatch::try_new(Arc::clone(&self.filter_schema), cols).map_err(arrow_err)?;
        let mask = self.eval_filter(filter, &candidate)?;
        // A NULL residual for one candidate right row must not stop a
        // DIFFERENT right row from deciding the answer — so this reads as
        // "is there any candidate whose mask entry is definitely TRUE",
        // exactly `WHERE`'s three-valued rule (NULL and FALSE both count as
        // "does not pass") applied per candidate rather than to filter rows.
        Ok(mask.iter().any(|v| v == Some(true)))
    }

    /// Process one left (probe) batch under an equijoin kind (everything but
    /// `Cross`). Returns the output batch for this input batch, which may be
    /// empty (e.g. an `Inner` join batch where nothing matched).
    fn probe_batch(&mut self, left_batch: &RecordBatch) -> Result<RecordBatch, ExecError> {
        let n = left_batch.num_rows();
        let key_cols = project_columns(left_batch, &self.left_keys);
        let mask = null_key_mask(&key_cols, n);
        // A keyless join encodes nothing: `convert_columns(&[])` yields a
        // `Rows` with ZERO rows regardless of `n`, and asking it for row `j`
        // panics inside arrow with "row index out of bounds" — which is not
        // an error the bridge can fall back from, it takes the session down.
        // That fired on ordinary SQL: `t JOIN u ON t.id < u.n`,
        // `ON a.id <> b.id`, `ON t.id IS NOT DISTINCT FROM u.tid`, and a
        // decorrelated `EXISTS (... WHERE t2.amt > t1.amt)`. `None` here is
        // what routes every probe row to `all_right_rows` instead.
        let rows = if self.left_keys.is_empty() {
            None
        } else {
            Some(
                self.row_converter
                    .convert_columns(&key_cols)
                    .map_err(arrow_err)?,
            )
        };

        // LeftSemi/LeftAnti: left indices only, at most one per left row,
        // right side never appears in the output at all.
        if matches!(self.kind, JoinKind::LeftSemi | JoinKind::LeftAnti) {
            let mut left_idx: Vec<u32> = Vec::new();
            for (j, &key_is_null) in mask.iter().enumerate() {
                let key = rows.as_ref().map(|r| r.row(j).owned());
                let key_matches = self.probe_matches(key_is_null, key.as_ref());
                // A residual, when present, is part of the MATCH TEST, not
                // an output filter — a left row is semi-matched only if
                // some key-matching right row also passes it (see the
                // module docs' "Residual filter" section for why filtering
                // the output afterwards is a different, wrong, query).
                let has_match = if key_matches.is_empty() {
                    false
                } else if let Some(filter) = &self.filter {
                    self.residual_passes_any(filter, left_batch, j as u32, key_matches)?
                } else {
                    true
                };
                let emit = match self.kind {
                    JoinKind::LeftSemi => has_match,
                    JoinKind::LeftAnti => !has_match,
                    _ => unreachable!(),
                };
                if emit {
                    left_idx.push(j as u32);
                }
            }
            let idx = UInt32Array::from(left_idx);
            let cols = take_by_indices(left_batch, &idx)?;
            return RecordBatch::try_new(Arc::clone(&self.schema), cols).map_err(arrow_err);
        }

        // Inner/Left/Right/Full: build parallel (left_idx, right_idx) index
        // lists; `None` in either means "NULL columns for this side" and is
        // what makes an unmatched row's other side come out all-NULL.
        let mut left_idx: Vec<Option<u32>> = Vec::new();
        let mut right_idx: Vec<Option<u32>> = Vec::new();
        for (j, &key_is_null) in mask.iter().enumerate() {
            let key = rows.as_ref().map(|r| r.row(j).owned());
            // Owned copy: `probe_matches` borrows `self.hash_table`
            // immutably, and the loop below needs to mutate
            // `self.right_matched` while iterating the matches.
            let matches: Vec<u32> = self.probe_matches(key_is_null, key.as_ref()).to_vec();
            if matches.is_empty() {
                match self.kind {
                    // Right preserves only the build side's unmatched rows,
                    // emitted later in Phase::UnmatchedRight — an unmatched
                    // *left* row under Right contributes nothing.
                    JoinKind::Right => {}
                    JoinKind::Inner => {}
                    JoinKind::Left | JoinKind::Full => {
                        left_idx.push(Some(j as u32));
                        right_idx.push(None);
                    }
                    JoinKind::Cross | JoinKind::LeftSemi | JoinKind::LeftAnti => unreachable!(),
                }
            } else {
                for m in matches {
                    self.right_matched[m as usize] = true;
                    left_idx.push(Some(j as u32));
                    right_idx.push(Some(m));
                }
            }
        }

        // Inner is the one equijoin kind a residual can attach to here (see
        // the constructor's guard: `Left`/`Right`/`Full` never carry one).
        // For Inner the residual is a plain post-join predicate — apply it
        // to the whole matched batch, same as a `Filter` sitting above the
        // join would. A no-op for Left/Right/Full, where `self.filter` is
        // always `None`.
        self.apply_residual_filter(self.combine_left_right(left_batch, &left_idx, &right_idx)?)
    }

    /// One left batch's worth of `Cross` output: every left row paired with
    /// every right row. Empty on either side yields zero rows.
    fn cross_batch(&self, left_batch: &RecordBatch) -> Result<RecordBatch, ExecError> {
        let l = left_batch.num_rows();
        let r = self.right_table().num_rows();
        if l == 0 || r == 0 {
            return Ok(RecordBatch::new_empty(Arc::clone(&self.schema)));
        }
        let mut left_idx: Vec<u32> = Vec::with_capacity(l * r);
        let mut right_idx: Vec<u32> = Vec::with_capacity(l * r);
        for li in 0..l {
            for ri in 0..r {
                left_idx.push(li as u32);
                right_idx.push(ri as u32);
            }
        }
        let left_idx = UInt32Array::from(left_idx);
        let right_idx = UInt32Array::from(right_idx);
        let mut cols = take_by_indices(left_batch, &left_idx)?;
        cols.extend(take_by_indices(self.right_table(), &right_idx)?);
        let out = RecordBatch::try_new(Arc::clone(&self.schema), cols).map_err(arrow_err)?;
        // In practice `basin-plan` always relabels a `Cross` carrying a
        // condition to `Inner` before it reaches this builder (see
        // `lower/select.rs`), so `self.filter` is `None` here today — this
        // is a no-op call, kept for symmetry with the Inner path above
        // rather than because it is currently exercised.
        self.apply_residual_filter(out)
    }

    fn combine_left_right(
        &self,
        left_batch: &RecordBatch,
        left_idx: &[Option<u32>],
        right_idx: &[Option<u32>],
    ) -> Result<RecordBatch, ExecError> {
        let left_idx = UInt32Array::from(left_idx.to_vec());
        let right_idx = UInt32Array::from(right_idx.to_vec());
        let mut cols = take_by_indices(left_batch, &left_idx)?;
        cols.extend(take_by_indices(self.right_table(), &right_idx)?);
        RecordBatch::try_new(Arc::clone(&self.schema), cols).map_err(arrow_err)
    }

    /// The tail batch of never-matched right rows for `Right`/`Full`: real
    /// right-side values paired with all-NULL left columns.
    fn unmatched_right_batch(&self) -> Result<RecordBatch, ExecError> {
        let unmatched: Vec<u32> = self
            .right_matched
            .iter()
            .enumerate()
            .filter(|(_, &m)| !m)
            .map(|(i, _)| i as u32)
            .collect();
        if unmatched.is_empty() {
            return Ok(RecordBatch::new_empty(Arc::clone(&self.schema)));
        }
        let idx = UInt32Array::from(unmatched.clone());
        let right_cols = take_by_indices(self.right_table(), &idx)?;
        let left_cols = null_columns(&self.left_schema, unmatched.len());
        let mut cols = left_cols;
        cols.extend(right_cols);
        RecordBatch::try_new(Arc::clone(&self.schema), cols).map_err(arrow_err)
    }
}

impl Operator for HashJoin {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ExecError> {
        loop {
            match self.phase {
                Phase::Building => self.build()?,
                Phase::Probing => match self.left.next_batch()? {
                    Some(left_batch) => {
                        let out = if self.kind == JoinKind::Cross {
                            self.cross_batch(&left_batch)?
                        } else {
                            self.probe_batch(&left_batch)?
                        };
                        if out.num_rows() > 0 {
                            return Ok(Some(out));
                        }
                        // Nothing came out of this particular left batch
                        // (e.g. none of it matched an Inner join) — keep
                        // pulling rather than returning an empty batch.
                    }
                    None => {
                        self.phase = if matches!(self.kind, JoinKind::Right | JoinKind::Full) {
                            Phase::UnmatchedRight
                        } else {
                            Phase::Done
                        };
                    }
                },
                Phase::UnmatchedRight => {
                    self.phase = Phase::Done;
                    let out = self.unmatched_right_batch()?;
                    if out.num_rows() > 0 {
                        return Ok(Some(out));
                    }
                }
                Phase::Done => return Ok(None),
            }
        }
    }

    fn memory_used(&self) -> usize {
        self.right_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::{DataType, Field};
    use basin_pgtype::Oid;
    use basin_plan::OpId;
    use std::collections::VecDeque;

    /// A child operator that replays a fixed list of batches, one per
    /// `next_batch` call — same helper shape as `sort.rs`'s `Feed`, so tests
    /// can control exactly where batch boundaries fall.
    struct Feed {
        schema: SchemaRef,
        batches: VecDeque<RecordBatch>,
    }

    impl Feed {
        fn new(schema: SchemaRef, batches: Vec<RecordBatch>) -> Self {
            Self {
                schema,
                batches: batches.into(),
            }
        }
    }

    impl Operator for Feed {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }

        fn next_batch(&mut self) -> Result<Option<RecordBatch>, ExecError> {
            Ok(self.batches.pop_front())
        }
    }

    fn int_schema(names: &[&str]) -> SchemaRef {
        Arc::new(Schema::new(
            names
                .iter()
                .map(|n| Field::new(*n, DataType::Int32, true))
                .collect::<Vec<_>>(),
        ))
    }

    fn int_batch(schema: &SchemaRef, cols: Vec<Vec<Option<i32>>>) -> RecordBatch {
        let arrays: Vec<ArrayRef> = cols
            .into_iter()
            .map(|c| Arc::new(Int32Array::from(c)) as ArrayRef)
            .collect();
        RecordBatch::try_new(Arc::clone(schema), arrays).unwrap()
    }

    fn feed_one(schema: SchemaRef, batch: RecordBatch) -> Box<dyn Operator> {
        Box::new(Feed::new(schema, vec![batch]))
    }

    fn feed_many(schema: SchemaRef, batches: Vec<RecordBatch>) -> Box<dyn Operator> {
        Box::new(Feed::new(schema, batches))
    }

    fn empty_feed(schema: SchemaRef) -> Box<dyn Operator> {
        Box::new(Feed::new(schema, vec![]))
    }

    /// Pull every batch until exhaustion and concatenate, so tests can
    /// assert on the whole result without caring how it was chunked across
    /// `next_batch` calls.
    fn collect_all(op: &mut dyn Operator) -> RecordBatch {
        let schema = op.schema();
        let mut batches = Vec::new();
        while let Some(b) = op.next_batch().unwrap() {
            batches.push(b);
        }
        if batches.is_empty() {
            return RecordBatch::new_empty(schema);
        }
        concat_batches(&schema, batches.iter()).unwrap()
    }

    fn col_i32(batch: &RecordBatch, i: usize) -> Vec<Option<i32>> {
        batch
            .column(i)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .collect()
    }

    /// Sort (key, payload) pairs so batch-boundary/hash-order nondeterminism
    /// doesn't make an otherwise-correct test flaky.
    fn sorted_pairs(batch: &RecordBatch, a: usize, b: usize) -> Vec<(Option<i32>, Option<i32>)> {
        let mut v: Vec<(Option<i32>, Option<i32>)> = col_i32(batch, a)
            .into_iter()
            .zip(col_i32(batch, b))
            .collect();
        v.sort();
        v
    }

    // ─── Inner ──────────────────────────────────────────────────────────

    #[test]
    fn inner_join_matches_on_equal_keys() {
        let left_schema = int_schema(&["lk", "lv"]);
        let right_schema = int_schema(&["rk", "rv"]);
        let left = feed_one(
            left_schema.clone(),
            int_batch(
                &left_schema,
                vec![
                    vec![Some(1), Some(2), Some(3)],
                    vec![Some(10), Some(20), Some(30)],
                ],
            ),
        );
        let right = feed_one(
            right_schema.clone(),
            int_batch(
                &right_schema,
                vec![
                    vec![Some(2), Some(3), Some(4)],
                    vec![Some(200), Some(300), Some(400)],
                ],
            ),
        );
        let mut join =
            HashJoin::new(left, right, JoinKind::Inner, vec![0], vec![0], 1 << 30).unwrap();
        let out = collect_all(&mut join);
        assert_eq!(out.num_rows(), 2);
        assert_eq!(
            sorted_pairs(&out, 1, 3),
            vec![(Some(20), Some(200)), (Some(30), Some(300))]
        );
    }

    // Trap 1: NULL never equals NULL. A hash table that groups NULL keys
    // together (rather than excluding them) would report matches here; the
    // wrong answer this prevents is a spurious NULL-to-NULL row appearing in
    // an Inner join's output.
    #[test]
    fn null_keys_never_match_each_other() {
        let left_schema = int_schema(&["lk"]);
        let right_schema = int_schema(&["rk"]);
        let left = feed_one(
            left_schema.clone(),
            int_batch(&left_schema, vec![vec![None, None, Some(1)]]),
        );
        let right = feed_one(
            right_schema.clone(),
            int_batch(&right_schema, vec![vec![None, None, Some(1)]]),
        );
        let mut join =
            HashJoin::new(left, right, JoinKind::Inner, vec![0], vec![0], 1 << 30).unwrap();
        let out = collect_all(&mut join);
        // Only the real 1/1 pair should match; every NULL/NULL combination
        // (2 left NULLs x 2 right NULLs = 4 possible spurious rows) must be
        // absent.
        assert_eq!(out.num_rows(), 1);
        assert_eq!(col_i32(&out, 0), vec![Some(1)]);
        assert_eq!(col_i32(&out, 1), vec![Some(1)]);
    }

    #[test]
    fn inner_join_with_no_matches_is_empty() {
        let schema = int_schema(&["k"]);
        let left = feed_one(schema.clone(), int_batch(&schema, vec![vec![Some(1)]]));
        let right = feed_one(schema.clone(), int_batch(&schema, vec![vec![Some(2)]]));
        let mut join =
            HashJoin::new(left, right, JoinKind::Inner, vec![0], vec![0], 1 << 30).unwrap();
        assert_eq!(collect_all(&mut join).num_rows(), 0);
    }

    // ─── Duplicate keys multiply ───────────────────────────────────────

    // Trap 6: duplicate keys on both sides must multiply, not dedupe or add.
    // 3 left rows with key=1 x 2 right rows with key=1 => 6 matched rows.
    #[test]
    fn duplicate_keys_on_both_sides_multiply() {
        let left_schema = int_schema(&["lk", "seq"]);
        let right_schema = int_schema(&["rk", "seq"]);
        let left = feed_one(
            left_schema.clone(),
            int_batch(
                &left_schema,
                vec![
                    vec![Some(1), Some(1), Some(1)],
                    vec![Some(0), Some(1), Some(2)],
                ],
            ),
        );
        let right = feed_one(
            right_schema.clone(),
            int_batch(
                &right_schema,
                vec![vec![Some(1), Some(1)], vec![Some(10), Some(11)]],
            ),
        );
        let mut join =
            HashJoin::new(left, right, JoinKind::Inner, vec![0], vec![0], 1 << 30).unwrap();
        let out = collect_all(&mut join);
        assert_eq!(
            out.num_rows(),
            6,
            "3 left x 2 right matching rows must multiply to 6"
        );
    }

    // ─── Left / Right / Full ────────────────────────────────────────────

    // Trap 4a: an unmatched left row must still be emitted, with the right
    // side entirely NULL — not dropped.
    #[test]
    fn left_join_emits_unmatched_left_rows_with_null_right() {
        let left_schema = int_schema(&["lk"]);
        let right_schema = int_schema(&["rk", "rv"]);
        let left = feed_one(
            left_schema.clone(),
            int_batch(&left_schema, vec![vec![Some(1), Some(2)]]),
        );
        let right = feed_one(
            right_schema.clone(),
            int_batch(&right_schema, vec![vec![Some(1)], vec![Some(100)]]),
        );
        let mut join =
            HashJoin::new(left, right, JoinKind::Left, vec![0], vec![0], 1 << 30).unwrap();
        let out = collect_all(&mut join);
        assert_eq!(out.num_rows(), 2);
        let mut pairs = sorted_pairs(&out, 0, 2);
        pairs.sort();
        assert_eq!(pairs, vec![(Some(1), Some(100)), (Some(2), None)]);
    }

    #[test]
    fn right_join_emits_unmatched_right_rows_with_null_left() {
        let left_schema = int_schema(&["lk", "lv"]);
        let right_schema = int_schema(&["rk"]);
        let left = feed_one(
            left_schema.clone(),
            int_batch(&left_schema, vec![vec![Some(1)], vec![Some(100)]]),
        );
        let right = feed_one(
            right_schema.clone(),
            int_batch(&right_schema, vec![vec![Some(1), Some(2)]]),
        );
        let mut join =
            HashJoin::new(left, right, JoinKind::Right, vec![0], vec![0], 1 << 30).unwrap();
        let out = collect_all(&mut join);
        assert_eq!(out.num_rows(), 2);
        // columns: lk, lv, rk
        let mut rows: Vec<(Option<i32>, Option<i32>, Option<i32>)> = (0..out.num_rows())
            .map(|i| {
                (
                    col_i32(&out, 0)[i],
                    col_i32(&out, 1)[i],
                    col_i32(&out, 2)[i],
                )
            })
            .collect();
        rows.sort();
        assert_eq!(
            rows,
            vec![(None, None, Some(2)), (Some(1), Some(100), Some(1))]
        );
    }

    // Trap 4b: Full join emits unmatched rows from BOTH sides, and never
    // emits a matched row twice (once from the left pass, once from a right
    // "unmatched" pass).
    #[test]
    fn full_join_emits_both_unmatched_sides_and_never_duplicates_a_match() {
        let left_schema = int_schema(&["lk"]);
        let right_schema = int_schema(&["rk"]);
        // left: 1 (matches), 2 (unmatched)
        // right: 1 (matches), 3 (unmatched)
        let left = feed_one(
            left_schema.clone(),
            int_batch(&left_schema, vec![vec![Some(1), Some(2)]]),
        );
        let right = feed_one(
            right_schema.clone(),
            int_batch(&right_schema, vec![vec![Some(1), Some(3)]]),
        );
        let mut join =
            HashJoin::new(left, right, JoinKind::Full, vec![0], vec![0], 1 << 30).unwrap();
        let out = collect_all(&mut join);
        assert_eq!(
            out.num_rows(),
            3,
            "1 matched + 1 unmatched-left + 1 unmatched-right"
        );
        let mut rows: Vec<(Option<i32>, Option<i32>)> = (0..out.num_rows())
            .map(|i| (col_i32(&out, 0)[i], col_i32(&out, 1)[i]))
            .collect();
        rows.sort();
        assert_eq!(
            rows,
            vec![(None, Some(3)), (Some(1), Some(1)), (Some(2), None)]
        );
    }

    #[test]
    fn full_join_with_no_overlap_emits_every_row_from_both_sides_once() {
        let left_schema = int_schema(&["lk"]);
        let right_schema = int_schema(&["rk"]);
        let left = feed_one(
            left_schema.clone(),
            int_batch(&left_schema, vec![vec![Some(1), Some(2)]]),
        );
        let right = feed_one(
            right_schema.clone(),
            int_batch(&right_schema, vec![vec![Some(3), Some(4)]]),
        );
        let mut join =
            HashJoin::new(left, right, JoinKind::Full, vec![0], vec![0], 1 << 30).unwrap();
        let out = collect_all(&mut join);
        assert_eq!(out.num_rows(), 4);
    }

    // ─── LeftSemi / LeftAnti ────────────────────────────────────────────

    // Trap 2a: LeftSemi emits each left row AT MOST ONCE, no matter how many
    // right rows match it — existence, not multiplication (contrast with
    // duplicate_keys_on_both_sides_multiply, which is Inner).
    #[test]
    fn left_semi_emits_each_matching_left_row_once_even_with_multiple_right_matches() {
        let left_schema = int_schema(&["lk"]);
        let right_schema = int_schema(&["rk"]);
        let left = feed_one(
            left_schema.clone(),
            int_batch(&left_schema, vec![vec![Some(1), Some(2)]]),
        );
        // Three right rows share key=1: a naive join would multiply the
        // left row with key=1 into 3 output rows.
        let right = feed_one(
            right_schema.clone(),
            int_batch(&right_schema, vec![vec![Some(1), Some(1), Some(1)]]),
        );
        let mut join =
            HashJoin::new(left, right, JoinKind::LeftSemi, vec![0], vec![0], 1 << 30).unwrap();
        let out = collect_all(&mut join);
        assert_eq!(
            out.num_rows(),
            1,
            "left row with key=1 must appear exactly once"
        );
        assert_eq!(col_i32(&out, 0), vec![Some(1)]);
    }

    // Trap 2b: LeftAnti emits left rows with NO match, and only those.
    #[test]
    fn left_anti_emits_only_unmatched_left_rows() {
        let left_schema = int_schema(&["lk"]);
        let right_schema = int_schema(&["rk"]);
        let left = feed_one(
            left_schema.clone(),
            int_batch(&left_schema, vec![vec![Some(1), Some(2), Some(3)]]),
        );
        let right = feed_one(
            right_schema.clone(),
            int_batch(&right_schema, vec![vec![Some(2)]]),
        );
        let mut join =
            HashJoin::new(left, right, JoinKind::LeftAnti, vec![0], vec![0], 1 << 30).unwrap();
        let out = collect_all(&mut join);
        let mut got: Vec<Option<i32>> = col_i32(&out, 0);
        got.sort();
        assert_eq!(got, vec![Some(1), Some(3)]);
    }

    // Trap 3: LeftSemi/LeftAnti output the LEFT schema only, never left ++
    // right — matches basin_plan::schema::join_schema's contract.
    #[test]
    fn left_semi_and_anti_output_schema_is_left_only() {
        let left_schema = int_schema(&["lk", "lv"]);
        let right_schema = int_schema(&["rk", "rv", "rw"]);
        for kind in [JoinKind::LeftSemi, JoinKind::LeftAnti] {
            let left = empty_feed(left_schema.clone());
            let right = empty_feed(right_schema.clone());
            let join = HashJoin::new(left, right, kind, vec![0], vec![0], 1 << 30).unwrap();
            assert_eq!(
                join.schema().fields().len(),
                2,
                "{kind:?} schema must be left-only (2 fields), not left++right (5)"
            );
            assert_eq!(join.schema(), left_schema);
        }
    }

    /// A NULL-keyed left row can never match, so it must fall on the
    /// LeftAnti side (and never the LeftSemi side) — same NULL-never-equals
    /// rule as trap 1, exercised through semi/anti instead of Inner.
    #[test]
    fn null_keyed_left_row_is_never_a_semi_match_and_always_an_anti_match() {
        let left_schema = int_schema(&["lk"]);
        let right_schema = int_schema(&["rk"]);
        let left = feed_one(
            left_schema.clone(),
            int_batch(&left_schema, vec![vec![None]]),
        );
        let right = feed_one(
            right_schema.clone(),
            int_batch(&right_schema, vec![vec![None]]),
        );

        let left_a = feed_one(
            left_schema.clone(),
            int_batch(&left_schema, vec![vec![None]]),
        );
        let right_a = feed_one(
            right_schema.clone(),
            int_batch(&right_schema, vec![vec![None]]),
        );

        let mut semi =
            HashJoin::new(left, right, JoinKind::LeftSemi, vec![0], vec![0], 1 << 30).unwrap();
        assert_eq!(collect_all(&mut semi).num_rows(), 0);

        let mut anti = HashJoin::new(
            left_a,
            right_a,
            JoinKind::LeftAnti,
            vec![0],
            vec![0],
            1 << 30,
        )
        .unwrap();
        assert_eq!(collect_all(&mut anti).num_rows(), 1);
    }

    // ─── Cross ──────────────────────────────────────────────────────────

    #[test]
    fn cross_join_produces_the_full_cartesian_product() {
        let left_schema = int_schema(&["lk"]);
        let right_schema = int_schema(&["rk"]);
        let left = feed_one(
            left_schema.clone(),
            int_batch(&left_schema, vec![vec![Some(1), Some(2)]]),
        );
        let right = feed_one(
            right_schema.clone(),
            int_batch(&right_schema, vec![vec![Some(10), Some(20), Some(30)]]),
        );
        let mut join =
            HashJoin::new(left, right, JoinKind::Cross, vec![], vec![], 1 << 30).unwrap();
        let out = collect_all(&mut join);
        assert_eq!(out.num_rows(), 6, "2 left x 3 right = 6");
    }

    // Trap 5: Cross join with an empty side yields zero rows, on both sides.
    #[test]
    fn cross_join_with_an_empty_side_yields_zero_rows() {
        let schema = int_schema(&["k"]);

        let left = feed_one(
            schema.clone(),
            int_batch(&schema, vec![vec![Some(1), Some(2)]]),
        );
        let right = empty_feed(schema.clone());
        let mut join =
            HashJoin::new(left, right, JoinKind::Cross, vec![], vec![], 1 << 30).unwrap();
        assert_eq!(collect_all(&mut join).num_rows(), 0, "empty right side");

        let left = empty_feed(schema.clone());
        let right = feed_one(
            schema.clone(),
            int_batch(&schema, vec![vec![Some(1), Some(2)]]),
        );
        let mut join =
            HashJoin::new(left, right, JoinKind::Cross, vec![], vec![], 1 << 30).unwrap();
        assert_eq!(collect_all(&mut join).num_rows(), 0, "empty left side");
    }

    // ─── Empty inputs, other kinds ─────────────────────────────────────

    #[test]
    fn every_kind_handles_both_sides_empty() {
        let left_schema = int_schema(&["lk"]);
        let right_schema = int_schema(&["rk"]);
        for kind in [
            JoinKind::Inner,
            JoinKind::Left,
            JoinKind::Right,
            JoinKind::Full,
            JoinKind::LeftSemi,
            JoinKind::LeftAnti,
        ] {
            let left = empty_feed(left_schema.clone());
            let right = empty_feed(right_schema.clone());
            let mut join = HashJoin::new(left, right, kind, vec![0], vec![0], 1 << 30).unwrap();
            assert_eq!(
                collect_all(&mut join).num_rows(),
                0,
                "{kind:?} on empty/empty"
            );
        }
    }

    #[test]
    fn left_join_with_empty_right_emits_every_left_row_with_null_right() {
        let left_schema = int_schema(&["lk"]);
        let right_schema = int_schema(&["rk"]);
        let left = feed_one(
            left_schema.clone(),
            int_batch(&left_schema, vec![vec![Some(1), Some(2)]]),
        );
        let right = empty_feed(right_schema.clone());
        let mut join =
            HashJoin::new(left, right, JoinKind::Left, vec![0], vec![0], 1 << 30).unwrap();
        let out = collect_all(&mut join);
        assert_eq!(out.num_rows(), 2);
        assert!(col_i32(&out, 1).iter().all(Option::is_none));
    }

    // ─── Multi-column keys ──────────────────────────────────────────────

    #[test]
    fn multi_column_keys_require_every_column_to_match() {
        let left_schema = int_schema(&["a", "b", "lv"]);
        let right_schema = int_schema(&["a", "b", "rv"]);
        // (1,1) should match; (1,2) on the left has no (1,2) on the right.
        let left = feed_one(
            left_schema.clone(),
            int_batch(
                &left_schema,
                vec![
                    vec![Some(1), Some(1)],
                    vec![Some(1), Some(2)],
                    vec![Some(100), Some(200)],
                ],
            ),
        );
        let right = feed_one(
            right_schema.clone(),
            int_batch(
                &right_schema,
                vec![
                    vec![Some(1), Some(1)],
                    vec![Some(1), Some(3)],
                    vec![Some(900), Some(999)],
                ],
            ),
        );
        let mut join = HashJoin::new(
            left,
            right,
            JoinKind::Inner,
            vec![0, 1],
            vec![0, 1],
            1 << 30,
        )
        .unwrap();
        let out = collect_all(&mut join);
        assert_eq!(
            out.num_rows(),
            1,
            "only (a=1,b=1) matches on both key columns"
        );
        // columns: left a, b, lv (0,1,2), right a, b, rv (3,4,5).
        assert_eq!(col_i32(&out, 2), vec![Some(100)]);
        assert_eq!(col_i32(&out, 5), vec![Some(900)]);
    }

    /// A NULL in just one of two key columns still disqualifies the whole
    /// key from matching — the NULL rule applies per-row across the key
    /// tuple, not per-column.
    #[test]
    fn a_null_in_any_one_key_column_disqualifies_the_whole_composite_key() {
        let left_schema = int_schema(&["a", "b"]);
        let right_schema = int_schema(&["a", "b"]);
        let left = feed_one(
            left_schema.clone(),
            int_batch(&left_schema, vec![vec![Some(1)], vec![None]]),
        );
        let right = feed_one(
            right_schema.clone(),
            int_batch(&right_schema, vec![vec![Some(1)], vec![None]]),
        );
        let mut join = HashJoin::new(
            left,
            right,
            JoinKind::Inner,
            vec![0, 1],
            vec![0, 1],
            1 << 30,
        )
        .unwrap();
        assert_eq!(
            collect_all(&mut join).num_rows(),
            0,
            "a=1 matches but b=NULL must veto the whole row"
        );
    }

    // ─── Batch boundaries ───────────────────────────────────────────────

    /// The probe side is streamed batch by batch; a match that spans a left
    /// batch boundary (i.e. the second batch matches a right row) must still
    /// be found — the hash table has to survive across `next_batch` calls.
    #[test]
    fn matches_are_found_across_multiple_left_batches() {
        let left_schema = int_schema(&["lk"]);
        let right_schema = int_schema(&["rk"]);
        let left = feed_many(
            left_schema.clone(),
            vec![
                int_batch(&left_schema, vec![vec![Some(1)]]),
                int_batch(&left_schema, vec![vec![Some(2)]]),
                int_batch(&left_schema, vec![vec![Some(3)]]),
            ],
        );
        let right = feed_one(
            right_schema.clone(),
            int_batch(&right_schema, vec![vec![Some(1), Some(2), Some(3)]]),
        );
        let mut join =
            HashJoin::new(left, right, JoinKind::Inner, vec![0], vec![0], 1 << 30).unwrap();
        assert_eq!(collect_all(&mut join).num_rows(), 3);
    }

    // ─── Memory budget ──────────────────────────────────────────────────

    #[test]
    fn memory_used_reports_the_build_side_honestly() {
        let left_schema = int_schema(&["lk"]);
        let right_schema = int_schema(&["rk"]);
        let left = empty_feed(left_schema.clone());
        let right_batch = int_batch(&right_schema, vec![(0..1000).map(Some).collect()]);
        let expected_bytes = right_batch.get_array_memory_size();
        let right = feed_one(right_schema.clone(), right_batch);
        let mut join =
            HashJoin::new(left, right, JoinKind::Inner, vec![0], vec![0], 1 << 30).unwrap();
        assert_eq!(
            join.memory_used(),
            0,
            "nothing materialized before the first pull"
        );
        join.next_batch().unwrap();
        assert_eq!(join.memory_used(), expected_bytes);
    }

    #[test]
    fn exceeding_the_memory_budget_during_build_is_out_of_memory() {
        let left_schema = int_schema(&["lk"]);
        let right_schema = int_schema(&["rk"]);
        let left = empty_feed(left_schema.clone());
        let right_batch = int_batch(&right_schema, vec![(0..1000).map(Some).collect()]);
        let size = right_batch.get_array_memory_size();
        let right = feed_one(right_schema.clone(), right_batch);
        let mut join =
            HashJoin::new(left, right, JoinKind::Inner, vec![0], vec![0], size - 1).unwrap();
        let err = join.next_batch().unwrap_err();
        assert!(matches!(err, ExecError::OutOfMemory { .. }), "{err:?}");
    }

    #[test]
    fn memory_budget_is_not_charged_for_the_probe_left_side() {
        // The left side is streamed, never buffered whole — a left input far
        // larger than the budget must not fail, so long as the (small)
        // right side fits.
        let left_schema = int_schema(&["lk"]);
        let right_schema = int_schema(&["rk"]);
        let big_left = feed_many(
            left_schema.clone(),
            (0..2000)
                .map(|i| int_batch(&left_schema, vec![vec![Some(i)]]))
                .collect(),
        );
        let right = feed_one(
            right_schema.clone(),
            int_batch(&right_schema, vec![vec![Some(1)]]),
        );
        let mut join =
            HashJoin::new(big_left, right, JoinKind::Inner, vec![0], vec![0], 4096).unwrap();
        // Should not error; exactly one match (key=1) exists somewhere in
        // the left stream.
        let out = collect_all(&mut join);
        assert_eq!(out.num_rows(), 1);
    }

    // ─── Mixed types via a string key column (sanity for RowConverter) ──

    #[test]
    fn string_key_columns_join_correctly() {
        let left_schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, true),
            Field::new("v", DataType::Int32, true),
        ]));
        let right_schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, true),
            Field::new("v", DataType::Int32, true),
        ]));
        let left_batch = RecordBatch::try_new(
            left_schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![Some("a"), Some("b"), None])) as ArrayRef,
                Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)])) as ArrayRef,
            ],
        )
        .unwrap();
        let right_batch = RecordBatch::try_new(
            right_schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![Some("b"), None])) as ArrayRef,
                Arc::new(Int32Array::from(vec![Some(20), Some(30)])) as ArrayRef,
            ],
        )
        .unwrap();
        let left = feed_one(left_schema, left_batch);
        let right = feed_one(right_schema, right_batch);
        let mut join =
            HashJoin::new(left, right, JoinKind::Inner, vec![0], vec![0], 1 << 30).unwrap();
        let out = collect_all(&mut join);
        // "b"/"b" matches; the two NULL keys never match each other.
        assert_eq!(out.num_rows(), 1);
        assert_eq!(col_i32(&out, 1), vec![Some(2)]);
        assert_eq!(col_i32(&out, 3), vec![Some(20)]);
    }

    // ─── Residual filter ────────────────────────────────────────────────
    //
    // Shapes and expected rows verified against a live PostgreSQL 18.2
    // before writing any of this: `l(lk,lv)` = (1,10),(2,20),(3,30);
    // `r(rk,rv)` = (1,100),(1,200),(2,5),(2,999); residual `r.rv < l.lv`.
    //
    //   key=1: both right rows fail (100<10, 200<10 both false) — the
    //          right rows exist but neither passes.
    //   key=2: one right row passes (5<20), one fails (999<20).
    //   key=3: no right row shares the key at all.
    //
    // Postgres:
    //   INNER ................ only (lk=2, rv=5)
    //   SEMI  (EXISTS) ....... only lk=2
    //   ANTI  (NOT EXISTS) ... lk=1 and lk=3
    //
    // A NULL residual (`r.rv` itself NULL) behaves like FALSE everywhere —
    // also checked against the live server, in
    // `null_residual_is_treated_as_not_passing`.
    //
    // These build `filter` with `relation: 1` on the right-side column —
    // the shape `opt/decorrelate.rs`'s `split_join_conjuncts` actually
    // produces for a decorrelated `EXISTS`/`IN` (see
    // `a_non_equality_correlation_stays_in_filter_not_on` in that module),
    // which is the shape this module's `flatten_filter` exists to handle —
    // rather than the already-flat `relation: 0` shape an ordinary `WHERE`
    // clause produces, so a broken `flatten_filter` is exercised, not
    // sidestepped.

    fn rel_col(relation: u16, index: u16, name: &str) -> Expr {
        Expr::Column(ColumnRef {
            relation,
            index,
            name: name.to_string(),
        })
    }

    fn op(oid_val: u32) -> OpId {
        OpId(Oid(oid_val))
    }

    /// `r.rv < l.lv`, addressed the way a decorrelated join's `filter`
    /// actually is: `relation: 0` for the join's own left (`l`, 0-based),
    /// `relation: 1` for its right (`r`, ALSO 0-based — not yet offset by
    /// `left_len`, which is exactly what `flatten_filter` must do before
    /// this is usable against a concatenated batch).
    fn rv_lt_lv() -> Expr {
        Expr::Binary {
            op: op(97),                         // int4 <
            lhs: Box::new(rel_col(1, 1, "rv")), // r.rv, right-relative index 1
            rhs: Box::new(rel_col(0, 1, "lv")), // l.lv, left-relative index 1
        }
    }

    /// `l(lk,lv)`/`r(rk,rv)` fixture shared by the Inner/Semi/Anti residual
    /// tests below, matching the psql session in this section's own doc
    /// comment exactly.
    fn residual_fixture() -> (Box<dyn Operator>, Box<dyn Operator>) {
        let left_schema = int_schema(&["lk", "lv"]);
        let right_schema = int_schema(&["rk", "rv"]);
        let left = feed_one(
            left_schema.clone(),
            int_batch(
                &left_schema,
                vec![
                    vec![Some(1), Some(2), Some(3)],
                    vec![Some(10), Some(20), Some(30)],
                ],
            ),
        );
        let right = feed_one(
            right_schema.clone(),
            int_batch(
                &right_schema,
                vec![
                    vec![Some(1), Some(1), Some(2), Some(2)],
                    vec![Some(100), Some(200), Some(5), Some(999)],
                ],
            ),
        );
        (left, right)
    }

    #[test]
    fn inner_join_residual_filters_the_matched_output() {
        let (left, right) = residual_fixture();
        let mut join = HashJoin::with_filter(
            left,
            right,
            JoinKind::Inner,
            vec![0],
            vec![0],
            Some(rv_lt_lv()),
            1 << 30,
        )
        .unwrap();
        let out = collect_all(&mut join);
        // Only (lk=2, rv=5) passes both the key equality and the residual —
        // a naive equijoin-only Inner would additionally emit (2,999),
        // (1,100), (1,200).
        assert_eq!(out.num_rows(), 1, "psql: only one row survives");
        assert_eq!(col_i32(&out, 0), vec![Some(2)]);
        assert_eq!(col_i32(&out, 3), vec![Some(5)]);
    }

    // Trap: a naive "filter the output afterwards" LeftSemi implementation
    // would build the same left-only rows an unfiltered semi join does and
    // then have nothing left-side-only to filter by (the residual reads
    // `r.rv`, which never reaches semi's output) — this only passes if the
    // residual is applied as part of the MATCH TEST itself, per right-side
    // candidate, before the left row is ever admitted.
    #[test]
    fn left_semi_residual_requires_a_matching_right_row_to_also_pass_it() {
        let (left, right) = residual_fixture();
        let mut join = HashJoin::with_filter(
            left,
            right,
            JoinKind::LeftSemi,
            vec![0],
            vec![0],
            Some(rv_lt_lv()),
            1 << 30,
        )
        .unwrap();
        let out = collect_all(&mut join);
        assert_eq!(
            col_i32(&out, 0),
            vec![Some(2)],
            "psql: only lk=2 has a key-matching right row that also passes the residual; lk=1's \
             two right-side matches both fail it, lk=3 has no right-side match at all"
        );
    }

    // Trap: a naive "filter the output afterwards" LeftAnti implementation
    // inverts the meaning entirely — it would EXCLUDE exactly the rows anti
    // must INCLUDE (lk=1, whose right-side matches all fail the residual)
    // and, since anti's output never carries `r.rv`, has no column left to
    // filter by at all.
    #[test]
    fn left_anti_residual_includes_a_left_row_whose_matches_all_fail_it() {
        let (left, right) = residual_fixture();
        let mut join = HashJoin::with_filter(
            left,
            right,
            JoinKind::LeftAnti,
            vec![0],
            vec![0],
            Some(rv_lt_lv()),
            1 << 30,
        )
        .unwrap();
        let out = collect_all(&mut join);
        let mut got = col_i32(&out, 0);
        got.sort();
        assert_eq!(
            got,
            vec![Some(1), Some(3)],
            "psql: lk=1 (matches exist, all fail the residual) and lk=3 (no match at all) are \
             anti; lk=2 (a match exists AND passes) is not"
        );
    }

    /// Same fixture, asymmetric column *widths* on top of the asymmetric
    /// relation tagging `rv_lt_lv` already exercises — `l` has 2 columns,
    /// `r` has 3 (an extra leading throwaway column), so `left_len` (2) and
    /// `right_len` (3) actually differ. If `flatten_filter` used the wrong
    /// offset (or no offset — treating `relation: 1, index: 1` as already a
    /// flat position instead of adding `left_len`), the residual would
    /// silently read `l`'s own column 1 (`lv`) instead of `r.rv`, making
    /// the predicate `l.lv < l.lv` — always false — rather than `r.rv <
    /// l.lv`. That wrong predicate would make every right-side match fail,
    /// which for THIS fixture (unlike `residual_fixture`, where key=2 has a
    /// row that passes) produces a *different, wrong* anti/semi split this
    /// test asserts against, so an off-by-`left_len` bug fails loudly
    /// rather than by coincidence.
    #[test]
    fn residual_filter_respects_asymmetric_left_right_widths() {
        let left_schema = int_schema(&["lk", "lv"]);
        let right_schema = int_schema(&["junk", "rk", "rv"]);
        let left = feed_one(
            left_schema.clone(),
            int_batch(&left_schema, vec![vec![Some(1)], vec![Some(10)]]),
        );
        // key=1 has two right-side matches: rv=100 (fails 100<10) and
        // rv=5 (passes 5<10) — same "one passes" shape as key=2 in
        // `residual_fixture`, just relocated to a 3-column right side with
        // the key now in the MIDDLE column (index 1), not the first.
        //
        // `junk` is deliberately always 999 (never < lv=10), NOT some value
        // that would coincidentally satisfy the residual too — a version of
        // this test caught reading the wrong column (`junk` at flat index 2
        // instead of `rv` at flat index 4) only after this was tightened:
        // an earlier `junk = -1` happened to also read as "always < lv",
        // so the wrong column silently produced the same pass/fail split as
        // the right one and the test passed for the wrong reason.
        let right = feed_one(
            right_schema.clone(),
            int_batch(
                &right_schema,
                vec![
                    vec![Some(999), Some(999)],
                    vec![Some(1), Some(1)],
                    vec![Some(100), Some(5)],
                ],
            ),
        );
        // `r.rv` is now right-relative index 2, not 1.
        let filter = Expr::Binary {
            op: op(97), // int4 <
            lhs: Box::new(rel_col(1, 2, "rv")),
            rhs: Box::new(rel_col(0, 1, "lv")),
        };

        let mut semi = HashJoin::with_filter(
            left,
            right,
            JoinKind::LeftSemi,
            vec![0],
            vec![1], // right's join key is its middle column
            Some(filter),
            1 << 30,
        )
        .unwrap();
        let out = collect_all(&mut semi);
        assert_eq!(
            out.num_rows(),
            1,
            "the rv=5 match passes the residual, so lk=1 must be semi-included — a wrong \
             offset would compare l.lv (10) against itself and find no passing match"
        );
    }

    /// `WHERE r.rv < l.lv` when `r.rv` is itself NULL is NULL, not TRUE or
    /// FALSE — and NULL must count as "does not pass", the same rule
    /// `WHERE` uses (`project.rs`'s `Filter` docs), confirmed against a
    /// live Postgres in this section's own doc comment. Checked through
    /// LeftSemi/LeftAnti since Inner's NULL handling is already
    /// `filter_record_batch`'s well-tested job, not new code this task
    /// added.
    #[test]
    fn null_residual_is_treated_as_not_passing() {
        let left_schema = int_schema(&["lk", "lv"]);
        let right_schema = int_schema(&["rk", "rv"]);
        let left = feed_one(
            left_schema.clone(),
            int_batch(&left_schema, vec![vec![Some(1)], vec![Some(10)]]),
        );
        let right = feed_one(
            right_schema.clone(),
            int_batch(&right_schema, vec![vec![Some(1)], vec![None]]),
        );
        let left_a = feed_one(
            left_schema.clone(),
            int_batch(&left_schema, vec![vec![Some(1)], vec![Some(10)]]),
        );
        let right_a = feed_one(
            right_schema.clone(),
            int_batch(&right_schema, vec![vec![Some(1)], vec![None]]),
        );

        let mut semi = HashJoin::with_filter(
            left,
            right,
            JoinKind::LeftSemi,
            vec![0],
            vec![0],
            Some(rv_lt_lv()),
            1 << 30,
        )
        .unwrap();
        assert_eq!(
            collect_all(&mut semi).num_rows(),
            0,
            "psql: a NULL residual excludes the row from SEMI, same as FALSE"
        );

        let mut anti = HashJoin::with_filter(
            left_a,
            right_a,
            JoinKind::LeftAnti,
            vec![0],
            vec![0],
            Some(rv_lt_lv()),
            1 << 30,
        )
        .unwrap();
        assert_eq!(
            collect_all(&mut anti).num_rows(),
            1,
            "psql: the same NULL residual leaves lk=1 in ANTI, since no right-side match \
             actually passed"
        );
    }

    /// `build.rs`'s join arm is expected to refuse a residual on
    /// `Left`/`Right`/`Full` before ever constructing a `HashJoin` (see the
    /// module docs) — this is the defence-in-depth check inside the
    /// constructor itself, proving the operator does not silently accept
    /// (and misexecute) a combination it has no correct implementation for.
    #[test]
    fn constructor_refuses_a_residual_filter_on_an_outer_join() {
        for kind in [JoinKind::Left, JoinKind::Right, JoinKind::Full] {
            let left_schema = int_schema(&["lk", "lv"]);
            let right_schema = int_schema(&["rk", "rv"]);
            let left = empty_feed(left_schema);
            let right = empty_feed(right_schema);
            // `HashJoin` does not implement `Debug` (nothing else needs
            // it), so `Result::unwrap_err` — which requires the `Ok` side
            // to be `Debug` for its panic message — cannot be used here;
            // `.err()` discards the `Ok` side entirely instead.
            let err = HashJoin::with_filter(
                left,
                right,
                kind,
                vec![0],
                vec![0],
                Some(rv_lt_lv()),
                1 << 30,
            )
            .err()
            .expect("a residual on an outer join must be refused");
            assert!(
                matches!(err, ExecError::Internal(_)),
                "{kind:?} with a residual must be refused, got {err:?}"
            );
        }
    }

    // ─── No equijoin key at all ─────────────────────────────────────────
    //
    // REGRESSION. Every test in this section used to PANIC, not fail: with
    // an empty key list `RowConverter::convert_columns(&[])` yields a `Rows`
    // holding zero rows whatever the batch width, and `probe_batch` asked it
    // for row `j` of `n` anyway — arrow answers that with
    // `panic!("row index out of bounds")` from inside `Rows::row`, which
    // unwinds past the engine's fallback-on-error path and takes the session
    // down. Four queries in the owned-engine probe hit it on plain SQL:
    //
    //   SELECT t.id, u.n   FROM t JOIN u ON t.id < u.n
    //   SELECT t.id, u.tag FROM t JOIN u ON t.id IS NOT DISTINCT FROM u.tid
    //   SELECT a.id, b.id  FROM t a JOIN t b ON a.id <> b.id AND a.id < b.id
    //   SELECT id FROM t t1 WHERE EXISTS (SELECT 1 FROM t t2 WHERE t2.amt > t1.amt)
    //
    // Every expected row below was read off a live PostgreSQL 18.2 with the
    // same data, not derived from what this implementation happens to do.
    // Fixture (same `l`/`r` as the residual section above):
    //   l(lk,lv) = (1,10),(2,20),(3,30)
    //   r(rk,rv) = (1,100),(1,200),(2,5),(2,999)
    //
    //   SELECT lk,lv,rk,rv FROM l JOIN r ON r.rv < l.lv  ->  3 rows, all rv=5
    //   SELECT lk FROM l WHERE EXISTS     (SELECT 1 FROM r WHERE r.rv<l.lv) -> 1,2,3
    //   SELECT lk FROM l WHERE NOT EXISTS (SELECT 1 FROM r WHERE r.rv<l.lv) -> none

    /// `r.rv > l.lv`, same relation-tagging convention as [`rv_lt_lv`].
    fn rv_gt_lv() -> Expr {
        Expr::Binary {
            op: op(521), // int4 >
            lhs: Box::new(rel_col(1, 0, "rk")),
            rhs: Box::new(rel_col(0, 0, "lk")),
        }
    }

    #[test]
    fn keyless_inner_join_pairs_every_row_and_lets_the_residual_decide() {
        let (left, right) = residual_fixture();
        let mut join = HashJoin::with_filter(
            left,
            right,
            JoinKind::Inner,
            Vec::new(),
            Vec::new(),
            Some(rv_lt_lv()),
            1 << 30,
        )
        .unwrap();
        let out = collect_all(&mut join);
        // psql: `l JOIN r ON r.rv < l.lv` — rv=5 is the only right row below
        // ANY left lv, and it pairs with all three left rows. An equijoin
        // that (wrongly) required a shared key would emit only (2,5).
        assert_eq!(out.num_rows(), 3);
        assert_eq!(
            sorted_pairs(&out, 0, 3),
            vec![(Some(1), Some(5)), (Some(2), Some(5)), (Some(3), Some(5))]
        );
    }

    #[test]
    fn keyless_left_semi_and_anti_use_every_right_row_as_a_candidate() {
        let (left, right) = residual_fixture();
        let mut semi = HashJoin::with_filter(
            left,
            right,
            JoinKind::LeftSemi,
            Vec::new(),
            Vec::new(),
            Some(rv_lt_lv()),
            1 << 30,
        )
        .unwrap();
        assert_eq!(
            col_i32(&collect_all(&mut semi), 0),
            vec![Some(1), Some(2), Some(3)],
            "psql: EXISTS (SELECT 1 FROM r WHERE r.rv < l.lv) is true for every left row, \
             because rv=5 is below all three lv values"
        );

        let (left, right) = residual_fixture();
        let mut anti = HashJoin::with_filter(
            left,
            right,
            JoinKind::LeftAnti,
            Vec::new(),
            Vec::new(),
            Some(rv_lt_lv()),
            1 << 30,
        )
        .unwrap();
        assert_eq!(
            collect_all(&mut anti).num_rows(),
            0,
            "psql: NOT EXISTS is false for every left row — the exact negation of the SEMI \
             result above"
        );
    }

    /// The one shape where the keyless path must do the OPPOSITE of the
    /// keyed path's NULL rule. `IS NOT DISTINCT FROM` reaches this operator
    /// with no equijoin key and a `DistinctFrom` residual, and NULL there
    /// *does* match NULL — whereas a keyed `=` join must never pair two NULL
    /// keys (`null_keys_never_match_each_other`). Routing a keyless join
    /// through the hash table's NULL-exclusion rule would silently drop the
    /// NULL/NULL row.
    ///
    /// psql, with `ln(lk)` = 1, NULL, 3 and `rn(rk)` = 1, NULL, 4:
    ///   SELECT lk,rk FROM ln JOIN rn ON ln.lk IS NOT DISTINCT FROM rn.rk
    ///     -> (1,1) and (NULL,NULL)
    ///   SELECT lk,rk FROM ln JOIN rn ON ln.lk = rn.rk   -> (1,1) alone
    #[test]
    fn keyless_join_on_is_not_distinct_from_matches_null_to_null() {
        let left_schema = int_schema(&["lk"]);
        let right_schema = int_schema(&["rk"]);
        let left = feed_one(
            left_schema.clone(),
            int_batch(&left_schema, vec![vec![Some(1), None, Some(3)]]),
        );
        let right = feed_one(
            right_schema.clone(),
            int_batch(&right_schema, vec![vec![Some(1), None, Some(4)]]),
        );
        let filter = Expr::DistinctFrom {
            lhs: Box::new(rel_col(0, 0, "lk")),
            rhs: Box::new(rel_col(1, 0, "rk")),
            negated: true,
        };
        let mut join = HashJoin::with_filter(
            left,
            right,
            JoinKind::Inner,
            Vec::new(),
            Vec::new(),
            Some(filter),
            1 << 30,
        )
        .unwrap();
        let out = collect_all(&mut join);
        assert_eq!(
            sorted_pairs(&out, 0, 1),
            vec![(None, None), (Some(1), Some(1))],
            "psql: IS NOT DISTINCT FROM pairs NULL with NULL — 3 is distinct from every rk, \
             and 4 from every lk"
        );
    }

    /// A NULL residual still excludes the candidate on the keyless path,
    /// exactly as it does on the keyed one — so a left row whose every
    /// candidate evaluates to NULL is absent from SEMI and present in ANTI.
    ///
    /// psql, with `ln(lk)` = 1, NULL, 3 and `rn(rk)` = 1, NULL, 4:
    ///   SELECT lk FROM ln WHERE EXISTS     (SELECT 1 FROM rn WHERE rn.rk > ln.lk) -> 1, 3
    ///   SELECT lk FROM ln WHERE NOT EXISTS (SELECT 1 FROM rn WHERE rn.rk > ln.lk) -> NULL
    #[test]
    fn keyless_semi_anti_treat_a_null_residual_as_not_passing() {
        let left_schema = int_schema(&["lk"]);
        let right_schema = int_schema(&["rk"]);
        let fixture = || {
            (
                feed_one(
                    left_schema.clone(),
                    int_batch(&left_schema, vec![vec![Some(1), None, Some(3)]]),
                ),
                feed_one(
                    right_schema.clone(),
                    int_batch(&right_schema, vec![vec![Some(1), None, Some(4)]]),
                ),
            )
        };

        let (left, right) = fixture();
        let mut semi = HashJoin::with_filter(
            left,
            right,
            JoinKind::LeftSemi,
            Vec::new(),
            Vec::new(),
            Some(rv_gt_lv()),
            1 << 30,
        )
        .unwrap();
        assert_eq!(
            col_i32(&collect_all(&mut semi), 0),
            vec![Some(1), Some(3)],
            "psql: lk=NULL has no candidate that is definitely TRUE, so EXISTS is false"
        );

        let (left, right) = fixture();
        let mut anti = HashJoin::with_filter(
            left,
            right,
            JoinKind::LeftAnti,
            Vec::new(),
            Vec::new(),
            Some(rv_gt_lv()),
            1 << 30,
        )
        .unwrap();
        assert_eq!(
            col_i32(&collect_all(&mut anti), 0),
            vec![None],
            "psql: NOT EXISTS keeps exactly the lk=NULL row"
        );
    }

    /// A keyless join with no residual at all is a cross product, and a
    /// keyless LEFT join over an EMPTY build side must still NULL-extend
    /// every preserved row rather than emit nothing — the same
    /// `matches.is_empty()` branch a keyed outer join takes, reached here
    /// because `all_right_rows` is itself empty.
    ///
    /// psql, `l(lk,lv)` = (1,10),(2,20),(3,30):
    ///   SELECT lk,rk FROM l LEFT JOIN r ON true                 -> 12 rows (3 x 4)
    ///   SELECT lk,rk FROM l LEFT JOIN (empty) z ON true         -> (1,NULL),(2,NULL),(3,NULL)
    #[test]
    fn keyless_outer_join_without_a_residual_is_a_cross_product() {
        let left_schema = int_schema(&["lk", "lv"]);
        let right_schema = int_schema(&["rk", "rv"]);

        let (left, right) = residual_fixture();
        let mut join = HashJoin::new(
            left,
            right,
            JoinKind::Left,
            Vec::new(),
            Vec::new(),
            1 << 30,
        )
        .unwrap();
        assert_eq!(
            collect_all(&mut join).num_rows(),
            12,
            "psql: 3 left rows x 4 right rows"
        );

        let left = feed_one(
            left_schema.clone(),
            int_batch(
                &left_schema,
                vec![
                    vec![Some(1), Some(2), Some(3)],
                    vec![Some(10), Some(20), Some(30)],
                ],
            ),
        );
        let mut empty_right = HashJoin::new(
            left,
            empty_feed(right_schema),
            JoinKind::Left,
            Vec::new(),
            Vec::new(),
            1 << 30,
        )
        .unwrap();
        let out = collect_all(&mut empty_right);
        assert_eq!(
            sorted_pairs(&out, 0, 2),
            vec![(Some(1), None), (Some(2), None), (Some(3), None)],
            "psql: every preserved left row survives NULL-extended when the build side is empty"
        );
    }

    /// Equijoin keys are pairs. A length mismatch would encode a different
    /// number of key columns on each side and silently compare mismatched
    /// row encodings, so the constructor refuses it rather than run it.
    #[test]
    fn constructor_refuses_mismatched_key_counts() {
        let err = HashJoin::new(
            empty_feed(int_schema(&["lk", "lv"])),
            empty_feed(int_schema(&["rk", "rv"])),
            JoinKind::Inner,
            vec![0, 1],
            vec![0],
            1 << 30,
        )
        .err()
        .expect("mismatched key counts must be refused");
        assert!(matches!(err, ExecError::Internal(_)), "got {err:?}");
    }
}
