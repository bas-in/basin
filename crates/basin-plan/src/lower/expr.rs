//! Lowering a `pg_query` expression parse-tree node into Basin's [`Expr`].
//!
//! This is the module that deletes `basin-engine/src/pg_operators.rs`
//! (9,546 lines of SQL-text rewriting whose own header admits it cannot
//! handle dollar-quoted strings, comments, or quoted identifiers). Lowering
//! walks the real Postgres parse tree instead of pattern-matching on
//! rewritten SQL text, and resolves operators/functions/columns through
//! traits rather than hardcoded string matches — the exact thing
//! `pg_operators.rs` cannot do because there is no catalog underneath it.
//!
//! # Catalogs that don't exist yet
//!
//! A column catalog and a function catalog (`pg_proc`) are not built yet.
//! Where one is missing, this module defines a trait seam ([`ColumnResolver`],
//! later `FunctionResolver`) instead of hardcoding OIDs or falling back to
//! name matching. A mock resolver makes the walk testable today; a real
//! catalog implements the same trait later without this file changing.
//!
//! `basin_pgtype::operator::resolve(name, left, right)` now exists as the
//! real `pg_operator` table, and [`OperatorResolver`]'s signature mirrors it
//! exactly (`left: Option<PgType>` for prefix operators, `right: PgType`
//! always present) so wiring a real implementation later is a one-line
//! adapter, not a redesign.
//!
//! # Why `AND`/`OR`/`NOT` go through `OperatorResolver` too
//!
//! Basin's [`Expr`] has no dedicated boolean-connective variant — deliberately,
//! since `AND`/`OR`/`NOT` are otherwise ordinary strict boolean operators, and
//! giving them their own IR shape would mean every consumer (the optimizer,
//! `EXPLAIN`, constant folding) special-cases them separately from every other
//! operator. They lower to [`Expr::Binary`] / [`Expr::Unary`] through the same
//! `OperatorResolver` seam as `+` or `~`, using the synthetic names `"AND"`,
//! `"OR"`, `"NOT"` that never collide with a real `pg_operator.oprname` (all of
//! which are made of punctuation).

use basin_pgtype::PgType;
use pg_query::protobuf::{node::Node as NodeEnum, AConst, AExpr, AExprKind, BoolExpr, BoolExprType, Node};

use crate::expr::{ColumnRef, Datum, Expr, OpId};
use crate::lower::LowerError;

// ─── Resolver seams ──────────────────────────────────────────────────────

/// Resolves a (possibly qualified) column name — `col`, `t.col`,
/// `schema.t.col` — to its position in the plan currently being built.
///
/// There is no catalog to back this yet; a mock implementation makes the
/// walk in this file testable today, and a real one (backed by the schema of
/// whatever relation is in scope) plugs in later without this file changing.
pub trait ColumnResolver {
    fn resolve(&self, parts: &[String]) -> Option<ColumnRef>;
}

/// Resolves an operator name and operand types to a `pg_operator` entry.
///
/// `left` is `None` for a prefix operator (`-x`, `NOT x`); every builtin
/// operator has a right operand, so `right` is not optional. This mirrors
/// `basin_pgtype::operator::resolve` deliberately, so a real implementation
/// of this trait is a thin adapter over that function rather than a new
/// design.
pub trait OperatorResolver {
    fn resolve(&self, name: &str, left: Option<PgType>, right: PgType) -> Option<OpId>;
}

/// Everything a scalar-expression lowering pass needs that isn't yet a real
/// catalog. Bundled into one struct purely for call-site ergonomics — each
/// field is exactly one of the trait seams documented above.
pub struct LowerCtx<'a> {
    pub columns: &'a dyn ColumnResolver,
    pub operators: &'a dyn OperatorResolver,
}

/// Lower one `pg_query` expression node into an [`Expr`].
///
/// Node kinds not yet covered return [`LowerError::Unsupported`] naming the
/// construct, per the module's incremental-coverage plan — see the crate's
/// `lower` module docs.
pub fn lower_expr(node: &Node, ctx: &LowerCtx) -> Result<Expr, LowerError> {
    match node.node.as_ref() {
        Some(NodeEnum::AConst(ac)) => lower_a_const(ac),
        Some(NodeEnum::ColumnRef(cr)) => lower_column_ref(cr, ctx.columns),
        Some(NodeEnum::AExpr(ae)) => lower_a_expr(ae, ctx),
        Some(NodeEnum::BoolExpr(be)) => lower_bool_expr(be, ctx),
        Some(NodeEnum::NullTest(nt)) => lower_null_test(nt, ctx),
        Some(NodeEnum::BooleanTest(bt)) => lower_boolean_test(bt, ctx),
        Some(NodeEnum::CaseExpr(ce)) => lower_case_expr(ce, ctx),
        Some(NodeEnum::CoalesceExpr(coa)) => lower_coalesce_expr(coa, ctx),
        Some(other) => Err(LowerError::Unsupported(format!(
            "{} is not yet lowered",
            node_kind_name(other)
        ))),
        None => Err(LowerError::Malformed("empty expression node")),
    }
}

/// A short, human-readable name for an expression node kind, used only for
/// precise `Unsupported` messages. Covers the kinds this module's scope
/// names explicitly plus a handful of common others; anything else falls
/// back to a generic label rather than failing to compile an error message.
fn node_kind_name(n: &NodeEnum) -> &'static str {
    match n {
        NodeEnum::FuncCall(_) => "a function call",
        NodeEnum::TypeCast(_) => "an explicit cast",
        NodeEnum::SubLink(_) => "a subquery expression",
        NodeEnum::AArrayExpr(_) => "an ARRAY[...] literal",
        NodeEnum::RowExpr(_) => "a ROW(...) literal",
        NodeEnum::AIndirection(_) => "a subscript or field-access expression",
        NodeEnum::ParamRef(_) => "a bind parameter",
        NodeEnum::MinMaxExpr(_) => "GREATEST/LEAST",
        NodeEnum::SqlvalueFunction(_) => "a SQL value function (CURRENT_DATE and friends)",
        NodeEnum::GroupingFunc(_) => "GROUPING(...)",
        NodeEnum::WindowDef(_) => "a window definition",
        _ => "this expression node kind",
    }
}

// ─── A_Const ──────────────────────────────────────────────────────────────

/// Lower an `A_Const` node (already unwrapped from its `Node` envelope).
fn lower_a_const(ac: &AConst) -> Result<Expr, LowerError> {
    use pg_query::protobuf::a_const::Val;

    if ac.isnull {
        return Ok(Expr::null_unknown());
    }

    match ac.val.as_ref() {
        Some(Val::Ival(i)) => Ok(Expr::Literal(Datum::Int32(i.ival), PgType::INT4)),
        Some(Val::Fval(f)) => {
            // Postgres itself splits an over-long numeric literal between
            // int8 (pure digits too big for int4) and `numeric` (anything
            // with a decimal point or exponent). Basin's `Datum` has no
            // arbitrary-precision numeric representation yet, so the
            // decimal/exponent case is approximated as float8 rather than
            // invented as bytes of unspecified encoding — a documented
            // narrowing, not a silent one.
            if let Ok(i) = f.fval.parse::<i64>() {
                Ok(Expr::Literal(Datum::Int64(i), PgType::INT8))
            } else {
                let v: f64 = f
                    .fval
                    .parse()
                    .map_err(|_| LowerError::Malformed("unparseable float literal"))?;
                Ok(Expr::Literal(Datum::Float64(v), PgType::FLOAT8))
            }
        }
        Some(Val::Boolval(b)) => Ok(Expr::Literal(Datum::Bool(b.boolval), PgType::BOOL)),
        // A bare string literal is `unknown`, not `text` — Postgres resolves
        // it from context (`transformAConst`, `T_String -> UNKNOWNOID`), the
        // same rule as a bare `NULL`.
        Some(Val::Sval(s)) => Ok(Expr::Literal(Datum::Utf8(s.sval.clone()), PgType::UNKNOWN)),
        Some(Val::Bsval(_)) => Err(LowerError::Unsupported(
            "bit-string literals (B'...' / X'...') are not yet lowered".into(),
        )),
        None => Ok(Expr::null_unknown()),
    }
}

// ─── ColumnRef ────────────────────────────────────────────────────────────

/// Lower a `ColumnRef` node's dotted `fields` list to `Vec<String>`, or
/// `Unsupported` if it contains the `*` marker (valid only in a target list,
/// never inside a scalar expression).
fn column_ref_parts(cr: &pg_query::protobuf::ColumnRef) -> Result<Vec<String>, LowerError> {
    cr.fields
        .iter()
        .map(|f| match f.node.as_ref() {
            Some(NodeEnum::String(s)) => Ok(s.sval.clone()),
            Some(NodeEnum::AStar(_)) => Err(LowerError::Unsupported(
                "`*` is not valid in a scalar expression position".into(),
            )),
            _ => Err(LowerError::Malformed(
                "ColumnRef field is neither a name nor `*`",
            )),
        })
        .collect()
}

fn lower_column_ref(
    cr: &pg_query::protobuf::ColumnRef,
    columns: &dyn ColumnResolver,
) -> Result<Expr, LowerError> {
    let parts = column_ref_parts(cr)?;
    columns
        .resolve(&parts)
        .map(Expr::Column)
        .ok_or_else(|| LowerError::UnknownName(parts.join(".")))
}

// ─── A_Expr (operator forms) ────────────────────────────────────────────

/// Extract an operator's name. Schema-qualified spellings
/// (`OPERATOR(pg_catalog.~)`) carry more than one name part; only the final
/// segment is the operator's own name, matching `pg_operator.oprname`.
fn operator_name(name_nodes: &[Node]) -> Result<String, LowerError> {
    let last = name_nodes
        .last()
        .ok_or(LowerError::Malformed("operator with no name"))?;
    match last.node.as_ref() {
        Some(NodeEnum::String(s)) => Ok(s.sval.clone()),
        _ => Err(LowerError::Malformed("operator name is not a string")),
    }
}

/// The type used for operator resolution when the true type of an operand is
/// not known. Column types are not available in this increment — the
/// `ColumnResolver` seam returns a position, not a catalog type — so every
/// operand that isn't a literal, cast, or other self-typed expression
/// resolves as `unknown` and the `OperatorResolver` must cope with that, the
/// same way Postgres's own operator resolution copes with an `unknown`-typed
/// operand (deferring to the other side, or to a default).
fn best_effort_type(e: &Expr) -> PgType {
    match e {
        Expr::Literal(_, ty) | Expr::Parameter { ty, .. } => *ty,
        Expr::Cast { to, .. } => *to,
        _ => PgType::UNKNOWN,
    }
}

fn resolve_op(
    ctx: &LowerCtx,
    name: &str,
    left: Option<&Expr>,
    right: &Expr,
) -> Result<OpId, LowerError> {
    let left_ty = left.map(best_effort_type);
    let right_ty = best_effort_type(right);
    ctx.operators
        .resolve(name, left_ty, right_ty)
        .ok_or_else(|| {
            LowerError::NoMatchingOperator(format!(
                "no operator `{name}` for argument types ({left_ty:?}, {right_ty:?})"
            ))
        })
}

fn lower_a_expr(ae: &AExpr, ctx: &LowerCtx) -> Result<Expr, LowerError> {
    let kind = AExprKind::try_from(ae.kind).unwrap_or(AExprKind::Undefined);
    match kind {
        AExprKind::AexprOp => lower_aexpr_op(ae, ctx),
        AExprKind::AexprIn => lower_aexpr_in(ae, ctx),
        AExprKind::AexprLike | AExprKind::AexprIlike => lower_aexpr_like(ae, ctx, kind),
        AExprKind::AexprBetween
        | AExprKind::AexprNotBetween
        | AExprKind::AexprBetweenSym
        | AExprKind::AexprNotBetweenSym => lower_aexpr_between(ae, ctx, kind),
        AExprKind::AexprDistinct | AExprKind::AexprNotDistinct => {
            lower_aexpr_distinct(ae, ctx, kind)
        }
        _ => Err(LowerError::Unsupported(format!(
            "A_Expr kind {kind:?} is not yet lowered"
        ))),
    }
}

/// Lower both sides of a binary `A_Expr` (everything below `AEXPR_OP` is
/// binary-only — `IN`, `LIKE`, `BETWEEN`, `IS [NOT] DISTINCT FROM` have no
/// prefix form). Shared by the handlers below so each one only deals with
/// its own shape past this point.
fn lower_binary_operands(ae: &AExpr, ctx: &LowerCtx) -> Result<(Expr, Expr), LowerError> {
    let lhs_node = ae
        .lexpr
        .as_deref()
        .ok_or(LowerError::Malformed("A_Expr with no left operand"))?;
    let rhs_node = ae
        .rexpr
        .as_deref()
        .ok_or(LowerError::Malformed("A_Expr with no right operand"))?;
    Ok((lower_expr(lhs_node, ctx)?, lower_expr(rhs_node, ctx)?))
}

/// Lower a `List` node's items. Used for the RHS of `IN` and `BETWEEN`,
/// which Postgres always represents as a `List` of the individual values.
fn lower_list_items(node: &Node, ctx: &LowerCtx) -> Result<Vec<Expr>, LowerError> {
    match node.node.as_ref() {
        Some(NodeEnum::List(l)) => l.items.iter().map(|n| lower_expr(n, ctx)).collect(),
        _ => Err(LowerError::Malformed("expected a list of values")),
    }
}

/// `x [NOT] IN (v1, v2, ...)`.
///
/// Postgres represents `NOT IN` (the scalar-list form) as this same
/// `AEXPR_IN` kind with operator name `"<>"` rather than `"="` — a single
/// node, not `NOT` wrapping an `IN`. That is exactly what makes `NOT IN`
/// distinguishable from `NOT (x IN (...))` (a `BoolExpr` wrapping an
/// `AEXPR_IN` with name `"="`) at the parse-tree level, and both must stay
/// distinguishable through lowering: `x NOT IN (...)` becomes one `InList`
/// node with `negated: true`, while `NOT (x IN (...))` becomes
/// `Unary { op: NOT, arg: InList { negated: false, .. } }`.
fn lower_aexpr_in(ae: &AExpr, ctx: &LowerCtx) -> Result<Expr, LowerError> {
    let name = operator_name(&ae.name)?;
    let negated = match name.as_str() {
        "=" => false,
        "<>" => true,
        _ => return Err(LowerError::Malformed("AEXPR_IN with unexpected operator name")),
    };
    let lhs_node = ae
        .lexpr
        .as_deref()
        .ok_or(LowerError::Malformed("IN with no left operand"))?;
    let rhs_node = ae
        .rexpr
        .as_deref()
        .ok_or(LowerError::Malformed("IN with no right operand"))?;
    let arg = lower_expr(lhs_node, ctx)?;
    let list = lower_list_items(rhs_node, ctx)?;
    Ok(Expr::InList {
        arg: Box::new(arg),
        list,
        negated,
    })
}

/// `x [NOT] LIKE y [ESCAPE z]` / `x [NOT] ILIKE y [ESCAPE z]`.
///
/// Postgres desugars `LIKE ... ESCAPE ...` at parse time into a call to
/// `pg_catalog.like_escape(pattern, escape)` as the right operand, rather
/// than a distinct node shape — confirmed against a live parse, not assumed.
/// This unwraps that call back into `Expr::Like`'s separate `pattern` /
/// `escape` fields.
fn lower_aexpr_like(ae: &AExpr, ctx: &LowerCtx, kind: AExprKind) -> Result<Expr, LowerError> {
    let name = operator_name(&ae.name)?;
    let negated = name.starts_with('!');
    let case_insensitive = kind == AExprKind::AexprIlike;

    let lhs_node = ae
        .lexpr
        .as_deref()
        .ok_or(LowerError::Malformed("LIKE with no left operand"))?;
    let rhs_node = ae
        .rexpr
        .as_deref()
        .ok_or(LowerError::Malformed("LIKE with no right operand"))?;
    let arg = lower_expr(lhs_node, ctx)?;
    let (pattern, escape) = lower_like_rhs(rhs_node, ctx)?;

    Ok(Expr::Like {
        arg: Box::new(arg),
        pattern: Box::new(pattern),
        escape: escape.map(Box::new),
        case_insensitive,
        negated,
    })
}

/// Lower the right-hand side of a `LIKE`/`ILIKE`, splitting out an `ESCAPE`
/// clause if present.
///
/// Postgres desugars `x LIKE y ESCAPE z` at parse time into `x ~~
/// pg_catalog.like_escape(y, z)` — the escape is not a separate AST slot,
/// it's folded into a two-argument call on the right operand (confirmed
/// against a live parse of `x LIKE 'foo' ESCAPE '\'`, not assumed). This
/// must be detected on the raw `Node`, before general `FuncCall` lowering
/// exists, precisely because `like_escape` is not a function a real
/// `FunctionResolver` should ever be asked to resolve — it is a syntax
/// artifact, not a callable.
fn lower_like_rhs(node: &Node, ctx: &LowerCtx) -> Result<(Expr, Option<Expr>), LowerError> {
    if let Some(NodeEnum::FuncCall(fc)) = node.node.as_ref() {
        let is_like_escape = fc
            .funcname
            .last()
            .and_then(|n| n.node.as_ref())
            .is_some_and(|n| matches!(n, NodeEnum::String(s) if s.sval == "like_escape"));
        if is_like_escape {
            let [pattern_node, escape_node] = fc.args.as_slice() else {
                return Err(LowerError::Malformed(
                    "like_escape call with other than two arguments",
                ));
            };
            let pattern = lower_expr(pattern_node, ctx)?;
            let escape = lower_expr(escape_node, ctx)?;
            return Ok((pattern, Some(escape)));
        }
    }
    Ok((lower_expr(node, ctx)?, None))
}

/// `x BETWEEN [SYMMETRIC] low AND high`, and the `NOT` variants.
///
/// `AEXPR_BETWEEN_SYM`/`AEXPR_NOT_BETWEEN_SYM` are Postgres's own dedicated
/// kinds for the `SYMMETRIC` form (order of `low`/`high` doesn't matter); the
/// non-`SYM` kinds require `low <= high` as written. Confirmed against a live
/// parse of `x BETWEEN SYMMETRIC 1 AND 10` (`kind = 13`, `AEXPR_BETWEEN_SYM`).
fn lower_aexpr_between(ae: &AExpr, ctx: &LowerCtx, kind: AExprKind) -> Result<Expr, LowerError> {
    let (symmetric, negated) = match kind {
        AExprKind::AexprBetween => (false, false),
        AExprKind::AexprNotBetween => (false, true),
        AExprKind::AexprBetweenSym => (true, false),
        AExprKind::AexprNotBetweenSym => (true, true),
        _ => unreachable!("caller only dispatches BETWEEN kinds here"),
    };
    let lhs_node = ae
        .lexpr
        .as_deref()
        .ok_or(LowerError::Malformed("BETWEEN with no left operand"))?;
    let rhs_node = ae
        .rexpr
        .as_deref()
        .ok_or(LowerError::Malformed("BETWEEN with no right operand"))?;
    let arg = lower_expr(lhs_node, ctx)?;
    let mut bounds = lower_list_items(rhs_node, ctx)?;
    if bounds.len() != 2 {
        return Err(LowerError::Malformed(
            "BETWEEN with other than two bounds",
        ));
    }
    let high = bounds.pop().expect("checked len == 2");
    let low = bounds.pop().expect("checked len == 2");
    Ok(Expr::Between {
        arg: Box::new(arg),
        low: Box::new(low),
        high: Box::new(high),
        symmetric,
        negated,
    })
}

/// `x IS [NOT] DISTINCT FROM y` — null-safe (in)equality that never itself
/// returns NULL.
fn lower_aexpr_distinct(ae: &AExpr, ctx: &LowerCtx, kind: AExprKind) -> Result<Expr, LowerError> {
    let negated = kind == AExprKind::AexprNotDistinct;
    let (lhs, rhs) = lower_binary_operands(ae, ctx)?;
    Ok(Expr::DistinctFrom {
        lhs: Box::new(lhs),
        rhs: Box::new(rhs),
        negated,
    })
}

fn lower_aexpr_op(ae: &AExpr, ctx: &LowerCtx) -> Result<Expr, LowerError> {
    let name = operator_name(&ae.name)?;
    let rhs_node = ae
        .rexpr
        .as_deref()
        .ok_or(LowerError::Malformed("A_Expr with no right operand"))?;
    let rhs = lower_expr(rhs_node, ctx)?;

    match ae.lexpr.as_deref() {
        // No left operand: a prefix operator, e.g. unary `-x`.
        None => {
            let op = resolve_op(ctx, &name, None, &rhs)?;
            Ok(Expr::Unary {
                op,
                arg: Box::new(rhs),
            })
        }
        Some(lhs_node) => {
            let lhs = lower_expr(lhs_node, ctx)?;
            let op = resolve_op(ctx, &name, Some(&lhs), &rhs)?;
            Ok(Expr::Binary {
                op,
                lhs: Box::new(lhs),
                rhs: Box::new(rhs),
            })
        }
    }
}

// ─── BoolExpr (AND / OR / NOT) ───────────────────────────────────────────

fn lower_bool_expr(be: &BoolExpr, ctx: &LowerCtx) -> Result<Expr, LowerError> {
    let op_kind = BoolExprType::try_from(be.boolop).unwrap_or(BoolExprType::Undefined);
    let mut args = be
        .args
        .iter()
        .map(|n| lower_expr(n, ctx))
        .collect::<Result<Vec<_>, _>>()?;

    match op_kind {
        BoolExprType::NotExpr => {
            if args.len() != 1 {
                return Err(LowerError::Malformed("NOT with other than one argument"));
            }
            let arg = args.pop().expect("checked len == 1");
            let op = resolve_op(ctx, "NOT", None, &arg)?;
            Ok(Expr::Unary {
                op,
                arg: Box::new(arg),
            })
        }
        BoolExprType::AndExpr | BoolExprType::OrExpr => {
            if args.len() < 2 {
                return Err(LowerError::Malformed(
                    "AND/OR with fewer than two arguments",
                ));
            }
            let name = if op_kind == BoolExprType::AndExpr {
                "AND"
            } else {
                "OR"
            };
            // Postgres flattens a run of ANDs (or ORs) into one N-ary
            // BoolExpr; `Expr::Binary` is strictly two-ary, so fold left.
            let mut iter = args.into_iter();
            let mut acc = iter.next().expect("checked len >= 2");
            for next in iter {
                let op = resolve_op(ctx, name, Some(&acc), &next)?;
                acc = Expr::Binary {
                    op,
                    lhs: Box::new(acc),
                    rhs: Box::new(next),
                };
            }
            Ok(acc)
        }
        BoolExprType::Undefined => Err(LowerError::Malformed("BoolExpr with unknown boolop")),
    }
}

// ─── NullTest / BooleanTest ──────────────────────────────────────────────

fn lower_null_test(
    nt: &pg_query::protobuf::NullTest,
    ctx: &LowerCtx,
) -> Result<Expr, LowerError> {
    use pg_query::protobuf::NullTestType;

    if nt.argisrow {
        return Err(LowerError::Unsupported(
            "IS [NOT] NULL on a row value is not yet lowered".into(),
        ));
    }
    let arg_node = nt
        .arg
        .as_deref()
        .ok_or(LowerError::Malformed("NullTest with no argument"))?;
    let arg = lower_expr(arg_node, ctx)?;
    let negated = match NullTestType::try_from(nt.nulltesttype).unwrap_or(NullTestType::Undefined)
    {
        NullTestType::IsNull => false,
        NullTestType::IsNotNull => true,
        NullTestType::Undefined => {
            return Err(LowerError::Malformed("NullTest with unknown type"))
        }
    };
    Ok(Expr::IsNull {
        arg: Box::new(arg),
        negated,
    })
}

fn lower_boolean_test(
    bt: &pg_query::protobuf::BooleanTest,
    ctx: &LowerCtx,
) -> Result<Expr, LowerError> {
    use crate::expr::BoolTest;
    use pg_query::protobuf::BoolTestType;

    let arg_node = bt
        .arg
        .as_deref()
        .ok_or(LowerError::Malformed("BooleanTest with no argument"))?;
    let arg = lower_expr(arg_node, ctx)?;
    let test = match BoolTestType::try_from(bt.booltesttype).unwrap_or(BoolTestType::Undefined) {
        BoolTestType::IsTrue => BoolTest::IsTrue,
        BoolTestType::IsNotTrue => BoolTest::IsNotTrue,
        BoolTestType::IsFalse => BoolTest::IsFalse,
        BoolTestType::IsNotFalse => BoolTest::IsNotFalse,
        BoolTestType::IsUnknown => BoolTest::IsUnknown,
        BoolTestType::IsNotUnknown => BoolTest::IsNotUnknown,
        BoolTestType::Undefined => {
            return Err(LowerError::Malformed("BooleanTest with unknown type"))
        }
    };
    Ok(Expr::BoolTest {
        arg: Box::new(arg),
        test,
    })
}

// ─── CaseExpr / CoalesceExpr ──────────────────────────────────────────────

/// `CASE [operand] WHEN w1 THEN r1 [WHEN w2 THEN r2 ...] [ELSE e] END`.
///
/// For a "simple" CASE (`operand` present), each `WHEN` value is the raw
/// comparison value, not an already-expanded `operand = value` equality —
/// `Expr::Case` keeps `operand` as its own field precisely so that expansion
/// (which needs an `=` resolved per branch's type) is deferred rather than
/// done here without a real operator/type context per branch.
fn lower_case_expr(
    ce: &pg_query::protobuf::CaseExpr,
    ctx: &LowerCtx,
) -> Result<Expr, LowerError> {
    let operand = ce
        .arg
        .as_deref()
        .map(|n| lower_expr(n, ctx))
        .transpose()?
        .map(Box::new);

    let whens = ce
        .args
        .iter()
        .map(|w| {
            let Some(NodeEnum::CaseWhen(cw)) = w.node.as_ref() else {
                return Err(LowerError::Malformed("CASE arm is not a CaseWhen node"));
            };
            let cond_node = cw
                .expr
                .as_deref()
                .ok_or(LowerError::Malformed("CaseWhen with no condition/value"))?;
            let result_node = cw
                .result
                .as_deref()
                .ok_or(LowerError::Malformed("CaseWhen with no result"))?;
            Ok((lower_expr(cond_node, ctx)?, lower_expr(result_node, ctx)?))
        })
        .collect::<Result<Vec<_>, LowerError>>()?;

    let else_ = ce
        .defresult
        .as_deref()
        .map(|n| lower_expr(n, ctx))
        .transpose()?
        .map(Box::new);

    Ok(Expr::Case {
        operand,
        whens,
        else_,
    })
}

/// `COALESCE(e1, e2, ...)`.
fn lower_coalesce_expr(
    coa: &pg_query::protobuf::CoalesceExpr,
    ctx: &LowerCtx,
) -> Result<Expr, LowerError> {
    if coa.args.is_empty() {
        return Err(LowerError::Malformed("COALESCE with no arguments"));
    }
    let args = coa
        .args
        .iter()
        .map(|n| lower_expr(n, ctx))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Expr::Coalesce(args))
}

// ─── Tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// A column resolver over one fixed, ordered relation schema, keyed by
    /// bare (last-segment) name — enough to exercise dotted references like
    /// `t.a` without a real catalog.
    struct MockColumns {
        names: Vec<&'static str>,
    }

    impl ColumnResolver for MockColumns {
        fn resolve(&self, parts: &[String]) -> Option<ColumnRef> {
            let last = parts.last()?;
            let idx = self.names.iter().position(|n| n == last)?;
            Some(ColumnRef {
                relation: 0,
                index: idx as u16,
                name: last.clone(),
            })
        }
    }

    /// An operator resolver backed by the real `pg_operator` table in
    /// `basin_pgtype::operator`, plus the synthetic `AND`/`OR`/`NOT` boolean
    /// connectives that table deliberately does not carry (see the module
    /// docs). This is what a real implementation of `OperatorResolver` looks
    /// like — the trait exists so today's mock and tomorrow's real catalog
    /// are interchangeable.
    struct CatalogOperators;

    impl OperatorResolver for CatalogOperators {
        fn resolve(&self, name: &str, left: Option<PgType>, right: PgType) -> Option<OpId> {
            match name {
                "AND" | "OR" | "NOT" => Some(OpId(Oid(u32::MAX))),
                _ => {
                    let left_oid = left.map(|t| t.oid);
                    basin_pgtype::operator::resolve(name, left_oid, right.oid)
                        .map(|sig| OpId(sig.oid))
                }
            }
        }
    }

    use basin_pgtype::Oid;

    fn ctx<'a>(columns: &'a MockColumns, operators: &'a CatalogOperators) -> LowerCtx<'a> {
        LowerCtx { columns, operators }
    }

    fn cols() -> MockColumns {
        MockColumns {
            names: vec!["id", "a", "b"],
        }
    }

    fn parse_expr(sql: &str) -> Node {
        let stmt_sql = format!("SELECT {sql}");
        let result = pg_query::parse(&stmt_sql).expect("parse failed");
        let raw = result.protobuf.stmts.first().expect("no stmt").clone();
        let stmt = raw.stmt.expect("no stmt node");
        let NodeEnum::SelectStmt(select) = stmt.node.expect("empty node") else {
            panic!("not a SELECT");
        };
        let target = select.target_list.first().expect("no target").clone();
        let NodeEnum::ResTarget(rt) = target.node.expect("empty target node") else {
            panic!("not a ResTarget");
        };
        *rt.val.expect("no val")
    }

    fn where_expr(sql_where: &str) -> Node {
        let stmt_sql = format!("SELECT 1 FROM t WHERE {sql_where}");
        let result = pg_query::parse(&stmt_sql).expect("parse failed");
        let raw = result.protobuf.stmts.first().expect("no stmt").clone();
        let stmt = raw.stmt.expect("no stmt node");
        let NodeEnum::SelectStmt(select) = stmt.node.expect("empty node") else {
            panic!("not a SELECT");
        };
        *select.where_clause.expect("no WHERE")
    }

    // --- A_Const -----------------------------------------------------------

    #[test]
    fn bare_null_lowers_to_unknown() {
        let node = parse_expr("NULL");
        let c = cols();
        let o = CatalogOperators;
        let e = lower_expr(&node, &ctx(&c, &o)).expect("lower failed");
        let Expr::Literal(Datum::Null, ty) = e else {
            panic!("expected a NULL literal, got {e:?}");
        };
        assert!(ty.is_unknown(), "bare NULL must lower to PgType::UNKNOWN");
    }

    #[test]
    fn integer_literal_lowers_to_int4() {
        let node = parse_expr("42");
        let c = cols();
        let o = CatalogOperators;
        let e = lower_expr(&node, &ctx(&c, &o)).expect("lower failed");
        assert_eq!(e, Expr::Literal(Datum::Int32(42), PgType::INT4));
    }

    #[test]
    fn boolean_literal_lowers_to_bool() {
        let node = parse_expr("TRUE");
        let c = cols();
        let o = CatalogOperators;
        let e = lower_expr(&node, &ctx(&c, &o)).expect("lower failed");
        assert_eq!(e, Expr::Literal(Datum::Bool(true), PgType::BOOL));
    }

    /// A bare string literal is `unknown`, exactly like NULL — Postgres
    /// resolves it from context. Typing it `TEXT` up front would be a
    /// fidelity bug, not a simplification.
    #[test]
    fn string_literal_lowers_to_unknown_not_text() {
        let node = parse_expr("'hello'");
        let c = cols();
        let o = CatalogOperators;
        let e = lower_expr(&node, &ctx(&c, &o)).expect("lower failed");
        let Expr::Literal(Datum::Utf8(s), ty) = e else {
            panic!("expected a string literal, got {e:?}");
        };
        assert_eq!(s, "hello");
        assert!(
            ty.is_unknown(),
            "a bare string literal must stay unknown until context resolves it"
        );
    }

    #[test]
    fn oversize_integer_literal_lowers_to_int8() {
        // 9999999999 overflows int4 but fits int64 — Postgres's own
        // transformAConst picks int8 here, not numeric.
        let node = parse_expr("9999999999");
        let c = cols();
        let o = CatalogOperators;
        let e = lower_expr(&node, &ctx(&c, &o)).expect("lower failed");
        assert_eq!(e, Expr::Literal(Datum::Int64(9999999999), PgType::INT8));
    }

    // --- ColumnRef -----------------------------------------------------------

    #[test]
    fn column_ref_resolves_through_the_resolver() {
        let node = parse_expr("a");
        let c = cols();
        let o = CatalogOperators;
        let e = lower_expr(&node, &ctx(&c, &o)).expect("lower failed");
        assert_eq!(
            e,
            Expr::Column(ColumnRef {
                relation: 0,
                index: 1,
                name: "a".into(),
            })
        );
    }

    #[test]
    fn unresolvable_column_is_an_unknown_name_error() {
        let node = parse_expr("nope");
        let c = cols();
        let o = CatalogOperators;
        let err = lower_expr(&node, &ctx(&c, &o)).expect_err("should not resolve");
        assert!(matches!(err, LowerError::UnknownName(_)));
    }

    #[test]
    fn star_outside_a_target_list_is_unsupported() {
        // `count(*)` parses its `*` specially inside FuncCall (agg_star), so
        // the only way a bare A_Star reaches ColumnRef lowering directly is
        // a malformed/edge construct; build the node by hand.
        let cr = pg_query::protobuf::ColumnRef {
            fields: vec![Node {
                node: Some(NodeEnum::AStar(pg_query::protobuf::AStar {})),
            }],
            location: 0,
        };
        let node = Node {
            node: Some(NodeEnum::ColumnRef(cr)),
        };
        let c = cols();
        let o = CatalogOperators;
        let err = lower_expr(&node, &ctx(&c, &o)).expect_err("* must be unsupported here");
        assert!(matches!(err, LowerError::Unsupported(_)));
    }

    // --- A_Expr / OperatorResolver -------------------------------------------

    /// The test named explicitly in the task: `a ~ 'x'` must resolve through
    /// the operator catalog to a real `OpId`, never through string rewriting
    /// — the entire reason `pg_operators.rs` is being deleted.
    #[test]
    fn regex_match_resolves_through_the_operator_catalog() {
        let node = where_expr("a ~ 'x'");
        let c = cols();
        let o = CatalogOperators;
        let e = lower_expr(&node, &ctx(&c, &o)).expect("lower failed");
        let Expr::Binary { op, lhs, rhs } = e else {
            panic!("expected a Binary expr, got {e:?}");
        };
        // `a`'s type is unknown at this layer (no catalog for columns yet),
        // and `'x'` is a bare string literal — also unknown. The `~`
        // operator only has a text/text row, so resolution here depends on
        // `basin_pgtype::operator::resolve`'s implicit-coercion fallback
        // treating `unknown` as coercible — which it does, the same way
        // Postgres treats an unknown-typed literal as coercible to anything.
        assert_eq!(op, OpId(Oid(641)), "must resolve to the real `~` oid");
        assert!(matches!(*lhs, Expr::Column(_)));
        assert!(matches!(*rhs, Expr::Literal(Datum::Utf8(_), _)));
    }

    #[test]
    fn unary_minus_resolves_as_a_prefix_operator() {
        let c = cols();
        let o = CatalogOperators;

        let typed_lhs = Node {
            node: Some(NodeEnum::AExpr(Box::new(AExpr {
                kind: AExprKind::AexprOp as i32,
                name: vec![Node {
                    node: Some(NodeEnum::String(pg_query::protobuf::String {
                        sval: "-".into(),
                    })),
                }],
                lexpr: None,
                rexpr: Some(Box::new(Node {
                    node: Some(NodeEnum::AConst(AConst {
                        isnull: false,
                        location: 0,
                        val: Some(pg_query::protobuf::a_const::Val::Ival(
                            pg_query::protobuf::Integer { ival: 5 },
                        )),
                    })),
                })),
                location: 0,
            }))),
        };
        let e = lower_expr(&typed_lhs, &ctx(&c, &o)).expect("lower failed");
        let Expr::Unary { op, arg } = e else {
            panic!("expected a Unary expr, got {e:?}");
        };
        assert_eq!(op, OpId(Oid(558)), "must resolve to the real prefix - oid");
        assert_eq!(*arg, Expr::Literal(Datum::Int32(5), PgType::INT4));
    }

    #[test]
    fn no_matching_operator_is_reported_precisely() {
        // `<=>` is not a name any row in `basin_pgtype::operator` carries —
        // syntactically valid (Postgres allows arbitrary operator-character
        // sequences as a name), semantically nonexistent, same as
        // `operator::tests::unknown_operator_name_does_not_resolve`.
        let node = where_expr("a <=> b");
        let c = cols();
        let o = CatalogOperators;
        let err = lower_expr(&node, &ctx(&c, &o)).expect_err("<=> must not resolve");
        assert!(matches!(err, LowerError::NoMatchingOperator(_)));
    }

    // --- BoolExpr (AND / OR / NOT) --------------------------------------------

    #[test]
    fn and_or_not_lower_through_the_operator_resolver() {
        let node = where_expr("NOT (a IS NULL AND b IS NULL)");
        let c = cols();
        let o = CatalogOperators;
        let e = lower_expr(&node, &ctx(&c, &o)).expect("lower failed");
        let Expr::Unary { op, arg } = e else {
            panic!("expected NOT to lower to Unary, got {e:?}");
        };
        assert_eq!(op, OpId(Oid(u32::MAX)));
        let Expr::Binary { op, .. } = *arg else {
            panic!("expected AND to lower to Binary");
        };
        assert_eq!(op, OpId(Oid(u32::MAX)));
    }

    #[test]
    fn three_way_or_folds_left_into_binary_pairs() {
        let node = where_expr("a IS NULL OR b IS NULL OR id IS NULL");
        let c = cols();
        let o = CatalogOperators;
        let e = lower_expr(&node, &ctx(&c, &o)).expect("lower failed");
        // Outer node is the fold of the third OR onto the first two.
        let Expr::Binary { lhs, .. } = e else {
            panic!("expected Binary");
        };
        assert!(
            matches!(*lhs, Expr::Binary { .. }),
            "three ORs must fold into nested binaries, not a 3-ary node"
        );
    }

    // --- NullTest / BooleanTest ------------------------------------------------

    #[test]
    fn is_null_and_is_not_null_set_negated_correctly() {
        let c = cols();
        let o = CatalogOperators;

        let e = lower_expr(&where_expr("a IS NULL"), &ctx(&c, &o)).unwrap();
        assert_eq!(
            e,
            Expr::IsNull {
                arg: Box::new(Expr::Column(ColumnRef {
                    relation: 0,
                    index: 1,
                    name: "a".into()
                })),
                negated: false,
            }
        );

        let e = lower_expr(&where_expr("a IS NOT NULL"), &ctx(&c, &o)).unwrap();
        let Expr::IsNull { negated, .. } = e else {
            panic!("expected IsNull");
        };
        assert!(negated);
    }

    #[test]
    fn boolean_tests_cover_all_six_forms() {
        use crate::expr::BoolTest;
        let c = cols();
        let o = CatalogOperators;
        let cases = [
            ("a IS TRUE", BoolTest::IsTrue),
            ("a IS NOT TRUE", BoolTest::IsNotTrue),
            ("a IS FALSE", BoolTest::IsFalse),
            ("a IS NOT FALSE", BoolTest::IsNotFalse),
            ("a IS UNKNOWN", BoolTest::IsUnknown),
            ("a IS NOT UNKNOWN", BoolTest::IsNotUnknown),
        ];
        for (sql, expected) in cases {
            let e = lower_expr(&where_expr(sql), &ctx(&c, &o)).unwrap_or_else(|e| {
                panic!("{sql} failed to lower: {e:?}");
            });
            let Expr::BoolTest { test, .. } = e else {
                panic!("{sql}: expected BoolTest, got {e:?}");
            };
            assert_eq!(test, expected, "{sql}");
        }
    }

    // --- Unsupported fallback --------------------------------------------------

    #[test]
    fn an_unhandled_node_kind_is_unsupported_with_a_precise_message() {
        // FuncCall lands in a later stage; still unhandled at this point in
        // the file's incremental build-out.
        let node = parse_expr("foo(a)");
        let c = cols();
        let o = CatalogOperators;
        let err = lower_expr(&node, &ctx(&c, &o)).expect_err("FuncCall not yet lowered");
        let LowerError::Unsupported(msg) = err else {
            panic!("expected Unsupported");
        };
        assert!(
            msg.contains("function call"),
            "message should name the construct: {msg}"
        );
    }

    // --- CaseExpr / CoalesceExpr ------------------------------------------------

    #[test]
    fn searched_case_lowers_whens_and_else() {
        let node = parse_expr("CASE WHEN a IS NULL THEN id ELSE b END");
        let c = cols();
        let o = CatalogOperators;
        let e = lower_expr(&node, &ctx(&c, &o)).expect("lower failed");
        let Expr::Case {
            operand,
            whens,
            else_,
        } = e
        else {
            panic!("expected Case");
        };
        assert!(operand.is_none(), "searched CASE has no operand");
        assert_eq!(whens.len(), 1);
        assert!(matches!(whens[0].0, Expr::IsNull { .. }));
        assert_eq!(
            whens[0].1,
            Expr::Column(ColumnRef {
                relation: 0,
                index: 0,
                name: "id".into()
            })
        );
        assert!(else_.is_some());
    }

    #[test]
    fn simple_case_keeps_operand_separate_from_when_values() {
        let node = parse_expr("CASE a WHEN 1 THEN id END");
        let c = cols();
        let o = CatalogOperators;
        let e = lower_expr(&node, &ctx(&c, &o)).expect("lower failed");
        let Expr::Case {
            operand, whens, ..
        } = e
        else {
            panic!("expected Case");
        };
        assert_eq!(
            *operand.expect("simple CASE has an operand"),
            Expr::Column(ColumnRef {
                relation: 0,
                index: 1,
                name: "a".into()
            })
        );
        // The WHEN value is the raw literal `1`, not an already-expanded
        // `a = 1` equality — expansion is deferred past this file.
        assert_eq!(whens[0].0, Expr::Literal(Datum::Int32(1), PgType::INT4));
    }

    #[test]
    fn coalesce_lowers_every_argument() {
        let node = parse_expr("COALESCE(a, b, id)");
        let c = cols();
        let o = CatalogOperators;
        let e = lower_expr(&node, &ctx(&c, &o)).expect("lower failed");
        let Expr::Coalesce(args) = e else {
            panic!("expected Coalesce");
        };
        assert_eq!(args.len(), 3);
    }

    // --- IN / NOT IN -------------------------------------------------------------

    #[test]
    fn in_list_lowers_with_negated_false() {
        let node = where_expr("a IN (1, 2, 3)");
        let c = cols();
        let o = CatalogOperators;
        let e = lower_expr(&node, &ctx(&c, &o)).expect("lower failed");
        let Expr::InList { list, negated, .. } = e else {
            panic!("expected InList");
        };
        assert!(!negated);
        assert_eq!(list.len(), 3);
    }

    /// The test the task calls out explicitly: `x NOT IN (...)` and
    /// `NOT (x IN (...))` must lower to different shapes, because Postgres
    /// itself gives them different parse trees (`AEXPR_IN` with operator
    /// name `"<>"` vs. a `BoolExpr` wrapping `AEXPR_IN` with `"="`), and
    /// collapsing them would be exactly the kind of fidelity bug this
    /// lowering pass exists to avoid — `NOT (x IN (...))` and `x NOT IN
    /// (...)` agree on every ordinary row but diverge when the list contains
    /// a NULL and `x` matches none of the non-NULL entries.
    #[test]
    fn not_in_is_distinguishable_from_not_wrapping_in() {
        let c = cols();
        let o = CatalogOperators;

        // `x NOT IN (...)`: one InList node, negated: true.
        let not_in = lower_expr(&where_expr("a NOT IN (1, 2)"), &ctx(&c, &o)).unwrap();
        let Expr::InList { negated, .. } = not_in else {
            panic!("expected InList for `NOT IN`, got {not_in:?}");
        };
        assert!(negated, "`x NOT IN (...)` must set InList.negated");

        // `NOT (x IN (...))`: Unary(NOT) wrapping an InList with negated: false.
        let not_wrapping_in =
            lower_expr(&where_expr("NOT (a IN (1, 2))"), &ctx(&c, &o)).unwrap();
        let Expr::Unary { arg, .. } = not_wrapping_in else {
            panic!(
                "expected Unary(NOT) for `NOT(IN)`, got {not_wrapping_in:?}"
            );
        };
        let Expr::InList { negated, .. } = *arg else {
            panic!("expected the NOT to wrap an InList");
        };
        assert!(
            !negated,
            "the inner IN must NOT itself be negated — the NOT is a separate node"
        );
    }

    // --- LIKE / ILIKE --------------------------------------------------------

    #[test]
    fn like_lowers_without_escape() {
        let node = where_expr("a LIKE 'foo%'");
        let c = cols();
        let o = CatalogOperators;
        let e = lower_expr(&node, &ctx(&c, &o)).expect("lower failed");
        let Expr::Like {
            escape,
            case_insensitive,
            negated,
            ..
        } = e
        else {
            panic!("expected Like");
        };
        assert!(escape.is_none());
        assert!(!case_insensitive);
        assert!(!negated);
    }

    #[test]
    fn like_escape_splits_pattern_and_escape() {
        let node = where_expr(r"a LIKE 'foo\%' ESCAPE '\'");
        let c = cols();
        let o = CatalogOperators;
        let e = lower_expr(&node, &ctx(&c, &o)).expect("lower failed");
        let Expr::Like {
            pattern, escape, ..
        } = e
        else {
            panic!("expected Like");
        };
        assert!(matches!(*pattern, Expr::Literal(Datum::Utf8(_), _)));
        let escape = escape.expect("ESCAPE clause must be captured");
        assert!(matches!(*escape, Expr::Literal(Datum::Utf8(_), _)));
    }

    #[test]
    fn not_like_and_ilike_set_negated_and_case_insensitive() {
        let c = cols();
        let o = CatalogOperators;

        let e = lower_expr(&where_expr("a NOT LIKE 'x'"), &ctx(&c, &o)).unwrap();
        let Expr::Like {
            negated,
            case_insensitive,
            ..
        } = e
        else {
            panic!("expected Like");
        };
        assert!(negated);
        assert!(!case_insensitive);

        let e = lower_expr(&where_expr("a ILIKE 'x'"), &ctx(&c, &o)).unwrap();
        let Expr::Like {
            negated,
            case_insensitive,
            ..
        } = e
        else {
            panic!("expected Like");
        };
        assert!(!negated);
        assert!(case_insensitive);
    }

    // --- BETWEEN -------------------------------------------------------------

    /// The test the task calls out explicitly: `BETWEEN SYMMETRIC` must set
    /// `symmetric: true` (order of bounds doesn't matter), distinct from
    /// plain `BETWEEN`.
    #[test]
    fn between_symmetric_sets_the_symmetric_flag() {
        let c = cols();
        let o = CatalogOperators;

        let e = lower_expr(&where_expr("a BETWEEN 1 AND 10"), &ctx(&c, &o)).unwrap();
        let Expr::Between {
            symmetric,
            negated,
            low,
            high,
            ..
        } = e
        else {
            panic!("expected Between");
        };
        assert!(!symmetric);
        assert!(!negated);
        assert_eq!(*low, Expr::Literal(Datum::Int32(1), PgType::INT4));
        assert_eq!(*high, Expr::Literal(Datum::Int32(10), PgType::INT4));

        let e = lower_expr(
            &where_expr("a BETWEEN SYMMETRIC 1 AND 10"),
            &ctx(&c, &o),
        )
        .unwrap();
        let Expr::Between { symmetric, .. } = e else {
            panic!("expected Between");
        };
        assert!(symmetric, "BETWEEN SYMMETRIC must set symmetric: true");

        let e = lower_expr(
            &where_expr("a NOT BETWEEN SYMMETRIC 1 AND 10"),
            &ctx(&c, &o),
        )
        .unwrap();
        let Expr::Between {
            symmetric, negated, ..
        } = e
        else {
            panic!("expected Between");
        };
        assert!(symmetric);
        assert!(negated);
    }

    // --- IS [NOT] DISTINCT FROM -----------------------------------------------

    #[test]
    fn distinct_from_and_not_distinct_from_set_negated_correctly() {
        let c = cols();
        let o = CatalogOperators;

        let e = lower_expr(&where_expr("a IS DISTINCT FROM b"), &ctx(&c, &o)).unwrap();
        let Expr::DistinctFrom { negated, .. } = e else {
            panic!("expected DistinctFrom");
        };
        assert!(!negated);

        let e = lower_expr(
            &where_expr("a IS NOT DISTINCT FROM b"),
            &ctx(&c, &o),
        )
        .unwrap();
        let Expr::DistinctFrom { negated, .. } = e else {
            panic!("expected DistinctFrom");
        };
        assert!(negated);
    }
}
