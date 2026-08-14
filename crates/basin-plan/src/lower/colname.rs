//! PostgreSQL's default output column name for an unaliased target-list entry.
//!
//! This is a faithful port of `FigureColname` / `FigureColnameInternal` in
//! PostgreSQL's `src/backend/parser/parse_target.c`, and it is deliberately a
//! function of the **raw parse tree**, not of the lowered [`crate::Expr`].
//! That is not an implementation convenience — it is where PostgreSQL itself
//! computes the name (`transformTargetEntry` calls `FigureColname(node)` on
//! the untransformed `ResTarget->val`), and it is the only place the
//! information survives: by the time lowering has run, `sum(x)` and
//! `upper(x)` are both an OID-carrying node (`Expr::Aggregate` /
//! `Expr::ScalarFn` with a [`crate::expr::FuncId`]) with no name attached, so
//! recovering `sum` from the plan would need `pg_proc` — while the parse tree
//! has had the name in hand the whole time.
//!
//! Naming these columns correctly is wire-visible, not cosmetic: the name
//! ends up in `RowDescription`, and a client that reads `row["count"]` breaks
//! when the column comes back as `?column?`.
//!
//! # The rule
//!
//! Each node yields a name and a *strength*:
//!
//! - **2 (strong)** — a name the node owns outright. A bare column reference
//!   (its own name), a function call (the last component of the function
//!   name, so `pg_catalog.upper(x)` is `upper` and `count(*)` is `count`),
//!   and the fixed-name constructs: `coalesce`, `nullif`, `greatest`/`least`,
//!   `array`, `row`, `exists`, `grouping`, the `current_date` family, the
//!   XML/JSON constructors.
//! - **1 (weak)** — a fallback name that a *wrapping* node is allowed to
//!   override. Only `CASE` (`case`) and a cast (the target type name)
//!   produce one.
//! - **0 (none)** — no name at all, which the caller renders as `?column?`.
//!
//! The strength is what makes casts behave the way they actually do:
//! `a::text` is named `a` (the column's strong name survives the cast), while
//! `(a + b)::text` is named `text` (the operator underneath has no name to
//! keep, so the type name fills in). Verified against PostgreSQL 18.2 — see
//! the tests, which record the server's answer per case.

use pg_query::protobuf::{
    node::Node as NodeEnum, AExprKind, MinMaxOp, Node, SqlValueFunctionOp, SubLinkType, XmlExprOp,
};

/// PostgreSQL's fallback name for a target-list entry it cannot name.
pub(crate) const UNNAMED: &str = "?column?";

/// The default column name PostgreSQL would give the unaliased target-list
/// entry `node`, or `None` when it would fall back to [`UNNAMED`].
pub(crate) fn figure_colname(node: &Node) -> Option<String> {
    let mut name = None;
    figure(node, &mut name);
    name
}

/// [`figure_colname`]'s [`UNNAMED`]-defaulting form, for the call sites that
/// want a `String` straight away.
pub(crate) fn figure_colname_or_unnamed(node: &Node) -> String {
    figure_colname(node).unwrap_or_else(|| UNNAMED.to_string())
}

/// `FigureColnameInternal`: writes the name into `out` and returns its
/// strength (see the module docs). A returned strength of 0 leaves `out`
/// untouched.
fn figure(node: &Node, out: &mut Option<String>) -> u8 {
    let Some(inner) = node.node.as_ref() else {
        return 0;
    };
    match inner {
        // `t.a` -> `a`; a trailing `*` has no name of its own (it is expanded
        // into one entry per column before this ever runs, each of which is a
        // plain column reference).
        NodeEnum::ColumnRef(cr) => match cr.fields.last().and_then(|f| f.node.as_ref()) {
            Some(NodeEnum::String(s)) => strong(out, &s.sval),
            _ => 0,
        },

        // `x[1]`, `(x).f`: the *last field name* anywhere in the indirection
        // wins, so `(x).f[2]` is `f`. With no field name at all (pure
        // subscripting), the name passes through from what is being
        // subscripted — `e[1]` over a column `e` is named `e`.
        NodeEnum::AIndirection(ind) => {
            let last_field = ind
                .indirection
                .iter()
                .filter_map(|i| match i.node.as_ref() {
                    Some(NodeEnum::String(s)) => Some(s.sval.as_str()),
                    _ => None,
                })
                .next_back();
            match (last_field, ind.arg.as_deref()) {
                (Some(f), _) => strong(out, f),
                (None, Some(arg)) => figure(arg, out),
                (None, None) => 0,
            }
        }

        // `count(*)` -> `count`, `pg_catalog.upper(x)` -> `upper`. The
        // grammar has already rewritten the special-syntax functions into
        // ordinary calls by this point, which is why `trim(x)` is named
        // `btrim` and `substring(x from 1)` is named `substring`.
        NodeEnum::FuncCall(fc) => match fc.funcname.last().and_then(|f| f.node.as_ref()) {
            Some(NodeEnum::String(s)) => strong(out, &s.sval),
            _ => 0,
        },

        // Of all the `A_Expr` kinds only NULLIF is named; every operator
        // expression (`a + b`, `a = b`, `a IS DISTINCT FROM b`, `a LIKE b`,
        // `a BETWEEN x AND y`) is `?column?`.
        NodeEnum::AExpr(e) => {
            if e.kind == AExprKind::AexprNullif as i32 {
                strong(out, "nullif")
            } else {
                0
            }
        }

        // The strength rule in its load-bearing form: a cast keeps a strong
        // name from underneath it (`a::text` is `a`) and otherwise supplies
        // the target type's own last name component, weakly — which is the
        // *internal* name, so `::bigint` is `int8` and `::double precision`
        // is `float8`. A typmod is not part of it: `::varchar(3)` is
        // `varchar`.
        NodeEnum::TypeCast(tc) => {
            let inner_strength = match tc.arg.as_deref() {
                Some(arg) => figure(arg, out),
                None => 0,
            };
            if inner_strength > 1 {
                return inner_strength;
            }
            match tc
                .type_name
                .as_ref()
                .and_then(|tn| tn.names.last())
                .and_then(|n| n.node.as_ref())
            {
                Some(NodeEnum::String(s)) => weak(out, &s.sval),
                _ => inner_strength,
            }
        }

        // A collation is invisible to naming: `a COLLATE "C"` is still `a`.
        NodeEnum::CollateClause(c) => match c.arg.as_deref() {
            Some(arg) => figure(arg, out),
            None => 0,
        },

        NodeEnum::SubLink(sl) => {
            if sl.sub_link_type == SubLinkType::ExistsSublink as i32 {
                return strong(out, "exists");
            }
            if sl.sub_link_type == SubLinkType::ArraySublink as i32 {
                return strong(out, "array");
            }
            if sl.sub_link_type != SubLinkType::ExprSublink as i32 {
                return 0;
            }
            // A scalar subquery is named by its own single target-list entry:
            // `(SELECT max(a) FROM t)` is `max` and `(SELECT a AS z ...)` is
            // `z`, while `(SELECT a + b ...)` — which the inner list cannot
            // name either — stays `?column?`.
            match sublink_first_target(sl) {
                Some(name) => strong(out, &name),
                None => 0,
            }
        }

        // Only the ELSE branch is consulted, and only for a strong name:
        // `CASE WHEN … THEN 1 ELSE b END` is named `b`, but
        // `CASE WHEN … THEN 1 ELSE 2 END` (and any CASE without an ELSE) is
        // the weak `case`, which a wrapping cast may still override.
        NodeEnum::CaseExpr(ce) => {
            let inner_strength = match ce.defresult.as_deref() {
                Some(d) => figure(d, out),
                None => 0,
            };
            if inner_strength > 1 {
                return inner_strength;
            }
            weak(out, "case")
        }

        NodeEnum::AArrayExpr(_) => strong(out, "array"),
        NodeEnum::RowExpr(_) => strong(out, "row"),
        NodeEnum::CoalesceExpr(_) => strong(out, "coalesce"),
        NodeEnum::MinMaxExpr(m) => {
            if m.op == MinMaxOp::IsLeast as i32 {
                strong(out, "least")
            } else {
                strong(out, "greatest")
            }
        }
        NodeEnum::SqlvalueFunction(f) => match sql_value_function_name(f.op) {
            Some(n) => strong(out, n),
            None => 0,
        },
        NodeEnum::XmlExpr(x) => match xml_expr_name(x.op) {
            Some(n) => strong(out, n),
            None => 0,
        },
        NodeEnum::XmlSerialize(_) => strong(out, "xmlserialize"),
        NodeEnum::GroupingFunc(_) => strong(out, "grouping"),
        NodeEnum::MergeSupportFunc(_) => strong(out, "merge_action"),
        NodeEnum::JsonObjectConstructor(_) => strong(out, "json_object"),
        NodeEnum::JsonArrayConstructor(_) | NodeEnum::JsonArrayQueryConstructor(_) => {
            strong(out, "json_array")
        }
        NodeEnum::JsonObjectAgg(_) => strong(out, "json_objectagg"),
        NodeEnum::JsonArrayAgg(_) => strong(out, "json_arrayagg"),

        // Everything else — a literal most of all (`SELECT 1`), plus every
        // node shape not listed above — has no name. `?column?` is not a
        // guess here, it is what PostgreSQL prints.
        _ => 0,
    }
}

fn strong(out: &mut Option<String>, name: &str) -> u8 {
    *out = Some(name.to_string());
    2
}

fn weak(out: &mut Option<String>, name: &str) -> u8 {
    *out = Some(name.to_string());
    1
}

/// The name the first target-list entry of a scalar subquery exposes — its
/// explicit alias if it has one, else whatever [`figure`] makes of it.
///
/// PostgreSQL reads this off the already-transformed `Query` hanging under
/// the `SubLink`; Basin is looking at the raw `SelectStmt`, so it reads the
/// same entry from the raw target list instead. The answer is identical
/// because `ResTarget.name` is exactly the `AS` alias that becomes
/// `TargetEntry.resname`, and when there is no alias PostgreSQL derived
/// `resname` by running this very function on that entry.
fn sublink_first_target(sl: &pg_query::protobuf::SubLink) -> Option<String> {
    let Some(NodeEnum::SelectStmt(sel)) = sl.subselect.as_deref().and_then(|n| n.node.as_ref())
    else {
        return None;
    };
    let Some(NodeEnum::ResTarget(rt)) = sel.target_list.first().and_then(|n| n.node.as_ref()) else {
        return None;
    };
    if !rt.name.is_empty() {
        return Some(rt.name.clone());
    }
    figure_colname(rt.val.as_deref()?)
}

/// `SELECT current_date` is named `current_date` — these keywords parse to a
/// dedicated node rather than a function call, so they need their names
/// spelled out. The `_N` variants are the precision-taking spellings
/// (`current_time(3)`), which carry the same name as their bare form.
fn sql_value_function_name(op: i32) -> Option<&'static str> {
    use SqlValueFunctionOp::*;
    Some(match op {
        x if x == SvfopCurrentDate as i32 => "current_date",
        x if x == SvfopCurrentTime as i32 || x == SvfopCurrentTimeN as i32 => "current_time",
        x if x == SvfopCurrentTimestamp as i32 || x == SvfopCurrentTimestampN as i32 => {
            "current_timestamp"
        }
        x if x == SvfopLocaltime as i32 || x == SvfopLocaltimeN as i32 => "localtime",
        x if x == SvfopLocaltimestamp as i32 || x == SvfopLocaltimestampN as i32 => {
            "localtimestamp"
        }
        x if x == SvfopCurrentRole as i32 => "current_role",
        x if x == SvfopCurrentUser as i32 => "current_user",
        x if x == SvfopUser as i32 => "user",
        x if x == SvfopSessionUser as i32 => "session_user",
        x if x == SvfopCurrentCatalog as i32 => "current_catalog",
        x if x == SvfopCurrentSchema as i32 => "current_schema",
        _ => return None,
    })
}

fn xml_expr_name(op: i32) -> Option<&'static str> {
    use XmlExprOp::*;
    Some(match op {
        x if x == IsXmlconcat as i32 => "xmlconcat",
        x if x == IsXmlelement as i32 => "xmlelement",
        x if x == IsXmlforest as i32 => "xmlforest",
        x if x == IsXmlparse as i32 => "xmlparse",
        x if x == IsXmlpi as i32 => "xmlpi",
        x if x == IsXmlroot as i32 => "xmlroot",
        x if x == IsXmlserialize as i32 => "xmlserialize",
        x if x == IsDocument as i32 => return None,
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The default name PostgreSQL gives the single unaliased target-list
    /// entry of `sql`.
    fn name_of(sql: &str) -> String {
        let parsed = pg_query::parse(sql).expect("probe SQL parses");
        let stmt = parsed
            .protobuf
            .stmts
            .first()
            .and_then(|s| s.stmt.as_ref())
            .and_then(|n| n.node.as_ref())
            .expect("one statement");
        let NodeEnum::SelectStmt(sel) = stmt else {
            panic!("probe SQL is not a SELECT");
        };
        let Some(NodeEnum::ResTarget(rt)) =
            sel.target_list.first().and_then(|n| n.node.as_ref())
        else {
            panic!("probe SQL has no target-list entry");
        };
        figure_colname_or_unnamed(rt.val.as_deref().expect("target entry has an expression"))
    }

    /// Every expectation below was read off PostgreSQL 18.2 by running the
    /// same statement and printing the column header — the server is the
    /// authority, not the incumbent engine's recorded answers (whose window
    /// labels, `row_number() ORDER BY [...]`, are a DataFusion-ism PostgreSQL
    /// never emits).
    #[track_caller]
    fn assert_named(sql: &str, expected: &str) {
        assert_eq!(name_of(sql), expected, "for {sql}");
    }

    /// The headline regression: an aggregate is named after its function, not
    /// `?column?`. A client reading `row["count"]` depends on this.
    #[test]
    fn aggregates_are_named_after_their_function() {
        assert_named("SELECT count(*) FROM t", "count");
        assert_named("SELECT count(a) FROM t", "count");
        assert_named("SELECT sum(x) FROM t", "sum");
        assert_named("SELECT avg(x) FROM t", "avg");
        assert_named("SELECT min(x) FROM t", "min");
        assert_named("SELECT max(x) FROM t", "max");
        assert_named("SELECT array_agg(x) FROM t", "array_agg");
        assert_named("SELECT string_agg(s, ',') FROM t", "string_agg");
        assert_named("SELECT sum(x) FILTER (WHERE x > 0) FROM t", "sum");
        assert_named("SELECT count(DISTINCT x) FROM t", "count");
    }

    /// The other half of the same regression. Note what is NOT here: the
    /// window's frame and ORDER BY do not appear in the name at all.
    #[test]
    fn window_functions_are_named_after_their_function_with_no_frame_decoration() {
        assert_named("SELECT row_number() OVER (ORDER BY a) FROM t", "row_number");
        assert_named("SELECT rank() OVER (ORDER BY a) FROM t", "rank");
        assert_named("SELECT dense_rank() OVER (ORDER BY a) FROM t", "dense_rank");
        assert_named("SELECT sum(x) OVER (PARTITION BY a) FROM t", "sum");
        assert_named("SELECT lag(a) OVER (ORDER BY a) FROM t", "lag");
        assert_named("SELECT count(*) OVER () FROM t", "count");
    }

    #[test]
    fn a_bare_column_keeps_its_own_name_qualified_or_not() {
        assert_named("SELECT a FROM t", "a");
        assert_named("SELECT t.a FROM t", "a");
        assert_named("SELECT s.t.a FROM s.t", "a");
    }

    /// PostgreSQL names an ordinary function call after the function, taking
    /// only the last component of a qualified name.
    #[test]
    fn scalar_function_calls_take_the_last_component_of_the_function_name() {
        assert_named("SELECT upper(s) FROM t", "upper");
        assert_named("SELECT pg_catalog.upper(s) FROM t", "upper");
        assert_named("SELECT length(s) FROM t", "length");
        assert_named("SELECT generate_series(1, 3)", "generate_series");
        // Special-syntax functions are ordinary calls by the time the
        // grammar is done with them, under their internal names.
        assert_named("SELECT trim(s) FROM t", "btrim");
        assert_named("SELECT substring(s FROM 1 FOR 2) FROM t", "substring");
        assert_named("SELECT position('h' IN s) FROM t", "position");
        assert_named("SELECT overlay(s PLACING 'x' FROM 1) FROM t", "overlay");
        assert_named("SELECT extract(year FROM now())", "extract");
    }

    /// Operators and literals are exactly where `?column?` is *correct*.
    #[test]
    fn operators_and_literals_are_unnamed() {
        assert_named("SELECT a + b FROM t", UNNAMED);
        assert_named("SELECT a = b FROM t", UNNAMED);
        assert_named("SELECT -a FROM t", UNNAMED);
        assert_named("SELECT NOT (a > 0) FROM t", UNNAMED);
        assert_named("SELECT a IS NULL FROM t", UNNAMED);
        assert_named("SELECT a IS DISTINCT FROM b FROM t", UNNAMED);
        assert_named("SELECT a IN (1, 2) FROM t", UNNAMED);
        assert_named("SELECT a BETWEEN 1 AND 2 FROM t", UNNAMED);
        assert_named("SELECT a LIKE 'x' FROM t", UNNAMED);
        assert_named("SELECT 1", UNNAMED);
        assert_named("SELECT 'lit'", UNNAMED);
        assert_named("SELECT NULL", UNNAMED);
        assert_named("SELECT 1 + 1", UNNAMED);
    }

    /// NULLIF is the single named `A_Expr` kind.
    #[test]
    fn nullif_is_the_only_named_operator_expression() {
        assert_named("SELECT nullif(a, b) FROM t", "nullif");
    }

    /// The strength rule. A cast cannot override a strong name from beneath
    /// it, but it does supply one where there was none — and the name it
    /// supplies is the type's internal name, without any typmod.
    #[test]
    fn a_cast_passes_a_strong_name_through_and_otherwise_names_the_target_type() {
        assert_named("SELECT a::text FROM t", "a");
        assert_named("SELECT cast(a AS text) FROM t", "a");
        assert_named("SELECT a::text::int FROM t", "a");
        assert_named("SELECT count(*)::text FROM t", "count");
        assert_named("SELECT (a + b)::text FROM t", "text");
        assert_named("SELECT cast(1 AS text)", "text");
        assert_named("SELECT (a + b)::bigint FROM t", "int8");
        assert_named("SELECT (a + b)::double precision FROM t", "float8");
        assert_named("SELECT (a + b)::varchar(3) FROM t", "varchar");
        assert_named("SELECT (a + b)::numeric(4, 1) FROM t", "numeric");
    }

    /// CASE consults only its ELSE branch, and its own `case` is weak enough
    /// for a wrapping cast to replace.
    #[test]
    fn case_is_named_from_its_else_branch_and_falls_back_weakly() {
        assert_named("SELECT CASE WHEN a > 0 THEN 1 ELSE 2 END FROM t", "case");
        assert_named("SELECT CASE WHEN a > 0 THEN 1 END FROM t", "case");
        assert_named("SELECT CASE WHEN a > 0 THEN 1 ELSE b END FROM t", "b");
        assert_named(
            "SELECT (CASE WHEN a > 0 THEN 1 ELSE 2 END)::text FROM t",
            "text",
        );
    }

    #[test]
    fn the_fixed_name_constructs() {
        assert_named("SELECT coalesce(a, b) FROM t", "coalesce");
        assert_named("SELECT greatest(a, b) FROM t", "greatest");
        assert_named("SELECT least(a, b) FROM t", "least");
        assert_named("SELECT ARRAY[1, 2]", "array");
        assert_named("SELECT ROW(1, 2)", "row");
        assert_named("SELECT (a, b) FROM t", "row");
        assert_named("SELECT EXISTS (SELECT 1)", "exists");
        assert_named("SELECT grouping(a) FROM t GROUP BY a", "grouping");
        assert_named("SELECT xmlelement(name foo)", "xmlelement");
        assert_named("SELECT xmlconcat(xmlelement(name a))", "xmlconcat");
    }

    #[test]
    fn the_keyword_value_functions_name_themselves() {
        assert_named("SELECT current_date", "current_date");
        assert_named("SELECT current_timestamp", "current_timestamp");
        assert_named("SELECT localtime", "localtime");
        assert_named("SELECT localtimestamp", "localtimestamp");
        assert_named("SELECT current_user", "current_user");
        assert_named("SELECT session_user", "session_user");
        assert_named("SELECT current_schema", "current_schema");
        // `now()` is an ordinary function call, not one of these.
        assert_named("SELECT now()", "now");
    }

    /// A scalar subquery borrows the name of its own target-list entry, which
    /// is why `(SELECT max(a) …)` is `max` while `(SELECT 1)` is `?column?`.
    /// `ARRAY(…)` does not: it is named `array` regardless.
    #[test]
    fn a_scalar_subquery_borrows_its_inner_targets_name() {
        assert_named("SELECT (SELECT 1)", UNNAMED);
        assert_named("SELECT (SELECT max(a) FROM t)", "max");
        assert_named("SELECT (SELECT a FROM t LIMIT 1)", "a");
        assert_named("SELECT (SELECT a AS z FROM t LIMIT 1)", "z");
        assert_named("SELECT (SELECT a + b FROM t LIMIT 1)", UNNAMED);
        assert_named("SELECT (SELECT a FROM t LIMIT 1)::text", "a");
        assert_named("SELECT ARRAY(SELECT a FROM t)", "array");
        assert_named("SELECT ARRAY(SELECT max(a) FROM t)", "array");
    }

    #[test]
    fn a_collation_is_invisible_to_naming() {
        assert_named("SELECT s COLLATE \"C\" FROM t", "s");
        assert_named("SELECT (a + b) COLLATE \"C\" FROM t", UNNAMED);
    }

    /// Subscripting passes the name through; a field selection replaces it.
    #[test]
    fn indirection_takes_the_last_field_name_or_passes_through() {
        assert_named("SELECT e[1] FROM t", "e");
        assert_named("SELECT e[1:2] FROM t", "e");
        assert_named("SELECT (c).f FROM t", "f");
        assert_named("SELECT (c).f[1] FROM t", "f");
    }
}
