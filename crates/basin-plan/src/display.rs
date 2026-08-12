//! Human-readable rendering of a [`LogicalPlan`], in Postgres's `EXPLAIN`
//! vocabulary.
//!
//! # Why Postgres's vocabulary, not our own
//!
//! Basin aims for full Postgres compatibility, and `EXPLAIN` output is part of
//! that surface: tools (pgAdmin, explain.depesz.com, ORMs that sniff plan
//! shape) and human operator intuition are both built against Postgres's node
//! names. Inventing a parallel vocabulary — "ScanNode", "FilterNode" — would
//! throw that away for no benefit, so this module reuses Postgres's own node
//! names (`Seq Scan`, `HashAggregate`, `Hash Join`, …) and attribute labels
//! (`Filter:`, `Group Key:`, `Hash Cond:`) wherever [`LogicalPlan`] has an
//! equivalent concept. It is also the practical reason the module exists at
//! all: an optimizer rule in a sibling module is far easier to verify against
//! a rendered tree than against a `{:#?}` dump of nested enums.
//!
//! # What is approximated, and why
//!
//! This crate is deliberately thin on dependencies (see `Cargo.toml`): no
//! catalog, no physical planner. Two consequences follow, and both are called
//! out at the render site rather than hidden:
//!
//! - **Names Basin cannot resolve here.** [`TableId`], [`ColId`] (outside an
//!   [`Expr::Column`], which carries a name for exactly this reason) and
//!   [`OpId`]/[`FuncId`] are catalog lookups this crate has no catalog to
//!   perform. They render as their numeric id instead of a name — e.g.
//!   `table_16384` and `OPERATOR(#96)` — which is uglier than real Postgres
//!   output but never wrong, and easy to grep for once
//!   `docs/migration/df-removal/11-pg-catalog-fidelity.md`'s catalog crate
//!   exists to resolve them properly.
//! - **Physical choices this IR does not make.** [`LogicalPlan::Join`] does
//!   not record whether Postgres would choose a hash, merge or nested-loop
//!   strategy — that is a physical-planner decision this crate does not yet
//!   have. `Hash Join` is used whenever equi-join keys are present (`on` is
//!   non-empty) and `Nested Loop` otherwise, which is the right node *shape*
//!   (same attributes, same children) even when a real Postgres planner would
//!   pick differently on cost.
//!
//! Cost estimates (`cost=0.00..1.05 rows=5 width=4`) and actual run
//! statistics (`actual time=…`) are Postgres's `ANALYZE`/planner-statistics
//! output, which this crate has neither of — they are simply absent here
//! rather than faked.

use crate::{
    ColId, ColumnRef, CteId, Datum, Expr, FuncId, GroupingSets, JoinKind, LogicalPlan, OnConflict,
    OpId, SetOpKind, SortKey, Subscript, TableId,
};
use basin_pgtype::{oid, PgType};

/// Render `plan` as an indented `EXPLAIN`-style tree.
///
/// Follows Postgres's own indentation rule: each child is printed two spaces
/// deeper than its parent's attribute lines, prefixed with `->  `. See
/// `write_node` for the exact recursion — it reproduces real `EXPLAIN` output
/// byte-for-byte for the indentation (verified against hand-checked Postgres
/// transcripts), even though the content it indents is approximated as
/// described in the module doc.
pub fn explain(plan: &LogicalPlan) -> String {
    let mut out = String::new();
    write_node(plan, 0, "", &mut out);
    if out.ends_with('\n') {
        out.pop();
    }
    out
}

/// One plan node's rendered shape: its own text, its attribute lines, and its
/// children — the children as unrendered plan references, since a
/// [`LogicalPlan::Filter`] node is spliced into its child's attributes rather
/// than becoming a node of its own (see the `Filter` arm of [`render_node`]).
struct Node<'a> {
    text: String,
    attrs: Vec<(&'static str, String)>,
    children: Vec<&'a LogicalPlan>,
}

impl<'a> Node<'a> {
    fn leaf(text: String) -> Node<'a> {
        Node {
            text,
            attrs: Vec::new(),
            children: Vec::new(),
        }
    }
}

/// Print `plan` at `indent` spaces, preceded by `marker` (`""` for the root,
/// `"->  "` for a child — Postgres's own convention).
///
/// The indent Postgres gives to a node's *content* (its attribute lines, and
/// the marker column for its own children) is `indent + marker.len() + 2`:
/// two spaces past wherever this node's own text started. Recursing with that
/// as the next `indent` is what makes nested children line up the way real
/// `EXPLAIN` output does — e.g. a `Hash` node whose marker sits at column 2
/// places its own child's marker at column 8 (`2 + "->  ".len() + 2`), not at
/// a flat `+2` per level.
fn write_node(plan: &LogicalPlan, indent: usize, marker: &str, out: &mut String) {
    let node = render_node(plan);
    out.push_str(&" ".repeat(indent));
    out.push_str(marker);
    out.push_str(&node.text);
    out.push('\n');

    let content_indent = indent + marker.len() + 2;
    for (key, value) in &node.attrs {
        out.push_str(&" ".repeat(content_indent));
        out.push_str(key);
        out.push_str(": ");
        out.push_str(value);
        out.push('\n');
    }
    for child in &node.children {
        write_node(child, content_indent, "->  ", out);
    }
}

/// Merge a `Filter:` predicate into an existing attribute list, combining
/// with any filter already there rather than emitting a second `Filter:`
/// line — Postgres always shows exactly one per node. Basin can accumulate
/// more than one source for the same node (a [`LogicalPlan::Scan`]'s own
/// pushed-down `filters`, plus one or more wrapping [`LogicalPlan::Filter`]
/// nodes for predicates that were not pushed down); this is where they
/// recombine into the single line Postgres would show.
fn merge_filter(attrs: &mut Vec<(&'static str, String)>, predicate: String) {
    if let Some((_, existing)) = attrs.iter_mut().find(|(k, _)| *k == "Filter") {
        existing.push_str(" AND ");
        existing.push_str(&predicate);
    } else {
        attrs.push(("Filter", predicate));
    }
}

fn render_node(plan: &LogicalPlan) -> Node<'_> {
    match plan {
        // Postgres never gives a WHERE-clause filter its own plan node — it is
        // an attribute of whatever node produces the rows being filtered
        // (`Filter:` on a scan, `Filter:` on an Aggregate for a HAVING
        // clause that could not be folded into the group, etc). So this arm
        // renders `input` and splices the predicate into it, rather than
        // returning a node of its own.
        LogicalPlan::Filter { input, predicate } => {
            let mut node = render_node(input);
            merge_filter(&mut node.attrs, render_expr(predicate));
            node
        }

        LogicalPlan::Scan {
            table,
            projection,
            filters,
            snapshot: _,
        } => {
            let mut attrs = Vec::new();
            if !projection.is_empty() {
                attrs.push((
                    "Output",
                    projection
                        .iter()
                        .map(|c| col_display(*c))
                        .collect::<Vec<_>>()
                        .join(", "),
                ));
            }
            if !filters.is_empty() {
                attrs.push(("Filter", join_and(filters)));
            }
            Node {
                text: format!("Seq Scan on {}", table_display(*table)),
                attrs,
                children: Vec::new(),
            }
        }

        // No real Postgres relation backs a VALUES list; `Values Scan on
        // "*VALUES*"` is literally how Postgres's own EXPLAIN spells it.
        LogicalPlan::Values { rows, .. } => Node::leaf(format!(
            "Values Scan on \"*VALUES*\"  (rows={})",
            rows.len()
        )),

        // Postgres's `Result` node is what a plan with no scan input at all
        // becomes (`SELECT 1`, or `SELECT … WHERE false` const-folded away).
        // `One-Time Filter: false` is Postgres's exact spelling for the
        // latter — it evaluates the condition once rather than per row.
        LogicalPlan::Empty {
            produce_one_row, ..
        } => {
            let mut attrs = Vec::new();
            if !produce_one_row {
                attrs.push(("One-Time Filter", "false".to_string()));
            }
            Node {
                text: "Result".to_string(),
                attrs,
                children: vec![],
            }
        }

        // A plain projection usually folds into whichever node already
        // produces the rows (shown as `Output:` under Postgres's VERBOSE
        // mode) rather than a node of its own. Basin's IR always keeps
        // `Project` distinct, so — conservatively, and unlike plain VERBOSE
        // output — this renders it as Postgres's `Result` node with an
        // `Output:` line, rather than guessing which upstream node it would
        // have folded into.
        LogicalPlan::Project { input, exprs } => Node {
            text: "Result".to_string(),
            attrs: vec![(
                "Output",
                exprs
                    .iter()
                    .map(|(e, alias)| format!("{} AS {}", render_expr(e), alias))
                    .collect::<Vec<_>>()
                    .join(", "),
            )],
            children: vec![input],
        },

        LogicalPlan::Aggregate {
            input,
            group,
            aggs: _,
            grouping_sets,
        } => {
            let mut attrs = Vec::new();
            match grouping_sets {
                // Postgres prints one `Group Key:` line per grouping set for
                // ROLLUP/CUBE/GROUPING SETS (visible in a `GroupAggregate`
                // plan for e.g. `GROUP BY ROLLUP (a, b)`).
                Some(GroupingSets(sets)) => {
                    for set in sets {
                        let key = set
                            .iter()
                            .filter_map(|&idx| group.get(idx as usize))
                            .map(render_expr)
                            .collect::<Vec<_>>()
                            .join(", ");
                        attrs.push(("Group Key", key));
                    }
                }
                None if !group.is_empty() => {
                    attrs.push(("Group Key", join_exprs(group)));
                }
                None => {}
            }
            Node {
                // Postgres picks `HashAggregate`, `GroupAggregate` or plain
                // `Aggregate` (no grouping columns) based on planner cost;
                // this IR has no physical planner yet, so every Aggregate
                // renders as `HashAggregate` regardless of shape.
                text: "HashAggregate".to_string(),
                attrs,
                children: vec![input],
            }
        }

        LogicalPlan::Sort { input, keys } => {
            let mut attrs = Vec::new();
            if !keys.is_empty() {
                attrs.push((
                    "Sort Key",
                    keys.iter()
                        .map(render_sort_key)
                        .collect::<Vec<_>>()
                        .join(", "),
                ));
            }
            Node {
                text: "Sort".to_string(),
                attrs,
                children: vec![input],
            }
        }

        // Postgres's own `EXPLAIN` (even VERBOSE) does not print the LIMIT
        // or OFFSET value as text anywhere — it only shows up baked into the
        // node's estimated `rows=`, which this crate does not compute. So,
        // faithfully, no attribute line here; `with_ties` is not surfaced
        // either, since Postgres has no separate spelling for it (`lib.rs`
        // already records it as degraded to plain `LIMIT`).
        LogicalPlan::Limit { input, .. } => Node {
            text: "Limit".to_string(),
            attrs: Vec::new(),
            children: vec![input],
        },

        LogicalPlan::Join {
            left,
            right,
            kind,
            on,
            filter,
        } => {
            // Hash-joinable whenever there is at least one equijoin
            // conjunct; Nested Loop otherwise (CROSS JOIN, or a join whose
            // condition is not an equality). A real Postgres planner also
            // considers Merge Join and cost; this IR does not have costs to
            // consider yet, so this is a shape choice, not a cost choice.
            let method = if on.is_empty() { "Nested Loop" } else { "Hash" };
            let mut attrs = Vec::new();
            if !on.is_empty() {
                attrs.push((
                    "Hash Cond",
                    on.iter()
                        .map(|(l, r)| format!("({} = {})", render_expr(l), render_expr(r)))
                        .collect::<Vec<_>>()
                        .join(" AND "),
                ));
            }
            if let Some(f) = filter {
                attrs.push(("Join Filter", render_expr(f)));
            }
            Node {
                text: join_text(method, *kind),
                attrs,
                children: vec![left, right],
            }
        }

        // LATERAL correlation forces re-evaluating the inner side per outer
        // row, which is exactly what a Nested Loop does — Postgres has no
        // separate node type for a lateral join, only a Nested Loop whose
        // inner side happens to be correlated (invisibly, in the node
        // label; the correlation shows up in the inner side's own scan).
        LogicalPlan::LateralJoin { outer, inner, kind } => Node {
            text: join_text("Nested Loop", *kind),
            attrs: Vec::new(),
            children: vec![outer, inner],
        },

        LogicalPlan::SetOp {
            left,
            right,
            op,
            all,
        } => {
            // `UNION ALL` needs no deduplication, so Postgres plans it as a
            // simple concatenation: `Append`. Everything else needs a hash
            // (or sort) to dedupe/intersect/subtract, which Postgres spells
            // `HashSetOp <Op>[ All]` — this IR has no physical planner to
            // pick the sort-based `SetOp` variant, so hash-based is assumed.
            let text = if matches!(op, SetOpKind::Union) && *all {
                "Append".to_string()
            } else {
                let opname = match op {
                    SetOpKind::Union => "Union",
                    SetOpKind::Intersect => "Intersect",
                    SetOpKind::Except => "Except",
                };
                format!("HashSetOp {opname}{}", if *all { " All" } else { "" })
            };
            Node {
                text,
                attrs: Vec::new(),
                children: vec![left, right],
            }
        }

        LogicalPlan::Distinct { input, on } => {
            let mut attrs = Vec::new();
            // Plain DISTINCT (`on` is `None`) dedupes on every output
            // column. Postgres would list them all in `Group Key:`; doing
            // the same here needs this node's output schema, which
            // `schema.rs` cannot yet infer for a `Distinct` (see its
            // `Unimplemented` fallback) — so this is left off rather than
            // guessed at.
            if let Some(on) = on {
                attrs.push(("Group Key", join_exprs(on)));
            }
            Node {
                // A plain DISTINCT is usually a sort-based `Unique` in
                // Postgres; `DISTINCT ON` has no direct Postgres node at all
                // (it is planner sugar over grouping). Both collapse to
                // `HashAggregate` here for the same no-physical-planner
                // reason as `Aggregate` above.
                text: "HashAggregate".to_string(),
                attrs,
                children: vec![input],
            }
        }

        LogicalPlan::Window { input, windows } => Node {
            text: "WindowAgg".to_string(),
            attrs: vec![("Output", join_exprs(windows))],
            children: vec![input],
        },

        LogicalPlan::ProjectSet { input, srfs } => Node {
            text: "ProjectSet".to_string(),
            attrs: vec![("Output", join_exprs(srfs))],
            children: vec![input],
        },

        // Real Postgres attaches a CTE's defining plan as an InitPlan under
        // whichever node scope first needs it, printed as a `CTE <name>`
        // sub-section rather than as a node in the main tree. This crate has
        // no such side-channel to render into, so — approximated, and noted
        // as such — the definition (`body`) is shown as an explicit first
        // child alongside the consuming query (`input`), rather than
        // reproducing Postgres's InitPlan layout.
        LogicalPlan::Cte {
            name,
            recursive,
            body,
            input,
        } => Node {
            text: format!(
                "CTE {}{}",
                if *recursive { "Recursive " } else { "" },
                cte_display(*name)
            ),
            attrs: Vec::new(),
            children: vec![body, input],
        },

        LogicalPlan::CteRef { name, schema } => {
            let mut attrs = Vec::new();
            if !schema.is_empty() {
                attrs.push((
                    "Output",
                    schema
                        .iter()
                        .map(|(n, _)| n.clone())
                        .collect::<Vec<_>>()
                        .join(", "),
                ));
            }
            Node {
                text: format!("CTE Scan on {}", cte_display(*name)),
                attrs,
                children: Vec::new(),
            }
        }

        LogicalPlan::Insert {
            table,
            input,
            columns: _,
            on_conflict,
            returning,
        } => {
            let mut attrs = Vec::new();
            if let Some(oc) = on_conflict {
                // Postgres's own attribute name and vocabulary for `ON
                // CONFLICT`, from `explain.c`'s `ExplainModifyTarget`.
                attrs.push((
                    "Conflict Resolution",
                    match oc {
                        OnConflict::DoNothing { .. } => "NOTHING".to_string(),
                        OnConflict::DoUpdate { .. } => "UPDATE".to_string(),
                    },
                ));
            }
            if let Some(ret) = returning {
                attrs.push(("Output", render_returning(ret)));
            }
            Node {
                text: format!("Insert on {}", table_display(*table)),
                attrs,
                children: vec![input],
            }
        }

        LogicalPlan::Update {
            table,
            set: _,
            from,
            predicate,
            returning,
            snapshot: _,
        } => {
            let mut attrs = Vec::new();
            // Real Postgres would attach this predicate to the target
            // table's own scan node (as that scan's `Filter:`), not to
            // `Update` itself — but Basin's `Update` has no such scan node
            // to attach it to (only `from`, for `UPDATE … FROM`, is a
            // plan). Shown here instead, so it stays visible.
            if let Some(p) = predicate {
                merge_filter(&mut attrs, render_expr(p));
            }
            if let Some(ret) = returning {
                attrs.push(("Output", render_returning(ret)));
            }
            Node {
                text: format!("Update on {}", table_display(*table)),
                attrs,
                children: from.as_deref().into_iter().collect(),
            }
        }

        LogicalPlan::Delete {
            table,
            using,
            predicate,
            returning,
            snapshot: _,
        } => {
            let mut attrs = Vec::new();
            if let Some(p) = predicate {
                merge_filter(&mut attrs, render_expr(p));
            }
            if let Some(ret) = returning {
                attrs.push(("Output", render_returning(ret)));
            }
            Node {
                text: format!("Delete on {}", table_display(*table)),
                attrs,
                children: using.as_deref().into_iter().collect(),
            }
        }
    }
}

fn render_returning(returning: &[(Expr, String)]) -> String {
    returning
        .iter()
        .map(|(e, alias)| format!("{} AS {}", render_expr(e), alias))
        .collect::<Vec<_>>()
        .join(", ")
}

/// `Hash`/`Nested Loop`/`Merge` + Postgres's qualifier placement, which is
/// irregular: `Nested Loop` drops the word `Join` entirely for an
/// unqualified (inner/cross) join, while `Hash Join` keeps it. Verified
/// against real `EXPLAIN` transcripts rather than guessed.
fn join_text(method: &str, kind: JoinKind) -> String {
    match kind {
        JoinKind::Inner | JoinKind::Cross => {
            if method == "Nested Loop" {
                method.to_string()
            } else {
                format!("{method} Join")
            }
        }
        JoinKind::Left => format!("{method} Left Join"),
        JoinKind::Right => format!("{method} Right Join"),
        JoinKind::Full => format!("{method} Full Join"),
        // `EXISTS`/`IN` and `NOT EXISTS`/`NOT IN` after decorrelation are
        // exactly Postgres's Semi and Anti joins — no renaming needed.
        JoinKind::LeftSemi => format!("{method} Semi Join"),
        JoinKind::LeftAnti => format!("{method} Anti Join"),
    }
}

fn join_exprs(exprs: &[Expr]) -> String {
    exprs.iter().map(render_expr).collect::<Vec<_>>().join(", ")
}

fn join_and(exprs: &[Expr]) -> String {
    exprs
        .iter()
        .map(render_expr)
        .collect::<Vec<_>>()
        .join(" AND ")
}

/// `Sort Key:` rendering, including Postgres's default-suppression: it only
/// prints `NULLS FIRST`/`NULLS LAST` when it deviates from the direction's
/// default (`NULLS LAST` for `ASC`, `NULLS FIRST` for `DESC`).
fn render_sort_key(key: &SortKey) -> String {
    let mut s = render_expr(&key.expr);
    if key.descending {
        s.push_str(" DESC");
    }
    let default_nulls_first = key.descending;
    if key.nulls_first != default_nulls_first {
        s.push_str(if key.nulls_first {
            " NULLS FIRST"
        } else {
            " NULLS LAST"
        });
    }
    s
}

/// A table this crate cannot resolve to a name — see the module doc. Kept as
/// a single function so the day a catalog dependency lands, this is the one
/// place that changes.
fn table_display(id: TableId) -> String {
    format!("table_{}", id.0)
}

/// A bare `ColId` outside an [`Expr::Column`] (i.e. a [`LogicalPlan::Scan`]'s
/// `projection`) carries no name — only [`ColumnRef`] does, because it is
/// resolved after the catalog is consulted during lowering. Same TODO as
/// [`table_display`].
fn col_display(id: ColId) -> String {
    format!("col_{}", id.0)
}

fn cte_display(id: CteId) -> String {
    format!("cte_{}", id.0)
}

/// `OPERATOR(#<oid>)` — deliberately shaped like Postgres's own
/// `OPERATOR(schema.opname)` syntax for referencing an operator
/// unambiguously by catalog identity rather than symbol (used when an
/// operator name alone would be ambiguous). This crate has no `pg_operator`
/// to resolve `op` down to a symbol, so the OID stands in for the name; swap
/// this for a real symbol once the operator catalog exists.
fn render_op(op: OpId) -> String {
    format!("OPERATOR(#{})", op.0)
}

/// Same TODO as [`render_op`], for `pg_proc`: no function-name catalog here,
/// so `fn_#<oid>` stands in for the name.
fn render_func(func: FuncId) -> String {
    format!("fn_{}", func.0)
}

fn pgtype_display(ty: PgType) -> String {
    match oid::type_name(ty.oid) {
        Some(name) => name.to_string(),
        // A user-defined type, or a builtin this crate's `oid` module has
        // not tabulated yet — see `basin_pgtype::oid`'s own module doc.
        None => format!("type_{}", ty.oid),
    }
}

fn render_datum(datum: &Datum) -> String {
    match datum {
        Datum::Null => "NULL".to_string(),
        Datum::Bool(b) => b.to_string(),
        Datum::Int16(v) => v.to_string(),
        Datum::Int32(v) => v.to_string(),
        Datum::Int64(v) => v.to_string(),
        Datum::Float32(v) => v.to_string(),
        Datum::Float64(v) => v.to_string(),
        // Postgres quotes text literals with `'`, doubling any embedded
        // quote — the same escaping `ruleutils.c` uses when deparsing.
        Datum::Utf8(s) => format!("'{}'", s.replace('\'', "''")),
        // `numeric` beyond i64, `uuid`, binary `jsonb`, array literals — all
        // of these need type-specific decoding this crate does not have
        // (that is the physical layer's job). Rendered as Postgres's own
        // `bytea` hex-escape syntax, since that is at least an honest
        // description of "these are bytes", not a claim about the decoded
        // value.
        Datum::Bytes(b) => {
            let hex: String = b.iter().map(|byte| format!("{byte:02x}")).collect();
            format!("'\\x{hex}'")
        }
    }
}

fn render_expr(expr: &Expr) -> String {
    match expr {
        Expr::Column(ColumnRef { name, .. }) => name.clone(),
        Expr::Literal(datum, _) => render_datum(datum),
        Expr::Parameter { index, .. } => format!("${index}"),

        Expr::Unary { op, arg } => format!("({} {})", render_op(*op), render_expr(arg)),
        Expr::Binary { op, lhs, rhs } => {
            format!(
                "({} {} {})",
                render_expr(lhs),
                render_op(*op),
                render_expr(rhs)
            )
        }

        Expr::Cast { arg, to, .. } => format!("({})::{}", render_expr(arg), pgtype_display(*to)),

        Expr::Case {
            operand,
            whens,
            else_,
        } => {
            let mut s = "CASE".to_string();
            if let Some(o) = operand {
                s.push(' ');
                s.push_str(&render_expr(o));
            }
            for (w, t) in whens {
                s.push_str(" WHEN ");
                s.push_str(&render_expr(w));
                s.push_str(" THEN ");
                s.push_str(&render_expr(t));
            }
            if let Some(e) = else_ {
                s.push_str(" ELSE ");
                s.push_str(&render_expr(e));
            }
            s.push_str(" END");
            s
        }
        Expr::Coalesce(xs) => format!("COALESCE({})", join_exprs(xs)),

        Expr::IsNull { arg, negated } => format!(
            "{} IS {}NULL",
            render_expr(arg),
            if *negated { "NOT " } else { "" }
        ),
        Expr::BoolTest { arg, test } => {
            format!("{} {}", render_expr(arg), bool_test_display(*test))
        }
        Expr::DistinctFrom { lhs, rhs, negated } => format!(
            "{} IS {}DISTINCT FROM {}",
            render_expr(lhs),
            if *negated { "NOT " } else { "" },
            render_expr(rhs)
        ),

        Expr::InList { arg, list, negated } => format!(
            "{} {}IN ({})",
            render_expr(arg),
            if *negated { "NOT " } else { "" },
            join_exprs(list)
        ),
        Expr::Between {
            arg,
            low,
            high,
            symmetric,
            negated,
        } => format!(
            "{} {}BETWEEN {}{} AND {}",
            render_expr(arg),
            if *negated { "NOT " } else { "" },
            if *symmetric { "SYMMETRIC " } else { "" },
            render_expr(low),
            render_expr(high)
        ),
        Expr::Like {
            arg,
            pattern,
            escape,
            case_insensitive,
            negated,
        } => {
            let mut s = format!(
                "{} {}{} {}",
                render_expr(arg),
                if *negated { "NOT " } else { "" },
                if *case_insensitive { "ILIKE" } else { "LIKE" },
                render_expr(pattern)
            );
            if let Some(e) = escape {
                s.push_str(" ESCAPE ");
                s.push_str(&render_expr(e));
            }
            s
        }

        Expr::ScalarFn { func, args } => format!("{}({})", render_func(*func), join_exprs(args)),

        Expr::Aggregate {
            func,
            args,
            distinct,
            filter,
            order_by,
        } => {
            let mut s = format!(
                "{}({}{}",
                render_func(*func),
                if *distinct { "DISTINCT " } else { "" },
                join_exprs(args)
            );
            if !order_by.is_empty() {
                s.push_str(" ORDER BY ");
                s.push_str(
                    &order_by
                        .iter()
                        .map(render_sort_key)
                        .collect::<Vec<_>>()
                        .join(", "),
                );
            }
            s.push(')');
            if let Some(f) = filter {
                s.push_str(" FILTER (WHERE ");
                s.push_str(&render_expr(f));
                s.push(')');
            }
            s
        }

        Expr::Window {
            func,
            args,
            partition_by,
            order_by,
            frame,
        } => {
            let mut s = format!("{}({}) OVER (", render_func(*func), join_exprs(args));
            let mut wrote = false;
            if !partition_by.is_empty() {
                s.push_str("PARTITION BY ");
                s.push_str(&join_exprs(partition_by));
                wrote = true;
            }
            if !order_by.is_empty() {
                if wrote {
                    s.push(' ');
                }
                s.push_str("ORDER BY ");
                s.push_str(
                    &order_by
                        .iter()
                        .map(render_sort_key)
                        .collect::<Vec<_>>()
                        .join(", "),
                );
            }
            s.push(')');
            let _ = frame; // Frame units/bounds omitted: no test exercises the
                           // text form yet and RANGE/ROWS/GROUPS with all five
                           // bound kinds is enough surface for its own pass.
            s
        }

        Expr::SetReturning { func, args } => {
            format!("{}({})", render_func(*func), join_exprs(args))
        }

        // A full nested rendering of the subplan (Postgres's own `SubPlan
        // N:` / `InitPlan N:` sections, printed after the tree with their
        // own indentation) needs multi-line output threaded through a
        // single-line expression renderer. Not attempted here — this
        // summarizes the subquery's kind inline instead, which is honestly
        // incomplete but never wrong.
        Expr::Subquery { kind, operand, .. } => match operand {
            Some(o) => format!("({} {:?} (SubPlan))", render_expr(o), kind),
            None => format!("({:?} (SubPlan))", kind),
        },

        Expr::ArrayLit(xs) => format!("ARRAY[{}]", join_exprs(xs)),
        Expr::RowLit(xs) => format!("ROW({})", join_exprs(xs)),

        Expr::Subscript { arg, indices } => {
            let mut s = render_expr(arg);
            for i in indices {
                match i {
                    Subscript::Index(e) => {
                        s.push('[');
                        s.push_str(&render_expr(e));
                        s.push(']');
                    }
                    Subscript::Slice { lower, upper } => {
                        s.push('[');
                        if let Some(l) = lower {
                            s.push_str(&render_expr(l));
                        }
                        s.push(':');
                        if let Some(u) = upper {
                            s.push_str(&render_expr(u));
                        }
                        s.push(']');
                    }
                }
            }
            s
        }

        // No catalog to name the field, same reasoning as `render_op`.
        Expr::FieldSelect { arg, field } => format!("({}).f{}", render_expr(arg), field),
    }
}

fn bool_test_display(test: crate::BoolTest) -> &'static str {
    use crate::BoolTest::*;
    match test {
        IsTrue => "IS TRUE",
        IsNotTrue => "IS NOT TRUE",
        IsFalse => "IS FALSE",
        IsNotFalse => "IS NOT FALSE",
        IsUnknown => "IS UNKNOWN",
        IsNotUnknown => "IS NOT UNKNOWN",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ColId, SnapshotId};
    use basin_pgtype::oid;

    fn col(idx: u16, name: &str) -> Expr {
        Expr::Column(ColumnRef {
            relation: 0,
            index: idx,
            name: name.to_string(),
        })
    }

    fn scan(table: u32, projection: Vec<ColId>, filters: Vec<Expr>) -> LogicalPlan {
        LogicalPlan::Scan {
            table: TableId(table),
            projection,
            filters,
            snapshot: SnapshotId(1),
        }
    }

    #[test]
    fn a_simple_scan_names_the_relation_and_its_output() {
        let plan = scan(16384, vec![ColId(0), ColId(1)], vec![]);
        let out = explain(&plan);
        assert_eq!(out, "Seq Scan on table_16384\n  Output: col_0, col_1");
    }

    /// The requirement this crate exists to serve: a Filter renders as an
    /// attribute *under* its child, not as a node of its own — matching how
    /// Postgres shows a WHERE clause on a Seq Scan.
    #[test]
    fn a_filter_over_a_scan_is_an_attribute_line_not_a_node() {
        let plan = LogicalPlan::Filter {
            input: Box::new(scan(16384, vec![ColId(0)], vec![])),
            predicate: Expr::Binary {
                op: OpId(oid::Oid(147)), // int4gt
                lhs: Box::new(col(0, "id")),
                rhs: Box::new(Expr::Literal(Datum::Int32(5), PgType::INT4)),
            },
        };
        let out = explain(&plan);
        assert_eq!(
            out,
            "Seq Scan on table_16384\n  Output: col_0\n  Filter: (id OPERATOR(#147) 5)"
        );
        // No separate "Filter" node — the tree has exactly one line naming a
        // node ("Seq Scan on …"); everything else is an attribute.
        assert_eq!(out.matches("Seq Scan").count(), 1);
        assert!(!out.contains("\nFilter\n"));
    }

    /// A scan's own pushed-down `filters` (as opposed to a wrapping `Filter`
    /// node) must also surface as `Filter:` — that is one of the two things
    /// the optimizer's pushdown work is supposed to make visible here.
    #[test]
    fn a_pushed_down_scan_filter_renders_without_a_wrapping_filter_node() {
        let plan = scan(
            16385,
            vec![ColId(0)],
            vec![Expr::Binary {
                op: OpId(oid::Oid(96)), // int4eq
                lhs: Box::new(col(0, "status")),
                rhs: Box::new(Expr::Literal(Datum::Int32(1), PgType::INT4)),
            }],
        );
        let out = explain(&plan);
        assert_eq!(
            out,
            "Seq Scan on table_16385\n  Output: col_0\n  Filter: (status OPERATOR(#96) 1)"
        );
    }

    /// Both sources of `Filter:` combine onto one line, the way Postgres
    /// only ever shows one `Filter:` per node.
    #[test]
    fn scan_filter_and_wrapping_filter_node_combine_onto_one_line() {
        let plan = LogicalPlan::Filter {
            input: Box::new(scan(
                1,
                vec![],
                vec![Expr::Literal(Datum::Bool(true), PgType::BOOL)],
            )),
            predicate: Expr::Literal(Datum::Bool(false), PgType::BOOL),
        };
        let out = explain(&plan);
        assert_eq!(out, "Seq Scan on table_1\n  Filter: true AND false");
    }

    /// A join tree indents its two children correctly: both at the same
    /// depth, each prefixed with Postgres's `->  ` marker.
    #[test]
    fn a_join_tree_indents_both_children_under_the_join() {
        let plan = LogicalPlan::Join {
            left: Box::new(scan(1, vec![ColId(0)], vec![])),
            right: Box::new(scan(2, vec![ColId(0)], vec![])),
            kind: JoinKind::Inner,
            on: vec![(col(0, "a_id"), col(0, "b_id"))],
            filter: None,
        };
        let out = explain(&plan);
        assert_eq!(
            out,
            "Hash Join\n  Hash Cond: (a_id = b_id)\n  ->  Seq Scan on table_1\n        Output: col_0\n  ->  Seq Scan on table_2\n        Output: col_0"
        );
    }

    /// A three-level tree (join over a filtered scan and a plain scan)
    /// checks that indentation compounds correctly rather than just working
    /// for one level of nesting.
    #[test]
    fn nested_children_indent_deeper_than_their_parent() {
        let plan = LogicalPlan::Join {
            left: Box::new(LogicalPlan::Filter {
                input: Box::new(scan(1, vec![], vec![])),
                predicate: Expr::Literal(Datum::Bool(true), PgType::BOOL),
            }),
            right: Box::new(scan(2, vec![], vec![])),
            kind: JoinKind::Left,
            on: vec![],
            filter: Some(Expr::Literal(Datum::Bool(true), PgType::BOOL)),
        };
        let out = explain(&plan);
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines[0], "Nested Loop Left Join");
        assert_eq!(lines[1], "  Join Filter: true");
        assert_eq!(lines[2], "  ->  Seq Scan on table_1");
        assert_eq!(lines[3], "        Filter: true");
        assert_eq!(lines[4], "  ->  Seq Scan on table_2");
    }

    #[test]
    fn literals_and_columns_render_readably() {
        assert_eq!(render_expr(&col(0, "email")), "email");
        assert_eq!(
            render_expr(&Expr::Literal(Datum::Utf8("a'b".into()), PgType::TEXT)),
            "'a''b'"
        );
        assert_eq!(
            render_expr(&Expr::Literal(Datum::Int64(42), PgType::INT8)),
            "42"
        );
        assert_eq!(
            render_expr(&Expr::Literal(Datum::Null, PgType::UNKNOWN)),
            "NULL"
        );
    }

    /// An operator this crate cannot name is rendered by OID rather than
    /// silently dropped or panicking — the operator catalog will replace
    /// this with the real symbol later.
    #[test]
    fn an_unresolved_operator_renders_by_oid() {
        let e = Expr::Binary {
            op: OpId(oid::Oid(96)),
            lhs: Box::new(col(0, "a")),
            rhs: Box::new(col(1, "b")),
        };
        assert_eq!(render_expr(&e), "(a OPERATOR(#96) b)");
    }
}
