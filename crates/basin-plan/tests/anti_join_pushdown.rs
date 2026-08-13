//! End-to-end regression: a `WHERE` predicate that reads a join's RIGHT side
//! must not be pushed into the join's LEFT input.
//!
//! The shape is the SQL anti-join idiom:
//!
//! ```sql
//! SELECT t.id, u.tag FROM t LEFT JOIN u ON u.tid = t.id WHERE u.tid IS NULL
//! ```
//!
//! `lower/select.rs` resolves every `WHERE` column to `relation: 0` with a
//! FLAT index across the concatenated left++right scope (see `Scope::resolve`),
//! so `u.tid` arrives as `relation: 0, index: 3` for a 3-column `t`. Reading
//! the `relation` TAG to decide which side a predicate belongs to therefore
//! attributed every such predicate to the left side and pushed it into the left
//! `Scan`, where index 3 is out of range — `opt::projection`'s `remap_expr`
//! panicked with "index out of bounds".
//!
//! These tests go through the real lowering path (so the flat-index convention
//! is the one actually under test, not a hand-built plan that assumes it) and
//! the real rule pipeline.

use basin_pgtype::{Oid, PgType};
use basin_plan::lower::expr::{FuncKind, FunctionResolver, OperatorResolver};
use basin_plan::lower::select::{lower_select, TableResolver};
use basin_plan::{ColumnRef, Expr, LogicalPlan, Schema, TableId};

struct Tables;

impl TableResolver for Tables {
    fn resolve_table(&self, name: &[String]) -> Option<(TableId, Schema)> {
        let cols: &[(&str, PgType)] = match name.last()?.as_str() {
            // Mirrors the fallback-histogram probe corpus exactly: `t` is 3
            // columns wide, so a right-side flat index (3..=6) is out of range
            // for it and the bug is a panic rather than a silent wrong column.
            "t" => &[
                ("id", PgType::INT8),
                ("name", PgType::TEXT),
                ("amt", PgType::FLOAT8),
            ],
            "u" => &[
                ("uid", PgType::INT8),
                ("tid", PgType::INT8),
                ("tag", PgType::TEXT),
                ("n", PgType::INT4),
            ],
            _ => return None,
        };
        let id = if name.last()?.as_str() == "t" {
            100
        } else {
            200
        };
        Some((
            TableId(id),
            cols.iter().map(|(n, t)| (n.to_string(), *t)).collect(),
        ))
    }
}

struct Operators;

impl OperatorResolver for Operators {
    fn resolve(&self, name: &str, left: Option<PgType>, right: PgType) -> Option<basin_plan::OpId> {
        match name {
            "AND" | "OR" | "NOT" => Some(basin_plan::OpId(Oid(u32::MAX))),
            _ => basin_pgtype::operator::resolve(name, left.map(|t| t.oid), right.oid)
                .map(|sig| basin_plan::OpId(sig.oid)),
        }
    }
}

struct Functions;

impl FunctionResolver for Functions {
    fn resolve(
        &self,
        _name: &[String],
        _args: &[PgType],
    ) -> Option<(basin_plan::FuncId, FuncKind)> {
        None
    }
}

fn optimized(sql: &str) -> LogicalPlan {
    let parsed = pg_query::parse(sql).expect("parse");
    let raw = parsed.protobuf.stmts.first().expect("stmt").clone();
    let node = *raw.stmt.expect("stmt node");
    let plan = lower_select(&node, &Tables, &Operators, &Functions).expect("lower");
    basin_plan::opt::optimize_default(plan).0
}

/// Every column index appearing anywhere in `plan`, paired with the width of
/// the node's own input(s), so an out-of-range reference is detectable without
/// knowing the plan's shape ahead of time.
fn assert_scan_filters_in_range(plan: &LogicalPlan) {
    if let LogicalPlan::Scan {
        projection,
        filters,
        ..
    } = plan
    {
        for f in filters {
            f.any(&mut |e| {
                if let Expr::Column(ColumnRef { index, .. }) = e {
                    assert!(
                        (*index as usize) < projection.len(),
                        "scan filter references column {index} but the scan has \
                         only {} projected columns: {f:?}",
                        projection.len()
                    );
                }
                false
            });
        }
    }
    plan.for_each_input(&mut |i| assert_scan_filters_in_range(i));
}

/// The exact query from the fallback-histogram probe. Pre-fix this panics in
/// `opt::projection::remap_expr` with "index out of bounds: the len is 3 but
/// the index is 4".
#[test]
fn left_join_where_right_col_is_null_does_not_push_into_left() {
    let plan =
        optimized("SELECT t.id, u.tag FROM t LEFT JOIN u ON u.tid = t.id WHERE u.tid IS NULL");
    assert_scan_filters_in_range(&plan);
}

/// Same shape, different right-side column (`u.tag`, flat index 5) — pre-fix
/// this panicked with "the index is 5".
#[test]
fn left_join_where_right_tag_is_null_does_not_push_into_left() {
    let plan =
        optimized("SELECT t.id, u.tag FROM t LEFT JOIN u ON u.tid = t.id WHERE u.tag IS NULL");
    assert_scan_filters_in_range(&plan);
}

/// The left-side control: flat index 0 is genuinely a left column and always
/// worked. It must keep being pushed into the left scan.
#[test]
fn left_join_where_left_col_is_null_still_pushes_into_left() {
    let plan =
        optimized("SELECT t.id, u.tag FROM t LEFT JOIN u ON u.tid = t.id WHERE t.id IS NULL");
    assert_scan_filters_in_range(&plan);
    let LogicalPlan::Project { input, .. } = &plan else {
        panic!("expected Project at the root, got {plan:?}");
    };
    let LogicalPlan::Join { left, .. } = input.as_ref() else {
        panic!("expected Join below the Project, got {input:?}");
    };
    let LogicalPlan::Scan { filters, .. } = left.as_ref() else {
        panic!("expected the left input to be a Scan, got {left:?}");
    };
    assert_eq!(filters.len(), 1, "t.id IS NULL should reach t's scan");
}

/// An INNER join with a right-side `WHERE` predicate.
///
/// This one never reaches filter pushdown's `Join` arm at all: `apply_where`
/// in `lower/select.rs` folds the whole `WHERE` clause of a top-level
/// `INNER`/`CROSS` join into that join's `on` list and residual `filter`
/// during lowering, so there is no `Filter` node left above the join for the
/// rule to push. Asserted here anyway, because the residual `filter` holds a
/// FLAT index over left++right — the same convention the pushdown bug
/// misread — and it must survive projection pruning still pointing at `u.n`.
#[test]
fn inner_join_right_side_predicate_stays_a_correctly_positioned_join_filter() {
    let plan = optimized("SELECT t.id, u.tag FROM t JOIN u ON u.tid = t.id WHERE u.n > 7");
    assert_scan_filters_in_range(&plan);
    let LogicalPlan::Project { input, .. } = &plan else {
        panic!("expected Project at the root, got {plan:?}");
    };
    let LogicalPlan::Join {
        left,
        right,
        filter,
        ..
    } = input.as_ref()
    else {
        panic!("expected Join below the Project, got {input:?}");
    };
    let filter = filter.as_ref().expect("u.n > 7 becomes the join's filter");
    let left_width = match left.as_ref() {
        LogicalPlan::Scan { projection, .. } => projection.len(),
        other => panic!("expected the left input to be a Scan, got {other:?}"),
    };
    let LogicalPlan::Scan {
        projection: right_projection,
        ..
    } = right.as_ref()
    else {
        panic!("expected the right input to be a Scan, got {right:?}");
    };
    // Exactly one column reference, and it must land on `u.n` — the right
    // side's own `ColId(3)` — after being read as a flat position.
    let mut seen = Vec::new();
    filter.any(&mut |e| {
        if let Expr::Column(c) = e {
            seen.push(c.clone());
        }
        false
    });
    assert_eq!(seen.len(), 1, "expected one column in {filter:?}");
    let flat = seen[0].index as usize;
    assert!(
        flat >= left_width,
        "u.n is a right-side column; flat index {flat} must be at or past the \
         pruned left width {left_width}"
    );
    assert_eq!(
        right_projection[flat - left_width],
        basin_plan::ColId(3),
        "the join filter must still address u.n"
    );
}

/// A right-side predicate that is not `IS NULL`, over a LEFT join: it may not
/// be pushed into the null-supplying side at all, so it must stay above the
/// join and never reach either scan.
#[test]
fn left_join_right_side_non_null_predicate_stays_above_the_join() {
    for sql in [
        "SELECT t.id, u.tag FROM t LEFT JOIN u ON u.tid = t.id WHERE u.tag <> 'x'",
        "SELECT t.id, u.tag FROM t LEFT JOIN u ON u.tid = t.id WHERE u.n > 7",
    ] {
        let plan = optimized(sql);
        assert_scan_filters_in_range(&plan);
        let mut scan_filters = 0;
        fn count(plan: &LogicalPlan, n: &mut usize) {
            if let LogicalPlan::Scan { filters, .. } = plan {
                *n += filters.len();
            }
            plan.for_each_input(&mut |i| count(i, n));
        }
        count(&plan, &mut scan_filters);
        assert_eq!(
            scan_filters, 0,
            "a LEFT join's null-supplying side must not absorb `{sql}`"
        );
    }
}
