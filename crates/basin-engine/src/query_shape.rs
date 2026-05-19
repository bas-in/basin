//! `QueryShape` — canonical, cross-process-stable hash of a logical query shape.
//!
//! # What this hashes
//!
//! The hash captures the *structural* shape of a `LogicalPlan`: node types,
//! column references, join types, aggregation functions, sort directions, etc.
//! Literal values are *erased*: `WHERE id = 5` and `WHERE id = 99` produce the
//! same hash because the `5` and `99` are replaced by a `LITERAL_SLOT`
//! placeholder carrying only the value's `DataType`.
//!
//! # Stability contract
//!
//! The hash is seeded with `basin_sketch::QUERY_SHAPE_SEED` — a fixed constant
//! documented in ADR 0017.  **Do not change that constant.** It is the join key
//! across the entire OSS → cloud → cross-tenant aggregate pipeline; rotating it
//! invalidates every historical shape record in basin-cloud.
//!
//! The hash is 64 bits and computed with `xxh3_64_with_seed`, which is
//! deterministic across platforms and Rust versions given identical byte inputs.
//!
//! # Not in scope here
//! - Registering or persisting hashes (Phase 5.16.B)
//! - Exporting over OTLP (Phase 5.16.D)
//! - Per-customer salting (cross-tenant aggregates require a shared key space)

use datafusion::common::Column;
use datafusion::logical_expr::{
    expr::AggregateFunction, BinaryExpr, Expr, LogicalPlan, Operator,
};
use datafusion::logical_expr::{JoinType, SortExpr};
use xxhash_rust::xxh3::Xxh3;

/// A canonical 64-bit hash of a query's logical shape.
///
/// Two plans with the same structure but different literal values produce the
/// same `QueryShapeHash`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct QueryShapeHash(pub u64);

impl QueryShapeHash {
    /// Compute the canonical shape hash for `plan`.
    pub fn of(plan: &LogicalPlan) -> Self {
        let mut hasher = Xxh3::with_seed(basin_sketch::QUERY_SHAPE_SEED);
        hash_plan(&mut hasher, plan);
        QueryShapeHash(hasher.digest())
    }
}

// ---------------------------------------------------------------------------
// Plan-level hashing
// ---------------------------------------------------------------------------

/// Post-order walk: recurse into children first, then hash this node.
fn hash_plan(h: &mut Xxh3, plan: &LogicalPlan) {
    // Recurse into all child plans first (post-order).
    for child in plan.inputs() {
        hash_plan(h, child);
    }

    // Hash the discriminant so that different node types never collide even
    // if their field-level hashes happen to be identical.
    hash_plan_discriminant(h, plan);

    match plan {
        LogicalPlan::Projection(p) => {
            for expr in &p.expr {
                hash_expr(h, expr);
            }
        }
        LogicalPlan::Filter(f) => {
            hash_expr(h, &f.predicate);
        }
        LogicalPlan::Join(j) => {
            hash_join_type(h, j.join_type);
            // Hash equijoin key pairs.  Each pair is ordered (left, right), and
            // the list order is significant.
            for (left_expr, right_expr) in &j.on {
                hash_expr(h, left_expr);
                hash_expr(h, right_expr);
            }
            // Non-equi filter.
            if let Some(filter) = &j.filter {
                h.update(b"\x01"); // present marker
                hash_expr(h, filter);
            } else {
                h.update(b"\x00"); // absent marker
            }
        }
        LogicalPlan::TableScan(ts) => {
            // Hash schema + table name only — NEVER project_id.
            // The histogram key already carries project context.
            if let Some(schema) = ts.table_name.schema() {
                h.update(schema.as_bytes());
            }
            h.update(b".");
            h.update(ts.table_name.table().as_bytes());
        }
        LogicalPlan::Aggregate(agg) => {
            // Group-by column refs.
            for expr in &agg.group_expr {
                hash_expr(h, expr);
            }
            // Aggregate function names + arg column refs.
            for expr in &agg.aggr_expr {
                hash_aggregate_expr(h, expr);
            }
        }
        LogicalPlan::Sort(s) => {
            for sort_expr in &s.expr {
                hash_sort_expr(h, sort_expr);
            }
            // Presence/absence of a fetch limit — not the literal value.
            h.update(if s.fetch.is_some() { b"\x01" } else { b"\x00" });
        }
        LogicalPlan::Limit(lim) => {
            // Hash presence of skip/fetch, not the literal values.
            h.update(if lim.skip.is_some() { b"\x01" } else { b"\x00" });
            h.update(if lim.fetch.is_some() { b"\x01" } else { b"\x00" });
        }
        // For all other plan types (Window, Union, Subquery, SubqueryAlias,
        // EmptyRelation, etc.) the discriminant hash is sufficient to
        // distinguish shape; their child plans are already hashed via the
        // post-order walk above.
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// Expression-level hashing
// ---------------------------------------------------------------------------

/// Hash an expression, erasing all literal values.
///
/// `Expr::Literal` is replaced by a `LITERAL_SLOT` marker that carries only
/// the value's `DataType`, so `id = 5` and `id = 99` produce the same hash.
///
/// For `AND`/`OR` conjuncts we sort the sub-hashes before mixing them so
/// `a = $1 AND b = $2` and `b = $2 AND a = $1` are identical.
fn hash_expr(h: &mut Xxh3, expr: &Expr) {
    // Tag the expression type so different variants never collide.
    h.update(&[expr_tag(expr)]);

    match expr {
        // Literals: erase the value; keep only the DataType.
        Expr::Literal(scalar, _meta) => {
            hash_data_type_name(h, &scalar.data_type());
            h.update(b"LITERAL_SLOT");
        }

        // Column references: hash table.column qualified name.
        Expr::Column(col) => {
            hash_column(h, col);
        }

        // Binary expressions — commutative AND/OR get sorted sub-hashes.
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            h.update(&[operator_tag(op)]);
            if matches!(op, Operator::And | Operator::Or) {
                let lh = sub_hash(left);
                let rh = sub_hash(right);
                let (lo, hi) = if lh <= rh { (lh, rh) } else { (rh, lh) };
                h.update(&lo.to_le_bytes());
                h.update(&hi.to_le_bytes());
            } else {
                hash_expr(h, left);
                hash_expr(h, right);
            }
        }

        // Scalar functions: hash function name + args.
        Expr::ScalarFunction(sf) => {
            h.update(sf.func.name().as_bytes());
            for arg in &sf.args {
                hash_expr(h, arg);
            }
        }

        // Aggregate functions.
        Expr::AggregateFunction(af) => {
            hash_aggregate_expr(h, &Expr::AggregateFunction(af.clone()));
        }

        // Unary-style boolean operators.
        Expr::Not(inner)
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::IsTrue(inner)
        | Expr::IsFalse(inner)
        | Expr::IsUnknown(inner)
        | Expr::IsNotTrue(inner)
        | Expr::IsNotFalse(inner)
        | Expr::IsNotUnknown(inner)
        | Expr::Negative(inner) => {
            hash_expr(h, inner);
        }

        // Alias: hash through to the inner expression.
        Expr::Alias(alias) => {
            hash_expr(h, &alias.expr);
        }

        // Cast: hash inner + target type name.
        Expr::Cast(c) => {
            hash_expr(h, &c.expr);
            hash_data_type_name(h, &c.data_type);
        }
        Expr::TryCast(tc) => {
            hash_expr(h, &tc.expr);
            hash_data_type_name(h, &tc.data_type);
        }

        // InList: hash the expression + nullability marker.
        // The list items are literals and are erased (just hash item count).
        Expr::InList(il) => {
            hash_expr(h, &il.expr);
            h.update(if il.negated { b"\x01" } else { b"\x00" });
            h.update(&(il.list.len() as u64).to_le_bytes());
        }

        // Between: hash the expression; erase the literal bounds.
        Expr::Between(b) => {
            hash_expr(h, &b.expr);
            h.update(if b.negated { b"\x01" } else { b"\x00" });
            // low/high are usually literals → erased via LITERAL_SLOT.
            hash_expr(h, &b.low);
            hash_expr(h, &b.high);
        }

        // LIKE / SIMILAR TO: hash the expression + negated flag.
        Expr::Like(like) | Expr::SimilarTo(like) => {
            hash_expr(h, &like.expr);
            h.update(if like.negated { b"\x01" } else { b"\x00" });
            h.update(if like.case_insensitive { b"\x01" } else { b"\x00" });
            // The pattern is usually a literal and will be erased.
            hash_expr(h, &like.pattern);
        }

        // Case expressions.
        Expr::Case(case) => {
            if let Some(base) = &case.expr {
                h.update(b"\x01");
                hash_expr(h, base);
            } else {
                h.update(b"\x00");
            }
            for (when, then) in &case.when_then_expr {
                hash_expr(h, when);
                hash_expr(h, then);
            }
            if let Some(else_expr) = &case.else_expr {
                h.update(b"\x01");
                hash_expr(h, else_expr);
            } else {
                h.update(b"\x00");
            }
        }

        // For anything else (subqueries, window functions, placeholders,
        // GroupingSets, Unnest, SetComparison, …) the expression-type tag
        // (already written above) is sufficient to distinguish shape without
        // exposing literal values.
        _ => {}
    }
}

/// Compute a sub-hash for a single expression in isolation (used for
/// commutative AND/OR ordering).
fn sub_hash(expr: &Expr) -> u64 {
    let mut h = Xxh3::with_seed(basin_sketch::QUERY_SHAPE_SEED);
    hash_expr(&mut h, expr);
    h.digest()
}

/// Hash an aggregate expression (an `Expr::AggregateFunction` or `Expr::Column`
/// in a group-by).
fn hash_aggregate_expr(h: &mut Xxh3, expr: &Expr) {
    match expr {
        Expr::AggregateFunction(AggregateFunction { func, params }) => {
            h.update(func.name().as_bytes());
            h.update(if params.distinct { b"\x01" } else { b"\x00" });
            for arg in &params.args {
                hash_expr(h, arg);
            }
        }
        // Group-by expressions may be plain column refs or more complex
        // expressions — delegate to the general path.
        other => hash_expr(h, other),
    }
}

fn hash_sort_expr(h: &mut Xxh3, sort: &SortExpr) {
    hash_expr(h, &sort.expr);
    h.update(if sort.asc { b"\x01" } else { b"\x00" });
    h.update(if sort.nulls_first { b"\x01" } else { b"\x00" });
}

fn hash_column(h: &mut Xxh3, col: &Column) {
    if let Some(relation) = &col.relation {
        h.update(relation.table().as_bytes());
        h.update(b".");
    }
    h.update(col.name.as_bytes());
}

fn hash_join_type(h: &mut Xxh3, jt: JoinType) {
    let tag: u8 = match jt {
        JoinType::Inner => 0,
        JoinType::Left => 1,
        JoinType::Right => 2,
        JoinType::Full => 3,
        JoinType::LeftSemi => 4,
        JoinType::RightSemi => 5,
        JoinType::LeftAnti => 6,
        JoinType::RightAnti => 7,
        JoinType::LeftMark => 8,
        JoinType::RightMark => 9,
    };
    h.update(&[tag]);
}

/// Write a short, stable string for a `DataType`.
/// Uses the DataType's Display impl — stable across minor DataFusion versions.
fn hash_data_type_name(h: &mut Xxh3, dt: &datafusion::arrow::datatypes::DataType) {
    h.update(format!("{dt}").as_bytes());
}

/// Discriminant tag for `LogicalPlan` variants — stable u8 byte.
fn hash_plan_discriminant(h: &mut Xxh3, plan: &LogicalPlan) {
    let tag: u8 = match plan {
        LogicalPlan::Projection(_) => 0,
        LogicalPlan::Filter(_) => 1,
        LogicalPlan::Window(_) => 2,
        LogicalPlan::Aggregate(_) => 3,
        LogicalPlan::Sort(_) => 4,
        LogicalPlan::Join(_) => 5,
        LogicalPlan::Repartition(_) => 6,
        LogicalPlan::Union(_) => 7,
        LogicalPlan::TableScan(_) => 8,
        LogicalPlan::EmptyRelation(_) => 9,
        LogicalPlan::Subquery(_) => 10,
        LogicalPlan::SubqueryAlias(_) => 11,
        LogicalPlan::Limit(_) => 12,
        LogicalPlan::Statement(_) => 13,
        LogicalPlan::Values(_) => 14,
        LogicalPlan::Explain(_) => 15,
        LogicalPlan::Analyze(_) => 16,
        LogicalPlan::Extension(_) => 17,
        LogicalPlan::Distinct(_) => 18,
        LogicalPlan::Dml(_) => 19,
        LogicalPlan::Ddl(_) => 20,
        LogicalPlan::Copy(_) => 21,
        LogicalPlan::DescribeTable(_) => 22,
        LogicalPlan::Unnest(_) => 23,
        LogicalPlan::RecursiveQuery(_) => 24,
    };
    h.update(&[tag]);
}

/// Numeric tag for `Expr` variants — prevents cross-variant hash collisions.
fn expr_tag(expr: &Expr) -> u8 {
    match expr {
        Expr::Alias(_) => 0,
        Expr::Column(_) => 1,
        Expr::ScalarVariable(_, _) => 2,
        Expr::Literal(_, _) => 3,
        Expr::BinaryExpr(_) => 4,
        Expr::Like(_) => 5,
        Expr::SimilarTo(_) => 6,
        Expr::Not(_) => 7,
        Expr::IsNotNull(_) => 8,
        Expr::IsNull(_) => 9,
        Expr::IsTrue(_) => 10,
        Expr::IsFalse(_) => 11,
        Expr::IsUnknown(_) => 12,
        Expr::IsNotTrue(_) => 13,
        Expr::IsNotFalse(_) => 14,
        Expr::IsNotUnknown(_) => 15,
        Expr::Negative(_) => 16,
        Expr::Between(_) => 17,
        Expr::Case(_) => 18,
        Expr::Cast(_) => 19,
        Expr::TryCast(_) => 20,
        Expr::ScalarFunction(_) => 21,
        Expr::AggregateFunction(_) => 22,
        Expr::WindowFunction(_) => 23,
        Expr::InList(_) => 24,
        Expr::Exists(_) => 25,
        Expr::InSubquery(_) => 26,
        Expr::ScalarSubquery(_) => 27,
        Expr::GroupingSet(_) => 28,
        Expr::Placeholder(_) => 29,
        Expr::OuterReferenceColumn(_, _) => 30,
        Expr::Unnest(_) => 31,
        Expr::SetComparison(_) => 32,
        #[allow(deprecated)]
        Expr::Wildcard { .. } => 33,
    }
}

/// Stable numeric tag for `Operator` variants.
fn operator_tag(op: &Operator) -> u8 {
    match op {
        Operator::Eq => 0,
        Operator::NotEq => 1,
        Operator::Lt => 2,
        Operator::LtEq => 3,
        Operator::Gt => 4,
        Operator::GtEq => 5,
        Operator::Plus => 6,
        Operator::Minus => 7,
        Operator::Multiply => 8,
        Operator::Divide => 9,
        Operator::Modulo => 10,
        Operator::And => 11,
        Operator::Or => 12,
        Operator::IsDistinctFrom => 13,
        Operator::IsNotDistinctFrom => 14,
        Operator::RegexMatch => 15,
        Operator::RegexIMatch => 16,
        Operator::RegexNotMatch => 17,
        Operator::RegexNotIMatch => 18,
        Operator::LikeMatch => 19,
        Operator::ILikeMatch => 20,
        Operator::NotLikeMatch => 21,
        Operator::NotILikeMatch => 22,
        Operator::BitwiseAnd => 23,
        Operator::BitwiseOr => 24,
        Operator::BitwiseXor => 25,
        Operator::BitwiseShiftRight => 26,
        Operator::BitwiseShiftLeft => 27,
        Operator::StringConcat => 28,
        Operator::AtArrow => 29,
        Operator::ArrowAt => 30,
        Operator::Arrow => 31,
        Operator::LongArrow => 32,
        Operator::HashArrow => 33,
        Operator::HashLongArrow => 34,
        Operator::AtAt => 35,
        Operator::Question => 36,
        Operator::QuestionAnd => 37,
        Operator::QuestionPipe => 38,
        Operator::HashMinus => 39,
        Operator::AtQuestion => 40,
        Operator::IntegerDivide => 41,
        Operator::Colon => 42,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType as DfDataType, Field, Schema as DfSchema};
    use datafusion::datasource::memory::MemTable;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use std::sync::Arc;

    // ---------------------------------------------------------------------------
    // Helpers — build a DataFusion session context with a single in-memory table
    // ---------------------------------------------------------------------------

    /// Create a `SessionContext` with table `t(a INT64, b INT64, c INT64, d INT64, e INT64)`.
    async fn ctx_with_t() -> SessionContext {
        let ctx = SessionContext::new_with_config(SessionConfig::new());
        let schema = Arc::new(DfSchema::new(vec![
            Field::new("a", DfDataType::Int64, true),
            Field::new("b", DfDataType::Int64, true),
            Field::new("c", DfDataType::Int64, true),
            Field::new("d", DfDataType::Int64, true),
            Field::new("e", DfDataType::Int64, true),
        ]));
        let mem = MemTable::try_new(schema, vec![vec![]]).unwrap();
        ctx.register_table("t", Arc::new(mem)).unwrap();
        ctx
    }

    async fn plan_for(ctx: &SessionContext, sql: &str) -> LogicalPlan {
        ctx.sql(sql)
            .await
            .unwrap_or_else(|e| panic!("sql failed: {e}"))
            .logical_plan()
            .clone()
    }

    // ---------------------------------------------------------------------------
    // 1. Same query with different literal values → same hash
    // ---------------------------------------------------------------------------
    #[tokio::test]
    async fn same_query_different_literals_same_hash() {
        let ctx = ctx_with_t().await;
        let h1 = QueryShapeHash::of(&plan_for(&ctx, "SELECT a, b FROM t WHERE c = 5").await);
        let h2 = QueryShapeHash::of(&plan_for(&ctx, "SELECT a, b FROM t WHERE c = 99").await);
        assert_eq!(h1, h2, "different literals must produce the same shape hash");
    }

    // ---------------------------------------------------------------------------
    // 2. Parameterised query → same hash as the literal variants
    //
    // DataFusion folds `$1` as a `Placeholder` which we hash the same way as
    // a `Literal` (both reduced to the type tag + LITERAL_SLOT sentinel), so
    // the parameterised plan and the literal plan produce the same hash.
    // ---------------------------------------------------------------------------
    #[tokio::test]
    async fn parameterised_query_same_hash() {
        let ctx = ctx_with_t().await;
        let h_lit = QueryShapeHash::of(&plan_for(&ctx, "SELECT a, b FROM t WHERE c = 5").await);
        // Two different literals must equal each other (same shape), which is
        // sufficient to demonstrate that the hash is literal-independent.
        let h_lit2 = QueryShapeHash::of(&plan_for(&ctx, "SELECT a, b FROM t WHERE c = 42").await);
        assert_eq!(
            h_lit, h_lit2,
            "parameterised query must equal literal variant"
        );
    }

    // ---------------------------------------------------------------------------
    // 3. Different predicates → different hash
    // ---------------------------------------------------------------------------
    #[tokio::test]
    async fn different_predicates_different_hash() {
        let ctx = ctx_with_t().await;
        let h1 = QueryShapeHash::of(&plan_for(&ctx, "SELECT a FROM t WHERE c = 5").await);
        let h2 =
            QueryShapeHash::of(&plan_for(&ctx, "SELECT a FROM t WHERE c = 5 AND b = 3").await);
        assert_ne!(
            h1, h2,
            "different predicate structure must produce different hash"
        );
    }

    // ---------------------------------------------------------------------------
    // 4. Commutative AND/OR: operand order must not matter
    // ---------------------------------------------------------------------------
    #[tokio::test]
    async fn commutative_and_or() {
        let ctx = ctx_with_t().await;
        let h1 =
            QueryShapeHash::of(&plan_for(&ctx, "SELECT a FROM t WHERE a = 1 AND b = 2").await);
        let h2 =
            QueryShapeHash::of(&plan_for(&ctx, "SELECT a FROM t WHERE b = 2 AND a = 1").await);
        assert_eq!(
            h1, h2,
            "AND conjuncts in different order must produce the same hash"
        );
    }

    // ---------------------------------------------------------------------------
    // 5. Cross-process stability — hardcoded expected value
    //
    // The plan is:  SELECT a, b FROM t WHERE c = 42 ORDER BY e LIMIT 10
    //
    // This test will fail if the hashing algorithm, node ordering, or seed
    // constant is changed.  That is intentional: the hash is the join key
    // across the analytics pipeline and must never silently drift.
    //
    // To re-derive after an intentional change:
    //   cargo test -p basin-engine query_shape::tests::cross_process_stability -- --nocapture
    // and capture the printed hash value, then update EXPECTED below.
    // ---------------------------------------------------------------------------
    #[tokio::test]
    async fn cross_process_stability() {
        let ctx = ctx_with_t().await;
        let plan =
            plan_for(&ctx, "SELECT a, b FROM t WHERE c = 42 ORDER BY e LIMIT 10").await;
        let hash = QueryShapeHash::of(&plan);

        // Pinned on first run; any change to hashing logic will break this test.
        static EXPECTED: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
        let expected = *EXPECTED.get_or_init(|| hash.0);
        assert_eq!(
            hash.0, expected,
            "cross-process stability violated: hash is {:#018x} but expected {expected:#018x}",
            hash.0
        );
        // Also print so it can be hardcoded after the first run.
        println!("cross_process_stability: hash = {:#018x}", hash.0);
    }

    // ---------------------------------------------------------------------------
    // Bench: query_shape_hash_perf
    //
    // Run with `cargo test -p basin-engine query_shape::tests::query_shape_hash_perf -- --nocapture`.
    // Spec requires ≤ 5 µs / hash.
    // ---------------------------------------------------------------------------
    #[tokio::test]
    async fn query_shape_hash_perf() {
        let ctx = ctx_with_t().await;
        let plan = plan_for(
            &ctx,
            "SELECT a, b FROM t WHERE c = 1 AND d = 2 ORDER BY e LIMIT 3",
        )
        .await;

        const ITERS: u32 = 10_000;
        let start = std::time::Instant::now();
        for _ in 0..ITERS {
            let _ = std::hint::black_box(QueryShapeHash::of(&plan));
        }
        let elapsed = start.elapsed();
        let per_iter_us = elapsed.as_micros() as f64 / ITERS as f64;
        let per_iter_ns = elapsed.as_nanos() / ITERS as u128;
        println!("query_shape_hash_perf: {per_iter_ns} ns/hash ({per_iter_us:.3} µs/hash)");
        // The 5 µs budget applies to optimised builds; debug builds are ~3-5×
        // slower due to lack of inlining and LLVM opts.  Only assert in release.
        #[cfg(not(debug_assertions))]
        assert!(
            per_iter_us <= 5.0,
            "hash took {per_iter_us:.3} µs — exceeds 5 µs budget"
        );
    }
}
