//! Rewrite enum-typed column references in `ORDER BY` and ordering
//! comparisons (`<`, `>`, `<=`, `>=`, `BETWEEN`) to ordinal-yielding
//! `CASE` expressions so the runtime sort/range follows PG's
//! declaration-order semantics rather than Arrow's lexicographic
//! `Utf8` compare.
//!
//! Storage stays unchanged: an enum-typed column is still Arrow `Utf8`
//! tagged with `BASIN_ENUM_TYPE=<name>`. Only the SQL the planner sees
//! is rewritten — for those positions where order matters we wrap a
//! bare column reference in
//!
//! ```sql
//! CASE
//!   WHEN <col> = 'label0' THEN 0
//!   WHEN <col> = 'label1' THEN 1
//!   ...
//! END
//! ```
//!
//! Equality (`=`, `!=`) and `IN (...)` lists stay on the label string
//! — they don't need ordering, and rewriting them would change nothing.
//!
//! The rewrite is best-effort. When the column-to-table mapping cannot
//! be resolved statically (joins with the same column name in multiple
//! tables, derived tables, CTEs, table aliases that mask the FROM
//! relation), we silently skip the rewrite for that expression. The
//! query still runs; it just falls back to label-string comparison for
//! the unresolvable cases. Resolving more shapes is a future
//! improvement.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::Schema;
use basin_catalog::{Catalog, EnumTypeDef};
use basin_common::{Result, TableName, TenantId};
use sqlparser::ast::{
    BinaryOperator, Expr, Ident, Query, Select, SetExpr, Statement, TableFactor, Value,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::types::BASIN_ENUM_TYPE_KEY;

/// Walk the parsed SQL once and rewrite every column reference to an
/// enum-typed column that appears in an ordering position (`ORDER BY`
/// expr, `<`/`>`/`<=`/`>=` operands, `BETWEEN` operands) to the
/// equivalent ordinal `CASE` expression.
///
/// Returns the rewritten SQL on success. Returns the input untouched
/// when the statement is not a query, when no enum-typed columns are
/// referenced, or when sqlparser cannot parse the input (in which case
/// the caller's own parse pass will surface the error).
pub(crate) async fn rewrite_enum_ordering(
    catalog: &Arc<dyn Catalog>,
    tenant: &TenantId,
    sql: &str,
) -> Result<String> {
    // Cheap pre-gate: only Query statements have ORDER BY / WHERE
    // ranges. Non-Query statements (DDL, INSERT, etc.) bail before
    // any catalog hop.
    let dialect = PostgreSqlDialect {};
    let mut stmts = match Parser::parse_sql(&dialect, sql) {
        Ok(s) => s,
        Err(_) => return Ok(sql.to_string()),
    };
    if stmts.len() != 1 {
        return Ok(sql.to_string());
    }
    let stmt = &mut stmts[0];
    let Statement::Query(q) = stmt else {
        return Ok(sql.to_string());
    };

    let mut ctx = RewriteCtx {
        catalog,
        tenant,
        // (table, schema_columns) — looked up lazily and cached so
        // a query that references the same table twice only costs one
        // catalog hop.
        tables: HashMap::new(),
        // (enum_name, EnumTypeDef) — cached to amortise lookups across
        // multiple referenced columns of the same enum type.
        enums: HashMap::new(),
        any_rewrites: false,
    };
    rewrite_query(&mut ctx, q).await?;

    if !ctx.any_rewrites {
        return Ok(sql.to_string());
    }
    Ok(stmts[0].to_string())
}

/// Per-call traversal state. Holds the catalog handle plus two caches
/// so a query referencing the same table or enum type repeatedly only
/// pays one catalog hop per name.
struct RewriteCtx<'a> {
    catalog: &'a Arc<dyn Catalog>,
    tenant: &'a TenantId,
    /// `(unqualified table name) -> resolved (Schema, optional alias)`.
    /// `None` means resolution failed (the load_table call returned
    /// NotFound or the name isn't a TableName).
    tables: HashMap<String, Option<Arc<Schema>>>,
    /// `(enum type name) -> EnumTypeDef`. `None` means lookup failed.
    enums: HashMap<String, Option<EnumTypeDef>>,
    any_rewrites: bool,
}

impl<'a> RewriteCtx<'a> {
    async fn load_schema(&mut self, table: &str) -> Option<Arc<Schema>> {
        if let Some(cached) = self.tables.get(table) {
            return cached.clone();
        }
        let resolved = match TableName::new(table.to_string()) {
            Ok(t) => match self.catalog.load_table(self.tenant, &t).await {
                Ok(meta) => Some(meta.schema),
                Err(_) => None,
            },
            Err(_) => None,
        };
        self.tables.insert(table.to_string(), resolved.clone());
        resolved
    }

    async fn lookup_enum(&mut self, name: &str) -> Option<EnumTypeDef> {
        if let Some(cached) = self.enums.get(name) {
            return cached.clone();
        }
        let resolved = self.catalog.lookup_enum_type(self.tenant, name).await;
        self.enums.insert(name.to_string(), resolved.clone());
        resolved
    }
}

/// Resolved single FROM relation for a Select. We only build this when
/// the Select has exactly one FROM with no joins and the relation is a
/// bare `TableFactor::Table`. Anything else (joins, derived tables,
/// table functions, UNNEST, ...) leaves `None` so the column-to-table
/// resolution silently bails.
struct FromBinding {
    /// Unqualified table name as written in FROM.
    table: String,
    /// Optional alias the user gave the table (`FROM orders o` →
    /// alias = "o"). When set, qualified column refs `o.status` are
    /// resolved against this binding.
    alias: Option<String>,
}

fn extract_from_binding(sel: &Select) -> Option<FromBinding> {
    if sel.from.len() != 1 {
        return None;
    }
    let twj = &sel.from[0];
    if !twj.joins.is_empty() {
        return None;
    }
    match &twj.relation {
        TableFactor::Table { name, alias, args, .. } => {
            if args.is_some() {
                // table-valued function — skip.
                return None;
            }
            if name.0.len() != 1 {
                return None;
            }
            Some(FromBinding {
                table: name.0[0].value.clone(),
                alias: alias.as_ref().map(|a| a.name.value.clone()),
            })
        }
        _ => None,
    }
}

/// Look up the enum-type metadata on `column` of `binding`'s table.
/// Returns the EnumTypeDef when the column exists on that table and
/// carries `BASIN_ENUM_TYPE`; returns `None` otherwise.
async fn resolve_enum_column(
    ctx: &mut RewriteCtx<'_>,
    binding: &FromBinding,
    qual: Option<&str>,
    column: &str,
) -> Option<EnumTypeDef> {
    // Qualified column ref: the qualifier must match the table name or
    // the alias. Mismatch → not our column, bail (no rewrite).
    if let Some(q) = qual {
        let matches = q.eq_ignore_ascii_case(&binding.table)
            || binding
                .alias
                .as_ref()
                .map(|a| q.eq_ignore_ascii_case(a))
                .unwrap_or(false);
        if !matches {
            return None;
        }
    }
    let schema = ctx.load_schema(&binding.table).await?;
    // Find the field. Field names round-trip via DataFusion's lower-
    // casing rules so a case-insensitive compare is safe.
    let field = schema
        .fields()
        .iter()
        .find(|f| f.name().eq_ignore_ascii_case(column))?;
    let enum_name = field.metadata().get(BASIN_ENUM_TYPE_KEY)?;
    ctx.lookup_enum(enum_name).await
}

/// Build the `CASE WHEN col = 'label0' THEN 0 ... END` expression
/// equivalent to the column's ordinal index in the enum's declared
/// label list. The resulting Expr's textual form is what the planner
/// sees; DataFusion folds the CASE into a const-time scalar evaluation
/// at sort.
fn build_case_for_enum(col_expr: &Expr, def: &EnumTypeDef) -> Expr {
    let mut conditions = Vec::with_capacity(def.labels.len());
    let mut results = Vec::with_capacity(def.labels.len());
    for (i, label) in def.labels.iter().enumerate() {
        conditions.push(Expr::BinaryOp {
            left: Box::new(col_expr.clone()),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::Value(Value::SingleQuotedString(label.clone()))),
        });
        results.push(Expr::Value(Value::Number(i.to_string(), false)));
    }
    Expr::Case {
        operand: None,
        conditions,
        results,
        else_result: None,
    }
}

/// Pull a `(qualifier, column)` pair from an `Expr` if it's a bare
/// identifier or a one-or-two-part compound identifier. Returns `None`
/// for anything else (function calls, arithmetic, sub-queries, etc.).
fn as_column_ref(e: &Expr) -> Option<(Option<String>, String)> {
    match e {
        Expr::Identifier(Ident { value, .. }) => Some((None, value.clone())),
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            Some((Some(parts[0].value.clone()), parts[1].value.clone()))
        }
        _ => None,
    }
}

async fn rewrite_query(ctx: &mut RewriteCtx<'_>, q: &mut Query) -> Result<()> {
    // Recurse into the query body first — a SELECT inside a SetExpr or
    // a subquery may itself have ORDER BY / range filters worth
    // rewriting. We do NOT descend into derived-table subqueries; the
    // outer Select's FROM binding is what scopes name resolution at
    // this level.
    rewrite_set_expr(ctx, &mut q.body).await?;
    // Rewrite ORDER BY at the top of the query (PG attaches ORDER BY
    // to Query, not Select).
    if let Some(order_by) = q.order_by.as_mut() {
        // Resolve the FROM binding from the body (only a single Select
        // with one bare table is supported — see [`extract_from_binding`]).
        if let SetExpr::Select(sel) = q.body.as_mut() {
            if let Some(binding) = extract_from_binding(sel) {
                for obe in order_by.exprs.iter_mut() {
                    rewrite_expr_for_ordering(ctx, &binding, &mut obe.expr).await?;
                }
            }
        }
    }
    Ok(())
}

async fn rewrite_set_expr(ctx: &mut RewriteCtx<'_>, set: &mut SetExpr) -> Result<()> {
    match set {
        SetExpr::Select(sel) => rewrite_select(ctx, sel).await,
        // Set ops / sub-queries are out of scope for v0.1 best-effort;
        // they fall through to label-string compare.
        _ => Ok(()),
    }
}

async fn rewrite_select(ctx: &mut RewriteCtx<'_>, sel: &mut Select) -> Result<()> {
    let binding = match extract_from_binding(sel) {
        Some(b) => b,
        None => return Ok(()),
    };
    if let Some(filter) = sel.selection.as_mut() {
        rewrite_expr_in_predicate(ctx, &binding, filter).await?;
    }
    if let Some(having) = sel.having.as_mut() {
        rewrite_expr_in_predicate(ctx, &binding, having).await?;
    }
    Ok(())
}

/// Rewrite ordering comparisons inside a boolean predicate. Walks the
/// expression tree; for every `<`, `>`, `<=`, `>=`, or `BETWEEN` whose
/// operands include an enum column on at least one side, swaps the
/// column for its ordinal CASE *and* swaps the matching literal on the
/// other side for its ordinal integer (so we don't end up comparing an
/// int to a Utf8 literal).
fn rewrite_expr_in_predicate<'a>(
    ctx: &'a mut RewriteCtx<'_>,
    binding: &'a FromBinding,
    expr: &'a mut Expr,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
    Box::pin(async move {
        match expr {
            Expr::BinaryOp { left, op, right } => match op {
                BinaryOperator::Lt
                | BinaryOperator::Gt
                | BinaryOperator::LtEq
                | BinaryOperator::GtEq => {
                    rewrite_ordering_pair(ctx, binding, left, right).await
                }
                BinaryOperator::And | BinaryOperator::Or => {
                    rewrite_expr_in_predicate(ctx, binding, left).await?;
                    rewrite_expr_in_predicate(ctx, binding, right).await?;
                    Ok(())
                }
                // Equality / inequality / arithmetic / etc. don't need
                // ordinal compare.
                _ => Ok(()),
            },
            Expr::Between {
                expr: e,
                low,
                high,
                ..
            } => {
                // BETWEEN: if the column is enum-typed, both bounds
                // must rewrite from label literal to ordinal.
                let col = as_column_ref(e.as_ref());
                let def = match col {
                    Some((qual, c)) => {
                        resolve_enum_column(ctx, binding, qual.as_deref(), &c).await
                    }
                    None => None,
                };
                if let Some(def) = def {
                    let case = build_case_for_enum(e.as_ref(), &def);
                    **e = case;
                    rewrite_label_to_ordinal(low, &def);
                    rewrite_label_to_ordinal(high, &def);
                    ctx.any_rewrites = true;
                }
                Ok(())
            }
            Expr::Nested(inner) => rewrite_expr_in_predicate(ctx, binding, inner).await,
            Expr::IsFalse(e)
            | Expr::IsNotFalse(e)
            | Expr::IsTrue(e)
            | Expr::IsNotTrue(e)
            | Expr::IsNull(e)
            | Expr::IsNotNull(e) => rewrite_expr_in_predicate(ctx, binding, e).await,
            _ => Ok(()),
        }
    })
}

/// Inspect both operands of an ordering comparison. If either is an
/// enum column reference, rewrite the column to its ordinal CASE and
/// the *other* side (when it's a string literal matching one of the
/// enum's labels) to the corresponding ordinal integer literal. If
/// neither is an enum column, no rewrite. If both are enum columns of
/// the same type (rare but legal), both rewrite.
async fn rewrite_ordering_pair(
    ctx: &mut RewriteCtx<'_>,
    binding: &FromBinding,
    left: &mut Expr,
    right: &mut Expr,
) -> Result<()> {
    let left_def = match as_column_ref(left) {
        Some((q, c)) => resolve_enum_column(ctx, binding, q.as_deref(), &c).await,
        None => None,
    };
    let right_def = match as_column_ref(right) {
        Some((q, c)) => resolve_enum_column(ctx, binding, q.as_deref(), &c).await,
        None => None,
    };

    if let Some(def) = left_def.as_ref() {
        let case = build_case_for_enum(left, def);
        *left = case;
        rewrite_label_to_ordinal(right, def);
        ctx.any_rewrites = true;
    }
    if let Some(def) = right_def.as_ref() {
        let case = build_case_for_enum(right, def);
        *right = case;
        rewrite_label_to_ordinal(left, def);
        ctx.any_rewrites = true;
    }
    Ok(())
}

/// If `expr` is a single-quoted string literal whose value matches one
/// of `def`'s labels, replace it in place with the matching ordinal
/// integer literal. No-op otherwise (the planner will surface the
/// mismatch — it isn't our place to invent ordinals for unknown
/// labels).
fn rewrite_label_to_ordinal(expr: &mut Expr, def: &EnumTypeDef) {
    if let Expr::Value(Value::SingleQuotedString(s)) = expr {
        if let Some(idx) = def.labels.iter().position(|l| l == s) {
            *expr = Expr::Value(Value::Number(idx.to_string(), false));
        }
    }
}

/// If `expr` is a column reference to an enum-typed column on
/// `binding`'s table, replace it in place with the ordinal CASE
/// expression. No-op for any other shape.
async fn rewrite_expr_for_ordering(
    ctx: &mut RewriteCtx<'_>,
    binding: &FromBinding,
    expr: &mut Expr,
) -> Result<()> {
    let Some((qual, col)) = as_column_ref(expr) else {
        return Ok(());
    };
    let Some(def) = resolve_enum_column(ctx, binding, qual.as_deref(), &col).await else {
        return Ok(());
    };
    // Build the replacement using a clone of the original column-ref
    // expression so each `WHEN col = 'lbl' ...` arm references the
    // exact identifier the user wrote (preserving qualification).
    let case = build_case_for_enum(expr, &def);
    *expr = case;
    ctx.any_rewrites = true;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow_schema::{DataType, Field};
    use basin_catalog::InMemoryCatalog;
    use std::collections::HashMap;

    async fn fixture() -> (Arc<dyn Catalog>, TenantId) {
        let cat: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
        let tenant = TenantId::new();
        cat.register_enum_type(EnumTypeDef {
            tenant,
            name: "status".into(),
            labels: vec![
                "pending".into(),
                "paid".into(),
                "shipped".into(),
                "cancelled".into(),
            ],
        })
        .await
        .unwrap();
        // Build a schema for `orders` whose `status` column is enum-tagged.
        let mut md = HashMap::new();
        md.insert(BASIN_ENUM_TYPE_KEY.to_string(), "status".to_string());
        let status_field = Field::new("status", DataType::Utf8, true).with_metadata(md);
        let id_field = Field::new("id", DataType::Int64, false);
        let name_field = Field::new("name", DataType::Utf8, true);
        let schema = Schema::new(vec![id_field, status_field, name_field]);
        let table = TableName::new("orders".to_string()).unwrap();
        cat.create_table(&tenant, &table, &schema).await.unwrap();
        // A second table with no enum column, for negative tests.
        let plain = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("note", DataType::Utf8, true),
        ]);
        let plain_t = TableName::new("notes".to_string()).unwrap();
        cat.create_table(&tenant, &plain_t, &plain).await.unwrap();
        (cat, tenant)
    }

    #[tokio::test]
    async fn simple_order_by_enum_col_rewrites_to_case() {
        let (cat, t) = fixture().await;
        let out = rewrite_enum_ordering(&cat, &t, "SELECT * FROM orders ORDER BY status")
            .await
            .unwrap();
        assert!(
            out.to_uppercase().contains("CASE WHEN"),
            "expected CASE rewrite, got: {out}"
        );
        assert!(out.contains("'pending'"));
        assert!(out.contains("'cancelled'"));
    }

    #[tokio::test]
    async fn equality_does_not_rewrite() {
        let (cat, t) = fixture().await;
        let sql = "SELECT * FROM orders WHERE status = 'paid'";
        let out = rewrite_enum_ordering(&cat, &t, sql).await.unwrap();
        // No CASE injected — equality comparison stays on the label.
        assert!(!out.to_uppercase().contains("CASE WHEN"), "got: {out}");
    }

    #[tokio::test]
    async fn range_comparison_rewrites() {
        let (cat, t) = fixture().await;
        let sql = "SELECT * FROM orders WHERE status > 'pending'";
        let out = rewrite_enum_ordering(&cat, &t, sql).await.unwrap();
        assert!(out.to_uppercase().contains("CASE WHEN"), "got: {out}");
    }

    #[tokio::test]
    async fn non_enum_column_unchanged() {
        let (cat, t) = fixture().await;
        let sql = "SELECT * FROM notes ORDER BY note";
        let out = rewrite_enum_ordering(&cat, &t, sql).await.unwrap();
        assert!(!out.to_uppercase().contains("CASE WHEN"), "got: {out}");
    }

    #[tokio::test]
    async fn unresolvable_column_falls_back() {
        // A query with a JOIN: extract_from_binding sees `joins.is_empty()`
        // is false and returns None. No rewrite, no error.
        let (cat, t) = fixture().await;
        let sql = "SELECT o.id FROM orders o JOIN notes n ON o.id = n.id ORDER BY status";
        let out = rewrite_enum_ordering(&cat, &t, sql).await.unwrap();
        assert!(!out.to_uppercase().contains("CASE WHEN"), "got: {out}");
    }
}
