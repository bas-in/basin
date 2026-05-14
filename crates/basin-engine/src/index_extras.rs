//! Helpers for extended `CREATE INDEX` syntax accepted in v0.1.
//!
//! Basin does not yet materialise secondary-index files (B-tree / GIN / etc.).
//! Every query against an indexed table still falls through to a full scan.
//! These helpers normalise all supported `CREATE INDEX` variants into the
//! name + column-list representation the catalog understands, logging a
//! one-time notice so operators know the declaration is metadata-only.
//!
//! Real enforcement is tracked as item B1 in WEDGE.md.

use sqlparser::ast::{CreateIndex, Expr, OrderByExpr};
use tracing::info;

/// Classification of a parsed index column.
#[derive(Debug, Clone)]
pub(crate) enum IndexColumn {
    /// A simple identifier: `CREATE INDEX … (col)`.
    Bare(String),
    /// A function-call or other expression: `CREATE INDEX … (LOWER(col))`.
    Expression(String),
}

impl IndexColumn {
    /// Return the string representation suitable for catalog storage.
    /// Bare columns are returned as-is; expressions are prefixed with
    /// `"expr:"` so callers can detect them without re-parsing.
    pub(crate) fn as_catalog_str(&self) -> String {
        match self {
            IndexColumn::Bare(s) => s.clone(),
            IndexColumn::Expression(s) => format!("expr:{s}"),
        }
    }

    pub(crate) fn is_expression(&self) -> bool {
        matches!(self, IndexColumn::Expression(_))
    }
}

/// Parse the `OrderByExpr` list from a `CreateIndex` node into a vec of
/// [`IndexColumn`] variants.  ASC / DESC / NULLS FIRST / NULLS LAST are
/// silently ignored — v0.1 storage is order-agnostic.
pub(crate) fn parse_index_columns(ci: &CreateIndex) -> Vec<IndexColumn> {
    ci.columns
        .iter()
        .map(|ob: &OrderByExpr| match &ob.expr {
            Expr::Identifier(ident) => IndexColumn::Bare(ident.value.clone()),
            Expr::CompoundIdentifier(parts) if parts.len() == 1 => {
                IndexColumn::Bare(parts[0].value.clone())
            }
            other => IndexColumn::Expression(other.to_string()),
        })
        .collect()
}

/// Emit a single `tracing::info!` notice that secondary indexes are accepted
/// but not yet enforced.  Called once per `CREATE INDEX` that is
/// metadata-only so that operators searching logs can find the caveat.
pub(crate) fn log_metadata_only_notice(index_name: &str, table_name: &str) {
    info!(
        index = %index_name,
        table = %table_name,
        "secondary indexes accepted but not yet enforced; \
         index lookups fall through to scan."
    );
}

/// Emit a notice when a UNIQUE index is accepted without enforcement.
pub(crate) fn log_unique_notice(index_name: &str) {
    info!(
        index = %index_name,
        "CREATE UNIQUE INDEX: uniqueness is not enforced in v0.1 \
         (declare UNIQUE on the table for enforcement); accepted as metadata."
    );
}

/// Emit a notice when a partial-index predicate is accepted as metadata.
pub(crate) fn log_partial_index_notice(index_name: &str, predicate: &str) {
    info!(
        index = %index_name,
        predicate = %predicate,
        "partial index WHERE predicate accepted as metadata but not enforced in v0.1."
    );
}

/// Emit a notice when a non-default access method is remapped to B-tree.
pub(crate) fn log_using_notice(index_name: &str, method: &str) {
    info!(
        index = %index_name,
        method = %method,
        "CREATE INDEX USING: access method accepted but treated as B-tree internally in v0.1.",
    );
}

/// Emit a notice when an expression index column is accepted as metadata.
pub(crate) fn log_expression_column_notice(index_name: &str, expr: &str) {
    info!(
        index = %index_name,
        expr = %expr,
        "expression index column accepted as metadata but not enforced in v0.1."
    );
}

/// Emit a notice when an INCLUDE covering-column list is accepted as metadata.
pub(crate) fn log_include_notice(index_name: &str, cols: &[String]) {
    info!(
        index = %index_name,
        include_cols = %cols.join(", "),
        "CREATE INDEX … INCLUDE (covering cols) accepted as metadata in v0.1; \
         covering-index lookups still fall through to scan."
    );
}
