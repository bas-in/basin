//! Per-tenant `CREATE DOMAIN` catalog.
//!
//! Customers will write
//!
//! ```sql
//! CREATE DOMAIN positive_int AS INT CHECK (VALUE > 0);
//! DROP DOMAIN positive_int;
//! ```
//!
//! and use the named domain in column declarations:
//!
//! ```sql
//! CREATE TABLE orders (id BIGINT, quantity positive_int);
//! ```
//!
//! once the DDL surface lands; this module ships the catalog row, the
//! lookup primitive, and validation. The engine maps a domain-typed
//! column to the underlying base Arrow type plus a
//! `BASIN_DOMAIN=<name>` field metadata marker so the INSERT path can
//! enforce the `CHECK` predicate against the row's value before commit.
//!
//! The catalog is a single shared `HashMap<(TenantId, String),
//! DomainDef>` — same shape as [`crate::functions`] — so per-tenant
//! cost stays `O(bytes)` with no per-tenant heavy resource.

use serde::{Deserialize, Serialize};

use basin_common::TenantId;

use crate::functions::SqlArgType;

/// Field-metadata key set on every Arrow column whose declared type is
/// a user-defined domain. The value is the catalog-side
/// `DomainDef::name`. The engine uses this on INSERT to evaluate the
/// CHECK predicate and on `drop_domain` to decide whether the domain
/// is still referenced.
pub const BASIN_DOMAIN_KEY: &str = "BASIN_DOMAIN";

/// Catalog row for a `CREATE DOMAIN` declaration. Stored per-tenant;
/// two tenants registering the same domain name are independent rows.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DomainDef {
    pub tenant: TenantId,
    /// Unqualified domain name within the tenant.
    pub name: String,
    /// Underlying PG-shape base type (`INT`, `TEXT`, `BOOLEAN`, …). The
    /// engine maps this to an Arrow `DataType` at column-resolution
    /// time, the same way it maps a bare-keyword column type.
    pub base_type: SqlArgType,
    /// Raw SQL text of the `CHECK (VALUE …)` predicate. `None` means
    /// the domain is a pure type alias with no constraint. The
    /// substring is the parenthesised expression with the outer parens
    /// stripped — the engine re-parses it on each INSERT, substitutes
    /// the row's column value for `VALUE`, and rejects rows where the
    /// expression evaluates to false (or NULL — PG treats NULL CHECK
    /// as a violation in domains).
    pub check_predicate: Option<String>,
}

/// Errors specific to domain registration. Mapped to
/// [`basin_common::BasinError`] at the catalog API boundary.
#[derive(Debug)]
pub enum DomainError {
    /// Domain with that name already exists for this tenant.
    Duplicate,
    /// `drop_domain` was called on a name that isn't registered.
    NotFound,
    /// `CHECK (...)` predicate failed to parse as a SQL expression.
    InvalidPredicate(String),
}

/// Validate a domain at `CREATE DOMAIN` time. The `CHECK` predicate
/// (if present) must parse as a SQL expression.
pub fn validate_new(def: &DomainDef) -> Result<(), DomainError> {
    if let Some(p) = &def.check_predicate {
        let dialect = sqlparser::dialect::PostgreSqlDialect {};
        let mut parser = sqlparser::parser::Parser::new(&dialect)
            .try_with_sql(p)
            .map_err(|e| DomainError::InvalidPredicate(format!("parse error: {e}")))?;
        parser
            .parse_expr()
            .map_err(|e| DomainError::InvalidPredicate(format!("parse error: {e}")))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn def(check: Option<&str>) -> DomainDef {
        DomainDef {
            tenant: TenantId::new(),
            name: "d".into(),
            base_type: SqlArgType::Int,
            check_predicate: check.map(|s| s.to_string()),
        }
    }

    #[test]
    fn validate_accepts_no_check() {
        assert!(validate_new(&def(None)).is_ok());
    }

    #[test]
    fn validate_accepts_simple_predicate() {
        assert!(validate_new(&def(Some("VALUE > 0"))).is_ok());
    }

    #[test]
    fn validate_accepts_compound_predicate() {
        assert!(validate_new(&def(Some("VALUE > 0 AND VALUE < 100"))).is_ok());
    }

    #[test]
    fn validate_rejects_garbage_predicate() {
        assert!(matches!(
            validate_new(&def(Some("@@@"))),
            Err(DomainError::InvalidPredicate(_))
        ));
    }
}
