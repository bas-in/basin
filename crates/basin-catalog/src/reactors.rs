//! Per-tenant SQL-bodied reactor catalog.
//!
//! Customers will eventually write
//!
//! ```sql
//! ALTER TABLE orders REACT ON UPDATE
//!   WHEN (NEW.status = 'paid' AND OLD.status != 'paid')
//!   EXECUTE INSERT INTO billing_events (order_id, ts) VALUES (NEW.id, now());
//! ```
//!
//! once the DDL surface lands; this module ships the catalog row, the
//! lookup primitive, and validation. The engine-side `ReactorSink`
//! (`crates/basin-engine/src/reactor_sink.rs`) consumes [`ReactorDef`]
//! lookups inside its `ChangeEventSink::publish` impl.
//!
//! The catalog is a single shared `HashMap<(TenantId, String),
//! ReactorDef>` keyed on a composite identifier — same shape as
//! [`crate::functions`] — so per-tenant cost stays `O(bytes)` with no
//! per-tenant heavy resource. The composite key is
//! `<table>:<reactor_name>` so two tables in the same tenant can register
//! reactors with the same name (matching PG's per-table trigger
//! namespace).

use bitflags::bitflags;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use basin_common::{ChangeOp, TableName, TenantId};

bitflags! {
    /// Mutation ops a reactor matches. Bitset so the SQL surface's
    /// `ON INSERT OR UPDATE` form maps to a single field.
    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
    pub struct ReactorOps: u8 {
        const INSERT = 0b001;
        const UPDATE = 0b010;
        const DELETE = 0b100;
    }
}

// Serde for `ReactorOps` is hand-rolled as the underlying `u8` so the
// `ReactorDef` JSONB payload the future Postgres backend will store is
// stable across bitflags-crate versions.
impl Serialize for ReactorOps {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        self.bits().serialize(s)
    }
}

impl<'de> Deserialize<'de> for ReactorOps {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let bits = u8::deserialize(d)?;
        Self::from_bits(bits)
            .ok_or_else(|| serde::de::Error::custom(format!("invalid ReactorOps bits: {bits}")))
    }
}

impl ReactorOps {
    /// Convenience: every op.
    pub fn all_ops() -> Self {
        Self::INSERT | Self::UPDATE | Self::DELETE
    }

    /// True iff this bitset includes `op`.
    pub fn matches(&self, op: ChangeOp) -> bool {
        match op {
            ChangeOp::Insert => self.contains(Self::INSERT),
            ChangeOp::Update => self.contains(Self::UPDATE),
            ChangeOp::Delete => self.contains(Self::DELETE),
        }
    }
}

/// Catalog row for a registered reactor. Stored per-tenant, scoped to
/// one table.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReactorDef {
    pub tenant: TenantId,
    pub table: TableName,
    /// Unqualified reactor name, unique per `(tenant, table)`. Validated
    /// to be a SQL identifier.
    pub name: String,
    /// Bitset of `INSERT` / `UPDATE` / `DELETE` ops the reactor fires
    /// on. Empty bitsets are rejected at registration.
    pub ops: ReactorOps,
    /// Raw SQL text of the `WHEN (...)` predicate. `None` matches every
    /// row (no predicate). The engine substitutes `NEW.col` / `OLD.col`
    /// / `TG_OP` / `TG_TABLE_NAME` references at evaluation time.
    pub when_predicate: Option<String>,
    /// Raw SQL of the `EXECUTE` statement. v0.1 accepts a single
    /// statement (INSERT / UPDATE / DELETE / SELECT). Multi-statement
    /// bodies are rejected — that's 5.11.F territory.
    pub body: String,
}

/// Errors specific to reactor registration. Mapped to
/// [`basin_common::BasinError`] at the catalog API boundary.
#[derive(Debug)]
pub enum ReactorError {
    /// Reactor name collides with an existing reactor on the same
    /// `(tenant, table)`.
    Duplicate,
    /// Body failed to parse as a single SQL statement.
    InvalidBody(String),
    /// `WHEN` predicate failed to parse as a SQL expression.
    InvalidPredicate(String),
    /// `ops` bitset was empty — no event would ever match.
    NoOps,
    /// Body parsed as multiple statements; v0.1 only accepts one.
    MultiStatementBody,
    /// Reactor named in a `drop_reactor` call doesn't exist.
    NotFound,
}

/// Parse + validate the body. Returns `Ok` if the body is exactly one
/// SQL statement of an acceptable kind. Used by the catalog at
/// registration time.
pub fn validate_body(body: &str) -> Result<(), ReactorError> {
    let dialect = sqlparser::dialect::PostgreSqlDialect {};
    let stmts = sqlparser::parser::Parser::parse_sql(&dialect, body)
        .map_err(|e| ReactorError::InvalidBody(format!("parse error: {e}")))?;
    if stmts.is_empty() {
        return Err(ReactorError::InvalidBody("body is empty".into()));
    }
    if stmts.len() > 1 {
        return Err(ReactorError::MultiStatementBody);
    }
    use sqlparser::ast::Statement;
    match &stmts[0] {
        Statement::Insert(_)
        | Statement::Update { .. }
        | Statement::Delete(_)
        | Statement::Query(_) => Ok(()),
        other => Err(ReactorError::InvalidBody(format!(
            "reactor body must be INSERT / UPDATE / DELETE / SELECT, got: {other}"
        ))),
    }
}

/// Parse + validate the `WHEN (...)` predicate. Returns `Ok` if the
/// expression parses; the engine evaluates it against the event's
/// `before` / `after` payloads at fire time. The placeholder names
/// `NEW`, `OLD`, `TG_OP`, `TG_TABLE_NAME` referenced in the predicate
/// are sqlparser identifiers and parse cleanly without engine
/// participation.
pub fn validate_predicate(predicate: &str) -> Result<(), ReactorError> {
    let dialect = sqlparser::dialect::PostgreSqlDialect {};
    let mut parser = sqlparser::parser::Parser::new(&dialect)
        .try_with_sql(predicate)
        .map_err(|e| ReactorError::InvalidPredicate(format!("parse error: {e}")))?;
    parser
        .parse_expr()
        .map_err(|e| ReactorError::InvalidPredicate(format!("parse error: {e}")))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_body_accepts_insert() {
        assert!(validate_body("INSERT INTO t VALUES (1)").is_ok());
    }

    #[test]
    fn validate_body_accepts_update() {
        assert!(validate_body("UPDATE t SET a = 1 WHERE id = 2").is_ok());
    }

    #[test]
    fn validate_body_accepts_delete() {
        assert!(validate_body("DELETE FROM t WHERE id = 1").is_ok());
    }

    #[test]
    fn validate_body_accepts_select() {
        assert!(validate_body("SELECT 1").is_ok());
    }

    #[test]
    fn validate_body_rejects_multi_statement() {
        assert!(matches!(
            validate_body("INSERT INTO t VALUES (1); INSERT INTO t VALUES (2)"),
            Err(ReactorError::MultiStatementBody)
        ));
    }

    #[test]
    fn validate_body_rejects_garbage() {
        assert!(matches!(
            validate_body("not even close to sql"),
            Err(ReactorError::InvalidBody(_))
        ));
    }

    #[test]
    fn validate_predicate_accepts_simple_expr() {
        assert!(validate_predicate("NEW.status = 'paid'").is_ok());
    }

    #[test]
    fn validate_predicate_accepts_compound_expr() {
        assert!(validate_predicate("NEW.status = 'paid' AND OLD.status != 'paid'").is_ok());
    }

    #[test]
    fn validate_predicate_rejects_garbage() {
        assert!(matches!(
            validate_predicate("@@@"),
            Err(ReactorError::InvalidPredicate(_))
        ));
    }

    #[test]
    fn ops_matches() {
        let ops = ReactorOps::INSERT | ReactorOps::UPDATE;
        assert!(ops.matches(ChangeOp::Insert));
        assert!(ops.matches(ChangeOp::Update));
        assert!(!ops.matches(ChangeOp::Delete));
    }
}
