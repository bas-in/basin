//! Per-project `CREATE PROCEDURE … LANGUAGE sql` catalog.
//!
//! Customers will write
//!
//! ```sql
//! CREATE PROCEDURE archive_project(tid TEXT) LANGUAGE sql AS $$
//!     INSERT INTO archive SELECT * FROM events WHERE project_id = tid;
//!     DELETE FROM events WHERE project_id = tid;
//! $$;
//!
//! CALL archive_project('01HABCD…');
//!
//! DROP PROCEDURE archive_project(TEXT);
//! ```
//!
//! and the engine looks up the registered body, substitutes call-site
//! arguments into each statement, and runs the statements in order.
//!
//! The catalog row is parallel to [`crate::functions::SqlFunctionDef`];
//! the differentiator is that a procedure body may contain *multiple*
//! statements (DML + DDL), separated by `;`. Functions stay
//! single-`SELECT`. We reuse [`crate::functions::SqlFunctionArg`] for
//! the parameter list because the substitution surface is identical.
//!
//! The catalog is a single shared `HashMap<(ProjectId, String),
//! SqlProcedureDef>` — same shape rule as every other typed-DDL row in
//! this crate — so per-project cost stays `O(bytes)` with no per-project
//! heavy resource.
//!
//! Validation at registration:
//!
//! * Body parses as SQL via sqlparser; multi-statement is permitted.
//! * Each statement is one of `INSERT` / `UPDATE` / `DELETE` /
//!   `SELECT` / `CREATE TABLE` / `DROP TABLE`. Anything else (notably a
//!   nested `CALL`) is rejected — v0.1 explicitly disallows nested
//!   calls.
//! * Argument names are unique within the procedure and are valid SQL
//!   identifiers.
//!
//! Out of scope (deferred):
//!
//! * Transaction wrapping — Phase 5 single-shard transactions.
//!   v0.1 runs the body sequentially, best-effort: a mid-statement
//!   error leaves earlier statements committed.
//! * Control flow (`IF`, `LOOP`, variables) — that needs the PL/pgSQL
//!   interpreter Basin explicitly skips per ADR 0012.
//! * `LANGUAGE plpgsql` — out of scope per ADR 0012.

use serde::{Deserialize, Serialize};

use basin_common::ProjectId;

use crate::functions::SqlFunctionArg;

/// Catalog row for a `CREATE PROCEDURE … LANGUAGE sql`. Stored
/// per-project; two projects registering the same procedure name are
/// independent rows.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqlProcedureDef {
    pub project: ProjectId,
    /// Unqualified procedure name within the project. Validated to be a
    /// SQL identifier at registration.
    pub name: String,
    /// Positional parameter list. We reuse `SqlFunctionArg` because the
    /// argument shape (named scalar) and the substitution rule
    /// (`Expr::Identifier(arg_name) → call-site Expr`) are identical to
    /// SQL functions.
    pub args: Vec<SqlFunctionArg>,
    /// Raw SQL of the procedure body. Validated at registration to
    /// parse as one or more statements drawn from a permitted whitelist
    /// (no nested `CALL`); stored verbatim so the engine reparses each
    /// invocation.
    pub body: String,
}

/// Errors specific to procedure registration. Mapped to
/// [`basin_common::BasinError`] at the catalog API boundary.
#[derive(Debug)]
pub enum ProcedureError {
    /// Procedure body failed to parse.
    InvalidBody(String),
    /// Body contained a statement kind that isn't on the v0.1 whitelist
    /// (e.g. `CALL`, control-flow PL/pgSQL).
    DisallowedStatement(String),
    /// `args` contained two parameters with the same name.
    DuplicateArgName(String),
    /// `name` is not a SQL identifier.
    InvalidName(String),
}

/// Validate a procedure at `CREATE PROCEDURE` time. Body is parsed via
/// sqlparser's `PostgreSqlDialect`; each statement is checked against
/// the v0.1 whitelist. Argument names must be unique and valid
/// identifiers.
pub fn validate_new(def: &SqlProcedureDef) -> Result<(), ProcedureError> {
    if !is_valid_identifier(&def.name) {
        return Err(ProcedureError::InvalidName(format!(
            "procedure name {:?} is not a valid SQL identifier",
            def.name
        )));
    }
    let mut seen = std::collections::HashSet::new();
    for arg in &def.args {
        if !is_valid_identifier(&arg.name) {
            return Err(ProcedureError::InvalidName(format!(
                "argument name {:?} is not a valid SQL identifier",
                arg.name
            )));
        }
        if !seen.insert(arg.name.to_ascii_lowercase()) {
            return Err(ProcedureError::DuplicateArgName(arg.name.clone()));
        }
    }

    let dialect = sqlparser::dialect::PostgreSqlDialect {};
    let stmts = sqlparser::parser::Parser::parse_sql(&dialect, &def.body)
        .map_err(|e| ProcedureError::InvalidBody(format!("parse error: {e}")))?;
    if stmts.is_empty() {
        return Err(ProcedureError::InvalidBody(
            "procedure body must contain at least one statement".into(),
        ));
    }
    for stmt in &stmts {
        if !is_allowed_statement(stmt) {
            return Err(ProcedureError::DisallowedStatement(format!(
                "statement {} is not permitted in a v0.1 procedure body \
                 (allowed: INSERT, UPDATE, DELETE, SELECT, CREATE TABLE, \
                 DROP TABLE; nested CALL is rejected)",
                short_kind(stmt)
            )));
        }
    }
    Ok(())
}

fn is_valid_identifier(s: &str) -> bool {
    let mut chars = s.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !(first.is_ascii_alphabetic() || first == '_') {
        return false;
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Whitelist for the v0.1 procedure body. Keep tight — extending the
/// list later is additive, removing a kind is a breaking change.
fn is_allowed_statement(stmt: &sqlparser::ast::Statement) -> bool {
    use sqlparser::ast::Statement;
    matches!(
        stmt,
        Statement::Insert(_)
            | Statement::Update { .. }
            | Statement::Delete(_)
            | Statement::Query(_)
            | Statement::CreateTable(_)
            | Statement::Drop {
                object_type: sqlparser::ast::ObjectType::Table,
                ..
            }
    )
}

/// Render a short kind label for diagnostics. We avoid printing the
/// full statement text because the body may be large; the kind is
/// enough to point a developer at the offending line.
fn short_kind(stmt: &sqlparser::ast::Statement) -> &'static str {
    use sqlparser::ast::Statement;
    match stmt {
        Statement::Insert(_) => "INSERT",
        Statement::Update { .. } => "UPDATE",
        Statement::Delete(_) => "DELETE",
        Statement::Query(_) => "SELECT",
        Statement::CreateTable(_) => "CREATE TABLE",
        Statement::Drop { .. } => "DROP",
        Statement::Call(_) => "CALL",
        Statement::CreateProcedure { .. } => "CREATE PROCEDURE",
        Statement::CreateFunction { .. } => "CREATE FUNCTION",
        Statement::AlterTable { .. } => "ALTER TABLE",
        _ => "<unsupported>",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::functions::SqlArgType;

    fn def(args: Vec<(&str, SqlArgType)>, body: &str) -> SqlProcedureDef {
        SqlProcedureDef {
            project: ProjectId::new(),
            name: "p".into(),
            args: args
                .into_iter()
                .map(|(n, t)| SqlFunctionArg {
                    name: n.into(),
                    data_type: t,
                })
                .collect(),
            body: body.into(),
        }
    }

    #[test]
    fn accepts_single_insert() {
        let d = def(vec![], "INSERT INTO log VALUES ('hi')");
        assert!(validate_new(&d).is_ok());
    }

    #[test]
    fn accepts_multi_statement() {
        let d = def(
            vec![("x", SqlArgType::BigInt)],
            "INSERT INTO archive SELECT * FROM events WHERE id = x; \
             DELETE FROM events WHERE id = x",
        );
        assert!(validate_new(&d).is_ok());
    }

    #[test]
    fn rejects_nested_call() {
        let d = def(vec![], "CALL other_proc()");
        let err = validate_new(&d).unwrap_err();
        assert!(matches!(err, ProcedureError::DisallowedStatement(_)));
    }

    #[test]
    fn rejects_create_function_in_body() {
        let d = def(
            vec![],
            "CREATE FUNCTION inner(x INT) RETURNS INT LANGUAGE sql AS $$ SELECT x $$",
        );
        let err = validate_new(&d).unwrap_err();
        assert!(matches!(err, ProcedureError::DisallowedStatement(_)));
    }

    #[test]
    fn rejects_duplicate_arg_names() {
        let d = def(
            vec![("a", SqlArgType::Int), ("a", SqlArgType::Text)],
            "SELECT 1",
        );
        let err = validate_new(&d).unwrap_err();
        assert!(matches!(err, ProcedureError::DuplicateArgName(_)));
    }

    #[test]
    fn rejects_invalid_arg_name() {
        let d = def(vec![("1bad", SqlArgType::Int)], "SELECT 1");
        let err = validate_new(&d).unwrap_err();
        assert!(matches!(err, ProcedureError::InvalidName(_)));
    }

    #[test]
    fn rejects_garbage_body() {
        let d = def(vec![], "@@@ NOT SQL");
        let err = validate_new(&d).unwrap_err();
        assert!(matches!(err, ProcedureError::InvalidBody(_)));
    }

    #[test]
    fn rejects_empty_body() {
        let d = def(vec![], "");
        let err = validate_new(&d).unwrap_err();
        assert!(matches!(err, ProcedureError::InvalidBody(_)));
    }

    #[test]
    fn accepts_create_table_in_body() {
        let d = def(vec![], "CREATE TABLE staging (x INT)");
        assert!(validate_new(&d).is_ok());
    }

    #[test]
    fn accepts_drop_table_in_body() {
        let d = def(vec![], "DROP TABLE staging");
        assert!(validate_new(&d).is_ok());
    }
}
