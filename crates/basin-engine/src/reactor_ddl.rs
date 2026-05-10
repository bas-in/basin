//! `ALTER TABLE ... REACT ON ...` and `DROP REACTOR` SQL surfaces
//! (5.11.C SQL-bodied reactors + 5.11.C2 constraint-shaped reactors).
//!
//! Three statement shapes route through this module:
//!
//! 1. ```sql
//!    ALTER TABLE <table> REACT ON {INSERT|UPDATE|DELETE}[, ...]
//!        [ WHEN (<predicate>) ]
//!        EXECUTE <single_sql_statement>;
//!    ```
//!    SQL-bodied reactor (5.11.C) — body runs through the existing
//!    [`Catalog::register_reactor`](basin_catalog::Catalog::register_reactor)
//!    API and is dispatched by `ReactorSink` on every matching mutation.
//!
//! 2. ```sql
//!    ALTER TABLE <table> REACT ON {INSERT|UPDATE} CONSTRAINT (<predicate>)
//!        [ WITH (name = '<name>') ];
//!    ```
//!    Constraint-shaped reactor (5.11.C2) — fires on the same path,
//!    but the body is a synthesised `SELECT 1 WHERE NOT (predicate)`-
//!    shaped probe whose `Err` causes the source mutation to roll
//!    back with SQLSTATE 23514. See "C2 design choice" below.
//!
//! 3. ```sql
//!    DROP REACTOR <reactor_name> ON <table>;
//!    ```
//!    Removes a reactor by `(tenant, table, name)`.
//!
//! sqlparser 0.52 has no `REACT` arm on `ALTER TABLE` and no `DROP
//! REACTOR` statement, so we recognise the three shapes textually
//! before sqlparser sees the SQL (mirrors `cv_ddl.rs` and
//! `function_ddl.rs`).
//!
//! ## C2 design choice: constraint = reactor with predicate-only body
//!
//! Rather than adding a `kind: Body | Constraint` field to
//! [`basin_catalog::ReactorDef`] (which is in `basin-catalog`,
//! co-edited by the in-flight 5.11.K2 agent), 5.11.C2 stores
//! constraint reactors as ordinary `ReactorDef` rows whose body is a
//! synthesised SQL probe of the shape:
//!
//! ```sql
//! SELECT CASE WHEN NOT (<predicate>)
//!        THEN CAST('SQLSTATE 23514 check_violation: ...' AS BIGINT)
//!        ELSE 1 END
//! ```
//!
//! When the predicate evaluates to `false`, the `CAST('SQLSTATE
//! 23514 ...' AS BIGINT)` raises a runtime error from the engine.
//! That error short-circuits `ReactorSink::publish`, which Tier 0
//! converts into a rolled-back mutation. The error message includes
//! the literal token `SQLSTATE 23514 check_violation` so router-side
//! error mapping (and PG-shaped clients) recognise the constraint
//! violation.
//!
//! This avoids touching `basin-catalog::reactors` entirely. The
//! trade-off is that `list_reactors` cannot tell a constraint
//! reactor from a body reactor by inspecting the row alone — that
//! capability is held back to the catalog-persistence work, where a
//! `kind` enum on `ReactorDef` can land cleanly under K2's
//! coordination.
//!
//! ## Atomicity caveat
//! Reactor bodies execute as independent transactions. If reactor body B fails
//! after reactor body A succeeds (same source mutation), A's writes persist
//! even though the source mutation rolls back. Full atomicity awaits a
//! transactional engine layer (Phase 5). This trade-off is documented in
//! `reactor_sink.rs:atomicity-caveat`.
//!
//! ## Deferred wiring
//!
//! The dispatch from `executor.rs` is **not** wired in this module
//! (5.11.K2 — `CREATE TYPE / DOMAIN` — is concurrently editing the
//! same pre-screen pipeline). A follow-up agent threads the
//! `match_*` recognisers into `executor.rs` once K2 lands. Tests in
//! this crate exercise the helpers directly via the public `match_*`
//! / `exec_*` API.

use std::sync::Arc;

use basin_catalog::{Catalog, ReactorDef, ReactorOps};
use basin_common::{BasinError, ChangeOp, Result, TableName, TenantId};

/// Parsed `ALTER TABLE ... REACT ON ... EXECUTE <body>` statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReactIntent {
    pub table: String,
    pub name: String,
    pub ops: ReactorOps,
    pub when_predicate: Option<String>,
    pub body: String,
}

/// Parsed `ALTER TABLE ... REACT ON ... CONSTRAINT (<predicate>)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConstraintReactIntent {
    pub table: String,
    pub name: String,
    pub ops: ReactorOps,
    pub predicate: String,
}

/// Parsed `DROP REACTOR <name> ON <table>` statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropIntent {
    pub table: String,
    pub name: String,
}

/// Recognise `ALTER TABLE <table> REACT ON ... EXECUTE <body>`.
/// Returns `None` when the statement is something else, an `Err` on
/// a malformed match. `EXECUTE` is the keyword that distinguishes
/// 5.11.C from 5.11.C2 ([`match_alter_table_react_constraint`]).
pub fn match_alter_table_react_on(sql: &str) -> Result<Option<ReactIntent>> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let head = match peek_react_head(trimmed)? {
        Some(h) => h,
        None => return Ok(None),
    };
    // CONSTRAINT-form is recognised by `match_alter_table_react_constraint`.
    if starts_with_kw(head.tail, "CONSTRAINT") {
        return Ok(None);
    }
    // Optional WHEN.
    let mut tail = head.tail;
    let when_predicate = if starts_with_kw(tail, "WHEN") {
        let after_when = skip_word(tail).trim_start();
        let (pred, after_pred) = read_paren_block(after_when)?;
        validate_predicate(&pred)?;
        tail = after_pred.trim_start();
        Some(pred.trim().to_string())
    } else {
        None
    };
    if !starts_with_kw(tail, "EXECUTE") {
        return Err(BasinError::InvalidSchema(
            "ALTER TABLE REACT ON: expected EXECUTE <body> or CONSTRAINT (<predicate>)".into(),
        ));
    }
    let body = skip_word(tail).trim_start().trim().to_string();
    if body.is_empty() {
        return Err(BasinError::InvalidSchema(
            "ALTER TABLE REACT ON ... EXECUTE: body must not be empty".into(),
        ));
    }

    let name = synthetic_reactor_name(&head.table, &head.ops, "react");
    Ok(Some(ReactIntent {
        table: head.table,
        name,
        ops: head.ops,
        when_predicate,
        body,
    }))
}

/// Recognise `ALTER TABLE <table> REACT ON ... CONSTRAINT (<predicate>)
/// [WITH (name = '...')]`. Returns `None` when the statement is
/// something else.
pub fn match_alter_table_react_constraint(sql: &str) -> Result<Option<ConstraintReactIntent>> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let head = match peek_react_head(trimmed)? {
        Some(h) => h,
        None => return Ok(None),
    };
    if !starts_with_kw(head.tail, "CONSTRAINT") {
        return Ok(None);
    }
    // Constraint-shaped reactors only meaningfully fire on
    // INSERT / UPDATE — DELETE never has a NEW row whose predicate
    // would matter. Reject DELETE in the OPS bitset.
    if head.ops.contains(ReactorOps::DELETE) {
        return Err(BasinError::InvalidSchema(
            "REACT ON ... CONSTRAINT: DELETE is not supported (constraints check NEW row)".into(),
        ));
    }
    let after_constraint = skip_word(head.tail).trim_start();
    let (predicate, after_pred) = read_paren_block(after_constraint)?;
    validate_predicate(&predicate)?;
    let mut tail = after_pred.trim_start();

    let mut explicit_name: Option<String> = None;
    if starts_with_kw(tail, "WITH") {
        let after_with = skip_word(tail).trim_start();
        let (opts, after_opts) = read_paren_block(after_with)?;
        tail = after_opts.trim_start();
        for raw in split_top_level_commas(&opts) {
            let token = raw.trim();
            if token.is_empty() {
                continue;
            }
            let Some(eq) = token.find('=') else {
                return Err(BasinError::InvalidSchema(format!(
                    "REACT ON CONSTRAINT WITH: option {token:?} has no '=' value"
                )));
            };
            let key = token[..eq].trim();
            let value = token[eq + 1..].trim();
            match key.to_ascii_lowercase().as_str() {
                "name" => {
                    let unquoted = unquote_string_literal(value).ok_or_else(|| {
                        BasinError::InvalidSchema(format!(
                            "REACT ON CONSTRAINT WITH: name must be a string literal, got {value:?}"
                        ))
                    })?;
                    explicit_name = Some(unquoted);
                }
                other => {
                    return Err(BasinError::InvalidSchema(format!(
                        "REACT ON CONSTRAINT WITH: unknown option {other:?}"
                    )));
                }
            }
        }
    }
    if !tail.is_empty() {
        return Err(BasinError::InvalidSchema(format!(
            "REACT ON CONSTRAINT: unexpected trailing input {tail:?}"
        )));
    }

    let name = explicit_name
        .unwrap_or_else(|| synthetic_reactor_name(&head.table, &head.ops, "constraint"));
    Ok(Some(ConstraintReactIntent {
        table: head.table,
        name,
        ops: head.ops,
        predicate: predicate.trim().to_string(),
    }))
}

/// Recognise `DROP REACTOR <name> ON <table>`. Returns `None` when
/// the statement is something else.
pub fn match_drop_reactor(sql: &str) -> Result<Option<DropIntent>> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    if !starts_with_kw(trimmed, "DROP") {
        return Ok(None);
    }
    let after_drop = skip_word(trimmed).trim_start();
    if !starts_with_kw(after_drop, "REACTOR") {
        return Ok(None);
    }
    let after_reactor = skip_word(after_drop).trim_start();
    let (name, after_name) = read_simple_identifier(after_reactor)?;
    let after_name = after_name.trim_start();
    if !starts_with_kw(after_name, "ON") {
        return Err(BasinError::InvalidSchema(
            "DROP REACTOR: expected ON <table>".into(),
        ));
    }
    let after_on = skip_word(after_name).trim_start();
    let (table, rest) = read_simple_identifier(after_on)?;
    let rest = rest.trim();
    if !rest.is_empty() {
        return Err(BasinError::InvalidSchema(format!(
            "DROP REACTOR: unexpected trailing input {rest:?}"
        )));
    }
    Ok(Some(DropIntent { table, name }))
}

/// Apply a [`ReactIntent`] against the catalog. Wraps
/// `Catalog::register_reactor` with intent-shape validation.
pub async fn exec_react_on(
    intent: ReactIntent,
    tenant: &TenantId,
    catalog: &Arc<dyn Catalog>,
) -> Result<()> {
    let table = TableName::new(&intent.table)?;
    if intent.ops.is_empty() {
        return Err(BasinError::InvalidSchema(
            "ALTER TABLE REACT ON: ON list is empty".into(),
        ));
    }
    let def = ReactorDef {
        tenant: *tenant,
        table,
        name: intent.name,
        ops: intent.ops,
        when_predicate: intent.when_predicate,
        body: intent.body,
    };
    catalog.register_reactor(def).await
}

/// Apply a [`ConstraintReactIntent`] against the catalog. The
/// constraint is encoded as a body-style reactor whose body raises
/// SQLSTATE 23514 when the predicate evaluates to false. See module
/// doc for the design rationale.
pub async fn exec_react_constraint(
    intent: ConstraintReactIntent,
    tenant: &TenantId,
    catalog: &Arc<dyn Catalog>,
) -> Result<()> {
    let table = TableName::new(&intent.table)?;
    if intent.ops.is_empty() {
        return Err(BasinError::InvalidSchema(
            "REACT ON CONSTRAINT: ON list is empty".into(),
        ));
    }
    // Synthesise a body that raises SQLSTATE 23514 when the predicate
    // is false. The body is parsed at registration time by
    // `Catalog::register_reactor` and re-parsed at fire time by
    // `ReactorSink`; the synthesised SQL must be a single statement
    // that the engine accepts and that fails *only* when the
    // predicate is false.
    //
    // The probe routes through the `__basin_assert(predicate, message)`
    // scalar UDF (registered in `udf::register_pg_compat_udfs`).
    // The previous CASE/CAST shape evaluated the falsy-branch CAST at
    // type-check time, raising the cast error regardless of the
    // predicate's runtime value. The UDF lifts the side-effect into
    // `invoke`, where it is gated by the per-row predicate.
    //
    // The error text threads the constraint name + raw predicate text
    // and includes the literal `SQLSTATE 23514 check_violation` token
    // so router-side error mapping classifies the failure correctly.
    let escaped_pred = intent.predicate.replace('\'', "''");
    let escaped_name = intent.name.replace('\'', "''");
    let escaped_table = intent.table.replace('\'', "''");
    let body = format!(
        "SELECT __basin_assert(({pred}), \
         'SQLSTATE 23514 check_violation: constraint {name} on {table} \
         failed predicate ({pred_lit})')",
        pred = intent.predicate,
        name = escaped_name,
        table = escaped_table,
        pred_lit = escaped_pred,
    );
    let def = ReactorDef {
        tenant: *tenant,
        table,
        name: intent.name,
        ops: intent.ops,
        when_predicate: None,
        body,
    };
    catalog.register_reactor(def).await
}

/// Apply a [`DropIntent`] against the catalog. Wraps
/// `Catalog::drop_reactor` and surfaces a clean `NotFound` when the
/// reactor doesn't exist.
pub async fn exec_drop_reactor(
    intent: DropIntent,
    tenant: &TenantId,
    catalog: &Arc<dyn Catalog>,
) -> Result<()> {
    let table = TableName::new(&intent.table)?;
    catalog.drop_reactor(tenant, &table, &intent.name).await
}

// --- Common parser plumbing ------------------------------------------

struct ReactHead<'a> {
    table: String,
    ops: ReactorOps,
    /// Slice positioned at the next clause (`WHEN`, `EXECUTE`, or
    /// `CONSTRAINT`).
    tail: &'a str,
}

fn peek_react_head(trimmed: &str) -> Result<Option<ReactHead<'_>>> {
    if !starts_with_kw(trimmed, "ALTER") {
        return Ok(None);
    }
    let after_alter = skip_word(trimmed).trim_start();
    if !starts_with_kw(after_alter, "TABLE") {
        return Ok(None);
    }
    let after_table = skip_word(after_alter).trim_start();
    let (table, after_tname) = read_simple_identifier(after_table)?;
    let after_tname = after_tname.trim_start();
    if !starts_with_kw(after_tname, "REACT") {
        return Ok(None);
    }
    let after_react = skip_word(after_tname).trim_start();
    if !starts_with_kw(after_react, "ON") {
        return Err(BasinError::InvalidSchema(
            "ALTER TABLE REACT: expected ON <op_list>".into(),
        ));
    }
    let after_on = skip_word(after_react).trim_start();
    let (ops, after_ops) = read_op_list(after_on)?;
    if ops.is_empty() {
        return Err(BasinError::InvalidSchema(
            "ALTER TABLE REACT ON: list must include at least one of \
             INSERT / UPDATE / DELETE"
                .into(),
        ));
    }
    Ok(Some(ReactHead {
        table,
        ops,
        tail: after_ops.trim_start(),
    }))
}

/// Read a comma-separated `INSERT|UPDATE|DELETE` list. Same shape as
/// `webhook_ddl::read_op_list` but yields a [`ReactorOps`] bitset.
fn read_op_list(s: &str) -> Result<(ReactorOps, &str)> {
    let mut ops = ReactorOps::empty();
    let mut rest = s.trim_start();
    loop {
        let (token, after) = match peek_word(rest) {
            Some(t) => t,
            None => break,
        };
        let bit = match token.to_ascii_uppercase().as_str() {
            "INSERT" => ReactorOps::INSERT,
            "UPDATE" => ReactorOps::UPDATE,
            "DELETE" => ReactorOps::DELETE,
            _ => break,
        };
        ops |= bit;
        rest = after.trim_start();
        if rest.starts_with(',') {
            rest = rest[1..].trim_start();
        } else {
            break;
        }
    }
    Ok((ops, rest))
}

fn validate_predicate(pred: &str) -> Result<()> {
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;
    let dialect = PostgreSqlDialect {};
    let mut parser = Parser::new(&dialect)
        .try_with_sql(pred)
        .map_err(|e| BasinError::InvalidSchema(format!("REACT predicate parse error: {e}")))?;
    parser
        .parse_expr()
        .map_err(|e| BasinError::InvalidSchema(format!("REACT predicate parse error: {e}")))?;
    Ok(())
}

fn synthetic_reactor_name(table: &str, ops: &ReactorOps, kind: &str) -> String {
    let mut s = format!("{kind}__{table}");
    if ops.contains(ReactorOps::INSERT) {
        s.push_str("__i");
    }
    if ops.contains(ReactorOps::UPDATE) {
        s.push_str("__u");
    }
    if ops.contains(ReactorOps::DELETE) {
        s.push_str("__d");
    }
    // Disambiguate same-shape reactors registered repeatedly: the
    // catalog rejects duplicates with `BasinError::Catalog`, so the
    // caller gets a clean error rather than silent overwrite.
    s
}

fn read_paren_block(s: &str) -> Result<(String, &str)> {
    let bytes = s.as_bytes();
    if bytes.is_empty() || bytes[0] != b'(' {
        return Err(BasinError::InvalidSchema(format!(
            "expected '(' at {:?}",
            s.chars().take(8).collect::<String>()
        )));
    }
    let mut depth: i32 = 1;
    let mut i = 1;
    while i < bytes.len() && depth > 0 {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => depth -= 1,
            b'\'' => {
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'\'' {
                        if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
            }
            _ => {}
        }
        i += 1;
    }
    if depth != 0 {
        return Err(BasinError::InvalidSchema(
            "unterminated '(' in REACT clause".into(),
        ));
    }
    Ok((s[1..i - 1].to_string(), &s[i..]))
}

fn split_top_level_commas(text: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let bytes = text.as_bytes();
    let mut depth = 0i32;
    let mut start = 0;
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => depth -= 1,
            b'\'' => {
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'\'' {
                        if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
            }
            b',' if depth == 0 => {
                out.push(&text[start..i]);
                start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }
    if start <= bytes.len() {
        out.push(&text[start..]);
    }
    out
}

fn unquote_string_literal(s: &str) -> Option<String> {
    let s = s.trim();
    if s.len() < 2 || !s.starts_with('\'') || !s.ends_with('\'') {
        return None;
    }
    Some(s[1..s.len() - 1].replace("''", "'"))
}

fn starts_with_kw(s: &str, kw: &str) -> bool {
    let s = s.trim_start();
    let bytes = s.as_bytes();
    let kw = kw.as_bytes();
    if bytes.len() < kw.len() {
        return false;
    }
    for (a, b) in bytes.iter().zip(kw.iter()) {
        if !a.eq_ignore_ascii_case(b) {
            return false;
        }
    }
    if bytes.len() == kw.len() {
        return true;
    }
    let next = bytes[kw.len()];
    !(next.is_ascii_alphanumeric() || next == b'_')
}

fn skip_word(s: &str) -> &str {
    let s = s.trim_start();
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
        i += 1;
    }
    &s[i..]
}

fn read_simple_identifier(s: &str) -> Result<(String, &str)> {
    let s = s.trim_start();
    let bytes = s.as_bytes();
    if bytes.is_empty() {
        return Err(BasinError::InvalidIdent(
            "expected an identifier, got end of statement".into(),
        ));
    }
    if !(bytes[0].is_ascii_alphabetic() || bytes[0] == b'_') {
        return Err(BasinError::InvalidIdent(format!(
            "expected an identifier at {:?}",
            s.chars().take(8).collect::<String>()
        )));
    }
    let mut i = 1;
    while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
        i += 1;
    }
    Ok((s[..i].to_string(), &s[i..]))
}

fn peek_word(s: &str) -> Option<(String, &str)> {
    let s = s.trim_start();
    let bytes = s.as_bytes();
    if bytes.is_empty() || !(bytes[0].is_ascii_alphabetic() || bytes[0] == b'_') {
        return None;
    }
    let mut i = 1;
    while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
        i += 1;
    }
    Some((s[..i].to_string(), &s[i..]))
}

// Keep `ChangeOp` referenced; the executor wiring will re-export it
// when threading reactor matches through the executor pipeline.
#[allow(dead_code)]
fn _enforce_changeop_in_scope(_: ChangeOp) {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn match_react_on_basic() {
        let intent = match_alter_table_react_on(
            "ALTER TABLE orders REACT ON UPDATE EXECUTE INSERT INTO log VALUES (NEW.id);",
        )
        .unwrap()
        .unwrap();
        assert_eq!(intent.table, "orders");
        assert_eq!(intent.ops, ReactorOps::UPDATE);
        assert_eq!(intent.when_predicate, None);
        assert!(intent.body.starts_with("INSERT INTO log"));
    }

    #[test]
    fn match_react_on_with_when() {
        let intent = match_alter_table_react_on(
            "ALTER TABLE orders REACT ON UPDATE WHEN (NEW.status = 'paid') \
             EXECUTE INSERT INTO log VALUES (NEW.id)",
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            intent.when_predicate.as_deref(),
            Some("NEW.status = 'paid'")
        );
    }

    #[test]
    fn match_react_constraint_basic() {
        let intent = match_alter_table_react_constraint(
            "ALTER TABLE orders REACT ON INSERT CONSTRAINT (count(*) <= 100)",
        )
        .unwrap()
        .unwrap();
        assert_eq!(intent.table, "orders");
        assert_eq!(intent.ops, ReactorOps::INSERT);
        assert_eq!(intent.predicate, "count(*) <= 100");
    }

    #[test]
    fn match_react_constraint_with_name() {
        let intent = match_alter_table_react_constraint(
            "ALTER TABLE orders REACT ON INSERT CONSTRAINT (count(*) <= 100) \
             WITH (name = 'cap')",
        )
        .unwrap()
        .unwrap();
        assert_eq!(intent.name, "cap");
    }

    #[test]
    fn match_react_constraint_rejects_delete() {
        let err = match_alter_table_react_constraint(
            "ALTER TABLE orders REACT ON DELETE CONSTRAINT (count(*) <= 100)",
        )
        .unwrap_err();
        assert!(matches!(err, BasinError::InvalidSchema(_)));
    }

    #[test]
    fn match_react_on_returns_none_for_constraint_form() {
        // The constraint form is the constraint matcher's job; the
        // body matcher must yield None so the dispatch routes
        // correctly.
        let r = match_alter_table_react_on(
            "ALTER TABLE orders REACT ON INSERT CONSTRAINT (count(*) <= 100)",
        )
        .unwrap();
        assert!(r.is_none());
    }

    #[test]
    fn match_drop_reactor_basic() {
        let intent = match_drop_reactor("DROP REACTOR cap ON orders;")
            .unwrap()
            .unwrap();
        assert_eq!(intent.table, "orders");
        assert_eq!(intent.name, "cap");
    }

    #[test]
    fn match_drop_reactor_non_match() {
        assert!(match_drop_reactor("DROP TABLE foo").unwrap().is_none());
        assert!(match_drop_reactor("DROP REACTOR cap").is_err());
    }

    #[test]
    fn match_react_non_match_returns_none() {
        assert!(match_alter_table_react_on("SELECT 1").unwrap().is_none());
        assert!(match_alter_table_react_constraint("SELECT 1")
            .unwrap()
            .is_none());
    }
}
