//! `CREATE [OR REPLACE] [TEMP] VIEW` / `DROP VIEW` SQL surface.
//!
//! Plain views (as opposed to continuous materialized views) are pure
//! query-rewrite objects: no backing table, no physical data files, no
//! refresh cycle. The workflow is:
//!
//! 1. `CREATE VIEW v AS SELECT …` — validate the SQL parses, store the
//!    definition in the catalog via [`Catalog::register_view`].
//! 2. `DROP VIEW [IF EXISTS] v [CASCADE|RESTRICT]` — remove from the
//!    catalog.
//! 3. At SELECT time the executor calls [`rewrite_view_refs`] which
//!    replaces every reference to a known view name in the FROM clause
//!    with an inline subquery `(SELECT …) AS v`, making DataFusion plan
//!    the query as if the view were a derived table.
//!
//! `CREATE TEMP VIEW` / `CREATE TEMPORARY VIEW` are accepted and stored
//! identically to a regular view in v0.1 — there is no session-scoped
//! storage yet. The SQL parses fine and the definition persists for the
//! lifetime of the catalog (the in-memory catalog is session-scoped in
//! the integration test harness, so the effect is correct anyway).

use basin_catalog::{Catalog, ViewDef};
use basin_common::{BasinError, Result, TenantId};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::{ExecResult, TenantSession};

/// Execute `CREATE [OR REPLACE] [TEMP] VIEW v AS <query>`.
///
/// `name` is the bare view name (already extracted from the AST).
/// `or_replace` mirrors `CREATE OR REPLACE VIEW`.
/// `query_sql` is the SELECT body as text.
pub(crate) async fn exec_create_view(
    sess: &TenantSession,
    name: &str,
    query_sql: &str,
    or_replace: bool,
) -> Result<ExecResult> {
    // Validate: the stored body must parse as a valid SQL query.
    let dialect = PostgreSqlDialect {};
    Parser::parse_sql(&dialect, query_sql)
        .map_err(|e| BasinError::InvalidSchema(format!("view body is not valid SQL: {e}")))?;

    let def = ViewDef {
        tenant: sess.tenant,
        name: name.to_string(),
        query_sql: query_sql.to_string(),
    };
    sess.engine
        .config()
        .catalog
        .register_view(def, or_replace)
        .await?;

    Ok(ExecResult::Empty {
        tag: "CREATE VIEW".into(),
    })
}

/// Execute `DROP VIEW [IF EXISTS] v`.
///
/// `CASCADE` / `RESTRICT` are accepted syntactically but ignored in v0.1
/// (plain views have no dependents tracked in the catalog).
pub(crate) async fn exec_drop_view(
    sess: &TenantSession,
    name: &str,
    if_exists: bool,
) -> Result<ExecResult> {
    sess.engine
        .config()
        .catalog
        .drop_view(&sess.tenant, name, if_exists)
        .await?;

    Ok(ExecResult::Empty {
        tag: "DROP VIEW".into(),
    })
}

/// Rewrite a SQL statement so every plain-view reference in a `FROM` clause
/// is replaced by an inline subquery.
///
/// Strategy: textual token replacement — scan the SQL for identifiers that
/// match a known view name and are preceded by `FROM` or `JOIN` (with any
/// amount of whitespace / join-type keywords in between). Replace
/// `<view_name>` with `(<view_body>) AS <view_name>`.
///
/// This is intentionally conservative: it only rewrites bare identifiers
/// immediately after a FROM/JOIN keyword (plus optional alias keywords like
/// INNER, LEFT, RIGHT, FULL, CROSS). Sub-selects, CTEs, and schema-qualified
/// names that shadow view names are left untouched — DataFusion will reject
/// them if the table doesn't exist and the error message will be clear.
///
/// A `None` return means no rewriting was needed (no known view names
/// appeared in the SQL).
pub(crate) async fn rewrite_view_refs(
    catalog: &dyn Catalog,
    tenant: &TenantId,
    sql: &str,
) -> Result<Option<String>> {
    // Fast path: no views registered for this tenant → nothing to do.
    let views = catalog.list_views(tenant).await;
    if views.is_empty() {
        return Ok(None);
    }

    // Build a lookup map: lowercase name → query body.
    let view_map: std::collections::HashMap<String, String> = views
        .into_iter()
        .map(|v| (v.name.to_ascii_lowercase(), v.query_sql))
        .collect();

    // Tokenise-and-rewrite: walk the SQL character by character, tracking
    // string literals and block comments so we don't accidentally match
    // inside them. When we see `FROM` or `JOIN` (any join type keyword),
    // we record the next bare identifier as a potential view reference and
    // replace it when it's in the view map.
    let bytes = sql.as_bytes();
    let len = bytes.len();
    let mut out = String::with_capacity(sql.len() + 64);
    let mut i = 0usize;
    let mut changed = false;

    while i < len {
        // String literal: copy verbatim.
        if bytes[i] == b'\'' {
            out.push('\'');
            i += 1;
            while i < len {
                let c = bytes[i] as char;
                out.push(c);
                if c == '\'' {
                    i += 1;
                    if i < len && bytes[i] == b'\'' {
                        out.push('\'');
                        i += 1;
                    } else {
                        break;
                    }
                } else {
                    i += 1;
                }
            }
            continue;
        }
        // Quoted identifier: copy verbatim.
        if bytes[i] == b'"' {
            out.push('"');
            i += 1;
            while i < len {
                let c = bytes[i] as char;
                out.push(c);
                i += 1;
                if c == '"' {
                    if i < len && bytes[i] == b'"' {
                        out.push('"');
                        i += 1;
                    } else {
                        break;
                    }
                }
            }
            continue;
        }
        // Line comment: copy verbatim.
        if bytes[i] == b'-' && i + 1 < len && bytes[i + 1] == b'-' {
            while i < len && bytes[i] != b'\n' {
                out.push(bytes[i] as char);
                i += 1;
            }
            continue;
        }
        // Block comment: copy verbatim.
        if bytes[i] == b'/' && i + 1 < len && bytes[i + 1] == b'*' {
            out.push('/');
            out.push('*');
            i += 2;
            while i + 1 < len && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                out.push(bytes[i] as char);
                i += 1;
            }
            if i + 1 < len {
                out.push('*');
                out.push('/');
                i += 2;
            }
            continue;
        }

        // Check whether we are at an identifier start.
        if bytes[i].is_ascii_alphabetic() || bytes[i] == b'_' {
            // Collect the full identifier.
            let start = i;
            while i < len && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                i += 1;
            }
            let ident = &sql[start..i];
            let ident_lc = ident.to_ascii_lowercase();

            // Is this identifier a view name? Only rewrite if the token that
            // immediately precedes it (ignoring whitespace) is a FROM/JOIN
            // context keyword.
            if view_map.contains_key(&ident_lc) && is_from_or_join_context(&out) {
                let body = &view_map[&ident_lc];
                out.push('(');
                out.push_str(body);
                out.push_str(") AS ");
                out.push_str(ident);
                changed = true;
                continue;
            }

            // Not a view ref — emit as-is.
            out.push_str(ident);
            continue;
        }

        // Everything else: copy through.
        out.push(bytes[i] as char);
        i += 1;
    }

    if changed {
        Ok(Some(out))
    } else {
        Ok(None)
    }
}

/// Return `true` if the trimmed tail of `out` ends in a keyword that
/// introduces a table reference: `FROM`, or any join-type keyword
/// (`JOIN`, `INNER`, `LEFT`, `RIGHT`, `FULL`, `CROSS`, `OUTER`, `NATURAL`).
fn is_from_or_join_context(out: &str) -> bool {
    let tail = out.trim_end_matches(|c: char| c.is_ascii_whitespace());
    // Walk backwards to find the last complete identifier.
    let tbytes = tail.as_bytes();
    let mut end = tbytes.len();
    if end == 0 {
        return false;
    }
    // Skip trailing non-identifier chars (e.g. nothing should be there if
    // we trimmed, but just in case).
    while end > 0 && !(tbytes[end - 1].is_ascii_alphanumeric() || tbytes[end - 1] == b'_') {
        end -= 1;
    }
    if end == 0 {
        return false;
    }
    let mut start = end;
    while start > 0 && (tbytes[start - 1].is_ascii_alphanumeric() || tbytes[start - 1] == b'_') {
        start -= 1;
    }
    let kw = tail[start..end].to_ascii_uppercase();
    matches!(
        kw.as_str(),
        "FROM" | "JOIN" | "INNER" | "LEFT" | "RIGHT" | "FULL" | "CROSS" | "OUTER" | "NATURAL"
    )
}
