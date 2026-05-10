//! `CREATE/DROP/ALTER FUNCTION` SQL surface.
//!
//! Three statement shapes route through this module:
//!
//! 1. `CREATE [OR REPLACE] FUNCTION <name>(<args>) RETURNS <type> LANGUAGE
//!    sql AS $$ <body> $$` — register a `LANGUAGE sql` scalar UDF.
//!    sqlparser's `Statement::CreateFunction` arm carries the parsed shape;
//!    we map it to a `SqlFunctionDef` and hand off to
//!    `Catalog::register_sql_function`.
//!
//! 2. `DROP FUNCTION [IF EXISTS] <name>(<args>)` — remove a previously-
//!    registered function. Argument list is parsed by sqlparser but ignored
//!    here: v0.1 disallows function-name overloading per tenant, so the
//!    name is the unique key.
//!
//! 3. `ALTER FUNCTION <name>(<args>) RENAME TO <new>` — sqlparser 0.52 has
//!    no `Statement::AlterFunction` AST node, so we recognise the form
//!    textually before sqlparser sees it.
//!
//! `LANGUAGE plpgsql` / `LANGUAGE wasm` / `RETURNS TABLE` / `CALL`
//! procedures are explicitly out of scope per ADR 0012; each is rejected
//! with a SQLSTATE-tagged message pointing at the right follow-up phase.

use std::sync::Arc;

use basin_catalog::{
    Catalog, SqlArgType, SqlFunctionArg, SqlFunctionDef, SqlFunctionLanguage, SqlReturnType,
};
use basin_common::{BasinError, Result};
use sqlparser::ast::{
    CreateFunctionBody, DataType as SqlDataType, Expr, Ident, ObjectName, OperateFunctionArg,
    TimezoneInfo, Value,
};

use crate::sql_functions::{validate_for_registration, validate_no_mutual_recursion};
use crate::{ExecResult, TenantSession};

/// Translate a sqlparser-parsed `CREATE FUNCTION` into a `SqlFunctionDef`
/// and persist it via `Catalog::register_sql_function`. Validation —
/// identifier shape, recursion check, body-is-single-SELECT, reserved-name
/// collisions — is delegated to
/// [`crate::sql_functions::validate_for_registration`] so the same rules
/// apply whether a function is created via this SQL surface or via a
/// future direct Rust API.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn exec_create_function(
    sess: &TenantSession,
    or_replace: bool,
    temporary: bool,
    name: ObjectName,
    args: Option<Vec<OperateFunctionArg>>,
    return_type: Option<SqlDataType>,
    function_body: Option<CreateFunctionBody>,
    language: Option<Ident>,
) -> Result<ExecResult> {
    if temporary {
        return Err(BasinError::InvalidSchema(
            "CREATE TEMPORARY FUNCTION is not supported; functions are tenant-scoped".into(),
        ));
    }
    let lang = language.as_ref().map(|l| l.value.as_str()).unwrap_or("");
    if !lang.eq_ignore_ascii_case("sql") {
        return Err(BasinError::InvalidSchema(format!(
            "CREATE FUNCTION: only LANGUAGE sql is supported (got LANGUAGE {lang:?}); \
             SQLSTATE 0A000 feature_not_supported. PL/pgSQL is out of scope per \
             docs/decisions/0012-change-event-primitive.md"
        )));
    }

    let fn_name = single_part_object_name(&name)?;

    let return_dt = return_type.ok_or_else(|| {
        BasinError::InvalidSchema(
            "CREATE FUNCTION: RETURNS clause is required".into(),
        )
    })?;
    let parsed_return = parse_return_type(&return_dt, &fn_name)?;

    let mut def_args: Vec<SqlFunctionArg> = Vec::new();
    if let Some(arg_list) = args {
        for (idx, a) in arg_list.iter().enumerate() {
            let aname = a
                .name
                .as_ref()
                .map(|i| i.value.clone())
                .ok_or_else(|| {
                    BasinError::InvalidSchema(format!(
                        "CREATE FUNCTION {fn_name}: argument #{idx} is unnamed; \
                         all arguments must be named in v0.1"
                    ))
                })?;
            let dt = sql_data_type_to_arg(&a.data_type).map_err(|e| {
                BasinError::InvalidSchema(format!(
                    "CREATE FUNCTION {fn_name}: arg {aname:?} type unsupported ({e})"
                ))
            })?;
            def_args.push(SqlFunctionArg {
                name: aname,
                data_type: dt,
            });
        }
    }

    let body_text = extract_body_text(&function_body, &fn_name)?;

    let def = SqlFunctionDef {
        tenant: sess.tenant,
        name: fn_name.clone(),
        args: def_args,
        return_type: parsed_return,
        body: body_text,
        language: SqlFunctionLanguage::Sql,
    };

    let validated = validate_for_registration(def)?;

    let catalog: Arc<dyn Catalog> = sess.engine.config().catalog.clone();
    if !or_replace {
        if let Some(_existing) = catalog.lookup_sql_function(&sess.tenant, &fn_name).await {
            return Err(BasinError::InvalidSchema(format!(
                "CREATE FUNCTION: function {fn_name:?} already exists; \
                 use CREATE OR REPLACE FUNCTION to replace it"
            )));
        }
    }
    let existing_map = collect_tenant_function_map(&catalog, &sess.tenant, &fn_name).await;
    validate_no_mutual_recursion(&validated, &existing_map)?;
    catalog.register_sql_function(validated).await?;

    Ok(ExecResult::Empty {
        tag: "CREATE FUNCTION".into(),
    })
}

/// Execute `DROP FUNCTION [IF EXISTS] <name>(<args>)`. The argument list
/// from the AST is accepted (and parsed) but not used to disambiguate;
/// v0.1 forbids overloading per tenant so `(tenant, name)` is the unique
/// key.
pub(crate) async fn exec_drop_function(
    sess: &TenantSession,
    if_exists: bool,
    names: Vec<ObjectName>,
) -> Result<ExecResult> {
    if names.is_empty() {
        return Err(BasinError::InvalidSchema(
            "DROP FUNCTION: at least one function name is required".into(),
        ));
    }
    let catalog: Arc<dyn Catalog> = sess.engine.config().catalog.clone();
    for name in names {
        let fn_name = single_part_object_name(&name)?;
        match catalog.drop_sql_function(&sess.tenant, &fn_name).await {
            Ok(()) => {}
            Err(BasinError::NotFound(_)) if if_exists => {}
            Err(BasinError::NotFound(_)) => {
                return Err(BasinError::not_found(format!(
                    "function {fn_name:?} does not exist"
                )));
            }
            Err(e) => return Err(e),
        }
    }
    Ok(ExecResult::Empty {
        tag: "DROP FUNCTION".into(),
    })
}

/// Execute `ALTER FUNCTION <old>(<args>) RENAME TO <new>`. sqlparser 0.52
/// has no `AlterFunction` AST node; the caller pre-screens the SQL via
/// [`match_alter_function_rename`] and routes here on a match.
pub(crate) async fn exec_alter_function_rename(
    sess: &TenantSession,
    old_name: &str,
    new_name: &str,
) -> Result<ExecResult> {
    if old_name.eq_ignore_ascii_case(new_name) {
        return Ok(ExecResult::Empty {
            tag: "ALTER FUNCTION".into(),
        });
    }
    let catalog: Arc<dyn Catalog> = sess.engine.config().catalog.clone();
    let existing = catalog
        .lookup_sql_function(&sess.tenant, old_name)
        .await
        .ok_or_else(|| {
            BasinError::not_found(format!("function {old_name:?} does not exist"))
        })?;
    if catalog
        .lookup_sql_function(&sess.tenant, new_name)
        .await
        .is_some()
    {
        return Err(BasinError::InvalidSchema(format!(
            "ALTER FUNCTION RENAME: target name {new_name:?} already exists"
        )));
    }
    let renamed = SqlFunctionDef {
        name: new_name.to_string(),
        ..existing
    };
    let validated = validate_for_registration(renamed)?;
    // Treat the rename as if registering `new_name` against the post-
    // rename catalog: skip both the old name (about to be dropped) and
    // the new name (replaced by `validated`).
    let mut existing_map = collect_tenant_function_map(&catalog, &sess.tenant, new_name).await;
    existing_map.remove(&old_name.to_ascii_lowercase());
    validate_no_mutual_recursion(&validated, &existing_map)?;
    catalog.register_sql_function(validated).await?;
    catalog.drop_sql_function(&sess.tenant, old_name).await?;
    Ok(ExecResult::Empty {
        tag: "ALTER FUNCTION".into(),
    })
}

/// Snapshot the tenant's currently-registered SQL functions into a map
/// keyed by lowercased name, omitting `excluded_name`. The exclusion is
/// for `CREATE OR REPLACE` semantics (the new def is authoritative for
/// its own name) and for `ALTER ... RENAME TO <new>` (where the new
/// name will displace any prior entry).
async fn collect_tenant_function_map(
    catalog: &Arc<dyn Catalog>,
    tenant: &basin_common::TenantId,
    excluded_name: &str,
) -> std::collections::HashMap<String, SqlFunctionDef> {
    let funcs = catalog.list_sql_functions(tenant).await;
    let excl = excluded_name.to_ascii_lowercase();
    let mut map = std::collections::HashMap::with_capacity(funcs.len());
    for f in funcs {
        let key = f.name.to_ascii_lowercase();
        if key == excl {
            continue;
        }
        map.insert(key, f);
    }
    map
}

/// Recognise `ALTER FUNCTION <name> [(<args>)] RENAME TO <new_name>`
/// (with optional trailing `;`). Returns `(old_name, new_name)` on a
/// match, `None` when the statement is something else. sqlparser 0.52
/// does not model `ALTER FUNCTION` so we handle the full statement
/// textually before sqlparser sees it.
pub(crate) fn match_alter_function_rename(sql: &str) -> Result<Option<(String, String)>> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    if !starts_with_kw(trimmed, "ALTER") {
        return Ok(None);
    }
    let after_alter = skip_word(trimmed).trim_start();
    if !starts_with_kw(after_alter, "FUNCTION") {
        return Ok(None);
    }
    let after_fn = skip_word(after_alter).trim_start();
    let (old_name, after_old_name) = read_simple_identifier(after_fn)?;
    // Optional `(arglist)` — accept and skip.
    let after_args = match skip_paren_block(after_old_name.trim_start()) {
        Some(rest) => rest,
        None => after_old_name,
    };
    let after_args = after_args.trim_start();
    if !starts_with_kw(after_args, "RENAME") {
        return Err(BasinError::InvalidSchema(
            "ALTER FUNCTION: only RENAME TO is supported in v0.1".into(),
        ));
    }
    let after_rename = skip_word(after_args).trim_start();
    if !starts_with_kw(after_rename, "TO") {
        return Err(BasinError::InvalidSchema(
            "ALTER FUNCTION RENAME: expected TO".into(),
        ));
    }
    let after_to = skip_word(after_rename).trim_start();
    let (new_name, rest) = read_simple_identifier(after_to)?;
    let rest = rest.trim();
    if !rest.is_empty() {
        return Err(BasinError::InvalidSchema(format!(
            "ALTER FUNCTION RENAME TO: unexpected trailing input {rest:?}"
        )));
    }
    Ok(Some((old_name, new_name)))
}

/// Translate the sqlparser-parsed RETURNS clause into a `SqlReturnType`.
/// `RETURNS TABLE(col type, ...)` parses as `DataType::Custom("TABLE", [c1, t1, c2, t2, ...])`
/// (the modifiers are flat strings); anything else is treated as a scalar
/// return and routed through [`sql_data_type_to_arg`].
fn parse_return_type(dt: &SqlDataType, fn_name: &str) -> Result<SqlReturnType> {
    if let SqlDataType::Custom(obj, modifiers) = dt {
        if obj.0.len() == 1 && obj.0[0].value.eq_ignore_ascii_case("TABLE") {
            return parse_returns_table(modifiers, fn_name);
        }
    }
    let arg = sql_data_type_to_arg(dt).map_err(|e| {
        BasinError::InvalidSchema(format!(
            "CREATE FUNCTION {fn_name}: unsupported RETURNS type ({e})"
        ))
    })?;
    Ok(SqlReturnType::Scalar(arg))
}

/// Parse `[col1, type1, col2, type2, ...]` into a `Table` return type.
/// sqlparser flattens `RETURNS TABLE(a BIGINT, b TEXT)` into a single
/// `Vec<String>` of `["a", "BIGINT", "b", "TEXT"]`; we re-parse each type
/// token via the dialect to reuse `sql_data_type_to_arg` rather than
/// hand-rolling a second mapping.
fn parse_returns_table(modifiers: &[String], fn_name: &str) -> Result<SqlReturnType> {
    if modifiers.is_empty() || modifiers.len() % 2 != 0 {
        return Err(BasinError::InvalidSchema(format!(
            "CREATE FUNCTION {fn_name}: RETURNS TABLE requires at least one (name type) pair"
        )));
    }
    let mut cols: Vec<(String, SqlArgType)> = Vec::with_capacity(modifiers.len() / 2);
    let mut seen = std::collections::HashSet::new();
    for pair in modifiers.chunks_exact(2) {
        let col_name = pair[0].clone();
        let type_text = pair[1].clone();
        if !seen.insert(col_name.to_ascii_lowercase()) {
            return Err(BasinError::InvalidSchema(format!(
                "CREATE FUNCTION {fn_name}: RETURNS TABLE column name {col_name:?} \
                 appears more than once"
            )));
        }
        let dt = parse_type_text(&type_text).map_err(|e| {
            BasinError::InvalidSchema(format!(
                "CREATE FUNCTION {fn_name}: RETURNS TABLE column {col_name:?} type \
                 {type_text:?} unsupported ({e})"
            ))
        })?;
        let arg = sql_data_type_to_arg(&dt).map_err(|e| {
            BasinError::InvalidSchema(format!(
                "CREATE FUNCTION {fn_name}: RETURNS TABLE column {col_name:?} type \
                 unsupported ({e})"
            ))
        })?;
        cols.push((col_name, arg));
    }
    Ok(SqlReturnType::Table(cols))
}

/// Re-parse a type token (e.g. `"BIGINT"`, `"TIMESTAMPTZ"`) back into
/// sqlparser's `DataType`. Used to bridge the flat-string modifier list
/// from `RETURNS TABLE` into the existing `sql_data_type_to_arg` mapping.
fn parse_type_text(s: &str) -> Result<SqlDataType> {
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;
    let dialect = PostgreSqlDialect {};
    let mut p = Parser::new(&dialect)
        .try_with_sql(s)
        .map_err(|e| BasinError::InvalidSchema(format!("type {s:?} does not parse: {e}")))?;
    p.parse_data_type()
        .map_err(|e| BasinError::InvalidSchema(format!("type {s:?} does not parse: {e}")))
}

/// Map a sqlparser `DataType` to one of the catalog's `SqlArgType`
/// variants. Mirrors (a subset of) `crate::types::arrow_data_type` but
/// targets the narrower scalar set the function catalog supports.
fn sql_data_type_to_arg(dt: &SqlDataType) -> Result<SqlArgType> {
    match dt {
        SqlDataType::Int(_) | SqlDataType::Integer(_) | SqlDataType::Int4(_) => {
            Ok(SqlArgType::Int)
        }
        SqlDataType::BigInt(_) | SqlDataType::Int8(_) => Ok(SqlArgType::BigInt),
        SqlDataType::Text
        | SqlDataType::Varchar(_)
        | SqlDataType::CharacterVarying(_)
        | SqlDataType::Char(_)
        | SqlDataType::Character(_)
        | SqlDataType::String(_) => Ok(SqlArgType::Text),
        SqlDataType::Boolean | SqlDataType::Bool => Ok(SqlArgType::Boolean),
        SqlDataType::Double
        | SqlDataType::DoublePrecision
        | SqlDataType::Float8
        | SqlDataType::Float(_) => Ok(SqlArgType::Double),
        SqlDataType::Bytea => Ok(SqlArgType::Bytea),
        SqlDataType::Date => Ok(SqlArgType::Date),
        SqlDataType::Timestamp(_, tz_info) => match tz_info {
            TimezoneInfo::Tz | TimezoneInfo::WithTimeZone => Ok(SqlArgType::TimestampTz),
            _ => Err(BasinError::InvalidSchema(
                "TIMESTAMP without time zone is not supported; use TIMESTAMPTZ".into(),
            )),
        },
        other => Err(BasinError::InvalidSchema(format!(
            "unsupported function arg / return type: {other}"
        ))),
    }
}

/// Pull the body text out of a parsed `CREATE FUNCTION ... AS $$ ... $$`.
/// sqlparser models the body as an `Expr::Value` wrapping either a
/// `DollarQuotedString` (the dollar-quoted form) or `SingleQuotedString`
/// (a plain string literal). We accept both. `RETURN <expr>` is the
/// SQL/PSM shape and is rejected here because we store and re-parse the
/// body as a SELECT statement.
fn extract_body_text(body: &Option<CreateFunctionBody>, fn_name: &str) -> Result<String> {
    let body = body.as_ref().ok_or_else(|| {
        BasinError::InvalidSchema(format!(
            "CREATE FUNCTION {fn_name}: missing function body (`AS $$ ... $$`)"
        ))
    })?;
    match body {
        CreateFunctionBody::AsBeforeOptions(expr) | CreateFunctionBody::AsAfterOptions(expr) => {
            match expr {
                Expr::Value(Value::DollarQuotedString(s)) => Ok(s.value.clone()),
                Expr::Value(Value::SingleQuotedString(s)) => Ok(s.clone()),
                other => Err(BasinError::InvalidSchema(format!(
                    "CREATE FUNCTION {fn_name}: function body must be a dollar-quoted \
                     or single-quoted string literal containing a single SELECT; got {other}"
                ))),
            }
        }
        CreateFunctionBody::Return(_) => Err(BasinError::InvalidSchema(format!(
            "CREATE FUNCTION {fn_name}: SQL/PSM `RETURN <expr>` form is not supported \
             in v0.1; use `AS $$ SELECT <expr> $$`"
        ))),
    }
}

/// Pull a bare-identifier function name out of an `ObjectName`. Schema-
/// qualified names are out of scope for v0.1 (matches the rest of the
/// engine's DDL).
fn single_part_object_name(name: &ObjectName) -> Result<String> {
    if name.0.len() != 1 {
        return Err(BasinError::InvalidIdent(format!(
            "schema-qualified function names not supported in v0.1: {name}"
        )));
    }
    Ok(name.0[0].value.clone())
}

// --- Lexer helpers (mirrors cv_ddl.rs's textual matchers) ---------------

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

/// If `s` starts with `(`, return the slice immediately after the
/// matching `)`. Tracks nested parens and skips simple string literals so
/// e.g. `('hello)world')` doesn't terminate early. Returns `None` when
/// `s` does not start with `(`.
fn skip_paren_block(s: &str) -> Option<&str> {
    let bytes = s.as_bytes();
    if bytes.is_empty() || bytes[0] != b'(' {
        return None;
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
        return None;
    }
    Some(&s[i..])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn match_alter_rename_basic() {
        let r = match_alter_function_rename("ALTER FUNCTION add_one(BIGINT) RENAME TO add_1")
            .unwrap();
        assert_eq!(r, Some(("add_one".into(), "add_1".into())));
    }

    #[test]
    fn match_alter_rename_no_args() {
        let r = match_alter_function_rename("ALTER FUNCTION add_one RENAME TO add_1").unwrap();
        assert_eq!(r, Some(("add_one".into(), "add_1".into())));
    }

    #[test]
    fn match_alter_rename_trailing_semicolon() {
        let r = match_alter_function_rename("ALTER FUNCTION f() RENAME TO g;").unwrap();
        assert_eq!(r, Some(("f".into(), "g".into())));
    }

    #[test]
    fn match_alter_rename_case_insensitive() {
        let r = match_alter_function_rename("alter function f(int) rename to g").unwrap();
        assert_eq!(r, Some(("f".into(), "g".into())));
    }

    #[test]
    fn match_alter_non_function_returns_none() {
        let r = match_alter_function_rename("ALTER TABLE foo ADD COLUMN bar INT").unwrap();
        assert!(r.is_none());
    }

    #[test]
    fn match_alter_rename_unsupported_clause_errors() {
        let err = match_alter_function_rename("ALTER FUNCTION f() OWNER TO bob").unwrap_err();
        assert!(matches!(err, BasinError::InvalidSchema(_)));
    }

    #[test]
    fn skip_paren_block_handles_nesting() {
        let s = "(a, (b, c), d)rest";
        assert_eq!(skip_paren_block(s), Some("rest"));
    }
}
