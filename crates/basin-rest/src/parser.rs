//! URL → SQL translation.
//!
//! The translator turns a `(table, query-string)` pair into a single SQL
//! statement that the engine can execute. The output is shape-equivalent to
//! what the same query would produce if the user had written the SQL by
//! hand against pgwire (modulo column-projection sugar).
//!
//! ## Injection defense
//!
//! Every value reaching this module is *untrusted user input*. We never
//! interpolate raw bytes into SQL. There are two strategies:
//!
//! 1. **Identifiers** (table name, column name, sort columns, projection
//!    columns) flow through [`basin_common::Ident::new`], which enforces
//!    `[A-Za-z_][A-Za-z0-9_]*` and length ≤ 63. Anything else is rejected
//!    with a `400 INVALID_REQUEST`. Because the regex is whitelisted by
//!    construction, the resulting string is safe to splice into SQL.
//!
//! 2. **Values** (the right-hand side of `eq.<value>`, the elements of an
//!    `in.(a,b,c)` list, the bodies of POST/PATCH JSON requests) are
//!    classified into one of three shapes:
//!
//!    - looks like an integer → emit as a bare integer literal,
//!    - looks like a float → emit as a bare float literal,
//!    - the literal `true` / `false` / `null` → emit the SQL keyword,
//!    - everything else → emit a `'…'`-quoted string with single quotes
//!      doubled (the standard SQL escape).
//!
//!    The escaping mirrors `basin_engine::prepared::quote_string` so REST
//!    and pgwire have one set of rules.
//!
//! Property: identical results to the equivalent pgwire query, given the
//! same body. The integration test `crud_round_trip` exercises this.

use basin_common::{BasinError, Ident};

use crate::errors::ApiError;

/// Outcome of parsing a `?key=value&...` query string.
#[derive(Debug, Default)]
pub(crate) struct Query {
    /// Column projection. `None` means `SELECT *`.
    pub select: Option<Vec<String>>,
    /// `(col, op, value)` filters. Joined with `AND` in `WHERE`.
    pub filters: Vec<Filter>,
    /// Ordering. Empty means no `ORDER BY`.
    pub order: Vec<OrderItem>,
    /// `?limit=N`. Caller clamps to `max_page_size`.
    pub limit: Option<u64>,
    /// `?offset=N`.
    pub offset: Option<u64>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Filter {
    pub column: String,
    pub op: FilterOp,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum FilterOp {
    Eq(Literal),
    Neq(Literal),
    Gt(Literal),
    Gte(Literal),
    Lt(Literal),
    Lte(Literal),
    In(Vec<Literal>),
    IsNull,
    IsNotNull,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct OrderItem {
    pub column: String,
    pub desc: bool,
}

/// A single user-supplied value, classified for safe rendering. We never store
/// the *rendered* form here; we render at the splice site so the splice
/// strategy lives in one place.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Literal {
    /// SQL `NULL`.
    Null,
    /// `true` / `false`.
    Bool(bool),
    /// An integer that fits in `i64`.
    Int(i64),
    /// A floating-point number that round-trips through `f64`. Stored as the
    /// original string to avoid `1e10` ↔ `10000000000` surprises.
    Float(String),
    /// Anything else. Will be emitted as a SQL string literal.
    Text(String),
}

/// Parse a sequence of `(key, value)` query-string pairs into a [`Query`].
pub(crate) fn parse_query<I, K, V>(pairs: I) -> std::result::Result<Query, ApiError>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: AsRef<str>,
{
    let mut q = Query::default();
    for (k, v) in pairs {
        let key = k.as_ref();
        let val = v.as_ref();
        match key {
            "select" => {
                let cols = parse_select(val)?;
                q.select = Some(cols);
            }
            "order" => {
                q.order = parse_order(val)?;
            }
            "limit" => {
                q.limit = Some(
                    val.parse()
                        .map_err(|_| ApiError::invalid(format!("invalid limit: {val:?}")))?,
                );
            }
            "offset" => {
                q.offset = Some(
                    val.parse()
                        .map_err(|_| ApiError::invalid(format!("invalid offset: {val:?}")))?,
                );
            }
            // Anything else is treated as a column filter: `<col>=<op>.<rest>`.
            col => {
                let column = validate_ident(col)?;
                let op = parse_op(val)?;
                q.filters.push(Filter { column, op });
            }
        }
    }
    Ok(q)
}

fn parse_select(v: &str) -> std::result::Result<Vec<String>, ApiError> {
    if v == "*" {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for raw in v.split(',') {
        let s = raw.trim();
        if s.is_empty() {
            continue;
        }
        out.push(validate_ident(s)?);
    }
    if out.is_empty() {
        // `select=` with nothing useful → behave as `*`.
        return Ok(Vec::new());
    }
    Ok(out)
}

fn parse_order(v: &str) -> std::result::Result<Vec<OrderItem>, ApiError> {
    let mut out = Vec::new();
    for raw in v.split(',') {
        let s = raw.trim();
        if s.is_empty() {
            continue;
        }
        let (col, dir) = match s.split_once('.') {
            Some((c, d)) => (c, Some(d)),
            None => (s, None),
        };
        let column = validate_ident(col)?;
        let desc = match dir {
            None | Some("asc") => false,
            Some("desc") => true,
            Some(other) => {
                return Err(ApiError::invalid(format!(
                    "order direction must be asc|desc, got {other:?}"
                )))
            }
        };
        out.push(OrderItem { column, desc });
    }
    Ok(out)
}

fn parse_op(v: &str) -> std::result::Result<FilterOp, ApiError> {
    let (op, rest) = v
        .split_once('.')
        .ok_or_else(|| ApiError::invalid(format!("filter must be `<op>.<value>`, got {v:?}")))?;
    Ok(match op {
        "eq" => FilterOp::Eq(parse_literal(rest)),
        "neq" => FilterOp::Neq(parse_literal(rest)),
        "gt" => FilterOp::Gt(parse_literal(rest)),
        "gte" => FilterOp::Gte(parse_literal(rest)),
        "lt" => FilterOp::Lt(parse_literal(rest)),
        "lte" => FilterOp::Lte(parse_literal(rest)),
        "in" => FilterOp::In(parse_in_list(rest)?),
        "is" => match rest {
            "null" => FilterOp::IsNull,
            "notnull" | "not.null" => FilterOp::IsNotNull,
            other => {
                return Err(ApiError::invalid(format!(
                    "is.<value> must be null|notnull, got {other:?}"
                )))
            }
        },
        other => {
            return Err(ApiError::invalid(format!(
                "unknown filter op {other:?}; supported: eq|neq|gt|gte|lt|lte|in|is"
            )))
        }
    })
}

fn parse_in_list(v: &str) -> std::result::Result<Vec<Literal>, ApiError> {
    let inner = v
        .strip_prefix('(')
        .and_then(|s| s.strip_suffix(')'))
        .ok_or_else(|| ApiError::invalid(format!("in.(…) must be parenthesised, got {v:?}")))?;
    if inner.is_empty() {
        return Err(ApiError::invalid("in.() must contain at least one value"));
    }
    Ok(inner.split(',').map(|s| parse_literal(s.trim())).collect())
}

/// Classify a raw value as a literal. Never fails — anything we can't
/// classify becomes [`Literal::Text`].
fn parse_literal(v: &str) -> Literal {
    match v {
        "null" => Literal::Null,
        "true" => Literal::Bool(true),
        "false" => Literal::Bool(false),
        _ => {
            if let Ok(n) = v.parse::<i64>() {
                Literal::Int(n)
            } else if v.parse::<f64>().is_ok()
                // Integer-with-leading-zero ("007") would otherwise round-trip
                // through f64 and lose digits. Accept floats only when the
                // string contains '.' or an exponent marker.
                && (v.contains('.') || v.contains('e') || v.contains('E'))
            {
                Literal::Float(v.to_owned())
            } else {
                Literal::Text(v.to_owned())
            }
        }
    }
}

/// Validate that `s` is a SQL identifier we'll accept. Wraps
/// [`basin_common::Ident::new`] to share the identifier rules with the
/// engine.
pub(crate) fn validate_ident(s: &str) -> std::result::Result<String, ApiError> {
    Ident::new(s)
        .map(|i| i.as_str().to_owned())
        .map_err(|e: BasinError| ApiError::invalid(e.to_string()))
}

// --- SQL rendering ----------------------------------------------------------

/// Build the `SELECT` SQL for a `(table, query)` pair.
pub(crate) fn build_select_sql(
    table: &str,
    query: &Query,
    default_limit: u64,
    max_limit: u64,
) -> std::result::Result<String, ApiError> {
    let table = validate_ident(table)?;
    let mut sql = String::new();
    sql.push_str("SELECT ");
    match query.select.as_deref() {
        None | Some([]) => sql.push('*'),
        Some(cols) => {
            for (i, c) in cols.iter().enumerate() {
                if i > 0 {
                    sql.push_str(", ");
                }
                sql.push_str(c);
            }
        }
    }
    sql.push_str(" FROM ");
    sql.push_str(&table);
    write_where(&mut sql, &query.filters)?;
    if !query.order.is_empty() {
        sql.push_str(" ORDER BY ");
        for (i, o) in query.order.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            sql.push_str(&o.column);
            sql.push_str(if o.desc { " DESC" } else { " ASC" });
        }
    }
    let limit = query.limit.unwrap_or(default_limit).min(max_limit);
    sql.push_str(&format!(" LIMIT {limit}"));
    if let Some(off) = query.offset {
        sql.push_str(&format!(" OFFSET {off}"));
    }
    Ok(sql)
}

/// Build the `DELETE` SQL. Filters are required — no `?` query string would
/// translate to a `DELETE FROM <t>` with no `WHERE`, which is too dangerous
/// to ship by default.
pub(crate) fn build_delete_sql(
    table: &str,
    query: &Query,
) -> std::result::Result<String, ApiError> {
    let table = validate_ident(table)?;
    if query.filters.is_empty() {
        return Err(ApiError::invalid(
            "DELETE requires at least one filter to avoid wiping the table",
        ));
    }
    let mut sql = String::new();
    sql.push_str("DELETE FROM ");
    sql.push_str(&table);
    write_where(&mut sql, &query.filters)?;
    Ok(sql)
}

/// Build the `UPDATE` SQL. The body is a JSON object whose keys are column
/// names and values are the new column values. As with `DELETE`, filters
/// are required — patching every row of a table from a single REST call
/// is the kind of foot-gun we don't ship by default.
pub(crate) fn build_update_sql(
    table: &str,
    query: &Query,
    body: &serde_json::Value,
) -> std::result::Result<String, ApiError> {
    let table = validate_ident(table)?;
    if query.filters.is_empty() {
        return Err(ApiError::invalid(
            "PATCH requires at least one filter to avoid updating every row",
        ));
    }
    let obj = match body {
        serde_json::Value::Object(o) => o,
        _ => {
            return Err(ApiError::invalid(
                "PATCH body must be a JSON object whose keys are column names",
            ));
        }
    };
    if obj.is_empty() {
        return Err(ApiError::invalid("PATCH body has no columns to update"));
    }
    let mut sql = String::new();
    sql.push_str("UPDATE ");
    sql.push_str(&table);
    sql.push_str(" SET ");
    let mut first = true;
    for (k, v) in obj {
        let col = validate_ident(k)?;
        let lit = json_to_literal(v)?;
        if !first {
            sql.push_str(", ");
        }
        first = false;
        sql.push_str(&col);
        sql.push_str(" = ");
        sql.push_str(&render_literal(&lit));
    }
    write_where(&mut sql, &query.filters)?;
    Ok(sql)
}

/// Build the `INSERT` SQL given a list of (column-vec, value-vec) rows.
/// `rows` must be non-empty; each row's value-vec must have the same length
/// as `columns`.
pub(crate) fn build_insert_sql(
    table: &str,
    columns: &[String],
    rows: &[Vec<Literal>],
) -> std::result::Result<String, ApiError> {
    let table = validate_ident(table)?;
    if columns.is_empty() {
        return Err(ApiError::invalid("INSERT body has no columns"));
    }
    if rows.is_empty() {
        return Err(ApiError::invalid("INSERT body has no rows"));
    }
    for c in columns {
        // We trust columns to be already-validated (caller did it). Re-check
        // defensively; cost is negligible compared to the engine call.
        validate_ident(c)?;
    }
    let mut sql = String::with_capacity(64 + 16 * columns.len() * rows.len());
    sql.push_str("INSERT INTO ");
    sql.push_str(&table);
    sql.push_str(" (");
    for (i, c) in columns.iter().enumerate() {
        if i > 0 {
            sql.push_str(", ");
        }
        sql.push_str(c);
    }
    sql.push_str(") VALUES ");
    for (ri, row) in rows.iter().enumerate() {
        if ri > 0 {
            sql.push_str(", ");
        }
        if row.len() != columns.len() {
            return Err(ApiError::invalid(format!(
                "row {ri} has {} values; expected {}",
                row.len(),
                columns.len()
            )));
        }
        sql.push('(');
        for (vi, v) in row.iter().enumerate() {
            if vi > 0 {
                sql.push_str(", ");
            }
            sql.push_str(&render_literal(v));
        }
        sql.push(')');
    }
    Ok(sql)
}

fn write_where(
    sql: &mut String,
    filters: &[Filter],
) -> std::result::Result<(), ApiError> {
    if filters.is_empty() {
        return Ok(());
    }
    sql.push_str(" WHERE ");
    for (i, f) in filters.iter().enumerate() {
        if i > 0 {
            sql.push_str(" AND ");
        }
        match &f.op {
            FilterOp::Eq(v) => {
                sql.push_str(&f.column);
                sql.push_str(" = ");
                sql.push_str(&render_literal(v));
            }
            FilterOp::Neq(v) => {
                sql.push_str(&f.column);
                sql.push_str(" <> ");
                sql.push_str(&render_literal(v));
            }
            FilterOp::Gt(v) => {
                sql.push_str(&f.column);
                sql.push_str(" > ");
                sql.push_str(&render_literal(v));
            }
            FilterOp::Gte(v) => {
                sql.push_str(&f.column);
                sql.push_str(" >= ");
                sql.push_str(&render_literal(v));
            }
            FilterOp::Lt(v) => {
                sql.push_str(&f.column);
                sql.push_str(" < ");
                sql.push_str(&render_literal(v));
            }
            FilterOp::Lte(v) => {
                sql.push_str(&f.column);
                sql.push_str(" <= ");
                sql.push_str(&render_literal(v));
            }
            FilterOp::In(items) => {
                sql.push_str(&f.column);
                sql.push_str(" IN (");
                for (j, it) in items.iter().enumerate() {
                    if j > 0 {
                        sql.push_str(", ");
                    }
                    sql.push_str(&render_literal(it));
                }
                sql.push(')');
            }
            FilterOp::IsNull => {
                sql.push_str(&f.column);
                sql.push_str(" IS NULL");
            }
            FilterOp::IsNotNull => {
                sql.push_str(&f.column);
                sql.push_str(" IS NOT NULL");
            }
        }
    }
    Ok(())
}

/// Render a [`Literal`] as a SQL value. Strings get single-quoted with
/// internal `'` doubled — the standard SQL escape, matching
/// `basin_engine::prepared::quote_string`.
pub(crate) fn render_literal(v: &Literal) -> String {
    match v {
        Literal::Null => "NULL".into(),
        Literal::Bool(true) => "TRUE".into(),
        Literal::Bool(false) => "FALSE".into(),
        Literal::Int(n) => n.to_string(),
        Literal::Float(s) => s.clone(),
        Literal::Text(s) => quote_sql_string(s),
    }
}

/// SQL string literal: wrap in `'…'`, double internal quotes. The engine
/// uses the same routine for prepared-statement substitution; keeping them
/// aligned means a REST query and a pgwire query for the same value produce
/// the same SQL.
pub(crate) fn quote_sql_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for c in s.chars() {
        if c == '\'' {
            out.push('\'');
            out.push('\'');
        } else {
            out.push(c);
        }
    }
    out.push('\'');
    out
}

/// Convert a [`serde_json::Value`] to a [`Literal`] for the INSERT/PATCH paths.
/// Objects and arrays are not supported — REST v1 doesn't ship JSON column
/// support.
pub(crate) fn json_to_literal(v: &serde_json::Value) -> std::result::Result<Literal, ApiError> {
    Ok(match v {
        serde_json::Value::Null => Literal::Null,
        serde_json::Value::Bool(b) => Literal::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Literal::Int(i)
            } else if n.is_f64() {
                Literal::Float(n.to_string())
            } else {
                // u64 that doesn't fit i64 — fall back to text to avoid
                // silent truncation.
                Literal::Text(n.to_string())
            }
        }
        serde_json::Value::String(s) => Literal::Text(s.clone()),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            return Err(ApiError::invalid(
                "JSON column values are not supported in REST v1",
            ))
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn q(pairs: &[(&str, &str)]) -> Query {
        parse_query(pairs.iter().copied()).expect("ok")
    }

    #[test]
    fn parses_eq_filter() {
        let parsed = q(&[("id", "eq.42")]);
        assert_eq!(parsed.filters.len(), 1);
        assert_eq!(parsed.filters[0].column, "id");
        match &parsed.filters[0].op {
            FilterOp::Eq(Literal::Int(42)) => {}
            other => panic!("unexpected op: {other:?}"),
        }
    }

    #[test]
    fn parses_in_list() {
        let parsed = q(&[("id", "in.(1,2,3)")]);
        match &parsed.filters[0].op {
            FilterOp::In(items) => assert_eq!(items.len(), 3),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn parses_order_and_limit() {
        let parsed = q(&[("order", "id.desc,name.asc"), ("limit", "10"), ("offset", "5")]);
        assert_eq!(parsed.order.len(), 2);
        assert!(parsed.order[0].desc);
        assert!(!parsed.order[1].desc);
        assert_eq!(parsed.limit, Some(10));
        assert_eq!(parsed.offset, Some(5));
    }

    #[test]
    fn rejects_bad_identifier() {
        let err = parse_query([("x;DROP TABLE users;--", "eq.1")]).unwrap_err();
        assert_eq!(err.code, crate::errors::ErrorCode::InvalidRequest);
    }

    #[test]
    fn select_sql_wraps_with_limit() {
        let q = q(&[("id", "eq.42"), ("limit", "10000")]);
        let sql = build_select_sql("users", &q, 100, 1000).unwrap();
        assert_eq!(sql, "SELECT * FROM users WHERE id = 42 LIMIT 1000");
    }

    #[test]
    fn select_sql_columns_filtered() {
        let q = q(&[("select", "id,name")]);
        let sql = build_select_sql("users", &q, 100, 1000).unwrap();
        assert_eq!(sql, "SELECT id, name FROM users LIMIT 100");
    }

    #[test]
    fn quote_sql_string_doubles_internal_quotes() {
        assert_eq!(quote_sql_string("o'reilly"), "'o''reilly'");
        assert_eq!(quote_sql_string("'; DROP TABLE x; --"), "'''; DROP TABLE x; --'");
    }

    #[test]
    fn delete_requires_filter() {
        let q = Query::default();
        assert!(build_delete_sql("users", &q).is_err());
    }

    #[test]
    fn delete_sql_renders() {
        let q = q(&[("id", "eq.42")]);
        let sql = build_delete_sql("users", &q).unwrap();
        assert_eq!(sql, "DELETE FROM users WHERE id = 42");
    }

    #[test]
    fn insert_sql_renders_two_rows() {
        let cols = vec!["id".to_string(), "name".to_string()];
        let rows = vec![
            vec![Literal::Int(1), Literal::Text("a".into())],
            vec![Literal::Int(2), Literal::Text("b".into())],
        ];
        let sql = build_insert_sql("t", &cols, &rows).unwrap();
        assert_eq!(sql, "INSERT INTO t (id, name) VALUES (1, 'a'), (2, 'b')");
    }

    #[test]
    fn float_only_with_decimal_or_exp() {
        assert!(matches!(parse_literal("1.5"), Literal::Float(_)));
        assert!(matches!(parse_literal("1e10"), Literal::Float(_)));
        // "007" parses as i64=7 (the integer fast path runs before the float
        // check). That's fine for a numeric column; for a text column the
        // user will quote it themselves. The point of guarding the float
        // path is to keep "1e10" out of `Literal::Int(1)`.
        assert!(matches!(parse_literal("007"), Literal::Int(7)));
        // Decimal-like text without a leading sign and without '.' or 'e'
        // that doesn't fit i64 lands in Text.
        assert!(matches!(
            parse_literal("99999999999999999999999"),
            Literal::Text(_)
        ));
    }

    #[test]
    fn injection_attempt_in_value_is_quoted() {
        let q = q(&[("name", "eq.'; DROP TABLE users; --")]);
        let sql = build_select_sql("t", &q, 100, 1000).unwrap();
        // The single quote in the payload gets doubled; the `;` and friends
        // sit inside the string literal where they're inert.
        assert!(
            sql.contains("''';"),
            "expected escaped quote in {sql}"
        );
        assert!(
            !sql.contains("DROP TABLE users; --;"),
            "control chars escaped into SQL: {sql}"
        );
    }

    #[test]
    fn neq_and_gte_lte() {
        let q = q(&[("a", "neq.1"), ("b", "gte.2"), ("c", "lte.3")]);
        let sql = build_select_sql("t", &q, 100, 1000).unwrap();
        assert!(sql.contains("a <> 1"));
        assert!(sql.contains("b >= 2"));
        assert!(sql.contains("c <= 3"));
    }

    #[test]
    fn is_null_and_notnull() {
        let q = q(&[("a", "is.null"), ("b", "is.notnull")]);
        let sql = build_select_sql("t", &q, 100, 1000).unwrap();
        assert!(sql.contains("a IS NULL"));
        assert!(sql.contains("b IS NOT NULL"));
    }
}
