//! CREATE TABLE: sqlparser AST → Arrow [`Schema`].

use std::collections::HashMap;

use arrow_schema::{DataType, Field, Schema, TimeUnit};
use basin_catalog::PartitionSpec;
use basin_common::{BasinError, Result};
use sqlparser::ast::{ColumnDef, ColumnOption, Expr, FunctionArg, FunctionArgExpr, FunctionArguments};

use crate::types::{arrow_data_type, BASIN_TYPE_JSONB, BASIN_TYPE_KEY, BASIN_TYPE_UUID};

/// Inspect `sql` to decide whether the user wrote JSONB (or plain JSON;
/// for v0.1 we treat them as the same logical type — see the comment in
/// `crate::types::arrow_data_type`). JSONB has no dedicated Arrow
/// `DataType`, so `schema_from_columns` tags the resulting `Field` with
/// metadata the rest of the engine reads to recover the logical type.
fn is_jsonb_sql(sql: &sqlparser::ast::DataType) -> bool {
    use sqlparser::ast::DataType as SqlDataType;
    match sql {
        SqlDataType::JSONB | SqlDataType::JSON => true,
        SqlDataType::Custom(name, modifiers) => {
            name.0.len() == 1
                && name.0[0].value.eq_ignore_ascii_case("jsonb")
                && modifiers.is_empty()
        }
        _ => false,
    }
}

/// Mirror of `is_jsonb_sql` for UUID columns. UUID rides on
/// `FixedSizeBinary(16)` at the Arrow level; the metadata marker tells the
/// pgwire encoder + REST layer to render bytes as the canonical hyphenated
/// text form (and to advertise OID 2950 instead of `bytea`).
fn is_uuid_sql(sql: &sqlparser::ast::DataType) -> bool {
    use sqlparser::ast::DataType as SqlDataType;
    match sql {
        SqlDataType::Uuid => true,
        SqlDataType::Custom(name, modifiers) => {
            name.0.len() == 1
                && name.0[0].value.eq_ignore_ascii_case("uuid")
                && modifiers.is_empty()
        }
        _ => false,
    }
}

/// Build an Arrow [`Schema`] from sqlparser column definitions.
///
/// Nullability defaults to `true`; `NOT NULL` flips it. Other column options
/// (DEFAULT, UNIQUE, PRIMARY KEY, FOREIGN KEY, etc.) are explicitly out of
/// scope for the PoC and trigger `InvalidSchema` so we don't silently drop
/// constraints the user expected to hold.
pub(crate) fn schema_from_columns(columns: &[ColumnDef]) -> Result<Schema> {
    if columns.is_empty() {
        return Err(BasinError::InvalidSchema(
            "CREATE TABLE requires at least one column".into(),
        ));
    }
    let mut fields = Vec::with_capacity(columns.len());
    for col in columns {
        let dt = arrow_data_type(&col.data_type)?;
        let mut nullable = true;
        for opt in &col.options {
            match &opt.option {
                ColumnOption::NotNull => nullable = false,
                ColumnOption::Null => nullable = true,
                other => {
                    return Err(BasinError::InvalidSchema(format!(
                        "unsupported column option in PoC: {other}"
                    )));
                }
            }
        }
        let mut field = Field::new(col.name.value.clone(), dt, nullable);
        if is_jsonb_sql(&col.data_type) {
            // Tag the field so downstream layers (INSERT coercion, the
            // pgwire row encoder) know the bytes are canonical JSON, not
            // arbitrary `bytea`.
            let mut md = HashMap::with_capacity(1);
            md.insert(BASIN_TYPE_KEY.to_string(), BASIN_TYPE_JSONB.to_string());
            field = field.with_metadata(md);
        } else if is_uuid_sql(&col.data_type) {
            // UUID rides on `FixedSizeBinary(16)`; the marker lets the
            // pgwire encoder and REST layer render the canonical
            // hyphenated form rather than 16 raw bytes.
            let mut md = HashMap::with_capacity(1);
            md.insert(BASIN_TYPE_KEY.to_string(), BASIN_TYPE_UUID.to_string());
            field = field.with_metadata(md);
        }
        fields.push(field);
    }
    Ok(Schema::new(fields))
}

/// Translate the AST's `PARTITION BY ...` expression (when present) into a
/// [`PartitionSpec`]. Validates the partition column exists in `schema` and
/// has a type the partition pruner knows how to bucket. We support exactly
/// one shape today: `RANGE (col)` over `TIMESTAMPTZ` or `BIGINT`-as-epoch
/// columns. Anything else surfaces as `InvalidSchema` so a user can't
/// silently end up with an unpartitioned-but-claimed-partitioned table.
pub(crate) fn partition_spec_from_ast(
    partition_by: Option<&Expr>,
    schema: &Schema,
) -> Result<PartitionSpec> {
    let Some(expr) = partition_by else {
        return Ok(PartitionSpec::Unpartitioned);
    };
    // sqlparser parses `PARTITION BY RANGE (ts)` as `Expr::Function {
    // name: "RANGE", args: (ts) }`. Reach in and pull out the column name.
    let func = match expr {
        Expr::Function(f) => f,
        other => {
            return Err(BasinError::InvalidSchema(format!(
                "PARTITION BY expression must be RANGE(col); got {other}"
            )));
        }
    };
    let strategy = func
        .name
        .0
        .last()
        .map(|i| i.value.to_ascii_uppercase())
        .unwrap_or_default();
    if strategy != "RANGE" {
        return Err(BasinError::InvalidSchema(format!(
            "only PARTITION BY RANGE is supported in v0.1; got {strategy}"
        )));
    }
    let args = match &func.args {
        FunctionArguments::List(list) => &list.args,
        _ => {
            return Err(BasinError::InvalidSchema(
                "PARTITION BY RANGE requires a column list".into(),
            ));
        }
    };
    if args.len() != 1 {
        return Err(BasinError::InvalidSchema(format!(
            "PARTITION BY RANGE requires exactly one column, got {}",
            args.len()
        )));
    }
    let col_name = match &args[0] {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(ident))) => {
            ident.value.clone()
        }
        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::CompoundIdentifier(parts))) => {
            parts
                .last()
                .map(|p| p.value.clone())
                .ok_or_else(|| {
                    BasinError::InvalidSchema("empty compound identifier".into())
                })?
        }
        other => {
            return Err(BasinError::InvalidSchema(format!(
                "PARTITION BY RANGE column must be a bare identifier, got {other}"
            )));
        }
    };
    let field = schema
        .fields()
        .iter()
        .find(|f| f.name() == &col_name)
        .ok_or_else(|| {
            BasinError::InvalidSchema(format!(
                "PARTITION BY column {col_name:?} is not in the table schema"
            ))
        })?;
    match field.data_type() {
        DataType::Timestamp(TimeUnit::Microsecond, _)
        | DataType::Timestamp(TimeUnit::Millisecond, _)
        | DataType::Timestamp(TimeUnit::Nanosecond, _)
        | DataType::Timestamp(TimeUnit::Second, _)
        | DataType::Int64 => Ok(PartitionSpec::RangeMonthly { column: col_name }),
        other => Err(BasinError::InvalidSchema(format!(
            "PARTITION BY RANGE column {col_name} must be TIMESTAMPTZ or BIGINT-as-epoch; got {other:?}"
        ))),
    }
}

/// Phase 5.7 B2: Pre-screen `CREATE TABLE … CLUSTER BY (col1, col2)` and
/// strip the trailing clause before sqlparser sees the SQL.
///
/// `sqlparser` 0.52's `PostgreSqlDialect` only recognises `CLUSTER BY` for
/// BigQuery, so the engine handles it the same way it handles other Basin
/// extension DDL: a small string-literal-aware scan that lifts the clause
/// out of the input. Returns `(stripped_sql, Some(columns))` on a match,
/// or `(original_sql, None)` if no `CLUSTER BY` clause is present at the
/// tail of the statement.
///
/// Only the trailing `CLUSTER BY (...)` immediately preceding any
/// `;`/whitespace tail is considered — `CLUSTER BY` deeper inside the
/// statement (e.g. inside a string literal or a comment, or theoretically
/// inside a future-supported subquery) is left alone.
pub(crate) fn extract_create_table_cluster_by(sql: &str) -> Result<(String, Option<Vec<String>>)> {
    // Cheap rejection: skip the scan unless the statement starts with
    // CREATE TABLE. We don't need a perfect leading-keyword check — the
    // tail scan won't match anything else anyway, but this keeps the
    // hot path (every non-CREATE statement) at one ASCII compare.
    let leading = sql.trim_start();
    if !leading.get(..6).map(|s| s.eq_ignore_ascii_case("create")).unwrap_or(false) {
        return Ok((sql.to_string(), None));
    }

    // Scan forward, skipping over string literals and comments, recording
    // the position of every top-level `CLUSTER BY` keyword pair. The tail
    // match is the last one whose closing `)` is followed only by
    // whitespace and an optional `;`.
    let bytes = sql.as_bytes();
    let mut i = 0usize;
    let mut last_match: Option<(usize, Vec<String>, usize)> = None; // (start, cols, end_after_paren)

    while i < bytes.len() {
        let b = bytes[i];
        // String literal: `'...'` with `''` escapes.
        if b == b'\'' {
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\'' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            continue;
        }
        // Quoted identifier: `"..."` (no escape handling beyond doubled quotes).
        if b == b'"' {
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'"' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            continue;
        }
        // Line comment: `-- ...` to end-of-line.
        if b == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        // Block comment: `/* ... */`.
        if b == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
            i += 2;
            while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                i += 1;
            }
            if i + 1 < bytes.len() {
                i += 2;
            }
            continue;
        }
        // Match the keyword pair `CLUSTER BY` only at a word boundary.
        if (b == b'C' || b == b'c')
            && bytes_match_kw(&bytes[i..], b"CLUSTER")
            && at_word_boundary(bytes, i)
            && at_word_boundary_end(bytes, i + 7)
        {
            // Skip whitespace between CLUSTER and BY.
            let mut j = i + 7;
            while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                j += 1;
            }
            if j < bytes.len()
                && bytes_match_kw(&bytes[j..], b"BY")
                && at_word_boundary_end(bytes, j + 2)
            {
                let after_by = j + 2;
                // Skip whitespace, then expect `(`.
                let mut k = after_by;
                while k < bytes.len() && bytes[k].is_ascii_whitespace() {
                    k += 1;
                }
                if k < bytes.len() && bytes[k] == b'(' {
                    // Find the matching `)`.
                    let open = k;
                    let mut depth = 1i32;
                    let mut m = k + 1;
                    while m < bytes.len() && depth > 0 {
                        match bytes[m] {
                            b'(' => depth += 1,
                            b')' => depth -= 1,
                            _ => {}
                        }
                        if depth == 0 {
                            break;
                        }
                        m += 1;
                    }
                    if depth == 0 && m < bytes.len() {
                        let inside = &sql[open + 1..m];
                        let cols = parse_paren_ident_list(inside)?;
                        last_match = Some((i, cols, m + 1));
                        i = m + 1;
                        continue;
                    }
                }
            }
        }
        i += 1;
    }

    // Only accept the match if it's at the tail (whitespace + optional `;` only).
    if let Some((start, cols, end)) = last_match {
        let tail = sql[end..].trim();
        if tail.is_empty() || tail == ";" {
            // Strip the clause; preserve everything up to `start`. Re-add
            // a trailing `;` if the original ended with one.
            let mut stripped = sql[..start].trim_end().to_string();
            if sql.trim_end().ends_with(';') {
                stripped.push(';');
            }
            return Ok((stripped, Some(cols)));
        }
    }
    Ok((sql.to_string(), None))
}

/// Parse a `(col1, col2, …)` body (without the surrounding parens) into a
/// vec of bare identifiers. Rejects empty lists and non-identifier
/// tokens. Mirrors the helper in `crate::alter`.
fn parse_paren_ident_list(inside: &str) -> Result<Vec<String>> {
    let mut out = Vec::new();
    for raw in inside.split(',') {
        let token = raw.trim();
        if token.is_empty() {
            continue;
        }
        // Reject anything that isn't a bare identifier.
        if !token
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_')
            || token
                .chars()
                .next()
                .map(|c| c.is_ascii_digit())
                .unwrap_or(true)
        {
            return Err(BasinError::InvalidSchema(format!(
                "CLUSTER BY: bad identifier {token:?}"
            )));
        }
        out.push(token.to_string());
    }
    if out.is_empty() {
        return Err(BasinError::InvalidSchema(
            "CLUSTER BY: column list cannot be empty".into(),
        ));
    }
    Ok(out)
}

/// Case-insensitive prefix match on an ASCII keyword.
fn bytes_match_kw(bytes: &[u8], kw: &[u8]) -> bool {
    if bytes.len() < kw.len() {
        return false;
    }
    for (a, b) in bytes.iter().zip(kw.iter()) {
        if !a.eq_ignore_ascii_case(b) {
            return false;
        }
    }
    true
}

fn at_word_boundary(bytes: &[u8], i: usize) -> bool {
    if i == 0 {
        return true;
    }
    let prev = bytes[i - 1];
    !(prev.is_ascii_alphanumeric() || prev == b'_')
}

fn at_word_boundary_end(bytes: &[u8], i: usize) -> bool {
    if i >= bytes.len() {
        return true;
    }
    let c = bytes[i];
    !(c.is_ascii_alphanumeric() || c == b'_')
}

#[cfg(test)]
mod cluster_by_tests {
    use super::*;

    #[test]
    fn extract_single_col() {
        let (stripped, cols) =
            extract_create_table_cluster_by("CREATE TABLE foo (id BIGINT) CLUSTER BY (id)").unwrap();
        assert_eq!(stripped.trim(), "CREATE TABLE foo (id BIGINT)");
        assert_eq!(cols, Some(vec!["id".into()]));
    }

    #[test]
    fn extract_multiple_cols() {
        let (stripped, cols) = extract_create_table_cluster_by(
            "CREATE TABLE foo (id BIGINT, ts BIGINT) CLUSTER BY (id, ts)",
        )
        .unwrap();
        assert_eq!(stripped.trim(), "CREATE TABLE foo (id BIGINT, ts BIGINT)");
        assert_eq!(cols, Some(vec!["id".into(), "ts".into()]));
    }

    #[test]
    fn extract_with_trailing_semicolon() {
        let (stripped, cols) =
            extract_create_table_cluster_by("CREATE TABLE foo (id BIGINT) CLUSTER BY (id);")
                .unwrap();
        assert_eq!(stripped.trim_end_matches(|c: char| c.is_whitespace()), "CREATE TABLE foo (id BIGINT);");
        assert_eq!(cols, Some(vec!["id".into()]));
    }

    #[test]
    fn no_cluster_by() {
        let (stripped, cols) =
            extract_create_table_cluster_by("CREATE TABLE foo (id BIGINT)").unwrap();
        assert_eq!(stripped, "CREATE TABLE foo (id BIGINT)");
        assert_eq!(cols, None);
    }

    #[test]
    fn cluster_by_inside_string_literal_ignored() {
        // A literal containing the substring `CLUSTER BY (x)` must NOT
        // confuse the extractor.
        let sql =
            "CREATE TABLE foo (id BIGINT, payload TEXT DEFAULT 'CLUSTER BY (xx)')";
        let (stripped, cols) = extract_create_table_cluster_by(sql).unwrap();
        assert_eq!(stripped, sql);
        assert_eq!(cols, None);
    }

    #[test]
    fn case_insensitive() {
        let (_stripped, cols) =
            extract_create_table_cluster_by("create table foo (id BIGINT) cluster by (id)")
                .unwrap();
        assert_eq!(cols, Some(vec!["id".into()]));
    }

    #[test]
    fn empty_column_list_rejected() {
        let err = extract_create_table_cluster_by("CREATE TABLE foo (id BIGINT) CLUSTER BY ()")
            .unwrap_err();
        assert!(matches!(err, BasinError::InvalidSchema(_)));
    }

    #[test]
    fn non_create_statement_returns_none() {
        let (stripped, cols) =
            extract_create_table_cluster_by("SELECT * FROM events WHERE id = 1").unwrap();
        assert_eq!(stripped, "SELECT * FROM events WHERE id = 1");
        assert_eq!(cols, None);
    }

    #[test]
    fn cluster_by_in_block_comment_ignored() {
        let sql =
            "CREATE TABLE foo (id BIGINT) /* CLUSTER BY (x) */";
        let (stripped, cols) = extract_create_table_cluster_by(sql).unwrap();
        assert_eq!(stripped, sql);
        assert_eq!(cols, None);
    }

    #[test]
    fn cluster_by_in_line_comment_ignored() {
        let sql = "CREATE TABLE foo (id BIGINT)\n-- CLUSTER BY (x)\n";
        let (stripped, cols) = extract_create_table_cluster_by(sql).unwrap();
        assert_eq!(stripped, sql);
        assert_eq!(cols, None);
    }

    #[test]
    fn rejects_word_prefix_match() {
        // `CLUSTERED` should NOT match `CLUSTER`.
        let sql = "CREATE TABLE foo (id BIGINT) /* CLUSTERED INDEX placeholder */";
        let (stripped, cols) = extract_create_table_cluster_by(sql).unwrap();
        assert_eq!(stripped, sql);
        assert_eq!(cols, None);
    }

    #[test]
    fn cluster_by_not_at_tail_left_alone() {
        // CLUSTER BY (x) followed by extra non-whitespace tokens isn't
        // the trailing clause shape we accept.
        let sql = "CREATE TABLE foo (id BIGINT) CLUSTER BY (id) EXTRA";
        let (stripped, cols) = extract_create_table_cluster_by(sql).unwrap();
        assert_eq!(stripped, sql);
        assert_eq!(cols, None);
    }
}
