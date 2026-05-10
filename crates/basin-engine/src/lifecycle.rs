//! Declarative lifecycle DDL extensions: `AUTO_UPDATE`, `AUDIT TO`,
//! `SOFT DELETE`, and the `INCLUDE DELETED` SELECT opt-out.
//!
//! sqlparser 0.52's `PostgreSqlDialect` doesn't model any of these forms,
//! so the engine pre-screens the raw SQL with small string-literal /
//! comment-aware scanners (the same shape `ddl::extract_create_table_cluster_by`
//! uses) and lifts the clauses out before sqlparser sees them. The lifted
//! configuration is then stamped onto the resulting Arrow schema's field /
//! schema metadata.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::builder::{
    Int64Builder, LargeBinaryBuilder, StringBuilder, TimestampMicrosecondBuilder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use basin_catalog::DataFileRef;
use basin_common::{BasinError, ChangeOp, PartitionKey, Result, TableName};
use datafusion::logical_expr::{Filter, LogicalPlan, LogicalPlanBuilder};
use datafusion::prelude::{DataFrame, SessionContext};
use serde_json::Value as JsonValue;

use crate::TenantSession;

/// Pre-screened lifecycle config extracted from a raw `CREATE TABLE`
/// statement. `auto_update_columns` and `soft_delete_columns` are the bare
/// column names the user marked; `audit_table` is the trailing
/// `AUDIT TO <name>` target if present.
#[derive(Debug, Default, Clone)]
pub(crate) struct CreateTableLifecycle {
    pub auto_update_columns: Vec<String>,
    pub soft_delete_columns: Vec<String>,
    pub audit_table: Option<String>,
}

/// Lift `AUTO_UPDATE` / `SOFT DELETE` per-column attributes and a trailing
/// `AUDIT TO <ident>` clause out of `sql`. Returns the cleaned-up SQL
/// (suitable for sqlparser) plus the lifted config. Statements that aren't
/// `CREATE TABLE` round-trip unchanged with empty config.
///
/// Recognised forms (case-insensitive):
///
/// - `<col> <type> AUTO_UPDATE` (anywhere in the column option list)
/// - `<col> <type> SOFT DELETE`
/// - trailing `AUDIT TO <ident>` (after the column list, optional `;`)
pub(crate) fn extract_create_table_lifecycle(sql: &str) -> Result<(String, CreateTableLifecycle)> {
    let leading = sql.trim_start();
    if !leading
        .get(..6)
        .map(|s| s.eq_ignore_ascii_case("create"))
        .unwrap_or(false)
    {
        return Ok((sql.to_string(), CreateTableLifecycle::default()));
    }

    let (after_audit_strip, audit_table) = strip_trailing_audit_to(sql)?;

    let bytes = after_audit_strip.as_bytes();
    // Find the `(` that opens the column list.
    let Some(open) = find_top_level_open_paren(bytes) else {
        return Ok((after_audit_strip, CreateTableLifecycle {
            audit_table,
            ..CreateTableLifecycle::default()
        }));
    };
    let Some(close) = find_matching_close_paren(bytes, open) else {
        return Ok((after_audit_strip, CreateTableLifecycle {
            audit_table,
            ..CreateTableLifecycle::default()
        }));
    };
    let body = &after_audit_strip[open + 1..close];
    let head = &after_audit_strip[..open + 1];
    let tail = &after_audit_strip[close..];

    // Walk the column list, splitting at top-level commas (commas inside
    // parens are part of a type modifier or DEFAULT call, not separators).
    let mut auto_update_columns = Vec::new();
    let mut soft_delete_columns = Vec::new();
    let mut rebuilt = String::with_capacity(body.len());
    let segments = split_top_level_commas(body);
    for (i, seg) in segments.iter().enumerate() {
        if i > 0 {
            rebuilt.push(',');
        }
        let (cleaned, hits) = strip_column_lifecycle_attrs(seg);
        for h in hits {
            match h {
                ColumnAttr::AutoUpdate(name) => auto_update_columns.push(name),
                ColumnAttr::SoftDelete(name) => soft_delete_columns.push(name),
            }
        }
        rebuilt.push_str(&cleaned);
    }

    let mut out = String::with_capacity(after_audit_strip.len());
    out.push_str(head);
    out.push_str(&rebuilt);
    out.push_str(tail);

    Ok((
        out,
        CreateTableLifecycle {
            auto_update_columns,
            soft_delete_columns,
            audit_table,
        },
    ))
}

/// What we pulled off one column definition. Anonymous because there's at
/// most one of each per column.
enum ColumnAttr {
    AutoUpdate(String),
    SoftDelete(String),
}

/// Strip `AUTO_UPDATE` and `SOFT DELETE` markers from a single column
/// definition and return the cleaned text plus a list of attributes
/// (carrying the column name) we lifted. If neither marker is present,
/// returns the original segment unchanged.
fn strip_column_lifecycle_attrs(segment: &str) -> (String, Vec<ColumnAttr>) {
    // First non-whitespace token is the column name.
    let trimmed_start = segment.trim_start();
    let leading_ws_len = segment.len() - trimmed_start.len();
    let mut chars = trimmed_start.char_indices();
    let mut name_end = 0;
    while let Some((idx, c)) = chars.next() {
        if c.is_ascii_alphanumeric() || c == '_' {
            name_end = idx + c.len_utf8();
        } else {
            break;
        }
    }
    if name_end == 0 {
        return (segment.to_string(), Vec::new());
    }
    let col_name = trimmed_start[..name_end].to_string();
    let prefix_len = leading_ws_len + name_end;

    let mut hits = Vec::new();
    let mut cleaned = String::with_capacity(segment.len());
    cleaned.push_str(&segment[..prefix_len]);

    let rest = &segment[prefix_len..];
    let bytes = rest.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'\'' {
            cleaned.push('\'');
            i += 1;
            while i < bytes.len() {
                cleaned.push(bytes[i] as char);
                if bytes[i] == b'\'' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        cleaned.push('\'');
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
        if b == b'"' {
            cleaned.push('"');
            i += 1;
            while i < bytes.len() {
                cleaned.push(bytes[i] as char);
                if bytes[i] == b'"' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                        cleaned.push('"');
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
        // Word-boundary check before testing a keyword.
        if at_word_boundary(rest.as_bytes(), i) {
            if matches_keyword(&bytes[i..], b"AUTO_UPDATE")
                && at_word_boundary_end(rest.as_bytes(), i + 11)
            {
                hits.push(ColumnAttr::AutoUpdate(col_name.clone()));
                // Drop one preceding space if present so the cleaned
                // column-def doesn't accumulate double spaces.
                if cleaned.ends_with(' ') && !cleaned.ends_with("  ") {
                    cleaned.pop();
                }
                i += 11;
                continue;
            }
            if matches_keyword(&bytes[i..], b"SOFT") && at_word_boundary_end(rest.as_bytes(), i + 4)
            {
                // Need following whitespace + `DELETE`.
                let mut j = i + 4;
                while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                    j += 1;
                }
                if matches_keyword(&bytes[j..], b"DELETE")
                    && at_word_boundary_end(rest.as_bytes(), j + 6)
                {
                    hits.push(ColumnAttr::SoftDelete(col_name.clone()));
                    if cleaned.ends_with(' ') && !cleaned.ends_with("  ") {
                        cleaned.pop();
                    }
                    i = j + 6;
                    continue;
                }
            }
        }
        cleaned.push(bytes[i] as char);
        i += 1;
    }

    (cleaned, hits)
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

fn matches_keyword(bytes: &[u8], kw: &[u8]) -> bool {
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

/// Find the first `(` at the top level (outside string literals and comments).
fn find_top_level_open_paren(bytes: &[u8]) -> Option<usize> {
    let mut i = 0usize;
    while i < bytes.len() {
        let b = bytes[i];
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
        if b == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
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
        if b == b'(' {
            return Some(i);
        }
        i += 1;
    }
    None
}

fn find_matching_close_paren(bytes: &[u8], open: usize) -> Option<usize> {
    let mut depth = 1i32;
    let mut i = open + 1;
    while i < bytes.len() {
        let b = bytes[i];
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
        if b == b'(' {
            depth += 1;
        } else if b == b')' {
            depth -= 1;
            if depth == 0 {
                return Some(i);
            }
        }
        i += 1;
    }
    None
}

/// Split `body` at commas that aren't enclosed in parens or string literals.
fn split_top_level_commas(body: &str) -> Vec<String> {
    let bytes = body.as_bytes();
    let mut out = Vec::new();
    let mut start = 0usize;
    let mut depth = 0i32;
    let mut i = 0usize;
    while i < bytes.len() {
        let b = bytes[i];
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
        if b == b'(' {
            depth += 1;
        } else if b == b')' {
            depth -= 1;
        } else if b == b',' && depth == 0 {
            out.push(body[start..i].to_string());
            start = i + 1;
        }
        i += 1;
    }
    out.push(body[start..].to_string());
    out
}

/// Find a trailing `AUDIT TO <ident>` clause. Returns the input with the
/// clause removed plus the captured table name (`None` if absent).
fn strip_trailing_audit_to(sql: &str) -> Result<(String, Option<String>)> {
    let bytes = sql.as_bytes();
    let mut last_match: Option<(usize, String, usize)> = None; // start, name, end
    let mut i = 0usize;
    while i < bytes.len() {
        let b = bytes[i];
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
        if b == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
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
        if at_word_boundary(bytes, i)
            && matches_keyword(&bytes[i..], b"AUDIT")
            && at_word_boundary_end(bytes, i + 5)
        {
            let mut j = i + 5;
            while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                j += 1;
            }
            if matches_keyword(&bytes[j..], b"TO") && at_word_boundary_end(bytes, j + 2) {
                let mut k = j + 2;
                while k < bytes.len() && bytes[k].is_ascii_whitespace() {
                    k += 1;
                }
                let name_start = k;
                while k < bytes.len() {
                    let c = bytes[k];
                    if c.is_ascii_alphanumeric() || c == b'_' {
                        k += 1;
                    } else {
                        break;
                    }
                }
                if k > name_start {
                    let name = sql[name_start..k].to_string();
                    last_match = Some((i, name, k));
                    i = k;
                    continue;
                }
            }
        }
        i += 1;
    }

    if let Some((start, name, end)) = last_match {
        let tail = sql[end..].trim();
        if tail.is_empty() || tail == ";" {
            let mut stripped = sql[..start].trim_end().to_string();
            if sql.trim_end().ends_with(';') {
                stripped.push(';');
            }
            // Validate the audit-table identifier is a clean bare ident.
            if name.is_empty()
                || name.chars().next().map(|c| c.is_ascii_digit()).unwrap_or(true)
            {
                return Err(BasinError::InvalidSchema(format!(
                    "AUDIT TO requires a bare identifier, got {name:?}"
                )));
            }
            return Ok((stripped, Some(name)));
        }
    }
    Ok((sql.to_string(), None))
}

/// Look for a trailing `INCLUDE DELETED` clause on a SELECT statement. The
/// clause sits anywhere a trailing keyword-only chunk would land — after
/// the FROM/WHERE/GROUP/ORDER/LIMIT tail. Returns the cleaned SQL plus a
/// boolean indicating whether the clause was present.
pub(crate) fn extract_select_include_deleted(sql: &str) -> (String, bool) {
    let trimmed = sql.trim_end_matches(|c: char| c.is_whitespace() || c == ';');
    let bytes = trimmed.as_bytes();
    // Walk to find the LAST `INCLUDE DELETED` token pair at top level.
    let mut last_match: Option<usize> = None;
    let mut i = 0usize;
    while i < bytes.len() {
        let b = bytes[i];
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
        if b == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
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
        if at_word_boundary(bytes, i)
            && matches_keyword(&bytes[i..], b"INCLUDE")
            && at_word_boundary_end(bytes, i + 7)
        {
            let mut j = i + 7;
            while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                j += 1;
            }
            if matches_keyword(&bytes[j..], b"DELETED") && at_word_boundary_end(bytes, j + 7) {
                last_match = Some(i);
            }
        }
        i += 1;
    }

    if let Some(start) = last_match {
        // Only treat as opt-out when nothing-but-whitespace follows.
        let tail_after = trimmed[start..].trim_start();
        // Skip the keyword pair itself: "INCLUDE", whitespace, "DELETED".
        // Find what's after DELETED.
        let after_include = tail_after[7..].trim_start();
        let after_deleted = after_include[7..].trim_start();
        if after_deleted.is_empty() {
            let mut stripped = trimmed[..start].trim_end().to_string();
            if sql.trim_end().ends_with(';') {
                stripped.push(';');
            }
            return (stripped, true);
        }
    }
    (sql.to_string(), false)
}

/// Inject `<soft_delete_col> IS NULL` filters into `df`'s logical plan
/// for every TableScan whose table appears in `soft_cols` (a `bare_table_name
/// -> column_name` map). Mirrors `rls::inject_select_predicates`.
pub(crate) async fn inject_soft_delete_predicates(
    ctx: &SessionContext,
    df: DataFrame,
    soft_cols: &HashMap<String, String>,
) -> Result<DataFrame> {
    use datafusion::common::tree_node::{Transformed, TreeNode};

    if soft_cols.is_empty() {
        return Ok(df);
    }

    // Build a per-table `Expr` for `<col> IS NULL` once, by way of a small
    // synthetic plan probe — same trick `rls.rs` uses to avoid hand-
    // building DataFusion `Expr` trees with `Column`s that need column
    // resolution.
    let mut filter_exprs: HashMap<String, datafusion::logical_expr::Expr> = HashMap::new();
    for (table, col) in soft_cols {
        let probe_sql = format!("SELECT 1 FROM \"{table}\" WHERE \"{col}\" IS NULL");
        let probe = ctx
            .sql(&probe_sql)
            .await
            .map_err(|e| BasinError::internal(format!("soft-delete probe plan: {e}")))?;
        let plan = probe.logical_plan().clone();
        let predicate = extract_first_filter(&plan).ok_or_else(|| {
            BasinError::internal(format!(
                "soft-delete: probe plan has no Filter (table={table})"
            ))
        })?;
        filter_exprs.insert(table.clone(), predicate);
    }

    let plan = df.logical_plan().clone();
    let transformed = plan
        .transform_up(|node: LogicalPlan| {
            if let LogicalPlan::TableScan(ref ts) = node {
                let bare = ts.table_name.table().to_string();
                if let Some(expr) = filter_exprs.get(&bare) {
                    let new_plan = LogicalPlanBuilder::from(node)
                        .filter(expr.clone())
                        .map_err(|e| {
                            datafusion::error::DataFusionError::Plan(format!(
                                "soft-delete filter wrap: {e}"
                            ))
                        })?
                        .build()
                        .map_err(|e| {
                            datafusion::error::DataFusionError::Plan(format!(
                                "soft-delete filter build: {e}"
                            ))
                        })?;
                    return Ok(Transformed::yes(new_plan));
                }
            }
            Ok(Transformed::no(node))
        })
        .map_err(|e| BasinError::internal(format!("soft-delete plan rewrite: {e}")))?;

    let session_state = ctx.state();
    Ok(DataFrame::new(session_state, transformed.data))
}

fn extract_first_filter(plan: &LogicalPlan) -> Option<datafusion::logical_expr::Expr> {
    match plan {
        LogicalPlan::Filter(Filter { predicate, .. }) => Some(predicate.clone()),
        other => {
            for child in other.inputs() {
                if let Some(p) = extract_first_filter(child) {
                    return Some(p);
                }
            }
            None
        }
    }
}

/// One row destined for the audit table: a (before, after) pair plus the
/// op classification. `before` is `None` for INSERT, `after` is `None`
/// for DELETE.
#[derive(Debug, Clone)]
pub(crate) struct AuditRecord {
    pub before: Option<JsonValue>,
    pub after: Option<JsonValue>,
}

/// Build the canonical audit-table schema. The shape is fixed; auto-create
/// uses this any time `AUDIT TO <name>` references a not-yet-existing
/// table. Same schema for every audit table in the workspace so a
/// downstream tool / `basinctl` view can read all of them uniformly.
pub(crate) fn audit_schema() -> Schema {
    let mut jsonb_md = HashMap::new();
    jsonb_md.insert(
        crate::types::BASIN_TYPE_KEY.to_string(),
        crate::types::BASIN_TYPE_JSONB.to_string(),
    );
    Schema::new(vec![
        Field::new("audit_id", DataType::Int64, false),
        Field::new("op", DataType::Utf8, false),
        Field::new("before_row", DataType::LargeBinary, true).with_metadata(jsonb_md.clone()),
        Field::new("after_row", DataType::LargeBinary, true).with_metadata(jsonb_md),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("causation_user", DataType::Utf8, true),
    ])
}

/// Ensure `audit_table` exists in the tenant's catalog, creating it on
/// first reference with the canonical [`audit_schema`]. No-op if it
/// already exists.
async fn ensure_audit_table_exists(
    sess: &TenantSession,
    audit_table: &TableName,
) -> Result<()> {
    let cat = sess.engine.config().catalog.clone();
    match cat.load_table(&sess.tenant, audit_table).await {
        Ok(_) => Ok(()),
        Err(BasinError::NotFound(_)) => {
            let schema = audit_schema();
            cat.create_table(&sess.tenant, audit_table, &schema).await?;
            crate::session::refresh_table(
                &sess.engine,
                &sess.tenant,
                &sess.ctx,
                &sess.state,
                audit_table,
            )
            .await?;
            Ok(())
        }
        Err(e) => Err(e),
    }
}

/// Append one audit row per `record` to the named audit table. Auto-
/// creates the table on first reference.
///
/// Atomicity caveat: this runs after the source table's
/// `replace_data_files` / `append_data_files` commit returns OK. A
/// process death between the source commit and the audit append will
/// land the source mutation without an audit row. Single-shard
/// transactions don't yet exist in this engine (see Phase 5 task),
/// which is the natural fix.
pub(crate) async fn write_audit_rows(
    sess: &TenantSession,
    audit_table_name: &str,
    op: ChangeOp,
    records: Vec<AuditRecord>,
) -> Result<()> {
    if records.is_empty() {
        return Ok(());
    }
    let audit_table = TableName::new(audit_table_name)?;
    ensure_audit_table_exists(sess, &audit_table).await?;
    let meta = sess
        .engine
        .config()
        .catalog
        .load_table(&sess.tenant, &audit_table)
        .await?;
    let schema = meta.schema.clone();
    let n = records.len();

    let op_text = match op {
        ChangeOp::Insert => "insert",
        ChangeOp::Update => "update",
        ChangeOp::Delete => "delete",
    };
    let user = if sess.current_user == crate::ANONYMOUS_USER {
        None
    } else {
        Some(sess.current_user.as_str())
    };
    let now_micros = chrono::Utc::now().timestamp_micros();

    let mut id_b = Int64Builder::with_capacity(n);
    let mut op_b = StringBuilder::with_capacity(n, n * 8);
    let mut before_b = LargeBinaryBuilder::with_capacity(n, n * 64);
    let mut after_b = LargeBinaryBuilder::with_capacity(n, n * 64);
    let mut ts_b =
        TimestampMicrosecondBuilder::with_capacity(n).with_data_type(schema.field(4).data_type().clone());
    let mut user_b = StringBuilder::with_capacity(n, n * 8);

    // Audit-id is a per-(tenant, audit_table) sequence; the engine's
    // existing `next_event_seq` table keeps a monotonic counter under
    // exactly that key shape so we lean on it instead of carrying our
    // own.
    for rec in records {
        let id = sess.engine.next_event_seq(&sess.tenant, &audit_table) as i64;
        id_b.append_value(id);
        op_b.append_value(op_text);
        match rec.before.as_ref() {
            Some(v) => {
                let bytes = serde_json::to_vec(v).map_err(|e| {
                    BasinError::internal(format!("audit before_row serialise: {e}"))
                })?;
                before_b.append_value(&bytes);
            }
            None => before_b.append_null(),
        }
        match rec.after.as_ref() {
            Some(v) => {
                let bytes = serde_json::to_vec(v).map_err(|e| {
                    BasinError::internal(format!("audit after_row serialise: {e}"))
                })?;
                after_b.append_value(&bytes);
            }
            None => after_b.append_null(),
        }
        ts_b.append_value(now_micros);
        match user {
            Some(u) => user_b.append_value(u),
            None => user_b.append_null(),
        }
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(id_b.finish()),
        Arc::new(op_b.finish()),
        Arc::new(before_b.finish()),
        Arc::new(after_b.finish()),
        Arc::new(ts_b.finish()),
        Arc::new(user_b.finish()),
    ];
    let batch = RecordBatch::try_new(schema.clone(), columns).map_err(|e| {
        BasinError::internal(format!("audit batch build: {e}"))
    })?;

    let storage = &sess.engine.config().storage;
    let part = PartitionKey::default_key();
    let df = storage
        .write_batch(&sess.tenant, &audit_table, &part, &batch)
        .await?;
    let file_ref = DataFileRef {
        path: df.path.as_ref().to_string(),
        size_bytes: df.size_bytes,
        row_count: df.row_count,
        column_stats: df.column_stats.clone(),
    };
    // Audit-table commits use the same optimistic-retry policy as the
    // engine's own INSERT path.
    let cat = sess.engine.config().catalog.clone();
    let expected = meta.current_snapshot;
    match cat
        .append_data_files(&sess.tenant, &audit_table, expected, vec![file_ref.clone()])
        .await
    {
        Ok(_) => {}
        Err(BasinError::CommitConflict(_)) => {
            let fresh = cat.load_table(&sess.tenant, &audit_table).await?;
            cat.append_data_files(&sess.tenant, &audit_table, fresh.current_snapshot, vec![file_ref])
                .await?;
        }
        Err(e) => return Err(e),
    }
    crate::session::refresh_table(
        &sess.engine,
        &sess.tenant,
        &sess.ctx,
        &sess.state,
        &audit_table,
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_auto_update_column_attr() {
        let sql = "CREATE TABLE foo (id BIGINT, updated_at TIMESTAMPTZ AUTO_UPDATE)";
        let (out, lc) = extract_create_table_lifecycle(sql).unwrap();
        assert_eq!(lc.auto_update_columns, vec!["updated_at".to_string()]);
        assert!(lc.soft_delete_columns.is_empty());
        assert!(lc.audit_table.is_none());
        assert!(!out.contains("AUTO_UPDATE"));
    }

    #[test]
    fn extract_soft_delete_column_attr() {
        let sql = "CREATE TABLE foo (id BIGINT, deleted_at TIMESTAMPTZ SOFT DELETE)";
        let (out, lc) = extract_create_table_lifecycle(sql).unwrap();
        assert_eq!(lc.soft_delete_columns, vec!["deleted_at".to_string()]);
        assert!(!out.contains("SOFT DELETE"));
    }

    #[test]
    fn extract_audit_to_trailing_clause() {
        let sql = "CREATE TABLE foo (id BIGINT) AUDIT TO foo_audit";
        let (out, lc) = extract_create_table_lifecycle(sql).unwrap();
        assert_eq!(lc.audit_table, Some("foo_audit".into()));
        assert!(!out.contains("AUDIT"));
    }

    #[test]
    fn extract_combined_attrs() {
        let sql = "CREATE TABLE foo (\
            id BIGINT NOT NULL,\
            updated_at TIMESTAMPTZ AUTO_UPDATE,\
            deleted_at TIMESTAMPTZ SOFT DELETE,\
            payload TEXT\
        ) AUDIT TO foo_audit;";
        let (out, lc) = extract_create_table_lifecycle(sql).unwrap();
        assert_eq!(lc.auto_update_columns, vec!["updated_at".to_string()]);
        assert_eq!(lc.soft_delete_columns, vec!["deleted_at".to_string()]);
        assert_eq!(lc.audit_table, Some("foo_audit".into()));
        assert!(!out.contains("AUTO_UPDATE"));
        assert!(!out.contains("SOFT DELETE"));
        assert!(!out.contains("AUDIT TO"));
    }

    #[test]
    fn no_lifecycle_attrs_returns_input() {
        let sql = "CREATE TABLE foo (id BIGINT, name TEXT)";
        let (out, lc) = extract_create_table_lifecycle(sql).unwrap();
        assert_eq!(out, sql);
        assert!(lc.auto_update_columns.is_empty());
        assert!(lc.soft_delete_columns.is_empty());
        assert!(lc.audit_table.is_none());
    }

    #[test]
    fn audit_inside_string_literal_ignored() {
        let sql = "CREATE TABLE foo (id BIGINT, name TEXT DEFAULT 'AUDIT TO bar')";
        let (out, lc) = extract_create_table_lifecycle(sql).unwrap();
        assert_eq!(out, sql);
        assert!(lc.audit_table.is_none());
    }

    #[test]
    fn select_include_deleted_strips() {
        let (out, included) =
            extract_select_include_deleted("SELECT id FROM foo INCLUDE DELETED");
        assert!(included);
        assert!(!out.to_ascii_uppercase().contains("INCLUDE DELETED"));

        let (out, included) =
            extract_select_include_deleted("SELECT id FROM foo");
        assert!(!included);
        assert!(!out.to_ascii_uppercase().contains("INCLUDE DELETED"));
    }

    #[test]
    fn select_include_deleted_with_semicolon() {
        let (_, included) =
            extract_select_include_deleted("SELECT id FROM foo INCLUDE DELETED;");
        assert!(included);
    }
}
