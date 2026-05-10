//! `ALTER TABLE` SQL surface.
//!
//! The catalog already exposes per-property mutators
//! ([`basin_catalog::Catalog::set_partition_spec`],
//! [`set_rls_state`](basin_catalog::Catalog::set_rls_state),
//! [`set_bloom_filter_columns`](basin_catalog::Catalog::set_bloom_filter_columns),
//! [`set_tier_policy`](basin_catalog::Catalog::set_tier_policy),
//! [`set_schema`](basin_catalog::Catalog::set_schema)) — every form the engine
//! accepts here is a thin AST-to-mutator translation followed by a
//! `refresh_table` so the active session sees the change immediately.
//!
//! Two flavours of ALTER TABLE statement reach this module:
//!
//! 1. **Standard `sqlparser`-recognised forms** — `ADD COLUMN`,
//!    `ENABLE/DISABLE ROW LEVEL SECURITY`. ENABLE/DISABLE RLS is intercepted
//!    earlier in [`crate::rls::match_rls_ddl`] (alongside CREATE/ALTER/DROP
//!    POLICY) so we never see those here. ADD COLUMN is dispatched from
//!    `executor::execute` with a [`Statement::AlterTable`].
//!
//! 2. **Custom Basin extensions** that `sqlparser` 0.52 doesn't model —
//!    `SET cold_after = N`, `SET cold_age_column = 'col'`,
//!    `SET BLOOM FILTERS ON (col, …)`, `SET row_group_rows = N`,
//!    `RESET row_group_rows`, `CLUSTER BY (col, …)`, and
//!    `RESET CLUSTER BY`. Rather than fork the parser we pre-screen the
//!    raw SQL with a small textual matcher ([`match_basin_alter_extension`])
//!    before handing the statement to `sqlparser`. The matcher is
//!    conservative: it only triggers on `ALTER TABLE …
//!    {SET <keyword> | RESET <keyword> | CLUSTER BY (...)}` and otherwise
//!    returns `None`, falling through to the standard path.
//!
//! Tenant scoping: every catalog mutator is tenant-scoped already; the
//! engine never sees a non-tenant-qualified `Catalog::set_*` call.

use arrow_schema::{Field, Schema};
use basin_catalog::Catalog;
use basin_common::{BasinError, Result, TableName};
use sqlparser::ast::{AlterTableOperation, ColumnDef, ColumnOption, ObjectName};
use std::sync::Arc;

use crate::types::arrow_data_type;

/// Custom Basin-specific ALTER TABLE extensions that sqlparser doesn't
/// recognise. The engine pre-screens the raw SQL string for these forms
/// and routes them here before invoking `sqlparser::Parser`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BasinAlterExtension {
    /// `ALTER TABLE <t> SET cold_after = <n>`
    SetColdAfter { table: TableName, seconds: u64 },
    /// `ALTER TABLE <t> SET cold_age_column = '<col>'`
    SetColdAgeColumn { table: TableName, column: String },
    /// `ALTER TABLE <t> SET BLOOM FILTERS ON (<c1>, <c2>, ...)`
    SetBloomFilterColumns {
        table: TableName,
        columns: Vec<String>,
    },
    /// `ALTER TABLE <t> SET row_group_rows = <n>`
    SetRowGroupRows { table: TableName, rows: usize },
    /// `ALTER TABLE <t> RESET row_group_rows`. Clears the per-table
    /// override and falls back to the writer's global default.
    ResetRowGroupRows { table: TableName },
    /// `ALTER TABLE <t> CLUSTER BY (<c1>, <c2>, ...)`. Sets the cluster
    /// columns; the writer physically `lexsort`s every batch by these
    /// columns before Parquet flush.
    SetClusterColumns {
        table: TableName,
        columns: Vec<String>,
    },
    /// `ALTER TABLE <t> RESET CLUSTER BY`. Clears the cluster spec; the
    /// writer reverts to the pre-B2 unsorted byte-equivalent path.
    ResetClusterColumns { table: TableName },
}

/// Try to recognise one of the Basin-specific ALTER TABLE forms in the
/// raw SQL string. Returns `Ok(Some(...))` on a clean match,
/// `Ok(None)` if the statement isn't one of our extensions (caller falls
/// through to standard sqlparser dispatch), or `Err(...)` if the shape
/// looked like ours but the arguments were malformed.
///
/// We intentionally do this with a small hand-rolled matcher rather than
/// modifying sqlparser: the surface area is three statement shapes and
/// the matcher is kept narrow (only triggers when the statement starts
/// with `ALTER TABLE … SET <one_of_three_keywords>`), so no real query
/// can accidentally take the extension path.
pub(crate) fn match_basin_alter_extension(sql: &str) -> Result<Option<BasinAlterExtension>> {
    // Strip leading whitespace and a trailing `;`. Lowercase a copy for
    // case-insensitive keyword matching but keep the original around so
    // identifier casing is preserved.
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let lower = trimmed.to_ascii_lowercase();

    if !lower.starts_with("alter table") {
        return Ok(None);
    }

    // Tokenise on whitespace and a few punctuation chars so "events" and
    // "(id," land as separate tokens. We don't need full SQL tokenisation
    // — the keyword set is small and identifiers never contain whitespace.
    let mut after_alter_table = trimmed["alter table".len()..].trim_start();
    // Read the table identifier (bare; schema-qualified names and quoted
    // idents are out of scope for v0.1, matching the rest of the engine).
    let (raw_name, rest) = read_identifier(after_alter_table)?;
    after_alter_table = rest.trim_start();
    let table = TableName::new(&raw_name)?;

    // The next keyword is either `SET` (most extensions), `RESET` (for
    // clearing a per-table override), or `CLUSTER BY` (the bare form).
    // Anything else falls through to the standard sqlparser path.
    let lower_rest = after_alter_table.to_ascii_lowercase();
    if let Some(after_reset) = strip_keyword(after_alter_table, &lower_rest, "reset") {
        let after_reset = after_reset.trim_start();
        let after_reset_lower = after_reset.to_ascii_lowercase();
        if let Some(tail) = strip_keyword(after_reset, &after_reset_lower, "row_group_rows") {
            // Allow trailing whitespace / nothing else.
            let trimmed = tail.trim();
            if !trimmed.is_empty() {
                return Err(BasinError::InvalidSchema(format!(
                    "ALTER TABLE … RESET row_group_rows: unexpected trailing input {trimmed:?}"
                )));
            }
            return Ok(Some(BasinAlterExtension::ResetRowGroupRows { table }));
        }
        if let Some(tail) = strip_keyword(after_reset, &after_reset_lower, "cluster by") {
            let trimmed = tail.trim();
            if !trimmed.is_empty() {
                return Err(BasinError::InvalidSchema(format!(
                    "ALTER TABLE … RESET CLUSTER BY: unexpected trailing input {trimmed:?}"
                )));
            }
            return Ok(Some(BasinAlterExtension::ResetClusterColumns { table }));
        }
        // RESET <something we don't recognise> — fall through.
        return Ok(None);
    }

    // Bare `CLUSTER BY (...)` — no `SET` prefix.
    if let Some(rest) = strip_keyword(after_alter_table, &lower_rest, "cluster by") {
        let columns = parse_paren_ident_list(rest)?;
        return Ok(Some(BasinAlterExtension::SetClusterColumns {
            table,
            columns,
        }));
    }

    if !after_alter_table
        .get(..3)
        .map(|s| s.eq_ignore_ascii_case("set"))
        .unwrap_or(false)
    {
        return Ok(None);
    }
    // Confirm `set` is a whole token (not e.g. `setof`).
    let after_set = &after_alter_table[3..];
    if !after_set
        .chars()
        .next()
        .map(|c| c.is_whitespace())
        .unwrap_or(false)
    {
        return Ok(None);
    }
    let after_set = after_set.trim_start();
    let after_set_lower = after_set.to_ascii_lowercase();

    if let Some(rest) = strip_keyword(after_set, &after_set_lower, "cold_after") {
        let value = parse_eq_int(rest, "cold_after")?;
        let seconds = u64::try_from(value).map_err(|_| {
            BasinError::InvalidSchema(format!(
                "ALTER TABLE … SET cold_after must be a non-negative integer, got {value}"
            ))
        })?;
        return Ok(Some(BasinAlterExtension::SetColdAfter { table, seconds }));
    }
    if let Some(rest) = strip_keyword(after_set, &after_set_lower, "cold_age_column") {
        let column = parse_eq_string_or_ident(rest, "cold_age_column")?;
        return Ok(Some(BasinAlterExtension::SetColdAgeColumn {
            table,
            column,
        }));
    }
    if let Some(rest) = strip_keyword(after_set, &after_set_lower, "bloom filters on") {
        let columns = parse_paren_ident_list(rest)?;
        return Ok(Some(BasinAlterExtension::SetBloomFilterColumns {
            table,
            columns,
        }));
    }
    if let Some(rest) = strip_keyword(after_set, &after_set_lower, "row_group_rows") {
        let value = parse_eq_int(rest, "row_group_rows")?;
        if value <= 0 {
            return Err(BasinError::InvalidSchema(format!(
                "ALTER TABLE … SET row_group_rows must be a positive integer, got {value}"
            )));
        }
        let rows = usize::try_from(value).map_err(|_| {
            BasinError::InvalidSchema(format!(
                "ALTER TABLE … SET row_group_rows {value} doesn't fit in usize"
            ))
        })?;
        return Ok(Some(BasinAlterExtension::SetRowGroupRows { table, rows }));
    }

    Ok(None)
}

impl BasinAlterExtension {
    pub(crate) fn table(&self) -> &TableName {
        match self {
            BasinAlterExtension::SetColdAfter { table, .. }
            | BasinAlterExtension::SetColdAgeColumn { table, .. }
            | BasinAlterExtension::SetBloomFilterColumns { table, .. }
            | BasinAlterExtension::SetRowGroupRows { table, .. }
            | BasinAlterExtension::ResetRowGroupRows { table }
            | BasinAlterExtension::SetClusterColumns { table, .. }
            | BasinAlterExtension::ResetClusterColumns { table } => table,
        }
    }

    /// Apply the extension via the appropriate catalog mutator. Returns
    /// the SQL command tag the engine should send back to the client.
    pub(crate) async fn apply(
        self,
        catalog: &Arc<dyn Catalog>,
        tenant: &basin_common::TenantId,
    ) -> Result<&'static str> {
        match self {
            BasinAlterExtension::SetColdAfter { table, seconds } => {
                let meta = catalog.load_table(tenant, &table).await?;
                catalog
                    .set_tier_policy(tenant, &table, Some(seconds), meta.cold_age_column.clone())
                    .await?;
                Ok("ALTER TABLE")
            }
            BasinAlterExtension::SetColdAgeColumn { table, column } => {
                // Validate the column exists in the schema.
                let meta = catalog.load_table(tenant, &table).await?;
                if meta.schema.field_with_name(&column).is_err() {
                    return Err(BasinError::InvalidSchema(format!(
                        "ALTER TABLE {table}: column {column:?} not in table schema"
                    )));
                }
                catalog
                    .set_tier_policy(tenant, &table, meta.cold_after_seconds, Some(column))
                    .await?;
                Ok("ALTER TABLE")
            }
            BasinAlterExtension::SetBloomFilterColumns { table, columns } => {
                // Validate every column exists in the schema before we
                // persist; an unknown column would silently disable
                // the bloom filter on any future write.
                let meta = catalog.load_table(tenant, &table).await?;
                for c in &columns {
                    if meta.schema.field_with_name(c).is_err() {
                        return Err(BasinError::InvalidSchema(format!(
                            "ALTER TABLE {table}: BLOOM FILTERS column {c:?} not in table schema"
                        )));
                    }
                }
                catalog
                    .set_bloom_filter_columns(tenant, &table, columns)
                    .await?;
                Ok("ALTER TABLE")
            }
            BasinAlterExtension::SetRowGroupRows { table, rows } => {
                // Validate the table exists; the catalog setter would
                // bubble NotFound up too, but we want a consistent error
                // shape with the other extensions.
                let _ = catalog.load_table(tenant, &table).await?;
                catalog
                    .set_row_group_rows(tenant, &table, Some(rows))
                    .await?;
                Ok("ALTER TABLE")
            }
            BasinAlterExtension::ResetRowGroupRows { table } => {
                let _ = catalog.load_table(tenant, &table).await?;
                catalog.set_row_group_rows(tenant, &table, None).await?;
                Ok("ALTER TABLE")
            }
            BasinAlterExtension::SetClusterColumns { table, columns } => {
                // Validate every cluster column exists in the schema; an
                // unknown column would silently disable the lexsort on
                // every future write.
                let meta = catalog.load_table(tenant, &table).await?;
                for c in &columns {
                    if meta.schema.field_with_name(c).is_err() {
                        return Err(BasinError::InvalidSchema(format!(
                            "ALTER TABLE {table}: CLUSTER BY column {c:?} not in table schema"
                        )));
                    }
                }
                catalog.set_cluster_columns(tenant, &table, columns).await?;
                Ok("ALTER TABLE")
            }
            BasinAlterExtension::ResetClusterColumns { table } => {
                let _ = catalog.load_table(tenant, &table).await?;
                catalog
                    .set_cluster_columns(tenant, &table, Vec::new())
                    .await?;
                Ok("ALTER TABLE")
            }
        }
    }
}

/// Apply an `ALTER TABLE` operation that sqlparser DID recognise
/// (currently only `ADD [COLUMN] <col_def>`). RLS forms are intercepted
/// upstream by [`crate::rls::match_rls_ddl`].
pub(crate) async fn apply_standard_alter_table(
    catalog: &Arc<dyn Catalog>,
    tenant: &basin_common::TenantId,
    name: &ObjectName,
    operations: &[AlterTableOperation],
) -> Result<&'static str> {
    let table = single_part_object_name(name)?;
    if operations.is_empty() {
        return Err(BasinError::InvalidSchema(
            "ALTER TABLE requires at least one operation".into(),
        ));
    }
    for op in operations {
        match op {
            AlterTableOperation::AddColumn { column_def, .. } => {
                add_column(catalog, tenant, &table, column_def).await?;
            }
            // RLS forms are absorbed upstream and never reach here.
            AlterTableOperation::EnableRowLevelSecurity
            | AlterTableOperation::DisableRowLevelSecurity => {
                return Err(BasinError::internal(
                    "RLS ENABLE/DISABLE should be handled by rls::match_rls_ddl",
                ));
            }
            other => {
                return Err(BasinError::InvalidSchema(format!(
                    "unsupported ALTER TABLE op in PoC: {other}"
                )));
            }
        }
    }
    Ok("ALTER TABLE")
}

/// Append one new column to the table's Arrow schema and persist the
/// evolved schema via [`Catalog::set_schema`]. Existing data files keep
/// their original schema; the reader pads missing columns with NULLs at
/// scan time, which is exactly Arrow's projection behaviour.
async fn add_column(
    catalog: &Arc<dyn Catalog>,
    tenant: &basin_common::TenantId,
    table: &TableName,
    column_def: &ColumnDef,
) -> Result<()> {
    let meta = catalog.load_table(tenant, table).await?;
    let name = column_def.name.value.clone();
    if meta.schema.field_with_name(&name).is_ok() {
        return Err(BasinError::InvalidSchema(format!(
            "ALTER TABLE {table}: column {name:?} already exists"
        )));
    }
    let dt = arrow_data_type(&column_def.data_type)?;
    // Default to nullable; a NOT NULL added column would force every
    // existing row into a back-fill or rejection, which is out of scope
    // for v0.1 schema evolution.
    let mut nullable = true;
    for opt in &column_def.options {
        match &opt.option {
            ColumnOption::NotNull => {
                return Err(BasinError::InvalidSchema(
                    "ALTER TABLE ADD COLUMN with NOT NULL is not supported in v0.1; \
                     existing rows would have no value for the new column"
                        .into(),
                ));
            }
            ColumnOption::Null => nullable = true,
            other => {
                return Err(BasinError::InvalidSchema(format!(
                    "unsupported column option in ADD COLUMN: {other}"
                )));
            }
        }
    }
    let mut fields: Vec<Field> = meta.schema.fields().iter().map(|f| (**f).clone()).collect();
    fields.push(Field::new(name, dt, nullable));
    let new_schema = Schema::new(fields);
    catalog.set_schema(tenant, table, new_schema).await?;
    Ok(())
}

fn single_part_object_name(name: &ObjectName) -> Result<TableName> {
    if name.0.len() != 1 {
        return Err(BasinError::InvalidIdent(format!(
            "schema-qualified table not supported in PoC: {name}"
        )));
    }
    TableName::new(name.0[0].value.clone())
}

// ---- Hand-rolled mini-parser for the Basin extension forms ---------------
//
// These helpers recognise just enough of the Basin extension grammar to
// route `ALTER TABLE … SET cold_after = …` and friends. Anything more
// elaborate is the user's bug and surfaces as InvalidSchema.

fn read_identifier(s: &str) -> Result<(String, &str)> {
    let s = s.trim_start();
    if s.is_empty() {
        return Err(BasinError::InvalidIdent("expected identifier".into()));
    }
    let mut end = 0usize;
    for (i, c) in s.char_indices() {
        if c.is_ascii_alphanumeric() || c == '_' {
            end = i + c.len_utf8();
        } else {
            break;
        }
    }
    if end == 0 {
        return Err(BasinError::InvalidIdent(format!(
            "expected identifier, got {s:?}"
        )));
    }
    Ok((s[..end].to_string(), &s[end..]))
}

/// If `lower` (the lowercased view of `s`) starts with `kw` followed by a
/// word-boundary, return the slice of `s` after `kw` (preserving original
/// casing). Otherwise None. `kw` may contain spaces — they match runs of
/// whitespace in `lower`.
fn strip_keyword<'a>(s: &'a str, lower: &str, kw: &str) -> Option<&'a str> {
    // For multi-word keywords we re-tokenise on whitespace.
    let mut s_cur = s;
    let mut lower_cur = lower;
    for (i, kw_word) in kw.split_whitespace().enumerate() {
        if i > 0 {
            // Require ≥1 whitespace between words.
            if !lower_cur.starts_with(|c: char| c.is_whitespace()) {
                return None;
            }
            let n = lower_cur
                .chars()
                .take_while(|c| c.is_whitespace())
                .map(|c| c.len_utf8())
                .sum::<usize>();
            s_cur = &s_cur[n..];
            lower_cur = &lower_cur[n..];
        }
        if !lower_cur.starts_with(kw_word) {
            return None;
        }
        s_cur = &s_cur[kw_word.len()..];
        lower_cur = &lower_cur[kw_word.len()..];
    }
    // Word boundary after the last keyword: end-of-string OR a whitespace
    // OR an `=` (for `cold_after=`) OR `(` (for `bloom filters on(`).
    let boundary = lower_cur
        .chars()
        .next()
        .map(|c| c.is_whitespace() || c == '=' || c == '(')
        .unwrap_or(true);
    if !boundary {
        return None;
    }
    Some(s_cur)
}

fn parse_eq_int(rest: &str, what: &str) -> Result<i64> {
    let rest = rest.trim_start();
    let after_eq = rest.strip_prefix('=').ok_or_else(|| {
        BasinError::InvalidSchema(format!(
            "ALTER TABLE … SET {what}: expected '=', got {rest:?}"
        ))
    })?;
    let val_str = after_eq.trim();
    val_str.parse::<i64>().map_err(|e| {
        BasinError::InvalidSchema(format!(
            "ALTER TABLE … SET {what}: expected integer, got {val_str:?} ({e})"
        ))
    })
}

fn parse_eq_string_or_ident(rest: &str, what: &str) -> Result<String> {
    let rest = rest.trim_start();
    let after_eq = rest.strip_prefix('=').ok_or_else(|| {
        BasinError::InvalidSchema(format!(
            "ALTER TABLE … SET {what}: expected '=', got {rest:?}"
        ))
    })?;
    let v = after_eq.trim();
    // Accept `'name'`, `"name"`, or a bare identifier.
    if let Some(stripped) = v.strip_prefix('\'').and_then(|s| s.strip_suffix('\'')) {
        return Ok(stripped.to_string());
    }
    if let Some(stripped) = v.strip_prefix('"').and_then(|s| s.strip_suffix('"')) {
        return Ok(stripped.to_string());
    }
    let (ident, rest) = read_identifier(v)?;
    if !rest.trim().is_empty() {
        return Err(BasinError::InvalidSchema(format!(
            "ALTER TABLE … SET {what}: unexpected trailing input {:?}",
            rest.trim()
        )));
    }
    Ok(ident)
}

fn parse_paren_ident_list(rest: &str) -> Result<Vec<String>> {
    let rest = rest.trim_start();
    let after_paren = rest.strip_prefix('(').ok_or_else(|| {
        BasinError::InvalidSchema(format!(
            "ALTER TABLE … SET BLOOM FILTERS ON: expected '(', got {rest:?}"
        ))
    })?;
    let close = after_paren.rfind(')').ok_or_else(|| {
        BasinError::InvalidSchema("ALTER TABLE … SET BLOOM FILTERS ON: missing closing ')'".into())
    })?;
    let inside = &after_paren[..close];
    let mut out = Vec::new();
    for raw in inside.split(',') {
        let token = raw.trim();
        if token.is_empty() {
            continue;
        }
        let (ident, rest) = read_identifier(token)?;
        if !rest.trim().is_empty() {
            return Err(BasinError::InvalidSchema(format!(
                "ALTER TABLE … SET BLOOM FILTERS ON: bad identifier {token:?}"
            )));
        }
        out.push(ident);
    }
    if out.is_empty() {
        return Err(BasinError::InvalidSchema(
            "ALTER TABLE … SET BLOOM FILTERS ON: column list cannot be empty".into(),
        ));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn match_set_cold_after() {
        let m = match_basin_alter_extension("ALTER TABLE events SET cold_after = 7776000")
            .unwrap()
            .unwrap();
        assert_eq!(
            m,
            BasinAlterExtension::SetColdAfter {
                table: TableName::new("events").unwrap(),
                seconds: 7_776_000,
            }
        );
    }

    #[test]
    fn match_set_cold_age_column_quoted() {
        let m = match_basin_alter_extension("ALTER TABLE events SET cold_age_column = 'ts'")
            .unwrap()
            .unwrap();
        assert_eq!(
            m,
            BasinAlterExtension::SetColdAgeColumn {
                table: TableName::new("events").unwrap(),
                column: "ts".into(),
            }
        );
    }

    #[test]
    fn match_set_bloom_filters() {
        let m =
            match_basin_alter_extension("ALTER TABLE events SET BLOOM FILTERS ON (id, owner_id)")
                .unwrap()
                .unwrap();
        assert_eq!(
            m,
            BasinAlterExtension::SetBloomFilterColumns {
                table: TableName::new("events").unwrap(),
                columns: vec!["id".into(), "owner_id".into()],
            }
        );
    }

    #[test]
    fn match_non_extension_returns_none() {
        // Standard ADD COLUMN — sqlparser handles this, the matcher
        // must not claim it.
        let m =
            match_basin_alter_extension("ALTER TABLE events ADD COLUMN device_id TEXT").unwrap();
        assert!(m.is_none());
        // Non-ALTER statement.
        let m = match_basin_alter_extension("SELECT * FROM events WHERE id = 1").unwrap();
        assert!(m.is_none());
        // ALTER TABLE … ENABLE RLS — handled by sqlparser AST + rls.rs.
        let m =
            match_basin_alter_extension("ALTER TABLE events ENABLE ROW LEVEL SECURITY").unwrap();
        assert!(m.is_none());
    }

    #[test]
    fn match_handles_trailing_semicolon_and_case() {
        let m = match_basin_alter_extension("alter table EVENTS set cold_after=42;")
            .unwrap()
            .unwrap();
        match m {
            BasinAlterExtension::SetColdAfter { seconds, .. } => assert_eq!(seconds, 42),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn match_set_row_group_rows() {
        let m = match_basin_alter_extension("ALTER TABLE events SET row_group_rows = 4096")
            .unwrap()
            .unwrap();
        assert_eq!(
            m,
            BasinAlterExtension::SetRowGroupRows {
                table: TableName::new("events").unwrap(),
                rows: 4096,
            }
        );
    }

    #[test]
    fn match_reset_row_group_rows() {
        let m = match_basin_alter_extension("ALTER TABLE events RESET row_group_rows")
            .unwrap()
            .unwrap();
        assert_eq!(
            m,
            BasinAlterExtension::ResetRowGroupRows {
                table: TableName::new("events").unwrap(),
            }
        );
    }

    #[test]
    fn set_row_group_rows_rejects_non_positive() {
        let err =
            match_basin_alter_extension("ALTER TABLE events SET row_group_rows = 0").unwrap_err();
        assert!(matches!(err, BasinError::InvalidSchema(_)));
        let err =
            match_basin_alter_extension("ALTER TABLE events SET row_group_rows = -7").unwrap_err();
        assert!(matches!(err, BasinError::InvalidSchema(_)));
    }

    #[test]
    fn match_set_cluster_by() {
        let m = match_basin_alter_extension("ALTER TABLE events CLUSTER BY (id, ts)")
            .unwrap()
            .unwrap();
        assert_eq!(
            m,
            BasinAlterExtension::SetClusterColumns {
                table: TableName::new("events").unwrap(),
                columns: vec!["id".into(), "ts".into()],
            }
        );
    }

    #[test]
    fn match_reset_cluster_by() {
        let m = match_basin_alter_extension("ALTER TABLE events RESET CLUSTER BY")
            .unwrap()
            .unwrap();
        assert_eq!(
            m,
            BasinAlterExtension::ResetClusterColumns {
                table: TableName::new("events").unwrap(),
            }
        );
    }

    #[test]
    fn match_set_cluster_by_case_and_semicolon() {
        let m = match_basin_alter_extension("alter table EVENTS cluster by (id);")
            .unwrap()
            .unwrap();
        match m {
            BasinAlterExtension::SetClusterColumns { columns, .. } => {
                assert_eq!(columns, vec!["id".to_string()]);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }
}
