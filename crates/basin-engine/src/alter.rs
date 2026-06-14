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
//! Project scoping: every catalog mutator is project-scoped already; the
//! engine never sees a non-project-qualified `Catalog::set_*` call.

use crate::pg_ast::ObjectNamePartExt;
use crate::schema_ddl::SchemaState;
use arrow_schema::{Field, Schema};
use basin_catalog::{Catalog, CheckConstraint, UniqueConstraint};
use basin_common::{BasinError, Result, TableName};
use sqlparser::ast::{
    AlterColumnOperation, AlterTableOperation, ColumnDef, ColumnOption, ObjectName, TableConstraint,
};
use std::sync::{Arc, RwLock};

use crate::types::{
    arrow_data_type, basin_type_marker, is_tsquery_sql, is_tsvector_sql, BASIN_COLUMN_DEFAULT,
    BASIN_TYPE_JSONB, BASIN_TYPE_KEY, BASIN_TYPE_TSQUERY, BASIN_TYPE_TSVECTOR, BASIN_TYPE_UUID,
};

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
    /// `ALTER TABLE <t> SET FILE_FORMAT = 'vortex' | 'parquet'` (#161).
    /// Switches the table's on-disk data-file format. Vortex is opt-in;
    /// `Parquet` stays the catalog default. In-place conversion of an
    /// already-populated table is deferred — the `apply` step rejects a
    /// non-empty target so the format only ever changes on a fresh table.
    SetFileFormat {
        table: TableName,
        format: basin_catalog::TableFileFormat,
    },
    /// `ALTER TABLE <t> VALIDATE CONSTRAINT <name>`. sqlparser 0.52
    /// doesn't model this PG-specific form; we recognise it textually
    /// and accept as a no-op (Basin doesn't have deferred / NOT VALID
    /// constraints today — all constraints are validated on every
    /// write — so VALIDATE is a no-op on a validated constraint and
    /// errors on an unknown one).
    ValidateConstraint { table: TableName, name: String },
    /// `ALTER TABLE <t> ATTACH PARTITION <p> [FOR VALUES …]`. The
    /// PG-style declarative-partition syntax — sqlparser 0.52 only
    /// parses ATTACH for the ClickHouse dialect. Basin treats this
    /// as a no-op accept: declarative partitions are computed from
    /// the PARTITION BY column at write time and the catalog
    /// doesn't need a per-partition row to route them.
    AttachPartition { table: TableName },
    /// `ALTER TABLE <t> DETACH PARTITION <p>`. Symmetric no-op
    /// accept.
    DetachPartition { table: TableName },
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
    // Read the table identifier. Accepts bare names (`t`) and
    // schema-qualified names (`myschema.t`): read the first identifier
    // part, and if followed by `.` consume the second part too (stripping
    // the schema prefix so the rest of the matcher sees only the bare name).
    let (first_part, rest_after_first) = read_identifier(after_alter_table)?;
    let (raw_name, rest) = if rest_after_first.starts_with('.') {
        // schema.table — consume the second part, discard the schema.
        let after_dot = &rest_after_first[1..];
        let (table_part, tail) = read_identifier(after_dot)?;
        (table_part, tail)
    } else {
        (first_part, rest_after_first)
    };
    after_alter_table = rest.trim_start();
    let table = TableName::new(&raw_name)?;

    // The next keyword is either `SET` (most extensions), `RESET` (for
    // clearing a per-table override), `CLUSTER BY` (the bare form), or
    // `VALIDATE CONSTRAINT` (PG-specific, sqlparser 0.52 doesn't parse).
    // Anything else falls through to the standard sqlparser path.
    let lower_rest = after_alter_table.to_ascii_lowercase();

    if let Some(rest) = strip_keyword(after_alter_table, &lower_rest, "validate constraint") {
        let rest = rest.trim_start();
        let (name, tail) = read_identifier(rest)?;
        if !tail.trim().is_empty() {
            return Err(BasinError::InvalidSchema(format!(
                "ALTER TABLE … VALIDATE CONSTRAINT: unexpected trailing input {:?}",
                tail.trim()
            )));
        }
        return Ok(Some(BasinAlterExtension::ValidateConstraint {
            table,
            name,
        }));
    }

    // PG-style declarative partition attach / detach. sqlparser only
    // models these for ClickHouse; we recognise the shape textually
    // and accept as no-op for PostgreSqlDialect users.
    if strip_keyword(after_alter_table, &lower_rest, "attach partition").is_some() {
        // The `FOR VALUES IN (...)` / `FOR VALUES FROM (...) TO (...)`
        // tail is consumed verbatim — we don't need its contents
        // since the operation is a no-op accept.
        return Ok(Some(BasinAlterExtension::AttachPartition { table }));
    }
    if strip_keyword(after_alter_table, &lower_rest, "detach partition").is_some() {
        return Ok(Some(BasinAlterExtension::DetachPartition { table }));
    }

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
    if let Some(rest) = strip_keyword(after_set, &after_set_lower, "file_format") {
        let format = parse_eq_file_format(rest)?;
        return Ok(Some(BasinAlterExtension::SetFileFormat { table, format }));
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
            | BasinAlterExtension::ResetClusterColumns { table }
            | BasinAlterExtension::SetFileFormat { table, .. }
            | BasinAlterExtension::ValidateConstraint { table, .. }
            | BasinAlterExtension::AttachPartition { table }
            | BasinAlterExtension::DetachPartition { table } => table,
        }
    }

    /// Apply the extension via the appropriate catalog mutator. Returns
    /// the SQL command tag the engine should send back to the client.
    pub(crate) async fn apply(
        self,
        catalog: &Arc<dyn Catalog>,
        project: &basin_common::ProjectId,
    ) -> Result<&'static str> {
        match self {
            BasinAlterExtension::SetColdAfter { table, seconds } => {
                let meta = catalog.load_table(project, &table).await?;
                catalog
                    .set_tier_policy(project, &table, Some(seconds), meta.cold_age_column.clone())
                    .await?;
                Ok("ALTER TABLE")
            }
            BasinAlterExtension::SetColdAgeColumn { table, column } => {
                // Validate the column exists in the schema.
                let meta = catalog.load_table(project, &table).await?;
                if meta.schema.field_with_name(&column).is_err() {
                    return Err(BasinError::InvalidSchema(format!(
                        "ALTER TABLE {table}: column {column:?} not in table schema"
                    )));
                }
                catalog
                    .set_tier_policy(project, &table, meta.cold_after_seconds, Some(column))
                    .await?;
                Ok("ALTER TABLE")
            }
            BasinAlterExtension::SetBloomFilterColumns { table, columns } => {
                // Validate every column exists in the schema before we
                // persist; an unknown column would silently disable
                // the bloom filter on any future write.
                let meta = catalog.load_table(project, &table).await?;
                for c in &columns {
                    if meta.schema.field_with_name(c).is_err() {
                        return Err(BasinError::InvalidSchema(format!(
                            "ALTER TABLE {table}: BLOOM FILTERS column {c:?} not in table schema"
                        )));
                    }
                }
                catalog
                    .set_bloom_filter_columns(project, &table, columns)
                    .await?;
                Ok("ALTER TABLE")
            }
            BasinAlterExtension::SetRowGroupRows { table, rows } => {
                // Validate the table exists; the catalog setter would
                // bubble NotFound up too, but we want a consistent error
                // shape with the other extensions.
                let _ = catalog.load_table(project, &table).await?;
                catalog
                    .set_row_group_rows(project, &table, Some(rows))
                    .await?;
                Ok("ALTER TABLE")
            }
            BasinAlterExtension::ResetRowGroupRows { table } => {
                let _ = catalog.load_table(project, &table).await?;
                catalog.set_row_group_rows(project, &table, None).await?;
                Ok("ALTER TABLE")
            }
            BasinAlterExtension::SetClusterColumns { table, columns } => {
                // Validate every cluster column exists in the schema; an
                // unknown column would silently disable the lexsort on
                // every future write.
                let meta = catalog.load_table(project, &table).await?;
                for c in &columns {
                    if meta.schema.field_with_name(c).is_err() {
                        return Err(BasinError::InvalidSchema(format!(
                            "ALTER TABLE {table}: CLUSTER BY column {c:?} not in table schema"
                        )));
                    }
                }
                catalog
                    .set_cluster_columns(project, &table, columns)
                    .await?;
                Ok("ALTER TABLE")
            }
            BasinAlterExtension::ResetClusterColumns { table } => {
                let _ = catalog.load_table(project, &table).await?;
                catalog
                    .set_cluster_columns(project, &table, Vec::new())
                    .await?;
                Ok("ALTER TABLE")
            }
            BasinAlterExtension::SetFileFormat { table, format } => {
                // In-place format change rewrites every live data file in
                // the new encoding (Parquet ⇄ Vortex). That conversion is
                // deferred (#161) — for now we only allow the switch on a
                // table with no live rows so existing files are never
                // stranded in the old format. Emptiness is read straight
                // from the catalog's live file set (the same
                // `live_data_files()` view the read path uses), so no
                // storage round-trip is needed.
                let meta = catalog.load_table(project, &table).await?;
                let live_rows: u64 = meta.live_data_files().iter().map(|f| f.row_count).sum();
                if live_rows != 0 {
                    return Err(BasinError::InvalidSchema(format!(
                        "ALTER TABLE {table}: SET FILE_FORMAT requires an empty table — \
                         in-place data-file format conversion on a populated table is not \
                         supported yet ({live_rows} live row(s)); set the format at CREATE \
                         TABLE time via WITH (basin.file_format = '…') instead"
                    )));
                }
                catalog.set_file_format(project, &table, format).await?;
                Ok("ALTER TABLE")
            }
            BasinAlterExtension::ValidateConstraint { table, name } => {
                // Look up the constraint by name. Basin doesn't have
                // deferred / NOT VALID constraints — every constraint
                // is enforced on every write — so a known constraint
                // is a no-op accept and an unknown one is an error.
                let meta = catalog.load_table(project, &table).await?;
                let exists = meta
                    .check_constraints
                    .iter()
                    .any(|c| c.name.eq_ignore_ascii_case(&name))
                    || meta
                        .foreign_keys
                        .iter()
                        .any(|f| f.name.eq_ignore_ascii_case(&name))
                    || meta
                        .unique_constraints
                        .iter()
                        .any(|u| u.name.eq_ignore_ascii_case(&name))
                    || format!("{table}_pkey").eq_ignore_ascii_case(&name);
                if !exists {
                    return Err(BasinError::InvalidSchema(format!(
                        "ALTER TABLE {table}: constraint {name:?} does not exist"
                    )));
                }
                Ok("ALTER TABLE")
            }
            BasinAlterExtension::AttachPartition { table }
            | BasinAlterExtension::DetachPartition { table } => {
                // Validate the parent table exists; declarative
                // partition routing is computed from the PARTITION BY
                // column at write time, so the attach / detach is a
                // no-op accept beyond the existence check.
                let _ = catalog.load_table(project, &table).await?;
                Ok("ALTER TABLE")
            }
        }
    }
}

/// Apply an `ALTER TABLE` operation that sqlparser DID recognise.
///
/// Supported forms (Phase 5.11 / task #29):
///   * `ADD [COLUMN] <col_def>` (additive schema evolution)
///   * `DROP [COLUMN] <col>` (schema-only — existing parquet files keep
///     their original schema; reads project on the new shape)
///   * `RENAME [COLUMN] <old> TO <new>` (schema metadata)
///   * `RENAME TO <new_table>` (catalog key change via `rename_table`)
///   * `ALTER [COLUMN] <c> [SET DATA] TYPE <dt>` (Arrow type metadata
///     swap — existing data is best-effort coerced by the reader)
///   * `ADD CONSTRAINT <n> CHECK (<expr>)` (append to
///     `check_constraints`)
///   * `ADD CONSTRAINT <n> UNIQUE (<cols>)` (validate existing rows via a
///     distinct scan, then append to `unique_constraints`; subsequent
///     INSERT / UPDATE enforcement rides the existing
///     `constraints::enforce_unique_on_insert` machinery — the Django
///     `unique_together` migration shape)
///   * `DROP CONSTRAINT <n>` (remove from `check_constraints`)
///   * `ATTACH PARTITION <p> FOR VALUES …` / `DETACH PARTITION <p>`
///     (no-op accept — declarative partitions are computed from the
///     PARTITION BY column at write time so the attach/detach
///     statement carries no state change for Basin)
///
/// RLS ENABLE/DISABLE is absorbed upstream by [`crate::rls::match_rls_ddl`]
/// and never reaches this dispatch.
pub(crate) async fn apply_standard_alter_table(
    catalog: &Arc<dyn Catalog>,
    storage: &basin_storage::Storage,
    project: &basin_common::ProjectId,
    name: &ObjectName,
    operations: &[AlterTableOperation],
    schema_state: &Arc<RwLock<SchemaState>>,
) -> Result<&'static str> {
    let table = single_part_object_name(name, schema_state)?;
    if operations.is_empty() {
        return Err(BasinError::InvalidSchema(
            "ALTER TABLE requires at least one operation".into(),
        ));
    }
    for op in operations {
        match op {
            AlterTableOperation::AddColumn { column_def, .. } => {
                add_column(catalog, project, &table, column_def).await?;
            }
            AlterTableOperation::DropColumn {
                column_names,
                if_exists,
                ..
            } => {
                for column_name in column_names {
                    drop_column(catalog, project, &table, &column_name.value, *if_exists).await?;
                }
            }
            AlterTableOperation::RenameColumn {
                old_column_name,
                new_column_name,
            } => {
                rename_column(
                    catalog,
                    project,
                    &table,
                    &old_column_name.value,
                    &new_column_name.value,
                )
                .await?;
            }
            AlterTableOperation::RenameTable { table_name } => {
                let new_name = match table_name {
                    sqlparser::ast::RenameTableNameKind::As(n)
                    | sqlparser::ast::RenameTableNameKind::To(n) => n,
                };
                let new_table = single_part_object_name(new_name, schema_state)?;
                catalog.rename_table(project, &table, &new_table).await?;
            }
            AlterTableOperation::AlterColumn { column_name, op } => match op {
                AlterColumnOperation::SetDataType { data_type, .. } => {
                    alter_column_type(catalog, project, &table, &column_name.value, data_type)
                        .await?;
                }
                AlterColumnOperation::DropDefault
                | AlterColumnOperation::SetDefault { .. }
                | AlterColumnOperation::SetNotNull
                | AlterColumnOperation::DropNotNull => {
                    // Accept as a metadata-only no-op so PG clients that
                    // emit these as part of a multi-step migration aren't
                    // blocked. v0.1 doesn't enforce per-column DEFAULT /
                    // NOT NULL changes on existing data.
                }
                AlterColumnOperation::AddGenerated { .. } => {
                    return Err(BasinError::InvalidSchema(
                        "ALTER COLUMN ADD GENERATED is not supported in v0.1; declare \
                         the column with GENERATED on the original CREATE TABLE"
                            .into(),
                    ));
                }
            },
            AlterTableOperation::AddConstraint { constraint, .. } => {
                add_constraint(catalog, storage, project, &table, constraint).await?;
            }
            AlterTableOperation::DropConstraint {
                name, if_exists, ..
            } => {
                drop_constraint(catalog, project, &table, &name.value, *if_exists).await?;
            }
            AlterTableOperation::AttachPartition { .. }
            | AlterTableOperation::DetachPartition { .. } => {
                // Declarative partitions are computed from the PARTITION
                // BY column at write time; ATTACH / DETACH PARTITION are
                // accepted syntactically so DDL emitted by PG-shaped
                // migration tools doesn't blow up. The partition table
                // already exists as a regular table in the catalog.
                let _ = catalog.load_table(project, &table).await?;
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

/// Drop one column from the table's Arrow schema. Existing parquet files
/// keep their original schema; the reader projects on the new shape, so
/// the dropped column simply stops appearing in result sets. The column
/// is also removed from the catalog's pk_columns / unique_constraints /
/// foreign_keys / check_constraints scopes when it appears there — v0.1
/// rejects rather than silently breaking a constraint that depended on
/// the column, matching the spirit of PG's RESTRICT default (we don't
/// implement CASCADE).
async fn drop_column(
    catalog: &Arc<dyn Catalog>,
    project: &basin_common::ProjectId,
    table: &TableName,
    name: &str,
    if_exists: bool,
) -> Result<()> {
    let meta = catalog.load_table(project, table).await?;
    let pos = meta
        .schema
        .fields()
        .iter()
        .position(|f| f.name().eq_ignore_ascii_case(name));
    let pos = match pos {
        Some(p) => p,
        None if if_exists => return Ok(()),
        None => {
            return Err(BasinError::InvalidSchema(format!(
                "ALTER TABLE {table}: column {name:?} does not exist"
            )));
        }
    };
    // Refuse to drop a column that participates in any catalog constraint.
    let lc = name.to_ascii_lowercase();
    if meta.pk_columns.iter().any(|c| c.eq_ignore_ascii_case(name)) {
        return Err(BasinError::InvalidSchema(format!(
            "ALTER TABLE {table}: cannot drop column {name:?} — it is part of the PRIMARY KEY"
        )));
    }
    for u in &meta.unique_constraints {
        if u.columns.iter().any(|c| c.eq_ignore_ascii_case(name)) {
            return Err(BasinError::InvalidSchema(format!(
                "ALTER TABLE {table}: cannot drop column {name:?} — it is part of UNIQUE \
                 constraint {:?}",
                u.name
            )));
        }
    }
    for fk in &meta.foreign_keys {
        if fk.columns.iter().any(|c| c.eq_ignore_ascii_case(name)) {
            return Err(BasinError::InvalidSchema(format!(
                "ALTER TABLE {table}: cannot drop column {name:?} — it is part of FOREIGN KEY \
                 constraint {:?}",
                fk.name
            )));
        }
    }
    // CHECK constraints reference columns by text; drop any check whose
    // predicate textually references the column. This is best-effort —
    // we don't reparse — but it matches PG's RESTRICT behaviour for the
    // common single-column-check case.
    let mut new_checks: Vec<CheckConstraint> = Vec::with_capacity(meta.check_constraints.len());
    for c in &meta.check_constraints {
        if predicate_references_column(&c.predicate, &lc) {
            // Drop the check so subsequent inserts don't trip an
            // un-resolvable reference. v0.1 doesn't support CASCADE
            // syntax explicitly; this is the practical equivalent.
            continue;
        }
        new_checks.push(c.clone());
    }
    let fields: Vec<Field> = meta
        .schema
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(i, f)| if i == pos { None } else { Some((**f).clone()) })
        .collect();
    let new_schema = Schema::new_with_metadata(fields, meta.schema.metadata().clone());
    catalog.set_schema(project, table, new_schema).await?;
    if new_checks.len() != meta.check_constraints.len() {
        catalog
            .set_table_constraints(
                project,
                table,
                meta.pk_columns.clone(),
                new_checks,
                meta.foreign_keys.clone(),
            )
            .await?;
    }
    Ok(())
}

/// Best-effort textual reference check for CHECK predicate strings.
/// Looks for `name` as a whole word (ASCII alphanumeric / underscore
/// boundary). The predicate text is the SQL we already stored on
/// `CHECK (...)`; we don't reparse it here.
fn predicate_references_column(predicate: &str, name_lc: &str) -> bool {
    let lower = predicate.to_ascii_lowercase();
    let bytes = lower.as_bytes();
    let needle = name_lc.as_bytes();
    let n = needle.len();
    if n == 0 || bytes.len() < n {
        return false;
    }
    let mut i = 0usize;
    while i + n <= bytes.len() {
        if bytes[i..i + n] == *needle {
            let before_ok = i == 0 || !is_ident_char(bytes[i - 1]);
            let after_ok = i + n == bytes.len() || !is_ident_char(bytes[i + n]);
            if before_ok && after_ok {
                return true;
            }
        }
        i += 1;
    }
    false
}

fn is_ident_char(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// Rename a column in the table's Arrow schema. The rename is metadata
/// only — existing parquet files keep their original column name and
/// the reader maps the old physical name to the new logical name at
/// scan time.
async fn rename_column(
    catalog: &Arc<dyn Catalog>,
    project: &basin_common::ProjectId,
    table: &TableName,
    old: &str,
    new: &str,
) -> Result<()> {
    let meta = catalog.load_table(project, table).await?;
    if meta.schema.field_with_name(new).is_ok() {
        return Err(BasinError::InvalidSchema(format!(
            "ALTER TABLE {table}: column {new:?} already exists"
        )));
    }
    let pos = meta
        .schema
        .fields()
        .iter()
        .position(|f| f.name().eq_ignore_ascii_case(old))
        .ok_or_else(|| {
            BasinError::InvalidSchema(format!(
                "ALTER TABLE {table}: column {old:?} does not exist"
            ))
        })?;
    let mut fields: Vec<Field> = meta.schema.fields().iter().map(|f| (**f).clone()).collect();
    let old_field = &fields[pos];
    let renamed = Field::new(new, old_field.data_type().clone(), old_field.is_nullable())
        .with_metadata(old_field.metadata().clone());
    fields[pos] = renamed;
    let new_schema = Schema::new_with_metadata(fields, meta.schema.metadata().clone());
    catalog.set_schema(project, table, new_schema).await?;
    Ok(())
}

/// Replace one column's Arrow `DataType`. v0.1 treats this as a
/// metadata-only swap — existing parquet rows are still on disk in the
/// old type and the reader best-effort coerces at scan time (Arrow's
/// cast helper handles the common int → bigint widening). Anything
/// the cast helper can't represent surfaces at SELECT time as a normal
/// type-mismatch error.
async fn alter_column_type(
    catalog: &Arc<dyn Catalog>,
    project: &basin_common::ProjectId,
    table: &TableName,
    column: &str,
    new_dt: &sqlparser::ast::DataType,
) -> Result<()> {
    let meta = catalog.load_table(project, table).await?;
    let pos = meta
        .schema
        .fields()
        .iter()
        .position(|f| f.name().eq_ignore_ascii_case(column))
        .ok_or_else(|| {
            BasinError::InvalidSchema(format!(
                "ALTER TABLE {table}: column {column:?} does not exist"
            ))
        })?;
    let dt = arrow_data_type(new_dt)?;
    let mut fields: Vec<Field> = meta.schema.fields().iter().map(|f| (**f).clone()).collect();
    let old = &fields[pos];
    fields[pos] =
        Field::new(old.name().clone(), dt, old.is_nullable()).with_metadata(old.metadata().clone());
    let new_schema = Schema::new_with_metadata(fields, meta.schema.metadata().clone());
    catalog.set_schema(project, table, new_schema).await?;
    Ok(())
}

/// Append one CHECK or UNIQUE constraint to the table.
///
/// CHECK additions are validated by the engine on subsequent writes only
/// (matching PG's `NOT VALID` semantics, just without the explicit syntax
/// for that flavour).
///
/// UNIQUE additions (`ADD CONSTRAINT <n> UNIQUE (cols)` — the shape Django
/// emits for `unique_together` / `Meta.constraints`) first validate that the
/// EXISTING rows satisfy the constraint via a projection-pushdown distinct
/// scan (`constraints::verify_unique_over_existing`, same cost shape as one
/// INSERT-side enforcement pass), then register the `UniqueConstraint` in the
/// catalog; from that point INSERT / UPDATE enforcement rides the existing
/// `enforce_unique_on_insert` machinery unchanged.
///
/// PRIMARY KEY / FOREIGN KEY additions mid-life remain out of scope for
/// v0.1 — their backfill validation (NOT NULL rewrite / referenced-row
/// existence) is still deferred.
async fn add_constraint(
    catalog: &Arc<dyn Catalog>,
    storage: &basin_storage::Storage,
    project: &basin_common::ProjectId,
    table: &TableName,
    tc: &TableConstraint,
) -> Result<()> {
    let meta = catalog.load_table(project, table).await?;
    match tc {
        TableConstraint::Check(sqlparser::ast::CheckConstraint { name, expr, .. }) => {
            let cname = match name {
                Some(n) => n.value.clone(),
                None => format!("{table}_check_{}", meta.check_constraints.len() + 1),
            };
            // Reject a duplicate name so the catalog row is unambiguous.
            if meta
                .check_constraints
                .iter()
                .any(|c| c.name.eq_ignore_ascii_case(&cname))
            {
                return Err(BasinError::InvalidSchema(format!(
                    "ALTER TABLE {table}: CHECK constraint {cname:?} already exists"
                )));
            }
            let mut checks = meta.check_constraints.clone();
            checks.push(CheckConstraint {
                name: cname,
                predicate: expr.to_string(),
            });
            catalog
                .set_table_constraints(
                    project,
                    table,
                    meta.pk_columns.clone(),
                    checks,
                    meta.foreign_keys.clone(),
                )
                .await?;
        }
        TableConstraint::Unique(sqlparser::ast::UniqueConstraint { name, columns, .. }) => {
            if columns.is_empty() {
                return Err(BasinError::InvalidSchema(
                    "ALTER TABLE ADD CONSTRAINT … UNIQUE: column list cannot be empty".into(),
                ));
            }
            let cols: Vec<String> = columns
                .iter()
                .map(crate::pg_ast::index_column_name)
                .collect();
            // Mirror the CREATE TABLE path's guards (`ddl::schema_from_columns`):
            // every column must exist in the schema, and no column may be
            // listed twice.
            let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
            for c in &cols {
                if meta.schema.field_with_name(c).is_err() {
                    return Err(BasinError::InvalidSchema(format!(
                        "ALTER TABLE {table}: UNIQUE column {c:?} not in table schema"
                    )));
                }
                if !seen.insert(c.to_ascii_lowercase()) {
                    return Err(BasinError::InvalidSchema(format!(
                        "ALTER TABLE {table}: UNIQUE column {c:?} listed twice"
                    )));
                }
            }
            // Same name derivation as CREATE TABLE's table-level UNIQUE:
            // user-supplied `CONSTRAINT <name>` wins, else
            // `<table>_<col1>_<col2>_key` (PG convention).
            let cname = match name {
                Some(n) => n.value.clone(),
                None => format!("{table}_{}_key", cols.join("_")),
            };
            if meta
                .unique_constraints
                .iter()
                .any(|u| u.name.eq_ignore_ascii_case(&cname))
            {
                return Err(BasinError::InvalidSchema(format!(
                    "ALTER TABLE {table}: UNIQUE constraint {cname:?} already exists"
                )));
            }
            // Backfill validation: the rows already in the table must satisfy
            // the constraint, else PG rejects the ALTER with a 23505 — and so
            // do we (`could not create unique constraint … is duplicated`).
            crate::constraints::verify_unique_over_existing(
                storage,
                project,
                table,
                &cname,
                &cols,
                meta.schema.as_ref(),
            )
            .await?;
            let mut uniques = meta.unique_constraints.clone();
            uniques.push(UniqueConstraint {
                name: cname,
                columns: cols,
            });
            catalog
                .set_unique_constraints(project, table, uniques)
                .await?;
        }
        TableConstraint::PrimaryKey(_) | TableConstraint::ForeignKey(_) => {
            return Err(BasinError::InvalidSchema(
                "ALTER TABLE ADD CONSTRAINT: only CHECK and UNIQUE are supported in v0.1 \
                 (PRIMARY KEY / FOREIGN KEY additions after table creation are deferred)"
                    .into(),
            ));
        }
        TableConstraint::Index { .. } | TableConstraint::FulltextOrSpatial { .. } => {
            return Err(BasinError::InvalidSchema(
                "ALTER TABLE ADD INDEX / FULLTEXT / SPATIAL: unsupported in v0.1".into(),
            ));
        }
    }
    Ok(())
}

/// Remove a CHECK constraint by name. Other constraint kinds (UNIQUE
/// / PRIMARY KEY / FOREIGN KEY) match by name too — drop is structural
/// catalog cleanup, not the backfill-flavoured ADD path — so we
/// support drop for all of them.
async fn drop_constraint(
    catalog: &Arc<dyn Catalog>,
    project: &basin_common::ProjectId,
    table: &TableName,
    name: &str,
    if_exists: bool,
) -> Result<()> {
    let meta = catalog.load_table(project, table).await?;
    let mut checks = meta.check_constraints.clone();
    let mut foreign_keys = meta.foreign_keys.clone();
    let mut uniques = meta.unique_constraints.clone();
    let n_before = checks.len()
        + foreign_keys.len()
        + uniques.len()
        + usize::from(!meta.pk_columns.is_empty());
    checks.retain(|c| !c.name.eq_ignore_ascii_case(name));
    foreign_keys.retain(|f| !f.name.eq_ignore_ascii_case(name));
    uniques.retain(|u| !u.name.eq_ignore_ascii_case(name));
    let pk_columns = if meta.pk_columns.is_empty() {
        Vec::new()
    } else {
        // PK constraint name is conventionally `<table>_pkey`; drop the
        // whole PK if the name matches.
        let pk_name = format!("{table}_pkey");
        if pk_name.eq_ignore_ascii_case(name) {
            Vec::new()
        } else {
            meta.pk_columns.clone()
        }
    };
    let n_after =
        checks.len() + foreign_keys.len() + uniques.len() + usize::from(!pk_columns.is_empty());
    if n_before == n_after {
        if if_exists {
            return Ok(());
        }
        return Err(BasinError::InvalidSchema(format!(
            "ALTER TABLE {table}: constraint {name:?} does not exist"
        )));
    }
    catalog
        .set_table_constraints(project, table, pk_columns, checks, foreign_keys)
        .await?;
    if uniques.len() != meta.unique_constraints.len() {
        catalog
            .set_unique_constraints(project, table, uniques)
            .await?;
    }
    Ok(())
}

/// Append one new column to the table's Arrow schema and persist the
/// evolved schema via [`Catalog::set_schema`]. Existing data files keep
/// their original schema; the reader pads missing columns with NULLs at
/// scan time, which is exactly Arrow's projection behaviour.
async fn add_column(
    catalog: &Arc<dyn Catalog>,
    project: &basin_common::ProjectId,
    table: &TableName,
    column_def: &ColumnDef,
) -> Result<()> {
    let meta = catalog.load_table(project, table).await?;
    let name = column_def.name.value.clone();
    if meta.schema.field_with_name(&name).is_ok() {
        return Err(BasinError::InvalidSchema(format!(
            "ALTER TABLE {table}: column {name:?} already exists"
        )));
    }
    let dt = arrow_data_type(&column_def.data_type)?;
    let mut nullable = true;
    let mut saw_not_null = false;
    let mut default_text: Option<String> = None;
    for opt in &column_def.options {
        match &opt.option {
            ColumnOption::NotNull => {
                saw_not_null = true;
            }
            ColumnOption::Null => nullable = true,
            // DEFAULT <expr>: store the expression text as column metadata so
            // the INSERT path (`apply_column_defaults`) stamps it on rows that
            // don't target the new column — same scheme CREATE TABLE uses
            // (`ddl::schema_from_columns`). Existing rows read NULL via the
            // scan-time pad (no eager back-fill in v0.1 schema evolution).
            ColumnOption::Default(expr) => {
                default_text = Some(expr.to_string());
            }
            other => {
                return Err(BasinError::InvalidSchema(format!(
                    "unsupported column option in ADD COLUMN: {other}"
                )));
            }
        }
    }
    // `ADD COLUMN … NOT NULL` is admitted ONLY when a DEFAULT is also present —
    // PostgreSQL's `ADD COLUMN x T DEFAULT v NOT NULL` is the canonical ORM
    // migration shape (Django/Rails add not-null-with-default columns
    // constantly). The default supplies the value: new rows are stamped by
    // `apply_column_defaults`, and a freshly-added column on an empty/new table
    // (the common migration case) has no pre-existing rows to violate the
    // constraint. A bare `ADD COLUMN x T NOT NULL` (no default) is still
    // rejected — existing rows genuinely have no value. (v0.1 limitation: rows
    // in data files written before this ALTER read NULL via the scan-time pad
    // rather than the default; eager back-fill is future work.)
    if saw_not_null {
        if default_text.is_some() {
            nullable = false;
        } else {
            return Err(BasinError::InvalidSchema(
                "ALTER TABLE ADD COLUMN with NOT NULL requires a DEFAULT in v0.1; \
                 without one, existing rows would have no value for the new column"
                    .into(),
            ));
        }
    }
    // Carry BASIN_TYPE metadata so that the INSERT coercion path, pgwire
    // encoder, and info_schema all recognise the logical type after a
    // schema round-trip via ALTER TABLE ADD COLUMN.  This mirrors the
    // identical metadata-stamping in `ddl::schema_from_columns`.
    let mut md: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    if is_jsonb_sql_add_col(&column_def.data_type) {
        md.insert(BASIN_TYPE_KEY.to_string(), BASIN_TYPE_JSONB.to_string());
    } else if is_uuid_sql_add_col(&column_def.data_type) {
        md.insert(BASIN_TYPE_KEY.to_string(), BASIN_TYPE_UUID.to_string());
    } else if let Some(marker) = basin_type_marker(&column_def.data_type) {
        // Handles: INET, CIDR, MACADDR, MACADDR8, MONEY, BIT(n), VARBIT(n),
        // CITEXT, XML, and the range types (INT4RANGE, INT8RANGE, NUMRANGE,
        // DATERANGE, TSRANGE, TSTZRANGE) when spelled as Custom identifiers.
        md.insert(BASIN_TYPE_KEY.to_string(), marker);
    } else if is_tsvector_sql(&column_def.data_type) {
        md.insert(BASIN_TYPE_KEY.to_string(), BASIN_TYPE_TSVECTOR.to_string());
    } else if is_tsquery_sql(&column_def.data_type) {
        md.insert(BASIN_TYPE_KEY.to_string(), BASIN_TYPE_TSQUERY.to_string());
    }
    if let Some(default_expr) = default_text.as_ref() {
        md.insert(BASIN_COLUMN_DEFAULT.to_string(), default_expr.clone());
    }
    let mut fields: Vec<Field> = meta.schema.fields().iter().map(|f| (**f).clone()).collect();
    let field = if md.is_empty() {
        Field::new(name, dt, nullable)
    } else {
        Field::new(name, dt, nullable).with_metadata(md)
    };
    fields.push(field);
    let new_schema = Schema::new(fields);
    catalog.set_schema(project, table, new_schema).await?;
    Ok(())
}

/// Returns `true` if the SQL type is JSONB or JSON (treated as JSONB synonym
/// in Basin v0.1). Mirrors `ddl::is_jsonb_sql` for the ADD COLUMN path.
fn is_jsonb_sql_add_col(sql: &sqlparser::ast::DataType) -> bool {
    use sqlparser::ast::DataType as SqlDataType;
    match sql {
        SqlDataType::JSONB | SqlDataType::JSON => true,
        SqlDataType::Custom(name, modifiers) => {
            name.0.len() == 1
                && name.0[0].id_val().eq_ignore_ascii_case("jsonb")
                && modifiers.is_empty()
        }
        _ => false,
    }
}

/// Returns `true` if the SQL type is UUID. Mirrors `ddl::is_uuid_sql` for
/// the ADD COLUMN path.
fn is_uuid_sql_add_col(sql: &sqlparser::ast::DataType) -> bool {
    use sqlparser::ast::DataType as SqlDataType;
    match sql {
        SqlDataType::Uuid => true,
        SqlDataType::Custom(name, modifiers) => {
            name.0.len() == 1
                && name.0[0].id_val().eq_ignore_ascii_case("uuid")
                && modifiers.is_empty()
        }
        _ => false,
    }
}

fn single_part_object_name(
    name: &ObjectName,
    schema_state: &Arc<RwLock<SchemaState>>,
) -> Result<TableName> {
    // Delegates to `schema_ddl::table_name_from_object` which accepts both
    // bare names and schema-qualified names (`myschema.t`). The schema is
    // validated (must be known on this session) and then stripped — all
    // tables live in the flat per-project catalog namespace, matching the
    // behaviour of CREATE/INSERT/SELECT/UPDATE/DELETE.
    let bare = crate::schema_ddl::table_name_from_object(name, schema_state)?;
    TableName::new(bare)
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
    // Handle double-quoted identifiers: `"table_name"`.
    // PG allows any characters inside double quotes; embedded `""` is a
    // literal double-quote.  Basin's matcher only needs to consume and
    // return the unquoted name.
    if s.starts_with('"') {
        let bytes = s.as_bytes();
        let mut i = 1usize; // skip opening `"`
        let mut name = String::new();
        loop {
            if i >= bytes.len() {
                return Err(BasinError::InvalidIdent(
                    "unterminated double-quoted identifier".into(),
                ));
            }
            if bytes[i] == b'"' {
                i += 1;
                if i < bytes.len() && bytes[i] == b'"' {
                    // Escaped double-quote `""` → literal `"`.
                    name.push('"');
                    i += 1;
                } else {
                    // End of quoted identifier.
                    return Ok((name, &s[i..]));
                }
            } else {
                // Push the char (may be multi-byte UTF-8).
                let ch_len = s[i..].chars().next().map_or(1, |c| c.len_utf8());
                name.push_str(&s[i..i + ch_len]);
                i += ch_len;
            }
        }
    }
    // Bare identifier: alphanumeric + underscore.
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
    // Accept either a bare integer (raw second count) or a quoted
    // duration literal such as `'7d'`, `'12h'`, `'30m'`, `'45s'`. The
    // latter shape is what every PG-compat migration tool emits for
    // `cold_after = '7d'` and similar.
    if let Some(inner) = val_str
        .strip_prefix('\'')
        .and_then(|s| s.strip_suffix('\''))
    {
        return parse_duration_to_seconds(inner, what);
    }
    if let Some(inner) = val_str.strip_prefix('"').and_then(|s| s.strip_suffix('"')) {
        return parse_duration_to_seconds(inner, what);
    }
    val_str.parse::<i64>().map_err(|e| {
        BasinError::InvalidSchema(format!(
            "ALTER TABLE … SET {what}: expected integer, got {val_str:?} ({e})"
        ))
    })
}

/// Hand-rolled duration parser. Accepts `<int><unit>` where unit is one
/// of `d` (days), `h` (hours), `m` (minutes), `s` (seconds). Returns the
/// equivalent total seconds. Also accepts a bare integer (treated as
/// seconds) so a numeric value inside quotes still parses.
fn parse_duration_to_seconds(s: &str, what: &str) -> Result<i64> {
    let s = s.trim();
    if s.is_empty() {
        return Err(BasinError::InvalidSchema(format!(
            "ALTER TABLE … SET {what}: empty duration"
        )));
    }
    // Split numeric prefix from unit suffix.
    let mut end = 0usize;
    for (i, c) in s.char_indices() {
        if c.is_ascii_digit() || (i == 0 && c == '-') {
            end = i + c.len_utf8();
        } else {
            break;
        }
    }
    if end == 0 {
        return Err(BasinError::InvalidSchema(format!(
            "ALTER TABLE … SET {what}: duration {s:?} has no numeric part"
        )));
    }
    let num: i64 = s[..end].parse().map_err(|e| {
        BasinError::InvalidSchema(format!(
            "ALTER TABLE … SET {what}: duration {s:?} number part: {e}"
        ))
    })?;
    let unit = s[end..].trim();
    let mul: i64 = match unit.to_ascii_lowercase().as_str() {
        "" | "s" | "sec" | "secs" | "second" | "seconds" => 1,
        "m" | "min" | "mins" | "minute" | "minutes" => 60,
        "h" | "hr" | "hrs" | "hour" | "hours" => 3600,
        "d" | "day" | "days" => 86_400,
        "w" | "wk" | "wks" | "week" | "weeks" => 7 * 86_400,
        other => {
            return Err(BasinError::InvalidSchema(format!(
                "ALTER TABLE … SET {what}: unknown duration unit {other:?}; \
                 use s/m/h/d/w"
            )));
        }
    };
    num.checked_mul(mul).ok_or_else(|| {
        BasinError::InvalidSchema(format!(
            "ALTER TABLE … SET {what}: duration {s:?} overflows i64 seconds"
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

/// Parse the `= '<fmt>'` tail of `SET FILE_FORMAT`. Accepts a quoted
/// string, a double-quoted string, or a bare identifier (mirrors
/// [`parse_eq_string_or_ident`]'s tolerance) and maps it
/// case-insensitively to a [`basin_catalog::TableFileFormat`]. Any value
/// other than `parquet` / `vortex` is rejected with the sibling
/// `InvalidSchema` error style.
fn parse_eq_file_format(rest: &str) -> Result<basin_catalog::TableFileFormat> {
    let raw = parse_eq_string_or_ident(rest, "file_format")?;
    match raw.trim().to_ascii_lowercase().as_str() {
        "parquet" => Ok(basin_catalog::TableFileFormat::Parquet),
        "vortex" => Ok(basin_catalog::TableFileFormat::Vortex),
        other => Err(BasinError::InvalidSchema(format!(
            "ALTER TABLE … SET file_format: unrecognised format {other:?}; \
             expected 'parquet' or 'vortex'"
        ))),
    }
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

    // ── SET FILE_FORMAT (#161) ────────────────────────────────────────────

    #[test]
    fn match_set_file_format_vortex() {
        let m = match_basin_alter_extension("ALTER TABLE events SET FILE_FORMAT = 'vortex'")
            .unwrap()
            .unwrap();
        assert_eq!(
            m,
            BasinAlterExtension::SetFileFormat {
                table: TableName::new("events").unwrap(),
                format: basin_catalog::TableFileFormat::Vortex,
            }
        );
    }

    #[test]
    fn match_set_file_format_parquet() {
        let m = match_basin_alter_extension("ALTER TABLE events SET FILE_FORMAT = 'parquet'")
            .unwrap()
            .unwrap();
        assert_eq!(
            m,
            BasinAlterExtension::SetFileFormat {
                table: TableName::new("events").unwrap(),
                format: basin_catalog::TableFileFormat::Parquet,
            }
        );
    }

    #[test]
    fn match_set_file_format_case_insensitive_value() {
        // Mixed/upper-case value and lower-case statement keywords both
        // resolve; the format token is matched case-insensitively.
        let m = match_basin_alter_extension("alter table EVENTS set file_format = 'VoRtEx';")
            .unwrap()
            .unwrap();
        match m {
            BasinAlterExtension::SetFileFormat { format, .. } => {
                assert_eq!(format, basin_catalog::TableFileFormat::Vortex);
            }
            other => panic!("unexpected: {other:?}"),
        }
        let m = match_basin_alter_extension("ALTER TABLE events SET FILE_FORMAT = 'PARQUET'")
            .unwrap()
            .unwrap();
        match m {
            BasinAlterExtension::SetFileFormat { format, .. } => {
                assert_eq!(format, basin_catalog::TableFileFormat::Parquet);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn match_set_file_format_bare_and_double_quoted() {
        // Bare identifier value (no quotes).
        let m = match_basin_alter_extension("ALTER TABLE events SET FILE_FORMAT = vortex")
            .unwrap()
            .unwrap();
        match m {
            BasinAlterExtension::SetFileFormat { format, .. } => {
                assert_eq!(format, basin_catalog::TableFileFormat::Vortex);
            }
            other => panic!("unexpected: {other:?}"),
        }
        // Double-quoted value.
        let m = match_basin_alter_extension("ALTER TABLE events SET FILE_FORMAT = \"parquet\"")
            .unwrap()
            .unwrap();
        match m {
            BasinAlterExtension::SetFileFormat { format, .. } => {
                assert_eq!(format, basin_catalog::TableFileFormat::Parquet);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn set_file_format_rejects_unrecognised_value() {
        let err =
            match_basin_alter_extension("ALTER TABLE events SET FILE_FORMAT = 'orc'").unwrap_err();
        assert!(matches!(err, BasinError::InvalidSchema(_)));
        let err =
            match_basin_alter_extension("ALTER TABLE events SET FILE_FORMAT = csv").unwrap_err();
        assert!(matches!(err, BasinError::InvalidSchema(_)));
    }

    #[test]
    fn match_schema_qualified_set_file_format() {
        // schema prefix must be stripped; bare table name preserved.
        let m =
            match_basin_alter_extension("ALTER TABLE myschema.events SET FILE_FORMAT = 'vortex'")
                .unwrap()
                .unwrap();
        assert_eq!(
            m,
            BasinAlterExtension::SetFileFormat {
                table: TableName::new("events").unwrap(),
                format: basin_catalog::TableFileFormat::Vortex,
            }
        );
    }

    // ── Schema-qualified extension forms (Site 3 fix) ─────────────────────

    #[test]
    fn match_schema_qualified_cold_after() {
        // `myschema.events` — schema prefix must be stripped; result table
        // name must equal bare `events`.
        let m = match_basin_alter_extension("ALTER TABLE myschema.events SET cold_after = 86400")
            .unwrap()
            .unwrap();
        assert_eq!(
            m,
            BasinAlterExtension::SetColdAfter {
                table: TableName::new("events").unwrap(),
                seconds: 86_400,
            }
        );
    }

    #[test]
    fn match_schema_qualified_cluster_by() {
        let m = match_basin_alter_extension("ALTER TABLE public.metrics CLUSTER BY (ts, id)")
            .unwrap()
            .unwrap();
        assert_eq!(
            m,
            BasinAlterExtension::SetClusterColumns {
                table: TableName::new("metrics").unwrap(),
                columns: vec!["ts".into(), "id".into()],
            }
        );
    }

    #[test]
    fn match_schema_qualified_reset_cluster_by() {
        let m = match_basin_alter_extension("ALTER TABLE myschema.events RESET CLUSTER BY")
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
    fn match_schema_qualified_bloom_filters() {
        let m = match_basin_alter_extension(
            "ALTER TABLE analytics.events SET BLOOM FILTERS ON (id, user_id)",
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            m,
            BasinAlterExtension::SetBloomFilterColumns {
                table: TableName::new("events").unwrap(),
                columns: vec!["id".into(), "user_id".into()],
            }
        );
    }

    #[test]
    fn match_schema_qualified_validate_constraint() {
        let m = match_basin_alter_extension(
            "ALTER TABLE myschema.orders VALIDATE CONSTRAINT orders_pkey",
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            m,
            BasinAlterExtension::ValidateConstraint {
                table: TableName::new("orders").unwrap(),
                name: "orders_pkey".to_string(),
            }
        );
    }

    // single_part_object_name schema-stripping (Sites 1 & 2)

    #[test]
    fn single_part_object_name_strips_known_schema() {
        use crate::pg_ast::ObjectNamePartExt as _;
        use crate::schema_ddl::SchemaState;
        use sqlparser::ast::{Ident, ObjectName};
        use std::sync::{Arc, RwLock};

        let mut st = SchemaState::default();
        st.insert("myschema".to_string());
        let schema_state: Arc<RwLock<SchemaState>> = Arc::new(RwLock::new(st));

        // Two-part name: myschema.events → bare "events"
        let name = ObjectName(vec![
            sqlparser::ast::ObjectNamePart::Identifier(Ident::new("myschema")),
            sqlparser::ast::ObjectNamePart::Identifier(Ident::new("events")),
        ]);
        let result = single_part_object_name(&name, &schema_state).unwrap();
        assert_eq!(result.as_str(), "events");
    }

    #[test]
    fn single_part_object_name_bare_still_works() {
        use crate::schema_ddl::SchemaState;
        use sqlparser::ast::{Ident, ObjectName};
        use std::sync::{Arc, RwLock};

        let schema_state: Arc<RwLock<SchemaState>> = Arc::new(RwLock::new(SchemaState::default()));

        let name = ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(
            Ident::new("events"),
        )]);
        let result = single_part_object_name(&name, &schema_state).unwrap();
        assert_eq!(result.as_str(), "events");
    }

    #[test]
    fn single_part_object_name_unknown_schema_errors() {
        use crate::schema_ddl::SchemaState;
        use sqlparser::ast::{Ident, ObjectName};
        use std::sync::{Arc, RwLock};

        // Only "public" is known by default; "badschema" is not.
        let schema_state: Arc<RwLock<SchemaState>> = Arc::new(RwLock::new(SchemaState::default()));

        let name = ObjectName(vec![
            sqlparser::ast::ObjectNamePart::Identifier(Ident::new("badschema")),
            sqlparser::ast::ObjectNamePart::Identifier(Ident::new("events")),
        ]);
        let err = single_part_object_name(&name, &schema_state).unwrap_err();
        assert!(matches!(err, BasinError::NotFound(_)));
    }

    #[test]
    fn single_part_object_name_public_schema_works() {
        use crate::schema_ddl::SchemaState;
        use sqlparser::ast::{Ident, ObjectName};
        use std::sync::{Arc, RwLock};

        // "public" is always in the default SchemaState.
        let schema_state: Arc<RwLock<SchemaState>> = Arc::new(RwLock::new(SchemaState::default()));

        let name = ObjectName(vec![
            sqlparser::ast::ObjectNamePart::Identifier(Ident::new("public")),
            sqlparser::ast::ObjectNamePart::Identifier(Ident::new("users")),
        ]);
        let result = single_part_object_name(&name, &schema_state).unwrap();
        assert_eq!(result.as_str(), "users");
    }
}
