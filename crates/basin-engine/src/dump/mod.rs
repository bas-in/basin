//! Phase 5.22.B/C — `pg_dump`-compatible export for Basin projects.
//!
//! ## Formats
//!
//! | Format              | File           | CLI flag               |
//! |---------------------|----------------|------------------------|
//! | Plain SQL (`-F p`)  | `plain.rs`     | `--format=plain` (default) |
//! | Custom binary (`-F c`) | `custom.rs` | `--format=custom`      |
//!
//! ## Public API (consumed by `basin-cli dump`)
//!
//! ```ignore
//! // Plain-text dump — returns a self-contained SQL script.
//! let sql: String = basin_engine::dump::dump_plain(&engine, project_id, opts).await?;
//!
//! // Custom binary dump — returns a self-describing archive as raw bytes.
//! let bytes: Vec<u8> = basin_engine::dump::dump_custom(&engine, project_id, opts).await?;
//!
//! // Restore from a plain SQL dump (replays the script via execute()).
//! basin_engine::dump::restore_plain(&engine, project_id, &sql).await?;
//! ```
//!
//! ## Compatibility notes
//!
//! - **Plain format** produces standard `CREATE TABLE` + `INSERT` SQL that is
//!   replay-compatible with Basin's own `execute()` path and also valid
//!   PostgreSQL syntax for compatible types.
//! - **Custom format** is Basin's own binary format — it is *not* byte-compatible
//!   with real `pg_dump`'s custom format (see 5.22.C spec: "Need not be
//!   byte-compatible"). Use plain format if you need `pg_restore` interop.
//!
//! ## Dependency order
//!
//! Tables are emitted in dependency order: a table is only emitted after all
//! tables it references via `FOREIGN KEY … REFERENCES` have been emitted.
//! Cycles (which Basin rejects at DDL time) cannot arise.

pub mod custom;
pub mod plain;

use basin_catalog::{Catalog, TableMetadata};
use basin_common::{ProjectId, Result, TableName};

/// Options controlling what the dumper emits.
#[derive(Debug, Clone, Default)]
pub struct DumpOptions {
    /// If non-empty, only these tables are dumped (in dependency order).
    /// An empty vec means "all tables". Table names are matched
    /// case-insensitively against the project's catalog.
    pub tables: Vec<String>,
    /// Include schema DDL (`CREATE TABLE …`) in the output. Defaults to `true`.
    pub schema: bool,
    /// Include data (`INSERT` / binary data blocks). Defaults to `true`.
    pub data: bool,
}

impl DumpOptions {
    /// Sensible defaults: all tables, schema + data.
    pub fn new() -> Self {
        Self {
            tables: vec![],
            schema: true,
            data: true,
        }
    }

    /// Restrict the dump to the given set of table names.
    pub fn with_tables(mut self, tables: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.tables = tables.into_iter().map(|s| s.into()).collect();
        self
    }

    /// Schema-only dump (no data rows).
    pub fn schema_only(mut self) -> Self {
        self.data = false;
        self
    }

    /// Data-only dump (no DDL).
    pub fn data_only(mut self) -> Self {
        self.schema = false;
        self
    }
}

/// Produce a plain-text SQL dump for `project` and return it as a `String`.
///
/// The returned string is a self-contained SQL script that can be replayed
/// against a Basin instance (or, for standard types, against PostgreSQL) via
/// `execute()` or `psql`.
///
/// This is the entry point `basin-cli dump --format=plain` calls.
pub async fn dump_plain(
    engine: &crate::Engine,
    project: ProjectId,
    opts: &DumpOptions,
) -> Result<String> {
    plain::dump(engine, project, opts).await
}

/// Produce a custom binary dump for `project` and return it as `Vec<u8>`.
///
/// The archive is Basin's own binary format — see `custom.rs` for the layout.
/// It is **not** byte-compatible with real `pg_dump --format=custom`. Use this
/// format for Basin→Basin transfers where you want selective restore or
/// metadata preservation beyond what the plain SQL script carries.
///
/// This is the entry point `basin-cli dump --format=custom` calls.
pub async fn dump_custom(
    engine: &crate::Engine,
    project: ProjectId,
    opts: &DumpOptions,
) -> Result<Vec<u8>> {
    custom::dump(engine, project, opts).await
}

/// Restore a project from a plain SQL dump produced by [`dump_plain`].
///
/// The script is split on statement boundaries (`;\n`) and each statement is
/// executed in order via `ProjectSession::execute`. Empty statements and
/// comment-only lines are skipped.
///
/// This is the entry point `basin-cli restore` calls for plain-format archives.
pub async fn restore_plain(
    engine: &crate::Engine,
    project: ProjectId,
    sql_script: &str,
) -> Result<()> {
    plain::restore(engine, project, sql_script).await
}

/// Restore a project from a custom binary dump produced by [`dump_custom`].
///
/// This is the entry point `basin-cli restore --format=custom` calls.
pub async fn restore_custom(
    engine: &crate::Engine,
    project: ProjectId,
    archive: &[u8],
) -> Result<()> {
    custom::restore(engine, project, archive).await
}

// ─── Internal helpers (shared between plain + custom) ────────────────────────

/// Resolve and topologically sort the set of tables to dump.
///
/// If `opts.tables` is empty, all project tables are included.
/// Returns tables in dependency order (referenced table before referencing
/// table) so that restored scripts can execute DDL top-to-bottom without FK
/// violations.
pub(crate) async fn resolve_tables(
    catalog: &dyn Catalog,
    project: &ProjectId,
    opts: &DumpOptions,
) -> Result<Vec<TableMetadata>> {
    let all_names = catalog.list_tables(project).await?;

    // Filter to requested tables (case-insensitive).
    let requested: Vec<TableName> = if opts.tables.is_empty() {
        all_names
    } else {
        all_names
            .into_iter()
            .filter(|n| {
                opts.tables
                    .iter()
                    .any(|req| req.eq_ignore_ascii_case(n.as_str()))
            })
            .collect()
    };

    // Load metadata for each requested table.
    let mut metas = Vec::with_capacity(requested.len());
    for name in &requested {
        let meta = catalog.load_table(project, name).await?;
        metas.push(meta);
    }

    // Topological sort by FK dependency.
    topo_sort(metas)
}

/// Kahn's algorithm for topological sort of tables by FK dependency.
/// Tables with no outgoing FKs (or whose FK targets are not in the dump set)
/// come first. Cycles cannot exist — Basin rejects them at DDL time.
fn topo_sort(metas: Vec<TableMetadata>) -> Result<Vec<TableMetadata>> {
    use std::collections::{HashMap, VecDeque};

    let n = metas.len();
    // Map name → index for O(1) lookup.
    let idx: HashMap<&str, usize> = metas
        .iter()
        .enumerate()
        .map(|(i, m)| (m.table.as_str(), i))
        .collect();

    // Build adjacency: edge (i → j) means "table i references table j"
    // (j must come before i in the output).
    let mut in_degree = vec![0usize; n];
    // deps[i] = list of tables i depends on (must precede i).
    let mut needed_by: Vec<Vec<usize>> = vec![vec![]; n]; // needed_by[j] = tables that need j
    for (i, meta) in metas.iter().enumerate() {
        for fk in &meta.foreign_keys {
            if let Some(&j) = idx.get(fk.ref_table.as_str()) {
                if j != i {
                    needed_by[j].push(i);
                    in_degree[i] += 1;
                }
            }
        }
    }

    // Start with tables that have no unresolved dependencies.
    let mut queue: VecDeque<usize> = (0..n).filter(|&i| in_degree[i] == 0).collect();
    let mut order: Vec<usize> = Vec::with_capacity(n);

    while let Some(j) = queue.pop_front() {
        order.push(j);
        for &i in &needed_by[j] {
            in_degree[i] -= 1;
            if in_degree[i] == 0 {
                queue.push_back(i);
            }
        }
    }

    // A cycle would leave some tables with in_degree > 0; Basin prevents
    // this at DDL time, so we treat any residual as an internal error.
    if order.len() < n {
        return Err(basin_common::BasinError::Internal(
            "dump: cyclic FK dependency detected — this should not happen in Basin".into(),
        ));
    }

    let mut by_idx: Vec<Option<TableMetadata>> = metas.into_iter().map(Some).collect();
    Ok(order
        .into_iter()
        .map(|i| by_idx[i].take().expect("each index visited once"))
        .collect())
}

/// Convert an Arrow `Schema` field to its PostgreSQL type name string,
/// honoring Basin logical-type markers stored in field metadata.
///
/// This is the single source of truth for the DDL renderer; both `plain.rs`
/// and `custom.rs` call it.
pub(crate) fn field_to_pg_type(field: &arrow_schema::Field) -> String {
    use crate::types::{
        BASIN_CHARLEN_KEY, BASIN_ENUM_TYPE_KEY, BASIN_TYPE_BIT_PREFIX, BASIN_TYPE_CIDR,
        BASIN_TYPE_CITEXT, BASIN_TYPE_DATERANGE, BASIN_TYPE_INET, BASIN_TYPE_INT4RANGE,
        BASIN_TYPE_INT8RANGE, BASIN_TYPE_JSONB, BASIN_TYPE_KEY, BASIN_TYPE_MACADDR,
        BASIN_TYPE_MACADDR8, BASIN_TYPE_MONEY, BASIN_TYPE_NUMRANGE, BASIN_TYPE_TSQUERY,
        BASIN_TYPE_TSRANGE, BASIN_TYPE_TSTZRANGE, BASIN_TYPE_TSVECTOR, BASIN_TYPE_UUID,
        BASIN_TYPE_VARBIT_PREFIX, BASIN_TYPE_XML,
    };
    use arrow_schema::{DataType, IntervalUnit};

    let meta = field.metadata();

    // Enum columns: stored as Utf8 with BASIN_ENUM_TYPE_KEY.
    if let Some(enum_name) = meta.get(BASIN_ENUM_TYPE_KEY) {
        return enum_name.clone();
    }

    // Basin logical type marker overrides the Arrow physical type.
    if let Some(basin_type) = meta.get(BASIN_TYPE_KEY) {
        match basin_type.as_str() {
            t if t == BASIN_TYPE_JSONB => return "JSONB".to_string(),
            t if t == BASIN_TYPE_UUID => return "UUID".to_string(),
            t if t == BASIN_TYPE_TSVECTOR => return "TSVECTOR".to_string(),
            t if t == BASIN_TYPE_TSQUERY => return "TSQUERY".to_string(),
            t if t == BASIN_TYPE_INET => return "INET".to_string(),
            t if t == BASIN_TYPE_CIDR => return "CIDR".to_string(),
            t if t == BASIN_TYPE_MACADDR => return "MACADDR".to_string(),
            t if t == BASIN_TYPE_MACADDR8 => return "MACADDR8".to_string(),
            t if t == BASIN_TYPE_CITEXT => return "CITEXT".to_string(),
            t if t == BASIN_TYPE_MONEY => return "MONEY".to_string(),
            t if t == BASIN_TYPE_XML => return "XML".to_string(),
            t if t == BASIN_TYPE_INT4RANGE => return "INT4RANGE".to_string(),
            t if t == BASIN_TYPE_INT8RANGE => return "INT8RANGE".to_string(),
            t if t == BASIN_TYPE_NUMRANGE => return "NUMRANGE".to_string(),
            t if t == BASIN_TYPE_DATERANGE => return "DATERANGE".to_string(),
            t if t == BASIN_TYPE_TSRANGE => return "TSRANGE".to_string(),
            t if t == BASIN_TYPE_TSTZRANGE => return "TSTZRANGE".to_string(),
            t if t.starts_with(BASIN_TYPE_BIT_PREFIX) => {
                // "BIT(n)" → "BIT(n)"
                return t.to_string();
            }
            t if t.starts_with(BASIN_TYPE_VARBIT_PREFIX) => {
                // "VARBIT" or "VARBIT(n)" → "BIT VARYING" / "BIT VARYING(n)"
                if t == BASIN_TYPE_VARBIT_PREFIX {
                    return "BIT VARYING".to_string();
                }
                // Strip "VARBIT(" prefix and ")" suffix to get n.
                let n = t
                    .strip_prefix("VARBIT(")
                    .and_then(|s| s.strip_suffix(')'))
                    .unwrap_or("0");
                return format!("BIT VARYING({n})");
            }
            _ => {} // fall through to Arrow type mapping
        }
    }

    // VARCHAR(n) / CHAR(n) via BASIN_CHARLEN_KEY.
    if let Some(charlen) = meta.get(BASIN_CHARLEN_KEY) {
        if let Some(rest) = charlen.strip_prefix("varchar(") {
            let n = rest.strip_suffix(')').unwrap_or("0");
            return format!("CHARACTER VARYING({n})");
        }
        if let Some(rest) = charlen.strip_prefix("char(") {
            let n = rest.strip_suffix(')').unwrap_or("1");
            return format!("CHARACTER({n})");
        }
    }

    // Arrow physical type → PG type name.
    match field.data_type() {
        DataType::Int8 | DataType::Int16 => "SMALLINT".to_string(),
        DataType::Int32 => "INTEGER".to_string(),
        DataType::Int64 => "BIGINT".to_string(),
        DataType::UInt8 | DataType::UInt16 => "SMALLINT".to_string(),
        DataType::UInt32 => "INTEGER".to_string(),
        DataType::UInt64 => "BIGINT".to_string(),
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::Float32 => "REAL".to_string(),
        DataType::Float64 => "DOUBLE PRECISION".to_string(),
        DataType::Utf8 | DataType::LargeUtf8 => "TEXT".to_string(),
        DataType::Binary | DataType::LargeBinary => "BYTEA".to_string(),
        DataType::Date32 => "DATE".to_string(),
        DataType::Timestamp(_, Some(_)) => "TIMESTAMPTZ".to_string(),
        DataType::Timestamp(_, None) => "TIMESTAMP".to_string(),
        DataType::Decimal128(p, s) => format!("NUMERIC({p},{s})"),
        DataType::Interval(IntervalUnit::MonthDayNano) => "INTERVAL".to_string(),
        DataType::Time32(_) | DataType::Time64(_) => "TIME".to_string(),
        DataType::FixedSizeBinary(16) => "UUID".to_string(), // UUID fallback
        // Arrays: drill into the element type.
        DataType::List(elem) | DataType::LargeList(elem) | DataType::FixedSizeList(elem, _) => {
            let elem_field = elem.as_ref().clone();
            let elem_pg = field_to_pg_type(&elem_field);
            format!("{elem_pg}[]")
        }
        _ => "TEXT".to_string(),
    }
}

/// Quote a bare identifier as a PostgreSQL double-quoted identifier.
/// Appends `"` around the name and escapes any `"` within it.
pub(crate) fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Escape a text value for inclusion in a SQL string literal (single-quote
/// escape style). Wraps the result in `'...'`.
pub(crate) fn sql_quote_str(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}
