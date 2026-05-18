// pg_query AST matchers + check-predicate helpers staged for ADR 0014 Phase 2.
#![allow(dead_code)]

//! `CREATE/ALTER/DROP TYPE` and `CREATE/DROP DOMAIN` SQL surface.
//!
//! Five statement shapes route through this module:
//!
//! 1. `CREATE TYPE <name> AS ENUM ('a', 'b', …)` — register an enum
//!    catalog row. sqlparser 0.52 carries the parsed shape natively as
//!    `Statement::CreateType { representation: Enum { labels } }`.
//!
//! 2. `DROP TYPE [IF EXISTS] <name>` — sqlparser routes this through
//!    `Statement::Drop` with `ObjectType::Type`. We intercept the
//!    Type-flavoured Drop in the executor and dispatch here.
//!
//! 3. `ALTER TYPE <name> ADD VALUE 'new'` — sqlparser 0.52 has no
//!    `Statement::AlterType` AST node, so we recognise the form
//!    textually before sqlparser sees the SQL.
//!
//! 4. `CREATE DOMAIN <name> AS <type> [CHECK (predicate)]` — sqlparser
//!    0.52 does not parse `CREATE DOMAIN` either; same textual
//!    pre-screen pattern.
//!
//! 5. `DROP DOMAIN [IF EXISTS] <name>` — also unparsed by sqlparser
//!    0.52; textual pre-screen.
//!
//! Type resolution at `CREATE TABLE` time is in
//! [`resolve_user_type_columns`]: any column whose declared type is an
//! `ObjectName` (`Custom` in sqlparser's AST) is matched against the
//! enum and domain catalogs and rewritten to its underlying Arrow
//! datatype with a `BASIN_ENUM_TYPE` / `BASIN_DOMAIN` field metadata
//! marker. The marker is read back at INSERT-time by
//! [`crate::dml`] (label / CHECK validation) and at
//! `drop_enum_type` / `drop_domain` time by the catalog (refcounting).

use crate::pg_ast::ObjectNamePartExt;
use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, BooleanArray, RecordBatch};
use arrow_schema::Field;
use basin_catalog::{Catalog, DomainDef, EnumTypeDef, SqlArgType};
use basin_common::{BasinError, ProjectId, Result};
use sqlparser::ast::{ColumnDef, DataType as SqlDataType, Ident, ObjectName};

use crate::types::{BASIN_DOMAIN_KEY, BASIN_ENUM_TYPE_KEY};
use crate::{ExecResult, ProjectSession};

/// Recognised intent for one column whose declared type is a name
/// (`status order_status`). Either the name matched an enum catalog
/// row or a domain catalog row; the engine then knows which Arrow
/// type to use for the column and which metadata marker to attach.
#[derive(Debug, Clone)]
pub(crate) enum UserTypeBinding {
    Enum(EnumTypeDef),
    Domain(DomainDef),
}

/// Translate a sqlparser `DataType` (the bare-keyword shape used by
/// `CREATE DOMAIN positive_int AS INT …`) into a catalog-side
/// `SqlArgType`. Only the scalar set the function / domain catalog
/// supports; anything else is `InvalidSchema`.
fn sql_data_type_to_arg(dt: &SqlDataType) -> Result<SqlArgType> {
    use sqlparser::ast::TimezoneInfo;
    match dt {
        SqlDataType::Int(_) | SqlDataType::Integer(_) | SqlDataType::Int4(_) => Ok(SqlArgType::Int),
        SqlDataType::BigInt(_) | SqlDataType::Int8(_) => Ok(SqlArgType::BigInt),
        SqlDataType::Text
        | SqlDataType::Varchar(_)
        | SqlDataType::CharacterVarying(_)
        | SqlDataType::Char(_)
        | SqlDataType::Character(_)
        | SqlDataType::String(_) => Ok(SqlArgType::Text),
        SqlDataType::Boolean | SqlDataType::Bool => Ok(SqlArgType::Boolean),
        SqlDataType::Double(_)
        | SqlDataType::DoublePrecision
        | SqlDataType::Float8
        | SqlDataType::Float(_) => Ok(SqlArgType::Double),
        SqlDataType::Bytea => Ok(SqlArgType::Bytea),
        SqlDataType::Date => Ok(SqlArgType::Date),
        SqlDataType::Timestamp(_, tz_info) => match tz_info {
            TimezoneInfo::Tz | TimezoneInfo::WithTimeZone => Ok(SqlArgType::TimestampTz),
            // Bare `TIMESTAMP` and explicit `TIMESTAMP WITHOUT TIME ZONE`
            // both land as the no-tz variant. PG semantic: a wall-clock
            // value with no zone information.
            _ => Ok(SqlArgType::Timestamp),
        },
        other => Err(BasinError::InvalidSchema(format!(
            "unsupported domain base type: {other}"
        ))),
    }
}

/// Walk `columns`, look up any custom type names against the enum and
/// domain catalogs, and return a per-column binding for those that
/// resolved. Columns with built-in types or unrecognised custom names
/// are left out of the map; built-ins fall through to the existing
/// `arrow_data_type` resolution and unrecognised names will surface
/// the existing "unsupported custom type" error from there.
pub(crate) async fn resolve_user_type_columns(
    catalog: &Arc<dyn Catalog>,
    project: &ProjectId,
    columns: &[ColumnDef],
) -> Result<HashMap<String, UserTypeBinding>> {
    let mut out: HashMap<String, UserTypeBinding> = HashMap::new();
    for col in columns {
        let SqlDataType::Custom(obj, modifiers) = &col.data_type else {
            continue;
        };
        if obj.0.len() != 1 || !modifiers.is_empty() {
            continue;
        }
        let name = &obj.0[0].id_val();
        // `vector(N)` / `JSONB` / `UUID` and all network/bit-string/money
        // types are recognised by `arrow_data_type`; we must not shadow them.
        // Skip the built-in custom-keyword set so unmodified `arrow_data_type`
        // logic stays canonical.
        let lc = name.to_ascii_lowercase();
        if matches!(
            lc.as_str(),
            "vector"
                | "jsonb"
                | "uuid"
                | "inet"
                | "cidr"
                | "macaddr"
                | "macaddr8"
                | "money"
                | "bit"
                | "varbit"
        ) {
            continue;
        }
        if let Some(def) = catalog.lookup_enum_type(project, name).await {
            out.insert(col.name.value.clone(), UserTypeBinding::Enum(def));
            continue;
        }
        if let Some(def) = catalog.lookup_domain(project, name).await {
            out.insert(col.name.value.clone(), UserTypeBinding::Domain(def));
            continue;
        }
    }
    Ok(out)
}

/// Replace every user-type column's `data_type` in `columns` with the
/// underlying Arrow-mappable shape so the standard schema-builder can
/// consume them. Returns the (additional) per-column metadata to layer
/// on top of the resulting `Field`s.
///
/// Two pieces of state move forward from this rewrite:
///
/// 1. The mutated `columns` slice's `data_type` for every bound
///    column is replaced by `Text` (for enums) or the domain's base
///    sqlparser type. The standard `arrow_data_type` resolves these
///    cleanly.
/// 2. The returned map records `(BASIN_*=name)` metadata to add to
///    each field in a post-processing pass.
pub(crate) fn rewrite_user_type_columns(
    columns: &mut [ColumnDef],
    bindings: &HashMap<String, UserTypeBinding>,
) -> Result<HashMap<String, HashMap<String, String>>> {
    let mut field_md: HashMap<String, HashMap<String, String>> = HashMap::new();
    for col in columns.iter_mut() {
        let Some(binding) = bindings.get(&col.name.value) else {
            continue;
        };
        match binding {
            UserTypeBinding::Enum(def) => {
                col.data_type = SqlDataType::Text;
                let mut md = HashMap::new();
                md.insert(BASIN_ENUM_TYPE_KEY.to_string(), def.name.clone());
                field_md.insert(col.name.value.clone(), md);
            }
            UserTypeBinding::Domain(def) => {
                col.data_type = arg_type_to_sql(def.base_type);
                let mut md = HashMap::new();
                md.insert(BASIN_DOMAIN_KEY.to_string(), def.name.clone());
                field_md.insert(col.name.value.clone(), md);
            }
        }
    }
    Ok(field_md)
}

/// Inverse of `sql_data_type_to_arg` — translates a domain's stored
/// base type back into a sqlparser `DataType` so `arrow_data_type`
/// can produce the matching Arrow type. The round-trip is lossless
/// for the scalar types domains accept.
fn arg_type_to_sql(arg: SqlArgType) -> SqlDataType {
    use sqlparser::ast::TimezoneInfo;
    match arg {
        SqlArgType::Int => SqlDataType::Int(None),
        SqlArgType::BigInt => SqlDataType::BigInt(None),
        SqlArgType::Text => SqlDataType::Text,
        SqlArgType::Boolean => SqlDataType::Boolean,
        SqlArgType::Double => SqlDataType::DoublePrecision,
        SqlArgType::Bytea => SqlDataType::Bytea,
        SqlArgType::Date => SqlDataType::Date,
        SqlArgType::TimestampTz => SqlDataType::Timestamp(None, TimezoneInfo::WithTimeZone),
        SqlArgType::Timestamp => SqlDataType::Timestamp(None, TimezoneInfo::WithoutTimeZone),
    }
}

/// Layer the per-column metadata produced by `rewrite_user_type_columns`
/// onto the freshly-built `Schema`. The schema builder produced
/// `Field`s with their own metadata (e.g. JSONB markers,
/// AUTO_UPDATE); we merge ours on top non-destructively.
pub(crate) fn apply_user_type_metadata(
    schema: arrow_schema::Schema,
    field_md: &HashMap<String, HashMap<String, String>>,
) -> arrow_schema::Schema {
    if field_md.is_empty() {
        return schema;
    }
    let schema_md = schema.metadata().clone();
    let mut new_fields = Vec::with_capacity(schema.fields().len());
    for f in schema.fields() {
        if let Some(extra) = field_md.get(f.name()) {
            let mut merged = f.metadata().clone();
            for (k, v) in extra {
                merged.insert(k.clone(), v.clone());
            }
            let mut new_f = Field::new(f.name(), f.data_type().clone(), f.is_nullable());
            new_f = new_f.with_metadata(merged);
            new_fields.push(new_f);
        } else {
            new_fields.push((**f).clone());
        }
    }
    arrow_schema::Schema::new_with_metadata(new_fields, schema_md)
}

// --- CREATE / ALTER / DROP TYPE -----------------------------------------

/// Execute `CREATE TYPE <name> AS ENUM ('label1', 'label2', …)`.
pub(crate) async fn exec_create_type_enum(
    sess: &ProjectSession,
    name: ObjectName,
    labels: Vec<Ident>,
) -> Result<ExecResult> {
    let type_name = single_part_object_name(&name)?;
    if labels.is_empty() {
        return Err(BasinError::InvalidSchema(
            "CREATE TYPE … AS ENUM requires at least one label".into(),
        ));
    }
    let label_strings: Vec<String> = labels.into_iter().map(|i| i.value).collect();
    let def = EnumTypeDef {
        project: sess.project,
        name: type_name,
        labels: label_strings,
    };
    let catalog: Arc<dyn Catalog> = sess.engine.config().catalog.clone();
    catalog.register_enum_type(def).await?;
    Ok(ExecResult::Empty {
        tag: "CREATE TYPE".into(),
    })
}

/// Execute `ALTER TYPE <name> ADD VALUE 'label'`.
pub(crate) async fn exec_alter_type_add_value(
    sess: &ProjectSession,
    name: &str,
    value: &str,
) -> Result<ExecResult> {
    let catalog: Arc<dyn Catalog> = sess.engine.config().catalog.clone();
    catalog.add_enum_value(&sess.project, name, value).await?;
    Ok(ExecResult::Empty {
        tag: "ALTER TYPE".into(),
    })
}

/// Execute `DROP TYPE [IF EXISTS] <name>`. `if_exists` swallows
/// NotFound; everything else (including refcount-blocked drops)
/// surfaces.
pub(crate) async fn exec_drop_type(
    sess: &ProjectSession,
    if_exists: bool,
    names: &[ObjectName],
) -> Result<ExecResult> {
    if names.is_empty() {
        return Err(BasinError::InvalidSchema(
            "DROP TYPE: at least one type name is required".into(),
        ));
    }
    let catalog: Arc<dyn Catalog> = sess.engine.config().catalog.clone();
    for name in names {
        let type_name = single_part_object_name(name)?;
        match catalog.drop_enum_type(&sess.project, &type_name).await {
            Ok(()) => {}
            Err(BasinError::NotFound(_)) if if_exists => {}
            Err(BasinError::NotFound(_)) => {
                return Err(BasinError::not_found(format!(
                    "type {type_name:?} does not exist"
                )));
            }
            Err(e) => return Err(e),
        }
    }
    Ok(ExecResult::Empty {
        tag: "DROP TYPE".into(),
    })
}

// --- CREATE / DROP DOMAIN ----------------------------------------------

/// Execute `CREATE DOMAIN <name> AS <base_type> [CHECK (<predicate>)]`.
/// The shape comes from [`match_create_domain`].
pub(crate) async fn exec_create_domain(
    sess: &ProjectSession,
    name: &str,
    base_type: SqlArgType,
    check_predicate: Option<String>,
) -> Result<ExecResult> {
    let def = DomainDef {
        project: sess.project,
        name: name.to_string(),
        base_type,
        check_predicate,
    };
    let catalog: Arc<dyn Catalog> = sess.engine.config().catalog.clone();
    catalog.register_domain(def).await?;
    Ok(ExecResult::Empty {
        tag: "CREATE DOMAIN".into(),
    })
}

/// Execute `DROP DOMAIN [IF EXISTS] <name>`.
pub(crate) async fn exec_drop_domain(
    sess: &ProjectSession,
    name: &str,
    if_exists: bool,
) -> Result<ExecResult> {
    let catalog: Arc<dyn Catalog> = sess.engine.config().catalog.clone();
    match catalog.drop_domain(&sess.project, name).await {
        Ok(()) => {}
        Err(BasinError::NotFound(_)) if if_exists => {}
        Err(BasinError::NotFound(_)) => {
            return Err(BasinError::not_found(format!(
                "domain {name:?} does not exist"
            )));
        }
        Err(e) => return Err(e),
    }
    Ok(ExecResult::Empty {
        tag: "DROP DOMAIN".into(),
    })
}

// --- Textual pre-screens -----------------------------------------------

/// Recognise `ALTER TYPE <name> ADD VALUE '<label>'` (with optional
/// trailing `;`). Returns `(name, label)` on a match. sqlparser 0.52
/// doesn't model `ALTER TYPE`, so we handle the full statement
/// textually before sqlparser sees the SQL.
pub(crate) fn match_alter_type_add_value(sql: &str) -> Result<Option<(String, String)>> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    if !starts_with_kw(trimmed, "ALTER") {
        return Ok(None);
    }
    let after_alter = skip_word(trimmed).trim_start();
    if !starts_with_kw(after_alter, "TYPE") {
        return Ok(None);
    }
    let after_type = skip_word(after_alter).trim_start();
    let (name, after_name) = read_simple_identifier(after_type)?;
    let after_name = after_name.trim_start();
    if !starts_with_kw(after_name, "ADD") {
        return Err(BasinError::InvalidSchema(
            "ALTER TYPE: only ADD VALUE is supported in v0.1".into(),
        ));
    }
    let after_add = skip_word(after_name).trim_start();
    if !starts_with_kw(after_add, "VALUE") {
        return Err(BasinError::InvalidSchema(
            "ALTER TYPE ADD: expected VALUE".into(),
        ));
    }
    let after_value = skip_word(after_add).trim_start();
    let (label, rest) = read_string_literal(after_value)?;
    if !rest.trim().is_empty() {
        return Err(BasinError::InvalidSchema(format!(
            "ALTER TYPE ADD VALUE: unexpected trailing input {:?}",
            rest.trim()
        )));
    }
    Ok(Some((name, label)))
}

/// Recognise `CREATE DOMAIN <name> AS <type> [CHECK (<expr>)]`.
/// Returns `(name, base_type, optional_check_predicate)`. sqlparser
/// 0.52's `CREATE` parser rejects `DOMAIN`, so we handle the full
/// statement textually.
pub(crate) fn match_create_domain(
    sql: &str,
) -> Result<Option<(String, SqlArgType, Option<String>)>> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    if !starts_with_kw(trimmed, "CREATE") {
        return Ok(None);
    }
    let after_create = skip_word(trimmed).trim_start();
    if !starts_with_kw(after_create, "DOMAIN") {
        return Ok(None);
    }
    let after_domain = skip_word(after_create).trim_start();
    let (name, after_name) = read_simple_identifier(after_domain)?;
    let after_name = after_name.trim_start();
    // `AS` is optional in PG: `CREATE DOMAIN d INT` is also legal.
    let after_as = if starts_with_kw(after_name, "AS") {
        skip_word(after_name).trim_start()
    } else {
        after_name
    };
    // Read the base type as one or two whitespace-separated identifier
    // tokens (`TIMESTAMPTZ`, `DOUBLE PRECISION`). We use a tiny
    // sqlparser sub-parse to catch keywords like `INT4` / `TIMESTAMPTZ`.
    let (type_text, after_type) = read_type_token(after_as)?;
    let parsed_dt = parse_data_type_text(&type_text)?;
    let base = sql_data_type_to_arg(&parsed_dt)?;

    let after_type = after_type.trim_start();
    let check = if starts_with_kw(after_type, "CHECK") {
        let after_check = skip_word(after_type).trim_start();
        let inside = read_paren_block_inner(after_check)?;
        Some(inside.trim().to_string())
    } else if !after_type.is_empty() {
        return Err(BasinError::InvalidSchema(format!(
            "CREATE DOMAIN: unexpected trailing input {after_type:?}"
        )));
    } else {
        None
    };

    Ok(Some((name, base, check)))
}

/// Recognise `DROP DOMAIN [IF EXISTS] <name> [CASCADE | RESTRICT]`.
/// Returns `(name, if_exists)`.
pub(crate) fn match_drop_domain(sql: &str) -> Result<Option<(String, bool)>> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    if !starts_with_kw(trimmed, "DROP") {
        return Ok(None);
    }
    let after_drop = skip_word(trimmed).trim_start();
    if !starts_with_kw(after_drop, "DOMAIN") {
        return Ok(None);
    }
    let after_domain = skip_word(after_drop).trim_start();
    let (after_if, if_exists) = if starts_with_kw(after_domain, "IF") {
        let after_if = skip_word(after_domain).trim_start();
        if !starts_with_kw(after_if, "EXISTS") {
            return Err(BasinError::InvalidSchema(
                "DROP DOMAIN IF: expected EXISTS".into(),
            ));
        }
        (skip_word(after_if).trim_start(), true)
    } else {
        (after_domain, false)
    };
    let (name, rest) = read_simple_identifier(after_if)?;
    let rest = rest.trim();
    if !rest.is_empty()
        && !rest.eq_ignore_ascii_case("cascade")
        && !rest.eq_ignore_ascii_case("restrict")
    {
        return Err(BasinError::InvalidSchema(format!(
            "DROP DOMAIN: unexpected trailing input {rest:?}"
        )));
    }
    Ok(Some((name, if_exists)))
}

// --- Domain CHECK enforcement on INSERT --------------------------------

/// For each batch column whose `Field` carries `BASIN_DOMAIN=<name>`,
/// look up the domain and evaluate its `CHECK (predicate)` expression
/// against every row of the batch. Rows that fail the predicate (or
/// where the predicate evaluates to NULL — PG models this as a
/// violation in domains) cause SQLSTATE `23514 check_violation`.
///
/// Returns `Ok(())` when every row passes (or the batch has no
/// domain-typed columns).
pub(crate) async fn enforce_domain_checks(
    catalog: &Arc<dyn Catalog>,
    project: &ProjectId,
    batch: &RecordBatch,
) -> Result<()> {
    // Walk fields once; for each domain column with a CHECK predicate,
    // evaluate the predicate via DataFusion against a one-column
    // RecordBatch where the column name has been renamed to `VALUE`.
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;

    let mut to_check: Vec<(usize, DomainDef, String)> = Vec::new();
    for (i, f) in batch.schema().fields().iter().enumerate() {
        let Some(domain_name) = f.metadata().get(BASIN_DOMAIN_KEY) else {
            continue;
        };
        let Some(def) = catalog.lookup_domain(project, domain_name).await else {
            continue;
        };
        let Some(predicate) = def.check_predicate.clone() else {
            continue;
        };
        to_check.push((i, def, predicate));
    }
    if to_check.is_empty() {
        return Ok(());
    }
    for (col_idx, def, predicate) in to_check {
        // Build a single-column workspace-arrow batch with field name
        // `VALUE` so the domain's predicate (which references `VALUE`)
        // resolves. We then bridge through `convert::batch_ws_to_df`
        // because DataFusion's `register_batch` expects its own arrow
        // version.
        // DataFusion folds unquoted identifiers to lowercase (PG's
        // identifier rule), so we name the bind column `value` —
        // matching the predicate's unquoted `VALUE` reference. PG-side
        // domain predicates conventionally use uppercase; we accept
        // either via case-insensitive resolution.
        let value_field = Field::new(
            "value",
            batch.schema().field(col_idx).data_type().clone(),
            true,
        );
        let mini_schema = Arc::new(arrow_schema::Schema::new(vec![value_field]));
        let mini_batch =
            RecordBatch::try_new(mini_schema.clone(), vec![batch.column(col_idx).clone()])
                .map_err(|e| BasinError::internal(format!("domain CHECK batch build: {e}")))?;
        let df_batch = crate::convert::batch_ws_to_df(&mini_batch)?;
        let df_schema = df_batch.schema();
        let provider = MemTable::try_new(df_schema, vec![vec![df_batch]])
            .map_err(|e| BasinError::internal(format!("domain CHECK MemTable: {e}")))?;
        let ctx = SessionContext::new();
        ctx.register_table("v", Arc::new(provider))
            .map_err(|e| BasinError::internal(format!("domain CHECK register: {e}")))?;
        let sql = format!("SELECT ({predicate}) AS ok, value FROM v");
        let df = ctx.sql(&sql).await.map_err(|e| {
            BasinError::InvalidSchema(format!(
                "domain {} CHECK predicate {predicate:?}: {e}",
                def.name
            ))
        })?;
        let results = df.collect().await.map_err(|e| {
            BasinError::InvalidSchema(format!("domain {} CHECK evaluation: {e}", def.name))
        })?;
        for rb in &results {
            let ws_rb = crate::convert::batch_df_to_ws(rb)?;
            let ok_arr = ws_rb
                .column(0)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    BasinError::InvalidSchema(format!(
                        "domain {} CHECK: predicate must return BOOLEAN",
                        def.name
                    ))
                })?;
            for row in 0..ws_rb.num_rows() {
                if ok_arr.is_null(row) || !ok_arr.value(row) {
                    return Err(BasinError::InvalidSchema(format!(
                        "SQLSTATE 23514 check_violation: domain {:?} CHECK ({}) failed",
                        def.name, predicate
                    )));
                }
            }
        }
    }
    Ok(())
}

/// For each batch column whose `Field` carries `BASIN_ENUM_TYPE=<name>`,
/// validate that every Utf8 cell is one of the enum's labels. NULL
/// cells skip the check (column nullability is enforced elsewhere).
/// Unknown labels surface SQLSTATE `22P02 invalid_text_representation`.
pub(crate) async fn enforce_enum_labels(
    catalog: &Arc<dyn Catalog>,
    project: &ProjectId,
    batch: &RecordBatch,
) -> Result<()> {
    use arrow_array::StringArray;
    for (i, f) in batch.schema().fields().iter().enumerate() {
        let Some(enum_name) = f.metadata().get(BASIN_ENUM_TYPE_KEY) else {
            continue;
        };
        let Some(def) = catalog.lookup_enum_type(project, enum_name).await else {
            continue;
        };
        let arr = batch
            .column(i)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                BasinError::internal(format!(
                    "enum column {} must be Utf8 (got {:?})",
                    f.name(),
                    batch.column(i).data_type()
                ))
            })?;
        let labels: std::collections::HashSet<&str> =
            def.labels.iter().map(|s| s.as_str()).collect();
        for row in 0..arr.len() {
            if arr.is_null(row) {
                continue;
            }
            let v = arr.value(row);
            if !labels.contains(v) {
                return Err(BasinError::InvalidSchema(format!(
                    "SQLSTATE 22P02 invalid_text_representation: \
                     {v:?} is not a valid label for enum {:?} (valid: {:?})",
                    def.name, def.labels
                )));
            }
        }
    }
    Ok(())
}

// --- Lexer helpers (mirrors function_ddl.rs / cv_ddl.rs) ---------------

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

/// Read a single-quoted string literal, returning the unescaped value
/// (PG's `''` doubled-quote escape collapses to a single quote).
fn read_string_literal(s: &str) -> Result<(String, &str)> {
    let s = s.trim_start();
    let bytes = s.as_bytes();
    if bytes.is_empty() || bytes[0] != b'\'' {
        return Err(BasinError::InvalidSchema(
            "expected a single-quoted string literal".into(),
        ));
    }
    let mut out = String::new();
    let mut i = 1;
    while i < bytes.len() {
        if bytes[i] == b'\'' {
            if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                out.push('\'');
                i += 2;
                continue;
            }
            return Ok((out, &s[i + 1..]));
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    Err(BasinError::InvalidSchema(
        "unterminated string literal".into(),
    ))
}

/// Read up to the next non-type-token boundary. Accepts a single
/// identifier optionally followed by another identifier
/// (`DOUBLE PRECISION`, `TIMESTAMP WITH TIME ZONE`) and an optional
/// parenthesised modifier list (`VARCHAR(255)`). Returns the raw
/// substring (a reparseable type expression) and the remainder.
fn read_type_token(s: &str) -> Result<(String, &str)> {
    let s = s.trim_start();
    let bytes = s.as_bytes();
    if bytes.is_empty() {
        return Err(BasinError::InvalidSchema(
            "CREATE DOMAIN: missing base type".into(),
        ));
    }
    let start = 0;
    let mut i = 0;
    // First identifier.
    if !(bytes[i].is_ascii_alphabetic() || bytes[i] == b'_') {
        return Err(BasinError::InvalidSchema(
            "CREATE DOMAIN: expected a type identifier".into(),
        ));
    }
    while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
        i += 1;
    }
    // Optional `(...)` modifier list.
    let mut j = i;
    while j < bytes.len() && bytes[j].is_ascii_whitespace() {
        j += 1;
    }
    if j < bytes.len() && bytes[j] == b'(' {
        // Skip balanced parens.
        let mut depth = 1i32;
        let mut k = j + 1;
        while k < bytes.len() && depth > 0 {
            match bytes[k] {
                b'(' => depth += 1,
                b')' => depth -= 1,
                _ => {}
            }
            if depth == 0 {
                k += 1;
                break;
            }
            k += 1;
        }
        if depth != 0 {
            return Err(BasinError::InvalidSchema(
                "CREATE DOMAIN: unterminated type modifier".into(),
            ));
        }
        i = k;
    }
    // Optional second identifier (e.g. `DOUBLE PRECISION`,
    // `TIMESTAMP WITH TIME ZONE` — the latter has THREE tokens, but
    // we only support `TIMESTAMPTZ` shape via the single keyword in
    // `sql_data_type_to_arg`; multi-word forms aren't required for
    // v0.1).
    let mut p = i;
    while p < bytes.len() && bytes[p].is_ascii_whitespace() {
        p += 1;
    }
    if p < bytes.len() && (bytes[p].is_ascii_alphabetic() || bytes[p] == b'_') {
        // Peek the next word; only consume it if it's part of a known
        // multi-word type. Anything else (`CHECK`, `DEFAULT`, ...) is
        // the next clause.
        let word_start = p;
        let mut q = p;
        while q < bytes.len() && (bytes[q].is_ascii_alphanumeric() || bytes[q] == b'_') {
            q += 1;
        }
        let word = &s[word_start..q];
        if word.eq_ignore_ascii_case("PRECISION") {
            i = q;
        }
    }
    Ok((s[start..i].to_string(), &s[i..]))
}

/// Re-parse a type-token substring through sqlparser's column-type
/// parser by wrapping it in a synthetic CREATE TABLE.
fn parse_data_type_text(text: &str) -> Result<SqlDataType> {
    let synth = format!("CREATE TABLE __t (c {text})");
    let dialect = sqlparser::dialect::PostgreSqlDialect {};
    let stmts = sqlparser::parser::Parser::parse_sql(&dialect, &synth).map_err(|e| {
        BasinError::InvalidSchema(format!("CREATE DOMAIN: bad base type {text:?}: {e}"))
    })?;
    use sqlparser::ast::Statement;
    if let Some(Statement::CreateTable(ct)) = stmts.into_iter().next() {
        if let Some(c) = ct.columns.into_iter().next() {
            return Ok(c.data_type);
        }
    }
    Err(BasinError::InvalidSchema(format!(
        "CREATE DOMAIN: could not parse base type {text:?}"
    )))
}

/// `s` must start with `(`. Returns the parenthesised body (without
/// the surrounding parens). Tracks depth and skips string literals
/// so a `)` inside `'…'` doesn't close early.
fn read_paren_block_inner(s: &str) -> Result<String> {
    let bytes = s.as_bytes();
    if bytes.is_empty() || bytes[0] != b'(' {
        return Err(BasinError::InvalidSchema(
            "CHECK: expected '(' after keyword".into(),
        ));
    }
    let mut depth = 1i32;
    let mut i = 1;
    while i < bytes.len() {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Ok(s[1..i].to_string());
                }
            }
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
    Err(BasinError::InvalidSchema("unterminated CHECK block".into()))
}

/// Pull a bare-identifier function/type name out of an `ObjectName`.
fn single_part_object_name(name: &ObjectName) -> Result<String> {
    if name.0.len() != 1 {
        return Err(BasinError::InvalidIdent(format!(
            "schema-qualified type names not supported in v0.1: {name}"
        )));
    }
    Ok(name.0[0].id_val().clone())
}

// ==========================================================================
// pg_query AST-based matchers (alongside the textual ones above)
// ==========================================================================

/// Intent produced by [`match_create_type_enum_ast`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CreateEnumIntent {
    /// Bare type name (single-part, no schema qualifier).
    pub name: String,
    /// Ordered list of enum label values.
    pub labels: Vec<String>,
}

/// AST-based matcher for `ALTER TYPE <name> ADD VALUE '<label>'`.
///
/// libpg_query parses this natively as
/// [`pg_query::protobuf::AlterEnumStmt`]. `type_name` is a list of
/// `String` nodes; `new_val` carries the new label text.
///
/// Returns `(type_name, new_label)`.
pub(crate) fn match_alter_type_add_value_ast(
    stmt: &pg_query::protobuf::AlterEnumStmt,
) -> Result<(String, String)> {
    use pg_query::NodeEnum;
    let type_name = stmt
        .type_name
        .first()
        .and_then(|n| match n.node.as_ref()? {
            NodeEnum::String(s) => Some(s.sval.clone()),
            _ => None,
        })
        .ok_or_else(|| {
            BasinError::InvalidSchema(
                "ALTER TYPE ADD VALUE: could not extract type name from AST".into(),
            )
        })?;
    if stmt.new_val.is_empty() {
        return Err(BasinError::InvalidSchema(
            "ALTER TYPE ADD VALUE: empty value in AST".into(),
        ));
    }
    Ok((type_name, stmt.new_val.clone()))
}

/// AST-based matcher for
/// `CREATE DOMAIN <name> AS <base_type> [CHECK (<predicate>)]`.
///
/// libpg_query parses this natively as
/// [`pg_query::protobuf::CreateDomainStmt`]. `domainname` holds the name
/// as a list of `String` nodes; `type_name.names` encodes the base type
/// (e.g., `["pg_catalog", "int4"]` for `INT`); `constraints` holds any
/// `CHECK` constraints.
///
/// Returns `(name, base_type, optional_check_predicate)`.
pub(crate) fn match_create_domain_ast(
    stmt: &pg_query::protobuf::CreateDomainStmt,
) -> Result<(String, SqlArgType, Option<String>)> {
    use pg_query::NodeEnum;

    // Extract the domain name.
    let name = stmt
        .domainname
        .first()
        .and_then(|n| match n.node.as_ref()? {
            NodeEnum::String(s) => Some(s.sval.clone()),
            _ => None,
        })
        .ok_or_else(|| {
            BasinError::InvalidSchema(
                "CREATE DOMAIN: could not extract domain name from AST".into(),
            )
        })?;

    // Extract the base type by mapping the pg_catalog type name.
    let type_name_ast = stmt.type_name.as_ref().ok_or_else(|| {
        BasinError::InvalidSchema("CREATE DOMAIN: missing type_name in AST".into())
    })?;
    let base_type = pg_type_name_to_sql_arg(type_name_ast)?;

    // Extract any CHECK constraint predicate.
    let check = extract_check_predicate(&stmt.constraints)?;

    Ok((name, base_type, check))
}

/// AST-based matcher for `DROP DOMAIN [IF EXISTS] <name>`.
///
/// libpg_query routes this as a [`pg_query::protobuf::DropStmt`] with
/// `remove_type == ObjectType::ObjectDomain`. Returns
/// `Some((name, if_exists))` for domain drops; `None` for other DROP forms.
pub(crate) fn match_drop_domain_ast(
    stmt: &pg_query::protobuf::DropStmt,
) -> Result<Option<(String, bool)>> {
    use pg_query::protobuf::ObjectType;
    if stmt.remove_type != ObjectType::ObjectDomain as i32 {
        return Ok(None);
    }
    let name = drop_stmt_first_name_type(&stmt.objects).ok_or_else(|| {
        BasinError::InvalidSchema("DROP DOMAIN: could not extract name from AST".into())
    })?;
    Ok(Some((name, stmt.missing_ok)))
}

/// AST-based matcher for `CREATE TYPE <name> AS ENUM ('label1', …)`.
///
/// libpg_query parses this natively as
/// [`pg_query::protobuf::CreateEnumStmt`]. `type_name` is a list of
/// `String` nodes; `vals` is a list of `String` nodes for the labels.
///
/// Returns `Some(CreateEnumIntent)`.
pub(crate) fn match_create_type_enum_ast(
    stmt: &pg_query::protobuf::CreateEnumStmt,
) -> Result<Option<CreateEnumIntent>> {
    use pg_query::NodeEnum;

    let name = stmt
        .type_name
        .first()
        .and_then(|n| match n.node.as_ref()? {
            NodeEnum::String(s) => Some(s.sval.clone()),
            _ => None,
        })
        .ok_or_else(|| {
            BasinError::InvalidSchema(
                "CREATE TYPE AS ENUM: could not extract type name from AST".into(),
            )
        })?;

    if stmt.vals.is_empty() {
        return Err(BasinError::InvalidSchema(
            "CREATE TYPE … AS ENUM requires at least one label".into(),
        ));
    }
    let mut labels = Vec::with_capacity(stmt.vals.len());
    for node in &stmt.vals {
        match node.node.as_ref() {
            Some(NodeEnum::String(s)) => labels.push(s.sval.clone()),
            _ => {
                return Err(BasinError::InvalidSchema(
                    "CREATE TYPE AS ENUM: unexpected label node type in AST".into(),
                ));
            }
        }
    }

    Ok(Some(CreateEnumIntent { name, labels }))
}

/// Convert a pg_query `TypeName` (from `CreateDomainStmt.type_name`) to the
/// catalog's `SqlArgType`. The `names` list contains pg_catalog-qualified
/// names like `["pg_catalog", "int4"]`; we look at the last element.
fn pg_type_name_to_sql_arg(tn: &pg_query::protobuf::TypeName) -> Result<SqlArgType> {
    use pg_query::NodeEnum;
    // The last element is the actual type name; the earlier ones are the
    // schema qualifier (`pg_catalog`).
    let last_name: String = tn
        .names
        .iter()
        .rev()
        .find_map(|n| match n.node.as_ref()? {
            NodeEnum::String(s) if s.sval != "pg_catalog" => Some(s.sval.clone()),
            _ => None,
        })
        .or_else(|| {
            tn.names.last().and_then(|n| match n.node.as_ref()? {
                NodeEnum::String(s) => Some(s.sval.clone()),
                _ => None,
            })
        })
        .ok_or_else(|| {
            BasinError::InvalidSchema(
                "CREATE DOMAIN: cannot determine base type from AST TypeName".into(),
            )
        })?;

    match last_name.as_str() {
        "int2" | "smallint" => Ok(SqlArgType::Int),
        "int4" | "int" | "integer" => Ok(SqlArgType::Int),
        "int8" | "bigint" => Ok(SqlArgType::BigInt),
        "text" | "varchar" | "bpchar" | "char" => Ok(SqlArgType::Text),
        "bool" | "boolean" => Ok(SqlArgType::Boolean),
        "float4" | "float8" | "numeric" | "real" | "double" => Ok(SqlArgType::Double),
        "bytea" => Ok(SqlArgType::Bytea),
        "date" => Ok(SqlArgType::Date),
        "timestamptz" | "timestamp with time zone" => Ok(SqlArgType::TimestampTz),
        "timestamp" | "timestamp without time zone" => Ok(SqlArgType::Timestamp),
        other => Err(BasinError::InvalidSchema(format!(
            "CREATE DOMAIN: unsupported base type {other:?} in AST"
        ))),
    }
}

/// Extract the first `CHECK` constraint's raw expression as a SQL string.
///
/// libpg_query's C deparser does not directly support expression-level
/// nodes (it dispatches on statement-level node types). To recover the
/// CHECK predicate text we wrap our raw_expr Constraint inside a
/// synthetic `CreateDomainStmt` (which the deparser DOES support),
/// deparse the whole thing, then extract the predicate substring
/// between `CHECK (` and the matching `)`.
///
/// Returns `None` when there are no CHECK constraints.
fn extract_check_predicate(constraints: &[pg_query::protobuf::Node]) -> Result<Option<String>> {
    use pg_query::protobuf::ConstrType;
    use pg_query::NodeEnum;

    for node in constraints {
        let Some(NodeEnum::Constraint(c)) = node.node.as_ref() else {
            continue;
        };
        if c.contype != ConstrType::ConstrCheck as i32 {
            continue;
        }
        if c.raw_expr.is_none() {
            continue;
        }
        // Build a minimal synthetic CREATE DOMAIN __t AS int <our check>
        // and deparse the whole thing, then extract the CHECK body.
        let pred_sql = deparse_check_via_synthetic_domain(node).map_err(|e| {
            BasinError::InvalidSchema(format!(
                "CREATE DOMAIN: failed to deparse CHECK predicate: {e}"
            ))
        })?;
        return Ok(Some(pred_sql));
    }
    Ok(None)
}

/// Reconstruct the CHECK predicate text by wrapping the Constraint node
/// in a synthetic `CREATE DOMAIN __t AS int <check>` parse result and
/// asking libpg_query's deparser to render it. The deparser supports
/// `CreateDomainStmt` natively; we then extract the substring inside
/// the rendered `CHECK (...)` clause.
fn deparse_check_via_synthetic_domain(
    constraint_node: &pg_query::protobuf::Node,
) -> std::result::Result<String, pg_query::Error> {
    // Parse a known-good template to get a fully-shaped CreateDomainStmt
    // (correct version, all required defaults populated by libpg_query),
    // then mutate its constraints list to hold ours.
    let template = pg_query::parse("CREATE DOMAIN __t AS int CHECK (true)")?;
    let mut pr = template.protobuf;
    // Replace the template's constraint with our real one.
    if let Some(raw_stmt) = pr.stmts.first_mut() {
        if let Some(stmt) = raw_stmt.stmt.as_mut() {
            if let Some(pg_query::NodeEnum::CreateDomainStmt(ref mut cds)) = stmt.node.as_mut() {
                cds.constraints = vec![constraint_node.clone()];
            }
        }
    }
    let sql = pg_query::deparse(&pr)?;
    // Extract the substring between `CHECK (` and the matching `)`.
    Ok(extract_check_body(&sql).unwrap_or(sql))
}

/// Locate `CHECK (...)` in deparsed CREATE DOMAIN SQL and return the
/// body between the parens. Falls back to `None` when the pattern isn't
/// found (caller uses the whole string as a last resort).
fn extract_check_body(sql: &str) -> Option<String> {
    let lower = sql.to_ascii_lowercase();
    let check_start = lower.find("check")?;
    // Skip past "check" and any whitespace to find the '('.
    let mut idx = check_start + "check".len();
    let bytes = sql.as_bytes();
    while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
        idx += 1;
    }
    if idx >= bytes.len() || bytes[idx] != b'(' {
        return None;
    }
    let open = idx;
    let mut depth = 1i32;
    let mut j = open + 1;
    while j < bytes.len() && depth > 0 {
        match bytes[j] {
            b'(' => depth += 1,
            b')' => depth -= 1,
            _ => {}
        }
        if depth == 0 {
            return Some(sql[open + 1..j].trim().to_string());
        }
        j += 1;
    }
    None
}

/// Extract the first name from a `DropStmt.objects` list for type/domain
/// drops. PG's parser emits different shapes depending on the object
/// kind:
///   * `DROP DOMAIN d` → `[ TypeName { names: [ String("d") ] } ]`
///   * `DROP TYPE  t` → `[ TypeName { names: [ String("t") ] } ]`
///   * `DROP TABLE x` → `[ List { items: [ String("x") ] } ]`
/// We handle both shapes plus a bare `String` fallback.
fn drop_stmt_first_name_type(objects: &[pg_query::protobuf::Node]) -> Option<String> {
    use pg_query::NodeEnum;
    let first = objects.first()?;
    match first.node.as_ref()? {
        NodeEnum::TypeName(tn) => {
            // Take the last String node in `names` (skip schema qualifiers).
            tn.names.iter().rev().find_map(|n| match n.node.as_ref()? {
                NodeEnum::String(s) => Some(s.sval.clone()),
                _ => None,
            })
        }
        NodeEnum::List(list) => {
            let item = list.items.first()?;
            match item.node.as_ref()? {
                NodeEnum::String(s) => Some(s.sval.clone()),
                _ => None,
            }
        }
        NodeEnum::String(s) => Some(s.sval.clone()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn match_alter_type_add_value_basic() {
        let r = match_alter_type_add_value("ALTER TYPE order_status ADD VALUE 'refunded'").unwrap();
        assert_eq!(r, Some(("order_status".into(), "refunded".into())));
    }

    #[test]
    fn match_alter_type_add_value_case_insensitive() {
        let r = match_alter_type_add_value("alter type order_status add value 'paid'").unwrap();
        assert_eq!(r, Some(("order_status".into(), "paid".into())));
    }

    #[test]
    fn match_alter_type_add_value_trailing_semicolon() {
        let r = match_alter_type_add_value("ALTER TYPE t ADD VALUE 'x';").unwrap();
        assert_eq!(r, Some(("t".into(), "x".into())));
    }

    #[test]
    fn match_alter_type_non_type_returns_none() {
        let r = match_alter_type_add_value("ALTER TABLE foo ADD COLUMN bar INT").unwrap();
        assert!(r.is_none());
    }

    #[test]
    fn match_alter_type_unsupported_clause_errors() {
        let err = match_alter_type_add_value("ALTER TYPE t RENAME TO t2").unwrap_err();
        assert!(matches!(err, BasinError::InvalidSchema(_)));
    }

    #[test]
    fn match_create_domain_basic() {
        let r = match_create_domain("CREATE DOMAIN d AS INT CHECK (VALUE > 0)").unwrap();
        let (name, base, check) = r.unwrap();
        assert_eq!(name, "d");
        assert_eq!(base, SqlArgType::Int);
        assert_eq!(check.unwrap(), "VALUE > 0");
    }

    #[test]
    fn match_create_domain_no_check() {
        let r = match_create_domain("CREATE DOMAIN d AS TEXT").unwrap();
        let (name, base, check) = r.unwrap();
        assert_eq!(name, "d");
        assert_eq!(base, SqlArgType::Text);
        assert!(check.is_none());
    }

    #[test]
    fn match_create_domain_double_precision_base_type() {
        let r =
            match_create_domain("CREATE DOMAIN p AS DOUBLE PRECISION CHECK (VALUE > 0.0)").unwrap();
        let (_, base, _) = r.unwrap();
        assert_eq!(base, SqlArgType::Double);
    }

    #[test]
    fn match_create_domain_non_match_returns_none() {
        let r = match_create_domain("CREATE TABLE t (c INT)").unwrap();
        assert!(r.is_none());
    }

    #[test]
    fn match_drop_domain_basic() {
        let r = match_drop_domain("DROP DOMAIN d").unwrap();
        assert_eq!(r, Some(("d".into(), false)));
    }

    #[test]
    fn match_drop_domain_if_exists() {
        let r = match_drop_domain("DROP DOMAIN IF EXISTS d CASCADE").unwrap();
        assert_eq!(r, Some(("d".into(), true)));
    }

    #[test]
    fn match_drop_domain_non_match_returns_none() {
        let r = match_drop_domain("DROP TABLE t").unwrap();
        assert!(r.is_none());
    }

    // --- AST-based matcher tests ------------------------------------------

    #[test]
    fn ast_match_alter_type_add_value_basic() {
        use pg_query::NodeEnum;
        let r = pg_query::parse("ALTER TYPE order_status ADD VALUE 'refunded'").unwrap();
        let stmt = r.protobuf.stmts.first().unwrap().stmt.as_ref().unwrap();
        let NodeEnum::AlterEnumStmt(ref aes) = stmt.node.as_ref().unwrap() else {
            panic!("expected AlterEnumStmt");
        };
        let (type_name, label) = match_alter_type_add_value_ast(aes).unwrap();
        assert_eq!(type_name, "order_status");
        assert_eq!(label, "refunded");
    }

    #[test]
    fn ast_match_create_domain_no_check() {
        use pg_query::NodeEnum;
        let r = pg_query::parse("CREATE DOMAIN d AS TEXT").unwrap();
        let stmt = r.protobuf.stmts.first().unwrap().stmt.as_ref().unwrap();
        let NodeEnum::CreateDomainStmt(ref cds) = stmt.node.as_ref().unwrap() else {
            panic!("expected CreateDomainStmt");
        };
        let (name, base, check) = match_create_domain_ast(cds).unwrap();
        assert_eq!(name, "d");
        assert_eq!(base, SqlArgType::Text);
        assert!(check.is_none());
    }

    #[test]
    fn ast_match_create_domain_int_with_check() {
        use pg_query::NodeEnum;
        let r = pg_query::parse("CREATE DOMAIN d AS INT CHECK (VALUE > 0)").unwrap();
        let stmt = r.protobuf.stmts.first().unwrap().stmt.as_ref().unwrap();
        let NodeEnum::CreateDomainStmt(ref cds) = stmt.node.as_ref().unwrap() else {
            panic!("expected CreateDomainStmt");
        };
        let (name, base, check) = match_create_domain_ast(cds).unwrap();
        assert_eq!(name, "d");
        assert_eq!(base, SqlArgType::Int);
        // The CHECK predicate should contain the expression (exact form
        // depends on deparse output, but must mention the comparison).
        let pred = check.expect("should have CHECK predicate");
        assert!(
            pred.contains('>') || pred.contains("value"),
            "pred={pred:?}"
        );
    }

    #[test]
    fn ast_match_drop_domain_if_exists() {
        use pg_query::NodeEnum;
        let r = pg_query::parse("DROP DOMAIN IF EXISTS d CASCADE").unwrap();
        let stmt = r.protobuf.stmts.first().unwrap().stmt.as_ref().unwrap();
        let NodeEnum::DropStmt(ref ds) = stmt.node.as_ref().unwrap() else {
            panic!("expected DropStmt");
        };
        let result = match_drop_domain_ast(ds).unwrap();
        assert_eq!(result, Some(("d".to_string(), true)));
    }

    #[test]
    fn ast_match_drop_domain_basic() {
        use pg_query::NodeEnum;
        let r = pg_query::parse("DROP DOMAIN d").unwrap();
        let stmt = r.protobuf.stmts.first().unwrap().stmt.as_ref().unwrap();
        let NodeEnum::DropStmt(ref ds) = stmt.node.as_ref().unwrap() else {
            panic!("expected DropStmt");
        };
        let result = match_drop_domain_ast(ds).unwrap();
        assert_eq!(result, Some(("d".to_string(), false)));
    }

    #[test]
    fn ast_match_drop_domain_returns_none_for_drop_table() {
        use pg_query::NodeEnum;
        let r = pg_query::parse("DROP TABLE t").unwrap();
        let stmt = r.protobuf.stmts.first().unwrap().stmt.as_ref().unwrap();
        let NodeEnum::DropStmt(ref ds) = stmt.node.as_ref().unwrap() else {
            panic!("expected DropStmt");
        };
        let result = match_drop_domain_ast(ds).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn ast_match_create_type_enum_basic() {
        use pg_query::NodeEnum;
        let r =
            pg_query::parse("CREATE TYPE order_status AS ENUM ('pending', 'paid', 'cancelled')")
                .unwrap();
        let stmt = r.protobuf.stmts.first().unwrap().stmt.as_ref().unwrap();
        let NodeEnum::CreateEnumStmt(ref ces) = stmt.node.as_ref().unwrap() else {
            panic!("expected CreateEnumStmt");
        };
        let intent = match_create_type_enum_ast(ces).unwrap().unwrap();
        assert_eq!(intent.name, "order_status");
        assert_eq!(intent.labels, vec!["pending", "paid", "cancelled"]);
    }

    #[test]
    fn ast_match_create_type_enum_single_label() {
        use pg_query::NodeEnum;
        let r = pg_query::parse("CREATE TYPE t AS ENUM ('x')").unwrap();
        let stmt = r.protobuf.stmts.first().unwrap().stmt.as_ref().unwrap();
        let NodeEnum::CreateEnumStmt(ref ces) = stmt.node.as_ref().unwrap() else {
            panic!("expected CreateEnumStmt");
        };
        let intent = match_create_type_enum_ast(ces).unwrap().unwrap();
        assert_eq!(intent.name, "t");
        assert_eq!(intent.labels, vec!["x"]);
    }
}
