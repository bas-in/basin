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

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, BooleanArray, RecordBatch};
use arrow_schema::Field;
use basin_catalog::{Catalog, DomainDef, EnumTypeDef, SqlArgType};
use basin_common::{BasinError, Result, TenantId};
use sqlparser::ast::{ColumnDef, DataType as SqlDataType, Ident, ObjectName};

use crate::types::{BASIN_DOMAIN_KEY, BASIN_ENUM_TYPE_KEY};
use crate::{ExecResult, TenantSession};

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
    tenant: &TenantId,
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
        let name = &obj.0[0].value;
        // `vector(N)` / `JSONB` / `UUID` are recognised by
        // `arrow_data_type`; we must not shadow them. Skip the
        // built-in custom-keyword set so unmodified `arrow_data_type`
        // logic stays canonical.
        let lc = name.to_ascii_lowercase();
        if lc == "vector" || lc == "jsonb" || lc == "uuid" {
            continue;
        }
        if let Some(def) = catalog.lookup_enum_type(tenant, name).await {
            out.insert(col.name.value.clone(), UserTypeBinding::Enum(def));
            continue;
        }
        if let Some(def) = catalog.lookup_domain(tenant, name).await {
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
    sess: &TenantSession,
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
        tenant: sess.tenant,
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
    sess: &TenantSession,
    name: &str,
    value: &str,
) -> Result<ExecResult> {
    let catalog: Arc<dyn Catalog> = sess.engine.config().catalog.clone();
    catalog.add_enum_value(&sess.tenant, name, value).await?;
    Ok(ExecResult::Empty {
        tag: "ALTER TYPE".into(),
    })
}

/// Execute `DROP TYPE [IF EXISTS] <name>`. `if_exists` swallows
/// NotFound; everything else (including refcount-blocked drops)
/// surfaces.
pub(crate) async fn exec_drop_type(
    sess: &TenantSession,
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
        match catalog.drop_enum_type(&sess.tenant, &type_name).await {
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
    sess: &TenantSession,
    name: &str,
    base_type: SqlArgType,
    check_predicate: Option<String>,
) -> Result<ExecResult> {
    let def = DomainDef {
        tenant: sess.tenant,
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
    sess: &TenantSession,
    name: &str,
    if_exists: bool,
) -> Result<ExecResult> {
    let catalog: Arc<dyn Catalog> = sess.engine.config().catalog.clone();
    match catalog.drop_domain(&sess.tenant, name).await {
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
    tenant: &TenantId,
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
        let Some(def) = catalog.lookup_domain(tenant, domain_name).await else {
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
        let mini_batch = RecordBatch::try_new(
            mini_schema.clone(),
            vec![batch.column(col_idx).clone()],
        )
        .map_err(|e| BasinError::internal(format!("domain CHECK batch build: {e}")))?;
        let df_batch = crate::convert::batch_ws_to_df(&mini_batch)?;
        let df_schema = df_batch.schema();
        let provider = MemTable::try_new(df_schema, vec![vec![df_batch]])
            .map_err(|e| BasinError::internal(format!("domain CHECK MemTable: {e}")))?;
        let ctx = SessionContext::new();
        ctx.register_table("v", Arc::new(provider))
            .map_err(|e| BasinError::internal(format!("domain CHECK register: {e}")))?;
        let sql = format!("SELECT ({predicate}) AS ok, value FROM v");
        let df = ctx
            .sql(&sql)
            .await
            .map_err(|e| {
                BasinError::InvalidSchema(format!(
                    "domain {} CHECK predicate {predicate:?}: {e}",
                    def.name
                ))
            })?;
        let results = df
            .collect()
            .await
            .map_err(|e| {
                BasinError::InvalidSchema(format!(
                    "domain {} CHECK evaluation: {e}",
                    def.name
                ))
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
    tenant: &TenantId,
    batch: &RecordBatch,
) -> Result<()> {
    use arrow_array::StringArray;
    for (i, f) in batch.schema().fields().iter().enumerate() {
        let Some(enum_name) = f.metadata().get(BASIN_ENUM_TYPE_KEY) else {
            continue;
        };
        let Some(def) = catalog.lookup_enum_type(tenant, enum_name).await else {
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
    Ok(name.0[0].value.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn match_alter_type_add_value_basic() {
        let r = match_alter_type_add_value("ALTER TYPE order_status ADD VALUE 'refunded'")
            .unwrap();
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
        let r = match_create_domain("CREATE DOMAIN p AS DOUBLE PRECISION CHECK (VALUE > 0.0)")
            .unwrap();
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
}
