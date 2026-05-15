// Resolver helpers staged for ADR 0014 Phase 2; suppress dead-code until wired.
#![allow(dead_code)]

//! `CREATE SCHEMA`, `DROP SCHEMA`, and `SET search_path` SQL surface.
//!
//! ## Basin's flat model
//!
//! Basin uses a flat per-project prefix in object storage. Schemas are
//! *namespace metadata* tracked in session state; they do not produce
//! separate storage prefixes or catalog namespaces. A `CREATE SCHEMA`
//! records the schema name; `DROP SCHEMA` removes it. All qualified
//! references (`myschema.t`) are resolved to the bare table name (`t`)
//! since all tables share the project's single catalog namespace.
//!
//! ## `public` is always present
//!
//! Every session pre-seeds `"public"` in its schema set. `CREATE SCHEMA
//! public` / `CREATE SCHEMA IF NOT EXISTS public` are both idempotent.
//!
//! ## search_path
//!
//! `SET search_path = myschema, public` stores the ordered list on the
//! session. Unqualified table-name resolution consults this list from
//! left to right. In v0.1 all tables are in one flat namespace, so
//! search_path mainly affects how client-facing qualified names are
//! accepted; the actual catalog lookup always uses the bare table name.
//!
//! ## AUTHORIZATION
//!
//! `CREATE SCHEMA AUTHORIZATION alice` is accepted. The owner is ignored —
//! Basin's auth model is JWT-based, not role-based.

use std::collections::HashSet;
use std::sync::{Arc, RwLock};

use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use basin_common::{BasinError, Result};
use sqlparser::ast::{ObjectName, SchemaName};

use crate::{ExecResult, ProjectSession};

/// Per-session schema state. Held behind `Arc<RwLock<...>>` inside
/// `SessionState` so all methods on `ProjectSession` can reach it without
/// borrowing `SessionState` directly.
#[derive(Debug)]
pub(crate) struct SchemaState {
    /// Set of schema names known to this session. `"public"` is always
    /// present; additional names are added by `CREATE SCHEMA` and removed
    /// by `DROP SCHEMA`.
    pub(crate) schemas: HashSet<String>,
    /// Ordered list of schemas to search when resolving unqualified table
    /// names. Initialised to `["public"]`; updated by `SET search_path`.
    pub(crate) search_path: Vec<String>,
}

impl Default for SchemaState {
    fn default() -> Self {
        let mut schemas = HashSet::new();
        schemas.insert("public".to_string());
        Self {
            schemas,
            search_path: vec!["public".to_string()],
        }
    }
}

impl SchemaState {
    /// True when `name` (case-insensitive) is a known schema.
    pub(crate) fn contains(&self, name: &str) -> bool {
        self.schemas.iter().any(|s| s.eq_ignore_ascii_case(name))
    }

    /// Insert `name` (normalised to lowercase). Returns `true` if newly added.
    pub(crate) fn insert(&mut self, name: String) -> bool {
        self.schemas.insert(name.to_ascii_lowercase())
    }

    /// Remove `name` (case-insensitive). Returns `true` if it existed.
    pub(crate) fn remove(&mut self, name: &str) -> bool {
        let key = name.to_ascii_lowercase();
        self.schemas.remove(&key)
    }
}

/// A schema-qualified (or bare) table name resolved from a sqlparser
/// `ObjectName`. The schema part is present only for two-part names.
#[derive(Debug, Clone)]
pub(crate) struct ResolvedName {
    /// Schema, if the user wrote `schema.table`. `None` for bare names.
    pub schema: Option<String>,
    /// The bare table name.
    pub table: String,
}

/// Resolve a sqlparser `ObjectName` to a `(schema?, table)` pair.
///
/// - One part → bare table name; schema is `None`.
/// - Two parts → `(schema, table)`.
/// - Three or more parts → rejected.
pub(crate) fn resolve_object_name(name: &ObjectName) -> Result<ResolvedName> {
    match name.0.len() {
        1 => Ok(ResolvedName {
            schema: None,
            table: name.0[0].value.clone(),
        }),
        2 => Ok(ResolvedName {
            schema: Some(name.0[0].value.clone()),
            table: name.0[1].value.clone(),
        }),
        _ => Err(BasinError::InvalidIdent(format!(
            "expected table name with at most one schema qualifier, got: {name}"
        ))),
    }
}

/// Extract the bare table name from an `ObjectName`, accepting both
/// bare (`t`) and schema-qualified (`myschema.t`) forms. The schema
/// qualifier is validated (must be a known schema on this session) but
/// stripped for catalog lookup — all tables live in the flat project
/// namespace.
pub(crate) fn table_name_from_object(
    name: &ObjectName,
    schema_state: &Arc<RwLock<SchemaState>>,
) -> Result<String> {
    let resolved = resolve_object_name(name)?;
    if let Some(schema) = &resolved.schema {
        let st = schema_state
            .read()
            .expect("schema_state lock poisoned");
        if !st.contains(schema) {
            return Err(BasinError::NotFound(format!(
                "schema {schema:?} does not exist"
            )));
        }
    }
    Ok(resolved.table)
}

// ─── CREATE SCHEMA ──────────────────────────────────────────────────────────

pub(crate) async fn exec_create_schema(
    sess: &ProjectSession,
    schema_name: SchemaName,
    if_not_exists: bool,
) -> Result<ExecResult> {
    let name = schema_name_str(&schema_name)?;

    // "public" always exists — treat any CREATE SCHEMA public as idempotent
    // regardless of IF NOT EXISTS.
    if name.eq_ignore_ascii_case("public") {
        return Ok(ExecResult::Empty {
            tag: "CREATE SCHEMA".into(),
        });
    }

    let mut st = sess
        .state
        .schema_state
        .write()
        .expect("schema_state lock poisoned");

    if st.contains(&name) {
        if if_not_exists {
            return Ok(ExecResult::Empty {
                tag: "CREATE SCHEMA".into(),
            });
        }
        return Err(BasinError::InvalidSchema(format!(
            "schema {name:?} already exists"
        )));
    }

    st.insert(name);
    Ok(ExecResult::Empty {
        tag: "CREATE SCHEMA".into(),
    })
}

// ─── DROP SCHEMA ────────────────────────────────────────────────────────────

pub(crate) async fn exec_drop_schema(
    sess: &ProjectSession,
    names: &[ObjectName],
    if_exists: bool,
    // CASCADE / RESTRICT — Basin's flat model has no owned objects per schema,
    // so both are accepted and treated identically (no cascade needed).
    _cascade: bool,
) -> Result<ExecResult> {
    let mut st = sess
        .state
        .schema_state
        .write()
        .expect("schema_state lock poisoned");

    for name_obj in names {
        let parts: Vec<&str> = name_obj.0.iter().map(|i| i.value.as_str()).collect();
        if parts.len() != 1 {
            return Err(BasinError::InvalidIdent(format!(
                "expected a simple schema name, got: {name_obj}"
            )));
        }
        let name = parts[0];

        if name.eq_ignore_ascii_case("public") {
            return Err(BasinError::InvalidSchema(
                "cannot drop the public schema".to_string(),
            ));
        }

        if !st.remove(name) {
            if !if_exists {
                return Err(BasinError::NotFound(format!(
                    "schema {name:?} does not exist"
                )));
            }
        }
    }

    Ok(ExecResult::Empty {
        tag: "DROP SCHEMA".into(),
    })
}

// ─── ALTER SCHEMA … RENAME TO ───────────────────────────────────────────────

/// Textual pre-screen for `ALTER SCHEMA name RENAME TO new_name`.
///
/// sqlparser 0.52 has no `AlterSchema` AST node, so we recognise the full
/// statement textually (case-insensitive, whitespace-tolerant). Returns
/// `(old_name, new_name)` on success; `None` when the SQL isn't this shape.
pub(crate) fn match_alter_schema_rename(sql: &str) -> Option<(String, String)> {
    // Normalise: collapse runs of whitespace to single space, trim.
    let norm: String = sql
        .split_ascii_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    // Expected shape (case-insensitive):
    //   ALTER SCHEMA <old> RENAME TO <new>
    let upper = norm.to_ascii_uppercase();
    if !upper.starts_with("ALTER SCHEMA ") {
        return None;
    }
    let rest = &norm["ALTER SCHEMA ".len()..];
    // Find " RENAME TO " (case-insensitive) in `rest`.
    let upper_rest = rest.to_ascii_uppercase();
    let pivot = upper_rest.find(" RENAME TO ")?;
    let old = rest[..pivot].trim().to_string();
    let new = rest[pivot + " RENAME TO ".len()..].trim().trim_end_matches(';').trim().to_string();
    if old.is_empty() || new.is_empty() {
        return None;
    }
    Some((old, new))
}

/// Execute `ALTER SCHEMA old RENAME TO new`.
///
/// Validates that `old` exists, `new` does not, and neither is `"public"`.
/// Updates the session-local schema state and the search_path in-place.
pub(crate) async fn exec_alter_schema_rename(
    sess: &ProjectSession,
    old: &str,
    new: &str,
) -> Result<ExecResult> {
    if old.eq_ignore_ascii_case("public") {
        return Err(BasinError::InvalidSchema(
            "cannot rename the public schema".to_string(),
        ));
    }
    if new.eq_ignore_ascii_case("public") {
        return Err(BasinError::InvalidSchema(
            "cannot rename to 'public'".to_string(),
        ));
    }

    let mut st = sess
        .state
        .schema_state
        .write()
        .expect("schema_state lock poisoned");

    if !st.contains(old) {
        return Err(BasinError::NotFound(format!(
            "schema {old:?} does not exist"
        )));
    }
    if st.contains(new) {
        return Err(BasinError::InvalidSchema(format!(
            "schema {new:?} already exists"
        )));
    }

    st.remove(old);
    st.insert(new.to_ascii_lowercase());

    // Also update search_path entries that reference the old name.
    let old_lc = old.to_ascii_lowercase();
    let new_lc = new.to_ascii_lowercase();
    for entry in &mut st.search_path {
        if *entry == old_lc {
            *entry = new_lc.clone();
        }
    }

    Ok(ExecResult::Empty {
        tag: "ALTER SCHEMA".into(),
    })
}

// ─── SHOW search_path ────────────────────────────────────────────────────────

/// Handle `SHOW search_path`.
///
/// Returns a single-column, single-row result set with the current
/// `search_path` value formatted as a comma-separated string, matching
/// Postgres's `SHOW search_path` response shape.
pub(crate) fn exec_show_search_path(sess: &ProjectSession) -> Result<ExecResult> {
    let st = sess
        .state
        .schema_state
        .read()
        .expect("schema_state lock poisoned");
    let value = st.search_path.join(", ");
    drop(st);

    let schema = Arc::new(Schema::new(vec![Field::new(
        "search_path",
        DataType::Utf8,
        false,
    )]));
    let arr: ArrayRef = Arc::new(StringArray::from(vec![value]));
    let batch = RecordBatch::try_new(schema.clone(), vec![arr])
        .map_err(|e| BasinError::internal(format!("SHOW search_path batch: {e}")))?;
    Ok(ExecResult::Rows {
        schema,
        batches: vec![batch],
    })
}

// ─── SET search_path ─────────────────────────────────────────────────────────

/// Handle `SET search_path = schema1, schema2, …` (and the `TO` variant).
///
/// The values are parsed by sqlparser as `Expr::Identifier` or
/// `Expr::Value(Value::SingleQuotedString(...))`. We accept both forms
/// and store the lowercase list on the session's `SchemaState`.
pub(crate) fn exec_set_search_path(
    sess: &ProjectSession,
    values: &[sqlparser::ast::Expr],
) -> Result<ExecResult> {
    use sqlparser::ast::{Expr, Value};

    let mut path: Vec<String> = Vec::with_capacity(values.len());
    for v in values {
        let name = match v {
            Expr::Identifier(id) => id.value.to_ascii_lowercase(),
            Expr::Value(Value::SingleQuotedString(s)) => s.to_ascii_lowercase(),
            Expr::Value(Value::DoubleQuotedString(s)) => s.to_ascii_lowercase(),
            other => {
                return Err(BasinError::InvalidSchema(format!(
                    "SET search_path: unsupported value {other}"
                )));
            }
        };
        path.push(name);
    }

    if path.is_empty() {
        return Err(BasinError::InvalidSchema(
            "SET search_path requires at least one schema name".to_string(),
        ));
    }

    sess.state
        .schema_state
        .write()
        .expect("schema_state lock poisoned")
        .search_path = path;

    Ok(ExecResult::Empty {
        tag: "SET".into(),
    })
}

// ─── Schema-qualifier stripping (for DataFusion) ─────────────────────────────

/// Rewrite `schema.table` two-part names in a SQL string to bare `table`
/// names before handing the SQL to DataFusion. DataFusion uses its own
/// catalog/schema namespace (`datafusion.<schema>.<table>`) which is
/// distinct from Basin's per-project flat namespace. This rewrite lets
/// clients write `SELECT ... FROM myschema.items` and have DataFusion see
/// `SELECT ... FROM items`, which *is* registered in the session context.
///
/// Strategy: for each known schema name (from the session's `SchemaState`),
/// replace all word-boundary occurrences of `<schema>.` at identifier start
/// positions. This is safe because:
/// - We only strip *known* schema names (validated at CREATE SCHEMA time).
/// - Schema names are lowercase identifiers (no special regex chars).
/// - The replacement is `\b<schema>\.` at word boundary, so `public.items`
///   → `items` but `mypublic.items` is not touched.
///
/// Returns `sql` unchanged when no known schema qualifier is present
/// (common fast path).
pub(crate) fn strip_schema_qualifiers_for_session(
    sql: &str,
    schema_state: &Arc<RwLock<SchemaState>>,
) -> String {
    // Quick short-circuit: if there's no '.' in the SQL, no schema qualifier.
    if !sql.contains('.') {
        return sql.to_string();
    }

    let st = schema_state.read().expect("schema_state lock poisoned");
    let schemas: Vec<String> = st.schemas.iter().cloned().collect();
    drop(st);

    let mut result = sql.to_string();
    for schema in &schemas {
        result = replace_schema_prefix(&result, schema);
    }
    result
}

/// Replace occurrences of `<schema>.` in `sql` with empty string, but only
/// when the `<schema>` token starts at a position that is not part of a
/// longer identifier (i.e., preceded by whitespace, `(`, `,`, or start-of-string).
///
/// The needle is always ASCII (schema names are lowercase identifiers), so
/// byte-level indexing is safe for the needle comparison. We iterate char
/// by char for correct UTF-8 handling of the surrounding SQL text.
fn replace_schema_prefix(sql: &str, schema: &str) -> String {
    // needle is always lowercase ASCII (schema names are validated identifiers).
    let needle_lc = format!("{schema}.");
    // Build a needle that we can compare case-insensitively. Since schema is
    // ASCII-only, we can use eq_ignore_ascii_case on byte slices.
    let nlen = needle_lc.len(); // byte length == char length (ASCII only)

    let sql_bytes = sql.as_bytes();
    let len = sql_bytes.len();

    let mut out = String::with_capacity(sql.len());
    let mut pos = 0usize; // byte position in `sql`

    while pos < len {
        // Try a match at `pos`. The needle is pure ASCII so byte offsets are
        // char-aligned for the needle itself. We still must advance by full
        // UTF-8 characters when no match.
        if pos + nlen <= len {
            // Only consider the match if the current position is at a char
            // boundary (always true since we advance char by char below).
            let candidate = &sql_bytes[pos..pos + nlen];
            // Case-insensitive ASCII comparison.
            let matches = candidate.eq_ignore_ascii_case(needle_lc.as_bytes());
            if matches {
                // Validate: preceding byte must not be an identifier char.
                let prev_ok = if pos == 0 {
                    true
                } else {
                    let prev = sql_bytes[pos - 1];
                    // ASCII identifier chars: [a-zA-Z0-9_]
                    !prev.is_ascii_alphanumeric() && prev != b'_'
                };
                if prev_ok {
                    // Skip the schema prefix — advance past the needle.
                    pos += nlen;
                    continue;
                }
            }
        }
        // Advance one char (UTF-8 safe).
        let ch = sql[pos..].chars().next().unwrap();
        out.push(ch);
        pos += ch.len_utf8();
    }
    out
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Extract the schema name string from a `SchemaName`. `AUTHORIZATION
/// alice` (unnamed) uses `"alice"` as a conventional schema name (per PG
/// docs). Named+authorization → uses the name part.
fn schema_name_str(sn: &SchemaName) -> Result<String> {
    match sn {
        SchemaName::Simple(name) => {
            if name.0.len() != 1 {
                return Err(BasinError::InvalidIdent(format!(
                    "expected a simple schema name, got: {name}"
                )));
            }
            Ok(name.0[0].value.clone())
        }
        SchemaName::UnnamedAuthorization(auth) => {
            // CREATE SCHEMA AUTHORIZATION alice → schema name is "alice"
            Ok(auth.value.clone())
        }
        SchemaName::NamedAuthorization(name, _auth) => {
            if name.0.len() != 1 {
                return Err(BasinError::InvalidIdent(format!(
                    "expected a simple schema name, got: {name}"
                )));
            }
            Ok(name.0[0].value.clone())
        }
    }
}
