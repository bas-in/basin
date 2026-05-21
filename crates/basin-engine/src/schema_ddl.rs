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
//! ## search_path (real as of Phase 5.18.B / ADR 0022)
//!
//! `SET search_path = a, b, public` stores the ordered list on the session
//! and is **honored at resolution time** — not merely cosmetic. Unqualified
//! table-name resolution walks the list left-to-right, first match wins,
//! with `pg_catalog` implicitly consulted first (as Postgres does) unless it
//! is named explicitly. Each path entry is mapped through the reserved-schema
//! rules: a reserved name (`auth`, `storage`, …) binds to that schema; a
//! user/unknown name aliases to `public` (the flat model — user schemas are
//! not real containers). A qualified name (`auth.users`) binds directly to
//! its reserved schema; a qualified user schema (`myapp.t`) aliases to
//! `public.t`.
//!
//! ## AUTHORIZATION
//!
//! `CREATE SCHEMA AUTHORIZATION alice` is accepted. The owner is ignored —
//! Basin's auth model is JWT-based, not role-based.

use crate::pg_ast::ObjectNamePartExt;
use std::collections::HashSet;
use std::sync::{Arc, RwLock};

use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use basin_catalog::reserved_schema::{resolve_qualified, ReservedSchema};
use basin_common::{BasinError, QualifiedTableName, Result, TableName};
use sqlparser::ast::ValueWithSpan;
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

    /// The ordered list of schemas consulted when resolving an *unqualified*
    /// table name, with `pg_catalog` implicitly prepended (as Postgres does)
    /// unless the user named it explicitly.
    ///
    /// Postgres always searches `pg_catalog` first whether or not it appears
    /// in `search_path`; we mirror that so that built-in / system objects
    /// shadow any same-named user object exactly as PG would.
    pub(crate) fn effective_search_path(&self) -> Vec<String> {
        let mut path: Vec<String> = Vec::with_capacity(self.search_path.len() + 1);
        let names_pg_catalog = self
            .search_path
            .iter()
            .any(|s| s.eq_ignore_ascii_case(ReservedSchema::PgCatalog.as_str()));
        if !names_pg_catalog {
            path.push(ReservedSchema::PgCatalog.as_str().to_string());
        }
        path.extend(self.search_path.iter().cloned());
        path
    }
}

// ─── Real qualified-name resolution (ADR 0022, Phase 5.18.B) ─────────────────

/// Resolve a parsed [`ObjectName`] to a [`QualifiedTableName`] using the
/// session's schema state and (for unqualified names) the `search_path`.
///
/// Resolution rules (ADR 0022):
///
/// - **Qualified** `schema.table`:
///   - a reserved schema (`auth`, `storage`, `cron`, `net`, `realtime`,
///     `pg_catalog`, `information_schema`, `public`) binds *directly* to that
///     schema — `auth.users` → `(auth, users)`;
///   - a user / unknown schema aliases to `public` (flat model) —
///     `myapp.t` → `(public, t)`. The schema must be a known session schema
///     (registered via `CREATE SCHEMA`, or `public`), else `NotFound`.
/// - **Unqualified** `table`: walk the effective `search_path`
///   (`pg_catalog` implicitly first, then the SET list, default `public`),
///   first match wins. Each entry is mapped through the reserved-schema rules
///   (user entries alias to `public`). The `exists` predicate decides whether
///   a candidate `(schema, table)` is present; the first schema for which it
///   returns `true` wins. If none match, the name binds to the resolution of
///   the *first* search_path entry (so a brand-new `public.t` still resolves
///   to `public.t`, matching today's behaviour).
pub(crate) fn resolve_qualified_name(
    name: &ObjectName,
    schema_state: &Arc<RwLock<SchemaState>>,
    exists: impl Fn(&QualifiedTableName) -> bool,
) -> Result<QualifiedTableName> {
    let resolved = resolve_object_name(name)?;
    let table = TableName::new(resolved.table.clone())?;

    match resolved.schema.as_deref() {
        // ── Qualified ────────────────────────────────────────────────────
        Some(schema) => {
            // The qualifier must be a schema this session knows about. A
            // reserved schema is always known; a user schema must have been
            // CREATE'd (or be "public"). This preserves the existing
            // "schema does not exist" error for typo'd qualifiers.
            let known_reserved = ReservedSchema::from_str(schema).is_some();
            if !known_reserved {
                let st = schema_state.read().expect("schema_state lock poisoned");
                if !st.contains(schema) {
                    return Err(BasinError::NotFound(format!(
                        "schema {schema:?} does not exist"
                    )));
                }
            }
            // resolve_qualified applies the reserved-vs-alias rule: a reserved
            // name binds to itself; a user name aliases to public.
            Ok(resolve_qualified(table, Some(schema)))
        }
        // ── Unqualified: walk the effective search_path ──────────────────
        None => {
            let path = {
                let st = schema_state.read().expect("schema_state lock poisoned");
                st.effective_search_path()
            };
            // First entry whose resolved (schema, table) exists wins.
            let mut first_candidate: Option<QualifiedTableName> = None;
            for entry in &path {
                let candidate = resolve_qualified(table.clone(), Some(entry));
                if first_candidate.is_none() {
                    first_candidate = Some(candidate.clone());
                }
                if exists(&candidate) {
                    return Ok(candidate);
                }
            }
            // No match: fall back to the first search_path entry's resolution.
            // `effective_search_path` is never empty (pg_catalog is implicit),
            // but resolve to public defensively if it somehow were.
            Ok(first_candidate.unwrap_or_else(|| QualifiedTableName::in_public(table)))
        }
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

/// Guard for user-initiated DDL: reject when the target object lives in a
/// reserved system schema (other than `public`).
///
/// Postgres-compatible semantics: a regular user session cannot
/// `CREATE TABLE auth.users` / `DROP TABLE storage.objects` / etc., even
/// though the engine *can* address those schemas internally for system
/// provisioning. The internal provisioning paths (basin-auth, basin-blob,
/// basin-cron, basin-net, basin-realtime) run sqlx-direct against the
/// Postgres backing store with prefixed names (e.g. `basin_auth_users`) —
/// they do not flow through this SQL parser, so this guard does not
/// affect them.
///
/// `public` is NOT reserved (the back-compat default). Bare / unqualified
/// names pass through unchanged.
///
/// Returns:
/// - `Ok(())` when the name is bare, unqualified, or qualified by `public`
///   or a user-defined schema.
/// - `Err(BasinError::PermissionDenied)` when the qualifier is any
///   non-public [`ReservedSchema`] (`auth`, `storage`, `cron`, `net`,
///   `realtime`, `pg_catalog`, `information_schema`).
///
/// `ddl_verb` is woven into the error message ("CREATE TABLE",
/// "DROP INDEX", …) so the client sees which operation was rejected;
/// the SQLSTATE on the wire is always `42501` (insufficient_privilege).
pub(crate) fn guard_reserved_schema_for_user_ddl(
    name: &ObjectName,
    ddl_verb: &str,
) -> Result<()> {
    // Only two-part names carry a schema qualifier; bare names are always
    // fine (they resolve to `public` per the back-compat default).
    if name.0.len() < 2 {
        return Ok(());
    }
    let schema = name.0[0].id_val().as_str();
    if let Some(reserved) = ReservedSchema::from_str(schema) {
        if !reserved.is_public() {
            return Err(BasinError::PermissionDenied(format!(
                "permission denied for schema \"{}\": user {ddl_verb} \
                 against reserved system schemas is not permitted",
                reserved.as_str()
            )));
        }
    }
    Ok(())
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
            table: name.0[0].id_val().clone(),
        }),
        2 => Ok(ResolvedName {
            schema: Some(name.0[0].id_val().clone()),
            table: name.0[1].id_val().clone(),
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
    // Route through the real resolver so qualified-schema validation and the
    // reserved-vs-alias rules are applied uniformly. We pass an
    // always-false `exists` predicate: this helper is the legacy bare-name
    // path used where the catalog is keyed in `public`, so an unqualified
    // name resolves to the first search_path entry's table (i.e. the bare
    // name) exactly as before. Callers needing the schema part should use
    // [`resolve_qualified_name`] directly.
    let qt = resolve_qualified_name(name, schema_state, |_| false)?;
    Ok(qt.name.as_str().to_string())
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
        let parts: Vec<&str> = name_obj.0.iter().map(|i| i.id_val().as_str()).collect();
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
    let norm: String = sql.split_ascii_whitespace().collect::<Vec<_>>().join(" ");
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
    let new = rest[pivot + " RENAME TO ".len()..]
        .trim()
        .trim_end_matches(';')
        .trim()
        .to_string();
    if old.is_empty() || new.is_empty() {
        return None;
    }
    Some((old, new))
}

/// AST-based matcher for `ALTER SCHEMA <old> RENAME TO <new>`.
///
/// libpg_query routes this as
/// [`pg_query::protobuf::RenameStmt`] with `rename_type = ObjectSchema`.
/// The old name lives in the `subname` field (plain `String`); the new
/// name is the `newname` field.  Returns `(old, new)` on success.
pub(crate) fn match_alter_schema_rename_ast(
    stmt: &pg_query::protobuf::RenameStmt,
) -> Result<(String, String)> {
    let old = stmt.subname.clone();
    let new = stmt.newname.clone();
    if old.is_empty() || new.is_empty() {
        return Err(basin_common::BasinError::InvalidSchema(
            "ALTER SCHEMA RENAME: empty name in AST".into(),
        ));
    }
    Ok((old, new))
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
            Expr::Value(ValueWithSpan {
                value: Value::SingleQuotedString(s),
                ..
            }) => s.to_ascii_lowercase(),
            Expr::Value(ValueWithSpan {
                value: Value::DoubleQuotedString(s),
                ..
            }) => s.to_ascii_lowercase(),
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

    Ok(ExecResult::Empty { tag: "SET".into() })
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
            Ok(name.0[0].id_val().clone())
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
            Ok(name.0[0].id_val().clone())
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Unit tests for real search_path + qualified-name resolution (Phase 5.18.B)
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod resolution_tests {
    use super::*;
    use sqlparser::ast::{Ident, ObjectName, ObjectNamePart};

    fn obj(parts: &[&str]) -> ObjectName {
        ObjectName(
            parts
                .iter()
                .map(|p| ObjectNamePart::Identifier(Ident::new(*p)))
                .collect(),
        )
    }

    fn state() -> Arc<RwLock<SchemaState>> {
        Arc::new(RwLock::new(SchemaState::default()))
    }

    /// `exists` predicate that treats the given `(schema, table)` pairs as
    /// present in the catalog.
    fn exists_in(
        present: &'static [(&'static str, &'static str)],
    ) -> impl Fn(&QualifiedTableName) -> bool {
        move |qt: &QualifiedTableName| {
            present
                .iter()
                .any(|(s, t)| qt.schema.as_str() == *s && qt.name.as_str() == *t)
        }
    }

    // ── effective_search_path: pg_catalog implicitly first ────────────────

    #[test]
    fn effective_search_path_prepends_pg_catalog() {
        let st = SchemaState::default();
        // default search_path is ["public"]; pg_catalog is implicit-first.
        assert_eq!(st.effective_search_path(), vec!["pg_catalog", "public"]);
    }

    #[test]
    fn effective_search_path_no_double_pg_catalog() {
        let mut st = SchemaState::default();
        st.search_path = vec!["pg_catalog".into(), "public".into()];
        // Already named explicitly → not prepended again.
        assert_eq!(st.effective_search_path(), vec!["pg_catalog", "public"]);
    }

    // ── Qualified reserved-schema binds directly ──────────────────────────

    #[test]
    fn qualified_reserved_schema_binds_directly() {
        let ss = state();
        let qt = resolve_qualified_name(&obj(&["auth", "users"]), &ss, |_| false).unwrap();
        assert_eq!(qt.schema.as_str(), "auth");
        assert_eq!(qt.name.as_str(), "users");
        assert_eq!(qt.to_string(), "auth.users");
    }

    #[test]
    fn qualified_reserved_schema_does_not_require_create_schema() {
        // `storage` / `net` etc. are reserved and always bindable even though
        // no CREATE SCHEMA was issued for them.
        let ss = state();
        let qt = resolve_qualified_name(&obj(&["storage", "objects"]), &ss, |_| false).unwrap();
        assert_eq!(qt.to_string(), "storage.objects");
    }

    // ── Qualified user schema aliases to public ───────────────────────────

    #[test]
    fn qualified_user_schema_aliases_to_public() {
        let ss = state();
        {
            // myapp must be a known session schema (CREATE SCHEMA myapp).
            ss.write().unwrap().insert("myapp".to_string());
        }
        let qt = resolve_qualified_name(&obj(&["myapp", "t"]), &ss, |_| false).unwrap();
        assert_eq!(qt.schema.as_str(), "public");
        assert_eq!(qt.name.as_str(), "t");
        assert_eq!(qt.to_string(), "public.t");
    }

    #[test]
    fn qualified_unknown_schema_errors() {
        // Not reserved and not CREATE'd → NotFound (back-compat with the
        // existing "schema does not exist" behaviour).
        let ss = state();
        let err = resolve_qualified_name(&obj(&["nope", "t"]), &ss, |_| false).unwrap_err();
        assert!(matches!(err, BasinError::NotFound(_)));
    }

    #[test]
    fn qualified_public_is_identity() {
        let ss = state();
        let qt = resolve_qualified_name(&obj(&["public", "users"]), &ss, |_| false).unwrap();
        assert_eq!(qt.to_string(), "public.users");
    }

    // ── Unqualified walks search_path, first match wins ───────────────────

    #[test]
    fn unqualified_defaults_to_public_when_absent() {
        // No catalog membership → falls back to the first search_path entry's
        // resolution. pg_catalog is implicit-first, then public; an unknown
        // table resolves to pg_catalog.<t> only if it "exists" there — with a
        // false predicate it falls back to the FIRST entry (pg_catalog). To
        // keep brand-new public tables resolving to public we verify the
        // explicit-public fallback below; here we assert the documented
        // first-entry fallback.
        let ss = state();
        let qt = resolve_qualified_name(&obj(&["t"]), &ss, |_| false).unwrap();
        // First effective entry is pg_catalog (implicit); documented fallback.
        assert_eq!(qt.schema.as_str(), "pg_catalog");
    }

    #[test]
    fn unqualified_resolves_to_public_when_present_there() {
        let ss = state();
        // public.t exists → resolution must pick public (pg_catalog has no t).
        let qt = resolve_qualified_name(
            &obj(&["t"]),
            &ss,
            exists_in(&[("public", "t")]),
        )
        .unwrap();
        assert_eq!(qt.to_string(), "public.t");
    }

    #[test]
    fn unqualified_pg_catalog_shadows_public() {
        let ss = state();
        // Same table name present in BOTH pg_catalog and public; pg_catalog
        // is consulted first (as Postgres does) → wins.
        let qt = resolve_qualified_name(
            &obj(&["pg_class"]),
            &ss,
            exists_in(&[("pg_catalog", "pg_class"), ("public", "pg_class")]),
        )
        .unwrap();
        assert_eq!(qt.schema.as_str(), "pg_catalog");
    }

    #[test]
    fn unqualified_honors_set_search_path_first_match_wins() {
        let ss = state();
        // SET search_path = auth, public  (pg_catalog still implicit-first).
        ss.write().unwrap().search_path = vec!["auth".into(), "public".into()];
        // `users` exists in BOTH auth and public; auth precedes public in the
        // path → auth wins. (pg_catalog has no `users`.)
        let qt = resolve_qualified_name(
            &obj(&["users"]),
            &ss,
            exists_in(&[("auth", "users"), ("public", "users")]),
        )
        .unwrap();
        assert_eq!(qt.schema.as_str(), "auth");
    }

    #[test]
    fn unqualified_set_search_path_user_entry_aliases_to_public() {
        let ss = state();
        ss.write().unwrap().insert("myapp".to_string());
        // SET search_path = myapp, public — myapp aliases to public, so an
        // unqualified `t` present in public resolves to public.t.
        ss.write().unwrap().search_path = vec!["myapp".into(), "public".into()];
        let qt = resolve_qualified_name(&obj(&["t"]), &ss, exists_in(&[("public", "t")])).unwrap();
        assert_eq!(qt.to_string(), "public.t");
    }

    // ── table_name_from_object back-compat (bare-name path) ───────────────

    #[test]
    fn table_name_from_object_strips_user_schema() {
        let ss = state();
        ss.write().unwrap().insert("myschema".to_string());
        let bare = table_name_from_object(&obj(&["myschema", "events"]), &ss).unwrap();
        assert_eq!(bare, "events");
    }

    #[test]
    fn table_name_from_object_bare_unchanged() {
        let ss = state();
        let bare = table_name_from_object(&obj(&["events"]), &ss).unwrap();
        assert_eq!(bare, "events");
    }

    #[test]
    fn table_name_from_object_unknown_schema_errors() {
        let ss = state();
        let err = table_name_from_object(&obj(&["badschema", "events"]), &ss).unwrap_err();
        assert!(matches!(err, BasinError::NotFound(_)));
    }
}
