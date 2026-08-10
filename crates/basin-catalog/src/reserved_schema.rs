//! Reserved-schema type and back-compat name resolution for Phase 5.18.A.
//!
//! # Design
//!
//! The catalog's notion of "schema" for 5.18.A is a **closed reserved set**.
//! User-defined / unknown schema names are NOT given real containers — they
//! alias to `public` (flat model per ADR 0022). Only the schemas listed in
//! [`ReservedSchema`] are first-class.
//!
//! Resolution rules (back-compat is the primary invariant):
//!
//! | Caller-supplied schema | Resolves to |
//! |---|---|
//! | `None` (unqualified table name) | `public` |
//! | `"public"` | `public` |
//! | any [`ReservedSchema`] variant | that reserved schema |
//! | any other string (user schema) | `public` (aliased, flat) |
//!
//! The alias-to-public rule means existing data and every existing test keep
//! passing unchanged: `users` and `public.users` and `myapp.users` all resolve
//! to the same catalog entry `(project, public, users)`.

use basin_common::{QualifiedTableName, SchemaName, TableName};

/// The closed, reserved set of system schemas.
///
/// `public` is the default schema; unqualified names resolve to it.
/// The remaining schemas match the system-namespace APIs Basin exposes
/// (auth, storage, cron, net, realtime, pg_catalog, information_schema).
///
/// This enum is **not user-extensible** in v1 — adding a new reserved
/// schema is a code change. User `CREATE SCHEMA` statements continue to
/// be accepted but are silently aliased to `public`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ReservedSchema {
    Public,
    Auth,
    Storage,
    Cron,
    Net,
    Realtime,
    PgCatalog,
    InformationSchema,
}

impl ReservedSchema {
    /// All variants in a stable order. Used to seed a project's schema
    /// list in `create_namespace` so every reserved schema is pre-registered.
    pub const ALL: &'static [ReservedSchema] = &[
        ReservedSchema::Public,
        ReservedSchema::Auth,
        ReservedSchema::Storage,
        ReservedSchema::Cron,
        ReservedSchema::Net,
        ReservedSchema::Realtime,
        ReservedSchema::PgCatalog,
        ReservedSchema::InformationSchema,
    ];

    /// The wire name used in SQL identifiers and catalog storage.
    pub const fn as_str(&self) -> &'static str {
        match self {
            ReservedSchema::Public => "public",
            ReservedSchema::Auth => "auth",
            ReservedSchema::Storage => "storage",
            ReservedSchema::Cron => "cron",
            ReservedSchema::Net => "net",
            ReservedSchema::Realtime => "realtime",
            ReservedSchema::PgCatalog => "pg_catalog",
            ReservedSchema::InformationSchema => "information_schema",
        }
    }

    /// Parse a string into a `ReservedSchema` variant.
    ///
    /// Returns `None` for any string that is not in the reserved set.
    /// The caller uses `None` to conclude "this is a user-defined schema
    /// → alias to `public`".
    pub fn from_str(s: &str) -> Option<ReservedSchema> {
        match s {
            "public" => Some(ReservedSchema::Public),
            "auth" => Some(ReservedSchema::Auth),
            "storage" => Some(ReservedSchema::Storage),
            "cron" => Some(ReservedSchema::Cron),
            "net" => Some(ReservedSchema::Net),
            "realtime" => Some(ReservedSchema::Realtime),
            "pg_catalog" => Some(ReservedSchema::PgCatalog),
            "information_schema" => Some(ReservedSchema::InformationSchema),
            _ => None,
        }
    }

    /// Convert to a [`SchemaName`]. Infallible because all reserved names
    /// are valid SQL identifiers.
    pub fn to_schema_name(self) -> SchemaName {
        // All reserved schema names are valid identifiers; the unwrap is
        // unreachable.
        SchemaName::new(self.as_str()).expect("reserved schema names are always valid identifiers")
    }

    /// True iff this is the `public` schema (the default).
    pub fn is_public(self) -> bool {
        self == ReservedSchema::Public
    }
}

impl std::fmt::Display for ReservedSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Back-compat resolution helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Resolve a schema name from an `Option<&str>` (e.g. from parsed SQL) into
/// a [`SchemaName`] that can be used as a catalog key.
///
/// Resolution rules:
///
/// - `None` (unqualified) → `"public"`
/// - `Some("public")` → `"public"` (identity)
/// - `Some` of any reserved schema name → that reserved schema's name
/// - `Some` of any unknown string → `"public"` (user-defined alias)
///
/// This is the single choke-point that enforces the ADR 0022 flat-model for
/// user-defined schemas. Every catalog call site that receives an optional
/// schema string should run it through this function before constructing a
/// [`QualifiedTableName`].
pub fn resolve_schema(schema: Option<&str>) -> SchemaName {
    match schema {
        // Unqualified → default to public (unchanged from today)
        None => SchemaName::public(),
        Some(s) => {
            match ReservedSchema::from_str(s) {
                // Known reserved schema → use its canonical name
                Some(reserved) => reserved.to_schema_name(),
                // Unknown / user-defined schema → alias to public
                None => SchemaName::public(),
            }
        }
    }
}

/// Convenience wrapper: build a [`QualifiedTableName`] from a bare
/// [`TableName`] and an optional schema string, applying the same
/// resolution rules as [`resolve_schema`].
///
/// This is the primary back-compat helper for call sites that parse
/// `[schema.]table` syntax and need to produce a catalog key:
///
/// ```text
/// // unqualified
/// let qt = resolve_qualified(table_name, None);   // → public.table_name
///
/// // public-qualified
/// let qt = resolve_qualified(table_name, Some("public")); // → public.table_name
///
/// // reserved schema
/// let qt = resolve_qualified(table_name, Some("auth"));   // → auth.table_name
///
/// // user-defined (unknown) → aliases to public
/// let qt = resolve_qualified(table_name, Some("myapp"));  // → public.table_name
/// ```
pub fn resolve_qualified(table: TableName, schema: Option<&str>) -> QualifiedTableName {
    QualifiedTableName::new(resolve_schema(schema), table)
}

// ─────────────────────────────────────────────────────────────────────────────
// Unit tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use basin_common::TableName;

    // ── ReservedSchema::from_str ──────────────────────────────────────────

    #[test]
    fn from_str_all_reserved_schemas() {
        assert_eq!(
            ReservedSchema::from_str("public"),
            Some(ReservedSchema::Public)
        );
        assert_eq!(ReservedSchema::from_str("auth"), Some(ReservedSchema::Auth));
        assert_eq!(
            ReservedSchema::from_str("storage"),
            Some(ReservedSchema::Storage)
        );
        assert_eq!(ReservedSchema::from_str("cron"), Some(ReservedSchema::Cron));
        assert_eq!(ReservedSchema::from_str("net"), Some(ReservedSchema::Net));
        assert_eq!(
            ReservedSchema::from_str("realtime"),
            Some(ReservedSchema::Realtime)
        );
        assert_eq!(
            ReservedSchema::from_str("pg_catalog"),
            Some(ReservedSchema::PgCatalog)
        );
        assert_eq!(
            ReservedSchema::from_str("information_schema"),
            Some(ReservedSchema::InformationSchema)
        );
    }

    #[test]
    fn from_str_user_defined_returns_none() {
        assert_eq!(ReservedSchema::from_str("myapp"), None);
        assert_eq!(ReservedSchema::from_str("billing"), None);
        assert_eq!(ReservedSchema::from_str(""), None);
        assert_eq!(ReservedSchema::from_str("PUBLIC"), None); // case-sensitive
    }

    #[test]
    fn all_slice_covers_all_variants() {
        // Verify that ALL contains exactly 8 unique entries.
        assert_eq!(ReservedSchema::ALL.len(), 8);
        let set: std::collections::HashSet<ReservedSchema> =
            ReservedSchema::ALL.iter().copied().collect();
        assert_eq!(set.len(), 8, "ALL must contain unique variants");
    }

    #[test]
    fn all_slice_parseable_back() {
        for &variant in ReservedSchema::ALL {
            assert_eq!(
                ReservedSchema::from_str(variant.as_str()),
                Some(variant),
                "from_str(as_str()) round-trip failed for {:?}",
                variant
            );
        }
    }

    #[test]
    fn to_schema_name_matches_as_str() {
        for &variant in ReservedSchema::ALL {
            assert_eq!(variant.to_schema_name().as_str(), variant.as_str());
        }
    }

    #[test]
    fn display_matches_as_str() {
        for &variant in ReservedSchema::ALL {
            assert_eq!(variant.to_string(), variant.as_str());
        }
    }

    #[test]
    fn public_is_public() {
        assert!(ReservedSchema::Public.is_public());
        assert!(!ReservedSchema::Auth.is_public());
    }

    // ── resolve_schema ────────────────────────────────────────────────────

    #[test]
    fn resolve_none_gives_public() {
        assert_eq!(resolve_schema(None).as_str(), "public");
    }

    #[test]
    fn resolve_public_string_gives_public() {
        assert_eq!(resolve_schema(Some("public")).as_str(), "public");
    }

    #[test]
    fn resolve_reserved_schemas() {
        assert_eq!(resolve_schema(Some("auth")).as_str(), "auth");
        assert_eq!(resolve_schema(Some("storage")).as_str(), "storage");
        assert_eq!(resolve_schema(Some("cron")).as_str(), "cron");
        assert_eq!(resolve_schema(Some("net")).as_str(), "net");
        assert_eq!(resolve_schema(Some("realtime")).as_str(), "realtime");
        assert_eq!(resolve_schema(Some("pg_catalog")).as_str(), "pg_catalog");
        assert_eq!(
            resolve_schema(Some("information_schema")).as_str(),
            "information_schema"
        );
    }

    #[test]
    fn resolve_user_defined_aliases_to_public() {
        assert_eq!(resolve_schema(Some("myapp")).as_str(), "public");
        assert_eq!(resolve_schema(Some("billing")).as_str(), "public");
        assert_eq!(resolve_schema(Some("project_42")).as_str(), "public");
    }

    // ── resolve_qualified ─────────────────────────────────────────────────

    #[test]
    fn resolve_qualified_unqualified_is_public_dot_table() {
        let t = TableName::new("users").unwrap();
        let qt = resolve_qualified(t.clone(), None);
        assert_eq!(qt.schema.as_str(), "public");
        assert_eq!(qt.name.as_str(), "users");
        assert_eq!(qt.to_string(), "public.users");
    }

    #[test]
    fn resolve_qualified_public_dot_table_identity() {
        let t = TableName::new("users").unwrap();
        let qt = resolve_qualified(t, Some("public"));
        assert_eq!(qt.to_string(), "public.users");
    }

    #[test]
    fn resolve_qualified_auth_dot_users_is_distinct_from_public_dot_users() {
        let t = TableName::new("users").unwrap();
        let auth_qt = resolve_qualified(t.clone(), Some("auth"));
        let public_qt = resolve_qualified(t, Some("public"));
        assert_ne!(auth_qt, public_qt);
        assert_eq!(auth_qt.to_string(), "auth.users");
        assert_eq!(public_qt.to_string(), "public.users");
    }

    #[test]
    fn resolve_qualified_user_schema_aliases_to_public() {
        let t = TableName::new("users").unwrap();
        let myapp_qt = resolve_qualified(t.clone(), Some("myapp"));
        let public_qt = resolve_qualified(t, None);
        // myapp.users → public.users (same key)
        assert_eq!(myapp_qt, public_qt);
    }
}
