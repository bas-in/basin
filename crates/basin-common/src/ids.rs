//! Newtypes for the IDs that flow through every layer.
//!
//! These exist so the type system tells us when a query is mis-routed (e.g.
//! a [`ProjectId`] used where a [`TableName`] is expected) instead of silently
//! crossing a project boundary. Every public constructor validates.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::error::{BasinError, Result};

/// Maximum length of a SQL identifier we accept (table, column, partition).
/// Postgres caps at 63; we mirror that so user data ports cleanly.
pub const MAX_IDENT_LEN: usize = 63;

/// Opaque project identifier.
///
/// Internally a [`Ulid`] so it sorts by creation time. Display form is the
/// 26-char Crockford base-32 ULID. Never log raw bytes — always Display.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ProjectId(Ulid);

impl ProjectId {
    pub fn new() -> Self {
        Self(Ulid::new())
    }

    pub fn from_ulid(u: Ulid) -> Self {
        Self(u)
    }

    pub fn as_ulid(&self) -> Ulid {
        self.0
    }

    /// Bucket-prefix-safe string form. This is what goes into S3 keys.
    pub fn as_prefix(&self) -> String {
        self.0.to_string()
    }
}

impl fmt::Display for ProjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for ProjectId {
    type Err = BasinError;
    fn from_str(s: &str) -> Result<Self> {
        Ulid::from_string(s)
            .map(Self)
            .map_err(|e| BasinError::InvalidIdent(format!("project_id: {e}")))
    }
}

impl Default for ProjectId {
    fn default() -> Self {
        Self::new()
    }
}

/// SQL identifier (table name, column name, etc.).
///
/// Validated to ASCII `[A-Za-z_][A-Za-z0-9_]*`, length ≤ 63. We do not yet
/// support quoted identifiers with arbitrary characters; if a customer asks
/// for them, revisit then.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Ident(String);

impl Ident {
    pub fn new(s: impl Into<String>) -> Result<Self> {
        let s: String = s.into();
        validate_ident(&s)?;
        Ok(Self(s))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Ident {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for Ident {
    type Err = BasinError;
    fn from_str(s: &str) -> Result<Self> {
        Self::new(s)
    }
}

/// Schema name. Distinct from [`TableName`] so the type system prevents
/// accidentally using one where the other is expected.
///
/// Postgres schema names follow the same identifier rules as table names:
/// `[A-Za-z_][A-Za-z0-9_]*`, max 63 characters, case-preserved (PG
/// case-folds *unquoted* identifiers to lowercase at parse time, but by the
/// time a name reaches this layer it has already been resolved; we store
/// exactly what was provided). Use [`SchemaName::public`] for the PG default
/// schema rather than hard-coding the string `"public"` at call sites.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SchemaName(Ident);

impl SchemaName {
    /// Construct a validated schema name.
    ///
    /// Returns an error if `name` violates PG identifier rules (empty,
    /// too long, or contains characters outside `[A-Za-z0-9_]`).
    pub fn new(name: impl Into<String>) -> Result<Self> {
        Ident::new(name).map(Self)
    }

    /// Returns the canonical PG default schema name `"public"`.
    ///
    /// Use this everywhere a schema is required but the caller has not
    /// specified one explicitly, to avoid scattering the literal string
    /// `"public"` across the codebase.
    pub fn public() -> Self {
        // "public" is a valid identifier, so this unwrap is infallible.
        Self(Ident::new("public").expect("'public' is always a valid identifier"))
    }

    /// Borrow the schema name as a string slice.
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Display for SchemaName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl AsRef<str> for SchemaName {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl FromStr for SchemaName {
    type Err = BasinError;
    fn from_str(s: &str) -> Result<Self> {
        Self::new(s)
    }
}

/// Table name. Distinct type from [`Ident`] so we can't accidentally pass a
/// column where a table is expected.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TableName(Ident);

impl TableName {
    pub fn new(s: impl Into<String>) -> Result<Self> {
        Ident::new(s).map(Self)
    }
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Display for TableName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for TableName {
    type Err = BasinError;
    fn from_str(s: &str) -> Result<Self> {
        Self::new(s)
    }
}

/// A fully-qualified table reference in PG canonical form: `<schema>.<table>`.
///
/// Wherever code previously accepted a bare [`TableName`] and implicitly
/// assumed the `public` schema, it should migrate to [`QualifiedTableName`]
/// so multi-schema isolation is enforced by the type system rather than by
/// convention. Use [`QualifiedTableName::in_public`] as a drop-in for the
/// single-schema legacy paths.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct QualifiedTableName {
    pub schema: SchemaName,
    pub name: TableName,
}

impl QualifiedTableName {
    /// Construct a qualified table name from explicit schema and table parts.
    pub fn new(schema: SchemaName, name: TableName) -> Self {
        Self { schema, name }
    }

    /// Convenience constructor that places `name` in the PG default `public`
    /// schema. Use this to migrate existing single-schema call sites without
    /// changing their semantics.
    pub fn in_public(name: TableName) -> Self {
        Self::new(SchemaName::public(), name)
    }

    /// Destructure into `(&schema, &name)` references for pattern-matching
    /// without moving out of the struct.
    pub fn as_pair(&self) -> (&SchemaName, &TableName) {
        (&self.schema, &self.name)
    }
}

impl fmt::Display for QualifiedTableName {
    /// Renders in PG canonical form: `schema.table` (e.g. `public.users`).
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.schema, self.name)
    }
}

// Manual Ord/PartialOrd: compare schema first, then name (lexicographic on
// the underlying identifier strings).
impl PartialOrd for QualifiedTableName {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for QualifiedTableName {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.schema
            .cmp(&other.schema)
            .then_with(|| self.name.cmp(&other.name))
    }
}

/// Logical partition key for a project's table.
///
/// Used to route writes/reads to a single shard owner. Bounded to keep object
/// keys reasonable. Empty is allowed and means "the default partition" for
/// small tables that don't need partitioning yet.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PartitionKey(String);

impl PartitionKey {
    pub const MAX_LEN: usize = 256;
    pub const DEFAULT: &'static str = "_default";

    pub fn new(s: impl Into<String>) -> Result<Self> {
        let s: String = s.into();
        if s.len() > Self::MAX_LEN {
            return Err(BasinError::InvalidIdent(format!(
                "partition key {} bytes (max {})",
                s.len(),
                Self::MAX_LEN
            )));
        }
        // `/` is allowed as a path-segment separator so a Hive-style key like
        // `year=2026/month=04` round-trips through the storage layout. We
        // still reject path-traversal segments and empty segments so the key
        // can never escape the project prefix or produce malformed paths.
        if s.starts_with('/') || s.ends_with('/') {
            return Err(BasinError::InvalidIdent(
                "partition key may not start or end with '/'".into(),
            ));
        }
        for seg in s.split('/') {
            if seg.is_empty() {
                return Err(BasinError::InvalidIdent(
                    "partition key has an empty segment".into(),
                ));
            }
            if seg == "." || seg == ".." {
                return Err(BasinError::InvalidIdent(
                    "partition key segment may not be '.' or '..'".into(),
                ));
            }
        }
        Ok(Self(s))
    }

    pub fn default_key() -> Self {
        Self(Self::DEFAULT.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PartitionKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for PartitionKey {
    type Err = BasinError;
    fn from_str(s: &str) -> Result<Self> {
        Self::new(s)
    }
}

fn validate_ident(s: &str) -> Result<()> {
    if s.is_empty() {
        return Err(BasinError::InvalidIdent("identifier is empty".into()));
    }
    if s.len() > MAX_IDENT_LEN {
        return Err(BasinError::InvalidIdent(format!(
            "identifier {} bytes (max {MAX_IDENT_LEN})",
            s.len()
        )));
    }
    let mut chars = s.chars();
    let first = chars.next().unwrap();
    if !(first.is_ascii_alphabetic() || first == '_') {
        return Err(BasinError::InvalidIdent(format!(
            "identifier must start with [A-Za-z_]: {s:?}"
        )));
    }
    for c in chars {
        if !(c.is_ascii_alphanumeric() || c == '_') {
            return Err(BasinError::InvalidIdent(format!(
                "identifier contains invalid char {c:?}: {s:?}"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod schema_name_tests {
    use super::*;

    #[test]
    fn public_returns_public_string() {
        assert_eq!(SchemaName::public().as_str(), "public");
    }

    #[test]
    fn display_matches_inner_string() {
        let s = SchemaName::new("analytics").unwrap();
        assert_eq!(s.to_string(), "analytics");
    }

    #[test]
    fn as_ref_str() {
        let s = SchemaName::new("reporting").unwrap();
        let r: &str = s.as_ref();
        assert_eq!(r, "reporting");
    }

    #[test]
    fn serde_json_roundtrip() {
        let original = SchemaName::new("my_schema").unwrap();
        let json = serde_json::to_string(&original).unwrap();
        // Transparent serde: stored as a plain JSON string.
        assert_eq!(json, r#""my_schema""#);
        let decoded: SchemaName = serde_json::from_str(&json).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn from_str_roundtrip() {
        let s: SchemaName = "events".parse().unwrap();
        assert_eq!(s.as_str(), "events");
    }

    #[test]
    fn rejects_invalid_identifier() {
        assert!(SchemaName::new("").is_err());
        assert!(SchemaName::new("1leading").is_err());
        assert!(SchemaName::new("has-hyphen").is_err());
        assert!(SchemaName::new("has.dot").is_err());
    }

    #[test]
    fn equality_and_hash_reflexive() {
        use std::collections::HashSet;
        let a = SchemaName::new("alpha").unwrap();
        let b = SchemaName::new("alpha").unwrap();
        assert_eq!(a, b);
        let mut set = HashSet::new();
        set.insert(a.clone());
        assert!(set.contains(&b));
    }

    #[test]
    fn ordering_lexicographic() {
        let a = SchemaName::new("alpha").unwrap();
        let b = SchemaName::new("beta").unwrap();
        assert!(a < b);
        assert!(b > a);
    }
}

#[cfg(test)]
mod qualified_table_name_tests {
    use super::*;

    #[test]
    fn new_and_display() {
        let schema = SchemaName::new("audit").unwrap();
        let table = TableName::new("events").unwrap();
        let q = QualifiedTableName::new(schema, table);
        assert_eq!(q.to_string(), "audit.events");
    }

    #[test]
    fn in_public_uses_public_schema() {
        let table = TableName::new("users").unwrap();
        let q = QualifiedTableName::in_public(table);
        assert_eq!(q.schema.as_str(), "public");
        assert_eq!(q.name.as_str(), "users");
        assert_eq!(q.to_string(), "public.users");
    }

    #[test]
    fn as_pair_destructures_correctly() {
        let schema = SchemaName::new("core").unwrap();
        let table = TableName::new("orders").unwrap();
        let q = QualifiedTableName::new(schema.clone(), table.clone());
        let (s, t) = q.as_pair();
        assert_eq!(s, &schema);
        assert_eq!(t, &table);
    }

    #[test]
    fn equality_reflexive() {
        let q1 = QualifiedTableName::in_public(TableName::new("items").unwrap());
        let q2 = QualifiedTableName::in_public(TableName::new("items").unwrap());
        assert_eq!(q1, q2);
    }

    #[test]
    fn hash_consistent_with_eq() {
        use std::collections::HashSet;
        let q1 = QualifiedTableName::in_public(TableName::new("foo").unwrap());
        let q2 = QualifiedTableName::in_public(TableName::new("foo").unwrap());
        let mut set = HashSet::new();
        set.insert(q1);
        assert!(set.contains(&q2));
    }

    #[test]
    fn ordering_schema_then_name() {
        let a = QualifiedTableName::new(
            SchemaName::new("alpha").unwrap(),
            TableName::new("z").unwrap(),
        );
        let b = QualifiedTableName::new(
            SchemaName::new("beta").unwrap(),
            TableName::new("a").unwrap(),
        );
        // alpha < beta even though "z" > "a" — schema is primary sort key.
        assert!(a < b);

        let c = QualifiedTableName::new(
            SchemaName::new("alpha").unwrap(),
            TableName::new("a").unwrap(),
        );
        let d = QualifiedTableName::new(
            SchemaName::new("alpha").unwrap(),
            TableName::new("z").unwrap(),
        );
        // Same schema — sort by name.
        assert!(c < d);
    }

    #[test]
    fn serde_json_roundtrip() {
        let q = QualifiedTableName::new(
            SchemaName::new("reporting").unwrap(),
            TableName::new("summary").unwrap(),
        );
        let json = serde_json::to_string(&q).unwrap();
        let decoded: QualifiedTableName = serde_json::from_str(&json).unwrap();
        assert_eq!(q, decoded);
    }

    #[test]
    fn in_public_serde_roundtrip() {
        let q = QualifiedTableName::in_public(TableName::new("logs").unwrap());
        let json = serde_json::to_string(&q).unwrap();
        let decoded: QualifiedTableName = serde_json::from_str(&json).unwrap();
        assert_eq!(q, decoded);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn project_id_roundtrip() {
        let id = ProjectId::new();
        let s = id.to_string();
        let parsed: ProjectId = s.parse().unwrap();
        assert_eq!(id, parsed);
        assert_eq!(s.len(), 26);
    }

    #[test]
    fn ident_accepts_valid() {
        for ok in ["x", "users", "_internal", "T1", "snake_case_v2"] {
            Ident::new(ok).unwrap_or_else(|e| panic!("{ok}: {e}"));
        }
    }

    #[test]
    fn ident_rejects_invalid() {
        for bad in ["", "1leading", "a-b", "a.b", "a/b", "with space"] {
            assert!(Ident::new(bad).is_err(), "{bad} should be rejected");
        }
        let too_long = "a".repeat(MAX_IDENT_LEN + 1);
        assert!(Ident::new(too_long).is_err());
    }

    #[test]
    fn partition_key_accepts_hive_style_segments() {
        // Hive-style structured keys are accepted so the storage layer can
        // map `(year, month)` into a directory chain.
        PartitionKey::new("year=2026/month=04").unwrap();
        PartitionKey::new("region:us-east-1").unwrap();
        PartitionKey::default_key();
    }

    #[test]
    fn partition_key_rejects_path_traversal_and_empty_segments() {
        assert!(PartitionKey::new("/a").is_err());
        assert!(PartitionKey::new("a/").is_err());
        assert!(PartitionKey::new("a//b").is_err());
        assert!(PartitionKey::new("a/../b").is_err());
        assert!(PartitionKey::new("a/./b").is_err());
    }

    #[test]
    fn partition_key_length() {
        let limit = "x".repeat(PartitionKey::MAX_LEN);
        PartitionKey::new(&limit).unwrap();
        let over = "x".repeat(PartitionKey::MAX_LEN + 1);
        assert!(PartitionKey::new(over).is_err());
    }
}
