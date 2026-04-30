//! Newtypes for the IDs that flow through every layer.
//!
//! These exist so the type system tells us when a query is mis-routed (e.g.
//! a [`TenantId`] used where a [`TableName`] is expected) instead of silently
//! crossing a tenant boundary. Every public constructor validates.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::error::{BasinError, Result};

/// Maximum length of a SQL identifier we accept (table, column, partition).
/// Postgres caps at 63; we mirror that so user data ports cleanly.
pub const MAX_IDENT_LEN: usize = 63;

/// Opaque tenant identifier.
///
/// Internally a [`Ulid`] so it sorts by creation time. Display form is the
/// 26-char Crockford base-32 ULID. Never log raw bytes — always Display.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TenantId(Ulid);

impl TenantId {
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

impl fmt::Display for TenantId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for TenantId {
    type Err = BasinError;
    fn from_str(s: &str) -> Result<Self> {
        Ulid::from_string(s)
            .map(Self)
            .map_err(|e| BasinError::InvalidIdent(format!("tenant_id: {e}")))
    }
}

impl Default for TenantId {
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

/// Logical partition key for a tenant's table.
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
        if s.contains('/') {
            return Err(BasinError::InvalidIdent(
                "partition key may not contain '/'".into(),
            ));
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
mod tests {
    use super::*;

    #[test]
    fn tenant_id_roundtrip() {
        let id = TenantId::new();
        let s = id.to_string();
        let parsed: TenantId = s.parse().unwrap();
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
    fn partition_key_rejects_slash() {
        assert!(PartitionKey::new("a/b").is_err());
        PartitionKey::new("region:us-east-1").unwrap();
        PartitionKey::default_key();
    }

    #[test]
    fn partition_key_length() {
        let limit = "x".repeat(PartitionKey::MAX_LEN);
        PartitionKey::new(&limit).unwrap();
        let over = "x".repeat(PartitionKey::MAX_LEN + 1);
        assert!(PartitionKey::new(over).is_err());
    }
}
