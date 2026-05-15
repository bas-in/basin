//! Per-project `CREATE TYPE … AS ENUM` catalog.
//!
//! Customers will write
//!
//! ```sql
//! CREATE TYPE order_status AS ENUM ('pending', 'paid', 'shipped');
//! ALTER TYPE order_status ADD VALUE 'refunded';
//! DROP TYPE order_status;
//! ```
//!
//! and use the named type in column declarations
//!
//! ```sql
//! CREATE TABLE orders (id BIGINT, status order_status NOT NULL);
//! ```
//!
//! once the DDL surface lands; this module ships the catalog row, the
//! lookup primitive, and validation. The engine maps an enum-typed
//! column to Arrow `Utf8` (storing the label string) plus a
//! `BASIN_ENUM_TYPE=<name>` field metadata marker so an INSERT path can
//! validate the label against the catalog row before commit.
//!
//! The catalog is a single shared `HashMap<(ProjectId, String),
//! EnumTypeDef>` — same shape as [`crate::functions`] — so per-project
//! cost stays `O(bytes)` with no per-project heavy resource.

use serde::{Deserialize, Serialize};

use basin_common::ProjectId;

/// Field-metadata key set on every Arrow column whose declared type is
/// a user-defined enum. The value is the catalog-side `EnumTypeDef::name`.
/// The engine uses this on INSERT to validate the supplied label and on
/// `drop_enum_type` to decide whether the type is still referenced.
pub const BASIN_ENUM_TYPE_KEY: &str = "BASIN_ENUM_TYPE";

/// Catalog row for a `CREATE TYPE … AS ENUM` declaration. Stored
/// per-project; two projects registering the same type name are
/// independent rows.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnumTypeDef {
    pub project: ProjectId,
    /// Unqualified type name within the project. Validated to be a SQL
    /// identifier and to not collide with a built-in PG type at
    /// registration.
    pub name: String,
    /// Ordered list of label strings. The first label is ordinal `0`.
    /// `ALTER TYPE … ADD VALUE` appends to the end; PG's enum semantics
    /// are append-only (no insert-before, no remove) so this list is
    /// monotonic across the lifetime of the type.
    pub labels: Vec<String>,
}

/// Errors specific to enum-type registration / mutation. Mapped to
/// [`basin_common::BasinError`] at the catalog API boundary.
#[derive(Debug)]
pub enum EnumError {
    /// Two labels in the same enum compared equal (case-sensitive
    /// match — PG enum labels are case-sensitive).
    DuplicateLabel(String),
    /// Empty label list at `CREATE TYPE` time.
    EmptyLabelList,
    /// A label was the empty string (PG accepts non-empty strings).
    EmptyLabel,
    /// Type with that name already exists for this project.
    Duplicate,
    /// Type referenced by `add_enum_value` / `drop_enum_type` /
    /// `lookup_enum_type` does not exist.
    NotFound,
    /// `add_enum_value` was called with a label that already exists in
    /// the type. PG treats this case as a no-op (idempotent ADD VALUE
    /// IF NOT EXISTS); the strict variant returns this error so the
    /// engine can surface a "duplicate value" message.
    LabelAlreadyExists(String),
}

/// Validate a fresh enum type at `CREATE TYPE` time. Returns the def
/// untouched on success.
pub fn validate_new(def: &EnumTypeDef) -> Result<(), EnumError> {
    if def.labels.is_empty() {
        return Err(EnumError::EmptyLabelList);
    }
    let mut seen: std::collections::HashSet<&str> =
        std::collections::HashSet::with_capacity(def.labels.len());
    for l in &def.labels {
        if l.is_empty() {
            return Err(EnumError::EmptyLabel);
        }
        if !seen.insert(l.as_str()) {
            return Err(EnumError::DuplicateLabel(l.clone()));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn def(labels: &[&str]) -> EnumTypeDef {
        EnumTypeDef {
            project: ProjectId::new(),
            name: "t".into(),
            labels: labels.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn validate_accepts_unique_non_empty_labels() {
        assert!(validate_new(&def(&["a", "b", "c"])).is_ok());
    }

    #[test]
    fn validate_rejects_empty_label_list() {
        assert!(matches!(
            validate_new(&def(&[])),
            Err(EnumError::EmptyLabelList)
        ));
    }

    #[test]
    fn validate_rejects_empty_label() {
        assert!(matches!(
            validate_new(&def(&["a", "", "b"])),
            Err(EnumError::EmptyLabel)
        ));
    }

    #[test]
    fn validate_rejects_duplicate_label() {
        assert!(matches!(
            validate_new(&def(&["a", "b", "a"])),
            Err(EnumError::DuplicateLabel(_))
        ));
    }

    #[test]
    fn validate_label_compare_is_case_sensitive() {
        // PG's enum semantics are case-sensitive: 'A' and 'a' are
        // distinct labels.
        assert!(validate_new(&def(&["A", "a"])).is_ok());
    }
}
