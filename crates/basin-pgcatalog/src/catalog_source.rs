//! What a catalog relation needs from the actual catalog.
//!
//! [`CatalogSource`] is deliberately narrow — the four questions `pg_class`,
//! `pg_namespace`, `pg_attribute`, `pg_index` and `pg_constraint` actually
//! ask (list tables, list a table's columns, list a table's indexes, list a
//! table's constraints), not a general query interface. A real
//! `basin-catalog`-backed implementation is a follow-up; [`crate::mock`]
//! implements this trait in memory for tests, and for the `pg_type` relation
//! (which needs no catalog at all — see [`crate::pg_type`]).

use crate::Oid;

/// One row of `pg_namespace`, minus the columns nothing here reports yet
/// (`nspacl`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceInfo {
    pub oid: Oid,
    pub name: String,
    pub owner: Oid,
}

/// `pg_class.relkind` — what kind of relation a `pg_class` row describes.
///
/// Postgres has more variants (`t` toast, `f` foreign table, `p` partitioned
/// table, `I` partitioned index); only the ones a first increment's tests
/// exercise are here. Extending this is additive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelKind {
    OrdinaryTable,
    Index,
    View,
    Sequence,
    MaterializedView,
}

impl RelKind {
    /// The single-character code Postgres stores in `pg_class.relkind`.
    pub fn as_char(self) -> char {
        match self {
            RelKind::OrdinaryTable => 'r',
            RelKind::Index => 'i',
            RelKind::View => 'v',
            RelKind::Sequence => 'S',
            RelKind::MaterializedView => 'm',
        }
    }
}

/// One row of `pg_class`, minus the many columns nothing here reports yet
/// (`reltype`, `relam`, storage stats, ACLs, ...).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableInfo {
    pub oid: Oid,
    pub name: String,
    pub namespace: Oid,
    pub owner: Oid,
    pub kind: RelKind,
}

/// One row of `pg_attribute`, minus `atttypmod`/dropped-column bookkeeping —
/// see doc 11 §5 on `atttypmod` being a real, tracked gap, not an oversight.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnInfo {
    pub table_oid: Oid,
    pub name: String,
    pub attnum: i16,
    pub type_oid: Oid,
    pub not_null: bool,
}

/// One row of `pg_index`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexInfo {
    pub oid: Oid,
    pub table_oid: Oid,
    pub name: String,
    pub is_unique: bool,
    pub is_primary: bool,
    /// `attnum`s of the indexed columns, in index key order.
    pub column_attnums: Vec<i16>,
}

/// `pg_constraint.contype`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConstraintKind {
    Check,
    PrimaryKey,
    ForeignKey,
    Unique,
}

impl ConstraintKind {
    /// The single-character code Postgres stores in `pg_constraint.contype`.
    pub fn as_char(self) -> char {
        match self {
            ConstraintKind::Check => 'c',
            ConstraintKind::PrimaryKey => 'p',
            ConstraintKind::ForeignKey => 'f',
            ConstraintKind::Unique => 'u',
        }
    }
}

/// One row of `pg_constraint`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConstraintInfo {
    pub oid: Oid,
    pub table_oid: Oid,
    pub name: String,
    pub kind: ConstraintKind,
}

/// What a [`crate::SystemView`] needs from the underlying catalog.
///
/// Every method returns owned data rather than borrowing, since the source
/// may be a live query against `basin-catalog` with no stable borrow to
/// return.
pub trait CatalogSource {
    /// All schemas in the current project's `search_path` scope.
    fn namespaces(&self) -> Vec<NamespaceInfo>;
    /// All relations (tables, indexes, views, ...) in the current project.
    fn tables(&self) -> Vec<TableInfo>;
    /// The columns of one table, in `attnum` order.
    fn columns(&self, table_oid: Oid) -> Vec<ColumnInfo>;
    /// The indexes defined on one table.
    fn indexes(&self, table_oid: Oid) -> Vec<IndexInfo>;
    /// The constraints defined on one table.
    fn constraints(&self, table_oid: Oid) -> Vec<ConstraintInfo>;
}
