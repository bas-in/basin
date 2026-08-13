//! `pg_catalog` as real relations.
//!
//! # Why this crate exists
//!
//! Basin already exposes roughly 65 `pg_catalog` / `information_schema`
//! relations in `crates/basin-engine/src/info_schema_provider.rs`, and they
//! are not stubs — the macro-generated ones call real queries against
//! `basin-catalog`. The problem is the *shape*: every relation is a
//! DataFusion `TableProvider`, and every `TableProvider::scan()` there
//! discards `_filters` and `_limit`, so `SELECT * FROM pg_attribute WHERE
//! attrelid = 16384` materializes every column of every table in the project
//! and throws almost all of it away afterward. See
//! `docs/migration/df-removal/11-pg-catalog-fidelity.md` §1.
//!
//! This crate is the replacement shape: [`SystemView::scan`] takes the
//! predicates the planner has already decided it can push, so `attrelid =
//! 16384` becomes a parameter of the catalog query rather than a filter
//! applied after it. It is deliberately a leaf crate — `basin-pgtype` plus
//! `arrow-array`/`arrow-schema` only, no DataFusion, no `basin-storage`, no
//! tokio — so a catalog relation can be described and tested without
//! reference to how or where it executes.
//!
//! # Status
//!
//! Sixteen relations, 228 of the 230 real columns those sixteen have.
//!
//! Five of them need no [`CatalogSource`] at all, because their content is
//! fixed builtin catalog data rather than anything about a particular
//! database: [`pg_type`], [`pg_am`], and [`pg_operator`]/[`pg_cast`]/
//! [`pg_proc`] (views over `basin-pgtype`'s own operator/cast/function
//! tables). Those three were chosen first per
//! `docs/migration/df-removal/11-pg-catalog-fidelity.md` §2, because the owned
//! planner needs exactly this data to resolve operators, casts and functions
//! by argument type — making the resolution table `pg_catalog` itself costs
//! almost nothing beyond compatibility.
//!
//! The rest are catalog-driven: [`pg_namespace`], [`pg_class`],
//! [`pg_attribute`], [`pg_attrdef`], [`pg_index`], [`pg_constraint`],
//! [`pg_description`], [`pg_depend`], [`pg_enum`], [`pg_sequence`] and
//! [`pg_inherits`]. [`real_source`] wires a real `basin-catalog` behind
//! [`CatalogSource`]; [`mock`] does the same in memory for tests.
//!
//! # How the data is kept honest
//!
//! `tests/catalog_fidelity.rs` is the crate's oracle, and it is where the
//! column counts above come from. Against a live PostgreSQL 18.2 it checks
//! two things on every run:
//!
//! 1. **Shape** — every relation's declared `arrow_schema()` against the
//!    server's real `pg_attribute`: name, `attnum` order, nullability and
//!    type. This exists because seven of these relations were once wrong in
//!    exactly the same way (right columns, wrong positions) and every hand-
//!    written unit test passed anyway.
//! 2. **Content** — the five catalog-independent relations' rows, cell by
//!    cell, against the server's own. The first time that check ran it found
//!    127 wrong cells across `pg_proc`, `pg_operator` and `pg_cast`,
//!    including two relations reporting a *monomorphized* signature
//!    (`lag(integer)`, `integer[] @> integer[]`) under an oid whose real row
//!    is polymorphic. All are fixed; see each module's docs.
//!
//! That oracle is also why several columns whose values Basin has no concept
//! of are nonetheless reported truthfully rather than omitted: values like
//! `pg_type.typinput` or `pg_operator.oprcode` are fixed properties of the
//! *type* or *operator*, assigned by PostgreSQL's own catalog bootstrap and
//! identical on every installation, so transcribing them is verifiable rather
//! than invented. Where a column genuinely has neither derivable data nor a
//! defensible constant, it is left out and the omission documented in the
//! module — currently exactly two: `pg_index.indclass` (an operator class
//! needs an access method, which [`catalog_source::IndexInfo`] does not
//! carry) and `pg_proc.prosqlbody` (a PostgreSQL-internal serialized parse
//! tree Basin cannot produce or read).
//!
//! The `information_schema` views, and the remainder of the ~65-relation
//! surface, are still a follow-up increment.

pub mod catalog_source;
pub mod error;
pub mod mock;
pub mod pg_am;
pub mod pg_attrdef;
pub mod pg_attribute;
pub mod pg_cast;
pub mod pg_class;
pub mod pg_constraint;
pub mod pg_depend;
pub mod pg_description;
pub mod pg_enum;
pub mod pg_index;
pub mod pg_inherits;
pub mod pg_namespace;
pub mod pg_operator;
pub mod pg_proc;
pub mod pg_sequence;
pub mod pg_type;
pub mod predicate;
pub mod real_source;
pub mod system_view;

pub use basin_pgtype::Oid;
pub use catalog_source::CatalogSource;
pub use error::Error;
pub use predicate::{Predicate, Value};
pub use system_view::SystemView;

/// `pg_namespace.oid` of the `pg_catalog` schema itself. Fixed across every
/// Postgres installation — confirmed live: `SELECT oid FROM pg_namespace
/// WHERE nspname = 'pg_catalog'` returns `11`.
pub const PG_CATALOG_NAMESPACE: Oid = Oid(11);
