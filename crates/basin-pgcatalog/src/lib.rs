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
//! First increment. [`pg_type`] is complete — it needs no catalog at all,
//! since it is fully determined by `basin-pgtype`'s own OID table.
//! [`pg_namespace`] and [`pg_class`] are implemented against the [`mock`]
//! [`CatalogSource`], proving the shape against something other than
//! `pg_type`'s static data. [`pg_operator`], [`pg_cast`] and [`pg_proc`] are
//! also complete and, like `pg_type`, need no catalog: each is a view over
//! `basin-pgtype`'s own operator/cast/function tables, chosen first per
//! `docs/migration/df-removal/11-pg-catalog-fidelity.md` §2 because the owned
//! planner needs exactly this data to resolve operators, casts and functions
//! by argument type — making the resolution table `pg_catalog` itself costs
//! almost nothing beyond compatibility. Everything else in the ~65-relation
//! surface — `pg_attribute`, `pg_index`, `pg_constraint`, the
//! `information_schema` views, and wiring a real `basin-catalog` backend for
//! [`CatalogSource`] — is a follow-up increment, not attempted here.

pub mod catalog_source;
pub mod error;
pub mod mock;
pub mod pg_cast;
pub mod pg_class;
pub mod pg_namespace;
pub mod pg_operator;
pub mod pg_proc;
pub mod pg_type;
pub mod predicate;
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
