//! `basin-iceberg-rest` — Lakekeeper-compatible Iceberg REST catalog server
//! that wraps Basin's internal [`basin_catalog::Catalog`] trait.
//!
//! See `README.md` for the supported endpoint matrix. Phase 6 entry-point:
//! GET endpoints (namespaces, list-tables, load-table) plus DELETE table,
//! plus the v0.1 subset of create-table and commit-table. `register-table`
//! returns 501 until we add foreign-metadata ingest. This crate
//! intentionally does **not** depend on the upstream `iceberg` crate — we
//! shape for external-client compatibility (pyiceberg, Spark, Trino,
//! DuckDB, Polars), not for reuse of apache code.
//!
//! ## Mounting
//!
//! ```ignore
//! let catalog: Arc<dyn basin_catalog::Catalog> = …;
//! let app = axum::Router::new()
//!     .nest("/iceberg", basin_iceberg_rest::router(catalog));
//! ```
//!
//! External clients connect with the Iceberg REST catalog URL
//! `https://basin.example.com/iceberg/v1/<warehouse>`. The `<warehouse>`
//! prefix is the Iceberg "warehouse" identifier — for v0.1 Basin accepts
//! any prefix and routes the same; the deployment-id mapping lands when
//! multi-deployment ships.

#![forbid(unsafe_code)]

use std::sync::Arc;

use axum::routing::get;
use axum::Router;
use basin_catalog::Catalog;

mod auth;
pub mod config;
mod error;
mod models;
mod namespaces;
mod tables;

pub use config::IcebergRestConfig;
pub use error::IcebergRestError;
pub use models::{
    CommitRequirement, CommitTableRequest, CommitUpdate, CreateTableRequest, IcebergPartitionSpec,
    IcebergSchema, IcebergSchemaField, IcebergSnapshot, IcebergSnapshotRef, IcebergSortOrder,
    IcebergTableMetadata, ListNamespacesResponse, ListTablesResponse, LoadTableResponse,
    RegisterTableRequest, TableIdentifier,
};

/// Shared handler state: the Catalog handle plus immutable config.
#[derive(Clone)]
pub struct State {
    pub(crate) catalog: Arc<dyn Catalog>,
    pub(crate) cfg: Arc<IcebergRestConfig>,
}

/// Build the Iceberg REST router. Does **not** apply tracing / CORS / body-
/// limit layers — the caller (basin-server) layers those at composition
/// time so the global stack stays consistent across all REST surfaces.
pub fn router(catalog: Arc<dyn Catalog>) -> Router {
    router_with_config(catalog, IcebergRestConfig::default())
}

/// Variant of [`router`] that takes an explicit config. Tests pass a
/// disabled-auth config; production wiring uses the default which still
/// requires a non-empty Bearer header.
pub fn router_with_config(catalog: Arc<dyn Catalog>, cfg: IcebergRestConfig) -> Router {
    let state = State {
        catalog,
        cfg: Arc::new(cfg),
    };

    Router::new()
        .route("/v1/:prefix/namespaces", get(namespaces::list_namespaces))
        .route(
            "/v1/:prefix/namespaces/:namespace/tables",
            get(tables::list_tables).post(tables::create_table),
        )
        .route(
            "/v1/:prefix/namespaces/:namespace/tables/:table",
            get(tables::load_table)
                .delete(tables::drop_table)
                .post(tables::commit_table),
        )
        .route(
            "/v1/:prefix/namespaces/:namespace/register",
            axum::routing::post(tables::register_table),
        )
        .with_state(state)
}
