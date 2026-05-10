//! Table handlers.
//!
//! Endpoints implemented:
//! - `GET .../tables` → `Catalog::list_tables`.
//! - `GET .../tables/{tbl}` → `Catalog::load_table` + Iceberg-shape translator.
//! - `DELETE .../tables/{tbl}` → `Catalog::drop_table`.
//!
//! Endpoints scaffolded with 501 (the structured Iceberg
//! `NotImplementedException` shape so pyiceberg surfaces a clean error):
//! - `POST .../tables` (create) — Basin's `create_table` takes an Arrow
//!   schema; the Iceberg → Arrow schema translator is v0.2 work.
//! - `POST .../tables/{tbl}` (commit) — Iceberg commits map to Basin's
//!   `append_data_files` / `replace_data_files` with optimistic
//!   concurrency, but the manifest-list / data-file translation needs
//!   careful design (see Basin snapshot model).

use std::collections::HashMap;
use std::str::FromStr;

use axum::extract::{Path, State as AxumState};
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use basin_catalog::TableMetadata as BasinTableMetadata;
use basin_common::{TableName, TenantId};

use crate::auth;
use crate::error::IcebergRestError;
use crate::models::{
    arrow_to_iceberg_type, synthesise_table_uuid, tenant_to_namespace, CreateTableRequest,
    IcebergPartitionSpec, IcebergSchema, IcebergSchemaField, IcebergSnapshot, IcebergSnapshotRef,
    IcebergSortOrder, IcebergTableMetadata, ListTablesResponse, LoadTableResponse, TableIdentifier,
};
use crate::State;

/// Decode the `:namespace` URL segment into a `TenantId`.
fn parse_namespace(raw: &str) -> Result<TenantId, IcebergRestError> {
    // Iceberg URL-encodes multi-element namespaces with the unit-separator
    // (0x1F) between segments. v0.1 only accepts a single segment.
    if raw.contains('\u{1F}') {
        return Err(IcebergRestError::BadRequest(
            "multi-segment namespaces unsupported; expected single tenant id".into(),
        ));
    }
    TenantId::from_str(raw)
        .map_err(|e| IcebergRestError::BadRequest(format!("invalid namespace: {e}")))
}

fn parse_table(raw: &str) -> Result<TableName, IcebergRestError> {
    TableName::from_str(raw)
        .map_err(|e| IcebergRestError::BadRequest(format!("invalid table name: {e}")))
}

pub(crate) async fn list_tables(
    AxumState(state): AxumState<State>,
    Path((_prefix, namespace)): Path<(String, String)>,
    headers: HeaderMap,
) -> Result<Json<ListTablesResponse>, IcebergRestError> {
    let url_tenant = parse_namespace(&namespace)?;
    let tenant = auth::authorize_namespace(&state.cfg, &headers, &url_tenant)?;
    let tables = state.catalog.list_tables(&tenant).await?;
    let identifiers = tables
        .into_iter()
        .map(|t| TableIdentifier {
            namespace: tenant_to_namespace(&tenant),
            name: t.to_string(),
        })
        .collect();
    Ok(Json(ListTablesResponse { identifiers }))
}

pub(crate) async fn load_table(
    AxumState(state): AxumState<State>,
    Path((_prefix, namespace, table)): Path<(String, String, String)>,
    headers: HeaderMap,
) -> Result<Json<LoadTableResponse>, IcebergRestError> {
    let url_tenant = parse_namespace(&namespace)?;
    let tenant = auth::authorize_namespace(&state.cfg, &headers, &url_tenant)?;
    let table_name = parse_table(&table)?;
    let meta = state.catalog.load_table(&tenant, &table_name).await?;
    Ok(Json(translate_table_metadata(&state, &meta)))
}

pub(crate) async fn drop_table(
    AxumState(state): AxumState<State>,
    Path((_prefix, namespace, table)): Path<(String, String, String)>,
    headers: HeaderMap,
) -> Result<StatusCode, IcebergRestError> {
    let url_tenant = parse_namespace(&namespace)?;
    let tenant = auth::authorize_namespace(&state.cfg, &headers, &url_tenant)?;
    let table_name = parse_table(&table)?;
    state.catalog.drop_table(&tenant, &table_name).await?;
    Ok(StatusCode::NO_CONTENT)
}

pub(crate) async fn create_table(
    AxumState(state): AxumState<State>,
    Path((_prefix, namespace)): Path<(String, String)>,
    headers: HeaderMap,
    Json(_body): Json<CreateTableRequest>,
) -> Result<StatusCode, IcebergRestError> {
    let url_tenant = parse_namespace(&namespace)?;
    let _ = auth::authorize_namespace(&state.cfg, &headers, &url_tenant)?;
    Err(IcebergRestError::NotImplemented(
        "POST tables (create-table) is deferred; Iceberg-schema → Arrow-schema translation lands in a follow-up. Use Basin's CREATE TABLE SQL surface."
            .into(),
    ))
}

pub(crate) async fn commit_table(
    AxumState(state): AxumState<State>,
    Path((_prefix, namespace, _table)): Path<(String, String, String)>,
    headers: HeaderMap,
    Json(_body): Json<serde_json::Value>,
) -> Result<StatusCode, IcebergRestError> {
    let url_tenant = parse_namespace(&namespace)?;
    let _ = auth::authorize_namespace(&state.cfg, &headers, &url_tenant)?;
    Err(IcebergRestError::NotImplemented(
        "POST commit (append / overwrite) is deferred; mapping Iceberg's manifest-list flow onto Basin's snapshot-delta model is a follow-up."
            .into(),
    ))
}

/// Translate Basin's internal [`BasinTableMetadata`] into the Iceberg
/// REST `TableMetadata` shape readers expect.
///
/// Surfaced fields:
/// - `format-version=2`, `table-uuid` (deterministic UUIDv5 over
///   `tenant/table`).
/// - `location` = `<base_location><tenant>/<table>/`.
/// - One `schemas` entry derived from Basin's Arrow `Schema`. Field
///   types map via [`arrow_to_iceberg_type`]; unknown types fall
///   back to `"binary"`.
/// - `partition-specs` = `[{ spec-id: 0, fields: [] }]` (Basin's
///   internal `PartitionSpec::RangeMonthly` doesn't yet round-trip
///   to Iceberg's transform vocabulary; v0.2 work).
/// - `current-snapshot-id` = Basin's head snapshot id (cast to i64).
/// - `snapshots` = one synthetic Iceberg snapshot per Basin snapshot,
///   carrying `(snapshot_id, parent_snapshot_id, timestamp-ms, summary)`.
///   `manifest-list` is left empty — Basin doesn't write Iceberg-shape
///   manifest lists.
/// - `refs.main` always points at the current snapshot (Iceberg
///   readers consult `refs` to resolve "the live branch").
pub(crate) fn translate_table_metadata(
    state: &State,
    meta: &BasinTableMetadata,
) -> LoadTableResponse {
    let location = format!(
        "{}{}/{}/",
        state.cfg.base_location, meta.tenant, meta.table
    );

    let fields: Vec<IcebergSchemaField> = meta
        .schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| IcebergSchemaField {
            id: (i as i32) + 1,
            name: f.name().clone(),
            required: !f.is_nullable(),
            kind: arrow_to_iceberg_type(f.data_type()),
        })
        .collect();
    let last_column_id = fields.len() as i32;

    let schema = IcebergSchema {
        schema_id: 0,
        kind: "struct".into(),
        fields,
    };

    let snapshots: Vec<IcebergSnapshot> = meta
        .snapshots
        .iter()
        .map(|s| {
            let mut summary: HashMap<String, String> = HashMap::new();
            summary.insert(
                "operation".into(),
                match s.summary.operation {
                    basin_catalog::SnapshotOperation::Genesis => "append".into(),
                    basin_catalog::SnapshotOperation::Append => "append".into(),
                    basin_catalog::SnapshotOperation::Replace => "overwrite".into(),
                },
            );
            summary.insert("added-files-count".into(), s.summary.added_files.to_string());
            summary.insert("added-records".into(), s.summary.added_rows.to_string());
            summary.insert(
                "added-files-size".into(),
                s.summary.added_bytes.to_string(),
            );
            if s.summary.removed_files > 0 {
                summary.insert(
                    "deleted-files-count".into(),
                    s.summary.removed_files.to_string(),
                );
            }
            IcebergSnapshot {
                snapshot_id: s.id.0 as i64,
                parent_snapshot_id: s.parent.map(|p| p.0 as i64),
                sequence_number: s.id.0 as i64,
                timestamp_ms: s.committed_at.timestamp_millis(),
                summary,
                manifest_list: String::new(),
                schema_id: 0,
            }
        })
        .collect();

    let last_updated_ms = snapshots
        .last()
        .map(|s| s.timestamp_ms)
        .unwrap_or_else(|| chrono::Utc::now().timestamp_millis());

    let mut refs: HashMap<String, IcebergSnapshotRef> = HashMap::new();
    refs.insert(
        "main".into(),
        IcebergSnapshotRef {
            snapshot_id: meta.current_snapshot.0 as i64,
            kind: "branch".into(),
        },
    );

    let metadata = IcebergTableMetadata {
        format_version: 2,
        table_uuid: synthesise_table_uuid(meta),
        location: location.clone(),
        last_sequence_number: meta.current_snapshot.0,
        last_updated_ms,
        last_column_id,
        current_schema_id: 0,
        schemas: vec![schema],
        default_spec_id: 0,
        partition_specs: vec![IcebergPartitionSpec {
            spec_id: 0,
            fields: Vec::new(),
        }],
        last_partition_id: 0,
        default_sort_order_id: 0,
        sort_orders: vec![IcebergSortOrder {
            order_id: 0,
            fields: Vec::new(),
        }],
        properties: HashMap::new(),
        current_snapshot_id: Some(meta.current_snapshot.0 as i64),
        snapshots,
        refs,
    };

    LoadTableResponse {
        metadata_location: format!("{}metadata/v{}.json", location, meta.current_snapshot.0),
        metadata,
        config: HashMap::new(),
    }
}
