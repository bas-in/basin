//! Iceberg REST request / response shapes.
//!
//! Trimmed to the fields v0.1 actually surfaces. The full Iceberg
//! open-api spec carries a much wider surface (statistics, snapshot
//! refs, sort orders, etc.); we add fields lazily as endpoints land.
//!
//! What's here is enough for pyiceberg / Spark / Trino to *load* a
//! Basin table: format-version=2, a `table-uuid`, a `location`, a
//! single schema entry, empty partition-specs, and either no snapshots
//! or one synthetic snapshot describing the current head.

use std::collections::HashMap;

use arrow_schema::DataType;
use basin_catalog::TableMetadata as BasinTableMetadata;
use basin_common::TenantId;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// `GET /v1/{prefix}/namespaces` response.
///
/// Iceberg models a namespace as an *array* of strings (so a hierarchy
/// like `["accounting", "us-east"]` round-trips); Basin flattens it to
/// the single-element ULID array `[<tenant_id>]`.
#[derive(Debug, Serialize, Deserialize)]
pub struct ListNamespacesResponse {
    pub namespaces: Vec<Vec<String>>,
}

/// One `(namespace, name)` pair in [`ListTablesResponse`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableIdentifier {
    pub namespace: Vec<String>,
    pub name: String,
}

/// `GET /v1/{prefix}/namespaces/{namespace}/tables` response.
#[derive(Debug, Serialize, Deserialize)]
pub struct ListTablesResponse {
    pub identifiers: Vec<TableIdentifier>,
}

/// `GET /v1/{prefix}/namespaces/{namespace}/tables/{table}` response.
#[derive(Debug, Serialize, Deserialize)]
pub struct LoadTableResponse {
    #[serde(rename = "metadata-location")]
    pub metadata_location: String,
    pub metadata: IcebergTableMetadata,
    /// Extra config (storage credentials, etc). Empty for v0.1.
    #[serde(default)]
    pub config: HashMap<String, String>,
}

/// `POST /v1/{prefix}/namespaces/{namespace}/tables` request body.
///
/// v0.1 surfaces the body shape; the handler returns 501 because Basin's
/// `Catalog::create_table` takes an Arrow `Schema` and the conversion
/// from Iceberg's schema JSON requires a translator we haven't built.
/// pyiceberg / Spark expect this endpoint to exist (some clients probe
/// it on connect even when not creating tables) so we accept the JSON
/// and respond with a structured 501.
#[derive(Debug, Serialize, Deserialize)]
pub struct CreateTableRequest {
    pub name: String,
    pub schema: serde_json::Value,
    #[serde(default)]
    pub location: Option<String>,
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

/// Iceberg `TableMetadata` — what every reader (pyiceberg, Spark,
/// Trino) deserialises off `LoadTableResponse::metadata`.
///
/// Field set covers the *required* spec fields plus the minimal optional
/// set readers consult. Anything we can't synthesise from Basin's
/// internal `TableMetadata` is left empty / null. See `translate_table_metadata`
/// in `tables.rs` for the field-by-field mapping decisions.
#[derive(Debug, Serialize, Deserialize)]
pub struct IcebergTableMetadata {
    #[serde(rename = "format-version")]
    pub format_version: u8,
    #[serde(rename = "table-uuid")]
    pub table_uuid: String,
    pub location: String,
    #[serde(rename = "last-sequence-number")]
    pub last_sequence_number: u64,
    #[serde(rename = "last-updated-ms")]
    pub last_updated_ms: i64,
    #[serde(rename = "last-column-id")]
    pub last_column_id: i32,
    #[serde(rename = "current-schema-id")]
    pub current_schema_id: i32,
    pub schemas: Vec<IcebergSchema>,
    #[serde(rename = "default-spec-id")]
    pub default_spec_id: i32,
    #[serde(rename = "partition-specs")]
    pub partition_specs: Vec<IcebergPartitionSpec>,
    #[serde(rename = "last-partition-id")]
    pub last_partition_id: i32,
    #[serde(rename = "default-sort-order-id")]
    pub default_sort_order_id: i32,
    #[serde(rename = "sort-orders")]
    pub sort_orders: Vec<IcebergSortOrder>,
    pub properties: HashMap<String, String>,
    #[serde(rename = "current-snapshot-id")]
    pub current_snapshot_id: Option<i64>,
    pub snapshots: Vec<IcebergSnapshot>,
    pub refs: HashMap<String, IcebergSnapshotRef>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IcebergSchema {
    #[serde(rename = "schema-id")]
    pub schema_id: i32,
    #[serde(rename = "type")]
    pub kind: String,
    pub fields: Vec<IcebergSchemaField>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IcebergSchemaField {
    pub id: i32,
    pub name: String,
    pub required: bool,
    #[serde(rename = "type")]
    pub kind: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IcebergPartitionSpec {
    #[serde(rename = "spec-id")]
    pub spec_id: i32,
    pub fields: Vec<IcebergPartitionField>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IcebergPartitionField {
    pub name: String,
    pub transform: String,
    #[serde(rename = "source-id")]
    pub source_id: i32,
    #[serde(rename = "field-id")]
    pub field_id: i32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IcebergSortOrder {
    #[serde(rename = "order-id")]
    pub order_id: i32,
    pub fields: Vec<serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IcebergSnapshot {
    #[serde(rename = "snapshot-id")]
    pub snapshot_id: i64,
    #[serde(rename = "parent-snapshot-id", skip_serializing_if = "Option::is_none")]
    pub parent_snapshot_id: Option<i64>,
    #[serde(rename = "sequence-number")]
    pub sequence_number: i64,
    #[serde(rename = "timestamp-ms")]
    pub timestamp_ms: i64,
    pub summary: HashMap<String, String>,
    #[serde(rename = "manifest-list", default, skip_serializing_if = "String::is_empty")]
    pub manifest_list: String,
    #[serde(rename = "schema-id")]
    pub schema_id: i32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IcebergSnapshotRef {
    #[serde(rename = "snapshot-id")]
    pub snapshot_id: i64,
    #[serde(rename = "type")]
    pub kind: String,
}

/// Map a [`TenantId`] onto Iceberg's array-of-strings namespace shape.
pub(crate) fn tenant_to_namespace(t: &TenantId) -> Vec<String> {
    vec![t.to_string()]
}

/// Best-effort Arrow → Iceberg type-string translation. Unknowns fall
/// back to `"binary"` because Iceberg readers tolerate that as a
/// pass-through; we never serialise `"unknown"`. The full Iceberg type
/// surface (decimal, fixed, struct, list, map) needs schema-tree
/// recursion — the v0.1 translator handles only the flat scalar set
/// Basin's CREATE TABLE produces today.
pub(crate) fn arrow_to_iceberg_type(dt: &DataType) -> String {
    match dt {
        DataType::Boolean => "boolean".into(),
        DataType::Int32 | DataType::UInt32 => "int".into(),
        DataType::Int64 | DataType::UInt64 => "long".into(),
        DataType::Float32 => "float".into(),
        DataType::Float64 => "double".into(),
        DataType::Date32 | DataType::Date64 => "date".into(),
        DataType::Timestamp(_, _) => "timestamptz".into(),
        DataType::Utf8 | DataType::LargeUtf8 => "string".into(),
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => "binary".into(),
        _ => "binary".into(),
    }
}

/// Stable UUID derived from `(tenant, table)`. Iceberg requires every
/// table metadata blob to carry a uuid; Basin doesn't store one
/// internally so we hash a synthetic one. Stable across reloads
/// because the input is deterministic.
pub(crate) fn synthesise_table_uuid(meta: &BasinTableMetadata) -> String {
    let key = format!("{}/{}", meta.tenant, meta.table);
    Uuid::new_v5(&Uuid::NAMESPACE_OID, key.as_bytes()).to_string()
}
