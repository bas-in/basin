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

use arrow_schema::{DataType, Field, Schema, TimeUnit};
use basin_catalog::TableMetadata as BasinTableMetadata;
use basin_common::TenantId;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::IcebergRestError;

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
/// Mirrors Iceberg's `CreateTableRequest` open-api shape. The
/// `partition-spec` / `write-order` / `stage-create` fields are accepted
/// for wire compatibility but only their empty / default forms are
/// honoured in v0.1 — Basin's `Catalog::create_table` doesn't (yet) take
/// partition / sort metadata at the create-table boundary, and
/// `stage-create=true` (defer-the-commit) maps awkwardly onto Basin's
/// always-committing model.
#[derive(Debug, Serialize, Deserialize)]
pub struct CreateTableRequest {
    pub name: String,
    pub schema: IcebergSchema,
    #[serde(default, rename = "partition-spec")]
    pub partition_spec: Option<IcebergPartitionSpec>,
    #[serde(default, rename = "write-order")]
    pub write_order: Option<IcebergSortOrder>,
    #[serde(default, rename = "stage-create")]
    pub stage_create: bool,
    #[serde(default)]
    pub location: Option<String>,
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

/// `POST /v1/{prefix}/namespaces/{namespace}/tables/{table}` (commit-table)
/// request body. Iceberg's optimistic-concurrency commit: `requirements`
/// are pre-condition assertions against the table's current metadata;
/// `updates` are the changes to apply atomically iff every requirement
/// holds.
#[derive(Debug, Serialize, Deserialize)]
pub struct CommitTableRequest {
    /// Optional caller-supplied identifier; ignored by Basin's
    /// single-table-per-URL routing but accepted for wire compatibility.
    #[serde(default)]
    pub identifier: Option<TableIdentifier>,
    #[serde(default)]
    pub requirements: Vec<CommitRequirement>,
    #[serde(default)]
    pub updates: Vec<CommitUpdate>,
}

/// One Iceberg pre-condition. `type` discriminates the variant; we
/// surface the subset Basin can map onto its single-schema /
/// single-branch model. Anything else (`assert-uuid` for ref-uuid,
/// `assert-default-spec-id`, etc.) is rejected as 501.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum CommitRequirement {
    AssertCreate,
    AssertTableUuid {
        uuid: String,
    },
    AssertCurrentSchemaId {
        #[serde(rename = "current-schema-id")]
        current_schema_id: i32,
    },
    AssertRefSnapshotId {
        #[serde(rename = "ref")]
        ref_name: String,
        /// Iceberg encodes "no snapshot expected" as `null`.
        #[serde(default, rename = "snapshot-id")]
        snapshot_id: Option<i64>,
    },
    /// Anything Iceberg ships that we don't model yet — e.g.
    /// `assert-default-spec-id`, `assert-default-sort-order-id`,
    /// `assert-last-assigned-field-id`. Captured as a passthrough so the
    /// handler can return a 501 with the offending type name.
    #[serde(other)]
    Other,
}

/// One Iceberg update action. Mirrors the `action` discriminator in the
/// open-api spec. v0.1 honours `add-snapshot` + `set-current-snapshot`
/// only; everything else is rejected as 501.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "kebab-case")]
pub enum CommitUpdate {
    AddSnapshot {
        snapshot: IcebergSnapshot,
    },
    SetCurrentSnapshot {
        #[serde(rename = "snapshot-id")]
        snapshot_id: i64,
    },
    /// Catch-all for actions Basin doesn't yet model:
    /// `add-schema`, `set-current-schema`, `add-partition-spec`,
    /// `set-default-spec`, `add-sort-order`, `set-default-sort-order`,
    /// `add-view-version`, `remove-snapshots`, `remove-snapshot-ref`,
    /// `set-properties`, `remove-properties`, `set-location`,
    /// `assign-uuid`, `upgrade-format-version`. Captured as a
    /// passthrough so the handler can name the offending action in the
    /// 501 envelope.
    #[serde(other)]
    Other,
}

/// `POST /v1/{prefix}/namespaces/{namespace}/register` request body.
/// v0.1 returns 501; the body is captured so the structured error can
/// echo the requested name back.
#[derive(Debug, Serialize, Deserialize)]
pub struct RegisterTableRequest {
    pub name: String,
    #[serde(rename = "metadata-location")]
    pub metadata_location: String,
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
    /// Optional on the wire — pyiceberg's `CreateTableRequest` omits it
    /// (Basin assigns `0` on create). Required in `LoadTableResponse`.
    #[serde(default, rename = "schema-id")]
    pub schema_id: i32,
    #[serde(rename = "type")]
    pub kind: String,
    pub fields: Vec<IcebergSchemaField>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IcebergSchemaField {
    /// Optional on the wire — pyiceberg sometimes elides the id when it
    /// expects the server to assign one. Defaults to 0; Basin assigns
    /// a stable id at create-table time based on field ordinal.
    #[serde(default)]
    pub id: i32,
    pub name: String,
    /// Iceberg `required` defaults to false (= nullable) when omitted.
    #[serde(default)]
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
    /// Optional on the wire (pyiceberg uses sequence-number == snapshot-id
    /// in the v2 fast-path; some clients omit the field on the request side).
    #[serde(default, rename = "sequence-number")]
    pub sequence_number: i64,
    #[serde(rename = "timestamp-ms")]
    pub timestamp_ms: i64,
    #[serde(default)]
    pub summary: HashMap<String, String>,
    #[serde(
        rename = "manifest-list",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub manifest_list: String,
    /// Optional on the wire — Basin's single-schema model always uses 0.
    #[serde(default, rename = "schema-id")]
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
/// surface (fixed, struct, list, map) needs schema-tree recursion —
/// the v0.1 translator handles the scalar set Basin's CREATE TABLE
/// produces today plus `Decimal128`.
pub(crate) fn arrow_to_iceberg_type(dt: &DataType) -> String {
    match dt {
        DataType::Boolean => "boolean".into(),
        DataType::Int32 | DataType::UInt32 => "int".into(),
        DataType::Int64 | DataType::UInt64 => "long".into(),
        DataType::Float32 => "float".into(),
        DataType::Float64 => "double".into(),
        DataType::Date32 | DataType::Date64 => "date".into(),
        DataType::Timestamp(_, Some(_)) => "timestamptz".into(),
        DataType::Timestamp(_, None) => "timestamp".into(),
        DataType::Utf8 | DataType::LargeUtf8 => "string".into(),
        DataType::Decimal128(p, s) => format!("decimal({p},{s})"),
        DataType::FixedSizeBinary(16) => "uuid".into(),
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => "binary".into(),
        _ => "binary".into(),
    }
}

/// Iceberg type-string → Arrow `DataType`. Inverse of
/// [`arrow_to_iceberg_type`] for the scalar subset Basin supports.
/// Unsupported types (`fixed`, `list`, `map`, `struct`, `time`) return
/// [`IcebergRestError::NotImplemented`] so the create-table handler
/// surfaces a clean 501.
pub(crate) fn iceberg_type_to_arrow(s: &str) -> Result<DataType, IcebergRestError> {
    let trimmed = s.trim();
    // `decimal(p,s)` — accept inner whitespace because some clients
    // pretty-print the type string.
    if let Some(inner) = trimmed
        .strip_prefix("decimal(")
        .and_then(|x| x.strip_suffix(')'))
    {
        let parts: Vec<&str> = inner.split(',').map(|p| p.trim()).collect();
        if parts.len() != 2 {
            return Err(IcebergRestError::BadRequest(format!(
                "decimal type must be `decimal(precision,scale)`, got `{s}`"
            )));
        }
        let p: u8 = parts[0].parse().map_err(|_| {
            IcebergRestError::BadRequest(format!("decimal precision not an integer: `{s}`"))
        })?;
        let scale: i8 = parts[1].parse().map_err(|_| {
            IcebergRestError::BadRequest(format!("decimal scale not an integer: `{s}`"))
        })?;
        if !(1..=38).contains(&p) {
            return Err(IcebergRestError::BadRequest(format!(
                "decimal precision {p} out of range; Arrow Decimal128 supports 1..=38"
            )));
        }
        return Ok(DataType::Decimal128(p, scale));
    }
    // `fixed(N)` is Iceberg's fixed-length binary; we don't ship it yet.
    if trimmed.starts_with("fixed(") {
        return Err(IcebergRestError::NotImplemented(format!(
            "Iceberg type `{s}` not supported by Basin v0.1 (fixed-length binary lands in a follow-up)"
        )));
    }
    Ok(match trimmed {
        "boolean" => DataType::Boolean,
        "int" => DataType::Int32,
        "long" => DataType::Int64,
        "float" => DataType::Float32,
        "double" => DataType::Float64,
        "string" => DataType::Utf8,
        "binary" => DataType::LargeBinary,
        "date" => DataType::Date32,
        "timestamp" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "timestamptz" => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        "uuid" => DataType::FixedSizeBinary(16),
        "time" | "list" | "map" | "struct" => {
            return Err(IcebergRestError::NotImplemented(format!(
                "Iceberg type `{s}` not supported by Basin v0.1"
            )))
        }
        other => {
            return Err(IcebergRestError::BadRequest(format!(
                "unrecognised Iceberg type: `{other}`"
            )))
        }
    })
}

/// Convert an Iceberg `IcebergSchema` (decoded from a `CreateTableRequest`)
/// into an Arrow `Schema`. Honours each field's `required` flag (Iceberg
/// `required=true` ↔ Arrow `nullable=false`) and threads each field
/// through [`iceberg_type_to_arrow`].
pub(crate) fn iceberg_schema_to_arrow(s: &IcebergSchema) -> Result<Schema, IcebergRestError> {
    if s.kind != "struct" {
        return Err(IcebergRestError::BadRequest(format!(
            "top-level schema must be a struct; got `{}`",
            s.kind
        )));
    }
    let mut arrow_fields: Vec<Field> = Vec::with_capacity(s.fields.len());
    for f in &s.fields {
        let dt = iceberg_type_to_arrow(&f.kind)?;
        arrow_fields.push(Field::new(&f.name, dt, !f.required));
    }
    Ok(Schema::new(arrow_fields))
}

/// Stable UUID derived from `(tenant, table)`. Iceberg requires every
/// table metadata blob to carry a uuid; Basin doesn't store one
/// internally so we hash a synthetic one. Stable across reloads
/// because the input is deterministic.
pub(crate) fn synthesise_table_uuid(meta: &BasinTableMetadata) -> String {
    let key = format!("{}/{}", meta.tenant, meta.table);
    Uuid::new_v5(&Uuid::NAMESPACE_OID, key.as_bytes()).to_string()
}
