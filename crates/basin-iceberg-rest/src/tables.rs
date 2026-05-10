//! Table handlers.
//!
//! Endpoints implemented:
//! - `GET .../tables` → `Catalog::list_tables`.
//! - `GET .../tables/{tbl}` → `Catalog::load_table` + Iceberg-shape translator.
//! - `DELETE .../tables/{tbl}` → `Catalog::drop_table`.
//! - `POST .../tables` (create-table) → translates the Iceberg schema to
//!   Arrow and calls `Catalog::create_table`. Returns the standard
//!   `LoadTableResponse` so clients pick up the freshly-created metadata.
//! - `POST .../tables/{tbl}` (commit-table) → validates Iceberg
//!   pre-conditions against the current `TableMetadata`, then translates
//!   `add-snapshot` + `set-current-snapshot` updates onto Basin's
//!   `append_data_files` primitive (which carries the same optimistic-
//!   concurrency contract). See the README for the v0.1 accept-vs-reject
//!   surface and the request-shape examples.
//! - `POST .../register` (register-table) → returns 501; Basin's catalog
//!   doesn't accept externally-authored metadata blobs in v0.1.

use std::collections::HashMap;
use std::str::FromStr;

use axum::extract::{Path, State as AxumState};
use axum::http::{HeaderMap, StatusCode};
use axum::Json;
use basin_catalog::{DataFileRef, TableMetadata as BasinTableMetadata};
use basin_common::{TableName, TenantId};

use crate::auth;
use crate::error::IcebergRestError;
use crate::models::{
    arrow_to_iceberg_type, iceberg_schema_to_arrow, synthesise_table_uuid, tenant_to_namespace,
    CommitRequirement, CommitTableRequest, CommitUpdate, CreateTableRequest, IcebergPartitionSpec,
    IcebergSchema, IcebergSchemaField, IcebergSnapshot, IcebergSnapshotRef, IcebergSortOrder,
    IcebergTableMetadata, ListTablesResponse, LoadTableResponse, RegisterTableRequest,
    TableIdentifier,
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

/// `POST /v1/{prefix}/namespaces/{namespace}/tables` — create-table.
///
/// Translates the Iceberg `CreateTableRequest` schema into an Arrow
/// `Schema` and calls `Catalog::create_table`. The response is the
/// standard [`LoadTableResponse`] over the freshly-created metadata so
/// clients (pyiceberg / Spark) pick up the genesis snapshot in one round
/// trip.
///
/// v0.1 rejects:
/// - `stage-create=true` (Iceberg's "create the metadata, but don't
///   commit yet" mode — Basin always commits at create time).
/// - Non-empty `partition-spec` / `write-order` (the Iceberg →
///   Basin partition-spec translator is v0.2 work).
/// - Any field type Basin doesn't yet model (`fixed`, `list`, `map`,
///   `struct`, `time`) — surfaced as 501 with the offending type name.
pub(crate) async fn create_table(
    AxumState(state): AxumState<State>,
    Path((_prefix, namespace)): Path<(String, String)>,
    headers: HeaderMap,
    Json(body): Json<CreateTableRequest>,
) -> Result<(StatusCode, Json<LoadTableResponse>), IcebergRestError> {
    let url_tenant = parse_namespace(&namespace)?;
    let tenant = auth::authorize_namespace(&state.cfg, &headers, &url_tenant)?;
    if body.stage_create {
        return Err(IcebergRestError::NotImplemented(
            "stage-create=true is not supported by Basin v0.1; commits are atomic at create time"
                .into(),
        ));
    }
    if let Some(spec) = body.partition_spec.as_ref() {
        if !spec.fields.is_empty() {
            return Err(IcebergRestError::NotImplemented(
                "non-empty partition-spec on create-table is not supported by Basin v0.1; \
                 declare partitioning via the SQL surface (`CREATE TABLE ... PARTITION BY`)"
                    .into(),
            ));
        }
    }
    if let Some(order) = body.write_order.as_ref() {
        if !order.fields.is_empty() {
            return Err(IcebergRestError::NotImplemented(
                "non-empty write-order on create-table is not supported by Basin v0.1".into(),
            ));
        }
    }
    let table_name = parse_table(&body.name)?;
    let arrow_schema = iceberg_schema_to_arrow(&body.schema)?;
    let meta = state
        .catalog
        .create_table(&tenant, &table_name, &arrow_schema)
        .await?;
    Ok((
        StatusCode::OK,
        Json(translate_table_metadata(&state, &meta)),
    ))
}

/// `POST /v1/{prefix}/namespaces/{namespace}/tables/{table}` —
/// commit-table.
///
/// Validates every Iceberg `requirement` against the current
/// [`BasinTableMetadata`] (loaded fresh) and rejects with 409 on the
/// first mismatch. Then walks the `updates` and translates the
/// supported subset onto `Catalog::append_data_files`. See
/// [`apply_commit`] for the per-action mapping.
///
/// On success, returns the freshly-loaded table metadata (same
/// translator as `GET tables/{tbl}`) so the client picks up the new
/// `current-snapshot-id`.
pub(crate) async fn commit_table(
    AxumState(state): AxumState<State>,
    Path((_prefix, namespace, table)): Path<(String, String, String)>,
    headers: HeaderMap,
    Json(body): Json<CommitTableRequest>,
) -> Result<Json<LoadTableResponse>, IcebergRestError> {
    let url_tenant = parse_namespace(&namespace)?;
    let tenant = auth::authorize_namespace(&state.cfg, &headers, &url_tenant)?;
    let table_name = parse_table(&table)?;

    // Snapshot the current metadata up front. Every requirement check
    // and the optimistic-concurrency boundary read off this view.
    let current = state.catalog.load_table(&tenant, &table_name).await?;
    validate_requirements(&current, &body.requirements)?;
    apply_commit(&state, &tenant, &table_name, &current, &body.updates).await?;

    // Reload to surface the post-commit metadata (snapshot pointer
    // advances inside `append_data_files`; we re-read so the response
    // reflects the same shape `GET tables/{tbl}` would return).
    let after = state.catalog.load_table(&tenant, &table_name).await?;
    Ok(Json(translate_table_metadata(&state, &after)))
}

/// `POST /v1/{prefix}/namespaces/{namespace}/register` — register-table.
///
/// v0.1: 501. Basin's catalog doesn't accept externally-authored
/// metadata blobs; the registration path lands when Basin can verify
/// and ingest a foreign metadata.json (multi-deployment work).
pub(crate) async fn register_table(
    AxumState(state): AxumState<State>,
    Path((_prefix, namespace)): Path<(String, String)>,
    headers: HeaderMap,
    Json(body): Json<RegisterTableRequest>,
) -> Result<StatusCode, IcebergRestError> {
    let url_tenant = parse_namespace(&namespace)?;
    let _ = auth::authorize_namespace(&state.cfg, &headers, &url_tenant)?;
    Err(IcebergRestError::NotImplemented(format!(
        "register-table is not supported by Basin v0.1 (table `{}` from `{}`); Basin's catalog \
         only accepts tables created via create-table or the SQL surface",
        body.name, body.metadata_location
    )))
}

/// Walk `requirements` against the current metadata view. Returns
/// `Ok(())` when every assertion holds; the first failure surfaces as
/// [`IcebergRestError::Conflict`] (HTTP 409) carrying Iceberg's
/// `CommitFailedException` envelope.
///
/// v0.1 mapping:
/// - `assert-create` → reject (the table already exists by the time
///   commit-table is reached).
/// - `assert-table-uuid` → compare the requested uuid against
///   [`synthesise_table_uuid`]'s deterministic UUIDv5 over `tenant/table`.
/// - `assert-current-schema-id` → Basin's `TableMetadata` is
///   single-schema; the only valid value is 0.
/// - `assert-ref-snapshot-id` for `ref="main"` → check
///   `meta.current_snapshot.0 as i64` matches; null means "expected no
///   snapshot" (genesis hasn't fired) which Basin never matches because
///   genesis is always present.
/// - Any other requirement → 501 (`NotImplementedException`).
fn validate_requirements(
    meta: &BasinTableMetadata,
    requirements: &[CommitRequirement],
) -> Result<(), IcebergRestError> {
    for req in requirements {
        match req {
            CommitRequirement::AssertCreate => {
                return Err(IcebergRestError::Conflict(
                    "assert-create failed: table already exists".into(),
                ));
            }
            CommitRequirement::AssertTableUuid { uuid } => {
                let expected = synthesise_table_uuid(meta);
                if !uuid.eq_ignore_ascii_case(&expected) {
                    return Err(IcebergRestError::Conflict(format!(
                        "assert-table-uuid failed: expected `{expected}`, got `{uuid}`"
                    )));
                }
            }
            CommitRequirement::AssertCurrentSchemaId { current_schema_id } => {
                if *current_schema_id != 0 {
                    return Err(IcebergRestError::Conflict(format!(
                        "assert-current-schema-id failed: Basin tables are single-schema (id 0); \
                         got expected={current_schema_id}"
                    )));
                }
            }
            CommitRequirement::AssertRefSnapshotId {
                ref_name,
                snapshot_id,
            } => {
                if ref_name != "main" {
                    return Err(IcebergRestError::NotImplemented(format!(
                        "assert-ref-snapshot-id on ref `{ref_name}` is not supported; Basin v0.1 \
                         only models the `main` branch"
                    )));
                }
                let current = meta.current_snapshot.0 as i64;
                match snapshot_id {
                    Some(expected) if *expected != current => {
                        return Err(IcebergRestError::Conflict(format!(
                            "assert-ref-snapshot-id failed on `main`: expected {expected}, \
                             current is {current}"
                        )));
                    }
                    None => {
                        // Iceberg null = "ref must not exist". Basin
                        // always has `main` (genesis); reject as
                        // conflict so the caller reloads.
                        return Err(IcebergRestError::Conflict(format!(
                            "assert-ref-snapshot-id failed on `main`: caller expected no \
                             snapshot, current is {current}"
                        )));
                    }
                    _ => {}
                }
            }
            CommitRequirement::Other => {
                return Err(IcebergRestError::NotImplemented(
                    "unsupported commit requirement; Basin v0.1 only models `assert-table-uuid`, \
                     `assert-current-schema-id`, and `assert-ref-snapshot-id` on `main`"
                        .into(),
                ));
            }
        }
    }
    Ok(())
}

/// Walk `updates` and translate the supported subset onto Basin's
/// `Catalog::append_data_files` primitive. v0.1 honours:
///
/// - `add-snapshot` whose summary carries an explicit
///   `added-files-paths` (comma-separated object-store paths) plus
///   matching `added-files-count` / `added-records` /
///   `added-files-size` totals. The manifest-list URL is ignored because
///   Basin doesn't write Iceberg manifest files; see the README for the
///   wire-shape contract.
/// - `set-current-snapshot` is implicit — Basin's `append_data_files`
///   advances the head pointer atomically. If the caller sends
///   `set-current-snapshot` after `add-snapshot`, we accept it iff the
///   target is the new head (consistency), otherwise 409.
///
/// Anything else surfaces as 501.
async fn apply_commit(
    state: &State,
    tenant: &TenantId,
    table: &TableName,
    current: &BasinTableMetadata,
    updates: &[CommitUpdate],
) -> Result<(), IcebergRestError> {
    // First pass: classify and validate. We need the add-snapshot
    // payload before we issue the catalog call, and we want to surface
    // 501 for any unsupported action without having mutated the
    // catalog. Basin doesn't have a multi-update transactional API; we
    // collapse the supported subset (one add-snapshot + one
    // set-current-snapshot) into a single `append_data_files` call.
    let mut pending_files: Option<Vec<DataFileRef>> = None;
    let mut pending_set_current: Option<i64> = None;

    for update in updates {
        match update {
            CommitUpdate::AddSnapshot { snapshot } => {
                if pending_files.is_some() {
                    return Err(IcebergRestError::NotImplemented(
                        "multiple `add-snapshot` updates in a single commit are not supported \
                         by Basin v0.1"
                            .into(),
                    ));
                }
                pending_files = Some(extract_added_files(snapshot)?);
            }
            CommitUpdate::SetCurrentSnapshot { snapshot_id } => {
                if pending_set_current.is_some() {
                    return Err(IcebergRestError::NotImplemented(
                        "multiple `set-current-snapshot` updates in a single commit are not \
                         supported by Basin v0.1"
                            .into(),
                    ));
                }
                pending_set_current = Some(*snapshot_id);
            }
            CommitUpdate::Other => {
                return Err(IcebergRestError::NotImplemented(
                    "unsupported commit action; Basin v0.1 honours `add-snapshot` and \
                     `set-current-snapshot` only"
                        .into(),
                ));
            }
        }
    }

    let Some(files) = pending_files else {
        // No add-snapshot: a "metadata-only" commit (e.g. set-properties)
        // would land here. v0.1 doesn't model those.
        if pending_set_current.is_none() {
            return Err(IcebergRestError::NotImplemented(
                "commit with no `add-snapshot` action is not supported by Basin v0.1".into(),
            ));
        }
        return Err(IcebergRestError::NotImplemented(
            "`set-current-snapshot` without a paired `add-snapshot` is not supported by Basin \
             v0.1; the catalog needs the data-file delta to advance the head"
                .into(),
        ));
    };

    // Optimistic-concurrency boundary: feed the snapshot we read at
    // load_table into append_data_files. The catalog's CommitConflict
    // (mapped to 409 by `IcebergRestError::From<BasinError>`) is the
    // exact `CommitFailedException` Iceberg clients expect.
    let new_meta = state
        .catalog
        .append_data_files(tenant, table, current.current_snapshot, files)
        .await?;

    // If the caller explicitly asked for a `set-current-snapshot`,
    // sanity-check it matches the head we just produced. Mismatch
    // means the caller had a stale view; surface as conflict so it
    // reloads.
    if let Some(target) = pending_set_current {
        let new_head = new_meta.current_snapshot.0 as i64;
        if target != new_head {
            return Err(IcebergRestError::Conflict(format!(
                "set-current-snapshot failed: caller asked for {target}, post-commit head is \
                 {new_head}"
            )));
        }
    }
    Ok(())
}

/// Pull data-file paths out of an Iceberg snapshot summary and
/// synthesise [`DataFileRef`]s for them. Basin's catalog doesn't read
/// from Iceberg manifest-list files, so the v0.1 contract is:
///
/// - The snapshot's `summary` map MUST carry an
///   `added-files-paths` key whose value is a comma-separated list of
///   object-store paths (no scheme, no bucket prefix; same shape
///   `basin-storage` writes).
/// - `added-files-count` MUST equal the path count when present.
/// - Optional `added-records-per-file` (comma-separated u64s) and
///   `added-files-size-per-file` (comma-separated u64s) populate the
///   per-file `row_count` / `size_bytes` fields. When absent they
///   default to 0 (legal but lossy — readers won't have row counts /
///   sizes for these snapshots).
fn extract_added_files(snapshot: &IcebergSnapshot) -> Result<Vec<DataFileRef>, IcebergRestError> {
    if !snapshot.manifest_list.is_empty() {
        return Err(IcebergRestError::NotImplemented(
            "Iceberg `manifest-list` references are not supported by Basin v0.1; pass file \
             paths inline via the snapshot `summary.added-files-paths` key (comma-separated)"
                .into(),
        ));
    }

    let paths = match snapshot.summary.get("added-files-paths") {
        Some(s) if !s.is_empty() => s,
        _ => {
            return Err(IcebergRestError::NotImplemented(
                "Basin v0.1 commits require the snapshot `summary.added-files-paths` key (comma-\
                 separated object-store paths). Manifest-list-only snapshots are deferred."
                    .into(),
            ));
        }
    };
    let paths: Vec<&str> = paths
        .split(',')
        .map(|p| p.trim())
        .filter(|p| !p.is_empty())
        .collect();
    if paths.is_empty() {
        return Err(IcebergRestError::BadRequest(
            "`added-files-paths` parsed to zero paths".into(),
        ));
    }

    let row_counts = parse_per_file_u64(snapshot.summary.get("added-records-per-file"))?;
    let sizes = parse_per_file_u64(snapshot.summary.get("added-files-size-per-file"))?;
    if !row_counts.is_empty() && row_counts.len() != paths.len() {
        return Err(IcebergRestError::BadRequest(format!(
            "added-records-per-file count ({}) must match added-files-paths count ({})",
            row_counts.len(),
            paths.len(),
        )));
    }
    if !sizes.is_empty() && sizes.len() != paths.len() {
        return Err(IcebergRestError::BadRequest(format!(
            "added-files-size-per-file count ({}) must match added-files-paths count ({})",
            sizes.len(),
            paths.len(),
        )));
    }

    let mut out: Vec<DataFileRef> = Vec::with_capacity(paths.len());
    for (i, p) in paths.iter().enumerate() {
        out.push(DataFileRef {
            path: (*p).to_string(),
            size_bytes: sizes.get(i).copied().unwrap_or(0),
            row_count: row_counts.get(i).copied().unwrap_or(0),
            column_stats: Default::default(),
        });
    }
    Ok(out)
}

fn parse_per_file_u64(raw: Option<&String>) -> Result<Vec<u64>, IcebergRestError> {
    let Some(raw) = raw else {
        return Ok(Vec::new());
    };
    if raw.is_empty() {
        return Ok(Vec::new());
    }
    raw.split(',')
        .map(|p| p.trim())
        .filter(|p| !p.is_empty())
        .map(|p| {
            p.parse::<u64>().map_err(|_| {
                IcebergRestError::BadRequest(format!(
                    "per-file numeric summary value not a u64: `{p}`"
                ))
            })
        })
        .collect()
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
    let location = format!("{}{}/{}/", state.cfg.base_location, meta.tenant, meta.table);

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
            summary.insert(
                "added-files-count".into(),
                s.summary.added_files.to_string(),
            );
            summary.insert("added-records".into(), s.summary.added_rows.to_string());
            summary.insert("added-files-size".into(), s.summary.added_bytes.to_string());
            if s.summary.removed_files > 0 {
                summary.insert(
                    "deleted-files-count".into(),
                    s.summary.removed_files.to_string(),
                );
            }
            // Echo file paths back so a client doing
            // load → modify → commit can pick the paths off the prior
            // snapshot without a separate manifest fetch. Cheap (one
            // string allocation per snapshot) and the wire shape is the
            // same the commit-table handler accepts.
            if !s.data_files.is_empty() {
                summary.insert(
                    "added-files-paths".into(),
                    s.data_files
                        .iter()
                        .map(|f| f.path.as_str())
                        .collect::<Vec<_>>()
                        .join(","),
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
