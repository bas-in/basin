//! Backup and restore admin routes.
//!
//! ## Surface
//!
//! | Verb + Path                                          | Action            | Status |
//! |------------------------------------------------------|-------------------|--------|
//! | `POST /admin/v1/projects/:project_id/snapshot`       | capture head      | 200    |
//! | `POST /admin/v1/projects/:project_id/restore`        | restore / PITR    | 200    |
//!
//! ## Auth
//!
//! Same admin gate as all `/admin/v1/*` routes — every request requires a JWT
//! with `is_admin: true`. The `project_id` claim in the token must match the
//! `:project_id` path segment.
//!
//! ## Snapshot wire shape
//!
//! `POST /admin/v1/projects/:project_id/snapshot` takes no body and returns:
//! ```json
//! {
//!   "engine_snapshot_id": "<opaque base64 string>",
//!   "size_bytes": 0,
//!   "created_at": "2026-01-01T00:00:00Z"
//! }
//! ```
//!
//! The `engine_snapshot_id` is an opaque string encoding a per-table map
//! `{ table_name → snapshot_id }` serialised as JSON and base64-encoded.
//! The cloud stores it verbatim and hands it back on a named-snapshot restore.
//!
//! `size_bytes` is best-effort (currently always 0 — the catalog does not
//! record per-snapshot byte counts at this layer).
//!
//! ## Restore wire shape
//!
//! `POST /admin/v1/projects/:project_id/restore` body (exactly one field must
//! be present):
//!
//! Named-snapshot restore:
//! ```json
//! { "engine_snapshot_id": "<opaque string from create_snapshot>" }
//! ```
//!
//! Point-in-time restore:
//! ```json
//! { "as_of": "2026-01-01T00:00:00Z" }
//! ```
//!
//! Success response:
//! ```json
//! { "ok": true, "restored_tables": 3 }
//! ```

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use axum::Json;
use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use basin_catalog::SnapshotId;
use basin_common::{ProjectId, TableName};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::errors::ApiError;
use crate::server::{authorize, Inner};

// ---------------------------------------------------------------------------
// Admin gate helpers (mirrors admin_projects.rs)
// ---------------------------------------------------------------------------

fn require_admin(claims: &basin_auth::Claims) -> Result<(), ApiError> {
    if !claims.is_admin {
        return Err(ApiError::unauthenticated(
            "admin endpoint requires `is_admin: true` claim",
        ));
    }
    Ok(())
}

fn assert_admin_for_path_project(
    claims: &basin_auth::Claims,
    path_project_id: &ProjectId,
) -> Result<(), ApiError> {
    // The control-plane super-admin token — scoped to INTERNAL_AUTH_PROJECT_ID —
    // may operate on any project. That is the single deploy-time `is_admin`
    // token the cloud control plane mints to provision and administer every
    // project (see `basin_auth::Claims::is_admin` docs); without this it could
    // create projects (the unscoped `POST /admin/v1/projects` route) but not
    // snapshot or restore them, which are path-scoped.
    if claims.project_id.to_string() == basin_auth::INTERNAL_AUTH_PROJECT_ID {
        return Ok(());
    }
    if claims.project_id != *path_project_id {
        return Err(ApiError::forbidden(format!(
            "admin token scoped to project {} cannot operate on project {}",
            claims.project_id, path_project_id,
        )));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Snapshot ID encoding / decoding
//
// The `engine_snapshot_id` is an opaque string handed to the cloud control
// plane. Internally it is base64(JSON({ "table" -> snapshot_id_u64 })). Using
// a well-known JSON shape lets us add fields later without breaking the decode
// path.
// ---------------------------------------------------------------------------

/// Encode a per-table snapshot-id map into an opaque engine_snapshot_id string.
fn encode_snapshot_id(map: &BTreeMap<TableName, SnapshotId>) -> String {
    // BTreeMap<TableName, SnapshotId> → JSON object {"<table>": <u64>}
    let obj: serde_json::Map<String, serde_json::Value> = map
        .iter()
        .map(|(t, s)| (t.as_str().to_string(), json!(s.0)))
        .collect();
    let json_bytes = serde_json::to_vec(&obj).unwrap_or_default();
    B64.encode(&json_bytes)
}

/// Decode an opaque engine_snapshot_id back into the per-table map.
/// Returns `ApiError::invalid` on any decode failure.
fn decode_snapshot_id(s: &str) -> Result<BTreeMap<TableName, SnapshotId>, ApiError> {
    let bytes = B64
        .decode(s)
        .map_err(|_| ApiError::invalid("engine_snapshot_id: invalid base64"))?;
    let obj: serde_json::Map<String, serde_json::Value> = serde_json::from_slice(&bytes)
        .map_err(|_| ApiError::invalid("engine_snapshot_id: invalid JSON payload"))?;
    let mut map = BTreeMap::new();
    for (k, v) in obj {
        let table = TableName::new(&k)
            .map_err(|e| ApiError::invalid(format!("engine_snapshot_id: bad table name {k:?}: {e}")))?;
        let id_u64 = v
            .as_u64()
            .ok_or_else(|| ApiError::invalid(format!("engine_snapshot_id: snapshot id for {k} must be a u64")))?;
        map.insert(table, SnapshotId(id_u64));
    }
    Ok(map)
}

// ---------------------------------------------------------------------------
// POST /admin/v1/projects/:project_id/snapshot
// ---------------------------------------------------------------------------

/// `POST /admin/v1/projects/:project_id/snapshot` response.
#[derive(Debug, Serialize)]
pub(crate) struct CreateSnapshotResponse {
    pub engine_snapshot_id: String,
    pub size_bytes: u64,
    pub created_at: String,
}

/// Capture the current per-table snapshot head for `project`.
///
/// Calls [`Catalog::list_snapshots_project_wide`] to collect the current head
/// `snapshot_id` for every table the project owns, serialises the
/// `{ table → snapshot_id }` map as an opaque base64/JSON string, and
/// returns it as `engine_snapshot_id`. The cloud stores this and hands it back
/// to [`restore_project`] when the user requests a named-snapshot restore.
///
/// `size_bytes` is returned as 0 — the catalog does not record per-snapshot
/// byte counts at this layer; the cloud may compute its own estimate from
/// object-store metrics.
///
/// `created_at` is the server-local UTC wall clock at the moment of capture.
#[axum::debug_handler]
pub(crate) async fn create_project_snapshot(
    State(state): State<Arc<Inner>>,
    headers: HeaderMap,
    Path(project_id): Path<String>,
) -> Result<Response, ApiError> {
    let claims = authorize(&state, &headers).await?;
    require_admin(&claims)?;

    let project: ProjectId = project_id
        .parse()
        .map_err(|e| ApiError::invalid(format!("invalid project_id: {e}")))?;
    assert_admin_for_path_project(&claims, &project)?;

    let catalog = state.cfg.engine.config().catalog.clone();

    // Collect the project-wide snapshot timeline (all tables, all snapshots,
    // sorted by committed_at ascending). The head for each table is the entry
    // with the largest committed_at (since entries are sorted ascending, the
    // last entry per table is the head).
    let entries = catalog
        .list_snapshots_project_wide(&project)
        .await
        .map_err(ApiError::from)?;

    // Build a per-table head map: last entry wins (sorted ascending by
    // committed_at, so the final entry per table IS the current head).
    let mut head: BTreeMap<TableName, SnapshotId> = BTreeMap::new();
    for entry in entries {
        // Overwrite with each successive entry — later entries are newer.
        head.insert(entry.table, entry.snapshot_id);
    }

    let created_at = Utc::now();
    let engine_snapshot_id = encode_snapshot_id(&head);

    tracing::info!(
        %project,
        tables = head.len(),
        "project-wide snapshot captured",
    );

    Ok(Json(CreateSnapshotResponse {
        engine_snapshot_id,
        size_bytes: 0,
        created_at: created_at.to_rfc3339(),
    })
    .into_response())
}

// ---------------------------------------------------------------------------
// POST /admin/v1/projects/:project_id/restore
// ---------------------------------------------------------------------------

/// `POST /admin/v1/projects/:project_id/restore` body.
///
/// Exactly one of `engine_snapshot_id` or `as_of` must be present.
#[derive(Debug, Deserialize)]
pub(crate) struct RestoreRequest {
    /// Named-snapshot restore: the opaque string returned by
    /// `POST /admin/v1/projects/:id/snapshot`. Decoded to a per-table
    /// `{ table → snapshot_id }` map; `rollback_to_snapshot` is called per
    /// table.
    #[serde(default)]
    pub engine_snapshot_id: Option<String>,

    /// Point-in-time restore: RFC-3339 timestamp. All tables are rewound to
    /// the latest snapshot whose `committed_at ≤ as_of` via
    /// `rollback_to_snapshot_project_wide`.
    #[serde(default)]
    pub as_of: Option<String>,
}

/// Restore a project to a named snapshot or a point in time.
///
/// ## Named-snapshot restore
///
/// When `engine_snapshot_id` is provided, the handler decodes the per-table
/// snapshot map and calls [`Catalog::rollback_to_snapshot`] for each table.
/// Tables that appear in the map but have since been dropped are skipped
/// (NotFound is treated as a no-op).
///
/// ## Point-in-time restore
///
/// When `as_of` is provided, the handler calls
/// [`Catalog::rollback_to_snapshot_project_wide`] which rewinds every current
/// table to the latest snapshot whose `committed_at ≤ as_of`. Tables created
/// after `as_of` have no qualifying snapshot and are left alone.
#[axum::debug_handler]
pub(crate) async fn restore_project(
    State(state): State<Arc<Inner>>,
    headers: HeaderMap,
    Path(project_id): Path<String>,
    Json(req): Json<RestoreRequest>,
) -> Result<Response, ApiError> {
    let claims = authorize(&state, &headers).await?;
    require_admin(&claims)?;

    let project: ProjectId = project_id
        .parse()
        .map_err(|e| ApiError::invalid(format!("invalid project_id: {e}")))?;
    assert_admin_for_path_project(&claims, &project)?;

    // Validate that exactly one restore mode is specified.
    match (&req.engine_snapshot_id, &req.as_of) {
        (None, None) => {
            return Err(ApiError::invalid(
                "exactly one of `engine_snapshot_id` or `as_of` must be provided",
            ));
        }
        (Some(_), Some(_)) => {
            return Err(ApiError::invalid(
                "only one of `engine_snapshot_id` or `as_of` may be provided, not both",
            ));
        }
        _ => {}
    }

    let catalog = state.cfg.engine.config().catalog.clone();

    if let Some(ref snap_id_str) = req.engine_snapshot_id {
        // --- Named-snapshot restore -------------------------------------------
        if snap_id_str.is_empty() {
            return Err(ApiError::invalid("engine_snapshot_id must not be empty"));
        }
        let snap_map = decode_snapshot_id(snap_id_str)?;
        if snap_map.is_empty() {
            return Err(ApiError::invalid(
                "engine_snapshot_id encodes an empty snapshot (no tables)",
            ));
        }

        let mut restored: usize = 0;
        for (table, snapshot_id) in &snap_map {
            match catalog
                .rollback_to_snapshot(&project, table, *snapshot_id)
                .await
            {
                Ok(_) => {
                    restored += 1;
                }
                // Table was dropped after the snapshot was taken — skip it.
                Err(basin_common::BasinError::NotFound(_)) => {}
                Err(e) => return Err(ApiError::from(e)),
            }
        }

        tracing::info!(
            %project,
            restored_tables = restored,
            "named-snapshot restore complete",
        );

        Ok(Json(json!({
            "ok": true,
            "restored_tables": restored,
        }))
        .into_response())
    } else {
        // --- Point-in-time restore --------------------------------------------
        let as_of_str = req.as_of.as_deref().unwrap_or("");
        if as_of_str.is_empty() {
            return Err(ApiError::invalid("as_of must not be empty"));
        }
        let as_of = chrono::DateTime::parse_from_rfc3339(as_of_str)
            .map(|dt| dt.with_timezone(&Utc))
            .map_err(|e| {
                ApiError::invalid(format!("as_of: invalid RFC-3339 timestamp: {e}"))
            })?;

        let results = catalog
            .rollback_to_snapshot_project_wide(&project, as_of)
            .await
            .map_err(ApiError::from)?;

        let restored = results.len();
        tracing::info!(
            %project,
            restored_tables = restored,
            as_of = %as_of_str,
            "project-wide PITR restore complete",
        );

        Ok(Json(json!({
            "ok": true,
            "restored_tables": restored,
        }))
        .into_response())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use basin_catalog::{Catalog as _, InMemoryCatalog, SnapshotId};
    use basin_common::{ProjectId, TableName};

    // -------------------------------------------------------------------------
    // encode / decode round-trip
    // -------------------------------------------------------------------------

    #[test]
    fn snapshot_id_encode_decode_roundtrip() {
        let mut map: BTreeMap<TableName, SnapshotId> = BTreeMap::new();
        map.insert(TableName::new("orders").unwrap(), SnapshotId(3));
        map.insert(TableName::new("users").unwrap(), SnapshotId(7));

        let encoded = encode_snapshot_id(&map);
        let decoded = decode_snapshot_id(&encoded).expect("decode should succeed");

        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[&TableName::new("orders").unwrap()], SnapshotId(3));
        assert_eq!(decoded[&TableName::new("users").unwrap()], SnapshotId(7));
    }

    #[test]
    fn snapshot_id_empty_map_encodes_and_decodes() {
        let map: BTreeMap<TableName, SnapshotId> = BTreeMap::new();
        let encoded = encode_snapshot_id(&map);
        let decoded = decode_snapshot_id(&encoded).expect("decode");
        assert!(decoded.is_empty());
    }

    #[test]
    fn snapshot_id_decode_rejects_garbage() {
        assert!(decode_snapshot_id("not-valid-base64!!!!").is_err());
        // Valid base64 but not JSON.
        let bad = B64.encode(b"hello world");
        assert!(decode_snapshot_id(&bad).is_err());
    }

    // -------------------------------------------------------------------------
    // Catalog round-trips — snapshot capture and named restore
    // -------------------------------------------------------------------------

    /// Helper: create a table, insert one batch (append), return table name.
    async fn seed_table(
        cat: &Arc<InMemoryCatalog>,
        project: &ProjectId,
        name: &str,
    ) -> TableName {
        use arrow_schema::{DataType, Field, Schema};
        let table = TableName::new(name).unwrap();
        let schema = Schema::new(vec![Field::new("v", DataType::Int64, false)]);
        cat.create_table(project, &table, &schema)
            .await
            .expect("create_table");
        table
    }

    #[tokio::test]
    async fn named_snapshot_restore_via_catalog() {
        use basin_catalog::DataFileRef;

        let cat = Arc::new(InMemoryCatalog::new());
        let project = ProjectId::new();

        // 1. Create two tables and advance each past genesis.
        let t_orders = seed_table(&cat, &project, "orders").await;
        let t_users = seed_table(&cat, &project, "users").await;

        // Append a dummy file to each table so they're past genesis.
        let dummy = DataFileRef {
            path: "a.parquet".to_string(),
            row_count: 1,
            size_bytes: 100,
            column_stats: Default::default(),
            bloom_filters: Default::default(),
            hll_sketches: Default::default(),
            tdigest_sketches: Default::default(),
        };
        let meta_orders = cat
            .append_data_files(&project, &t_orders, SnapshotId::GENESIS, vec![dummy.clone()])
            .await
            .expect("append orders");
        let meta_users = cat
            .append_data_files(&project, &t_users, SnapshotId::GENESIS, vec![dummy.clone()])
            .await
            .expect("append users");

        // 2. Capture the "snapshot" (head map) at this point.
        let entries = cat
            .list_snapshots_project_wide(&project)
            .await
            .expect("list_snapshots");
        let mut head: BTreeMap<TableName, SnapshotId> = BTreeMap::new();
        for entry in entries {
            head.insert(entry.table, entry.snapshot_id);
        }
        let snap_id_str = encode_snapshot_id(&head);

        // Verify the heads are what we expect (snapshot 1 for each table).
        assert_eq!(head[&t_orders], meta_orders.current_snapshot);
        assert_eq!(head[&t_users], meta_users.current_snapshot);

        // 3. Mutate: append another file to orders.
        let dummy2 = DataFileRef {
            path: "b.parquet".to_string(),
            row_count: 1,
            size_bytes: 100,
            column_stats: Default::default(),
            bloom_filters: Default::default(),
            hll_sketches: Default::default(),
            tdigest_sketches: Default::default(),
        };
        cat.append_data_files(
            &project,
            &t_orders,
            meta_orders.current_snapshot,
            vec![dummy2],
        )
        .await
        .expect("append orders second");

        // Confirm orders is now at snapshot 2.
        let current = cat.load_table(&project, &t_orders).await.expect("load");
        assert!(current.current_snapshot.0 > meta_orders.current_snapshot.0);

        // 4. Restore via named-snapshot.
        let snap_map = decode_snapshot_id(&snap_id_str).expect("decode");
        for (table, snapshot_id) in &snap_map {
            cat.rollback_to_snapshot(&project, table, *snapshot_id)
                .await
                .expect("rollback");
        }

        // 5. Confirm orders rolled back to snapshot 1.
        let after = cat.load_table(&project, &t_orders).await.expect("load after");
        assert_eq!(
            after.current_snapshot, meta_orders.current_snapshot,
            "orders should be back at the captured head"
        );
    }

    #[tokio::test]
    async fn pitr_restore_via_catalog() {
        use basin_catalog::DataFileRef;

        let cat = Arc::new(InMemoryCatalog::new());
        let project = ProjectId::new();

        let table = seed_table(&cat, &project, "events").await;

        let dummy = DataFileRef {
            path: "ev1.parquet".to_string(),
            row_count: 10,
            size_bytes: 200,
            column_stats: Default::default(),
            bloom_filters: Default::default(),
            hll_sketches: Default::default(),
            tdigest_sketches: Default::default(),
        };
        let snap1 = cat
            .append_data_files(&project, &table, SnapshotId::GENESIS, vec![dummy])
            .await
            .expect("append 1");

        // Record the timestamp after snap1 but before snap2.
        let cutoff = Utc::now();

        // A small sleep is not possible in a unit test, but since we're testing
        // the catalog's as-of logic: the cutoff is after snap1's committed_at
        // (both happen "now" but snap1 was committed first, so snap1.committed_at
        // <= cutoff is guaranteed when the wall clock advances even by 1ms).
        // The test is structured to be correct even without a sleep: we capture
        // `cutoff` AFTER snap1 completes, so snap1.committed_at <= cutoff.

        let dummy2 = DataFileRef {
            path: "ev2.parquet".to_string(),
            row_count: 5,
            size_bytes: 100,
            column_stats: Default::default(),
            bloom_filters: Default::default(),
            hll_sketches: Default::default(),
            tdigest_sketches: Default::default(),
        };
        let _snap2 = cat
            .append_data_files(&project, &table, snap1.current_snapshot, vec![dummy2])
            .await
            .expect("append 2");

        // Project-wide PITR to `cutoff`: should land at snap1 (or genesis if
        // snap1.committed_at > cutoff, which in theory cannot happen since we
        // captured cutoff after snap1 returned, but we allow either result and
        // only assert we did not stay at snap2).
        let results = cat
            .rollback_to_snapshot_project_wide(&project, cutoff)
            .await
            .expect("pitr");
        assert_eq!(results.len(), 1, "one table should have been rolled back");
        // The rolled-back snapshot must not be snap2.
        let (_, restored_id) = &results[0];
        let snap2_head = cat.load_table(&project, &table).await.expect("load");
        // After rollback the current snapshot is whichever one had committed_at <= cutoff.
        // We just assert it is NOT the post-cutoff snap2 (which has 2 data files).
        let files = snap2_head
            .snapshots
            .iter()
            .find(|s| s.id == *restored_id)
            .map(|s| s.data_files.len())
            .unwrap_or(0);
        // snap2 has 1 data file (the delta b file, in the snapshot-delta model).
        // The restored snapshot (snap1) has 1 data file too, but it is the first delta.
        // Genesis has 0 files. Either way, we must not be at snap2 (id > snap1).
        assert!(
            *restored_id <= snap1.current_snapshot,
            "PITR must not land past the cutoff: restored={restored_id:?}, snap1={:?}",
            snap1.current_snapshot,
        );
    }
}
