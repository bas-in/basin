//! Integration tests for `basin-iceberg-rest`.
//!
//! Tests build a real `Router` over an in-memory catalog and drive it
//! via `tower::ServiceExt::oneshot`, which exercises the full axum
//! routing + extractor + handler stack without binding a port.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use axum::body::{to_bytes, Body};
use axum::http::{header, Method, Request, StatusCode};
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::{TableName, ProjectId};
use basin_iceberg_rest::{
    router_with_config, IcebergRestConfig, ListNamespacesResponse, ListTablesResponse,
    LoadTableResponse,
};
use tower::ServiceExt;

fn make_schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new(
            "created_at",
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
            true,
        ),
    ])
}

async fn seed_catalog(catalog: &Arc<dyn Catalog>, project: &ProjectId, tables: &[&str]) {
    catalog.create_namespace(project).await.unwrap();
    for t in tables {
        let table = TableName::new(*t).unwrap();
        catalog
            .create_table(project, &table, &make_schema())
            .await
            .unwrap();
    }
}

fn auth_router(catalog: Arc<dyn Catalog>) -> axum::Router {
    router_with_config(catalog, IcebergRestConfig::default())
}

fn open_router(catalog: Arc<dyn Catalog>) -> axum::Router {
    router_with_config(catalog, IcebergRestConfig::for_tests())
}

async fn body_json<T: serde::de::DeserializeOwned>(body: Body) -> T {
    let bytes = to_bytes(body, 1 << 20).await.unwrap();
    serde_json::from_slice(&bytes)
        .unwrap_or_else(|e| panic!("decode body: {e}; raw={}", String::from_utf8_lossy(&bytes)))
}

#[tokio::test]
async fn list_namespaces_returns_caller_project() {
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let alice = ProjectId::new();
    let bob = ProjectId::new();
    seed_catalog(&catalog, &alice, &["users"]).await;
    seed_catalog(&catalog, &bob, &["events"]).await;

    let app = auth_router(catalog);
    // Caller authenticates as Alice — sees only Alice's namespace.
    let req = Request::builder()
        .method(Method::GET)
        .uri("/v1/main/namespaces")
        .header(header::AUTHORIZATION, format!("Bearer {alice}"))
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body: ListNamespacesResponse = body_json(res.into_body()).await;
    assert_eq!(body.namespaces, vec![vec![alice.to_string()]]);

    // Caller authenticates as Bob — sees only Bob's namespace.
    let req = Request::builder()
        .method(Method::GET)
        .uri("/v1/main/namespaces")
        .header(header::AUTHORIZATION, format!("Bearer {bob}"))
        .body(Body::empty())
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body: ListNamespacesResponse = body_json(res.into_body()).await;
    assert_eq!(body.namespaces, vec![vec![bob.to_string()]]);
}

#[tokio::test]
async fn list_tables_in_namespace() {
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let project = ProjectId::new();
    seed_catalog(&catalog, &project, &["users", "orders"]).await;

    let app = auth_router(catalog);
    let req = Request::builder()
        .method(Method::GET)
        .uri(format!("/v1/main/namespaces/{project}/tables"))
        .header(header::AUTHORIZATION, format!("Bearer {project}"))
        .body(Body::empty())
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body: ListTablesResponse = body_json(res.into_body()).await;
    let names: Vec<String> = body.identifiers.iter().map(|i| i.name.clone()).collect();
    assert!(names.contains(&"users".to_string()));
    assert!(names.contains(&"orders".to_string()));
    for ident in &body.identifiers {
        assert_eq!(ident.namespace, vec![project.to_string()]);
    }
}

#[tokio::test]
async fn load_table_returns_iceberg_metadata() {
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let project = ProjectId::new();
    seed_catalog(&catalog, &project, &["users"]).await;

    let app = auth_router(catalog);
    let req = Request::builder()
        .method(Method::GET)
        .uri(format!("/v1/main/namespaces/{project}/tables/users"))
        .header(header::AUTHORIZATION, format!("Bearer {project}"))
        .body(Body::empty())
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body: LoadTableResponse = body_json(res.into_body()).await;

    assert_eq!(body.metadata.format_version, 2);
    assert!(body.metadata.location.contains(&project.to_string()));
    assert!(body.metadata.location.ends_with("/users/"));
    assert_eq!(body.metadata.schemas.len(), 1);
    assert_eq!(body.metadata.schemas[0].fields.len(), 3);
    assert_eq!(body.metadata.schemas[0].fields[0].name, "id");
    assert_eq!(body.metadata.schemas[0].fields[0].kind, "long");
    assert!(body.metadata.schemas[0].fields[0].required);
    assert_eq!(body.metadata.schemas[0].fields[1].name, "name");
    assert!(!body.metadata.schemas[0].fields[1].required);
    // Genesis snapshot is present.
    assert!(!body.metadata.snapshots.is_empty());
    assert!(body.metadata.refs.contains_key("main"));
    assert_eq!(body.metadata.current_snapshot_id, Some(0));
    // table-uuid is stable / parseable.
    assert!(uuid::Uuid::parse_str(&body.metadata.table_uuid).is_ok());
}

#[tokio::test]
async fn unauthorized_returns_401() {
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let project = ProjectId::new();
    seed_catalog(&catalog, &project, &["users"]).await;

    let app = auth_router(catalog);

    // No bearer.
    let req = Request::builder()
        .method(Method::GET)
        .uri(format!("/v1/main/namespaces/{project}/tables"))
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);

    // Wrong scheme.
    let req = Request::builder()
        .method(Method::GET)
        .uri(format!("/v1/main/namespaces/{project}/tables"))
        .header(header::AUTHORIZATION, "Basic abc")
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);

    // Empty bearer.
    let req = Request::builder()
        .method(Method::GET)
        .uri(format!("/v1/main/namespaces/{project}/tables"))
        .header(header::AUTHORIZATION, "Bearer ")
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);

    // Non-ULID bearer.
    let req = Request::builder()
        .method(Method::GET)
        .uri(format!("/v1/main/namespaces/{project}/tables"))
        .header(header::AUTHORIZATION, "Bearer not-a-ulid")
        .body(Body::empty())
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn load_nonexistent_table_returns_404() {
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let project = ProjectId::new();
    seed_catalog(&catalog, &project, &[]).await;

    let app = auth_router(catalog);
    let req = Request::builder()
        .method(Method::GET)
        .uri(format!("/v1/main/namespaces/{project}/tables/missing"))
        .header(header::AUTHORIZATION, format!("Bearer {project}"))
        .body(Body::empty())
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
    let bytes = to_bytes(res.into_body(), 1 << 20).await.unwrap();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v["error"]["code"], 404);
    assert_eq!(v["error"]["type"], "NoSuchTableException");
}

#[tokio::test]
async fn cross_project_isolation() {
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let alice = ProjectId::new();
    let bob = ProjectId::new();
    seed_catalog(&catalog, &alice, &["users"]).await;
    seed_catalog(&catalog, &bob, &["events"]).await;

    let app = auth_router(catalog);
    // Bob tries to list Alice's tables — must be rejected even though
    // both projects exist.
    let req = Request::builder()
        .method(Method::GET)
        .uri(format!("/v1/main/namespaces/{alice}/tables"))
        .header(header::AUTHORIZATION, format!("Bearer {bob}"))
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);

    // And the same on load_table.
    let req = Request::builder()
        .method(Method::GET)
        .uri(format!("/v1/main/namespaces/{alice}/tables/users"))
        .header(header::AUTHORIZATION, format!("Bearer {bob}"))
        .body(Body::empty())
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn drop_table_round_trip() {
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let project = ProjectId::new();
    seed_catalog(&catalog, &project, &["users"]).await;

    let app = auth_router(catalog.clone());
    let req = Request::builder()
        .method(Method::DELETE)
        .uri(format!("/v1/main/namespaces/{project}/tables/users"))
        .header(header::AUTHORIZATION, format!("Bearer {project}"))
        .body(Body::empty())
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT);
    // Table is gone from the underlying catalog.
    let names = catalog.list_tables(&project).await.unwrap();
    assert!(names.is_empty());
}

#[tokio::test]
async fn create_table_round_trip() {
    // POST a CreateTableRequest with three fields (one required, one
    // nullable, one Decimal128). A subsequent GET load-table returns
    // metadata with the same column shape and the same `table-uuid`.
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let project = ProjectId::new();
    seed_catalog(&catalog, &project, &[]).await;

    let app = auth_router(catalog);
    let body = serde_json::json!({
        "name": "events",
        "schema": {
            "type": "struct",
            "fields": [
                { "id": 1, "name": "event_id", "type": "long",   "required": true },
                { "id": 2, "name": "label",    "type": "string", "required": false },
                { "id": 3, "name": "ts",       "type": "timestamptz", "required": true }
            ]
        }
    })
    .to_string();
    let req = Request::builder()
        .method(Method::POST)
        .uri(format!("/v1/main/namespaces/{project}/tables"))
        .header(header::AUTHORIZATION, format!("Bearer {project}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body))
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let created: LoadTableResponse = body_json(res.into_body()).await;
    assert_eq!(created.metadata.schemas[0].fields.len(), 3);
    assert_eq!(created.metadata.schemas[0].fields[0].name, "event_id");
    assert_eq!(created.metadata.schemas[0].fields[0].kind, "long");
    assert!(created.metadata.schemas[0].fields[0].required);
    assert_eq!(created.metadata.schemas[0].fields[1].name, "label");
    assert!(!created.metadata.schemas[0].fields[1].required);

    // Same uuid via GET load-table.
    let req = Request::builder()
        .method(Method::GET)
        .uri(format!("/v1/main/namespaces/{project}/tables/events"))
        .header(header::AUTHORIZATION, format!("Bearer {project}"))
        .body(Body::empty())
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let loaded: LoadTableResponse = body_json(res.into_body()).await;
    assert_eq!(loaded.metadata.table_uuid, created.metadata.table_uuid);
    assert_eq!(loaded.metadata.schemas[0].fields.len(), 3);
}

#[tokio::test]
async fn create_table_with_decimal_type() {
    // Decimal128(p,s) round-trips through the create-table flow.
    // Depends on Decimal128 having landed in the engine's type
    // mapping; if the workspace doesn't yet support it, the
    // load-table response will round-trip as `decimal(P,S)`.
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let project = ProjectId::new();
    seed_catalog(&catalog, &project, &[]).await;

    let app = auth_router(catalog);
    let body = serde_json::json!({
        "name": "ledger",
        "schema": {
            "type": "struct",
            "fields": [
                { "id": 1, "name": "id",     "type": "long",          "required": true },
                { "id": 2, "name": "amount", "type": "decimal(10,2)", "required": false }
            ]
        }
    })
    .to_string();
    let req = Request::builder()
        .method(Method::POST)
        .uri(format!("/v1/main/namespaces/{project}/tables"))
        .header(header::AUTHORIZATION, format!("Bearer {project}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body))
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let created: LoadTableResponse = body_json(res.into_body()).await;
    let amount = &created.metadata.schemas[0].fields[1];
    assert_eq!(amount.name, "amount");
    assert_eq!(amount.kind, "decimal(10,2)");
    assert!(!amount.required);
}

#[tokio::test]
async fn create_table_unsupported_type_returns_501() {
    // `fixed` and `list` aren't supported by Basin v0.1; the handler
    // returns a structured 501 with `NotImplementedException`.
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let project = ProjectId::new();
    seed_catalog(&catalog, &project, &[]).await;

    let app = auth_router(catalog);
    let body = serde_json::json!({
        "name": "events",
        "schema": {
            "type": "struct",
            "fields": [
                { "id": 1, "name": "blob", "type": "fixed(16)", "required": true }
            ]
        }
    })
    .to_string();
    let req = Request::builder()
        .method(Method::POST)
        .uri(format!("/v1/main/namespaces/{project}/tables"))
        .header(header::AUTHORIZATION, format!("Bearer {project}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body))
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::NOT_IMPLEMENTED);
    let bytes = to_bytes(res.into_body(), 1 << 20).await.unwrap();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v["error"]["code"], 501);
    assert_eq!(v["error"]["type"], "NotImplementedException");
}

#[tokio::test]
async fn commit_table_assert_uuid_passes() {
    // Round-trip: load-table to read the synthesised UUID, send a
    // commit-table with `assert-table-uuid` referencing it. Adds one
    // file via `summary.added-files-paths`. Post-commit head advances.
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let project = ProjectId::new();
    seed_catalog(&catalog, &project, &["users"]).await;

    let app = auth_router(catalog);
    // Read the synthesised UUID off the GET endpoint.
    let req = Request::builder()
        .method(Method::GET)
        .uri(format!("/v1/main/namespaces/{project}/tables/users"))
        .header(header::AUTHORIZATION, format!("Bearer {project}"))
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let loaded: LoadTableResponse = body_json(res.into_body()).await;
    let uuid = loaded.metadata.table_uuid.clone();

    let body = serde_json::json!({
        "requirements": [
            { "type": "assert-table-uuid", "uuid": uuid },
            { "type": "assert-current-schema-id", "current-schema-id": 0 },
            { "type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": 0 }
        ],
        "updates": [
            {
                "action": "add-snapshot",
                "snapshot": {
                    "snapshot-id": 1,
                    "parent-snapshot-id": 0,
                    "sequence-number": 1,
                    "timestamp-ms": 0,
                    "summary": {
                        "operation": "append",
                        "added-files-count": "1",
                        "added-files-paths": "users/data/a.parquet",
                        "added-records-per-file": "10",
                        "added-files-size-per-file": "1024"
                    },
                    "schema-id": 0
                }
            }
        ]
    })
    .to_string();
    let req = Request::builder()
        .method(Method::POST)
        .uri(format!("/v1/main/namespaces/{project}/tables/users"))
        .header(header::AUTHORIZATION, format!("Bearer {project}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body))
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let after: LoadTableResponse = body_json(res.into_body()).await;
    assert_eq!(after.metadata.current_snapshot_id, Some(1));
    // The `added-files-paths` echo round-trips so a second commit can
    // chain off this snapshot without re-fetching the manifest list.
    let head = after
        .metadata
        .snapshots
        .iter()
        .find(|s| s.snapshot_id == 1)
        .unwrap();
    assert_eq!(
        head.summary.get("added-files-paths").map(String::as_str),
        Some("users/data/a.parquet"),
    );
}

#[tokio::test]
async fn commit_table_assert_uuid_fails_returns_409() {
    // Wrong uuid → CommitFailedException-shaped 409 envelope.
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let project = ProjectId::new();
    seed_catalog(&catalog, &project, &["users"]).await;

    let app = auth_router(catalog);
    let body = serde_json::json!({
        "requirements": [
            { "type": "assert-table-uuid", "uuid": "00000000-0000-0000-0000-000000000000" }
        ],
        "updates": []
    })
    .to_string();
    let req = Request::builder()
        .method(Method::POST)
        .uri(format!("/v1/main/namespaces/{project}/tables/users"))
        .header(header::AUTHORIZATION, format!("Bearer {project}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body))
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::CONFLICT);
    let bytes = to_bytes(res.into_body(), 1 << 20).await.unwrap();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v["error"]["code"], 409);
}

#[tokio::test]
async fn commit_table_set_current_snapshot() {
    // Multi-step: create-table via the REST surface, append data files
    // via commit-table with `add-snapshot` + `set-current-snapshot`,
    // then load-table sees the new head.
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let project = ProjectId::new();
    seed_catalog(&catalog, &project, &["users"]).await;

    let app = auth_router(catalog);
    let body = serde_json::json!({
        "requirements": [
            { "type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": 0 }
        ],
        "updates": [
            {
                "action": "add-snapshot",
                "snapshot": {
                    "snapshot-id": 1,
                    "parent-snapshot-id": 0,
                    "timestamp-ms": 0,
                    "summary": {
                        "operation": "append",
                        "added-files-count": "2",
                        "added-files-paths": "users/data/a.parquet,users/data/b.parquet",
                        "added-records-per-file": "10,20",
                        "added-files-size-per-file": "1024,2048"
                    }
                }
            },
            { "action": "set-current-snapshot", "snapshot-id": 1 }
        ]
    })
    .to_string();
    let req = Request::builder()
        .method(Method::POST)
        .uri(format!("/v1/main/namespaces/{project}/tables/users"))
        .header(header::AUTHORIZATION, format!("Bearer {project}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body))
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // load-table sees current = 1, refs.main = 1, with both files.
    let req = Request::builder()
        .method(Method::GET)
        .uri(format!("/v1/main/namespaces/{project}/tables/users"))
        .header(header::AUTHORIZATION, format!("Bearer {project}"))
        .body(Body::empty())
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    let loaded: LoadTableResponse = body_json(res.into_body()).await;
    assert_eq!(loaded.metadata.current_snapshot_id, Some(1));
    assert_eq!(loaded.metadata.refs["main"].snapshot_id, 1);
    let head = loaded
        .metadata
        .snapshots
        .iter()
        .find(|s| s.snapshot_id == 1)
        .unwrap();
    let paths = head.summary.get("added-files-paths").unwrap();
    assert!(paths.contains("a.parquet") && paths.contains("b.parquet"));
}

#[tokio::test]
async fn commit_table_unsupported_action_returns_501() {
    // `add-sort-order` isn't modelled in v0.1; reject as 501.
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let project = ProjectId::new();
    seed_catalog(&catalog, &project, &["users"]).await;

    let app = auth_router(catalog);
    let body = serde_json::json!({
        "requirements": [],
        "updates": [
            { "action": "add-sort-order", "sort-order": { "order-id": 1, "fields": [] } }
        ]
    })
    .to_string();
    let req = Request::builder()
        .method(Method::POST)
        .uri(format!("/v1/main/namespaces/{project}/tables/users"))
        .header(header::AUTHORIZATION, format!("Bearer {project}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body))
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::NOT_IMPLEMENTED);
    let bytes = to_bytes(res.into_body(), 1 << 20).await.unwrap();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v["error"]["type"], "NotImplementedException");
}

#[tokio::test]
async fn register_table_returns_501() {
    // POST /register isn't supported by Basin v0.1.
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let project = ProjectId::new();
    seed_catalog(&catalog, &project, &[]).await;

    let app = auth_router(catalog);
    let body = serde_json::json!({
        "name": "external_table",
        "metadata-location": "s3://bucket/path/to/metadata.json"
    })
    .to_string();
    let req = Request::builder()
        .method(Method::POST)
        .uri(format!("/v1/main/namespaces/{project}/register"))
        .header(header::AUTHORIZATION, format!("Bearer {project}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body))
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::NOT_IMPLEMENTED);
    let bytes = to_bytes(res.into_body(), 1 << 20).await.unwrap();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v["error"]["type"], "NotImplementedException");
    let msg = v["error"]["message"].as_str().unwrap();
    assert!(msg.contains("external_table"));
}

#[tokio::test]
async fn open_router_skips_auth() {
    // Sanity: the for_tests config disables auth. Useful for the
    // future pyiceberg test crate that wants to drive the surface
    // without crafting bearer tokens.
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let project = ProjectId::new();
    seed_catalog(&catalog, &project, &["users"]).await;

    let app = open_router(catalog);
    let req = Request::builder()
        .method(Method::GET)
        .uri(format!("/v1/main/namespaces/{project}/tables"))
        .body(Body::empty())
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}
