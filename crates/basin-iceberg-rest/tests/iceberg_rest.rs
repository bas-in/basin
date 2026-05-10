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
use basin_common::{TableName, TenantId};
use basin_iceberg_rest::{
    router_with_config, IcebergRestConfig, ListNamespacesResponse, ListTablesResponse,
    LoadTableResponse,
};
use tower::ServiceExt;

fn make_schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("created_at", DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None), true),
    ])
}

async fn seed_catalog(catalog: &Arc<dyn Catalog>, tenant: &TenantId, tables: &[&str]) {
    catalog.create_namespace(tenant).await.unwrap();
    for t in tables {
        let table = TableName::new(*t).unwrap();
        catalog
            .create_table(tenant, &table, &make_schema())
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
async fn list_namespaces_returns_caller_tenant() {
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let alice = TenantId::new();
    let bob = TenantId::new();
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
    let tenant = TenantId::new();
    seed_catalog(&catalog, &tenant, &["users", "orders"]).await;

    let app = auth_router(catalog);
    let req = Request::builder()
        .method(Method::GET)
        .uri(format!("/v1/main/namespaces/{tenant}/tables"))
        .header(header::AUTHORIZATION, format!("Bearer {tenant}"))
        .body(Body::empty())
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body: ListTablesResponse = body_json(res.into_body()).await;
    let names: Vec<String> = body.identifiers.iter().map(|i| i.name.clone()).collect();
    assert!(names.contains(&"users".to_string()));
    assert!(names.contains(&"orders".to_string()));
    for ident in &body.identifiers {
        assert_eq!(ident.namespace, vec![tenant.to_string()]);
    }
}

#[tokio::test]
async fn load_table_returns_iceberg_metadata() {
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let tenant = TenantId::new();
    seed_catalog(&catalog, &tenant, &["users"]).await;

    let app = auth_router(catalog);
    let req = Request::builder()
        .method(Method::GET)
        .uri(format!("/v1/main/namespaces/{tenant}/tables/users"))
        .header(header::AUTHORIZATION, format!("Bearer {tenant}"))
        .body(Body::empty())
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body: LoadTableResponse = body_json(res.into_body()).await;

    assert_eq!(body.metadata.format_version, 2);
    assert!(body.metadata.location.contains(&tenant.to_string()));
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
    let tenant = TenantId::new();
    seed_catalog(&catalog, &tenant, &["users"]).await;

    let app = auth_router(catalog);

    // No bearer.
    let req = Request::builder()
        .method(Method::GET)
        .uri(format!("/v1/main/namespaces/{tenant}/tables"))
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);

    // Wrong scheme.
    let req = Request::builder()
        .method(Method::GET)
        .uri(format!("/v1/main/namespaces/{tenant}/tables"))
        .header(header::AUTHORIZATION, "Basic abc")
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);

    // Empty bearer.
    let req = Request::builder()
        .method(Method::GET)
        .uri(format!("/v1/main/namespaces/{tenant}/tables"))
        .header(header::AUTHORIZATION, "Bearer ")
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);

    // Non-ULID bearer.
    let req = Request::builder()
        .method(Method::GET)
        .uri(format!("/v1/main/namespaces/{tenant}/tables"))
        .header(header::AUTHORIZATION, "Bearer not-a-ulid")
        .body(Body::empty())
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn load_nonexistent_table_returns_404() {
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let tenant = TenantId::new();
    seed_catalog(&catalog, &tenant, &[]).await;

    let app = auth_router(catalog);
    let req = Request::builder()
        .method(Method::GET)
        .uri(format!("/v1/main/namespaces/{tenant}/tables/missing"))
        .header(header::AUTHORIZATION, format!("Bearer {tenant}"))
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
async fn cross_tenant_isolation() {
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let alice = TenantId::new();
    let bob = TenantId::new();
    seed_catalog(&catalog, &alice, &["users"]).await;
    seed_catalog(&catalog, &bob, &["events"]).await;

    let app = auth_router(catalog);
    // Bob tries to list Alice's tables — must be rejected even though
    // both tenants exist.
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
    let tenant = TenantId::new();
    seed_catalog(&catalog, &tenant, &["users"]).await;

    let app = auth_router(catalog.clone());
    let req = Request::builder()
        .method(Method::DELETE)
        .uri(format!("/v1/main/namespaces/{tenant}/tables/users"))
        .header(header::AUTHORIZATION, format!("Bearer {tenant}"))
        .body(Body::empty())
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT);
    // Table is gone from the underlying catalog.
    let names = catalog.list_tables(&tenant).await.unwrap();
    assert!(names.is_empty());
}

#[tokio::test]
async fn create_table_returns_501() {
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let tenant = TenantId::new();
    seed_catalog(&catalog, &tenant, &[]).await;

    let app = auth_router(catalog);
    let body = serde_json::json!({
        "name": "events",
        "schema": { "type": "struct", "fields": [] }
    })
    .to_string();
    let req = Request::builder()
        .method(Method::POST)
        .uri(format!("/v1/main/namespaces/{tenant}/tables"))
        .header(header::AUTHORIZATION, format!("Bearer {tenant}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body))
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::NOT_IMPLEMENTED);
}

#[tokio::test]
async fn open_router_skips_auth() {
    // Sanity: the for_tests config disables auth. Useful for the
    // future pyiceberg test crate that wants to drive the surface
    // without crafting bearer tokens.
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let tenant = TenantId::new();
    seed_catalog(&catalog, &tenant, &["users"]).await;

    let app = open_router(catalog);
    let req = Request::builder()
        .method(Method::GET)
        .uri(format!("/v1/main/namespaces/{tenant}/tables"))
        .body(Body::empty())
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}
