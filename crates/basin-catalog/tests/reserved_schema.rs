//! Phase 5.18.A — catalog `(schema, table)` keying for reserved schemas.
//!
//! Tests cover:
//! 1. `ReservedSchema` enum — all variants, parsing, display.
//! 2. `resolve_schema` / `resolve_qualified` back-compat helpers.
//! 3. Round-trip create/load/drop for `InMemoryCatalog` across reserved schemas.
//! 4. Isolation: `auth.users` and `public.users` are distinct catalog entries.
//! 5. Back-compat: unqualified `users` → `public.users`; `public.users` →
//!    same entry; unknown schema `myapp.users` → `public.users`.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use basin_catalog::{resolve_qualified, resolve_schema, Catalog, InMemoryCatalog, ReservedSchema};
use basin_common::{ProjectId, QualifiedTableName, SchemaName, TableName};

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

fn tiny_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
}

fn table(name: &str) -> TableName {
    TableName::new(name).unwrap()
}

fn schema(name: &str) -> SchemaName {
    SchemaName::new(name).unwrap()
}

// ─────────────────────────────────────────────────────────────────────────────
// ReservedSchema enum tests
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn reserved_schema_all_has_eight_variants() {
    assert_eq!(ReservedSchema::ALL.len(), 8);
}

#[test]
fn reserved_schema_from_str_round_trips_for_all_variants() {
    for &variant in ReservedSchema::ALL {
        let parsed = ReservedSchema::from_str(variant.as_str());
        assert_eq!(
            parsed,
            Some(variant),
            "from_str(as_str()) failed for {:?}",
            variant
        );
    }
}

#[test]
fn reserved_schema_from_str_user_defined_is_none() {
    assert_eq!(ReservedSchema::from_str("myapp"), None);
    assert_eq!(ReservedSchema::from_str("billing"), None);
    assert_eq!(ReservedSchema::from_str(""), None);
    // case-sensitive
    assert_eq!(ReservedSchema::from_str("PUBLIC"), None);
    assert_eq!(ReservedSchema::from_str("Auth"), None);
}

#[test]
fn reserved_schema_display_matches_as_str() {
    for &variant in ReservedSchema::ALL {
        assert_eq!(
            variant.to_string(),
            variant.as_str(),
            "Display != as_str for {:?}",
            variant
        );
    }
}

#[test]
fn reserved_schema_public_is_public() {
    assert!(ReservedSchema::Public.is_public());
    for &v in ReservedSchema::ALL {
        if v != ReservedSchema::Public {
            assert!(!v.is_public(), "{:?} should not be_public", v);
        }
    }
}

#[test]
fn reserved_schema_to_schema_name() {
    assert_eq!(ReservedSchema::Auth.to_schema_name().as_str(), "auth");
    assert_eq!(
        ReservedSchema::InformationSchema.to_schema_name().as_str(),
        "information_schema"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// resolve_schema / resolve_qualified — back-compat rules
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn resolve_schema_none_is_public() {
    assert_eq!(resolve_schema(None).as_str(), "public");
}

#[test]
fn resolve_schema_public_is_public() {
    assert_eq!(resolve_schema(Some("public")).as_str(), "public");
}

#[test]
fn resolve_schema_reserved_names_pass_through() {
    for &v in ReservedSchema::ALL {
        let resolved = resolve_schema(Some(v.as_str()));
        assert_eq!(
            resolved.as_str(),
            v.as_str(),
            "reserved schema {:?} should pass through",
            v
        );
    }
}

#[test]
fn resolve_schema_user_defined_aliases_to_public() {
    assert_eq!(resolve_schema(Some("myapp")).as_str(), "public");
    assert_eq!(resolve_schema(Some("billing")).as_str(), "public");
    assert_eq!(resolve_schema(Some("BILLING")).as_str(), "public");
}

#[test]
fn resolve_qualified_unqualified_is_public_dot_table() {
    let qt = resolve_qualified(table("users"), None);
    assert_eq!(qt.schema.as_str(), "public");
    assert_eq!(qt.name.as_str(), "users");
    assert_eq!(qt.to_string(), "public.users");
}

#[test]
fn resolve_qualified_public_explicit_is_same_as_unqualified() {
    let qt_explicit = resolve_qualified(table("users"), Some("public"));
    let qt_implicit = resolve_qualified(table("users"), None);
    assert_eq!(qt_explicit, qt_implicit);
}

#[test]
fn resolve_qualified_auth_is_distinct_from_public() {
    let auth_users = resolve_qualified(table("users"), Some("auth"));
    let public_users = resolve_qualified(table("users"), None);
    assert_ne!(auth_users, public_users);
    assert_eq!(auth_users.to_string(), "auth.users");
    assert_eq!(public_users.to_string(), "public.users");
}

#[test]
fn resolve_qualified_user_schema_aliases_to_public() {
    let myapp_users = resolve_qualified(table("users"), Some("myapp"));
    let public_users = resolve_qualified(table("users"), None);
    assert_eq!(myapp_users, public_users);
}

// ─────────────────────────────────────────────────────────────────────────────
// InMemoryCatalog — round-trip tests
// ─────────────────────────────────────────────────────────────────────────────

/// `auth.users` and `public.users` must be distinct catalog entries that
/// don't collide even though the bare table name is the same.
#[tokio::test]
async fn in_memory_auth_users_distinct_from_public_users() {
    let cat = InMemoryCatalog::new();
    let proj = ProjectId::new();
    cat.create_namespace(&proj).await.unwrap();

    let auth_users = QualifiedTableName::new(schema("auth"), table("users"));
    let public_users = QualifiedTableName::new(schema("public"), table("users"));

    // Create both.
    cat.create_table_qualified(&proj, &auth_users, tiny_schema())
        .await
        .unwrap();
    cat.create_table_qualified(&proj, &public_users, tiny_schema())
        .await
        .unwrap();

    // Load each and verify schema label.
    let am = cat.load_table_qualified(&proj, &auth_users).await.unwrap();
    assert_eq!(am.table.as_str(), "users");

    let pm = cat
        .load_table_qualified(&proj, &public_users)
        .await
        .unwrap();
    assert_eq!(pm.table.as_str(), "users");

    // They are independent entries: drop one, the other still exists.
    cat.drop_table_qualified(&proj, &auth_users).await.unwrap();
    assert!(cat.load_table_qualified(&proj, &auth_users).await.is_err());
    assert!(cat.load_table_qualified(&proj, &public_users).await.is_ok());
}

/// Creating a table with an unqualified name goes to `public`.
#[tokio::test]
async fn in_memory_unqualified_resolves_to_public() {
    let cat = InMemoryCatalog::new();
    let proj = ProjectId::new();
    cat.create_namespace(&proj).await.unwrap();

    // Old `create_table` (unqualified path).
    cat.create_table(&proj, &table("orders"), tiny_schema().as_ref())
        .await
        .unwrap();

    // Can load it through the qualified path with `public`.
    let public_orders = QualifiedTableName::new(schema("public"), table("orders"));
    cat.load_table_qualified(&proj, &public_orders)
        .await
        .unwrap();

    // Cannot load it under a different schema.
    let auth_orders = QualifiedTableName::new(schema("auth"), table("orders"));
    assert!(cat.load_table_qualified(&proj, &auth_orders).await.is_err());
}

/// `public.table` (explicit) and unqualified `table` resolve to the same
/// catalog entry — back-compat invariant.
#[tokio::test]
async fn in_memory_public_qualified_equals_unqualified() {
    let cat = InMemoryCatalog::new();
    let proj = ProjectId::new();
    cat.create_namespace(&proj).await.unwrap();

    // Create via unqualified API.
    cat.create_table(&proj, &table("events"), tiny_schema().as_ref())
        .await
        .unwrap();

    // Load via public-qualified API — must succeed and point to same entry.
    let public_events = QualifiedTableName::new(schema("public"), table("events"));
    let meta = cat
        .load_table_qualified(&proj, &public_events)
        .await
        .unwrap();
    assert_eq!(meta.table.as_str(), "events");
}

/// Unknown schema `myapp.users` aliases to `public.users` via `resolve_qualified`.
#[tokio::test]
async fn in_memory_user_schema_aliases_to_public_via_resolver() {
    let cat = InMemoryCatalog::new();
    let proj = ProjectId::new();
    cat.create_namespace(&proj).await.unwrap();

    // Create `public.users`.
    let public_users = QualifiedTableName::new(schema("public"), table("users"));
    cat.create_table_qualified(&proj, &public_users, tiny_schema())
        .await
        .unwrap();

    // resolve_qualified("myapp") → public: so loading through the resolved
    // key finds the same table.
    let resolved = resolve_qualified(table("users"), Some("myapp"));
    assert_eq!(resolved.schema.as_str(), "public");
    let meta = cat.load_table_qualified(&proj, &resolved).await.unwrap();
    assert_eq!(meta.table.as_str(), "users");
}

/// `list_tables_qualified` returns tables across all reserved schemas.
#[tokio::test]
async fn in_memory_list_tables_qualified_spans_schemas() {
    let cat = InMemoryCatalog::new();
    let proj = ProjectId::new();
    cat.create_namespace(&proj).await.unwrap();

    let auth_users = QualifiedTableName::new(schema("auth"), table("users"));
    let storage_objects = QualifiedTableName::new(schema("storage"), table("objects"));
    let public_orders = QualifiedTableName::new(schema("public"), table("orders"));

    cat.create_table_qualified(&proj, &auth_users, tiny_schema())
        .await
        .unwrap();
    cat.create_table_qualified(&proj, &storage_objects, tiny_schema())
        .await
        .unwrap();
    cat.create_table_qualified(&proj, &public_orders, tiny_schema())
        .await
        .unwrap();

    let all = cat.list_tables_qualified(&proj).await.unwrap();
    assert!(all.contains(&auth_users), "auth.users missing: {all:?}");
    assert!(
        all.contains(&storage_objects),
        "storage.objects missing: {all:?}"
    );
    assert!(
        all.contains(&public_orders),
        "public.orders missing: {all:?}"
    );
}

/// `create_namespace` pre-seeds all 8 reserved schemas.
#[tokio::test]
async fn in_memory_create_namespace_seeds_all_reserved_schemas() {
    let cat = InMemoryCatalog::new();
    let proj = ProjectId::new();
    cat.create_namespace(&proj).await.unwrap();

    let schemas = cat.list_schemas(&proj).await.unwrap();
    for &reserved in ReservedSchema::ALL {
        assert!(
            schemas.iter().any(|s| s.as_str() == reserved.as_str()),
            "reserved schema {:?} missing from list_schemas after create_namespace",
            reserved
        );
    }
}

/// Creating tables in every reserved schema all succeed (no explicit
/// `create_schema` call needed — they are pre-seeded by `create_namespace`).
#[tokio::test]
async fn in_memory_create_table_in_every_reserved_schema() {
    let cat = InMemoryCatalog::new();
    let proj = ProjectId::new();
    cat.create_namespace(&proj).await.unwrap();

    for &reserved in ReservedSchema::ALL {
        let qtable = QualifiedTableName::new(reserved.to_schema_name(), table("t"));
        cat.create_table_qualified(&proj, &qtable, tiny_schema())
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "create_table_qualified failed for schema {:?}: {e}",
                    reserved
                )
            });
        cat.load_table_qualified(&proj, &qtable)
            .await
            .unwrap_or_else(|e| {
                panic!("load_table_qualified failed for schema {:?}: {e}", reserved)
            });
    }
}

/// `list_tables` (old API) returns only public-schema tables — back-compat.
#[tokio::test]
async fn in_memory_list_tables_old_api_returns_only_public() {
    let cat = InMemoryCatalog::new();
    let proj = ProjectId::new();
    cat.create_namespace(&proj).await.unwrap();

    let auth_users = QualifiedTableName::new(schema("auth"), table("users"));
    let public_orders = QualifiedTableName::new(schema("public"), table("orders"));

    cat.create_table_qualified(&proj, &auth_users, tiny_schema())
        .await
        .unwrap();
    cat.create_table_qualified(&proj, &public_orders, tiny_schema())
        .await
        .unwrap();

    // Old API: only public.
    let old = cat.list_tables(&proj).await.unwrap();
    assert!(old.iter().any(|t| t.as_str() == "orders"), "orders missing");
    assert!(
        !old.iter().any(|t| t.as_str() == "users"),
        "auth.users must not appear in old list_tables"
    );
}
