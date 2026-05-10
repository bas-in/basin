# basin-iceberg-rest

Lakekeeper-compatible Iceberg REST catalog server backed by Basin's
internal `Catalog` trait. Lets pyiceberg / Spark / Trino / DuckDB / Polars
read Basin's table metadata via the Iceberg REST spec.

## Status (Phase 6 entry)

- `GET /v1/{prefix}/namespaces` — implemented (returns the caller's tenant)
- `GET /v1/{prefix}/namespaces/{ns}/tables` — implemented
- `GET /v1/{prefix}/namespaces/{ns}/tables/{tbl}` — implemented
  (minimal Iceberg metadata shape — covers format-version=2, schemas,
  snapshots; partition-specs stay empty for v0.1 because Basin's
  `RangeMonthly` doesn't yet round-trip to Iceberg transform vocab)
- `DELETE /v1/{prefix}/namespaces/{ns}/tables/{tbl}` — implemented
- `POST /v1/{prefix}/namespaces/{ns}/tables` — scaffolded, returns 501
  (depends on the Iceberg → Arrow schema translator)
- `POST /v1/{prefix}/namespaces/{ns}/tables/{tbl}` (commit) — scaffolded,
  returns 501. Iceberg REST commits are non-trivial; Basin's
  `replace_data_files` semantics need careful mapping.

## Auth

v0.1 ships an auth stub: the `Authorization: Bearer <token>` header is
parsed as a Basin `TenantId` (ULID). The handler enforces that the URL
`:namespace` segment matches the caller's tenant — cross-tenant
isolation is preserved without yet wiring `basin-auth`'s JWT verifier.
Production wiring will replace the stub with a call into
`basin_auth::AuthService::verify_jwt` (the same path `basin-rest` uses)
and pull `claims.tenant_id` from the verified token. The handler-side
contract — "the URL `:namespace` segment must equal the caller's
tenant" — stays the same; only the token decoder changes.

## Integration plan

Drop into `basin-server` startup as

```rust
let catalog: Arc<dyn basin_catalog::Catalog> = …;
app = app.nest("/iceberg", basin_iceberg_rest::router(catalog));
```

External clients connect with the Iceberg REST catalog URL
`https://basin.example.com/iceberg/v1/<warehouse>`. The `<warehouse>`
prefix is the Iceberg "warehouse" identifier — Basin v0.1 accepts any
prefix and routes the same; the deployment-id mapping lands when
multi-deployment ships.

## Open work

- Full Iceberg metadata translation (manifest-list paths, statistics,
  partition transforms)
- `POST /v1/{prefix}/namespaces/{ns}/tables` create-table flow with an
  Iceberg-schema → Arrow-schema translator
- `POST /v1/{prefix}/namespaces/{ns}/tables/{tbl}` commit flow with
  optimistic concurrency mapped onto `append_data_files` /
  `replace_data_files`
- pyiceberg integration test (separate test crate; currently scaffolded)
- Real auth (JWT verification via `basin-auth`) replacing the bearer-as-
  tenant-id stub
