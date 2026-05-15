# basin-iceberg-rest

Lakekeeper-compatible Iceberg REST catalog server backed by Basin's
internal `Catalog` trait. Lets pyiceberg / Spark / Trino / DuckDB / Polars
read **and write** Basin's table metadata via the Iceberg REST spec.

## Status (Phase 6 entry)

- `GET /v1/{prefix}/namespaces` — implemented (returns the caller's project)
- `GET /v1/{prefix}/namespaces/{ns}/tables` — implemented
- `GET /v1/{prefix}/namespaces/{ns}/tables/{tbl}` — implemented
  (minimal Iceberg metadata shape — covers format-version=2, schemas,
  snapshots; partition-specs stay empty for v0.1 because Basin's
  `RangeMonthly` doesn't yet round-trip to Iceberg transform vocab)
- `DELETE /v1/{prefix}/namespaces/{ns}/tables/{tbl}` — implemented
- `POST /v1/{prefix}/namespaces/{ns}/tables` (create-table) — ✅ implemented
  (Iceberg → Arrow schema translation; `Decimal128`, `timestamp[tz]`,
  `uuid`, the scalar set; `fixed` / `list` / `map` / `struct` / `time`
  return 501)
- `POST /v1/{prefix}/namespaces/{ns}/tables/{tbl}` (commit-table) — ✅
  implemented (subset; see "Commit subset" below)
- `POST /v1/{prefix}/namespaces/{ns}/register` (register-table) — returns
  501 (Basin doesn't accept externally-authored metadata blobs in v0.1)

## Commit subset (v0.1)

The handler maps Iceberg's optimistic-concurrency commit onto Basin's
existing `Catalog::append_data_files` primitive — same atomic-snapshot
contract. Anything outside this subset returns a structured 501 with
`NotImplementedException` (or a structured 409 / 4xx when the action is
known but the request is malformed).

### Accepted requirements

| Iceberg requirement              | Basin check                                          |
| -------------------------------- | ---------------------------------------------------- |
| `assert-table-uuid`              | UUIDv5 over `<project>/<table>` matches               |
| `assert-current-schema-id`       | Basin tables are single-schema; only `0` is accepted |
| `assert-ref-snapshot-id` (`main`)| `meta.current_snapshot.0 as i64` matches             |

Rejected:
- `assert-create` (table already exists by the time we reach commit-table)
- `assert-ref-snapshot-id` for any branch other than `main` (Basin v0.1
  has no multi-branch support)
- Every other `assert-*` requirement type Iceberg ships
  (`assert-default-spec-id`, `assert-last-assigned-field-id`, etc.)

### Accepted updates

| Iceberg action          | Basin mapping                                                |
| ----------------------- | ------------------------------------------------------------ |
| `add-snapshot`          | One per commit; data files extracted from snapshot summary   |
| `set-current-snapshot`  | Implicit via `append_data_files`; explicit form is sanity-checked |

The `add-snapshot` payload must carry data-file paths inline via the
snapshot `summary` map — Basin doesn't read Iceberg manifest-list files
(it doesn't write them either). The contract:

- `summary.added-files-paths` — comma-separated object-store paths.
  Required.
- `summary.added-records-per-file` — comma-separated u64 row counts;
  optional, must match `added-files-paths` count when present.
- `summary.added-files-size-per-file` — comma-separated u64 byte sizes;
  optional, same length contract.
- `manifest-list` — must be empty (we don't follow the URL).

Example commit body:

```json
{
  "requirements": [
    { "type": "assert-table-uuid", "uuid": "<from prior load-table>" },
    { "type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": 0 }
  ],
  "updates": [
    {
      "action": "add-snapshot",
      "snapshot": {
        "snapshot-id": 1,
        "parent-snapshot-id": 0,
        "timestamp-ms": 1699999999000,
        "summary": {
          "operation": "append",
          "added-files-count": "2",
          "added-files-paths": "users/data/a.parquet,users/data/b.parquet",
          "added-records-per-file": "100,200",
          "added-files-size-per-file": "10240,20480"
        }
      }
    },
    { "action": "set-current-snapshot", "snapshot-id": 1 }
  ]
}
```

`load-table` echoes `added-files-paths` back into each snapshot's
`summary` so a `load → modify → commit` loop chains cleanly without a
separate manifest fetch.

### Rejected updates

`add-schema`, `set-current-schema`, `add-partition-spec`,
`set-default-spec`, `add-sort-order`, `set-default-sort-order`,
`add-view-version`, `remove-snapshots`, `remove-snapshot-ref`,
`set-properties`, `remove-properties`, `set-location`, `assign-uuid`,
`upgrade-format-version` — all 501 with the offending action named in
the error envelope. They land as Basin's catalog grows the
corresponding primitives.

## Auth

v0.1 ships an auth stub: the `Authorization: Bearer <token>` header is
parsed as a Basin `ProjectId` (ULID). The handler enforces that the URL
`:namespace` segment matches the caller's project — cross-project
isolation is preserved without yet wiring `basin-auth`'s JWT verifier.
Production wiring will replace the stub with a call into
`basin_auth::AuthService::verify_jwt` (the same path `basin-rest` uses)
and pull `claims.project_id` from the verified token. The handler-side
contract — "the URL `:namespace` segment must equal the caller's
project" — stays the same; only the token decoder changes.

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

- Full Iceberg metadata translation (manifest-list write path,
  statistics, partition transforms)
- Multi-branch refs (`assert-ref-snapshot-id` on names other than `main`)
- Schema evolution at the REST surface (`add-schema` /
  `set-current-schema` updates)
- `register-table` ingest from a foreign metadata.json
- `replace_data_files` mapping for `overwrite` / copy-on-write commits
  (Basin has the primitive; the Iceberg `add-snapshot` summary needs to
  carry both `added-files-paths` and `removed-files-paths`)
- pyiceberg integration test (separate test crate; currently scaffolded)
- Real auth (JWT verification via `basin-auth`) replacing the bearer-as-
  project-id stub
