# basin_project

A basin-cloud project — a logical database boundary inside an org.
Maps 1:1 to an engine tenant.

## Example Usage

```hcl
resource "basin_project" "api" {
  org_slug = "acme"
  name     = "api"
  region   = "jnb"
}

output "connection_string" {
  value     = basin_project.api.connection_string
  sensitive = true
}
```

## Argument Reference

- `org_slug` (Required, ForceNew) — Slug of the parent org.
- `name` (Required) — Human-readable project name. The only mutable
  field; everything else is ForceNew.
- `region` (Required, ForceNew) — Region code (e.g. `jnb`, `iad`).

## Attribute Reference

- `id` — Project ULID.
- `org_id` — ULID of the parent org.
- `ref` — Stable URL-safe ref (`<slug>-<8-hex>`).
- `status` — Lifecycle: `active`, `paused`, `deleting`.
- `engine_tenant_id` — Engine-side tenant identifier.
- `created_at` — RFC3339 create timestamp.
- `connection_string` (Sensitive) — Postgres-wire connection URL.
  Returned exactly once on create; subsequent reads leave the cached
  state value intact.

## Import

```sh
terraform import basin_project.api acme/api-1a2b3c4d
```

The import ID is `<org_slug>/<ref>`. The connection string is **not**
recoverable via import — rotate the password if you need it back.
