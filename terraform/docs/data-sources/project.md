# basin_project (Data Source)

Look up a basin-cloud project by ref.

## Example Usage

```hcl
data "basin_project" "api" {
  ref = "api-1a2b3c4d"
}
```

## Argument Reference

- `ref` (Required) — Project ref.

## Attribute Reference

- `id`, `org_id`, `name`, `region`, `status`, `engine_tenant_id`,
  `created_at`.
