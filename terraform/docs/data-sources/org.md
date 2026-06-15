# basin_org (Data Source)

Look up a basin-cloud org by slug.

## Example Usage

```hcl
data "basin_org" "acme" {
  slug = "acme"
}

output "plan" {
  value = data.basin_org.acme.plan
}
```

## Argument Reference

- `slug` (Required) — Org slug.

## Attribute Reference

- `id` — Org ULID.
- `name` — Display name.
- `plan` — Billing plan code (e.g. `team`, `business`).
- `billing_email` — Address invoices go to.
