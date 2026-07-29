# basin_custom_domain

A custom domain mapping for a project. After create, place the TXT
record in your DNS provider (the provider exposes the record as a
computed attribute), then call the verify endpoint via the basin
dashboard or the CLI.

## Example Usage

```hcl
resource "basin_custom_domain" "api" {
  project_ref = basin_project.api.ref
  domain      = "api.example.com"
}

resource "aws_route53_record" "verify" {
  zone_id = var.zone_id
  name    = basin_custom_domain.api.txt_record_name
  type    = "TXT"
  ttl     = 60
  records = [basin_custom_domain.api.txt_record_value]
}
```

## Argument Reference

- `project_ref` (Required, ForceNew) — Ref of the parent project.
- `domain` (Required, ForceNew) — FQDN to attach (e.g.
  `api.example.com`).

## Attribute Reference

- `id` — Domain ULID.
- `status` — Lifecycle: `pending`, `verified`, `retired`.
- `verification_token` (Sensitive) — Random token to include in the
  TXT record.
- `txt_record_name` — DNS name for the TXT record (e.g.
  `_basin-verify.api.example.com`).
- `txt_record_value` — Full TXT record value to publish.
- `cert_status` — TLS cert lifecycle: `none`, `pending`, `issued`,
  `failed`.
- `created_at` / `verified_at` — RFC3339 timestamps.

## Import

```sh
terraform import basin_custom_domain.api api-1a2b3c4d/dom_01H...
```

The import ID is `<project_ref>/<domain_id>`.
