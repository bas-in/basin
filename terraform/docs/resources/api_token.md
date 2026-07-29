# basin_api_token

A scoped basin-cloud personal access token. The plaintext `token`
attribute is returned exactly once on create — store it in a secret
manager.

## Example Usage

```hcl
resource "basin_api_token" "ci_deploy" {
  org_slug     = "acme"
  name         = "ci-deploy-bot"
  scope        = "write"
  project_ids  = [basin_project.api.id]
  ip_allowlist = ["203.0.113.0/24"]
  expires_at   = "2027-05-10T00:00:00Z"
}
```

## Argument Reference

- `org_slug` (Required, ForceNew) — Slug of the parent org.
- `name` (Required) — Human-readable token label.
- `scope` (Required) — Scope band: `read`, `write`, or `admin`.
- `description` (Optional) — Free-form description.
- `project_ids` (Optional) — Set of project ULIDs the token is
  restricted to. Empty = any project in the org.
- `ip_allowlist` (Optional) — CIDR allowlist for inbound API calls.
  Empty = any IP.
- `expires_at` (Optional) — RFC3339 expiry. Omit for no expiry.

### Narrowing-only updates

Updates may only **narrow** privileges. Mirroring the backend's
enforcement:

- `scope` may only step down (admin → write → read). Widening forces
  a `terraform taint` + recreate.
- `project_ids` may only shrink. Adding new IDs or removing the
  allowlist entirely (going from N IDs to 0) is rejected at plan
  time.
- `ip_allowlist` is unrestricted (operationally tightening or
  loosening this isn't a privilege change).

## Attribute Reference

- `id` — Token ULID.
- `prefix` — First 12 chars of the token (for log correlation).
- `token` (Sensitive) — Plaintext token. Returned exactly once on
  create.
- `created_at` — RFC3339 create timestamp.

## Import

```sh
terraform import basin_api_token.ci_deploy acme/tok_01H...
```

The import ID is `<org_slug>/<token_id>`. The plaintext token is
**not** recoverable via import — mint a new token if you need it.
