# basin_webhook

A project webhook subscription. The cloud POSTs signed events to
`url`; verify them with HMAC-SHA256 of the body using the
once-revealed `secret`.

## Example Usage

```hcl
resource "basin_webhook" "snapshots" {
  project_ref = basin_project.api.ref
  url         = "https://hooks.example.com/basin"
  events = [
    "project.snapshot.completed",
    "project.snapshot.failed",
  ]
}
```

## Argument Reference

- `project_ref` (Required, ForceNew) — Ref of the parent project.
- `url` (Required) — Endpoint URL the cloud POSTs events to.
- `events` (Required) — Set of event kinds to subscribe to.
- `active` (Optional) — Whether deliveries are enabled. Defaults to
  `true`.

## Attribute Reference

- `id` — Webhook ULID.
- `secret` (Sensitive) — HMAC signing secret. Returned exactly once on
  create.
- `secret_set` — True once the server has a signing secret stored.
- `created_at` — RFC3339 create timestamp.

## Import

```sh
terraform import basin_webhook.snapshots api-1a2b3c4d/wh_01H...
```

The import ID is `<project_ref>/<webhook_id>`.
