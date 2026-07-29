# terraform-provider-basin

Terraform provider for [basin-cloud](https://basin.to) — manage projects,
API tokens, webhooks, and custom domains as code.

Built on [terraform-plugin-framework](https://github.com/hashicorp/terraform-plugin-framework)
(Plugin Protocol v6).

---

## Requirements

- Terraform >= 1.0
- Go >= 1.21 (to build from source)

---

## Installation

Add a `required_providers` block to your root module:

```hcl
terraform {
  required_providers {
    basin = {
      source  = "bas-in/basin"
      version = "~> 0.1"
    }
  }
}
```

Run `terraform init` to download the provider from the Terraform registry
once it is published there. For local development, see [Build & install
locally](#build--install-locally) below.

---

## Provider configuration

```hcl
provider "basin" {
  # token is required. Prefer the env var to avoid storing secrets in HCL.
  token = var.basin_token   # or export BASIN_TOKEN=bso_org_…

  # api_url is optional. Defaults to https://api.<BASIN_DOMAIN>
  # (BASIN_DOMAIN env var; falls back to https://api.basin.run).
  # Override only for self-hosted deployments.
  # api_url = "https://api.basin.run"
}
```

### Arguments

| Argument  | Type   | Required | Description |
|-----------|--------|----------|-------------|
| `token`   | string | yes*     | Personal access token (`bso_org_…`). Falls back to `BASIN_TOKEN` env var. |
| `api_url` | string | no       | Override the API base URL. Falls back to `BASIN_API_URL` env var, then `https://api.<BASIN_DOMAIN>` (defaulting to `https://api.basin.run`). |

*The token may be provided via the `BASIN_TOKEN` environment variable instead
of the HCL block.

Mint a token at <https://app.basin.to/settings/api-tokens>. The provider
validates the PAT at `terraform plan`/`apply` time by calling `GET /v1/me`;
a 401 surfaces as a clean diagnostic.

---

## Resources

### `basin_project`

A logical database boundary inside an org. Maps 1:1 to an engine tenant.

`org_slug` and `region` are immutable after create (changes force
replacement). `name` is the only mutable field. `connection_string` is
returned exactly once on create — store it immediately (e.g. in Terraform
Cloud workspace variables or a secrets manager).

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

**Arguments**

| Attribute  | Type   | Required | Description |
|------------|--------|----------|-------------|
| `org_slug` | string | yes      | Parent org slug. Forces replacement on change. |
| `name`     | string | yes      | Human-readable name. Mutable. |
| `region`   | string | yes      | Region code (e.g. `jnb`, `iad`). Forces replacement on change. |

**Computed attributes**

`id`, `org_id`, `ref`, `status`, `engine_tenant_id`, `created_at`,
`connection_string` (sensitive, revealed once).

---

### `basin_api_token`

Scoped personal access token. The plaintext `token` value is returned exactly
once on create — store it before closing the apply output.

Scope updates may only narrow (`admin` → `write` → `read`). Project and IP
allowlist updates may only shrink the set. To widen, destroy and recreate the
token.

```hcl
resource "basin_api_token" "ci" {
  org_slug    = "acme"
  name        = "ci-deploy"
  description = "Token used by CI to run migrations."
  scope       = "write"
  project_ids = [basin_project.api.id]
  # ip_allowlist = ["203.0.113.0/24"]
  # expires_at   = "2027-01-01T00:00:00Z"
}

output "ci_token" {
  value     = basin_api_token.ci.token
  sensitive = true
}
```

**Arguments**

| Attribute      | Type        | Required | Description |
|----------------|-------------|----------|-------------|
| `org_slug`     | string      | yes      | Parent org slug. Forces replacement on change. |
| `name`         | string      | yes      | Label. |
| `description`  | string      | no       | Free-form description. |
| `scope`        | string      | yes      | `read`, `write`, or `admin`. May only narrow on update. |
| `project_ids`  | set(string) | no       | Project ID allowlist. Empty = any project in the org. May only shrink on update. |
| `ip_allowlist` | set(string) | no       | CIDR allowlist. Empty = any IP. May only shrink on update. |
| `expires_at`   | string      | no       | RFC3339 expiry. Omit for no expiry. |

**Computed attributes**

`id`, `prefix`, `token` (sensitive, revealed once), `created_at`.

---

### `basin_webhook`

HTTP endpoint subscription for project events. The HMAC-SHA256 signing
`secret` is generated server-side and revealed exactly once on create.

`project_ref` is immutable after create (changes force replacement).

```hcl
resource "basin_webhook" "deploys" {
  project_ref = basin_project.api.ref
  url         = "https://hooks.example.com/basin/deploys"
  events      = ["project.migration.applied", "project.snapshot.completed"]
  # active defaults to true
}

output "webhook_secret" {
  value     = basin_webhook.deploys.secret
  sensitive = true
}
```

**Arguments**

| Attribute     | Type        | Required | Description |
|---------------|-------------|----------|-------------|
| `project_ref` | string      | yes      | Parent project ref. Forces replacement on change. |
| `url`         | string      | yes      | Endpoint URL the cloud POSTs signed events to. |
| `events`      | set(string) | yes      | Event kinds to subscribe to (e.g. `project.snapshot.completed`). |
| `active`      | bool        | no       | Enable deliveries. Defaults to `true`. |

**Computed attributes**

`id`, `secret` (sensitive, revealed once), `secret_set`, `created_at`.

---

### `basin_custom_domain`

DNS-verified custom domain attached to a project. After `terraform apply`,
create the TXT record the provider outputs (`txt_record_name` /
`txt_record_value`) in your DNS provider, then trigger verification through
the basin-cloud API or console.

Both `project_ref` and `domain` are immutable (changes force replacement).

```hcl
resource "basin_custom_domain" "api" {
  project_ref = basin_project.api.ref
  domain      = "api.example.com"
}

output "txt_record" {
  value = {
    name  = basin_custom_domain.api.txt_record_name
    value = basin_custom_domain.api.txt_record_value
  }
}
```

**Arguments**

| Attribute     | Type   | Required | Description |
|---------------|--------|----------|-------------|
| `project_ref` | string | yes      | Parent project ref. Forces replacement on change. |
| `domain`      | string | yes      | FQDN to attach (e.g. `api.example.com`). Forces replacement on change. |

**Computed attributes**

`id`, `status`, `verification_token` (sensitive), `txt_record_name`,
`txt_record_value`, `cert_status`, `created_at`, `verified_at`.

---

## Data sources

### `data "basin_org"`

Look up an org by slug.

```hcl
data "basin_org" "acme" {
  slug = "acme"
}

output "org_plan" {
  value = data.basin_org.acme.plan
}
```

**Arguments:** `slug` (required).
**Computed:** `id`, `name`, `plan`, `billing_email`.

---

### `data "basin_project"`

Look up a project by ref.

```hcl
data "basin_project" "api" {
  ref = "api-a1b2c3d4"
}
```

**Arguments:** `ref` (required).
**Computed:** `id`, `org_id`, `name`, `region`, `status`, `engine_tenant_id`,
`created_at`.

---

### `data "basin_engine_versions"`

List all engine versions in the catalog, with channel, recommended flag, and
EOL dates.

```hcl
data "basin_engine_versions" "catalog" {}

output "recommended" {
  value = [
    for v in data.basin_engine_versions.catalog.versions :
    v.version if v.is_recommended
  ]
}
```

**Arguments:** none.
**Computed:** `id`, `versions` (list of objects with `version`, `channel`,
`released_at`, `eol_at`, `release_notes_url`, `is_recommended`, `notes`).

---

## Build & install locally

```sh
# Build a local binary
make build

# Copy binary into ~/.terraform.d/plugins/registry.terraform.io/bas-in/basin/<version>/<os_arch>/
make install
```

After `make install`, configure a [dev override](https://developer.hashicorp.com/terraform/cli/config/config-file#development-overrides-for-provider-developers)
so `terraform init` resolves the local binary instead of the registry:

```hcl
# ~/.terraformrc  (or %APPDATA%\terraform.rc on Windows)
provider_installation {
  dev_overrides {
    "bas-in/basin" = "/path/to/your/home/.terraform.d/plugins/registry.terraform.io/bas-in/basin/0.1.0/linux_amd64"
  }
  direct {}
}
```

With dev overrides active, `terraform init` is not needed — run
`terraform plan` directly.

---

## Running tests

```sh
# Unit tests and lifecycle tests against the in-process mock cloud
make test

# Full Terraform acceptance suite (requires a live basin-cloud backend)
export BASIN_TOKEN=bso_org_…
export BASIN_TEST_ORG_SLUG=myorg
make testacc
```

---

## Examples

| Directory | What it shows |
|-----------|---------------|
| `examples/project_basic` | Minimal project create with connection string output. |
| `examples/api_token_scoped` | Write-scoped PAT narrowed to a single project. |
| `examples/webhook_with_signing_secret` | HMAC-signed deploy webhook with secret output. |
| `examples/custom_domain_with_verify` | Custom domain + DNS verification guidance. |
| `examples/full_stack` | Project + token + webhook + domain in one workspace. |

---

## Versioning

Follows semver. Schema-breaking changes bump the major version; new resources
and new optional attributes are minor releases; bug fixes are patches.

**Status:** pre-1.0 — the schema may evolve as the basin-cloud API surface
settles. Pin to a specific minor (`~> 0.1`) until 1.0 ships.
