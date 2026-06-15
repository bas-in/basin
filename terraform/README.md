# terraform-provider-basin

Manage [basin-cloud](https://basin.to) projects, API tokens,
webhooks, and custom domains via Terraform.

## Install

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

Once published, `terraform init` will pull the provider from the
public Terraform registry. For local development see "Build" below.

## Authentication

Provider expects a basin-cloud personal access token (`bso_org_…`).
Two ways to provide it:

```sh
export BASIN_TOKEN=bso_org_XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
```

or in HCL:

```hcl
provider "basin" {
  token = var.basin_token
}
```

Mint a token at <https://app.basin.to/settings/api-tokens>. The
provider validates the PAT at `terraform plan`/`apply` time by
calling `GET /v1/me`; a 401 surfaces a clean diagnostic.

## Quick example

```hcl
provider "basin" {}

resource "basin_project" "api" {
  org_slug = "acme"
  name     = "api"
  region   = "jnb"
}

resource "basin_api_token" "ci" {
  org_slug    = "acme"
  name        = "ci-deploy"
  scope       = "write"
  project_ids = [basin_project.api.id]
}

resource "basin_webhook" "deploys" {
  project_ref = basin_project.api.ref
  url         = "https://hooks.example.com/basin"
  events      = ["project.migration.applied"]
}
```

See `examples/` for more scenarios:

- `examples/project_basic` — minimal project create
- `examples/api_token_scoped` — narrowing-only PAT scoping
- `examples/webhook_with_signing_secret` — HMAC-signed deploy webhook
- `examples/custom_domain_with_verify` — TXT record + Route53
- `examples/full_stack` — project + token + webhook + domain in one
  workspace

## Resources

| Resource | Purpose |
|---|---|
| `basin_project` | Logical database boundary inside an org. |
| `basin_api_token` | Scoped PAT (read/write/admin × project allowlist × IP allowlist). |
| `basin_webhook` | Project-scoped HTTP endpoint that receives signed events. |
| `basin_custom_domain` | DNS-verified custom domain mapping. |

## Data Sources

| Data Source | Purpose |
|---|---|
| `basin_org` | Look up an org by slug. |
| `basin_project` | Look up a project by ref. |
| `basin_engine_versions` | List of engine versions catalog. |

Per-resource docs live under `docs/`.

## Build

```sh
make build           # local binary in repo root
make install         # symlink into ~/.terraform.d/plugins/...
make test            # unit + lifecycle tests against the in-process mock cloud
make testacc         # full Terraform acceptance suite (TF_ACC=1)
```

The provider speaks Plugin Protocol v6.

## Versioning

Provider follows semver. Schema-breaking changes bump the major
version; new resources / new optional attributes are minors;
bug fixes are patches.

## Status

Pre-1.0 — schema may evolve as the basin-cloud API surface settles.
Pin to a specific minor (`~> 0.1`) until 1.0 ships.
