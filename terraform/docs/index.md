# basin Provider

Manage [basin-cloud](https://basin.to) projects, API tokens, webhooks,
and custom domains via Terraform. The provider talks to the public
`/v1/*` REST API using a personal access token (PAT).

## Example Usage

```hcl
terraform {
  required_providers {
    basin = {
      source  = "bas-in/basin"
      version = "~> 0.1"
    }
  }
}

provider "basin" {
  # token comes from BASIN_TOKEN env var
}

resource "basin_project" "api" {
  org_slug = "acme"
  name     = "api"
  region   = "jnb"
}
```

## Authentication

The provider expects a basin-cloud personal access token. PATs look
like `bso_org_<32 b64u>` and carry a scope band + project allowlist +
optional IP allowlist.

Three ways to provide the token (in order of precedence):

1. `token` argument in the provider block
2. `BASIN_TOKEN` environment variable

Mint a PAT at <https://app.basin.to/settings/api-tokens>.

The provider validates the token at configure time by calling
`GET /v1/me` — a 401 surfaces a clean diagnostic instead of a
per-resource cascade during plan.

## Schema

### Optional

- `api_url` — Base URL of the basin-cloud API. Defaults to
  `https://api.basin.to`. Override for self-hosted deployments. Also
  reads `BASIN_API_URL`.
- `token` — Personal access token. Falls back to `BASIN_TOKEN`.
  Sensitive.
