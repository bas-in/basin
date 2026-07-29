// Full stack — a project, a write-scoped CI token, a deploy webhook,
// and a custom domain in one workspace.
terraform {
  required_providers {
    basin = {
      source  = "bas-in/basin"
      version = "~> 0.1"
    }
  }
}

data "basin_org" "acme" {
  slug = "acme"
}

resource "basin_project" "api" {
  org_slug = data.basin_org.acme.slug
  name     = "api"
  region   = "jnb"
}

resource "basin_api_token" "ci" {
  org_slug    = data.basin_org.acme.slug
  name        = "ci-deploy"
  scope       = "write"
  project_ids = [basin_project.api.id]
}

resource "basin_webhook" "deploys" {
  project_ref = basin_project.api.ref
  url         = "https://hooks.example.com/basin/deploys"
  events      = ["project.migration.applied"]
}

resource "basin_custom_domain" "api" {
  project_ref = basin_project.api.ref
  domain      = "api.example.com"
}

data "basin_engine_versions" "catalog" {}

output "recommended_engine" {
  value = [for v in data.basin_engine_versions.catalog.versions : v.version if v.is_recommended]
}
