terraform {
  required_providers {
    basin = {
      source  = "bas-in/basin"
      version = "~> 0.1"
    }
  }
}

resource "basin_project" "api" {
  org_slug = "acme"
  name     = "api"
  region   = "jnb"
}

# Write-scoped CI deploy token, locked to a single project + an
# office IP range, expiring in a year.
resource "basin_api_token" "ci_deploy" {
  org_slug     = "acme"
  name         = "ci-deploy-bot"
  description  = "GitHub Actions deploy token"
  scope        = "write"
  project_ids  = [basin_project.api.id]
  ip_allowlist = ["203.0.113.0/24"]
  expires_at   = "2027-05-10T00:00:00Z"
}

output "deploy_token" {
  value     = basin_api_token.ci_deploy.token
  sensitive = true
}
