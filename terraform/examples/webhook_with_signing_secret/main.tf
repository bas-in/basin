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

resource "basin_webhook" "snapshots" {
  project_ref = basin_project.api.ref
  url         = "https://hooks.example.com/basin"
  events = [
    "project.snapshot.completed",
    "project.snapshot.failed",
  ]
}

output "webhook_secret" {
  description = "HMAC signing secret. Returned exactly once."
  value       = basin_webhook.snapshots.secret
  sensitive   = true
}
