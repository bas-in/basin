terraform {
  required_providers {
    basin = {
      source  = "bas-in/basin"
      version = "~> 0.1"
    }
  }
}

provider "basin" {
  # token via BASIN_TOKEN env var
}

resource "basin_project" "api" {
  org_slug = "acme"
  name     = "api"
  region   = "jnb"
}

output "ref" {
  value = basin_project.api.ref
}

output "connection_string" {
  value     = basin_project.api.connection_string
  sensitive = true
}
