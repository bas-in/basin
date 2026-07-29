terraform {
  required_providers {
    basin = {
      source  = "bas-in/basin"
      version = "~> 0.1"
    }
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

resource "basin_project" "api" {
  org_slug = "acme"
  name     = "api"
  region   = "jnb"
}

# Attach a custom domain. Provider returns the TXT record; we publish
# it to Route53 so basin can verify DNS control.
resource "basin_custom_domain" "api" {
  project_ref = basin_project.api.ref
  domain      = "api.example.com"
}

resource "aws_route53_record" "verify" {
  zone_id = var.route53_zone_id
  name    = basin_custom_domain.api.txt_record_name
  type    = "TXT"
  ttl     = 60
  records = [basin_custom_domain.api.txt_record_value]
}

variable "route53_zone_id" { type = string }
