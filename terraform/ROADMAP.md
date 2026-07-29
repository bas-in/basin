# Roadmap

The terraform-provider-basin goal is to give platform teams a complete,
idiomatic way to manage every basin-cloud resource as code — from project
lifecycle through DNS-verified custom domains and fine-grained API tokens —
and to make basin-cloud a first-class citizen of IaC pipelines alongside
databases, object stores, and compute.

The provider uses [terraform-plugin-framework](https://github.com/hashicorp/terraform-plugin-framework)
(Plugin Protocol v6) and calls the basin-cloud REST API directly.

---

## Shipped (v0.1)

**Resources**

- `basin_project` — create / read / update name / delete a project. Emits the
  Postgres-wire `connection_string` once on create.
- `basin_api_token` — create / read / update (narrowing-only) / delete a
  scoped PAT. Emits the full `token` once on create.
- `basin_webhook` — create / read / update (url, events, active) / delete.
  Emits the HMAC-SHA256 `secret` once on create.
- `basin_custom_domain` — create / read / delete with DNS-verification token
  and TLS cert status surfaced as computed attributes.

**Data sources**

- `data.basin_org` — look up an org by slug (id, plan, billing_email).
- `data.basin_project` — look up a project by ref.
- `data.basin_engine_versions` — full engine version catalog (channel,
  recommended flag, EOL dates).

**Quality**

- In-process mock cloud used in unit tests; no live backend needed.
- Makefile targets for build, install, unit test, and acceptance test.
- Examples covering all four resources and all three data sources.

---

## Near-term (v0.2)

- **Import support.** `terraform import basin_project.<label> <ref>` and
  equivalent for `basin_api_token` (import by `org_slug/token_id`),
  `basin_webhook` (by `ref/webhook_id`), and `basin_custom_domain` (by
  `ref/domain_id`). Unblocks adopting existing basin-cloud resources without
  destroying and recreating them.

- **Acceptance-test coverage.** A proper acceptance suite per resource,
  gated behind `TF_ACC=1`, running against a live staging backend in CI via
  GitHub Actions `workflow_dispatch` or a nightly scheduled run. Covers
  create/plan/apply/import/destroy and the narrowing-only enforcement on
  `basin_api_token`.

- **`basin_project` pause/resume.** Expose the `status` field as a writable
  `paused` bool so operators can pause a project in place without destroying
  it.

---

## Medium-term (v0.3 – v0.4)

- **Terraform Registry publishing.** Wire GoReleaser + GPG signing so
  `terraform init` pulls the provider from the public registry at
  `registry.terraform.io/bas-in/basin`. Requires a signed release artifact
  and a Registry API key.

- **tfplugindocs-generated docs.** Replace hand-maintained `docs/` with
  [`tfplugindocs`](https://github.com/hashicorp/terraform-plugin-docs)
  generation driven by schema `MarkdownDescription` fields. Ensures docs stay
  in sync with the schema on every release.

- **`basin_project_migration` resource.** Trigger and track schema migrations
  as Terraform-managed resources, with `apply_on_create` and `revert_on_destroy`
  safety knobs.

- **`basin_snapshot` resource / data source.** Create point-in-time snapshots
  and look up existing ones; surface `download_url` for backup pipelines.

---

## Longer-term

- **`basin_org_member` resource.** Invite/remove org members and manage roles
  as code.

- **`basin_ip_allowlist` resource.** Org-level IP allowlists as a standalone
  resource (instead of inline on `basin_api_token`).

- **Provider-level fleet data source.** `data.basin_projects` (list of all
  projects in an org) to drive `for_each`-based multi-project configurations.

- **Mutable `region` via live migration.** Once the basin-cloud API supports
  cross-region project migration, lift the `RequiresReplace` constraint on
  `basin_project.region`.
