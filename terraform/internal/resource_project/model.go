// State / plan shape for basin_project. Mirrors the
// `apiclient.Project` wire shape but uses tfsdk types so the
// framework can marshal it through Terraform's wire format.
package resource_project

import "github.com/hashicorp/terraform-plugin-framework/types"

type projectModel struct {
	ID               types.String `tfsdk:"id"`
	OrgSlug          types.String `tfsdk:"org_slug"`
	OrgID            types.String `tfsdk:"org_id"`
	Name             types.String `tfsdk:"name"`
	Region           types.String `tfsdk:"region"`
	Ref              types.String `tfsdk:"ref"`
	Status           types.String `tfsdk:"status"`
	EngineTenantID   types.String `tfsdk:"engine_tenant_id"`
	CreatedAt        types.String `tfsdk:"created_at"`
	ConnectionString types.String `tfsdk:"connection_string"`
}
