package resource_api_token

import "github.com/hashicorp/terraform-plugin-framework/types"

type apiTokenModel struct {
	ID          types.String `tfsdk:"id"`
	OrgSlug     types.String `tfsdk:"org_slug"`
	Name        types.String `tfsdk:"name"`
	Description types.String `tfsdk:"description"`
	Scope       types.String `tfsdk:"scope"`
	ProjectIDs  types.Set    `tfsdk:"project_ids"`
	IPAllowlist types.Set    `tfsdk:"ip_allowlist"`
	ExpiresAt   types.String `tfsdk:"expires_at"`
	Prefix      types.String `tfsdk:"prefix"`
	Token       types.String `tfsdk:"token"`
	CreatedAt   types.String `tfsdk:"created_at"`
}
