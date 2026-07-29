// Schema for basin_api_token. org_slug is RequiresReplace; everything
// else is mutable subject to the narrowing-only enforcement in Update.
package resource_api_token

import (
	"context"

	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

func (r *apiTokenResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "A scoped personal access token. The plaintext `token` is returned exactly once on create — store it in a secret manager.",
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				Computed: true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"org_slug": schema.StringAttribute{
				MarkdownDescription: "Slug of the parent org.",
				Required:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"name": schema.StringAttribute{
				MarkdownDescription: "Human-readable token label.",
				Required:            true,
			},
			"description": schema.StringAttribute{
				MarkdownDescription: "Free-form description.",
				Optional:            true,
			},
			"scope": schema.StringAttribute{
				MarkdownDescription: "Scope band. One of `read`, `write`, `admin`. Updates may only narrow (admin→write, write→read).",
				Required:            true,
			},
			"project_ids": schema.SetAttribute{
				MarkdownDescription: "Allowlist of project IDs. Empty = any project in the org. Updates may only shrink the set.",
				Optional:            true,
				Computed:            true,
				ElementType:         types.StringType,
			},
			"ip_allowlist": schema.SetAttribute{
				MarkdownDescription: "CIDR allowlist for inbound API calls. Empty = any IP.",
				Optional:            true,
				Computed:            true,
				ElementType:         types.StringType,
			},
			"expires_at": schema.StringAttribute{
				MarkdownDescription: "RFC3339 expiry. Omit for no expiry.",
				Optional:            true,
				Computed:            true,
			},
			"prefix": schema.StringAttribute{
				MarkdownDescription: "First 12 chars of the token (for log correlation).",
				Computed:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"token": schema.StringAttribute{
				MarkdownDescription: "Plaintext token. Returned exactly once on create.",
				Computed:            true,
				Sensitive:           true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"created_at": schema.StringAttribute{
				Computed: true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
		},
	}
}
