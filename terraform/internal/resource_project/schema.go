// Schema for basin_project. RequiresReplace plan modifiers on
// org_slug/region (and on the implicit ref) match the backend's
// "you can't move a project" enforcement.
package resource_project

import (
	"context"

	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
)

func (r *projectResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "A basin-cloud project — a logical database boundary inside an org. Maps 1:1 to an engine tenant.",
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				MarkdownDescription: "ULID. Stable across renames.",
				Computed:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"org_slug": schema.StringAttribute{
				MarkdownDescription: "Slug of the parent org. Replaces the resource if changed.",
				Required:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"org_id": schema.StringAttribute{
				MarkdownDescription: "ULID of the parent org (computed from org_slug at create time).",
				Computed:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"name": schema.StringAttribute{
				MarkdownDescription: "Human-readable project name. The only mutable field.",
				Required:            true,
			},
			"region": schema.StringAttribute{
				MarkdownDescription: "Region code (e.g. `jnb`, `iad`). Replaces the resource if changed — projects don't move.",
				Required:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"ref": schema.StringAttribute{
				MarkdownDescription: "Stable URL-safe ref (`<slug>-<8-hex>`). Server-assigned at create.",
				Computed:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"status": schema.StringAttribute{
				MarkdownDescription: "Lifecycle: `active`, `paused`, `deleting`.",
				Computed:            true,
			},
			"engine_tenant_id": schema.StringAttribute{
				MarkdownDescription: "Engine-side tenant identifier this project routes to.",
				Computed:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"created_at": schema.StringAttribute{
				MarkdownDescription: "RFC3339 create timestamp.",
				Computed:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"connection_string": schema.StringAttribute{
				MarkdownDescription: "Postgres-wire connection URL (`postgresql://…`). Returned exactly once on create; subsequent reads leave the plan value intact.",
				Computed:            true,
				Sensitive:           true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
		},
	}
}
