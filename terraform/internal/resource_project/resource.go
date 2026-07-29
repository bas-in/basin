// basin_project — CRUD for /v1/orgs/:slug/projects + /v1/projects/:ref.
//
// Create POSTs the name+region under the parent org slug, persists the
// returned ref + pgwire connection_string. Read GETs by ref. Update
// patches name only (other fields are RequiresReplace via the schema).
// Delete DELETEs by ref. Import accepts "<org_slug>/<ref>".
package resource_project

import (
	"context"
	"fmt"
	"strings"

	"github.com/bas-in/terraform-provider-basin/internal/apiclient"

	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

var (
	_ resource.Resource                = (*projectResource)(nil)
	_ resource.ResourceWithConfigure   = (*projectResource)(nil)
	_ resource.ResourceWithImportState = (*projectResource)(nil)
)

// New is the factory consumed by provider.Resources().
func New() resource.Resource {
	return &projectResource{}
}

type projectResource struct {
	c *apiclient.Client
}

func (r *projectResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_project"
}

func (r *projectResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	if req.ProviderData == nil {
		return
	}
	c, ok := req.ProviderData.(*apiclient.Client)
	if !ok {
		resp.Diagnostics.AddError("Provider configure error",
			fmt.Sprintf("expected *apiclient.Client, got %T", req.ProviderData))
		return
	}
	r.c = c
}

func (r *projectResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var plan projectModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}

	body := apiclient.CreateProjectRequest{
		Name:   plan.Name.ValueString(),
		Region: plan.Region.ValueString(),
	}
	var out apiclient.CreateProjectResponse
	path := "/v1/orgs/" + plan.OrgSlug.ValueString() + "/projects"
	if err := r.c.Do(ctx, "POST", path, body, &out); err != nil {
		resp.Diagnostics.AddError("Create project failed", err.Error())
		return
	}
	if out.Project == nil {
		resp.Diagnostics.AddError("Create project failed", "API returned an empty project envelope.")
		return
	}
	plan.ID = types.StringValue(out.Project.ID)
	plan.OrgID = types.StringValue(out.Project.OrgID)
	plan.Ref = types.StringValue(out.Project.Ref)
	plan.Status = types.StringValue(out.Project.Status)
	plan.EngineTenantID = types.StringValue(out.Project.EngineTenantID)
	plan.CreatedAt = types.StringValue(out.Project.CreatedAt)
	if out.Pgwire != nil && out.Pgwire.ConnectionURL != "" {
		plan.ConnectionString = types.StringValue(out.Pgwire.ConnectionURL)
	} else {
		plan.ConnectionString = types.StringValue("")
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

func (r *projectResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state projectModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}
	var p apiclient.Project
	if err := r.c.Do(ctx, "GET", "/v1/projects/"+state.Ref.ValueString(), nil, &p); err != nil {
		if apiclient.IsNotFound(err) {
			resp.State.RemoveResource(ctx)
			return
		}
		resp.Diagnostics.AddError("Read project failed", err.Error())
		return
	}
	state.ID = types.StringValue(p.ID)
	state.OrgID = types.StringValue(p.OrgID)
	state.Name = types.StringValue(p.Name)
	state.Region = types.StringValue(p.Region)
	state.Ref = types.StringValue(p.Ref)
	state.Status = types.StringValue(p.Status)
	state.EngineTenantID = types.StringValue(p.EngineTenantID)
	state.CreatedAt = types.StringValue(p.CreatedAt)
	resp.Diagnostics.Append(resp.State.Set(ctx, &state)...)
}

func (r *projectResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan, state projectModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}
	name := plan.Name.ValueString()
	body := apiclient.UpdateProjectRequest{Name: &name}
	var p apiclient.Project
	if err := r.c.Do(ctx, "PATCH", "/v1/projects/"+state.Ref.ValueString(), body, &p); err != nil {
		resp.Diagnostics.AddError("Update project failed", err.Error())
		return
	}
	plan.ID = state.ID
	plan.OrgID = state.OrgID
	plan.Ref = state.Ref
	plan.Status = types.StringValue(p.Status)
	plan.EngineTenantID = state.EngineTenantID
	plan.CreatedAt = state.CreatedAt
	plan.ConnectionString = state.ConnectionString
	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

func (r *projectResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var state projectModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}
	if err := r.c.Do(ctx, "DELETE", "/v1/projects/"+state.Ref.ValueString(), nil, nil); err != nil {
		if !apiclient.IsNotFound(err) {
			resp.Diagnostics.AddError("Delete project failed", err.Error())
			return
		}
	}
}

func (r *projectResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	parts := strings.SplitN(req.ID, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		resp.Diagnostics.AddError("Invalid import ID",
			"Expected '<org_slug>/<ref>' (e.g. 'acme/api-1a2b3c4d'); got "+req.ID)
		return
	}
	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("org_slug"), parts[0])...)
	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("ref"), parts[1])...)
}
