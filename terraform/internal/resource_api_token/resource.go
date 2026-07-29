// basin_api_token — CRUD for /v1/orgs/:slug/api-tokens.
//
// Create POSTs and persists the once-only `full_token`. Update enforces
// narrowing-only semantics client-side (matches the backend's check):
// scope may only step down (admin→write→read), project_ids may only
// shrink. Delete revokes. Import takes "<org_slug>/<id>".
package resource_api_token

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
	_ resource.Resource                = (*apiTokenResource)(nil)
	_ resource.ResourceWithConfigure   = (*apiTokenResource)(nil)
	_ resource.ResourceWithImportState = (*apiTokenResource)(nil)
)

func New() resource.Resource {
	return &apiTokenResource{}
}

type apiTokenResource struct {
	c *apiclient.Client
}

func (r *apiTokenResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_api_token"
}

func (r *apiTokenResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

// scopeRank maps a scope band to its capability number. Higher = wider.
func scopeRank(s string) int {
	switch s {
	case "read":
		return 1
	case "write":
		return 2
	case "admin":
		return 3
	default:
		return 0
	}
}

func setToStrings(ctx context.Context, s types.Set) ([]string, error) {
	if s.IsNull() || s.IsUnknown() {
		return nil, nil
	}
	var out []string
	diags := s.ElementsAs(ctx, &out, false)
	if diags.HasError() {
		return nil, fmt.Errorf("set element extraction: %v", diags)
	}
	return out, nil
}

func stringsToSet(values []string) types.Set {
	if values == nil {
		values = []string{}
	}
	elems := make([]types.String, len(values))
	for i, v := range values {
		elems[i] = types.StringValue(v)
	}
	out, _ := types.SetValueFrom(context.Background(), types.StringType, elems)
	return out
}

func (r *apiTokenResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var plan apiTokenModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}
	projectIDs, err := setToStrings(ctx, plan.ProjectIDs)
	if err != nil {
		resp.Diagnostics.AddError("project_ids decode", err.Error())
		return
	}
	ipAllow, err := setToStrings(ctx, plan.IPAllowlist)
	if err != nil {
		resp.Diagnostics.AddError("ip_allowlist decode", err.Error())
		return
	}
	body := apiclient.CreateTokenRequest{
		Name:        plan.Name.ValueString(),
		Description: plan.Description.ValueString(),
		Scope:       plan.Scope.ValueString(),
		ProjectIDs:  projectIDs,
		IPAllowlist: ipAllow,
	}
	if !plan.ExpiresAt.IsNull() && !plan.ExpiresAt.IsUnknown() && plan.ExpiresAt.ValueString() != "" {
		v := plan.ExpiresAt.ValueString()
		body.ExpiresAt = &v
	}
	var out apiclient.CreateTokenResponse
	if err := r.c.Do(ctx, "POST", "/v1/orgs/"+plan.OrgSlug.ValueString()+"/api-tokens", body, &out); err != nil {
		resp.Diagnostics.AddError("Create token failed", err.Error())
		return
	}
	if out.Token == nil {
		resp.Diagnostics.AddError("Create token failed", "API returned an empty token envelope.")
		return
	}
	plan.ID = types.StringValue(out.Token.ID)
	plan.Prefix = types.StringValue(out.Token.Prefix)
	plan.Token = types.StringValue(out.FullToken)
	plan.CreatedAt = types.StringValue(out.Token.CreatedAt)
	plan.ProjectIDs = stringsToSet(out.Token.ProjectIDs)
	plan.IPAllowlist = stringsToSet(out.Token.IPAllowlist)
	if out.Token.ExpiresAt != nil {
		plan.ExpiresAt = types.StringValue(*out.Token.ExpiresAt)
	} else if plan.ExpiresAt.IsUnknown() {
		plan.ExpiresAt = types.StringValue("")
	}
	if plan.Description.IsUnknown() || plan.Description.IsNull() {
		plan.Description = types.StringValue(out.Token.Description)
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

func (r *apiTokenResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state apiTokenModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}
	var t apiclient.APIToken
	urlPath := "/v1/orgs/" + state.OrgSlug.ValueString() + "/api-tokens/" + state.ID.ValueString()
	if err := r.c.Do(ctx, "GET", urlPath, nil, &t); err != nil {
		if apiclient.IsNotFound(err) {
			resp.State.RemoveResource(ctx)
			return
		}
		resp.Diagnostics.AddError("Read token failed", err.Error())
		return
	}
	state.Name = types.StringValue(t.Name)
	state.Description = types.StringValue(t.Description)
	state.Scope = types.StringValue(t.Scope)
	state.Prefix = types.StringValue(t.Prefix)
	state.CreatedAt = types.StringValue(t.CreatedAt)
	state.ProjectIDs = stringsToSet(t.ProjectIDs)
	state.IPAllowlist = stringsToSet(t.IPAllowlist)
	if t.ExpiresAt != nil {
		state.ExpiresAt = types.StringValue(*t.ExpiresAt)
	} else {
		state.ExpiresAt = types.StringValue("")
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, &state)...)
}

func (r *apiTokenResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan, state apiTokenModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	// Narrowing-only enforcement (mirrors the backend).
	if scopeRank(plan.Scope.ValueString()) > scopeRank(state.Scope.ValueString()) {
		resp.Diagnostics.AddAttributeError(path.Root("scope"),
			"Cannot widen token scope",
			fmt.Sprintf("Scope may only narrow (admin→write→read). Tried %s → %s. Replace the resource to widen.",
				state.Scope.ValueString(), plan.Scope.ValueString()))
		return
	}

	planProjects, err := setToStrings(ctx, plan.ProjectIDs)
	if err != nil {
		resp.Diagnostics.AddError("project_ids decode", err.Error())
		return
	}
	stateProjects, err := setToStrings(ctx, state.ProjectIDs)
	if err != nil {
		resp.Diagnostics.AddError("project_ids decode", err.Error())
		return
	}
	if len(stateProjects) > 0 {
		// state has an allowlist; plan must be a (non-empty) subset
		if len(planProjects) == 0 {
			resp.Diagnostics.AddAttributeError(path.Root("project_ids"),
				"Cannot widen project allowlist",
				"Removing the allowlist (going from N project IDs to 0) widens the token. Replace the resource to widen.")
			return
		}
		stateSet := map[string]struct{}{}
		for _, id := range stateProjects {
			stateSet[id] = struct{}{}
		}
		for _, id := range planProjects {
			if _, ok := stateSet[id]; !ok {
				resp.Diagnostics.AddAttributeError(path.Root("project_ids"),
					"Cannot widen project allowlist",
					fmt.Sprintf("Project ID %s isn't in the existing allowlist. Updates may only shrink the set.", id))
				return
			}
		}
	}

	ipAllow, err := setToStrings(ctx, plan.IPAllowlist)
	if err != nil {
		resp.Diagnostics.AddError("ip_allowlist decode", err.Error())
		return
	}

	scopePtr := plan.Scope.ValueString()
	namePtr := plan.Name.ValueString()
	descPtr := plan.Description.ValueString()
	body := apiclient.UpdateTokenRequest{
		Name:        &namePtr,
		Description: &descPtr,
		Scope:       &scopePtr,
		ProjectIDs:  &planProjects,
		IPAllowlist: &ipAllow,
	}
	if !plan.ExpiresAt.IsNull() && !plan.ExpiresAt.IsUnknown() && plan.ExpiresAt.ValueString() != "" {
		v := plan.ExpiresAt.ValueString()
		body.ExpiresAt = &v
	}
	urlPath := "/v1/orgs/" + state.OrgSlug.ValueString() + "/api-tokens/" + state.ID.ValueString()
	var t apiclient.APIToken
	if err := r.c.Do(ctx, "PATCH", urlPath, body, &t); err != nil {
		resp.Diagnostics.AddError("Update token failed", err.Error())
		return
	}
	plan.ID = state.ID
	plan.Prefix = state.Prefix
	plan.Token = state.Token
	plan.CreatedAt = state.CreatedAt
	plan.ProjectIDs = stringsToSet(t.ProjectIDs)
	plan.IPAllowlist = stringsToSet(t.IPAllowlist)
	if t.ExpiresAt != nil {
		plan.ExpiresAt = types.StringValue(*t.ExpiresAt)
	} else {
		plan.ExpiresAt = types.StringValue("")
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

func (r *apiTokenResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var state apiTokenModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}
	urlPath := "/v1/orgs/" + state.OrgSlug.ValueString() + "/api-tokens/" + state.ID.ValueString()
	if err := r.c.Do(ctx, "DELETE", urlPath, nil, nil); err != nil {
		if !apiclient.IsNotFound(err) {
			resp.Diagnostics.AddError("Revoke token failed", err.Error())
			return
		}
	}
}

func (r *apiTokenResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	parts := strings.SplitN(req.ID, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		resp.Diagnostics.AddError("Invalid import ID",
			"Expected '<org_slug>/<token_id>'; got "+req.ID)
		return
	}
	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("org_slug"), parts[0])...)
	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("id"), parts[1])...)
}
