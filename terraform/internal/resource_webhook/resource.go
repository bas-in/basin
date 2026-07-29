// basin_webhook — CRUD for /v1/projects/:ref/webhooks.
//
// Create POSTs URL+events; the response carries a one-time-reveal
// `secret_plain` that we store in state (sensitive). Update PATCHes
// url/events/active. Delete revokes. Import takes "<project_ref>/<id>".
package resource_webhook

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
	_ resource.Resource                = (*webhookResource)(nil)
	_ resource.ResourceWithConfigure   = (*webhookResource)(nil)
	_ resource.ResourceWithImportState = (*webhookResource)(nil)
)

func New() resource.Resource {
	return &webhookResource{}
}

type webhookResource struct {
	c *apiclient.Client
}

func (r *webhookResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_webhook"
}

func (r *webhookResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

func (r *webhookResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var plan webhookModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}
	events, err := setToStrings(ctx, plan.Events)
	if err != nil {
		resp.Diagnostics.AddError("events decode", err.Error())
		return
	}
	body := apiclient.CreateWebhookRequest{
		URL:    plan.URL.ValueString(),
		Events: events,
	}
	var out apiclient.CreateWebhookResponse
	urlPath := "/v1/projects/" + plan.ProjectRef.ValueString() + "/webhooks"
	if err := r.c.Do(ctx, "POST", urlPath, body, &out); err != nil {
		resp.Diagnostics.AddError("Create webhook failed", err.Error())
		return
	}
	if out.Webhook == nil {
		resp.Diagnostics.AddError("Create webhook failed", "API returned an empty webhook envelope.")
		return
	}
	plan.ID = types.StringValue(out.Webhook.ID)
	plan.Active = types.BoolValue(out.Webhook.Active)
	plan.SecretSet = types.BoolValue(out.Webhook.SecretSet)
	plan.Secret = types.StringValue(out.SecretPlain)
	plan.CreatedAt = types.StringValue(out.Webhook.CreatedAt)
	plan.Events = stringsToSet(out.Webhook.Events)
	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

func (r *webhookResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state webhookModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}
	var w apiclient.Webhook
	urlPath := "/v1/projects/" + state.ProjectRef.ValueString() + "/webhooks/" + state.ID.ValueString()
	if err := r.c.Do(ctx, "GET", urlPath, nil, &w); err != nil {
		if apiclient.IsNotFound(err) {
			resp.State.RemoveResource(ctx)
			return
		}
		resp.Diagnostics.AddError("Read webhook failed", err.Error())
		return
	}
	state.URL = types.StringValue(w.URL)
	state.Active = types.BoolValue(w.Active)
	state.SecretSet = types.BoolValue(w.SecretSet)
	state.CreatedAt = types.StringValue(w.CreatedAt)
	state.Events = stringsToSet(w.Events)
	resp.Diagnostics.Append(resp.State.Set(ctx, &state)...)
}

func (r *webhookResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan, state webhookModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}
	url := plan.URL.ValueString()
	active := plan.Active.ValueBool()
	events, err := setToStrings(ctx, plan.Events)
	if err != nil {
		resp.Diagnostics.AddError("events decode", err.Error())
		return
	}
	body := apiclient.PatchWebhookRequest{
		URL:    &url,
		Events: &events,
		Active: &active,
	}
	var w apiclient.Webhook
	urlPath := "/v1/projects/" + state.ProjectRef.ValueString() + "/webhooks/" + state.ID.ValueString()
	if err := r.c.Do(ctx, "PATCH", urlPath, body, &w); err != nil {
		resp.Diagnostics.AddError("Update webhook failed", err.Error())
		return
	}
	plan.ID = state.ID
	plan.Secret = state.Secret
	plan.SecretSet = types.BoolValue(w.SecretSet)
	plan.CreatedAt = state.CreatedAt
	plan.Events = stringsToSet(w.Events)
	plan.Active = types.BoolValue(w.Active)
	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

func (r *webhookResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var state webhookModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}
	urlPath := "/v1/projects/" + state.ProjectRef.ValueString() + "/webhooks/" + state.ID.ValueString()
	if err := r.c.Do(ctx, "DELETE", urlPath, nil, nil); err != nil {
		if !apiclient.IsNotFound(err) {
			resp.Diagnostics.AddError("Delete webhook failed", err.Error())
			return
		}
	}
}

func (r *webhookResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	parts := strings.SplitN(req.ID, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		resp.Diagnostics.AddError("Invalid import ID",
			"Expected '<project_ref>/<webhook_id>'; got "+req.ID)
		return
	}
	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("project_ref"), parts[0])...)
	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("id"), parts[1])...)
}
