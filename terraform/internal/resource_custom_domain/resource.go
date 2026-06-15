// basin_custom_domain — CRUD for /v1/projects/:ref/custom-domains.
//
// Create POSTs the FQDN; the response carries the TXT record + token
// the operator must publish in DNS. Read GETs by id (status updates
// flow through this — verification, cert issuance). Update is a no-op
// (domain is RequiresReplace via the schema). Delete retires.
package resource_custom_domain

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
	_ resource.Resource                = (*customDomainResource)(nil)
	_ resource.ResourceWithConfigure   = (*customDomainResource)(nil)
	_ resource.ResourceWithImportState = (*customDomainResource)(nil)
)

func New() resource.Resource {
	return &customDomainResource{}
}

type customDomainResource struct {
	c *apiclient.Client
}

func (r *customDomainResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_custom_domain"
}

func (r *customDomainResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

func (r *customDomainResource) populate(d *apiclient.CustomDomain, m *customDomainModel) {
	m.ID = types.StringValue(d.ID)
	m.Domain = types.StringValue(d.Domain)
	m.Status = types.StringValue(d.Status)
	m.VerificationToken = types.StringValue(d.VerificationToken)
	m.TXTRecordName = types.StringValue(d.TXTRecordName)
	m.TXTRecordValue = types.StringValue(d.TXTRecordValue)
	m.CertStatus = types.StringValue(d.CertStatus)
	m.CreatedAt = types.StringValue(d.CreatedAt)
	m.VerifiedAt = types.StringValue(d.VerifiedAt)
}

func (r *customDomainResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var plan customDomainModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}
	body := apiclient.CreateCustomDomainRequest{Domain: plan.Domain.ValueString()}
	var d apiclient.CustomDomain
	urlPath := "/v1/projects/" + plan.ProjectRef.ValueString() + "/custom-domains"
	if err := r.c.Do(ctx, "POST", urlPath, body, &d); err != nil {
		resp.Diagnostics.AddError("Create custom domain failed", err.Error())
		return
	}
	r.populate(&d, &plan)
	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

func (r *customDomainResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state customDomainModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}
	var d apiclient.CustomDomain
	urlPath := "/v1/projects/" + state.ProjectRef.ValueString() + "/custom-domains/" + state.ID.ValueString()
	if err := r.c.Do(ctx, "GET", urlPath, nil, &d); err != nil {
		if apiclient.IsNotFound(err) {
			resp.State.RemoveResource(ctx)
			return
		}
		resp.Diagnostics.AddError("Read custom domain failed", err.Error())
		return
	}
	r.populate(&d, &state)
	resp.Diagnostics.Append(resp.State.Set(ctx, &state)...)
}

// Update is a no-op since domain + project_ref are both RequiresReplace.
// The framework still calls it when Computed attributes drift, so we
// just mirror the state through.
func (r *customDomainResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan customDomainModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

func (r *customDomainResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var state customDomainModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}
	urlPath := "/v1/projects/" + state.ProjectRef.ValueString() + "/custom-domains/" + state.ID.ValueString()
	if err := r.c.Do(ctx, "DELETE", urlPath, nil, nil); err != nil {
		if !apiclient.IsNotFound(err) {
			resp.Diagnostics.AddError("Delete custom domain failed", err.Error())
			return
		}
	}
}

func (r *customDomainResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	parts := strings.SplitN(req.ID, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		resp.Diagnostics.AddError("Invalid import ID",
			"Expected '<project_ref>/<domain_id>'; got "+req.ID)
		return
	}
	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("project_ref"), parts[0])...)
	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("id"), parts[1])...)
}
