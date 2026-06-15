package resource_custom_domain

import (
	"context"

	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
)

func (r *customDomainResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Custom domain mapping for a project. After create, place the TXT record returned in `txt_record_name` / `verification_token` in your DNS provider, then call POST `/v1/projects/:ref/custom-domains/:id/verify`.",
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				Computed: true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"project_ref": schema.StringAttribute{
				MarkdownDescription: "Ref of the parent project.",
				Required:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"domain": schema.StringAttribute{
				MarkdownDescription: "FQDN to attach (e.g. `api.example.com`). RequiresReplace.",
				Required:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"status": schema.StringAttribute{
				MarkdownDescription: "Lifecycle: `pending`, `verified`, `retired`.",
				Computed:            true,
			},
			"verification_token": schema.StringAttribute{
				MarkdownDescription: "Random token to include in the TXT record so basin-cloud can prove DNS control.",
				Computed:            true,
				Sensitive:           true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"txt_record_name": schema.StringAttribute{
				MarkdownDescription: "DNS name where the TXT verification record must be placed (e.g. `_basin-verify.api.example.com`).",
				Computed:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"txt_record_value": schema.StringAttribute{
				MarkdownDescription: "Full TXT record value to publish.",
				Computed:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"cert_status": schema.StringAttribute{
				MarkdownDescription: "TLS cert lifecycle: `none`, `pending`, `issued`, `failed`.",
				Computed:            true,
			},
			"created_at": schema.StringAttribute{
				Computed: true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"verified_at": schema.StringAttribute{
				MarkdownDescription: "RFC3339 timestamp when DNS verification succeeded; empty until then.",
				Computed:            true,
			},
		},
	}
}
