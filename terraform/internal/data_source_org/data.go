// data "basin_org" — read-only lookup by slug.
// Surfaces id, plan, billing_email so other resources can reference them.
package data_source_org

import (
	"context"
	"fmt"

	"github.com/bas-in/terraform-provider-basin/internal/apiclient"

	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/datasource/schema"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

var (
	_ datasource.DataSource              = (*orgDataSource)(nil)
	_ datasource.DataSourceWithConfigure = (*orgDataSource)(nil)
)

func New() datasource.DataSource {
	return &orgDataSource{}
}

type orgDataSource struct {
	c *apiclient.Client
}

type orgModel struct {
	ID           types.String `tfsdk:"id"`
	Slug         types.String `tfsdk:"slug"`
	Name         types.String `tfsdk:"name"`
	Plan         types.String `tfsdk:"plan"`
	BillingEmail types.String `tfsdk:"billing_email"`
}

func (d *orgDataSource) Metadata(_ context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_org"
}

func (d *orgDataSource) Schema(_ context.Context, _ datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Look up an org by slug.",
		Attributes: map[string]schema.Attribute{
			"slug":          schema.StringAttribute{Required: true, MarkdownDescription: "Org slug (URL-safe handle)."},
			"id":            schema.StringAttribute{Computed: true, MarkdownDescription: "Org ULID."},
			"name":          schema.StringAttribute{Computed: true},
			"plan":          schema.StringAttribute{Computed: true, MarkdownDescription: "Billing plan code."},
			"billing_email": schema.StringAttribute{Computed: true},
		},
	}
}

func (d *orgDataSource) Configure(_ context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
	if req.ProviderData == nil {
		return
	}
	c, ok := req.ProviderData.(*apiclient.Client)
	if !ok {
		resp.Diagnostics.AddError("Provider configure error",
			fmt.Sprintf("expected *apiclient.Client, got %T", req.ProviderData))
		return
	}
	d.c = c
}

func (d *orgDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	var state orgModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}
	var o apiclient.Org
	if err := d.c.Do(ctx, "GET", "/v1/orgs/"+state.Slug.ValueString(), nil, &o); err != nil {
		resp.Diagnostics.AddError("Read org failed", err.Error())
		return
	}
	state.ID = types.StringValue(o.ID)
	state.Name = types.StringValue(o.Name)
	state.Plan = types.StringValue(o.Plan)
	state.BillingEmail = types.StringValue(o.BillingEmail)
	resp.Diagnostics.Append(resp.State.Set(ctx, &state)...)
}
