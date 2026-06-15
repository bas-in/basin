// data "basin_project" — read-only lookup by ref.
package data_source_project

import (
	"context"
	"fmt"

	"github.com/bas-in/terraform-provider-basin/internal/apiclient"

	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/datasource/schema"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

var (
	_ datasource.DataSource              = (*projectDataSource)(nil)
	_ datasource.DataSourceWithConfigure = (*projectDataSource)(nil)
)

func New() datasource.DataSource {
	return &projectDataSource{}
}

type projectDataSource struct {
	c *apiclient.Client
}

type model struct {
	Ref            types.String `tfsdk:"ref"`
	ID             types.String `tfsdk:"id"`
	OrgID          types.String `tfsdk:"org_id"`
	Name           types.String `tfsdk:"name"`
	Region         types.String `tfsdk:"region"`
	Status         types.String `tfsdk:"status"`
	EngineTenantID types.String `tfsdk:"engine_tenant_id"`
	CreatedAt      types.String `tfsdk:"created_at"`
}

func (d *projectDataSource) Metadata(_ context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_project"
}

func (d *projectDataSource) Schema(_ context.Context, _ datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Look up a project by ref.",
		Attributes: map[string]schema.Attribute{
			"ref":              schema.StringAttribute{Required: true, MarkdownDescription: "Project ref (`<slug>-<8-hex>`)."},
			"id":               schema.StringAttribute{Computed: true},
			"org_id":           schema.StringAttribute{Computed: true},
			"name":             schema.StringAttribute{Computed: true},
			"region":           schema.StringAttribute{Computed: true},
			"status":           schema.StringAttribute{Computed: true},
			"engine_tenant_id": schema.StringAttribute{Computed: true},
			"created_at":       schema.StringAttribute{Computed: true},
		},
	}
}

func (d *projectDataSource) Configure(_ context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
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

func (d *projectDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	var state model
	resp.Diagnostics.Append(req.Config.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}
	var p apiclient.Project
	if err := d.c.Do(ctx, "GET", "/v1/projects/"+state.Ref.ValueString(), nil, &p); err != nil {
		resp.Diagnostics.AddError("Read project failed", err.Error())
		return
	}
	state.ID = types.StringValue(p.ID)
	state.OrgID = types.StringValue(p.OrgID)
	state.Name = types.StringValue(p.Name)
	state.Region = types.StringValue(p.Region)
	state.Status = types.StringValue(p.Status)
	state.EngineTenantID = types.StringValue(p.EngineTenantID)
	state.CreatedAt = types.StringValue(p.CreatedAt)
	resp.Diagnostics.Append(resp.State.Set(ctx, &state)...)
}
