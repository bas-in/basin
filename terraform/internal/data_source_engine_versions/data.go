// data "basin_engine_versions" — list of engine versions catalog rows.
package data_source_engine_versions

import (
	"context"
	"fmt"

	"github.com/bas-in/terraform-provider-basin/internal/apiclient"

	"github.com/hashicorp/terraform-plugin-framework/attr"
	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/datasource/schema"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

var (
	_ datasource.DataSource              = (*engineVersionsDataSource)(nil)
	_ datasource.DataSourceWithConfigure = (*engineVersionsDataSource)(nil)
)

func New() datasource.DataSource {
	return &engineVersionsDataSource{}
}

type engineVersionsDataSource struct {
	c *apiclient.Client
}

type model struct {
	ID       types.String `tfsdk:"id"`
	Versions types.List   `tfsdk:"versions"`
}

var versionObjectType = types.ObjectType{
	AttrTypes: map[string]attr.Type{
		"version":           types.StringType,
		"channel":           types.StringType,
		"released_at":       types.StringType,
		"eol_at":            types.StringType,
		"release_notes_url": types.StringType,
		"is_recommended":    types.BoolType,
		"notes":             types.StringType,
	},
}

func (d *engineVersionsDataSource) Metadata(_ context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_engine_versions"
}

func (d *engineVersionsDataSource) Schema(_ context.Context, _ datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "List of engine versions in the catalog (channels, recommended flag, EOL dates).",
		Attributes: map[string]schema.Attribute{
			"id":       schema.StringAttribute{Computed: true, MarkdownDescription: "Stable identifier (`engine-versions`)."},
			"versions": schema.ListNestedAttribute{
				Computed: true,
				NestedObject: schema.NestedAttributeObject{
					Attributes: map[string]schema.Attribute{
						"version":           schema.StringAttribute{Computed: true},
						"channel":           schema.StringAttribute{Computed: true},
						"released_at":       schema.StringAttribute{Computed: true},
						"eol_at":            schema.StringAttribute{Computed: true, MarkdownDescription: "Empty if no EOL set."},
						"release_notes_url": schema.StringAttribute{Computed: true},
						"is_recommended":    schema.BoolAttribute{Computed: true},
						"notes":             schema.StringAttribute{Computed: true},
					},
				},
			},
		},
	}
}

func (d *engineVersionsDataSource) Configure(_ context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
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

func (d *engineVersionsDataSource) Read(ctx context.Context, _ datasource.ReadRequest, resp *datasource.ReadResponse) {
	var out struct {
		Items []apiclient.EngineVersion `json:"items"`
	}
	if err := d.c.Do(ctx, "GET", "/v1/admin/engine-versions", nil, &out); err != nil {
		resp.Diagnostics.AddError("Read engine versions failed", err.Error())
		return
	}
	values := make([]attr.Value, 0, len(out.Items))
	for _, v := range out.Items {
		eol := ""
		if v.EOLAt != nil {
			eol = *v.EOLAt
		}
		obj, diags := types.ObjectValue(versionObjectType.AttrTypes, map[string]attr.Value{
			"version":           types.StringValue(v.Version),
			"channel":           types.StringValue(v.Channel),
			"released_at":       types.StringValue(v.ReleasedAt),
			"eol_at":            types.StringValue(eol),
			"release_notes_url": types.StringValue(v.ReleaseNotesURL),
			"is_recommended":    types.BoolValue(v.IsRecommended),
			"notes":             types.StringValue(v.Notes),
		})
		resp.Diagnostics.Append(diags...)
		values = append(values, obj)
	}
	listVal, diags := types.ListValue(versionObjectType, values)
	resp.Diagnostics.Append(diags...)
	state := model{
		ID:       types.StringValue("engine-versions"),
		Versions: listVal,
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, &state)...)
}
