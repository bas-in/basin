// Provider entrypoint — wires terraform-plugin-framework to the
// basin-cloud REST API.
//
// Configuration is provider-block-or-env: api_url defaults to
// https://api.<BASIN_DOMAIN> (basin.run if unset), token falls back
// to BASIN_TOKEN. We validate the PAT at Configure time by calling
// GET /v1/me; a 401 surfaces as a clean diagnostic instead of a
// per-resource explosion during plan.
package provider

import (
	"context"
	"os"

	"github.com/bas-in/terraform-provider-basin/internal/apiclient"
	"github.com/bas-in/terraform-provider-basin/internal/data_source_engine_versions"
	"github.com/bas-in/terraform-provider-basin/internal/data_source_org"
	"github.com/bas-in/terraform-provider-basin/internal/data_source_project"
	"github.com/bas-in/terraform-provider-basin/internal/resource_api_token"
	"github.com/bas-in/terraform-provider-basin/internal/resource_custom_domain"
	"github.com/bas-in/terraform-provider-basin/internal/resource_project"
	"github.com/bas-in/terraform-provider-basin/internal/resource_webhook"

	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/provider"
	"github.com/hashicorp/terraform-plugin-framework/provider/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

// defaultAPIURL is computed lazily from BASIN_DOMAIN with a "basin.run"
// fallback so a fresh checkout still resolves to the canonical brand.
func defaultAPIURL() string {
	d := os.Getenv("BASIN_DOMAIN")
	if d == "" {
		d = "basin.run"
	}
	return "https://api." + d
}

// New is the provider factory consumed by main and by the acceptance
// tests' `providerserver.NewProtocol6WithError` adapter.
func New(version string) func() provider.Provider {
	return func() provider.Provider {
		return &basinProvider{version: version}
	}
}

type basinProvider struct {
	version string
}

type basinProviderModel struct {
	APIURL types.String `tfsdk:"api_url"`
	Token  types.String `tfsdk:"token"`
}

func (p *basinProvider) Metadata(_ context.Context, _ provider.MetadataRequest, resp *provider.MetadataResponse) {
	resp.TypeName = "basin"
	resp.Version = p.version
}

func (p *basinProvider) Schema(_ context.Context, _ provider.SchemaRequest, resp *provider.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Manage basin-cloud projects, API tokens, webhooks, and custom domains via Terraform.",
		Attributes: map[string]schema.Attribute{
			"api_url": schema.StringAttribute{
				MarkdownDescription: "Base URL of the basin-cloud API. Defaults to `https://api.<BASIN_DOMAIN>` (set via the `BASIN_DOMAIN` env var; falls back to `https://api.basin.run`). Override for self-hosted deployments.",
				Optional:            true,
			},
			"token": schema.StringAttribute{
				MarkdownDescription: "Personal access token (`bso_org_…`). Falls back to the `BASIN_TOKEN` environment variable.",
				Optional:            true,
				Sensitive:           true,
			},
		},
	}
}

func (p *basinProvider) Configure(ctx context.Context, req provider.ConfigureRequest, resp *provider.ConfigureResponse) {
	var cfg basinProviderModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &cfg)...)
	if resp.Diagnostics.HasError() {
		return
	}

	apiURL := defaultAPIURL()
	if !cfg.APIURL.IsNull() && !cfg.APIURL.IsUnknown() && cfg.APIURL.ValueString() != "" {
		apiURL = cfg.APIURL.ValueString()
	} else if v := os.Getenv("BASIN_API_URL"); v != "" {
		apiURL = v
	}

	token := ""
	if !cfg.Token.IsNull() && !cfg.Token.IsUnknown() {
		token = cfg.Token.ValueString()
	}
	if token == "" {
		token = os.Getenv("BASIN_TOKEN")
	}
	if token == "" {
		resp.Diagnostics.AddError(
			"Missing basin-cloud token",
			"Set `token` in the provider block or export BASIN_TOKEN. Tokens look like `bso_org_<32 b64u>`.",
		)
		return
	}

	c := apiclient.NewClient(apiURL, token)

	// Validate at Configure time — GET /v1/me. A 401 here surfaces a
	// clean diagnostic instead of every resource erroring during plan.
	if !skipPing() {
		var u apiclient.User
		if err := c.Do(ctx, "GET", "/v1/me", nil, &u); err != nil {
			if ae, ok := apiclient.AsAPIError(err); ok && ae.HTTPStatus == 401 {
				resp.Diagnostics.AddError(
					"basin-cloud rejected the token (401)",
					"The PAT in `token` (or BASIN_TOKEN) was rejected. Verify it hasn't been revoked, isn't expired, and your IP is in the token's allowlist.",
				)
				return
			}
			resp.Diagnostics.AddError(
				"Could not reach basin-cloud at "+apiURL,
				err.Error(),
			)
			return
		}
	}

	// Hand the client to every resource + data source. Each
	// resource_*/data_* package's Configure pulls it out via type
	// assertion against *apiclient.Client.
	resp.ResourceData = c
	resp.DataSourceData = c
}

// skipPing lets the acceptance tests bypass the GET /v1/me probe
// (the mock server still implements it, but we expose this knob for
// CI environments using a partial mock).
func skipPing() bool {
	return os.Getenv("BASIN_PROVIDER_SKIP_PING") == "1"
}

func (p *basinProvider) Resources(_ context.Context) []func() resource.Resource {
	return []func() resource.Resource{
		resource_project.New,
		resource_api_token.New,
		resource_webhook.New,
		resource_custom_domain.New,
	}
}

func (p *basinProvider) DataSources(_ context.Context) []func() datasource.DataSource {
	return []func() datasource.DataSource{
		data_source_org.New,
		data_source_project.New,
		data_source_engine_versions.New,
	}
}
