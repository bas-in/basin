package resource_webhook

import "github.com/hashicorp/terraform-plugin-framework/types"

type webhookModel struct {
	ID         types.String `tfsdk:"id"`
	ProjectRef types.String `tfsdk:"project_ref"`
	URL        types.String `tfsdk:"url"`
	Events     types.Set    `tfsdk:"events"`
	Active     types.Bool   `tfsdk:"active"`
	Secret     types.String `tfsdk:"secret"`
	SecretSet  types.Bool   `tfsdk:"secret_set"`
	CreatedAt  types.String `tfsdk:"created_at"`
}
