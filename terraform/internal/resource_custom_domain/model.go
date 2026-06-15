package resource_custom_domain

import "github.com/hashicorp/terraform-plugin-framework/types"

type customDomainModel struct {
	ID                types.String `tfsdk:"id"`
	ProjectRef        types.String `tfsdk:"project_ref"`
	Domain            types.String `tfsdk:"domain"`
	Status            types.String `tfsdk:"status"`
	VerificationToken types.String `tfsdk:"verification_token"`
	TXTRecordName     types.String `tfsdk:"txt_record_name"`
	TXTRecordValue    types.String `tfsdk:"txt_record_value"`
	CertStatus        types.String `tfsdk:"cert_status"`
	CreatedAt         types.String `tfsdk:"created_at"`
	VerifiedAt        types.String `tfsdk:"verified_at"`
}
